package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"
	"kvraft/pkg/raft"
	"kvraft/pkg/wal"
	"kvraft/pkg/watch"
)

type Op struct {
	Me  int   // 发起请求的服务器 id
	Id  int64 // 每次为一个请求生成一个唯一的 id
	Req any   // 请求内容
}

func opEquals(a *Op, b *Op) bool {
	return a.Me == b.Me && a.Id == b.Id
}

// StateMachine 是状态机接口。应用层必须实现 DoOp 以执行操作（如 Get/Put 请求），
// 并通过 Snapshot/Restore 实现快照功能以持久化和恢复状态。
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type waitingOp struct {
	oper   Op
	result any
	done   chan bool
}

// RSM 是复制状态机，管理 Raft 共识、快照、WAL 和 Watch 的生命周期。
type RSM struct {
	mu            sync.Mutex
	me            int
	rf            raft.Node
	applyCh       chan raft.ApplyMsg
	maxraftstate  int
	sm            StateMachine
	persister     raft.Persister
	idCounter     int64
	waitingOps    map[int]*waitingOp
	shutdown      atomic.Bool
	watchMgr      *watch.Manager
	opListener    OpCompleteListener
	walLogger     *wal.Logger
	leaseRead     atomic.Bool
	walGCInFly    atomic.Bool
	lastSnapIndex int
	lastApplied   int
	lastSnapAt    time.Time
	snapMinDelta  int
	snapMinGap    time.Duration

	applyLoopBlockedNanos int64
	applyLoopProcessNanos int64
	applyLoopIterCount    int64
}

func leaseReadEnabledFromEnv() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("KV_LEASE_READ")))
	if v == "" {
		return true
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func boolEnvDefault(name string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

// Close 优雅关闭 RSM 相关后台组件。
func (rsm *RSM) Close() {
	if !rsm.shutdown.CompareAndSwap(false, true) {
		return
	}

	if rsm.rf != nil {
		rsm.rf.Kill()
	}
	if rsm.watchMgr != nil {
		rsm.watchMgr.Close()
	}
	if rsm.walLogger != nil {
		if err := rsm.walLogger.Close(); err != nil {
			log.Printf("[RSM-%d] close WAL failed: %v", rsm.me, err)
		}
	}

	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	for _, wop := range rsm.waitingOps {
		select {
		case wop.done <- false:
		default:
		}
	}
	rsm.waitingOps = make(map[int]*waitingOp)
}

// MakeRSM 创建复制状态机实例。应快速返回，后台 goroutine 进行长期运行的工作。
func MakeRSM(
	peers []string,
	me int,
	persister raft.Persister,
	maxraftstate int,
	sm StateMachine,
) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raft.ApplyMsg),
		sm:           sm,
		persister:    persister,
		idCounter:    0,
		waitingOps:   make(map[int]*waitingOp),
		watchMgr:     watch.NewManager(watch.DefaultConfig()),
		snapMinDelta: 64,
		snapMinGap:   200 * time.Millisecond,
	}
	rsm.shutdown.Store(false)
	rsm.leaseRead.Store(leaseReadEnabledFromEnv())
	walPath := filepath.Join(runtimeDataRoot(), "wal", fmt.Sprintf("rsm-node-%d.log", me))
	walEnabled := boolEnvDefault("KV_WAL_ENABLED", true)
	walSync := boolEnvDefault("KV_WAL_SYNC", true)
	walLogger, err := wal.NewLogger(walPath, walEnabled, walSync)
	if err != nil {
		log.Printf("[RSM-%d] WAL disabled due to init error: %v", me, err)
	} else {
		rsm.walLogger = walLogger
		log.Printf("[RSM-%d] wal enabled=%v sync=%v", me, walEnabled, walSync)
	}
	rsm.rf = raft.Make(peers, me, persister, rsm.applyCh)
	log.Printf("[RSM-%d] lease-read enabled=%v", me, rsm.leaseRead.Load())
	rsm.lastSnapAt = time.Now()
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		r := bytes.NewBuffer(snapshot)
		d := gob.NewDecoder(r)
		var idctr int64
		var smSnapshot []byte
		if d.Decode(&idctr) != nil ||
			d.Decode(&smSnapshot) != nil {
			panic("RSM unable to read snapshot")
		}
		atomic.StoreInt64(&rsm.idCounter, idctr)
		rsm.sm.Restore(smSnapshot)
		rsm.lastSnapAt = time.Now()
	}

	rsm.lastSnapIndex = rsm.rf.GetLastIncludedIndex()
	rsm.lastApplied = rsm.lastSnapIndex

	if rsm.walLogger != nil && rsm.walLogger.Enabled() {
		recoveredIndex, err := rsm.recoverFromWAL(rsm.lastSnapIndex)
		if err != nil {
			panic(fmt.Sprintf("RSM WAL replay failed: %v", err))
		}
		rsm.rf.SyncAppliedIndex(recoveredIndex)
		rsm.lastApplied = recoveredIndex
	}
	go rsm.applyLoop()
	return rsm
}

func (rsm *RSM) genID() int64 {
	return atomic.AddInt64(&rsm.idCounter, 1)
}

func (rsm *RSM) IsLeaderWithLease() bool {
	if rf, ok := rsm.rf.(*raft.Raft); ok {
		return rf.IsLeaderWithLease()
	}
	_, isLeader := rsm.rf.GetState()
	return isLeader
}

func (rsm *RSM) RaftPerfStatsSnapshot() raft.RaftPerfStats {
	if rf, ok := rsm.rf.(*raft.Raft); ok {
		return rf.PerfStatsSnapshot()
	}
	return raft.RaftPerfStats{}
}

func (rsm *RSM) GetState() (int, bool) {
	return rsm.rf.GetState()
}

func (rsm *RSM) GetLastApplied() int {
	return rsm.rf.GetLastApplied()
}

func (rsm *RSM) EnableLeaseRead(enable bool) {
	rsm.leaseRead.Store(enable)
}

// SubmitLeaseRead tries local lease read first, then falls back to consensus path.
func (rsm *RSM) SubmitLeaseRead(req any) (Err, any) {
	err, ret, _ := rsm.SubmitLeaseReadWithMode(req)
	return err, ret
}

// SubmitLeaseReadWithMode returns whether the request was served via local lease read.
func (rsm *RSM) SubmitLeaseReadWithMode(req any) (Err, any, bool) {
	if !rsm.leaseRead.Load() {
		err, ret := rsm.Submit(req)
		return err, ret, false
	}
	if rsm.IsLeaderWithLease() {
		result := rsm.sm.DoOp(req)
		return OK, result, true
	}
	err, ret := rsm.Submit(req)
	return err, ret, false
}

// reqPtr normalizes a request value to its pointer form for uniform type switching.
func reqPtr[T any](v any) *T {
	switch t := v.(type) {
	case T:
		return &t
	case *T:
		return t
	default:
		return nil
	}
}

func walEntryFromOp(me int, commandIndex int, term int, oper Op) wal.Entry {
	entry := wal.Entry{
		RaftIndex: int64(commandIndex),
		Term:      term,
		NodeID:    me,
		ReqID:     oper.Id,
		Timestamp: time.Now().UnixNano(),
	}
	switch oper.Req.(type) {
	case *GetArgs, GetArgs:
		t := reqPtr[GetArgs](oper.Req)
		entry.OpType = "GET"
		entry.Key = t.Key
	case *ScanArgs, ScanArgs:
		entry.OpType = "SCAN"
	case *PutArgs, PutArgs:
		t := reqPtr[PutArgs](oper.Req)
		entry.OpType = "PUT"
		entry.Key = t.Key
		entry.Value = t.Value
		entry.Version = int64(t.Version)
		entry.TTL = t.TTL
	case *DeleteArgs, DeleteArgs:
		t := reqPtr[DeleteArgs](oper.Req)
		entry.OpType = "DELETE"
		entry.Key = t.Key
	case *ExpireArgs, ExpireArgs:
		t := reqPtr[ExpireArgs](oper.Req)
		entry.OpType = "EXPIRE"
		entry.Keys = append([]string(nil), t.Keys...)
		entry.Cutoff = t.Cutoff
	case *CleanupShardArgs, CleanupShardArgs:
		t := reqPtr[CleanupShardArgs](oper.Req)
		entry.OpType = "CLEANUP_SHARD"
		entry.Version = int64(t.ShardID)
		entry.Keys = make([]string, 0, len(t.Keys))
		entry.Versions = make([]int64, 0, len(t.Keys))
		for _, key := range t.Keys {
			entry.Keys = append(entry.Keys, key.Key)
			entry.Versions = append(entry.Versions, int64(key.ExpectedVersion))
		}
	case *SetShardStateArgs, SetShardStateArgs:
		t := reqPtr[SetShardStateArgs](oper.Req)
		entry.OpType = "SET_SHARD_STATE"
		entry.ShardID = t.ShardID
		entry.ShardState = int32(t.State)
		entry.TargetGroup = t.TargetGroup
		entry.Epoch = t.TopologyEpoch
		entry.Replicas = append([]string(nil), t.TargetReplicas...)
	case *PrepareTxArgs, PrepareTxArgs:
		t := reqPtr[PrepareTxArgs](oper.Req)
		entry.OpType = "PREPARE_TX"
		entry.Key = t.TxID
		entry.TxReadKeys = make([]string, 0, len(t.ReadKeys))
		entry.TxReadVersions = make([]int64, 0, len(t.ReadKeys))
		for _, k := range t.ReadKeys {
			entry.TxReadKeys = append(entry.TxReadKeys, k.Key)
			entry.TxReadVersions = append(entry.TxReadVersions, int64(k.ExpectedVersion))
		}
		entry.TxWriteKeys = make([]string, 0, len(t.WriteKeys))
		entry.TxWriteValues = make([]string, 0, len(t.WriteKeys))
		entry.TxWriteVersions = make([]int64, 0, len(t.WriteKeys))
		entry.TxWriteDeletes = make([]bool, 0, len(t.WriteKeys))
		for _, k := range t.WriteKeys {
			entry.TxWriteKeys = append(entry.TxWriteKeys, k.Key)
			entry.TxWriteValues = append(entry.TxWriteValues, k.Value)
			entry.TxWriteVersions = append(entry.TxWriteVersions, int64(k.Version))
			entry.TxWriteDeletes = append(entry.TxWriteDeletes, k.IsDelete)
		}
		entry.TxTimeoutMs = t.TimeoutMs
	case *CommitTxArgs, CommitTxArgs:
		t := reqPtr[CommitTxArgs](oper.Req)
		entry.OpType = "COMMIT_TX"
		entry.Key = t.TxID
		entry.TxWriteKeys = make([]string, 0, len(t.WriteKeys))
		entry.TxWriteValues = make([]string, 0, len(t.WriteKeys))
		entry.TxWriteVersions = make([]int64, 0, len(t.WriteKeys))
		entry.TxWriteDeletes = make([]bool, 0, len(t.WriteKeys))
		for _, k := range t.WriteKeys {
			entry.TxWriteKeys = append(entry.TxWriteKeys, k.Key)
			entry.TxWriteValues = append(entry.TxWriteValues, k.Value)
			entry.TxWriteVersions = append(entry.TxWriteVersions, int64(k.Version))
			entry.TxWriteDeletes = append(entry.TxWriteDeletes, k.IsDelete)
		}
	case *AbortTxArgs, AbortTxArgs:
		t := reqPtr[AbortTxArgs](oper.Req)
		entry.OpType = "ABORT_TX"
		entry.Key = t.TxID
	case *ResolveTxStatusArgs, ResolveTxStatusArgs:
		t := reqPtr[ResolveTxStatusArgs](oper.Req)
		entry.OpType = "RESOLVE_TX_STATUS"
		entry.Key = t.TxID
	default:
		entry.OpType = fmt.Sprintf("%T", oper.Req)
	}
	return entry
}

func walEntryToRequest(entry wal.Entry) (any, bool, error) {
	opType := strings.ToUpper(strings.TrimSpace(entry.OpType))
	switch opType {
	case "GET":
		return nil, false, nil
	case "SCAN":
		return nil, false, nil
	case "PUT":
		return &PutArgs{
			Key:     entry.Key,
			Value:   entry.Value,
			Version: Tversion(entry.Version),
			TTL:     entry.TTL,
		}, true, nil
	case "DELETE":
		return &DeleteArgs{Key: entry.Key}, true, nil
	case "EXPIRE":
		return &ExpireArgs{
			Keys:   append([]string(nil), entry.Keys...),
			Cutoff: entry.Cutoff,
		}, true, nil
	case "CLEANUP_SHARD":
		if len(entry.Keys) != len(entry.Versions) {
			return nil, false, fmt.Errorf("invalid cleanup shard WAL entry")
		}
		keys := make([]CleanupShardKey, 0, len(entry.Keys))
		for i, key := range entry.Keys {
			keys = append(keys, CleanupShardKey{Key: key, ExpectedVersion: Tversion(entry.Versions[i])})
		}
		return &CleanupShardArgs{ShardID: int(entry.Version), Keys: keys}, true, nil
	case "SET_SHARD_STATE":
		return &SetShardStateArgs{ShardID: entry.ShardID, State: pb.ShardState(entry.ShardState), TargetGroup: entry.TargetGroup, TopologyEpoch: entry.Epoch, TargetReplicas: append([]string(nil), entry.Replicas...)}, true, nil
	case "PREPARE_TX":
		if len(entry.TxReadKeys) != len(entry.TxReadVersions) || len(entry.TxWriteKeys) != len(entry.TxWriteValues) || len(entry.TxWriteKeys) != len(entry.TxWriteVersions) || len(entry.TxWriteKeys) != len(entry.TxWriteDeletes) {
			return nil, false, fmt.Errorf("invalid prepare tx WAL entry")
		}
		reads := make([]ReadKey, 0, len(entry.TxReadKeys))
		for i, key := range entry.TxReadKeys {
			reads = append(reads, ReadKey{Key: key, ExpectedVersion: Tversion(entry.TxReadVersions[i])})
		}
		writes := make([]WriteKey, 0, len(entry.TxWriteKeys))
		for i, key := range entry.TxWriteKeys {
			writes = append(writes, WriteKey{Key: key, Value: entry.TxWriteValues[i], Version: Tversion(entry.TxWriteVersions[i]), IsDelete: entry.TxWriteDeletes[i]})
		}
		return &PrepareTxArgs{TxID: entry.Key, ReadKeys: reads, WriteKeys: writes, TimeoutMs: entry.TxTimeoutMs}, true, nil
	case "COMMIT_TX":
		if len(entry.TxWriteKeys) != len(entry.TxWriteValues) || len(entry.TxWriteKeys) != len(entry.TxWriteVersions) || len(entry.TxWriteKeys) != len(entry.TxWriteDeletes) {
			return nil, false, fmt.Errorf("invalid commit tx WAL entry")
		}
		writes := make([]WriteKey, 0, len(entry.TxWriteKeys))
		for i, key := range entry.TxWriteKeys {
			writes = append(writes, WriteKey{Key: key, Value: entry.TxWriteValues[i], Version: Tversion(entry.TxWriteVersions[i]), IsDelete: entry.TxWriteDeletes[i]})
		}
		return &CommitTxArgs{TxID: entry.Key, WriteKeys: writes}, true, nil
	case "ABORT_TX":
		return &AbortTxArgs{TxID: entry.Key}, true, nil
	case "RESOLVE_TX_STATUS":
		return &ResolveTxStatusArgs{TxID: entry.Key}, false, nil
	default:
		if strings.Contains(entry.OpType, "GetArgs") || strings.Contains(entry.OpType, "ScanArgs") {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("unsupported WAL op type: %s", entry.OpType)
	}
}

func (rsm *RSM) recoverFromWAL(snapshotIndex int) (int, error) {
	recoveredIndex := snapshotIndex
	err := rsm.walLogger.Replay(func(entry wal.Entry) error {
		entryIndex := int(entry.RaftIndex)
		if entryIndex <= snapshotIndex {
			return nil
		}
		if entryIndex <= recoveredIndex {
			return nil
		}

		req, mutates, err := walEntryToRequest(entry)
		if err != nil {
			return err
		}
		if mutates {
			rsm.sm.DoOp(req)
		}
		recoveredIndex = entryIndex
		return nil
	})
	if err != nil {
		return snapshotIndex, err
	}
	return recoveredIndex, nil
}

func (rsm *RSM) truncateWALAsync(upToIndex int) {
	if upToIndex <= 0 || rsm.walLogger == nil || !rsm.walLogger.Enabled() {
		return
	}
	if !rsm.walGCInFly.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer rsm.walGCInFly.Store(false)
		if err := rsm.walLogger.TruncateUpTo(int64(upToIndex)); err != nil {
			log.Printf("[RSM-%d] WAL truncate failed upTo=%d: %v", rsm.me, upToIndex, err)
		}
	}()
}

// GetWatchManager 返回 Watch 管理器
func (rsm *RSM) GetWatchManager() *watch.Manager {
	return rsm.watchMgr
}

// RegisterOpCompleteListener 注册操作完成监听器
func (rsm *RSM) RegisterOpCompleteListener(listener OpCompleteListener) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	rsm.opListener = listener
}

func (rsm *RSM) ApplyLoopPerfStatsSnapshot() ApplyLoopPerfStats {
	blockedNanos := atomic.LoadInt64(&rsm.applyLoopBlockedNanos)
	processNanos := atomic.LoadInt64(&rsm.applyLoopProcessNanos)
	iterCount := atomic.LoadInt64(&rsm.applyLoopIterCount)

	avg := func(total int64, count int64) float64 {
		if count <= 0 {
			return 0
		}
		return float64(total) / float64(count)
	}

	return ApplyLoopPerfStats{
		BlockedNanos:    blockedNanos,
		ProcessNanos:    processNanos,
		IterationCount:  iterCount,
		BlockedAvgNanos: avg(blockedNanos, iterCount),
		ProcessAvgNanos: avg(processNanos, iterCount),
	}
}

// Submit 向 Raft 提交一条命令并等待其被提交。
// 包含 per-key 冲突检测：如果该 key 已有待处理写操作，直接返回 ErrVersion 避免无效的 Raft 提案。
// Submit 向 Raft 提交一条命令并等待其被提交。
// 如果当前节点不是 Leader，返回 ErrWrongLeader；客户端应重新查找 Leader 后重试。
func (rsm *RSM) Submit(req any) (Err, any) {
	if rsm.shutdown.Load() {
		return ErrWrongLeader, nil
	}

	opID := rsm.genID()
	oper := Op{Me: rsm.me, Id: opID, Req: req}
	index, _, isLeader := rsm.rf.Start(oper)
	if !isLeader {
		return ErrWrongLeader, nil
	}
	waitingOp := &waitingOp{
		oper: oper,
		done: make(chan bool, 1),
	}
	rsm.mu.Lock()
	if wop, exists := rsm.waitingOps[index]; exists {
		select {
		case wop.done <- false:
		default:
		}
	}
	rsm.waitingOps[index] = waitingOp
	rsm.mu.Unlock()

	err, result := func() (Err, any) {
		timer := time.NewTimer(1500 * time.Millisecond)
		defer timer.Stop()
		for {
			if rsm.shutdown.Load() {
				return ErrWrongLeader, nil
			}
			select {
			case <-timer.C:
				return ErrWrongLeader, nil
			case res := <-waitingOp.done:
				if res {
					return OK, waitingOp.result
				} else {
					return ErrWrongLeader, nil
				}
			}
		}
	}()

	rsm.mu.Lock()
	delete(rsm.waitingOps, index)
	rsm.mu.Unlock()
	return err, result
}

func (rsm *RSM) kill() {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	rsm.shutdown.Store(true)
	for _, wop := range rsm.waitingOps {
		select {
		case wop.done <- false:
		default:
		}
	}
	rsm.waitingOps = make(map[int]*waitingOp)
}

func (rsm *RSM) applyLoop() {
	for {
		waitStarted := time.Now()
		msg, ok := <-rsm.applyCh
		atomic.AddInt64(&rsm.applyLoopBlockedNanos, time.Since(waitStarted).Nanoseconds())
		if !ok {
			rsm.kill()
			return
		}
		if rsm.shutdown.Load() {
			return
		}
		processStarted := time.Now()
		if msg.CommandValid {
			rsm.applyCommand(msg)
		} else {
			rsm.applySnapshot(msg)
		}
		atomic.AddInt64(&rsm.applyLoopProcessNanos, time.Since(processStarted).Nanoseconds())
		atomic.AddInt64(&rsm.applyLoopIterCount, 1)
	}
}

func (rsm *RSM) applyCommand(msg raft.ApplyMsg) {
	oper, ok := msg.Command.(Op)
	if !ok {
		return
	}

	term, _ := rsm.rf.GetState()
	if rsm.walLogger != nil {
		if err := rsm.walLogger.AppendAsync(walEntryFromOp(rsm.me, msg.CommandIndex, term, oper)); err != nil {
			log.Printf("[RSM-%d] WAL append failed at index=%d: %v", rsm.me, msg.CommandIndex, err)
		}
	}

	result := rsm.sm.DoOp(oper.Req)
	rsm.mu.Lock()
	if msg.CommandIndex > rsm.lastApplied {
		rsm.lastApplied = msg.CommandIndex
	}
	rsm.mu.Unlock()

	rsm.mu.Lock()
	listener := rsm.opListener
	rsm.mu.Unlock()

	if listener != nil {
		go listener.OnOpComplete(oper.Req, result, int64(msg.CommandIndex))
	}

	rsm.mu.Lock()
	if wop, exists := rsm.waitingOps[msg.CommandIndex]; exists {
		if opEquals(&wop.oper, &oper) {
			wop.result = result
			select {
			case wop.done <- true:
			default:
			}
		} else {
			select {
			case wop.done <- false:
			default:
			}
		}
	}
	rsm.mu.Unlock()

	if rsm.shouldCreateSnapshot(msg.CommandIndex) {
		rsm.lastSnapAt = time.Now()
		go rsm.createSnapshot(msg.CommandIndex)
	}
}

func (rsm *RSM) shouldCreateSnapshot(commandIndex int) bool {
	if rsm.maxraftstate == -1 {
		return false
	}
	if rsm.rf.PersistBytes() <= (rsm.maxraftstate*3)/4 {
		return false
	}
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	if commandIndex-rsm.lastSnapIndex < rsm.snapMinDelta {
		return false
	}
	if time.Since(rsm.lastSnapAt) < rsm.snapMinGap {
		return false
	}
	return true
}

func (rsm *RSM) applySnapshot(msg raft.ApplyMsg) {
	if rsm.shutdown.Load() {
		return
	}
	r := bytes.NewBuffer(msg.Snapshot)
	d := gob.NewDecoder(r)
	var idctr int64
	var smSnapshot []byte
	if d.Decode(&idctr) != nil ||
		d.Decode(&smSnapshot) != nil {
		panic("RSM unable to read snapshot")
	}
	if idctr > atomic.LoadInt64(&rsm.idCounter) {
		atomic.StoreInt64(&rsm.idCounter, idctr)
	}
	rsm.sm.Restore(smSnapshot)
	rsm.mu.Lock()
	if msg.SnapshotIndex > rsm.lastSnapIndex {
		rsm.lastSnapIndex = msg.SnapshotIndex
	}
	if msg.SnapshotIndex > rsm.lastApplied {
		rsm.lastApplied = msg.SnapshotIndex
	}
	rsm.lastSnapAt = time.Now()
	rsm.mu.Unlock()
	rsm.truncateWALAsync(msg.SnapshotIndex)
}

func (rsm *RSM) createSnapshot(snapshotToIndex int) {
	if rsm.shutdown.Load() {
		return
	}
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	smSnapshot := rsm.sm.Snapshot()
	idctr := atomic.LoadInt64(&rsm.idCounter)
	e.Encode(idctr)
	e.Encode(smSnapshot)
	rsm.rf.Snapshot(snapshotToIndex, w.Bytes())
	rsm.truncateWALAsync(snapshotToIndex)
	rsm.mu.Lock()
	rsm.lastSnapIndex = snapshotToIndex
	if snapshotToIndex > rsm.lastApplied {
		rsm.lastApplied = snapshotToIndex
	}
	rsm.lastSnapAt = time.Now()
	rsm.mu.Unlock()
}
