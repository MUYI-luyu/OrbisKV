package raft

// Package raft implements the Raft consensus protocol.
// The Node interface (defined in types.go) specifies the methods that
// a Raft node must expose to the application layer (e.g., KVServer).
// Make() creates a new Raft node implementing the Node interface.

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Persister interface {
	ReadRaftState() []byte
	ReadHardState() []byte
	ReadSnapshot() []byte
	EnqueueSave(raftstate []byte, snapshot []byte) <-chan error
	EnqueueAppendRaftState(data []byte) <-chan error
	EnqueueSaveHardState(hardstate []byte) <-chan error
	Save(raftstate []byte, snapshot []byte)
	AppendRaftState(data []byte)
	SaveHardState(hardstate []byte)
	RaftStateSize() int
}

const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
	// commandBytes caches gob-encoded command payload for persistence.
	// It is intentionally unexported, so RPC replication does not transmit it.
	commandBytes []byte
}

type hardState struct {
	CurrentTerm int
	VotedFor    int
}

type RaftPerfStats struct {
	MuWaitCount         int64
	MuWaitNanos         int64
	MuWaitAvgNanos      float64
	PersistWaitCount    int64
	PersistWaitNanos    int64
	PersistWaitAvgNanos float64
}

const (
	persistFormatMagic uint32 = 0x4b565233 // "KVR3"
	persistHeaderSize         = 44

	persistOffMagic            = 0
	persistOffCurrentTerm      = 4
	persistOffVotedFor         = 12
	persistOffLastIncludedIdx  = 20
	persistOffLastIncludedTerm = 28
	persistOffLogCount         = 36

	persistAppendPayloadMagic uint32 = 0x52504131 // "RPA1"
	persistAppendPayloadSize         = 28

	persistAppendOffMagic     = 0
	persistAppendOffPrevLen   = 4
	persistAppendOffNewLen    = 12
	persistAppendOffHeaderLen = 20
	persistAppendOffDeltaLen  = 24
)

type persistedCommandEnvelope struct {
	Value interface{}
}

// 实现单个 Raft 节点的 Go 对象。
type Raft struct {
	mu        sync.RWMutex // 锁，用于保护该节点状态的并发访问
	peers     []string     // 所有节点的 RPC 端点
	persister Persister    // 用于保存该节点持久化状态的对象
	me        int          // 当前节点在 peers[] 中的索引
	dead      int32        // 由 Kill() 设置为已死亡

	Votes int // 当前选举的得票数
	state int // 当前角色：Follower / Candidate / Leader

	electionTimeout time.Duration // 选举超时时长
	lastHeard       time.Time     // 上次收到权威消息的时间

	// 所有服务器上的持久化状态，在回复RPC之前更新持久化存储
	CurrentTerm int
	VotedFor    int // 保存的是 candidate 的 ID
	log         []LogEntry

	// 所有服务器上的易失性状态
	CommitIndex int
	LastApplied int

	// persistedIndex 记录本地磁盘已确认持久化的最高逻辑日志索引。
	// applier 和 commit 推进均受此字段约束：绝不允许 LastApplied > persistedIndex。
	// 由异步持久化完成回调、Snapshot() 及启动恢复路径更新，只在 mu 锁内读写。
	persistedIndex int

	// commitCond 用于在 CommitIndex 或 persistedIndex 推进时唤醒 applier，
	// 替代原来的 busy-loop 轮询。
	commitCond *sync.Cond

	//  leader上的易失性状态，在选举之后重新初始化
	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg

	heartbeatInterval  time.Duration
	replicateTrigger   chan struct{}
	leaseRatio         float64
	leaseUntil         time.Time
	leaseUntilUnixNano int64
	stateAtomic        int32

	lastIncludedIndex int
	lastIncludedTerm  int

	// RPC 客户端连接缓存
	rpcMu      sync.Mutex
	rpcClients map[string]*rpc.Client

	// 增量持久化缓存：state = header + encoded log entries。
	persistState     []byte
	persistEntryEnd  []int
	persistLogCount  int
	persistDirtyFrom int
	persistReady     bool

	persistPersistedLen      int
	persistPersistedLogCount int
	persistQueuedLen         int
	persistQueuedLogCount    int

	muWaitNanos      int64
	muWaitCount      int64
	persistWaitNanos int64
	persistWaitCount int64
}

// callPeer 通过 RPC 调用远程 Raft 节点
func (rf *Raft) callPeer(server int, method string, args interface{}, reply interface{}) bool {
	if server == rf.me {
		return false
	}

	if server < 0 || server >= len(rf.peers) {
		return false
	}

	address := rf.peers[server]

	rf.rpcMu.Lock()
	client := rf.rpcClients[address]
	rf.rpcMu.Unlock()

	if client == nil {
		newClient, err := rpc.Dial("tcp", address)
		if err != nil {
			return false
		}
		rf.rpcMu.Lock()
		if existing := rf.rpcClients[address]; existing != nil {
			_ = newClient.Close()
			client = existing
		} else {
			rf.rpcClients[address] = newClient
			client = newClient
		}
		rf.rpcMu.Unlock()
	}

	err := client.Call(method, args, reply)
	if err == nil {
		return true
	}

	// 连接失效时清理缓存，后续自动重建。
	rf.rpcMu.Lock()
	if cur := rf.rpcClients[address]; cur == client {
		delete(rf.rpcClients, address)
		_ = client.Close()
	}
	rf.rpcMu.Unlock()
	return false
}

// 返回当前任期以及该服务器是否认为自己是领导者。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.CurrentTerm, rf.state == Leader
}

func (rf *Raft) GetLastApplied() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.LastApplied
}

func (rf *Raft) lockWithMetrics() {
	started := time.Now()
	rf.mu.Lock()
	atomic.AddInt64(&rf.muWaitNanos, time.Since(started).Nanoseconds())
	atomic.AddInt64(&rf.muWaitCount, 1)
}

func (rf *Raft) waitPersistDone(done <-chan error) error {
	if done == nil {
		return nil
	}
	started := time.Now()
	err := <-done
	atomic.AddInt64(&rf.persistWaitNanos, time.Since(started).Nanoseconds())
	atomic.AddInt64(&rf.persistWaitCount, 1)
	return err
}

func immediatePersistError(err error) <-chan error {
	ch := make(chan error, 1)
	ch <- err
	close(ch)
	return ch
}

func (rf *Raft) PerfStatsSnapshot() RaftPerfStats {
	muWaitCount := atomic.LoadInt64(&rf.muWaitCount)
	muWaitNanos := atomic.LoadInt64(&rf.muWaitNanos)
	persistWaitCount := atomic.LoadInt64(&rf.persistWaitCount)
	persistWaitNanos := atomic.LoadInt64(&rf.persistWaitNanos)

	avg := func(total int64, count int64) float64 {
		if count <= 0 {
			return 0
		}
		return float64(total) / float64(count)
	}

	return RaftPerfStats{
		MuWaitCount:         muWaitCount,
		MuWaitNanos:         muWaitNanos,
		MuWaitAvgNanos:      avg(muWaitNanos, muWaitCount),
		PersistWaitCount:    persistWaitCount,
		PersistWaitNanos:    persistWaitNanos,
		PersistWaitAvgNanos: avg(persistWaitNanos, persistWaitCount),
	}
}

func (rf *Raft) setStateLocked(state int) {
	rf.state = state
	atomic.StoreInt32(&rf.stateAtomic, int32(state))
}

func (rf *Raft) setLeaseUntilLocked(t time.Time) {
	rf.leaseUntil = t
	atomic.StoreInt64(&rf.leaseUntilUnixNano, t.UnixNano())
}

func durationMsFromEnv(name string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return def
	}
	return time.Duration(ms) * time.Millisecond
}

func floatFromEnv(name string, def float64) float64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 {
		return def
	}
	return v
}

func (rf *Raft) leaseDurationLocked() time.Duration {
	leaseDur := time.Duration(float64(rf.heartbeatInterval) * rf.leaseRatio)
	maxLease := rf.electionTimeout - 20*time.Millisecond
	if maxLease <= 0 {
		maxLease = rf.electionTimeout / 2
	}
	if maxLease <= 0 {
		maxLease = 100 * time.Millisecond
	}
	if leaseDur > maxLease {
		leaseDur = maxLease
	}
	if leaseDur <= 0 {
		leaseDur = 100 * time.Millisecond
	}
	return leaseDur
}

// GetLastIncludedIndex 返回当前快照覆盖到的最大日志索引。
func (rf *Raft) GetLastIncludedIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastIncludedIndex
}

// SyncAppliedIndex 在状态机外部恢复完成后对齐 Raft 的进度。
func (rf *Raft) SyncAppliedIndex(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index < rf.lastIncludedIndex {
		index = rf.lastIncludedIndex
	}
	if index > rf.getLastLogIndex() {
		index = rf.getLastLogIndex()
	}
	if index > rf.CommitIndex {
		rf.CommitIndex = index
		rf.commitCond.Signal()
	}
	if index > rf.LastApplied {
		rf.LastApplied = index
	}
}

// IsLeaderWithLease returns true only when this node is leader and the lease is still valid.
func (rf *Raft) IsLeaderWithLease() bool {
	if rf.killed() {
		return false
	}
	if int(atomic.LoadInt32(&rf.stateAtomic)) != Leader {
		return false
	}
	leaseUntil := atomic.LoadInt64(&rf.leaseUntilUnixNano)
	return time.Now().UnixNano() < leaseUntil
}

func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.getTerm(rf.getLastLogIndex())
}

func (rf *Raft) getTerm(index int) int {
	if index <= rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[index-rf.lastIncludedIndex].Term
}

func makePersistHeader() []byte {
	buf := make([]byte, persistHeaderSize)
	binary.LittleEndian.PutUint32(buf[persistOffMagic:persistOffMagic+4], persistFormatMagic)
	return buf
}

func putPersistHeaderFields(buf []byte, currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm, logCount int) bool {
	if len(buf) < persistHeaderSize {
		return false
	}
	binary.LittleEndian.PutUint32(buf[persistOffMagic:persistOffMagic+4], persistFormatMagic)
	binary.LittleEndian.PutUint64(buf[persistOffCurrentTerm:persistOffCurrentTerm+8], uint64(int64(currentTerm)))
	binary.LittleEndian.PutUint64(buf[persistOffVotedFor:persistOffVotedFor+8], uint64(int64(votedFor)))
	binary.LittleEndian.PutUint64(buf[persistOffLastIncludedIdx:persistOffLastIncludedIdx+8], uint64(int64(lastIncludedIndex)))
	binary.LittleEndian.PutUint64(buf[persistOffLastIncludedTerm:persistOffLastIncludedTerm+8], uint64(int64(lastIncludedTerm)))
	binary.LittleEndian.PutUint64(buf[persistOffLogCount:persistOffLogCount+8], uint64(int64(logCount)))
	return true
}

func appendInt64LE(dst []byte, v int64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v))
	return append(dst, b[:]...)
}

func readInt64LE(src []byte, offset *int) (int64, bool) {
	if offset == nil || *offset < 0 || *offset+8 > len(src) {
		return 0, false
	}
	v := int64(binary.LittleEndian.Uint64(src[*offset : *offset+8]))
	*offset += 8
	return v, true
}

func (rf *Raft) initPersistCacheLocked() {
	rf.persistState = makePersistHeader()
	rf.persistEntryEnd = rf.persistEntryEnd[:0]
	rf.persistLogCount = 0
	rf.persistDirtyFrom = 0
	rf.persistReady = true
}

func (rf *Raft) invalidatePersistCacheLocked() {
	rf.persistState = nil
	rf.persistEntryEnd = nil
	rf.persistLogCount = 0
	rf.persistDirtyFrom = 0
	rf.persistReady = false
	rf.persistPersistedLen = 0
	rf.persistPersistedLogCount = 0
	rf.persistQueuedLen = 0
	rf.persistQueuedLogCount = 0
}

func canUsePersistAppend(state []byte, prevLen int) bool {
	if prevLen < persistHeaderSize {
		return false
	}
	if len(state) < prevLen {
		return false
	}
	return len(state) >= persistHeaderSize
}

func buildPersistAppendPayload(state []byte, prevLen int) ([]byte, bool) {
	if !canUsePersistAppend(state, prevLen) {
		return nil, false
	}

	delta := state[prevLen:]
	deltaLen := len(delta)
	payloadLen := persistAppendPayloadSize + persistHeaderSize + deltaLen
	payload := make([]byte, payloadLen)

	binary.LittleEndian.PutUint32(payload[persistAppendOffMagic:persistAppendOffMagic+4], persistAppendPayloadMagic)
	binary.LittleEndian.PutUint64(payload[persistAppendOffPrevLen:persistAppendOffPrevLen+8], uint64(prevLen))
	binary.LittleEndian.PutUint64(payload[persistAppendOffNewLen:persistAppendOffNewLen+8], uint64(len(state)))
	binary.LittleEndian.PutUint32(payload[persistAppendOffHeaderLen:persistAppendOffHeaderLen+4], uint32(persistHeaderSize))
	binary.LittleEndian.PutUint32(payload[persistAppendOffDeltaLen:persistAppendOffDeltaLen+4], uint32(deltaLen))

	copy(payload[persistAppendPayloadSize:persistAppendPayloadSize+persistHeaderSize], state[:persistHeaderSize])
	copy(payload[persistAppendPayloadSize+persistHeaderSize:], delta)
	return payload, true
}

func (rf *Raft) canPersistAppendLocked(snapshot []byte, state []byte, dirtyFromBefore int) bool {
	if snapshot != nil {
		return false
	}
	if !rf.persistReady {
		return false
	}
	if rf.persistQueuedLogCount > len(rf.log) {
		return false
	}
	if dirtyFromBefore < rf.persistQueuedLogCount {
		return false
	}
	return canUsePersistAppend(state, rf.persistQueuedLen)
}

func (rf *Raft) markPersistPersistedLocked(stateLen int, logCount int) {
	if stateLen < 0 {
		stateLen = 0
	}
	if logCount < 0 {
		logCount = 0
	}
	// 使用 max 语义防止异步持久化回调乱序到达时覆盖较新的结果
	if stateLen > rf.persistPersistedLen {
		rf.persistPersistedLen = stateLen
	}
	if logCount > rf.persistPersistedLogCount {
		rf.persistPersistedLogCount = logCount
	}
}

func (rf *Raft) markPersistQueuedLocked(stateLen int, logCount int) {
	if stateLen < 0 {
		stateLen = 0
	}
	if logCount < 0 {
		logCount = 0
	}
	rf.persistQueuedLen = stateLen
	rf.persistQueuedLogCount = logCount
}

func (rf *Raft) markPersistDirtyFromLocked(pos int) {
	if !rf.persistReady {
		return
	}
	if pos < 0 {
		pos = 0
	}
	if pos > len(rf.log) {
		pos = len(rf.log)
	}
	if rf.persistDirtyFrom > pos {
		rf.persistDirtyFrom = pos
	}
}

func (rf *Raft) truncatePersistToLocked(keep int) bool {
	if !rf.persistReady {
		return false
	}
	if keep < 0 {
		keep = 0
	}
	if keep == 0 {
		rf.persistState = rf.persistState[:persistHeaderSize]
		rf.persistEntryEnd = rf.persistEntryEnd[:0]
		rf.persistLogCount = 0
		return true
	}
	if keep > len(rf.persistEntryEnd) {
		return false
	}
	end := rf.persistEntryEnd[keep-1]
	if end < persistHeaderSize || end > len(rf.persistState) {
		return false
	}
	rf.persistState = rf.persistState[:end]
	rf.persistEntryEnd = rf.persistEntryEnd[:keep]
	rf.persistLogCount = keep
	return true
}

func (rf *Raft) appendPersistEntryLocked(entry *LogEntry) bool {
	if entry == nil {
		return false
	}
	if !ensureEntryCommandEncoded(entry) {
		return false
	}
	rf.persistState = appendInt64LE(rf.persistState, int64(entry.Term))
	rf.persistState = appendInt64LE(rf.persistState, int64(len(entry.commandBytes)))
	rf.persistState = append(rf.persistState, entry.commandBytes...)
	rf.persistEntryEnd = append(rf.persistEntryEnd, len(rf.persistState))
	rf.persistLogCount++
	return true
}

func (rf *Raft) encodePersistentStateIncrementalLocked() ([]byte, bool) {
	if !rf.persistReady {
		rf.initPersistCacheLocked()
	}

	if rf.persistDirtyFrom > len(rf.log) {
		rf.persistDirtyFrom = len(rf.log)
	}

	if rf.persistLogCount > len(rf.log) {
		if !rf.truncatePersistToLocked(len(rf.log)) {
			rf.initPersistCacheLocked()
		}
	}

	if rf.persistDirtyFrom < rf.persistLogCount {
		if !rf.truncatePersistToLocked(rf.persistDirtyFrom) {
			rf.initPersistCacheLocked()
		}
	}

	for rf.persistLogCount < len(rf.log) {
		idx := rf.persistLogCount
		if !rf.appendPersistEntryLocked(&rf.log[idx]) {
			return nil, false
		}
	}

	if !putPersistHeaderFields(
		rf.persistState,
		rf.CurrentTerm,
		rf.VotedFor,
		rf.lastIncludedIndex,
		rf.lastIncludedTerm,
		len(rf.log),
	) {
		return nil, false
	}

	rf.persistDirtyFrom = len(rf.log)
	return rf.persistState, true
}

func (rf *Raft) enqueuePersistLocked(snapshot []byte) (<-chan error, int, int) {
	dirtyFromBefore := rf.persistDirtyFrom
	state, ok := rf.encodePersistentStateIncrementalLocked()
	if !ok {
		rf.invalidatePersistCacheLocked()
		return immediatePersistError(errors.New("persist encode failed")), -1, -1
	}
	logCount := len(rf.log)
	stateLen := len(state)
	if rf.canPersistAppendLocked(snapshot, state, dirtyFromBefore) {
		if payload, payloadOK := buildPersistAppendPayload(state, rf.persistQueuedLen); payloadOK {
			rf.markPersistQueuedLocked(stateLen, logCount)
			return rf.persister.EnqueueAppendRaftState(payload), stateLen, logCount
		}
	}
	rf.markPersistQueuedLocked(stateLen, logCount)
	return rf.persister.EnqueueSave(state, snapshot), stateLen, logCount
}

func (rf *Raft) commitPersistResult(stateLen int, logCount int, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if err != nil {
		rf.invalidatePersistCacheLocked()
		log.Fatalf("[raft-%d] persist failed, exiting: %v", rf.me, err)
	}
	if stateLen >= 0 {
		rf.markPersistPersistedLocked(stateLen, logCount)
	}
}

func (rf *Raft) enqueuePersistHardStateLocked() <-chan error {
	return rf.persister.EnqueueSaveHardState(encodeHardState(rf.CurrentTerm, rf.VotedFor))
}

// 将 Raft 的持久状态保存到稳定存储中，
// 以便在崩溃并重启后能够恢复。
func (rf *Raft) persist(snapshot []byte) {
	done, stateLen, logCount := rf.enqueuePersistLocked(snapshot)
	err := rf.waitPersistDone(done)
	if err != nil {
		rf.invalidatePersistCacheLocked()
		log.Fatalf("[raft-%d] persist snapshot/state failed, exiting: %v", rf.me, err)
	}
	if stateLen >= 0 {
		rf.markPersistPersistedLocked(stateLen, logCount)
	}
}

func (rf *Raft) persistHardState() {
	err := rf.waitPersistDone(rf.enqueuePersistHardStateLocked())
	if err != nil {
		rf.invalidatePersistCacheLocked()
		log.Fatalf("[raft-%d] persist hardstate failed, exiting: %v", rf.me, err)
	}
}

// 恢复先前持久化的状态。
func (rf *Raft) readPersist(data []byte) {
	if rf.readPersistBinary(data) {
		return
	}
	rf.invalidatePersistCacheLocked()
}

func (rf *Raft) readPersistBinary(data []byte) bool {
	if data == nil || len(data) < persistHeaderSize {
		return false
	}

	if binary.LittleEndian.Uint32(data[persistOffMagic:persistOffMagic+4]) != persistFormatMagic {
		return false
	}

	currentTerm := int(int64(binary.LittleEndian.Uint64(data[persistOffCurrentTerm : persistOffCurrentTerm+8])))
	votedFor := int(int64(binary.LittleEndian.Uint64(data[persistOffVotedFor : persistOffVotedFor+8])))
	lastIncludedIndex := int(int64(binary.LittleEndian.Uint64(data[persistOffLastIncludedIdx : persistOffLastIncludedIdx+8])))
	lastIncludedTerm := int(int64(binary.LittleEndian.Uint64(data[persistOffLastIncludedTerm : persistOffLastIncludedTerm+8])))
	logCount64 := int64(binary.LittleEndian.Uint64(data[persistOffLogCount : persistOffLogCount+8]))
	if logCount64 < 0 {
		return false
	}

	logCount := int(logCount64)
	decodedLog := make([]LogEntry, 0, logCount)
	entryEnds := make([]int, 0, logCount)

	offset := persistHeaderSize
	for i := 0; i < logCount; i++ {
		term64, ok := readInt64LE(data, &offset)
		if !ok {
			return false
		}
		cmdLen64, ok := readInt64LE(data, &offset)
		if !ok || cmdLen64 < 0 || cmdLen64 > int64(len(data)-offset) {
			return false
		}
		cmdLen := int(cmdLen64)
		cmdData := append([]byte(nil), data[offset:offset+cmdLen]...)
		offset += cmdLen

		cmd, ok := decodeCommandFromBytes(cmdData)
		if !ok {
			return false
		}
		decodedLog = append(decodedLog, LogEntry{
			Term:         int(term64),
			Command:      cmd,
			commandBytes: cmdData,
		})
		entryEnds = append(entryEnds, offset)
	}

	if offset != len(data) {
		return false
	}

	if len(decodedLog) == 0 {
		decodedLog = []LogEntry{{Term: lastIncludedTerm}}
		rf.invalidatePersistCacheLocked()
	} else {
		rf.persistState = append([]byte(nil), data...)
		rf.persistEntryEnd = append([]int(nil), entryEnds...)
		rf.persistLogCount = len(decodedLog)
		rf.persistDirtyFrom = len(decodedLog)
		rf.persistReady = true
		rf.persistPersistedLen = len(data)
		rf.persistPersistedLogCount = len(decodedLog)
		rf.persistQueuedLen = len(data)
		rf.persistQueuedLogCount = len(decodedLog)
	}

	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.log = decodedLog
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	return true
}

func (rf *Raft) readHardState(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var hs hardState
	if d.Decode(&hs.CurrentTerm) != nil || d.Decode(&hs.VotedFor) != nil {
		return
	}

	if hs.CurrentTerm > rf.CurrentTerm || (hs.CurrentTerm == rf.CurrentTerm && hs.VotedFor != -1) {
		rf.CurrentTerm = hs.CurrentTerm
		rf.VotedFor = hs.VotedFor
	}
}

// 返回 Raft 持久化日志的字节数。
func (rf *Raft) PersistBytes() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.persister.RaftStateSize()
}

func encodeCommandToBytes(command interface{}) ([]byte, bool) {
	if command == nil {
		return nil, true
	}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(persistedCommandEnvelope{Value: command}); err != nil {
		return nil, false
	}
	return buf.Bytes(), true
}

func decodeCommandFromBytes(data []byte) (interface{}, bool) {
	if len(data) == 0 {
		return nil, true
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var envelope persistedCommandEnvelope
	if err := dec.Decode(&envelope); err != nil {
		return nil, false
	}
	return envelope.Value, true
}

func ensureEntryCommandEncoded(entry *LogEntry) bool {
	if entry == nil {
		return false
	}
	if len(entry.commandBytes) > 0 || entry.Command == nil {
		return true
	}
	encoded, ok := encodeCommandToBytes(entry.Command)
	if !ok {
		return false
	}
	entry.commandBytes = encoded
	return true
}

func encodeHardState(currentTerm, votedFor int) []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)

	_ = enc.Encode(currentTerm)
	_ = enc.Encode(votedFor)

	return buf.Bytes()
}

// 服务端通知 Raft：它已创建了一个包含
// index 及其之前所有信息的快照。
// 这意味着服务端不再需要该索引（及之前）的日志。
// Raft 此时应尽可能地截断日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 不能对已经被快照覆盖的 index 再做 snapshot
	if index <= rf.lastIncludedIndex {
		return
	}

	// 2. 不能 snapshot 未 commit 的日志
	if index > rf.CommitIndex {
		return
	}

	// 3. 找到对应 term
	// 数组下标 = 真实 index - lastIncludedIndex
	offset := index - rf.lastIncludedIndex
	lastIncludedTerm := rf.log[offset].Term

	// 4. 构造新的日志数组
	// 保留 index 之后的日志
	newLogs := make([]LogEntry, 0)
	newLogs = append(newLogs, LogEntry{
		Term: lastIncludedTerm,
	}) // dummy entry

	if offset+1 < len(rf.log) {
		newLogs = append(newLogs, rf.log[offset+1:]...)
	}

	rf.log = newLogs
	rf.markPersistDirtyFromLocked(0)

	// 5. 更新 snapshot 元数据
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = lastIncludedTerm

	// 6. 更新 lastApplied
	if rf.LastApplied < index {
		rf.LastApplied = index
	}

	// 7. 持久化 (状态 + snapshot) — 同步落盘
	rf.persist(snapshot)

	// 快照数据已落盘，persistedIndex 至少推进到快照覆盖的 index
	if index > rf.persistedIndex {
		rf.persistedIndex = index
	}
	rf.commitCond.Signal()
}

// RequestVote RPC 参数结构体
type RequestVoteArgs struct {
	Term         int // candidate的任期号
	CandidateId  int // 发起投票的candidate的ID
	LastLogIndex int // candidate的最高日志条目索引
	LastLogTerm  int // candidate的最高日志条目的任期号
}

// RequestVote RPC 回复结构体
type RequestVoteReply struct {
	Term        int  // 服务器的当前任期号，让candidate更新自己
	VoteGranted bool // 如果是true，意味着candidate收到了选票
}

// RequestVote RPC 处理函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.lockWithMetrics()
	persistNeeded := false

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return nil
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.setStateLocked(Follower)
		rf.VotedFor = -1
		persistNeeded = true
	}

	if args.LastLogTerm < rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex()) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		var persistDone <-chan error
		if persistNeeded {
			persistDone = rf.enqueuePersistHardStateLocked()
		}
		rf.mu.Unlock()
		if err := rf.waitPersistDone(persistDone); err != nil {
			rf.commitPersistResult(-1, -1, err)
		}
		return nil
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		persistNeeded = true
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.CurrentTerm
	if reply.VoteGranted {
		rf.resetElectionTimer()
	}

	var persistDone <-chan error
	if persistNeeded {
		persistDone = rf.enqueuePersistHardStateLocked()
	}
	rf.mu.Unlock()
	if err := rf.waitPersistDone(persistDone); err != nil {
		rf.commitPersistResult(-1, -1, err)
	}
	return nil
}

type AppendEntriesArgs struct {
	Term         int        // leader的任期号
	LeaderId     int        // 用来让follower把客户端请求定向到leader
	PrevLogIndex int        // 紧接新条目之前的日志条目索引(当前最大的日志条目索引)
	PrevLogTerm  int        // prevLogIndex的任期
	Entries      []LogEntry // 储存的日志条目
	LeaderCommit int        // leader的commitIndex
}

type AppendEntriesReply struct {
	Term          int
	Success       bool // 如果follower包含的日志匹配参数汇总的prevLogIndex和prevLogTerm，返回true
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) resetElectionTimer() {
	rf.lastHeard = time.Now()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.callPeer(server, "Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.lockWithMetrics()
	hardPersistNeeded := false
	persistStateLen := -1
	persistLogCount := -1
	var persistDone <-chan error
	finalize := func() error {
		if persistDone == nil && hardPersistNeeded {
			persistDone = rf.enqueuePersistHardStateLocked()
		}
		rf.mu.Unlock()
		err := rf.waitPersistDone(persistDone)
		if err == nil && persistLogCount >= 0 {
			rf.mu.Lock()
			coveredIndex := rf.lastIncludedIndex + persistLogCount - 1
			if coveredIndex > rf.persistedIndex {
				rf.persistedIndex = coveredIndex
				rf.commitCond.Signal()
			}
			rf.mu.Unlock()
		}
		if persistStateLen >= 0 || err != nil {
			if err != nil && reply.Success {
				reply.Success = false
			}
			rf.commitPersistResult(persistStateLen, persistLogCount, err)
		}
		return nil
	}
	// ---（第 1 步）任期检查：拒绝过期 leader---
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return finalize()
	}
	// ---（第 2 步）如果 leader 的 term 更大，更新自己的 term 并转为 follower---
	if args.Term >= rf.CurrentTerm {
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
			hardPersistNeeded = true
		}
		rf.setStateLocked(Follower)
	}

	reply.Term = rf.CurrentTerm

	// ---（第 3 步）收到 AppendEntries 则重置选举计时器（心跳）---
	rf.resetElectionTimer()

	// ---（第 4 步）prevLogIndex 不存在：日志冲突---
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		return finalize()
	}

	// ---（第 5 步）检查 prevLogTerm 是否匹配---
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return finalize()
	}

	if rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.getTerm(args.PrevLogIndex)

		// 找到该 term 的第一个 index
		index := args.PrevLogIndex
		for index > rf.lastIncludedIndex &&
			rf.getTerm(index-1) == reply.ConflictTerm {
			index--
		}

		reply.ConflictIndex = index
		return finalize()
	}

	// ---（第 6 步）删除冲突日志（同 index 不同 term）---
	// 从第一个冲突点开始删除
	persistNeeded := false
	i := 0
	for ; i < len(args.Entries); i++ {
		index := args.PrevLogIndex + 1 + i

		if index > rf.getLastLogIndex() {
			break
		}

		if rf.getTerm(index) != args.Entries[i].Term {
			// 删除 index 及之后的日志
			cut := index - rf.lastIncludedIndex
			if cut < 1 {
				cut = 1
			}
			if cut < len(rf.log) {
				rf.markPersistDirtyFromLocked(cut)
				rf.log = rf.log[:cut]
				persistNeeded = true
			}
			break
		}
	}

	// ---（第 7 步）追加 leader 发来的新日志---
	// 注意：只追加 follower 没有的部分
	if i < len(args.Entries) {
		appendFrom := len(rf.log)
		rf.markPersistDirtyFromLocked(appendFrom)
		newEntries := make([]LogEntry, len(args.Entries)-i)
		for j := i; j < len(args.Entries); j++ {
			entry := args.Entries[j]
			_ = ensureEntryCommandEncoded(&entry)
			newEntries[j-i] = entry
		}
		rf.log = append(rf.log, newEntries...)
		persistNeeded = true
	}

	if persistNeeded {
		persistDone, persistStateLen, persistLogCount = rf.enqueuePersistLocked(nil)
	} else if hardPersistNeeded {
		persistDone = rf.enqueuePersistHardStateLocked()
	}

	// ---（第 8 步）更新 CommitIndex ---
	if args.LeaderCommit > rf.CommitIndex {
		// rf.CommitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.CommitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.commitCond.Signal()
	}

	reply.Success = true
	return finalize()
}

// 向某个服务器发送 RequestVote RPC 的示例
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.callPeer(server, "Raft.RequestVote", args, reply)
	return ok
}

// 使用 Raft 的服务（例如 k/v 服务器）希望就下一个命令
// 达成一致并将其追加到 Raft 日志中。
// 如果该服务器不是领导者，则返回 false。
// 如果是领导者，则启动一致性流程并立即返回。
// 不保证该命令最终一定会被提交到日志中，
// 因为领导者可能崩溃或在选举中失败。
// 即使该 Raft 实例已被 Kill()，本函数也应安全返回。
//
// 第一个返回值是命令将出现的日志索引（若被提交）。
// 第二个返回值是当前任期。
// 第三个返回值表示该服务器是否认为自己是领导者。
//
// Start() 将命令追加到内存日志后立即返回，不等待磁盘持久化。
// 持久化由后台 goroutine 异步完成，安全由 persistedIndex 门控保证：
//   - applier 绝不会应用 LastApplied > persistedIndex 的条目
//   - Leader 提交推进时，若条目未持久化则不计入自己的那一票
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.lockWithMetrics()

	// 1. 如果不是 Leader，直接拒绝
	if rf.state != Leader {
		term := rf.CurrentTerm
		rf.mu.Unlock()
		return -1, term, false
	}

	// 2. 计算新日志的 index
	index := rf.getLastLogIndex() + 1
	term := rf.CurrentTerm

	// 3. 追加日志（内存操作，微秒级）
	entry := LogEntry{
		Term:    term,
		Command: command,
	}
	rf.markPersistDirtyFromLocked(len(rf.log))
	_ = ensureEntryCommandEncoded(&entry)
	rf.log = append(rf.log, entry)

	// 4. 入队持久化，但不等待磁盘完成
	persistDone, persistStateLen, persistLogCount := rf.enqueuePersistLocked(nil)
	// 本次持久化期望覆盖的最高逻辑索引
	coveredIndex := rf.lastIncludedIndex + persistLogCount - 1
	rf.mu.Unlock()

	// 5. 异步 goroutine：等待磁盘 I/O 完成后更新 persistedIndex
	go rf.completePersistAsync(persistDone, persistStateLen, persistLogCount, coveredIndex)

	// 6. 触发网络复制（不阻塞）
	select {
	case rf.replicateTrigger <- struct{}{}:
	default:
	}
	// 7. 立即返回（不等提交，不等持久化）
	return index, term, true
}

// completePersistAsync 是 Start() 的异步持久化回调。
// 当磁盘 I/O 完成后，更新 persistedIndex 并唤醒 applier。
func (rf *Raft) completePersistAsync(persistDone <-chan error, stateLen, logCount, coveredIndex int) {
	err := rf.waitPersistDone(persistDone)
	if err == nil {
		rf.mu.Lock()
		if coveredIndex > rf.persistedIndex {
			rf.persistedIndex = coveredIndex
		}
		rf.commitCond.Signal()
		rf.mu.Unlock()
	}
	rf.commitPersistResult(stateLen, logCount, err)
}

// 测试器不会在每次测试结束后停止 Raft 创建的 goroutine，
// 但会调用 Kill() 方法。你的代码可以使用 killed()
// 来检查 Kill() 是否已被调用。使用 atomic 可避免使用锁。
//
// 问题在于：长时间运行的 goroutine 会占用内存和 CPU，
// 可能导致后续测试失败，并产生混乱的调试输出。
// 任何包含长循环的 goroutine 都应调用 killed() 检查是否应退出。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// 如有需要，可在此添加你的代码。
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		rf.mu.Unlock()
		return nil
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.setStateLocked(Follower)
		rf.setLeaseUntilLocked(time.Unix(0, 0))
		rf.persistHardState()
	}

	rf.resetElectionTimer()

	// 过期的快照，直接返回
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return nil
	}

	// 关键修复：正确处理日志截断
	if args.LastIncludedIndex <= rf.getLastLogIndex() &&
		rf.getTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		// 快照覆盖的日志与本地一致，保留后续日志
		newLog := make([]LogEntry, 0)
		newLog = append(newLog, LogEntry{Term: args.LastIncludedTerm})

		// 使用新的 LastIncludedIndex 计算 offset
		offset := args.LastIncludedIndex - rf.lastIncludedIndex
		if offset+1 < len(rf.log) {
			newLog = append(newLog, rf.log[offset+1:]...)
		}
		rf.log = newLog
	} else {
		// 快照覆盖的日志不存在或不一致，直接用快照
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}
	rf.markPersistDirtyFromLocked(0)
	// 更新快照元数据
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.CommitIndex = max(rf.CommitIndex, rf.lastIncludedIndex)
	rf.LastApplied = max(rf.LastApplied, rf.lastIncludedIndex)

	rf.persist(args.Data)

	// 快照数据已同步落盘
	if rf.lastIncludedIndex > rf.persistedIndex {
		rf.persistedIndex = rf.lastIncludedIndex
	}
	rf.commitCond.Signal()

	// 保存 snapshot 数据
	snapshotData := args.Data
	snapshotTerm := rf.lastIncludedTerm
	snapshotIndex := rf.lastIncludedIndex

	rf.mu.Unlock() // 解锁

	// 发送 snapshot
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshotData,
		SnapshotTerm:  snapshotTerm,
		SnapshotIndex: snapshotIndex,
	}
	return nil
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	var successCount int32 = 1 // leader itself

	// 向每个 Follower 发送心跳或日志同步
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			// Follower 需要的日志已经被 Leader 的快照截掉了，则给 follower 安装快照
			if rf.nextIndex[server] <= rf.lastIncludedIndex {
				snapshotData := rf.persister.ReadSnapshot()

				// 检查快照是否存在
				if len(snapshotData) == 0 {
					rf.nextIndex[server] = rf.lastIncludedIndex + 1
					rf.mu.Unlock()
					return
				}

				args := &InstallSnapshotArgs{
					Term:              rf.CurrentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              snapshotData,
				}

				rf.mu.Unlock()

				reply := &InstallSnapshotReply{}
				ok := rf.callPeer(server, "Raft.InstallSnapshot", args, reply)

				rf.mu.Lock()
				if !ok {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.setStateLocked(Follower)
					rf.setLeaseUntilLocked(time.Unix(0, 0))
					rf.VotedFor = -1
					rf.persistHardState()
					rf.mu.Unlock()
					return
				}

				rf.nextIndex[server] = rf.lastIncludedIndex + 1
				rf.matchIndex[server] = rf.lastIncludedIndex
				rf.mu.Unlock()
				return
			}

			// 获取 Follower 的上一个日志索引和任期
			prevIndex := rf.nextIndex[server] - 1
			if prevIndex < 0 {
				prevIndex = 0
			}

			prevTerm := 0
			if prevIndex == rf.lastIncludedIndex {
				prevTerm = rf.lastIncludedTerm
			} else {
				prevTerm = rf.log[prevIndex-rf.lastIncludedIndex].Term
			}

			term := rf.CurrentTerm

			// 构造日志条目，可能为空（即仅发送心跳）
			// entries := make([]LogEntry, len(rf.log[rf.nextIndex[server]:]))
			// copy(entries, rf.log[rf.nextIndex[server]:])
			start := rf.nextIndex[server] - rf.lastIncludedIndex
			if start < 0 {
				start = 0
			}
			entries := make([]LogEntry, len(rf.log[start:]))
			copy(entries, rf.log[start:])
			// 如果没有日志，就会是空的

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      entries, // 如果是心跳，entries 会为空
				LeaderCommit: rf.CommitIndex,
			}

			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			// 发送 AppendEntries RPC
			ok := rf.sendAppendEntries(server, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Leader || term != rf.CurrentTerm {
				return
			}

			// 如果收到的 Term 更大，说明需要转换为 Follower
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.setStateLocked(Follower)
				rf.setLeaseUntilLocked(time.Unix(0, 0))
				rf.VotedFor = -1
				rf.persistHardState()
				return
			}

			if rf.CurrentTerm != args.Term || rf.state != Leader {
				return
			}

			// 更新 nextIndex 和 matchIndex
			if reply.Success {
				rf.matchIndex[server] = prevIndex + len(entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				n := atomic.AddInt32(&successCount, 1)
				if n > int32(len(rf.peers)/2) {
					d := rf.leaseDurationLocked()
					rf.setLeaseUntilLocked(time.Now().Add(d))
				}
			} else {
				if reply.ConflictTerm == -1 {
					// follower 太短
					rf.nextIndex[server] = reply.ConflictIndex
				} else {
					// follower 有冲突 term
					lastIndex := -1

					// 查找 leader 中是否存在该 term
					for i := len(rf.log) - 1; i >= 0; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							lastIndex = rf.lastIncludedIndex + i
							break
						}
					}

					if lastIndex != -1 {
						// leader 也有该 term
						rf.nextIndex[server] = lastIndex + 1
					} else {
						// leader 没有该 term
						rf.nextIndex[server] = reply.ConflictIndex
					}
				}
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
			}

			// fmt.Printf("[log]:later AppendEntries %d peer, matchIndex[%d] = %d, nextIndex[%d] = %d\n", server, server, rf.matchIndex[server], server, rf.nextIndex[server])

			// 推进 commitIndex
			// 安全红线：Leader 只有在条目已本地持久化时才将自己计入多数派。
			// 若未持久化，则需要额外一个 Follower 的 ACK 才能提交。
			for N := rf.getLastLogIndex(); N > rf.CommitIndex && N >= 1; N-- {

				// 只提交当前任期的日志
				if N == rf.lastIncludedIndex {
					continue
				}
				if rf.log[N-rf.lastIncludedIndex].Term != rf.CurrentTerm {
					continue
				}

				count := 0
				// Leader 只有在条目已落盘时才将自己计入多数派
				if N <= rf.persistedIndex {
					count = 1
				}

				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= N {
						count++
					}
				}

				if count > len(rf.peers)/2 {
					rf.CommitIndex = N
					rf.commitCond.Signal()
					break
				}
			}

		}(i)
	}
}

func (rf *Raft) leaderLoop() {
	go func() {
		ticker := time.NewTicker(rf.heartbeatInterval)
		defer ticker.Stop()
		for rf.killed() == false {
			select {
			case <-ticker.C:
			case <-rf.replicateTrigger:
			}
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			rf.sendHeartbeats() // 发送一次心跳
		}
	}()
}

func (rf *Raft) becomeLeader() {
	rf.setStateLocked(Leader)
	d := rf.leaseDurationLocked()
	rf.setLeaseUntilLocked(time.Now().Add(d))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	// fmt.Printf("[leader]:%d become leader,term:%d\n", rf.me, rf.CurrentTerm)
	rf.leaderLoop()
}

func (rf *Raft) handleVoteResponse(peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 收到比自己大的 term，变回 follower
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.setStateLocked(Follower)
		rf.setLeaseUntilLocked(time.Unix(0, 0))
		rf.VotedFor = -1
		rf.persistHardState()
		return
	}

	if reply.Term < rf.CurrentTerm {
		return
	}

	// 如果不是 Candidate 了（可能已经变成 follower 或 leader），忽略
	if rf.state != Candidate {
		return
	}

	// 计算投票
	if reply.VoteGranted {
		rf.Votes++
		// 是否超过一半？
		if rf.Votes > len(rf.peers)/2 {
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.setStateLocked(Candidate)
	rf.setLeaseUntilLocked(time.Unix(0, 0))
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persistHardState()
	rf.Votes = 1
	rf.resetElectionTimer()
	if rf.Votes > len(rf.peers)/2 {
		rf.becomeLeader()
	}

	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				rf.handleVoteResponse(server, args, reply)
			}
		}(i, args)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// 检查是否需要发起领导者选举（只读，使用 RLock）。
		needStart := false
		rf.mu.RLock()
		if rf.state != Leader && time.Since(rf.lastHeard) > rf.electionTimeout {
			// 超时未收到心跳，发起选举
			needStart = true
		}
		rf.mu.RUnlock()
		if needStart {
			rf.startElection()
		}

		// 随机暂停 50~350 毫秒
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		var msgs []ApplyMsg

		rf.mu.Lock()

		// 安全红线：LastApplied 不能超过 CommitIndex 也不能超过 persistedIndex。
		// 这确保只有已提交且已持久化的条目才会被应用到状态机。
		for rf.LastApplied < rf.CommitIndex && rf.LastApplied < rf.persistedIndex {
			rf.LastApplied++
			index := rf.LastApplied

			// 跳过快照已覆盖的日志
			if index <= rf.lastIncludedIndex {
				continue
			}

			// 正确的索引转换
			pos := index - rf.lastIncludedIndex

			// 边界检查
			if pos < 1 || pos >= len(rf.log) {
				rf.LastApplied--
				break
			}

			command := rf.log[pos].Command
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: index,
			})
		}

		// 没有可应用的条目时，通过条件变量休眠（替代 busy-loop）
		if len(msgs) == 0 && !rf.killed() {
			rf.commitCond.Wait()
		}

		rf.mu.Unlock()

		// 释放锁后批量发送
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
}

// 服务或测试器希望创建一个 Raft 服务器。
// 所有 Raft 服务器的端口都存放在 peers[] 中，
// 当前服务器的端口为 peers[me]。
// 所有服务器的 peers[] 数组顺序一致。
// persister 用于保存该服务器的持久化状态，
// 同时也保存了崩溃前的最近一次状态（若有）。
// applyCh 是一个通道，Raft 会通过它向测试器或服务发送 ApplyMsg。
// Make() 必须快速返回，因此应为任何长期运行的任务启动 goroutine。
func Make(peers []string, me int,
	persister Persister, applyCh chan ApplyMsg) Node {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.electionTimeout = time.Duration(300+rand.Intn(200)) * time.Millisecond
	rf.lastHeard = time.Now()

	// commitCond 必须在 mu 分配后立即初始化（依赖 &rf.mu）
	rf.commitCond = sync.NewCond(&rf.mu)

	rf.setStateLocked(Follower)
	rf.VotedFor = -1
	rf.log = []LogEntry{{Term: 0}}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.lastIncludedIndex = 0 // 显式初始化
	rf.lastIncludedTerm = 0  // 显式初始化

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.rpcClients = make(map[string]*rpc.Client, len(peers))
	rf.heartbeatInterval = durationMsFromEnv("RAFT_HEARTBEAT_INTERVAL_MS", 80*time.Millisecond)
	rf.replicateTrigger = make(chan struct{}, 1)
	rf.leaseRatio = floatFromEnv("RAFT_LEASE_RATIO", 1.5)
	rf.setLeaseUntilLocked(time.Now())

	// 从崩溃前的持久化状态中恢复
	rf.readPersist(persister.ReadRaftState())
	rf.readHardState(persister.ReadHardState())
	rf.CommitIndex = rf.lastIncludedIndex
	rf.LastApplied = rf.lastIncludedIndex

	// 启动时所有恢复的日志条目均来自磁盘，persistedIndex 覆盖全部已恢复日志。
	// rf.log[0] 为 dummy 条目（位于 lastIncludedIndex），
	// 因此实际持久化的最高逻辑索引 = lastIncludedIndex + len(rf.log) - 1。
	rf.persistedIndex = rf.lastIncludedIndex + len(rf.log) - 1

	// 快照由 InstallSnapshot 和 applier 自动处理

	// 注册 Raft 为 RPC 服务
	// 注意：TCP 监听由上层 KVServer 负责，不在这里创建
	rpc.Register(rf)

	go rf.ticker()
	go rf.applier()

	return rf
}
