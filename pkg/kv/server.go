package kv

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"
	"kvraft/pkg/persister"
	"kvraft/pkg/raft"
	"kvraft/pkg/storage"
	"kvraft/pkg/watch"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc"
)

func isExpired(expires int64, now int64) bool {
	return expires > 0 && expires <= now
}

func absoluteExpiryFromTTL(ttlSeconds int64, now int64) int64 {
	if ttlSeconds <= 0 {
		return 0
	}
	return now + ttlSeconds*int64(time.Second)
}

// KVServer - 高性能分布式键值存储服务器

type OperationInfo struct {
	Type       string
	Key        string
	OldValue   string
	NewValue   string
	OldVersion int64
	NewVersion int64
	Timestamp  time.Time
	Success    bool
	Error      Err
}

type KVServer struct {
	me               int
	groupID          int // 所属 Raft group（分片拓扑中的 group ID）
	dead             int32
	address          string
	rsm              RSMInterface
	mu               sync.RWMutex
	store            *storage.Store
	stats            *ServerStats
	leaseStatEnabled bool
	ttlEvery         time.Duration
	ttlBatch         int
	rpcLn            net.Listener
	grpcLn           net.Listener
	grpcSrv          *grpc.Server
	shardMgr         *shardStateManager // shard 状态机（迁移时控制双写/拒绝）
	txMgr            *TxManager          // 2PC 事务管理器
}


type ServerStats struct {
	TotalRequests  int64
	TotalWrites    int64
	TotalReads     int64
	FailedRequests int64
	WatchNotifies  int64
	LeaseHits      int64
	LeaseFallbacks int64
	TTLExpiredOps  int64
}

type RuntimePerfStats struct {
	Persister persister.PersisterMetrics
	Raft      raft.RaftPerfStats
	ApplyLoop ApplyLoopPerfStats
}

func NewKVServer(me int, groupID int, address string, store *storage.Store) *KVServer {
	kv := &KVServer{
		me:               me,
		groupID:          groupID,
		address:          address,
		store:            store,
		stats:            &ServerStats{},
		leaseStatEnabled: leaseStatsEnabledFromEnv(),
		ttlEvery:         2 * time.Second,
		ttlBatch:         128,
		shardMgr:         newShardStateManager(groupID, 1024),
	}
	kv.txMgr = NewTxManager(kv)
	return kv
}

// TopologyEpoch 返回本节点已知的拓扑版本号。
func (kv *KVServer) TopologyEpoch() int64 {
	return kv.shardMgr.GetEpoch()
}

// SetShardState 修改本节点上 shard 的状态（由迁移协调器调用）。
func (kv *KVServer) SetShardState(shardID int, state pb.ShardState, targetGroup int, epoch int64, targetReplicas []string) error {
	if err := kv.shardMgr.SetShardState(shardID, state, targetGroup, epoch); err != nil {
		return err
	}
	// 如果是 MIGRATING 状态，预先建立到目标 group 的转发连接
	if state == pb.ShardState_MIGRATING && len(targetReplicas) > 0 {
		if _, err := kv.shardMgr.ensureForwardClient(targetGroup, targetReplicas); err != nil {
			return fmt.Errorf("forward client setup failed: %w", err)
		}
	}
	return nil
}

// GetShardStates 返回所有 shard 状态快照。
func (kv *KVServer) GetShardStates() ([]*pb.ShardStateEntry, int64) {
	all := kv.shardMgr.AllStates()
	entries := make([]*pb.ShardStateEntry, 0, len(all))
	for shardID, meta := range all {
		entries = append(entries, &pb.ShardStateEntry{
			ShardId:     int32(shardID),
			State:       meta.state,
			TargetGroup: int32(meta.targetGroup),
		})
	}
	return entries, kv.shardMgr.GetEpoch()
}

func (kv *KVServer) DoOp(req any) any {
	switch req.(type) {
	case *GetArgs, GetArgs:
		return kv.doGet(reqPtr[GetArgs](req))
	case *PutArgs, PutArgs:
		return kv.doPut(reqPtr[PutArgs](req))
	case *DeleteArgs, DeleteArgs:
		return kv.doDelete(reqPtr[DeleteArgs](req))
	case *ScanArgs, ScanArgs:
		return kv.doScan(reqPtr[ScanArgs](req))
	case *ExpireArgs, ExpireArgs:
		return kv.doExpire(reqPtr[ExpireArgs](req))
	case *PrepareTxArgs, PrepareTxArgs:
		return kv.txMgr.Prepare(reqPtr[PrepareTxArgs](req))
	case *CommitTxArgs, CommitTxArgs:
		return kv.txMgr.Commit(reqPtr[CommitTxArgs](req))
	case *AbortTxArgs, AbortTxArgs:
		return kv.txMgr.Abort(reqPtr[AbortTxArgs](req))
	case *ResolveTxStatusArgs, ResolveTxStatusArgs:
		return kv.txMgr.ResolveTxStatus(reqPtr[ResolveTxStatusArgs](req))
	default:
		log.Printf("[KVServer-%d] Unknown request type: %T", kv.me, req)
		return GetReply{Err: ErrWrongLeader}
	}
}

func (kv *KVServer) doGet(args *GetArgs) GetReply {
	if kv.killed() {
		return GetReply{Err: ErrWrongLeader}
	}

	// shard 状态检查
	shardID := kv.shardForKey(args.Key)
	if meta, ok := kv.shardMgr.GetShardState(shardID); ok {
		if meta.state == pb.ShardState_ABSENT {
			return GetReply{Err: ErrWrongGroup}
		}
	}

	now := time.Now().UnixNano()
	value, version, expires, exists, err := kv.store.Get(args.Key)
	if err != nil {
		log.Printf("[KVServer-%d] Get error: %v", kv.me, err)
		kv.stats.RecordFailure()
		return GetReply{Err: ErrWrongLeader}
	}
	if exists && !isExpired(expires, now) {
		return GetReply{
			Value:   value,
			Version: Tversion(version),
			Expires: expires,
			Err:     OK,
		}
	}
	return GetReply{Err: ErrNoKey}
}

// shardForKey 计算 key 所属的 shard ID。
func (kv *KVServer) shardForKey(key string) int {
	return int(xxhash.Sum64String(key) % 1024)
}

func (kv *KVServer) doPut(args *PutArgs) PutReply {
	if kv.killed() {
		return PutReply{Err: ErrWrongLeader}
	}

	// shard 状态检查
	shardID := kv.shardForKey(args.Key)
	if meta, ok := kv.shardMgr.GetShardState(shardID); ok {
		switch meta.state {
		case pb.ShardState_ABSENT:
			return PutReply{Err: ErrWrongGroup}
		case pb.ShardState_MIGRATING:
			// 双写阶段：先写本地，再转发到目标 group
			return kv.doPutWithForward(args, meta)
		}
		// OWNED / IMPORTING：正常写入本地
	}

	now := time.Now().UnixNano()
	newExpires := absoluteExpiryFromTTL(args.TTL, now)
	oldValue, status, err := kv.store.PutCASWithTTL(args.Key, args.Value, uint64(args.Version), newExpires)
	if err != nil {
		log.Printf("[KVServer-%d] Put error: %v", kv.me, err)
		kv.stats.RecordFailure()
		return PutReply{Err: ErrWrongLeader}
	}
	switch status {
	case storage.PutCASOK:
		kv.stats.RecordWrite()
		return PutReply{Err: OK, OldValue: oldValue}
	case storage.PutCASVersionMismatch:
		kv.stats.RecordFailure()
		return PutReply{Err: ErrVersion}
	case storage.PutCASNoKey:
		kv.stats.RecordFailure()
		return PutReply{Err: ErrNoKey}
	default:
		kv.stats.RecordFailure()
		return PutReply{Err: ErrWrongLeader}
	}
}

// doPutWithForward 在 MIGRATING 状态下：先本地写入，再转发到目标 group。
// 转发失败则整体失败（CP 语义：双写必须同时成功）。
func (kv *KVServer) doPutWithForward(args *PutArgs, meta shardMeta) PutReply {
	now := time.Now().UnixNano()
	newExpires := absoluteExpiryFromTTL(args.TTL, now)
	oldValue, status, err := kv.store.PutCASWithTTL(args.Key, args.Value, uint64(args.Version), newExpires)
	if err != nil {
		log.Printf("[KVServer-%d] Put error during migration: %v", kv.me, err)
		kv.stats.RecordFailure()
		return PutReply{Err: ErrWrongLeader}
	}
	if status != storage.PutCASOK {
		kv.stats.RecordFailure()
		return PutReply{Err: ErrVersion}
	}
	kv.stats.RecordWrite()

	// 转发到目标 group
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if fwdErr := kv.shardMgr.ForwardWrite(ctx, meta.targetGroup, args.Key, args.Value, int64(args.Version)); fwdErr != nil {
		log.Printf("[KVServer-%d] forward write to group %d failed: %v", kv.me, meta.targetGroup, fwdErr)
		return PutReply{Err: ErrWrongGroup}
	}
	return PutReply{Err: OK, OldValue: oldValue}
}

// doDeleteWithForward 在 MIGRATING 状态下：本地删除后转发到目标 group。
func (kv *KVServer) doDeleteWithForward(args *DeleteArgs, meta shardMeta) DeleteReply {
	now := time.Now().UnixNano()
	oldValue, _, expires, exists, err := kv.store.Get(args.Key)
	if err != nil {
		log.Printf("[KVServer-%d] Get error during delete migration: %v", kv.me, err)
		kv.stats.RecordFailure()
		return DeleteReply{Err: ErrWrongLeader}
	}
	if exists && isExpired(expires, now) {
		exists = false
	}
	if !exists {
		return DeleteReply{Err: ErrNoKey}
	}
	if err := kv.store.Delete(args.Key); err != nil {
		log.Printf("[KVServer-%d] Delete error during migration: %v", kv.me, err)
		kv.stats.RecordFailure()
		return DeleteReply{Err: ErrWrongLeader}
	}
	kv.stats.RecordWrite()

	// 转发删除到目标 group
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if fwdErr := kv.shardMgr.ForwardDelete(ctx, meta.targetGroup, args.Key); fwdErr != nil {
		log.Printf("[KVServer-%d] forward delete to group %d failed: %v", kv.me, meta.targetGroup, fwdErr)
		return DeleteReply{Err: ErrWrongGroup}
	}
	return DeleteReply{Err: OK, OldValue: oldValue}
}

func (kv *KVServer) doDelete(args *DeleteArgs) DeleteReply {
	if kv.killed() {
		return DeleteReply{Err: ErrWrongLeader}
	}

	// shard 状态检查
	shardID := kv.shardForKey(args.Key)
	if meta, ok := kv.shardMgr.GetShardState(shardID); ok {
		switch meta.state {
		case pb.ShardState_ABSENT:
			return DeleteReply{Err: ErrWrongGroup}
		case pb.ShardState_MIGRATING:
			// 双写：本地删除 + 转发到目标 group
			return kv.doDeleteWithForward(args, meta)
		}
	}
	now := time.Now().UnixNano()
	oldValue, _, expires, exists, err := kv.store.Get(args.Key)
	if err != nil {
		log.Printf("[KVServer-%d] Get error during Delete: %v", kv.me, err)
		kv.stats.RecordFailure()
		return DeleteReply{Err: ErrWrongLeader}
	}
	if exists && isExpired(expires, now) {
		exists = false
	}
	if !exists {
		return DeleteReply{Err: ErrNoKey}
	}
	if err := kv.store.Delete(args.Key); err != nil {
		log.Printf("[KVServer-%d] Delete error: %v", kv.me, err)
		kv.stats.RecordFailure()
		return DeleteReply{Err: ErrWrongLeader}
	}
	kv.stats.RecordWrite()
	return DeleteReply{Err: OK, OldValue: oldValue}
}

func (kv *KVServer) doScan(args *ScanArgs) ScanReply {
	if kv.killed() {
		return ScanReply{Err: ErrWrongLeader}
	}
	limit := int(args.Limit)
	entries, keys, err := kv.store.ScanPrefix(args.Prefix, limit)
	if err != nil {
		log.Printf("[KVServer-%d] Scan ScanPrefix error: %v", kv.me, err)
		kv.stats.RecordFailure()
		return ScanReply{Err: ErrWrongLeader}
	}
	items := make([]ScanItem, 0, len(keys))
	now := time.Now().UnixNano()
	for i, k := range keys {
		v := entries[i]
		if isExpired(v.Expires, now) {
			continue
		}
		items = append(items, ScanItem{
			Key:     k,
			Value:   v.Value,
			Version: Tversion(v.Version),
			Expires: v.Expires,
		})
	}
	return ScanReply{Items: items, Err: OK}
}

func (kv *KVServer) doExpire(args *ExpireArgs) ExpireReply {
	if kv.killed() {
		return ExpireReply{Err: ErrWrongLeader}
	}
	if len(args.Keys) == 0 {
		return ExpireReply{Err: OK}
	}
	expired := make([]string, 0, len(args.Keys))
	expiredOldValues := make(map[string]string, len(args.Keys))
	for _, key := range args.Keys {
		// 跳过被事务锁定的 key（事务可能仍在进行中）
		if kv.txMgr != nil {
			kv.txMgr.mu.RLock()
			_, locked := kv.txMgr.lockTable[key]
			kv.txMgr.mu.RUnlock()
			if locked {
				continue
			}
		}
		oldValue, _, expires, exists, err := kv.store.Get(key)
		if err != nil {
			continue
		}
		if !exists || !isExpired(expires, args.Cutoff) {
			continue
		}
		if err := kv.store.Delete(key); err != nil {
			continue
		}
		expired = append(expired, key)
		expiredOldValues[key] = oldValue
	}
	if len(expired) > 0 {
		kv.stats.RecordTTLExpired(int64(len(expired)))
	}
	return ExpireReply{ExpiredKeys: expired, ExpiredOldValues: expiredOldValues, Err: OK}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if kv.killed() {
		return nil
	}
	data, err := kv.store.SaveSnapshot()
	if err != nil {
		log.Printf("[KVServer-%d] Snapshot error: %v", kv.me, err)
		return nil
	}
	return data
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}
	if err := kv.store.LoadSnapshot(data); err != nil {
		log.Printf("[KVServer-%d] Restore snapshot error: %v", kv.me, err)
	}
	// 快照恢复后重建 lock table（_tx:prepare:* 记录已随 store 恢复）
	if kv.txMgr != nil {
		if err := kv.txMgr.RebuildLockTable(); err != nil {
			log.Printf("[KVServer-%d] Restore: RebuildLockTable error: %v", kv.me, err)
		}
	}
}

func (kv *KVServer) OnOpComplete(req any, result any, index int64) {
	if kv.killed() {
		return
	}
	watchMgr := kv.rsm.GetWatchManager()
	if watchMgr == nil {
		return
	}
	switch req.(type) {
	case *PutArgs, PutArgs:
		kv.notifyPutEvent(reqPtr[PutArgs](req), result, watchMgr)
	case *DeleteArgs, DeleteArgs:
		kv.notifyDeleteEvent(reqPtr[DeleteArgs](req), result, watchMgr)
	case *ExpireArgs, ExpireArgs:
		kv.notifyExpireEvent(reqPtr[ExpireArgs](req), result, watchMgr)
	}
}

func (kv *KVServer) notifyPutEvent(putArgs *PutArgs, result any, watchMgr *watch.Manager) {
	putReply, ok := result.(PutReply)
	if !ok {
		return
	}
	if putReply.Err == OK {
		oldValueStr := putReply.OldValue
		eventType := "PUT"
		if oldValueStr == "" {
			eventType = "SET"
		}
		err := watchMgr.Notify(putArgs.Key, oldValueStr, putArgs.Value, int64(putArgs.Version), eventType)
		if err == nil {
			kv.stats.RecordWatchNotify()
		}
	}
}

func (kv *KVServer) notifyDeleteEvent(delArgs *DeleteArgs, result any, watchMgr *watch.Manager) {
	delReply, ok := result.(DeleteReply)
	if !ok || delReply.Err != OK {
		return
	}
	err := watchMgr.Notify(delArgs.Key, delReply.OldValue, "", 0, "DELETE")
	if err == nil {
		kv.stats.RecordWatchNotify()
	}
}

func (kv *KVServer) notifyExpireEvent(_ *ExpireArgs, result any, watchMgr *watch.Manager) {
	expReply, ok := result.(ExpireReply)
	if !ok || expReply.Err != OK {
		return
	}
	for _, key := range expReply.ExpiredKeys {
		oldValue := ""
		if expReply.ExpiredOldValues != nil {
			oldValue = expReply.ExpiredOldValues[key]
		}
		err := watchMgr.Notify(key, oldValue, "", 0, "EXPIRE")
		if err == nil {
			kv.stats.RecordWatchNotify()
		}
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	if kv.grpcSrv != nil {
		kv.grpcSrv.Stop()
	}
	if kv.grpcLn != nil {
		kv.grpcLn.Close()
	}
	if kv.rpcLn != nil {
		kv.rpcLn.Close()
	}
	if kv.rsm != nil {
		kv.rsm.Close()
	}
	if kv.store != nil {
		if err := kv.store.Close(); err != nil {
			log.Printf("[KVServer-%d] close store error: %v", kv.me, err)
		}
	}
	if kv.shardMgr != nil {
		kv.shardMgr.Close()
	}
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *KVServer) IsAlive() bool {
	return !kv.killed()
}

func (kv *KVServer) StatsSnapshot() ServerStats {
	return ServerStats{
		TotalRequests:  atomic.LoadInt64(&kv.stats.TotalRequests),
		TotalWrites:    atomic.LoadInt64(&kv.stats.TotalWrites),
		TotalReads:     atomic.LoadInt64(&kv.stats.TotalReads),
		FailedRequests: atomic.LoadInt64(&kv.stats.FailedRequests),
		WatchNotifies:  atomic.LoadInt64(&kv.stats.WatchNotifies),
		LeaseHits:      atomic.LoadInt64(&kv.stats.LeaseHits),
		LeaseFallbacks: atomic.LoadInt64(&kv.stats.LeaseFallbacks),
		TTLExpiredOps:  atomic.LoadInt64(&kv.stats.TTLExpiredOps),
	}
}

func (kv *KVServer) PerfSnapshot() RuntimePerfStats {
	perf := RuntimePerfStats{}
	if kv == nil || kv.rsm == nil {
		return perf
	}
	perf.Raft = kv.rsm.RaftPerfStatsSnapshot()
	return perf
}

func (s *ServerStats) RecordRead() {
	atomic.AddInt64(&s.TotalRequests, 1)
	atomic.AddInt64(&s.TotalReads, 1)
}

func (s *ServerStats) RecordWrite() {
	atomic.AddInt64(&s.TotalRequests, 1)
	atomic.AddInt64(&s.TotalWrites, 1)
}

func (s *ServerStats) RecordFailure() {
	atomic.AddInt64(&s.FailedRequests, 1)
}

func (s *ServerStats) RecordWatchNotify() {
	atomic.AddInt64(&s.WatchNotifies, 1)
}

func (s *ServerStats) RecordLeaseHit() {
	atomic.AddInt64(&s.LeaseHits, 1)
}

func (s *ServerStats) RecordLeaseFallback() {
	atomic.AddInt64(&s.LeaseFallbacks, 1)
}

func (s *ServerStats) RecordTTLExpired(n int64) {
	atomic.AddInt64(&s.TTLExpiredOps, n)
}

func (s *ServerStats) GetStats() (requests, writes, reads, failures int64) {
	return atomic.LoadInt64(&s.TotalRequests),
		atomic.LoadInt64(&s.TotalWrites),
		atomic.LoadInt64(&s.TotalReads),
		atomic.LoadInt64(&s.FailedRequests)
}

func (kv *KVServer) SetRSM(rsm RSMInterface) {
	kv.rsm = rsm
}

func (kv *KVServer) SetRPCListener(l net.Listener) {
	kv.rpcLn = l
}

func (kv *KVServer) TTLCleanupLoop() {
	ticker := time.NewTicker(kv.ttlEvery)
	defer ticker.Stop()
	for !kv.killed() {
		<-ticker.C
		if kv.killed() {
			return
		}
		_, isLeader := kv.rsm.GetState()
		if !isLeader {
			continue
		}
		now := time.Now().UnixNano()
		keys, err := kv.store.GetExpiredKeys(now, kv.ttlBatch)
		if err != nil || len(keys) == 0 {
			continue
		}
		args := &ExpireArgs{Keys: keys, Cutoff: now}
		_, _ = kv.rsm.Submit(args)
	}
}
