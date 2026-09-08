package kv

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"kvraft/pkg/storage"
)

const (
	txDefaultTimeout = 10 * time.Second
	txPreparePrefix  = "prepare:"
	txCommitPrefix   = "commit:"
	txAbortPrefix    = "abort:"
	txDecisionPrefix = "decision:"
)

// preparedTxRecord 是 _tx:prepare:{txID} 的磁盘存储格式。
type preparedTxRecord struct {
	TxID                string     `json:"tx_id"`
	ReadKeys            []ReadKey  `json:"read_keys"`
	WriteKeys           []WriteKey `json:"write_keys"`
	PreparedAt          int64      `json:"prepared_at"`
	TimeoutMs           int64      `json:"timeout_ms"`
	CoordinatorGroupID  int        `json:"coordinator_group_id"`
	ParticipantGroupIDs []int      `json:"participant_group_ids"`
}

// TxManager 管理单个 Raft Group 内的 2PC 参与者状态。
// lock table 是内存中的派生状态，可通过扫描 _tx:prepare:* 记录重建。
type TxManager struct {
	mu          sync.RWMutex
	lockTable   map[string]string            // key → txID
	preparedTxs map[string]*preparedTxRecord // txID → 记录（内存缓存）
	kv          *KVServer
}

// NewTxManager 创建一个新的事务管理器。
func NewTxManager(kv *KVServer) *TxManager {
	return &TxManager{
		lockTable:   make(map[string]string),
		preparedTxs: make(map[string]*preparedTxRecord),
		kv:          kv,
	}
}

// RebuildLockTable 扫描所有 _tx:prepare:* 记录并重建内存中的 lock table
// 和 preparedTxs 缓存。在启动时和快照恢复后调用。
func (tm *TxManager) RebuildLockTable() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.lockTable = make(map[string]string)
	tm.preparedTxs = make(map[string]*preparedTxRecord)

	records, err := tm.kv.store.ScanTxRecordsByPrefix(txPreparePrefix)
	if err != nil {
		return fmt.Errorf("RebuildLockTable scan: %w", err)
	}

	for key, raw := range records {
		var rec preparedTxRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			log.Printf("[TxManager] RebuildLockTable: 跳过损坏记录 %s: %v", key, err)
			continue
		}

		// 检查该事务是否已提交或已回滚
		if _, found, _ := tm.kv.store.GetTxRecord(txCommitPrefix + rec.TxID); found {
			// 已提交 — 清理残留的 prepare 记录
			_ = tm.kv.store.DeleteTxRecord(txPreparePrefix + rec.TxID)
			continue
		}
		if _, found, _ := tm.kv.store.GetTxRecord(txAbortPrefix + rec.TxID); found {
			// 已回滚 — 清理残留的 prepare 记录
			_ = tm.kv.store.DeleteTxRecord(txPreparePrefix + rec.TxID)
			continue
		}

		if len(rec.ParticipantGroupIDs) == 0 && rec.TimeoutMs > 0 && time.Now().UnixNano() > rec.PreparedAt+rec.TimeoutMs*int64(time.Millisecond) {
			_ = tm.kv.store.PutTxRecord(txAbortPrefix+rec.TxID, []byte("1"))
			_ = tm.kv.store.DeleteTxRecord(txPreparePrefix + rec.TxID)
			continue
		}

		// 有效的 prepare 事务 — 重建锁
		for _, wk := range rec.WriteKeys {
			tm.lockTable[wk.Key] = rec.TxID
		}
		tm.preparedTxs[rec.TxID] = &rec
	}

	log.Printf("[TxManager] RebuildLockTable: %d 个已 prepare 事务, %d 个已锁定 key", len(tm.preparedTxs), len(tm.lockTable))
	return nil
}

// Prepare 执行 2PC 的 Phase 1：
//  1. 检查 write-set 中的 key 是否已被其他事务锁定
//  2. 检查 read-set 中每个 key 的当前版本是否与 ExpectedVersion 一致
//  3. 加写锁：lockTable[key] = txID
//  4. 持久化 _tx:prepare:{txID} 记录
func (tm *TxManager) Prepare(args *PrepareTxArgs) PrepareTxReply {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	now := time.Now().UnixNano()

	// 1. 检查冲突锁并解决过期锁
	for _, wk := range args.WriteKeys {
		lockHolder, locked := tm.lockTable[wk.Key]
		if locked && lockHolder != args.TxID {
			// 尝试解决过期锁
			resolved := tm.resolveLockLocked(lockHolder)
			if !resolved {
				return PrepareTxReply{Err: ErrTxConflict}
			}
			// 解决后重新检查
			if holder, stillLocked := tm.lockTable[wk.Key]; stillLocked && holder != args.TxID {
				return PrepareTxReply{Err: ErrTxConflict}
			}
		}
	}

	// 2. 校验读集版本
	for _, rk := range args.ReadKeys {
		value, version, expires, exists, err := tm.kv.store.Get(rk.Key)
		if err != nil {
			log.Printf("[TxManager] Prepare 读取 %s 失败: %v", rk.Key, err)
			return PrepareTxReply{Err: ErrWrongLeader}
		}
		currentVersion := Tversion(version)
		if exists && isExpired(expires, now) {
			exists = false
			currentVersion = 0
		}
		_ = value

		expectedVersion := rk.ExpectedVersion
		if !exists && expectedVersion != 0 {
			return PrepareTxReply{Err: ErrTxConflict}
		}
		if exists && expectedVersion != currentVersion {
			return PrepareTxReply{Err: ErrTxConflict}
		}
	}

	// 3. 获取写锁
	for _, wk := range args.WriteKeys {
		tm.lockTable[wk.Key] = args.TxID
	}

	// 4. 持久化 prepare 记录
	timeoutMs := args.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = int64(txDefaultTimeout / time.Millisecond)
	}
	rec := preparedTxRecord{
		TxID:                args.TxID,
		ReadKeys:            args.ReadKeys,
		WriteKeys:           args.WriteKeys,
		PreparedAt:          now,
		TimeoutMs:           timeoutMs,
		CoordinatorGroupID:  args.CoordinatorGroupID,
		ParticipantGroupIDs: append([]int(nil), args.ParticipantGroupIDs...),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		// 序列化失败时释放锁
		for _, wk := range args.WriteKeys {
			delete(tm.lockTable, wk.Key)
		}
		return PrepareTxReply{Err: ErrWrongLeader}
	}
	if err := tm.kv.store.PutTxRecord(txPreparePrefix+args.TxID, raw); err != nil {
		// 存储失败时释放锁
		for _, wk := range args.WriteKeys {
			delete(tm.lockTable, wk.Key)
		}
		log.Printf("[TxManager] Prepare 持久化失败: %v", err)
		return PrepareTxReply{Err: ErrWrongLeader}
	}

	// 缓存到内存
	tm.preparedTxs[args.TxID] = &rec

	return PrepareTxReply{Err: OK}
}

type txDecisionRecord struct {
	Decision            TxStatus `json:"decision"`
	ParticipantGroupIDs []int    `json:"participant_group_ids"`
}

func (tm *TxManager) RecordDecision(args *RecordTxDecisionArgs) RecordTxDecisionReply {
	if args.Decision != TxStatusCommitted && args.Decision != TxStatusAborted {
		return RecordTxDecisionReply{Err: ErrTxConflict}
	}
	key := txDecisionPrefix + args.TxID
	if raw, found, _ := tm.kv.store.GetTxRecord(key); found {
		var existing txDecisionRecord
		if json.Unmarshal(raw, &existing) == nil && existing.Decision == args.Decision {
			return RecordTxDecisionReply{Err: OK}
		}
		return RecordTxDecisionReply{Err: ErrTxConflict}
	}
	raw, err := json.Marshal(txDecisionRecord{Decision: args.Decision, ParticipantGroupIDs: append([]int(nil), args.ParticipantGroupIDs...)})
	if err != nil || tm.kv.store.PutTxRecord(key, raw) != nil {
		return RecordTxDecisionReply{Err: ErrWrongLeader}
	}
	return RecordTxDecisionReply{Err: OK}
}

// Commit 执行 2PC 的 Phase 2：
//  1. 幂等检查：commit 或 abort 记录是否已存在
//  2. WriteBatchWithCASAndRecord 在单个 BadgerDB txn 中原子执行：
//     CAS 批量写入所有 write-set key + 持久化 _tx:commit:{txID}
//  3. 释放锁并清理 prepare 记录
func (tm *TxManager) Commit(args *CommitTxArgs) CommitTxReply {
	tm.mu.Lock()

	// 1. 幂等检查
	if _, found, _ := tm.kv.store.GetTxRecord(txCommitPrefix + args.TxID); found {
		tm.mu.Unlock()
		return CommitTxReply{Err: OK} // 已提交
	}
	if _, found, _ := tm.kv.store.GetTxRecord(txAbortPrefix + args.TxID); found {
		tm.mu.Unlock()
		return CommitTxReply{Err: ErrTxConflict} // 已回滚
	}

	if raw, found, _ := tm.kv.store.GetTxRecord(txPreparePrefix + args.TxID); found {
		var pending preparedTxRecord
		if json.Unmarshal(raw, &pending) == nil && len(pending.ParticipantGroupIDs) > 0 && tm.kv.groupID == pending.CoordinatorGroupID {
			if decision, ok, _ := tm.kv.store.GetTxRecord(txDecisionPrefix + args.TxID); !ok {
				return CommitTxReply{Err: ErrTxConflict}
			} else {
				var d txDecisionRecord
				if json.Unmarshal(decision, &d) != nil || d.Decision != TxStatusCommitted {
					return CommitTxReply{Err: ErrTxConflict}
				}
			}
		}
	}

	// Use the durable Prepare write set; never trust a different phase-2 payload.
	rec, prepared := tm.preparedTxs[args.TxID]
	tm.mu.Unlock()

	if !prepared {
		// 尝试从磁盘加载（可能重启过）
		raw, found, _ := tm.kv.store.GetTxRecord(txPreparePrefix + args.TxID)
		if !found {
			return CommitTxReply{Err: ErrTxNotFound}
		}
		var diskRec preparedTxRecord
		if err := json.Unmarshal(raw, &diskRec); err != nil {
			return CommitTxReply{Err: ErrTxNotFound}
		}
		// 重新加载到缓存并重建锁
		tm.mu.Lock()
		rec = &diskRec
		tm.preparedTxs[args.TxID] = rec
		for _, wk := range rec.WriteKeys {
			tm.lockTable[wk.Key] = rec.TxID
		}
		tm.mu.Unlock()
	}

	// 2. 构建 WriteBatchOps
	ops := make([]storage.WriteBatchOp, 0, len(rec.WriteKeys))
	for _, wk := range rec.WriteKeys {
		ops = append(ops, storage.WriteBatchOp{
			Key:             wk.Key,
			Value:           wk.Value,
			ExpectedVersion: uint64(wk.Version),
			IsDelete:        wk.IsDelete,
		})
	}

	// 3. 在单个 BadgerDB 事务中原子执行：CAS 批量写入 + commit 记录持久化。
	// 两者要么一起落盘，要么一起回滚，消除崩溃窗口——
	// 防止用户数据已写入但 _tx:commit:{txID} 丢失，导致 Raft 重放时
	// 版本冲突、事务永久卡死。
	commitData := []byte("1")
	if err := tm.kv.store.WriteBatchWithCASAndRecord(ops, txCommitPrefix+args.TxID, commitData); err != nil {
		log.Printf("[TxManager] Commit 原子写入失败: %v", err)
		return CommitTxReply{Err: ErrTxConflict}
	}

	// 4. 释放锁并清理
	tm.mu.Lock()
	for _, wk := range rec.WriteKeys {
		delete(tm.lockTable, wk.Key)
	}
	delete(tm.preparedTxs, args.TxID)
	tm.mu.Unlock()

	// 清理 prepare 记录（尽力而为）
	_ = tm.kv.store.DeleteTxRecord(txPreparePrefix + args.TxID)

	return CommitTxReply{Err: OK}
}

// Abort 释放事务持有的所有锁并持久化 abort 记录。
// 必须先持久化 abort 记录再释放内存锁，防止崩溃后锁被错误重建。
func (tm *TxManager) Abort(args *AbortTxArgs) AbortTxReply {
	tm.mu.Lock()

	// 检查是否已提交 — 已提交的事务不能回滚
	if _, found, _ := tm.kv.store.GetTxRecord(txCommitPrefix + args.TxID); found {
		tm.mu.Unlock()
		return AbortTxReply{Err: ErrTxConflict} // 提交后不能回滚
	}

	// 检查是否已回滚
	if _, found, _ := tm.kv.store.GetTxRecord(txAbortPrefix + args.TxID); found {
		tm.mu.Unlock()
		return AbortTxReply{Err: OK} // 已回滚
	}

	// 先持久化 abort 记录，再释放内存锁（防止崩溃导致锁被错误重建）
	tm.mu.Unlock()

	abortData := []byte("1")
	if err := tm.kv.store.PutTxRecord(txAbortPrefix+args.TxID, abortData); err != nil {
		log.Printf("[TxManager] Abort 持久化失败: %v", err)
		return AbortTxReply{Err: ErrWrongLeader}
	}

	// 持久化成功后再释放内存锁
	tm.mu.Lock()
	rec, ok := tm.preparedTxs[args.TxID]
	if ok {
		for _, wk := range rec.WriteKeys {
			if holder, exists := tm.lockTable[wk.Key]; exists && holder == args.TxID {
				delete(tm.lockTable, wk.Key)
			}
		}
		delete(tm.preparedTxs, args.TxID)
	}
	tm.mu.Unlock()

	// 清理 prepare 记录（尽力而为）
	_ = tm.kv.store.DeleteTxRecord(txPreparePrefix + args.TxID)

	return AbortTxReply{Err: OK}
}

// ResolveTxStatus 返回事务的当前状态。
// 按顺序检查：commit → abort → prepare → not found。
func (tm *TxManager) ResolveTxStatus(args *ResolveTxStatusArgs) ResolveTxStatusReply {
	if raw, found, _ := tm.kv.store.GetTxRecord(txDecisionPrefix + args.TxID); found {
		var d txDecisionRecord
		if json.Unmarshal(raw, &d) == nil {
			return ResolveTxStatusReply{Status: d.Decision, ParticipantGroupIDs: d.ParticipantGroupIDs, Err: OK}
		}
	}
	// 检查 commit
	if commitData, found, _ := tm.kv.store.GetTxRecord(txCommitPrefix + args.TxID); found {
		_ = commitData
		return ResolveTxStatusReply{Status: TxStatusCommitted, Err: OK}
	}

	// 检查 abort
	if _, found, _ := tm.kv.store.GetTxRecord(txAbortPrefix + args.TxID); found {
		return ResolveTxStatusReply{Status: TxStatusAborted, Err: OK}
	}

	// 检查 prepare
	if raw, found, _ := tm.kv.store.GetTxRecord(txPreparePrefix + args.TxID); found {
		var rec preparedTxRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return ResolveTxStatusReply{Status: TxStatusNotFound, Err: ErrTxNotFound}
		}

		if len(rec.ParticipantGroupIDs) == 0 && rec.TimeoutMs > 0 && time.Now().UnixNano() > rec.PreparedAt+rec.TimeoutMs*int64(time.Millisecond) {
			tm.mu.Lock()
			tm.releaseLocksForTxLocked(args.TxID)
			delete(tm.preparedTxs, args.TxID)
			tm.mu.Unlock()
			_ = tm.kv.store.PutTxRecord(txAbortPrefix+args.TxID, []byte("1"))
			_ = tm.kv.store.DeleteTxRecord(txPreparePrefix + args.TxID)
			return ResolveTxStatusReply{Status: TxStatusAborted, Err: OK}
		}

		return ResolveTxStatusReply{
			Status:              TxStatusPrepared,
			PreparedAt:          rec.PreparedAt,
			WriteKeys:           rec.WriteKeys,
			Err:                 OK,
			CoordinatorGroupID:  rec.CoordinatorGroupID,
			ParticipantGroupIDs: append([]int(nil), rec.ParticipantGroupIDs...),
		}
	}

	return ResolveTxStatusReply{Status: TxStatusNotFound, Err: ErrTxNotFound}
}

// resolveLockLocked 尝试解决 txID 持有的锁。必须在持有 tm.mu 的情况下调用。
// 如果锁被释放（事务已提交/已回滚/已超时），返回 true。
func (tm *TxManager) resolveLockLocked(txID string) bool {
	// 检查 commit 记录
	if _, found, _ := tm.kv.store.GetTxRecord(txCommitPrefix + txID); found {
		tm.releaseLocksForTxLocked(txID)
		return true
	}

	// 检查 abort 记录
	if _, found, _ := tm.kv.store.GetTxRecord(txAbortPrefix + txID); found {
		tm.releaseLocksForTxLocked(txID)
		return true
	}

	// 检查是否已 prepare
	rec, ok := tm.preparedTxs[txID]
	if !ok {
		// 尝试从磁盘加载
		raw, found, _ := tm.kv.store.GetTxRecord(txPreparePrefix + txID)
		if found {
			var diskRec preparedTxRecord
			if err := json.Unmarshal(raw, &diskRec); err == nil {
				rec = &diskRec
				tm.preparedTxs[txID] = rec
			}
		}
		if rec == nil {
			// 未找到 prepare 记录 — 过期锁，直接释放
			tm.releaseLocksForTxLocked(txID)
			return true
		}
	}

	if len(rec.ParticipantGroupIDs) == 0 && rec.TimeoutMs > 0 && time.Now().UnixNano() > rec.PreparedAt+rec.TimeoutMs*int64(time.Millisecond) {
		tm.releaseLocksForTxLocked(txID)
		delete(tm.preparedTxs, txID)
		_ = tm.kv.store.PutTxRecord(txAbortPrefix+txID, []byte("1"))
		_ = tm.kv.store.DeleteTxRecord(txPreparePrefix + txID)
		return true
	}

	// 仍然是有效的 prepare 事务 — 锁未被释放
	return false
}

// releaseLocksForTxLocked 删除 txID 在 lockTable 中的所有条目。必须持有 tm.mu。
func (tm *TxManager) releaseLocksForTxLocked(txID string) {
	for key, holder := range tm.lockTable {
		if holder == txID {
			delete(tm.lockTable, key)
		}
	}
}
