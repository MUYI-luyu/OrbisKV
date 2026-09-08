package kv

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"kvraft/pkg/storage"
)

// setupTestTxManager 创建一个用于测试的 TxManager，
// 带有独立的 BadgerDB 存储和最小化的 KVServer。
func setupTestTxManager(t *testing.T) (*TxManager, *KVServer, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "kvraft-tx-test-*")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}

	store, err := storage.NewStore(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("创建 Store 失败: %v", err)
	}

	kv := &KVServer{
		me:       0,
		groupID:  0,
		address:  "test",
		store:    store,
		stats:    &ServerStats{},
		shardMgr: newShardStateManager(0, 1024),
	}
	kv.txMgr = NewTxManager(kv)

	cleanup := func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}

	return kv.txMgr, kv, cleanup
}

// seedKey 向存储中写入一个 key，用于测试读集版本校验。
func seedKey(t *testing.T, kv *KVServer, key, value string, version Tversion) {
	t.Helper()
	err := kv.store.Put(key, value, uint64(version), 0)
	if err != nil {
		t.Fatalf("seedKey %s 失败: %v", key, err)
	}
}

// getKey 从存储中读取 key 的当前值和版本号。
func getKey(t *testing.T, kv *KVServer, key string) (string, Tversion, bool) {
	t.Helper()
	val, ver, _, exists, err := kv.store.Get(key)
	if err != nil {
		t.Fatalf("getKey %s 失败: %v", key, err)
	}
	return val, Tversion(ver), exists
}

func TestCleanupShardDeletesMatchingVersionsAtomically(t *testing.T) {
	_, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	key := "cleanup-key"
	seedKey(t, kv, key, "value", 3)
	shardID := kv.shardForKey(key)
	reply := kv.doCleanupShard(&CleanupShardArgs{ShardID: shardID, Keys: []CleanupShardKey{{Key: key, ExpectedVersion: 3}}})
	if reply.Err != OK || reply.Deleted != 1 {
		t.Fatalf("cleanup failed: %+v", reply)
	}
	if _, _, exists := getKey(t, kv, key); exists {
		t.Fatal("cleanup key still exists")
	}
}

func TestCleanupShardVersionConflictKeepsBatch(t *testing.T) {
	_, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	key1 := "cleanup-a"
	shardID := kv.shardForKey(key1)
	key2 := ""
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("cleanup-b-%d", i)
		if kv.shardForKey(candidate) == shardID {
			key2 = candidate
			break
		}
	}
	if key2 == "" {
		t.Fatal("failed to find second key in shard")
	}
	seedKey(t, kv, key1, "a", 2)
	seedKey(t, kv, key2, "b", 4)
	reply := kv.doCleanupShard(&CleanupShardArgs{ShardID: shardID, Keys: []CleanupShardKey{
		{Key: key1, ExpectedVersion: 2},
		{Key: key2, ExpectedVersion: 3},
	}})
	if reply.Err != ErrVersion {
		t.Fatalf("want ErrVersion, got %+v", reply)
	}
	if _, _, exists := getKey(t, kv, key1); !exists {
		t.Fatal("atomic cleanup deleted first key on later conflict")
	}
	if _, _, exists := getKey(t, kv, key2); !exists {
		t.Fatal("conflicting key was deleted")
	}
}

func TestCleanupShardRejectsKeyFromAnotherShard(t *testing.T) {
	_, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	key := "cleanup-wrong-shard"
	seedKey(t, kv, key, "value", 1)
	wrongShardID := (kv.shardForKey(key) + 1) % 1024
	reply := kv.doCleanupShard(&CleanupShardArgs{ShardID: wrongShardID, Keys: []CleanupShardKey{{Key: key, ExpectedVersion: 1}}})
	if reply.Err != ErrWrongGroup {
		t.Fatalf("want ErrWrongGroup, got %+v", reply)
	}
	if _, _, exists := getKey(t, kv, key); !exists {
		t.Fatal("cleanup deleted a key outside the requested shard")
	}
}

func TestCleanupShardWALRoundTrip(t *testing.T) {
	want := &CleanupShardArgs{ShardID: 37, Keys: []CleanupShardKey{
		{Key: "cleanup-wal-a", ExpectedVersion: 4},
		{Key: "cleanup-wal-b", ExpectedVersion: 9},
	}}
	entry := walEntryFromOp(2, 11, 3, Op{Me: 2, Id: 7, Req: want})
	gotReq, mutates, err := walEntryToRequest(entry)
	if err != nil {
		t.Fatalf("WAL round trip failed: %v", err)
	}
	if !mutates {
		t.Fatal("cleanup WAL entry must be replayed as a mutation")
	}
	got, ok := gotReq.(*CleanupShardArgs)
	if !ok {
		t.Fatalf("unexpected WAL request type %T", gotReq)
	}
	if got.ShardID != want.ShardID || len(got.Keys) != len(want.Keys) {
		t.Fatalf("cleanup WAL metadata changed: got %+v want %+v", got, want)
	}
	for i := range want.Keys {
		if got.Keys[i] != want.Keys[i] {
			t.Fatalf("cleanup WAL key %d changed: got %+v want %+v", i, got.Keys[i], want.Keys[i])
		}
	}
}

func TestTxWALRoundTripPreservesTransactionPayload(t *testing.T) {
	wantPrepare := &PrepareTxArgs{TxID: "wal-tx", ReadKeys: []ReadKey{{Key: "read", ExpectedVersion: 4}}, WriteKeys: []WriteKey{{Key: "put", Value: "v", Version: 2}, {Key: "del", Version: 7, IsDelete: true}}, TimeoutMs: 1234}
	prepEntry := walEntryFromOp(1, 3, 2, Op{Req: wantPrepare})
	gotReq, mutates, err := walEntryToRequest(prepEntry)
	if err != nil || !mutates {
		t.Fatalf("prepare WAL decode failed: mutates=%v err=%v", mutates, err)
	}
	gotPrepare, ok := gotReq.(*PrepareTxArgs)
	if !ok || gotPrepare.TxID != wantPrepare.TxID || len(gotPrepare.ReadKeys) != 1 || len(gotPrepare.WriteKeys) != 2 || !gotPrepare.WriteKeys[1].IsDelete || gotPrepare.TimeoutMs != wantPrepare.TimeoutMs {
		t.Fatalf("prepare WAL payload lost: %#v", gotReq)
	}
	wantCommit := &CommitTxArgs{TxID: "wal-tx", WriteKeys: wantPrepare.WriteKeys}
	commitEntry := walEntryFromOp(1, 4, 2, Op{Req: wantCommit})
	gotReq, mutates, err = walEntryToRequest(commitEntry)
	if err != nil || !mutates {
		t.Fatalf("commit WAL decode failed: mutates=%v err=%v", mutates, err)
	}
	gotCommit, ok := gotReq.(*CommitTxArgs)
	if !ok || len(gotCommit.WriteKeys) != 2 || !gotCommit.WriteKeys[1].IsDelete {
		t.Fatalf("commit WAL payload lost: %#v", gotReq)
	}
}

// ========== Prepare 阶段测试 ==========

func TestPrepareSuccess(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	// 写入初始数据：a=1, b=2
	seedKey(t, kv, "a", "val-a", 1)
	seedKey(t, kv, "b", "val-b", 2)

	reply := tm.Prepare(&PrepareTxArgs{
		TxID: "tx-001",
		ReadKeys: []ReadKey{
			{Key: "a", ExpectedVersion: 1},
		},
		WriteKeys: []WriteKey{
			{Key: "b", Value: "new-b", Version: 2},
		},
		TimeoutMs: 10000,
	})

	if reply.Err != OK {
		t.Fatalf("Prepare 应返回 OK，实际返回: %s", reply.Err)
	}

	// 验证锁已被获取
	tm.mu.RLock()
	holder := tm.lockTable["b"]
	tm.mu.RUnlock()
	if holder != "tx-001" {
		t.Fatalf("lockTable[b] 应为 tx-001，实际为: %s", holder)
	}

	// 验证 prepare 记录已持久化
	raw, found, _ := kv.store.GetTxRecord("prepare:tx-001")
	if !found {
		t.Fatal("prepare 记录未持久化")
	}
	var rec preparedTxRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatalf("prepare 记录解析失败: %v", err)
	}
	if rec.TxID != "tx-001" {
		t.Fatalf("prepare 记录 txID 不匹配: %s", rec.TxID)
	}
}

func TestPrepareWriteLockConflict(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)
	seedKey(t, kv, "b", "val-b", 2)

	// Tx1 Prepare 成功，锁定 b
	reply1 := tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-001",
		ReadKeys:  []ReadKey{{Key: "a", ExpectedVersion: 1}},
		WriteKeys: []WriteKey{{Key: "b", Value: "new-b", Version: 2}},
		TimeoutMs: 10000,
	})
	if reply1.Err != OK {
		t.Fatalf("Tx1 Prepare 应返回 OK，实际返回: %s", reply1.Err)
	}

	// Tx2 尝试锁定 b，应冲突
	reply2 := tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-002",
		ReadKeys:  nil,
		WriteKeys: []WriteKey{{Key: "b", Value: "other", Version: 2}},
		TimeoutMs: 10000,
	})
	if reply2.Err != ErrTxConflict {
		t.Fatalf("Tx2 Prepare 应返回 ErrTxConflict，实际返回: %s", reply2.Err)
	}
}

func TestPrepareReadVersionConflict(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 5)

	// 使用错误的 ExpectedVersion 进行 Prepare
	reply := tm.Prepare(&PrepareTxArgs{
		TxID: "tx-001",
		ReadKeys: []ReadKey{
			{Key: "a", ExpectedVersion: 3}, // 实际版本是 5
		},
		WriteKeys: []WriteKey{
			{Key: "b", Value: "new-b", Version: 0},
		},
		TimeoutMs: 10000,
	})

	if reply.Err != ErrTxConflict {
		t.Fatalf("Prepare 应返回 ErrTxConflict（读集版本冲突），实际返回: %s", reply.Err)
	}

	// 冲突后锁不应被持有
	tm.mu.RLock()
	_, locked := tm.lockTable["b"]
	tm.mu.RUnlock()
	if locked {
		t.Fatal("读集冲突后不应持有写锁")
	}
}

func TestPrepareReadVersionKeyNotExist(t *testing.T) {
	tm, _, cleanup := setupTestTxManager(t)
	defer cleanup()

	// key "a" 不存在，但 ExpectedVersion 设为 1（期望存在）
	reply := tm.Prepare(&PrepareTxArgs{
		TxID: "tx-001",
		ReadKeys: []ReadKey{
			{Key: "a", ExpectedVersion: 1}, // key 不存在但期望版本不为 0
		},
		WriteKeys: []WriteKey{
			{Key: "b", Value: "new-b", Version: 0},
		},
		TimeoutMs: 10000,
	})

	if reply.Err != ErrTxConflict {
		t.Fatalf("Prepare 应返回 ErrTxConflict（key 不存在但期望版本非零），实际返回: %s", reply.Err)
	}
}

func TestPrepareReadVersionKeyNotExistExpectedZero(t *testing.T) {
	tm, _, cleanup := setupTestTxManager(t)
	defer cleanup()

	// key "a" 不存在，ExpectedVersion=0 表示 "key 应不存在"
	reply := tm.Prepare(&PrepareTxArgs{
		TxID: "tx-001",
		ReadKeys: []ReadKey{
			{Key: "a", ExpectedVersion: 0}, // 期望 key 不存在
		},
		WriteKeys: []WriteKey{
			{Key: "b", Value: "new-b", Version: 0},
		},
		TimeoutMs: 10000,
	})

	if reply.Err != OK {
		t.Fatalf("Prepare 应返回 OK（key 不存在且 ExpectedVersion=0），实际返回: %s", reply.Err)
	}
}

func TestPrepareReadVersionExpiredKey(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	// 写入已过期的 key（Expires 在过去）
	pastExpiry := time.Now().UnixNano() - int64(time.Hour)
	err := kv.store.Put("a", "val-a", 5, pastExpiry)
	if err != nil {
		t.Fatalf("写入过期 key 失败: %v", err)
	}

	// 期望版本为 5（实际存储的版本），但 key 已过期→视为不存在
	reply := tm.Prepare(&PrepareTxArgs{
		TxID: "tx-001",
		ReadKeys: []ReadKey{
			{Key: "a", ExpectedVersion: 5},
		},
		WriteKeys: []WriteKey{
			{Key: "b", Value: "new-b", Version: 0},
		},
		TimeoutMs: 10000,
	})

	// 过期 key 被视为不存在，exists=false，expectedVersion=5 != 0 → 冲突
	if reply.Err != ErrTxConflict {
		t.Fatalf("Prepare 应返回 ErrTxConflict（过期 key 版本校验），实际返回: %s", reply.Err)
	}
}

// ========== Commit 阶段测试 ==========

func TestCommitSuccess(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)
	seedKey(t, kv, "b", "val-b", 2)

	// Phase 1: Prepare
	prepReply := tm.Prepare(&PrepareTxArgs{
		TxID: "tx-001",
		ReadKeys: []ReadKey{
			{Key: "a", ExpectedVersion: 1},
		},
		WriteKeys: []WriteKey{
			{Key: "b", Value: "new-b", Version: 2},
			{Key: "c", Value: "new-c", Version: 0},
		},
		TimeoutMs: 10000,
	})
	if prepReply.Err != OK {
		t.Fatalf("Prepare 失败: %s", prepReply.Err)
	}

	// Phase 2: Commit
	commitReply := tm.Commit(&CommitTxArgs{
		TxID: "tx-001",
		WriteKeys: []WriteKey{
			{Key: "b", Value: "new-b", Version: 2},
			{Key: "c", Value: "new-c", Version: 0},
		},
	})
	if commitReply.Err != OK {
		t.Fatalf("Commit 应返回 OK，实际返回: %s", commitReply.Err)
	}

	// 验证数据已写入
	valB, verB, existsB := getKey(t, kv, "b")
	if !existsB || valB != "new-b" || verB != 3 { // version 2 + 1 = 3
		t.Fatalf("b 应为 new-b(v3)，实际: %s(v%d, exists=%v)", valB, verB, existsB)
	}

	valC, verC, existsC := getKey(t, kv, "c")
	if !existsC || valC != "new-c" || verC != 1 { // new key, version starts at 1
		t.Fatalf("c 应为 new-c(v1)，实际: %s(v%d, exists=%v)", valC, verC, existsC)
	}

	// 验证锁已释放
	tm.mu.RLock()
	_, locked := tm.lockTable["b"]
	tm.mu.RUnlock()
	if locked {
		t.Fatal("Commit 后 b 的锁应已释放")
	}

	// 验证 commit 记录已持久化
	_, found, _ := kv.store.GetTxRecord("commit:tx-001")
	if !found {
		t.Fatal("commit 记录未持久化")
	}

	// 验证 prepare 记录已清理
	_, found, _ = kv.store.GetTxRecord("prepare:tx-001")
	if found {
		t.Fatal("prepare 记录应在 Commit 后清理")
	}
}

func TestCommitIdempotent(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// Prepare
	tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		TimeoutMs: 10000,
	})

	// 第一次 Commit
	reply1 := tm.Commit(&CommitTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
	})
	if reply1.Err != OK {
		t.Fatalf("第一次 Commit 应返回 OK，实际返回: %s", reply1.Err)
	}

	// 第二次 Commit（幂等重试）
	reply2 := tm.Commit(&CommitTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
	})
	if reply2.Err != OK {
		t.Fatalf("第二次 Commit（幂等）应返回 OK，实际返回: %s", reply2.Err)
	}

	// key 版本号只递增了一次
	_, ver, _ := getKey(t, kv, "a")
	if ver != 2 { // 1 + 1
		t.Fatalf("幂等 Commit 后版本号应为 2，实际为: %d", ver)
	}
}

func TestCommitAfterAbort(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// Prepare
	tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		TimeoutMs: 10000,
	})

	// Abort
	tm.Abort(&AbortTxArgs{TxID: "tx-001"})

	// 尝试 Commit 已 abort 的事务
	reply := tm.Commit(&CommitTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
	})
	if reply.Err != ErrTxConflict {
		t.Fatalf("已 Abort 的事务 Commit 应返回 ErrTxConflict，实际返回: %s", reply.Err)
	}

	// key 值不变
	val, ver, _ := getKey(t, kv, "a")
	if val != "val-a" || ver != 1 {
		t.Fatalf("已 Abort 后 key 不应被修改，实际: %s(v%d)", val, ver)
	}
}

func TestCommitNotFound(t *testing.T) {
	tm, _, cleanup := setupTestTxManager(t)
	defer cleanup()

	// 直接 Commit 一个不存在的 txID
	reply := tm.Commit(&CommitTxArgs{
		TxID:      "tx-nonexistent",
		WriteKeys: []WriteKey{{Key: "a", Value: "v", Version: 0}},
	})
	if reply.Err != ErrTxNotFound {
		t.Fatalf("Commit 不存在的 tx 应返回 ErrTxNotFound，实际返回: %s", reply.Err)
	}
}

func TestCommitVersionConflictDuringWriteBatch(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// Prepare 时 a 版本是 1
	tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		TimeoutMs: 10000,
	})

	// 但在 Prepare 和 Commit 之间，其他写入修改了 a（绕过锁）
	// 模拟这种情况：直接操作存储，将 a 版本改为 3
	tm.mu.Lock()
	delete(tm.lockTable, "a") // 释放锁（模拟并发写入绕过）
	tm.mu.Unlock()
	kv.store.Put("a", "intermediate", 3, 0)

	// Commit 时 WriteBatchWithCAS 应检测到版本冲突
	reply := tm.Commit(&CommitTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
	})
	if reply.Err != ErrTxConflict {
		t.Fatalf("版本冲突时 Commit 应返回 ErrTxConflict，实际返回: %s", reply.Err)
	}
}

// ========== Abort 阶段测试 ==========

func TestAbortSuccess(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// Prepare
	tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		TimeoutMs: 10000,
	})

	// Abort
	reply := tm.Abort(&AbortTxArgs{TxID: "tx-001"})
	if reply.Err != OK {
		t.Fatalf("Abort 应返回 OK，实际返回: %s", reply.Err)
	}

	// 验证锁已释放
	tm.mu.RLock()
	_, locked := tm.lockTable["a"]
	tm.mu.RUnlock()
	if locked {
		t.Fatal("Abort 后 a 的锁应已释放")
	}

	// 验证 abort 记录已持久化
	_, found, _ := kv.store.GetTxRecord("abort:tx-001")
	if !found {
		t.Fatal("abort 记录未持久化")
	}

	// 验证 prepare 记录已清理
	_, found, _ = kv.store.GetTxRecord("prepare:tx-001")
	if found {
		t.Fatal("prepare 记录应在 Abort 后清理")
	}

	// 验证数据未被修改
	val, ver, _ := getKey(t, kv, "a")
	if val != "val-a" || ver != 1 {
		t.Fatalf("Abort 后数据应不变，实际: %s(v%d)", val, ver)
	}
}

func TestAbortAfterCommit(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// Prepare + Commit
	tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		TimeoutMs: 10000,
	})
	tm.Commit(&CommitTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
	})

	// 尝试 Abort 已提交的事务
	reply := tm.Abort(&AbortTxArgs{TxID: "tx-001"})
	if reply.Err != ErrTxConflict {
		t.Fatalf("已 Commit 后 Abort 应返回 ErrTxConflict，实际返回: %s", reply.Err)
	}

	// 数据仍为提交后的值
	val, _, _ := getKey(t, kv, "a")
	if val != "new-a" {
		t.Fatalf("已 Commit 后 Abort 不应回滚数据，实际: %s", val)
	}
}

func TestAbortIdempotent(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		TimeoutMs: 10000,
	})

	// 第一次 Abort
	reply1 := tm.Abort(&AbortTxArgs{TxID: "tx-001"})
	if reply1.Err != OK {
		t.Fatalf("第一次 Abort 应返回 OK，实际返回: %s", reply1.Err)
	}

	// 第二次 Abort（幂等）
	reply2 := tm.Abort(&AbortTxArgs{TxID: "tx-001"})
	if reply2.Err != OK {
		t.Fatalf("第二次 Abort（幂等）应返回 OK，实际返回: %s", reply2.Err)
	}
}

// ========== ResolveTxStatus 测试 ==========

func TestResolveTxStatusPrepared(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		TimeoutMs: 10000,
	})

	reply := tm.ResolveTxStatus(&ResolveTxStatusArgs{TxID: "tx-001"})
	if reply.Err != OK {
		t.Fatalf("ResolveTxStatus 应返回 OK，实际返回: %s", reply.Err)
	}
	if reply.Status != TxStatusPrepared {
		t.Fatalf("状态应为 TxStatusPrepared(%d)，实际: %d", TxStatusPrepared, reply.Status)
	}
}

func TestResolveTxStatusCommitted(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		TimeoutMs: 10000,
	})
	tm.Commit(&CommitTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
	})

	reply := tm.ResolveTxStatus(&ResolveTxStatusArgs{TxID: "tx-001"})
	if reply.Status != TxStatusCommitted {
		t.Fatalf("状态应为 TxStatusCommitted(%d)，实际: %d", TxStatusCommitted, reply.Status)
	}
}

func TestResolveTxStatusAborted(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		TimeoutMs: 10000,
	})
	tm.Abort(&AbortTxArgs{TxID: "tx-001"})

	reply := tm.ResolveTxStatus(&ResolveTxStatusArgs{TxID: "tx-001"})
	if reply.Status != TxStatusAborted {
		t.Fatalf("状态应为 TxStatusAborted(%d)，实际: %d", TxStatusAborted, reply.Status)
	}
}

func TestResolveTxStatusNotFound(t *testing.T) {
	tm, _, cleanup := setupTestTxManager(t)
	defer cleanup()

	reply := tm.ResolveTxStatus(&ResolveTxStatusArgs{TxID: "tx-nonexistent"})
	if reply.Status != TxStatusNotFound {
		t.Fatalf("状态应为 TxStatusNotFound(%d)，实际: %d", TxStatusNotFound, reply.Status)
	}
	if reply.Err != ErrTxNotFound {
		t.Fatalf("Err 应为 ErrTxNotFound，实际: %s", reply.Err)
	}
}

// ========== 锁恢复测试 ==========

func TestResolveLockLockedCommitted(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// 手动设置锁（模拟只有内存锁，无 prepare 记录的场景）
	tm.mu.Lock()
	tm.lockTable["a"] = "tx-orphan"
	tm.mu.Unlock()

	// 手动写入 commit 记录
	kv.store.PutTxRecord("commit:tx-orphan", []byte("1"))

	// 锁 recover 应成功，锁被释放
	tm.mu.Lock()
	resolved := tm.resolveLockLocked("tx-orphan")
	tm.mu.Unlock()

	if !resolved {
		t.Fatal("已提交事务的锁应被成功 resolve")
	}

	tm.mu.RLock()
	_, locked := tm.lockTable["a"]
	tm.mu.RUnlock()
	if locked {
		t.Fatal("已提交事务的锁应被释放")
	}
}

func TestResolveLockLockedAborted(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	tm.mu.Lock()
	tm.lockTable["a"] = "tx-orphan"
	tm.mu.Unlock()

	kv.store.PutTxRecord("abort:tx-orphan", []byte("1"))

	tm.mu.Lock()
	resolved := tm.resolveLockLocked("tx-orphan")
	tm.mu.Unlock()

	if !resolved {
		t.Fatal("已回滚事务的锁应被成功 resolve")
	}

	tm.mu.RLock()
	_, locked := tm.lockTable["a"]
	tm.mu.RUnlock()
	if locked {
		t.Fatal("已回滚事务的锁应被释放")
	}
}

func TestResolveLockTimeoutAutoAbort(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// 使用极短超时（1ms）创建一个 Prepare 记录
	rec := preparedTxRecord{
		TxID:       "tx-timeout",
		ReadKeys:   nil,
		WriteKeys:  []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		PreparedAt: time.Now().UnixNano() - int64(10*time.Second), // 10 秒前
		TimeoutMs:  1,                                             // 1ms 超时
	}
	raw, _ := json.Marshal(rec)
	kv.store.PutTxRecord("prepare:tx-timeout", raw)

	// 设置内存锁
	tm.mu.Lock()
	tm.lockTable["a"] = "tx-timeout"
	tm.preparedTxs["tx-timeout"] = &rec
	tm.mu.Unlock()

	// 尝试 resolve —— 应检测到超时并自动 abort
	tm.mu.Lock()
	resolved := tm.resolveLockLocked("tx-timeout")
	tm.mu.Unlock()

	if !resolved {
		t.Fatal("超时事务的锁应被成功 resolve")
	}

	// 锁应被释放
	tm.mu.RLock()
	_, locked := tm.lockTable["a"]
	tm.mu.RUnlock()
	if locked {
		t.Fatal("超时事务的锁应被释放")
	}

	// abort 记录应已持久化
	_, found, _ := kv.store.GetTxRecord("abort:tx-timeout")
	if !found {
		t.Fatal("超时自动 abort 应持久化 abort 记录")
	}
}

func TestResolveLockStillValid(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// 刚创建的 Prepare 记录（未超时）
	rec := preparedTxRecord{
		TxID:       "tx-valid",
		WriteKeys:  []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		PreparedAt: time.Now().UnixNano(),
		TimeoutMs:  60000, // 60 秒
	}
	raw, _ := json.Marshal(rec)
	kv.store.PutTxRecord("prepare:tx-valid", raw)

	tm.mu.Lock()
	tm.lockTable["a"] = "tx-valid"
	tm.preparedTxs["tx-valid"] = &rec
	tm.mu.Unlock()

	tm.mu.Lock()
	resolved := tm.resolveLockLocked("tx-valid")
	tm.mu.Unlock()

	if resolved {
		t.Fatal("未超时且未提交/回滚的事务不应被 resolve")
	}

	// 锁应仍然存在
	tm.mu.RLock()
	holder := tm.lockTable["a"]
	tm.mu.RUnlock()
	if holder != "tx-valid" {
		t.Fatalf("有效事务的锁不应被释放，实际 holder: %s", holder)
	}
}

// ========== RebuildLockTable 测试 ==========

func TestRebuildLockTable(t *testing.T) {
	_, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)
	seedKey(t, kv, "b", "val-b", 2)

	// 手动写入两个 prepare 记录
	rec1 := preparedTxRecord{
		TxID:       "tx-001",
		WriteKeys:  []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		PreparedAt: time.Now().UnixNano(),
		TimeoutMs:  60000,
	}
	raw1, _ := json.Marshal(rec1)
	kv.store.PutTxRecord("prepare:tx-001", raw1)

	rec2 := preparedTxRecord{
		TxID:       "tx-002",
		WriteKeys:  []WriteKey{{Key: "b", Value: "new-b", Version: 2}},
		PreparedAt: time.Now().UnixNano(),
		TimeoutMs:  60000,
	}
	raw2, _ := json.Marshal(rec2)
	kv.store.PutTxRecord("prepare:tx-002", raw2)

	// 手动写入一个已 commit 的残留 prepare 记录
	kv.store.PutTxRecord("commit:tx-003", []byte("1"))
	rec3 := preparedTxRecord{
		TxID:       "tx-003",
		WriteKeys:  []WriteKey{{Key: "c", Value: "new-c", Version: 0}},
		PreparedAt: time.Now().UnixNano(),
		TimeoutMs:  60000,
	}
	raw3, _ := json.Marshal(rec3)
	kv.store.PutTxRecord("prepare:tx-003", raw3)

	// 手动写入一个已 abort 的残留 prepare 记录
	kv.store.PutTxRecord("abort:tx-004", []byte("1"))
	rec4 := preparedTxRecord{
		TxID:       "tx-004",
		WriteKeys:  []WriteKey{{Key: "d", Value: "new-d", Version: 0}},
		PreparedAt: time.Now().UnixNano(),
		TimeoutMs:  60000,
	}
	raw4, _ := json.Marshal(rec4)
	kv.store.PutTxRecord("prepare:tx-004", raw4)

	// 重建
	tm2 := NewTxManager(kv)
	err := tm2.RebuildLockTable()
	if err != nil {
		t.Fatalf("RebuildLockTable 失败: %v", err)
	}

	// 验证锁：tx-001 和 tx-002 的锁应恢复
	tm2.mu.RLock()
	holderA := tm2.lockTable["a"]
	holderB := tm2.lockTable["b"]
	_, lockedC := tm2.lockTable["c"] // tx-003 已 commit，锁不应重建
	_, lockedD := tm2.lockTable["d"] // tx-004 已 abort，锁不应重建
	tm2.mu.RUnlock()

	if holderA != "tx-001" {
		t.Fatalf("lockTable[a] 应为 tx-001，实际: %s", holderA)
	}
	if holderB != "tx-002" {
		t.Fatalf("lockTable[b] 应为 tx-002，实际: %s", holderB)
	}
	if lockedC {
		t.Fatal("已 commit 的 tx-003 的锁不应被重建")
	}
	if lockedD {
		t.Fatal("已 abort 的 tx-004 的锁不应被重建")
	}

	// 验证残留 prepare 记录已被清理
	_, found, _ := kv.store.GetTxRecord("prepare:tx-003")
	if found {
		t.Fatal("已 commit 的 tx-003 的残留 prepare 记录应被清理")
	}
	_, found, _ = kv.store.GetTxRecord("prepare:tx-004")
	if found {
		t.Fatal("已 abort 的 tx-004 的残留 prepare 记录应被清理")
	}
}

func TestRebuildLockTableWithTimeout(t *testing.T) {
	_, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// 已超时的 prepare 记录
	rec := preparedTxRecord{
		TxID:       "tx-timeout",
		WriteKeys:  []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		PreparedAt: time.Now().UnixNano() - int64(10*time.Second),
		TimeoutMs:  1, // 1ms 超时
	}
	raw, _ := json.Marshal(rec)
	kv.store.PutTxRecord("prepare:tx-timeout", raw)

	tm2 := NewTxManager(kv)
	err := tm2.RebuildLockTable()
	if err != nil {
		t.Fatalf("RebuildLockTable 失败: %v", err)
	}

	// 已超时的锁不应重建
	tm2.mu.RLock()
	_, locked := tm2.lockTable["a"]
	tm2.mu.RUnlock()
	if locked {
		t.Fatal("已超时事务的锁不应被重建")
	}

	// abort 记录应已自动写入
	_, found, _ := kv.store.GetTxRecord("abort:tx-timeout")
	if !found {
		t.Fatal("RebuildLockTable 应对超时事务自动写入 abort 记录")
	}

	// prepare 记录应已清理
	_, found, _ = kv.store.GetTxRecord("prepare:tx-timeout")
	if found {
		t.Fatal("RebuildLockTable 应清理超时的 prepare 记录")
	}
}

// ========== prepare 持久化失败测试 ==========

func TestPreparePersistenceFailureRollsBack(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// 在 Prepare 前先写入一个伪造的 prepare 记录，
	// 这样 PutTxRecord("prepare:tx-001", ...) 中的 key 已存在但会被覆盖，
	// 实际不会触发失败。我们改为验证：如果 disk 写入真的失败，锁会回滚。
	//
	// 为了真正测试，我们关闭存储，模拟持久化失败。
	kv.store.Close()

	reply := tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-001",
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		TimeoutMs: 10000,
	})

	// 存储关闭后 Prepare 应返回错误
	if reply.Err != ErrWrongLeader {
		t.Fatalf("持久化失败时 Prepare 应返回 ErrWrongLeader，实际返回: %s", reply.Err)
	}

	// 锁应被回滚（即使持久化失败）
	tm.mu.RLock()
	_, locked := tm.lockTable["a"]
	tm.mu.RUnlock()
	if locked {
		t.Fatal("持久化失败后 Prepare 的锁应被回滚")
	}
}

// ========== 完整流程测试 ==========

func TestFullPrepareCommitFlow(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	// 初始化数据
	seedKey(t, kv, "balance-alice", "100", 1)
	seedKey(t, kv, "balance-bob", "50", 2)

	// 转账：alice -30, bob +30
	txID := "transfer-001"

	// Phase 1: Prepare — 校验 alice=100, bob=50；写入 alice=70, bob=80
	prepReply := tm.Prepare(&PrepareTxArgs{
		TxID: txID,
		ReadKeys: []ReadKey{
			{Key: "balance-alice", ExpectedVersion: 1},
			{Key: "balance-bob", ExpectedVersion: 2},
		},
		WriteKeys: []WriteKey{
			{Key: "balance-alice", Value: "70", Version: 1},
			{Key: "balance-bob", Value: "80", Version: 2},
		},
		TimeoutMs: 10000,
	})
	if prepReply.Err != OK {
		t.Fatalf("转账 Prepare 失败: %s", prepReply.Err)
	}

	// Phase 2: Commit
	commitReply := tm.Commit(&CommitTxArgs{
		TxID: txID,
		WriteKeys: []WriteKey{
			{Key: "balance-alice", Value: "70", Version: 1},
			{Key: "balance-bob", Value: "80", Version: 2},
		},
	})
	if commitReply.Err != OK {
		t.Fatalf("转账 Commit 失败: %s", commitReply.Err)
	}

	// 验证转账结果
	aliceVal, aliceVer, _ := getKey(t, kv, "balance-alice")
	bobVal, bobVer, _ := getKey(t, kv, "balance-bob")

	if aliceVal != "70" || aliceVer != 2 {
		t.Fatalf("alice 余额应为 70(v2)，实际: %s(v%d)", aliceVal, aliceVer)
	}
	if bobVal != "80" || bobVer != 3 {
		t.Fatalf("bob 余额应为 80(v3)，实际: %s(v%d)", bobVal, bobVer)
	}
}

func TestFullPrepareAbortFlow(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	txID := "tx-abort-001"

	tm.Prepare(&PrepareTxArgs{
		TxID:      txID,
		WriteKeys: []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		TimeoutMs: 10000,
	})

	tm.Abort(&AbortTxArgs{TxID: txID})

	// 数据不变
	val, ver, _ := getKey(t, kv, "a")
	if val != "val-a" || ver != 1 {
		t.Fatalf("Abort 后数据应不变，实际: %s(v%d)", val, ver)
	}

	// 锁已释放，新事务可继续操作
	reply := tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-002",
		WriteKeys: []WriteKey{{Key: "a", Value: "final-a", Version: 1}},
		TimeoutMs: 10000,
	})
	if reply.Err != OK {
		t.Fatalf("Abort 后新事务应能 Prepare，实际: %s", reply.Err)
	}
}

// ========== ResolveTxStatus 超时自动 Abort 测试 ==========

func TestResolveTxStatusAutoAbortOnTimeout(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// 创建已超时的 prepare 记录
	rec := preparedTxRecord{
		TxID:       "tx-timeout",
		ReadKeys:   nil,
		WriteKeys:  []WriteKey{{Key: "a", Value: "new-a", Version: 1}},
		PreparedAt: time.Now().UnixNano() - int64(30*time.Second),
		TimeoutMs:  1,
	}
	raw, _ := json.Marshal(rec)
	kv.store.PutTxRecord("prepare:tx-timeout", raw)

	tm.mu.Lock()
	tm.lockTable["a"] = "tx-timeout"
	tm.preparedTxs["tx-timeout"] = &rec
	tm.mu.Unlock()

	// 查询状态（应触发自动 abort）
	reply := tm.ResolveTxStatus(&ResolveTxStatusArgs{TxID: "tx-timeout"})
	if reply.Status != TxStatusAborted {
		t.Fatalf("超时事务状态应为 TxStatusAborted，实际: %d", reply.Status)
	}

	// 锁应被释放
	tm.mu.RLock()
	_, locked := tm.lockTable["a"]
	tm.mu.RUnlock()
	if locked {
		t.Fatal("超时自动 abort 后锁应被释放")
	}

	// abort + cleanup 应持久化
	_, found, _ := kv.store.GetTxRecord("abort:tx-timeout")
	if !found {
		t.Fatal("auto-abort 应持久化 abort 记录")
	}
	_, found, _ = kv.store.GetTxRecord("prepare:tx-timeout")
	if found {
		t.Fatal("auto-abort 应清理 prepare 记录")
	}
}

// ========== 版本号与锁一致性测试 ==========

func TestLockTableAndDiskConsistency(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "k1", "v1", 1)
	seedKey(t, kv, "k2", "v2", 2)
	seedKey(t, kv, "k3", "v3", 3)

	// Prepare 3 个事务
	for i, key := range []string{"k1", "k2", "k3"} {
		txID := "tx-" + key
		reply := tm.Prepare(&PrepareTxArgs{
			TxID:      txID,
			WriteKeys: []WriteKey{{Key: key, Value: "updated-" + key, Version: Tversion(i + 1)}},
			TimeoutMs: 10000,
		})
		if reply.Err != OK {
			t.Fatalf("Prepare %s 失败: %s", txID, reply.Err)
		}
	}

	// 验证 lockTable 和 preparedTxs 中的条目数一致
	tm.mu.RLock()
	lockCount := len(tm.lockTable)
	prepCount := len(tm.preparedTxs)
	tm.mu.RUnlock()

	if lockCount != 3 || prepCount != 3 {
		t.Fatalf("应各有 3 条，实际 lockTable=%d, preparedTxs=%d", lockCount, prepCount)
	}

	// 提交 k1
	tm.Commit(&CommitTxArgs{
		TxID:      "tx-k1",
		WriteKeys: []WriteKey{{Key: "k1", Value: "updated-k1", Version: 1}},
	})

	// Abort k2
	tm.Abort(&AbortTxArgs{TxID: "tx-k2"})

	// k3 仍处于 prepared
	tm.mu.RLock()
	lockCount = len(tm.lockTable)
	prepCount = len(tm.preparedTxs)
	k3Holder := tm.lockTable["k3"]
	tm.mu.RUnlock()

	if lockCount != 1 {
		t.Fatalf("应只剩 1 把锁（k3），实际: %d", lockCount)
	}
	if prepCount != 1 {
		t.Fatalf("应只剩 1 个 prepared 事务（k3），实际: %d", prepCount)
	}
	if k3Holder != "tx-k3" {
		t.Fatalf("k3 的锁持有者应为 tx-k3，实际: %s", k3Holder)
	}
}

// ========== 边界条件测试 ==========

func TestPrepareWithNoKeys(t *testing.T) {
	tm, _, cleanup := setupTestTxManager(t)
	defer cleanup()

	// 没有任何 read/write key 的 Prepare
	reply := tm.Prepare(&PrepareTxArgs{
		TxID:      "tx-empty",
		ReadKeys:  nil,
		WriteKeys: nil,
		TimeoutMs: 10000,
	})

	if reply.Err != OK {
		t.Fatalf("空 Prepare 应返回 OK，实际返回: %s", reply.Err)
	}
}

func TestPrepareWithOnlyReadKeys(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()

	seedKey(t, kv, "a", "val-a", 1)

	// 只有读集，没有写集
	reply := tm.Prepare(&PrepareTxArgs{
		TxID: "tx-readonly",
		ReadKeys: []ReadKey{
			{Key: "a", ExpectedVersion: 1},
		},
		WriteKeys: nil,
		TimeoutMs: 10000,
	})

	if reply.Err != OK {
		t.Fatalf("只读 Prepare 应返回 OK，实际返回: %s", reply.Err)
	}

	// 没有写锁
	tm.mu.RLock()
	lockCount := len(tm.lockTable)
	tm.mu.RUnlock()
	if lockCount != 0 {
		t.Fatalf("只读 Prepare 不应有任何写锁，实际: %d", lockCount)
	}
}

func TestPrepareWithEmptyTxID(t *testing.T) {
	tm, _, cleanup := setupTestTxManager(t)
	defer cleanup()

	reply := tm.Prepare(&PrepareTxArgs{
		TxID:      "",
		WriteKeys: []WriteKey{{Key: "a", Value: "v", Version: 0}},
		TimeoutMs: 10000,
	})

	if reply.Err != OK {
		t.Fatalf("空 txID Prepare 应返回 OK，实际返回: %s", reply.Err)
	}
}

func TestDurableCoordinatorDecisionSurvivesParticipantResolution(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()
	seedKey(t, kv, "a", "old", 1)
	reply := tm.Prepare(&PrepareTxArgs{TxID: "tx-durable", WriteKeys: []WriteKey{{Key: "a", Value: "new", Version: 1}}, CoordinatorGroupID: 7, ParticipantGroupIDs: []int{2, 7}})
	if reply.Err != OK {
		t.Fatalf("prepare: %s", reply.Err)
	}
	if tm.RecordDecision(&RecordTxDecisionArgs{TxID: "tx-durable", Decision: TxStatusCommitted, ParticipantGroupIDs: []int{2, 7}}).Err != OK {
		t.Fatal("record decision")
	}
	status := tm.ResolveTxStatus(&ResolveTxStatusArgs{TxID: "tx-durable"})
	if status.Status != TxStatusCommitted {
		t.Fatalf("status=%v", status.Status)
	}
	if status.CoordinatorGroupID != 0 {
		t.Fatalf("coordinator metadata on decision-only response=%d", status.CoordinatorGroupID)
	}
	if tm.Commit(&CommitTxArgs{TxID: "tx-durable", WriteKeys: []WriteKey{{Key: "a", Value: "tampered", Version: 999}}}).Err != OK {
		t.Fatal("commit should use prepared write set")
	}
	value, version, _, exists, err := kv.store.Get("a")
	if err != nil || !exists || value != "new" || version != 2 {
		t.Fatalf("restored write=%q/%d exists=%v err=%v", value, version, exists, err)
	}
}

func TestPreparedWithoutDecisionRemainsPrepared(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()
	seedKey(t, kv, "a", "old", 1)
	if tm.Prepare(&PrepareTxArgs{TxID: "tx-blocked", WriteKeys: []WriteKey{{Key: "a", Value: "new", Version: 1}}, CoordinatorGroupID: 0, ParticipantGroupIDs: []int{0}}).Err != OK {
		t.Fatal("prepare")
	}
	status := tm.ResolveTxStatus(&ResolveTxStatusArgs{TxID: "tx-blocked"})
	if status.Status != TxStatusPrepared {
		t.Fatalf("status=%v", status.Status)
	}
	if tm.Commit(&CommitTxArgs{TxID: "tx-blocked", WriteKeys: nil}).Err != ErrTxConflict && tm.Commit(&CommitTxArgs{TxID: "tx-blocked"}).Err != ErrTxConflict {
		t.Fatal("expected CAS failure while undecided")
	}
}

func TestTransactionDecisionWALRoundTrip(t *testing.T) {
	entry := walEntryFromOp(1, 9, 3, Op{Req: &RecordTxDecisionArgs{TxID: "wal-decision", Decision: TxStatusCommitted, ParticipantGroupIDs: []int{1, 4}}})
	req, mutating, err := walEntryToRequest(entry)
	if err != nil || !mutating {
		t.Fatalf("round trip: mutating=%v err=%v", mutating, err)
	}
	decision, ok := req.(*RecordTxDecisionArgs)
	if !ok || decision.TxID != "wal-decision" || decision.Decision != TxStatusCommitted || len(decision.ParticipantGroupIDs) != 2 {
		t.Fatalf("decision=%#v", req)
	}
}

func TestParticipantRestartRestoresCoordinatorMetadataAndPreparedWrite(t *testing.T) {
	tm, kv, cleanup := setupTestTxManager(t)
	defer cleanup()
	seedKey(t, kv, "restart-key", "old", 1)
	if got := tm.Prepare(&PrepareTxArgs{TxID: "participant-restart", WriteKeys: []WriteKey{{Key: "restart-key", Value: "new", Version: 1}}, CoordinatorGroupID: 7, ParticipantGroupIDs: []int{2, 7}}); got.Err != OK {
		t.Fatalf("prepare: %s", got.Err)
	}
	restarted := NewTxManager(kv)
	if err := restarted.RebuildLockTable(); err != nil {
		t.Fatal(err)
	}
	status := restarted.ResolveTxStatus(&ResolveTxStatusArgs{TxID: "participant-restart"})
	if status.Status != TxStatusPrepared || status.CoordinatorGroupID != 7 || len(status.ParticipantGroupIDs) != 2 {
		t.Fatalf("restored status=%+v", status)
	}
	if got := restarted.Commit(&CommitTxArgs{TxID: "participant-restart", WriteKeys: []WriteKey{{Key: "restart-key", Value: "tampered", Version: 99}}}); got.Err != OK {
		t.Fatalf("commit: %s", got.Err)
	}
	value, version, _, exists, err := kv.store.Get("restart-key")
	if err != nil || !exists || value != "new" || version != 2 {
		t.Fatalf("value=%q/%d exists=%v err=%v", value, version, exists, err)
	}
}
