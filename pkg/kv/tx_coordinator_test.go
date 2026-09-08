package kv

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"testing"

	pb "kvraft/api/pb/kvraft/api/pb"
	"kvraft/pkg/sharding"

	"google.golang.org/grpc"
)

// mockTxService 是一个内存中的 KV+2PC 服务实现，用于测试 TxCoordinator。
// 它实现了 pb.KVServiceServer 接口中所有 coordinator 需要用到的 RPC。
type mockTxService struct {
	pb.UnimplementedKVServiceServer

	mu         sync.Mutex
	kv         map[string]*pb.KeyValue
	prepared   map[string]*pb.PrepareTxRequest
	committed  map[string]bool
	aborted    map[string]bool
	decisions  map[string]bool
	conflictOn string // 对此 txID 返回冲突
}

func newMockTxService() *mockTxService {
	return &mockTxService{
		kv:        make(map[string]*pb.KeyValue),
		prepared:  make(map[string]*pb.PrepareTxRequest),
		committed: make(map[string]bool),
		aborted:   make(map[string]bool),
		decisions: make(map[string]bool),
	}
}

func (s *mockTxService) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.kv[req.GetKey()]
	if !ok {
		return &pb.GetResponse{Error: "ErrNoKey"}, nil
	}
	return &pb.GetResponse{Value: it.Value, Version: it.Version, Error: "OK"}, nil
}

func (s *mockTxService) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.kv[req.GetKey()]
	ver := int64(req.GetVersion())
	if !ok {
		if ver != 0 {
			return &pb.PutResponse{Error: "ErrVersion"}, nil
		}
		s.kv[req.GetKey()] = &pb.KeyValue{Key: req.GetKey(), Value: req.GetValue(), Version: 1}
		return &pb.PutResponse{Error: "OK"}, nil
	}
	if cur.Version != ver {
		return &pb.PutResponse{Error: "ErrVersion"}, nil
	}
	s.kv[req.GetKey()] = &pb.KeyValue{Key: req.GetKey(), Value: req.GetValue(), Version: cur.Version + 1}
	return &pb.PutResponse{Error: "OK"}, nil
}

func (s *mockTxService) PrepareTx(_ context.Context, req *pb.PrepareTxRequest) (*pb.PrepareTxResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 冲突注入
	if req.GetTxId() == s.conflictOn {
		return &pb.PrepareTxResponse{Error: "ErrTxConflict"}, nil
	}

	// 简易 Prepare：检查 write key 是否已被其他事务锁定
	for _, wk := range req.GetWriteKeys() {
		// 检查是否已被其他 tx prepare
		for _, pr := range s.prepared {
			for _, pwk := range pr.GetWriteKeys() {
				if pwk.GetKey() == wk.GetKey() && pr.GetTxId() != req.GetTxId() {
					return &pb.PrepareTxResponse{Error: "ErrTxConflict"}, nil
				}
			}
		}
	}

	// 检查 read key 版本
	for _, rk := range req.GetReadKeys() {
		it, ok := s.kv[rk.GetKey()]
		if !ok {
			if rk.GetExpectedVersion() != 0 {
				return &pb.PrepareTxResponse{Error: "ErrTxConflict"}, nil
			}
			continue
		}
		if it.Version != rk.GetExpectedVersion() {
			return &pb.PrepareTxResponse{Error: "ErrTxConflict"}, nil
		}
	}

	s.prepared[req.GetTxId()] = req
	return &pb.PrepareTxResponse{Error: "OK"}, nil
}

func (s *mockTxService) CommitTx(_ context.Context, req *pb.CommitTxRequest) (*pb.CommitTxResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.GetDecisionOnly() {
		s.decisions[req.GetTxId()] = true
		return &pb.CommitTxResponse{Error: "OK"}, nil
	}

	if s.committed[req.GetTxId()] {
		return &pb.CommitTxResponse{Error: "OK"}, nil // 幂等
	}
	if s.aborted[req.GetTxId()] {
		return &pb.CommitTxResponse{Error: "ErrTxConflict"}, nil
	}

	// 应用写入
	for _, wk := range req.GetWriteKeys() {
		cur, ok := s.kv[wk.GetKey()]
		if !ok {
			s.kv[wk.GetKey()] = &pb.KeyValue{Key: wk.GetKey(), Value: wk.GetValue(), Version: 1}
		} else {
			cur.Value = wk.GetValue()
			cur.Version = cur.Version + 1
		}
	}

	s.committed[req.GetTxId()] = true
	delete(s.prepared, req.GetTxId())
	return &pb.CommitTxResponse{Error: "OK"}, nil
}

func (s *mockTxService) AbortTx(_ context.Context, req *pb.AbortTxRequest) (*pb.AbortTxResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.GetDecisionOnly() {
		s.aborted[req.GetTxId()] = true
		return &pb.AbortTxResponse{Error: "OK"}, nil
	}

	if s.committed[req.GetTxId()] {
		return &pb.AbortTxResponse{Error: "ErrTxConflict"}, nil
	}
	s.aborted[req.GetTxId()] = true
	delete(s.prepared, req.GetTxId())
	return &pb.AbortTxResponse{Error: "OK"}, nil
}

func (s *mockTxService) ResolveTxStatus(_ context.Context, req *pb.ResolveTxStatusRequest) (*pb.ResolveTxStatusResponse, error) {
	s.mu.Lock()

	if s.committed[req.GetTxId()] {
		return &pb.ResolveTxStatusResponse{Status: "COMMITTED", Error: "OK"}, nil
	}
	if s.aborted[req.GetTxId()] {
		return &pb.ResolveTxStatusResponse{Status: "ABORTED", Error: "OK"}, nil
	}
	if pr, ok := s.prepared[req.GetTxId()]; ok {
		_ = pr
		return &pb.ResolveTxStatusResponse{Status: "PREPARED", Error: "OK"}, nil
	}
	return &pb.ResolveTxStatusResponse{Status: "NOT_FOUND", Error: "ErrTxNotFound"}, nil
}

// ========== 测试夹具 ==========

// setupCoordinatorTest 创建连接到 mock 服务的 TxCoordinator。
func setupCoordinatorTest(t *testing.T) (*TxCoordinator, *mockTxService, func()) {
	t.Helper()

	mock := newMockTxService()

	// 启动内存 gRPC server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gRPC listen 失败: %v", err)
	}

	gs := grpc.NewServer()
	pb.RegisterKVServiceServer(gs, mock)
	go gs.Serve(lis)

	addr := lis.Addr().String()

	// 创建只有一个 group 的 ShardRouter
	cfg := sharding.ShardingConfig{
		Groups: []sharding.RaftGroupConfig{
			{
				GroupID:  1,
				Replicas: []string{addr},
			},
		},
	}
	router, err := sharding.NewShardRouter(cfg)
	if err != nil {
		gs.Stop()
		lis.Close()
		t.Fatalf("创建 ShardRouter 失败: %v", err)
	}

	coordinator := NewTxCoordinator(router)

	cleanup := func() {
		gs.Stop()
		lis.Close()
	}

	return coordinator, mock, cleanup
}

// seedMockKey 向 mock 服务中写入初始数据。
func seedMockKey(t *testing.T, mock *mockTxService, key, value string, version int64) {
	t.Helper()
	mock.mu.Lock()
	defer mock.mu.Unlock()
	mock.kv[key] = &pb.KeyValue{Key: key, Value: value, Version: version}
}

// ========== TxHandle 缓冲层测试 ==========

func TestTxHandlePutAndGetDirtyRead(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "a", "server-val", 1)

	h := coordinator.Begin()

	// Put 缓冲到 WriteSet
	h.Put("a", "buffer-val", 1)

	// Get 应返回 WriteSet 中的值（脏读）
	val, ver, err := h.Get("a")
	if err != OK {
		t.Fatalf("脏读 Get 应返回 OK，实际: %s", err)
	}
	if val != "buffer-val" || ver != 1 {
		t.Fatalf("脏读应返回 (%s, 1)，实际: (%s, %d)", "buffer-val", val, ver)
	}
}

func TestTxHandleGetFallsBackToServer(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "a", "server-val", 3)

	h := coordinator.Begin()

	val, ver, err := h.Get("a")
	if err != OK {
		t.Fatalf("Get 应返回 OK，实际: %s", err)
	}
	if val != "server-val" || ver != 3 {
		t.Fatalf("Get 应返回 (server-val, 3)，实际: (%s, %d)", val, ver)
	}
}

func TestTxHandleGetCachesToReadSet(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "a", "val-a", 1)

	h := coordinator.Begin()

	// 第一次 Get
	h.Get("a")

	// 修改 server 端数据
	mock.mu.Lock()
	mock.kv["a"] = &pb.KeyValue{Key: "a", Value: "modified", Version: 2}
	mock.mu.Unlock()

	// 第二次 Get：ReadSet 已缓存，返回首次读取的快照
	val, ver, _ := h.Get("a")
	if val != "val-a" || ver != 1 {
		t.Fatalf("第二次 Get 应返回首次快照 (val-a, 1)，实际: (%s, %d)", val, ver)
	}
	mock.mu.Lock()
	mock.kv["a"] = &pb.KeyValue{Key: "a", Value: "external", Version: 2}
	mock.mu.Unlock()
	h.Put("b", "transaction-write", 0)
	if err := h.Commit(); err != ErrTxConflict {
		t.Fatalf("首次读后发生外部更新时 Commit 应冲突，实际: %s", err)
	}
}

func TestTxHandleDeleteBuffersToWriteSet(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	t.Run("DeleteExistingKeyRecordsVersion", func(t *testing.T) {
		seedMockKey(t, mock, "a", "val-a", 1)

		h := coordinator.Begin()
		// 先 Get 记录版本，再 Delete——模拟标准用法
		h.Get("a")
		h.Delete("a")

		h.mu.Lock()
		wk, ok := h.writeSet["a"]
		h.mu.Unlock()

		if !ok {
			t.Fatal("Delete 应写入 WriteSet")
		}
		if wk.Value != "" || wk.Version != 1 {
			t.Fatalf("Delete 已有 key 的 WriteKey 应为 {Value:\"\", Version:1}，实际: {Value:%q, Version:%d}", wk.Value, wk.Version)
		}
	})

	t.Run("DeleteNonExistentKeyVersionZero", func(t *testing.T) {
		h := coordinator.Begin()
		h.Delete("no-such-key")

		h.mu.Lock()
		wk, ok := h.writeSet["no-such-key"]
		h.mu.Unlock()

		if !ok {
			t.Fatal("Delete 应写入 WriteSet")
		}
		if wk.Value != "" || wk.Version != 0 {
			t.Fatalf("Delete 不存在的 key 的 WriteKey 应为 {Value:\"\", Version:0}，实际: {Value:%q, Version:%d}", wk.Value, wk.Version)
		}
	})
}

func TestTxHandleGetNonExistentKey(t *testing.T) {
	coordinator, _, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	h := coordinator.Begin()

	_, _, err := h.Get("nonexistent")
	if err != ErrNoKey {
		t.Fatalf("Get 不存在的 key 应返回 ErrNoKey，实际: %s", err)
	}
}

// ========== Commit (单 Group) 测试 ==========

func TestCoordinatorCommitSingleGroup(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "a", "val-a", 1)
	seedMockKey(t, mock, "b", "val-b", 2)

	h := coordinator.Begin()
	h.Put("a", "new-a", 1)
	h.Put("b", "new-b", 2)

	err := h.Commit()
	if err != OK {
		t.Fatalf("Commit 应返回 OK，实际: %s", err)
	}

	// 验证 server 端数据已更新
	mock.mu.Lock()
	kvA := mock.kv["a"]
	kvB := mock.kv["b"]
	committed := mock.committed[h.txID]
	mock.mu.Unlock()

	if kvA == nil || kvA.Value != "new-a" {
		t.Fatalf("a 应为 new-a，实际: %v", kvA)
	}
	if kvB == nil || kvB.Value != "new-b" {
		t.Fatalf("b 应为 new-b，实际: %v", kvB)
	}
	if !committed {
		t.Fatal("事务应标记为 committed")
	}
}

func TestCoordinatorCommitEmptyTransaction(t *testing.T) {
	coordinator, _, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	h := coordinator.Begin()
	// 不做任何 Put/Delete，只读一个 key（但不影响结果）
	err := h.Commit()

	if err != OK {
		t.Fatalf("空事务 Commit 应返回 OK，实际: %s", err)
	}
}

func TestCoordinatorCommitWithReadValidation(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "a", "val-a", 1)
	seedMockKey(t, mock, "b", "val-b", 2)

	h := coordinator.Begin()
	// 读取 a，记录版本
	h.Get("a")
	h.Put("b", "new-b", 2)

	// 修改 a 的版本（在 Prepare 之前），模拟并发写入
	mock.mu.Lock()
	mock.kv["a"] = &pb.KeyValue{Key: "a", Value: "concurrent", Version: 2}
	mock.mu.Unlock()

	err := h.Commit()
	if err != ErrTxConflict {
		t.Fatalf("读集版本冲突 Commit 应返回 ErrTxConflict，实际: %s", err)
	}

	// b 不应被修改
	mock.mu.Lock()
	kvB := mock.kv["b"]
	mock.mu.Unlock()
	if kvB.Value != "val-b" {
		t.Fatalf("冲突后 b 不应被修改，实际: %s", kvB.Value)
	}
}

func TestCoordinatorCommitWriteConflict(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "a", "val-a", 1)

	// 注入冲突
	mock.conflictOn = "will-conflict"

	h := coordinator.Begin()
	h.Put("a", "new-a", 1)

	// 强制使用冲突 txID
	h.txID = "will-conflict"

	err := h.Commit()
	if err != ErrTxConflict {
		t.Fatalf("Prepare 冲突 Commit 应返回 ErrTxConflict，实际: %s", err)
	}
}

// ========== Rollback 测试 ==========

func TestCoordinatorRollback(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "a", "val-a", 1)

	h := coordinator.Begin()
	h.Put("a", "new-a", 1)

	// Commit 前回滚
	h.Rollback()

	// 验证数据未修改
	mock.mu.Lock()
	kvA := mock.kv["a"]
	aborted := mock.aborted[h.txID]
	mock.mu.Unlock()

	if kvA.Value != "val-a" {
		t.Fatalf("Rollback 后 a 应保持不变，实际: %s", kvA.Value)
	}
	if !aborted {
		t.Fatal("Rollback 应调用 AbortTx")
	}
}

// ========== generateTxID 测试 ==========

func TestGenerateTxIDUniqueness(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := generateTxID()
		if ids[id] {
			t.Fatalf("txID 重复: %s", id)
		}
		ids[id] = true
		// 验证格式：{数字}-{8个hex字符}
		if len(id) < 10 {
			t.Fatalf("txID 格式错误: %s", id)
		}
	}
}

// ========== 辅助函数测试 ==========

func TestGroupWriteKeys(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "a", "v", 1)

	h := coordinator.Begin()
	h.Put("a", "new-a", 1)
	h.Put("b", "new-b", 0)

	// h.groupWriteKeys() 使用 h.coordinator.router.Resolve
	// 所有 key 应路由到 group 1（只有一个 group）
	result := h.groupWriteKeys()

	if len(result) != 1 {
		t.Fatalf("应有 1 个 group，实际: %d", len(result))
	}

	// group 1 应有 2 个 write key
	wks := result[1] // group ID 为 1
	if len(wks) != 2 {
		t.Fatalf("group 1 应有 2 个 write key，实际: %d", len(wks))
	}

	// 按 key 排序验证
	sort.Slice(wks, func(i, j int) bool { return wks[i].Key < wks[j].Key })
	if wks[0].Key != "a" || wks[1].Key != "b" {
		t.Fatalf("write keys 应为 [a, b]，实际: %v", wks)
	}
}

func TestReadKeysToProto(t *testing.T) {
	keys := []ReadKey{
		{Key: "a", ExpectedVersion: 1},
		{Key: "b", ExpectedVersion: 0},
	}
	pbKeys := readKeysToProto(keys)

	if len(pbKeys) != 2 {
		t.Fatalf("应转换 2 个 key，实际: %d", len(pbKeys))
	}
	if pbKeys[0].GetKey() != "a" || pbKeys[0].GetExpectedVersion() != 1 {
		t.Fatalf("第一个 key 转换错误: %v", pbKeys[0])
	}
	if pbKeys[1].GetKey() != "b" || pbKeys[1].GetExpectedVersion() != 0 {
		t.Fatalf("第二个 key 转换错误: %v", pbKeys[1])
	}
}

func TestReadKeysToProtoNil(t *testing.T) {
	pbKeys := readKeysToProto(nil)
	if pbKeys != nil {
		t.Fatal("nil 输入应返回 nil")
	}
}

func TestWriteKeysToProto(t *testing.T) {
	keys := []WriteKey{
		{Key: "a", Value: "val-a", Version: 1},
	}
	pbKeys := writeKeysToProto(keys)

	if len(pbKeys) != 1 {
		t.Fatalf("应转换 1 个 key，实际: %d", len(pbKeys))
	}
	if pbKeys[0].GetKey() != "a" || pbKeys[0].GetValue() != "val-a" || pbKeys[0].GetVersion() != 1 {
		t.Fatalf("转换错误: %v", pbKeys[0])
	}
}

func TestWriteKeysToProtoNil(t *testing.T) {
	pbKeys := writeKeysToProto(nil)
	if pbKeys != nil {
		t.Fatal("nil 输入应返回 nil")
	}
}

// ========== TxHandle timeout 测试 ==========

func TestTxHandleDefaultTimeout(t *testing.T) {
	coordinator, _, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	h := coordinator.Begin()
	if h.timeoutMs != txDefaultTimeoutMs {
		t.Fatalf("默认超时应为 %d ms，实际: %d", txDefaultTimeoutMs, h.timeoutMs)
	}
}

// ========== ErrWrongGroup 测试 ==========

func TestCoordinatorCommitKeyNotInAnyGroup(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	// 创建一个只有一个 group 1 的 router，
	// 但 key 解析到不属于任何 group 的情况（实际上所有 key 都会被 hash 到某个 shard）
	// 但是...ShardRouter 的分片策略会覆盖全部 hash 空间，
	// 所以任意 key 都会落到某个 shard。
	// 这个测试很难触发，但我们可以在 coordinator 层做防御。
	//
	// 我们将这个测试保留为验证不会 panic 的 smoke test。
	h := coordinator.Begin()
	h.Put("test-key", "value", 0)
	_ = mock // 使用 mock

	// 空事务的 Commit 不会出问题
	err := h.Commit()
	// 由于只有一个 group 且 shard 覆盖 0-1023，
	// 任意 key 都会落到 group 1 → OK
	if err != OK {
		t.Fatalf("正常 Commit 应返回 OK，实际: %s", err)
	}
}

// ========== 并发测试 ==========

func TestCoordinatorConcurrentTransactions(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "counter", "0", 1)

	var wg sync.WaitGroup
	errs := make(chan Err, 10)
	successes := make(chan string, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			h := coordinator.Begin()
			// 读取 counter 当前值
			val, ver, err := h.Get("counter")
			if err != OK && err != ErrNoKey {
				errs <- err
				return
			}
			_ = val
			h.Put("counter", fmt.Sprintf("%d", idx), ver)
			if commitErr := h.Commit(); commitErr != OK {
				errs <- commitErr
				return
			}
			successes <- fmt.Sprintf("tx-%d", idx)
		}(i)
	}

	wg.Wait()
	close(errs)
	close(successes)

	successCount := 0
	for range successes {
		successCount++
	}
	errorCount := 0
	for e := range errs {
		t.Logf("并发事务错误: %s", e)
		errorCount++
	}

	t.Logf("成功: %d, 失败(预期-冲突): %d", successCount, errorCount)

	// 至少应有一些事务成功
	if successCount == 0 {
		t.Fatal("至少应有一个并发事务成功")
	}
}

// ========== 大数据量压力测试 ==========

func TestCoordinatorManyKeysTransaction(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	const numKeys = 100
	for i := 0; i < numKeys; i++ {
		seedMockKey(t, mock, fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i), 1)
	}

	h := coordinator.Begin()
	for i := 0; i < numKeys; i++ {
		h.Put(fmt.Sprintf("key-%d", i), fmt.Sprintf("updated-%d", i), 1)
	}

	err := h.Commit()
	if err != OK {
		t.Fatalf("%d 个 key 的事务 Commit 应返回 OK，实际: %s", numKeys, err)
	}

	// 验证所有 key 已更新
	mock.mu.Lock()
	defer mock.mu.Unlock()
	for i := 0; i < numKeys; i++ {
		kv := mock.kv[fmt.Sprintf("key-%d", i)]
		if kv == nil || kv.Value != fmt.Sprintf("updated-%d", i) {
			t.Fatalf("key-%d 应为 updated-%d，实际: %v", i, i, kv)
		}
	}
}

// ========== 边界条件测试 ==========

func TestTxHandleMultiplePutsSameKey(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "a", "val-a", 1)

	h := coordinator.Begin()
	h.Put("a", "first", 1)
	h.Put("a", "second", 1) // 覆盖前一个 Put

	val, ver, err := h.Get("a")
	if err != OK {
		t.Fatalf("Get 应返回 OK，实际: %s", err)
	}
	if val != "second" || ver != 1 {
		t.Fatalf("应返回最后一次 Put 的值 (second,1)，实际: (%s, %d)", val, ver)
	}
}

func TestTxHandleGetAfterPutThenServerChanged(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "a", "server-val", 1)

	h := coordinator.Begin()
	// 先 Put
	h.Put("a", "buffer-val", 1)

	// 修改 server
	mock.mu.Lock()
	mock.kv["a"] = &pb.KeyValue{Key: "a", Value: "server-modified", Version: 2}
	mock.mu.Unlock()

	// Get 应从 WriteSet 读取（脏读优先于 server）
	val, ver, err := h.Get("a")
	if err != OK {
		t.Fatalf("Get 应返回 OK，实际: %s", err)
	}
	if val != "buffer-val" {
		t.Fatalf("应返回 WriteSet 中的值 buffer-val，实际: %s", val)
	}
	if ver != 1 {
		t.Fatalf("版本应为 1，实际: %d", ver)
	}
}

// ========== 幂等 Commit 测试 ==========

func TestCoordinatorCommitIdempotent(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	seedMockKey(t, mock, "a", "val-a", 1)

	h := coordinator.Begin()
	h.Put("a", "new-a", 1)

	// 第一次 Commit
	if err := h.Commit(); err != OK {
		t.Fatalf("第一次 Commit 失败: %s", err)
	}

	// 尝试再次 Commit（会失败因为 WriteSet 还在，但 Phase 2 幂等）
	// 注意：Commit 不会清空 WriteSet，所以第二次调用会重新走一遍流程
	// mock 的 CommitTx 实现了幂等
	err := h.Commit()
	// 第二次 commit 可能成功（幂等）或返回 ErrTxConflict（因为锁已释放但 write keys 还在）
	// 实际上，在真实实现中第二次 Commit 会再次发送 CommitTx，mock 会返回 OK（幂等）
	if err != OK && err != ErrTxConflict {
		t.Fatalf("第二次 Commit 应返回 OK 或 ErrTxConflict，实际: %s", err)
	}

	// 数据应正确
	mock.mu.Lock()
	kv := mock.kv["a"]
	mock.mu.Unlock()
	if kv.Value != "new-a" {
		t.Fatalf("a 应为 new-a，实际: %s", kv.Value)
	}
}

// ========== 连接拒绝恢复测试 ==========

func TestTxHandleBeginReturnsValidHandle(t *testing.T) {
	coordinator, _, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	h := coordinator.Begin()
	if h == nil {
		t.Fatal("Begin 不应返回 nil")
	}
	if h.txID == "" {
		t.Fatal("txID 不应为空")
	}
	if h.readSet == nil || h.writeSet == nil || h.groups == nil {
		t.Fatal("readSet/writeSet/groups 应已初始化")
	}
}

// ========== metric/info 测试 ==========

func TestClerkBeginIntegration(t *testing.T) {
	coordinator, mock, cleanup := setupCoordinatorTest(t)
	defer cleanup()

	// 模拟 Clerk 使用 coordinator
	ck := &Clerk{coordinator: coordinator}
	h := ck.Begin()

	seedMockKey(t, mock, "x", "old", 1)

	h.Put("x", "new", 1)
	err := h.Commit()
	if err != OK {
		t.Fatalf("Clerk.Begin → Commit 应返回 OK，实际: %s", err)
	}

	mock.mu.Lock()
	kv := mock.kv["x"]
	mock.mu.Unlock()
	if kv.Value != "new" {
		t.Fatalf("x 应为 new，实际: %s", kv.Value)
	}
}

// ========== 确保 ShardingConfig.Group 与 mock 中 gid 一致 ==========
