package kv

import (
	"context"
	"encoding/json"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"
	"kvraft/pkg/sharding"
	"kvraft/pkg/storage"
	"kvraft/pkg/watch"

	"google.golang.org/grpc"
)

// TestCoordinatorRecoveryFullLifecycle 完整的生命周期测试：
// 1. Client 创建并触发初始恢复
// 2. 执行事务 Phase 1 (所有 Participant PREPARED)
// 3. 持久化 COMMITTED 决策
// 4. 只让一个 Participant 完成 Commit（故意阻断第二个）
// 5. Server（Coordinator）重启（全新实例）
// 6. Client 不重启，再次连接并触发 Recovery
// 7. 验证第二个 Participant 最终也完成 Commit
func TestCoordinatorRecoveryFullLifecycle(t *testing.T) {
	// === Phase 0: 启动集群 ===
	coordinatorNode := startLifecycleNode(t, 1)
	participantNode := startLifecycleNode(t, 2)

	cfg := sharding.ShardingConfig{
		Groups: []sharding.RaftGroupConfig{
			{GroupID: 1, Replicas: []string{coordinatorNode.addr}},
			{GroupID: 2, Replicas: []string{participantNode.addr}},
		},
		NumShards: 1024,
	}

	// === Phase 1: Client 创建（触发初始恢复）===
	t.Logf("Phase 1: Create client and trigger initial recovery")
	ck, err := MakeShardedClerk(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// 等待初始恢复完成
	time.Sleep(100 * time.Millisecond)

	// 找到两个不同 Group 的 key
	key1, key2 := findKeysInGroups(t, ck, 1, 2)

	// 准备初始数据
	if err := ck.Put(key1, "initial1", 0); err != OK {
		t.Fatalf("seed key1: %s", err)
	}
	if err := ck.Put(key2, "initial2", 0); err != OK {
		t.Fatalf("seed key2: %s", err)
	}

	// === Phase 2: 执行事务 Phase 1（所有 Participant PREPARED）===
	t.Logf("Phase 2: Transaction Phase 1 - Prepare all participants")
	txID := "lifecycle-test-tx"
	groups32 := []int32{1, 2}

	// Prepare Coordinator (Group 1)
	prepareResp1, err := ck.router.PrepareTxToGroup(context.Background(), 1, &pb.PrepareTxRequest{
		TxId: txID,
		WriteKeys: []*pb.WriteKey{{Key: key1, Value: "committed1", Version: 1}},
		CoordinatorGroupId: 1,
		ParticipantGroupIds: groups32,
		TimeoutMs: 10000,
	})
	if err != nil || prepareResp1.GetError() != string(OK) {
		t.Fatalf("prepare coordinator: %v/%v", prepareResp1, err)
	}

	// 验证 Coordinator prepare 记录已存在
	prepareKey1 := "prepare:" + txID
	if raw, found, err := coordinatorNode.store.GetTxRecord(prepareKey1); !found || err != nil {
		t.Fatalf("After prepare Group 1: prepare record missing: found=%v err=%v", found, err)
	} else {
		t.Logf("After prepare Group 1: prepare record exists, size=%d bytes", len(raw))
	}

	// Prepare Participant (Group 2)
	prepareResp2, err := ck.router.PrepareTxToGroup(context.Background(), 2, &pb.PrepareTxRequest{
		TxId: txID,
		WriteKeys: []*pb.WriteKey{{Key: key2, Value: "committed2", Version: 1}},
		CoordinatorGroupId: 1,
		ParticipantGroupIds: groups32,
		TimeoutMs: 10000,
	})
	if err != nil || prepareResp2.GetError() != string(OK) {
		t.Fatalf("prepare participant: %v/%v", prepareResp2, err)
	}

	// === Phase 3: 持久化 COMMITTED 决策到 Coordinator ===
	t.Logf("Phase 3: Persist COMMITTED decision to Coordinator")
	decisionResp, err := ck.router.CommitTxToGroup(context.Background(), 1, &pb.CommitTxRequest{
		TxId: txID,
		DecisionOnly: true,
		ParticipantGroupIds: groups32,
		WriteKeys: []*pb.WriteKey{
			{Key: key1, Value: "committed1", Version: 1},
			{Key: key2, Value: "committed2", Version: 1},
		},
	})
	if err != nil || decisionResp.GetError() != string(OK) {
		t.Fatalf("persist decision: %v/%v", decisionResp, err)
	}

	// 验证 decision 记录已包含 writeKeys
	decisionKey := "decision:" + txID
	if raw, found, err := coordinatorNode.store.GetTxRecord(decisionKey); !found || err != nil {
		t.Fatalf("After persist decision: decision record missing: found=%v err=%v", found, err)
	} else {
		var dec txDecisionRecord
		if err2 := json.Unmarshal(raw, &dec); err2 != nil {
			t.Fatalf("Failed to unmarshal decision: %v", err2)
		}
		t.Logf("After persist decision: status=%v, participants=%v, writeKeys count=%d", dec.Decision, dec.ParticipantGroupIDs, len(dec.WriteKeys))
		if len(dec.WriteKeys) != 2 {
			t.Fatalf("Decision record should have 2 writeKeys, got %d!", len(dec.WriteKeys))
		}
	}

	// === Phase 4: 只让 Coordinator 自己（Group 1）完成 Commit ===
	t.Logf("Phase 4: Commit only Coordinator (Group 1), block Participant (Group 2)")
	commitResp1, err := ck.router.CommitTxToGroup(context.Background(), 1, &pb.CommitTxRequest{
		TxId: txID,
		WriteKeys: []*pb.WriteKey{{Key: key1, Value: "committed1", Version: 1}},
	})
	if err != nil || commitResp1.GetError() != string(OK) {
		t.Fatalf("commit coordinator: %v/%v", commitResp1, err)
	}

	// 验证 Group 1 已提交
	value1, version1, _, err1 := ck.Get(key1)
	if err1 != OK || value1 != "committed1" || version1 != 2 {
		t.Fatalf("after commit group 1: key1=%q version=%d err=%s", value1, version1, err1)
	}

	// 验证 Group 2 仍然在 PREPARED（未提交）
	value2, version2, _, err2 := ck.Get(key2)
	if err2 != OK || value2 != "initial2" || version2 != 1 {
		t.Fatalf("before recovery: key2=%q version=%d err=%s (expected initial2/1)", value2, version2, err2)
	}

	t.Logf("Verified: Group 1 committed, Group 2 still prepared")

	// === Phase 5: Coordinator (Group 1) 重启（全新实例）===
	t.Logf("Phase 5: Restart Coordinator (Group 1) with fresh instance")

	// 保存数据目录和地址
	dataDir := coordinatorNode.dataDir
	addr := coordinatorNode.addr

	t.Logf("Data directory: %s", dataDir)

	// 验证 decision 记录存在且包含 writeKeys
	if raw2, found2, err2 := coordinatorNode.store.GetTxRecord("decision:" + txID); !found2 || err2 != nil {
		t.Fatalf("Before restart: decision record missing or error: found=%v err=%v", found2, err2)
	} else {
		t.Logf("Before restart: decision record exists, size=%d bytes", len(raw2))
		var dec2 txDecisionRecord
		if err3 := json.Unmarshal(raw2, &dec2); err3 != nil {
			t.Fatalf("Failed to unmarshal decision: %v", err3)
		}
		t.Logf("Decision record before restart: status=%v, participants=%v, writeKeys count=%d", dec2.Decision, dec2.ParticipantGroupIDs, len(dec2.WriteKeys))
		if len(dec2.WriteKeys) != 2 {
			t.Fatalf("Decision record should have 2 writeKeys before restart, got %d!", len(dec2.WriteKeys))
		}
	}

	// 完全停止旧节点
	atomic.StoreInt32(&coordinatorNode.kv.dead, 1)
	coordinatorNode.server.Stop()
	_ = coordinatorNode.listener.Close()
	coordinatorNode.wm.Close()
	_ = coordinatorNode.store.Close()

	// 等待完全停止
	time.Sleep(100 * time.Millisecond)

	// 重新打开存储（复用数据）
	store, err := storage.NewStore(dataDir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}

	// 验证重新打开后 decision 记录仍在
	if raw, found, err := store.GetTxRecord("decision:" + txID); !found || err != nil {
		t.Fatalf("After reopen: decision record missing or error: found=%v err=%v", found, err)
	} else {
		t.Logf("After reopen: decision record still exists, size=%d bytes", len(raw))
		var dec3 txDecisionRecord
		if err2 := json.Unmarshal(raw, &dec3); err2 != nil {
			t.Fatalf("Failed to unmarshal decision after reopen: %v", err2)
		}
		t.Logf("Decision record after reopen: status=%v, writeKeys count=%d", dec3.Decision, len(dec3.WriteKeys))
		if len(dec3.WriteKeys) != 2 {
			t.Fatalf("Decision record should have 2 writeKeys after reopen, got %d!", len(dec3.WriteKeys))
		}
	}

	// 创建新的监听器
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("reopen listener: %v", err)
	}

	// 创建全新的 KVServer 实例（模拟真实重启）
	newKV := NewKVServer(0, 1, coordinatorNode.addr, store)
	newWM := watch.NewManager(watch.DefaultConfig())
	newKV.SetRSM(&e2eRSM{kv: newKV, wm: newWM})

	// Participant 恢复锁表
	if err := newKV.txMgr.RebuildLockTable(); err != nil {
		t.Logf("RebuildLockTable error: %v", err)
	}

	// 启动 gRPC 服务
	gs := grpc.NewServer()
	pb.RegisterKVServiceServer(gs, &grpcKVService{kv: newKV})
	go gs.Serve(lis)

	// 更新节点引用
	coordinatorNode.kv = newKV
	coordinatorNode.store = store
	coordinatorNode.wm = newWM
	coordinatorNode.listener = lis
	coordinatorNode.server = gs

	t.Logf("Coordinator restarted with fresh KVServer instance")

	// 等待服务就绪
	time.Sleep(200 * time.Millisecond)

	// 验证此时 TxManager.router 应该是 nil（全新实例）
	if newKV.txMgr.router != nil {
		t.Fatalf("BUG: Fresh TxManager should have nil router, got non-nil")
	}

	// === Phase 6: Client 不重启，再次连接并触发 Recovery ===
	t.Logf("Phase 6: Client (not restarted) reconnects and triggers recovery")

	// Client 主动触发恢复（模拟重连或心跳）
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := ck.router.TriggerRecoveryToGroup(ctx, 1, cfg); err != nil {
		t.Fatalf("trigger recovery failed: %v", err)
	}

	t.Logf("Recovery triggered successfully")

	// 验证 router 已设置
	time.Sleep(100 * time.Millisecond)
	if newKV.txMgr.router == nil {
		t.Fatalf("BUG: After SetRouterAndRecover, router should be set")
	}

	// === Phase 7: 验证 Participant (Group 2) 最终完成 Commit ===
	t.Logf("Phase 7: Verify Participant (Group 2) eventually commits")

	deadline := time.Now().Add(5 * time.Second)
	for {
		value2, version2, _, err2 := ck.Get(key2)
		if err2 == OK && value2 == "committed2" && version2 == 2 {
			t.Logf("SUCCESS: Group 2 committed after recovery")
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("Recovery timeout: key2=%q version=%d err=%s (expected committed2/2)", value2, version2, err2)
		}

		time.Sleep(100 * time.Millisecond)
	}

	// 最终验证两个 key 都已提交
	value1, version1, _, err1 = ck.Get(key1)
	value2, version2, _, err2 = ck.Get(key2)

	if err1 != OK || value1 != "committed1" || version1 != 2 {
		t.Fatalf("final key1: %q/%d/%s", value1, version1, err1)
	}
	if err2 != OK || value2 != "committed2" || version2 != 2 {
		t.Fatalf("final key2: %q/%d/%s", value2, version2, err2)
	}

	t.Logf("✅ Full lifecycle verified: Coordinator recovered COMMITTED decision and completed Phase 2")

	// 清理
	ck.Close()
	cleanupLifecycleNode(coordinatorNode)
	cleanupLifecycleNode(participantNode)
}

// === 辅助结构和函数 ===

type lifecycleNode struct {
	groupID  int
	kv       *KVServer
	store    *storage.Store
	wm       *watch.Manager
	listener net.Listener
	server   *grpc.Server
	addr     string
	dataDir  string
}

func startLifecycleNode(t *testing.T, groupID int) *lifecycleNode {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	dataDir := t.TempDir()
	store, err := storage.NewStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}

	kv := NewKVServer(0, groupID, lis.Addr().String(), store)
	wm := watch.NewManager(watch.DefaultConfig())
	kv.SetRSM(&e2eRSM{kv: kv, wm: wm})

	gs := grpc.NewServer()
	pb.RegisterKVServiceServer(gs, &grpcKVService{kv: kv})
	go gs.Serve(lis)

	return &lifecycleNode{
		groupID:  groupID,
		kv:       kv,
		store:    store,
		wm:       wm,
		listener: lis,
		server:   gs,
		addr:     lis.Addr().String(),
		dataDir:  dataDir,
	}
}

func cleanupLifecycleNode(node *lifecycleNode) {
	if node == nil {
		return
	}
	atomic.StoreInt32(&node.kv.dead, 1)
	node.server.Stop()
	_ = node.listener.Close()
	node.wm.Close()
	_ = node.store.Close()
}

func findKeysInGroups(t *testing.T, ck *Clerk, gid1, gid2 int) (string, string) {
	t.Helper()

	var key1, key2 string

	for i := 0; i < 10000; i++ {
		key := "lifecycle-key-" + string(rune('a'+i%26)) + string(rune('0'+(i/26)%10))
		gid := ck.router.Resolve(key)

		if key1 == "" && gid == gid1 {
			key1 = key
		}
		if key2 == "" && gid == gid2 {
			key2 = key
		}

		if key1 != "" && key2 != "" {
			return key1, key2
		}
	}

	t.Fatal("could not find keys in specified groups")
	return "", ""
}
