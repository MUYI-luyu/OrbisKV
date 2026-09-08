package kv

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	pb "kvraft/api/pb/kvraft/api/pb"
	"kvraft/pkg/sharding"
	"kvraft/pkg/storage"
	"kvraft/pkg/watch"

	"google.golang.org/grpc"
)

type txE2ENode struct {
	kv       *KVServer
	store    *storage.Store
	wm       *watch.Manager
	listener net.Listener
	server   *grpc.Server
}

func startTxE2ECluster(t *testing.T) (*Clerk, func()) {
	t.Helper()
	nodes := make([]txE2ENode, 2)
	groups := make([]sharding.RaftGroupConfig, 0, 2)
	for i := range nodes {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		store, err := storage.NewStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		gid := i + 1
		kv := NewKVServer(0, gid, lis.Addr().String(), store)
		wm := watch.NewManager(watch.DefaultConfig())
		kv.SetRSM(&e2eRSM{kv: kv, wm: wm})
		gs := grpc.NewServer()
		pb.RegisterKVServiceServer(gs, &grpcKVService{kv: kv})
		go gs.Serve(lis)
		nodes[i] = txE2ENode{kv: kv, store: store, wm: wm, listener: lis, server: gs}
		groups = append(groups, sharding.RaftGroupConfig{GroupID: gid, Replicas: []string{lis.Addr().String()}})
	}
	ck, err := MakeShardedClerk(sharding.ShardingConfig{Groups: groups, NumShards: 1024})
	if err != nil {
		t.Fatal(err)
	}
	return ck, func() {
		ck.Close()
		for i := range nodes {
			atomic.StoreInt32(&nodes[i].kv.dead, 1)
			nodes[i].server.Stop()
			_ = nodes[i].listener.Close()
			nodes[i].wm.Close()
			_ = nodes[i].store.Close()
		}
	}
}

func keysInDifferentGroups(t *testing.T, ck *Clerk) (string, string, int, int) {
	t.Helper()
	first := ""
	firstGroup := -1
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("tx-e2e-%d", i)
		gid := ck.router.Resolve(key)
		if first == "" {
			first, firstGroup = key, gid
			continue
		}
		if gid != firstGroup {
			return first, key, firstGroup, gid
		}
	}
	t.Fatal("could not find keys in distinct groups")
	return "", "", 0, 0
}

func seedTxE2EKeys(t *testing.T, ck *Clerk, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if err := ck.Put(key, "initial", 0); err != OK {
			t.Fatalf("seed %s: %s", key, err)
		}
	}
}

func TestCrossShard2PCGRPCEndToEnd(t *testing.T) {
	ck, cleanup := startTxE2ECluster(t)
	defer cleanup()
	key1, key2, _, _ := keysInDifferentGroups(t, ck)
	seedTxE2EKeys(t, ck, key1, key2)
	tx := ck.Begin()
	tx.Put(key1, "one", 1)
	tx.Put(key2, "two", 1)
	if err := tx.Commit(); err != OK {
		t.Fatalf("cross-group commit: %s", err)
	}
	value1, version1, _, err1 := ck.Get(key1)
	value2, version2, _, err2 := ck.Get(key2)
	if err1 != OK || err2 != OK || value1 != "one" || value2 != "two" || version1 != 2 || version2 != 2 {
		t.Fatalf("committed values: %s/%d/%s, %s/%d/%s", value1, version1, err1, value2, version2, err2)
	}
}

func TestCrossShard2PCPrepareFailureAbortsOtherGroup(t *testing.T) {
	ck, cleanup := startTxE2ECluster(t)
	defer cleanup()
	key1, key2, gid1, _ := keysInDifferentGroups(t, ck)
	seedTxE2EKeys(t, ck, key1, key2)
	ctx := context.Background()
	lockResp, err := ck.router.PrepareTxToGroup(ctx, gid1, &pb.PrepareTxRequest{TxId: "blocking-tx", WriteKeys: []*pb.WriteKey{{Key: key1, Value: "blocked", Version: 1}}, TimeoutMs: 10000})
	if err != nil || lockResp.GetError() != string(OK) {
		t.Fatalf("blocking prepare: resp=%v err=%v", lockResp, err)
	}
	tx := ck.Begin()
	tx.Put(key1, "one", 1)
	tx.Put(key2, "two", 1)
	if err := tx.Commit(); err != ErrTxConflict {
		t.Fatalf("commit with lock conflict=%s", err)
	}
	value, version, _, getErr := ck.Get(key2)
	if getErr != OK || value != "initial" || version != 1 {
		t.Fatalf("other group was not aborted: %s/%d/%s", value, version, getErr)
	}
	retry := ck.Begin()
	retry.Put(key2, "after-abort", 1)
	if err := retry.Commit(); err != OK {
		t.Fatalf("lock on prepared peer was not released: %s", err)
	}
	_, _ = ck.router.AbortTxToGroup(ctx, gid1, &pb.AbortTxRequest{TxId: "blocking-tx"})
}

func TestCrossShard2PCRecoversPartialCommitFromDurableDecision(t *testing.T) {
	ck, cleanup := startTxE2ECluster(t)
	defer cleanup()
	key1, key2, gid1, gid2 := keysInDifferentGroups(t, ck)
	seedTxE2EKeys(t, ck, key1, key2)
	coordinator := gid1
	if gid2 < coordinator {
		coordinator = gid2
	}
	txID := "partial-commit-recovery"
	groups := []int32{int32(gid1), int32(gid2)}
	prepares := []struct {
		gid        int
		key, value string
	}{{gid1, key1, "one"}, {gid2, key2, "two"}}
	for _, p := range prepares {
		resp, err := ck.router.PrepareTxToGroup(context.Background(), p.gid, &pb.PrepareTxRequest{TxId: txID, WriteKeys: []*pb.WriteKey{{Key: p.key, Value: p.value, Version: 1}}, CoordinatorGroupId: int32(coordinator), ParticipantGroupIds: groups, TimeoutMs: 1})
		if err != nil || resp.GetError() != string(OK) {
			t.Fatalf("prepare group %d: %v/%v", p.gid, resp, err)
		}
	}
	decision, err := ck.router.CommitTxToGroup(context.Background(), coordinator, &pb.CommitTxRequest{TxId: txID, DecisionOnly: true, ParticipantGroupIds: groups})
	if err != nil || decision.GetError() != string(OK) {
		t.Fatalf("durable decision: %v/%v", decision, err)
	}
	first := gid1
	second := gid2
	if first != coordinator {
		first, second = second, first
	}
	resp, err := ck.router.CommitTxToGroup(context.Background(), first, &pb.CommitTxRequest{TxId: txID})
	if err != nil || resp.GetError() != string(OK) {
		t.Fatalf("first commit: %v/%v", resp, err)
	}
	if errCode := ck.RecoverTransaction(txID, second); errCode != OK {
		t.Fatalf("recover: %s", errCode)
	}
	for key, want := range map[string]string{key1: "one", key2: "two"} {
		value, version, _, getErr := ck.Get(key)
		if getErr != OK || value != want || version != 2 {
			t.Fatalf("%s=%q/%d/%s", key, value, version, getErr)
		}
	}
}

func TestCrossShard2PCPrepareFailureRecordsDurableAbort(t *testing.T) {
	ck, cleanup := startTxE2ECluster(t)
	defer cleanup()
	key1, key2, gid1, gid2 := keysInDifferentGroups(t, ck)
	seedTxE2EKeys(t, ck, key1, key2)
	_, _ = ck.router.PrepareTxToGroup(context.Background(), gid1, &pb.PrepareTxRequest{TxId: "block", WriteKeys: []*pb.WriteKey{{Key: key1, Value: "x", Version: 1}}, TimeoutMs: 10000})
	tx := ck.Begin()
	tx.Put(key1, "one", 1)
	tx.Put(key2, "two", 1)
	if got := tx.Commit(); got != ErrTxConflict {
		t.Fatalf("commit=%s", got)
	}
	coordinator := gid1
	if gid2 < coordinator {
		coordinator = gid2
	}
	status, err := ck.router.ResolveTxStatusToGroup(context.Background(), coordinator, &pb.ResolveTxStatusRequest{TxId: tx.txID})
	if err != nil || status.GetStatus() != "ABORTED" {
		t.Fatalf("durable abort=%v/%v", status, err)
	}
	_, _ = ck.router.AbortTxToGroup(context.Background(), gid1, &pb.AbortTxRequest{TxId: "block"})
}

func TestCrossGroupReadOnlyParticipantIsPrepared(t *testing.T) {
	ck, cleanup := startTxE2ECluster(t)
	defer cleanup()
	readKey, writeKey, _, _ := keysInDifferentGroups(t, ck)
	seedTxE2EKeys(t, ck, readKey, writeKey)
	tx := ck.Begin()
	if value, _, err := tx.Get(readKey); err != OK || value != "initial" {
		t.Fatalf("read=%q/%s", value, err)
	}
	if err := ck.Put(readKey, "external", 1); err != OK {
		t.Fatalf("external update: %s", err)
	}
	tx.Put(writeKey, "transactional", 1)
	if got := tx.Commit(); got != ErrTxConflict {
		t.Fatalf("commit=%s, want conflict", got)
	}
	value, version, _, err := ck.Get(writeKey)
	if err != OK || value != "initial" || version != 1 {
		t.Fatalf("write participant changed: %q/%d/%s", value, version, err)
	}
}
