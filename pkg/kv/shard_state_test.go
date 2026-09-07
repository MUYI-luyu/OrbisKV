package kv

import (
	"testing"

	pb "kvraft/api/pb/kvraft/api/pb"
)

func TestShardStateSnapshotRoundTrip(t *testing.T) {
	manager := newShardStateManager(1, 4)
	if err := manager.SetShardState(2, pb.ShardState_MIGRATING, 7, 9, []string{"127.0.0.1:9001", "127.0.0.1:9002"}); err != nil {
		t.Fatal(err)
	}

	snapshot := manager.Snapshot()
	restored := newShardStateManager(1, 4)
	if err := restored.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	meta, ok := restored.GetShardState(2)
	if !ok || meta.state != pb.ShardState_MIGRATING || meta.targetGroup != 7 {
		t.Fatalf("state not restored: %+v, ok=%v", meta, ok)
	}
	if len(meta.targetReplicas) != 2 || meta.targetReplicas[1] != "127.0.0.1:9002" {
		t.Fatalf("replicas not restored: %v", meta.targetReplicas)
	}
	if restored.GetEpoch() != 9 {
		t.Fatalf("epoch not restored: %d", restored.GetEpoch())
	}
}

func snapshotRestorePair(t *testing.T) (*KVServer, *KVServer, func()) {
	t.Helper()
	_, source, cleanup1 := setupTestTxManager(t)
	snapshot := source.Snapshot()
	_, restored, cleanup2 := setupTestTxManager(t)
	restored.Restore(snapshot)
	return source, restored, func() { cleanup1(); cleanup2() }
}

func TestSnapshotRestorePutData(t *testing.T) {
	_, source, cleanup := setupTestTxManager(t)
	defer cleanup()
	seedKey(t, source, "snapshot-put", "value", 1)
	snapshot := source.Snapshot()
	_, restored, cleanup2 := setupTestTxManager(t)
	defer cleanup2()
	restored.Restore(snapshot)
	value, version, _, exists, err := restored.store.Get("snapshot-put")
	if err != nil || !exists || value != "value" || version != 1 {
		t.Fatalf("put restore mismatch: %q/%d/%v/%v", value, version, exists, err)
	}
}

func TestSnapshotRestoreTransactionCommitIsIdempotent(t *testing.T) {
	_, source, cleanup := setupTestTxManager(t)
	defer cleanup()
	seedKey(t, source, "snapshot-tx", "tx-value", 1)
	if err := source.store.PutTxRecord(txCommitPrefix+"snapshot-tx-id", []byte("1")); err != nil {
		t.Fatal(err)
	}
	snapshot := source.Snapshot()
	_, restored, cleanup2 := setupTestTxManager(t)
	defer cleanup2()
	restored.Restore(snapshot)
	if _, found, _ := restored.store.GetTxRecord(txCommitPrefix + "snapshot-tx-id"); !found {
		t.Fatal("commit record not restored")
	}
	before, beforeVersion, _, _, _ := restored.store.Get("snapshot-tx")
	if reply := restored.txMgr.Commit(&CommitTxArgs{TxID: "snapshot-tx-id", WriteKeys: []WriteKey{{Key: "snapshot-tx", Value: "tx-value", Version: 1}}}); reply.Err != OK {
		t.Fatalf("repeat commit failed: %+v", reply)
	}
	after, afterVersion, _, _, _ := restored.store.Get("snapshot-tx")
	if before != after || beforeVersion != afterVersion {
		t.Fatalf("repeat commit changed data: %q/%d -> %q/%d", before, beforeVersion, after, afterVersion)
	}
}

func TestSnapshotRestoreShardMetadata(t *testing.T) {
	_, source, cleanup := setupTestTxManager(t)
	defer cleanup()
	shard := source.shardForKey("snapshot-shard")
	if err := source.shardMgr.SetShardState(shard, pb.ShardState_MIGRATING, 7, 9, []string{"target-a", "target-b"}); err != nil {
		t.Fatal(err)
	}
	snapshot := source.Snapshot()
	_, restored, cleanup2 := setupTestTxManager(t)
	defer cleanup2()
	restored.Restore(snapshot)
	meta, ok := restored.shardMgr.GetShardState(shard)
	if !ok || meta.state != pb.ShardState_MIGRATING || meta.targetGroup != 7 || restored.TopologyEpoch() != 9 || len(meta.targetReplicas) != 2 {
		t.Fatalf("metadata restore mismatch: %+v epoch=%d", meta, restored.TopologyEpoch())
	}
}

func TestSnapshotRestoreCombinedState(t *testing.T) {
	_, source, cleanup := setupTestTxManager(t)
	defer cleanup()
	seedKey(t, source, "combined-data", "data", 1)
	seedKey(t, source, "combined-tx", "tx", 1)
	if err := source.store.PutTxRecord(txCommitPrefix+"combined-id", []byte("1")); err != nil {
		t.Fatal(err)
	}
	shard := source.shardForKey("combined-data")
	if err := source.shardMgr.SetShardState(shard, pb.ShardState_ABSENT, 8, 12, []string{"target-c"}); err != nil {
		t.Fatal(err)
	}
	snapshot := source.Snapshot()
	_, restored, cleanup2 := setupTestTxManager(t)
	defer cleanup2()
	restored.Restore(snapshot)
	value, _, _, exists, err := restored.store.Get("combined-data")
	if err != nil || !exists || value != "data" {
		t.Fatalf("combined KV restore mismatch: %q/%v/%v", value, exists, err)
	}
	if _, found, _ := restored.store.GetTxRecord(txCommitPrefix + "combined-id"); !found {
		t.Fatal("combined transaction record missing")
	}
	meta, ok := restored.shardMgr.GetShardState(shard)
	if !ok || meta.state != pb.ShardState_ABSENT || meta.targetGroup != 8 || restored.TopologyEpoch() != 12 {
		t.Fatalf("combined metadata mismatch: %+v epoch=%d", meta, restored.TopologyEpoch())
	}
}
