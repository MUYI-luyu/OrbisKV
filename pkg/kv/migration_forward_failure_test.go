package kv

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	pb "kvraft/api/pb/kvraft/api/pb"
)

type rejectingMigrationTarget struct {
	pb.UnimplementedKVServiceServer
}

func (*rejectingMigrationTarget) Put(context.Context, *pb.PutRequest) (*pb.PutResponse, error) {
	return &pb.PutResponse{Error: string(ErrVersion)}, nil
}
func (*rejectingMigrationTarget) Delete(context.Context, *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	return &pb.DeleteResponse{Error: string(ErrWrongGroup)}, nil
}

func attachRejectingMigrationTarget(t *testing.T, kv *KVServer, targetGroup int) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	pb.RegisterKVServiceServer(gs, &rejectingMigrationTarget{})
	go gs.Serve(lis)
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	kv.shardMgr.mu.Lock()
	kv.shardMgr.forwardConns[targetGroup] = conn
	kv.shardMgr.forwardClients[targetGroup] = pb.NewKVServiceClient(conn)
	kv.shardMgr.mu.Unlock()
	t.Cleanup(func() { gs.Stop(); _ = lis.Close() })
}

func TestMigrationForwardPutFailureRestoresSource(t *testing.T) {
	_, kv, cleanup := setupTestTxManager(t)
	defer cleanup()
	key := "migration-put-rollback"
	seedKey(t, kv, key, "old", 5)
	attachRejectingMigrationTarget(t, kv, 2)
	reply := kv.doPutWithForward(&PutArgs{Key: key, Value: "new", Version: 5}, shardMeta{state: pb.ShardState_MIGRATING, targetGroup: 2})
	if reply.Err != ErrWrongGroup {
		t.Fatalf("reply=%+v", reply)
	}
	value, version, _, exists, err := kv.store.Get(key)
	if err != nil || !exists || value != "old" || version != 5 {
		t.Fatalf("source not restored: value=%q version=%d exists=%v err=%v", value, version, exists, err)
	}
}

func TestMigrationForwardDeleteFailureRestoresSource(t *testing.T) {
	_, kv, cleanup := setupTestTxManager(t)
	defer cleanup()
	key := "migration-delete-rollback"
	seedKey(t, kv, key, "old", 5)
	attachRejectingMigrationTarget(t, kv, 2)
	reply := kv.doDeleteWithForward(&DeleteArgs{Key: key}, shardMeta{state: pb.ShardState_MIGRATING, targetGroup: 2})
	if reply.Err != ErrWrongGroup {
		t.Fatalf("reply=%+v", reply)
	}
	value, version, _, exists, err := kv.store.Get(key)
	if err != nil || !exists || value != "old" || version != 5 {
		t.Fatalf("source not restored: value=%q version=%d exists=%v err=%v", value, version, exists, err)
	}
}
