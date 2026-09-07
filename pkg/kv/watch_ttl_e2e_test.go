package kv

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	pb "kvraft/api/pb/kvraft/api/pb"
	"kvraft/pkg/raft"
	"kvraft/pkg/storage"
	"kvraft/pkg/watch"
)

type e2eRSM struct {
	kv *KVServer
	wm *watch.Manager
}

func (r *e2eRSM) Submit(req any) (Err, any) {
	ret := r.kv.DoOp(req)
	r.kv.OnOpComplete(req, ret, 1)
	return OK, ret
}
func (r *e2eRSM) SubmitLeaseReadWithMode(req any) (Err, any, bool) {
	ret := r.kv.DoOp(req)
	return OK, ret, true
}
func (r *e2eRSM) GetWatchManager() *watch.Manager                { return r.wm }
func (r *e2eRSM) RegisterOpCompleteListener(OpCompleteListener)  {}
func (r *e2eRSM) GetState() (int, bool)                          { return 1, true }
func (r *e2eRSM) GetLastApplied() int                            { return 1 }
func (r *e2eRSM) IsLeaderWithLease() bool                        { return true }
func (r *e2eRSM) Close()                                         {}
func (r *e2eRSM) ApplyLoopPerfStatsSnapshot() ApplyLoopPerfStats { return ApplyLoopPerfStats{} }
func (r *e2eRSM) RaftPerfStatsSnapshot() raft.RaftPerfStats      { return raft.RaftPerfStats{} }

func startE2EGRPC(t *testing.T) (pb.KVServiceClient, *KVServer, func()) {
	t.Helper()
	store, err := storage.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	kv := NewKVServer(0, 1, "e2e", store)
	wm := watch.NewManager(watch.DefaultConfig())
	kv.SetRSM(&e2eRSM{kv: kv, wm: wm})
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterKVServiceServer(gs, &grpcKVService{kv: kv})
	go gs.Serve(lis)
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	return pb.NewKVServiceClient(conn), kv, func() {
		atomic.StoreInt32(&kv.dead, 1)
		wait := kv.ttlEvery + 20*time.Millisecond
		if wait > 100*time.Millisecond {
			wait = 100 * time.Millisecond
		}
		time.Sleep(wait)
		conn.Close()
		gs.Stop()
		lis.Close()
		wm.Close()
		store.Close()
	}
}

func TestGRPCWatchPutDeleteE2E(t *testing.T) {
	client, kv, cleanup := startE2EGRPC(t)
	defer cleanup()
	stream, err := client.Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.WatchRequest{RequestType: &pb.WatchRequest_Create{Create: &pb.WatchCreateRequest{Key: "watched"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Put(context.Background(), &pb.PutRequest{Key: "watched", Value: "v1"}); err != nil {
		t.Fatal(err)
	}
	ev, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if ev.GetKey() != "watched" || ev.GetNewValue() != "v1" || ev.GetEventType() != "set" {
		t.Fatalf("put event=%+v", ev)
	}
	if _, err := client.Delete(context.Background(), &pb.DeleteRequest{Key: "watched"}); err != nil {
		t.Fatal(err)
	}
	ev, err = stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if ev.GetEventType() != "delete" || ev.GetOldValue() != "v1" {
		t.Fatalf("delete event=%+v", ev)
	}
	_ = kv
}

func TestGRPCTTLExpiryE2E(t *testing.T) {
	client, kv, cleanup := startE2EGRPC(t)
	defer cleanup()
	kv.ttlEvery = 10 * time.Millisecond
	stream, err := client.Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.WatchRequest{RequestType: &pb.WatchRequest_Create{Create: &pb.WatchCreateRequest{Key: "ttl-key"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Put(context.Background(), &pb.PutRequest{Key: "ttl-key", Value: "short", TtlSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	go kv.TTLCleanupLoop()
	setEvent, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if setEvent.GetEventType() != "set" || setEvent.GetNewValue() != "short" {
		t.Fatalf("put event=%+v", setEvent)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, _ := client.Get(context.Background(), &pb.GetRequest{Key: "ttl-key"})
		if resp.GetError() == "ErrNoKey" {
			ev, recvErr := stream.Recv()
			if recvErr != nil {
				t.Fatal(recvErr)
			}
			if ev.GetEventType() != "expire" || ev.GetOldValue() != "short" {
				t.Fatalf("expiry event=%+v", ev)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("TTL key did not expire")
}
