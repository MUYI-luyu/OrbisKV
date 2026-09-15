package sharding

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"

	"google.golang.org/grpc"
)

type testKVService struct {
	pb.UnimplementedKVServiceServer

	mu                sync.Mutex
	kv                map[string]*pb.KeyValue
	alwaysWrongLeader bool
	failDelete        bool
}

func (s *testKVService) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if s.alwaysWrongLeader {
		return &pb.GetResponse{Error: "ErrWrongLeader"}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.kv[req.GetKey()]
	if !ok {
		return &pb.GetResponse{Error: "ErrNoKey"}, nil
	}
	return &pb.GetResponse{Value: it.GetValue(), Version: it.GetVersion(), Error: "OK"}, nil
}

func (s *testKVService) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if s.alwaysWrongLeader {
		return &pb.PutResponse{Error: "ErrWrongLeader"}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.kv[req.GetKey()]
	if !ok {
		if req.GetVersion() != 0 {
			return &pb.PutResponse{Error: "ErrNoKey"}, nil
		}
		s.kv[req.GetKey()] = &pb.KeyValue{Key: req.GetKey(), Value: req.GetValue(), Version: 1}
		return &pb.PutResponse{Error: "OK"}, nil
	}
	if cur.GetVersion() != req.GetVersion() {
		return &pb.PutResponse{Error: "ErrVersion"}, nil
	}
	next := cur.GetVersion() + 1
	s.kv[req.GetKey()] = &pb.KeyValue{Key: req.GetKey(), Value: req.GetValue(), Version: next}
	return &pb.PutResponse{Error: "OK"}, nil
}

func (s *testKVService) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if s.alwaysWrongLeader {
		return &pb.DeleteResponse{Error: "ErrWrongLeader"}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failDelete {
		return &pb.DeleteResponse{Error: "ErrInternal"}, nil
	}
	if _, ok := s.kv[req.GetKey()]; !ok {
		return &pb.DeleteResponse{Error: "ErrNoKey"}, nil
	}
	delete(s.kv, req.GetKey())
	return &pb.DeleteResponse{Error: "OK"}, nil
}

func (s *testKVService) Scan(_ context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	if s.alwaysWrongLeader {
		return &pb.ScanResponse{Error: "ErrWrongLeader"}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	prefix := req.GetPrefix()
	keys := make([]string, 0, len(s.kv))
	for k := range s.kv {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	limit := int(req.GetLimit())
	if limit <= 0 || limit > len(keys) {
		limit = len(keys)
	}

	items := make([]*pb.KeyValue, 0, limit)
	for i := 0; i < limit; i++ {
		kv := s.kv[keys[i]]
		items = append(items, &pb.KeyValue{Key: kv.GetKey(), Value: kv.GetValue(), Version: kv.GetVersion()})
	}

	return &pb.ScanResponse{Items: items, Error: "OK"}, nil
}

func (s *testKVService) Watch(_ grpc.BidiStreamingServer[pb.WatchRequest, pb.WatchEvent]) error {
	return nil
}

func (s *testKVService) GetClusterStatus(_ context.Context, _ *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	return &pb.ClusterStatusResponse{CurrentTerm: 1}, nil
}

func startTestKVServer(t *testing.T, svc *testKVService) (addr string, stop func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	gs := grpc.NewServer()
	pb.RegisterKVServiceServer(gs, svc)

	go func() {
		_ = gs.Serve(lis)
	}()

	return lis.Addr().String(), func() {
		gs.Stop()
		_ = lis.Close()
	}
}

func findKeyForGroup(t *testing.T, r *ShardRouter, gid int, prefix string) string {
	t.Helper()
	// Shard 拓扑下每个 group 拥有约 numShards/numGroups 的 shard，
	// 随机 key 命中目标 group 的概率 ≈ 1/numGroups，平均只需数次尝试。
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("%s-%d", prefix, i)
		if r.Resolve(k) == gid {
			return k
		}
	}
	t.Fatalf("cannot find key for group %d (prefix=%s, tried 200)", gid, prefix)
	return ""
}

func TestShardRouterResolveAndCRUD(t *testing.T) {
	s1 := &testKVService{kv: map[string]*pb.KeyValue{}}
	a1, stop1 := startTestKVServer(t, s1)
	defer stop1()

	s2 := &testKVService{kv: map[string]*pb.KeyValue{}}
	a2, stop2 := startTestKVServer(t, s2)
	defer stop2()

	r, err := NewShardRouter(ShardingConfig{
		Groups: []RaftGroupConfig{
			{GroupID: 1, Replicas: []string{a1}, LeaderIdx: 0},
			{GroupID: 2, Replicas: []string{a2}, LeaderIdx: 0},
		},
		VirtualNodeCount: 64,
		ConnectTimeout:   2 * time.Second,
		RequestTimeout:   1200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new shard router failed: %v", err)
	}
	defer r.Close()

	key1 := findKeyForGroup(t, r, 1, "g1")
	key2 := findKeyForGroup(t, r, 2, "g2")

	ctx := context.Background()
	if _, err := r.PutRouteWithTTL(ctx, key1, "v1", 0, 0); err != nil {
		t.Fatalf("put key1 failed: %v", err)
	}
	if _, err := r.PutRouteWithTTL(ctx, key2, "v2", 0, 0); err != nil {
		t.Fatalf("put key2 failed: %v", err)
	}

	g1, err := r.GetRoute(ctx, key1)
	if err != nil || g1.GetError() != "OK" || g1.GetValue() != "v1" {
		t.Fatalf("get key1 unexpected: resp=%+v err=%v", g1, err)
	}
	g2, err := r.GetRoute(ctx, key2)
	if err != nil || g2.GetError() != "OK" || g2.GetValue() != "v2" {
		t.Fatalf("get key2 unexpected: resp=%+v err=%v", g2, err)
	}
}

func TestShardRouterFailoverWithinGroup(t *testing.T) {
	wrongLeaderSvc := &testKVService{kv: map[string]*pb.KeyValue{}, alwaysWrongLeader: true}
	wrongAddr, stopWrong := startTestKVServer(t, wrongLeaderSvc)
	defer stopWrong()

	leaderSvc := &testKVService{kv: map[string]*pb.KeyValue{}}
	leaderAddr, stopLeader := startTestKVServer(t, leaderSvc)
	defer stopLeader()

	r, err := NewShardRouter(ShardingConfig{
		Groups: []RaftGroupConfig{
			{GroupID: 1, Replicas: []string{wrongAddr, leaderAddr}, LeaderIdx: 0},
		},
		VirtualNodeCount: 32,
		ConnectTimeout:   2 * time.Second,
		RequestTimeout:   1200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new shard router failed: %v", err)
	}
	defer r.Close()

	key := findKeyForGroup(t, r, 1, "failover")

	putResp, err := r.PutRouteWithTTL(context.Background(), key, "ok-after-failover", 0, 0)
	if err != nil {
		t.Fatalf("put with failover failed: %v", err)
	}
	if putResp.GetError() != "OK" {
		t.Fatalf("put error: %s", putResp.GetError())
	}

	if got := r.leaderCache[1]; got != leaderAddr {
		t.Fatalf("leader cache not updated, want=%s got=%s", leaderAddr, got)
	}
}


func TestShardRouterScanRoute(t *testing.T) {
	s1 := &testKVService{kv: map[string]*pb.KeyValue{}}
	a1, stop1 := startTestKVServer(t, s1)
	defer stop1()

	s2 := &testKVService{kv: map[string]*pb.KeyValue{}}
	a2, stop2 := startTestKVServer(t, s2)
	defer stop2()

	r, err := NewShardRouter(ShardingConfig{
		Groups: []RaftGroupConfig{
			{GroupID: 1, Replicas: []string{a1}, LeaderIdx: 0},
			{GroupID: 2, Replicas: []string{a2}, LeaderIdx: 0},
		},
		VirtualNodeCount: 64,
		ConnectTimeout:   2 * time.Second,
		RequestTimeout:   1200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new shard router failed: %v", err)
	}
	defer r.Close()

	ctx := context.Background()
	k1 := findKeyForGroup(t, r, 1, "scan-user")
	k2 := findKeyForGroup(t, r, 2, "scan-user")
	k3 := findKeyForGroup(t, r, 1, "scan-order")

	_, _ = r.PutRouteWithTTL(ctx, k1, "v1", 0, 0)
	_, _ = r.PutRouteWithTTL(ctx, k2, "v2", 0, 0)
	_, _ = r.PutRouteWithTTL(ctx, k3, "v3", 0, 0)

	items, err := r.ScanRoute(ctx, "scan-user", 0)
	if err != nil {
		t.Fatalf("scan route failed: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("scan route count mismatch, want=2 got=%d", len(items))
	}

	if !(strings.HasPrefix(items[0].GetKey(), "scan-user") && strings.HasPrefix(items[1].GetKey(), "scan-user")) {
		t.Fatalf("scan route returned unexpected keys: %+v", items)
	}

	limited, err := r.ScanRoute(ctx, "scan-", 1)
	if err != nil {
		t.Fatalf("scan route(limit) failed: %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("scan route(limit) count mismatch, want=1 got=%d", len(limited))
	}
}
