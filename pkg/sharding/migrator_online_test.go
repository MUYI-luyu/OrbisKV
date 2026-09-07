package sharding

import (
	"context"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "kvraft/api/pb/kvraft/api/pb"
)

type onlineKVService struct {
	pb.UnimplementedKVServiceServer
	mu             sync.Mutex
	kv             map[string]*pb.KeyValue
	states         map[int32]*pb.ShardStateEntry
	failPut        bool
	failCleanup    bool
	targetReplicas []string
	scanHook       func()
}

func newOnlineKVService() *onlineKVService {
	return &onlineKVService{kv: map[string]*pb.KeyValue{}, states: map[int32]*pb.ShardStateEntry{}}
}
func (s *onlineKVService) Get(_ context.Context, r *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.kv[r.GetKey()]
	if v == nil {
		return &pb.GetResponse{Error: "ErrNoKey"}, nil
	}
	return &pb.GetResponse{Error: "OK", Value: v.GetValue(), Version: v.GetVersion()}, nil
}
func (s *onlineKVService) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	s.mu.Lock()
	if s.failPut {
		s.mu.Unlock()
		return &pb.PutResponse{Error: "ErrInternal"}, nil
	}
	cur := s.kv[r.GetKey()]
	if cur == nil {
		if r.GetVersion() != 0 {
			s.mu.Unlock()
			return &pb.PutResponse{Error: "ErrNoKey"}, nil
		}
		s.kv[r.GetKey()] = &pb.KeyValue{Key: r.GetKey(), Value: r.GetValue(), Version: 1}
	} else {
		if cur.GetVersion() != r.GetVersion() {
			s.mu.Unlock()
			return &pb.PutResponse{Error: "ErrVersion"}, nil
		}
		s.kv[r.GetKey()] = &pb.KeyValue{Key: r.GetKey(), Value: r.GetValue(), Version: cur.GetVersion() + 1}
	}
	var replicas []string
	for _, st := range s.states {
		if st.GetState() == pb.ShardState_MIGRATING {
			replicas = append([]string(nil), s.targetReplicas...)
			break
		}
	}
	s.mu.Unlock()
	if len(replicas) > 0 {
		fctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		conn, err := grpc.DialContext(fctx, replicas[0], grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			return &pb.PutResponse{Error: "ErrWrongGroup"}, nil
		}
		defer conn.Close()
		resp, err := pb.NewKVServiceClient(conn).Put(fctx, &pb.PutRequest{Key: r.GetKey(), Value: r.GetValue(), Version: r.GetVersion()})
		if err != nil || resp.GetError() != "OK" {
			return &pb.PutResponse{Error: "ErrWrongGroup"}, nil
		}
	}
	return &pb.PutResponse{Error: "OK"}, nil
}
func (s *onlineKVService) Delete(_ context.Context, r *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kv[r.GetKey()] == nil {
		return &pb.DeleteResponse{Error: "ErrNoKey"}, nil
	}
	delete(s.kv, r.GetKey())
	return &pb.DeleteResponse{Error: "OK"}, nil
}
func (s *onlineKVService) Scan(_ context.Context, r *pb.ScanRequest) (*pb.ScanResponse, error) {
	s.mu.Lock()
	keys := make([]string, 0)
	for k := range s.kv {
		if r.GetPrefix() == "" || strings.HasPrefix(k, r.GetPrefix()) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := make([]*pb.KeyValue, 0, len(keys))
	for _, k := range keys {
		v := s.kv[k]
		out = append(out, &pb.KeyValue{Key: k, Value: v.GetValue(), Version: v.GetVersion()})
	}
	hook := s.scanHook
	s.scanHook = nil
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return &pb.ScanResponse{Items: out, Error: "OK"}, nil
}
func (s *onlineKVService) GetClusterStatus(context.Context, *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	return &pb.ClusterStatusResponse{Nodes: []*pb.NodeStatus{{IsLeader: true}}, CurrentTerm: 1}, nil
}
func (s *onlineKVService) SetShardState(_ context.Context, r *pb.SetShardStateRequest) (*pb.SetShardStateResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[r.GetShardId()] = &pb.ShardStateEntry{ShardId: r.GetShardId(), State: r.GetState(), TargetGroup: r.GetTargetGroup()}
	s.targetReplicas = append([]string(nil), r.GetTargetReplicas()...)
	return &pb.SetShardStateResponse{}, nil
}
func (s *onlineKVService) GetShardStates(context.Context, *pb.GetShardStatesRequest) (*pb.GetShardStatesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*pb.ShardStateEntry, 0, 1024)
	for i := 0; i < 1024; i++ {
		if e := s.states[int32(i)]; e != nil {
			out = append(out, e)
		} else {
			out = append(out, &pb.ShardStateEntry{ShardId: int32(i), State: pb.ShardState_OWNED})
		}
	}
	return &pb.GetShardStatesResponse{States: out, TopologyEpoch: 1}, nil
}
func (s *onlineKVService) CleanupShard(_ context.Context, r *pb.CleanupShardRequest) (*pb.CleanupShardResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failCleanup {
		return &pb.CleanupShardResponse{Error: "ErrInternal"}, nil
	}
	n := 0
	for _, k := range r.GetKeys() {
		if v := s.kv[k.GetKey()]; v != nil && v.GetVersion() == k.GetExpectedVersion() {
			delete(s.kv, k.GetKey())
			n++
		}
	}
	return &pb.CleanupShardResponse{Error: "OK", Deleted: int32(n)}, nil
}
func startOnlineServer(t *testing.T, s *onlineKVService) (string, func()) {
	lis, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	gs := grpc.NewServer()
	pb.RegisterKVServiceServer(gs, s)
	go gs.Serve(lis)
	return lis.Addr().String(), func() { gs.Stop(); lis.Close() }
}

func onlineRouters(t *testing.T, srcAddr, dstAddr string) (*ShardRouter, *ShardRouter) {
	src, e := NewShardRouter(ShardingConfig{Groups: []RaftGroupConfig{{GroupID: 1, Replicas: []string{srcAddr}}, {GroupID: 2, Replicas: []string{dstAddr}}}, NumShards: 1024})
	if e != nil {
		t.Fatal(e)
	}
	dst, e := NewShardRouter(ShardingConfig{Groups: []RaftGroupConfig{{GroupID: 2, Replicas: []string{dstAddr}}}, NumShards: 1024})
	if e != nil {
		t.Fatal(e)
	}
	return src, dst
}

func TestMigrateShardOnlineEndToEnd(t *testing.T) {
	srcSvc, dstSvc := newOnlineKVService(), newOnlineKVService()
	sa, ss := startOnlineServer(t, srcSvc)
	defer ss()
	da, ds := startOnlineServer(t, dstSvc)
	defer ds()
	src, dst := onlineRouters(t, sa, da)
	defer src.Close()
	defer dst.Close()
	key := "online-e2e-key"
	if _, e := src.PutToGroup(context.Background(), 1, key, "value", 0); e != nil {
		t.Fatal(e)
	}
	shard := shardKey(key, 1024)
	if e := NewMigrator(src, dst).MigrateShardOnline(context.Background(), shard, 1, 2, ""); e != nil {
		t.Fatal(e)
	}
	srcSvc.mu.Lock()
	_, srcOK := srcSvc.kv[key]
	srcState := srcSvc.states[int32(shard)]
	srcSvc.mu.Unlock()
	dstSvc.mu.Lock()
	_, dstOK := dstSvc.kv[key]
	dstState := dstSvc.states[int32(shard)]
	dstSvc.mu.Unlock()
	if srcOK || !dstOK || srcState.GetState() != pb.ShardState_ABSENT || dstState.GetState() != pb.ShardState_OWNED {
		t.Fatalf("migration state/data mismatch src=%v dst=%v", srcState, dstState)
	}
	if got := src.Resolve(key); got != 2 {
		t.Fatalf("MoveShard not applied: %d", got)
	}
}

func TestMigrateShardOnlineConcurrentWriteIsDoubleWritten(t *testing.T) {
	srcSvc, dstSvc := newOnlineKVService(), newOnlineKVService()
	sa, ss := startOnlineServer(t, srcSvc)
	defer ss()
	da, ds := startOnlineServer(t, dstSvc)
	defer ds()
	src, dst := onlineRouters(t, sa, da)
	defer src.Close()
	defer dst.Close()
	key := "online-concurrent-write"
	shard := shardKey(key, 1024)
	if _, err := srcSvc.SetShardState(context.Background(), &pb.SetShardStateRequest{ShardId: int32(shard), State: pb.ShardState_MIGRATING, TargetGroup: 2, TargetReplicas: []string{da}}); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutToGroup(context.Background(), 1, key, "during", 0); err != nil {
		t.Fatal(err)
	}
	srcGot, err := src.GetFromGroup(context.Background(), 1, key)
	if err != nil {
		t.Fatal(err)
	}
	dstGot, err := dst.GetFromGroup(context.Background(), 2, key)
	if err != nil {
		t.Fatal(err)
	}
	if srcGot.GetError() != "OK" || dstGot.GetError() != "OK" || srcGot.GetValue() != "during" || dstGot.GetValue() != "during" || srcGot.GetVersion() != dstGot.GetVersion() {
		t.Fatalf("double write mismatch source=%+v target=%+v", srcGot, dstGot)
	}
}

func TestMigrateShardOnlineBulkCopyRollback(t *testing.T) {
	srcSvc, dstSvc := newOnlineKVService(), newOnlineKVService()
	dstSvc.failPut = true
	sa, ss := startOnlineServer(t, srcSvc)
	defer ss()
	da, ds := startOnlineServer(t, dstSvc)
	defer ds()
	src, dst := onlineRouters(t, sa, da)
	defer src.Close()
	defer dst.Close()
	key := "online-bulk-fail"
	_, _ = src.PutToGroup(context.Background(), 1, key, "value", 0)
	err := NewMigrator(src, dst).MigrateShardOnline(context.Background(), shardKey(key, 1024), 1, 2, "")
	if err == nil {
		t.Fatal("expected bulk copy failure")
	}
	srcSvc.mu.Lock()
	st := srcSvc.states[int32(shardKey(key, 1024))]
	srcSvc.mu.Unlock()
	if st.GetState() != pb.ShardState_OWNED {
		t.Fatalf("source state not rolled back: %v", st)
	}
	srcValue, _ := src.GetFromGroup(context.Background(), 1, key)
	dstValue, _ := dst.GetFromGroup(context.Background(), 2, key)
	if srcValue.GetError() != "OK" || srcValue.GetValue() != "value" || dstValue.GetError() != "ErrNoKey" {
		t.Fatalf("bulk rollback data mismatch source=%+v target=%+v", srcValue, dstValue)
	}
}

func TestMigrateShardOnlineCleanupFailure(t *testing.T) {
	srcSvc, dstSvc := newOnlineKVService(), newOnlineKVService()
	srcSvc.failCleanup = true
	sa, ss := startOnlineServer(t, srcSvc)
	defer ss()
	da, ds := startOnlineServer(t, dstSvc)
	defer ds()
	src, dst := onlineRouters(t, sa, da)
	defer src.Close()
	defer dst.Close()
	key := "online-cleanup-fail"
	_, _ = src.PutToGroup(context.Background(), 1, key, "value", 0)
	err := NewMigrator(src, dst).MigrateShardOnline(context.Background(), shardKey(key, 1024), 1, 2, "")
	if err == nil {
		t.Fatal("expected cleanup failure")
	}
	srcSvc.mu.Lock()
	srcState := srcSvc.states[int32(shardKey(key, 1024))]
	_, sourceRetained := srcSvc.kv[key]
	srcSvc.mu.Unlock()
	dstSvc.mu.Lock()
	dstState := dstSvc.states[int32(shardKey(key, 1024))]
	targetValue, targetRetained := dstSvc.kv[key]
	dstSvc.mu.Unlock()
	if srcState.GetState() != pb.ShardState_ABSENT {
		t.Fatalf("source should remain ABSENT after cleanup failure: %v", srcState)
	}
	if dstState.GetState() != pb.ShardState_OWNED || !targetRetained || targetValue.GetValue() != "value" || !sourceRetained {
		t.Fatalf("cleanup failure lost authoritative data: sourceRetained=%v target=%+v state=%+v", sourceRetained, targetValue, dstState)
	}
	if got := src.Resolve(key); got != 1 {
		t.Fatalf("MoveShard must not run after cleanup failure: owner=%d", got)
	}
}

var _ = time.Second
