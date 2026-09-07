package kv

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"
	"kvraft/pkg/watch"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// grpcKVService 将现有 KVServer 能力暴露为 gRPC 接口，
// SetShardState/GetShardStates 已合并到 KVService。
type grpcKVService struct {
	pb.UnimplementedKVServiceServer
	kv *KVServer
}

// SetShardState 实现 KVServiceServer。由迁移协调器调用来修改 shard 状态。
func (s *grpcKVService) SetShardState(ctx context.Context, req *pb.SetShardStateRequest) (*pb.SetShardStateResponse, error) {
	err := s.kv.SetShardState(int(req.GetShardId()), req.GetState(), int(req.GetTargetGroup()), req.GetTopologyEpoch(), req.GetTargetReplicas())
	if err != nil {
		return &pb.SetShardStateResponse{Error: err.Error()}, nil
	}
	return &pb.SetShardStateResponse{}, nil
}

// GetShardStates 实现 KVServiceServer。
func (s *grpcKVService) GetShardStates(ctx context.Context, req *pb.GetShardStatesRequest) (*pb.GetShardStatesResponse, error) {
	entries, epoch := s.kv.GetShardStates()
	return &pb.GetShardStatesResponse{States: entries, TopologyEpoch: epoch}, nil
}

func (s *grpcKVService) CleanupShard(ctx context.Context, req *pb.CleanupShardRequest) (*pb.CleanupShardResponse, error) {
	meta, ok := s.kv.shardMgr.GetShardState(int(req.GetShardId()))
	if !ok || meta.state != pb.ShardState_ABSENT {
		return &pb.CleanupShardResponse{Error: errReply(ErrWrongGroup)}, nil
	}
	keys := make([]CleanupShardKey, 0, len(req.GetKeys()))
	for _, key := range req.GetKeys() {
		if key != nil {
			keys = append(keys, CleanupShardKey{Key: key.GetKey(), ExpectedVersion: Tversion(key.GetExpectedVersion())})
		}
	}
	err, ret := s.kv.rsm.Submit(&CleanupShardArgs{ShardID: int(req.GetShardId()), Keys: keys})
	if err != OK {
		return &pb.CleanupShardResponse{Error: errReply(err)}, nil
	}
	reply, ok := ret.(CleanupShardReply)
	if !ok {
		return &pb.CleanupShardResponse{Error: "ErrInternal"}, nil
	}
	return &pb.CleanupShardResponse{Error: errReply(reply.Err), Deleted: int32(reply.Deleted)}, nil
}

// 根据已有的 Raft RPC 地址，自动生成一个用于 gRPC 服务的监听地址
func grpcAddrFromRPC(addr string) string {
	if explicit := strings.TrimSpace(os.Getenv("GRPC_LISTEN")); explicit != "" {
		return explicit
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return addr
	}
	return net.JoinHostPort(host, strconv.Itoa(p+1000))
}

func errReply(e Err) string {
	return string(e)
}

func (s *grpcKVService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if s.kv.killed() {
		s.kv.stats.RecordFailure()
		return &pb.GetResponse{Error: errReply(ErrWrongLeader)}, nil
	}

	err, ret, leaseHit := s.kv.rsm.SubmitLeaseReadWithMode(&GetArgs{Key: req.GetKey()})
	if err != OK {
		s.kv.stats.RecordFailure()
		s.kv.stats.RecordLeaseFallback()
		return &pb.GetResponse{Error: errReply(err)}, nil
	}

	reply, ok := ret.(GetReply)
	if !ok {
		s.kv.stats.RecordFailure()
		return &pb.GetResponse{Error: "ErrInternal"}, nil
	}

	s.kv.stats.RecordRead()
	if s.kv.leaseStatEnabled {
		if leaseHit {
			s.kv.stats.RecordLeaseHit()
		} else {
			s.kv.stats.RecordLeaseFallback()
		}
	}

	return &pb.GetResponse{
		Value:   reply.Value,
		Version: int64(reply.Version),
		Error:   errReply(reply.Err),
		Expires: reply.Expires,
	}, nil
}

func (s *grpcKVService) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if s.kv.killed() {
		return &pb.PutResponse{Error: errReply(ErrWrongLeader)}, nil
	}

	args := &PutArgs{Key: req.GetKey(), Value: req.GetValue(), Version: Tversion(req.GetVersion()), TTL: req.GetTtlSeconds()}
	err, ret := s.kv.rsm.Submit(args)
	if err != OK {
		return &pb.PutResponse{Error: errReply(err)}, nil
	}

	reply, ok := ret.(PutReply)
	if !ok {
		return &pb.PutResponse{Error: "ErrInternal"}, nil
	}

	return &pb.PutResponse{Error: errReply(reply.Err)}, nil
}

func (s *grpcKVService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if s.kv.killed() {
		return &pb.DeleteResponse{Error: errReply(ErrWrongLeader)}, nil
	}

	err, ret := s.kv.rsm.Submit(&DeleteArgs{Key: req.GetKey()})
	if err != OK {
		return &pb.DeleteResponse{Error: errReply(err)}, nil
	}

	reply, ok := ret.(DeleteReply)
	if !ok {
		return &pb.DeleteResponse{Error: "ErrInternal"}, nil
	}

	return &pb.DeleteResponse{Error: errReply(reply.Err)}, nil
}

func (s *grpcKVService) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	if s.kv.killed() {
		return &pb.ScanResponse{Error: errReply(ErrWrongLeader)}, nil
	}

	err, ret := s.kv.rsm.Submit(&ScanArgs{Prefix: req.GetPrefix(), Limit: req.GetLimit()})
	if err != OK {
		return &pb.ScanResponse{Error: errReply(err)}, nil
	}

	reply, ok := ret.(ScanReply)
	if !ok {
		return &pb.ScanResponse{Error: "ErrInternal"}, nil
	}

	items := make([]*pb.KeyValue, 0, len(reply.Items))
	for _, it := range reply.Items {
		items = append(items, &pb.KeyValue{Key: it.Key, Value: it.Value, Version: int64(it.Version), Expires: it.Expires})
	}

	return &pb.ScanResponse{Items: items, Error: errReply(reply.Err)}, nil
}

func (s *grpcKVService) Watch(stream grpc.BidiStreamingServer[pb.WatchRequest, pb.WatchEvent]) error {
	watchMgr := s.kv.rsm.GetWatchManager()
	if watchMgr == nil {
		return nil
	}

	var currentID int64
	var currentStop chan struct{}

	stopCurrent := func() {
		if currentStop != nil {
			close(currentStop)
			currentStop = nil
		}
		if currentID != 0 {
			_ = watchMgr.Unsubscribe(currentID)
			currentID = 0
		}
	}
	defer stopCurrent()

	for {
		req, err := stream.Recv()
		if err != nil {
			stopCurrent()
			return nil
		}

		switch t := req.GetRequestType().(type) {
		case *pb.WatchRequest_Create:
			stopCurrent()

			w, subErr := watchMgr.Subscribe(t.Create.GetKey(), t.Create.GetPrefix())
			if subErr != nil {
				_ = stream.Send(&pb.WatchEvent{EventType: "error", NewValue: subErr.Error()})
				continue
			}
			currentID = w.ID

			stopCh := make(chan struct{})
			currentStop = stopCh
			go func(src <-chan watch.Event, watchID int64, stop <-chan struct{}) {
				for {
					select {
					case <-stop:
						return
					case e, ok := <-src:
						if !ok {
							return
						}
						ev := &pb.WatchEvent{
							WatchId:    watchID,
							Key:        e.Key,
							OldValue:   e.OldValue,
							NewValue:   e.NewValue,
							NewVersion: e.NewVersion,
							EventType:  strings.ToLower(e.EventType),
						}
						if sendErr := stream.Send(ev); sendErr != nil {
							return
						}
					}
				}
			}(w.Channel, w.ID, stopCh)
		case *pb.WatchRequest_Cancel:
			id := t.Cancel.GetWatchId()
			if id == 0 {
				id = currentID
			}
			if id != 0 {
				_ = watchMgr.Unsubscribe(id)
			}
			if currentStop != nil {
				close(currentStop)
				currentStop = nil
			}
			currentID = 0
			return nil
		}
	}
}

// convertReadKeysFromProto 将 proto ReadKey 转为内部 ReadKey 类型。
func convertReadKeysFromProto(pbKeys []*pb.ReadKey) []ReadKey {
	if pbKeys == nil {
		return nil
	}
	keys := make([]ReadKey, 0, len(pbKeys))
	for _, k := range pbKeys {
		if k != nil {
			keys = append(keys, ReadKey{Key: k.GetKey(), ExpectedVersion: Tversion(k.GetExpectedVersion())})
		}
	}
	return keys
}

// convertWriteKeysFromProto 将 proto WriteKey 转为内部 WriteKey 类型。
func convertWriteKeysFromProto(pbKeys []*pb.WriteKey) []WriteKey {
	if pbKeys == nil {
		return nil
	}
	keys := make([]WriteKey, 0, len(pbKeys))
	for _, k := range pbKeys {
		if k != nil {
			keys = append(keys, WriteKey{Key: k.GetKey(), Value: k.GetValue(), Version: Tversion(k.GetVersion()), IsDelete: k.GetIsDelete()})
		}
	}
	return keys
}

// convertWriteKeysToProto 将内部 WriteKey 转为 proto 格式。
func convertWriteKeysToProto(keys []WriteKey) []*pb.WriteKey {
	if keys == nil {
		return nil
	}
	pbKeys := make([]*pb.WriteKey, 0, len(keys))
	for _, k := range keys {
		pbKeys = append(pbKeys, &pb.WriteKey{Key: k.Key, Value: k.Value, Version: int64(k.Version), IsDelete: k.IsDelete})
	}
	return pbKeys
}

func (s *grpcKVService) PrepareTx(ctx context.Context, req *pb.PrepareTxRequest) (*pb.PrepareTxResponse, error) {
	if s.kv.killed() {
		return &pb.PrepareTxResponse{Error: errReply(ErrWrongLeader)}, nil
	}

	args := &PrepareTxArgs{
		TxID:      req.GetTxId(),
		ReadKeys:  convertReadKeysFromProto(req.GetReadKeys()),
		WriteKeys: convertWriteKeysFromProto(req.GetWriteKeys()),
		TimeoutMs: req.GetTimeoutMs(),
	}
	err, ret := s.kv.rsm.Submit(args)
	if err != OK {
		return &pb.PrepareTxResponse{Error: errReply(err)}, nil
	}

	reply, ok := ret.(PrepareTxReply)
	if !ok {
		return &pb.PrepareTxResponse{Error: "ErrInternal"}, nil
	}

	return &pb.PrepareTxResponse{Error: errReply(reply.Err)}, nil
}

func (s *grpcKVService) CommitTx(ctx context.Context, req *pb.CommitTxRequest) (*pb.CommitTxResponse, error) {
	if s.kv.killed() {
		return &pb.CommitTxResponse{Error: errReply(ErrWrongLeader)}, nil
	}

	args := &CommitTxArgs{
		TxID:      req.GetTxId(),
		WriteKeys: convertWriteKeysFromProto(req.GetWriteKeys()),
	}
	err, ret := s.kv.rsm.Submit(args)
	if err != OK {
		return &pb.CommitTxResponse{Error: errReply(err)}, nil
	}

	reply, ok := ret.(CommitTxReply)
	if !ok {
		return &pb.CommitTxResponse{Error: "ErrInternal"}, nil
	}

	return &pb.CommitTxResponse{Error: errReply(reply.Err)}, nil
}

func (s *grpcKVService) AbortTx(ctx context.Context, req *pb.AbortTxRequest) (*pb.AbortTxResponse, error) {
	if s.kv.killed() {
		return &pb.AbortTxResponse{Error: errReply(ErrWrongLeader)}, nil
	}

	args := &AbortTxArgs{TxID: req.GetTxId()}
	err, ret := s.kv.rsm.Submit(args)
	if err != OK {
		return &pb.AbortTxResponse{Error: errReply(err)}, nil
	}

	reply, ok := ret.(AbortTxReply)
	if !ok {
		return &pb.AbortTxResponse{Error: "ErrInternal"}, nil
	}

	return &pb.AbortTxResponse{Error: errReply(reply.Err)}, nil
}

func (s *grpcKVService) ResolveTxStatus(ctx context.Context, req *pb.ResolveTxStatusRequest) (*pb.ResolveTxStatusResponse, error) {
	if s.kv.killed() {
		return &pb.ResolveTxStatusResponse{Error: errReply(ErrWrongLeader)}, nil
	}

	args := &ResolveTxStatusArgs{TxID: req.GetTxId()}
	// 注意：ResolveTxStatus 有副作用（超时自动 abort），必须走 Raft 共识路径
	err, ret := s.kv.rsm.Submit(args)
	if err != OK {
		return &pb.ResolveTxStatusResponse{Error: errReply(err)}, nil
	}

	reply, ok := ret.(ResolveTxStatusReply)
	if !ok {
		return &pb.ResolveTxStatusResponse{Error: "ErrInternal"}, nil
	}

	statusStr := "NOT_FOUND"
	switch reply.Status {
	case TxStatusPrepared:
		statusStr = "PREPARED"
	case TxStatusCommitted:
		statusStr = "COMMITTED"
	case TxStatusAborted:
		statusStr = "ABORTED"
	}

	return &pb.ResolveTxStatusResponse{
		Status:     statusStr,
		PreparedAt: reply.PreparedAt,
		WriteKeys:  convertWriteKeysToProto(reply.WriteKeys),
		Error:      errReply(reply.Err),
	}, nil
}

func (s *grpcKVService) GetClusterStatus(ctx context.Context, req *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	term, isLeader := s.kv.rsm.GetState()
	lastApplied := s.kv.rsm.GetLastApplied()
	node := &pb.NodeStatus{Id: int32(s.kv.me), Address: grpcAddrFromRPC(s.kv.address), IsLeader: isLeader, IsAlive: !s.kv.killed()}
	return &pb.ClusterStatusResponse{
		LeaderId:      fmt.Sprintf("node-%d", s.kv.me),
		Nodes:         []*pb.NodeStatus{node},
		CurrentTerm:   int64(term),
		LastApplied:   int64(lastApplied),
		GroupId:       int32(s.kv.groupID),
		TopologyEpoch: 0, // 服务端暂无 topology 对象，单 group 场景下 epoch 恒为 1
	}, nil
}

func StartGRPCServer(kv *KVServer, rpcAddr string) (*grpc.Server, net.Listener) {
	grpcAddr := grpcAddrFromRPC(rpcAddr)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("start grpc listener %s failed: %v", grpcAddr, err)
	}

	// Unary interceptor: 每个响应 header 注入 group ID 和拓扑版本号
	topoInterceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		// 通过 trailer 返回拓扑元数据（不影响业务响应体）
		_ = grpc.SetHeader(ctx, metadata.Pairs(
			"x-group-id", strconv.Itoa(kv.groupID),
			"x-topology-epoch", strconv.FormatInt(kv.TopologyEpoch(), 10),
		))
		return resp, err
	}

	gs := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    2 * time.Minute,
			Timeout: 20 * time.Second,
		}),
		grpc.UnaryInterceptor(topoInterceptor),
	)

	svc := &grpcKVService{kv: kv}
	pb.RegisterKVServiceServer(gs, svc)
	go func() {
		if serveErr := gs.Serve(lis); serveErr != nil {
			log.Printf("grpc serve stopped on %s: %v", grpcAddr, serveErr)
		}
	}()

	time.Sleep(20 * time.Millisecond)
	kv.grpcSrv = gs
	kv.grpcLn = lis
	return gs, lis
}
