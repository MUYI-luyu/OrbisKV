package kv

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"
	"kvraft/pkg/sharding"
)

// Clerk 是 KVraft 的分布式客户端。
// 内部始终通过 ShardRouter 做路由——单 Raft 组自动退化为 1-group 路由。
type Clerk struct {
	router      *sharding.ShardRouter
	coordinator *TxCoordinator // 2PC 事务协调器
}

// backoffWithJitter 计算带抖动的指数退避延迟。
// attempt: 重试轮次(0-based), base: 基础延迟, maxDelay: 上限。
// 延迟 = min(base * 2^attempt, maxDelay) ± 25% 随机抖动，以分散惊群效应。
func backoffWithJitter(attempt int, base, maxDelay time.Duration) time.Duration {
	delay := base * (1 << attempt)
	if delay <= 0 || delay > maxDelay {
		delay = maxDelay
	}
	jitter := time.Duration(float64(delay) * 0.25 * (float64(time.Now().UnixNano()%1000)/1000.0*2 - 1))
	result := delay + time.Duration(int64(jitter))
	if result < 0 {
		return base
	}
	if result > maxDelay {
		return maxDelay
	}
	return result
}

const (
	retryWindow          = 10 * time.Second
	retryBackoffBase     = 8 * time.Millisecond
	retryBackoffMax      = 500 * time.Millisecond
	maxAttempts          = 80
	getAttemptTimeout    = 350 * time.Millisecond
	putAttemptTimeout    = 450 * time.Millisecond
	deleteAttemptTimeout = 450 * time.Millisecond
	scanAttemptTimeout   = 450 * time.Millisecond
)

// WatchSubscription 表示一个可取消的 Watch 订阅。
type WatchSubscription struct {
	Events <-chan *pb.WatchEvent
	cancel context.CancelFunc
}

// Cancel 主动取消订阅。
func (s *WatchSubscription) Cancel() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

// MakeClerk 创建单 Raft 组的 Clerk。内部使用 ShardRouter（1 group），
// 所有方法走统一的分片路由路径，不再区分 classic/sharded 代码分支。
func MakeClerk(servers []string) *Clerk {
	grpcAddrs := make([]string, len(servers))
	copy(grpcAddrs, servers)

	cfg := sharding.ShardingConfig{
		Groups: []sharding.RaftGroupConfig{{
			GroupID:   1,
			Replicas:  grpcAddrs,
			LeaderIdx: 0,
		}},
		VirtualNodeCount:  150,
		PreferredReplicas: len(grpcAddrs), // 单 group 必须尝试全部副本
		ConnectTimeout:    2 * time.Second,
		RequestTimeout:    1200 * time.Millisecond,
	}

	router, err := sharding.NewShardRouter(cfg)
	if err != nil {
		panic(fmt.Sprintf("MakeClerk: %v", err))
	}
	return &Clerk{router: router, coordinator: NewTxCoordinator(router)}
}

// MakeShardedClerk 创建多 Group 分片 Clerk。
func MakeShardedClerk(cfg sharding.ShardingConfig) (*Clerk, error) {
	router, err := sharding.NewShardRouter(cfg)
	if err != nil {
		return nil, err
	}
	return &Clerk{router: router, coordinator: NewTxCoordinator(router)}, nil
}

// Begin 开始一个分布式事务。
// RecoverTransaction completes a prepared transaction from its durable coordinator decision.
func (ck *Clerk) RecoverTransaction(txID string, participantGroupID int) Err {
	return ck.coordinator.RecoverTransaction(txID, participantGroupID)
}

func (ck *Clerk) Begin() *TxHandle {
	return ck.coordinator.Begin()
}

// Rollback 放弃事务，释放所有 Group 上的锁。
func (ck *Clerk) Rollback(h *TxHandle) {
	h.Rollback()
}

// mapPBErr 将 gRPC 响应中的错误字符串转为 Err 类型。
func mapPBErr(errText string) Err {
	switch errText {
	case string(OK), "":
		return OK
	case string(ErrNoKey):
		return ErrNoKey
	case string(ErrWrongLeader):
		return ErrWrongLeader
	case string(ErrVersion):
		return ErrVersion
	case string(ErrMaybe):
		return ErrMaybe
	case string(ErrWrongGroup):
		return ErrWrongGroup
	case string(ErrTxConflict):
		return ErrTxConflict
	case string(ErrTxNotFound):
		return ErrTxNotFound
	case string(ErrTxTimeout):
		return ErrTxTimeout
	default:
		if strings.Contains(strings.ToLower(errText), "unimplemented") {
			return ErrWrongLeader
		}
		if strings.Contains(strings.ToLower(errText), "wrong") && strings.Contains(strings.ToLower(errText), "leader") {
			return ErrWrongLeader
		}
		return ErrWrongLeader
	}
}

// Get 获取一个键的当前值、版本和过期时间（UnixNano 绝对时间）。如果键不存在，返回 ErrNoKey。
// 在面对所有其他错误时，它会不断重试。
func (ck *Clerk) Get(key string) (string, Tversion, int64, Err) {
	deadline := time.Now().Add(retryWindow)
	attempts := 0

	for {
		if attempts >= maxAttempts || time.Now().After(deadline) {
			return "", 0, 0, ErrWrongLeader
		}

		attemptCtx, cancel := context.WithDeadline(context.Background(), deadline)
		attemptCtx, cancel2 := context.WithTimeout(attemptCtx, getAttemptTimeout)
		resp, err := ck.router.GetRoute(attemptCtx, key)
		cancel2()
		cancel()
		attempts++

		if err != nil || resp == nil {
			time.Sleep(backoffWithJitter(attempts, retryBackoffBase, retryBackoffMax))
			continue
		}

		errCode := mapPBErr(resp.GetError())
		switch errCode {
		case OK:
			return resp.GetValue(), Tversion(resp.GetVersion()), resp.GetExpires(), OK
		case ErrNoKey:
			return "", 0, 0, ErrNoKey
		case ErrWrongGroup:
			ck.tryRefreshTopology()
			time.Sleep(backoffWithJitter(attempts, retryBackoffBase, retryBackoffMax))
		default:
			time.Sleep(backoffWithJitter(attempts, retryBackoffBase, retryBackoffMax))
		}
	}
}

// readVersionFast 快速从路由读取 key 的当前版本号。
// 用于 ErrVersion 时自动刷新，避免 Sleep 浪费吞吐。
func (ck *Clerk) readVersionFast(key string) Tversion {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	resp, err := ck.router.GetRoute(ctx, key)
	if err != nil || resp == nil {
		return 0
	}
	if mapPBErr(resp.GetError()) != OK {
		return 0
	}
	return Tversion(resp.GetVersion())
}

// doPut 是 Put 和 PutWithTTL 的底层实现。
//
// 语义约定（满足 MIT 6.5840 Lab 要求）：
//   - 首次 RPC 即收到 ErrVersion → 返回 ErrVersion（Put 确定未执行）
//   - 重试期间收到 ErrVersion → 自动刷新版本号并立即重试（最多 3 次），
//     耗尽后返回 ErrMaybe（Put 可能已被执行但响应丢失）
//   - 网络/Leader 错误 → 指数退避后重试
func (ck *Clerk) doPut(key string, value string, version Tversion, ttlSeconds int64) Err {
	deadline := time.Now().Add(retryWindow)
	attempts := 0
	rpcAttempted := false // 是否已发出过至少一次 RPC 调用
	autoRefreshLeft := 3  // ErrVersion 自动刷新版本号的次数上限

	for {
		if attempts >= maxAttempts || time.Now().After(deadline) {
			return ErrWrongLeader
		}

		attemptCtx, cancel := context.WithDeadline(context.Background(), deadline)
		attemptCtx, cancel2 := context.WithTimeout(attemptCtx, putAttemptTimeout)
		resp, err := ck.router.PutRouteWithTTL(attemptCtx, key, value, int64(version), ttlSeconds)
		cancel2()
		cancel()
		attempts++

		if err != nil || resp == nil {
			rpcAttempted = true
			time.Sleep(backoffWithJitter(attempts, retryBackoffBase, retryBackoffMax))
			continue
		}

		errCode := mapPBErr(resp.GetError())
		switch errCode {
		case OK, ErrNoKey:
			return errCode
		case ErrWrongGroup:
			// shard 已迁移：尝试刷新拓扑后重试
			ck.tryRefreshTopology()
			rpcAttempted = true
			time.Sleep(backoffWithJitter(attempts, retryBackoffBase, retryBackoffMax))
		case ErrVersion:
			if !rpcAttempted {
				// 首次 RPC 即版本冲突：Put 确定未执行
				return ErrVersion
			}
			// 重试期间的版本冲突：尝试自动刷新并立即重试（不 sleep）
			if autoRefreshLeft > 0 {
				autoRefreshLeft--
				if latestV := ck.readVersionFast(key); latestV > version {
					version = latestV
				}
				break // 跳出 switch，立即用新版本重试
			}
			// 自动刷新次数耗尽
			return ErrMaybe
		default:
			// WrongLeader / 其他错误
			rpcAttempted = true
			time.Sleep(backoffWithJitter(attempts, retryBackoffBase, retryBackoffMax))
		}
	}
}

// tryRefreshTopology 在收到 ErrWrongGroup 时尝试从 server 刷新拓扑。
// 当前简化实现：依赖 ShardRouter 内部的重试机制找到正确的 group。
// 完整实现需调用 GetClusterStatus 获取新拓扑后重建 router。
func (ck *Clerk) tryRefreshTopology() {
	// TODO: 调用 ck.router.RefreshTopologyFromCluster() 获取最新拓扑
	// 当前依赖 ShardRouter 的候选节点遍历自动找到正确的 group
}

// Put 仅当请求中的版本与服务器上该键的版本匹配时，才会使用值更新键。
func (ck *Clerk) Put(key string, value string, version Tversion) Err {
	return ck.doPut(key, value, version, 0)
}

// PutWithTTL 修改一个键值对，带有TTL支持。
func (ck *Clerk) PutWithTTL(key string, value string, version Tversion, ttlSeconds int64) Err {
	return ck.doPut(key, value, version, ttlSeconds)
}

// Delete 删除指定 key。
func (ck *Clerk) Delete(key string) Err {
	deadline := time.Now().Add(retryWindow)
	attempts := 0

	for {
		if attempts >= maxAttempts || time.Now().After(deadline) {
			return ErrWrongLeader
		}

		attemptCtx, cancel := context.WithDeadline(context.Background(), deadline)
		attemptCtx, cancel2 := context.WithTimeout(attemptCtx, deleteAttemptTimeout)
		resp, err := ck.router.DeleteRoute(attemptCtx, key)
		cancel2()
		cancel()
		attempts++

		if err != nil || resp == nil {
			time.Sleep(backoffWithJitter(attempts, retryBackoffBase, retryBackoffMax))
			continue
		}

		errCode := mapPBErr(resp.GetError())
		if errCode == OK || errCode == ErrNoKey {
			return errCode
		}
		if errCode == ErrWrongGroup {
			ck.tryRefreshTopology()
		}

		time.Sleep(backoffWithJitter(attempts, retryBackoffBase, retryBackoffMax))
	}
}

// Scan 按前缀扫描键值。
func (ck *Clerk) Scan(prefix string, limit int32) ([]*pb.KeyValue, Err) {
	deadline := time.Now().Add(retryWindow)
	attempts := 0

	for {
		if attempts >= maxAttempts || time.Now().After(deadline) {
			return nil, ErrWrongLeader
		}

		attemptCtx, cancel := context.WithDeadline(context.Background(), deadline)
		attemptCtx, cancel2 := context.WithTimeout(attemptCtx, scanAttemptTimeout)
		items, err := ck.router.ScanRoute(attemptCtx, prefix, limit)
		cancel2()
		cancel()
		attempts++

		if err == nil {
			return items, OK
		}

		time.Sleep(backoffWithJitter(attempts, retryBackoffBase, retryBackoffMax))
	}
}

// Watch 订阅指定 key 或 prefix 的变化。
// prefix=false 时仅监听一个 key；prefix=true 时监听指定前缀。
func (ck *Clerk) Watch(key string, prefix bool) (*WatchSubscription, error) {
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *pb.WatchEvent, 128)

	go func() {
		defer close(out)
		_ = ck.router.WatchRoute(ctx, key, prefix, func(groupID int, event *pb.WatchEvent) {
			if event == nil {
				return
			}
			ev := &pb.WatchEvent{
				WatchId:    event.GetWatchId(),
				Key:        event.GetKey(),
				OldValue:   event.GetOldValue(),
				NewValue:   event.GetNewValue(),
				NewVersion: event.GetNewVersion(),
				EventType:  fmt.Sprintf("group-%d:%s", groupID, event.GetEventType()),
			}
			select {
			case out <- ev:
			case <-ctx.Done():
			}
		})
	}()

	return &WatchSubscription{Events: out, cancel: cancel}, nil
}

// Close 释放 Clerk 持有的连接资源。
func (ck *Clerk) Close() {
	if ck.router != nil {
		ck.router.Close()
	}
}
