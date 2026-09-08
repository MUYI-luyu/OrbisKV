// Package kv 定义 KVraft 分布式键值存储的业务类型。
// 该包包含所有客户端/服务器之间的请求/响应数据结构，
// 以及错误码和版本号类型。
package kv

import (
	pb "kvraft/api/pb/kvraft/api/pb"
	"kvraft/pkg/raft"
	"kvraft/pkg/watch"
)

// Tversion 表示一个键的版本号。
type Tversion int

// Err 表示操作结果的错误码。
type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrVersion     Err = "ErrVersion"
	ErrMaybe       Err = "ErrMaybe"
	ErrWrongGroup  Err = "ErrWrongGroup" // shard 不属于本 group
	ErrTxConflict  Err = "ErrTxConflict" // prepare 时版本冲突或 lock 已被持有
	ErrTxNotFound  Err = "ErrTxNotFound" // txID 不存在
	ErrTxTimeout   Err = "ErrTxTimeout"  // prepare 超时
)

// GetArgs 是 Get 操作的参数。
type GetArgs struct {
	Key string
}

// GetReply 是 Get 操作的结果。
type GetReply struct {
	Value   string
	Version Tversion
	Expires int64
	Err     Err
}

// PutArgs 是 Put 操作的参数。
type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
	TTL     int64 // seconds; <=0 means no expiry
}

// PutReply 是 Put 操作的结果。
type PutReply struct {
	Err      Err
	OldValue string // 修改前的值（用于Watch事件）
}

// DeleteArgs 是 Delete 操作的参数。
type DeleteArgs struct {
	Key string
}

// DeleteReply 是 Delete 操作的结果。
type DeleteReply struct {
	Err      Err
	OldValue string
}

type CleanupShardKey struct {
	Key             string
	ExpectedVersion Tversion
}

type CleanupShardArgs struct {
	ShardID int
	Keys    []CleanupShardKey
}

type CleanupShardReply struct {
	Deleted int
	Err     Err
}

type SetShardStateArgs struct {
	ShardID        int
	State          pb.ShardState
	TargetGroup    int
	TopologyEpoch  int64
	TargetReplicas []string
}

type SetShardStateReply struct {
	Error string
}

// ScanArgs 是 Scan 操作的参数。
type ScanArgs struct {
	Prefix string
	Limit  int32
}

// ScanItem 是 Scan 操作返回的单个键值项。
type ScanItem struct {
	Key     string
	Value   string
	Version Tversion
	Expires int64
}

// ScanReply 是 Scan 操作的结果。
type ScanReply struct {
	Items []ScanItem
	Err   Err
}

// ExpireArgs 是过期键清理操作的参数。
type ExpireArgs struct {
	Keys   []string
	Cutoff int64
}

// ExpireReply 是过期键清理操作的结果。
type ExpireReply struct {
	ExpiredKeys      []string
	ExpiredOldValues map[string]string
	Err              Err
}

// OpCompleteListener 操作完成监听器接口
// 用于在 Raft 日志提交后回调，例如触发 Watch 事件
type OpCompleteListener interface {
	// OnOpComplete 在操作被 Raft 提交和应用后调用
	// req: 原始请求
	// result: 操作结果
	// index: Raft 日志索引
	OnOpComplete(req any, result any, index int64)
}

// ApplyLoopPerfStats 记录 applyLoop 的性能统计
type ApplyLoopPerfStats struct {
	BlockedNanos    int64
	ProcessNanos    int64
	IterationCount  int64
	BlockedAvgNanos float64
	ProcessAvgNanos float64
}

// RSMInterface 是复制状态机（RSM）的核心接口，供 KVServer 和 gRPC 层调用。
type RSMInterface interface {
	Submit(req any) (Err, any)
	SubmitLeaseReadWithMode(req any) (Err, any, bool)
	GetWatchManager() *watch.Manager
	RegisterOpCompleteListener(listener OpCompleteListener)
	GetState() (int, bool)
	GetLastApplied() int
	IsLeaderWithLease() bool
	Close()
	ApplyLoopPerfStatsSnapshot() ApplyLoopPerfStats
	RaftPerfStatsSnapshot() raft.RaftPerfStats
}

// ============ 2PC 事务类型 ============

// ReadKey 描述事务中读取的一个 key，带预期版本用于冲突校验。
type ReadKey struct {
	Key             string
	Value           string
	ExpectedVersion Tversion
}

// WriteKey 描述事务中要写入的一个 key。
type WriteKey struct {
	Key   string
	Value string
	// Version 是预期的当前版本（CAS）。0 表示 "key 必须不存在"。
	Version Tversion
	// IsDelete distinguishes transactional delete from an empty-value Put.
	IsDelete bool
}

// PrepareTxArgs 是 Phase-1 请求：校验读集 + 获取写锁。
type PrepareTxArgs struct {
	TxID                string
	ReadKeys            []ReadKey
	WriteKeys           []WriteKey
	TimeoutMs           int64
	CoordinatorGroupID  int
	ParticipantGroupIDs []int
}

// PrepareTxReply 是 Phase-1 响应。
type PrepareTxReply struct {
	Err Err
}

// CommitTxArgs 是 Phase-2 请求：原子应用写操作。
type CommitTxArgs struct {
	TxID      string
	WriteKeys []WriteKey
}

// CommitTxReply 是 Phase-2 响应。
type CommitTxReply struct {
	Err Err
}

// AbortTxArgs 释放已 prepare 事务持有的锁。
type AbortTxArgs struct {
	TxID string
}

// AbortTxReply 是 abort 响应。
type AbortTxReply struct {
	Err Err
}

type RecordTxDecisionArgs struct {
	TxID                string
	Decision            TxStatus
	ParticipantGroupIDs []int
}

type RecordTxDecisionReply struct{ Err Err }

// TxStatus 枚举事务可能的状态。
type TxStatus int

const (
	TxStatusNotFound TxStatus = iota
	TxStatusPrepared
	TxStatusCommitted
	TxStatusAborted
)

// ResolveTxStatusArgs 查询事务状态（用于锁恢复）。
type ResolveTxStatusArgs struct {
	TxID string
}

// ResolveTxStatusReply 返回事务状态，如果已 prepare 则返回其 write keys。
type ResolveTxStatusReply struct {
	Status              TxStatus
	PreparedAt          int64
	WriteKeys           []WriteKey
	Err                 Err
	CoordinatorGroupID  int
	ParticipantGroupIDs []int
}
