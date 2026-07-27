package sharding

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MigrationPlanItem 描述单个 key 的迁移计划。
type MigrationPlanItem struct {
	Key         string
	Value       string
	Version     int64
	SourceGroup int
	TargetGroup int
}

// MigrationStats 描述一次迁移执行结果。
type MigrationStats struct {
	Planned          int
	Migrated         int
	Skipped          int
	DeletedFromSrc   int
	Failed           int
	LastErrorMessage string
}

// Migrator 用于在不同分片拓扑之间迁移数据。
type Migrator struct {
	source *ShardRouter
	target *ShardRouter
}

// NewMigrator 创建迁移器。
func NewMigrator(source *ShardRouter, target *ShardRouter) *Migrator {
	return &Migrator{source: source, target: target}
}

// BuildPlan 扫描 source 中的数据，并计算目标拓扑下需要迁移的 key。
func (m *Migrator) BuildPlan(ctx context.Context, prefix string, limit int) ([]MigrationPlanItem, error) {
	if m == nil || m.source == nil || m.target == nil {
		return nil, fmt.Errorf("migrator/source/target is nil")
	}

	plan := make([]MigrationPlanItem, 0)
	for _, sourceGID := range m.source.GroupIDs() {
		items, err := m.source.ScanGroup(ctx, sourceGID, prefix, 0)
		if err != nil {
			return nil, err
		}

		for _, item := range items {
			if item == nil {
				continue
			}

			targetGID := m.target.Resolve(item.GetKey())
			if targetGID < 0 {
				return nil, fmt.Errorf("target router has no group for key %q", item.GetKey())
			}

			if targetGID == sourceGID {
				continue
			}

			plan = append(plan, MigrationPlanItem{
				Key:         item.GetKey(),
				Value:       item.GetValue(),
				Version:     item.GetVersion(),
				SourceGroup: sourceGID,
				TargetGroup: targetGID,
			})
			if limit > 0 && len(plan) >= limit {
				sort.Slice(plan, func(i, j int) bool {
					if plan[i].SourceGroup == plan[j].SourceGroup {
						return plan[i].Key < plan[j].Key
					}
					return plan[i].SourceGroup < plan[j].SourceGroup
				})
				return plan, nil
			}
		}
	}

	sort.Slice(plan, func(i, j int) bool {
		if plan[i].SourceGroup == plan[j].SourceGroup {
			return plan[i].Key < plan[j].Key
		}
		return plan[i].SourceGroup < plan[j].SourceGroup
	})
	return plan, nil
}

func migrationPutVersion(getResp *pb.GetResponse) int64 {
	if getResp == nil {
		return 0
	}
	if getResp.GetError() == "ErrNoKey" {
		return 0
	}
	if getResp.GetError() == "OK" {
		return getResp.GetVersion()
	}
	return 0
}

func migrationPutAccepted(resp *pb.PutResponse) bool {
	if resp == nil {
		return false
	}
	return resp.GetError() == "OK"
}

func migrationDeleteAccepted(resp *pb.DeleteResponse) bool {
	if resp == nil {
		return false
	}
	return resp.GetError() == "OK" || resp.GetError() == "ErrNoKey"
}

func migrationGetAccepted(resp *pb.GetResponse) bool {
	if resp == nil {
		return false
	}
	errText := resp.GetError()
	return errText == "OK" || errText == "ErrNoKey"
}

func migrationValueEquals(resp *pb.GetResponse, item MigrationPlanItem) bool {
	if resp == nil || resp.GetError() != "OK" {
		return false
	}
	return resp.GetValue() == item.Value
}

func (m *Migrator) fetchGroupValue(ctx context.Context, router *ShardRouter, gid int, key string) (*pb.GetResponse, error) {
	getCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	resp, err := router.GetFromGroup(getCtx, gid, key)
	if err != nil {
		return nil, err
	}
	if !migrationGetAccepted(resp) {
		return nil, fmt.Errorf("unexpected get response: %v", resp.GetError())
	}
	return resp, nil
}

func (m *Migrator) rollbackTarget(ctx context.Context, item MigrationPlanItem, before *pb.GetResponse) error {
	if before == nil {
		return nil
	}

	if before.GetError() == "ErrNoKey" {
		delCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		defer cancel()
		delResp, delErr := m.target.DeleteFromGroup(delCtx, item.TargetGroup, item.Key)
		if delErr != nil {
			return delErr
		}
		if !migrationDeleteAccepted(delResp) {
			return fmt.Errorf("rollback delete target failed: %s", delResp.GetError())
		}
		return nil
	}

	current, err := m.fetchGroupValue(ctx, m.target, item.TargetGroup, item.Key)
	if err != nil {
		return err
	}
	if current.GetError() != "OK" {
		return fmt.Errorf("rollback get current target failed: %s", current.GetError())
	}

	putCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	putResp, putErr := m.target.PutToGroup(putCtx, item.TargetGroup, item.Key, before.GetValue(), current.GetVersion())
	if putErr != nil {
		return putErr
	}
	if !migrationPutAccepted(putResp) {
		return fmt.Errorf("rollback restore target failed: %s", putResp.GetError())
	}
	return nil
}

func (m *Migrator) deleteSourceWithVerify(ctx context.Context, item MigrationPlanItem) error {
	delCtx, cancelDel := context.WithTimeout(ctx, 1500*time.Millisecond)
	delResp, delErr := m.source.DeleteFromGroup(delCtx, item.SourceGroup, item.Key)
	cancelDel()
	if delErr != nil {
		return delErr
	}
	if !migrationDeleteAccepted(delResp) {
		return fmt.Errorf("delete source failed: %s", delResp.GetError())
	}

	srcAfter, err := m.fetchGroupValue(ctx, m.source, item.SourceGroup, item.Key)
	if err != nil {
		return err
	}
	if srcAfter.GetError() != "ErrNoKey" {
		return fmt.Errorf("source key still exists after delete")
	}
	return nil
}

// ExecutePlan 执行迁移计划。
// deleteSource=true 时，目标写入成功后删除源数据。
func (m *Migrator) ExecutePlan(ctx context.Context, plan []MigrationPlanItem, deleteSource bool) (MigrationStats, error) {
	stats := MigrationStats{Planned: len(plan)}
	if len(plan) == 0 {
		return stats, nil
	}

	if m == nil || m.source == nil || m.target == nil {
		return stats, fmt.Errorf("migrator/source/target is nil")
	}

	for _, item := range plan {
		if ctx.Err() != nil {
			stats.LastErrorMessage = ctx.Err().Error()
			return stats, ctx.Err()
		}

		sourceBefore, srcErr := m.fetchGroupValue(ctx, m.source, item.SourceGroup, item.Key)
		if srcErr != nil {
			stats.Failed++
			stats.LastErrorMessage = srcErr.Error()
			continue
		}
		if sourceBefore.GetError() == "ErrNoKey" {
			stats.Skipped++
			continue
		}
		if sourceBefore.GetValue() != item.Value {
			stats.Failed++
			stats.LastErrorMessage = fmt.Sprintf("source value changed for key %s, plan stale", item.Key)
			continue
		}

		targetBefore, getErr := m.fetchGroupValue(ctx, m.target, item.TargetGroup, item.Key)
		if getErr != nil {
			stats.Failed++
			stats.LastErrorMessage = getErr.Error()
			continue
		}

		if migrationValueEquals(targetBefore, item) {
			if deleteSource {
				if err := m.deleteSourceWithVerify(ctx, item); err != nil {
					stats.Failed++
					stats.LastErrorMessage = err.Error()
					continue
				}
				stats.DeletedFromSrc++
				stats.Migrated++
				continue
			}
			stats.Skipped++
			continue
		}

		putVersion := migrationPutVersion(targetBefore)
		putCtx, cancelPut := context.WithTimeout(ctx, 1500*time.Millisecond)
		putResp, putErr := m.target.PutToGroup(putCtx, item.TargetGroup, item.Key, item.Value, putVersion)
		cancelPut()
		if putErr != nil || !migrationPutAccepted(putResp) {
			stats.Failed++
			if putErr != nil {
				stats.LastErrorMessage = putErr.Error()
			} else if putResp != nil {
				stats.LastErrorMessage = putResp.GetError()
			}
			continue
		}

		targetAfter, verifyErr := m.fetchGroupValue(ctx, m.target, item.TargetGroup, item.Key)
		if verifyErr != nil || !migrationValueEquals(targetAfter, item) {
			_ = m.rollbackTarget(ctx, item, targetBefore)
			stats.Failed++
			if verifyErr != nil {
				stats.LastErrorMessage = verifyErr.Error()
			} else {
				stats.LastErrorMessage = "target verify failed after put"
			}
			continue
		}

		stats.Migrated++
		if !deleteSource {
			continue
		}

		if err := m.deleteSourceWithVerify(ctx, item); err != nil {
			rollbackErr := m.rollbackTarget(ctx, item, targetBefore)
			stats.Failed++
			if rollbackErr != nil {
				stats.LastErrorMessage = fmt.Sprintf("%v; rollback failed: %v", err, rollbackErr)
			} else {
				stats.LastErrorMessage = fmt.Sprintf("%v; rollback applied", err)
			}
			continue
		}
		stats.DeletedFromSrc++
	}

	if stats.Failed > 0 {
		return stats, fmt.Errorf("migration finished with %d failures", stats.Failed)
	}
	return stats, nil
}

// connectToKVService 建立到指定 group 中第一个可达 replica 的 KVService 连接。
func connectToKVService(ctx context.Context, router *ShardRouter, gid int) (pb.KVServiceClient, *grpc.ClientConn, error) {
	addrs := router.groupReplicaAddrs(gid)
	if len(addrs) == 0 {
		return nil, nil, fmt.Errorf("group %d has no replicas", gid)
	}
	for _, addr := range addrs {
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			continue
		}
		return pb.NewKVServiceClient(conn), conn, nil
	}
	return nil, nil, fmt.Errorf("no reachable replica for group %d", gid)
}

// shardKey 计算 key 所属的 shard ID。
func shardKey(key string, numShards int) int {
	return int(xxhash.Sum64String(key) % uint64(numShards))
}

// groupReplicaAddrs 获取 group 的可用 replica 地址。
func (r *ShardRouter) groupReplicaAddrs(gid int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.groupsByID[gid]
	if !ok {
		return nil
	}
	return append([]string(nil), g.Replicas...)
}

// MigrateShardOnline 在线迁移一个 shard，迁移期间不停止写入（6 阶段）。
//
// 流程：
//   Phase 1: target ← IMPORTING
//   Phase 2: source ← MIGRATING(target)    ← 双写开始
//   Phase 3: 批量同步存量数据
//   Phase 4: target ← OWNED                ← 双写结束
//   Phase 5: source ← ABSENT
//   Phase 6: 清理源数据
func (m *Migrator) MigrateShardOnline(ctx context.Context, shardID int, sourceGID, targetGID int, prefix string) error {
	if m == nil || m.source == nil || m.target == nil {
		return fmt.Errorf("migrator is nil")
	}

	// 连接到源 group 和目标 group 的 server（用于调用 SetShardState）
	srcCtx, srcCancel := context.WithTimeout(ctx, 5*time.Second)
	src, srcConn, err := connectToKVService(srcCtx, m.source, sourceGID)
	srcCancel()
	if err != nil {
		return fmt.Errorf("connect to source group %d: %w", sourceGID, err)
	}
	defer srcConn.Close()

	tgtCtx, tgtCancel := context.WithTimeout(ctx, 5*time.Second)
	tgt, tgtConn, err := connectToKVService(tgtCtx, m.target, targetGID)
	tgtCancel()
	if err != nil {
		return fmt.Errorf("connect to target group %d: %w", targetGID, err)
	}
	defer tgtConn.Close()

	baseEpoch := m.source.TopologyEpoch()
	nextEpoch := baseEpoch + 1

	// Phase 1: Target ← IMPORTING（准备接收）
	log.Printf("[migrate] Phase 1: shard=%d target=%d ← IMPORTING", shardID, targetGID)
	if _, err := tgt.SetShardState(ctx, &pb.SetShardStateRequest{
		ShardId: int32(shardID), State: pb.ShardState_IMPORTING,
		TargetGroup: int32(sourceGID), TopologyEpoch: nextEpoch,
	}); err != nil {
		return fmt.Errorf("phase 1 target.IMPORTING failed: %w", err)
	}
	nextEpoch++

	// Phase 2: Source ← MIGRATING(target)（双写开始）
	log.Printf("[migrate] Phase 2: shard=%d source=%d ← MIGRATING → target=%d", shardID, sourceGID, targetGID)
	// 获取 target group 的 replica 地址列表供双写转发使用
	targetReplicas := m.target.groupReplicaAddrs(targetGID)
	if _, err := src.SetShardState(ctx, &pb.SetShardStateRequest{
		ShardId: int32(shardID), State: pb.ShardState_MIGRATING,
		TargetGroup: int32(targetGID), TopologyEpoch: nextEpoch,
	}); err != nil {
		// 回滚 Phase 1
		tgt.SetShardState(ctx, &pb.SetShardStateRequest{
			ShardId: int32(shardID), State: pb.ShardState_OWNED, TopologyEpoch: nextEpoch + 1,
		})
		return fmt.Errorf("phase 2 source.MIGRATING failed: %w", err)
	}
	_ = targetReplicas // replicas are already configured on the source server
	nextEpoch++

	// Phase 3: 批量同步存量数据
	log.Printf("[migrate] Phase 3: shard=%d bulk copy data", shardID)
	if err := m.bulkCopyShard(ctx, shardID, sourceGID, targetGID, prefix); err != nil {
		return fmt.Errorf("phase 3 bulk copy failed: %w", err)
	}

	// Phase 4: Target ← OWNED（双写结束，target 成为正式 owner）
	log.Printf("[migrate] Phase 4: shard=%d target=%d ← OWNED", shardID, targetGID)
	if _, err := tgt.SetShardState(ctx, &pb.SetShardStateRequest{
		ShardId: int32(shardID), State: pb.ShardState_OWNED, TopologyEpoch: nextEpoch,
	}); err != nil {
		return fmt.Errorf("phase 4 target.OWNED failed: %w", err)
	}
	nextEpoch++

	// Phase 5: Source ← ABSENT（停止服务该 shard）
	log.Printf("[migrate] Phase 5: shard=%d source=%d ← ABSENT", shardID, sourceGID)
	if _, err := src.SetShardState(ctx, &pb.SetShardStateRequest{
		ShardId: int32(shardID), State: pb.ShardState_ABSENT, TopologyEpoch: nextEpoch,
	}); err != nil {
		return fmt.Errorf("phase 5 source.ABSENT failed: %w", err)
	}
	nextEpoch++

	// Phase 6: 清理源数据
	log.Printf("[migrate] Phase 6: shard=%d clean source=%d data", shardID, sourceGID)
	if err := m.cleanShardData(ctx, shardID, sourceGID, prefix); err != nil {
		log.Printf("[migrate] Phase 6 warning: clean source data failed: %v (data will be orphaned)", err)
	}

	log.Printf("[migrate] shard=%d migration complete: %d → %d", shardID, sourceGID, targetGID)
	return nil
}

// bulkCopyShard 从源 group 扫描属于指定 shard 的 key，批量写入目标 group。
// 注意：使用单次全量扫描（limit=0），避免 BadgerDB Prefix 模式下游标分页的语义错误。
// 对于超大规模数据集，可扩展为 Seek-based 的增量扫描。
func (m *Migrator) bulkCopyShard(ctx context.Context, shardID, sourceGID, targetGID int, prefix string) error {
	totalCopied := 0
	numShards := m.source.TopologyNumShards()

	items, err := m.source.ScanGroup(ctx, sourceGID, "", 0)
	if err != nil {
		return fmt.Errorf("scan source failed: %w", err)
	}

	for _, item := range items {
		if item == nil {
			continue
		}
		key := item.GetKey()
		// 只迁移属于目标 shard 的 key
		if shardKey(key, numShards) != shardID {
			continue
		}
		// 可选 prefix 过滤
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		if _, err := m.target.PutToGroup(ctx, targetGID, key, item.GetValue(), item.GetVersion()); err != nil {
			return fmt.Errorf("put to target failed key=%s: %w", key, err)
		}
		totalCopied++
	}
	log.Printf("[migrate] bulk copied %d keys for shard %d", totalCopied, shardID)
	return nil
}

// cleanShardData 清理源 group 中属于指定 shard 的数据。
// 使用单次全量扫描（limit=0），避免 BadgerDB Prefix 模式下游标分页的语义错误。
func (m *Migrator) cleanShardData(ctx context.Context, shardID, sourceGID int, prefix string) error {
	totalDeleted := 0
	numShards := m.source.TopologyNumShards()

	items, err := m.source.ScanGroup(ctx, sourceGID, "", 0)
	if err != nil {
		return err
	}

	for _, item := range items {
		if item == nil {
			continue
		}
		key := item.GetKey()
		if shardKey(key, numShards) != shardID {
			continue
		}
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}
		if _, err := m.source.DeleteFromGroup(ctx, sourceGID, key); err != nil {
			log.Printf("[migrate] clean: delete %s failed: %v", key, err)
			continue
		}
		totalDeleted++
	}
	log.Printf("[migrate] cleaned %d keys from source group %d", totalDeleted, sourceGID)
	return nil
}

// TopologyNumShards 返回 router 中 topology 的 shard 总数。
func (r *ShardRouter) TopologyNumShards() int {
	if r.topology == nil {
		return 1024
	}
	return r.topology.NumShards()
}
