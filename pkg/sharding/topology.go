package sharding

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

// ShardTopology 定义 key → shard → group 的两级映射。
//
// 参考 Redis Cluster 的 hash slot 设计：固定数量的 shard（默认 1024），
// 每个 shard 有明确的所有者 group。key 通过 xxhash 取模映射到 shard，
// shard 再通过 ownership table 映射到 group。
//
// 与 ConsistentHash 方案的区别：
//   - 加新 group 时只需转移部分 shard，不改变 hash 函数，不引发全局重排
//   - shard 是迁移的最小单位，天然支持批量操作
//   - slice 索引 O(1)，比哈希环二分查找更快
type ShardTopology struct {
	mu           sync.RWMutex
	numShards    int              // 固定 shard 总数
	epoch        atomic.Int64     // 拓扑版本号，任何所有权变更后单调递增
	shardToGroup []int            // shardToGroup[shardID] = groupID
	groups       map[int][]string // groupID → replica gRPC 地址
}

// NewShardTopology 创建拓扑，shards 按 round-robin 均匀分配给各 group。
// numShards 必须为正整数，groups 必须非空。
func NewShardTopology(numShards int, groups map[int][]string) *ShardTopology {
	if numShards < 1 {
		numShards = 1024
	}

	// 收集并排序 group ID，保证 shard 分配确定性
	groupIDs := make([]int, 0, len(groups))
	for gid := range groups {
		groupIDs = append(groupIDs, gid)
	}
	sort.Ints(groupIDs)

	st := &ShardTopology{
		numShards:    numShards,
		shardToGroup: make([]int, numShards),
		groups:       make(map[int][]string, len(groups)),
	}
	st.epoch.Store(1)

	for gid, replicas := range groups {
		st.groups[gid] = append([]string(nil), replicas...)
	}

	// round-robin 分配 shard
	for i := 0; i < numShards; i++ {
		st.shardToGroup[i] = groupIDs[i%len(groupIDs)]
	}

	return st
}

// shardForKey 计算 key 的 shard ID。
// NewShardTopologyWithOwnership creates a topology from an explicit owner table.
func NewShardTopologyWithOwnership(numShards int, groups map[int][]string, owners []int, epoch int64) *ShardTopology {
	if numShards < 1 || len(groups) == 0 || len(owners) != numShards {
		return nil
	}
	st := NewShardTopology(numShards, groups)
	for _, gid := range owners {
		if _, ok := groups[gid]; !ok {
			return nil
		}
	}
	st.mu.Lock()
	st.shardToGroup = append([]int(nil), owners...)
	st.mu.Unlock()
	if epoch > 0 {
		st.epoch.Store(epoch)
	}
	return st
}

func (st *ShardTopology) shardForKey(key string) int {
	return int(xxhash.Sum64String(key) % uint64(st.numShards))
}

// Resolve 返回 key 所属的 group ID。
func (st *ShardTopology) Resolve(key string) int {
	gid, _ := st.ResolveWithEpoch(key)
	return gid
}

// ResolveWithEpoch 返回 group ID 和当前拓扑版本号。
// 调用方可将 epoch 放入请求中，服务端校验拓扑是否过期。
func (st *ShardTopology) ResolveWithEpoch(key string) (int, int64) {
	shard := st.shardForKey(key)
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.shardToGroup[shard], st.epoch.Load()
}

// GetEpoch 返回当前拓扑版本号。
func (st *ShardTopology) GetEpoch() int64 {
	return st.epoch.Load()
}

// ShardForKey 返回 key 的 shard ID（公开方法，供迁移等场景使用）。
func (st *ShardTopology) ShardForKey(key string) int {
	return st.shardForKey(key)
}

// NumShards 返回 shard 总数。
func (st *ShardTopology) NumShards() int {
	return st.numShards
}

// GroupReplicas 返回 group 的副本 gRPC 地址列表。
// Ownership returns a copy of the current shard owner table.
func (st *ShardTopology) Ownership() []int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return append([]int(nil), st.shardToGroup...)
}

// GroupForShard returns the current owner of a shard.
func (st *ShardTopology) GroupForShard(shard int) (int, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	if shard < 0 || shard >= len(st.shardToGroup) {
		return 0, false
	}
	return st.shardToGroup[shard], true
}

// PlanAddGroup computes the post-expansion ownership and shards to migrate.
func (st *ShardTopology) PlanAddGroup(gid int, replicas []string) ([]int, []int) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	if _, exists := st.groups[gid]; exists || len(replicas) == 0 {
		return nil, nil
	}
	owners := append([]int(nil), st.shardToGroup...)
	counts := make(map[int]int, len(st.groups)+1)
	for _, owner := range owners {
		counts[owner]++
	}
	counts[gid] = 0
	target := st.numShards / (len(st.groups) + 1)
	ids := st.allGroupIDsLocked()
	ids = append(ids, gid)
	sort.Ints(ids)
	moved := make([]int, 0)
	for _, from := range ids {
		if from == gid {
			continue
		}
		for counts[from] > target && counts[gid] < target {
			for shard := len(owners) - 1; shard >= 0; shard-- {
				if owners[shard] != from {
					continue
				}
				owners[shard] = gid
				counts[from]--
				counts[gid]++
				moved = append(moved, shard)
				break
			}
		}
	}
	return owners, moved
}

func (st *ShardTopology) GroupReplicas(gid int) []string {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return append([]string(nil), st.groups[gid]...)
}

// GroupIDs 返回所有 group ID（升序）。
func (st *ShardTopology) GroupIDs() []int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	ids := make([]int, 0, len(st.groups))
	for gid := range st.groups {
		ids = append(ids, gid)
	}
	sort.Ints(ids)
	return ids
}

// MoveShard 将 shard 所有权转移到目标 group（迁移用）。
// 调用方负责保证迁移期间的数据一致性（双写/锁）。
func (st *ShardTopology) MoveShard(shard int, toGroup int) bool {
	if shard < 0 || shard >= st.numShards {
		return false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if _, ok := st.groups[toGroup]; !ok {
		return false
	}
	st.shardToGroup[shard] = toGroup
	st.epoch.Add(1)
	return true
}

// AddGroup 动态增加一个新 group 并重新均衡 shard（将已有 group 的部分 shard 转移给新 group）。
// 返回被转移到新 group 的 shard 列表。
func (st *ShardTopology) AddGroup(gid int, replicas []string) []int {
	st.mu.Lock()
	defer st.mu.Unlock()

	if _, exists := st.groups[gid]; exists {
		return nil
	}
	st.groups[gid] = append([]string(nil), replicas...)

	// 重新均衡：每个 group 应拥有 numShards / len(groups) 个 shard
	newCount := len(st.groups)
	targetPerGroup := st.numShards / newCount

	// 统计当前分布
	owned := make(map[int][]int) // groupID → shard list
	for s := 0; s < st.numShards; s++ {
		g := st.shardToGroup[s]
		owned[g] = append(owned[g], s)
	}

	// 选出需要转移的 shard（从超额 group 中取）
	var moved []int
	for _, g := range st.allGroupIDsLocked() {
		if g == gid {
			continue
		}
		shards := owned[g]
		for len(shards) > targetPerGroup && len(owned[gid]) < targetPerGroup {
			// 取最后一个 shard 转移给新 group
			s := shards[len(shards)-1]
			shards = shards[:len(shards)-1]
			st.shardToGroup[s] = gid
			owned[gid] = append(owned[gid], s)
			moved = append(moved, s)
		}
	}
	if len(moved) > 0 {
		st.epoch.Add(1)
	}
	return moved
}

func (st *ShardTopology) allGroupIDsLocked() []int {
	ids := make([]int, 0, len(st.groups))
	for gid := range st.groups {
		ids = append(ids, gid)
	}
	sort.Ints(ids)
	return ids
}
