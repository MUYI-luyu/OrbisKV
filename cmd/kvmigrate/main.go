package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kvraft/pkg/sharding"
)

// kvmigrate 使用方法（中文速览）:
//
// 1) 最常用（显式指定源/目标配置）
//    go run ./cmd/kvmigrate \
//      -source-config ./data/cluster/sharding.json \
//      -target-config ./data/cluster/sharding-next.json
//
// 2) 只看迁移计划，不执行写入（推荐先 dry-run）
//    go run ./cmd/kvmigrate \
//      -source-config ./data/cluster/sharding.json \
//      -target-config ./data/cluster/sharding-next.json \
//      -dry-run
//
// 3) 仅迁移某个前缀，并限制条数
//    go run ./cmd/kvmigrate \
//      -source-config ./data/cluster/sharding.json \
//      -target-config ./data/cluster/sharding-next.json \
//      -prefix user: -limit 1000
//
// 4) 迁移完成后删除源数据（谨慎使用）
//    go run ./cmd/kvmigrate \
//      -source-config ./data/cluster/sharding.json \
//      -target-config ./data/cluster/sharding-next.json \
//      -delete-source
//
// 5) 自动读取运行时配置（无需参数）
//    直接执行: go run ./cmd/kvmigrate
//    将优先读取 data/cluster/runtime.env 里的:
//      SHARDING_CONFIG      (source-config)
//      SHARDING_NEXT_CONFIG (target-config)
//
// 6) 在线迁移指定 shard
//    go run ./cmd/kvmigrate -online -shard 12 -source-group 1 -target-group 2 \
//      -source-config ./data/cluster/sharding.json \
//      -target-config ./data/cluster/sharding-next.json

type jsonGroup struct {
	GroupID   int      `json:"group_id"`
	Replicas  []string `json:"replicas"`
	LeaderIdx int      `json:"leader_idx"`
}

type jsonShardingConfig struct {
	Groups            []jsonGroup `json:"groups"`
	VirtualNodeCount  int         `json:"virtual_node_count"`
	ConnectTimeoutMS  int         `json:"connect_timeout_ms"`
	RequestTimeoutMS  int         `json:"request_timeout_ms"`
	PreferredReplicas int         `json:"preferred_replicas"`
	NumShards         int         `json:"num_shards"`
	ShardToGroup      []int       `json:"shard_to_group,omitempty"`
	TopologyEpoch     int64       `json:"topology_epoch,omitempty"`
}

func loadRuntimeMetadata() map[string]string {
	meta := map[string]string{}
	path := filepath.Join("data", "cluster", "runtime.env")
	raw, err := os.ReadFile(path)
	if err != nil {
		return meta
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		meta[strings.TrimSpace(line[:idx])] = strings.TrimSpace(line[idx+1:])
	}
	return meta
}

func loadConfig(path string) (sharding.ShardingConfig, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return sharding.ShardingConfig{}, err
	}

	var jc jsonShardingConfig
	if err := json.Unmarshal(content, &jc); err != nil {
		return sharding.ShardingConfig{}, err
	}

	cfg := sharding.ShardingConfig{
		VirtualNodeCount:  jc.VirtualNodeCount,
		PreferredReplicas: jc.PreferredReplicas,
		NumShards:         jc.NumShards,
		ShardToGroup:      append([]int(nil), jc.ShardToGroup...),
		TopologyEpoch:     jc.TopologyEpoch,
	}
	if jc.ConnectTimeoutMS > 0 {
		cfg.ConnectTimeout = time.Duration(jc.ConnectTimeoutMS) * time.Millisecond
	}
	if jc.RequestTimeoutMS > 0 {
		cfg.RequestTimeout = time.Duration(jc.RequestTimeoutMS) * time.Millisecond
	}

	cfg.Groups = make([]sharding.RaftGroupConfig, 0, len(jc.Groups))
	for _, g := range jc.Groups {
		cfg.Groups = append(cfg.Groups, sharding.RaftGroupConfig{
			GroupID:   g.GroupID,
			Replicas:  append([]string(nil), g.Replicas...),
			LeaderIdx: g.LeaderIdx,
		})
	}
	return cfg, nil
}

func main() {
	sourceCfgPath := flag.String("source-config", "", "source sharding config json path")
	targetCfgPath := flag.String("target-config", "", "target sharding config json path")
	prefix := flag.String("prefix", "", "migrate keys with this prefix only")
	limit := flag.Int("limit", 0, "max number of keys in migration plan (0 means no limit)")
	dryRun := flag.Bool("dry-run", false, "only build and print migration plan")
	deleteSource := flag.Bool("delete-source", false, "delete source keys after successful copy")
	timeoutSec := flag.Int("timeout-sec", 30, "migration timeout in seconds")
	online := flag.Bool("online", false, "use the existing online shard migration protocol")
	shardID := flag.Int("shard", -1, "shard id for online migration")
	sourceGroup := flag.Int("source-group", -1, "source group id for online migration")
	targetGroup := flag.Int("target-group", -1, "target group id for online migration")
	expandGroup := flag.Int("expand-group", -1, "add a group and migrate planned shards")
	expandReplicas := flag.String("expand-replicas", "", "comma-separated gRPC replicas for the new group")
	outputConfig := flag.String("output-config", "", "write the explicit post-expansion topology JSON")

	flag.Parse()
	meta := loadRuntimeMetadata()

	if strings.TrimSpace(*sourceCfgPath) == "" {
		if p := strings.TrimSpace(meta["SHARDING_CONFIG"]); p != "" {
			*sourceCfgPath = p
		}
	}
	if strings.TrimSpace(*targetCfgPath) == "" {
		if p := strings.TrimSpace(meta["SHARDING_NEXT_CONFIG"]); p != "" {
			*targetCfgPath = p
		}
	}
	if strings.TrimSpace(*targetCfgPath) == "" && strings.TrimSpace(*sourceCfgPath) != "" {
		*targetCfgPath = *sourceCfgPath
	}

	if *sourceCfgPath == "" || *targetCfgPath == "" {
		fmt.Println("usage:")
		fmt.Println("  go run ./cmd/kvmigrate -source-config ./source.json -target-config ./target.json [-prefix p] [-limit n] [--dry-run] [--delete-source]")
		fmt.Println("hint:")
		fmt.Println("  直接执行时会优先读取 data/cluster/runtime.env 中的 SHARDING_CONFIG / SHARDING_NEXT_CONFIG")
		os.Exit(2)
	}
	if *online && (*shardID < 0 || *sourceGroup < 0 || *targetGroup < 0) {
		fmt.Fprintln(os.Stderr, "online migration requires -shard, -source-group and -target-group")
		os.Exit(2)
	}

	sourceCfg, err := loadConfig(*sourceCfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load source config failed: %v\n", err)
		os.Exit(1)
	}
	targetCfg, err := loadConfig(*targetCfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load target config failed: %v\n", err)
		os.Exit(1)
	}

	sourceRouter, err := sharding.NewShardRouter(sourceCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create source router failed: %v\n", err)
		os.Exit(1)
	}
	defer sourceRouter.Close()

	targetRouter, err := sharding.NewShardRouter(targetCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create target router failed: %v\n", err)
		os.Exit(1)
	}
	defer targetRouter.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
	defer cancel()

	if *expandGroup >= 0 {
		if strings.TrimSpace(*expandReplicas) == "" || strings.TrimSpace(*outputConfig) == "" {
			fmt.Fprintln(os.Stderr, "expansion requires -expand-replicas and -output-config")
			os.Exit(2)
		}
		if containsGroup(sourceCfg, *expandGroup) {
			fmt.Fprintf(os.Stderr, "group %d already exists\n", *expandGroup)
			os.Exit(2)
		}
		replicas := strings.Split(*expandReplicas, ",")
		for i := range replicas {
			replicas[i] = strings.TrimSpace(replicas[i])
		}
		owners, moved := sourceRouter.PlanAddGroup(*expandGroup, replicas)
		if len(owners) == 0 {
			fmt.Fprintln(os.Stderr, "unable to plan expansion")
			os.Exit(1)
		}
		expandedCfg := sourceCfg
		expandedCfg.NumShards = sourceRouter.TopologyNumShards()
		expandedCfg.Groups = append(append([]sharding.RaftGroupConfig(nil), sourceCfg.Groups...), sharding.RaftGroupConfig{GroupID: *expandGroup, Replicas: replicas})
		expandedCfg.ShardToGroup = owners
		expandedCfg.TopologyEpoch = sourceRouter.TopologyEpoch() + 1
		expandedRouter, err := sharding.NewShardRouter(expandedCfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create expanded router failed: %v\n", err)
			os.Exit(1)
		}
		defer expandedRouter.Close()
		expander := sharding.NewMigrator(sourceRouter, expandedRouter)
		for _, shard := range moved {
			sourceGroup, ok := sourceRouter.GroupForShard(shard)
			if !ok {
				fmt.Fprintf(os.Stderr, "shard %d has no source group\n", shard)
				os.Exit(1)
			}
			if err := expander.MigrateShardOnline(ctx, shard, sourceGroup, *expandGroup, *prefix); err != nil {
				fmt.Fprintf(os.Stderr, "expand shard %d failed: %v\n", shard, err)
				os.Exit(1)
			}
		}
		if err := writeConfig(*outputConfig, expandedCfg); err != nil {
			fmt.Fprintf(os.Stderr, "write expanded config failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("expansion done: group=%d moved_shards=%d config=%s\n", *expandGroup, len(moved), *outputConfig)
		return
	}

	migrator := sharding.NewMigrator(sourceRouter, targetRouter)
	if *online {
		if *sourceGroup == *targetGroup {
			fmt.Fprintln(os.Stderr, "online migration requires different source and target groups")
			os.Exit(2)
		}
		if *shardID >= sourceRouter.TopologyNumShards() {
			fmt.Fprintf(os.Stderr, "shard %d out of range [0,%d)\n", *shardID, sourceRouter.TopologyNumShards())
			os.Exit(2)
		}
		if !containsGroup(sourceCfg, *sourceGroup) || !containsGroup(targetCfg, *targetGroup) {
			fmt.Fprintln(os.Stderr, "source/target group is not present in the corresponding config")
			os.Exit(2)
		}
		if *dryRun {
			fmt.Printf("online migration dry-run: shard=%d source=%d target=%d\n", *shardID, *sourceGroup, *targetGroup)
			return
		}
		if err := migrator.MigrateShardOnline(ctx, *shardID, *sourceGroup, *targetGroup, *prefix); err != nil {
			fmt.Fprintf(os.Stderr, "online migration failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("online migration done")
		return
	}
	plan, err := migrator.BuildPlan(ctx, *prefix, *limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build migration plan failed: %v\n", err)
		os.Exit(1)
	}

	if *sourceCfgPath == *targetCfgPath {
		fmt.Println("提示: source 和 target 配置相同，本次迁移计划通常为空（用于连通性与流程验证）。")
	}

	fmt.Printf("plan size: %d\n", len(plan))
	for i, item := range plan {
		if i >= 20 {
			fmt.Printf("... (%d more)\n", len(plan)-20)
			break
		}
		fmt.Printf("[%03d] key=%s src=%d dst=%d version=%d\n", i+1, item.Key, item.SourceGroup, item.TargetGroup, item.Version)
	}

	if *dryRun {
		fmt.Println("dry-run mode: no data changed")
		return
	}

	stats, execErr := migrator.ExecutePlan(ctx, plan, *deleteSource)
	fmt.Printf("migration stats: %+v\n", stats)
	if execErr != nil {
		fmt.Fprintf(os.Stderr, "migration finished with error: %v\n", execErr)
		os.Exit(1)
	}

	fmt.Println("migration done")
}

func writeConfig(path string, cfg sharding.ShardingConfig) error {
	groups := make([]jsonGroup, 0, len(cfg.Groups))
	for _, g := range cfg.Groups {
		groups = append(groups, jsonGroup{GroupID: g.GroupID, Replicas: append([]string(nil), g.Replicas...), LeaderIdx: g.LeaderIdx})
	}
	data, err := json.MarshalIndent(jsonShardingConfig{Groups: groups, VirtualNodeCount: cfg.VirtualNodeCount, ConnectTimeoutMS: int(cfg.ConnectTimeout / time.Millisecond), RequestTimeoutMS: int(cfg.RequestTimeout / time.Millisecond), PreferredReplicas: cfg.PreferredReplicas, NumShards: cfg.NumShards, ShardToGroup: cfg.ShardToGroup, TopologyEpoch: cfg.TopologyEpoch}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

func containsGroup(cfg sharding.ShardingConfig, gid int) bool {
	for _, group := range cfg.Groups {
		if group.GroupID == gid {
			return true
		}
	}
	return false
}
