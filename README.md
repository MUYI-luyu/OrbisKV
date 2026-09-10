# OrbisKV

> 基于自研 Raft 协议实现的分布式强一致性 KV 存储系统，支持水平分片、在线迁移与跨分片事务。

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev/)
[![gRPC](https://img.shields.io/badge/gRPC-1.79-244c5a?logo=google)](https://grpc.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://docs.docker.com/compose/)
[![License](https://img.shields.io/badge/license-MIT-blue)](https://opensource.org/licenses/MIT)

---

## 核心特性

- **自研 Raft 共识** — 选举、日志复制、快照压缩及崩溃恢复。增量持久化与异步流水线解耦磁盘 I/O 与共识路径，**租约读**绕过共识降低读延迟。
- **跨分片 2PC 事务** — 零外部依赖（无 TSO/MVCC），事务决策经 Raft 共识持久化。乐观读校验 + 悲观写锁。CAS 批量写与事务标记在单个 BadgerDB 事务中原子落盘，Raft 日志重放安全。
- **哈希槽分片路由** — 固定 1024 个哈希槽（类 Redis Cluster），key 经两级映射（key→shard→group）定位到 group，数组索引 O(1) 路由，支持水平扩展与负载均衡。
- **在线安全迁移** — 6 阶段 Shard 状态机（OWNED → MIGRATING → IMPORTING → ABSENT），迁移期间以双写保证源和目标一致性（CP 语义），业务写入零中断。
- **CAS 版本控制** — 乐观锁并发模型，Put 操作校验版本号，规避分布式环境下的丢失更新。
- **Watch 事件订阅** — 基于 gRPC 双向流实现 Key/Prefix 级变更推送，Leader 感知自动重连。
- **TTL 过期治理** — 被动失效检测 + 最小堆主动扫描，精准清理过期键。
- **全链路可观测** — 内置 Prometheus `/metrics` 端点暴露 QPS 与 Raft 运行状态，支持 pprof 火焰图。

---

## 快速开始

### 环境要求

- Linux / macOS
- Go 1.25+

### 构建

```bash
git clone https://github.com/MUYI-luyu/OrbisKV.git
cd OrbisKV
go build ./...
```

### 启动集群

**方式一：脚本（本地开发）**

```bash
# 单 Raft 组（3 副本）
bash scripts/run_cluster.sh --arch node-ring --servers 3 --clean

# 多 Group 分片（3 Group × 3 Replica = 9 节点）
bash scripts/run_cluster.sh --arch group-ring --groups 3 --replicas 3 --clean
```

**方式二：Docker Compose（一键部署 + 监控）**

```bash
# 启动 3 节点集群 + Prometheus + Grafana
docker compose -f deployments/docker-compose.yml up -d

# 跑压测
docker compose -f deployments/docker-compose.yml --profile benchmark up benchmark

# 停止
docker compose -f deployments/docker-compose.yml down
```

| 服务 | 端口 | 说明 |
|------|------|------|
| OrbisKV 节点 (×3) | 6000-6002 (gRPC) / 8001-8003 (REST) | Raft 集群 |
| Prometheus | 9090 | 指标采集 |
| Grafana | 3000 (admin/admin) | 可视化面板 |

### 使用客户端

```go
import "kvraft/pkg/kv"

// 单 Group 模式
ck := kv.MakeClerk([]string{"127.0.0.1:15000", "127.0.0.1:15001", "127.0.0.1:15002"})
ck.Put("key", "value", 0)        // Create
val, ver, _, _ := ck.Get("key")  // Read
ck.Put("key", "newval", ver)     // Update (CAS)
ck.Delete("key")                 // Delete

// 跨分片事务
h := ck.Begin()
h.Put("alice", "40", 1)          // 缓冲写
h.Put("bob",   "60", 1)
if err := h.Commit(); err != OK { // 2PC：并行 Prepare → 并行 Commit
    h.Rollback()
}
```

或使用 CLI：

```bash
go run cmd/kvcli/main.go                     # 交互式操作
go run cmd/kvmigrate/main.go --dry-run       # 迁移计划预览
```

### 性能压测

```bash
bash scripts/test_perf.sh --groups 1 --replicas 3
```

---

## 架构概览

```
 Client (Clerk / TxCoordinator)      Client (Clerk / TxCoordinator)
      │                                          │
      │  gRPC (KVService + ShardService)         │
      │                                          │
  ┌───▼───────────────┐    ┌────────────▼───────────┐
  │     Group 1       │    │       Group 2           │
  │  ┌─────────────┐  │    │  ┌─────────────┐        │
  │  │  Raft Node  │  │    │  │  Raft Node  │  ...   │
  │  │ (Leader)    │◄─┼────┼─►│ (Leader)    │        │
  │  └──────┬──────┘  │    │  └──────┬──────┘        │
  │         │ Raft RPC│    │         │               │
  │  ┌──────▼──────┐  │    │         │               │
  │  │  KVServer   │  │    │         │               │
  │  │ ┌──────────┐│  │    │         │               │
  │  │ │ TxManager││  │    │         │               │
  │  │ │ (2PC参与)││  │    │         │               │
  │  │ ├──────────┤│  │    │         │               │
  │  │ │ ShardMgr ││  │    │         │               │
  │  │ │ (6-phase)││  │    │         │               │
  │  │ ├──────────┤│  │    │         │               │
  │  │ │ BadgerDB ││  │    │         │               │
  │  │ └──────────┘│  │    │         │               │
  │  └──────────────┘  │    │         │               │
  └────────────────────┘    └─────────────────────────┘
```

### 模块说明

| 包 | 职责 |
|------|------|
| `pkg/raft/` | Raft 共识：选举、日志复制、异步持久化、快照 |
| `pkg/kv/` | KV 服务核心：Server、Clerk、RSM 桥接、gRPC、TxManager(2PC 参与者)、TxCoordinator(2PC 协调器)、Shard 状态机 |
| `pkg/sharding/` | 分片拓扑（1024 槽）、ShardRouter、在线迁移编排（6 阶段）、事务路由 |
| `pkg/storage/` | 基于 BadgerDB 的持久化封装 |
| `pkg/watch/` | Key/Prefix 变更订阅与事件分发 |
| `pkg/wal/` | 预写日志分段管理 |
| `pkg/persister/` | Raft 持久化文件 I/O |
| `api/pb/` | Protobuf 契约与生成代码 |
| `cmd/` | 入口：server / kvcli / kvmigrate / benchmarks |

### 写入路径

```
Clerk.Put(key, value, ver)
  │
  ▼
ShardRouter ──► hash(key) % 1024 ──► Group ID ──► gRPC ──► KVServer
                                                              │
                                                    ┌─────────▼─────────┐
                                                    │ Shard 状态检查     │
                                                    │ OWNED → 本地写     │
                                                    │ MIGRATING → 双写   │
                                                    │ ABSENT → 重定向    │
                                                    └─────────┬─────────┘
                                                              │
                                              Raft.Submit ──► 日志复制 ──► Commit
                                                              │
                                              BadgerDB.PutCASWithTTL ◄────┘
```

### 事务提交路径（2PC）

```
TxHandle.Commit()
  │
  ├─ Phase 1: PrepareTx ──► 所有涉及 Group（并行）
  │     │                    TxManager 校验读集版本 → 加写锁 → 持久化 prepare 记录
  │     └─ 任一失败？──► parallelAbort 全部已 Prepare 的 Group
  │
  └─ Phase 2: CommitTx  ──► 所有涉及 Group（并行，幂等重试）
        │                    WriteBatchWithCASAndRecord 原子写用户数据 + commit 标记
        └─ 释放锁，清理 prepare 记录
```

### 读取路径（租约优化）

```
Clerk.Get(key)
  │
  ▼
ShardRouter ──► hash(key) % 1024 ──► Group ID ──► gRPC ──► Leader?
                                                              │
                                              ┌─ Lease 有效？──► 本地读（无 Raft 往返）
                                              │
                                              └─ Lease 过期 ──► Raft 共识读
```

### 性能基准

> 9 节点集群（3 Group × 3 Replica），1000 客户端 × 10 请求，70% 写 / 30% 读

| 指标 | 数值 |
|------|------|
| 总吞吐 | **16,976 ops/s** |
| 写入吞吐 | 11,044 ops/s |
| 平均延迟 | 50.15 ms |
| P99 延迟 | 144.68 ms |
| 写入冲突率 | 7.64% |

**说明**：
- 三副本 Raft 日志同步是分布式一致性的代价，写入受限于网络往返与 fsync
- 租约读优化可降低读延迟（绕过 Raft 往返直达 BadgerDB）
- 实际吞吐受客户端并发数、网络环境、硬件配置影响

---

## 在线迁移流程

```
Phase 1: Target ← IMPORTING       目标准备接收
Phase 2: Source ← MIGRATING       双写开始（本地写 + 转发到 target）
Phase 3: bulkCopyShard            批量拷贝存量数据（全量扫描 + Put）
Phase 4: Target ← OWNED          目标成为正式 owner，双写结束
Phase 5: Source ← ABSENT         源停止服务该 shard
Phase 6: cleanShardData           清理源端过期数据
```

---

## 脚本

| 脚本 | 用途 |
|------|------|
| `run_cluster.sh` | 启动本地集群（支持 node-ring / group-ring 两种模式） |
| `stop_cluster.sh` | 停止集群 |
| `check_status.sh` | 查看各节点 Leader / Term 状态 |
| `test_all.sh` | 全量单元测试 |
| `test_perf.sh` | 自动化性能压测并保存报告 |

---

## License

MIT
