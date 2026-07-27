#!/usr/bin/env bash

# 启动本地 KVraft 集群（后台模式）。
# 支持两种运行模式（参数名为兼容保留）：
# 1) node-ring  : 单 Raft 组模式（常规 Raft 集群访问）
# 2) group-ring : 多 group 分片模式（按 group 做一致性哈希）
# 示例：
#   ./scripts/run_cluster.sh
#   ./scripts/run_cluster.sh --arch node-ring --servers 3
#   ./scripts/run_cluster.sh --arch group-ring --groups 2 --replicas 3 --base-port 15000 --clean

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${KV_DATA_DIR:-${ROOT_DIR}/data}"
CLUSTER_DIR="${DATA_ROOT}/cluster"
PID_DIR="${CLUSTER_DIR}/pids"
LOG_DIR="${CLUSTER_DIR}/logs"
BIN_DIR="${DATA_ROOT}/bin"
SERVER_BIN="${BIN_DIR}/kvserver"
RUNTIME_ENV="${CLUSTER_DIR}/runtime.env"
SHARDING_JSON="${CLUSTER_DIR}/sharding.json"

ARCH="group-ring"
SERVERS=3
SHARD_GROUPS=1
REPLICAS=3
BASE_PORT=15000
CLEAN=0
PRUNE_STALE=${KV_PRUNE_STALE_ARCH_DATA:-1}

prune_node_ring_dirs() {
    local root="$1"
    local keep_nodes="$2"
    [[ -d "${root}" ]] || return 0
    shopt -s nullglob
    for d in "${root}"/node-*; do
        [[ -d "${d}" ]] || continue
        local name="${d##*/}"
        local idx="${name#node-}"
        if [[ ! "${idx}" =~ ^[0-9]+$ ]]; then
            continue
        fi
        if (( idx >= keep_nodes )); then
            echo "清理冗余 node-ring 数据目录: ${d}"
            rm -rf "${d}"
        fi
    done
    shopt -u nullglob
}

prune_group_ring_dirs() {
    local root="$1"
    local keep_groups="$2"
    local keep_replicas="$3"
    [[ -d "${root}" ]] || return 0
    shopt -s nullglob
    for d in "${root}"/g*-n*; do
        [[ -d "${d}" ]] || continue
        local name="${d##*/}"
        if [[ ! "${name}" =~ ^g([0-9]+)-n([0-9]+)$ ]]; then
            continue
        fi
        local gid="${BASH_REMATCH[1]}"
        local rid="${BASH_REMATCH[2]}"
        if (( gid >= keep_groups || rid >= keep_replicas )); then
            echo "清理冗余 group-ring 数据目录: ${d}"
            rm -rf "${d}"
        fi
    done
    shopt -u nullglob
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --arch)
            ARCH="$2"
            shift 2
            ;;
        --servers)
            SERVERS="$2"
            shift 2
            ;;
        --groups)
            SHARD_GROUPS="$2"
            shift 2
            ;;
        --replicas)
            REPLICAS="$2"
            shift 2
            ;;
        --base-port)
            BASE_PORT="$2"
            shift 2
            ;;
        --clean)
            CLEAN=1
            shift
            ;;
        *)
            echo "未知参数: $1"
            exit 1
            ;;
    esac
done

if [[ "${ARCH}" != "node-ring" && "${ARCH}" != "group-ring" ]]; then
    echo "未知模式: ${ARCH}，可选值: node-ring(单Raft组) | group-ring(多group分片)"
    exit 1
fi

mkdir -p "${PID_DIR}" "${LOG_DIR}" "${BIN_DIR}"

for pid_file in "${PID_DIR}"/*.pid; do
    [[ -f "${pid_file}" ]] || continue
    pid="$(cat "${pid_file}")"
    if kill -0 "${pid}" 2>/dev/null; then
        echo "检测到集群仍在运行 (pid=${pid})，请先执行 ./scripts/stop_cluster.sh"
        exit 1
    fi
done

if [[ "${PRUNE_STALE}" == "1" ]]; then
    prune_node_ring_dirs "${DATA_ROOT}/arch-node-ring" "${SERVERS}"
    prune_group_ring_dirs "${DATA_ROOT}/arch-group-ring" "${SHARD_GROUPS}" "${REPLICAS}"
fi

echo "编译服务端二进制..."
cd "${ROOT_DIR}"
go build -o "${SERVER_BIN}" ./cmd/server

RAFT_SERVERS=""
GRPC_SERVERS=""
PPROF_SERVERS=""

append_csv() {
    local var_name="$1"
    local value="$2"
    local current="${!var_name:-}"
    if [[ -z "${current}" ]]; then
        printf -v "${var_name}" '%s' "${value}"
    else
        printf -v "${var_name}" '%s' "${current},${value}"
    fi
}

wait_port_ready() {
    local host="$1"
    local port="$2"
    local timeout_sec="$3"
    local start_ts now_ts
    start_ts="$(date +%s)"
    while true; do
        if (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
            return 0
        fi
        now_ts="$(date +%s)"
        if (( now_ts - start_ts >= timeout_sec )); then
            return 1
        fi
        sleep 0.1
    done
}

if [[ "${ARCH}" == "node-ring" ]]; then
    TOTAL_NODES="${SERVERS}"
    node_ring_peers=""
    for ((i=0; i<SERVERS; i++)); do
        rpc_port=$((BASE_PORT + i))
        if [[ -z "${node_ring_peers}" ]]; then
            node_ring_peers="127.0.0.1:${rpc_port}"
        else
            node_ring_peers="${node_ring_peers},127.0.0.1:${rpc_port}"
        fi
    done

    for ((i=0; i<SERVERS; i++)); do
        rpc_port=$((BASE_PORT + i))
        grpc_port=$((rpc_port + 1000))
        rest_port=$((18080 + i))
        metrics_port=$((19100 + i))
        pprof_port=$((17060 + i))

        if [[ "${CLEAN}" -eq 1 ]]; then
            if [[ "${i}" -eq 0 ]]; then
                rm -rf "${DATA_ROOT}/arch-node-ring"
            fi
            rm -rf "${DATA_ROOT}/arch-node-ring/node-${i}"
        fi

        log_file="${LOG_DIR}/node-${i}.log"
        pid_file="${PID_DIR}/node-${i}.pid"

        echo "启动 node ${i} (rpc=${rpc_port}, grpc=${grpc_port}, rest=${rest_port}, metrics=${metrics_port})"
        NODE_ID="${i}" \
        RAFT_PEERS="${node_ring_peers}" \
        REST_LISTEN="127.0.0.1:${rest_port}" \
        METRICS_LISTEN="127.0.0.1:${metrics_port}" \
        PPROF_LISTEN="127.0.0.1:${pprof_port}" \
        KV_DATA_DIR="${DATA_ROOT}/arch-node-ring/node-${i}" \
        nohup "${SERVER_BIN}" >"${log_file}" 2>&1 &

        echo "$!" > "${pid_file}"
        append_csv RAFT_SERVERS "127.0.0.1:${rpc_port}"
        append_csv GRPC_SERVERS "127.0.0.1:${grpc_port}"
        append_csv PPROF_SERVERS "127.0.0.1:${pprof_port}"
    done
else
    TOTAL_NODES=$((SHARD_GROUPS * REPLICAS))
    global_idx=0
    for ((g=0; g<SHARD_GROUPS; g++)); do
        peers=""
        for ((r=0; r<REPLICAS; r++)); do
            rpc_port=$((BASE_PORT + g*100 + r))
            if [[ -z "${peers}" ]]; then
                peers="127.0.0.1:${rpc_port}"
            else
                peers="${peers},127.0.0.1:${rpc_port}"
            fi
        done

        for ((r=0; r<REPLICAS; r++)); do
            rpc_port=$((BASE_PORT + g*100 + r))
            grpc_port=$((rpc_port + 1000))
            rest_port=$((18080 + global_idx))
            metrics_port=$((19100 + global_idx))
            pprof_port=$((17060 + global_idx))

            if [[ "${CLEAN}" -eq 1 ]]; then
                if [[ "${g}" -eq 0 && "${r}" -eq 0 ]]; then
                    rm -rf "${DATA_ROOT}/arch-group-ring"
                fi
                rm -rf "${DATA_ROOT}/arch-group-ring/g${g}-n${r}"
            fi

            log_file="${LOG_DIR}/group-${g}-node-${r}.log"
            pid_file="${PID_DIR}/group-${g}-node-${r}.pid"

            echo "启动 group ${g} (gid=$((g+1))) node ${r} (rpc=${rpc_port}, grpc=${grpc_port}, rest=${rest_port}, metrics=${metrics_port})"
            NODE_ID="${r}" \
            GROUP_ID="$((g+1))" \
            RAFT_PEERS="${peers}" \
            REST_LISTEN="127.0.0.1:${rest_port}" \
            METRICS_LISTEN="127.0.0.1:${metrics_port}" \
            PPROF_LISTEN="127.0.0.1:${pprof_port}" \
            KV_DATA_DIR="${DATA_ROOT}/arch-group-ring/g${g}-n${r}" \
            nohup "${SERVER_BIN}" >"${log_file}" 2>&1 &

            echo "$!" > "${pid_file}"
            append_csv RAFT_SERVERS "127.0.0.1:${rpc_port}"
            append_csv GRPC_SERVERS "127.0.0.1:${grpc_port}"
            append_csv PPROF_SERVERS "127.0.0.1:${pprof_port}"
            global_idx=$((global_idx + 1))
        done
    done
fi

sleep 1

for pid_file in "${PID_DIR}"/*.pid; do
    [[ -f "${pid_file}" ]] || continue
    pid="$(cat "${pid_file}")"
    if ! kill -0 "${pid}" 2>/dev/null; then
        echo "节点启动失败，请检查日志: ${pid_file}"
        exit 1
    fi
done

IFS=',' read -r -a grpc_addrs <<< "${GRPC_SERVERS}"
for addr in "${grpc_addrs[@]}"; do
    host="${addr%:*}"
    port="${addr##*:}"
    if ! wait_port_ready "${host}" "${port}" 8; then
        echo "节点 gRPC 端口未就绪: ${addr}"
        echo "最近日志:"
        ls -1t "${LOG_DIR}"/*.log 2>/dev/null | head -n 3 | while read -r f; do
            echo "===== ${f} ====="
            tail -n 40 "${f}" || true
        done
        exit 1
    fi
done

{
    echo "# 由 scripts/run_cluster.sh 自动生成"
    echo "ARCH=${ARCH}"
    echo "SERVERS=${SERVERS}"
    echo "GROUPS=${SHARD_GROUPS}"
    echo "REPLICAS=${REPLICAS}"
    echo "BASE_PORT=${BASE_PORT}"
    echo "TOTAL_NODES=${TOTAL_NODES}"
    echo "RAFT_SERVERS=${RAFT_SERVERS}"
    echo "GRPC_SERVERS=${GRPC_SERVERS}"
    echo "PPROF_SERVERS=${PPROF_SERVERS}"
    echo "SHARDING_CONFIG=${SHARDING_JSON}"
    echo "SHARDING_NEXT_CONFIG=${CLUSTER_DIR}/sharding.next.json"
} > "${RUNTIME_ENV}"

echo "{" > "${SHARDING_JSON}"
echo "  \"groups\": [" >> "${SHARDING_JSON}"
if [[ "${ARCH}" == "node-ring" ]]; then
    replicas_json=""
    for ((i=0; i<SERVERS; i++)); do
        grpc_port=$((BASE_PORT + i + 1000))
        if [[ -z "${replicas_json}" ]]; then
            replicas_json="\"127.0.0.1:${grpc_port}\""
        else
            replicas_json="${replicas_json}, \"127.0.0.1:${grpc_port}\""
        fi
    done
    cat >> "${SHARDING_JSON}" <<EOF
    {"group_id": 1, "replicas": [${replicas_json}], "leader_idx": 0}
EOF
else
    for ((g=0; g<SHARD_GROUPS; g++)); do
        replicas_json=""
        for ((r=0; r<REPLICAS; r++)); do
            grpc_port=$((BASE_PORT + g*100 + r + 1000))
            if [[ -z "${replicas_json}" ]]; then
                replicas_json="\"127.0.0.1:${grpc_port}\""
            else
                replicas_json="${replicas_json}, \"127.0.0.1:${grpc_port}\""
            fi
        done
        comma=","
        if [[ "${g}" -eq $((SHARD_GROUPS - 1)) ]]; then
            comma=""
        fi
        cat >> "${SHARDING_JSON}" <<EOF
    {"group_id": $((g + 1)), "replicas": [${replicas_json}], "leader_idx": 0}${comma}
EOF
    done
fi
cat >> "${SHARDING_JSON}" <<EOF
  ],
  "num_shards": 1024,
  "virtual_node_count": 150,
  "connect_timeout_ms": 2000,
  "request_timeout_ms": 1200,
  "preferred_replicas": 3
}
EOF

echo "集群已启动"
if [[ "${ARCH}" == "node-ring" ]]; then
    echo "模式: 单 Raft 组（参数: node-ring）"
else
    echo "模式: 多 group 分片（参数: group-ring）"
fi
echo "PID 目录: ${PID_DIR}"
echo "日志目录: ${LOG_DIR}"
echo "运行元数据: ${RUNTIME_ENV}"
echo "分片配置: ${SHARDING_JSON}"
echo "查看状态: ./scripts/check_status.sh"
