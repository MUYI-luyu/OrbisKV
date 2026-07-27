#!/usr/bin/env bash

# 对本地集群执行自动化性能压测，并将报告保存到 data/perf。
# 示例：
#   ./scripts/test_perf.sh
#   ./scripts/test_perf.sh --clients 20 --requests 2000 --read-ratio 0.7 --duration 45s
#   ./scripts/test_perf.sh --maxraftstate 1048576 --sharded true
#   ./scripts/test_perf.sh --restart-cluster true --clean true
#   ./scripts/test_perf.sh --fresh-run true

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${KV_DATA_DIR:-${ROOT_DIR}/data}"
PERF_DIR="${DATA_ROOT}/perf"
RUNTIME_ENV="${DATA_ROOT}/cluster/runtime.env"
mkdir -p "${PERF_DIR}"

SERVERS=3
SERVERS_EXPLICIT=false
CLIENTS=10
REQUESTS=1000
READ_RATIO=0.7
DURATION="30s"
KEYS=10000
MAXRAFTSTATE=1048576
INIT_KEYS=1000
SKIP_INIT_IF_SEEDED=true
BASE_PORT=15000
RESTART_CLUSTER=false
CLEAN=false
FRESH_RUN=false
SHARDED_MODE="auto"
REGRESSION=false
REGRESSION_ROUNDS=3
ELECTION_SAMPLE_MS=1000

ARCH=""
SHARD_GROUPS=""
REPLICAS=""

runtime_value() {
  local key="$1"
  if [[ ! -f "${RUNTIME_ENV}" ]]; then
    echo ""
    return
  fi
  grep -E "^${key}=" "${RUNTIME_ENV}" | head -n1 | cut -d'=' -f2-
}

csv_count() {
  local csv="$1"
  if [[ -z "${csv}" ]]; then
    echo 0
    return
  fi
  awk -F',' '{print NF}' <<<"${csv}"
}

build_addr_csv() {
  local count="$1"
  local start_port="$2"
  local result=""
  local i
  for ((i=0; i<count; i++)); do
    local addr="127.0.0.1:$((start_port + i))"
    if [[ -z "${result}" ]]; then
      result="${addr}"
    else
      result="${result},${addr}"
    fi
  done
  echo "${result}"
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

wait_grpc_servers_ready() {
  local csv="$1"
  local timeout_sec="$2"
  local ok=1
  IFS=',' read -r -a addrs <<< "${csv}"
  for addr in "${addrs[@]}"; do
    local host="${addr%:*}"
    local port="${addr##*:}"
    if ! wait_port_ready "${host}" "${port}" "${timeout_sec}"; then
      echo "gRPC 端口未就绪: ${addr}"
      ok=0
    fi
  done
  [[ "${ok}" -eq 1 ]]
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --servers)
      SERVERS="$2"
      SERVERS_EXPLICIT=true
      shift 2
      ;;
    --clients)
      CLIENTS="$2"
      shift 2
      ;;
    --requests)
      REQUESTS="$2"
      shift 2
      ;;
    --read-ratio)
      READ_RATIO="$2"
      shift 2
      ;;
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --keys)
      KEYS="$2"
      shift 2
      ;;
    --maxraftstate)
      MAXRAFTSTATE="$2"
      shift 2
      ;;
    --sharded)
      SHARDED_MODE="$2"
      shift 2
      ;;
    --init-keys)
      INIT_KEYS="$2"
      shift 2
      ;;
    --skip-init-if-seeded)
      SKIP_INIT_IF_SEEDED="$2"
      shift 2
      ;;
    --base-port)
      BASE_PORT="$2"
      shift 2
      ;;
    --restart-cluster)
      RESTART_CLUSTER="$2"
      shift 2
      ;;
    --clean)
      CLEAN="$2"
      shift 2
      ;;
    --fresh-run)
      FRESH_RUN="$2"
      shift 2
      ;;
    --arch)
      ARCH="$2"
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
    --sharded-mode)
      SHARDED_MODE="$2"
      shift 2
      ;;
    --regression)
      REGRESSION="$2"
      shift 2
      ;;
    --regression-rounds)
      REGRESSION_ROUNDS="$2"
      shift 2
      ;;
    --election-sample-ms)
      ELECTION_SAMPLE_MS="$2"
      shift 2
      ;;
    *)
      echo "未知参数: $1"
      exit 1
      ;;
  esac
done

if [[ "${FRESH_RUN}" == "true" ]]; then
  RESTART_CLUSTER=true
  CLEAN=true
fi

cd "${ROOT_DIR}"

R_ARCH="$(runtime_value ARCH)"
R_BASE_PORT="$(runtime_value BASE_PORT)"
R_GROUPS="$(runtime_value GROUPS)"
R_REPLICAS="$(runtime_value REPLICAS)"
R_TOTAL_NODES="$(runtime_value TOTAL_NODES)"
R_RAFT_SERVERS="$(runtime_value RAFT_SERVERS)"
R_GRPC_SERVERS="$(runtime_value GRPC_SERVERS)"
R_SHARDING_CONFIG="$(runtime_value SHARDING_CONFIG)"

if [[ -z "${ARCH}" ]]; then
  ARCH="${R_ARCH:-group-ring}"
fi
if [[ -z "${R_BASE_PORT}" ]]; then
  R_BASE_PORT="${BASE_PORT}"
fi
if [[ -z "${SHARD_GROUPS}" ]]; then
  SHARD_GROUPS="${R_GROUPS:-1}"
fi
if [[ -z "${REPLICAS}" ]]; then
  REPLICAS="${R_REPLICAS:-3}"
fi
if [[ "${SERVERS_EXPLICIT}" != "true" && -n "${R_TOTAL_NODES}" && "${SERVERS}" == "3" ]]; then
  SERVERS="${R_TOTAL_NODES}"
fi

start_cluster() {
  if [[ "${ARCH}" == "group-ring" ]]; then
    if [[ "${CLEAN}" == "true" ]]; then
      ./scripts/run_cluster.sh --arch group-ring --groups "${SHARD_GROUPS}" --replicas "${REPLICAS}" --base-port "${R_BASE_PORT}" --clean >/dev/null
    else
      ./scripts/run_cluster.sh --arch group-ring --groups "${SHARD_GROUPS}" --replicas "${REPLICAS}" --base-port "${R_BASE_PORT}" >/dev/null
    fi
  else
    if [[ "${CLEAN}" == "true" ]]; then
      ./scripts/run_cluster.sh --arch node-ring --servers "${SERVERS}" --base-port "${R_BASE_PORT}" --clean >/dev/null
    else
      ./scripts/run_cluster.sh --arch node-ring --servers "${SERVERS}" --base-port "${R_BASE_PORT}" >/dev/null
    fi
  fi
}

# 检测：请求的 groups/replicas 与运行中集群是否匹配
# runtime.env 由 run_cluster.sh 写入，包含上次启动的集群参数
if [[ "${RESTART_CLUSTER}" != "true" ]]; then
  R_RUN_GROUPS="$(runtime_value GROUPS)"
  R_RUN_REPLICAS="$(runtime_value REPLICAS)"
  R_RUN_ARCH="$(runtime_value ARCH)"

  if [[ "${ARCH}" == "group-ring" ]]; then
    if [[ -n "${R_RUN_GROUPS}" && "${R_RUN_GROUPS}" != "${SHARD_GROUPS}" ]]; then
      echo "检测到 groups 配置变化: 运行中=${R_RUN_GROUPS}, 请求=${SHARD_GROUPS}，自动重启集群"
      RESTART_CLUSTER=true
    elif [[ -n "${R_RUN_REPLICAS}" && "${R_RUN_REPLICAS}" != "${REPLICAS}" ]]; then
      echo "检测到 replicas 配置变化: 运行中=${R_RUN_REPLICAS}, 请求=${REPLICAS}，自动重启集群"
      RESTART_CLUSTER=true
    elif [[ -n "${R_RUN_ARCH}" && "${R_RUN_ARCH}" != "${ARCH}" ]]; then
      echo "检测到 arch 配置变化: 运行中=${R_RUN_ARCH}, 请求=${ARCH}，自动重启集群"
      RESTART_CLUSTER=true
    fi
  else
    # node-ring 模式：servers 变化也需要重启
    R_RUN_SERVERS="$(runtime_value TOTAL_NODES)"
    if [[ -n "${R_RUN_SERVERS}" && "${R_RUN_SERVERS}" != "${SERVERS}" ]]; then
      echo "检测到 servers 配置变化: 运行中=${R_RUN_SERVERS}, 请求=${SERVERS}，自动重启集群"
      RESTART_CLUSTER=true
    fi
  fi
fi

if [[ "${RESTART_CLUSTER}" == "true" ]]; then
  ./scripts/stop_cluster.sh >/dev/null 2>&1 || true
  start_cluster
else
  CHECK_SERVERS="${R_GRPC_SERVERS}"
  if [[ "${SERVERS_EXPLICIT}" == "true" || -z "${CHECK_SERVERS}" ]]; then
    CHECK_SERVERS="$(build_addr_csv "${SERVERS}" "$((R_BASE_PORT + 1000))")"
  fi

  status_out="$(./scripts/check_status.sh --servers "${CHECK_SERVERS}" 2>/dev/null || true)"
  checked_nodes="$(grep -Eo 'checked nodes: [0-9]+' <<<"${status_out}" | awk '{print $3}' | tail -n1)"
  bad_nodes="$(grep -Ec 'UNREACH|ERROR' <<<"${status_out}" || true)"
  if [[ -z "${checked_nodes}" ]]; then
    checked_nodes=0
  fi

  if [[ "${checked_nodes}" -eq 0 || "${bad_nodes}" -ge "${checked_nodes}" ]]; then
    start_cluster
  fi
fi

# 重新读取运行时元数据，确保压测地址与当前集群一致。
R_ARCH="$(runtime_value ARCH)"
R_GROUPS="$(runtime_value GROUPS)"
R_REPLICAS="$(runtime_value REPLICAS)"
R_RAFT_SERVERS="$(runtime_value RAFT_SERVERS)"
R_GRPC_SERVERS="$(runtime_value GRPC_SERVERS)"
R_SHARDING_CONFIG="$(runtime_value SHARDING_CONFIG)"

if [[ "${SERVERS_EXPLICIT}" == "true" ]]; then
  RPC_SERVERS="$(build_addr_csv "${SERVERS}" "${R_BASE_PORT}")"
elif [[ -n "${R_RAFT_SERVERS}" ]]; then
  RPC_SERVERS="${R_RAFT_SERVERS}"
  SERVERS="$(csv_count "${RPC_SERVERS}")"
else
  RPC_SERVERS="$(build_addr_csv "${SERVERS}" "${R_BASE_PORT}")"
fi

if [[ "${SERVERS_EXPLICIT}" == "true" ]]; then
  STATUS_SERVERS="$(build_addr_csv "${SERVERS}" "$((R_BASE_PORT + 1000))")"
elif [[ -n "${R_GRPC_SERVERS}" ]]; then
  STATUS_SERVERS="${R_GRPC_SERVERS}"
else
  STATUS_SERVERS="$(build_addr_csv "${SERVERS}" "$((R_BASE_PORT + 1000))")"
fi

if ! wait_grpc_servers_ready "${STATUS_SERVERS}" 8; then
  echo "检测到 gRPC 端口不可达，尝试自动重启集群后重试..."
  ./scripts/stop_cluster.sh >/dev/null 2>&1 || true
  CLEAN=true
  start_cluster

  R_ARCH="$(runtime_value ARCH)"
  R_RAFT_SERVERS="$(runtime_value RAFT_SERVERS)"
  R_GRPC_SERVERS="$(runtime_value GRPC_SERVERS)"
  R_SHARDING_CONFIG="$(runtime_value SHARDING_CONFIG)"

  if [[ "${SERVERS_EXPLICIT}" == "true" ]]; then
    RPC_SERVERS="$(build_addr_csv "${SERVERS}" "${R_BASE_PORT}")"
  elif [[ -n "${R_RAFT_SERVERS}" ]]; then
    RPC_SERVERS="${R_RAFT_SERVERS}"
    SERVERS="$(csv_count "${RPC_SERVERS}")"
  else
    RPC_SERVERS="$(build_addr_csv "${SERVERS}" "${R_BASE_PORT}")"
  fi
  if [[ "${SERVERS_EXPLICIT}" == "true" ]]; then
    STATUS_SERVERS="$(build_addr_csv "${SERVERS}" "$((R_BASE_PORT + 1000))")"
  elif [[ -n "${R_GRPC_SERVERS}" ]]; then
    STATUS_SERVERS="${R_GRPC_SERVERS}"
  else
    STATUS_SERVERS="$(build_addr_csv "${SERVERS}" "$((R_BASE_PORT + 1000))")"
  fi

  if ! wait_grpc_servers_ready "${STATUS_SERVERS}" 8; then
    echo "错误: 集群 gRPC 端口仍不可达，请检查 data/cluster/logs/*.log"
    exit 1
  fi
fi

if [[ "${SHARDED_MODE}" == "auto" ]]; then
  if [[ "${R_ARCH}" == "group-ring" && "${R_GROUPS:-1}" -gt 1 ]]; then
    SHARDED="true"
  else
    SHARDED="false"
  fi
else
  SHARDED="${SHARDED_MODE}"
fi

stamp="$(date +%Y%m%d_%H%M%S)"
report_file="${PERF_DIR}/perf_${stamp}.log"

echo "开始执行压测..."
echo "报告文件: ${report_file}"
echo "运行模式: arch=${R_ARCH:-unknown}, servers=${SERVERS}, sharded=${SHARDED}, fresh-run=${FRESH_RUN}"
echo "RPC_SERVERS=${RPC_SERVERS}"

bench_cmd=(
  go run ./cmd/benchmarks/benchmark.go
  --servers="${SERVERS}"
  --server-addrs="${RPC_SERVERS}"
  --clients="${CLIENTS}"
  --requests="${REQUESTS}"
  --read-ratio="${READ_RATIO}"
  --duration="${DURATION}"
  --keys="${KEYS}"
  --init-keys="${INIT_KEYS}"
  --skip-init-if-seeded="${SKIP_INIT_IF_SEEDED}"
  --maxraftstate="${MAXRAFTSTATE}"
  --sharded="${SHARDED}"
)

if [[ "${SHARDED}" == "true" && -n "${R_SHARDING_CONFIG}" && -f "${R_SHARDING_CONFIG}" ]]; then
  bench_cmd+=(--sharding-config="${R_SHARDING_CONFIG}")
fi

extract_metric() {
  local key="$1"
  local file="$2"
  case "${key}" in
    throughput)
      grep -E '^吞吐:' "${file}" | tail -n1 | awk '{print $2}'
      ;;
    p99)
      grep -E 'P99:' "${file}" | tail -n1 | awk '{print $2}'
      ;;
    avg)
      grep -E '平均:' "${file}" | tail -n1 | awk '{print $2}'
      ;;
  esac
}

sample_election_loop() {
  local stop_file="$1"
  local stats_file="$2"
  local prev_sig=""
  local changes=0
  local term_jumps=0
  local prev_term=-1
  local samples=0

  while [[ ! -f "${stop_file}" ]]; do
    local status_out
    status_out="$(./scripts/check_status.sh --servers "${STATUS_SERVERS}" 2>/dev/null || true)"

    local sig
    sig="$(awk '
      $0 ~ /Leader/ {
        term=""
        for (i=1; i<=NF; i++) {
          if (($i=="true" || $i=="false") && i>1 && $(i-1) ~ /^[0-9]+$/) {
            term=$(i-1)
          }
        }
        print $1 ":" $2 ":" term
      }
    ' <<<"${status_out}" | sort | tr '\n' ';')"

    local max_term
    max_term="$(awk '
      {
        for (i=1; i<=NF; i++) {
          if (($i=="true" || $i=="false") && i>1 && $(i-1) ~ /^[0-9]+$/) {
            if ($(i-1)+0 > m) m=$(i-1)+0
          }
        }
      }
      END { if (m=="") m=0; print m }
    ' <<<"${status_out}")"

    if [[ -n "${sig}" ]]; then
      if [[ -n "${prev_sig}" && "${sig}" != "${prev_sig}" ]]; then
        changes=$((changes + 1))
      fi
      prev_sig="${sig}"
    fi
    if [[ "${prev_term}" -ge 0 && "${max_term}" -gt "${prev_term}" ]]; then
      term_jumps=$((term_jumps + 1))
    fi
    prev_term="${max_term}"
    samples=$((samples + 1))
    sleep "$(awk "BEGIN {printf \"%.3f\", ${ELECTION_SAMPLE_MS}/1000}")"
  done

  echo "${changes},${term_jumps},${samples}" > "${stats_file}"
}

run_regression_suite() {
  local csv_file="${PERF_DIR}/perf_regression_${stamp}.csv"
  echo "round,throughput_ops,p99_ms,avg_ms,leader_changes,term_jumps,samples,report" > "${csv_file}"

  local round
  for ((round=1; round<=REGRESSION_ROUNDS; round++)); do
    local round_report="${PERF_DIR}/perf_${stamp}_round${round}.log"
    local stop_file="${PERF_DIR}/.election_stop_${stamp}_${round}"
    local election_file="${PERF_DIR}/election_${stamp}_round${round}.log"
    rm -f "${stop_file}" "${election_file}"

    sample_election_loop "${stop_file}" "${election_file}" &
    local sampler_pid=$!

    "${bench_cmd[@]}" | tee "${round_report}" >/dev/null

    touch "${stop_file}"
    wait "${sampler_pid}" || true

    local throughput p99 avg election_stats leader_changes term_jumps samples
    throughput="$(extract_metric throughput "${round_report}")"
    p99="$(extract_metric p99 "${round_report}")"
    avg="$(extract_metric avg "${round_report}")"
    election_stats="$(cat "${election_file}" 2>/dev/null || echo "0,0,0")"
    leader_changes="$(cut -d',' -f1 <<<"${election_stats}")"
    term_jumps="$(cut -d',' -f2 <<<"${election_stats}")"
    samples="$(cut -d',' -f3 <<<"${election_stats}")"

    echo "${round},${throughput:-0},${p99:-0},${avg:-0},${leader_changes:-0},${term_jumps:-0},${samples:-0},${round_report}" >> "${csv_file}"
    echo "回归轮次 ${round}/${REGRESSION_ROUNDS}: throughput=${throughput:-0} ops/s, p99=${p99:-0} ms, leader_changes=${leader_changes:-0}, term_jumps=${term_jumps:-0}"
  done

  echo "回归测试完成，汇总: ${csv_file}"
}

post_cleanup() {
  if [[ -x "./scripts/cleanup_artifacts.sh" ]]; then
    ./scripts/cleanup_artifacts.sh >/dev/null 2>&1 || true
  fi
}

if [[ "${REGRESSION}" == "true" ]]; then
  echo "开始执行回归模式..."
  run_regression_suite
  post_cleanup
  exit 0
fi

"${bench_cmd[@]}" | tee "${report_file}"
post_cleanup

echo "压测完成，集群保持运行。"
