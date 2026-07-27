#!/usr/bin/env bash

# 一键初始化开发环境：下载依赖、生成 proto 代码、可选执行测试。
# 示例：
#   ./scripts/setup_env.sh
#   ./scripts/setup_env.sh --skip-proto
#   ./scripts/setup_env.sh --with-test

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SKIP_PROTO=0
WITH_TEST=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-proto)
      SKIP_PROTO=1
      shift
      ;;
    --with-test)
      WITH_TEST=1
      shift
      ;;
    *)
      echo "未知参数: $1"
      exit 1
      ;;
  esac
done

cd "${ROOT_DIR}"

echo "下载 Go 依赖..."
go mod download

echo "安装 protoc Go 插件..."
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

if [[ "${SKIP_PROTO}" -eq 0 ]]; then
  if command -v protoc >/dev/null 2>&1; then
    echo "生成 protobuf/grpc 代码..."
    bash ./api/pb/compile.sh
  else
    echo "未检测到 protoc，请先安装后执行: ./api/pb/compile.sh"
  fi
fi

mkdir -p data
if [[ ! -f data/.gitkeep ]]; then
  : > data/.gitkeep
fi

chmod +x ./scripts/*.sh

if [[ "${WITH_TEST}" -eq 1 ]]; then
  echo "执行全量测试..."
  ./scripts/test_all.sh
fi

echo "环境初始化完成。"
