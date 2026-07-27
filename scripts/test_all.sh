#!/usr/bin/env bash

# 编译全部 cmd 并执行全量测试。
# 示例：
#   ./scripts/test_all.sh
#   ./scripts/test_all.sh -run TestTx
#   ./scripts/test_all.sh -v -count=1

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

echo "=== 编译 ==="
go build ./cmd/...

echo
echo "=== 全量测试 ==="
go test -count=1 -timeout 300s ./pkg/... "$@"

echo
echo "结果: PASS"
