#!/usr/bin/env bash
set -euo pipefail

# 在 bash 层把 .env.chatwoot 注入环境（可选；node 里我们也会从 common/src/env.js 读取）
if [ -f ".env" ]; then
  set -a
  # shellcheck disable=SC1091
  source ./.env.chatwoot
  set +a
fi

# 日志目录
mkdir -p logs

echo "[start] bridge ..."
node ./bridge/whatsapp-manager.js > logs/bridge.out 2>&1 &

# 如果现在还没上 collector/sender，可以先注释掉
# echo "[start] collector ..."
# node ./collector/src/index.js > logs/collector.out 2>&1 &
#
# echo "[start] sender ..."
# node ./sender/src/index.js > logs/sender.out 2>&1 &

echo "== all started =="
