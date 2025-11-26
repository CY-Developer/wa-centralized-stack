#!/usr/bin/env bash
set -e
DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$DIR/sender"
node src/webhook.js &
node src/worker.js
