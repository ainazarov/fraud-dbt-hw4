#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
docker compose up -d --wait --wait-timeout 240 kafka clickhouse
docker compose --profile tools build producer
docker compose --profile test run --rm --no-deps tests
# A new namespace makes this demonstration safely repeatable without deleting volumes.
dataset="demo_$(date -u +%Y%m%d_%H%M%S)_${RANDOM}"
docker compose --profile tools run --rm producer /app/data/demo.csv --dataset "$dataset" --manifest "/results/manifest_${dataset}.json"
docker compose --profile tools run --rm verify /app/data/demo.csv --dataset "$dataset" --output-dir /results
docker compose --profile tools run --rm --no-deps benchmark --dataset "$dataset" --output-dir /results
printf 'Demo passed. Results: results/max_by_state_%s.csv\n' "$dataset"
