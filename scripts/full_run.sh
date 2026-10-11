#!/usr/bin/env bash
# Requires train.csv and test.csv in DATA_DIR (./data by default).
# Run once with these dataset names; use a new Compose project for a clean repeat.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
docker compose up -d --wait --wait-timeout 240 kafka clickhouse
docker compose --profile tools build producer
docker compose --profile test run --rm --no-deps tests
for dataset in train test; do
    docker compose --profile tools run --rm producer "/data/${dataset}.csv" --dataset "$dataset" --manifest "/results/manifest_${dataset}.json"
    docker compose --profile tools run --rm verify "/data/${dataset}.csv" --dataset "$dataset" --output-dir /results
done
for dataset in train test; do
    docker compose --profile tools run --rm --no-deps benchmark --dataset "$dataset" --repetitions 15 --compact --output-dir /results
done
printf 'Full dataset verification and benchmarks passed. See results/.\n'
