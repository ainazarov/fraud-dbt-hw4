#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "${HW4_CONTAINER:-0}" == "1" ]]; then
    exec "$@"
fi
exec docker compose run --rm -T tools "$@"
