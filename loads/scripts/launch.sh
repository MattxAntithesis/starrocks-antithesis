#!/usr/bin/env bash
# Build, validate and launch the "loads" workload on Antithesis.
#   scripts/launch.sh <max-compute-hours> [description] [duration, e.g. 90m]
# Check the compute budget (snouty runs usage) before choosing the hours.
set -euo pipefail
hours="${1:?usage: launch.sh <max-compute-hours> [description] [duration]}"
desc="${2:-StarRocks loads/transactions workload}"
# --max-compute-hours is only a budget *cap*: the server still runs its default
# 15m duration, so a large budget alone does not buy a long run (verified: a
# 72-hour budget produced a 15-minute, 13.8-hour run). --duration sets the
# length, and the two flags are mutually exclusive, so pass one or the other.
dur="${3:-}"
here="$(cd "$(dirname "$0")/.." && pwd)"
# The StarRocks version lives in config/.env, not the shell: see that file.
if [ -n "${STARROCKS_VERSION:-}" ]; then
  echo "refusing to launch: STARROCKS_VERSION is set in the shell; set it in config/.env instead" >&2
  exit 1
fi
cd "$here/config"
# A local run leaves its containers stopped; snouty validate refuses to start then.
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
# SKIP_BUILD=1 launches the images already in the local store. Only use it when
# they are known current (e.g. Docker Hub is rate-limiting the base-image pull);
# snouty pushes whatever is in the store, so a stale image launches silently.
if [ "${SKIP_BUILD:-0}" = "1" ]; then
  echo "SKIP_BUILD=1: launching the images already built locally"
  docker image inspect starrocks-fe-antithesis:latest starrocks-be-antithesis:latest \
    starrocks-loads-driver:latest >/dev/null
else
  docker compose build
fi
snouty validate "$here/config" --timeout 10m
plugin_version="0.0.9"
SNOUTY_UNSTABLE_FEATURES=teams snouty launch \
  --launcher basic_test \
  --config "$here/config" \
  --test-name "starrocks-loads" \
  --description "$desc" \
  $( [ -n "$dur" ] && echo "--duration $dur" || echo "--max-compute-hours $hours" ) \
  --fault-tolerance high_availability \
  --fault-exclusions '^driver$' \
  --param attrs.workload=starrocks-loads \
  --param attrs.starrocks_version=4.1.7 \
  --param attrs.plugin_version="$plugin_version"
