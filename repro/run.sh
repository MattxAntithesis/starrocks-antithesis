#!/usr/bin/env bash
# Fresh 1-FE/3-BE cluster from loads/config (no driver), then the repro script.
#   repro/run.sh <iterations> [rows]   -> prints per-iteration status and BE crashes
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here/../loads/config"
docker compose down -v --remove-orphans >/dev/null 2>&1
docker compose up -d fe be1 be2 be3 >/dev/null 2>&1
net=$(docker inspect fe -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
docker run --rm --network "$net" -e ALTER_AT="${ALTER_AT:-none}" -e DELAY="${DELAY:-0}" -e MODE="${MODE:-multi}" -v "$here:/repro:ro" --entrypoint sh starrocks-loads-driver:latest \
  /repro/multi_table_txn.sh "$@"
for b in be1 be2 be3; do
  echo "$b: $(docker inspect $b -f '{{.State.Status}} exit={{.State.ExitCode}}')"
  docker logs $b 2>&1 | grep -A6 "SIGSEGV" | grep -E "SIGSEGV|@ +0x[0-9a-f]+ starrocks" | head -6
done
