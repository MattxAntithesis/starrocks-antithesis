#!/usr/bin/env bash
# Variant of run.sh: be1 runs a gdb-enabled image (OVERRIDE compose file) and has gdb
# attached with GDB_SCRIPT before the workload starts. gdb output -> $OUT (default ./gdb_be1.out).
#   MODE=multi ALTER_AT=during repro/run_gdb.sh <iterations> [rows]
# First build the gdb image once:
#   docker build -t starrocks-be-debug:latest -f repro/debug/Dockerfile.be-gdb repro/debug
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
OUT="${OUT:-$PWD/gdb_be1.out}"
OVERRIDE="${OVERRIDE:-$here/debug/be-gdb.override.yaml}"
GDB_SCRIPT="${GDB_SCRIPT:-$here/debug/catch.py}"
cd "$here/../loads/config"
dc() { docker compose -f docker-compose.yaml -f "$OVERRIDE" "$@"; }
dc down -v --remove-orphans >/dev/null 2>&1
dc up -d fe be1 be2 be3 >/dev/null 2>&1
until docker exec be1 pgrep -f lib/starrocks_be >/dev/null 2>&1; do sleep 1; done
sleep 5
pid=$(docker exec be1 pgrep -f lib/starrocks_be | head -1)
docker cp "$GDB_SCRIPT" be1:/catch.py
docker exec -d be1 sh -c "gdb -batch -x /catch.py -p $pid > /gdb.out 2>&1"
until docker exec be1 grep -q GDB_READY /gdb.out 2>/dev/null; do sleep 2; done
echo "gdb attached to be1 pid $pid"
net=$(docker inspect fe -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
docker run --rm --network "$net" -e ALTER_AT="${ALTER_AT:-none}" -e DELAY="${DELAY:-0}" -e MODE="${MODE:-multi}" -v "$here:/repro:ro" --entrypoint sh starrocks-loads-driver:latest \
  /repro/multi_table_txn.sh "$@"
sleep "${POST_WAIT:-20}"
docker cp be1:/gdb.out "$OUT" 2>/dev/null
for b in be1 be2 be3; do
  echo "$b: $(docker inspect $b -f '{{.State.Status}} exit={{.State.ExitCode}}')"
done
