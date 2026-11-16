#!/usr/bin/env bash
# Lead L6: CN segfault in the publish_log_version cancel callback during an
# orderly shutdown (StarRocks 4.1.7, shared-data).
#   cn_shutdown_publish.sh <trials>
# Starts s3, fe and cn1 from loads/config-shared with debug/cn-shutdown.override.yaml
# (single-thread publish pool), sets up sd.t (cn_shutdown_setup.sh), then per trial:
#  1. runs concurrent INSERTs into sd.t while a schema change on it is pinned in
#     WAITING_TXN (cn_shutdown_load.sh), so each publish sends the CN a
#     publish_log_version_batch RPC with one pool task per shadow tablet;
#  2. S3PAUSE=1 (default): pauses the s3 container, so the single publish worker
#     blocks on S3 and later tasks stay queued; waits for >= MINQ queued tasks;
#  3. GDB=1: attaches gdb (debug/l6_widen.gdb, needs CN_IMAGE=starrocks-be-l6-gdb)
#     to pause the main thread for L6_USEC us right after the cancel callback's
#     latch.count_down(), before it unlocks response_mtx;
#  4. docker stop (SIGTERM, orderly shutdown); unpauses s3 after UNPAUSE_AFTER s
#     (default 30: past the CN's ~20 s wait_for_finish while fragments run) so the
#     blocked worker finishes and ThreadPool::shutdown runs the cancel callbacks;
#  5. records exit code, number of cancelled publish-log tasks, SIGSEGV; restarts cn1.
# Controls: ALTER=0 (no schema change: no publish_log_version tasks), LOAD=0 (no load).
# REUSE=1 reuses a running cluster; KEEP=1 leaves it up.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
TRIALS=${1:-10}
OUTDIR=${OUTDIR:-/tmp/cn_shutdown_$(date +%H%M%S)}; mkdir -p "$OUTDIR"
cd "$here/../loads/config-shared"
dc() { docker compose -f docker-compose.yaml -f "$here/debug/cn-shutdown.override.yaml" "$@"; }
net=config-shared_default
if [ "${REUSE:-0}" != 1 ]; then
  dc down -v --remove-orphans >/dev/null 2>&1
  dc up -d s3 fe cn1 >/dev/null 2>&1
  docker run --rm --network "$net" -v "$here:/repro:ro" --entrypoint sh starrocks-loads-driver:latest /repro/cn_shutdown_setup.sh | tail -1
fi
vars() { docker exec cn1 sh -c 'curl -s --max-time 2 localhost:8060/vars' 2>/dev/null; }
queued() { vars | tr -d '\r' | awk '/^lake_publish_version_queued_tasks/{print $3}'; }
lc=""; stopdir=$(mktemp -d)
start_load() {
  rm -f "$stopdir/now"
  lc=$(docker run -d --rm --network "$net" -e ALTER="${ALTER:-1}" -e WRITERS="${WRITERS:-6}" \
    -v "$here:/repro:ro" -v "$stopdir:/stop" --entrypoint sh starrocks-loads-driver:latest /repro/cn_shutdown_load.sh)
}
stop_load() { [ -n "$lc" ] && { touch "$stopdir/now"; docker rm -f "$lc" >/dev/null 2>&1; lc=""; }; }
trap 'stop_load; docker unpause s3 >/dev/null 2>&1; rm -rf "$stopdir"' EXIT
for t in $(seq 1 "$TRIALS"); do
  for _ in $(seq 1 120); do vars | grep -q lake_publish_version_queued_tasks && break; sleep 1; done
  sleep 5
  if [ "${LOAD:-1}" = 1 ]; then
    start_load
    until docker logs "$lc" 2>&1 | grep -q LOAD_START; do sleep 1; done
    docker logs "$lc" 2>&1 | grep "alter job state" | sed 's/^/  /'
    sleep "${LOAD_WARMUP:-5}"
  fi
  if [ "${GDB:-0}" = 1 ]; then
    pid=$(docker exec cn1 pgrep -f lib/starrocks_be | head -1)
    sed -e "s/L6_PID/$pid/" -e "s/L6_USEC/${L6_USEC:-200000}/" -e "s/L6_ADDR/${L6_ADDR:-0x10e7a6e1}/" "$here/debug/l6_widen.gdb" > "$OUTDIR/l6.gdb"
    docker cp "$OUTDIR/l6.gdb" cn1:/l6.gdb >/dev/null
    docker exec -d cn1 sh -c 'gdb -q -nx -batch -x /l6.gdb > /gdb.out 2>&1'
    for _ in $(seq 1 120); do docker exec cn1 grep -q L6_GDB_READY /gdb.out 2>/dev/null && break; sleep 1; done
  fi
  if [ "${LOAD:-1}" = 1 ]; then
    for _ in $(seq 1 300); do qn=$(queued); [ "${qn:-0}" -ge "${MINQ:-20}" ] && break; sleep 0.05; done
    [ "${S3PAUSE:-1}" = 1 ] && docker pause s3 >/dev/null
  fi
  qn=$(queued)
  since=$(date -u +%Y-%m-%dT%H:%M:%S)
  docker stop -t 180 cn1 >/dev/null &
  stopper=$!
  if [ "${S3PAUSE:-1}" = 1 ] && [ "${LOAD:-1}" = 1 ]; then
    sleep "${UNPAUSE_AFTER:-30}"; docker unpause s3 >/dev/null
  fi
  wait $stopper
  code=$(docker inspect cn1 -f '{{.State.ExitCode}}')
  docker logs --since "$since" cn1 > "$OUTDIR/trial$t.log" 2>&1
  [ "${GDB:-0}" = 1 ] && docker cp cn1:/gdb.out "$OUTDIR/trial$t.gdb.out" >/dev/null 2>&1
  ncancel=$(grep -c "publish log version task has been cancelled" "$OUTDIR/trial$t.log")
  segv=$(grep -c "SIGSEGV" "$OUTDIR/trial$t.log")
  gseg=$( [ -f "$OUTDIR/trial$t.gdb.out" ] && grep -c "L6SIGNAL" "$OUTDIR/trial$t.gdb.out" || echo -)
  echo "trial $t: queued_at_stop=${qn:-?} exit=$code cancelled_log_tasks=$ncancel sigsegv_in_log=$segv gdb_signal=$gseg"
  stop_load
  docker start cn1 >/dev/null
done
echo "logs in $OUTDIR"
[ "${KEEP:-0}" = 1 ] || dc down -v --remove-orphans >/dev/null 2>&1
