#!/usr/bin/env bash
# Deterministic version of the "ALTER during load" race. The FE runs with JDWP
# (OVERRIDE compose file); jdb suspends only the thread planning the `ev` load at
# LoadPlanner.java:$BP_LINE (4.1.7: 327 = after generateTupleDescriptor, before the
# OlapTableSink schema is built; 285 = start of plan(), before getFullSchema()),
# then runs ALTER TABLE ev ADD COLUMN, then resumes the thread.
#   MODE=multi|single BP_LINE=327|285 ALTER=1|0 repro/bp_race.sh [iterations]
# OVERRIDE defaults to repro/debug/fe-jdwp.override.yaml (FE with a JDWP agent on :5005).
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
MODE=${MODE:-multi}; BP_LINE=${BP_LINE:-327}; ALTER=${ALTER:-1}; N=${1:-1}
OUTDIR=${OUTDIR:-$PWD}
OVERRIDE="${OVERRIDE:-$here/debug/fe-jdwp.override.yaml}"
cd "$here/../loads/config"
dc() { docker compose -f docker-compose.yaml -f "$OVERRIDE" "$@"; }
dc down -v --remove-orphans >/dev/null 2>&1
docker rm -f bp-driver >/dev/null 2>&1
dc up -d fe be1 be2 be3 >/dev/null 2>&1
net=$(docker inspect fe -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
docker run -d --name bp-driver --network "$net" --entrypoint sleep starrocks-loads-driver:latest infinity >/dev/null
d() { docker exec -i bp-driver sh -c "$1"; }
q() { d "mysql -h fe -P9030 -uroot -N -e \"$1\""; }
until q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
for be in be1 be2 be3; do q "ALTER SYSTEM ADD BACKEND '$be:9050'" >/dev/null 2>&1; done
until [ "$(d 'mysql -h fe -P9030 -uroot -e "SHOW BACKENDS\G"' 2>/dev/null | grep -c 'Alive: true')" -ge 3 ]; do sleep 2; done
q "CREATE DATABASE IF NOT EXISTS aw"
for t in ev ev2; do
  until q "CREATE TABLE IF NOT EXISTS aw.$t (batch BIGINT NOT NULL, seq INT NOT NULL, k INT NOT NULL, val BIGINT NOT NULL) DUPLICATE KEY(batch, seq) DISTRIBUTED BY HASH(batch, seq) BUCKETS 4 PROPERTIES('replication_num'='3')" >/dev/null 2>&1; do sleep 2; done
done
echo "tables ready"
# jdb session driven through a fifo
fifo=$(mktemp -u); mkfifo "$fifo"; jout="$OUTDIR/jdb_${MODE}_${BP_LINE}_${ALTER}.out"
docker exec -i fe jdb -attach localhost:5005 < "$fifo" > "$jout" 2>&1 &
exec 7>"$fifo"
J() { echo "$1" >&7; }
sleep 3
CURL='curl -sS --location-trusted -u root: '
for i in $(seq 1 "$N"); do
  L="bp${i}_$(date +%s%N)"
  data=$(s=1; while [ $s -le 5 ]; do echo "$i,$s,$((s % 12)),$((i*1000+s))"; s=$((s+1)); done)
  TT=""; [ "$MODE" = multi ] && TT='-H transaction_type:multi'
  d "$CURL -X POST -H Content-Length:0 -H label:$L -H db:aw -H table:ev $TT http://fe:8030/api/transaction/begin" >/dev/null
  J "stop thread at com.starrocks.sql.LoadPlanner:$BP_LINE"
  sleep 1
  hits_before=$(grep -c "Breakpoint hit" "$jout")
  printf '%s\n' "$data" | d "$CURL -X PUT -H Expect:100-continue --data-binary @- -H label:$L -H db:aw -H table:ev -H column_separator:, -H columns:batch,seq,k,val $TT http://fe:8030/api/transaction/load" > "$OUTDIR/load_ev_$i.json" &
  lp=$!
  for _ in $(seq 1 60); do [ "$(grep -c 'Breakpoint hit' "$jout")" -gt "$hits_before" ] && break; sleep 0.5; done
  echo "iter $i: breakpoint hit at LoadPlanner:$BP_LINE: $(grep 'Breakpoint hit' "$jout" | tail -1 | cut -c1-160)"
  if [ "$ALTER" = 1 ]; then q "ALTER TABLE aw.ev ADD COLUMN pad$i INT NULL" && echo "iter $i: ALTER pad$i done while ev planner thread is suspended"; fi
  J "clear com.starrocks.sql.LoadPlanner:$BP_LINE"
  J "resume"
  wait $lp; echo "iter $i: ev load -> $(jq -c '{Status,Message}' "$OUTDIR/load_ev_$i.json" 2>/dev/null)"
  if [ "$MODE" = multi ]; then
    printf '%s\n' "$data" | d "$CURL -X PUT -H Expect:100-continue --data-binary @- -H label:$L -H db:aw -H table:ev2 -H column_separator:, -H columns:batch,seq,k,val $TT http://fe:8030/api/transaction/load" >/dev/null
  else
    d "$CURL -X POST -H Content-Length:0 -H label:$L -H db:aw http://fe:8030/api/transaction/prepare" >/dev/null
  fi
  c=$(d "$CURL -X POST -H Content-Length:0 -H label:$L -H db:aw $TT http://fe:8030/api/transaction/commit")
  sleep 5
  echo "iter $i: commit -> $(echo "$c" | jq -c '{Status,Message}' 2>/dev/null)"
  for b in be1 be2 be3; do echo "   $b: $(docker inspect $b -f '{{.State.Status}} exit={{.State.ExitCode}}')"; done
  [ "$(docker inspect be1 -f '{{.State.Status}}')" != running ] && break
done
J "quit"; exec 7>&-; rm -f "$fifo"
docker rm -f bp-driver >/dev/null 2>&1
