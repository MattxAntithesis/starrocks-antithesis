#!/usr/bin/env bash
# Lead L8, deterministic: hold one INSERT between commit and publish while
# ALTER TABLE ... OPTIMIZE swaps the partition it wrote to. A Java debugger
# suspends only the FE's publish thread (a thread-only breakpoint at the start
# of DatabaseTransactionMgr.finishTransaction); every other thread, including
# the alter job scheduler, keeps running. Nothing else is changed.
#   repro/optimize_loss_jdb.sh            the bug: OPTIMIZE finishes while the INSERT is held
#   CONTROL=1 repro/optimize_loss_jdb.sh  same hold, no OPTIMIZE: the row must be there
# Fresh 1-FE/3-BE cluster from loads/config with the stock 4.1.7 BE.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
out=$(mktemp -d)
cd "$here/../loads/config"
export BE_IMAGE=starrocks-be-antithesis:latest
cat > "$out/jdwp.yaml" <<'EOF'
services:
  fe:
    environment:
      FE_EXTRA_CONF: |
        JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true -Xmx1536m -XX:+UseG1GC -Djava.security.policy=${STARROCKS_HOME}/conf/udf_security.policy -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:5005"
EOF
export COMPOSE_FILE="docker-compose.yaml:$out/jdwp.yaml"
docker compose down -v --remove-orphans >/dev/null 2>&1
docker compose up -d --no-build fe be1 be2 be3 >/dev/null 2>&1
q() { docker exec fe mysql -h127.0.0.1 -P9030 -uroot -N -e "$1"; }
until q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
for be in be1 be2 be3; do q "ALTER SYSTEM ADD BACKEND '$be:9050'" 2>/dev/null; done
until [ "$(docker exec fe mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW BACKENDS\G' | grep -c 'Alive: true')" -ge 3 ]; do sleep 2; done
# A slower alter scheduler tick (a mutable FE config) gives the script time to
# set the breakpoint between the rewrite finishing and the partition swap.
q "ADMIN SET FRONTEND CONFIG ('alter_scheduler_interval_millisecond' = '30000')"
q "CREATE DATABASE ol"
until q "CREATE TABLE ol.t (d DATE NOT NULL, id BIGINT NOT NULL) DUPLICATE KEY(d, id)
   PARTITION BY RANGE(d) (PARTITION p1 VALUES LESS THAN ('2026-01-02'), PARTITION p2 VALUES LESS THAN ('2026-01-03'))
   DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='3')" 2>/dev/null; do sleep 2; done
q "INSERT INTO ol.t VALUES ('2026-01-01',1),('2026-01-01',2),('2026-01-02',3)"
echo "before: $(q 'SELECT count(*) FROM ol.t') rows"

if [ "${CONTROL:-0}" != 1 ]; then
  q "ALTER TABLE ol.t DISTRIBUTED BY HASH(id) BUCKETS 3"
  # Wait until both rewrite tasks (INSERT ... SELECT into temp partitions) succeeded and published.
  until [ "$(q "SELECT count(*) FROM information_schema.task_runs WHERE task_name LIKE 'optimize-%' AND state='SUCCESS'" 2>/dev/null || echo 0)" -ge 2 ] 2>/dev/null; do sleep 0.5; done
  echo "rewrite tasks done; optimize job: $(q 'SHOW ALTER TABLE OPTIMIZE FROM ol' | grep -oE 'RUNNING|FINISHED|CANCELLED' | tail -1)"
fi

# Attach jdb and suspend only the publish thread at its next finishTransaction.
mkfifo "$out/jdb.in"
( docker exec -i fe jdb -attach 127.0.0.1:5005 < "$out/jdb.in" > "$out/jdb.out" 2>&1 ) &
exec 3> "$out/jdb.in"
echo "stop thread at com.starrocks.transaction.DatabaseTransactionMgr:1175" >&3
until grep -q "Set breakpoint\|Deferring" "$out/jdb.out"; do sleep 0.2; done

( r=$(q "INSERT INTO ol.t WITH LABEL held_insert VALUES ('2026-01-01', 999)" 2>&1); echo "INSERT result: ${r:-OK}" > "$out/insert" ) &
until grep -q "Breakpoint hit" "$out/jdb.out"; do sleep 0.2; done
echo "INSERT committed; publish thread held at finishTransaction"

if [ "${CONTROL:-0}" != 1 ]; then
  until q 'SHOW ALTER TABLE OPTIMIZE FROM ol' | grep -qE 'FINISHED|CANCELLED'; do sleep 1; done
  echo "optimize job: $(q 'SHOW ALTER TABLE OPTIMIZE FROM ol' | grep -oE 'FINISHED|CANCELLED.*' | tail -1)"
else
  sleep 35
fi
echo "clear com.starrocks.transaction.DatabaseTransactionMgr:1175" >&3
echo "resume" >&3
wait %2 2>/dev/null; until [ -s "$out/insert" ]; do sleep 0.5; done
cat "$out/insert"
echo "row 999 present: $(q 'SELECT count(*) FROM ol.t WHERE id = 999')   total rows: $(q 'SELECT count(*) FROM ol.t') (expect 4)"
docker logs fe 2>&1 | grep -E "is dropped, skip version check" | grep -o "partition [0-9]* is dropped, skip version check.*label: held_insert" | head -1
echo "exit" >&3; exec 3>&-
