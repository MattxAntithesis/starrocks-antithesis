#!/bin/sh
# Standalone reproduction attempt for FINDINGS.md Finding 2: BEs segfault in
# MemTable::finalize while committing a multi-table Stream Load transaction.
# Runs inside a container on the cluster network, with curl and mysql.
#
# Results on a clean, unshared Docker host (repro/run.sh, 1 FE + 3 BE, 4.1.7):
#   MODE=multi  ALTER_AT=during  all 3 BEs SIGSEGV, 3/3 fresh clusters, on iteration 2
#   MODE=multi  ALTER_AT=none    no crash, 160 iterations
#   MODE=single ALTER_AT=during  no crash, 190 iterations (all loads and ALTERs succeeded)
#   MODE=plain  ALTER_AT=during  no crash, 160 iterations
# Earlier single-table "crashes" were from a shared cluster another test was
# crashing at the same time; they do not count.
#   multi_table_txn.sh <iterations> [rows per table]
# ALTER_AT=begin|load1|during|none: add a column to `ev` after the transaction
# begins, after the first table's load, or concurrently with that load
# (started DELAY seconds before it, default 0), before commit.
set -u
FE=${FE:-fe}; N=${1:-50}; ROWS=${2:-5}
q() { mysql -h "$FE" -P9030 -uroot -N -e "$1"; }
until q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
for be in ${BE_HOSTS:-be1 be2 be3}; do q "ALTER SYSTEM ADD BACKEND '$be:9050'" 2>/dev/null; done
alive() { mysql -h "$FE" -P9030 -uroot -e "SHOW BACKENDS\G" 2>/dev/null | grep -c "Alive: true"; }
until [ "$(alive)" -ge 3 ]; do sleep 2; done
q "CREATE DATABASE IF NOT EXISTS aw"
for t in ev ev2; do
  until q "CREATE TABLE IF NOT EXISTS aw.$t (batch BIGINT NOT NULL, seq INT NOT NULL, k INT NOT NULL, val BIGINT NOT NULL)
    DUPLICATE KEY(batch, seq) DISTRIBUTED BY HASH(batch, seq) BUCKETS 4 PROPERTIES('replication_num'='3')" 2>/dev/null; do sleep 2; done
done
echo "tables ready"
post() { curl -sS --location-trusted -u root: -X POST -H "Content-Length:0" -H "label:$1" -H "db:aw" "$@"; }
i=1
while [ $i -le $N ]; do
  L="r${i}_$(date +%s%N)"
  data=$(s=1; while [ $s -le $ROWS ]; do echo "$i,$s,$((s % 12)),$((i*1000+s))"; s=$((s+1)); done)
  case "${MODE:-multi}" in
  multi)
    b=$(curl -sS --location-trusted -u root: -X POST -H "Content-Length:0" -H "label:$L" -H "db:aw" -H "table:ev" \
        -H "transaction_type:multi" "http://$FE:8030/api/transaction/begin")
    [ "${ALTER_AT:-none}" = begin ] && q "ALTER TABLE aw.ev ADD COLUMN pad$i INT NULL"
    [ "${ALTER_AT:-none}" = during ] && { q "ALTER TABLE aw.ev ADD COLUMN pad$i INT NULL" & sleep "${DELAY:-0}"; }
    for t in ev ev2; do
      l=$(printf '%s\n' "$data" | curl -sS --location-trusted -u root: -X PUT -H "Expect:100-continue" --data-binary @- \
        -H "label:$L" -H "db:aw" -H "table:$t" -H "column_separator:," -H "columns:batch,seq,k,val" \
        -H "transaction_type:multi" "http://$FE:8030/api/transaction/load")
      [ "$t" = ev ] && [ "${ALTER_AT:-none}" = load1 ] && q "ALTER TABLE aw.ev ADD COLUMN pad$i INT NULL"
    done
    wait
    c=$(curl -sS --location-trusted -u root: -X POST -H "Content-Length:0" -H "label:$L" -H "db:aw" \
        -H "transaction_type:multi" "http://$FE:8030/api/transaction/commit")
    ;;
  plain)
    # Ordinary single-table Stream Load, the most common load path.
    q "ALTER TABLE aw.ev ADD COLUMN pad$i INT NULL" & sleep "${DELAY:-0}"
    c=$(printf '%s\n' "$data" | curl -sS --location-trusted -u root: -X PUT -H "Expect:100-continue" --data-binary @- \
        -H "label:$L" -H "column_separator:," -H "columns:batch,seq,k,val" "http://$FE:8030/api/aw/ev/_stream_load")
    wait
    ;;
  single)
    # Stream Load transaction interface, one table: begin, load, prepare, commit.
    curl -sS --location-trusted -u root: -X POST -H "Content-Length:0" -H "label:$L" -H "db:aw" -H "table:ev" \
      "http://$FE:8030/api/transaction/begin" >/dev/null
    q "ALTER TABLE aw.ev ADD COLUMN pad$i INT NULL" & sleep "${DELAY:-0}"
    printf '%s\n' "$data" | curl -sS --location-trusted -u root: -X PUT -H "Expect:100-continue" --data-binary @- \
      -H "label:$L" -H "db:aw" -H "table:ev" -H "column_separator:," -H "columns:batch,seq,k,val" \
      "http://$FE:8030/api/transaction/load" >/dev/null
    wait
    curl -sS --location-trusted -u root: -X POST -H "Content-Length:0" -H "label:$L" -H "db:aw" \
      "http://$FE:8030/api/transaction/prepare" >/dev/null
    c=$(curl -sS --location-trusted -u root: -X POST -H "Content-Length:0" -H "label:$L" -H "db:aw" \
      "http://$FE:8030/api/transaction/commit")
    ;;
  esac
  st=$(echo "$c" | jq -r .Status 2>/dev/null)
  alive=$(alive)
  echo "iter $i commit=$st alive_bes=$alive $(echo "$c" | jq -c .Message 2>/dev/null)"
  [ "$alive" -lt 3 ] && { echo "BE DOWN after iter $i"; sleep 5; break; }
  i=$((i+1))
done
