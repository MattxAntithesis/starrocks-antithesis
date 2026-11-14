#!/bin/sh
# Variant of torn_read.sh for root-causing. Same writer; reader query and
# session prefix are configurable:
#   PRE   - statements run before the read in the same session (e.g. "SET cbo_use_lock_db=true;")
#   QUERY - the read; must return two columns (default: two filtered scalar subqueries)
#   QID=1 - also print last_query_id() for torn reads
#   torn_read_var.sh <seconds> [buckets] [readers]
set -u
FE=${FE:-fe}; SECS=${1:-60}; BUCKETS=${2:-32}; READERS=${3:-4}
QUERY=${QUERY:-"SELECT (SELECT count(*) FROM tr.a WHERE v > 0), (SELECT count(*) FROM tr.b WHERE v > 0)"}
PRE=${PRE:-}
q() { mysql -h "$FE" -P9030 -uroot -N -e "$1"; }
until q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
for be in ${BE_HOSTS:-be1 be2 be3}; do q "ALTER SYSTEM ADD BACKEND '$be:9050'" 2>/dev/null; done
until [ "$(mysql -h "$FE" -P9030 -uroot -e 'SHOW BACKENDS\G' 2>/dev/null | grep -c 'Alive: true')" -ge 3 ]; do sleep 2; done
q "DROP DATABASE IF EXISTS tr FORCE"
q "CREATE DATABASE tr"
for t in a b; do
  until q "CREATE TABLE IF NOT EXISTS tr.$t (id BIGINT NOT NULL, v INT NOT NULL) DUPLICATE KEY(id)
     DISTRIBUTED BY HASH(id) BUCKETS $BUCKETS PROPERTIES('replication_num'='${REPL:-3}')" 2>/dev/null; do sleep 2; done
done
echo "tables ready ($BUCKETS buckets) PRE=[$PRE] QUERY=[$QUERY]"
end=$(( $(date +%s) + SECS ))
( i=0; while [ "$(date +%s)" -lt "$end" ]; do i=$((i+1));
    mysql -h "$FE" -P9030 -uroot -D tr -e "BEGIN WORK; INSERT INTO a VALUES ($i,1),($i,2); INSERT INTO b VALUES ($i,1),($i,2); COMMIT WORK;" 2>/dev/null
  done; echo "writer: $i transactions" ) &
r=1; while [ $r -le "$READERS" ]; do
  ( n=0; torn=0; while [ "$(date +%s)" -lt "$end" ]; do n=$((n+1));
      post=""; [ "${QID:-0}" = 1 ] && post="; SELECT last_query_id()"
      out=$(q "${PRE}${QUERY}${post}" 2>/dev/null)
      set -- $out
      if [ $# -ge 2 ] && [ "$1" != "$2" ]; then torn=$((torn+1)); echo "TORN READ (reader $r): a=$1 b=$2 ${3:-} t=$(date +%T)"; fi
    done; echo "reader $r: $n reads, $torn torn" ) &
  r=$((r+1))
done
wait
echo "final: $(q 'SELECT (SELECT count(*) FROM tr.a), (SELECT count(*) FROM tr.b)')"
