#!/bin/sh
# Finding 3, persisted: can INSERT ... SELECT store a half-applied transaction?
# A writer commits SQL transactions inserting 2 rows into each of a and b.
# Snapshotters run INSERT INTO snap SELECT <count a>, <count b>. Every stored
# row with ca != cb is a torn read written to a table for good.
#   torn_persist.sh <seconds> [buckets] [snapshotters]
set -u
FE=${FE:-fe}; SECS=${1:-120}; BUCKETS=${2:-32}; SNAPS=${3:-4}
q() { mysql -h "$FE" -P9030 -uroot -N -e "$1"; }
until q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
for be in ${BE_HOSTS:-be1 be2 be3}; do q "ALTER SYSTEM ADD BACKEND '$be:9050'" 2>/dev/null; done
until [ "$(mysql -h "$FE" -P9030 -uroot -e 'SHOW BACKENDS\G' 2>/dev/null | grep -c 'Alive: true')" -ge 3 ]; do sleep 2; done
q "DROP DATABASE IF EXISTS tp2 FORCE"; q "CREATE DATABASE tp2"
for t in a b; do
  until q "CREATE TABLE tp2.$t (id BIGINT NOT NULL, v INT NOT NULL) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS $BUCKETS PROPERTIES('replication_num'='1')" 2>/dev/null; do sleep 2; done
done
until q "CREATE TABLE tp2.snap (who INT NOT NULL, ca BIGINT NOT NULL, cb BIGINT NOT NULL) DUPLICATE KEY(who) DISTRIBUTED BY HASH(who) BUCKETS 1 PROPERTIES('replication_num'='1')" 2>/dev/null; do sleep 2; done
echo "tables ready"
end=$(( $(date +%s) + SECS ))
( i=0; while [ "$(date +%s)" -lt "$end" ]; do i=$((i+1));
    mysql -h "$FE" -P9030 -uroot -D tp2 -e "BEGIN WORK; INSERT INTO a VALUES ($i,1),($i,2); INSERT INTO b VALUES ($i,1),($i,2); COMMIT WORK;" 2>/dev/null
  done; echo "writer: $i transactions" ) &
r=1; while [ $r -le "$SNAPS" ]; do
  ( n=0; while [ "$(date +%s)" -lt "$end" ]; do n=$((n+1));
      q "INSERT INTO tp2.snap SELECT $r, (SELECT count(*) FROM tp2.a WHERE v > 0), (SELECT count(*) FROM tp2.b WHERE v > 0)" >/dev/null 2>&1
    done; echo "snapshotter $r: $n inserts" ) &
  r=$((r+1))
done
wait
echo "stored snapshots: $(q 'SELECT count(*) FROM tp2.snap')"
echo "stored torn snapshots (ca != cb): $(q 'SELECT count(*) FROM tp2.snap WHERE ca != cb')"
q "SELECT who, ca, cb FROM tp2.snap WHERE ca != cb LIMIT 5" | sed 's/^/   /'
echo "final: $(q 'SELECT (SELECT count(*) FROM tp2.a), (SELECT count(*) FROM tp2.b)')"
