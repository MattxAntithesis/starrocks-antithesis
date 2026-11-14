#!/bin/sh
# Does one query see a single-statement INSERT half-applied across two
# partitions of ONE table? Each INSERT writes one row to p1 and one to p2 in a
# single transaction, so any consistent read sees the same count in both.
#   torn_partitions.sh <seconds> [buckets] [readers]
set -u
FE=${FE:-fe}; SECS=${1:-90}; BUCKETS=${2:-32}; READERS=${3:-4}
q() { mysql -h "$FE" -P9030 -uroot -N -e "$1"; }
until q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
for be in ${BE_HOSTS:-be1 be2 be3}; do q "ALTER SYSTEM ADD BACKEND '$be:9050'" 2>/dev/null; done
until [ "$(mysql -h "$FE" -P9030 -uroot -e 'SHOW BACKENDS\G' 2>/dev/null | grep -c 'Alive: true')" -ge 3 ]; do sleep 2; done
q "DROP DATABASE IF EXISTS tp FORCE"; q "CREATE DATABASE tp"
until q "CREATE TABLE tp.t (d INT NOT NULL, id BIGINT NOT NULL, v INT NOT NULL) DUPLICATE KEY(d, id)
   PARTITION BY RANGE(d) (PARTITION p1 VALUES LESS THAN ('2'), PARTITION p2 VALUES LESS THAN ('3'))
   DISTRIBUTED BY HASH(id) BUCKETS $BUCKETS PROPERTIES('replication_num'='1')" 2>/dev/null; do sleep 2; done
echo "table ready"
end=$(( $(date +%s) + SECS ))
( i=0; while [ "$(date +%s)" -lt "$end" ]; do i=$((i+1)); q "INSERT INTO tp.t VALUES (1,$i,1),(2,$i,1)" >/dev/null 2>&1; done; echo "writer: $i inserts" ) &
r=1; while [ $r -le "$READERS" ]; do
  ( n=0; torn=0; while [ "$(date +%s)" -lt "$end" ]; do n=$((n+1));
      set -- $(q "SELECT sum(if(d=1,1,0)), sum(if(d=2,1,0)) FROM tp.t WHERE v > 0" 2>/dev/null)
      if [ $# -eq 2 ] && [ "$1" != "$2" ]; then torn=$((torn+1)); echo "TORN (reader $r): p1=$1 p2=$2"; fi
    done; echo "reader $r: $n reads, $torn torn" ) &
  r=$((r+1))
done
wait
