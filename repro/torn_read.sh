#!/bin/sh
# Lead L5: can one SELECT see a multi-table transaction in one table but not
# the other? Writers commit SQL transactions inserting the same number of rows
# into a and b; readers count both tables in ONE statement. Any read where
# the counts differ saw a transaction half-applied.
#   torn_read.sh <seconds> [buckets] [readers]
# SHARED=1 targets the shared-data deployment (loads/config-shared).
# LOCKDB=1 sets cbo_use_lock_db for the readers: planning then holds the table
# locks throughout instead of planning lock-free (the control).
set -u
FE=${FE:-fe}; SECS=${1:-60}; BUCKETS=${2:-32}; READERS=${3:-4}
q() { mysql -h "$FE" -P9030 -uroot -N -e "$1"; }
until q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
if [ "${SHARED:-0}" = 1 ]; then
  # Shared-data (loads/config-shared): compute nodes, an S3 bucket and a default storage volume.
  for cn in ${CN_HOSTS:-cn1 cn2 cn3}; do q "ALTER SYSTEM ADD COMPUTE NODE '$cn:9050'" 2>/dev/null; done
  until [ "$(mysql -h "$FE" -P9030 -uroot -e 'SHOW COMPUTE NODES\G' 2>/dev/null | grep -c 'Alive: true')" -ge 3 ]; do sleep 2; done
  until curl -s -o /dev/null -X PUT http://s3:8333/starrocks; do sleep 1; done
  until q "CREATE STORAGE VOLUME IF NOT EXISTS sv TYPE = S3 LOCATIONS = ('s3://starrocks/data') PROPERTIES ('enabled'='true','aws.s3.endpoint'='http://s3:8333','aws.s3.region'='us-east-1','aws.s3.access_key'='a','aws.s3.secret_key'='a','aws.s3.enable_path_style_access'='true','aws.s3.use_instance_profile'='false','aws.s3.use_aws_sdk_default_behavior'='false')" 2>/dev/null; do sleep 2; done
  q "SET sv AS DEFAULT STORAGE VOLUME"
else
  for be in ${BE_HOSTS:-be1 be2 be3}; do q "ALTER SYSTEM ADD BACKEND '$be:9050'" 2>/dev/null; done
  until [ "$(mysql -h "$FE" -P9030 -uroot -e 'SHOW BACKENDS\G' 2>/dev/null | grep -c 'Alive: true')" -ge 3 ]; do sleep 2; done
fi
q "DROP DATABASE IF EXISTS tr FORCE"
q "CREATE DATABASE tr"
for t in a b; do
  until q "CREATE TABLE IF NOT EXISTS tr.$t (id BIGINT NOT NULL, v INT NOT NULL) DUPLICATE KEY(id)
     DISTRIBUTED BY HASH(id) BUCKETS $BUCKETS $( [ "${SHARED:-0}" = 1 ] || echo "PROPERTIES('replication_num'='${REPL:-3}')" )" 2>/dev/null; do sleep 2; done
done
echo "tables ready ($BUCKETS buckets)"
end=$(( $(date +%s) + SECS ))
( i=0; while [ "$(date +%s)" -lt "$end" ]; do i=$((i+1));
    mysql -h "$FE" -P9030 -uroot -D tr -e "BEGIN WORK; INSERT INTO a VALUES ($i,1),($i,2); INSERT INTO b VALUES ($i,1),($i,2); COMMIT WORK;" 2>/dev/null
  done; echo "writer: $i transactions" ) &
r=1; while [ $r -le "$READERS" ]; do
  ( n=0; torn=0; while [ "$(date +%s)" -lt "$end" ]; do n=$((n+1));
      pre=""; [ "${LOCKDB:-0}" = 1 ] && pre="SET cbo_use_lock_db = true; "
      # FILTER=1 adds a predicate that is true for every row, so the counts must
      # scan data instead of being folded from metadata row counts.
      w=""; [ "${FILTER:-0}" = 1 ] && w=" WHERE v > 0"
      out=$(q "${pre}SELECT (SELECT count(*) FROM tr.a$w), (SELECT count(*) FROM tr.b$w)" 2>/dev/null)
      set -- $out
      if [ $# -eq 2 ] && [ "$1" != "$2" ]; then torn=$((torn+1)); echo "TORN READ (reader $r): a=$1 b=$2"; fi
    done; echo "reader $r: $n reads, $torn torn" ) &
  r=$((r+1))
done
wait
echo "final: $(q 'SELECT (SELECT count(*) FROM tr.a), (SELECT count(*) FROM tr.b)')"
