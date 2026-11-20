#!/bin/sh
# Lead L8: an INSERT that commits while ALTER TABLE ... OPTIMIZE (here a bucket
# count change) is swapping in its rewritten partition is acknowledged OK, but
# its row is gone. Writers insert one uniquely numbered row per statement and
# record every id the FE acknowledged; the main loop keeps changing the bucket
# count. At the end every acknowledged id must be in the table.
#   optimize_loss.sh <optimize rounds> [writers]
# No faults. Shared-nothing, default FE config (enable_online_optimize_table is
# false, so every OPTIMIZE runs as OptimizeJobV2).
set -u
FE=${FE:-fe}; ROUNDS=${1:-20}; WRITERS=${2:-6}
q() { mysql -h "$FE" -P9030 -uroot -N -e "$1"; }
until q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
for be in ${BE_HOSTS:-be1 be2 be3}; do q "ALTER SYSTEM ADD BACKEND '$be:9050'" 2>/dev/null; done
until [ "$(mysql -h "$FE" -P9030 -uroot -e 'SHOW BACKENDS\G' 2>/dev/null | grep -c 'Alive: true')" -ge 3 ]; do sleep 2; done
q "DROP DATABASE IF EXISTS ol FORCE"
q "CREATE DATABASE ol"
until q "CREATE TABLE IF NOT EXISTS ol.t (d DATE NOT NULL, id BIGINT NOT NULL) DUPLICATE KEY(d, id)
   PARTITION BY RANGE(d) (PARTITION p1 VALUES LESS THAN ('2026-01-02'), PARTITION p2 VALUES LESS THAN ('2026-01-03'))
   DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES('replication_num'='3')" 2>/dev/null; do sleep 2; done
dir=$(mktemp -d); stop=$dir/stop
w=1; while [ $w -le "$WRITERS" ]; do
  ( i=0; while [ ! -e "$stop" ]; do i=$((i+1)); id=$((w*10000000+i))
      if q "INSERT INTO ol.t VALUES ('2026-01-01', $id)" >/dev/null 2>&1; then echo $id >> "$dir/acked.$w"; fi
    done ) &
  w=$((w+1))
done
r=1; while [ $r -le "$ROUNDS" ]; do
  b=$(( (r % 3) + 2 ))
  until q "ALTER TABLE ol.t DISTRIBUTED BY HASH(id) BUCKETS $b" 2>/dev/null; do sleep 1; done
  until s=$(q "SHOW ALTER TABLE OPTIMIZE FROM ol" | sort -n | tail -1 | grep -oE "FINISHED|CANCELLED.*"); do sleep 0.5; done
  echo "round $r (buckets $b): $s"
  r=$((r+1))
done
touch "$stop"; wait
cat "$dir"/acked.* | sort -n > "$dir/acked"
q "SELECT id FROM ol.t ORDER BY id" > "$dir/present"
echo "acknowledged: $(wc -l < "$dir/acked")  present: $(wc -l < "$dir/present")  duplicates: $(uniq -d "$dir/present" | wc -l)"
missing=$(comm -23 "$dir/acked" "$dir/present")
if [ -n "$missing" ]; then
  echo "LOST ACKNOWLEDGED ROWS: $(echo "$missing" | wc -l)"
  for id in $(echo "$missing" | head -5); do echo "  id $id"; done
else
  echo "no acknowledged row lost"
fi
