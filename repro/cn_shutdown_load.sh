#!/bin/sh
# Load side of cn_shutdown_publish.sh (run inside the driver image on the
# config-shared network). Pins a schema change on sd.t in WAITING_TXN with an
# open Stream Load transaction, so sd.t has a shadow index and every load's
# publish sends publish_log_version_batch (one task per shadow tablet) to the CN.
# Then runs $WRITERS concurrent INSERT loops until /stop/now exists.
#   ALTER=0 skips the schema change (control: no publish_log_version at all).
set -u
FE=${FE:-fe}; WRITERS=${WRITERS:-6}
q() { mysql -h "$FE" -P9030 -uroot -N -e "$1"; }
# Fresh table per trial: a CN restart aborts the pinning transaction (its
# coordinator was the CN), which lets the previous schema change finish.
q "CANCEL ALTER TABLE COLUMN FROM sd.t" >/dev/null 2>&1
until q "DROP TABLE IF EXISTS sd.t FORCE" 2>/dev/null; do sleep 1; done
until q "CREATE TABLE sd.t (id BIGINT NOT NULL, v INT NOT NULL) DUPLICATE KEY(id)
   DISTRIBUTED BY HASH(id) BUCKETS ${BUCKETS:-64}" 2>/dev/null; do sleep 2; done
q "INSERT INTO sd.t SELECT generate_series, 1 FROM TABLE(generate_series(1, 1000))"
if [ "${ALTER:-1}" = 1 ]; then
  lbl=pin_$(date +%s)
  curl -s --location-trusted -u root: -XPOST -H "label:$lbl" -H "db:sd" -H "table:t" -H "timeout:7200" \
    http://$FE:8030/api/transaction/begin >/dev/null
  printf '9999999\t1\n' | curl -s --location-trusted -u root: -XPUT -H "label:$lbl" -H "db:sd" -H "table:t" -T - \
    http://$FE:8030/api/transaction/load >/dev/null
  q "ALTER TABLE sd.t MODIFY COLUMN v VARCHAR(32) NOT NULL"
  sleep 3
fi
mysql -h "$FE" -P9030 -uroot -e "SHOW ALTER TABLE COLUMN FROM sd\\G" | awk '/State:/{s=$2} END{print "latest alter job state:", s}'
echo LOAD_START
w=1; while [ $w -le "$WRITERS" ]; do
  ( i=0; while [ ! -e /stop/now ]; do i=$((i+1)); mysql --connect-timeout=5 -h "$FE" -P9030 -uroot -N -e "SET query_timeout=20; INSERT INTO sd.t VALUES ($w$i,1),($w$i+7,2),($w$i+13,3)" >/dev/null 2>&1; done ) &
  w=$((w+1))
done
wait
