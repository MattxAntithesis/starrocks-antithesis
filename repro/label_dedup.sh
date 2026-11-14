#!/usr/bin/env bash
# Finding 1 standalone: a Stream Load label stops deduplicating once
# label_keep_max_num is exceeded and the label cleaner runs (an FE restart
# makes it run within seconds). Fresh 1-FE/3-BE cluster from loads/config.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here/../loads/config"
docker compose down -v --remove-orphans >/dev/null 2>&1
docker compose up -d fe be1 be2 be3 >/dev/null 2>&1
# Keep only 2 finished transactions' labels per database (default 1000).
docker exec fe sh -c 'echo "label_keep_max_num = 2" >> /opt/starrocks/fe/conf/fe.conf'
net=$(docker inspect fe -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
run() { docker run --rm --network "$net" --entrypoint sh starrocks-loads-driver:latest -c "$1"; }
q() { run "mysql -h fe -P9030 -uroot -N -e \"$1\""; }
load() {  # load <label>: the same 5 rows every time
  run "printf '1,1\n1,2\n1,3\n1,4\n1,5\n' | curl -sS --location-trusted -u root: -X PUT -H 'Expect:100-continue' \
    --data-binary @- -H 'label:$1' -H 'column_separator:,' http://fe:8030/api/d/t/_stream_load" \
    | grep -oE '"Status": *"[^"]*"|"Message": *"[^"]{0,90}'| tr '\n' ' '; echo; }
wait_fe() { until q "SELECT 1" >/dev/null 2>&1; do sleep 2; done; }
docker restart fe >/dev/null; wait_fe
for be in be1 be2 be3; do q "ALTER SYSTEM ADD BACKEND '$be:9050'" >/dev/null 2>&1; done
until [ "$(run "mysql -h fe -P9030 -uroot -e 'SHOW BACKENDS\G'" | grep -c 'Alive: true')" -ge 3 ]; do sleep 2; done
q "CREATE DATABASE d"
until q "CREATE TABLE d.t (k INT NOT NULL, v INT NOT NULL) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='3')" >/dev/null 2>&1; do sleep 2; done
echo "1. load under label L:        $(load L)"
echo "   rows: $(q 'SELECT count(*) FROM d.t')"
echo "2. retry under label L:       $(load L)"
for l in M N O; do load $l >/dev/null; done
echo "3. three more loads (M N O); rows: $(q 'SELECT count(*) FROM d.t')"
echo "4. retry under L (no restart): $(load L)"
echo "5. restarting FE"; docker restart fe >/dev/null; wait_fe; sleep 15
docker logs fe 2>&1 | grep "are expired, remove them" | tail -1 | grep -oE "transaction list \[[^]]{0,60}\][^,]*" | sed 's/^/   FE log: /'
echo "6. retry under L after restart: $(load L)"
sleep 3
echo "   rows: $(q 'SELECT count(*) FROM d.t')   (rows with label L's data were 5; a second copy makes this 25)"
