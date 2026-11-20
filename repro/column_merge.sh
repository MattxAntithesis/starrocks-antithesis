#!/usr/bin/env bash
# Lead L9: on a shared-nothing primary-key table, a column-mode partial update
# with merge_condition is accepted and applied even when its condition value is
# older, so older data overwrites newer. The docs say this combination fails on
# shared-nothing; row mode applies the condition correctly (the control).
#   repro/column_merge.sh
# The table has a column (s) the updates leave out: only then is the load a
# partial update. Updating every column takes the full-row path, which works.
# Fresh 1-FE/3-BE cluster from loads/config with the stock 4.1.7 images.
set -uo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here/../loads/config"
export BE_IMAGE=starrocks-be-antithesis:latest BE_DOCKERFILE=be/Dockerfile
docker compose down -v --remove-orphans >/dev/null 2>&1
docker compose up -d --no-build fe be1 be2 be3 >/dev/null 2>&1
q() { docker exec fe mysql -h127.0.0.1 -P9030 -uroot -N -e "$1"; }
until q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
for be in be1 be2 be3; do q "ALTER SYSTEM ADD BACKEND '$be:9050'" 2>/dev/null; done
until [ "$(docker exec fe mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW BACKENDS\G' | grep -c 'Alive: true')" -ge 3 ]; do sleep 2; done
echo "FE $(q 'SELECT current_version()'); BE image $(docker inspect be1 -f '{{.Config.Image}}')"
until q "CREATE DATABASE mc" 2>/dev/null; do sleep 2; done
until q "CREATE TABLE mc.t (k INT, v INT, s VARCHAR(20), ver INT) PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='3')" 2>/dev/null; do sleep 2; done
q "INSERT INTO mc.t VALUES (1, 100, 'orig', 10), (2, 100, 'orig', 10)"
# load <label> <csv row> <extra header args...>
load() { local label=$1 row=$2; shift 2
  docker exec fe sh -c "printf '$row\n' | curl -s --location-trusted -u root: -H 'label:$label' -H 'column_separator:,' \
    -H 'columns:k,v,ver' -H 'partial_update:true' -H 'merge_condition:ver' $* -T - http://127.0.0.1:8030/api/mc/t/_stream_load" \
    | grep -oE '"Status": "[^"]*"' ; }
echo "before:             $(q 'SELECT k, v, s, ver FROM mc.t ORDER BY k' | tr '\n\t' '; ')"
echo "row mode    key 1, older ver 5: $(load row1 '1,6,5')"
echo "column mode key 2, older ver 5: $(load col2 '2,6,5' -H partial_update_mode:column)"
echo "after:              $(q 'SELECT k, v, s, ver FROM mc.t ORDER BY k' | tr '\n\t' '; ')"
echo "expected:           1 100 orig 10; 2 100 orig 10;  (both updates are older than the stored rows)"
