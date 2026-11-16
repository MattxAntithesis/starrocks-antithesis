#!/bin/sh
# Setup for cn_shutdown_publish.sh (run inside the driver image on the
# config-shared network): one compute node (cn1), the S3 bucket, the default
# storage volume, and table sd.t with $BUCKETS buckets plus some rows.
# Adapted from torn_read.sh (SHARED=1).
set -u
FE=${FE:-fe}; BUCKETS=${BUCKETS:-64}
q() { mysql -h "$FE" -P9030 -uroot -N -e "$1"; }
until q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
for cn in ${CN_HOSTS:-cn1}; do q "ALTER SYSTEM ADD COMPUTE NODE '$cn:9050'" 2>/dev/null; done
until [ "$(mysql -h "$FE" -P9030 -uroot -e 'SHOW COMPUTE NODES\G' 2>/dev/null | grep -c 'Alive: true')" -ge 1 ]; do sleep 2; done
until curl -s -o /dev/null -X PUT http://s3:8333/starrocks; do sleep 1; done
until q "CREATE STORAGE VOLUME IF NOT EXISTS sv TYPE = S3 LOCATIONS = ('s3://starrocks/data') PROPERTIES ('enabled'='true','aws.s3.endpoint'='http://s3:8333','aws.s3.region'='us-east-1','aws.s3.access_key'='a','aws.s3.secret_key'='a','aws.s3.enable_path_style_access'='true','aws.s3.use_instance_profile'='false','aws.s3.use_aws_sdk_default_behavior'='false')" 2>/dev/null; do sleep 2; done
q "SET sv AS DEFAULT STORAGE VOLUME"
q "DROP DATABASE IF EXISTS sd FORCE"
q "CREATE DATABASE sd"
until q "CREATE TABLE IF NOT EXISTS sd.t (id BIGINT NOT NULL, v INT NOT NULL) DUPLICATE KEY(id)
   DISTRIBUTED BY HASH(id) BUCKETS $BUCKETS" 2>/dev/null; do sleep 2; done
q "INSERT INTO sd.t SELECT generate_series, 1 FROM TABLE(generate_series(1, 1000))"
echo "SETUP_DONE"
