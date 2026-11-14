#!/bin/sh
# One-process SeaweedFS (master + volume + filer + S3 on :8333) with no auth.
# The driver creates the bucket with a plain S3 PUT (idempotent).
mkdir -p /data
exec weed server -dir=/data -s3 -s3.port=8333 -ip=s3 -master.volumeSizeLimitMB=64 -volume.max=100
