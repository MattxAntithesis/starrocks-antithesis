# S3-compatible object store for the shared-data deployment. MinIO's images are
# not pullable from public registries any more; SeaweedFS's S3 gateway is.
FROM chrislusf/seaweedfs:3.80
COPY s3/entrypoint.sh /s3-entrypoint.sh
RUN chmod 755 /s3-entrypoint.sh
ENTRYPOINT ["/s3-entrypoint.sh"]
