# StarRocks BE for the Antithesis "loads" workload.
# Official release image; not built from this checkout (see antithesis/README.md).
ARG STARROCKS_VERSION=4.1.7
FROM starrocks/be-ubuntu:${STARROCKS_VERSION}
USER root
RUN apt-get update -y && apt-get install -y --no-install-recommends jq && rm -rf /var/lib/apt/lists/*
COPY be/be.conf.append /tmp/be.conf.append
RUN cat /tmp/be.conf.append >> /opt/starrocks/be/conf/be.conf && rm /tmp/be.conf.append
COPY be/entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
