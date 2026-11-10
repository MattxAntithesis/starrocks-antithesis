# StarRocks FE for the Antithesis "loads" workload.
# Official release image; not built from this checkout (see antithesis/README.md).
ARG STARROCKS_VERSION=4.1.7
FROM starrocks/fe-ubuntu:${STARROCKS_VERSION}
USER root
RUN apt-get update -y && apt-get install -y --no-install-recommends jq && rm -rf /var/lib/apt/lists/*
# Smaller heap so 1 FE + 3 BE fit in one machine.
RUN sed -i 's/-Xmx8192m/-Xmx1536m/' /opt/starrocks/fe/conf/fe.conf
# Lower bdb-je's je.freeDisk default: the Antithesis RAM disk is under the FE's
# hardcoded 5GB startup gate. See fe/patch_je_free_disk.py.
RUN apt-get update -y && apt-get install -y --no-install-recommends python3 && rm -rf /var/lib/apt/lists/*
COPY fe/patch_je_free_disk.py /tmp/patch_je_free_disk.py
RUN set -eu; jar=$(ls /opt/starrocks/fe/lib/starrocks-bdb-je-*.jar | head -1); \
    python3 -I /tmp/patch_je_free_disk.py "$jar" && rm /tmp/patch_je_free_disk.py
COPY fe/fe.conf.append /tmp/fe.conf.append
RUN cat /tmp/fe.conf.append >> /opt/starrocks/fe/conf/fe.conf && rm /tmp/fe.conf.append
# Antithesis instruments (and catalogs) Java code it finds here: StarRocks's own jars only.
RUN mkdir -p /opt/antithesis/catalog && cd /opt/starrocks/fe/lib && \
    for j in fe-core-*.jar fe-parser-*.jar fe-type-*.jar fe-utils-*.jar starrocks-bdb-je-*.jar; do \
      [ -e "$j" ] && ln -s /opt/starrocks/fe/lib/$j /opt/antithesis/catalog/$j || true; done
COPY fe/entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
