#!/bin/bash
# Single FE, addressed by hostname so it survives an IP change after a restart.
# The FE refuses to start when the filesystem holding its meta dir has less than
# bdb-je's 5GB FREE_DISK default usable (MetaHelper#checkMetaDir, hardcoded in
# every released branch), so report what each candidate filesystem offers.
echo "[entrypoint] filesystems:"
df -h / /opt/starrocks/fe/meta /tmp /dev/shm 2>&1 || true
# Older StarRocks images ship tini elsewhere, or not at all. Without it the
# server process is PID 1 itself, which is what Antithesis needs anyway: it
# must receive the signals that stop and kill the container.
exec_with_init() {
  for t in /usr/bin/tini-static /usr/bin/tini /sbin/tini; do
    [ -x "$t" ] && exec "$t" -- "$@"
  done
  exec "$@"
}
exec_with_init /opt/starrocks/fe/bin/start_fe.sh --host_type FQDN --logconsole
