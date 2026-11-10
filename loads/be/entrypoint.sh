#!/bin/bash
# The driver registers this BE with the FE by hostname (ALTER SYSTEM ADD BACKEND).
echo "[entrypoint] filesystems:"
df -h / /opt/starrocks/be/storage /tmp 2>&1 || true
# Older StarRocks images ship tini elsewhere, or not at all. Without it the
# server process is PID 1 itself, which is what Antithesis needs anyway: it
# must receive the signals that stop and kill the container.
exec_with_init() {
  for t in /usr/bin/tini-static /usr/bin/tini /sbin/tini; do
    [ -x "$t" ] && exec "$t" -- "$@"
  done
  exec "$@"
}
exec_with_init /opt/starrocks/be/bin/start_be.sh --logconsole
