#!/bin/bash
# One FE, or a leader plus followers (FE_HELPER), addressed by hostname so it survives an IP change after a restart.
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
# Several FEs share one machine with the BEs, so a deployment can cap the heap.
if [ -n "${FE_XMX:-}" ]; then
  sed -i -E "s/-Xmx[0-9]+[mMgG]/-Xmx${FE_XMX}/" /opt/starrocks/fe/conf/fe.conf
fi
# FE_RUN_MODE=shared_data: data lives in object storage, compute nodes are
# stateless. The driver creates the storage volume once the FE is up.
if [ "${FE_RUN_MODE:-}" = shared_data ] && ! grep -q "^run_mode = shared_data" /opt/starrocks/fe/conf/fe.conf; then
  printf '\nrun_mode = shared_data\ncloud_native_storage_type = S3\n' >> /opt/starrocks/fe/conf/fe.conf
fi
# FE_EXTRA_CONF: extra fe.conf lines (newline-separated), appended once.
if [ -n "${FE_EXTRA_CONF:-}" ] && ! grep -q "^# --- FE_EXTRA_CONF ---" /opt/starrocks/fe/conf/fe.conf; then
  printf '\n# --- FE_EXTRA_CONF ---\n%s\n' "$FE_EXTRA_CONF" >> /opt/starrocks/fe/conf/fe.conf
fi
# A follower (FE_HELPER=<leader>:9010) joins through the leader on its first
# start only; once it has metadata it restarts like any FE, and finds the
# current leader itself. The leader must list it before it may join, and the
# driver adds it (ALTER SYSTEM ADD FOLLOWER), so wait for that first.
if [ -n "${FE_HELPER:-}" ] && [ ! -d /opt/starrocks/fe/meta/bdb ]; then
  me=$(hostname)
  helper_http="${FE_HELPER%%:*}:8030"
  echo "[entrypoint] follower $me: waiting for $helper_http to list it"
  until curl -s -m 3 -u root: "http://$helper_http/api/show_proc?path=/frontends" | grep -q "$me"; do
    sleep 1
  done
  echo "[entrypoint] follower $me: joining through $FE_HELPER"
  exec_with_init /opt/starrocks/fe/bin/start_fe.sh --helper "$FE_HELPER" --host_type FQDN --logconsole
fi
exec_with_init /opt/starrocks/fe/bin/start_fe.sh --host_type FQDN --logconsole
