#!/usr/bin/env bash
# Run the workload locally without faults and collect its assertions.
#   scripts/local-run.sh [seconds=300] [eventually_after_secs]
# With eventually_after_secs, the driver runs its final checks after that many
# seconds of workload and exits 0. Output goes to antithesis/loads/runs/<timestamp>/.
set -euo pipefail
here="$(cd "$(dirname "$0")/.." && pwd)"
secs="${1:-300}"
ev="${2:-}"
out="$here/runs/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$out"
cd "$here/config"
export COMPOSE_FILE="docker-compose.yaml:../scripts/docker-compose.local.yaml"
if [ -n "$ev" ]; then export DRIVER_EVENTUALLY=on DRIVER_EVENTUALLY_AFTER_SECS="$ev"; fi
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload
echo "waiting for setup complete..."
timeout 900 sh -c 'until docker compose exec -T driver grep -qs antithesis_setup /tmp/sdk.jsonl 2>/dev/null; do sleep 5; done' \
  || { echo "setup did not complete"; docker compose logs driver | tail -40; }
echo "setup complete; running workload for ${secs}s"
end=$(( $(date +%s) + secs ))
while [ "$(date +%s)" -lt "$end" ]; do
  if [ "$(docker inspect -f '{{.State.Running}}' driver)" != "true" ]; then echo "driver exited"; break; fi
  sleep 5
done
docker compose cp driver:/tmp/sdk.jsonl "$out/sdk.jsonl" || true
for s in fe be1 be2 be3 driver; do docker compose logs --no-color "$s" > "$out/$s.log" 2>&1 || true; done
docker compose ps -a > "$out/ps.txt"
docker compose stop -t 60
echo "assertions that would fail in a run:"
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
  | select(if .[0].assert_type == "sometimes" then any(.hit and .condition) | not
           else any(.hit and (.condition | not)) or (.[0].must_hit and (any(.hit) | not)) end)
  | "\(.[0].display_type)\t\(.[0].message)"' "$out/sdk.jsonl" | tee "$out/unmet.txt"
echo "output: $out"
