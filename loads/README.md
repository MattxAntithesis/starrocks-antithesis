# Antithesis workloads for StarRocks

Workloads that run StarRocks in [Antithesis](https://antithesis.com) and check its guarantees under faults (node kills, network partitions, pauses, clock skips, thread pausing).

Findings so far are in [FINDINGS.md](../FINDINGS.md).

## `loads/`: loads and transactions on a shared-nothing cluster

**Deployment** (`loads/config/docker-compose.yaml`): 1 FE, 3 BEs, tables with `replication_num = 3`, and a fault-excluded `driver`. The FE and BE images are the official `starrocks/{fe,be}-ubuntu:4.1.7` release images with small config changes (`loads/fe`, `loads/be`). They are **not** built from the StarRocks source tree, and the C++ BE is not coverage-instrumented. The FE's own jars are listed in `/opt/antithesis/catalog`, so Antithesis instruments the Java FE.

**Driver** (`loads/driver`, Rust, [hegel](https://hegel.dev) concurrent state machine). It registers the BEs, creates the database `aw`, reports setup complete, and then runs these actions concurrently:

| Action | What it sends |
| --- | --- |
| kv insert / delete / stream load | `INSERT`, `DELETE`, and Stream Load with `__op` (upsert/delete) on the primary-key table `kv` (12 keys, unique values) |
| batch load | A batch of 1–8 rows into `ev` via `INSERT ... WITH LABEL`, Stream Load through the FE or directly to a BE, a SQL transaction (`BEGIN`/`COMMIT`/`ROLLBACK`) over `ev`+`ev2`, or the Stream Load transaction interface (single table with begin/load/prepare/commit or rollback, possibly committed in a later step; or multi-table) |
| batch retry | Resends a load that was not acknowledged, with the **same label** |
| reads | Point/multi-key/full reads of `kv`; per-batch counts in `ev`/`ev2`; one-statement reads of a multi-table batch in both tables |
| pk upsert / partial update / delete / read | The primary-key table `pk`, whose columns are all derived from one generation number. Partial column updates in row and column mode, over a subset of columns, optionally with `merge_condition`; an `AUTO_INCREMENT` column that must survive them |
| schema change | `ALTER TABLE ... ADD/DROP COLUMN` on `ev`/`ev2`, aimed at a table with a prepared transaction open on it |
| inspect | `information_schema.be_tablets`, `be_txns`, `fe_tablet_schedules`: the replica-divergence oracle, and the signals that steer toward rowset pile-up, the publish window and tablet repair |
| observe | `SHOW BACKENDS`, `SHOW PROC '/statistic'` (guides exploration toward dead BEs and unhealthy tablets) |
| eventually | Rarely, and only once the workload has run for `MIN_EVENTUALLY_AGE` (60 s): stops faults, waits for recovery, runs the final checks, ends the timeline |

**Properties** (guarantee assertions):

- *a batch is visible entirely or not at all*; *a batch's rows hold the values the workload wrote*; *a load is never applied twice* (label idempotency under retries)
- *a batch acknowledged visible is visible to later reads*; *a batch seen visible never disappears*
- *a rolled-back transaction never becomes visible*
- *a multi-table transaction is visible in all its tables or none* (one-statement read)
- *a kv read reflects every write acknowledged before it started*; *kv reads return only values the workload wrote*; *a primary key appears at most once in a read*
- *replicas of a tablet agree on the row count at the same version* — compares replicas directly, which every FE-routed read cannot do
- *a row keeps its auto-increment value until it is deleted* — a partial update must not reassign it
- *a pk read reflects every write acknowledged before it started*, per column, so a partial update is judged against the column it wrote
- After faults stop: *the cluster serves reads and writes again*, *a committed transaction becomes visible*, *acknowledged batches / kv writes survive*, *repeated reads agree*, *only batches the workload wrote appear*, *a multi-table transaction is in all its tables or none*

Exploration assertions aimed by `../BUG-HISTORY.md` steer Antithesis toward the states the
project's own fixes cluster on: a schema change starting while a prepared transaction is
open on the same table, a column-mode partial update carrying a merge condition, rowset
pile-up, transactions committed but not yet published, and tablet repair in flight.

The driver also has exploration assertions (`Sometimes`) on rare outcomes: unknown outcomes, publish timeouts, labels already used, BEs seen down, unhealthy tablets.

**Run locally** (no faults): `loads/scripts/local-run.sh [seconds] [eventually_after_secs]`. It prints the assertions that would fail in a run, and keeps logs in `loads/runs/`.

**Launch**: `loads/scripts/launch.sh <max-compute-hours>`. It builds, runs `snouty validate`, and launches with `--fault-tolerance high_availability --fault-exclusions '^driver$'`.

**The eventually must stay rare.** It calls `ANTITHESIS_STOP_FAULTS` and exits the
driver, which ends the timeline. In run `a1def8152144102b8b521a92cc35e1df-67-0` it
fired within seconds in every timeline, so timelines lasted ~15 s of virtual time and
Antithesis injected almost no faults: sampled timelines had zero kills, partitions or
clogs, and every property passed for the wrong reason. `MIN_EVENTUALLY_AGE` and
`EVENTUALLY_P` in `driver/src/main.rs` keep it from firing until faults have had time
to land. When changing them, check fault density in a run's log:
`jq -r 'select(.fault) | .fault.name' log.ndjson | sort | uniq -c`.

**Workarounds and notes**
- The FE rejects Stream Load requests without `Expect: 100-continue`. The driver uses curl (`--location-trusted`) for HTTP.
- **The FE cannot start in Antithesis unpatched.** Antithesis containers keep files in a RAM disk sized at half the machine's 10 GB, so every filesystem reports ~4.9 GB, and the FE refuses to start below bdb-je's 5 GB `je.freeDisk` default. Released branches read that library default directly (only `main` made it configurable), so `loads/fe/patch_je_free_disk.py` lowers the constant in the bundled bdb-je jar to 512 MB. The clean fix is to ask Antithesis to raise the machine's memory; see FINDINGS.md. Both entrypoints log `df`.
- Right after the BEs are added, the FE reports them as "backends without enough disk space" until their first disk report arrives, so `CREATE TABLE` fails. The driver retries DDL until it succeeds.
- Multi-table Stream Load transactions (`transaction_type: multi`) have no prepare step, need the header on commit/rollback too, and their commit can answer `OK` before publish. The driver treats that `OK` as committed, not visible.
