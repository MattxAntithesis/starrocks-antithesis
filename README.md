# StarRocks under deterministic simulation

We ran unmodified StarRocks 4.1.7 in [Antithesis](https://antithesis.com) and
checked its load and transaction guarantees while the simulator killed nodes,
cut the network, paused processes, skewed clocks and varied CPU speed.

## What it found

All six bugs were found in the official 4.1.7 release images.

| | Bug | Impact | Status | Known upstream? |
| --- | --- | --- | --- | --- |
| 1 | **A retried load is saved twice** | Silent duplicate data | Root cause found and reproduced outside Antithesis | No report or fix found |
| 2 | **All three storage servers crash at once** | Storage outage | Root cause found and reproduced outside Antithesis | Yes: fixed after 4.1.7 (#82219, #82329) |
| 3 | **One query sees a transaction half-applied** | Inconsistent results, which can be stored | Root cause found and reproduced outside Antithesis | No report or fix found for the core cause |
| 4 | **A compute node crashes while shutting down** | Crash on restart, in-flight publishes fail | Root cause found; reproduced outside Antithesis with a debugger widening the race | No report or fix found |
| 5 | **`OPTIMIZE` deletes a row the database just confirmed** | Silent data loss | Root cause found; reproduced outside Antithesis with a debugger widening the race | Yes: fixed after 4.1.7 (#77754), not yet released |
| 6 | **An "only if newer" update overwrites newer data** | Silent stale overwrites | Root cause found and reproduced outside Antithesis | Yes: fixed after 4.1.7 (#82370), not yet released |

**1. A retried load is saved twice.** StarRocks promises that resending a load
under the same label is safe: the duplicate is rejected, for up to 3 days. In
practice labels are also capped at 1,000 per database, and the cap ignores
age. A restart (a crash, a failover or a rolling upgrade) enforces the cap within
seconds. In our run, a retry that had been rejected three times was accepted
after the FE restarted, and the data landed twice. There was no error and
nothing in the logs. Reproduced outside Antithesis ([`repro/label_dedup.sh`](repro/label_dedup.sh)).
Full write-up for StarRocks: [REPORT-label-dedup.md](REPORT-label-dedup.md).

**2. All three storage servers crash at once.** Add a column to a table while a
multi-table Stream Load transaction is loading into it, and all three BEs segfault.
No faults are needed. We reproduced it outside Antithesis on every fresh cluster we
tried. The FE plans the load against two versions of the table schema at once.
StarRocks found and fixed the same crash independently after 4.1.7 was released
([#82219](https://github.com/StarRocks/starrocks/pull/82219), backported to 4.1 in
[#82329](https://github.com/StarRocks/starrocks/pull/82329)). Script: [`repro/`](repro/).

**3. One query sees a transaction half-applied.** StarRocks promises that a
transaction writing to several tables becomes visible all at once. A query reading
two of those tables while the transaction commits can see it in one and not the
other, because each table's version is picked separately, without a lock. About
1 read in 440 under a steady write load. Inside an `INSERT ... SELECT`, the
half-applied state is saved to a table for good. Scripts:
[`repro/torn_read.sh`](repro/torn_read.sh), [`repro/torn_persist.sh`](repro/torn_persist.sh).

**4. A compute node crashes while shutting down (shared-data).** Stopping a compute
node while it has publish work queued can segfault it. A cancelled task signals
"done" while still holding a lock that belongs to the waiting request, which frees
the lock before it is released. Rare, and reproduced with a debugger holding the
window open. Script: [`repro/cn_shutdown_publish.sh`](repro/cn_shutdown_publish.sh).

**5. `OPTIMIZE` deletes a row the database just confirmed.** An `INSERT` acknowledged OK in the
last few milliseconds before an `ALTER TABLE ... OPTIMIZE` finishes can be lost, with the load
still recorded as successful. Before swapping in the rewritten partition, `OPTIMIZE` checks for
new data, but only data that is fully published. A load that has committed but not yet published
slips past, and the swap deletes it. No fault needed. Reproduced outside Antithesis with a
debugger holding the load at that point: 3 of 3 lost the row
([`repro/optimize_loss_jdb.sh`](repro/optimize_loss_jdb.sh)). Another user reported it
([#80731](https://github.com/StarRocks/starrocks/issues/80731)) and StarRocks fixed it after 4.1.7
([#77754](https://github.com/StarRocks/starrocks/pull/77754)); no 4.1 release has the fix yet.

**6. An "only if newer" update overwrites newer data.** A load can carry a merge condition so it
applies only when its version is at least the stored one. On standard (shared-nothing) tables,
4.1.7 accepts such a load in column-mode partial-update form, which the docs say fails, and
applies it unconditionally: an update carrying version 5 replaced a row at version 10, 3 of 3
times, while the same update in row mode was correctly ignored
([`repro/column_merge.sh`](repro/column_merge.sh)). Found by checking an Antithesis run's
behaviour against the docs. StarRocks fixed it after 4.1.7
([#82370](https://github.com/StarRocks/starrocks/pull/82370)); no 4.1 release has the fix yet.

Details, evidence and why these needed Antithesis: [FINDINGS.md](FINDINGS.md).

## What's in this repo

| File | What it is |
| --- | --- |
| [FINDINGS.md](FINDINGS.md) | All six bugs, the evidence, and why ordinary testing missed them |
| [REPORT-label-dedup.md](REPORT-label-dedup.md) | Bug 1 written up for StarRocks engineers: causal chain, code walk, reproduction steps |
| [BUG-HISTORY.md](BUG-HISTORY.md) | How we chose what to test: 338 past StarRocks bug fixes, ranked by how often they were backported |
| [`loads/`](loads/README.md) | The test workload: deployment, driver, and every property it checks |
| [`repro/`](repro/) | Standalone reproductions of all six bugs: no Antithesis, no faults |

## The workload

A [hegel](https://hegel.dev) driver runs loads concurrently and checks every read
against a model of what it sent. It covers `INSERT`, Stream Load via the FE and
directly to a BE, the Stream Load transaction interface, SQL transactions,
primary-key upserts, partial column updates, schema changes and retries under
the same label. It also covers partitions being overwritten and truncated under
load, table rewrites (`OPTIMIZE`, sort-key changes, compaction) during loads, an
async materialized view, and the query-cache and short-circuit read paths. It runs
on four deployments: a single FE, three FEs with leader failover, shared-data mode
on S3 storage, and shared-data with three FEs.

It also checks query answers: random filters, aggregates and joins over fixed tables,
each compared with an answer the driver computes itself, under randomized session
settings.

It also checks something a normal client can't: that replicas of a tablet agree
on their row count. It reads them directly through
`information_schema.be_tablets`, because a normal query only ever sees one
replica.

## Running it

Locally, without fault injection (enough to exercise the workload and its assertions):

```bash
loads/scripts/local-run.sh 300
```

In Antithesis (needs `snouty` and a tenant):

```bash
loads/scripts/launch.sh 12 "StarRocks loads/transactions" 90m
```

To change the StarRocks version, edit `loads/config/.env`.

## Caveats

- **The instrumented BE is optional.** Runs before Finding 5 used the stock BE, so
  Antithesis had no coverage feedback or thread pausing inside the storage engine.
  [`loads/be/instrumented-build/`](loads/be/instrumented-build/) builds an
  instrumented `starrocks_be`; uncomment `BE_IMAGE`/`BE_DOCKERFILE` in a config's `.env`
  to use it. The binary is too large for git, so it isn't included.
- **One FE patch, for startup only.** The FE won't start when the filesystem
  reports under 5 GB, which Antithesis containers do. `loads/fe/patch_je_free_disk.py`
  lowers bdb-je's `je.freeDisk` default; StarRocks `main` already exposes this as
  `bdbje_free_disk_size`. It has nothing to do with any of the bugs.
