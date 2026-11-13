# StarRocks under deterministic simulation

We ran unmodified StarRocks 4.1.7 in [Antithesis](https://antithesis.com) and
checked its load and transaction guarantees while the simulator killed nodes,
cut the network, paused processes, skewed clocks and varied CPU speed.

## What it found

Both bugs came from one 90-minute run against the official 4.1.7 release images.

| | Bug | Impact | Status |
| --- | --- | --- | --- |
| 1 | **A retried load is saved twice** | Silent duplicate data | Root cause found in the code |
| 2 | **All three storage servers crash at once** | Storage outage | Reproduced on demand outside Antithesis |

**1. A retried load is saved twice.** StarRocks promises that resending a load
under the same label is safe: the duplicate is rejected, for up to 3 days. In
practice labels are also capped at 1,000 per database, and the cap ignores
age. A restart (a crash, a failover or a rolling upgrade) enforces the cap within
seconds. In our run, a retry that had been rejected three times was accepted
after the FE restarted, and the data landed twice. There was no error and
nothing in the logs. Full write-up for StarRocks: [REPORT-label-dedup.md](REPORT-label-dedup.md).

**2. All three storage servers crash at once.** Add a column to a table while a
multi-table Stream Load transaction is loading into it, and all three BEs segfault.
No faults are needed. We reproduced it outside Antithesis on every fresh cluster we
tried; ordinary single-table loads with the same column add did not crash. Script:
[`repro/`](repro/).

Details, evidence and why these needed Antithesis: [FINDINGS.md](FINDINGS.md).

## What's in this repo

| File | What it is |
| --- | --- |
| [FINDINGS.md](FINDINGS.md) | Both bugs, the evidence, and why ordinary testing missed them |
| [REPORT-label-dedup.md](REPORT-label-dedup.md) | Bug 1 written up for StarRocks engineers: causal chain, code walk, reproduction steps |
| [BUG-HISTORY.md](BUG-HISTORY.md) | How we chose what to test: 338 past StarRocks bug fixes, ranked by how often they were backported |
| [`loads/`](loads/README.md) | The test workload: deployment, driver, and every property it checks |
| [`repro/`](repro/) | Standalone reproduction of bug 2: no Antithesis, no faults |

## The workload

A [hegel](https://hegel.dev) driver runs loads concurrently and checks every read
against a model of what it sent. It covers `INSERT`, Stream Load via the FE and
directly to a BE, the Stream Load transaction interface, SQL transactions,
primary-key upserts, partial column updates, schema changes and retries under
the same label.

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

- **Only the FE was instrumented.** The C++ BE runs from the stock image, so
  Antithesis gets no coverage feedback or thread pausing inside the storage
  engine. An instrumented BE build would be the biggest single improvement.
- **One FE patch, for startup only.** The FE won't start when the filesystem
  reports under 5 GB, which Antithesis containers do. `loads/fe/patch_je_free_disk.py`
  lowers bdb-je's `je.freeDisk` default; StarRocks `main` already exposes this as
  `bdbje_free_disk_size`. It has nothing to do with either bug.
