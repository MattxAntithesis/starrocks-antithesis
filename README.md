# StarRocks under deterministic simulation

A [hegel](https://hegel.dev) workload that runs StarRocks in
[Antithesis](https://antithesis.com) and checks its load, transaction and
primary-key guarantees while the simulator kills nodes, partitions the network,
pauses processes, skews clocks and varies CPU speed.

StarRocks itself is unmodified: the FE and BE run from the official
`starrocks/{fe,be}-ubuntu` release images.

## What it found

**[A Stream Load label stops deduplicating once `label_keep_max_num` is exceeded, so an ordinary client retry applies the load twice.](REPORT-label-dedup.md)**
Reproduced on 4.1.7, root-caused, and reproducible without any fault injection.

StarRocks documents the load label as the at-most-once mechanism and documents
its retention as `label_keep_max_second` (3 days). The label index is also
capped by `label_keep_max_num` (1000 per database), which evicts regardless of
age and is enforced in bursts — by the `LoadLabelCleaner` daemon every
`label_clean_interval_second` (4 hours) and by every checkpoint. A restarted FE
enforces it within seconds of becoming ready. In the run below a label that had
been correctly rejected three times was accepted 147 seconds after its
transaction committed, and a five-row load landed twice. No error, no crash,
nothing above INFO in the log.

The report has the full causal chain, the code walk and a standalone
reproduction. Two further findings — a write-path SIGSEGV on 4.1.7 and a
shutdown-path SIGSEGV on 3.5.19 — are in [FINDINGS.md](FINDINGS.md), both
weaker and labelled as such.

[BUG-HISTORY.md](BUG-HISTORY.md) is the analysis that aimed the workload: 833
merged `[BugFix]` PRs reduced to 338 distinct fixes, ranked by backport count,
mapped to the states a test would have to reach to catch them.

## The workload

[`loads/README.md`](loads/README.md) documents the deployment, every action the
driver takes, and every property it checks. In short: `INSERT`, Stream Load via
the FE and directly to a BE, the Stream Load transaction interface, SQL
transactions, primary-key upserts, partial column updates in row and column
mode, schema changes, retries under the same label, and reads — all concurrent,
all checked against a model of what was sent.

Some properties are ones a client cannot check for itself. *Replicas of a
tablet agree on the row count at the same version* compares replicas directly
through `information_schema.be_tablets`, because every FE-routed read sees only
one of them.

## Running it

Locally, without Antithesis or fault injection — this is enough to exercise the
workload and see the assertions:

```bash
loads/scripts/local-run.sh 300
```

In Antithesis (needs `snouty` and a tenant):

```bash
loads/scripts/launch.sh 12 "StarRocks loads/transactions" 90m
```

Change the StarRocks version in `loads/config/.env`, not in the shell.

## Caveats

- Only the Java FE carries Antithesis coverage instrumentation. The C++ BE runs
  from the stock release image, so the search gets no coverage feedback from the
  storage engine and no thread pausing there. An instrumented BE build is the
  single biggest improvement available to this workload.
- The FE will not start in a container whose filesystem reports under 5 GB, so
  `loads/fe/patch_je_free_disk.py` lowers bdb-je's `je.freeDisk` default in the
  bundled jar. StarRocks `main` already makes this configurable via
  `bdbje_free_disk_size`. It is FE-side only and unrelated to any finding.
