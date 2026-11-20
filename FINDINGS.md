# Findings: StarRocks 4.1.7

We found six bugs in StarRocks 4.1.7, all against the official release images
(`starrocks/{fe,be}-ubuntu:4.1.7`, build `b81ff9c`). Findings 1 and 2 came from one 90-minute
Antithesis run (`c7d09923c0006169e56a8e0b37ee9c3a-67-0`). Finding 3 came from a later run that
added partition, table-rebuild and multi-table read checks (`05d22732e178b35d79c3fab4f7bc4287-67-0`).
Finding 4 came from a shared-data run (`6efc9d648a0c3c0bf66fc45cc9a085f7-67-6`). Finding 5 came
from the first run with an Antithesis-instrumented BE (`49b70483ef11574867ea0dbfe3415d47-67-6`), and
Finding 6 from that same run together with a review of our checks against the StarRocks docs.

| # | What goes wrong | Impact | How solid | Known upstream? |
| --- | --- | --- | --- | --- |
| 1 | A retried load is saved twice | Silent duplicate data | Root cause found and reproduced outside Antithesis | No report or fix found |
| 2 | All three storage servers crash at once | Storage outage | Root cause found and reproduced outside Antithesis | Yes: fixed after 4.1.7 (#82219, #82329) |
| 3 | One query sees a transaction half-applied | Inconsistent results, which can be stored | Root cause found and reproduced outside Antithesis | No report or fix found for the core cause |
| 4 | A compute node crashes while shutting down | Crash on restart, in-flight publishes fail | Root cause found; reproduced outside Antithesis with a debugger widening the race | No report or fix found |
| 5 | `OPTIMIZE` deletes a row the database just confirmed | Silent data loss | Root cause found; reproduced outside Antithesis with a debugger widening the race | Yes: reported in #80731, fixed after 4.1.7 in #77754, not yet in a release |
| 6 | An "only if newer" update overwrites newer data | Silent stale overwrites | Root cause found and reproduced outside Antithesis | Yes: fixed after 4.1.7 in #82370, not yet in a release |

---

## Finding 1: a retried load can be saved twice

**In one line:** after a restart, StarRocks forgets which loads it has already applied,
so a client's ordinary retry writes the same data a second time. Nothing errors, and the
totals just come out too high.

**Why it matters.** Every load carries a label, and StarRocks documents the label as its
at-most-once guarantee: resend under the same label and the duplicate is rejected. The
Flink, Spark and Kafka connectors all rely on this to make retries safe. StarRocks says it
keeps labels for 3 days (`label_keep_max_second`).

**What actually happens.** Labels are also capped at 1,000 per database
(`label_keep_max_num`), and that cap evicts labels whatever their age. It is enforced by
the label cleaner, which runs every 4 hours, on every checkpoint, and within about a second
of an FE starting. After a restart (a crash, a failover or a rolling upgrade), the
deduplication window drops from 3 days to the last 1,000 loads. On a busy cluster that is
minutes.

**What we saw** (one timeline, virtual seconds):

```
 26.5   load "b4_stream_load_be" commits (txn 32)    client never got the reply, so it retries
 75–82  retry rejected 3 times: "Label has already been used"     dedup working
131.4   FE killed, restarts
175.4   label cleaner evicts txn 32 (147 s old) to get under the 1,000 cap
278.7   retry accepted (txn 8227), commits, becomes visible
299.7   assertion fails: "a load is never applied twice"
```

The batch's row count, `sum(seq)` and `sum(val)` were all exactly doubled, so every row
was there twice. Three properties failed on this one event: *a load is never applied
twice*, *a batch acknowledged visible is visible to later reads* and *acknowledged batches
survive once faults stop*.

**Cause.** `DatabaseTransactionMgr.removeExpiredTxns` drops the oldest finished
transactions when the count exceeds `label_keep_max_num`, and `clearTransactionState`
removes their labels from `labelToTxnIds`, which is the only thing `beginTransaction`
checks. Restart recovery itself is correct. The restart only triggers the cleaner.

**Reproduced outside Antithesis.** [`repro/label_dedup.sh`](repro/label_dedup.sh) starts a plain
4.1.7 cluster with `label_keep_max_num = 2` and replays the sequence. The retry is rejected until
the FE restarts. After the restart it is accepted, and the same 5 rows are stored twice:

```
1. load under label L:           Success                  rows: 5
2. retry under label L:          Label Already Exists
3. three more loads (M N O)                               rows: 20
4. retry under L (no restart):   Label Already Exists
5. restart FE -> FE log: "transaction list [2, 4] are expired, remove them"
6. retry under L after restart:  Success                  rows: 25
```

**Not yet done:** reporting it to StarRocks. Seen on single-FE, shared-nothing tables only.

Full write-up for StarRocks, with code walk and reproduction steps:
[REPORT-label-dedup.md](REPORT-label-dedup.md).

---

## Finding 2: adding a column during a multi-table load crashes every storage server

**In one line:** if a column is added to a table while a multi-table Stream Load
transaction is loading into it, all three BEs segfault at once. That is a full outage of
the storage layer, set off by two ordinary operations. No faults are needed.

**Reproduced outside Antithesis.** On a plain Docker host with the official 4.1.7 images,
the trigger crashed all three BEs in every fresh cluster we tried. The controls show it
needs both parts together:

| What we ran (each with a concurrent `ALTER TABLE ... ADD COLUMN`, unless noted) | Result |
| --- | --- |
| Multi-table Stream Load transaction, column added during the load | **All 3 BEs crash**, 3 of 3 clean clusters, on the 2nd attempt |
| Same transaction, no column added | No crash, 160 attempts |
| Single-table Stream Load transaction interface | No crash, 190 attempts |
| Ordinary single-table Stream Load | No crash, 160 attempts |

So the exposure is narrower than "any load": it takes the multi-table transaction
(`transaction_type: multi`, new in 4.0) and a schema change on one of its tables at the
same moment.

**The sequence** (from the original Antithesis run, virtual seconds):

```
24.275  multi-table txn "b1_txn_stream_load" (txn 4) starts its load into ev
24.284  ALTER TABLE ev ADD COLUMN pad1 INT  -> applied as fast schema evolution
24.301  ev load starts sending data, built before the column existed
24.326  commit
24.44   be3, be1, be2: SIGSEGV, same stack, within 10 ms
```

```
SIGSEGV (@0x0)  FixedLengthColumnBase<int>::append_selective
                Chunk::rolling_append_selective
                MemTable::_sort → MemTable::finalize
                DeltaWriter::flush_memtable_async → DeltaWriter::close
                AsyncDeltaWriter::_execute
```

The column added is an `INT`, and the crash is in the `int` column code. Our local
reproduction follows the same order and dies with the same stack.

**Cause: the FE plans the load against two versions of the table.** Confirmed by
attaching gdb to a crashing BE and pausing the FE's planner with a Java debugger.
Code references are to the 4.1.7 tag:

1. A transaction Stream Load is planned with no table lock. `LoadPlanner.plan()` builds the
   load's column slots from the schema as it is then (`destColumns = destTable.getFullSchema()`),
   four columns.
2. The column add lands next. Fast schema evolution changes the table's schema in place.
3. The plan's sink then reads the schema again, under its own short lock, and sends the BEs
   five columns and the new schema version, alongside slots for only four.
4. The BE drops the column that has no slot (`OlapTableSchemaParam::init`, `tablet_info.cpp`)
   but builds its memtable five columns wide. `MemTable::insert` fills only the four slotted
   columns, so the new one stays empty.
5. Sorting the memtable reads the empty column's data pointer, which is null. That is the
   `SIGSEGV (@0x0)` on every replica, since all three get the same plan.

Pausing the FE planner right after step 1 and adding the column crashes all three BEs on the
first try. Pausing it before step 1, or adding nothing, does not. Plain and single-table
transaction loads without channel headers are planned under the table lock instead, which is
why they did not crash.

**Already fixed upstream, after 4.1.7.** StarRocks found the same crash independently, in
shared-data mode, and fixed it in
[#82219](https://github.com/StarRocks/starrocks/pull/82219) (main), backported to the 4.1
branch in [#82329](https://github.com/StarRocks/starrocks/pull/82329). Both merged on
2026-11-03, after 4.1.7 was tagged on 2026-10-22. The fix plans the load under the table lock,
and makes the BE reject a load whose slots don't match its schema. 4.1.7 is affected; the
next 4.1 patch release should not be.

**Antithesis had the trigger from the start.** The column add is in the log of the
original run (`c7d09923…`, moment `-8215334601661802087`) on 2026-11-10. We noticed it on
2026-11-13, when a local run of the same workload crashed the same way.

**Reproduce it:** [`repro/`](repro/). Build the images with
`cd loads/config && docker compose build`, then run
`MODE=multi ALTER_AT=during repro/run.sh 20`. It starts a fresh 1-FE, 3-BE cluster,
repeats the transaction with a concurrent column add, and prints the BE crash stacks.

**Nothing to report:** the fix is already merged (above). Older 4.x releases were not checked.

---

## Finding 3: one query can see a committed transaction half-applied

**In one line:** a query that reads two tables can see a multi-table transaction in one table
but not the other. That happens when the transaction becomes visible while the query is
being planned. Nothing is lost, but that query's answer matches no real state of the
database.

**Why it matters.** StarRocks documents multi-table transactions as atomic. SQL transactions
are "applied or undone together" (`docs/en/loading/SQL_transaction.md`), and multi-table Stream
Load data "becomes visible atomically — all or nothing"
(`docs/en/loading/Flink_multi_table_transaction.md`). A report that joins or compares two tables
can come out inconsistent for that one query: an order without its line items, or two ledgers
that don't balance. Rerunning the query gives the right answer, so it looks like a glitch nobody
can explain.

**Reproduced outside Antithesis.** One client commits SQL transactions that insert the same rows
into tables `a` and `b`. Others repeatedly run
`SELECT (SELECT count(*) FROM a WHERE v > 0), (SELECT count(*) FROM b WHERE v > 0)`.
On 4.1.7, with no faults and one replica, about 1 read in 440 returns different counts. The
totals always match in the end.

```
FILTER=1 REPL=1 repro/torn_read.sh 90 32 4
```

In one 90-second run of this command, 5 of 2,205 reads were torn. In every torn read, the
table planned first was one transaction behind. With the subquery order swapped, the one torn
read we got lagged in the other table.

**The wrong answer can be stored, not just shown.** When the torn read happens inside an
`INSERT INTO ... SELECT`, the half-applied state is written to a table and stays there. In a
3-minute run of [`repro/torn_persist.sh`](repro/torn_persist.sh), 4 clients ran
`INSERT INTO snap SELECT (SELECT count(*) FROM a WHERE v > 0), (SELECT count(*) FROM b WHERE v > 0)`
while transactions committed. 3 of the 2,813 stored rows hold counts that never existed
together (for example 236 and 238). So any job that copies from several tables, such as an
ETL step, a summary table or a reconciliation snapshot, can save inconsistent data with no
error. That this goes through the same planner path is inferred from the identical
signature (b one transaction ahead); we did not instrument this variant.

**Cause (4.1.7 source, confirmed by instrumenting the running FE).**

1. *Commit is atomic.* `DatabaseTransactionMgr.finishTransaction` holds write locks on every
   table in the transaction while it bumps each partition's visible version.
2. *Reads take no snapshot of those versions.* Queries are planned lock-free.
   `OlapTable.copyOnlyForQuery` copies the tables under a lock, then releases it, but
   `Partition.shallowCopy` keeps pointing at the live `PhysicalPartition` objects.
3. *Each table's version is read later, separately.* `OlapScanNode.addScanRangeLocations` reads
   `physicalPartition.getVisibleVersion()` when the plan builder reaches that scan, with no lock
   held. A commit landing between the scan for `a` and the scan for `b` gives `a` at version N
   and `b` at N+1.
4. *Nothing notices.* The post-planning check (`OptimisticVersion.validateTableUpdate`) looks
   only at schema changes, not at data commits.

A probe on the live FE caught one query in the act:

```
READV  query 3bc1  table=a  version=5
FIN_BEGIN txn 4245 ; version a 5->6 ; version b 5->6 ; FIN_END
READV  query 3bc1  table=b  version=6
```

The workaround one would try, `SET cbo_use_lock_db = true`, does not help this query shape. The
planner builds its lock list before analysis, when subqueries in the SELECT list are still
invisible to it, so it locks nothing. That collector gap is already reported upstream for
another reason (issue #82969, PR #82970, both open). The missing version snapshot is not, as
far as we found.

**Also seen in Antithesis**, with a multi-table Stream Load transaction: the read began 2 ms
before the transaction became visible and saw 3 rows in `ev` and none in `ev2`.

**Shared-data mode is affected too, and more often.** On a local shared-data cluster (S3
storage, three compute nodes; `SHARED=1 FILTER=1 repro/torn_read.sh 180 16 4`), 47 of 4,544
reads were torn, about 1 in 97, against about 1 in 440 on shared-nothing. Antithesis also hit it
in a shared-data run (`6efc9d648a0c3c0bf66fc45cc9a085f7-67-6`): the query started 4 ms before
the transaction became visible, and saw 0 rows in `ev` and 4 in `ev2`. We believe the higher
rate comes from slower planning in shared-data, which widens the window, but we have not
measured it.

**Not yet done:** reporting it to StarRocks, and testing whether a materialized view refresh can
store the half-applied state the way `INSERT ... SELECT` does.
We also saw a single
plain INSERT appear half-applied across two partitions of one table, once in about 8,500
reads. The mechanism allows it, but one sighting is not enough to claim it.

---

## Finding 4: a compute node can crash while shutting down (shared-data)

**In one line:** when a compute node is stopped normally while it has publish work queued, it
can segfault instead of exiting. Publishing is the step that makes newly loaded data visible.

**What happens.** On shutdown the CN cancels the tasks still waiting in its publish thread pool.
Each cancelled task's callback takes a lock, records the failure, and then signals "done" to the
request handler waiting on all the tasks. The lock and the "done" counter live in that handler's
stack frame. When the last task signals "done", the handler wakes, returns, and its frame (lock
included) is gone. The callback then releases the lock it is still holding, which is freed
memory, and the process segfaults.

```
SIGSEGV (@0x0)  bthread::butex_wake
                bthread_mutex_unlock
                LakeServiceImpl::_submit_publish_log_version_task(...)::{lambda}   <- cancel callback
                ThreadPool::_pop_and_cancel_tasks_in_queue
                ThreadPool::shutdown
                AgentServer::Impl::stop
                ExecEnv::stop
                start_be
```

**The code** (`be/src/service/service_be/lake_service.cpp`, `_submit_publish_log_version_task`,
4.1.7 and unchanged on `main`). The cancel callback signals while still holding the lock:

```cpp
[&] {                                   // cancel callback
    ...
    std::lock_guard l(response_mtx);    // response_mtx and latch are the handler's locals
    response->add_failed_tablets(tablet_id);
    latch.count_down();                 // may release the handler, which frees both
});                                     // lock_guard now unlocks freed memory
```

The normal path gets the order right: its `count_down()` runs from a `DeferOp` declared before
the lock, so it happens after the lock is released.

**When it happens.** These tasks (`publish_log_version`) are sent when loads commit into a
shared-data table with a schema change in progress. So the trigger is a CN being stopped or
restarted (rolling restart, scale-in, upgrade) while loads and a schema change are running, and
while publish work is backed up, for example because object storage is slow. The crash itself
needs the waiting handler to free its frame in the few instructions between the "done" signal
and the unlock, so it is rare.

**Reproduced outside Antithesis, with help.** [`repro/cn_shutdown_publish.sh`](repro/cn_shutdown_publish.sh)
starts a shared-data cluster with a one-thread publish pool. It pins a schema change, runs loads,
pauses the S3 store so publish tasks queue, and then stops the CN.
- With gdb pausing the callback right after `count_down()` and before the unlock
  ([`repro/debug/l6_widen.gdb`](repro/debug/l6_widen.gdb)): **5 of 5 stops crash**, with the
  same stack and the same addresses as the Antithesis crash.
- Control, pausing just before `count_down()`: 0 of 3.
- With no debugger: 0 of 15 stops with up to 60 queued tasks. The window is too narrow to hit by
  chance on a two-core VM. Antithesis reached it by exploring thread timing.

**Impact.** The node was stopping anyway. It ends in a segfault instead of a clean exit, and the
publishes it was cancelling report failure, so those loads must be retried. In the run that found
it, all data checks passed once faults stopped. We saw no data effect, but have not proven there is
none.

**Not yet done:** reporting it to StarRocks. This is the same family as a 3.5.19 shutdown crash we
saw earlier (a publish task touching torn-down state during BE shutdown), but different code.

---

## Finding 5: `ALTER TABLE ... OPTIMIZE` can delete a row the database just confirmed

**In one line:** an `INSERT` that is acknowledged OK in the last few milliseconds before an
`OPTIMIZE` finishes can be silently lost. No error, and the load is recorded as successful.

**What happens.** `OPTIMIZE` (changing a table's bucket count, sort key or distribution) copies
each partition into a temporary partition, then swaps the copy in and deletes the original. Loads
are allowed to keep running meanwhile. Just before the swap, the job checks whether a partition
received new data during the copy, and if so skips that partition. But it only checks the
partition's *visible* version, which moves only once a load is fully published. A load that has
committed but not yet published, a gap of a few milliseconds, passes the check. The swap then
deletes the original partition, data included. When the load's publish runs, its partition no
longer exists. The FE logs `partition ... is dropped, skip version check`, drops that partition
from the transaction and marks the transaction VISIBLE. The client gets OK; the row is gone.

**The code** (`fe/fe-core/src/main/java/com/starrocks/alter/OptimizeJobV2.java`, 4.1.7):

- `onFinished` compares each source partition's visible version with the version the rewrite
  started from. A committed-but-unpublished load leaves the visible version unchanged.
- The committed-but-not-visible guard (`hasCommittedNotVisible`) is applied only to the
  *temporary* partition, never to the source partition being replaced.
- `DatabaseTransactionMgr.finishTransaction` skips partitions that have been dropped instead of
  failing the transaction.

With the default `enable_online_optimize_table = false`, every `OPTIMIZE` in 4.1.7 runs this job.

**Seen in Antithesis.** The workload inserted one row into partition `p3`, labelled `pt_247`. The
FE committed it, and 50 ms later an `OPTIMIZE` changing the bucket count swapped `p3` for its
copy. The publish then logged `partition 12254 is dropped, skip version check` and made the
transaction VISIBLE. The `INSERT` returned OK, and the next read of `p3` didn't have the row. No
fault was involved at that moment.

**Reproduced outside Antithesis, with help.**
[`repro/optimize_loss_jdb.sh`](repro/optimize_loss_jdb.sh) starts a 1-FE/3-BE cluster on the
stock 4.1.7 images. It starts an `OPTIMIZE`, waits for the copy to finish, and then uses jdb to
pause only the FE's publish thread while one `INSERT` commits. The `OPTIMIZE` finishes and the
publish is released.
- **3 of 3 runs lost the acknowledged row**, with the same `partition ... is dropped` log line as
  in Antithesis.
- Control (`CONTROL=1`, the same pause with no `OPTIMIZE`): the row is there.
- With no debugger ([`repro/optimize_loss.sh`](repro/optimize_loss.sh)): 0 rows lost across 15
  `OPTIMIZE` runs and about 24,000 inserts. Continuous inserts trip the version check and the
  partition is skipped; a loss needs a load to commit in the milliseconds before the swap.

**Impact.** Silent loss of acknowledged rows, with the load recorded as successful. Only a row
count or checksum against the source reveals it. It needs no fault, just loads running while an
`OPTIMIZE` finishes. Each occurrence loses the loads caught in that window, not the table.

**Already fixed upstream, after 4.1.7.** Reported by another user in
[#80731](https://github.com/StarRocks/starrocks/issues/80731) (2026-10-14), from a stress test where
row counts came up short while every load reported FINISHED. Fixed in
[#77754](https://github.com/StarRocks/starrocks/pull/77754) (merged 2026-11-02), which checks
committed and in-flight transactions per source partition before the swap, and backported to the
4.1 branch. 4.1.7 (released 2026-11-03) doesn't include it, and no 4.1 release does yet. We worked
out the cause from the run and the 4.1.7 source before finding the fix; its description matches.

---

## Finding 6: an "only if newer" update can overwrite newer data with older data

**In one line:** on a standard (shared-nothing) primary-key table, a column-mode partial update
with a merge condition is accepted and applied unconditionally, so an older row overwrites a
newer one. The load reports `Success`.

**Why it matters.** A merge condition (`merge_condition`) is how a pipeline makes updates "apply
only if newer": the update is applied only when its value in the condition column (a version or
timestamp) is greater than or equal to the stored one. It protects fresh data from late, replayed
or out-of-order updates. StarRocks documents that column-mode partial updates can't be combined
with it on shared-nothing tables: "In shared-nothing clusters, partial updates in column mode
cannot be used with conditional updates, and such a load fails"
(`docs/en/loading/Load_to_Primary_Key_tables.md`). In 4.1.7 the load doesn't fail. It succeeds and
ignores the condition.

**Reproduced outside Antithesis, no debugger.** [`repro/column_merge.sh`](repro/column_merge.sh)
starts a 1-FE/3-BE cluster on the stock 4.1.7 images. Table `t (k, v, s, ver)`, both rows at
`v = 100, ver = 10`. Two partial updates of `k, v, ver`, each carrying the older `ver = 5`, with
`merge_condition: ver`:

```
before:                       1 100 orig 10; 2 100 orig 10
row mode,    key 1, ver 5:    Success
column mode, key 2, ver 5:    Success
after:                        1 100 orig 10; 2 6 orig 5     <- key 2 went back to ver 5
```

3 of 3 runs, identical. Row mode (the control) correctly ignores the older update every time.
The update must leave at least one column out (`s` here); an update of every column takes the
full-row path, which applies the condition correctly.

**Cause** (4.1.7 source). No check rejects the combination: the FE's `Load.checkMergeCondition`
doesn't look at the partial-update mode, and neither does the BE's `DeltaWriter`. The BE then
applies a column-mode rowset with `TabletUpdates::_apply_column_partial_update_commit`, which,
like the `RowsetColumnUpdateState` behind it, never reads the merge condition. Only the row-mode
path (`_apply_normal_rowset_commit`) does. Shared-data tables have their own column-mode handler
that does evaluate the condition (supported there from v4.1.1).

**How it was found.** An Antithesis run logged column-mode loads with a merge condition
succeeding on a shared-nothing cluster. Our review of the workload's checks against the StarRocks
documentation then flagged that, per the docs, those loads should have failed. A direct test
showed what they actually did. Our own data check couldn't catch it: it treats any conditional
update as "either value is acceptable".

**Impact.** Silent overwrites of newer data with older data, for anyone combining column-mode
partial updates with a merge condition on shared-nothing tables. No error, and the stale row
looks like any other.

**Already fixed upstream, after 4.1.7.** StarRocks fixed it on 2026-11-05 in
[#82370](https://github.com/StarRocks/starrocks/pull/82370) ("Reject column-mode partial update with
merge_condition on shared-nothing tables"), backported to the 4.1 branch. The BE now refuses the
load before writing any data. 4.1.7 (2026-11-03) doesn't have it, and no 4.1 release does yet.

---

## Why these bugs needed Antithesis

All six bugs are in code StarRocks already tests heavily. Here is why ordinary methods
missed them, and what was different here.

**1. The duplicate-data bug is invisible to normal testing.** No error, no crash and no
warning in the logs. A unit test checks one function, and this needs four things to go
wrong in order: a lost reply, a restart, more than 1,000 loads, and then a retry. An
integration test doesn't inject the lost reply or the restart. Production monitoring
watches for errors, and there are none. The only symptom is that the totals are too high.
Antithesis got there because it injects real faults (kills, partitions, pauses) across
the whole cluster and steers toward rare situations. One of our assertions asked it to
find "a retried load whose label was already used", and it focused its search on retries
under faults.

**2. It found a crash that needs two operations to overlap, and kept the evidence.**
The storage crash needs a column added in the fraction of a second while a multi-table load
is in flight. An earlier 15-minute round of ordinary stress testing never hit it. Antithesis did, because its workload mixes every kind of operation
concurrently and varies their timing. Because the run is deterministic, the exact moment can
be replayed identically. We replayed it to identify the load, the tables and the thread,
and the run's single ordered log also held the column add that turned out to be the
trigger. With that, the bug went from "seen once" to a short script anyone can run.
StarRocks' own testing also found this crash and fixed it after 4.1.7 shipped. Antithesis
found it independently, from the released images, with no knowledge of that fix.

**3. It caught a wrong answer that raises no error.** The half-applied transaction in
Finding 3 shows up as one query, among thousands, returning numbers that don't add up. There
is no error, no log line and no crash. The workload checks every read against what it wrote,
including reads that compare two tables in one statement, and that is what flagged it. The
run's single ordered log then showed the query starting 2 ms before the transaction became
visible, which pointed straight at the planner.

**4. It found a data loss that lasts milliseconds.** Finding 5 needs a load to commit in the few
milliseconds between an `OPTIMIZE`'s last check and its swap. Our own stress test of the same
operations, 24,000 inserts across 15 `OPTIMIZE` runs, never lost a row. Antithesis hit it in a
2.5-hour run, and its ordered log showed the commit, the swap 50 ms later and the publish that
skipped the deleted partition, which is the whole causal chain.

**5. One exact story across every machine.** Normally you piece a cluster bug together
from separate logs on separate machines, with clocks that disagree. Antithesis gives a
single timeline of the FE, all three BEs, the client and every injected fault, in exact
order. That is how we could say the label was deleted 1 second after the restart and 147
seconds after the load, and connect it straight to the line of code responsible.

**Replaying a moment with changes.** Any moment of a run can be reopened and run forward
differently, for example without a fault or without a concurrent operation. That is the tool
for bugs that won't reproduce outside the simulator. All six here did reproduce outside it (two
with a debugger holding the race open),
so the standalone scripts in [`repro/`](repro/) are the evidence.

---

## Limits of this testing

- **The FE's own code was instrumented in every run** (all FE JARs; Antithesis lists them under the
  name of the one with a `Main-Class`, the bdb-je library). **The BE was instrumented only from Finding 5's run on.** Earlier runs used the stock BE, so
  Antithesis had no coverage feedback or thread pausing inside the storage engine. The
  instrumented build ([`loads/be/instrumented-build/`](loads/be/instrumented-build/)) gives it
  about 4.7 million coverage points in `starrocks_be`.
- **A passing property means only what the run reached.** See `loads/README.md` for the
  fault-density check that guards against tests passing because no faults happened.
- **Setup workaround.** Antithesis containers report about 4.9 GB of disk, and the FE
  refuses to start below 5 GB. We patched bdb-je's `je.freeDisk` in the FE jar
  (`loads/fe/patch_je_free_disk.py`). This only affects startup and is unrelated to any
  finding. Asking Antithesis for more machine memory would remove the need for the patch.
