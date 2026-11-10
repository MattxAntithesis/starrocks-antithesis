# Stream Load label deduplication is silently bounded by `label_keep_max_num`, so an ordinary client retry can apply a load twice

**Affects:** StarRocks 4.1.7 (release image `starrocks/{fe,be}-ubuntu:4.1.7`, build `b81ff9c`). Code paths unchanged on `main`.
**Class:** silent data duplication. No error, no crash, no log at WARN or above on the duplicating path.
**Found by:** deterministic simulation run under Antithesis, run `c7d09923c0006169e56a8e0b37ee9c3a-67-0`, timeline `-866572852979010910`.

---

## 1. Summary

StarRocks documents the load label as the at-most-once mechanism:

> "The Stream Load transaction interface carries over the labeling mechanism of StarRocks. You can bind a unique label to each transaction to achieve at-most-once guarantees."
> — `docs/en/loading/Stream_Load_transaction_interface.md`

and documents the retention of that label as `label_keep_max_second`, default **3 days**.

In fact the label index is also capped by `label_keep_max_num`, default **1000 per database**. When the cap is enforced, the oldest finished transactions are dropped from `labelToTxnIds` regardless of age, and their labels become reusable. A client that retries under such a label — the exact behaviour the label exists to make safe — gets a fresh transaction and the data is applied a second time.

Enforcement is not continuous. It happens only when `clearExpiredJobs()` runs, which is:

- the `LoadLabelCleaner` daemon, every `label_clean_interval_second` — **default 4 hours**; and
- `GlobalStateCheckpointWorker`, on every checkpoint.

A freshly started FE runs the cleaner within a second or two of becoming ready. So **any FE restart — crash, failover, rolling upgrade — immediately collapses the deduplication window from three days to the most recent 1000 transactions.** On a cluster doing a few thousand loads an hour, that is minutes of history, not days.

We observed a label that had been correctly rejected three times become accepted **147 seconds after its transaction committed**, and the five-row load was applied twice.

## 2. Why this matters

- Every connector and custom loader that retries on timeout relies on the label. The Flink, Spark and Kafka paths all do.
- The client cannot tell the two cases apart. "Label never used" and "label used, then forgotten" both answer `OK` and begin a new transaction. There is no error for the application to handle and no way to make the retry safe from the client side.
- The trigger is mundane. A rolling upgrade restarts every FE by design.
- It is silent. `finishTransaction` logs at INFO like any other successful load. The only evidence is the data.
- Reads diverge from the write history without any node being down, any replica diverging, or any transaction failing. Every internal consistency check still passes — the cluster is behaving exactly as its code says.

## 3. What the test asserted and what it saw

The workload loads batches of rows into a duplicate-key table, each batch under a label derived from its id (`b<id>_<method>`), and retries a batch whenever it did not get a success response — always under the same label. It then reads back per-batch aggregates and checks them against what it sent.

Three properties failed on this one event:

| Property | passed | failed |
| --- | ---: | ---: |
| a load is never applied twice | 122 | 4 |
| a batch acknowledged visible is visible to later reads | 95 | 4 |
| acknowledged batches survive once faults stop | 13 | 5 |

The counterexample, as the driver recorded it:

```json
{"batch": 4, "label": "b4_stream_load_be", "method": "stream_load_be", "attempts": 15,
 "rows": 5, "count": 10, "want_seq_sum": 10, "seq_sum": 20,
 "want_val_sum": 20010, "val_sum": 40020}
```

Row count, `sum(seq)` and `sum(val)` are each exactly double. Every row is present twice. Three independent aggregates doubling rules out a partial write, a torn batch, or rows arriving from some other batch — row `s` of batch `b` carries `val = b*1000 + s`, so a foreign row would break `val_sum` without breaking `count`.

For reference, a read at vtime 48.36 saw the same batch with `count: 5`. The duplication appeared later.

## 4. Evidence

All lines below are from one timeline. Times are virtual seconds from the start of the simulation; the FE's own wall-clock timestamps are kept so lines can be matched to an ordinary `fe.log`.

**The load succeeds once.**

```
26.4586  [FrontendServiceImpl.loadTxnBegin():1301]  receive txn begin request, db: aw, tbl: ev,
                                                    label: b4_stream_load_be, backend: 10.89.0.6
26.4586  [DatabaseTransactionMgr.beginTransaction():193]  begin transaction: txn_id: 32
                                                    with label b4_stream_load_be
26.4803  [DatabaseTransactionMgr.commitPreparedTransaction():573]  txn_id: 32 ... COMMITTED
28.1223  [DatabaseTransactionMgr.finishTransaction():1446]  txn_id: 32, label: b4_stream_load_be,
                                                    db id: 10004, table id list: 10031,
                                                    transaction status: VISIBLE ... successfully
```

The client never saw this. Its request failed in transit; from its side the outcome was unknown, which is why it retried. This is the ordinary case the label is for.

**Deduplication works — three times.**

```
75.4388  begin transaction: txn_id: 5760 with label b4_stream_load_be
75.4388  WARN [FrontendServiceImpl.loadTxnBeginImpl():1399]
         STREAM_LOAD_TASK:13230, label{b4_stream_load_be},
         error_msg{cancel stream task for exception: Label [b4_stream_load_be] has already been used.}
81.0531  ... txn_id: 5878 ... Label [b4_stream_load_be] has already been used.
82.0572  ... txn_id: 5899 ... Label [b4_stream_load_be] has already been used.
```

(`beginTransaction` logs the id it allocated before it runs the label check, so a logged `txn_id` here does not mean a transaction was created.)

**The FE is killed and restarts.**

```
131.37   fault_injector  kill ["fe"]
138.21   [entrypoint] filesystems: ...
138.28   start time: Tue Nov 10 03:47:34 UTC 2026
174.33   [GlobalStateMgr.loadImage():1693]   start load image from .../image/v2/image.132
174.48   [GlobalStateMgr.replayJournal():2420]  start to replay journal from 133 to 7394
```

Metadata recovery is correct. Transaction 32 is restored — finished transactions are written to the image (`DatabaseTransactionMgr.unprotectWriteAllTransactionStatesV2` writes `finalStatusTransactionStateDeque`) and replayed from the journal, and `putTransactionStats` → `unprotectUpsertTransactionState` rebuilds `labelToTxnIds`. **Nothing is lost in recovery.** This is worth stating plainly, because "the FE forgets labels across a restart" was our first hypothesis and it is wrong.

**One second after the FE is ready, the cleaner throws the label away.**

```
175.3970  INFO (LoadLabelCleaner|97) [DatabaseTransactionMgr.removeExpiredTxns():1956]
          transaction list [2, 3, 4, 6, 7, 8, 9, 10, ..., 29, 31, 30, 32, 33, 1002, 1004, 1003, ...]
          are expired, remove them from transaction manager
```

Transaction **32** is in that list. It was 147 seconds old. `label_keep_max_second` is 3 days. It was removed because the restored FE held about 7000 finished transactions and the cap is 1000.

The checkpoint worker does the same thing on its own schedule:

```
238.389  INFO (global-state-checkpoint-worker|121) [DatabaseTransactionMgr.removeExpiredTxns():1956] ...
```

**The retry is now accepted, and the data lands twice.**

```
278.7034  receive txn begin request, db: aw, tbl: ev, label: b4_stream_load_be, backend: 10.89.0.21
278.7038  begin transaction: txn_id: 8227 with label b4_stream_load_be
278.7038  [StreamLoadTask.beginTxn():339]  stream load b4_stream_load_be channel_id 0 begin.
                                           db: aw, tbl: ev, txn_id: 8227
278.7264  commitPreparedTransaction  txn_id: 8227 ... COMMITTED
278.7813  finishTransaction          txn_id: 8227 ... VISIBLE ... successfully
299.7180  assertion failed: a load is never applied twice  {count: 10, rows: 5, ...}
```

Both transactions targeted database 10004, table 10031 (`ev`), partition 10033 — version 5 for txn 32, version 222 for txn 8227.

## 5. Root cause

Line numbers below are from `main` at the time of writing; the 4.1.7 build in the run logs `removeExpiredTxns():1956`.

`DatabaseTransactionMgr.removeExpiredTxns` (`fe/fe-core/src/main/java/com/starrocks/transaction/DatabaseTransactionMgr.java:2308`) applies two retention rules through one loop:

```java
int numJobsToRemove = getTransactionNum() - Config.label_keep_max_num;
while (!finalStatusTransactionStateDeque.isEmpty()) {
    TransactionState transactionState = finalStatusTransactionStateDeque.getFirst();
    if (transactionState.isExpired(currentMillis) || numJobsToRemove > 0) {
        finalStatusTransactionStateDeque.pop();
        clearTransactionState(transactionState);
        --numJobsToRemove;
        ...
```

`isExpired` is the documented, age-based rule (`label_keep_max_second`). `numJobsToRemove > 0` is the count-based rule, and it ignores age entirely. `clearTransactionState` is what makes the label reusable:

```java
private void clearTransactionState(TransactionState transactionState) {
    idToFinalStatusTransactionState.remove(transactionState.getTransactionId());
    Set<Long> txnIds = unprotectedGetTxnIdsByLabel(transactionState.getLabel());
    txnIds.remove(transactionState.getTransactionId());
    if (txnIds.isEmpty()) {
        labelToTxnIds.remove(transactionState.getLabel());
    }
}
```

`labelToTxnIds` is the only thing `beginTransaction` consults (`DatabaseTransactionMgr.java:217-239`). Once the entry is gone the label is indistinguishable from one never used.

The two callers of `clearExpiredJobs()` → `GlobalTransactionMgr.removeExpiredTxns()`:

- `GlobalStateMgr.createLabelCleaner()` (`server/GlobalStateMgr.java:2521`) — `new FrontendDaemon("LoadLabelCleaner", Config.label_clean_interval_second * 1000L)`, default `4 * 3600`.
- `GlobalStateCheckpointWorker` (`journal/GlobalStateCheckpointWorker.java:46`).

So the cap is enforced in bursts. Between bursts the index can hold far more than 1000 entries and deduplication looks like it works — which is what the three correct rejections at 75 s, 81 s and 82 s were. The first burst after a restart discards everything above the cap at once.

Two consequences worth separating:

1. **The headline:** a restart (crash, failover, rolling upgrade) forces immediate enforcement. This is the easy, reliable trigger.
2. **The broader exposure:** a restart is not required. Every checkpoint does the same thing, on a long-running leader, with no operator action. The window is `min(label_keep_max_second, last label_keep_max_num transactions)`, and the second term dominates on any cluster with real load.

## 6. Reproduction

Does not need Antithesis or any fault injection. Single FE, any table.

To be straight about provenance: this recipe follows from the root cause in section 5. What
we ran is the simulation in section 4; we have not yet executed these steps standalone.

```sql
-- fe.conf, then start the FE:
--   label_keep_max_num = 2
--   label_clean_interval_second = 5
```

(`label_clean_interval_second` is mutable, but `FrontendDaemon` re-reads it only at the end of a tick, so lowering it at runtime on a fresh FE takes effect only after the current 4-hour sleep. Set it in `fe.conf`, or restart the FE, which is the real-world trigger anyway.)

1. Stream Load 5 rows into `t` under label `L`. Confirm `SELECT count(*) FROM t` is 5.
2. Stream Load twice more under labels `M`, `N` — enough to put `L` over the cap.
3. Re-send the identical load under label `L`. Expect `LABEL_ALREADY_EXISTS` / `Label [L] has already been used.` — deduplication is working.
4. Wait for one `LoadLabelCleaner` tick, or restart the FE. Look for `transaction list [...] are expired, remove them from transaction manager` in `fe.log` and confirm `L`'s transaction id is in it.
5. Re-send the identical load under label `L` again. It is accepted, commits, and becomes visible.
6. `SELECT count(*) FROM t` is now 10.

With defaults, substitute "1000 loads" for step 2 and "restart the FE" for step 4.

A deterministic FE unit test in `DatabaseTransactionMgrTest` would be tighter: begin + commit + finish a transaction under label `L`, assert `beginTransaction(L)` throws `LabelAlreadyUsedException`, set `Config.label_keep_max_num = 0`, call `removeExpiredTxns(now)`, assert `beginTransaction(L)` now succeeds. No restart or timing involved — the restart only matters because it is what makes the cleaner run.

## 7. Suggested directions

Not prescriptive — you know the memory budget that motivated the cap (`label_keep_max_num` arrived in 2.0 to stop full GCs during frequent loading).

- **Separate the dedup index from the job history.** The cap exists to bound memory held by `TransactionState` objects. The dedup decision needs only `label → (txnId, status, finishTime)`. Retaining that tuple for `label_keep_max_second` costs a tiny fraction of retaining the states, and would let the count cap keep doing its job on the heavy objects without quietly shortening the guarantee.
- **Make the trimmed window observable.** Record the oldest `finishTime` still covered, and surface it (a metric, or `SHOW PROC`). Today an operator has no way to know their effective dedup window is four minutes rather than three days.
- **Fail loudly rather than silently accepting.** If a label falls outside the retained window, a distinguishable error lets a client escalate instead of duplicating. Accepting is the one answer that cannot be recovered from.
- **At minimum, document it.** `label_keep_max_num` is described as retaining "the information of historical jobs" — a memory-management knob. Nothing connects it to the at-most-once guarantee, and the loading docs that promise at-most-once never mention it.

## 8. What we did not establish

- Whether any released version behaves differently. Only 4.1.7 was run; the code paths are the same on `main`.
- The behaviour under multi-FE leader failover (this was a single-FE cluster). The new leader replays the journal and then runs the same cleaner, so we expect the same result, but we did not test it.
- Whether shared-data (lake) tables differ. The run used shared-nothing tables with `replication_num = 3`.
- Whether the three correct rejections and the later acceptance could have been separated by anything other than the cleaner. The eviction line naming txn 32 is direct evidence, but it is one timeline.

---

## Appendix: how this was found

You know DST; this is only what is specific to Antithesis.

**The setup.** The SUT is an ordinary `docker-compose` deployment — 1 FE, 3 BEs, official 4.1.7 release images, plus a driver container that is excluded from fault injection. Antithesis runs the whole thing inside a deterministic hypervisor, so every container, the Linux scheduler, the network and the clock are all under the simulator's control. Nothing in StarRocks is modified. (One caveat specific to us: the FE will not start in a container whose filesystem reports under 5 GB, so we patched bdb-je's `je.freeDisk` default in the bundled jar. That constant is already configurable on `main` via `bdbje_free_disk_size`. It is FE-side only and unrelated to this finding.)

**Determinism, and what it buys.** A run explores many *timelines*, each a different schedule and fault pattern, all replayable. A point in a timeline is a *moment* — an input hash plus a virtual time. The log excerpts above are not reconstructed from timestamps across containers; they are a single totally ordered trace of FE, BE, driver and the fault injector together, which is why the causal chain reads the way it does. Any moment can be re-executed with a debugger attached and will reproduce exactly.

**The driver.** A Rust state machine (hegel) that issues `INSERT`, Stream Load via FE and via BE, the Stream Load transaction interface, SQL transactions, primary-key upserts and partial column updates, reads, schema changes, and retries, concurrently; it keeps a model of what it sent and checks reads against it. Guarantees are expressed as `assert_always` / `assert_sometimes`. The `sometimes` assertions double as a steering signal: Antithesis treats a rarely-satisfied `sometimes` as a target and biases its search toward satisfying it. "A retried load finds its label already used" is one such assertion, which is why the search spent effort on retry-under-fault at all.

**Faults.** Node kills, stops, pauses, network partitions, clogs, clock skew and CPU-speed modulation. The FE kill at vtime 131 came from the fault injector. The duplication itself involved no fault — it was the ordinary restart path.

**Honest limitation.** Only the Java FE carries Antithesis coverage instrumentation; the C++ BE runs from the stock release image, so the search gets no coverage feedback from the storage engine and no thread pausing there. An instrumented BE build would be the single biggest improvement.

**What else the same run turned up.** All three BEs took a SIGSEGV in `MemTable::finalize()` → `Chunk::rolling_append_selective` → `FixedLengthColumnBase<int>::append_selective`, null dereference, no faults active, no memory pressure. One occurrence, mechanism unproven, not reproduced outside Antithesis. It is written up as Finding 2 in `FINDINGS.md`; we are not asking you to act on it as it stands.
