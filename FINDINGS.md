# Findings

Bugs and suspicious behavior found by the workload in `loads/`. Each entry gives the run, the failing property, and the evidence.

Findings 2 and 3 below both came from a single 90-minute run on StarRocks 4.1.7
(`c7d09923c0006169e56a8e0b37ee9c3a-67-0`). Finding 3 is the one ready to report and is
written up in [REPORT-label-dedup.md](REPORT-label-dedup.md). Finding 1 is from a replay
of 3.5.19 and has not been shown on a current release.

**What a clean property does not show.** A passing property is only as strong as the
states the run actually reached, and an early version of this workload ended its timelines
before Antithesis had injected any faults — every property passed for the wrong reason.
`loads/README.md` documents that failure mode and the fault-density check that catches it.
Treat any green result here as conditional on that check.

A second limit: only the Java FE carries Antithesis coverage
instrumentation. The C++ BE runs from the official release image, so Antithesis gets no
coverage feedback from the backend, where the storage and transaction logic lives. An
instrumented BE build (needs a larger build host than this VM) is the single biggest
improvement to this workload's bug-finding power.

## Open question for the user

**Ask Antithesis to raise this tenant's machine memory.** Antithesis containers store
files in a RAM disk sized at half the machine's memory, and the default machine has
10 GB, so every filesystem in every container reports about 4.9 GB
(`/docs/configuration/the_antithesis_environment/`, "Filesystem and disk"). StarRocks'
FE refuses to start when the filesystem holding its meta dir has less than 5 GB usable,
so it died 1.2 s into setup in runs `70c96cbac8d3808fe2b468ec803d2d59-67-0`,
`090cb636f9fd1dfa09a2c3fecc18b3e6-67-0` and `ffca0491df15d681c0fdb754a29daecf-67-0`.

The docs say the memory limit "can be increased upon request". Until then the workload
patches the constant in the FE's bundled bdb-je jar
(`loads/fe/patch_je_free_disk.py`), which is what StarRocks `main` already allows
through `bdbje_free_disk_size`. Raising the machine memory would let the workload run
the official jars unmodified, and would also give the 3 BEs more room.

## Notes (not bugs)

- Stream Load via FE without an `Expect: 100-continue` header fails with
  `There is no 100-continue header` (documented behavior).

---

## Finding 1: BE segfaults during shutdown, finishing a publish-version task

**Status:** reproduced once on StarRocks 3.5.19. Not yet shown on a current release.

**Run:** `020fab52b14b7d6657ff47bc4e2dbe10-67-0`, property `No unexpected crashes:
starrocks_be`, 0 passing and 1 failing example, moment `7166186290534918060` at
vtime `41.865`.

**What the BE printed:**

```
*** SIGSEGV (@0x10) received by PID 1 (TID 0x7fc6628dd640) LWP(508) from PID 16
PC: @ 0x7fc6768acef4 pthread_mutex_lock
    @ 0xbda1e00  starrocks::ClientCacheHelper::close_connections(starrocks::TNetworkAddress const&)
    @ 0xc201512  starrocks::ThriftRpcHelper::rpc<starrocks::FrontendServiceClient>(...)
    @ 0xa8ad807  starrocks::MasterServerClient::finish_task(starrocks::TFinishTaskRequest const&, ...)
    @ 0xa8ac6d6  starrocks::finish_task(starrocks::TFinishTaskRequest const&)
    @ 0xa8a8eb5  starrocks::PublishVersionManager::finish_publish_version_task()::{lambda()#1}
    @ 0xc2242fb  starrocks::ThreadPool::dispatch_thread()
```

The faulting instruction is `pthread_mutex_lock` on address `0x10`, so the mutex
`ClientCacheHelper::_lock` guards memory that is already gone: a background
publish-version task was still calling into the Thrift client cache after that cache
had been torn down.

**What led to it** (vtime, from the run's log):

```
31.573  fault_injector  stop          <- Antithesis stops a container
32.743  be2             got signal: Terminated, is going to exit
34.860  fault_injector  kill
36.740  fault_injector  pause
37.042  fault_injector  clog
39.770  fault_injector  partition
41.457  fault_injector  stop
41.851  be2             SIGSEGV in ClientCacheHelper::close_connections
```

be2 received SIGTERM at 32.7 and was still shutting down 9 s later when a
publish-version finish task ran, tried to report to the FE, and segfaulted. The RPC
itself was failing — `close_connections` is on the error path — which is expected while
the network is partitioned and clogged.

**Why it may matter:** SIGTERM is the ordinary stop path, so this is reachable by any
rolling restart or upgrade while loads are in flight, not only under injected faults.
`ClientCacheHelper::close_connections` is unchanged in `main` at
`be/src/common/util/thrift_client_cache.cpp`. The nearest related fixes are a Thrift
client *leak* after cache invalidation (#77072, Aug 2026) and a 2023 fix for
`finish_publish_version_thread` getting *stuck* (#29362); neither addresses a
use-after-free on this path.

**Not yet established:** whether it reproduces on a current release, how often, and
whether the shutdown ordering changed between 3.5.19 and 4.1.7. A 90-minute run on
4.1.7 is testing exactly that.

---

## Finding 2: all three BEs segfault committing a multi-table Stream Load transaction on 4.1.7

**Status:** observed once, on the current release, with no faults active. The triggering
operation is now identified by replaying the moment in Antithesis. Not reproduced outside
the simulator. Mechanism still unproven.

**Run:** `c7d09923c0006169e56a8e0b37ee9c3a-67-0` (StarRocks 4.1.7 RELEASE, build b81ff9c,
90-minute run). Properties `No unexpected crashes: starrocks_be` (3 counterexamples) and
`No unexpected container exits` for `be1`, `be2` and `be3`, each exit code 139 (SIGSEGV).
Moment `-8215334601661802087`, vtime 24.44.

**Stack** (identical on all three backends):

```
PC: @ 0xa85493b  starrocks::FixedLengthColumnBase<int>::append_selective(Column const&, uint32_t const*, uint32_t, uint32_t)
*** SIGSEGV (@0x0) ***
    @ 0xa85493b   FixedLengthColumnBase<int>::append_selective(...)
    @ 0xa8ced70   Chunk::rolling_append_selective(Chunk&, uint32_t const*, uint32_t, uint32_t)
    @ 0x106a81c4  MemTable::_sort(bool, bool)
    @ 0x106a9b01  MemTable::finalize()
    @ 0x1069a942  DeltaWriter::flush_memtable_async(bool)
    @ 0x1069c1b4  DeltaWriter::close()
    @ 0x10d1cc37  AsyncDeltaWriter::_execute(void*, bthread::TaskIterator<AsyncDeltaWriter::Task>&)
```

This is the ordinary tablet write path: closing a delta writer flushes its memtable.

**Conditions, from the run's log:**

- The workload had been active for **0.2 s** (`workload active` at vtime 24.219, crash at
  24.44), on `txn_id: 4`.
- **No faults were active.** The last fault event beforehand was `restore/network` at
  24.162, which *clears* network faults; `active_faults` was empty at the crash. No kills,
  partitions, clogs or pauses were in play.
- **No memory pressure.** The BE's own statistics moments earlier:
  `process(0) query_pool(0) load(0) ... jemalloc_allocated(25025608)` — 25 MB against the
  workload's 1500 MB `mem_limit`. The fault address is `0x0`: a null dereference, not a
  failed allocation.
- All three replicas died within ~70 ms of each other (be3 24.439, be1 24.447, be2 24.450)
  with the same stack, flushing the same replicated load.

**Mechanism — hypothesis, not established.** `Chunk::rolling_append_selective` in
`be/src/storage/chunk.cpp` frees each source column as it goes and dereferences the next
without a null check, guarded only by a `DCHECK` that release builds compile out:

```cpp
size_t num_columns = _columns.size();
DCHECK_EQ(num_columns, src.columns().size());
for (size_t i = 0; i < num_columns; ++i) {
    _columns[i]->as_mutable_raw_ptr()->append_selective(*src.columns()[i].get(), indexes, from, size);
    src.columns()[i].reset();
}
```

A second pass over a chunk this loop has already drained lands exactly on the observed
frame. `MemTable::finalize()` guards re-entry only with `if (_chunk == nullptr) return`,
which two threads can both pass. That is consistent with the crash but **unproven**: no
second `finalize()` was observed, and a pure thread race would not obviously take all
three replicas at the same instant on the same transaction, which hints the input matters
too.

**Not reproduced locally.** About 15 minutes across two configurations — four busy-loop CPU
hogs on 2 cores, then additionally `write_buffer_size=65536` with
`flush_thread_num_per_store=8` to force constant memtable flushing — produced no crash,
through 698 column-mode partial updates and ~1700 batch loads. Across the whole 90-minute
Antithesis run the signature appears in exactly one moment and did not recur in the
following ~40 minutes, so it is rare. Antithesis reaches it by modulating the simulated
CPU's relative instruction speeds, which explores thread orderings that a loaded 2-core VM
does not produce.

### What the moment replay showed

The moment was rewarmed with `snouty debug` and inspected with `snouty runs exec`. Four
things came out of it that the crash stack alone did not give.

**The operation is a multi-table Stream Load transaction.** Label `b1_txn_stream_load`,
`txn_id: 4`, `transaction_type: multi`, writing the same five rows to `ev` and `ev2` under
one transaction:

```
24.275  [fe]  StreamLoadTask.tryBegin()   channel_id 0 begin.  db: aw, tbl: ev,  txn_id: 4
24.310  [fe]  StreamLoadTask.tryBegin()   channel_id 0 begin.  db: aw, tbl: ev2, txn_id: 4
24.326  [fe]  StreamLoadTask.prepareChannel()   tbl: ev2, txn_id: 4
24.327  [fe]  unprotectedFinishStreamLoadChannel()  label: b1_txn_stream_load channel id 0
24.327  [fe]  StreamLoadTask.prepareChannel()   tbl: ev,  txn_id: 4
24.362  [fe]  unprotectedFinishStreamLoadChannel()  label: b1_txn_stream_load channel id 0
```

**The tablets resolve to tables.** From the `LocalTabletsChannel ... open 4 delta writer`
lines: load `01a1c38f-bfe1-...` is `ev` with tablets 10034, 10038, 10042, 10046; load
`01a1c38f-c004-...` is `ev2` with 10054, 10058, 10062, 10066. Both tables are
`(batch BIGINT, seq INT, k INT, val BIGINT)` — the two `INT` columns match the crashing
`FixedLengthColumnBase<int>`.

**Each BE crashes microseconds after its own tablets-channel commit, on a disjoint tablet
set.** `LocalTabletsChannel::commit` is what calls `DeltaWriter::close()`, the top of the
crashing stack:

```
24.43964063632302  [be3]  LocalTabletsChannel txn_id: 4 ... commit 2 tablets: 10046,10034
24.43964063678868  [be3]  *** SIGSEGV (@0x0) ***
24.44364722422324  [be2]  LocalTabletsChannel txn_id: 4 ... commit 1 tablets: 10038
24.443647224456072 [be2]  *** Aborted ***
24.443647573469207 [be1]  LocalTabletsChannel txn_id: 4 ... commit 1 tablets: 10042
24.44792373664677  [be1]  *** SIGSEGV (@0x0) ***
```

Disjoint tablets per BE argue against one malformed row replicated three ways, and for the
same code path entered concurrently on each node.

**The crashing thread is a `delta_writer` thread.** At the moment, `/proc/20/task` holds
352 threads; LWP 523 — the TID in the SIGSEGV line — is `delta_writer` in state `R`. The
second `delta_writer` (836) and both `memtable_flush` threads were in state `S`. That is
weak evidence either way: `AsyncDeltaWriter::_execute` runs as a brpc bthread, multiplexed
over pthreads, so OS-level thread states do not show how many logical tasks were in the
flush path.

**How rare it is.** Over the run, `snouty runs search` counts 2,511 multi-table Stream Load
transaction begins and 456,370 `LocalTabletsChannel` events, against exactly 3 events
matching `rolling_append_selective` — all at the single moment above. Roughly one crash per
2,500 executions of a path that otherwise works, which is consistent with a narrow race and
explains why local hammering did not reach it.

**Still unresolved:** whether two logical tasks were inside `finalize()` concurrently. The
official BE image writes no logs to disk (`/opt/starrocks/be/log/` is empty; all diagnostics
go to stdout), so there is nothing beyond the timeline to read. `core_pattern` pipes to
Antithesis's own coredump handler with `ulimit -c unlimited`, so a core was likely captured;
retrieving and opening it is the next step, and an instrumented BE build would make the
concurrency directly visible.

---

## Finding 3: a Stream Load label stops deduplicating once `label_keep_max_num` is exceeded, so a retry applies the data twice

**Status: the strongest finding of this work.** Observed once on StarRocks 4.1.7, with a
complete causal chain in the log and the root cause established in the FE code. The
standalone reproduction follows from that root cause and has **not yet been executed**.
Data duplication, no crash involved.

**Run:** `c7d09923c0006169e56a8e0b37ee9c3a-67-0` (4.1.7 RELEASE, 90 minutes). Failing
properties, all three the same underlying event:

| Property | ✓ | ✗ |
| --- | ---: | ---: |
| a load is never applied twice | 122 | 4 |
| a batch acknowledged visible is visible to later reads | 95 | 4 |
| acknowledged batches survive once faults stop | 13 | 5 |

**The violation.** Batch 4, five rows, loaded by Stream Load to a BE under label
`b4_stream_load_be` and retried by the workload under that same label:

```json
{"batch": 4, "label": "b4_stream_load_be", "method": "stream_load_be", "attempts": 11,
 "rows": 5, "count": 10, "want_seq_sum": 10, "seq_sum": 20,
 "want_val_sum": 20010, "val_sum": 40020}
```

Row count, `sum(seq)` and `sum(val)` are each exactly double: every row is present twice.

**Causal chain**, all from timeline `-866572852979010910`:

```
 26.46  beginTransaction txn_id: 32   label b4_stream_load_be   coordinator BE 10.89.0.6
 26.48  commitPreparedTransaction     txn_id: 32
 28.12  finishTransaction             txn_id: 32      <- applied once, visible
 75.44  beginTransaction txn_id: 5760 label b4_stream_load_be -> "Label ... has already been used"
 81.05  beginTransaction txn_id: 5878 label b4_stream_load_be -> "Label ... has already been used"
 82.06  beginTransaction txn_id: 5899 label b4_stream_load_be -> "Label ... has already been used"
131.37  fault_injector kill ["fe"]                    <- the FE is killed
131.82  containers_meta died starrocks-fe-antithesis
138.57  containers_meta init starrocks-fe-antithesis  <- and restarts
278.70  beginTransaction txn_id: 8227 label b4_stream_load_be -> ACCEPTED
278.73  commitPreparedTransaction     txn_id: 8227
278.78  finishTransaction             txn_id: 8227    <- applied a second time
```

Deduplication rejected the retry three times, then accepted an identical label after the
FE restarted. Both transactions targeted the same database (10004) and table (10031, `ev`).

**Why it matters.** StarRocks documents the label as the at-most-once mechanism: "The
Stream Load transaction interface carries over the labeling mechanism of StarRocks. You
can bind a unique label to each transaction to achieve at-most-once guarantees"
(`docs/en/loading/Stream_Load_transaction_interface.md`). Connectors and custom loaders
retry on timeout and rely on exactly that. An FE restart — a crash, a failover, or a
rolling upgrade — silently removes the protection, and an ordinary client retry duplicates
rows. Nothing errors; the data is simply there twice.

**Ruled out:**

- *Label expiry.* `label_keep_max_second` defaults to three days; the gap here is about
  four minutes of virtual time.
- *A workload bug.* The driver derives the label from the batch id (`b<id>_<method>`) and
  never varies it, and the duplication is exact (2x on three independent aggregates).
- *A consequence of the segfault in Finding 2.* That crash is in a different timeline
  (`-8215334601661802087`, vtime 24.4); this timeline has no BE crash.

**Root cause (established).** Metadata recovery is correct: the finished transaction is in
the image and the journal, and `putTransactionStats` rebuilds `labelToTxnIds`. The label is
lost one second later, to the cap:

```
175.3970  INFO (LoadLabelCleaner|97) [DatabaseTransactionMgr.removeExpiredTxns():1956]
          transaction list [2, 3, 4, ..., 31, 30, 32, 33, 1002, ...] are expired,
          remove them from transaction manager
```

`removeExpiredTxns` drops the oldest finished transactions whenever
`getTransactionNum() > Config.label_keep_max_num` (default 1000, per database), **regardless
of age**, and `clearTransactionState` removes the label from `labelToTxnIds`. Transaction 32
was 147 s old; `label_keep_max_second` is 3 days. The cap is enforced only by
`clearExpiredJobs()`, called from the `LoadLabelCleaner` daemon every
`label_clean_interval_second` (default **4 hours**) and from `GlobalStateCheckpointWorker` on
each checkpoint — so a fresh FE enforces it within seconds of becoming ready, and a
long-running leader enforces it at every checkpoint. The restart is the trigger, not the
cause.

Written up for StarRocks in [REPORT-label-dedup.md](REPORT-label-dedup.md), with a
reproduction that needs no fault injection.

**To reproduce:** exceeding the cap is the prerequisite, not the restart. Load under a
label, confirm the label is rejected as already used, push the retained transaction count
past `label_keep_max_num`, then let the cleaner run (a restart is the practical way) and
retry the same label. Full recipe, with the config values that make it quick, in
[REPORT-label-dedup.md](REPORT-label-dedup.md#6-reproduction).
