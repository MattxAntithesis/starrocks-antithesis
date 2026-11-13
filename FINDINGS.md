# Findings: StarRocks 4.1.7

We found two bugs in StarRocks 4.1.7. Both turned up in one 90-minute Antithesis run
(`c7d09923c0006169e56a8e0b37ee9c3a-67-0`) against the official release images
(`starrocks/{fe,be}-ubuntu:4.1.7`, build `b81ff9c`).

| # | What goes wrong | Impact | How solid |
| --- | --- | --- | --- |
| 1 | A retried load is saved twice | Silent duplicate data | Root cause found in the code |
| 2 | All three storage servers crash at once | Storage outage | Reproduced on demand outside Antithesis |

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

**Not yet done:** the standalone reproduction (no Antithesis needed) follows from the code
but hasn't been run. Seen on single-FE, shared-nothing tables only.

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

**Likely cause, not proven.** The load's data was shaped for the old table, and the
memtable now expects the new one. `Chunk::rolling_append_selective` walks the columns
assuming both sides have the same number. The only guard is a `DCHECK`, which release
builds compile out, so a missing column becomes a null pointer. This fits everything we
saw, but we haven't traced it line by line in a debugger.

**Antithesis had the trigger from the start.** The column add is in the log of the
original run (`c7d09923…`, moment `-8215334601661802087`). We missed it three days ago
and found it today, when a local run of the same workload crashed the same way.

**Reproduce it:** [`repro/`](repro/). Build the images with
`cd loads/config && docker compose build`, then run
`MODE=multi ALTER_AT=during repro/run.sh 20`. It starts a fresh 1-FE, 3-BE cluster,
repeats the transaction with a concurrent column add, and prints the BE crash stacks.

**Not yet done:** reporting it to StarRocks (we found no matching GitHub issue), and
checking older 4.x releases.

---

## Why these bugs needed Antithesis

Both bugs are in code StarRocks already tests heavily. Here is why ordinary methods
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
is in flight. An earlier round of ordinary stress testing (15 minutes, without schema
changes) never hit it. Antithesis did, because its workload mixes every kind of operation
concurrently and varies their timing. Because the run is deterministic, the exact moment can
be replayed identically. We replayed it to identify the load, the tables and the thread,
and the run's single ordered log also held the column add that turned out to be the
trigger. With that, the bug went from "seen once" to a five-line reproduction anyone can
run.

**3. One exact story across every machine.** Normally you piece a cluster bug together
from separate logs on separate machines, with clocks that disagree. Antithesis gives a
single timeline of the FE, all three BEs, the client and every injected fault, in exact
order. That is how we could say the label was deleted 1 second after the restart and 147
seconds after the load, and connect it straight to the line of code responsible.

**What the replay makes possible next.** Because any moment can be reopened and run
forward again, we can rewrite history and watch what changes:

- Rerun the duplicate-data timeline from just before the FE kill without the kill. If the
  retry is then rejected, the restart is shown to be the trigger.
- Rerun the crash moment without the column add, to confirm inside the simulator what the
  local reproduction already shows.

These experiments haven't been run yet. We list them because they are the direct way to
turn "observed once" into "proven", and no other testing method can do them.

---

## Limits of this testing

- **Only the FE was instrumented.** The C++ BE ran from the stock image, so Antithesis had
  no coverage feedback or thread pausing inside the storage engine. An instrumented BE
  build would make this testing much stronger.
- **A passing property means only what the run reached.** See `loads/README.md` for the
  fault-density check that guards against tests passing because no faults happened.
- **Setup workaround.** Antithesis containers report about 4.9 GB of disk, and the FE
  refuses to start below 5 GB. We patched bdb-je's `je.freeDisk` in the FE jar
  (`loads/fe/patch_je_free_disk.py`). This only affects startup and is unrelated to either
  finding. Asking Antithesis for more machine memory would remove the need for the patch.
