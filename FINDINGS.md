# Findings: StarRocks 4.1.7

We found two bugs in StarRocks 4.1.7. Both turned up in one 90-minute Antithesis run
(`c7d09923c0006169e56a8e0b37ee9c3a-67-0`) against the official release images
(`starrocks/{fe,be}-ubuntu:4.1.7`, build `b81ff9c`).

| # | What goes wrong | Impact | How solid |
| --- | --- | --- | --- |
| 1 | A retried load is saved twice | Silent duplicate data | Root cause found in the code |
| 2 | All three storage servers crash at once | Storage outage | Seen once, cause unproven |

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

## Finding 2: all three storage servers crash at once during a normal load

**In one line:** all three BEs segfaulted within 70 ms of each other while saving one
ordinary load, with no faults injected and no memory pressure.

**What we saw.** At virtual second 24.4, while committing a multi-table Stream Load
transaction (label `b1_txn_stream_load`, writing to `ev` and `ev2`), each BE crashed
microseconds after committing its own tablets. They had the same stack and were working
on different tablets:

```
SIGSEGV (@0x0)  FixedLengthColumnBase<int>::append_selective
                Chunk::rolling_append_selective
                MemTable::_sort → MemTable::finalize
                DeltaWriter::flush_memtable_async → DeltaWriter::close
                AsyncDeltaWriter::_execute
```

Failing properties: *No unexpected crashes: starrocks_be* (3) and *No unexpected container
exits* for `be1`, `be2` and `be3` (exit code 139).

**How rare.** One crash moment in about 2,500 runs of this path. A local stress test
didn't reproduce it.

**Best guess, unproven.** `Chunk::rolling_append_selective` frees each source column as
it goes and has only a `DCHECK` (compiled out in release) against a second pass. A second
`finalize()` on the same memtable would crash exactly here. We have not observed one, and
a plain race wouldn't obviously hit all three replicas at the same instant.

**Next step:** pull the core dump from the run, or rerun with an instrumented BE.

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

**2. A crash that would normally be closed as "cannot reproduce" can now be replayed on
demand.** The storage crash happened once in about 2,500 attempts. Fifteen minutes of
stress testing on a real machine didn't reproduce it. Antithesis runs the whole cluster
deterministically, so that exact moment can be replayed identically, as often as needed.
We rewarmed it and ran commands inside the crashed cluster to find the operation, the
tables, the tablets on each node and the thread involved. None of that was in the crash
report.

**3. One exact story across every machine.** Normally you piece a cluster bug together
from separate logs on separate machines, with clocks that disagree. Antithesis gives a
single timeline of the FE, all three BEs, the client and every injected fault, in exact
order. That is how we could say the label was deleted 1 second after the restart and 147
seconds after the load, and connect it straight to the line of code responsible.

**What the replay makes possible next.** Because any moment can be reopened and run
forward again, we can rewrite history and watch what changes:

- Rerun the duplicate-data timeline from just before the FE kill without the kill. If the
  retry is then rejected, the restart is shown to be the trigger.
- Rerun the crash from milliseconds before it, many times with small variations, to
  measure how often it happens and whether it's timing or the data that sets it off.

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
