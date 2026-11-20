> **Draft GitHub issue for StarRocks/starrocks. Not yet filed.** The first heading is the issue title; everything after it is the issue body.
# [Bug] A retried load is applied twice after an FE restart: `label_keep_max_num` evicts labels regardless of age, so label deduplication is lost

Hello,

When testing StarRocks with [Antithesis](https://antithesis.com/), we found that a load retried under the same label can be applied a second time. Labels are documented as the at-most-once mechanism for loads and are kept for `label_keep_max_second` (3 days). But they are also capped at `label_keep_max_num` (1,000 per database), and the cap evicts labels whatever their age. An FE restart makes the cleaner enforce the cap within about a second. After that, a retry of any load older than the last 1,000 is accepted as new, and its rows are stored twice.

## StarRocks version (Required)

`4.1.7-b81ff9c` (official `starrocks/fe-ubuntu:4.1.7` and `starrocks/be-ubuntu:4.1.7` images). The code below is unchanged on `main` as of 2026-11-20.

## Impact

**Correctness (silent duplicate data).** The retry returns `Success`, nothing is logged above INFO, and every replica agrees. The only symptom is that totals are too high. Any client that retries on timeout under the same label (the Flink, Spark and Kafka connectors, or a custom loader) can double-load after a crash, failover or rolling upgrade.

## Triggers

1. A load commits, but the client doesn't get the reply (timeout, dropped connection), so it retries under the same label later.
2. The database accumulates more than `label_keep_max_num` finished transactions after that load.
3. The label cleaner runs. It runs every `label_clean_interval_second` (4 hours), on every checkpoint, and within about a second of an FE starting. An FE restart is the easy trigger.

## Symptom

A retry that was correctly rejected with `Label [L] has already been used` is accepted after the FE restarts. Both transactions are VISIBLE and the rows are stored twice.

## Steps to reproduce the behavior (Required)

No fault injection is needed. 1 FE, 3 BEs, shared-nothing. The cap is lowered to 2 so that 3 loads are enough to exceed it.

1. Add to `fe.conf` and start the cluster:
   ```
   label_keep_max_num = 2
   ```
2. Create the table:
   ```sql
   CREATE DATABASE d;
   CREATE TABLE d.t (k INT, v INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1;
   ```
3. Load 5 rows under label `L`:
   ```bash
   printf '1,1\n2,2\n3,3\n4,4\n5,5\n' > rows.csv
   curl --location-trusted -u root: -H "label:L" -H "column_separator:," -T rows.csv http://<fe>:8030/api/d/t/_stream_load
   ```
   `SELECT count(*) FROM d.t` returns 5.
4. Retry under `L`: `"Status": "Label Already Exists"`. Deduplication works.
5. Load the same file under labels `M`, `N` and `O`. `SELECT count(*)` returns 20.
6. Retry under `L` again: still `Label Already Exists`.
7. Restart the FE. Within a second of startup `fe.log` shows
   `transaction list [2, 4] are expired, remove them from transaction manager`, which includes `L`'s transaction.
8. Retry under `L` again: `"Status": "Success"`.
9. `SELECT count(*) FROM d.t` returns 25. The five rows of `L` are stored twice.

Script: [`repro/label_dedup.sh`](https://github.com/MattxAntithesis/starrocks-antithesis/blob/main/repro/label_dedup.sh). With default settings the same happens with 1,000 later loads in place of step 5.

## Expected behavior (Required)

Step 8 returns `Label Already Exists`, as in steps 4 and 6, for as long as `label_keep_max_second` (3 days) promises. If StarRocks can no longer tell whether a label was used, it should fail the request, not accept it as new.

On the documentation: `label_keep_max_num` is documented, but as a memory setting for job history ("The maximum number of load jobs that can be retained within a period of time. If this number is exceeded, the information of historical jobs will be deleted"). The label documentation says the opposite of what happens: "The system does not allow you to use one label to load a data batch multiple times... By default, the system retains the labels of load jobs that were successfully completed over the most recent three days" (`STREAM_LOAD.md`). Nothing tells a user that the 1,000-job history limit also shortens the deduplication window, or that a restart applies it at once. If the cap is intended, the label docs and the at-most-once wording should say so. We think the dedup index shouldn't depend on it.

## Real behavior (Required)

Step 8 returns `Success`, the transaction becomes VISIBLE, and the table holds the load twice (25 rows instead of 20).

## What Antithesis saw

The workload loads 5-row batches under labels like `b4_stream_load_be` and retries any batch whose outcome it didn't learn, always under the same label. It checks every read against what it sent. One timeline, in virtual seconds:

```
 26.46  begin transaction: txn_id: 32 with label b4_stream_load_be        (reply lost; client will retry)
 28.12  finishTransaction txn_id: 32 ... VISIBLE
 75.44  Label [b4_stream_load_be] has already been used.                  (retry rejected, correct)
 81.05  Label [b4_stream_load_be] has already been used.
 82.06  Label [b4_stream_load_be] has already been used.
131.37  FE killed by the fault injector; restarts
175.40  [DatabaseTransactionMgr.removeExpiredTxns():1956] transaction list [2, 3, 4, ..., 32, 33, ...]
        are expired, remove them from transaction manager                 (txn 32 is 147 s old)
278.70  begin transaction: txn_id: 8227 with label b4_stream_load_be      (retry accepted)
278.78  finishTransaction txn_id: 8227 ... VISIBLE
299.72  assertion failed: a load is never applied twice
        {"label":"b4_stream_load_be","rows":5,"count":10,"seq_sum":20,"want_seq_sum":10}
```

Row count, `sum(seq)` and `sum(val)` were all exactly doubled. Restart recovery itself was correct: transaction 32 was restored from the image and journal. It was the cleaner, one second later, that dropped it.

## Root cause

`removeExpiredTxns` applies two retention rules in one loop. `isExpired` is the documented age rule. `numJobsToRemove > 0` is the count cap, and it ignores age:
https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/transaction/DatabaseTransactionMgr.java#L1945-L1951

`clearTransactionState` then removes the label from `labelToTxnIds`:
https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/transaction/DatabaseTransactionMgr.java#L1973-L1978

`labelToTxnIds` is the only thing `beginTransaction` checks, so the label now looks unused:
https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/transaction/DatabaseTransactionMgr.java#L212-L233

The cleaner runs as the `LoadLabelCleaner` daemon and from every checkpoint:
https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/server/GlobalStateMgr.java#L1996
https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/journal/GlobalStateCheckpointWorker.java#L46

Between cleaner runs the index can hold far more than 1,000 labels, so deduplication appears to work. The first run after a restart discards everything above the cap at once. A restart isn't required: every checkpoint does the same on a long-running leader. In practice the dedup window is the shorter of 3 days and the last 1,000 transactions, and on a busy database the second is minutes.

A unit test would show it without a restart: finish a transaction under label `L`, check that `beginTransaction(L)` throws `LabelAlreadyUsedException`, set `Config.label_keep_max_num = 0`, call `removeExpiredTxns(now)`, and `beginTransaction(L)` now succeeds.

Possible directions, if useful: keep a small `label -> (txnId, status, finishTime)` index for the full `label_keep_max_second` and apply the count cap only to the full `TransactionState` objects. Or reject a label that might have been evicted with a distinct error instead of accepting it.

## Related issues

#79872 / #79874 concern `removeExpiredTxns` failing to expire transactions in shared-data mode (the opposite symptom). This report is about the count cap removing labels that are still inside their retention period. We found no existing report of it.

Full write-up with logs: [FINDINGS.md, Finding 1](https://github.com/MattxAntithesis/starrocks-antithesis/blob/main/FINDINGS.md#finding-1-a-retried-load-can-be-saved-twice) and [REPORT-label-dedup.md](https://github.com/MattxAntithesis/starrocks-antithesis/blob/main/REPORT-label-dedup.md).
