> **Draft GitHub issue for StarRocks/starrocks. Not yet filed.** The first heading is the issue title; everything after it is the issue body.
# Duplicate data: retried load accepted after frontend restart

Hello,

When testing StarRocks with [Antithesis](https://antithesis.com/), we found that a load retried under the same label can be applied twice. Labels are documented to deduplicate retries for 3 days (`label_keep_max_second`), but `label_keep_max_num` (1,000 per database) evicts them regardless of age, and a frontend (FE) restart enforces that cap within a second.

## StarRocks version

4.1.7-b81ff9c. Code unchanged on `main`.

## Impact

**Correctness.** Silent duplicate rows. The retry returns `Success`, nothing is logged above the INFO level. Any client that retries on timeout (Flink, Spark and Kafka connectors, custom loaders) can double-load after a crash, failover or rolling upgrade.

## Triggers

1. A load commits but the client misses the reply, so it retries under the same label.
2. More than `label_keep_max_num` transactions finish in the database after it.
3. The label cleaner runs: on frontend startup, on every checkpoint, or every 4 hours.

## Steps to reproduce the behavior

1. Set `label_keep_max_num = 2` in `fe.conf` and start a cluster.
2. Stream Load 5 rows into a table under label `L`.
3. Retry under `L`: `Label Already Exists` (correct).
4. Stream Load under labels `M`, `N`, `O`.
5. Retry under `L`: still `Label Already Exists`.
6. Restart the frontend. `fe.log` shows `transaction list [...] are expired, remove them`, including `L`'s transaction.
7. Retry under `L`.

Script: [`repro/label_dedup.sh`](https://github.com/MattxAntithesis/starrocks-antithesis/blob/main/repro/label_dedup.sh)

## Expected behavior

Step 7 returns `Label Already Exists`.

## Real behavior

Step 7 returns `Success`. The table holds `L`'s 5 rows twice (25 rows instead of 20).

In Antithesis, a retry rejected 3 times was accepted after a frontend restart. The cleaner had evicted the original transaction 1 second after startup, when it was 147 seconds old. Row count, `sum(seq)` and `sum(val)` were all exactly doubled.

## Root cause

- `removeExpiredTxns` evicts a finished transaction if it is past `label_keep_max_second` **or** the count exceeds `label_keep_max_num`. The count rule ignores age: [DatabaseTransactionMgr.java#L1945-L1951](https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/transaction/DatabaseTransactionMgr.java#L1945-L1951)
- `clearTransactionState` removes the label from `labelToTxnIds`: [#L1973-L1978](https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/transaction/DatabaseTransactionMgr.java#L1973-L1978)
- `labelToTxnIds` is all `beginTransaction` checks, so the label now looks unused: [#L212-L233](https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/transaction/DatabaseTransactionMgr.java#L212-L233)

Restart recovery itself is correct. The restart only makes the cleaner run, and its first run discards every label beyond the newest 1,000. So the real dedup window is the shorter of 3 days and the last 1,000 transactions, which is minutes on a busy database.

The docs describe `label_keep_max_num` only as a limit on job history. The label docs promise 3-day retention (`STREAM_LOAD.md`) and never mention the cap.

## Related issues

#79872 / #79874 cover `removeExpiredTxns` failing to expire transactions in shared-data mode, which is the opposite symptom.
