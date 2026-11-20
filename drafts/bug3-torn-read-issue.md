> **Draft GitHub issue for StarRocks/starrocks. Not yet filed.** The first heading is the issue title; everything after it is the issue body.
# Torn read: query sees a multi-table transaction half-applied

Hello,

When testing StarRocks with [Antithesis](https://antithesis.com/), we found that one query reading two tables can see a committed multi-table transaction in one table but not the other. Commit is atomic, but the planner reads each table's visible version separately, without a lock, while the transaction can become visible in between.

## StarRocks version

4.1.7-b81ff9c, shared-nothing and shared-data.

## Impact

**Correctness.** StarRocks documents multi-table transactions as all-or-nothing. A query joining or comparing the tables can return a state that never existed, for example an order without its line items. Inside `INSERT ... SELECT`, that state is written to a table permanently.

## Triggers

1. A transaction writes to two tables (SQL transaction or multi-table Stream Load).
2. A query reading both tables is planned while that transaction becomes visible.

No faults needed.

## Steps to reproduce the behavior

1. Create two identical tables:
   ```sql
   CREATE TABLE a (id BIGINT, v INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 32;
   CREATE TABLE b (id BIGINT, v INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 32;
   ```
2. In a loop, commit transactions that insert the same rows into both:
   `BEGIN WORK; INSERT INTO a VALUES (i,1),(i,2); INSERT INTO b VALUES (i,1),(i,2); COMMIT WORK;`
3. In 4 other sessions, repeatedly run:
   `SELECT (SELECT count(*) FROM a WHERE v > 0), (SELECT count(*) FROM b WHERE v > 0);`

Script: [`repro/torn_read.sh`](https://github.com/MattxAntithesis/starrocks-antithesis/blob/main/repro/torn_read.sh)

## Expected behavior

Both counts are always equal.

## Real behavior

About 1 read in 440 returns different counts (5 of 2,205 in one 90-second run, for example `236 238`). On shared-data it is about 1 in 97. Using `INSERT INTO snap SELECT ...` with the same query, 3 of 2,813 stored rows held counts that never coexisted.

## Root cause

1. Commit is atomic: `finishTransaction` write-locks every table in the transaction while it updates their visible versions. [DatabaseTransactionMgr.java#L1208-L1218](https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/transaction/DatabaseTransactionMgr.java#L1208-L1218)
2. The query copies each table under a lock ([OlapTable.java#L386](https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/catalog/OlapTable.java#L386)), but the copy shares the live `PhysicalPartition` objects ([Partition.java#L147](https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/catalog/Partition.java#L147)).
3. Each scan reads `getVisibleVersion()` later, with no lock held ([OlapScanNode.java#L637](https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/planner/OlapScanNode.java#L637)). A commit between the scan of `a` and the scan of `b` gives them different versions.
4. The post-plan check only looks at schema changes, not data commits ([OptimisticVersion.java#L38-L41](https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/sql/OptimisticVersion.java#L38-L41)).

Tracing the versions on a live frontend (FE) confirmed the order: the query read table `a` at version 5, then transaction 4245 moved both tables to version 6, then the query read table `b` at version 6.

```
READV  query 3bc1  table=a  version=5
FIN_BEGIN txn 4245 ; version a 5->6 ; version b 5->6 ; FIN_END
READV  query 3bc1  table=b  version=6
```

## Related issues

#82969 / #82970: the lock collector misses tables referenced only in SELECT-list subqueries, so `SET cbo_use_lock_db = true` doesn't help the query above. With a `CROSS JOIN` form, which the collector does see, `cbo_use_lock_db = true` gave 0 torn reads in about 6,000. We found no report of the missing version snapshot itself.
