> **Draft GitHub issue for StarRocks/starrocks. Not yet filed.** The first heading is the issue title; everything after it is the issue body.
# [Bug] A query reading two tables can see a multi-table transaction half-applied (and INSERT ... SELECT can store that state)

Hello,

When testing StarRocks with [Antithesis](https://antithesis.com/), we found that a single query reading two tables can see a committed multi-table transaction in one table but not the other. This happens when the transaction becomes visible while the query is being planned. Commit is atomic, but the planner reads each table's visible version separately, with no lock held, from live partition objects. A commit that lands between the two reads gives the query table A at version N and table B at N+1. Inside `INSERT ... SELECT`, that half-applied state is written to a table and stays there.

## StarRocks version (Required)

`4.1.7-b81ff9c` (official `starrocks/fe-ubuntu:4.1.7` and `starrocks/be-ubuntu:4.1.7` images), shared-nothing and shared-data.

## Impact

**Correctness (atomicity of reads).** StarRocks documents SQL transactions as applied or undone together (`docs/en/loading/SQL_transaction.md`), and multi-table Stream Load transactions as becoming visible "all or nothing" (`docs/en/loading/Flink_multi_table_transaction.md`). A query that joins or compares two tables touched by one transaction can return an answer that matches no state the database was ever in: an order without its line items, or two ledgers that don't balance. Rerunning gives the right answer. With `INSERT ... SELECT`, an ETL step, summary table or reconciliation snapshot can store the inconsistent result with no error.

No faults are needed. Locally it happens in about 1 read in 440 on shared-nothing and about 1 in 97 on shared-data, under a steady stream of transactions.

## Triggers

1. A transaction writes to two tables (SQL transaction or multi-table Stream Load transaction).
2. A query reading both tables is being planned while that transaction becomes visible.

## Symptom

`SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM b)` returns different counts, although every transaction inserts the same rows into both tables.

## Steps to reproduce the behavior (Required)

1 FE, 3 BEs. One replica makes it more frequent but isn't required.

1. Create the tables:
   ```sql
   CREATE DATABASE tr;
   CREATE TABLE tr.a (id BIGINT NOT NULL, v INT NOT NULL) DUPLICATE KEY(id)
     DISTRIBUTED BY HASH(id) BUCKETS 32 PROPERTIES ('replication_num' = '1');
   CREATE TABLE tr.b (id BIGINT NOT NULL, v INT NOT NULL) DUPLICATE KEY(id)
     DISTRIBUTED BY HASH(id) BUCKETS 32 PROPERTIES ('replication_num' = '1');
   ```
2. In one shell, commit transactions that insert 2 rows into each table:
   ```bash
   i=0; while true; do i=$((i+1))
     mysql -h <fe> -P9030 -uroot -D tr -e "BEGIN WORK; INSERT INTO a VALUES ($i,1),($i,2); INSERT INTO b VALUES ($i,1),($i,2); COMMIT WORK;"
   done
   ```
3. In 4 other shells, read both counts in one statement:
   ```bash
   while true; do
     mysql -h <fe> -P9030 -uroot -N -e "SELECT (SELECT count(*) FROM tr.a WHERE v > 0), (SELECT count(*) FROM tr.b WHERE v > 0)" \
       | awk '$1 != $2 { print "TORN:", $0 }'
   done
   ```
   (`WHERE v > 0` is true for every row. It keeps the counts from being answered from metadata.)

Script: [`repro/torn_read.sh`](https://github.com/MattxAntithesis/starrocks-antithesis/blob/main/repro/torn_read.sh) (`FILTER=1 REPL=1 torn_read.sh 90 32 4`; `SHARED=1` for shared-data). For the stored variant, [`repro/torn_persist.sh`](https://github.com/MattxAntithesis/starrocks-antithesis/blob/main/repro/torn_persist.sh) runs `INSERT INTO snap SELECT (SELECT count(*) ...), (SELECT count(*) ...)` the same way.

## Expected behavior (Required)

Every read returns two equal counts. Each transaction is either visible in both tables or in neither.

## Real behavior (Required)

About 1 read in 440 returns different counts. In one 90-second run, 5 of 2,205 reads were torn, for example `236 238`: table `a` one transaction behind `b`. In every torn read, the table planned first was the one behind; with the subqueries swapped, the lag moved to the other table. On shared-data (S3 storage, 3 CNs), 47 of 4,544 reads were torn. With `INSERT ... SELECT`, 3 of 2,813 stored rows held counts that never existed together.

## What Antithesis saw

With a multi-table Stream Load transaction into tables `ev` and `ev2`, a read that began 2 ms before the transaction became visible saw 3 of its rows in `ev` and none in `ev2`. The same happened in a shared-data run: the read started 4 ms before visibility and saw 0 rows in `ev` and 4 in `ev2`.

We confirmed the order of events on a live FE by logging, per query, the version read for each table, and the transaction's version bumps:

```
READV  query 3bc1  table=a  version=5
FIN_BEGIN txn 4245 ; version a 5->6 ; version b 5->6 ; FIN_END
READV  query 3bc1  table=b  version=6
```

## Root cause

1. **Commit is atomic.** `finishTransaction` takes write locks on every table in the transaction while it updates their visible versions:
   https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/transaction/DatabaseTransactionMgr.java#L1208-L1218
2. **Queries take no snapshot of the versions.** Queries are planned without holding table locks. `OlapTable.copyOnlyForQuery` copies the table under a lock and releases it:
   https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/catalog/OlapTable.java#L386
   but `Partition.shallowCopy` keeps references to the live `PhysicalPartition` objects, which hold the visible version:
   https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/catalog/Partition.java#L147
3. **Each table's version is read later, separately, with no lock.** `OlapScanNode.addScanRangeLocations` reads `physicalPartition.getVisibleVersion()` when the plan builder reaches that scan:
   https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/planner/OlapScanNode.java#L637
   A commit between the scan for `a` and the scan for `b` gives the two scans different versions.
4. **Nothing catches it.** The check after planning, `OptimisticVersion.validateTableUpdate`, only compares the table's schema-change time, not data commits:
   https://github.com/StarRocks/starrocks/blob/b81ff9cc465a5f2e5e53104d87a59b349808abd9/fe/fe-core/src/main/java/com/starrocks/sql/OptimisticVersion.java#L38-L41

A possible direction: snapshot the visible versions of all scanned partitions once, under the table locks taken for the copy, and have the scan nodes use that snapshot.

## Related issues

#82969 / #82970: the pre-analysis collector misses tables referenced only in a SELECT-list subquery. Because of it, `SET cbo_use_lock_db = true` locks nothing for the query shape above and doesn't help. That is a separate gap: with a query shape the collector does see (`CROSS JOIN`), `cbo_use_lock_db = true` gave 0 torn reads in about 6,000, which matches the cause above. We found no existing report of the missing version snapshot.

Full write-up: [FINDINGS.md, Finding 3](https://github.com/MattxAntithesis/starrocks-antithesis/blob/main/FINDINGS.md#finding-3-one-query-can-see-a-committed-transaction-half-applied).
