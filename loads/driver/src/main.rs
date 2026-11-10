//! Antithesis driver for StarRocks (shared-nothing): loads and transactions.
//!
//! Tables (database `aw`, replication 3 over 3 BEs):
//! * `kv`   primary-key table, one row per key: upserts and deletes via INSERT,
//!          DELETE and Stream Load (`__op`); reads checked against per-key
//!          histories.
//! * `ev`, `ev2` duplicate-key tables: each batch of rows is loaded by one
//!          transaction (INSERT, Stream Load via FE or BE, SQL transaction
//!          across both tables, Stream Load transaction interface across both
//!          tables). Reads check that a batch is all-or-nothing, never doubled
//!          (label idempotency), and never lost once acknowledged.

mod client;
mod inspect;
mod model;

use antithesis_sdk::prelude::*;
use antithesis_sdk::{assert_always, assert_reachable, assert_sometimes};
use client::{classify_stream, exec_dml, trunc, Cluster, Http, Outcome, DB};
use hegel::generators as gs;
use hegel::TestCase;
use model::{Batch, Batches, KeyHistory, Method, Seen, Write};
use mysql::prelude::Queryable;
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

const NKEYS: i32 = 12;
/// How long the workload must have been running before an eventually may fire.
/// Antithesis needs time to inject faults: with no floor here, an eventually
/// fired within seconds, stopped faults and ended the timeline before a single
/// kill or partition landed (run a1def8152144102b8b521a92cc35e1df-67-0).
const MIN_EVENTUALLY_AGE: Duration = Duration::from_secs(60);
/// Per-call probability once past that floor. The rule is called tens of times
/// a second across the workers, so this puts an eventually minutes in, at a
/// depth that varies between timelines.
const EVENTUALLY_P: f64 = 0.0002;
/// Replicas per tablet, so a partial reading of a tablet is not read as agreement.
const REPLICATION: usize = 3;
/// Keys from here up are never upserted whole or deleted, only partially
/// updated, so their auto-increment value is assigned once and must then
/// survive every partial update. Without such a population the check almost
/// never applies, because any full upsert legitimately reassigns the value.
const AI_STABLE_FROM: i32 = NKEYS - 4;
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);
const READ_TIMEOUT: Duration = Duration::from_secs(10);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn log(msg: impl AsRef<str>) {
    println!("[driver] {}", msg.as_ref());
}

/// A Stream Load transaction that was prepared and awaits commit or rollback.
struct Prepared {
    batch: i64,
    label: String,
}

pub struct Inner {
    cl: Cluster,
    http: Http,
    t0: Instant,
    keys: Vec<Mutex<KeyHistory>>,
    /// Every value ever sent to `kv`, with its key. Never pruned.
    written: Mutex<HashMap<i64, i32>>,
    next_value: AtomicI64,
    next_batch: AtomicI64,
    batches: Mutex<Batches>,
    prepared: Mutex<Vec<Prepared>>,
    /// Rules hold it shared while acting; an eventually takes it exclusively.
    quiesce: RwLock<()>,
    eventually_lock: Mutex<()>,
    eventually_enabled: bool,
    /// Multi-table Stream Load transactions need StarRocks 4.0 or newer.
    multi_table_txn: bool,
    /// When `/workload-active` appeared, so the eventually can wait for faults.
    workload_start: Instant,
    last_observe: Mutex<Option<Instant>>,
    last_inspect: Mutex<Option<Instant>>,
    /// When a schema change was last started, to pace them.
    last_alter: Mutex<Option<Instant>>,
    /// Columns this workload added to `ev`/`ev2`, so it can drop them again.
    added_columns: Mutex<HashMap<&'static str, Vec<String>>>,
    next_alter: AtomicI64,
    /// Per-column histories for `pk`, so a partial update that touches one
    /// column is judged against that column alone.
    pk_v: Vec<Mutex<KeyHistory>>,
    pk_s: Vec<Mutex<KeyHistory>>,
    /// Last auto-increment value seen for a key and when it was seen. A row
    /// keeps its value until the row goes away, so this is only comparable
    /// while no delete has been sent since that observation.
    pk_ai: Vec<Mutex<Option<(i64, Instant)>>>,
    /// When each key's row was last sent away or replaced wholesale. A full
    /// upsert does not mention `n`, so StarRocks assigns the new row a fresh
    /// value; only a partial update has to preserve it.
    pk_last_replace: Vec<Mutex<Option<Instant>>>,
    /// Outcome counters, printed periodically.
    stats: Mutex<BTreeMap<String, u64>>,
}

#[derive(Clone)]
pub struct Driver(Arc<Inner>);

impl Deref for Driver {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.0
    }
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

fn sql_ok(cl: &Cluster, db: bool, sql: &str) -> Result<(), String> {
    let mut c = cl.conn(db, WRITE_TIMEOUT)?;
    c.query_drop(sql).map_err(|e| e.to_string())
}

/// Rows of `SHOW BACKENDS` as (host, alive).
fn backends(cl: &Cluster) -> Result<Vec<(String, bool)>, String> {
    let mut c = cl.conn(false, READ_TIMEOUT)?;
    let res: Vec<mysql::Row> = c.query("SHOW BACKENDS").map_err(|e| e.to_string())?;
    let mut out = vec![];
    for row in res {
        let cols = row.columns();
        let idx = |name: &str| cols.iter().position(|c| c.name_str() == name);
        let (Some(h), Some(a)) = (idx("IP"), idx("Alive")) else {
            return Err("SHOW BACKENDS without IP/Alive".into());
        };
        let host: String = row.get_opt(h).and_then(|r| r.ok()).unwrap_or_default();
        let alive: String = row.get_opt(a).and_then(|r| r.ok()).unwrap_or_default();
        out.push((host, alive.eq_ignore_ascii_case("true")));
    }
    Ok(out)
}

/// Retries `f` until it succeeds, printing why it is waiting when that changes.
fn wait_for(what: &str, mut f: impl FnMut() -> Result<(), String>) {
    let mut last = String::new();
    loop {
        match f() {
            Ok(()) => {
                log(format!("{what}: ok"));
                return;
            }
            Err(e) => {
                if e != last {
                    log(format!("waiting for {what}: {}", trunc(&e)));
                    last = e;
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

const DDL: &[&str] = &[
    "CREATE DATABASE IF NOT EXISTS aw",
    "CREATE TABLE IF NOT EXISTS aw.kv (k INT NOT NULL, v BIGINT NOT NULL) \
     PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES(\"replication_num\"=\"3\")",
    "CREATE TABLE IF NOT EXISTS aw.ev (batch BIGINT NOT NULL, seq INT NOT NULL, k INT NOT NULL, val BIGINT NOT NULL) \
     DUPLICATE KEY(batch, seq) DISTRIBUTED BY HASH(batch, seq) BUCKETS 4 PROPERTIES(\"replication_num\"=\"3\")",
    "CREATE TABLE IF NOT EXISTS aw.ev2 (batch BIGINT NOT NULL, seq INT NOT NULL, k INT NOT NULL, val BIGINT NOT NULL) \
     DUPLICATE KEY(batch, seq) DISTRIBUTED BY HASH(batch, seq) BUCKETS 4 PROPERTIES(\"replication_num\"=\"3\")",
    // Partial column updates are the project's largest cluster of backported
    // fixes, and several of them involve an AUTO_INCREMENT column or a
    // conditional update (antithesis/BUG-HISTORY.md). `s` is CHAR so trailing
    // padding is expressible; `ver` is the merge_condition column; `n` is the
    // auto-increment column whose value must survive a partial update.
    "CREATE TABLE IF NOT EXISTS aw.pk (k INT NOT NULL, v BIGINT NOT NULL, s CHAR(16) NOT NULL, \
     ver BIGINT NOT NULL, n BIGINT NOT NULL AUTO_INCREMENT) \
     PRIMARY KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 4 PROPERTIES(\"replication_num\"=\"3\")",
    // Seed the partial-update-only keys. INSERT upserts by key, so a retry is
    // harmless. v=0 and s='' are generation 0, the column defaults.
    "INSERT INTO aw.pk (k, v, s, ver) VALUES (8,0,'',0),(9,0,'',0),(10,0,'',0),(11,0,'',0)",
    "CREATE TABLE IF NOT EXISTS aw.probe (id BIGINT NOT NULL) \
     DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES(\"replication_num\"=\"3\")",
];

/// A write and a read both work: the cluster serves the workload.
fn probe(cl: &Cluster, id: i64) -> Result<(), String> {
    match exec_dml(cl, &format!("INSERT INTO probe VALUES ({id})"), WRITE_TIMEOUT) {
        Outcome::Visible => {}
        o => return Err(format!("probe insert: {} {}", o.kind(), o.msg())),
    }
    let mut c = cl.conn(true, READ_TIMEOUT)?;
    let n: Option<i64> = c
        .query_first(format!("SELECT count(*) FROM probe WHERE id = {id}"))
        .map_err(|e| format!("probe read: {e}"))?;
    if n == Some(1) {
        Ok(())
    } else {
        Err(format!("probe read returned {n:?}"))
    }
}

fn all_backends_alive(cl: &Cluster) -> Result<(), String> {
    let b = backends(cl)?;
    let alive = b.iter().filter(|(_, a)| *a).count();
    if alive >= cl.bes.len() {
        Ok(())
    } else {
        Err(format!("{alive}/{} backends alive: {b:?}", cl.bes.len()))
    }
}

fn startup(cl: &Cluster) {
    wait_for("FE answers queries", || sql_ok(cl, false, "SELECT 1"));
    for be in &cl.bes {
        wait_for(&format!("add backend {be}"), || {
            match sql_ok(cl, false, &format!("ALTER SYSTEM ADD BACKEND '{be}:9050'")) {
                Ok(()) => Ok(()),
                Err(e) if e.to_lowercase().contains("already exist") => Ok(()),
                Err(e) => Err(e),
            }
        });
    }
    wait_for("all backends alive", || all_backends_alive(cl));
    for ddl in DDL {
        wait_for(&format!("ddl {}", &ddl[..ddl.len().min(40)]), || sql_ok(cl, false, ddl));
    }
    let mut id = 0;
    wait_for("probe write and read", || {
        id += 1;
        probe(cl, -id)
    });
}

// ---------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------

fn csv_rows(rows: &[String]) -> Vec<u8> {
    let mut s = rows.join("\n");
    s.push('\n');
    s.into_bytes()
}

impl Driver {
    fn new(cl: Cluster) -> Self {
        let t0 = Instant::now();
        Driver(Arc::new(Inner {
            http: Http::new(WRITE_TIMEOUT),
            cl,
            t0,
            keys: (0..NKEYS).map(|_| Mutex::new(KeyHistory::new(t0))).collect(),
            written: Mutex::new(HashMap::new()),
            next_value: AtomicI64::new(1),
            next_batch: AtomicI64::new(1),
            batches: Mutex::new(Batches::default()),
            prepared: Mutex::new(vec![]),
            quiesce: RwLock::new(()),
            eventually_lock: Mutex::new(()),
            eventually_enabled: std::env::var("DRIVER_EVENTUALLY").as_deref() != Ok("off"),
            multi_table_txn: std::env::var("DRIVER_MULTI_TABLE_TXN").as_deref() != Ok("off"),
            workload_start: t0,
            last_observe: Mutex::new(None),
            last_inspect: Mutex::new(None),
            last_alter: Mutex::new(None),
            added_columns: Mutex::new(HashMap::new()),
            next_alter: AtomicI64::new(1),
            pk_v: (0..NKEYS)
                .map(|k| Mutex::new(if k >= AI_STABLE_FROM {
                    KeyHistory::seeded(t0, 0)
                } else {
                    KeyHistory::new(t0)
                }))
                .collect(),
            pk_s: (0..NKEYS)
                .map(|k| Mutex::new(if k >= AI_STABLE_FROM {
                    KeyHistory::seeded(t0, 0)
                } else {
                    KeyHistory::new(t0)
                }))
                .collect(),
            pk_ai: (0..NKEYS).map(|_| Mutex::new(None)).collect(),
            pk_last_replace: (0..NKEYS).map(|_| Mutex::new(None)).collect(),
            stats: Mutex::new(BTreeMap::new()),
        }))
    }

    fn count(&self, what: impl Into<String>) {
        *lock(&self.stats).entry(what.into()).or_insert(0) += 1;
    }

    fn ms(&self, t: Instant) -> u128 {
        t.saturating_duration_since(self.t0).as_millis()
    }

    // ----- kv: primary-key table -------------------------------------------

    /// Records writes to keys as sent; returns their indices for `kv_ack`.
    fn kv_send(&self, writes: &[(i32, Option<i64>)]) -> Vec<(i32, usize)> {
        let now = Instant::now();
        let mut idx = vec![];
        for (k, v) in writes {
            if let Some(v) = v {
                lock(&self.written).insert(*v, *k);
            }
            let mut h = lock(&self.keys[*k as usize]);
            h.writes.push(Write { value: *v, sent: now, acked: None });
            idx.push((*k, h.writes.len() - 1));
        }
        idx
    }

    fn kv_ack(&self, writes: &[(i32, Option<i64>)], outcome: &Outcome) {
        let now = Instant::now();
        for (k, v) in writes {
            let mut h = lock(&self.keys[*k as usize]);
            if *outcome == Outcome::Visible {
                // Find our write by value (deletes: the latest unacked delete
                // sent by us is indistinguishable, any one works).
                if let Some(w) = h
                    .writes
                    .iter_mut()
                    .rev()
                    .find(|w| w.value == *v && w.acked.is_none())
                {
                    w.acked = Some(now);
                }
            }
            h.prune(now);
        }
    }

    fn draw_kv_writes(&self, tc: &TestCase, allow_delete: bool) -> Vec<(i32, Option<i64>)> {
        let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(3));
        let mut keys: Vec<i32> = vec![];
        let mut out = vec![];
        for _ in 0..n {
            let k = tc.draw(gs::integers::<i32>().min_value(0).max_value(NKEYS - 1));
            let del = allow_delete && tc.draw(gs::weighted_booleans(0.2));
            if keys.contains(&k) {
                continue;
            }
            keys.push(k);
            let v = if del { None } else { Some(self.next_value.fetch_add(1, Ordering::SeqCst)) };
            out.push((k, v));
        }
        out
    }

    fn kv_outcome_assertions(&self, how: &str, o: &Outcome) {
        self.count(format!("kv_{how}:{}", o.kind()));
        if *o != Outcome::Visible {
            log(format!("kv {how}: {} {}", o.kind(), trunc(&o.msg())));
        }
        let d = json!({"method": how, "outcome": o.kind(), "msg": trunc(&o.msg())});
        match how {
            "insert" => assert_sometimes!(*o == Outcome::Visible, "kv insert is acknowledged visible", &d),
            "delete" => assert_sometimes!(*o == Outcome::Visible, "kv delete is acknowledged visible", &d),
            _ => assert_sometimes!(*o == Outcome::Visible, "kv stream load is acknowledged visible", &d),
        }
        assert_sometimes!(
            matches!(o, Outcome::Unknown(_)),
            "a kv write has an unknown outcome",
            &d
        );
    }

    fn kv_insert(&self, tc: &TestCase) {
        let writes = self.draw_kv_writes(tc, false);
        let values: Vec<String> =
            writes.iter().map(|(k, v)| format!("({k}, {})", v.unwrap())).collect();
        let sql = format!("INSERT INTO kv VALUES {}", values.join(", "));
        self.kv_send(&writes);
        let o = exec_dml(&self.cl, &sql, WRITE_TIMEOUT);
        self.kv_outcome_assertions("insert", &o);
        self.kv_ack(&writes, &o);
    }

    fn kv_delete(&self, tc: &TestCase) {
        let k = tc.draw(gs::integers::<i32>().min_value(0).max_value(NKEYS - 1));
        let writes = vec![(k, None)];
        self.kv_send(&writes);
        let o = exec_dml(&self.cl, &format!("DELETE FROM kv WHERE k = {k}"), WRITE_TIMEOUT);
        self.kv_outcome_assertions("delete", &o);
        self.kv_ack(&writes, &o);
    }

    fn kv_stream_load(&self, tc: &TestCase) {
        let writes = self.draw_kv_writes(tc, true);
        let via_be = tc.draw(gs::booleans());
        let be = tc.draw(gs::sampled_from(self.cl.bes.clone()));
        let rows: Vec<String> = writes
            .iter()
            .map(|(k, v)| match v {
                Some(v) => format!("{k},{v},0"),
                None => format!("{k},0,1"),
            })
            .collect();
        let label = format!("kv_{}", self.next_value.fetch_add(1, Ordering::SeqCst));
        let host = if via_be { format!("{be}:8040") } else { format!("{}:8030", self.cl.fe) };
        let url = format!("http://{host}/api/{DB}/kv/_stream_load");
        self.kv_send(&writes);
        let r = self.http.send(
            "PUT",
            &url,
            &[
                ("label", label),
                ("column_separator", ",".into()),
                ("columns", "k,v,__op".into()),
            ],
            &csv_rows(&rows),
        );
        let o = classify_stream(&r);
        self.kv_outcome_assertions("stream_load", &o);
        self.kv_ack(&writes, &o);
    }

    fn kv_read(&self, tc: &TestCase) {
        let full = tc.draw(gs::weighted_booleans(0.3));
        let keys: Vec<i32> = if full {
            (0..NKEYS).collect()
        } else {
            let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(4));
            let mut ks = vec![];
            for _ in 0..n {
                let k = tc.draw(gs::integers::<i32>().min_value(0).max_value(NKEYS - 1));
                if !ks.contains(&k) {
                    ks.push(k);
                }
            }
            ks
        };
        let sql = if full {
            "SELECT k, v FROM kv".to_string()
        } else {
            let list: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
            format!("SELECT k, v FROM kv WHERE k IN ({})", list.join(","))
        };
        let start = Instant::now();
        let res: Result<Vec<(i32, i64)>, String> = self
            .cl
            .conn(true, READ_TIMEOUT)
            .and_then(|mut c| c.query(&sql).map_err(|e| e.to_string()));
        let end = Instant::now();
        let rows = match res {
            Ok(r) => r,
            Err(e) => {
                self.count("kv_read:failed");
                tc.note(&format!("kv read failed: {e}"));
                return;
            }
        };
        self.count("kv_read:ok");
        self.check_kv_rows(&keys, &rows, start, end, false);
    }

    fn check_kv_rows(&self, keys: &[i32], rows: &[(i32, i64)], start: Instant, end: Instant, final_check: bool) {
        let mut got: BTreeMap<i32, Vec<i64>> = BTreeMap::new();
        for (k, v) in rows {
            got.entry(*k).or_default().push(*v);
        }
        for (k, vs) in &got {
            assert_always!(
                vs.len() == 1,
                "a primary key appears at most once in a read",
                &json!({"key": k, "values": vs, "final": final_check})
            );
            if !(0..NKEYS).contains(k) {
                assert_always!(false, "kv reads return only values the workload wrote", &json!({"key": k, "values": vs}));
            }
        }
        for k in keys {
            let v = got.get(k).map(|vs| vs[0]);
            if let Some(v) = v {
                let wk = lock(&self.written).get(&v).copied();
                assert_always!(
                    wk == Some(*k),
                    "kv reads return only values the workload wrote",
                    &json!({"key": k, "value": v, "written_to_key": wk})
                );
            }
            let h = lock(&self.keys[*k as usize]);
            let ok = h.may_return(v, start, end);
            let details = json!({
                "key": k, "got": v, "read_start_ms": self.ms(start), "read_end_ms": self.ms(end),
                "history": h.summary(end), "final": final_check,
            });
            if final_check {
                assert_always!(ok, "acknowledged kv writes survive once faults stop", &details);
            } else {
                assert_always!(ok, "a kv read reflects every write acknowledged before it started", &details);
            }
            let pending = h.writes.iter().any(|w| w.acked.is_none() && w.sent < end);
            if !final_check {
                assert_sometimes!(pending && ok, "a kv read overlaps a write to the same key", &details);
                assert_sometimes!(v.is_some(), "a kv read finds a value", &details);
                assert_sometimes!(v.is_none() && h.writes.len() > 1, "a kv read finds a key deleted", &details);
            }
        }
    }

    // ----- pk: partial column updates ---------------------------------------

    /// Every column of `pk` is derived from one generation number, so a read
    /// can be attributed to the write that produced it: `v` is the generation
    /// and `s` is `p<generation>`.
    fn pk_row(k: i32, g: i64) -> String {
        format!("{k},{g},p{g},{g}")
    }

    /// How long a row-replacing write keeps the auto-increment value in doubt.
    fn replace_horizon(o: &Outcome) -> Instant {
        let now = Instant::now();
        match o {
            Outcome::Visible => now,
            _ => now + model::AMBIGUITY_WINDOW,
        }
    }

    fn pk_hist(&self, col: &str, k: i32) -> &Mutex<KeyHistory> {
        if col == "v" { &self.pk_v[k as usize] } else { &self.pk_s[k as usize] }
    }

    /// Records a write to `cols` of key `k` as sent. `g` is None for a delete.
    fn pk_send(&self, k: i32, cols: &[&str], g: Option<i64>) {
        let now = Instant::now();
        for c in cols {
            let mut h = lock(self.pk_hist(c, k));
            h.writes.push(Write { value: g, sent: now, acked: None });
        }
        if g.is_none() {
            self.pk_row_replaced(k, now);
        }
    }

    /// The row is gone or written whole, so its auto-increment value is no
    /// longer the one previously observed. `until` is the latest moment the
    /// replacement might still land: now for one already acknowledged visible,
    /// and a whole ambiguity window later for one whose outcome is unknown,
    /// since such a write can take effect at any point in that window.
    fn pk_row_replaced(&self, k: i32, until: Instant) {
        *lock(&self.pk_ai[k as usize]) = None;
        let mut last = lock(&self.pk_last_replace[k as usize]);
        *last = Some(match *last {
            Some(t) if t > until => t,
            _ => until,
        });
    }

    fn pk_ack(&self, k: i32, cols: &[&str], g: Option<i64>, acked: bool) {
        let now = Instant::now();
        for c in cols {
            let mut h = lock(self.pk_hist(c, k));
            if acked {
                if let Some(w) = h.writes.iter_mut().rev().find(|w| w.value == g && w.acked.is_none()) {
                    w.acked = Some(now);
                }
            }
            h.prune(now);
        }
    }

    /// A full upsert through Stream Load: every column written together.
    fn pk_upsert(&self, tc: &TestCase) {
        let k = tc.draw(gs::integers::<i32>().min_value(0).max_value(AI_STABLE_FROM - 1));
        let be = tc.draw(gs::sampled_from(self.cl.bes.clone()));
        let via_be = tc.draw(gs::booleans());
        let g = self.next_value.fetch_add(1, Ordering::SeqCst);
        let cols = ["v", "s"];
        self.pk_send(k, &cols, Some(g));
        // A full upsert writes the row whole without naming `n`, so the row it
        // leaves behind carries a freshly assigned auto-increment value.
        self.pk_row_replaced(k, Instant::now());
        let host = if via_be { format!("{be}:8040") } else { format!("{}:8030", self.cl.fe) };
        let r = self.http.send(
            "PUT",
            &format!("http://{host}/api/{DB}/pk/_stream_load"),
            &[
                ("label", format!("pk_{g}")),
                ("column_separator", ",".into()),
                ("columns", "k,v,s,ver".into()),
            ],
            &csv_rows(&[Self::pk_row(k, g)]),
        );
        let o = classify_stream(&r);
        self.pk_row_replaced(k, Self::replace_horizon(&o));
        self.count(format!("pk_upsert:{}", o.kind()));
        assert_sometimes!(o == Outcome::Visible, "a pk upsert is acknowledged visible",
            &json!({"key": k, "gen": g, "outcome": o.kind(), "msg": trunc(&o.msg())}));
        self.pk_ack(k, &cols, Some(g), o == Outcome::Visible);
    }

    /// A partial column update: only some columns are written, and the rest
    /// (including the auto-increment column) must survive untouched.
    fn pk_partial(&self, tc: &TestCase) {
        let k = tc.draw(gs::integers::<i32>().min_value(0).max_value(NKEYS - 1));
        let be = tc.draw(gs::sampled_from(self.cl.bes.clone()));
        let via_be = tc.draw(gs::booleans());
        let column_mode = tc.draw(gs::weighted_booleans(0.7));
        let conditional = tc.draw(gs::weighted_booleans(0.3));
        let which = tc.draw(gs::sampled_from(vec!["v", "s"]));
        let g = self.next_value.fetch_add(1, Ordering::SeqCst);
        let cols: Vec<&str> = vec![which];
        // merge_condition needs the condition column in the payload.
        let (columns, row) = if conditional {
            (format!("k,{which},ver"), match which {
                "v" => format!("{k},{g},{g}"),
                _ => format!("{k},p{g},{g}"),
            })
        } else {
            (format!("k,{which}"), match which {
                "v" => format!("{k},{g}"),
                _ => format!("{k},p{g}"),
            })
        };
        self.pk_send(k, &cols, Some(g));
        // A partial update to a key with no row creates one, leaving every
        // column it does not mention at that column's default. Record the
        // default as possible for those columns; it never gets acknowledged,
        // so it stays possible only for the ambiguity window.
        let untouched: Vec<&str> = ["v", "s"].into_iter().filter(|c| *c != which).collect();
        self.pk_send(k, &untouched, Some(0));
        let host = if via_be { format!("{be}:8040") } else { format!("{}:8030", self.cl.fe) };
        let mut hdr: Vec<(&str, String)> = vec![
            ("label", format!("pkp_{g}")),
            ("column_separator", ",".into()),
            ("columns", columns.clone()),
            ("partial_update", "true".into()),
        ];
        if column_mode {
            hdr.push(("partial_update_mode", "column".into()));
        }
        if conditional {
            hdr.push(("merge_condition", "ver".into()));
        }
        let r = self.http.send(
            "PUT",
            &format!("http://{host}/api/{DB}/pk/_stream_load"),
            &hdr,
            &csv_rows(&[row]),
        );
        let o = classify_stream(&r);
        self.count(format!("pk_partial:{}{}", if column_mode { "col:" } else { "row:" }, o.kind()));
        let d = json!({"key": k, "gen": g, "column": which, "column_mode": column_mode,
                       "conditional": conditional, "outcome": o.kind(), "msg": trunc(&o.msg())});
        antithesis_sdk::assert_sometimes_all!(
            {
                partial_update_succeeded: o == Outcome::Visible,
                in_column_mode: column_mode,
                with_a_merge_condition: conditional
            },
            "a column-mode partial update with a merge condition succeeds",
            &d
        );
        assert_sometimes!(o == Outcome::Visible && !conditional,
            "a partial update is acknowledged visible", &d);
        // A conditional update applies only when its condition holds, so its
        // outcome is unknown even when the request succeeds: leave it unacked,
        // which lets a read return either the old or the new value.
        self.pk_ack(k, &cols, Some(g), o == Outcome::Visible && !conditional);
    }

    fn pk_delete(&self, tc: &TestCase) {
        let k = tc.draw(gs::integers::<i32>().min_value(0).max_value(AI_STABLE_FROM - 1));
        let cols = ["v", "s"];
        self.pk_send(k, &cols, None);
        let o = exec_dml(&self.cl, &format!("DELETE FROM pk WHERE k = {k}"), WRITE_TIMEOUT);
        self.pk_row_replaced(k, Self::replace_horizon(&o));
        self.count(format!("pk_delete:{}", o.kind()));
        self.pk_ack(k, &cols, None, o == Outcome::Visible);
    }

    /// Decodes the `s` column into the generation that wrote it. Generation 0
    /// is the column default, which a partial update leaves behind when it
    /// creates a row that did not exist.
    ///
    /// Trailing padding is tolerated so a padded CHAR is not reported as a
    /// wrong value, but it is counted: returning CHAR padded from a
    /// primary-key lookup has been a bug before.
    fn decode_s(&self, s: &str) -> Option<i64> {
        if s.len() != s.trim_end().len() {
            self.count("pk_read:char_padded");
        }
        let t = s.trim_end();
        if t.is_empty() {
            return Some(0);
        }
        t.strip_prefix('p')?.parse::<i64>().ok()
    }

    fn pk_read(&self, tc: &TestCase) {
        let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(4));
        let mut keys = vec![];
        for _ in 0..n {
            let k = tc.draw(gs::integers::<i32>().min_value(0).max_value(NKEYS - 1));
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
        let list: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        let sql = format!("SELECT k, v, s, n FROM pk WHERE k IN ({})", list.join(","));
        let start = Instant::now();
        let res: Result<Vec<(i32, i64, String, i64)>, String> = self
            .cl
            .conn(true, READ_TIMEOUT)
            .and_then(|mut c| c.query(&sql).map_err(|e| e.to_string()));
        let end = Instant::now();
        let Ok(rows) = res else {
            self.count("pk_read:failed");
            return;
        };
        self.count("pk_read:ok");
        let mut got: HashMap<i32, (i64, String, i64)> = HashMap::new();
        for (k, v, s, n) in rows {
            got.insert(k, (v, s, n));
        }
        for k in &keys {
            let row = got.get(k);
            let gv = row.map(|r| r.0);
            let gs_dec = row.and_then(|r| self.decode_s(&r.1));
            let raw_s = row.map(|r| r.1.clone()).unwrap_or_default();
            // `v` holds the generation that last wrote it.
            let hv = lock(&self.pk_v[*k as usize]);
            let ok_v = hv.may_return(gv, start, end);
            let d = json!({"key": k, "v": gv, "s": raw_s, "s_decoded": gs_dec,
                           "history": hv.summary(end), "column": "v"});
            assert_always!(ok_v, "a pk read reflects every write acknowledged before it started", &d);
            drop(hv);
            assert_always!(
                row.is_none() || gs_dec.is_some(),
                "pk reads return only values the workload wrote",
                &json!({"key": k, "s": raw_s, "s_decoded": gs_dec})
            );
            let hs = lock(&self.pk_s[*k as usize]);
            let ok_s = hs.may_return(gs_dec, start, end);
            assert_always!(ok_s, "a pk read reflects every write acknowledged before it started",
                &json!({"key": k, "s": raw_s, "s_decoded": gs_dec,
                        "history": hs.summary(end), "column": "s"}));
            drop(hs);
            // The auto-increment value is assigned once per row and must
            // survive partial updates that do not mention it.
            if let Some((_, _, n)) = row {
                // A delete that has not been acknowledged may still land, so a
                // row read now can vanish and come back with a new value.
                let delete_pending = lock(&self.pk_v[*k as usize])
                    .writes
                    .iter()
                    .any(|w| w.value.is_none() && w.acked.is_none());
                let last_replace = *lock(&self.pk_last_replace[*k as usize]);
                let mut ai = lock(&self.pk_ai[*k as usize]);
                if delete_pending {
                    *ai = None;
                } else {
                    match *ai {
                        // `observed` is when the earlier read *started*: a
                        // replacement that completed while that read was in
                        // flight leaves it holding a value already stale.
                        Some((prev, observed)) if last_replace.map_or(true, |t| t <= observed) => {
                            assert_always!(
                                prev == *n,
                                "a row keeps its auto-increment value until it is deleted",
                                &json!({"key": k, "previous_n": prev, "n": n, "s": raw_s,
                                        "observed_ms_ago": observed.elapsed().as_millis()})
                            )
                        }
                        _ => *ai = Some((*n, start)),
                    }
                }
            }
        }
    }

    // ----- batches: duplicate-key tables, all-or-nothing --------------------

    fn new_batch(&self, rows: i64, tables: Vec<&'static str>, method: Method) -> Batch {
        let id = self.next_batch.fetch_add(1, Ordering::SeqCst);
        let b = Batch {
            id,
            rows,
            tables,
            method,
            label: format!("b{id}_{}", method.name()),
            sent: Instant::now(),
            visible_at: None,
            committed: false,
            rolled_back: false,
            seen_at: None,
            last_outcome: "pending".into(),
            attempts: 1,
        };
        lock(&self.batches).by_id.insert(id, b.clone());
        b
    }

    fn batch_csv(b: &Batch) -> Vec<u8> {
        let rows: Vec<String> =
            (0..b.rows).map(|s| format!("{},{s},{},{}", b.id, b.id % 97, b.id * 1000 + s)).collect();
        csv_rows(&rows)
    }

    fn batch_values(b: &Batch) -> String {
        let rows: Vec<String> =
            (0..b.rows).map(|s| format!("({}, {s}, {}, {})", b.id, b.id % 97, b.id * 1000 + s)).collect();
        rows.join(", ")
    }

    fn record_batch(&self, id: i64, o: &Outcome) {
        let now = Instant::now();
        let mut bs = lock(&self.batches);
        let Some(b) = bs.by_id.get_mut(&id) else { return };
        b.last_outcome = format!("{} {}", o.kind(), trunc(&o.msg()));
        *lock(&self.stats).entry(format!("batch_{}:{}", b.method.name(), o.kind())).or_insert(0) += 1;
        if *o != Outcome::Visible {
            log(format!("batch {} {} attempt {}: {} {}", id, b.method.name(), b.attempts, o.kind(), trunc(&o.msg())));
        }
        match o {
            Outcome::Visible => {
                if b.visible_at.is_none() {
                    b.visible_at = Some(now);
                }
            }
            Outcome::Committed => b.committed = true,
            _ => {}
        }
        let d = json!({"batch": id, "method": b.method.name(), "outcome": o.kind(), "msg": trunc(&o.msg()), "attempt": b.attempts});
        match b.method {
            Method::Insert => assert_sometimes!(*o == Outcome::Visible, "batch insert is acknowledged visible", &d),
            Method::StreamLoadFe => assert_sometimes!(*o == Outcome::Visible, "batch stream load via FE is acknowledged visible", &d),
            Method::StreamLoadBe => assert_sometimes!(*o == Outcome::Visible, "batch stream load via BE is acknowledged visible", &d),
            Method::SqlTxn => assert_sometimes!(*o == Outcome::Visible, "SQL transaction commit is acknowledged visible", &d),
            Method::TxnStreamLoad => assert_sometimes!(*o == Outcome::Visible, "stream load transaction commit is acknowledged visible", &d),
        }
        assert_sometimes!(*o == Outcome::Committed, "a load is committed but publish times out", &d);
        assert_sometimes!(matches!(o, Outcome::Unknown(_)), "a batch load has an unknown outcome", &d);
        assert_sometimes!(matches!(o, Outcome::Failed(_)), "a batch load fails", &d);
    }

    /// Sends one labelled single-table load (INSERT or Stream Load).
    fn send_single(&self, b: &Batch, via_be: Option<String>) -> Outcome {
        match b.method {
            Method::Insert => exec_dml(
                &self.cl,
                &format!("INSERT INTO ev WITH LABEL {} (batch, seq, k, val) VALUES {}", b.label, Self::batch_values(b)),
                WRITE_TIMEOUT,
            ),
            Method::StreamLoadFe | Method::StreamLoadBe => {
                let host = match (&b.method, via_be) {
                    (Method::StreamLoadBe, Some(be)) => format!("{be}:8040"),
                    _ => format!("{}:8030", self.cl.fe),
                };
                let r = self.http.send(
                    "PUT",
                    &format!("http://{host}/api/{DB}/ev/_stream_load"),
                    &[
                        ("label", b.label.clone()),
                        ("column_separator", ",".into()),
                        ("columns", "batch,seq,k,val".into()),
                    ],
                    &Self::batch_csv(b),
                );
                classify_stream(&r)
            }
            _ => unreachable!(),
        }
    }

    fn batch_load(&self, tc: &TestCase) {
        let method = tc.draw(gs::sampled_from(vec![
            Method::Insert,
            Method::StreamLoadFe,
            Method::StreamLoadBe,
            Method::SqlTxn,
            Method::TxnStreamLoad,
        ]));
        let rows = tc.draw(gs::integers::<i64>().min_value(1).max_value(8));
        let be = tc.draw(gs::sampled_from(self.cl.bes.clone()));
        match method {
            Method::Insert | Method::StreamLoadFe | Method::StreamLoadBe => {
                let retry_now = tc.draw(gs::weighted_booleans(0.5));
                let b = self.new_batch(rows, vec!["ev"], method);
                let o = self.send_single(&b, Some(be.clone()));
                self.record_batch(b.id, &o);
                if retry_now && !matches!(o, Outcome::Visible | Outcome::Committed) {
                    self.retry_batch(b.id, Some(be));
                }
            }
            Method::SqlTxn => {
                let both = tc.draw(gs::weighted_booleans(0.7));
                let rollback = tc.draw(gs::weighted_booleans(0.15));
                let tables = if both { vec!["ev", "ev2"] } else { vec!["ev"] };
                let b = self.new_batch(rows, tables, method);
                self.sql_txn(&b, rollback);
            }
            Method::TxnStreamLoad => {
                let both = tc.draw(gs::weighted_booleans(0.7)) && self.multi_table_txn;
                let finish = tc.draw(gs::sampled_from(vec!["commit", "commit", "commit", "rollback", "later"]));
                let tables = if both { vec!["ev", "ev2"] } else { vec!["ev"] };
                let b = self.new_batch(rows, tables, method);
                self.txn_stream_load(&b, finish);
            }
        }
    }

    /// Resends a single-table load with its original label. Must never apply twice.
    fn retry_batch(&self, id: i64, via_be: Option<String>) {
        let b = {
            let mut bs = lock(&self.batches);
            let Some(b) = bs.by_id.get_mut(&id) else { return };
            b.attempts += 1;
            b.clone()
        };
        let o = self.send_single(&b, via_be);
        let d = json!({"batch": id, "label": b.label, "attempt": b.attempts, "outcome": o.kind(), "msg": trunc(&o.msg())});
        self.count(format!("retry:{}", o.kind()));
        assert_sometimes!(o == Outcome::LabelExists, "a retried load finds its label already used", &d);
        assert_sometimes!(o == Outcome::Visible, "a retried load is acknowledged visible", &d);
        if o != Outcome::LabelExists {
            self.record_batch(id, &o);
        }
    }

    fn batch_retry(&self, tc: &TestCase) {
        let candidates: Vec<i64> = {
            let bs = lock(&self.batches);
            let mut v: Vec<i64> = bs
                .by_id
                .values()
                .filter(|b| {
                    matches!(b.method, Method::Insert | Method::StreamLoadFe | Method::StreamLoadBe)
                        && b.visible_at.is_none()
                        && !b.committed
                })
                .map(|b| b.id)
                .collect();
            v.sort();
            v
        };
        if candidates.is_empty() {
            return;
        }
        let id = tc.draw(gs::sampled_from(candidates));
        let be = tc.draw(gs::sampled_from(self.cl.bes.clone()));
        self.retry_batch(id, Some(be));
    }

    fn sql_txn(&self, b: &Batch, rollback: bool) {
        let mut c = match self.cl.conn(true, WRITE_TIMEOUT) {
            Ok(c) => c,
            Err(e) => {
                self.record_batch(b.id, &Outcome::Failed(e));
                return;
            }
        };
        if let Err(e) = c.query_drop("BEGIN") {
            self.record_batch(b.id, &Outcome::Failed(format!("begin: {e}")));
            return;
        }
        for t in &b.tables {
            let sql = format!("INSERT INTO {t} (batch, seq, k, val) VALUES {}", Self::batch_values(b));
            if let Err(e) = c.query_drop(&sql) {
                let _ = c.query_drop("ROLLBACK");
                self.record_batch(b.id, &Outcome::Failed(format!("insert {t}: {e}")));
                return;
            }
        }
        if rollback {
            let r = c.query_drop("ROLLBACK");
            let d = json!({"batch": b.id, "result": format!("{r:?}")});
            assert_sometimes!(r.is_ok(), "a SQL transaction is rolled back", &d);
            if r.is_ok() {
                let mut bs = lock(&self.batches);
                if let Some(bb) = bs.by_id.get_mut(&b.id) {
                    bb.rolled_back = true;
                    bb.last_outcome = "rolled back".into();
                }
            }
            return;
        }
        let o = match c.query_drop("COMMIT") {
            Ok(()) => client::classify_info(&c.info_str()),
            Err(e) => client::classify_sql_err(&e),
        };
        self.record_batch(b.id, &o);
    }

    fn txn_http(&self, op: &str, label: &str, extra: &[(&str, String)], be: &str) -> Result<client::HttpResp, String> {
        // begin/prepare/commit/rollback go to the FE, which redirects to a BE;
        // occasionally address the BE directly instead.
        let host = if be.is_empty() { format!("{}:8030", self.cl.fe) } else { format!("{be}:8040") };
        let mut h: Vec<(&str, String)> = vec![("label", label.to_string()), ("db", DB.to_string())];
        h.extend(extra.iter().cloned());
        self.http.send("POST", &format!("http://{host}/api/transaction/{op}"), &h, &[])
    }

    fn status_of(r: &Result<client::HttpResp, String>) -> String {
        match r {
            Ok(r) => r.json.get("Status").and_then(|v| v.as_str()).unwrap_or("?").to_string(),
            Err(e) => format!("error: {}", trunc(e)),
        }
    }

    fn txn_stream_load(&self, b: &Batch, finish: &str) {
        let multi = b.tables.len() > 1;
        let mut begin_hdr: Vec<(&str, String)> = vec![("table", b.tables[0].to_string())];
        if multi {
            begin_hdr.push(("transaction_type", "multi".into()));
        }
        let r = self.txn_http("begin", &b.label, &begin_hdr, "");
        if Self::status_of(&r) != "OK" {
            self.record_batch(b.id, &Outcome::Failed(format!("begin: {}", Self::status_of(&r))));
            return;
        }
        for t in &b.tables {
            let mut h: Vec<(&str, String)> = vec![
                ("label", b.label.clone()),
                ("db", DB.to_string()),
                ("table", t.to_string()),
                ("column_separator", ",".into()),
                ("columns", "batch,seq,k,val".into()),
            ];
            if multi {
                h.push(("transaction_type", "multi".into()));
            }
            let r = self.http.send(
                "PUT",
                &format!("http://{}:8030/api/transaction/load", self.cl.fe),
                &h,
                &Self::batch_csv(b),
            );
            if Self::status_of(&r) != "OK" {
                let _ = self.txn_http("rollback", &b.label, &[], "");
                self.record_batch(b.id, &Outcome::Failed(format!("load {t}: {}", Self::status_of(&r))));
                return;
            }
        }
        if multi {
            // Multi-table transactions have no prepare step; every call names the type.
            // Their commit can answer OK before publish finishes, so OK means committed.
            let commit = finish != "rollback";
            let r = self.txn_http(if commit { "commit" } else { "rollback" }, &b.label,
                &[("transaction_type", "multi".into())], "");
            let st = Self::status_of(&r);
            if commit {
                let o = match classify_stream(&r) {
                    Outcome::Visible => Outcome::Committed,
                    o => o,
                };
                assert_sometimes!(o == Outcome::Committed, "a multi-table stream load transaction commits",
                    &json!({"batch": b.id, "status": st}));
                self.record_batch(b.id, &o);
            } else {
                self.mark_rolled_back(b.id, &st, "a multi-table stream load transaction is rolled back");
            }
            return;
        }
        let r = self.txn_http("prepare", &b.label, &[], "");
        let st = Self::status_of(&r);
        assert_sometimes!(st == "OK", "a stream load transaction is prepared", &json!({"batch": b.id, "status": st}));
        if st != "OK" {
            self.record_batch(b.id, &classify_stream(&r).not_visible());
            return;
        }
        match finish {
            "later" => lock(&self.prepared).push(Prepared { batch: b.id, label: b.label.clone() }),
            "rollback" => self.finish_txn(b.id, &b.label, false, ""),
            _ => self.finish_txn(b.id, &b.label, true, ""),
        }
    }

    fn mark_rolled_back(&self, id: i64, st: &str, _what: &'static str) {
        if st == "OK" {
            let mut bs = lock(&self.batches);
            if let Some(b) = bs.by_id.get_mut(&id) {
                b.rolled_back = true;
                b.last_outcome = "rolled back".into();
            }
        }
        self.count(format!("txn_rollback:{st}"));
    }

    fn finish_txn(&self, id: i64, label: &str, commit: bool, be: &str) {
        let r = self.txn_http(if commit { "commit" } else { "rollback" }, label, &[], be);
        if commit {
            let o = classify_stream(&r);
            self.record_batch(id, &o);
        } else {
            let st = Self::status_of(&r);
            assert_sometimes!(st == "OK", "a prepared stream load transaction is rolled back", &json!({"batch": id, "status": st}));
            self.mark_rolled_back(id, &st, "");
        }
    }

    fn finish_prepared(&self, tc: &TestCase) {
        let commit = tc.draw(gs::weighted_booleans(0.8));
        let direct = tc.draw(gs::weighted_booleans(0.3));
        let be = tc.draw(gs::sampled_from(self.cl.bes.clone()));
        let p = {
            let mut ps = lock(&self.prepared);
            if ps.is_empty() {
                return;
            }
            ps.remove(0)
        };
        let age = lock(&self.batches).by_id.get(&p.batch).map(|b| b.sent.elapsed().as_millis());
        assert_sometimes!(true, "a prepared transaction is finished in a later step", &json!({"batch": p.batch, "age_ms": age}));
        self.finish_txn(p.batch, &p.label, commit, if direct { &be } else { "" });
    }

    /// Reads per-batch counts from `table` for `ids`.
    fn read_counts(&self, table: &str, ids: Option<&[i64]>) -> Result<HashMap<i64, Seen>, String> {
        let filter = match ids {
            Some(ids) => {
                let l: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
                format!("WHERE batch IN ({})", l.join(","))
            }
            None => String::new(),
        };
        let sql =
            format!("SELECT batch, count(*), sum(seq), sum(val) FROM {table} {filter} GROUP BY batch");
        let mut c = self.cl.conn(true, READ_TIMEOUT)?;
        let rows: Vec<(i64, i64, i64, i64)> = c.query(&sql).map_err(|e| e.to_string())?;
        Ok(rows
            .into_iter()
            .map(|(b, n, s, v)| (b, Seen { count: n, seq_sum: s, val_sum: v }))
            .collect())
    }

    fn check_batch(&self, b: &Batch, table: &str, seen: Option<Seen>, start: Instant, end: Instant, final_check: bool) -> bool {
        let count = seen.map(|s| s.count).unwrap_or(0);
        let d = json!({
            "batch": b.id, "table": table, "rows": b.rows, "count": count,
            "seq_sum": seen.map(|s| s.seq_sum), "want_seq_sum": b.seq_sum(),
            "val_sum": seen.map(|s| s.val_sum), "want_val_sum": b.val_sum(),
            "method": b.method.name(), "label": b.label, "attempts": b.attempts, "last_outcome": b.last_outcome,
            "sent_ms": self.ms(b.sent), "visible_ms": b.visible_at.map(|t| self.ms(t)),
            "seen_ms": b.seen_at.map(|t| self.ms(t)), "committed": b.committed, "rolled_back": b.rolled_back,
            "read_start_ms": self.ms(start), "read_end_ms": self.ms(end), "final": final_check,
        });
        assert_always!(count <= b.rows, "a load is never applied twice", &d);
        let whole = count == b.rows
            && seen.map(|s| s.seq_sum) == Some(b.seq_sum())
            && seen.map(|s| s.val_sum) == Some(b.val_sum());
        if count == b.rows {
            // The rows are all there: their contents must be the ones written.
            assert_always!(
                whole,
                "a batch's rows hold the values the workload wrote",
                &d
            );
        }
        assert_always!(count == 0 || whole || count > b.rows, "a batch is visible entirely or not at all", &d);
        if b.rolled_back {
            assert_always!(count == 0, "a rolled-back transaction never becomes visible", &d);
        }
        let acked_before = b.visible_at.map_or(false, |t| t < start);
        let seen_before = b.seen_at.map_or(false, |t| t < start);
        if final_check {
            if b.visible_at.is_some() || b.seen_at.is_some() {
                assert_always!(whole, "acknowledged batches survive once faults stop", &d);
            }
        } else {
            if acked_before {
                assert_always!(whole, "a batch acknowledged visible is visible to later reads", &d);
            }
            if seen_before {
                assert_always!(count > 0, "a batch seen visible never disappears", &d);
            }
            assert_sometimes!(whole && b.visible_at.is_none(), "a read sees a batch whose load was not acknowledged", &d);
            assert_sometimes!(whole && b.attempts > 1, "a read sees a retried batch", &d);
        }
        whole
    }

    fn batch_read(&self, tc: &TestCase) {
        let (ids, multi): (Vec<i64>, bool) = {
            let max = self.next_batch.load(Ordering::SeqCst) - 1;
            if max < 1 {
                return;
            }
            let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(10));
            let recent = tc.draw(gs::weighted_booleans(0.6));
            let mut ids = vec![];
            for _ in 0..n {
                let lo = if recent { (max - 10).max(1) } else { 1 };
                let id = tc.draw(gs::integers::<i64>().min_value(lo).max_value(max));
                if !ids.contains(&id) {
                    ids.push(id);
                }
            }
            (ids, tc.draw(gs::booleans()))
        };
        let start = Instant::now();
        let ev = self.read_counts("ev", Some(&ids));
        let ev2 = if multi { Some(self.read_counts("ev2", Some(&ids))) } else { None };
        let end = Instant::now();
        let Ok(ev) = ev else {
            self.count("batch_read:failed");
            tc.note(&format!("batch read failed: {:?}", ev.err()));
            return;
        };
        self.count("batch_read:ok");
        let bs: Vec<Batch> = {
            let bs = lock(&self.batches);
            ids.iter().filter_map(|i| bs.by_id.get(i).cloned()).collect()
        };
        for b in &bs {
            let whole = self.check_batch(b, "ev", ev.get(&b.id).copied(), start, end, false);
            let mut whole_all = whole;
            if let Some(Ok(ev2)) = &ev2 {
                if b.tables.contains(&"ev2") {
                    whole_all &= self.check_batch(b, "ev2", ev2.get(&b.id).copied(), start, end, false);
                }
            }
            if whole_all {
                if let Some(bb) = lock(&self.batches).by_id.get_mut(&b.id) {
                    if bb.seen_at.is_none() {
                        bb.seen_at = Some(end);
                    }
                }
            }
        }
    }

    /// Reads one multi-table batch from both tables in one statement: a
    /// committed transaction is all-or-nothing across its tables.
    fn multi_table_read(&self, tc: &TestCase) {
        let ids: Vec<i64> = {
            let bs = lock(&self.batches);
            let mut v: Vec<i64> = bs.by_id.values().filter(|b| b.tables.len() > 1).map(|b| b.id).collect();
            v.sort();
            v
        };
        if ids.is_empty() {
            return;
        }
        let recent = tc.draw(gs::weighted_booleans(0.7));
        let pool: Vec<i64> = if recent { ids[ids.len().saturating_sub(5)..].to_vec() } else { ids };
        let id = tc.draw(gs::sampled_from(pool));
        let sql = format!(
            "SELECT (SELECT count(*) FROM ev WHERE batch = {id}), (SELECT count(*) FROM ev2 WHERE batch = {id})"
        );
        let start = Instant::now();
        let r: Result<Option<(i64, i64)>, String> = self
            .cl
            .conn(true, READ_TIMEOUT)
            .and_then(|mut c| c.query_first(&sql).map_err(|e| e.to_string()));
        let end = Instant::now();
        let Ok(Some((a, b2))) = r else { return };
        let b = lock(&self.batches).by_id.get(&id).cloned().unwrap();
        let d = json!({"batch": id, "ev": a, "ev2": b2, "rows": b.rows, "method": b.method.name(),
            "last_outcome": b.last_outcome, "read_start_ms": self.ms(start), "read_end_ms": self.ms(end)});
        assert_always!(a == b2, "a multi-table transaction is visible in all its tables or none", &d);
        assert_sometimes!(a == b.rows && b2 == b.rows, "a read sees a multi-table transaction in both tables", &d);
    }

    // ----- cluster observation ---------------------------------------------

    fn observe(&self, _tc: &TestCase) {
        {
            let mut last = lock(&self.last_observe);
            if last.map_or(false, |t| t.elapsed() < Duration::from_secs(3)) {
                return;
            }
            *last = Some(Instant::now());
        }
        if let Ok(b) = backends(&self.cl) {
            let alive = b.iter().filter(|(_, a)| *a).count() as i64;
            let d = json!({"backends": format!("{b:?}")});
            assert_sometimes!(alive < self.cl.bes.len() as i64, "the FE sees a backend down", &d);
        }
        let unhealthy: Result<Option<i64>, String> = self.cl.conn(false, READ_TIMEOUT).and_then(|mut c| {
            let rows: Vec<mysql::Row> = c.query("SHOW PROC '/statistic'").map_err(|e| e.to_string())?;
            let mut total = None;
            for row in rows {
                let cols = row.columns();
                if let Some(i) = cols.iter().position(|c| c.name_str() == "UnhealthyTabletNum") {
                    let v: Option<String> = row.get_opt(i).and_then(|r| r.ok());
                    if let Some(n) = v.and_then(|s| s.parse::<i64>().ok()) {
                        total = Some(total.unwrap_or(0).max(n));
                    }
                }
            }
            Ok(total)
        });
        if let Ok(Some(n)) = unhealthy {
            antithesis_sdk::assert_sometimes_greater_than!(n, 0, "some tablets are unhealthy", &json!({"unhealthy": n}));
        }
    }

    // ----- StarRocks' own view of its internals -----------------------------

    /// Reads `be_tablets` and friends: the replica-divergence oracle, plus the
    /// signals that steer Antithesis toward the states the project's bug
    /// history keeps pointing at (antithesis/BUG-HISTORY.md).
    fn inspect(&self, _tc: &TestCase) {
        {
            let mut last = lock(&self.last_inspect);
            if last.map_or(false, |t| t.elapsed() < Duration::from_secs(5)) {
                return;
            }
            *last = Some(Instant::now());
        }
        match inspect::tablets(&self.cl, DB, REPLICATION) {
            Err(e) => {
                self.count("inspect:failed");
                log(format!("inspect failed: {}", trunc(&e)));
            }
            Ok(snap) => {
                self.count("inspect:ok");
                self.check_divergence(&snap);
                // Version pile-up is what brings on compaction, and compaction
                // is the project's second largest cluster of backported fixes.
                antithesis_sdk::assert_sometimes_greater_than!(
                    snap.max_rowsets, 20,
                    "a tablet accumulates many rowsets",
                    &json!({"max_rowsets": snap.max_rowsets, "tablets": snap.replicas.len()})
                );
            }
        }
        if let Ok(n) = inspect::committed_unpublished(&self.cl) {
            antithesis_sdk::assert_sometimes_greater_than!(
                n, 0,
                "transactions are committed but not yet published",
                &json!({"committed_unpublished": n})
            );
        }
        if let Ok(n) = inspect::repairs_in_flight(&self.cl) {
            antithesis_sdk::assert_sometimes_greater_than!(
                n, 0,
                "the FE is repairing tablets",
                &json!({"scheduled_repairs": n})
            );
        }
    }

    fn check_divergence(&self, snap: &inspect::Snapshot) {
        let d = json!({
            "divergences": snap.divergences.iter().take(5)
                .map(|d| json!({"tablet": d.tablet_id, "version": d.version, "rows_by_be": d.rows}))
                .collect::<Vec<_>>(),
            "tablets_seen": snap.replicas.len(),
            "tablets_with_all_replicas": snap.tablets_with_all_replicas,
        });
        assert_always!(
            snap.divergences.is_empty(),
            "replicas of a tablet agree on the row count at the same version",
            &d
        );
        assert_sometimes!(
            snap.tablets_with_all_replicas > 0,
            "every replica of some tablet reports in",
            &d
        );
    }

    /// A schema change, which StarRocks runs in the background. Loads that are
    /// in flight must survive it: ignoring a PREPARED transaction while doing
    /// one is a data-loss bug the project has already had (#34381), and the
    /// workload holds PREPARED transactions open, so it can reach that state.
    fn schema_change(&self, tc: &TestCase) {
        {
            let mut last = lock(&self.last_alter);
            if last.map_or(false, |t| t.elapsed() < Duration::from_secs(20)) {
                return;
            }
            *last = Some(Instant::now());
        }
        // Prefer the table a prepared transaction is open on: that conjunction
        // is the state worth reaching, and waiting for it to happen by chance
        // wastes the timeline.
        let aim_at_prepared = tc.draw(gs::weighted_booleans(0.7));
        let drop_one = tc.draw(gs::weighted_booleans(0.4));
        let drawn = tc.draw(gs::sampled_from(vec!["ev", "ev2"]));
        let prepared_tables: Vec<&'static str> = {
            let ps = lock(&self.prepared);
            let bs = lock(&self.batches);
            ps.iter()
                .filter_map(|p| bs.by_id.get(&p.batch))
                .flat_map(|b| b.tables.clone())
                .collect()
        };
        let table = if aim_at_prepared && !prepared_tables.is_empty() {
            prepared_tables[0]
        } else {
            drawn
        };
        let prepared_here = prepared_tables.contains(&table);
        let alters = inspect::alter_jobs(&self.cl, DB).unwrap_or_default();
        let running = alters
            .iter()
            .any(|(t, st)| t == table && !st.eq_ignore_ascii_case("FINISHED") && !st.eq_ignore_ascii_case("CANCELLED"));
        if running {
            self.count("alter:already_running");
            return;
        }
        let existing = lock(&self.added_columns).get(table).cloned().unwrap_or_default();
        let sql = if drop_one && !existing.is_empty() {
            format!("ALTER TABLE {table} DROP COLUMN {}", existing[0])
        } else {
            let n = self.next_alter.fetch_add(1, Ordering::SeqCst);
            format!("ALTER TABLE {table} ADD COLUMN pad{n} INT NULL DEFAULT \"0\"")
        };
        let o = match exec_dml(&self.cl, &sql, WRITE_TIMEOUT) {
            // ALTER answers with a bare OK packet: no label, no status.
            Outcome::Unknown(m) if m.starts_with("ok without status") => Outcome::Visible,
            other => other,
        };
        self.count(format!("alter:{}", o.kind()));
        let d = json!({
            "table": table, "sql": sql, "outcome": o.kind(), "msg": trunc(&o.msg()),
            "prepared_txns_open": prepared_tables.len(),
        });
        // The state the bug history says to reach: a schema change starting
        // while a PREPARED transaction is open on the same table.
        antithesis_sdk::assert_sometimes_all!(
            {
                schema_change_accepted: matches!(o, Outcome::Visible | Outcome::Unknown(_)),
                prepared_txn_open_on_same_table: prepared_here
            },
            "a schema change starts while a prepared transaction is open on the same table",
            &d
        );
        if matches!(o, Outcome::Visible | Outcome::Unknown(_)) {
            let mut cols = lock(&self.added_columns);
            let e = cols.entry(table).or_default();
            if sql.contains("DROP COLUMN") {
                e.retain(|c| !sql.ends_with(c.as_str()));
            } else if let Some(name) = sql.split("ADD COLUMN ").nth(1).and_then(|r| r.split(' ').next()) {
                e.push(name.to_string());
            }
        }
    }

    // ----- eventually -------------------------------------------------------

    fn eventually(&self) -> ! {
        let _e = lock(&self.eventually_lock);
        let _q = self.quiesce.write().unwrap_or_else(|e| e.into_inner());
        log(format!(
            "eventually: stopping faults after {}s of workload",
            self.workload_start.elapsed().as_secs()
        ));
        if let Ok(cmd) = std::env::var("ANTITHESIS_STOP_FAULTS") {
            let _ = std::process::Command::new(cmd).arg("3600").status();
        }
        let start = Instant::now();
        let deadline = start + Duration::from_secs(180);
        let mut last = String::new();
        let mut probe_id = -1_000_000;
        let ready = loop {
            probe_id -= 1;
            let r = all_backends_alive(&self.cl).and_then(|_| probe(&self.cl, probe_id));
            match r {
                Ok(()) => break true,
                Err(e) => {
                    if e != last {
                        log(format!("eventually: waiting for recovery: {}", trunc(&e)));
                        last = e;
                    }
                }
            }
            if Instant::now() > deadline {
                break false;
            }
            std::thread::sleep(Duration::from_secs(1));
        };
        log("eventually: assertions start");
        assert_always!(ready, "the cluster serves reads and writes again once faults stop",
            &json!({"waited_ms": start.elapsed().as_millis(), "last_error": last,
                    "workload_age_s": self.workload_start.elapsed().as_secs()}));
        if !ready {
            self.end_timeline();
        }
        // Committed (publish-pending) batches must become visible.
        let committed: Vec<Batch> = lock(&self.batches)
            .by_id
            .values()
            .filter(|b| b.committed && b.visible_at.is_none() && b.seen_at.is_none())
            .cloned()
            .collect();
        if !committed.is_empty() {
            let ids: Vec<i64> = committed.iter().map(|b| b.id).collect();
            let deadline = Instant::now() + Duration::from_secs(90);
            let mut missing: Vec<i64>;
            loop {
                missing = match self.read_counts("ev", Some(&ids)) {
                    Ok(c) => committed
                        .iter()
                        .filter(|b| c.get(&b.id).map(|s| s.count) != Some(b.rows))
                        .map(|b| b.id)
                        .collect(),
                    Err(_) => ids.clone(),
                };
                if missing.is_empty() || Instant::now() > deadline {
                    break;
                }
                std::thread::sleep(Duration::from_secs(2));
            }
            assert_always!(missing.is_empty(), "a committed transaction becomes visible once faults stop",
                &json!({"committed": ids, "missing": missing}));
        }
        // Full scans, repeated: replicas must agree and nothing acknowledged is lost.
        let mut scans: Vec<(HashMap<i64, (i64, i64, i64)>, HashMap<i64, (i64, i64, i64)>)> = vec![];
        for _ in 0..3 {
            let a = self.read_counts("ev", None);
            let b = self.read_counts("ev2", None);
            match (a, b) {
                (Ok(a), Ok(b)) => {
                    let f = |m: HashMap<i64, Seen>| {
                        m.into_iter().map(|(k, s)| (k, (s.count, s.seq_sum, s.val_sum))).collect()
                    };
                    scans.push((f(a), f(b)));
                }
                (a, b) => {
                    assert_always!(false, "the cluster serves reads and writes again once faults stop",
                        &json!({"phase": "final scan", "ev": format!("{:?}", a.err()), "ev2": format!("{:?}", b.err())}));
                    self.end_timeline();
                }
            }
        }
        let now = Instant::now();
        let all_agree = scans.windows(2).all(|w| w[0] == w[1]);
        let diff: Vec<String> = if all_agree {
            vec![]
        } else {
            let (a, b) = (&scans[0].0, &scans[1].0);
            a.keys().chain(b.keys()).filter(|k| a.get(k) != b.get(k)).take(20)
                .map(|k| format!("ev batch {k}: {:?} vs {:?}", a.get(k), b.get(k))).collect()
        };
        assert_always!(all_agree, "repeated reads agree once faults stop", &json!({"diff": diff}));
        let (ev, ev2) = &scans[0];
        let bs = lock(&self.batches).by_id.clone();
        for (table, scan) in [("ev", ev), ("ev2", ev2)] {
            for (id, (count, _, _)) in scan.iter() {
                let known = bs.get(id).map_or(false, |b| b.tables.contains(&table));
                assert_always!(known, "only batches the workload wrote appear",
                    &json!({"table": table, "batch": id, "count": count}));
            }
        }
        for b in bs.values() {
            let mut counts = vec![];
            for t in &b.tables {
                let scan = if *t == "ev" { ev } else { ev2 };
                let seen = scan.get(&b.id).map(|(c, s, v)| Seen { count: *c, seq_sum: *s, val_sum: *v });
                counts.push(seen.map_or(0, |s| s.count));
                self.check_batch(b, t, seen, now, now, true);
            }
            if counts.len() > 1 {
                assert_always!(counts.iter().all(|c| *c == counts[0]),
                    "a multi-table transaction is in all its tables or none once faults stop",
                    &json!({"batch": b.id, "counts": counts, "method": b.method.name(), "last_outcome": b.last_outcome}));
            }
        }
        // kv: every key reflects its acknowledged writes.
        let start = Instant::now();
        let rows: Result<Vec<(i32, i64)>, String> = self
            .cl
            .conn(true, READ_TIMEOUT)
            .and_then(|mut c| c.query("SELECT k, v FROM kv").map_err(|e| e.to_string()));
        let end = Instant::now();
        match rows {
            Ok(rows) => self.check_kv_rows(&(0..NKEYS).collect::<Vec<_>>(), &rows, start, end, true),
            Err(e) => assert_always!(false, "the cluster serves reads and writes again once faults stop",
                &json!({"phase": "final kv scan", "error": e})),
        }
        log(format!("eventually: checked {} batches", bs.len()));
        self.end_timeline();
    }

    fn end_timeline(&self) -> ! {
        log("eventually: ending the timeline");
        std::process::exit(0);
    }

    fn act<F: FnOnce()>(&self, f: F) {
        let _q = self.quiesce.read().unwrap_or_else(|e| e.into_inner());
        f();
    }
}

impl Outcome {
    /// A prepare that failed: the transaction is not visible yet, whatever happened.
    fn not_visible(self) -> Outcome {
        match self {
            Outcome::Visible | Outcome::Committed => Outcome::Unknown("prepare answered".into()),
            o => o,
        }
    }
}

#[hegel::concurrent_state_machine]
impl Driver {
    #[rule(weight = 3.0)]
    fn rule_kv_insert(&self, tc: TestCase) {
        self.act(|| self.kv_insert(&tc));
    }

    #[rule(weight = 1.0)]
    fn rule_kv_delete(&self, tc: TestCase) {
        self.act(|| self.kv_delete(&tc));
    }

    #[rule(weight = 2.0)]
    fn rule_kv_stream_load(&self, tc: TestCase) {
        self.act(|| self.kv_stream_load(&tc));
    }

    #[rule(weight = 4.0)]
    fn rule_kv_read(&self, tc: TestCase) {
        self.act(|| self.kv_read(&tc));
    }

    #[rule(weight = 5.0)]
    fn rule_batch_load(&self, tc: TestCase) {
        self.act(|| self.batch_load(&tc));
    }

    #[rule(weight = 1.0)]
    fn rule_batch_retry(&self, tc: TestCase) {
        self.act(|| self.batch_retry(&tc));
    }

    #[rule(weight = 1.0)]
    fn rule_finish_prepared(&self, tc: TestCase) {
        self.act(|| self.finish_prepared(&tc));
    }

    #[rule(weight = 4.0)]
    fn rule_batch_read(&self, tc: TestCase) {
        self.act(|| self.batch_read(&tc));
    }

    #[rule(weight = 2.0)]
    fn rule_multi_table_read(&self, tc: TestCase) {
        self.act(|| self.multi_table_read(&tc));
    }

    #[rule(weight = 1.0)]
    fn rule_observe(&self, tc: TestCase) {
        self.act(|| self.observe(&tc));
    }

    #[rule(weight = 2.0)]
    fn rule_pk_upsert(&self, tc: TestCase) {
        self.act(|| self.pk_upsert(&tc));
    }

    #[rule(weight = 3.0)]
    fn rule_pk_partial(&self, tc: TestCase) {
        self.act(|| self.pk_partial(&tc));
    }

    #[rule(weight = 1.0)]
    fn rule_pk_delete(&self, tc: TestCase) {
        self.act(|| self.pk_delete(&tc));
    }

    #[rule(weight = 3.0)]
    fn rule_pk_read(&self, tc: TestCase) {
        self.act(|| self.pk_read(&tc));
    }

    #[rule(weight = 2.0)]
    fn rule_inspect(&self, tc: TestCase) {
        self.act(|| self.inspect(&tc));
    }

    #[rule(weight = 1.0)]
    fn rule_schema_change(&self, tc: TestCase) {
        self.act(|| self.schema_change(&tc));
    }

    #[rule]
    fn rule_eventually(&self, tc: TestCase) {
        // Draw unconditionally: the choice stream must not depend on timing.
        let go = tc.draw(gs::weighted_booleans(EVENTUALLY_P));
        if go && self.eventually_enabled && self.workload_start.elapsed() >= MIN_EVENTUALLY_AGE {
            self.eventually();
        }
    }
}

#[hegel::main(phases = [hegel::Phase::Generate])]
fn main(tc: TestCase) {
    antithesis_init();
    let cl = Cluster::from_env();
    startup(&cl);
    antithesis_sdk::lifecycle::setup_complete(&json!({"fe": cl.fe, "bes": cl.bes}));
    log("setup complete; waiting for /workload-active");
    while !std::path::Path::new("/workload-active").exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_reachable!("the workload starts", &json!({}));
    log("workload active");
    let d = Driver::new(cl);
    {
        let d2 = d.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(30));
            let s = lock(&d2.stats).clone();
            log(format!("stats: {s:?}"));
        });
    }
    if let Some(secs) = std::env::var("DRIVER_EVENTUALLY_AFTER_SECS").ok().and_then(|s| s.parse::<u64>().ok()) {
        let d2 = d.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(secs));
            d2.eventually();
        });
    }
    hegel::stateful::machine(d).max_concurrency(4).steps(i64::MAX).run_concurrent(tc);
}
