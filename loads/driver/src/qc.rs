//! Query correctness over a static dataset.
//!
//! Two tables in database `qc` are loaded once during setup and never written
//! again. The driver keeps the same rows in memory and computes the right
//! answer to every query itself, with SQL's three-valued logic. A query's
//! result must match that answer whatever plan, execution setting or fault the
//! cluster is under. Without concurrent writes there is no ambiguity: any
//! difference is a wrong result.

use std::collections::{BTreeMap, BTreeSet, HashMap};

pub const N1: i64 = 1500;
pub const N2: i64 = 400;
const STRS: [&str; 8] = ["alpha", "beta", "gamma", "delta", "ab", "abc", "b", ""];

#[derive(Clone, Debug)]
pub struct Row1 {
    pub id: i64,
    pub a: Option<i64>,
    pub b: Option<i64>,
    pub c: Option<&'static str>,
    /// Days after 2026-01-01.
    pub d: Option<i64>,
    /// DECIMAL(10,2) in cents.
    pub e: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct Row2 {
    pub id: i64,
    pub k: Option<i64>,
    pub v: Option<i64>,
    pub s: Option<&'static str>,
}

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo + 1) as u64)) as i64
    }
    fn null(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
}

pub struct Data {
    pub t1: Vec<Row1>,
    pub t2: Vec<Row2>,
}

impl Data {
    /// Deterministic: the same rows on every run and in every process.
    pub fn generate() -> Data {
        let mut r = Lcg(0x5eed_0f_57a7_1c);
        let t1 = (1..=N1)
            .map(|id| Row1 {
                id,
                a: (!r.null(10)).then(|| r.range(-50, 50)),
                b: (!r.null(10)).then(|| r.range(-1_000_000_000, 1_000_000_000)),
                c: (!r.null(10)).then(|| STRS[r.range(0, STRS.len() as i64 - 1) as usize]),
                d: (!r.null(10)).then(|| r.range(0, 59)),
                e: (!r.null(10)).then(|| r.range(-99_999, 99_999)),
            })
            .collect();
        let t2 = (1..=N2)
            .map(|id| Row2 {
                id,
                k: (!r.null(15)).then(|| r.range(-50, 50)),
                v: (!r.null(10)).then(|| r.range(-1_000_000, 1_000_000)),
                s: (!r.null(10)).then(|| STRS[r.range(0, STRS.len() as i64 - 1) as usize]),
            })
            .collect();
        Data { t1, t2 }
    }

    pub fn insert_statements(&self) -> Vec<String> {
        let mut out = vec![];
        for (i, chunk) in self.t1.chunks(250).enumerate() {
            let vals: Vec<String> = chunk
                .iter()
                .map(|r| {
                    format!("({},{},{},{},{},{})", r.id, int_sql(r.a), int_sql(r.b), str_sql(r.c),
                        r.d.map_or("NULL".into(), |d| format!("'{}'", date_str(d))),
                        r.e.map_or("NULL".into(), cents_str))
                })
                .collect();
            out.push(format!("INSERT INTO qc.t1 WITH LABEL qc_t1_{i} VALUES {}", vals.join(",")));
        }
        for (i, chunk) in self.t2.chunks(200).enumerate() {
            let vals: Vec<String> = chunk
                .iter()
                .map(|r| format!("({},{},{},{})", r.id, int_sql(r.k), int_sql(r.v), str_sql(r.s)))
                .collect();
            out.push(format!("INSERT INTO qc.t2 WITH LABEL qc_t2_{i} VALUES {}", vals.join(",")));
        }
        out
    }
}

pub const DDL: [&str; 3] = [
    "CREATE DATABASE IF NOT EXISTS qc",
    "CREATE TABLE IF NOT EXISTS qc.t1 (id BIGINT NOT NULL, a INT NULL, b BIGINT NULL, c VARCHAR(16) NULL, \
     d DATE NULL, e DECIMAL(10,2) NULL) DUPLICATE KEY(id) \
     PARTITION BY RANGE(id) (PARTITION p1 VALUES LESS THAN ('400'), PARTITION p2 VALUES LESS THAN ('800'), \
     PARTITION p3 VALUES LESS THAN ('1200'), PARTITION p4 VALUES LESS THAN MAXVALUE) \
     DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES(\"replication_num\"=\"3\")",
    "CREATE TABLE IF NOT EXISTS qc.t2 (id BIGINT NOT NULL, k INT NULL, v BIGINT NULL, s VARCHAR(16) NULL) \
     DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3 PROPERTIES(\"replication_num\"=\"3\")",
];

fn int_sql(v: Option<i64>) -> String {
    v.map_or("NULL".into(), |x| x.to_string())
}
fn str_sql(v: Option<&str>) -> String {
    v.map_or("NULL".into(), |s| format!("'{s}'"))
}
pub fn cents_str(c: i64) -> String {
    let sign = if c < 0 { "-" } else { "" };
    format!("{sign}{}.{:02}", c.abs() / 100, c.abs() % 100)
}
/// Days after 2026-01-01 as YYYY-MM-DD, for offsets -31..=89.
pub fn date_str(off: i64) -> String {
    let months = [(2025, 12, 31), (2026, 1, 31), (2026, 2, 28), (2026, 3, 31)];
    let mut rem = off + 31;
    for (y, m, len) in months {
        if rem < len {
            return format!("{y:04}-{m:02}-{:02}", rem + 1);
        }
        rem -= len;
    }
    panic!("date offset out of range: {off}");
}

// ----- predicates ------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Col {
    Id,
    A,
    B,
    C,
    D,
    E,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Lit {
    Int(i64),
    Str(&'static str),
}

#[derive(Clone, Copy, Debug)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug)]
pub enum Pred {
    Cmp(Col, Op, Lit),
    IsNull(Col, bool),
    In(Col, Vec<Lit>),
    Between(Col, Lit, Lit),
    LikePrefix(&'static str),
    Not(Box<Pred>),
    And(Box<Pred>, Box<Pred>),
    Or(Box<Pred>, Box<Pred>),
}

impl Col {
    fn name(&self) -> &'static str {
        match self {
            Col::Id => "id",
            Col::A => "a",
            Col::B => "b",
            Col::C => "c",
            Col::D => "d",
            Col::E => "e",
        }
    }
    fn value(&self, r: &Row1) -> Option<Lit> {
        match self {
            Col::Id => Some(Lit::Int(r.id)),
            Col::A => r.a.map(Lit::Int),
            Col::B => r.b.map(Lit::Int),
            Col::C => r.c.map(Lit::Str),
            Col::D => r.d.map(Lit::Int),
            Col::E => r.e.map(Lit::Int),
        }
    }
    fn lit_sql(&self, l: &Lit) -> String {
        match (self, l) {
            (Col::D, Lit::Int(d)) => format!("'{}'", date_str(*d)),
            (Col::E, Lit::Int(c)) => cents_str(*c),
            (_, Lit::Int(i)) => i.to_string(),
            (_, Lit::Str(s)) => format!("'{s}'"),
        }
    }
}

fn cmp_lit(x: &Lit, y: &Lit) -> std::cmp::Ordering {
    match (x, y) {
        (Lit::Int(a), Lit::Int(b)) => a.cmp(b),
        (Lit::Str(a), Lit::Str(b)) => a.as_bytes().cmp(b.as_bytes()),
        _ => panic!("type mismatch"),
    }
}

impl Pred {
    /// SQL three-valued logic: None is UNKNOWN.
    pub fn eval(&self, r: &Row1) -> Option<bool> {
        use std::cmp::Ordering::*;
        match self {
            Pred::Cmp(c, op, l) => {
                let v = c.value(r)?;
                let o = cmp_lit(&v, l);
                Some(match op {
                    Op::Eq => o == Equal,
                    Op::Ne => o != Equal,
                    Op::Lt => o == Less,
                    Op::Le => o != Greater,
                    Op::Gt => o == Greater,
                    Op::Ge => o != Less,
                })
            }
            Pred::IsNull(c, want_null) => Some(c.value(r).is_none() == *want_null),
            Pred::In(c, ls) => {
                let v = c.value(r)?;
                Some(ls.iter().any(|l| cmp_lit(&v, l) == Equal))
            }
            Pred::Between(c, lo, hi) => {
                let v = c.value(r)?;
                Some(cmp_lit(&v, lo) != Less && cmp_lit(&v, hi) != Greater)
            }
            Pred::LikePrefix(p) => r.c.map(|s| s.starts_with(p)),
            Pred::Not(p) => p.eval(r).map(|b| !b),
            Pred::And(p, q) => match (p.eval(r), q.eval(r)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            Pred::Or(p, q) => match (p.eval(r), q.eval(r)) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
        }
    }

    pub fn sql(&self, alias: &str) -> String {
        let col = |c: &Col| format!("{alias}{}", c.name());
        match self {
            Pred::Cmp(c, op, l) => {
                let o = match op {
                    Op::Eq => "=",
                    Op::Ne => "<>",
                    Op::Lt => "<",
                    Op::Le => "<=",
                    Op::Gt => ">",
                    Op::Ge => ">=",
                };
                format!("{} {o} {}", col(c), c.lit_sql(l))
            }
            Pred::IsNull(c, true) => format!("{} IS NULL", col(c)),
            Pred::IsNull(c, false) => format!("{} IS NOT NULL", col(c)),
            Pred::In(c, ls) => format!(
                "{} IN ({})",
                col(c),
                ls.iter().map(|l| c.lit_sql(l)).collect::<Vec<_>>().join(", ")
            ),
            Pred::Between(c, lo, hi) => format!("{} BETWEEN {} AND {}", col(c), c.lit_sql(lo), c.lit_sql(hi)),
            Pred::LikePrefix(p) => format!("{alias}c LIKE '{p}%'"),
            Pred::Not(p) => format!("NOT ({})", p.sql(alias)),
            Pred::And(p, q) => format!("({}) AND ({})", p.sql(alias), q.sql(alias)),
            Pred::Or(p, q) => format!("({}) OR ({})", p.sql(alias), q.sql(alias)),
        }
    }
}

/// Draws a literal suited to the column, sometimes outside the data's range.
pub fn lit_for(c: Col, n: i64, s: usize) -> Lit {
    match c {
        Col::Id => Lit::Int(n.rem_euclid(N1 + 100) - 50),
        Col::A => Lit::Int(n.rem_euclid(121) - 60),
        Col::B => Lit::Int(n.rem_euclid(2_400_000_001) - 1_200_000_000),
        Col::C => Lit::Str(STRS[s % STRS.len()]),
        Col::D => Lit::Int(n.rem_euclid(80) - 10),
        Col::E => Lit::Int(n.rem_euclid(240_001) - 120_000),
    }
}

// ----- answers -----------------------------------------------------------------

/// A result row as the server returns it over the text protocol.
pub type Got = Vec<Vec<Option<String>>>;

/// Decimal string ("-12.3400") to cents, for exact comparison.
pub fn parse_cents(s: &str) -> Option<i128> {
    let neg = s.starts_with('-');
    let t = s.trim_start_matches('-');
    let (ip, fp) = t.split_once('.').unwrap_or((t, ""));
    let mut frac = fp.to_string();
    while frac.len() < 2 {
        frac.push('0');
    }
    if frac[2..].chars().any(|ch| ch != '0') {
        return None;
    }
    let v: i128 = ip.parse::<i128>().ok()? * 100 + frac[..2].parse::<i128>().ok()?;
    Some(if neg { -v } else { v })
}

/// Normalizes a returned cell so numbers compare by value.
pub fn norm(cell: &Option<String>, decimal: bool) -> String {
    match cell {
        None => "NULL".into(),
        Some(s) if decimal => parse_cents(s).map_or(format!("?{s}"), |c| c.to_string()),
        Some(s) => s.clone(),
    }
}

pub fn opt_sum(it: impl Iterator<Item = Option<i64>>) -> String {
    let mut any = false;
    let mut sum: i128 = 0;
    for v in it.flatten() {
        any = true;
        sum += v as i128;
    }
    if any { sum.to_string() } else { "NULL".into() }
}
fn opt_min(it: impl Iterator<Item = Option<i64>>) -> String {
    it.flatten().min().map_or("NULL".into(), |v| v.to_string())
}
fn opt_max(it: impl Iterator<Item = Option<i64>>) -> String {
    it.flatten().max().map_or("NULL".into(), |v| v.to_string())
}

/// One query and its right answer, as normalized rows (order-insensitive).
pub struct Case {
    pub kind: &'static str,
    pub sql: String,
    pub want: BTreeSet<Vec<String>>,
    /// Which result columns are decimals (compared as cents).
    pub decimal_cols: Vec<usize>,
}

impl Case {
    pub fn check(&self, got: &Got) -> Result<(), String> {
        let got: BTreeSet<Vec<String>> = got
            .iter()
            .map(|r| r.iter().enumerate().map(|(i, c)| norm(c, self.decimal_cols.contains(&i))).collect())
            .collect();
        if got == self.want {
            Ok(())
        } else {
            let missing: Vec<_> = self.want.difference(&got).take(5).collect();
            let extra: Vec<_> = got.difference(&self.want).take(5).collect();
            Err(format!("want {} rows, got {}; missing {:?}; unexpected {:?}", self.want.len(), got.len(), missing, extra))
        }
    }
}

pub fn agg_case(d: &Data, p: &Pred) -> Case {
    let rows: Vec<&Row1> = d.t1.iter().filter(|r| p.eval(r) == Some(true)).collect();
    let distinct_c: BTreeSet<&str> = rows.iter().filter_map(|r| r.c).collect();
    let want = vec![
        rows.len().to_string(),
        rows.iter().filter(|r| r.a.is_some()).count().to_string(),
        opt_sum(rows.iter().map(|r| r.a)),
        opt_min(rows.iter().map(|r| r.b)),
        opt_max(rows.iter().map(|r| r.b)),
        opt_sum(rows.iter().map(|r| r.e)),
        distinct_c.len().to_string(),
    ];
    Case {
        kind: "aggregate",
        sql: format!(
            "SELECT count(*), count(a), sum(a), min(b), max(b), sum(e), count(DISTINCT c) FROM qc.t1 WHERE {}",
            p.sql("")
        ),
        want: [want].into_iter().collect(),
        decimal_cols: vec![5],
    }
}

pub fn group_case(d: &Data, p: &Pred) -> Case {
    let mut groups: BTreeMap<Option<&str>, (usize, Vec<Option<i64>>, Vec<Option<i64>>)> = BTreeMap::new();
    for r in d.t1.iter().filter(|r| p.eval(r) == Some(true)) {
        let g = groups.entry(r.c).or_default();
        g.0 += 1;
        g.1.push(r.a);
        g.2.push(r.e);
    }
    let want = groups
        .into_iter()
        .map(|(c, (n, a, e))| {
            vec![c.map_or("NULL".into(), |s| s.to_string()), n.to_string(), opt_sum(a.into_iter()), opt_sum(e.into_iter())]
        })
        .collect();
    Case {
        kind: "group by",
        sql: format!("SELECT c, count(*), sum(a), sum(e) FROM qc.t1 WHERE {} GROUP BY c", p.sql("")),
        want,
        decimal_cols: vec![3],
    }
}

/// Inner join on t1.a = t2.k (NULLs never match), with a filter on each side.
pub fn join_case(d: &Data, p: &Pred, vmin: i64) -> Case {
    let t2: Vec<&Row2> = d.t2.iter().filter(|r| r.v.map_or(false, |v| v > vmin)).collect();
    let mut by_k: HashMap<i64, Vec<&Row2>> = HashMap::new();
    for r in &t2 {
        if let Some(k) = r.k {
            by_k.entry(k).or_default().push(r);
        }
    }
    let (mut n, mut sa, mut sv, mut any_a, mut any_v) = (0i64, 0i128, 0i128, false, false);
    for r in d.t1.iter().filter(|r| p.eval(r) == Some(true)) {
        let Some(a) = r.a else { continue };
        for m in by_k.get(&a).into_iter().flatten() {
            n += 1;
            sa += a as i128;
            any_a = true;
            if let Some(v) = m.v {
                sv += v as i128;
                any_v = true;
            }
        }
    }
    let s = |any: bool, v: i128| if any { v.to_string() } else { "NULL".into() };
    Case {
        kind: "inner join",
        sql: format!(
            "SELECT count(*), sum(t1.a), sum(t2.v) FROM qc.t1 t1 JOIN qc.t2 t2 ON t1.a = t2.k WHERE ({}) AND t2.v > {vmin}",
            p.sql("t1.")
        ),
        want: [vec![n.to_string(), s(any_a, sa), s(any_v, sv)]].into_iter().collect(),
        decimal_cols: vec![],
    }
}

/// Left join with the t2 filter in the ON clause: every qualifying t1 row
/// appears at least once.
pub fn left_join_case(d: &Data, p: &Pred, vmin: i64) -> Case {
    let mut by_k: HashMap<i64, usize> = HashMap::new();
    for r in d.t2.iter().filter(|r| r.v.map_or(false, |v| v > vmin)) {
        if let Some(k) = r.k {
            *by_k.entry(k).or_default() += 1;
        }
    }
    let (mut rows, mut matched) = (0usize, 0usize);
    for r in d.t1.iter().filter(|r| p.eval(r) == Some(true)) {
        let m = r.a.and_then(|a| by_k.get(&a).copied()).unwrap_or(0);
        rows += m.max(1);
        matched += m;
    }
    Case {
        kind: "left join",
        sql: format!(
            "SELECT count(*), count(t2.id) FROM qc.t1 t1 LEFT JOIN qc.t2 t2 ON t1.a = t2.k AND t2.v > {vmin} WHERE {}",
            p.sql("t1.")
        ),
        want: [vec![rows.to_string(), matched.to_string()]].into_iter().collect(),
        decimal_cols: vec![],
    }
}

/// a IN / NOT IN (subquery over t2), with SQL's NULL rules. NOT IN against a
/// set holding a NULL is never true; against an empty set it is always true,
/// even for a NULL operand.
pub fn in_subquery_case(d: &Data, p: &Pred, vmin: i64, negate: bool) -> Case {
    let set: Vec<Option<i64>> = d.t2.iter().filter(|r| r.v.map_or(false, |v| v > vmin)).map(|r| r.k).collect();
    let has_null = set.iter().any(|k| k.is_none());
    let vals: BTreeSet<i64> = set.iter().flatten().copied().collect();
    let n = d
        .t1
        .iter()
        .filter(|r| p.eval(r) == Some(true))
        .filter(|r| {
            let in_set: Option<bool> = if set.is_empty() {
                Some(false)
            } else {
                match r.a {
                    None => None,
                    Some(a) if vals.contains(&a) => Some(true),
                    Some(_) if has_null => None,
                    Some(_) => Some(false),
                }
            };
            (if negate { in_set.map(|b| !b) } else { in_set }) == Some(true)
        })
        .count();
    Case {
        kind: if negate { "not in subquery" } else { "in subquery" },
        sql: format!(
            "SELECT count(*) FROM qc.t1 WHERE ({}) AND a {}IN (SELECT k FROM qc.t2 WHERE v > {vmin})",
            p.sql(""),
            if negate { "NOT " } else { "" }
        ),
        want: [vec![n.to_string()]].into_iter().collect(),
        decimal_cols: vec![],
    }
}

/// What the static tables must hold once faults stop.
pub fn final_case(d: &Data) -> Case {
    Case {
        kind: "final contents",
        sql: "SELECT (SELECT count(*) FROM qc.t1), (SELECT sum(id) FROM qc.t1), (SELECT sum(a) FROM qc.t1), \
              (SELECT sum(e) FROM qc.t1), (SELECT count(*) FROM qc.t2), (SELECT sum(v) FROM qc.t2)"
            .into(),
        want: [vec![
            d.t1.len().to_string(),
            opt_sum(d.t1.iter().map(|r| Some(r.id))),
            opt_sum(d.t1.iter().map(|r| r.a)),
            opt_sum(d.t1.iter().map(|r| r.e)),
            d.t2.len().to_string(),
            opt_sum(d.t2.iter().map(|r| r.v)),
        ]]
        .into_iter()
        .collect(),
        decimal_cols: vec![3],
    }
}

/// Session settings that change the plan or execution path, not the answer.
/// Each entry: (variable, candidate values).
pub const SETTINGS: [(&str, &[&str]); 12] = [
    ("pipeline_dop", &["1", "2", "0"]),
    ("new_planner_agg_stage", &["0", "1", "2", "3", "4"]),
    ("enable_global_runtime_filter", &["true", "false"]),
    ("cbo_enable_low_cardinality_optimize", &["true", "false"]),
    ("enable_query_cache", &["true", "false"]),
    ("enable_rewrite_simple_agg_to_meta_scan", &["true", "false"]),
    ("disable_join_reorder", &["true", "false"]),
    ("enable_local_shuffle_agg", &["true", "false"]),
    ("streaming_preaggregation_mode", &["'auto'", "'force_streaming'", "'force_preaggregation'"]),
    ("enable_rewrite_sum_by_associative_rule", &["true", "false"]),
    ("enable_sort_aggregate", &["true", "false"]),
    ("cbo_cte_reuse", &["true", "false"]),
];
