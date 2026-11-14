//! `pt`: a range-partitioned duplicate-key table exercised by loads, INSERT
//! OVERWRITE and TRUNCATE on the same partitions, concurrently.
//!
//! Every operation writes rows tagged with a fresh `tag` (one op, one tag), so a
//! read can be attributed op by op. The checks need no total order between
//! concurrent operations; they only use what the client knows for certain:
//!
//! * an op is visible entirely or not at all, and never twice;
//! * a row only ever appears in the partition its op wrote to;
//! * an erasing op (overwrite or truncate) acknowledged before a read started
//!   removes every op acknowledged before the erasing op was *sent*;
//! * an op acknowledged before a read started is present, unless some erasing
//!   op on that partition might have taken effect after it.

use std::collections::HashMap;
use std::time::Instant;

pub const PARTITIONS: [(&str, &str); 4] = [
    ("p1", "2026-01-01"),
    ("p2", "2026-01-02"),
    ("p3", "2026-01-03"),
    ("p4", "2026-01-04"),
];

pub const DDL: &str = "CREATE TABLE IF NOT EXISTS aw.pt (d DATE NOT NULL, tag BIGINT NOT NULL, \
     seq INT NOT NULL, val BIGINT NOT NULL) DUPLICATE KEY(d, tag) \
     PARTITION BY RANGE(d) (\
       PARTITION p1 VALUES [('2026-01-01'),('2026-01-02')), \
       PARTITION p2 VALUES [('2026-01-02'),('2026-01-03')), \
       PARTITION p3 VALUES [('2026-01-03'),('2026-01-04')), \
       PARTITION p4 VALUES [('2026-01-04'),('2026-01-05'))) \
     DISTRIBUTED BY HASH(tag) BUCKETS 2 PROPERTIES(\"replication_num\"=\"3\")";

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    Load,
    Overwrite,
    Truncate,
}

impl Kind {
    pub fn name(&self) -> &'static str {
        match self {
            Kind::Load => "load",
            Kind::Overwrite => "overwrite",
            Kind::Truncate => "truncate",
        }
    }
    pub fn erases(&self) -> bool {
        !matches!(self, Kind::Load)
    }
}

#[derive(Clone, Debug)]
pub struct Op {
    pub tag: i64,
    pub part: usize,
    pub kind: Kind,
    pub rows: i64,
    pub sent: Instant,
    /// Acknowledged visible (or, for truncate/overwrite, OK).
    pub acked: Option<Instant>,
    /// The server answered with a definite error: the op did not happen.
    pub failed: bool,
}

#[derive(Default)]
pub struct PartState {
    pub ops: Vec<Op>,
    pub by_tag: HashMap<i64, usize>,
}

/// One violation found by a check, with the property it breaks.
pub struct Violation {
    pub property: &'static str,
    pub detail: serde_json::Value,
}

impl PartState {
    pub fn add(&mut self, op: Op) {
        self.by_tag.insert(op.tag, self.ops.len());
        self.ops.push(op);
    }

    pub fn ack(&mut self, tag: i64, acked: Option<Instant>, failed: bool) {
        if let Some(&i) = self.by_tag.get(&tag) {
            self.ops[i].acked = acked;
            self.ops[i].failed = failed;
        }
    }

    /// Checks one read of partition `part`, which returned `seen` (tag -> row
    /// count) and ran between `start` and `end`.
    pub fn check(&self, part: usize, seen: &HashMap<i64, i64>, start: Instant, end: Instant) -> Vec<Violation> {
        let mut v = vec![];
        let ops: Vec<&Op> = self.ops.iter().filter(|o| o.part == part).collect();
        for (tag, count) in seen {
            match self.by_tag.get(tag).map(|&i| &self.ops[i]) {
                None => v.push(Violation {
                    property: "pt: only rows the workload wrote appear",
                    detail: serde_json::json!({"part": part, "tag": tag, "count": count}),
                }),
                Some(op) if op.part != part => v.push(Violation {
                    property: "pt: a row appears only in the partition it was written to",
                    detail: serde_json::json!({"part": part, "tag": tag, "op_part": op.part}),
                }),
                Some(op) => {
                    if *count > op.rows {
                        v.push(Violation {
                            property: "pt: an operation is never applied twice",
                            detail: serde_json::json!({"part": part, "tag": tag, "kind": op.kind.name(), "rows": op.rows, "count": count}),
                        });
                    } else if *count != op.rows {
                        v.push(Violation {
                            property: "pt: an operation is visible entirely or not at all",
                            detail: serde_json::json!({"part": part, "tag": tag, "kind": op.kind.name(), "rows": op.rows, "count": count}),
                        });
                    }
                    if op.sent > end {
                        v.push(Violation {
                            property: "pt: only rows the workload wrote appear",
                            detail: serde_json::json!({"part": part, "tag": tag, "why": "seen before it was sent"}),
                        });
                    }
                    // Erased: some erasing op acknowledged before the read
                    // started was sent after this op was acknowledged.
                    if let Some(a) = op.acked {
                        if let Some(e) = ops.iter().find(|e| {
                            e.kind.erases() && e.tag != op.tag && e.sent > a && matches!(e.acked, Some(ea) if ea < start)
                        }) {
                            v.push(Violation {
                                property: "pt: an overwrite or truncate removes everything acknowledged before it started",
                                detail: serde_json::json!({"part": part, "tag": tag, "kind": op.kind.name(),
                                    "eraser": e.tag, "eraser_kind": e.kind.name()}),
                            });
                        }
                    }
                }
            }
        }
        // Durable: acknowledged before the read, and no erasing op could have
        // taken effect after it.
        for op in &ops {
            if op.kind == Kind::Truncate {
                continue;
            }
            let Some(a) = op.acked else { continue };
            if a >= start || seen.contains_key(&op.tag) {
                continue;
            }
            let maybe_erased = ops.iter().any(|e| {
                e.kind.erases() && e.tag != op.tag && !e.failed && e.sent < end
                    && e.acked.map_or(true, |ea| ea > op.sent)
            });
            if !maybe_erased {
                v.push(Violation {
                    property: "pt: an acknowledged operation stays visible until erased",
                    detail: serde_json::json!({"part": part, "tag": op.tag, "kind": op.kind.name(), "rows": op.rows}),
                });
            }
        }
        v
    }
}
