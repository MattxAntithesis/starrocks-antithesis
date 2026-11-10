//! What the driver expects StarRocks to contain.
//!
//! * `KeyHistory`: every write sent to one key of the primary-key table `kv`,
//!   with when it was sent and when (if ever) it was acknowledged as visible.
//! * `Batch`: a group of rows loaded into the duplicate-key tables `ev`/`ev2`
//!   by one transaction. A batch must be visible entirely or not at all.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// How long a write with an unknown outcome stays possible. StarRocks keeps
/// retrying publish of a committed transaction, so this is generous.
pub const AMBIGUITY_WINDOW: Duration = Duration::from_secs(900);
/// Longest a read may run (the driver's read timeout), with margin.
pub const READ_WINDOW: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct Write {
    /// `None` is a delete.
    pub value: Option<i64>,
    pub sent: Instant,
    pub acked: Option<Instant>,
}

#[derive(Default)]
pub struct KeyHistory {
    pub writes: Vec<Write>,
}

impl KeyHistory {
    /// A key whose row already exists, holding generation `g`.
    pub fn seeded(t0: Instant, g: i64) -> Self {
        KeyHistory { writes: vec![Write { value: Some(g), sent: t0, acked: Some(t0) }] }
    }

    pub fn new(t0: Instant) -> Self {
        // The key starts absent: model that as a delete acknowledged at t0.
        KeyHistory { writes: vec![Write { value: None, sent: t0, acked: Some(t0) }] }
    }

    /// w was acknowledged, and some write sent after w's acknowledgement was
    /// itself acknowledged before t.
    fn overwritten_before(&self, w: &Write, t: Instant) -> bool {
        let Some(a) = w.acked else { return false };
        self.writes
            .iter()
            .any(|w2| w2.sent > a && matches!(w2.acked, Some(a2) if a2 < t))
    }

    /// Values a read running from `start` to `end` may return, with the reason
    /// it is allowed. A value may be returned if its write was sent before
    /// `end` and was not overwritten before `start`.
    pub fn may_return(&self, got: Option<i64>, start: Instant, end: Instant) -> bool {
        self.writes
            .iter()
            .any(|w| w.value == got && w.sent < end && !self.overwritten_before(w, start))
    }

    pub fn summary(&self, now: Instant) -> Vec<String> {
        self.writes
            .iter()
            .map(|w| {
                format!(
                    "{:?} sent -{}ms acked {}",
                    w.value,
                    now.saturating_duration_since(w.sent).as_millis(),
                    match w.acked {
                        Some(a) => format!("-{}ms", now.saturating_duration_since(a).as_millis()),
                        None => "no".into(),
                    }
                )
            })
            .collect()
    }

    pub fn prune(&mut self, now: Instant) {
        let cutoff = now.checked_sub(READ_WINDOW * 2);
        let amb = now.checked_sub(AMBIGUITY_WINDOW);
        let keep: Vec<bool> = self
            .writes
            .iter()
            .map(|w| match w.acked {
                Some(_) => match cutoff {
                    Some(c) => !self.overwritten_before(w, c),
                    None => true,
                },
                None => match amb {
                    Some(a) => w.sent > a,
                    None => true,
                },
            })
            .collect();
        let mut i = 0;
        self.writes.retain(|_| {
            let k = keep[i];
            i += 1;
            k
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Method {
    Insert,
    StreamLoadFe,
    StreamLoadBe,
    SqlTxn,
    TxnStreamLoad,
}

impl Method {
    pub fn name(&self) -> &'static str {
        match self {
            Method::Insert => "insert",
            Method::StreamLoadFe => "stream_load_fe",
            Method::StreamLoadBe => "stream_load_be",
            Method::SqlTxn => "sql_txn",
            Method::TxnStreamLoad => "txn_stream_load",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Batch {
    pub id: i64,
    pub rows: i64,
    /// Tables the batch writes, each with all `rows` rows.
    pub tables: Vec<&'static str>,
    pub method: Method,
    pub label: String,
    pub sent: Instant,
    /// Acknowledged visible at this time.
    pub visible_at: Option<Instant>,
    /// Acknowledged committed (publish pending): must become visible.
    pub committed: bool,
    /// Rolled back on request (acknowledged): must never become visible.
    pub rolled_back: bool,
    /// End time of a read that saw the whole batch.
    pub seen_at: Option<Instant>,
    /// The last outcome kind, for details.
    pub last_outcome: String,
    pub attempts: u32,
}

impl Batch {
    /// Sum of `seq` over the batch's rows (seq runs 0..rows).
    pub fn seq_sum(&self) -> i64 {
        self.rows * (self.rows - 1) / 2
    }

    /// Sum of `val` over the batch's rows; row s holds `id * 1000 + s`, so a
    /// row whose value was corrupted or came from another batch shows up here
    /// even when the row count and seq sum are right.
    pub fn val_sum(&self) -> i64 {
        self.rows * self.id * 1000 + self.seq_sum()
    }
}

/// Every batch the workload created. Batches are never forgotten: the final
/// check needs all of them, and there are only a few thousand per timeline.
#[derive(Default)]
pub struct Batches {
    pub by_id: HashMap<i64, Batch>,
}

/// One row of `SELECT batch, count(*), sum(seq) ... GROUP BY batch`.
#[derive(Clone, Copy, Debug)]
pub struct Seen {
    pub count: i64,
    pub seq_sum: i64,
    pub val_sum: i64,
}
