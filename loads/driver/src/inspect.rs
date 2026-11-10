//! Reading StarRocks' internal state through `information_schema`.
//!
//! The BE runs from an official image, so it carries no Antithesis assertions.
//! StarRocks publishes the BE's own view over SQL instead, which gives the
//! driver two things it otherwise could not have:
//!
//! * **A replica-divergence oracle.** `be_tablets` reports `NUM_ROW` per BE per
//!   tablet. Every check that goes through the FE sees one replica, so a
//!   diverged replica is invisible to them; comparing replicas at the same
//!   version catches it. See antithesis/BUG-HISTORY.md.
//! * **Steering signals.** Rowset pile-up, transactions committed but not yet
//!   published, and tablet repairs in flight are the states the project's own
//!   bug history keeps pointing at. Reported through rich `Sometimes`
//!   assertions, they let Antithesis climb toward those states.
//!
//! These tables query the BEs live, so under faults they fail or time out. A
//! failed read is no information, never a violation.

use crate::client::Cluster;
use mysql::prelude::Queryable;
use std::collections::HashMap;
use std::time::Duration;

/// One tablet replica, as one BE reports it.
#[derive(Clone, Debug)]
pub struct TabletReplica {
    pub be_id: i64,
    pub tablet_id: i64,
    pub max_version: i64,
    pub num_rowset: i64,
    pub num_row: i64,
    pub state: String,
}

/// Replicas of one tablet that agree on `max_version` but not on `num_row`.
#[derive(Clone, Debug)]
pub struct Divergence {
    pub tablet_id: i64,
    pub version: i64,
    /// `(be_id, num_row)`, one per replica, sorted by BE.
    pub rows: Vec<(i64, i64)>,
}

pub struct Snapshot {
    pub replicas: Vec<TabletReplica>,
    /// Highest `NUM_ROWSET` over the workload's tablets: version pile-up, which
    /// is what brings on compaction.
    pub max_rowsets: i64,
    /// Tablets whose replicas disagree at one version.
    pub divergences: Vec<Divergence>,
    /// Replicas seen for each tablet, so a one-replica reading is not mistaken
    /// for agreement.
    pub tablets_with_all_replicas: usize,
}

fn table_ids(c: &mut mysql::Conn, db: &str) -> Result<Vec<i64>, String> {
    let sql = format!(
        "SELECT TABLE_ID FROM information_schema.tables_config WHERE TABLE_SCHEMA = '{db}'"
    );
    c.query(&sql).map_err(|e| format!("tables_config: {e}"))
}

/// Reads every replica of the workload's tablets and looks for divergence.
pub fn tablets(cl: &Cluster, db: &str, replication: usize) -> Result<Snapshot, String> {
    let mut c = cl.conn(false, Duration::from_secs(20))?;
    let ids = table_ids(&mut c, db)?;
    if ids.is_empty() {
        return Err("no tables in tables_config yet".into());
    }
    let list: Vec<String> = ids.iter().map(|i| i.to_string()).collect();
    let sql = format!(
        "SELECT BE_ID, TABLET_ID, MAX_VERSION, NUM_ROWSET, NUM_ROW, STATE \
         FROM information_schema.be_tablets WHERE TABLE_ID IN ({})",
        list.join(",")
    );
    let rows: Vec<(i64, i64, i64, i64, i64, String)> =
        c.query(&sql).map_err(|e| format!("be_tablets: {e}"))?;
    let replicas: Vec<TabletReplica> = rows
        .into_iter()
        .map(|(be_id, tablet_id, max_version, num_rowset, num_row, state)| TabletReplica {
            be_id,
            tablet_id,
            max_version,
            num_rowset,
            num_row,
            state,
        })
        .collect();

    let max_rowsets = replicas.iter().map(|r| r.num_rowset).max().unwrap_or(0);

    // Compare only replicas that are NORMAL and agree on the version: a replica
    // still catching up legitimately holds fewer rows.
    let mut by_tablet: HashMap<i64, Vec<&TabletReplica>> = HashMap::new();
    for r in &replicas {
        by_tablet.entry(r.tablet_id).or_default().push(r);
    }
    let mut divergences = vec![];
    let mut complete = 0;
    for (tablet_id, rs) in &by_tablet {
        if rs.len() >= replication {
            complete += 1;
        }
        let mut by_version: HashMap<i64, Vec<&&TabletReplica>> = HashMap::new();
        for r in rs {
            if r.state.eq_ignore_ascii_case("NORMAL") {
                by_version.entry(r.max_version).or_default().push(r);
            }
        }
        for (version, same) in by_version {
            if same.len() < 2 {
                continue;
            }
            let first = same[0].num_row;
            if same.iter().any(|r| r.num_row != first) {
                let mut rows: Vec<(i64, i64)> = same.iter().map(|r| (r.be_id, r.num_row)).collect();
                rows.sort();
                divergences.push(Divergence { tablet_id: *tablet_id, version, rows });
            }
        }
    }
    divergences.sort_by_key(|d| d.tablet_id);
    Ok(Snapshot {
        replicas,
        max_rowsets,
        divergences,
        tablets_with_all_replicas: complete,
    })
}

/// Transactions the BEs have committed but not yet published. The project's
/// publish-version fixes live in exactly this window.
pub fn committed_unpublished(cl: &Cluster) -> Result<i64, String> {
    let mut c = cl.conn(false, Duration::from_secs(20))?;
    let n: Option<i64> = c
        .query_first(
            "SELECT count(*) FROM information_schema.be_txns \
             WHERE COMMIT_TIME > 0 AND (PUBLISH_TIME IS NULL OR PUBLISH_TIME = 0)",
        )
        .map_err(|e| format!("be_txns: {e}"))?;
    Ok(n.unwrap_or(0))
}

/// Tablet repairs the FE has scheduled: clone, decommission and friends.
pub fn repairs_in_flight(cl: &Cluster) -> Result<i64, String> {
    let mut c = cl.conn(false, Duration::from_secs(20))?;
    let n: Option<i64> = c
        .query_first("SELECT count(*) FROM information_schema.fe_tablet_schedules")
        .map_err(|e| format!("fe_tablet_schedules: {e}"))?;
    Ok(n.unwrap_or(0))
}

/// Column-change jobs the FE is running on `db`, as `SHOW ALTER TABLE COLUMN`
/// reports them: `(table_name, state)`.
pub fn alter_jobs(cl: &Cluster, db: &str) -> Result<Vec<(String, String)>, String> {
    let mut c = cl.conn(false, Duration::from_secs(20))?;
    let rows: Vec<mysql::Row> = c
        .query(format!("SHOW ALTER TABLE COLUMN FROM {db}"))
        .map_err(|e| format!("show alter: {e}"))?;
    let mut out = vec![];
    for row in rows {
        let cols = row.columns();
        let idx = |name: &str| cols.iter().position(|c| c.name_str() == name);
        let (Some(t), Some(s)) = (idx("TableName"), idx("State")) else {
            continue;
        };
        let table: String = row.get_opt(t).and_then(|r| r.ok()).unwrap_or_default();
        let state: String = row.get_opt(s).and_then(|r| r.ok()).unwrap_or_default();
        out.push((table, state));
    }
    Ok(out)
}
