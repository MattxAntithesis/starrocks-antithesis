//! Thin clients for the two protocols StarRocks serves: MySQL (port 9030) and
//! HTTP (Stream Load on FE 8030 / BE 8040). Every call has a short timeout and
//! returns a classified outcome instead of panicking.

use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder};
use std::time::Duration;

pub const DB: &str = "aw";

#[derive(Clone)]
pub struct Cluster {
    pub fe: String,
    pub bes: Vec<String>,
}

impl Cluster {
    pub fn from_env() -> Self {
        let fe = std::env::var("FE_HOST").unwrap_or_else(|_| "fe".into());
        let bes = std::env::var("BE_HOSTS")
            .unwrap_or_else(|_| "be1,be2,be3".into())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Cluster { fe, bes }
    }

    /// Opens a MySQL connection to the FE. `db` selects the workload database.
    pub fn conn(&self, db: bool, timeout: Duration) -> Result<Conn, String> {
        let mut o = OptsBuilder::new()
            .ip_or_hostname(Some(self.fe.clone()))
            .tcp_port(9030)
            .user(Some("root"))
            .prefer_socket(false)
            .tcp_connect_timeout(Some(Duration::from_secs(2)))
            .read_timeout(Some(timeout))
            .write_timeout(Some(timeout));
        if db {
            o = o.db_name(Some(DB));
        }
        Conn::new(o).map_err(|e| format!("connect: {e}"))
    }
}

/// What a write's response says about whether it took effect.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// Acknowledged and visible to reads.
    Visible,
    /// Committed, but publish had not finished: it becomes visible later.
    Committed,
    /// The label was already used by an earlier attempt.
    LabelExists,
    /// The server returned an error. The write most likely did not happen, but
    /// the model treats it as possibly applied.
    Failed(String),
    /// No answer (timeout, lost connection): may take effect now or later.
    Unknown(String),
}

impl Outcome {
    pub fn kind(&self) -> &'static str {
        match self {
            Outcome::Visible => "visible",
            Outcome::Committed => "committed",
            Outcome::LabelExists => "label_exists",
            Outcome::Failed(_) => "failed",
            Outcome::Unknown(_) => "unknown",
        }
    }
    pub fn msg(&self) -> String {
        match self {
            Outcome::Failed(m) | Outcome::Unknown(m) => m.clone(),
            _ => String::new(),
        }
    }
}

/// Classifies a MySQL error: a server error packet is a definite failure,
/// anything else (I/O, timeout, driver) leaves the outcome unknown.
pub fn classify_sql_err(e: &mysql::Error) -> Outcome {
    match e {
        mysql::Error::MySqlError(me) => {
            let m = me.message.to_lowercase();
            if m.contains("has already been used") || m.contains("label already") {
                Outcome::LabelExists
            } else if m.contains("timeout") || m.contains("timed out") {
                // A server-side timeout may still commit afterwards.
                Outcome::Unknown(me.message.clone())
            } else {
                Outcome::Failed(me.message.clone())
            }
        }
        other => Outcome::Unknown(other.to_string()),
    }
}

/// Classifies the OK packet's info string of an INSERT/DELETE/COMMIT:
/// `{'label':'..', 'status':'VISIBLE'|'COMMITTED', 'txnId':'..'}`.
pub fn classify_info(info: &str) -> Outcome {
    if info.contains("'status':'VISIBLE'") {
        Outcome::Visible
    } else if info.contains("'status':'COMMITTED'") {
        Outcome::Committed
    } else {
        // An OK with no status (e.g. an empty insert) - treat as unknown so the
        // model does not overclaim.
        Outcome::Unknown(format!("ok without status: {info}"))
    }
}

/// Runs one DML statement on its own connection and classifies the result.
pub fn exec_dml(cl: &Cluster, sql: &str, timeout: Duration) -> Outcome {
    let mut c = match cl.conn(true, timeout) {
        Ok(c) => c,
        // Could not connect: nothing was sent.
        Err(e) => return Outcome::Failed(e),
    };
    match c.query_drop(sql) {
        Ok(()) => classify_info(&c.info_str()),
        Err(e) => classify_sql_err(&e),
    }
}

/// HTTP through curl, the client StarRocks documents for Stream Load
/// (`--location-trusted` keeps the credentials across the FE -> BE redirect).
pub struct Http {
    timeout: Duration,
}

pub struct HttpResp {
    pub status: u16,
    pub body: String,
    pub json: serde_json::Value,
}

impl Http {
    pub fn new(timeout: Duration) -> Self {
        Http { timeout }
    }

    /// Sends a PUT or POST. Err means no complete response arrived (outcome unknown).
    pub fn send(
        &self,
        method: &str,
        url: &str,
        headers: &[(&str, String)],
        body: &[u8],
    ) -> Result<HttpResp, String> {
        use std::io::Write as _;
        use std::process::{Command, Stdio};
        let mut cmd = Command::new("curl");
        cmd.args(["-sS", "--location-trusted", "-u", "root:", "-X", method])
            .arg("--max-time")
            .arg(self.timeout.as_secs().to_string())
            .args(["--connect-timeout", "2", "-w", "\n%{http_code}"]);
        for (k, v) in headers {
            cmd.arg("-H").arg(format!("{k}:{v}"));
        }
        if method == "PUT" {
            // The FE refuses Stream Load requests without it.
            cmd.args(["-H", "Expect:100-continue", "--data-binary", "@-"]);
        } else {
            cmd.args(["-H", "Content-Length:0"]);
        }
        cmd.arg(url).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd.spawn().map_err(|e| format!("spawn curl: {e}"))?;
        if let Some(mut stdin) = child.stdin.take() {
            if method == "PUT" {
                let _ = stdin.write_all(body);
            }
        }
        let out = child.wait_with_output().map_err(|e| format!("curl: {e}"))?;
        let text = String::from_utf8_lossy(&out.stdout).to_string();
        if !out.status.success() {
            return Err(format!(
                "{method} {url}: curl exit {:?}: {}",
                out.status.code(),
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        let (body, code) = text.rsplit_once('\n').unwrap_or(("", text.as_str()));
        let status = code.trim().parse::<u16>().unwrap_or(0);
        let json = serde_json::from_str(body).unwrap_or(serde_json::Value::Null);
        Ok(HttpResp { status, body: body.to_string(), json })
    }
}

/// Classifies a Stream Load / transaction-interface JSON response.
pub fn classify_stream(r: &Result<HttpResp, String>) -> Outcome {
    match r {
        Err(e) => Outcome::Unknown(e.clone()),
        Ok(r) => {
            let st = r.json.get("Status").and_then(|v| v.as_str()).unwrap_or("");
            match st {
                "Success" | "OK" => Outcome::Visible,
                "Publish Timeout" | "PUBLISH_TIMEOUT" => Outcome::Committed,
                "TIMEOUT" | "TXN_IN_PROCESSING" => {
                    Outcome::Unknown(format!("{st}: {}", trunc(&r.body)))
                }
                "Label Already Exists" | "LABEL_ALREADY_EXISTS" => Outcome::LabelExists,
                "" => Outcome::Unknown(format!("http {}: {}", r.status, trunc(&r.body))),
                _ => {
                    let msg = r.json.get("Message").and_then(|v| v.as_str()).unwrap_or("");
                    let ml = msg.to_lowercase();
                    if ml.contains("timeout") || ml.contains("timed out") {
                        Outcome::Unknown(format!("{st}: {msg}"))
                    } else {
                        Outcome::Failed(format!("{st}: {msg}"))
                    }
                }
            }
        }
    }
}

pub fn trunc(s: &str) -> String {
    if s.len() > 300 {
        let mut end = 300;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    } else {
        s.to_string()
    }
}
