//! `manifest.json`: what one day directory holds and how trustworthy it is.
//!
//! Several runs may write to the same day (a restart, a crash). Each run merges its own
//! counts into the file, so the manifest always describes the whole day.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::schema::{SCHEMA_VERSION, Table};
use crate::frames::Header;
use crate::timing::MsHistogram;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Run {
    pub recorder: String,
    pub first_recv_ts: Option<i64>,
    pub last_recv_ts: Option<i64>,
    /// The book's update ID when this run's data for the day began, if the book was valid
    /// then: a run that continues over midnight. `None` for a run that starts on this day.
    #[serde(default)]
    pub book_at_start: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DayManifest {
    pub schema_version: u32,
    pub symbol: String,
    pub date: String,
    pub price_step: String,
    pub qty_step: String,
    pub host: String,
    pub runs: Vec<Run>,
    /// Rows per table.
    pub rows: BTreeMap<String, u64>,
    /// Parts per table, relative to the day directory.
    pub files: BTreeMap<String, Vec<String>>,
    /// Quality events per kind (`gap`, `resync`, `reconnect`, `overflow`, ...).
    pub quality: BTreeMap<String, u64>,
    /// Receive time covered by frames, and the part of it with a valid book.
    pub covered_ns: i64,
    pub synced_ns: i64,
    pub synced_share: f64,
    /// Time in `late` intervals: data more than a second behind the exchange.
    #[serde(default)]
    pub late_ns: i64,
    /// Lag of depth updates behind their exchange time (`crate::timing`), and its p50 and p99
    /// in ms for reading at a glance.
    #[serde(default)]
    pub lag: MsHistogram,
    #[serde(default)]
    pub lag_p50_ms: Option<u64>,
    #[serde(default)]
    pub lag_p99_ms: Option<u64>,
    /// Round trips of the recorder's own pings (`crate::timing`), with p50 and p99 in ms.
    #[serde(default)]
    pub rtt: MsHistogram,
    #[serde(default)]
    pub rtt_p50_ms: Option<u64>,
    #[serde(default)]
    pub rtt_p99_ms: Option<u64>,
    /// Messages dropped because another connection had delivered them first.
    #[serde(default)]
    pub duplicates: u64,
}

fn host() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| fs::read_to_string("/etc/hostname").ok())
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

impl DayManifest {
    pub fn new(header: &Header, date: &str, recorder: &str) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            symbol: header.symbol.clone(),
            date: date.to_owned(),
            price_step: header.price_step.clone(),
            qty_step: header.qty_step.clone(),
            host: host(),
            runs: vec![Run {
                recorder: recorder.to_owned(),
                first_recv_ts: None,
                last_recv_ts: None,
                book_at_start: None,
            }],
            rows: Table::ALL
                .iter()
                .map(|t| (t.name().to_owned(), 0))
                .collect(),
            files: BTreeMap::new(),
            quality: BTreeMap::new(),
            covered_ns: 0,
            synced_ns: 0,
            synced_share: 0.0,
            late_ns: 0,
            lag: MsHistogram::default(),
            lag_p50_ms: None,
            lag_p99_ms: None,
            rtt: MsHistogram::default(),
            rtt_p50_ms: None,
            rtt_p99_ms: None,
            duplicates: 0,
        }
    }

    pub fn touch(&mut self, ts: i64) {
        let run = self.runs.last_mut().expect("a manifest has a run");
        run.first_recv_ts = Some(run.first_recv_ts.map_or(ts, |t| t.min(ts)));
        run.last_recv_ts = Some(run.last_recv_ts.map_or(ts, |t| t.max(ts)));
    }

    pub fn add_rows(&mut self, table: Table, n: u64) {
        *self.rows.entry(table.name().to_owned()).or_default() += n;
    }

    pub fn add_file(&mut self, table: Table, rel: String) {
        self.files
            .entry(table.name().to_owned())
            .or_default()
            .push(rel);
    }

    pub fn count_quality(&mut self, kind: &str) {
        *self.quality.entry(kind.to_owned()).or_default() += 1;
    }

    /// The figures derived from the counts.
    fn share(&mut self) {
        self.synced_share = if self.covered_ns > 0 {
            self.synced_ns as f64 / self.covered_ns as f64
        } else {
            0.0
        };
        self.lag_p50_ms = self.lag.quantile_ms(0.5);
        self.lag_p99_ms = self.lag.quantile_ms(0.99);
        self.rtt_p50_ms = self.rtt.quantile_ms(0.5);
        self.rtt_p99_ms = self.rtt.quantile_ms(0.99);
    }

    fn key(&self) -> (u32, &str, &str, &str, &str) {
        (
            self.schema_version,
            &self.symbol,
            &self.date,
            &self.price_step,
            &self.qty_step,
        )
    }

    fn load(path: &Path) -> Result<Option<DayManifest>, String> {
        if !path.exists() {
            return Ok(None);
        }
        let text = fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Refuse to add to a day directory that holds other data: checked before the first
    /// part is written, so mixed data never lands next to each other.
    pub fn check_file(&self, path: &Path) -> Result<(), String> {
        match Self::load(path)? {
            Some(old) if old.key() != self.key() => Err(format!(
                "{} describes other data: {:?}, this run writes {:?} (schema, symbol, date, price step, qty step)",
                path.display(),
                old.key(),
                self.key()
            )),
            _ => Ok(()),
        }
    }

    /// Merge this run into `path` (create it if missing), atomically.
    pub fn merge_into_file(&mut self, path: &Path) -> Result<(), String> {
        self.check_file(path)?;
        let mut merged = if let Some(mut old) = Self::load(path)? {
            old.runs.extend(self.runs.iter().cloned());
            for (k, v) in &self.rows {
                *old.rows.entry(k.clone()).or_default() += v;
            }
            for (k, v) in &self.files {
                old.files
                    .entry(k.clone())
                    .or_default()
                    .extend(v.iter().cloned());
            }
            for (k, v) in &self.quality {
                *old.quality.entry(k.clone()).or_default() += v;
            }
            old.covered_ns += self.covered_ns;
            old.synced_ns += self.synced_ns;
            old.late_ns += self.late_ns;
            old.lag.merge(&self.lag);
            old.rtt.merge(&self.rtt);
            old.duplicates += self.duplicates;
            old
        } else {
            self.clone()
        };
        merged.share();
        let tmp = path.with_extension("json.tmp");
        let text = serde_json::to_string_pretty(&merged).map_err(|e| e.to_string())?;
        fs::write(&tmp, text + "\n").map_err(|e| e.to_string())?;
        fs::File::open(&tmp)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
        fs::rename(&tmp, path).map_err(|e| e.to_string())?;
        *self = merged;
        Ok(())
    }
}
