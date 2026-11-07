use std::path::PathBuf;

use anyhow::Result;
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

pub use crate::store::harvest::{
    current_snapshot, new_snapshot, now, read_json, sanitize, snapshot, write_json, Lock, Log,
};

pub fn parse_ts(ts: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

pub fn fmt_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn plus_ms(ts: &str) -> Option<String> {
    parse_ts(ts).map(|t| fmt_ts(t + TimeDelta::milliseconds(1)))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Unit {
    pub entity: String,
    pub id: String,
    pub provider: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub expected: u64,
    #[serde(default, skip_serializing)]
    pub cap: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityPlan {
    pub name: String,
    pub global: Option<u64>,
    pub provider_sum: u64,
    pub providers: Vec<(String, u64)>,
    pub units: Vec<Unit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub snapshot: String,
    pub created: String,
    pub entities: Vec<EntityPlan>,
}

impl Plan {
    pub fn cap(&self, unit: &Unit) -> u64 {
        let Some(ep) = self.entities.iter().find(|p| p.name == unit.entity) else {
            return unit.expected;
        };
        let provider = unit
            .provider
            .as_ref()
            .and_then(|p| ep.providers.iter().find(|(n, _)| n == p).map(|(_, c)| *c));
        provider
            .or(ep.global)
            .unwrap_or(unit.expected)
            .max(unit.expected)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "GapRecord")]
pub struct Gap {
    pub from: String,
    pub to: String,
    pub kind: Option<String>,
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records: Option<u64>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum GapRecord {
    Pair(String, String),
    Full {
        from: String,
        to: String,
        #[serde(default)]
        kind: Option<String>,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        records: Option<u64>,
    },
}

impl std::fmt::Display for Gap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} .. {} {} {}",
            self.from,
            self.to,
            self.kind.as_deref().unwrap_or("-"),
            self.reason.as_deref().unwrap_or("-")
        )?;
        match self.records {
            Some(n) => write!(f, " records={n}"),
            None => Ok(()),
        }
    }
}

impl From<GapRecord> for Gap {
    fn from(r: GapRecord) -> Self {
        match r {
            GapRecord::Pair(from, to) => Self {
                from,
                to,
                kind: None,
                reason: None,
                records: None,
            },
            GapRecord::Full {
                from,
                to,
                kind,
                reason,
                records,
            } => Self {
                from,
                to,
                kind,
                reason,
                records,
            },
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UnitState {
    pub cursor: Option<String>,
    pub seen: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub salvage_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub salvage_seen: Vec<String>,
    pub page_size: usize,
    pub pages: u64,
    pub records: u64,
    pub parts: u32,
    pub bytes: u64,
    pub ties: u32,
    pub last_total: Option<u64>,
    #[serde(default)]
    pub index: u64,
    #[serde(default)]
    pub gaps: Vec<Gap>,
    pub done: bool,
    pub failed: Option<String>,
    pub started: Option<String>,
    pub updated: Option<String>,
    pub finished: Option<String>,
}

pub struct Paths {
    pub root: PathBuf,
    pub snapshot: String,
}

impl Paths {
    pub fn state(&self) -> PathBuf {
        self.root.join("state").join(&self.snapshot)
    }

    pub fn plan(&self) -> PathBuf {
        self.state().join("plan.json")
    }

    pub fn changes(&self) -> PathBuf {
        self.state().join("changes.json")
    }

    pub fn unit_state(&self, unit: &Unit) -> PathBuf {
        self.state()
            .join("units")
            .join(&unit.entity)
            .join(format!("{}.json", unit.id))
    }

    pub fn raw(&self) -> PathBuf {
        self.root.join("raw").join(&self.snapshot)
    }

    pub fn part(&self, unit: &Unit, seq: u32) -> PathBuf {
        self.raw()
            .join(&unit.entity)
            .join(&unit.id)
            .join(format!("part-{seq:05}.parquet"))
    }

    pub fn meta(&self, name: &str) -> PathBuf {
        self.raw().join("_meta").join(format!("{name}.parquet"))
    }

    pub fn log(&self) -> PathBuf {
        self.root.join("log").join(format!("{}.log", self.snapshot))
    }

    pub fn pages_log(&self) -> PathBuf {
        self.root
            .join("log")
            .join(format!("{}-pages.jsonl", self.snapshot))
    }

    pub fn open_log(&self) -> Result<Log> {
        Log::open(&self.log(), Some(&self.pages_log()))
    }
}

#[cfg(test)]
mod tests {
    use super::{plus_ms, sanitize, Gap, UnitState};

    #[test]
    fn ms_arithmetic() {
        assert_eq!(
            plus_ms("2024-02-01T20:05:45.967Z").as_deref(),
            Some("2024-02-01T20:05:45.968Z")
        );
        assert_eq!(
            plus_ms("2024-02-01T20:05:45.999Z").as_deref(),
            Some("2024-02-01T20:05:46.000Z")
        );
        assert_eq!(
            plus_ms("2024-02-01T20:05:45Z").as_deref(),
            Some("2024-02-01T20:05:45.001Z")
        );
    }

    #[test]
    fn names() {
        assert_eq!(
            sanitize("Agentschap Landbouw en Visserij "),
            "Agentschap_Landbouw_en_Visserij"
        );
        assert_eq!(sanitize("DOSP_UCLL"), "DOSP_UCLL");
    }

    #[test]
    fn gaps_read_legacy_pairs_and_keep_fault_details() -> anyhow::Result<()> {
        let legacy = r#"{"cursor":null,"seen":[],"page_size":1,"pages":0,"records":0,"parts":0,"bytes":0,"ties":0,"last_total":null,"gaps":[["","2020-09-09T10:48:10.259Z"]],"done":true,"failed":null,"started":null,"updated":null,"finished":null}"#;
        let st: UnitState = serde_json::from_str(legacy)?;
        assert_eq!(
            st.gaps,
            [Gap {
                from: String::new(),
                to: "2020-09-09T10:48:10.259Z".into(),
                kind: None,
                reason: None,
                records: None,
            }]
        );
        let gap = Gap {
            from: "a".into(),
            to: "b".into(),
            kind: Some("soap:Server".into()),
            reason: Some("Fault occurred while processing.".into()),
            records: None,
        };
        let text = serde_json::to_string(&gap)?;
        assert!(text.contains("\"kind\":\"soap:Server\""));
        assert_eq!(serde_json::from_str::<Gap>(&text)?, gap);
        Ok(())
    }
}
