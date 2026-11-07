use std::fmt::Write as _;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{TimeDelta, Utc};
use serde::Serialize;

use crate::changes::ChangesState;
use crate::config::Config;
use crate::harvest::RESIDUAL;
use crate::net::Signal;
use crate::state::{self, EntityPlan, Gap, Lock, Paths, Plan, UnitState};
use crate::store::harvest::{difference, UnitStatus};

fn gib(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0 / 1024.0
}

fn recent(path: &Path, window_min: i64) -> (u64, u64, f64) {
    let Ok(mut f) = fs::File::open(path) else {
        return (0, 0, 0.0);
    };
    let len = f.metadata().map_or(0, |m| m.len());
    let _ = f.seek(SeekFrom::Start(len.saturating_sub(8 * 1024 * 1024)));
    let mut buf = String::new();
    if f.read_to_string(&mut buf).is_err() {
        return (0, 0, 0.0);
    }
    let cutoff = Utc::now() - TimeDelta::minutes(window_min);
    let (mut records, mut bytes, mut first) = (0u64, 0u64, None);
    for line in buf.lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some(t) = v["t"].as_str().and_then(state::parse_ts) else {
            continue;
        };
        if t < cutoff || v["mode"] == "probe" {
            continue;
        }
        first = Some(first.map_or(t, |f: chrono::DateTime<Utc>| f.min(t)));
        records += v["n"].as_u64().unwrap_or(0);
        bytes += v["bytes"].as_u64().unwrap_or(0);
    }
    let minutes = first.map_or(0.0, |f| (Utc::now() - f).num_seconds() as f64 / 60.0);
    (records, bytes, minutes)
}

fn rate_line(paths: &Paths, entities: &[(String, u64, u64, u64)]) -> Option<String> {
    let (records, bytes, minutes) = recent(&paths.pages_log(), 10);
    if minutes <= 0.0 || bytes == 0 {
        return None;
    }
    let per_sec = bytes as f64 / minutes / 60.0;
    let mut remaining = 0.0;
    let mut unsampled = Vec::new();
    for (name, rec, exp, got_bytes) in entities {
        let left = exp.saturating_sub(*rec);
        if left == 0 {
            continue;
        }
        if *rec == 0 {
            unsampled.push(name.as_str());
            continue;
        }
        remaining += left as f64 * (*got_bytes as f64 / *rec as f64);
    }
    let eta = remaining / per_sec / 3600.0;
    let tail = if unsampled.is_empty() {
        String::new()
    } else {
        format!(" (+ not yet sampled: {})", unsampled.join(", "))
    };
    Some(format!(
        "last {minutes:.0} min: {:.0} records/min, {:.2} MiB/s; sampled entities need ~{:.1} GiB more, eta {eta:.1} h{tail}",
        records as f64 / minutes,
        per_sec / 1024.0 / 1024.0,
        remaining / 1024.0 / 1024.0 / 1024.0
    ))
}

fn tail(paths: &Paths) -> String {
    let mut out = String::new();
    if let Ok(text) = fs::read_to_string(paths.log()) {
        let lines: Vec<&str> = text.lines().rev().take(5).collect();
        for line in lines.into_iter().rev() {
            let _ = writeln!(out, "  {line}");
        }
    }
    out
}

fn entity_line(
    paths: &Paths,
    ep: &EntityPlan,
    out: &mut String,
    failures: &mut Vec<String>,
    residual: &mut Vec<String>,
) -> Result<(u64, u64, u64)> {
    let (mut done, mut failed, mut rec, mut exp, mut pages, mut bytes) =
        (0, 0, 0u64, 0u64, 0u64, 0u64);
    for u in &ep.units {
        let st: UnitState = state::read_json(&paths.unit_state(u))?.unwrap_or_default();
        if u.id == RESIDUAL {
            residual.push(format!("{} {}", ep.name, st.records));
        } else {
            exp += u.expected;
            rec += st.records;
        }
        pages += st.pages;
        bytes += st.bytes;
        if st.done {
            done += 1;
        }
        for g in &st.gaps {
            failures.push(format!("{}/{}: poison gap {g}", u.entity, u.id));
        }
        if let Some(f) = &st.failed {
            failed += 1;
            failures.push(format!("{}/{}: {f}", u.entity, u.id));
        }
    }
    let pct = if exp == 0 {
        100.0
    } else {
        rec as f64 * 100.0 / exp as f64
    };
    let _ = writeln!(
        out,
        "{:<16} {:>3}/{:<3} {:>6} {:>12} {:>12} {:>6.1}% {:>9} {:>8.2}",
        ep.name,
        done,
        ep.units.len(),
        failed,
        rec,
        exp,
        pct,
        pages,
        gib(bytes)
    );
    Ok((rec, exp, bytes))
}

pub fn report(cfg: &Config) -> Result<String> {
    let mut out = String::new();
    let Some(snapshot) = state::current_snapshot(&cfg.source_dir()) else {
        return Ok("no snapshot yet\n".into());
    };
    let paths = Paths {
        root: cfg.root.clone(),
        snapshot: snapshot.clone(),
    };
    let running = Lock::held(&cfg.source_dir());
    let bw = fs::read_to_string(Signal::Bandwidth.path(&cfg.control_dir()))
        .unwrap_or_else(|_| cfg.bandwidth.clone());
    let paused = Signal::Pause.path(&cfg.control_dir()).exists();
    let stopping = Signal::Stop.path(&cfg.control_dir()).exists();
    let _ = writeln!(
        out,
        "snapshot {snapshot}  running={running}  bandwidth={}  paused={paused}  stop_requested={stopping}",
        bw.trim()
    );
    let plan: Option<Plan> = state::read_json(&paths.plan()).context("read plan")?;
    let Some(plan) = plan else {
        return Ok(out + "no plan yet\n");
    };
    let _ = writeln!(
        out,
        "{:<16} {:>7} {:>6} {:>12} {:>12} {:>7} {:>9} {:>8}",
        "entity", "units", "failed", "records", "expected", "pct", "pages", "GiB"
    );
    let (mut all_rec, mut all_exp, mut all_bytes) = (0u64, 0u64, 0u64);
    let mut failures = Vec::new();
    let mut per_entity = Vec::new();
    let mut residual = Vec::new();
    for ep in &plan.entities {
        let (rec, exp, bytes) = entity_line(&paths, ep, &mut out, &mut failures, &mut residual)?;
        all_rec += rec;
        all_exp += exp;
        all_bytes += bytes;
        per_entity.push((ep.name.clone(), rec, exp, bytes));
    }
    let pct = if all_exp == 0 {
        100.0
    } else {
        all_rec as f64 * 100.0 / all_exp as f64
    };
    let _ = writeln!(
        out,
        "{:<16} {:>7} {:>6} {:>12} {:>12} {:>6.1}% {:>9} {:>8.2}",
        "total",
        "",
        "",
        all_rec,
        all_exp,
        pct,
        "",
        gib(all_bytes)
    );
    if !residual.is_empty() {
        let _ = writeln!(
            out,
            "residual units, outside the percentages: {}",
            residual.join(", ")
        );
    }
    if let Some(line) = rate_line(&paths, &per_entity) {
        let _ = writeln!(out, "{line}");
    }
    for f in failures {
        let _ = writeln!(out, "FAILED {f}");
    }
    out.push_str(&tail(&paths));
    Ok(out)
}

#[derive(Serialize)]
struct Status {
    source: &'static str,
    snapshot: Option<String>,
    running: bool,
    planned: bool,
    terminal: bool,
    changes_run: Option<String>,
    units: Vec<UnitStatus<Gap>>,
}

fn label(st: &UnitState) -> &'static str {
    if st.done && !st.gaps.is_empty() {
        "gaps"
    } else if st.done {
        "done"
    } else if st.failed.is_some() {
        "failed"
    } else if st.pages > 0 {
        "partial"
    } else {
        "pending"
    }
}

pub fn json(cfg: &Config) -> Result<String> {
    let snapshot = state::current_snapshot(&cfg.source_dir());
    let mut status = Status {
        source: "fris",
        snapshot: snapshot.clone(),
        running: Lock::held(&cfg.source_dir()),
        planned: false,
        terminal: false,
        changes_run: None,
        units: Vec::new(),
    };
    if let Some(snapshot) = snapshot {
        let paths = Paths {
            root: cfg.root.clone(),
            snapshot,
        };
        let changes: Option<ChangesState> =
            state::read_json(&paths.changes()).context("read changes")?;
        status.changes_run = changes.and_then(|c| c.run).map(|r| r.started);
        let plan: Option<Plan> = state::read_json(&paths.plan()).context("read plan")?;
        if let Some(plan) = plan {
            status.planned = cfg
                .entities
                .iter()
                .all(|e| plan.entities.iter().any(|p| p.name == e.name));
            for u in plan.entities.iter().flat_map(|e| &e.units) {
                let st: UnitState = state::read_json(&paths.unit_state(u))?.unwrap_or_default();
                let expected = (u.id != RESIDUAL).then_some(u.expected);
                status.units.push(UnitStatus {
                    unit: format!("{}/{}", u.entity, u.id),
                    state: label(&st),
                    terminal: st.done,
                    expected,
                    rows: st.records,
                    count_difference: expected
                        .filter(|_| st.done)
                        .map(|e| difference(st.records, e)),
                    missing_metadata: 0,
                    failed: st.failed.clone(),
                    gaps: st.gaps,
                });
            }
            status.terminal = status.planned && status.units.iter().all(|u| u.terminal);
        }
    }
    Ok(serde_json::to_string_pretty(&status)? + "\n")
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::state::{self, EntityPlan, Gap, Paths, Plan, Unit, UnitState};

    #[test]
    fn json_reports_plan_units_and_gaps() -> anyhow::Result<()> {
        let mut cfg: Config = toml::from_str(include_str!("../config/fris.toml"))?;
        cfg.root = std::env::temp_dir().join(format!("fris-status-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cfg.root);
        cfg.entities.retain(|e| e.name == "person");
        let value = |cfg: &Config| -> anyhow::Result<serde_json::Value> {
            Ok(serde_json::from_str(&super::json(cfg)?)?)
        };
        assert_eq!(value(&cfg)?["snapshot"], serde_json::Value::Null);
        let paths = Paths {
            root: cfg.root.clone(),
            snapshot: state::new_snapshot(&cfg.source_dir())?,
        };
        let units: Vec<Unit> = ["a", "b"]
            .iter()
            .map(|id| Unit {
                entity: "person".into(),
                id: (*id).into(),
                provider: None,
                start: None,
                end: None,
                expected: 1,
                cap: 0,
            })
            .collect();
        state::write_json(
            &paths.plan(),
            &Plan {
                snapshot: paths.snapshot.clone(),
                created: state::now(),
                entities: vec![EntityPlan {
                    name: "person".into(),
                    global: None,
                    provider_sum: 0,
                    providers: Vec::new(),
                    units: units.clone(),
                }],
            },
        )?;
        state::write_json(
            &paths.unit_state(&units[0]),
            &UnitState {
                done: true,
                records: 3,
                gaps: vec![Gap {
                    from: "x".into(),
                    to: "y".into(),
                    kind: None,
                    reason: Some("fault".into()),
                    records: None,
                }],
                ..UnitState::default()
            },
        )?;
        let status = value(&cfg)?;
        assert_eq!(
            (status["planned"].clone(), status["terminal"].clone()),
            (true.into(), false.into())
        );
        assert_eq!(status["units"][0]["unit"], "person/a");
        assert_eq!(status["units"][0]["state"], "gaps");
        assert_eq!(status["units"][0]["gaps"][0]["reason"], "fault");
        assert_eq!(status["units"][1]["state"], "pending");
        state::write_json(
            &paths.unit_state(&units[1]),
            &UnitState {
                done: true,
                ..UnitState::default()
            },
        )?;
        assert_eq!(value(&cfg)?["terminal"], true);
        let mut wider = cfg.clone();
        wider.entities = toml::from_str::<Config>(include_str!("../config/fris.toml"))?.entities;
        assert_eq!(value(&wider)?["terminal"], false);
        std::fs::remove_dir_all(&cfg.root)?;
        Ok(())
    }
}
