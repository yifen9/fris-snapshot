pub mod build;
pub mod changes;
pub mod config;
pub mod fetch;
pub mod harvest;
pub mod net;
pub mod plan;
pub mod soap;
pub mod state;
pub mod status;
pub mod store;
pub mod xml;

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::Utc;

use crate::build::Tables;
use crate::config::Config;
use crate::harvest::{Abort, Harvester};
use crate::net::Gate;
use crate::state::{Lock, Paths};

pub fn gate(cfg: &Config) -> Result<Gate> {
    let rate = crate::net::parse_rate(&cfg.bandwidth).context("bandwidth")?;
    Gate::new(
        &cfg.control_dir(),
        rate,
        Duration::from_millis(cfg.min_interval_ms),
    )
}

pub fn paths(cfg: &Config) -> Result<Paths> {
    let snapshot = state::current_snapshot(&cfg.source_dir()).context("no current snapshot")?;
    Ok(Paths {
        root: cfg.root.clone(),
        snapshot,
    })
}

fn harvester<'a>(cfg: &'a Config, gate: &'a Gate, paths: Paths) -> Result<Harvester<'a>> {
    Ok(Harvester {
        cfg,
        gate,
        log: paths.open_log()?,
        paths,
        timeout: Duration::from_secs(cfg.timeout_s),
        shared: crate::net::client(false, Duration::from_secs(cfg.timeout_s))?,
    })
}

pub fn changes(cfg: &Config) -> Result<bool> {
    let _lock = Lock::acquire(&cfg.source_dir())?;
    let gate = gate(cfg)?;
    gate.clear_stop();
    let h = harvester(cfg, &gate, paths(cfg)?)?;
    let plan: state::Plan =
        state::read_json(&h.paths.plan())?.context("no plan for current snapshot")?;
    h.changes(&plan)
}

pub fn run(cfg: &Config, only: &[String], fresh: bool, plan_only: bool) -> Result<bool> {
    let _lock = Lock::acquire(&cfg.source_dir())?;
    let gate = gate(cfg)?;
    gate.clear_stop();
    let snapshot = state::snapshot(&cfg.source_dir(), fresh)?;
    let paths = Paths {
        root: cfg.root.clone(),
        snapshot,
    };
    let h = harvester(cfg, &gate, paths)?;
    h.log.line(&format!(
        "begin snapshot={} pid={} concurrency={} bandwidth={}",
        h.paths.snapshot,
        std::process::id(),
        cfg.concurrency,
        crate::net::format_rate(gate.rate())
    ));
    let plan = h.plan(only)?;
    if plan_only {
        h.log.line("plan written");
        return Ok(true);
    }
    let (done, total) = h.harvest(&plan, only)?;
    h.log.line(&format!(
        "end units_done={done}/{total} received_bytes={}",
        gate.received()
    ));
    if done != total {
        return Ok(false);
    }
    let wanted = cfg
        .entities
        .iter()
        .filter(|e| only.is_empty() || only.contains(&e.name))
        .count();
    if plan
        .entities
        .iter()
        .filter(|p| only.is_empty() || only.contains(&p.name))
        .count()
        < wanted
    {
        h.log
            .line("some entities are not planned yet; rerun to plan and harvest them");
        return Ok(false);
    }
    if !only.is_empty() {
        return Ok(true);
    }
    h.changes(&plan)
}

pub fn fetch(cfg: &Config, entity: &str, uuids: &[String]) -> Result<Vec<(String, String)>> {
    let e = cfg
        .entity(entity)
        .with_context(|| format!("unknown entity {entity}"))?;
    let _lock = Lock::acquire(&cfg.source_dir())?;
    let gate = gate(cfg)?;
    gate.clear_stop();
    let h = harvester(cfg, &gate, paths(cfg)?)?;
    let targets: BTreeSet<&str> = uuids
        .iter()
        .map(|u| u.trim())
        .filter(|u| !u.is_empty())
        .collect();
    let targets: Vec<(String, Option<String>)> =
        targets.into_iter().map(|u| (u.to_owned(), None)).collect();
    let unit = fetch::unit(
        &e.name,
        format!("_fetch-{}", Utc::now().format("%Y%m%dT%H%M%S%3fZ")),
        targets.len(),
    );
    h.log.line(&format!(
        "fetch-begin entity={} unit={} targets={}",
        e.name,
        unit.id,
        targets.len()
    ));
    match h.refetch(e, &unit, "fetch", &targets) {
        Ok(refused) => Ok(refused),
        Err(Abort::Stop) => bail!("stopped"),
        Err(Abort::Fatal(m)) => {
            h.log.line(&format!(
                "fetch-failed entity={} unit={} {m}",
                e.name, unit.id
            ));
            bail!(m)
        }
    }
}

pub fn recover(
    cfg: &Config,
    tables: &Tables,
    entity: &str,
    out: Option<&Path>,
    rev: &str,
) -> Result<String> {
    let e = cfg
        .entity(entity)
        .with_context(|| format!("unknown entity {entity}"))?;
    anyhow::ensure!(
        !e.references.is_empty(),
        "no references configured for {entity}"
    );
    let paths = paths(cfg)?;
    let out = out.map_or_else(|| build::derived(&paths), Path::to_path_buf);
    let mut tried: BTreeSet<String> = BTreeSet::new();
    let mut report = String::new();
    for round in 1.. {
        let wanted: Vec<String> = build::absent(&out, tables, e)?
            .into_iter()
            .filter(|u| !tried.contains(u))
            .collect();
        let _ = writeln!(report, "round {round}: absent and untried {}", wanted.len());
        if wanted.is_empty() {
            break;
        }
        tried.extend(wanted.iter().cloned());
        let refused = fetch(cfg, entity, &wanted)?;
        let _ = writeln!(
            report,
            "  recovered {} refused {}",
            wanted.len() - refused.len(),
            refused.len()
        );
        for (u, why) in &refused {
            let _ = writeln!(report, "  refused {u} {why}");
        }
        report += &build::build(&paths, tables, &[entity.to_owned()], rev, Some(&out))?;
    }
    Ok(report)
}

pub fn handles(cfg: &Config) -> crate::store::Handles {
    crate::store::Handles {
        control: cfg.control_dir(),
        lock: Lock::path(&cfg.source_dir()),
    }
}
