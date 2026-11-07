use std::collections::BTreeMap;
use std::fs;

use anyhow::{anyhow, Context, Result};
use chrono::{TimeDelta, Utc};
use roxmltree::{Document, Node};
use serde::{Deserialize, Serialize};

use crate::fetch;
use crate::harvest::{Abort, Harvester, Step};
use crate::net::Admit;
use crate::soap;
use crate::state::{self, Plan};
use crate::store::{read_strings, Column, Table};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: String,
    pub since: String,
    pub started: String,
    pub token: Option<String>,
    pub events: u64,
    pub parts: u32,
    pub listed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChangesState {
    pub done_until: Option<String>,
    pub run: Option<Run>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub id: String,
    pub occurred: String,
    pub change_type: String,
    pub entity_type: String,
    pub identifier: String,
    pub provider: Option<String>,
}

fn text<'a>(node: Node<'a, 'a>, name: &str) -> Option<&'a str> {
    node.children()
        .find(|c| c.is_element() && c.tag_name().name() == name)
        .and_then(|c| c.text())
}

pub fn parse_changes(body: &str) -> std::result::Result<(Vec<Change>, Option<String>), String> {
    let doc = Document::parse(body).map_err(|e| e.to_string())?;
    if let Some(f) = doc
        .descendants()
        .find(|n| n.is_element() && n.tag_name().name() == "Fault")
    {
        return Err(format!("fault: {}", text(f, "faultstring").unwrap_or("")));
    }
    let resp = doc
        .descendants()
        .find(|n| n.is_element() && n.tag_name().name() == "changesResponse")
        .ok_or_else(|| "no changesResponse".to_owned())?;
    let token = resp.attribute("resumptionToken").map(str::to_owned);
    let changes = resp
        .children()
        .filter(|n| n.is_element() && n.tag_name().name() == "change")
        .map(|n| Change {
            id: n.attribute("id").unwrap_or_default().to_owned(),
            occurred: text(n, "occurredOn").unwrap_or_default().to_owned(),
            change_type: text(n, "changeType").unwrap_or_default().to_owned(),
            entity_type: text(n, "entityType").unwrap_or_default().to_owned(),
            identifier: text(n, "identifier").unwrap_or_default().to_owned(),
            provider: text(n, "dataProvider").map(str::to_owned),
        })
        .collect();
    Ok((changes, token))
}

fn criteria(since: &str, token: Option<&str>) -> String {
    let resume = token.map_or_else(String::new, |t| {
        format!("<c:resumeToken>{}</c:resumeToken>", soap::escape(t))
    });
    format!(
        "<changesCriteria>{resume}<c:from>{}</c:from></changesCriteria>",
        soap::escape(since)
    )
}

impl Harvester<'_> {
    fn list_page(&self, since: &str, token: Option<&str>) -> Step<(Vec<Change>, Option<String>)> {
        let url = self.cfg.url("ChangesService");
        let body = soap::envelope("getChanges", &criteria(since, token));
        let headers = [
            ("Content-Type", "text/xml; charset=utf-8"),
            ("SOAPAction", "\"\""),
        ];
        let mut attempt = 0u32;
        loop {
            if self.gate.admit(&url) == Admit::Stop {
                return Err(Abort::Stop);
            }
            match crate::net::post(self.gate, &self.shared, &url, body.clone(), &headers) {
                Ok(reply) => {
                    let (raw, lossy) = crate::xml::text::decode(reply.body);
                    let (text, cleaned) = crate::xml::text::clean(&raw);
                    if cleaned + lossy > 0 {
                        self.log.line(&format!(
                            "changes-cleaned attempt={attempt} cleaned={cleaned} lossy={lossy}"
                        ));
                    }
                    match parse_changes(&text) {
                        Ok(found) if reply.status == 200 => return Ok(found),
                        _ if reply.status >= 500 || reply.status == 429 => {}
                        Ok(_) | Err(_) if attempt >= self.cfg.fault_retries => {
                            let head: String = text.chars().take(300).collect();
                            return Err(Abort::Fatal(format!(
                                "changes listing failed: status {} {head}",
                                reply.status
                            )));
                        }
                        _ => {}
                    }
                }
                Err(err) => self
                    .log
                    .line(&format!("net-error changes attempt={attempt} {err:#}")),
            }
            self.backoff(attempt)?;
            attempt += 1;
        }
    }

    fn write_events(&self, run: &Run, changes: Vec<Change>) -> Result<()> {
        let n = changes.len();
        let mut cols: [Vec<Option<String>>; 6] = Default::default();
        for c in changes {
            cols[0].push(Some(c.id));
            cols[1].push(Some(c.occurred));
            cols[2].push(Some(c.change_type));
            cols[3].push(Some(c.entity_type));
            cols[4].push(Some(c.identifier));
            cols[5].push(c.provider);
        }
        let [id, occurred, change_type, entity_type, identifier, provider] = cols;
        let path = self
            .paths
            .raw()
            .join("_changes")
            .join(&run.id)
            .join(format!("events-{:05}.parquet", run.parts));
        Table::new()
            .col("id", Column::Str(id))
            .col("occurred_on", Column::Str(occurred))
            .col("change_type", Column::Str(change_type))
            .col("entity_type", Column::Str(entity_type))
            .col("identifier", Column::Str(identifier))
            .col("data_provider", Column::Str(provider))
            .col("run", Column::Str(vec![Some(run.id.clone()); n]))
            .write(&path)?;
        Ok(())
    }

    fn touched(&self, run: &Run) -> Result<BTreeMap<String, BTreeMap<String, String>>> {
        let dir = self.paths.raw().join("_changes").join(&run.id);
        let mut out: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        if !dir.exists() {
            return Ok(out);
        }
        for f in fs::read_dir(&dir)? {
            let f = f?.path();
            let cols = read_strings(&f, &["occurred_on", "entity_type", "identifier"])?;
            for ((occurred, etype), ident) in cols[0].iter().zip(&cols[1]).zip(&cols[2]) {
                let (Some(occurred), Some(etype), Some(ident)) = (occurred, etype, ident) else {
                    continue;
                };
                let slot = out
                    .entry(etype.clone())
                    .or_default()
                    .entry(ident.clone())
                    .or_default();
                if occurred.as_str() > slot.as_str() {
                    slot.clone_from(occurred);
                }
            }
        }
        Ok(out)
    }

    pub fn changes(&self, plan: &Plan) -> Result<bool> {
        let path = self.paths.changes();
        let mut cs: ChangesState = state::read_json(&path)?.unwrap_or_default();
        let mut run = if let Some(r) = cs.run.clone() {
            r
        } else {
            let base = cs
                .done_until
                .clone()
                .or_else(|| Some(plan.created.clone()))
                .and_then(|t| state::parse_ts(&t))
                .context("no start time for changes")?;
            let since = state::fmt_ts(base - TimeDelta::seconds(self.cfg.changes_margin_s));
            Run {
                id: Utc::now().format("%Y%m%dT%H%M%SZ").to_string(),
                since,
                started: state::now(),
                token: None,
                events: 0,
                parts: 0,
                listed: false,
            }
        };
        cs.run = Some(run.clone());
        state::write_json(&path, &cs)?;
        self.log
            .line(&format!("changes-begin run={} since={}", run.id, run.since));
        let lift = |s: Step<_>| {
            s.map_err(|a| match a {
                Abort::Stop => anyhow!("stopped"),
                Abort::Fatal(m) => anyhow!(m),
            })
        };
        while !run.listed {
            let (changes, token) = lift(self.list_page(&run.since, run.token.as_deref()))?;
            let n = changes.len();
            if n > 0 {
                self.write_events(&run, changes)?;
                run.parts += 1;
                run.events += n as u64;
            }
            if n == 0 || token.is_none() || token == run.token {
                run.listed = true;
            } else {
                run.token = token;
            }
            cs.run = Some(run.clone());
            state::write_json(&path, &cs)?;
            self.log.line(&format!(
                "changes-list run={} events={} token={:?}",
                run.id, run.events, run.token
            ));
        }
        let touched = self.touched(&run)?;
        let mut complete = true;
        for e in &self.cfg.entities {
            let mut targets: BTreeMap<String, String> = BTreeMap::new();
            for t in &e.changes {
                if let Some(ids) = touched.get(t) {
                    for (u, o) in ids {
                        let slot = targets.entry(u.clone()).or_default();
                        if o > slot {
                            slot.clone_from(o);
                        }
                    }
                }
            }
            if targets.is_empty() {
                continue;
            }
            let targets: Vec<(String, Option<String>)> =
                targets.into_iter().map(|(u, o)| (u, Some(o))).collect();
            let unit = fetch::unit(&e.name, format!("_changes-{}", run.id), targets.len());
            match self.refetch(e, &unit, "changes", &targets) {
                Ok(_) => {}
                Err(Abort::Stop) => return Ok(false),
                Err(Abort::Fatal(m)) => {
                    self.log
                        .line(&format!("changes-failed entity={} {m}", e.name));
                    complete = false;
                }
            }
        }
        if complete {
            cs.done_until = Some(run.started.clone());
            cs.run = None;
            state::write_json(&path, &cs)?;
            self.log
                .line(&format!("changes-end run={} events={}", run.id, run.events));
        }
        Ok(complete)
    }
}

#[cfg(test)]
mod tests {
    use super::parse_changes;

    #[test]
    fn parses_change_page() {
        let body = r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><fris:getChangesResponse xmlns:fris="http://fris.ewi.be/"><changesResponse resumptionToken="42"><fris:change id="7"><fris:occurredOn>2026-06-26T10:21:08.196Z</fris:occurredOn><fris:changeType>UPDATE</fris:changeType><fris:entityType>RESEARCH_OUTPUT</fris:entityType><fris:identifier>a33d</fris:identifier><fris:dataProvider>X</fris:dataProvider></fris:change></changesResponse></fris:getChangesResponse></soap:Body></soap:Envelope>"#;
        let (changes, token) = parse_changes(body).unwrap_or_default();
        assert_eq!(token.as_deref(), Some("42"));
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].identifier, "a33d");
        assert_eq!(changes[0].entity_type, "RESEARCH_OUTPUT");
        let empty = "<x><changesResponse/></x>";
        assert_eq!(parse_changes(empty), Ok((Vec::new(), None)));
    }
}
