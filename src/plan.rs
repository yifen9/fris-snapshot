use std::collections::HashSet;

use anyhow::{anyhow, Result};
use chrono::{DateTime, TimeDelta, Utc};

use crate::config::{Entity, Mode, Partition};
use crate::harvest::{Abort, Ctx, Harvester, Outcome, Step, RESIDUAL};
use crate::net::Admit;
use crate::soap::{self, Query};
use crate::state::{self, EntityPlan, Plan, Unit};
use crate::store::{Column, Table};

fn lift<T>(step: Step<T>) -> Result<T> {
    step.map_err(|a| match a {
        Abort::Stop => anyhow!("stopped"),
        Abort::Fatal(m) => anyhow!(m),
    })
}

type Span = (Option<String>, Option<String>, u64);

struct Range {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    count: u64,
}

fn add_residual(ep: &mut EntityPlan) -> bool {
    let Some(global) = ep.global else {
        return false;
    };
    if ep.providers.is_empty()
        || global <= ep.provider_sum
        || ep.units.iter().any(|u| u.id == RESIDUAL)
    {
        return false;
    }
    ep.units.push(Unit {
        entity: ep.name.clone(),
        id: RESIDUAL.into(),
        provider: None,
        start: None,
        end: None,
        expected: global,
        cap: 0,
    });
    true
}

impl Harvester<'_> {
    fn probe(
        &self,
        e: &Entity,
        provider: Option<&str>,
        order: Option<(&str, bool)>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Step<soap::Page> {
        let client = self.shared.clone();
        let q = Query {
            page_size: 1,
            order: order.map(|(o, _)| o),
            descending: order.is_some_and(|(_, d)| d),
            locale: &self.cfg.locale,
            provider,
            start,
            end,
            ..Query::default()
        };
        let ctx = Ctx {
            unit: "plan",
            mode: "probe",
            page: 0,
            page_size: 1,
        };
        let mut faults = 0u32;
        loop {
            match self.request(&client, e, &q.criteria(), &ctx)? {
                Outcome::Page(p, _) => return Ok(p),
                Outcome::Fault(f) => {
                    faults += 1;
                    if faults > self.cfg.fault_retries {
                        return Err(Abort::Fatal(f.to_string()));
                    }
                    self.backoff(faults)?;
                }
            }
        }
    }

    pub(crate) fn span_count(
        &self,
        e: &Entity,
        unit: &Unit,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Step<Option<u64>> {
        let order = Some((e.order.as_str(), false));
        match self.probe(e, unit.provider.as_deref(), order, start, end) {
            Ok(p) => Ok(p.total),
            Err(Abort::Fatal(why)) => {
                self.log.line(&format!(
                    "gap-count-failed entity={} unit={} {why}",
                    e.name, unit.id
                ));
                Ok(None)
            }
            Err(stop) => Err(stop),
        }
    }

    fn count(
        &self,
        e: &Entity,
        provider: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Step<u64> {
        let p = self.probe(e, provider, Some((&e.order, true)), start, end)?;
        p.total
            .ok_or_else(|| Abort::Fatal("probe without total".into()))
    }

    fn edge(
        &self,
        e: &Entity,
        provider: Option<&str>,
        descending: bool,
    ) -> Step<Option<DateTime<Utc>>> {
        let p = self.probe(e, provider, Some((&e.order, descending)), None, None)?;
        Ok(p.records
            .first()
            .and_then(|r| r.modified.as_deref())
            .and_then(state::parse_ts))
    }

    fn bisect(
        &self,
        e: &Entity,
        provider: &str,
        limit: u64,
        range: Range,
        out: &mut Vec<Range>,
    ) -> Step<()> {
        if range.count <= limit || range.end - range.start <= TimeDelta::milliseconds(1) {
            out.push(range);
            return Ok(());
        }
        let mid = range.start + (range.end - range.start) / 2;
        let right = mid + TimeDelta::milliseconds(1);
        let (lo_s, mid_s, right_s, hi_s) = (
            state::fmt_ts(range.start),
            state::fmt_ts(mid),
            state::fmt_ts(right),
            state::fmt_ts(range.end),
        );
        let counts = self
            .count(e, Some(provider), Some(&lo_s), Some(&mid_s))
            .and_then(|l| {
                Ok((
                    l,
                    self.count(e, Some(provider), Some(&right_s), Some(&hi_s))?,
                ))
            });
        let (left_n, right_n) = match counts {
            Ok(c) => c,
            Err(Abort::Stop) => return Err(Abort::Stop),
            Err(Abort::Fatal(m)) => {
                self.log.line(&format!(
                    "split-stop entity={} provider={provider} range={lo_s}..{hi_s} count={} {m}",
                    e.name, range.count
                ));
                out.push(range);
                return Ok(());
            }
        };
        if left_n > 0 {
            self.bisect(
                e,
                provider,
                limit,
                Range {
                    start: range.start,
                    end: mid,
                    count: left_n,
                },
                out,
            )?;
        }
        if right_n > 0 {
            self.bisect(
                e,
                provider,
                limit,
                Range {
                    start: right,
                    end: range.end,
                    count: right_n,
                },
                out,
            )?;
        }
        Ok(())
    }

    fn split(&self, e: &Entity, provider: &str, count: u64, limit: u64) -> Step<Vec<Span>> {
        let lo = self.edge(e, Some(provider), false)?;
        let hi = self.edge(e, Some(provider), true)?;
        let (Some(lo), Some(hi)) = (lo, hi) else {
            return Ok(vec![(None, None, count)]);
        };
        let mut ranges = Vec::new();
        self.bisect(
            e,
            provider,
            limit,
            Range {
                start: lo,
                end: hi,
                count,
            },
            &mut ranges,
        )?;
        let n = ranges.len();
        let mut out = Vec::with_capacity(n);
        let mut prev_end: Option<DateTime<Utc>> = None;
        for (i, r) in ranges.into_iter().enumerate() {
            let start = if i == 0 {
                None
            } else {
                prev_end.map(|t| state::fmt_ts(t + TimeDelta::milliseconds(1)))
            };
            let end = if i + 1 == n {
                None
            } else {
                Some(state::fmt_ts(r.end))
            };
            prev_end = Some(r.end);
            out.push((start, end, r.count));
        }
        Ok(out)
    }

    fn providers(&self, e: &Entity) -> Result<Vec<String>> {
        let client = self.shared.clone();
        let url = self.cfg.url(&e.service);
        if self.gate.admit(&url) == Admit::Stop {
            return Err(anyhow!("stopped"));
        }
        let headers = [
            ("Content-Type", "text/xml; charset=utf-8"),
            ("SOAPAction", "\"\""),
        ];
        let reply = crate::net::post(
            self.gate,
            &client,
            &url,
            soap::envelope("getDataProviders", ""),
            &headers,
        )?;
        let (text, _) = crate::xml::text::clean(&crate::xml::text::decode(reply.body).0);
        let mut names = soap::names(&text, "dataProviderName");
        names.sort();
        names.dedup();
        anyhow::ensure!(!names.is_empty(), "no data providers from {}", e.service);
        Ok(names)
    }

    fn plan_entity(&self, e: &Entity) -> Result<EntityPlan> {
        let global = lift(self.count(e, None, None, None))?;
        let mut plan = EntityPlan {
            name: e.name.clone(),
            global: Some(global),
            provider_sum: 0,
            providers: Vec::new(),
            units: Vec::new(),
        };
        if e.partition == Partition::None {
            plan.units.push(Unit {
                entity: e.name.clone(),
                id: "all".into(),
                provider: None,
                start: None,
                end: None,
                expected: global,
                cap: 0,
            });
            self.log
                .line(&format!("plan entity={} global={global} units=1", e.name));
            return Ok(plan);
        }
        let mut ids = HashSet::new();
        let mut counted = Vec::new();
        let mut uncounted = Vec::new();
        for p in self.providers(e)? {
            match self.count(e, Some(&p), None, None) {
                Ok(n) => counted.push((p, n)),
                Err(Abort::Stop) => return Err(anyhow!("stopped")),
                Err(Abort::Fatal(m)) => {
                    self.log
                        .line(&format!("count-failed entity={} provider={p} {m}", e.name));
                    uncounted.push(p);
                }
            }
        }
        let known: u64 = counted.iter().map(|(_, n)| n).sum();
        let share = global.saturating_sub(known) / (uncounted.len().max(1) as u64);
        counted.extend(uncounted.into_iter().map(|p| (p, share.max(1))));
        for (p, n) in counted {
            plan.providers.push((p.clone(), n));
            plan.provider_sum += n;
            if n == 0 {
                continue;
            }
            let ranges = match e.split_above {
                Some(limit) if n > limit && e.mode == Mode::Keyset => {
                    match self.split(e, &p, n, limit) {
                        Ok(r) => r,
                        Err(Abort::Stop) => return Err(anyhow!("stopped")),
                        Err(Abort::Fatal(m)) => {
                            self.log
                                .line(&format!("split-failed entity={} provider={p} {m}", e.name));
                            vec![(None, None, n)]
                        }
                    }
                }
                _ => vec![(None, None, n)],
            };
            let base = state::sanitize(&p);
            let multi = ranges.len() > 1;
            for (k, (start, end, expected)) in ranges.into_iter().enumerate() {
                let mut id = if multi {
                    format!("{base}.{k:02}")
                } else {
                    base.clone()
                };
                while !ids.insert(id.clone()) {
                    id.push('~');
                }
                plan.units.push(Unit {
                    entity: e.name.clone(),
                    id,
                    provider: Some(p.clone()),
                    start,
                    end,
                    expected,
                    cap: 0,
                });
            }
        }
        add_residual(&mut plan);
        let gap = global.abs_diff(plan.provider_sum);
        self.log.line(&format!(
            "plan entity={} global={global} provider_sum={} gap={gap} units={}",
            e.name,
            plan.provider_sum,
            plan.units.len()
        ));
        Ok(plan)
    }

    pub fn meta(&self) -> Result<()> {
        let mut services: Vec<String> = self.cfg.meta_services.clone();
        services.extend(self.cfg.entities.iter().map(|e| e.service.clone()));
        services.sort();
        services.dedup();
        let entity_ops: HashSet<(String, String)> = self
            .cfg
            .entities
            .iter()
            .map(|e| (e.service.clone(), e.op.clone()))
            .collect();
        let client = self.shared.clone();
        let headers = [
            ("Content-Type", "text/xml; charset=utf-8"),
            ("SOAPAction", "\"\""),
        ];
        let (mut kind, mut service, mut op, mut status, mut fetched, mut body) =
            (vec![], vec![], vec![], vec![], vec![], vec![]);
        for svc in &services {
            let url = format!("{}?wsdl", self.cfg.url(svc));
            if self.gate.admit(&url) == Admit::Stop {
                return Err(anyhow!("stopped"));
            }
            let wsdl = crate::net::get(self.gate, &client, &url, &[])?;
            let text = String::from_utf8_lossy(&wsdl.body).into_owned();
            let ops = soap::operations(&text);
            kind.push(Some("wsdl".to_owned()));
            service.push(Some(svc.clone()));
            op.push(None);
            status.push(u32::from(wsdl.status));
            fetched.push(Some(state::now()));
            body.push(Some(text));
            for o in ops {
                if entity_ops.contains(&(svc.clone(), o.clone())) || self.cfg.meta_skip.contains(&o)
                {
                    continue;
                }
                if self.gate.admit(&url) == Admit::Stop {
                    return Err(anyhow!("stopped"));
                }
                let reply = crate::net::post(
                    self.gate,
                    &client,
                    &self.cfg.url(svc),
                    soap::envelope(&o, ""),
                    &headers,
                )?;
                let text = String::from_utf8_lossy(&reply.body).into_owned();
                if let Some(f) = soap::fault_of(&text) {
                    self.log
                        .line(&format!("meta-fault service={svc} op={o} {f}"));
                }
                kind.push(Some("call".to_owned()));
                service.push(Some(svc.clone()));
                op.push(Some(o));
                status.push(u32::from(reply.status));
                fetched.push(Some(state::now()));
                body.push(Some(text));
            }
        }
        let n = Table::new()
            .col("kind", Column::Str(kind))
            .col("service", Column::Str(service))
            .col("op", Column::Str(op))
            .col("status", Column::U32(status))
            .col("fetched_at", Column::Str(fetched))
            .col("body", Column::Str(body))
            .plain("body")
            .write(&self.paths.meta("services"))?;
        self.log.line(&format!("meta rows={n}"));
        Ok(())
    }

    pub fn plan(&self, only: &[String]) -> Result<Plan> {
        let existing: Option<Plan> = state::read_json(&self.paths.plan())?;
        let mut plan = existing.unwrap_or_else(|| Plan {
            snapshot: self.paths.snapshot.clone(),
            created: state::now(),
            entities: Vec::new(),
        });
        if !self.paths.meta("services").exists() {
            self.meta()?;
        }
        for e in &self.cfg.entities {
            if !only.is_empty() && !only.contains(&e.name) {
                continue;
            }
            if let Some(ep) = plan.entities.iter_mut().find(|p| p.name == e.name) {
                if add_residual(ep) {
                    self.log.line(&format!(
                        "residual entity={} global={:?} provider_sum={}",
                        ep.name, ep.global, ep.provider_sum
                    ));
                }
                continue;
            }
            match self.plan_entity(e) {
                Ok(ep) => {
                    plan.entities.push(ep);
                    state::write_json(&self.paths.plan(), &plan)?;
                }
                Err(err) if self.gate.stopping() => return Err(err),
                Err(err) => self
                    .log
                    .line(&format!("plan-failed entity={} {err:#}", e.name)),
            }
        }
        state::write_json(&self.paths.plan(), &plan)?;
        Ok(plan)
    }
}
