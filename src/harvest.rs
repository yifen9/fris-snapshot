use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use reqwest::blocking::Client;
use serde_json::json;

use crate::config::{Config, Entity, Mode};
use crate::net::{Admit, Gate};
use crate::soap::{self, Fault, Page, Query, Rec, Reply};
use crate::state::{self, Gap, Log, Paths, Plan, Unit, UnitState};
use crate::store::{Column, Table};

pub enum Abort {
    Stop,
    Fatal(String),
}

impl From<anyhow::Error> for Abort {
    fn from(e: anyhow::Error) -> Self {
        Self::Fatal(format!("{e:#}"))
    }
}

pub type Step<T> = std::result::Result<T, Abort>;

pub const RESIDUAL: &str = "_residual";
const BACKOFF_S: u64 = 10;

pub enum Outcome {
    Page(Page, Duration),
    Fault(Fault),
}

#[derive(Debug)]
pub(crate) enum Class {
    Page(Page),
    Fault(Fault),
    Outage(String),
    Bad(String),
}

pub(crate) fn classify(status: u16, reply: Reply) -> Class {
    match reply {
        Reply::Fault(f) => Class::Fault(f),
        Reply::Page(p) if status == 200 => Class::Page(p),
        Reply::Page(_) | Reply::Malformed(_) if status >= 500 || status == 429 => {
            Class::Outage(format!("http {status}"))
        }
        Reply::Malformed(m) => Class::Bad(m),
        Reply::Page(_) => Class::Bad(format!("http {status}")),
    }
}

pub struct Ctx<'a> {
    pub unit: &'a str,
    pub mode: &'a str,
    pub page: usize,
    pub page_size: usize,
}

#[derive(Default)]
pub(crate) struct Buffer {
    uuid: Vec<Option<String>>,
    kind: Vec<Option<String>>,
    provider: Vec<Option<String>>,
    modified: Vec<Option<String>>,
    page: Vec<u32>,
    fetched: Vec<Option<String>>,
    xml: Vec<Option<String>>,
    bytes: usize,
    pub(crate) pages: usize,
    committed: UnitState,
}

impl Buffer {
    pub(crate) fn new(st: &UnitState) -> Self {
        Self {
            committed: st.clone(),
            ..Self::default()
        }
    }

    pub(crate) fn push_gone(&mut self, uuid: &str, occurred: &str, page: u64, fetched: &str) {
        self.uuid.push(Some(uuid.to_owned()));
        self.kind.push(Some("_gone".to_owned()));
        self.provider.push(None);
        self.modified.push(Some(occurred.to_owned()));
        self.page.push(u32::try_from(page).unwrap_or(u32::MAX));
        self.fetched.push(Some(fetched.to_owned()));
        self.xml.push(None);
    }

    pub(crate) fn push(&mut self, r: Rec, page: u64, fetched: &str) {
        self.bytes += r.xml.len();
        self.uuid.push(r.uuid);
        self.kind.push(Some(r.kind));
        self.provider.push(r.provider);
        self.modified.push(r.modified);
        self.page.push(u32::try_from(page).unwrap_or(u32::MAX));
        self.fetched.push(Some(fetched.to_owned()));
        self.xml.push(Some(r.xml));
    }

    fn is_empty(&self) -> bool {
        self.uuid.is_empty()
    }

    pub(crate) fn full(&self, cfg: &Config) -> bool {
        self.pages >= cfg.part_pages || self.bytes >= cfg.part_bytes
    }

    fn take(&mut self, unit: &str) -> Table {
        let n = self.uuid.len();
        self.bytes = 0;
        Table::new()
            .col("uuid", Column::Str(std::mem::take(&mut self.uuid)))
            .col("kind", Column::Str(std::mem::take(&mut self.kind)))
            .col(
                "data_provider",
                Column::Str(std::mem::take(&mut self.provider)),
            )
            .col(
                "last_modified",
                Column::Str(std::mem::take(&mut self.modified)),
            )
            .col("unit", Column::Str(vec![Some(unit.to_owned()); n]))
            .col("page", Column::U32(std::mem::take(&mut self.page)))
            .col("fetched_at", Column::Str(std::mem::take(&mut self.fetched)))
            .col("xml", Column::Str(std::mem::take(&mut self.xml)))
            .plain("xml")
            .plain("uuid")
    }
}

pub struct Harvester<'a> {
    pub cfg: &'a Config,
    pub gate: &'a Gate,
    pub paths: Paths,
    pub log: Log,
    pub timeout: Duration,
    pub shared: Client,
}

impl Harvester<'_> {
    pub fn client(&self, cookies: bool) -> Step<Client> {
        Ok(crate::net::client(cookies, self.timeout)?)
    }

    pub(crate) fn backoff(&self, attempt: u32) -> Step<()> {
        if self.gate.pause(crate::net::backoff(
            BACKOFF_S,
            self.cfg.backoff_cap_s,
            attempt,
        )) == Admit::Stop
        {
            return Err(Abort::Stop);
        }
        Ok(())
    }

    pub fn request(&self, client: &Client, e: &Entity, criteria: &str, ctx: &Ctx) -> Step<Outcome> {
        let url = self.cfg.url(&e.service);
        let body = soap::envelope(&e.op, criteria);
        let headers = [
            ("Content-Type", "text/xml; charset=utf-8"),
            ("SOAPAction", "\"\""),
        ];
        let mut attempt = 0u32;
        let mut malformed = 0u32;
        loop {
            if self.gate.admit(&url) == Admit::Stop {
                return Err(Abort::Stop);
            }
            let result = crate::net::post(self.gate, client, &url, body.clone(), &headers);
            let reply = match result {
                Ok(r) => r,
                Err(err) => {
                    self.log.line(&format!(
                        "net-error entity={} unit={} attempt={attempt} {err:#}",
                        e.name, ctx.unit
                    ));
                    self.backoff(attempt)?;
                    attempt += 1;
                    continue;
                }
            };
            let bytes = reply.body.len();
            let (raw, lossy) = crate::xml::text::decode(reply.body);
            let (text, cleaned) = crate::xml::text::clean(&raw);
            let parsed = soap::parse(&text);
            let (kind, n, total) = match &parsed {
                Reply::Page(p) => ("page", p.records.len(), p.total),
                Reply::Fault(_) => ("fault", 0, None),
                Reply::Malformed(_) => ("malformed", 0, None),
            };
            self.log.record(&json!({
                "t": state::now(), "entity": e.name, "unit": ctx.unit, "mode": ctx.mode, "page": ctx.page,
                "ps": ctx.page_size, "status": reply.status, "kind": kind, "n": n, "total": total,
                "bytes": bytes, "ttfb": reply.ttfb.as_millis(), "ms": reply.elapsed.as_millis(),
                "cleaned": cleaned, "lossy": lossy,
            }));
            match classify(reply.status, parsed) {
                Class::Page(p) => return Ok(Outcome::Page(p, reply.ttfb)),
                Class::Outage(why) => {
                    self.log.line(&format!(
                        "server-unavailable entity={} unit={} status={} attempt={attempt} {why}",
                        e.name, ctx.unit, reply.status
                    ));
                    self.backoff(attempt)?;
                    attempt += 1;
                }
                Class::Bad(why) => {
                    malformed += 1;
                    let dump = self.paths.root.join("log").join(format!(
                        "bad-{}-{}-{}.xml",
                        e.name,
                        state::sanitize(ctx.unit),
                        malformed.min(self.cfg.fault_retries + 1)
                    ));
                    let _ = std::fs::write(&dump, text.as_bytes());
                    self.log.line(&format!(
                        "bad-reply entity={} unit={} status={} n={malformed} {why} dump={}",
                        e.name,
                        ctx.unit,
                        reply.status,
                        dump.display()
                    ));
                    if malformed > self.cfg.fault_retries {
                        self.log.line(&format!(
                            "server-unavailable entity={} unit={} status={} attempt={attempt} persistent bad reply: {why}",
                            e.name, ctx.unit, reply.status
                        ));
                        self.backoff(attempt)?;
                        attempt += 1;
                    } else {
                        self.backoff(malformed)?;
                    }
                }
                Class::Fault(f) => {
                    self.log.line(&format!(
                        "fault entity={} unit={} page_size={} ttfb_ms={} {f}",
                        e.name,
                        ctx.unit,
                        ctx.page_size,
                        reply.ttfb.as_millis()
                    ));
                    return Ok(Outcome::Fault(f));
                }
            }
        }
    }

    pub(crate) fn flush(&self, unit: &Unit, st: &mut UnitState, buf: &mut Buffer) -> Step<()> {
        if !buf.is_empty() {
            let bytes = buf.bytes as u64;
            let path = self.paths.part(unit, st.parts);
            if let Err(err) = buf.take(&unit.id).write(&path) {
                *st = buf.committed.clone();
                return Err(err.into());
            }
            st.parts += 1;
            st.bytes += bytes;
        }
        buf.pages = 0;
        st.updated = Some(state::now());
        state::write_json(&self.paths.unit_state(unit), st)?;
        buf.committed = st.clone();
        Ok(())
    }

    pub(crate) fn check_total(unit: &Unit, total: Option<u64>) -> Step<()> {
        let bound = unit.expected.max(unit.cap).saturating_mul(11) / 10 + 1000;
        match total {
            Some(t) if t > bound => Err(Abort::Fatal(format!(
                "total {t} exceeds bound {bound}: criteria ignored?"
            ))),
            _ => Ok(()),
        }
    }

    fn resize(&self, e: &Entity, st: &mut UnitState, unit: &Unit, size: usize, why: &str) {
        let size = size.clamp(self.cfg.min_page_size, e.page_size);
        if size != st.page_size {
            self.log.line(&format!(
                "resize entity={} unit={} page_size={}->{size} {why}",
                e.name, unit.id, st.page_size
            ));
            st.page_size = size;
        }
    }

    fn foreign(&self, e: &Entity, unit: &Unit, page: &Page, faults: &mut u32) -> Step<bool> {
        let Some(want) = unit.provider.as_deref() else {
            return Ok(false);
        };
        let Some(other) = page
            .records
            .iter()
            .filter_map(|r| r.provider.as_deref())
            .find(|p| *p != want)
        else {
            return Ok(false);
        };
        *faults += 1;
        self.log.line(&format!(
            "provider-mismatch entity={} unit={} want={want} got={other} n={faults}",
            e.name, unit.id
        ));
        if *faults > self.cfg.fault_retries {
            return Err(Abort::Fatal(format!(
                "server returned records of {other} for {want}"
            )));
        }
        self.backoff(*faults)?;
        Ok(true)
    }

    pub(crate) fn on_fault(
        &self,
        e: &Entity,
        st: &mut UnitState,
        unit: &Unit,
        faults: &mut u32,
        fault: &Fault,
    ) -> Step<()> {
        if st.page_size > self.cfg.min_page_size {
            self.resize(e, st, unit, st.page_size / 2, "after fault");
            return Ok(());
        }
        *faults += 1;
        if *faults > self.cfg.fault_retries {
            return Err(Abort::Fatal(format!("fault at minimum page size: {fault}")));
        }
        self.backoff(*faults)
    }

    fn adapt(&self, e: &Entity, st: &mut UnitState, unit: &Unit, streak: &mut u32, ttfb: Duration) {
        let ms = ttfb.as_millis() as u64;
        if ms > self.cfg.slow_ms {
            *streak = 0;
            self.resize(e, st, unit, st.page_size / 2, "slow");
            return;
        }
        if ms < self.cfg.fast_ms {
            *streak += 1;
        } else {
            *streak = 0;
        }
        if *streak >= self.cfg.grow_after && st.page_size < e.page_size {
            *streak = 0;
            self.resize(e, st, unit, st.page_size * 2, "fast");
        }
    }

    fn walk(
        &self,
        e: &Entity,
        unit: &Unit,
        st: &mut UnitState,
        buf: &mut Buffer,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Step<()> {
        let seen: HashSet<String> = st.seen.iter().chain(&st.salvage_seen).cloned().collect();
        let mut verifications = 0;
        let mut faults = 0u32;
        let mut collected: HashSet<String> = HashSet::new();
        'attempt: loop {
            let client = self.client(true)?;
            let size = st.page_size;
            let mut latest = None;
            let mut page = 0usize;
            loop {
                let q = Query {
                    page_size: size,
                    page,
                    order: Some(&e.order),
                    locale: &self.cfg.locale,
                    provider: unit.provider.as_deref(),
                    start,
                    end,
                    ..Query::default()
                };
                let ctx = Ctx {
                    unit: &unit.id,
                    mode: "walk",
                    page,
                    page_size: size,
                };
                let p = match self.request(&client, e, &q.criteria(), &ctx)? {
                    Outcome::Page(p, _) => p,
                    Outcome::Fault(m) => {
                        self.on_fault(e, st, unit, &mut faults, &m)?;
                        continue 'attempt;
                    }
                };
                Self::check_total(unit, p.total)?;
                if self.foreign(e, unit, &p, &mut faults)? {
                    continue 'attempt;
                }
                latest = p.total.or(latest);
                let n = p.records.len();
                let fetched = state::now();
                for r in p.records {
                    let fresh = r
                        .uuid
                        .as_ref()
                        .is_none_or(|u| collected.insert(u.clone()) && !seen.contains(u));
                    if fresh {
                        st.records += 1;
                        buf.push(r, st.pages, &fetched);
                    }
                }
                st.pages += 1;
                buf.pages += 1;
                page += 1;
                if buf.full(self.cfg) {
                    self.flush(unit, st, buf)?;
                }
                let needed = latest.map_or(0, |t: u64| t.div_ceil(size as u64)) as usize;
                if n == 0 || page >= needed {
                    break;
                }
            }
            let expected = latest.unwrap_or(0);
            let got = collected.len() as u64;
            verifications += 1;
            if got == expected || verifications >= 3 {
                if got != expected {
                    let gap = Gap {
                        from: start.unwrap_or_default().to_owned(),
                        to: end.map_or_else(state::now, str::to_owned),
                        kind: Some("walk-mismatch".into()),
                        reason: Some(format!("got {got} of total {expected}")),
                        records: Some(expected.saturating_sub(got)),
                    };
                    self.log.line(&format!(
                        "walk-mismatch entity={} unit={} {gap}",
                        e.name, unit.id
                    ));
                    st.gaps.push(gap);
                }
                st.ties += 1;
                return Ok(());
            }
            self.log.line(&format!(
                "walk-retry entity={} unit={} got={got} total={expected}",
                e.name, unit.id
            ));
        }
    }

    fn salvage(
        &self,
        e: &Entity,
        unit: &Unit,
        st: &mut UnitState,
        buf: &mut Buffer,
        why: &str,
    ) -> Step<()> {
        let lo = st.cursor.clone().or_else(|| unit.start.clone());
        if st.salvage_cursor.is_none() {
            st.salvage_cursor = Some(unit.end.clone().unwrap_or_else(state::now));
            st.page_size = e.page_size;
        }
        let mut streak = 0;
        let mut faults = 0;
        self.log.line(&format!(
            "salvage-begin entity={} unit={} from={lo:?} to={:?} after {why}",
            e.name, unit.id, st.salvage_cursor
        ));
        loop {
            let hi = st.salvage_cursor.clone();
            if let (Some(a), Some(b)) = (
                lo.as_deref().and_then(state::parse_ts),
                hi.as_deref().and_then(state::parse_ts),
            ) {
                if b < a {
                    return Ok(());
                }
            }
            let size = st.page_size;
            let query = Query {
                page_size: size,
                order: Some(&e.order),
                descending: true,
                locale: &self.cfg.locale,
                provider: unit.provider.as_deref(),
                start: lo.as_deref(),
                end: hi.as_deref(),
                ..Query::default()
            };
            let ctx = Ctx {
                unit: &unit.id,
                mode: "salvage",
                page: st.pages as usize,
                page_size: size,
            };
            let (page, ttfb) = match self.request(&self.shared, e, &query.criteria(), &ctx)? {
                Outcome::Page(p, ttfb) => (p, ttfb),
                Outcome::Fault(_) if size > self.cfg.min_page_size => {
                    streak = 0;
                    self.resize(e, st, unit, size / 2, "salvage fault");
                    continue;
                }
                Outcome::Fault(f) => {
                    let records = self.span_count(e, unit, lo.as_deref(), hi.as_deref())?;
                    let gap = Gap {
                        from: lo.clone().unwrap_or_default(),
                        to: hi.clone().unwrap_or_else(state::now),
                        kind: Some(f.code),
                        reason: Some(f.reason),
                        records,
                    };
                    self.log.line(&format!(
                        "poison-gap entity={} unit={} from={} to={} kind={:?} reason={:?} records={records:?}",
                        e.name, unit.id, gap.from, gap.to, gap.kind, gap.reason
                    ));
                    st.gaps.push(gap);
                    return Ok(());
                }
            };
            Self::check_total(unit, page.total)?;
            if self.foreign(e, unit, &page, &mut faults)? {
                continue;
            }
            if self.salvage_page(e, unit, st, buf, page)? {
                return Ok(());
            }
            self.adapt(e, st, unit, &mut streak, ttfb);
        }
    }

    fn salvage_page(
        &self,
        e: &Entity,
        unit: &Unit,
        st: &mut UnitState,
        buf: &mut Buffer,
        mut page: Page,
    ) -> Step<bool> {
        let count = page.records.len();
        if count == 0 {
            return Ok(true);
        }
        page.records.reverse();
        let stamps = Self::stamps(&page, st.cursor.as_deref().or(unit.start.as_deref()))?;
        let (oldest, newest) = (stamps[0], stamps[count - 1]);
        if st
            .salvage_cursor
            .as_deref()
            .and_then(state::parse_ts)
            .is_some_and(|h| newest > h)
        {
            return Err(Abort::Fatal("record after salvage cursor".into()));
        }
        if count == st.page_size && oldest == newest {
            let at = state::fmt_ts(oldest);
            self.walk(e, unit, st, buf, Some(&at), Some(&at))?;
            st.salvage_cursor = Some(state::fmt_ts(oldest - chrono::TimeDelta::milliseconds(1)));
            st.salvage_seen.clear();
            self.flush(unit, st, buf)?;
            return Ok(false);
        }
        let seen: HashSet<&str> = st
            .seen
            .iter()
            .chain(&st.salvage_seen)
            .map(String::as_str)
            .collect();
        let mut next_seen = Vec::new();
        let fetched = state::now();
        for (r, t) in page.records.into_iter().zip(stamps) {
            if t == oldest {
                if let Some(u) = &r.uuid {
                    next_seen.push(u.clone());
                }
            }
            if r.uuid.as_deref().is_none_or(|u| !seen.contains(u)) {
                st.records += 1;
                buf.push(r, st.pages, &fetched);
            }
        }
        st.pages += 1;
        buf.pages += 1;
        st.salvage_cursor = Some(state::fmt_ts(oldest));
        st.salvage_seen = next_seen;
        if buf.full(self.cfg) {
            self.flush(unit, st, buf)?;
        }
        Ok(count < st.page_size)
    }

    fn stamps(page: &Page, cursor: Option<&str>) -> Step<Vec<chrono::DateTime<chrono::Utc>>> {
        let stamps: Option<Vec<_>> = page
            .records
            .iter()
            .map(|r| r.modified.as_deref().and_then(state::parse_ts))
            .collect();
        let Some(stamps) = stamps else {
            return Err(Abort::Fatal("record without parseable lastModified".into()));
        };
        if stamps.windows(2).any(|w| w[0] > w[1]) {
            return Err(Abort::Fatal("page not ordered by lastModified".into()));
        }
        if let (Some(c), Some(first)) = (cursor.and_then(state::parse_ts), stamps.first()) {
            if *first < c {
                return Err(Abort::Fatal(format!("record before cursor {c}")));
            }
        }
        Ok(stamps)
    }

    fn keyset(&self, e: &Entity, unit: &Unit, st: &mut UnitState, buf: &mut Buffer) -> Step<()> {
        if st.salvage_cursor.is_some() {
            return self.salvage(e, unit, st, buf, "resume");
        }
        let client = self.shared.clone();
        let mut faults = 0u32;
        let mut streak = 0u32;
        loop {
            let cursor = st.cursor.clone().or_else(|| unit.start.clone());
            let size = st.page_size;
            let query = Query {
                page_size: size,
                order: Some(&e.order),
                locale: &self.cfg.locale,
                provider: unit.provider.as_deref(),
                start: cursor.as_deref(),
                end: unit.end.as_deref(),
                ..Query::default()
            };
            let ctx = Ctx {
                unit: &unit.id,
                mode: "keyset",
                page: st.pages as usize,
                page_size: size,
            };
            let (reply, ttfb) = match self.request(&client, e, &query.criteria(), &ctx)? {
                Outcome::Page(found, ttfb) => {
                    faults = 0;
                    (found, ttfb)
                }
                Outcome::Fault(m) => {
                    streak = 0;
                    match self.on_fault(e, st, unit, &mut faults, &m) {
                        Ok(()) => continue,
                        Err(Abort::Fatal(why)) if st.page_size <= self.cfg.min_page_size => {
                            return self.salvage(e, unit, st, buf, &why);
                        }
                        Err(a) => return Err(a),
                    }
                }
            };
            Self::check_total(unit, reply.total)?;
            if self.foreign(e, unit, &reply, &mut faults)? {
                continue;
            }
            st.last_total = reply.total;
            let count = reply.records.len();
            if count == 0 {
                return Ok(());
            }
            let stamps = Self::stamps(&reply, cursor.as_deref())?;
            let (first, last) = (stamps[0], stamps[count - 1]);
            if count == size && first == last {
                let t = state::fmt_ts(first);
                self.log.line(&format!(
                    "tie entity={} unit={} at={t} page_size={size}",
                    e.name, unit.id
                ));
                self.walk(e, unit, st, buf, Some(&t), Some(&t))?;
                st.cursor = state::plus_ms(&t);
                st.seen.clear();
                if buf.full(self.cfg) {
                    self.flush(unit, st, buf)?;
                }
                continue;
            }
            let seen: HashSet<&str> = st.seen.iter().map(String::as_str).collect();
            let mut next_seen = Vec::new();
            let fetched = state::now();
            for (r, t) in reply.records.into_iter().zip(&stamps) {
                if *t == last {
                    if let Some(u) = &r.uuid {
                        next_seen.push(u.clone());
                    }
                }
                if r.uuid.as_deref().is_none_or(|u| !seen.contains(u)) {
                    st.records += 1;
                    buf.push(r, st.pages, &fetched);
                }
            }
            st.seen = next_seen;
            st.cursor = Some(state::fmt_ts(last));
            st.pages += 1;
            buf.pages += 1;
            if buf.full(self.cfg) {
                self.flush(unit, st, buf)?;
            }
            if count < size {
                return Ok(());
            }
            self.adapt(e, st, unit, &mut streak, ttfb);
        }
    }

    fn run_unit(&self, unit: &Unit) {
        let Some(e) = self.cfg.entity(&unit.entity) else {
            self.log.line(&format!("unknown entity {}", unit.entity));
            return;
        };
        let path = self.paths.unit_state(unit);
        let mut st: UnitState = match state::read_json(&path) {
            Ok(s) => s.unwrap_or_default(),
            Err(err) => {
                self.log
                    .line(&format!("state-error unit={} {err:#}", unit.id));
                return;
            }
        };
        if st.done {
            return;
        }
        if st.page_size == 0 {
            st.page_size = e.page_size;
        }
        st.failed = None;
        st.started.get_or_insert_with(state::now);
        self.log.line(&format!(
            "start entity={} unit={} expected={} resume_pages={} cursor={:?}",
            e.name, unit.id, unit.expected, st.pages, st.cursor
        ));
        let mut buf = Buffer::new(&st);
        let result = match e.mode {
            Mode::Keyset => self.keyset(e, unit, &mut st, &mut buf),
            Mode::Session => {
                st.seen.clear();
                self.walk(
                    e,
                    unit,
                    &mut st,
                    &mut buf,
                    unit.start.as_deref(),
                    unit.end.as_deref(),
                )
            }
        };
        let flushed = self.flush(unit, &mut st, &mut buf);
        let outcome = match (result, flushed) {
            (Ok(()), Ok(())) => {
                st.done = true;
                st.finished = Some(state::now());
                format!(
                    "done records={} pages={} parts={}",
                    st.records, st.pages, st.parts
                )
            }
            (Err(Abort::Stop), _) | (_, Err(Abort::Stop)) => "stopped".to_owned(),
            (Err(Abort::Fatal(m)), _) | (_, Err(Abort::Fatal(m))) => {
                st.failed = Some(m.clone());
                format!("failed {m}")
            }
        };
        if let Err(err) = state::write_json(&path, &st) {
            self.log
                .line(&format!("state-write-error unit={} {err:#}", unit.id));
        }
        self.log
            .line(&format!("{outcome} entity={} unit={}", e.name, unit.id));
    }

    pub fn harvest(&self, plan: &Plan, only: &[String]) -> Result<(usize, usize)> {
        let mut units: Vec<Unit> = plan
            .entities
            .iter()
            .filter(|p| only.is_empty() || only.contains(&p.name))
            .flat_map(|p| p.units.iter().cloned())
            .collect();
        for u in &mut units {
            u.cap = plan.cap(u);
        }
        let priority = |u: &Unit| self.cfg.entity(&u.entity).map_or(u32::MAX, |e| e.priority);
        units.sort_by_key(|u| {
            (
                u.provider.is_none() && u.id == RESIDUAL,
                priority(u),
                std::cmp::Reverse(u.expected),
            )
        });
        let total = units.len();
        let all = units.clone();
        let queue = Mutex::new(units.into_iter().collect::<VecDeque<_>>());
        std::thread::scope(|s| {
            for _ in 0..self.cfg.concurrency {
                s.spawn(|| loop {
                    if self.gate.stopping() {
                        break;
                    }
                    let next = queue.lock().ok().and_then(|mut q| q.pop_front());
                    let Some(unit) = next else { break };
                    self.run_unit(&unit);
                });
            }
        });
        let mut done = 0;
        for u in &all {
            let st: Option<UnitState> = state::read_json(&self.paths.unit_state(u))?;
            if st.is_some_and(|s| s.done) {
                done += 1;
            }
        }
        Ok((done, total))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{classify, Class, Harvester};
    use crate::config::Config;
    use crate::soap::parse;
    use crate::state::{self, Paths, Unit, UnitState};

    const PAGE: &str = r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><r total="1"><fris:x xmlns:fris="http://fris.ewi.be/" uuid="a"/></r></soap:Body></soap:Envelope>"#;
    const FAULT: &str = r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><soap:Fault><faultcode>soap:Server</faultcode><faultstring>Fault occurred while processing.</faultstring></soap:Fault></soap:Body></soap:Envelope>"#;
    const HTML: &str = "<html><head><title>Request Rejected</title></head><body>The requested URL was rejected.</body></html>";
    const WAF: &str = "<!DOCTYPE html><html><body><p>Access denied</body></html>";

    fn class(status: u16, text: &str) -> Class {
        classify(status, parse(text))
    }

    #[test]
    fn only_soap_faults_are_faults() {
        assert!(matches!(class(200, PAGE), Class::Page(p) if p.records.len() == 1));
        assert!(
            matches!(class(500, FAULT), Class::Fault(f) if f.code == "soap:Server" && f.reason.starts_with("Fault occurred"))
        );
        assert!(matches!(class(200, FAULT), Class::Fault(_)));
        assert!(matches!(class(503, FAULT), Class::Fault(_)));
        assert!(matches!(class(503, HTML), Class::Outage(_)));
        assert!(matches!(class(502, ""), Class::Outage(_)));
        assert!(matches!(class(429, PAGE), Class::Outage(_)));
        assert!(matches!(class(200, HTML), Class::Bad(_)));
        assert!(matches!(class(200, WAF), Class::Bad(_)));
        assert!(matches!(class(403, WAF), Class::Bad(_)));
        assert!(matches!(class(404, PAGE), Class::Bad(w) if w == "http 404"));
        assert!(matches!(
            class(200, "<Fault><faultstring>x</faultstring></Fault>"),
            Class::Bad(_)
        ));
    }

    pub(crate) struct SoapMock {
        pub(crate) address: std::net::SocketAddr,
        pub(crate) server: std::thread::JoinHandle<anyhow::Result<Vec<String>>>,
    }

    pub(crate) fn soap_server(replies: Vec<String>) -> anyhow::Result<SoapMock> {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::time::{Duration, Instant};

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let server = std::thread::spawn(move || -> anyhow::Result<Vec<String>> {
            let mut requests = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(10);
            while requests.len() < replies.len() && Instant::now() < deadline {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                };
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                let mut raw = Vec::new();
                let mut chunk = [0; 4096];
                let body = loop {
                    let n = stream.read(&mut chunk)?;
                    anyhow::ensure!(n > 0, "incomplete request");
                    raw.extend_from_slice(&chunk[..n]);
                    if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&raw[..end]);
                        let length: usize = header
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse().ok())
                                    .flatten()
                            })
                            .unwrap_or(0);
                        if raw.len() >= end + 4 + length {
                            break String::from_utf8(raw[end + 4..].to_vec())?;
                        }
                    }
                };
                let reply = &replies[requests.len()];
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                    reply.len()
                )?;
                requests.push(body);
            }
            Ok(requests)
        });
        Ok(SoapMock { address, server })
    }

    #[test]
    fn salvage_resumes_in_batches_without_losing_boundary_ties() -> anyhow::Result<()> {
        use std::fmt::Write;
        use std::time::Duration;

        let stamp = |day: u32| format!("2024-01-{day:02}T00:00:00.000Z");
        let batches = [
            vec![("f", 5), ("e", 4), ("d", 3)],
            vec![("d", 3), ("c", 3), ("b", 3)],
            vec![("b", 3), ("c", 3), ("d", 3)],
            vec![("a", 1)],
        ];
        let replies: Vec<String> = batches.iter().map(|batch| {
            let mut records = String::new();
            for (id, day) in batch {
                let _ = write!(records, "<fris:person xmlns:fris=\"http://fris.ewi.be/\" uuid=\"{id}\"><fris:lastModified>{}</fris:lastModified></fris:person>", stamp(*day));
            }
            format!("<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"><soap:Body><r total=\"{}\">{records}</r></soap:Body></soap:Envelope>", batch.len())
        }).collect();
        let mock = soap_server(replies)?;
        let mut cfg: Config = toml::from_str(include_str!("../config/fris.toml"))?;
        cfg.root = std::env::temp_dir().join(format!("fris-salvage-{}", std::process::id()));
        cfg.base = format!("http://{}/", mock.address);
        cfg.part_pages = 1;
        cfg.slow_ms = u64::MAX;
        cfg.fast_ms = 0;
        cfg.entities.retain(|e| e.name == "person");
        cfg.entities[0].page_size = 3;
        let paths = Paths {
            root: cfg.root.clone(),
            snapshot: "test".into(),
        };
        let unit = Unit {
            entity: "person".into(),
            id: "test".into(),
            provider: None,
            start: None,
            end: None,
            expected: 7,
            cap: 7,
        };
        let seed = UnitState {
            cursor: Some(stamp(1)),
            seen: vec!["a".into()],
            salvage_cursor: Some(stamp(5)),
            salvage_seen: vec!["f".into()],
            page_size: 3,
            records: 3,
            ..UnitState::default()
        };
        state::write_json(&paths.unit_state(&unit), &seed)?;
        let gate = crate::net::Gate::new(&cfg.control_dir(), 0, Duration::ZERO)?;
        let h = Harvester {
            cfg: &cfg,
            gate: &gate,
            log: paths.open_log()?,
            paths,
            timeout: Duration::from_secs(2),
            shared: crate::net::client(false, Duration::from_secs(2))?,
        };
        h.run_unit(&unit);
        let requests = mock.server.join().expect("mock server")?;
        let st: UnitState = state::read_json(&h.paths.unit_state(&unit))?.expect("state");
        assert!(
            st.done && st.failed.is_none() && st.gaps.is_empty(),
            "{st:?}"
        );
        assert_eq!(st.records, 7);
        assert_eq!(requests.len(), 4);
        assert!(requests
            .iter()
            .all(|r| r.contains("<c:pageSize>3</c:pageSize>")));
        assert!(requests[0].contains(&format!("<c:end>{}</c:end>", stamp(5))));
        assert!(requests[1].contains(&format!("<c:end>{}</c:end>", stamp(3))));
        assert!(requests[2].contains("ASCENDING"));
        assert!(requests[3].contains("2024-01-02T23:59:59.999Z"));
        let mut ids = Vec::new();
        for part in 0..st.parts {
            ids.extend(
                crate::store::read_strings(&h.paths.part(&unit, part), &["uuid"])?[0].clone(),
            );
        }
        ids.sort();
        assert_eq!(ids, ["b", "c", "d", "e"].map(|s| Some(s.to_owned())));
        std::fs::remove_dir_all(&cfg.root)?;
        Ok(())
    }
}
