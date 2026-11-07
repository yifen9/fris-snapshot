use std::collections::HashSet;

use crate::config::Entity;
use crate::harvest::{Buffer, Ctx, Harvester, Outcome, Step};
use crate::soap::{Query, Rec};
use crate::state::{self, Unit, UnitState};

pub(crate) fn unit(entity: &str, id: String, targets: usize) -> Unit {
    Unit {
        entity: entity.to_owned(),
        id,
        provider: None,
        start: None,
        end: None,
        expected: targets as u64,
        cap: 0,
    }
}

impl Harvester<'_> {
    fn single(&self, e: &Entity, ctx: &Ctx, uuid: &str) -> Step<Result<Rec, String>> {
        let ids = [uuid.to_owned()];
        let query = Query {
            page_size: 1,
            locale: &self.cfg.locale,
            uuids: &ids,
            ..Query::default()
        };
        let ctx = Ctx {
            page_size: 1,
            ..*ctx
        };
        let mut faults = 0u32;
        loop {
            match self.request(&self.shared, e, &query.criteria(), &ctx)? {
                Outcome::Page(p, _) => {
                    let total = p
                        .total
                        .map_or_else(|| "absent".to_owned(), |t| t.to_string());
                    return Ok(p
                        .records
                        .into_iter()
                        .find(|r| r.uuid.as_deref() == Some(uuid))
                        .ok_or_else(|| format!("not returned: total={total}")));
                }
                Outcome::Fault(f) => {
                    faults += 1;
                    if faults > self.cfg.fault_retries {
                        return Ok(Err(f.to_string()));
                    }
                    self.backoff(faults)?;
                }
            }
        }
    }

    pub(crate) fn refetch(
        &self,
        e: &Entity,
        unit: &Unit,
        mode: &str,
        targets: &[(String, Option<String>)],
    ) -> Step<Vec<(String, String)>> {
        let path = self.paths.unit_state(unit);
        let mut st: UnitState = state::read_json(&path)?.unwrap_or_default();
        if st.done {
            return Ok(Vec::new());
        }
        if st.page_size == 0 {
            st.page_size = e.page_size;
        }
        st.started.get_or_insert_with(state::now);
        let undated = targets.iter().any(|(_, occurred)| occurred.is_none());
        let mut buf = Buffer::new(&st);
        let mut faults = 0u32;
        let mut refused = Vec::new();
        let result = (|| -> Step<()> {
            while (st.index as usize) < targets.len() {
                let index = st.index as usize;
                let size = st.page_size.min(targets.len() - index);
                let chunk = &targets[index..index + size];
                let ids: Vec<String> = chunk.iter().map(|(u, _)| u.clone()).collect();
                let query = Query {
                    page_size: size,
                    locale: &self.cfg.locale,
                    uuids: &ids,
                    ..Query::default()
                };
                let ctx = Ctx {
                    unit: &unit.id,
                    mode,
                    page: st.pages as usize,
                    page_size: size,
                };
                let records = match self.request(&self.shared, e, &query.criteria(), &ctx)? {
                    Outcome::Page(p, _) => {
                        faults = 0;
                        Self::check_total(unit, p.total)?;
                        p.records
                    }
                    Outcome::Fault(_) if undated => Vec::new(),
                    Outcome::Fault(f) => {
                        self.on_fault(e, &mut st, unit, &mut faults, &f)?;
                        continue;
                    }
                };
                let fetched = state::now();
                let returned: HashSet<String> =
                    records.iter().filter_map(|r| r.uuid.clone()).collect();
                for r in records {
                    st.records += 1;
                    buf.push(r, st.pages, &fetched);
                }
                for (u, occurred) in chunk.iter().filter(|(u, _)| !returned.contains(u)) {
                    if let Some(occurred) = occurred {
                        buf.push_gone(u, occurred, st.pages, &fetched);
                        continue;
                    }
                    match self.single(e, &ctx, u)? {
                        Ok(r) => {
                            st.records += 1;
                            buf.push(r, st.pages, &fetched);
                        }
                        Err(why) => {
                            self.log.line(&format!(
                                "{mode}-refused entity={} unit={} uuid={u} {why}",
                                e.name, unit.id
                            ));
                            refused.push((u.clone(), why));
                        }
                    }
                }
                st.index += size as u64;
                st.pages += 1;
                buf.pages += 1;
                if buf.full(self.cfg) {
                    self.flush(unit, &mut st, &mut buf)?;
                }
            }
            Ok(())
        })();
        self.flush(unit, &mut st, &mut buf)?;
        result?;
        st.done = true;
        st.finished = Some(state::now());
        state::write_json(&path, &st)?;
        self.log.line(&format!(
            "{mode}-done entity={} targets={} returned={}",
            e.name,
            targets.len(),
            st.records
        ));
        Ok(refused)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;
    use std::time::Duration;

    use crate::config::Config;
    use crate::harvest::tests::soap_server;
    use crate::harvest::Harvester;
    use crate::state::{self, Paths, UnitState};

    fn reply(ids: &[&str]) -> String {
        let mut records = String::new();
        for id in ids {
            let _ = write!(records, "<fris:organisation xmlns:fris=\"http://fris.ewi.be/\" uuid=\"{id}\"><fris:lastModified>2024-01-01T00:00:00.000Z</fris:lastModified></fris:organisation>");
        }
        format!("<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"><soap:Body><r total=\"{}\">{records}</r></soap:Body></soap:Envelope>", ids.len())
    }

    const FAULT: &str = r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><soap:Fault><faultcode>soap:Server</faultcode><faultstring>Fault occurred while processing.</faultstring></soap:Fault></soap:Body></soap:Envelope>"#;

    #[test]
    fn undated_targets_are_probed_alone_and_never_become_tombstones() -> anyhow::Result<()> {
        let replies = vec![
            reply(&["a"]),
            reply(&[]),
            FAULT.to_owned(),
            FAULT.to_owned(),
            reply(&["d"]),
        ];
        let mock = soap_server(replies)?;
        let mut cfg: Config = toml::from_str(include_str!("../config/fris.toml"))?;
        cfg.root = std::env::temp_dir().join(format!("fris-fetch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&cfg.root);
        cfg.base = format!("http://{}/", mock.address);
        cfg.fault_retries = 1;
        cfg.backoff_cap_s = 0;
        cfg.entities.retain(|e| e.name == "organisation");
        cfg.entities[0].page_size = 3;
        let paths = Paths {
            root: cfg.root.clone(),
            snapshot: "test".into(),
        };
        let gate = crate::net::Gate::new(&cfg.control_dir(), 0, Duration::ZERO)?;
        let h = Harvester {
            cfg: &cfg,
            gate: &gate,
            log: paths.open_log()?,
            paths,
            timeout: Duration::from_secs(2),
            shared: crate::net::client(false, Duration::from_secs(2))?,
        };
        let e = &cfg.entities[0];
        let undated: Vec<(String, Option<String>)> =
            ["a", "b", "c"].map(|u| (u.to_owned(), None)).to_vec();
        let fetched = super::unit(&e.name, "_fetch-t".into(), undated.len());
        let Ok(refused) = h.refetch(e, &fetched, "fetch", &undated) else {
            anyhow::bail!("fetch aborted")
        };
        assert_eq!(
            refused,
            [
                ("b".to_owned(), "not returned: total=0".to_owned()),
                (
                    "c".to_owned(),
                    "soap:Server Fault occurred while processing.".to_owned()
                ),
            ]
        );
        let dated = vec![
            ("d".to_owned(), Some("2024-02-01T00:00:00.000Z".to_owned())),
            ("e".to_owned(), Some("2024-02-02T00:00:00.000Z".to_owned())),
        ];
        let changed = super::unit(&e.name, "_changes-t".into(), dated.len());
        let Ok(refused) = h.refetch(e, &changed, "changes", &dated) else {
            anyhow::bail!("changes aborted")
        };
        assert!(refused.is_empty());
        let requests = mock.server.join().expect("mock server")?;
        assert_eq!(requests.len(), 5);
        assert!(["a", "b", "c"]
            .iter()
            .all(|u| requests[0].contains(&format!("<c:identifier>{u}</c:identifier>"))));
        assert!(requests[1].contains("<c:pageSize>1</c:pageSize>"));
        assert!(requests[1].contains("<c:identifier>b</c:identifier>"));
        assert!(!requests[1].contains("<c:identifier>a</c:identifier>"));
        assert!(requests[2..4]
            .iter()
            .all(|r| r.contains("<c:identifier>c</c:identifier>")));
        let st: UnitState = state::read_json(&h.paths.unit_state(&fetched))?.expect("state");
        assert!(st.done && st.records == 1 && st.parts == 1, "{st:?}");
        let rows = crate::store::read_strings(&h.paths.part(&fetched, 0), &["uuid", "kind"])?;
        assert_eq!(rows[0], [Some("a".to_owned())]);
        assert_eq!(rows[1], [Some("organisation".to_owned())]);
        let rows = crate::store::read_strings(&h.paths.part(&changed, 0), &["uuid", "kind"])?;
        assert_eq!(rows[0], [Some("d".to_owned()), Some("e".to_owned())]);
        assert_eq!(
            rows[1],
            [Some("organisation".to_owned()), Some("_gone".to_owned())]
        );
        std::fs::remove_dir_all(&cfg.root)?;
        Ok(())
    }
}
