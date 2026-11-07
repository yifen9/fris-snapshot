use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::hash::{BuildHasher, BuildHasherDefault};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::store::{read_strings, row_count, Column, Table};
use crate::xml::extract::{inventory as walk, record_root, select, value, wrap};

#[derive(Debug, Clone, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub entity: String,
    pub rows: String,
    pub columns: Vec<(String, String)>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Tables {
    pub threads: usize,
    #[serde(default)]
    pub winner_rows: Option<u64>,
    pub tables: Vec<TableDef>,
    #[serde(skip)]
    pub source: PathBuf,
    #[serde(skip)]
    pub sha256: String,
}

impl Tables {
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let mut tables: Self =
            toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
        tables.source = path.to_path_buf();
        tables.sha256 = format!("{:x}", Sha256::digest(text.as_bytes()));
        Ok(tables)
    }

    pub fn entities(&self, only: &[String]) -> Vec<&str> {
        let mut out: Vec<&str> = self
            .tables
            .iter()
            .map(|t| t.entity.as_str())
            .filter(|e| only.is_empty() || only.iter().any(|o| o == e))
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

pub struct Origin<'a> {
    pub rev: &'a str,
    pub snapshot: &'a str,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Built {
    pub entity: String,
    pub rows: u64,
    #[serde(default)]
    pub source_rows: u64,
    #[serde(default)]
    pub records: u64,
    #[serde(default)]
    pub tombstones: u64,
    pub git_rev: String,
    pub config_sha256: String,
    pub snapshot: String,
    pub built_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub git_rev: String,
    pub config: String,
    pub config_sha256: String,
    pub snapshot: String,
    pub entities: Vec<String>,
    pub built_at: String,
    pub tables: BTreeMap<String, Built>,
}

fn stamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

impl Manifest {
    pub fn path(out: &Path) -> PathBuf {
        out.join("_build.json")
    }

    pub fn load(out: &Path) -> Result<Option<Self>> {
        let path = Self::path(out);
        match fs::read_to_string(&path) {
            Ok(text) => Ok(Some(
                serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))?,
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
        }
    }

    pub fn save(&self, out: &Path) -> Result<()> {
        let path = Self::path(out);
        fs::create_dir_all(out).with_context(|| format!("create {}", out.display()))?;
        let tmp = path.with_extension("json.tmp");
        let mut file = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        file.write_all(&serde_json::to_vec_pretty(self)?)
            .and_then(|()| file.sync_all())
            .with_context(|| format!("write {}", tmp.display()))?;
        drop(file);
        crate::store::replace(&tmp, &path)
    }

    fn begin(out: &Path, tables: &Tables, origin: &Origin) -> Result<Self> {
        Ok(Self {
            git_rev: origin.rev.to_owned(),
            config: tables.source.display().to_string(),
            config_sha256: tables.sha256.clone(),
            snapshot: origin.snapshot.to_owned(),
            entities: Vec::new(),
            built_at: stamp(),
            tables: Self::load(out)?.map(|m| m.tables).unwrap_or_default(),
        })
    }

    fn record(&mut self, entity: &str, table: &str, rows: u64, counts: &Counts) {
        let built = Built {
            entity: entity.to_owned(),
            rows,
            source_rows: counts.source_rows,
            records: counts.records,
            tombstones: counts.tombstones,
            git_rev: self.git_rev.clone(),
            config_sha256: self.config_sha256.clone(),
            snapshot: self.snapshot.clone(),
            built_at: stamp(),
        };
        self.tables.insert(table.to_owned(), built);
    }
}

pub struct Counts {
    pub source_rows: u64,
    pub records: u64,
    pub tombstones: u64,
}

pub struct Spec<'a> {
    pub key: &'a str,
    pub order: &'a [&'a str],
    pub carry: &'a [&'a str],
    pub open: &'a str,
    pub envelope: Option<&'a str>,
    pub skip: Option<(&'a str, &'a str)>,
}

fn millis(ts: Option<&String>) -> i64 {
    let Some(t) = ts else {
        return i64::MIN;
    };
    DateTime::parse_from_rfc3339(t)
        .map(|d| d.timestamp_millis())
        .ok()
        .or_else(|| {
            NaiveDate::parse_from_str(t, "%Y-%m-%d")
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|d| d.and_utc().timestamp_millis())
        })
        .unwrap_or(i64::MIN)
}

struct Winner {
    key: Vec<i64>,
    file: u32,
    row: u32,
    skip: bool,
}

fn winners(files: &[PathBuf], spec: &Spec, limit: Option<u64>) -> Result<(Vec<Vec<u32>>, u64)> {
    let mut names = vec![spec.key];
    names.extend(spec.order);
    if let Some((c, _)) = spec.skip {
        names.push(c);
    }
    let flag = spec.skip.map(|(_, v)| (names.len() - 1, v));
    let passes = match limit {
        Some(limit) => {
            let rows: Vec<u64> = files
                .par_iter()
                .map(|f| row_count(f))
                .collect::<Result<_>>()?;
            rows.iter().sum::<u64>().div_ceil(limit.max(1)).max(1)
        }
        None => 1,
    };
    let hasher = BuildHasherDefault::<DefaultHasher>::default();
    let mut keep: Vec<Vec<u32>> = vec![Vec::new(); files.len()];
    let mut total = 0u64;
    let width = rayon::current_num_threads().max(1);
    for pass in 0..passes {
        let mut best: HashMap<String, Winner> = HashMap::new();
        for (batch, group) in files.chunks(width).enumerate() {
            let heads: Vec<Vec<Vec<Option<String>>>> = group
                .par_iter()
                .map(|f| read_strings(f, &names))
                .collect::<Result<_>>()?;
            for (offset, cols) in heads.into_iter().enumerate() {
                let fi = batch * width + offset;
                for (ri, id) in cols[0].iter().enumerate() {
                    let skip = flag.is_some_and(|(i, v)| cols[i][ri].as_deref() == Some(v));
                    let (file, row) = (fi as u32, ri as u32);
                    let Some(id) = id else {
                        if pass == 0 {
                            total += 1;
                            if !skip {
                                keep[fi].push(row);
                            }
                        }
                        continue;
                    };
                    if passes > 1 && hasher.hash_one(id) % passes != pass {
                        continue;
                    }
                    total += 1;
                    let key: Vec<i64> = (1..=spec.order.len())
                        .map(|i| millis(cols[i][ri].as_ref()))
                        .collect();
                    let w = Winner {
                        key,
                        file,
                        row,
                        skip,
                    };
                    match best.get_mut(id) {
                        Some(b) if b.key >= w.key => {}
                        Some(b) => *b = w,
                        None => {
                            best.insert(id.clone(), w);
                        }
                    }
                }
            }
        }
        for w in best.values().filter(|w| !w.skip) {
            keep[w.file as usize].push(w.row);
        }
    }
    for k in &mut keep {
        k.sort_unstable();
    }
    Ok((keep, total))
}

struct Sink {
    key: Vec<Option<String>>,
    row: Vec<u32>,
    carry: Vec<Vec<Option<String>>>,
    cols: Vec<Vec<Option<String>>>,
}

impl Sink {
    fn new(cols: usize, carry: usize) -> Self {
        Self {
            key: Vec::new(),
            row: Vec::new(),
            carry: vec![Vec::new(); carry],
            cols: vec![Vec::new(); cols],
        }
    }

    fn write(self, def: &TableDef, spec: &Spec, path: &Path) -> Result<usize> {
        let n = self.key.len();
        if n == 0 {
            return Ok(0);
        }
        let mut t = Table::new()
            .col(spec.key, Column::Str(self.key))
            .col("row", Column::U32(self.row));
        for (name, values) in spec.carry.iter().zip(self.carry) {
            t = t.col(name, Column::Str(values));
        }
        for ((name, _), values) in def.columns.iter().zip(self.cols) {
            t = t.col(name, Column::Str(values));
        }
        t.write(path)?;
        Ok(n)
    }
}

fn read(path: &Path, spec: &Spec) -> Result<(Vec<Vec<Option<String>>>, usize)> {
    let mut names = vec![spec.key, "xml"];
    if let Some(e) = spec.envelope {
        names.push(e);
    }
    let base = names.len();
    names.extend(spec.carry);
    Ok((read_strings(path, &names)?, base))
}

fn extract_file(
    path: &Path,
    keep: &[u32],
    defs: &[&TableDef],
    spec: &Spec,
) -> Result<(Vec<Sink>, u64)> {
    let (cols, base) = read(path, spec)?;
    let mut sinks: Vec<Sink> = defs
        .iter()
        .map(|d| Sink::new(d.columns.len(), spec.carry.len()))
        .collect();
    let mut tombstones = 0;
    for &r in keep {
        let r = r as usize;
        let Some(xml) = cols[1][r].as_deref() else {
            tombstones += 1;
            continue;
        };
        let ns = spec
            .envelope
            .and_then(|_| cols[2][r].as_deref())
            .unwrap_or("");
        let text = wrap(spec.open, ns, xml);
        let doc = roxmltree::Document::parse(&text)
            .with_context(|| format!("xml row {r} in {}", path.display()))?;
        let Some(root) = record_root(&doc) else {
            continue;
        };
        for (def, sink) in defs.iter().zip(sinks.iter_mut()) {
            for (k, node) in select(root, &def.rows).into_iter().enumerate() {
                sink.key.push(cols[0][r].clone());
                sink.row.push(k as u32);
                for (c, values) in sink.carry.iter_mut().enumerate() {
                    values.push(cols[base + c][r].clone());
                }
                for (c, (_, expr)) in def.columns.iter().enumerate() {
                    sink.cols[c].push(value(node, expr, &text));
                }
            }
        }
    }
    Ok((sinks, tombstones))
}

fn aside(out: &Path, name: &str, kind: &str) -> PathBuf {
    out.join(format!(".{name}.{kind}"))
}

fn clear(dir: &Path) -> Result<()> {
    if dir.exists() {
        fs::remove_dir_all(dir).with_context(|| format!("clear {}", dir.display()))?;
    }
    Ok(())
}

fn swap(staged: &Path, dir: &Path, old: &Path) -> Result<()> {
    clear(old)?;
    if dir.exists() {
        fs::rename(dir, old).with_context(|| format!("move aside {}", dir.display()))?;
    }
    if staged.exists() {
        fs::rename(staged, dir).with_context(|| format!("move into {}", dir.display()))?;
    }
    clear(old)
}

pub fn build(
    groups: &[(String, Vec<PathBuf>)],
    out: &Path,
    tables: &Tables,
    spec: &Spec,
    origin: &Origin,
) -> Result<String> {
    for t in &tables.tables {
        let mut seen: Vec<&str> = vec![spec.key, "row"];
        seen.extend(spec.carry);
        for (name, _) in &t.columns {
            anyhow::ensure!(
                !seen.contains(&name.as_str()),
                "table {}: column {name} repeats or shadows a key column",
                t.name
            );
            seen.push(name);
        }
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(tables.threads)
        .build()?;
    let mut manifest = Manifest::begin(out, tables, origin)?;
    let mut report = String::new();
    for (entity, files) in groups {
        let defs: Vec<&TableDef> = tables
            .tables
            .iter()
            .filter(|t| &t.entity == entity)
            .collect();
        if files.is_empty() {
            let _ = writeln!(report, "{entity}: no raw parts");
            continue;
        }
        let staged: Vec<PathBuf> = defs.iter().map(|d| aside(out, &d.name, "tmp")).collect();
        for dir in &staged {
            clear(dir)?;
        }
        let (keep, total) = pool.install(|| winners(files, spec, tables.winner_rows))?;
        let kept: usize = keep.iter().map(Vec::len).sum();
        let tag = entity.replace('/', "_");
        let counts: Result<Vec<(Vec<usize>, u64)>> = pool.install(|| {
            files
                .par_iter()
                .enumerate()
                .map(|(fi, path)| -> Result<(Vec<usize>, u64)> {
                    let (sinks, tombstones) = extract_file(path, &keep[fi], &defs, spec)?;
                    let rows = defs
                        .iter()
                        .zip(sinks)
                        .zip(&staged)
                        .map(|((def, sink), dir)| {
                            sink.write(def, spec, &dir.join(format!("{tag}-{fi:05}.parquet")))
                        })
                        .collect::<Result<_>>()?;
                    Ok((rows, tombstones))
                })
                .collect()
        });
        let counts = match counts {
            Ok(c) => c,
            Err(e) => {
                for dir in &staged {
                    let _ = fs::remove_dir_all(dir);
                }
                return Err(e);
            }
        };
        for (d, dir) in defs.iter().zip(&staged) {
            swap(dir, &out.join(&d.name), &aside(out, &d.name, "old"))?;
        }
        let summary = Counts {
            source_rows: total,
            records: kept as u64,
            tombstones: counts.iter().map(|c| c.1).sum(),
        };
        let _ = writeln!(
            report,
            "{entity}: files={} raw_rows={total} unique={kept} dropped={} tombstones={}",
            files.len(),
            total - kept as u64,
            summary.tombstones
        );
        for (k, d) in defs.iter().enumerate() {
            let n: usize = counts.iter().map(|c| c.0[k]).sum();
            let _ = writeln!(report, "  {:<28} rows={n}", d.name);
            manifest.record(entity, &d.name, n as u64, &summary);
        }
        manifest.entities.push(entity.clone());
        manifest.save(out)?;
    }
    let _ = writeln!(report, "manifest {}", Manifest::path(out).display());
    Ok(report)
}

pub fn inventory(files: &[PathBuf], spec: &Spec, limit: usize) -> Result<String> {
    let mut inv: BTreeMap<String, (u64, BTreeMap<String, u64>)> = BTreeMap::new();
    let mut kinds: BTreeMap<String, u64> = BTreeMap::new();
    let mut seen = 0usize;
    'files: for f in files {
        let (cols, _) = read(f, spec)?;
        for (r, xml) in cols[1].iter().enumerate() {
            let Some(xml) = xml else {
                continue;
            };
            let ns = spec
                .envelope
                .and_then(|_| cols[2][r].as_deref())
                .unwrap_or("");
            let text = wrap(spec.open, ns, xml);
            let doc = roxmltree::Document::parse(&text)?;
            if let Some(root) = record_root(&doc) {
                *kinds.entry(root.tag_name().name().to_owned()).or_default() += 1;
                walk(root, "", &mut inv);
            }
            seen += 1;
            if seen >= limit {
                break 'files;
            }
        }
    }
    let mut text = format!("records\t{seen}\t{kinds:?}\npath\tcount\tshare\tattrs\n");
    for (p, (n, attrs)) in &inv {
        let a: Vec<String> = attrs.iter().map(|(k, v)| format!("{k}:{v}")).collect();
        let _ = writeln!(
            text,
            "{p}\t{n}\t{:.4}\t{}",
            *n as f64 / seen.max(1) as f64,
            a.join(",")
        );
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{build, winners, Manifest, Origin, Spec, TableDef, Tables};
    use crate::store::{Column, Table};

    const SPEC: Spec<'static> = Spec {
        key: "uuid",
        order: &["last_modified", "fetched_at"],
        carry: &[],
        open: "<r",
        envelope: None,
        skip: None,
    };

    fn raw(path: &Path, ids: &[&str]) -> anyhow::Result<()> {
        let col =
            |f: &dyn Fn(&str) -> String| Column::Str(ids.iter().map(|i| Some(f(i))).collect());
        Table::new()
            .col("uuid", col(&|i| i.to_owned()))
            .col("last_modified", col(&|_| "2024-01-01".to_owned()))
            .col("fetched_at", col(&|_| "2024-01-02".to_owned()))
            .col("xml", col(&|i| format!("<x><title>T{i}</title></x>")))
            .write(path)?;
        Ok(())
    }

    fn tables(sha: &str) -> Tables {
        let def = |name: &str, entity: &str| TableDef {
            name: name.to_owned(),
            entity: entity.to_owned(),
            rows: ".".to_owned(),
            columns: vec![("title".to_owned(), "title".to_owned())],
        };
        Tables {
            threads: 1,
            winner_rows: None,
            tables: vec![def("t_a", "a"), def("t_b", "b")],
            source: PathBuf::from("tables.toml"),
            sha256: sha.to_owned(),
        }
    }

    #[test]
    fn manifest_merges_partial_builds() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("fris-xml-manifest-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (fa, fb) = (dir.join("raw/a.parquet"), dir.join("raw/b.parquet"));
        raw(&fa, &["1", "2", "2"])?;
        raw(&fb, &["9"])?;
        let out = dir.join("out");
        let groups = vec![
            ("a".to_owned(), vec![fa.clone()]),
            ("b".to_owned(), vec![fb]),
        ];
        let first = Origin {
            rev: "r1",
            snapshot: "S1",
        };
        build(&groups, &out, &tables("c1"), &SPEC, &first)?;
        let one = Manifest::load(&out)?.ok_or_else(|| anyhow::anyhow!("no manifest"))?;
        let second = Origin {
            rev: "r2",
            snapshot: "S2",
        };
        let report = build(&groups[..1], &out, &tables("c2"), &SPEC, &second)?;
        let two = Manifest::load(&out)?.ok_or_else(|| anyhow::anyhow!("no manifest"))?;
        let leftover = out.join("_build.json.tmp").exists();
        std::fs::remove_dir_all(&dir)?;
        assert_eq!(one.entities, ["a", "b"]);
        assert_eq!((one.tables["t_a"].rows, one.tables["t_b"].rows), (2, 1));
        assert_eq!(one.tables["t_b"].git_rev, "r1");
        assert_eq!(
            (two.git_rev.as_str(), two.config_sha256.as_str()),
            ("r2", "c2")
        );
        assert_eq!(
            (two.snapshot.as_str(), two.config.as_str()),
            ("S2", "tables.toml")
        );
        assert_eq!(two.entities, ["a"]);
        assert_eq!(
            (
                two.tables["t_a"].git_rev.as_str(),
                two.tables["t_a"].config_sha256.as_str()
            ),
            ("r2", "c2")
        );
        assert_eq!(
            (
                two.tables["t_b"].git_rev.as_str(),
                two.tables["t_b"].config_sha256.as_str(),
                two.tables["t_b"].snapshot.as_str()
            ),
            ("r1", "c1", "S1")
        );
        assert!(!two.built_at.is_empty());
        assert!(!leftover);
        assert!(report.contains("_build.json"));
        Ok(())
    }

    #[test]
    fn manifest_counts_sources_winners_and_tombstones() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("fris-xml-counts-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (fa, fb) = (dir.join("raw/a.parquet"), dir.join("raw/b.parquet"));
        raw(&fa, &["1", "2"])?;
        let text = |v: [Option<&str>; 2]| Column::Str(v.map(|x| x.map(str::to_owned)).to_vec());
        Table::new()
            .col("uuid", text([Some("2"), Some("3")]))
            .col(
                "last_modified",
                text([Some("2024-02-01"), Some("2024-02-01")]),
            )
            .col("fetched_at", text([Some("2024-02-02"), Some("2024-02-02")]))
            .col("xml", text([Some("<x><title>T2b</title></x>"), None]))
            .write(&fb)?;
        let out = dir.join("out");
        let groups = vec![("a".to_owned(), vec![fa, fb])];
        let origin = Origin {
            rev: "r1",
            snapshot: "S1",
        };
        let report = build(&groups, &out, &tables("c1"), &SPEC, &origin)?;
        let manifest = Manifest::load(&out)?.ok_or_else(|| anyhow::anyhow!("no manifest"))?;
        let titles = crate::store::read_strings(&out.join("t_a/a-00001.parquet"), &["title"])?;
        std::fs::remove_dir_all(&dir)?;
        let built = &manifest.tables["t_a"];
        assert_eq!(
            (
                built.source_rows,
                built.records,
                built.tombstones,
                built.rows
            ),
            (4, 3, 1, 2)
        );
        assert_eq!(built.records - built.tombstones, built.rows);
        assert_eq!(titles, vec![vec![Some("T2b".to_owned())]]);
        assert!(report.contains("tombstones=1"));
        Ok(())
    }

    #[test]
    fn bounded_winner_passes_keep_the_same_rows() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("fris-xml-passes-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (fa, fb) = (dir.join("raw/a.parquet"), dir.join("raw/b.parquet"));
        raw(&fa, &["1", "2", "3", "4", "2"])?;
        let text = |v: [Option<&str>; 3]| Column::Str(v.map(|x| x.map(str::to_owned)).to_vec());
        Table::new()
            .col("uuid", text([Some("3"), Some("5"), None]))
            .col(
                "last_modified",
                text([Some("2024-03-01"), Some("2024-01-01"), Some("2024-01-01")]),
            )
            .col(
                "fetched_at",
                text([Some("2024-03-02"), Some("2024-01-02"), Some("2024-01-02")]),
            )
            .col(
                "xml",
                text([
                    Some("<x><title>T3b</title></x>"),
                    Some("<x><title>T5</title></x>"),
                    Some("<x><title>T-</title></x>"),
                ]),
            )
            .write(&fb)?;
        let files = vec![fa, fb];
        let single = winners(&files, &SPEC, None)?;
        for limit in [1, 2, 3, 100] {
            assert_eq!(winners(&files, &SPEC, Some(limit))?, single);
        }
        std::fs::remove_dir_all(&dir)?;
        assert_eq!(single, (vec![vec![0, 1, 3], vec![0, 1, 2]], 8));
        Ok(())
    }

    #[test]
    fn failed_build_keeps_the_previous_tables() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("fris-xml-swap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let fa = dir.join("raw/a.parquet");
        raw(&fa, &["1", "2"])?;
        let out = dir.join("out");
        let groups = vec![("a".to_owned(), vec![fa.clone()])];
        let origin = Origin {
            rev: "r1",
            snapshot: "S1",
        };
        build(&groups, &out, &tables("c1"), &SPEC, &origin)?;
        let table = out.join("t_a").join("a-00000.parquet");
        let before = std::fs::read(&table)?;
        let manifest = std::fs::read(Manifest::path(&out))?;
        Table::new()
            .col("uuid", Column::Str(vec![Some("3".to_owned())]))
            .col(
                "last_modified",
                Column::Str(vec![Some("2024-01-01".to_owned())]),
            )
            .col(
                "fetched_at",
                Column::Str(vec![Some("2024-01-02".to_owned())]),
            )
            .col("xml", Column::Str(vec![Some("<x><title>".to_owned())]))
            .write(&fa)?;
        assert!(build(&groups, &out, &tables("c2"), &SPEC, &origin).is_err());
        let after = std::fs::read(&table)?;
        let kept = std::fs::read(Manifest::path(&out))?;
        let leftovers: Vec<_> = std::fs::read_dir(&out)?
            .filter_map(std::result::Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with('.'))
            .collect();
        raw(&fa, &["4"])?;
        build(&groups, &out, &tables("c3"), &SPEC, &origin)?;
        let rebuilt = crate::store::read_strings(&table, &["uuid"])?;
        std::fs::remove_dir_all(&dir)?;
        assert_eq!(before, after);
        assert_eq!(manifest, kept);
        assert!(leftovers.is_empty(), "{leftovers:?}");
        assert_eq!(rebuilt, vec![vec![Some("4".to_owned())]]);
        Ok(())
    }

    #[test]
    fn tables_hash_their_source() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("fris-xml-tables-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("t.toml");
        std::fs::write(&path, "threads = 1\ntables = []\n")?;
        let t = Tables::load(&path)?;
        std::fs::remove_dir_all(&dir)?;
        assert_eq!(t.source, path);
        assert_eq!(
            t.sha256,
            "16bb528f841614c5149cb15c305b00af2c7f24497829303650c22ec1dddbecb8"
        );
        Ok(())
    }
}
