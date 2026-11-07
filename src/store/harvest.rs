use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub use crate::store::now;

pub fn sanitize(name: &str) -> String {
    let s: String = name
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if s.is_empty() {
        "_".into()
    } else {
        s
    }
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(
            serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut name = path.as_os_str().to_owned();
    name.push(".tmp");
    let tmp = PathBuf::from(name);
    let mut file = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .with_context(|| format!("write {}", tmp.display()))?;
    drop(file);
    crate::store::replace(&tmp, path)
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    write_atomic(path, &serde_json::to_vec_pretty(value)?)
}

fn pointer(dir: &Path) -> PathBuf {
    dir.join("current")
}

pub fn current_snapshot(dir: &Path) -> Option<String> {
    fs::read_to_string(pointer(dir))
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric()))
}

pub fn new_snapshot(dir: &Path) -> Result<String> {
    let id = Utc::now().format("%Y%m%dT%H%MZ").to_string();
    write_atomic(&pointer(dir), id.as_bytes())?;
    Ok(id)
}

pub fn snapshot(dir: &Path, fresh: bool) -> Result<String> {
    match current_snapshot(dir) {
        Some(s) if !fresh => Ok(s),
        _ => new_snapshot(dir),
    }
}

pub fn append(path: &Path) -> Result<File> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))
}

pub struct Jsonl(Mutex<File>);

impl Jsonl {
    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self(Mutex::new(append(path)?)))
    }

    pub fn write(&self, value: &serde_json::Value) {
        if let Ok(mut f) = self.0.lock() {
            let _ = writeln!(f, "{value}");
        }
    }
}

pub struct Log {
    text: Mutex<File>,
    records: Option<Jsonl>,
}

impl Log {
    pub fn open(text: &Path, records: Option<&Path>) -> Result<Self> {
        Ok(Self {
            text: Mutex::new(append(text)?),
            records: records.map(Jsonl::open).transpose()?,
        })
    }

    pub fn line(&self, msg: &str) {
        let line = format!("{} {msg}\n", now());
        eprint!("{line}");
        if let Ok(mut f) = self.text.lock() {
            let _ = f.write_all(line.as_bytes());
        }
    }

    pub fn record(&self, value: &serde_json::Value) {
        if let Some(r) = &self.records {
            r.write(value);
        }
    }
}

pub struct Lock {
    _file: File,
}

impl Lock {
    pub fn path(dir: &Path) -> PathBuf {
        dir.join("harvest.lock")
    }

    pub fn acquire(dir: &Path) -> Result<Self> {
        let path = Self::path(dir);
        fs::create_dir_all(dir)?;
        let mut file = crate::store::exclusive(&path)
            .with_context(|| format!("another harvest holds {}", path.display()))?;
        file.set_len(0)?;
        writeln!(file, "{} {}", std::process::id(), now())?;
        Ok(Self { _file: file })
    }

    pub fn held(dir: &Path) -> bool {
        crate::store::held(&Self::path(dir))
    }
}

#[derive(Debug, Serialize)]
pub struct UnitStatus<G> {
    pub unit: String,
    pub state: &'static str,
    pub terminal: bool,
    pub expected: Option<u64>,
    pub rows: u64,
    pub count_difference: Option<i64>,
    pub missing_metadata: u64,
    pub gaps: Vec<G>,
    pub failed: Option<String>,
}

pub fn difference(rows: u64, expected: u64) -> i64 {
    i64::try_from(rows).unwrap_or(i64::MAX) - i64::try_from(expected).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{current_snapshot, new_snapshot, read_json, sanitize, snapshot, write_json, Lock};

    #[test]
    fn names() {
        assert_eq!(
            sanitize("Agentschap Landbouw en Visserij "),
            "Agentschap_Landbouw_en_Visserij"
        );
        assert_eq!(sanitize("publications:all"), "publications_all");
        assert_eq!(sanitize(""), "_");
    }

    #[test]
    fn json_and_pointer_are_atomic() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("fris-store-harvest-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("a").join("u.json");
        write_json(&path, &vec![1u32, 2])?;
        write_json(&path, &vec![3u32])?;
        assert_eq!(read_json::<Vec<u32>>(&path)?, Some(vec![3]));
        assert_eq!(read_json::<Vec<u32>>(&dir.join("none.json"))?, None);
        assert!(!dir.join("a").join("u.json.tmp").exists());
        assert_eq!(current_snapshot(&dir), None);
        let id = new_snapshot(&dir)?;
        assert_eq!(current_snapshot(&dir).as_deref(), Some(id.as_str()));
        assert_eq!(snapshot(&dir, false)?, id);
        assert!(!dir.join("current.tmp").exists());
        std::fs::write(dir.join("current"), "../x\n")?;
        assert_eq!(current_snapshot(&dir), None);
        std::fs::write(dir.join("current"), "20260928T1338Z\n")?;
        assert_eq!(current_snapshot(&dir).as_deref(), Some("20260928T1338Z"));
        let lock = Lock::acquire(&dir)?;
        assert!(Lock::held(&dir));
        assert!(Lock::acquire(&dir).is_err());
        drop(lock);
        assert!(!Lock::held(&dir));
        std::fs::remove_dir_all(dir)?;
        Ok(())
    }
}
