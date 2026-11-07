use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Keyset,
    Session,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Partition {
    Provider,
    None,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Entity {
    pub name: String,
    pub service: String,
    pub op: String,
    pub page_size: usize,
    pub mode: Mode,
    pub partition: Partition,
    pub order: String,
    pub priority: u32,
    pub split_above: Option<u64>,
    #[serde(default)]
    pub changes: Vec<String>,
    #[serde(default)]
    pub references: Vec<(String, String)>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub root: PathBuf,
    pub base: String,
    pub concurrency: usize,
    pub min_interval_ms: u64,
    pub bandwidth: String,
    pub timeout_s: u64,
    pub part_pages: usize,
    pub part_bytes: usize,
    pub fault_retries: u32,
    pub min_page_size: usize,
    pub backoff_cap_s: u64,
    pub changes_margin_s: i64,
    pub slow_ms: u64,
    pub fast_ms: u64,
    pub grow_after: u32,
    pub locale: String,
    pub meta_services: Vec<String>,
    pub meta_skip: Vec<String>,
    pub entities: Vec<Entity>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let mut cfg: Self =
            toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
        if let (Ok(rest), Some(home)) = (cfg.root.strip_prefix("~"), std::env::var_os("HOME")) {
            cfg.root = PathBuf::from(home).join(rest);
        }
        anyhow::ensure!(cfg.concurrency > 0, "concurrency must be positive");
        anyhow::ensure!(cfg.part_pages > 0, "part_pages must be positive");
        Ok(cfg)
    }

    pub fn entity(&self, name: &str) -> Option<&Entity> {
        self.entities.iter().find(|e| e.name == name)
    }

    pub fn url(&self, service: &str) -> String {
        format!("{}{}", self.base, service)
    }

    pub fn source_dir(&self) -> PathBuf {
        self.root.join("state")
    }

    pub fn control_dir(&self) -> PathBuf {
        self.root.join("control")
    }
}
