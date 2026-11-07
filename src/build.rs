use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::config::Entity;
use crate::state::Paths;
use crate::store::read_strings;
use crate::xml::build::{Origin, Spec};
pub use crate::xml::build::{TableDef, Tables};

const SPEC: Spec<'static> = Spec {
    key: "uuid",
    order: &["last_modified", "fetched_at"],
    carry: &[],
    open: "<r xmlns:fris=\"http://fris.ewi.be/\" xmlns:crit=\"http://fris.ewi.be/criteria\" xmlns:resp=\"http://fris.ewi.be/response\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"",
    envelope: None,
    skip: None,
};

pub fn derived(paths: &Paths) -> PathBuf {
    paths.root.join("derived").join(&paths.snapshot)
}

pub fn parts(paths: &Paths, entity: &str) -> Result<Vec<PathBuf>> {
    let dir = paths.raw().join(entity);
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for unit in fs::read_dir(&dir)? {
        let unit = unit?.path();
        if !unit.is_dir() {
            continue;
        }
        for f in fs::read_dir(&unit)? {
            let f = f?.path();
            if f.extension().is_some_and(|e| e == "parquet") {
                out.push(f);
            }
        }
    }
    out.sort();
    Ok(out)
}

pub fn build(
    paths: &Paths,
    tables: &Tables,
    only: &[String],
    rev: &str,
    out: Option<&Path>,
) -> Result<String> {
    let groups = tables
        .entities(only)
        .into_iter()
        .map(|e| Ok((e.to_owned(), parts(paths, e)?)))
        .collect::<Result<Vec<_>>>()?;
    let origin = Origin {
        rev,
        snapshot: &paths.snapshot,
    };
    let out = out.map_or_else(|| derived(paths), Path::to_path_buf);
    crate::xml::build::build(&groups, &out, tables, &SPEC, &origin)
}

fn values(out: &Path, table: &str, column: &str) -> Result<HashSet<String>> {
    let dir = out.join(table);
    let mut found = HashSet::new();
    for f in fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
        let f = f?.path();
        if f.extension().is_some_and(|e| e == "parquet") {
            let col = read_strings(&f, &[column])?.into_iter().next();
            found.extend(col.into_iter().flatten().flatten());
        }
    }
    Ok(found)
}

pub fn absent(out: &Path, tables: &Tables, entity: &Entity) -> Result<BTreeSet<String>> {
    let own = tables
        .tables
        .iter()
        .find(|t| t.entity == entity.name && t.rows == ".")
        .with_context(|| format!("no record table for {}", entity.name))?;
    let present = values(out, &own.name, SPEC.key)?;
    let mut missing = BTreeSet::new();
    for (table, column) in &entity.references {
        missing.extend(
            values(out, table, column)?
                .into_iter()
                .filter(|u| !present.contains(u)),
        );
    }
    Ok(missing)
}

pub fn paths_inventory(paths: &Paths, entity: &str, limit: usize) -> Result<PathBuf> {
    let text = crate::xml::build::inventory(&parts(paths, entity)?, &SPEC, limit)?;
    let dir = derived(paths).join("_paths");
    fs::create_dir_all(&dir)?;
    let out = dir.join(format!("{entity}.tsv"));
    fs::write(&out, text)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{absent, Tables};
    use crate::config::Config;
    use crate::store::{Column, Table};

    #[test]
    fn absent_lists_references_missing_from_the_record_table() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("fris-absent-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let write = |table: &str, column: &str, values: &[Option<&str>]| {
            Table::new()
                .col(
                    column,
                    Column::Str(values.iter().map(|v| v.map(str::to_owned)).collect()),
                )
                .write(&dir.join(table).join("part.parquet"))
        };
        write("organisation", "uuid", &[Some("a"), Some("b")])?;
        write(
            "ro_assignment",
            "organisation",
            &[Some("a"), None, Some("c")],
        )?;
        write(
            "person_organisation",
            "organisation",
            &[Some("d"), Some("b")],
        )?;
        write("organisation_relation", "other", &[Some("c"), Some("e")])?;
        let cfg: Config = toml::from_str(include_str!("../config/fris.toml"))?;
        let tables: Tables = toml::from_str(include_str!("../config/fris-tables.toml"))?;
        let e = cfg.entity("organisation").expect("organisation entity");
        let missing: Vec<String> = absent(&dir, &tables, e)?.into_iter().collect();
        assert_eq!(missing, ["c", "d", "e"]);
        let person = cfg.entity("person").expect("person entity");
        assert!(person.references.is_empty());
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
