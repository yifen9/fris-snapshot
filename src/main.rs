use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use fris_snapshot::config::Config;
use fris_snapshot::net::Signal;
use fris_snapshot::state::Lock;
use fris_snapshot::store::Handles;

#[derive(Parser)]
#[command(name = "fris-snapshot")]
struct Cli {
    #[arg(long, default_value = "config/fris.toml")]
    config: PathBuf,
    #[command(subcommand)]
    cmd: Fris,
}

#[derive(Args, Clone)]
struct Scope {
    #[arg(long)]
    only: Vec<String>,
    #[arg(long)]
    fresh: bool,
}

#[derive(Subcommand)]
enum Control {
    Bandwidth { rate: String },
    Pause,
    Resume,
    Stop,
}

#[derive(Subcommand)]
enum Fris {
    Plan(Scope),
    Harvest {
        #[command(flatten)]
        scope: Scope,
        #[arg(long)]
        background: bool,
    },
    Spawn(Scope),
    Status {
        #[arg(long)]
        json: bool,
    },
    Changes,
    Fetch {
        #[arg(long)]
        entity: String,
        #[arg(long)]
        uuids: Option<PathBuf>,
        #[arg(long)]
        tables: Option<PathBuf>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Build {
        #[arg(long)]
        tables: Option<PathBuf>,
        #[arg(long)]
        only: Vec<String>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Paths {
        entity: String,
        #[arg(long, default_value_t = 20000)]
        limit: usize,
    },
    #[command(flatten)]
    Control(Control),
}

const REV: &str = env!("GIT_REV");

type BuildFn = fn(&Path, &Path, &[String]) -> Result<String>;

fn tables_beside(config: &Path) -> Option<PathBuf> {
    let stem = config.file_stem()?.to_str()?;
    Some(config.with_file_name(format!("{stem}-tables.toml"))).filter(|p| p.is_file())
}

fn tables(config: &Path, given: Option<&PathBuf>) -> Result<PathBuf> {
    given
        .cloned()
        .or_else(|| tables_beside(config))
        .with_context(|| format!("no tables config beside {}", config.display()))
}

fn finish(
    terminal: bool,
    scope: &Scope,
    control: &Path,
    config: &Path,
    build: Option<BuildFn>,
) -> Result<bool> {
    if Signal::Stop.path(control).exists() {
        return Ok(true);
    }
    if terminal && scope.only.is_empty() {
        if let (Some(build), Some(tables)) = (build, tables_beside(config)) {
            print!("{}", build(config, &tables, &[])?);
        }
    }
    Ok(terminal)
}

fn build_fris(config: &Path, tables: &Path, only: &[String]) -> Result<String> {
    fris_build(&Config::load(config)?, tables, only, None)
}

fn fris_build(cfg: &Config, tables: &Path, only: &[String], out: Option<&Path>) -> Result<String> {
    let tables = fris_snapshot::build::Tables::load(tables)?;
    fris_snapshot::build::build(&fris_snapshot::paths(cfg)?, &tables, only, REV, out)
}

#[cfg(windows)]
fn lower_priority() {
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, SetPriorityClass, BELOW_NORMAL_PRIORITY_CLASS,
        PROCESS_MODE_BACKGROUND_BEGIN,
    };
    unsafe {
        SetPriorityClass(GetCurrentProcess(), PROCESS_MODE_BACKGROUND_BEGIN);
        SetPriorityClass(GetCurrentProcess(), BELOW_NORMAL_PRIORITY_CLASS);
    }
}

#[cfg(not(windows))]
fn lower_priority() {}

#[cfg(windows)]
fn detach(cmd: &mut Command, breakaway: bool) {
    use std::os::windows::process::CommandExt;
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const BELOW_NORMAL_PRIORITY_CLASS: u32 = 0x0000_4000;
    const CREATE_BREAKAWAY_FROM_JOB: u32 = 0x0100_0000;
    let mut flags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | BELOW_NORMAL_PRIORITY_CLASS;
    if breakaway {
        flags |= CREATE_BREAKAWAY_FROM_JOB;
    }
    cmd.creation_flags(flags);
}

#[cfg(not(windows))]
fn detach(_cmd: &mut Command, _breakaway: bool) {}

fn spawn(root: &Path, config: &Path, scope: &Scope) -> Result<()> {
    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir)?;
    let bin = bin_dir.join(format!("fris-snapshot{}", std::env::consts::EXE_SUFFIX));
    let me = std::env::current_exe()?;
    fs::copy(&me, &bin).with_context(|| {
        format!(
            "copy {} -> {} (already running?)",
            me.display(),
            bin.display()
        )
    })?;
    let config = std::path::absolute(config)?;
    let log_dir = root.join("log");
    fs::create_dir_all(&log_dir)?;
    let console = log_dir.join("console.log");
    let mut args: Vec<String> = vec![
        "--config".into(),
        config.display().to_string(),
        "harvest".into(),
        "--background".into(),
    ];
    for o in &scope.only {
        args.push("--only".into());
        args.push(o.clone());
    }
    if scope.fresh {
        args.push("--fresh".into());
    }
    let launch = |breakaway: bool| -> Result<u32> {
        let out = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&console)?;
        let err = out.try_clone()?;
        let mut cmd = Command::new(&bin);
        cmd.args(&args)
            .current_dir(std::env::current_dir()?)
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err);
        detach(&mut cmd, breakaway);
        Ok(cmd.spawn()?.id())
    };
    let pid = match launch(true) {
        Ok(pid) => pid,
        Err(_) => launch(false)?,
    };
    println!(
        "spawned pid={pid} bin={} console={}",
        bin.display(),
        console.display()
    );
    Ok(())
}

fn control(h: &Handles, c: &Control) -> Result<bool> {
    let dir = &h.control;
    fs::create_dir_all(dir)?;
    match c {
        Control::Bandwidth { rate } => {
            let parsed = fris_snapshot::net::parse_rate(rate).context("unparseable rate")?;
            fs::write(Signal::Bandwidth.path(dir), rate)?;
            println!("bandwidth -> {}", fris_snapshot::net::format_rate(parsed));
        }
        Control::Pause => fs::write(Signal::Pause.path(dir), "")?,
        Control::Resume => {
            let _ = fs::remove_file(Signal::Pause.path(dir));
        }
        Control::Stop => {
            fs::write(Signal::Stop.path(dir), "")?;
            if fris_snapshot::store::held(&h.lock) {
                println!(
                    "stop requested; waiting for {} to be released",
                    h.lock.display()
                );
                while fris_snapshot::store::held(&h.lock) {
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
    }
    Ok(true)
}

fn fris(config: &Path, f: Fris) -> Result<bool> {
    let cfg = Config::load(config)?;
    match f {
        Fris::Plan(s) => fris_snapshot::run(&cfg, &s.only, s.fresh, true),
        Fris::Harvest { scope, background } => {
            if background {
                lower_priority();
            }
            let terminal = fris_snapshot::run(&cfg, &scope.only, scope.fresh, false)?;
            finish(
                terminal,
                &scope,
                &cfg.control_dir(),
                config,
                Some(build_fris),
            )
        }
        Fris::Spawn(s) => {
            if Lock::held(&cfg.source_dir()) {
                bail!("a harvest is already running");
            }
            spawn(&cfg.root, config, &s).map(|()| true)
        }
        Fris::Status { json } => {
            let report = if json {
                fris_snapshot::status::json(&cfg)?
            } else {
                fris_snapshot::status::report(&cfg)?
            };
            print!("{report}");
            Ok(true)
        }
        Fris::Changes => fris_snapshot::changes(&cfg),
        Fris::Fetch {
            entity,
            uuids: Some(file),
            ..
        } => {
            let text =
                fs::read_to_string(&file).with_context(|| format!("read {}", file.display()))?;
            let uuids: Vec<String> = text.lines().map(str::to_owned).collect();
            for (uuid, why) in fris_snapshot::fetch(&cfg, &entity, &uuids)? {
                println!("refused {uuid} {why}");
            }
            Ok(true)
        }
        Fris::Fetch {
            entity,
            uuids: None,
            tables: given,
            out,
        } => {
            let tables = fris_snapshot::build::Tables::load(&tables(config, given.as_ref())?)?;
            print!(
                "{}",
                fris_snapshot::recover(&cfg, &tables, &entity, out.as_deref(), REV)?
            );
            Ok(true)
        }
        Fris::Build {
            tables: given,
            only,
            out,
        } => {
            print!(
                "{}",
                fris_build(
                    &cfg,
                    &tables(config, given.as_ref())?,
                    &only,
                    out.as_deref()
                )?
            );
            Ok(true)
        }
        Fris::Paths { entity, limit } => {
            let out = fris_snapshot::build::paths_inventory(
                &fris_snapshot::paths(&cfg)?,
                &entity,
                limit,
            )?;
            println!("{}", out.display());
            Ok(true)
        }
        Fris::Control(c) => control(&fris_snapshot::handles(&cfg), &c),
    }
}

fn main() -> ExitCode {
    match real_main() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(2),
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn real_main() -> Result<bool> {
    let cli = Cli::parse();
    fris(&cli.config, cli.cmd)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use fris_snapshot::net::Signal;

    use super::{finish, tables, tables_beside, Scope};

    static BUILDS: AtomicUsize = AtomicUsize::new(0);

    fn counted(_: &Path, _: &Path, only: &[String]) -> anyhow::Result<String> {
        anyhow::ensure!(only.is_empty(), "scoped build after a full harvest");
        BUILDS.fetch_add(1, Ordering::SeqCst);
        Ok(String::new())
    }

    #[test]
    fn builds_once_every_unit_is_terminal() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("fris-cli-finish-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)?;
        let config = dir.join("src.toml");
        let stop = Signal::Stop.path(&dir);
        let all = Scope {
            only: Vec::new(),
            fresh: false,
        };
        let some = Scope {
            only: vec!["unit".into()],
            fresh: false,
        };
        assert_eq!(tables_beside(&config), None);
        assert!(tables(&config, None).is_err());
        assert_eq!(tables(&config, Some(&stop))?, stop);
        assert!(finish(true, &all, &dir, &config, Some(counted))?);
        assert_eq!(BUILDS.load(Ordering::SeqCst), 0);
        std::fs::write(dir.join("src-tables.toml"), "")?;
        assert_eq!(tables_beside(&config), Some(dir.join("src-tables.toml")));
        assert_eq!(tables(&config, None)?, dir.join("src-tables.toml"));
        assert!(!finish(false, &all, &dir, &config, Some(counted))?);
        assert!(finish(true, &some, &dir, &config, Some(counted))?);
        assert!(finish(true, &all, &dir, &config, None)?);
        assert_eq!(BUILDS.load(Ordering::SeqCst), 0);
        assert!(finish(true, &all, &dir, &config, Some(counted))?);
        assert_eq!(BUILDS.load(Ordering::SeqCst), 1);
        std::fs::write(&stop, "")?;
        assert!(finish(false, &all, &dir, &config, Some(counted))?);
        assert!(finish(true, &all, &dir, &config, Some(counted))?);
        assert_eq!(BUILDS.load(Ordering::SeqCst), 1);
        std::fs::remove_dir_all(&dir)?;
        let shipped = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config");
        assert!(tables_beside(&shipped.join("fris.toml")).is_some());
        Ok(())
    }
}
