use std::path::Path;
use std::process::Command;

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for watched in [".git/HEAD", ".git/index", ".git/refs", "src", "Cargo.lock"] {
        println!("cargo:rerun-if-changed={}", root.join(watched).display());
    }
    let rev = Command::new("git")
        .args(["describe", "--always", "--dirty"])
        .current_dir(root)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=GIT_REV={rev}");
}
