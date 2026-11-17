// Embeds the commit the binary was built from, for manifests and frames headers.
fn main() {
    let commit = std::process::Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .filter(|s| !s.is_empty());
    let dirty = std::process::Command::new("git")
        .args(["diff", "--quiet", "HEAD"])
        .status()
        .map(|s| !s.success())
        .unwrap_or(false);
    if let Some(c) = commit {
        println!(
            "cargo:rustc-env=MARKETREC_GIT={c}{}",
            if dirty { "-dirty" } else { "" }
        );
    }
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
    println!("cargo:rerun-if-changed=src"); // keeps the -dirty mark honest
}
