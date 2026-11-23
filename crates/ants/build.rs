//! With the `web` feature, embed the page of the web client: the bundle `scripts/web.sh` puts in `target/web` (or
//! the directory `ANTS_WEB_BUNDLE` names), checked to be the one `web.sha256` pins and built for this ants version,
//! its files included in the binary by `include_bytes!` (`src/cmd/web.rs`). Without it, nothing:
//! a build needs neither the bundle nor what builds it.

#[cfg(feature = "web")]
#[path = "src/cmd/web/bundle.rs"]
mod bundle;

fn main() {
    println!("cargo::rerun-if-changed=build.rs");
    #[cfg(feature = "web")]
    if let Err(e) = embed() {
        println!("cargo::error={e}");
    }
}

#[cfg(feature = "web")]
fn embed() -> Result<(), String> {
    use std::fmt::Write as _;
    use std::path::{Path, PathBuf};

    let env = |name: &str| std::env::var(name).map_err(|e| format!("{name}: {e}"));
    let manifest = PathBuf::from(env("CARGO_MANIFEST_DIR")?);
    let pin_file = manifest.join("web.sha256");
    println!("cargo::rerun-if-changed={}", pin_file.display());
    println!("cargo::rerun-if-env-changed=ANTS_WEB_BUNDLE");
    let dir = match std::env::var_os("ANTS_WEB_BUNDLE") {
        Some(dir) => PathBuf::from(dir),
        None => manifest.join("../../target/web"),
    };
    let dir = std::path::absolute(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    println!("cargo::rerun-if-changed={}", dir.display());

    let pin = std::fs::read_to_string(&pin_file).map_err(|e| format!("reading {}: {e}", pin_file.display()))?;
    let pin = bundle::Pin::parse(&pin).map_err(|e| format!("{}: {e}", pin_file.display()))?;
    if !dir.is_dir() {
        return Err(format!(
            "the `web` feature embeds the page scripts/web.sh puts in {}, and there is none: run scripts/web.sh",
            dir.display()
        ));
    }
    let files = bundle::check(&dir, &pin, &env("CARGO_PKG_VERSION")?)
        .map_err(|e| format!("{e}: run scripts/web.sh for the pinned page"))?;

    let mut out = String::new();
    let _ = writeln!(out, "/// The ants-web build of the page, as `web.sha256` names it.");
    let _ = writeln!(out, "pub(crate) const BUILD: &str = {:?};", pin.build());
    let _ = writeln!(out, "/// The page's files: each path, `/`-separated, with its contents.");
    let _ = writeln!(out, "pub(crate) static FILES: &[(&str, &[u8])] = &[");
    for path in &files {
        let file: &Path = &dir.join(path);
        let _ = writeln!(out, "    ({path:?}, include_bytes!({:?})),", file.display().to_string());
    }
    let _ = writeln!(out, "];");
    let generated = PathBuf::from(env("OUT_DIR")?).join("page.rs");
    std::fs::write(&generated, out).map_err(|e| format!("writing {}: {e}", generated.display()))
}
