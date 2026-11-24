//! The page's bundle as a build embeds it: its files, their digest, and the pin in `crates/ants/web.sha256` it is
//! checked against. The build script includes this file whole under the `web` feature, which embeds the bundle; the
//! crate's tests include it too. It uses nothing but std and ring, a build dependency of that feature.

use std::path::Path;

/// The file of the bundle naming the ants version it is built for, in a line.
const VERSION_FILE: &str = "ants-version";

/// The page a pin names: the bundle's SHA-256, the ants-web commit it is built from, and the ants version it is built
/// for.
#[derive(Debug, PartialEq, Eq)]
pub struct Pin {
    pub digest: String,
    pub commit: String,
    pub version: String,
}

impl Pin {
    /// The pin of the pin file's `text`: its one line other than comments, `SHA-256  ants-web COMMIT for ants
    /// VERSION`.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty() && !line.starts_with('#'));
        let line = lines.next().ok_or("it pins no page")?;
        if lines.next().is_some() {
            return Err("it pins more than one page".to_owned());
        }
        match line.split_whitespace().collect::<Vec<_>>()[..] {
            [digest, "ants-web", commit, "for", "ants", version]
                if digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) =>
            {
                Ok(Self { digest: digest.to_owned(), commit: commit.to_owned(), version: version.to_owned() })
            }
            _ => Err(format!("`{line}` is not `SHA-256  ants-web COMMIT for ants VERSION`")),
        }
    }

    /// The build the pin names, as messages show it.
    pub fn build(&self) -> String {
        format!("ants-web {} for ants {}", self.commit, self.version)
    }
}

/// The paths of the files under `dir`, `/`-separated and sorted by their bytes, as ants-web's build lists them for
/// the digest: regular files only, symbolic links neither followed nor listed.
pub fn files(dir: &Path) -> std::io::Result<Vec<String>> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<String>) -> std::io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name().into_string().map_err(|name| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, format!("{name:?}: a file name not in UTF-8"))
            })?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                walk(&entry.path(), &format!("{prefix}{name}/"), out)?;
            } else if kind.is_file() {
                out.push(format!("{prefix}{name}"));
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    walk(dir, "", &mut out)?;
    out.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    Ok(out)
}

/// The digest of the bundle of `files` under `dir`: the SHA-256 of its listing, a line `<SHA-256>  <path>` per file
/// in order, as `shasum -a 256` prints them.
pub fn digest(dir: &Path, files: &[String]) -> std::io::Result<String> {
    let mut listing = String::new();
    for path in files {
        listing.push_str(&sha256(&std::fs::read(dir.join(path))?));
        listing.push_str("  ");
        listing.push_str(path);
        listing.push('\n');
    }
    Ok(sha256(listing.as_bytes()))
}

fn sha256(data: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, data);
    digest.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// The files of the bundle in `dir`, checked to be the page `pin` names and built for the ants `version`.
pub fn check(dir: &Path, pin: &Pin, version: &str) -> Result<Vec<String>, String> {
    if pin.version != version {
        return Err(format!("the pinned page is {}, not for ants {version}", pin.build()));
    }
    let shown = dir.display();
    let files = files(dir).map_err(|e| format!("reading {shown}: {e}"))?;
    if !files.iter().any(|path| path == "index.html") {
        return Err(format!("{shown} holds no index.html"));
    }
    let digest = digest(dir, &files).map_err(|e| format!("reading {shown}: {e}"))?;
    if digest != pin.digest {
        return Err(format!("the bundle in {shown} has the digest {digest}, not that of the pinned {}", pin.build()));
    }
    let built_for = std::fs::read_to_string(dir.join(VERSION_FILE)).map_err(|e| format!("reading {shown}: {e}"))?;
    if built_for.trim() != version {
        return Err(format!("the bundle in {shown} is built for ants {}, not {version}", built_for.trim()));
    }
    Ok(files)
}
