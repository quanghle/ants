//! Changes to the config file's text that leave the text around as it is, comments included: an entry appended, an
//! allow rule added, an ant's table removed, each checked to load before the file is written whole ([`write()`]); and
//! the template an ant's entry names ([`template_of`]).

use std::path::{Path, PathBuf};

use ants_proto::UserId;
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

use super::{home_dir, parse, toml_string};

/// Add `entry` ([`entry`]) at the end of the config file at `path`, creating the file (and its directory) when
/// missing. The text already there stays as it is, comments included; the file must still parse ([`parse`]) with the
/// entry, or nothing is written.
///
/// [`entry`]: fn@super::entry
pub fn append(path: &Path, entry: &str) -> Result<()> {
    let old = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("reading config {}", path.display())),
    };
    let mut text = old;
    if !text.is_empty() {
        if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push('\n');
    }
    text.push_str(entry);
    parse(&text, &std::path::absolute(path)?, &home_dir()?)?;
    if let Some(dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    write(path, &text)
}

/// Write `text` to the config file at `path`: beside it first, with the old file's permissions, flushed to disk, then
/// renamed over it, so a failure leaves the old file whole.
pub fn write(path: &Path, text: &str) -> Result<()> {
    use std::io::Write as _;
    let mut new = path.as_os_str().to_owned();
    new.push(".new");
    let new = PathBuf::from(new);
    let written = (|| {
        let mut file = std::fs::File::create(&new)?;
        file.write_all(text.as_bytes())?;
        if let Ok(old) = std::fs::metadata(path) {
            file.set_permissions(old.permissions())?;
        }
        file.sync_all()
    })();
    written.with_context(|| format!("writing {}", new.display()))?;
    std::fs::rename(&new, path).with_context(|| format!("writing config {}", path.display()))
}

/// The text of the config file at `path` with `rule` added to the `allow` list of `ant`'s `[[ant]]` table, or None
/// when the list holds it already. The text around stays as it is, comments included: the rule goes after the list's
/// last item, or a new `allow` line after the table's `name`. Fails, leaving the file alone, when the file does not
/// list the ant or does not parse ([`parse`]) with the rule in place.
pub fn with_allow_rule(path: &Path, ant: &UserId, rule: &str) -> Result<Option<String>> {
    #[derive(Deserialize)]
    struct Spans {
        #[serde(default, rename = "ant")]
        ants: Vec<AntSpans>,
    }
    #[derive(Deserialize)]
    struct AntSpans {
        name: toml::Spanned<String>,
        allow: Option<toml::Spanned<Vec<toml::Spanned<String>>>>,
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    let spans: Spans = toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
    let entry = spans
        .ants
        .into_iter()
        .find(|entry| entry.name.get_ref() == ant.as_str())
        .with_context(|| format!("config {} lists no ant {ant}", path.display()))?;
    let (at, insert) = match &entry.allow {
        Some(allow) if allow.get_ref().iter().any(|r| r.get_ref() == rule) => return Ok(None),
        Some(allow) => match allow.get_ref().last() {
            Some(last) => (last.span().end, format!(", {}", toml_string(rule))),
            // Just inside the `[`.
            None => (allow.span().start + 1, toml_string(rule)),
        },
        None => {
            let end = entry.name.span().end;
            match text[end..].find('\n') {
                Some(newline) => (end + newline + 1, format!("allow = [{}]\n", toml_string(rule))),
                None => (text.len(), format!("\nallow = [{}]\n", toml_string(rule))),
            }
        }
    };
    let mut new = text;
    new.insert_str(at, &insert);
    let loaded = parse(&new, &std::path::absolute(path)?, &home_dir()?)?;
    if !loaded.ants.iter().any(|a| &a.name == ant && a.allow.iter().any(|r| r == rule)) {
        bail!("cannot add {} to the allow list of {ant} in {}: add it by hand", toml_string(rule), path.display());
    }
    Ok(Some(new))
}

/// The text of the config file at `path` without the `[[ant]]` table of `ant`: from its header up to the next table's
/// header, the comment lines right above that one left to it. The text around stays as it is, comments included.
/// Fails, leaving the file alone, when the file does not list the ant, or when the cut would change anything else or
/// leave a file that does not parse ([`parse`]).
pub fn without_ant(path: &Path, ant: &UserId) -> Result<String> {
    #[derive(Deserialize)]
    struct Spans {
        #[serde(default, rename = "ant")]
        ants: Vec<Named>,
    }
    #[derive(Deserialize)]
    struct Named {
        name: toml::Spanned<String>,
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    let spans: Spans = toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
    let index = spans
        .ants
        .iter()
        .position(|entry| entry.name.get_ref() == ant.as_str())
        .with_context(|| format!("config {} lists no ant {ant}", path.display()))?;
    let at = spans.ants[index].name.span().start;
    let by_hand = || format!("cannot remove the [[ant]] table of {ant} from {}: remove it by hand", path.display());
    let mut lines = Vec::new();
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        lines.push((offset, line));
        offset += line.len();
    }
    let bare = |line: &str| -> String {
        line.split('#').next().unwrap_or_default().chars().filter(|c| !c.is_whitespace()).collect()
    };
    let header = |line: &str| {
        let bare = bare(line);
        bare.starts_with('[') && bare.ends_with(']')
    };
    let first =
        lines.iter().rposition(|(start, line)| *start <= at && bare(line) == "[[ant]]").with_context(by_hand)?;
    let next = lines.iter().skip(first + 1).position(|(_, line)| header(line)).map(|n| first + 1 + n);
    let (mut from, mut to) = (first, next.unwrap_or(lines.len()));
    match next {
        Some(_) => {
            while to > from + 1 && lines[to - 1].1.trim_start().starts_with('#') {
                to -= 1;
            }
        }
        // Last in the file: the blank lines that set it apart go too.
        None => {
            while from > 0 && lines[from - 1].1.trim().is_empty() {
                from -= 1;
            }
        }
    }
    let end = lines.get(to).map_or(text.len(), |(start, _)| *start);
    let new = format!("{}{}", &text[..lines[from].0], &text[end..]);
    let mut expected: toml::Table = toml::from_str(&text)?;
    if let Some(toml::Value::Array(ants)) = expected.get_mut("ant") {
        ants.remove(index);
        if ants.is_empty() {
            expected.remove("ant");
        }
    }
    if toml::from_str::<toml::Table>(&new).ok() != Some(expected) {
        bail!(by_hand());
    }
    parse(&new, &std::path::absolute(path)?, &home_dir()?)?;
    Ok(new)
}

/// The template the config file at `path` adds `ant` from, None when the file lists the ant without one. Fails when the
/// file does not list the ant or does not parse as TOML.
pub fn template_of(path: &Path, ant: &UserId) -> Result<Option<String>> {
    #[derive(Deserialize)]
    struct Names {
        #[serde(default, rename = "ant")]
        ants: Vec<Named>,
    }
    #[derive(Deserialize)]
    struct Named {
        name: String,
        template: Option<String>,
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    let names: Names = toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
    let entry = names
        .ants
        .into_iter()
        .find(|entry| entry.name == ant.as_str())
        .with_context(|| format!("config {} lists no ant {ant}", path.display()))?;
    Ok(entry.template)
}
