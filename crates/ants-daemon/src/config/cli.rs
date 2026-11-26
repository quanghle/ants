//! The `[cli]` table of the config file: the CLI's own connection and settings ([`CliDefaults`]), and its credentials
//! set after `ants login` ([`with_cli_login`]).

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

use super::{home_dir, resolve};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CliSection {
    pub(super) nats: Option<String>,
    creds: Option<PathBuf>,
    js_domain: Option<JsDomain>,
    human: Option<UserId>,
    #[serde(default, deserialize_with = "ants_proto::duration::deserialize")]
    auto_archive: Duration,
    #[serde(default)]
    summaries: Summaries,
    summary_ant: Option<UserId>,
    #[serde(default)]
    summary_daily_tokens: u64,
    #[serde(default)]
    inline_images: bool,
    #[serde(default)]
    theme: Theme,
    animate: Option<bool>,
    mouse: Option<bool>,
    #[serde(default)]
    notify: Notify,
    web_export: Option<PathBuf>,
}

/// How an unfocused `ants chat` announces a new item on home.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Notify {
    #[default]
    Bell,
    Terminal,
    Off,
}

/// The terminal background the avatars' colours are picked for: `[cli]` `theme`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Theme {
    /// Ask the terminal for its background colour (OSC 11), else go by `COLORFGBG`, else dark.
    #[default]
    Auto,
    Dark,
    Light,
}

impl CliSection {
    /// Whether the section names the CLI's credentials, which only a server taking credentials needs.
    pub(super) fn has_creds(&self) -> bool {
        self.creds.is_some()
    }

    /// What the section says of catch-up summaries: refused for `ahead` with no daily tokens to spend.
    pub(super) fn summary(&self) -> Result<SummaryConfig> {
        if self.summaries == Summaries::Ahead && self.summary_daily_tokens == 0 {
            bail!("[cli] summaries = \"ahead\" needs summary_daily_tokens above 0");
        }
        Ok(SummaryConfig {
            mode: self.summaries,
            ant: self.summary_ant.clone().unwrap_or_else(UserId::messenger),
            daily_tokens: self.summary_daily_tokens,
        })
    }
}

/// When the CLI asks for a thread's TL;DR: `[cli]` `summaries`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Summaries {
    /// Only when a human asks for one.
    #[default]
    OnDemand,
    /// Also ahead of time, for each thread shown with entries newer than its TL;DR, within the daily tokens.
    Ahead,
}

/// What `[cli]` says of catch-up summaries: when to ask, which ant to ask, and how many tokens a human's summaries
/// spend a UTC day at most when asked ahead of time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummaryConfig {
    pub mode: Summaries,
    pub ant: UserId,
    pub daily_tokens: u64,
}

impl Default for SummaryConfig {
    fn default() -> Self {
        Self { mode: Summaries::OnDemand, ant: UserId::messenger(), daily_tokens: 0 }
    }
}

/// What the CLI connects with where neither a flag nor the environment sets it: the `[cli]` section of a config
/// file, else its top-level `nats`, `js_domain` and `human`. Credentials come from `[cli]` alone, an ant's being no human's,
/// as do `auto_archive`, the summary keys, `inline_images`, `theme` and `animate`.
#[derive(Debug, PartialEq)]
pub struct CliDefaults {
    pub nats: Option<String>,
    pub creds: Option<PathBuf>,
    pub js_domain: Option<JsDomain>,
    /// Whom the CLI acts as without credentials.
    pub human: Option<UserId>,
    /// How long a thread goes without a message before `ants thread list`, `ants dash`, `ants channel` and `ants chat`
    /// take it as archived for the CLI's human; None (unset or 0) for never.
    pub auto_archive: Option<Duration>,
    /// When and whom to ask for catch-up summaries.
    pub summaries: SummaryConfig,
    /// Whether `ants chat` draws image attachments in the terminal.
    pub inline_images: bool,
    /// The terminal background the avatars' colours are picked for.
    pub theme: Theme,
    /// Whether `ants chat` moves the avatars with their ants' states; still heads otherwise.
    pub animate: bool,
    pub mouse: bool,
    pub notify: Notify,
    /// The directory `ants web export` last wrote the page into, whose version `ants doctor` checks.
    pub web_export: Option<PathBuf>,
}

impl Default for CliDefaults {
    fn default() -> Self {
        Self {
            nats: None,
            creds: None,
            js_domain: None,
            human: None,
            auto_archive: None,
            summaries: SummaryConfig::default(),
            inline_images: false,
            theme: Theme::Auto,
            animate: true,
            mouse: true,
            notify: Notify::Bell,
            web_export: None,
        }
    }
}

impl CliDefaults {
    /// Read the defaults of the file at `path`; none when there is no file.
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text, &std::path::absolute(path)?, &home_dir()?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e).with_context(|| format!("reading config {}", path.display())),
        }
    }

    /// Only the keys the CLI uses are read and checked, so that a mistake in the ants' settings does not stop every
    /// command; the daemon refuses it.
    pub(super) fn parse(text: &str, path: &Path, home: &Path) -> Result<Self> {
        #[derive(Deserialize)]
        struct View {
            nats: Option<String>,
            js_domain: Option<JsDomain>,
            human: Option<UserId>,
            #[serde(default)]
            cli: CliSection,
        }
        let view: View = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        let summaries = view.cli.summary().with_context(|| format!("config {}", path.display()))?;
        let nats = view.cli.nats.or(view.nats);
        if let Some(url) = &nats {
            crate::bus::check_url(url).with_context(|| format!("config {}", path.display()))?;
        }
        let creds = view.cli.creds.map(|c| resolve(&c, base, home));
        let auto_archive = Some(view.cli.auto_archive).filter(|d| !d.is_zero());
        let js_domain = view.cli.js_domain.or(view.js_domain);
        let human = view.cli.human.or(view.human);
        let (inline_images, theme, animate) =
            (view.cli.inline_images, view.cli.theme, view.cli.animate.unwrap_or(true));
        let mouse = view.cli.mouse.unwrap_or(true);
        let notify = view.cli.notify;
        let web_export = view.cli.web_export.map(|dir| resolve(&dir, base, home));
        Ok(Self {
            nats,
            creds,
            js_domain,
            human,
            auto_archive,
            summaries,
            inline_images,
            theme,
            animate,
            mouse,
            notify,
            web_export,
        })
    }
}

/// The text of the config file at `path` (none when missing) with `[cli]` `web_export` naming `dir`, an absolute
/// path, as `ants web export` records the directory it wrote; None when it names `dir` already. The text around
/// stays as it is, comments included. Fails when the file does not parse, or would not with the key in place.
pub fn with_web_export(path: &Path, dir: &Path) -> Result<Option<String>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("reading config {}", path.display())),
    };
    web_export_text(&text, &std::path::absolute(path)?, &home_dir()?, dir)
}

pub(super) fn web_export_text(text: &str, path: &Path, home: &Path, dir: &Path) -> Result<Option<String>> {
    if CliDefaults::parse(text, path, home)?.web_export.as_deref() == Some(dir) {
        return Ok(None);
    }
    let dir_text = dir.to_str().with_context(|| format!("{} is not UTF-8", dir.display()))?;
    let mut doc: toml_edit::DocumentMut = text.parse().with_context(|| format!("parsing config {}", path.display()))?;
    let cli = doc.entry("cli").or_insert_with(toml_edit::table);
    let cli = cli.as_table_like_mut().with_context(|| format!("config {}: cli is no table", path.display()))?;
    cli.insert("web_export", toml_edit::value(dir_text));
    let new = doc.to_string();
    let fail = || format!("cannot set [cli] in config {}: set web_export = {dir_text:?} there by hand", path.display());
    if CliDefaults::parse(&new, path, home).with_context(fail)?.web_export.as_deref() != Some(dir) {
        bail!(fail());
    }
    Ok(Some(new))
}

/// The text of the config file at `path` (none when missing) with `[cli]` naming the credentials at `creds`, and the
/// server at `nats` unless `[cli]` names one, as `ants login` sets them; None when `[cli]` names credentials already,
/// which stay the CLI's. Returns whether `nats` was set too. The text around stays as it is, comments included: the
/// keys go right after the `[cli]` header, or in a new `[cli]` table at the end. Fails when the file does not parse,
/// or would not with the keys in place.
pub fn with_cli_login(path: &Path, nats: &str, creds: &Path) -> Result<Option<(String, bool)>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).with_context(|| format!("reading config {}", path.display())),
    };
    cli_login_text(&text, &std::path::absolute(path)?, &home_dir()?, nats, creds)
}

pub(super) fn cli_login_text(
    text: &str,
    path: &Path,
    home: &Path,
    nats: &str,
    creds: &Path,
) -> Result<Option<(String, bool)>> {
    CliDefaults::parse(text, path, home)?;
    let table: toml::Table = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
    let cli = table.get("cli").and_then(toml::Value::as_table);
    if cli.is_some_and(|cli| cli.contains_key("creds")) {
        return Ok(None);
    }
    let set_nats = !cli.is_some_and(|cli| cli.contains_key("nats"));
    let quoted = |s: &str| serde_json::to_string(s).expect("a string serializes");
    let creds_text = creds.to_str().with_context(|| format!("{} is not UTF-8", creds.display()))?;
    let mut keys = format!("creds = {}\n", quoted(creds_text));
    if set_nats {
        let _ = writeln!(keys, "nats = {}", quoted(nats));
    }
    let is_header = |line: &str| {
        let Some(rest) = line.trim_start().strip_prefix('[') else { return false };
        let Some((inside, after)) = rest.split_once(']') else { return false };
        let after = after.trim_start();
        inside.trim() == "cli" && (after.is_empty() || after.starts_with('#'))
    };
    let mut new = String::with_capacity(text.len() + keys.len() + 8);
    let mut placed = false;
    for line in text.split_inclusive('\n') {
        new.push_str(line);
        if !placed && is_header(line) {
            if !line.ends_with('\n') {
                new.push('\n');
            }
            new.push_str(&keys);
            placed = true;
        }
    }
    if !placed {
        if !new.is_empty() {
            if !new.ends_with('\n') {
                new.push('\n');
            }
            new.push('\n');
        }
        new.push_str("[cli]\n");
        new.push_str(&keys);
    }
    let fail = || format!("cannot set [cli] in config {}: set creds = {creds_text:?} there by hand", path.display());
    let defaults = CliDefaults::parse(&new, path, home).with_context(fail)?;
    let nats_ok = !set_nats || defaults.nats.as_deref() == Some(nats);
    if defaults.creds.as_deref() != Some(creds) || !nats_ok {
        bail!(fail());
    }
    Ok(Some((new, set_nats)))
}
