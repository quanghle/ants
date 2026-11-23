//! Shell completions: `ants completions SHELL` prints the script registering them for bash, zsh or fish, and
//! `--install` writes it where the shell loads it from. The script asks `ants` itself for the candidates (clap's
//! dynamic completion, with `ANTS_COMPLETE` set), so they follow the installed version: subcommands and flags, and
//! values looked up as the command line is completed: ants, channels, recent threads and messages, pending tool
//! requests and dead letters. A lookup gives up after [`TIMEOUT`], and on any error completes nothing: it never
//! prints an error or asks for a password, and reads no seed from the OS credential store.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::{CliDefaults, Summary};
use ants_daemon::delivery::dead;
use ants_daemon::secrets::keystore::Store;
use ants_daemon::{bus, colony};
use ants_proto::{CHANNELS_BUCKET, COLONY_BUCKET, JsDomain, STREAM, UserId};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream;
use clap::CommandFactory as _;
use clap_complete::engine::CompletionCandidate;
use clap_complete::env::{CompleteEnv, Shells};
use futures::TryStreamExt as _;

use crate::people::humans;
use crate::threads::{thread, thread_meta};
use crate::{defaults, tools};

/// The environment variable the scripts set to ask `ants` for completions.
pub const VAR: &str = "ANTS_COMPLETE";

/// How long a lookup of values over NATS may take.
const TIMEOUT: Duration = Duration::from_secs(2);

/// How many messages a completion of a message id offers at most.
const MESSAGES: usize = 50;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Shell {
    Bash,
    Zsh,
    Fish,
}

impl Shell {
    fn name(self) -> &'static str {
        match self {
            Shell::Bash => "bash",
            Shell::Zsh => "zsh",
            Shell::Fish => "fish",
        }
    }

    /// The shell `$SHELL` names, if one of these.
    pub fn login() -> Option<Self> {
        let shell = PathBuf::from(std::env::var_os("SHELL")?);
        match shell.file_name()?.to_str()? {
            "bash" => Some(Shell::Bash),
            "zsh" => Some(Shell::Zsh),
            "fish" => Some(Shell::Fish),
            _ => None,
        }
    }

    /// The script registering the completions of `ants`, which runs `bin` for the candidates.
    pub fn script(self, bin: &str) -> Result<String> {
        let mut out = Vec::new();
        let shells = Shells::builtins();
        let completer = shells.completer(self.name()).ok_or_else(|| anyhow!("no completer for {}", self.name()))?;
        completer.write_registration(VAR, "ants", "ants", bin, &mut out)?;
        let mut script = String::from_utf8(out)?;
        if self == Shell::Zsh {
            // Loaded from fpath, the file is the body of `_ants`, run on the first completion: it defines and
            // registers the completer then, and must also complete.
            script.push_str("[[ $funcstack[1] == _ants ]] && _clap_dynamic_completer_ants \"$@\"\n");
        }
        Ok(script)
    }

    /// Where the shell loads the completions of `ants` from, for a user whose home is `home` and whose environment
    /// variables `env` reads.
    fn target(self, home: &Path, env: &dyn Fn(&str) -> Option<OsString>) -> PathBuf {
        let xdg = |var: &str, default: &str| match env(var).map(PathBuf::from) {
            Some(dir) if dir.is_absolute() => dir,
            _ => home.join(default),
        };
        match self {
            Shell::Bash => xdg("XDG_DATA_HOME", ".local/share").join("bash-completion/completions/ants"),
            Shell::Zsh => xdg("XDG_DATA_HOME", ".local/share").join("zsh/site-functions/_ants"),
            Shell::Fish => xdg("XDG_CONFIG_HOME", ".config").join("fish/completions/ants.fish"),
        }
    }

    /// What to do so the shell loads the file at `target`, when it may not by itself.
    fn hint(self, home: &Path, target: &Path) -> Option<String> {
        match self {
            Shell::Bash => Some(format!(
                "bash-completion 2 loads it; without bash-completion, add `source {}` to ~/.bashrc",
                target.display()
            )),
            Shell::Zsh => {
                let dir = target.parent()?;
                let zshrc = std::fs::read_to_string(home.join(".zshrc")).unwrap_or_default();
                let named = |d: &Path| d.to_str().is_some_and(|d| zshrc.contains(d));
                let tilde = dir.strip_prefix(home).ok().map(|rest| Path::new("~").join(rest));
                if named(dir) || tilde.as_deref().is_some_and(named) {
                    return None;
                }
                Some(format!("add `fpath=({} $fpath)` to ~/.zshrc, before compinit", dir.display()))
            }
            Shell::Fish => None,
        }
    }
}

impl std::fmt::Display for Shell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// What [`install`] did with the script.
#[derive(Debug, PartialEq, Eq)]
pub enum Installed {
    New,
    Updated,
    Unchanged,
}

/// The installed completions of one shell: where, what was done, and what else the shell may need.
pub struct Install {
    pub shell: Shell,
    pub path: PathBuf,
    pub installed: Installed,
    pub hint: Option<String>,
}

impl std::fmt::Display for Install {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let done = match self.installed {
            Installed::New => "installed",
            Installed::Updated => "updated",
            Installed::Unchanged => "up to date",
        };
        write!(f, "{} completions {done}: {}", self.shell, self.path.display())?;
        if let Some(hint) = &self.hint {
            write!(f, "\n  {hint}")?;
        }
        Ok(())
    }
}

/// `ants completions`: print the script of `shell` (default: the login shell's), or install it.
pub fn run(shell: Option<Shell>, install: bool) -> Result<()> {
    let shell = match shell.or_else(Shell::login) {
        Some(shell) => shell,
        None => bail!("$SHELL is not bash, zsh or fish: name the shell"),
    };
    if install {
        println!("{}", self::install(shell, &std::env::home_dir().context("no home directory")?, false)?);
    } else {
        print!("{}", shell.script(&bin())?);
    }
    Ok(())
}

/// Write the completions script of `shell` where the shell of a user whose home is `home` loads it from, unless
/// it is there already; with `dry_run`, only say what would be done.
pub fn install(shell: Shell, home: &Path, dry_run: bool) -> Result<Install> {
    install_in(shell, home, &|var| std::env::var_os(var), dry_run)
}

fn install_in(shell: Shell, home: &Path, env: &dyn Fn(&str) -> Option<OsString>, dry_run: bool) -> Result<Install> {
    let path = shell.target(home, env);
    let script = shell.script(&bin())?;
    let installed = match std::fs::read_to_string(&path) {
        Ok(text) if text == script => Installed::Unchanged,
        Ok(_) => Installed::Updated,
        Err(_) => Installed::New,
    };
    if installed != Installed::Unchanged && !dry_run {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(&path, &script)?;
    }
    let hint = shell.hint(home, &path);
    Ok(Install { shell, path, installed, hint })
}

/// How the scripts run `ants`: as invoked, when found on PATH by name, else by its absolute path, as clap's
/// [`CompleteEnv`] does.
pub fn bin() -> String {
    let arg0 = std::env::args_os().next().map_or_else(|| "ants".into(), PathBuf::from);
    let path = match arg0.components().count() {
        1 => arg0,
        _ => std::path::absolute(&arg0).unwrap_or(arg0),
    };
    path.to_string_lossy().into_owned()
}

/// When a script asks for completions, print them and exit; otherwise return. Runs before anything else may print.
pub fn answer() {
    CompleteEnv::with_factory(crate::cli::Cli::command).var(VAR).complete();
}

/// The ants of the config file and the colony, and the messenger.
pub fn ants() -> Vec<CompletionCandidate> {
    let mut ants = known_ants();
    ants.sort();
    ants.dedup();
    ants.into_iter().map(CompletionCandidate::new).collect()
}

/// [`ants`] as `@ant`, the recipient of `ants ask` or a mention.
pub fn mentions() -> Vec<CompletionCandidate> {
    let mut ants = known_ants();
    ants.sort();
    ants.dedup();
    ants.into_iter().map(|ant| CompletionCandidate::new(format!("@{ant}"))).collect()
}

/// The channels.
pub fn channels() -> Vec<CompletionCandidate> {
    lookup(async |js, _| {
        let kv = js.get_key_value(CHANNELS_BUCKET).await?;
        let keys: Vec<String> = kv.keys().await?.try_collect().await?;
        Ok(keys.into_iter().map(CompletionCandidate::new).collect())
    })
}

/// `last` and the threads the CLI's human took part in recently, most recent first, each with its title, else its
/// latest text.
pub fn threads() -> Vec<CompletionCandidate> {
    let last = CompletionCandidate::new("last").help(Some("the latest thread".into()));
    let creds = Connection::of(&words()).creds;
    let rights = humans::Rights::from_creds(creds.as_deref());
    let titled = rights.is_ok_and(|r| r.may_publish(&thread_meta::titles_right()));
    let recent = lookup(async |js, me| {
        let entries = thread::recent_threads(js, me).await?;
        let mut titles = if titled {
            match thread_meta::Meta::open(js, None, me).await {
                Ok(meta) => meta
                    .opening(
                        crate::people::sealed::Keyring::lazy(creds.as_deref(), &NoStore, js).map(std::sync::Arc::new),
                    )
                    .titles(entries.iter().map(|e| &e.thread_id))
                    .await
                    .unwrap_or_default(),
                Err(_) => Default::default(),
            }
        } else {
            Default::default()
        };
        Ok(entries
            .into_iter()
            .enumerate()
            .map(|(i, e)| {
                let help = match titles.remove(&e.thread_id) {
                    Some(title) => title.title,
                    None => e.text.lines().next().unwrap_or_default().chars().take(60).collect::<String>(),
                };
                CompletionCandidate::new(e.thread_id.to_string()).help(Some(help.into())).display_order(Some(i))
            })
            .collect())
    });
    std::iter::once(last).chain(recent).collect()
}

/// The latest messages to or from the CLI's human whose id starts with `current`, most recent first, each with its
/// route and first line.
pub fn messages(current: &OsStr) -> Vec<CompletionCandidate> {
    let current = current.to_string_lossy().to_ascii_lowercase();
    lookup(async |js, me| {
        let messages = thread::recent_messages(js, me).await?;
        Ok(messages
            .into_iter()
            .filter_map(|e| Some((e.id?.hyphenated().to_string(), e)))
            .filter(|(id, _)| id.starts_with(&current))
            .take(MESSAGES)
            .enumerate()
            .map(|(i, (id, e))| {
                let help = format!("{}: {}", e.route(), crate::text::excerpt(&e.text, 60));
                CompletionCandidate::new(id).help(Some(help.into())).display_order(Some(i))
            })
            .collect())
    })
}

/// The reactions suggested ([`ants_daemon::records::reactions::SUGGESTED`]); any other of the same syntax is taken too.
pub fn reactions() -> Vec<CompletionCandidate> {
    ants_daemon::records::reactions::SUGGESTED.into_iter().map(CompletionCandidate::new).collect()
}

/// The pending tool requests, as `<ant>/<id>`.
pub fn tool_requests() -> Vec<CompletionCandidate> {
    lookup(async |js, _| {
        let stream = js.get_stream(STREAM).await?;
        let pending = tools::pending(js, &stream, chrono::Utc::now()).await?;
        Ok(pending
            .iter()
            .map(|r| CompletionCandidate::new(tools::qualified(r)).help(Some(r.tool.title.clone().into())))
            .collect())
    })
}

/// The dead letters' sequence numbers.
pub fn dead_letters() -> Vec<CompletionCandidate> {
    lookup(async |js, _| {
        let stream = js.get_stream(STREAM).await?;
        let letters = dead::list(&stream).await?;
        Ok(letters
            .into_iter()
            .map(|(seq, l)| {
                let error = l.error.lines().next().unwrap_or_default().chars().take(50).collect::<String>();
                CompletionCandidate::new(seq.to_string()).help(Some(format!("{}: {error}", l.ant).into()))
            })
            .collect())
    })
}

fn known_ants() -> Vec<String> {
    let connection = Connection::of(&words());
    let mut ants = vec![UserId::messenger().to_string()];
    if let Ok(summary) = Summary::load(&connection.config) {
        ants.extend(summary.ants.into_iter().map(|(ant, _)| ant.to_string()));
    }
    ants.extend(lookup(async |js, _| {
        let kv = js.get_key_value(COLONY_BUCKET).await?;
        let entries = colony::list_entries(&kv).await?;
        Ok(entries.into_iter().map(|e| e.ant.to_string()).collect())
    }));
    ants
}

/// The command line being completed: the words after the script's `--`.
fn words() -> Vec<String> {
    let args: Vec<String> = std::env::args_os().map(|a| a.to_string_lossy().into_owned()).collect();
    match args.iter().position(|a| a == "--") {
        Some(i) => args[i + 1..].to_vec(),
        None => Vec::new(),
    }
}

/// How the command being completed would reach NATS, as far as its own flags, the environment and the config file
/// say.
#[derive(Debug, PartialEq, Eq)]
struct Connection {
    nats: String,
    creds: Option<PathBuf>,
    js_domain: Option<JsDomain>,
    config: PathBuf,
}

impl Connection {
    fn of(words: &[String]) -> Self {
        let flag = |name: &str| {
            let long = format!("--{name}");
            let mut found = None;
            for (i, word) in words.iter().enumerate() {
                if word == "--" {
                    break;
                }
                if *word == long {
                    found = words.get(i + 1).cloned().or(found);
                } else if let Some(value) = word.strip_prefix(&format!("{long}=")) {
                    found = Some(value.to_owned());
                }
            }
            found.or_else(|| std::env::var_os(env_of(name)).map(|v: OsString| v.to_string_lossy().into_owned()))
        };
        fn env_of(name: &str) -> &'static str {
            match name {
                "nats" => "ANTS_NATS_URL",
                "creds" => "ANTS_CREDS",
                "js-domain" => "ANTS_JS_DOMAIN",
                _ => ants_daemon::config::ENV,
            }
        }
        let config = flag("config").filter(|c| !c.is_empty()).map(PathBuf::from).or_else(|| defaults::path().ok());
        let config = config.unwrap_or_default();
        let file = CliDefaults::load(&config).unwrap_or_default();
        Connection {
            nats: flag("nats").or(file.nats).unwrap_or_else(|| crate::cli::DEFAULT_NATS.to_owned()),
            creds: flag("creds").map(PathBuf::from).or(file.creds),
            js_domain: flag("js-domain").and_then(|d| d.parse().ok()).or(file.js_domain),
            config,
        }
    }
}

/// A seed store that has none, so a lookup never reads the OS credential store, which may ask for a password.
struct NoStore;

impl Store for NoStore {
    fn get(&self, _: &str) -> Result<Option<String>> {
        Ok(None)
    }

    fn set(&self, _: &str, _: &str) -> Result<()> {
        Err(anyhow!("no credential store while completing"))
    }

    fn delete(&self, _: &str) -> Result<()> {
        Err(anyhow!("no credential store while completing"))
    }
}

/// What `find` finds over NATS, connected as the command being completed would be, within [`TIMEOUT`]; nothing
/// on any error.
fn lookup<T>(find: impl AsyncFnOnce(&jetstream::Context, &UserId) -> Result<Vec<T>>) -> Vec<T> {
    let connection = Connection::of(&words());
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread().enable_all().build() else { return Vec::new() };
    runtime.block_on(async {
        let found = tokio::time::timeout(TIMEOUT, async {
            let me = humans::Me::from_creds(connection.creds.as_deref())?;
            let creds = connection.creds.as_deref();
            let domain = connection.js_domain.as_ref();
            let (_client, js) =
                bus::connect_with(&connection.nats, creds, &NoStore, domain, &me.id, "ants-cli complete").await?;
            find(&js, &me.id).await
        });
        found.await.ok().and_then(Result::ok).unwrap_or_default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(line: &str) -> Vec<String> {
        line.split(' ').map(str::to_owned).collect()
    }

    #[test]
    fn the_connection_comes_from_the_command_line_being_completed_then_the_config_file() {
        let dir = ants_test_support::scratch_dir("complete-connection").canonicalize().unwrap();
        let config = dir.join("config.toml");
        std::fs::write(&config, "nats = \"nats://leaf:4222\"\n[cli]\ncreds = \"me.creds\"\n").unwrap();
        let config_flag = format!("--config={}", config.display());
        let from_file = Connection::of(&words(&format!("ants spend {config_flag} ")));
        assert_eq!(from_file.nats, "nats://leaf:4222");
        assert_eq!(from_file.creds, Some(dir.join("me.creds")));
        let line = format!("ants --nats nats://hub:4222 spend --creds=other.creds {config_flag} --js-domain hub x");
        let flags = Connection::of(&words(&line));
        assert_eq!(flags.nats, "nats://hub:4222");
        assert_eq!(flags.creds, Some(PathBuf::from("other.creds")));
        assert_eq!(flags.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn scripts_run_ants_for_the_candidates_and_install_once() {
        let home = ants_test_support::scratch_dir("complete-install");
        let install = |shell, dry_run| install_in(shell, &home, &|_| None, dry_run);
        for shell in [Shell::Bash, Shell::Zsh, Shell::Fish] {
            let script = shell.script("/opt/ants dir/ants").unwrap();
            assert!(script.contains(&format!("{VAR}=")) || script.contains(VAR), "{shell}: {script}");
            assert!(script.contains("'/opt/ants dir/ants'"), "{shell}: {script}");
        }
        assert!(Shell::Zsh.script("ants").unwrap().starts_with("#compdef ants"));

        let dry = install(Shell::Fish, true).unwrap();
        assert_eq!(dry.installed, Installed::New);
        assert!(!dry.path.exists(), "a dry run writes nothing");
        assert_eq!(install(Shell::Fish, false).unwrap().installed, Installed::New);
        assert_eq!(install(Shell::Fish, false).unwrap().installed, Installed::Unchanged);
        let zsh = install(Shell::Zsh, false).unwrap();
        assert!(zsh.path.ends_with("_ants") && zsh.hint.is_some_and(|h| h.contains("fpath=(")));
        std::fs::write(home.join(".zshrc"), "fpath=(~/.local/share/zsh/site-functions $fpath)\n").unwrap();
        assert!(install(Shell::Zsh, false).unwrap().hint.is_none(), "already on fpath");
        std::fs::remove_dir_all(&home).unwrap();
    }
}
