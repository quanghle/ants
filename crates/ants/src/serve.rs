//! `ants daemon`: which ants to serve, from the config file and the command line, and serving them together.

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::{self, Ant, Summary};
use ants_daemon::{Config, Harnesses};
use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use tracing::Instrument as _;

#[derive(clap::Args, Debug)]
pub struct DaemonArgs {
    /// Serve only this ant: picked from the config file when one is given, described by the flags below otherwise.
    #[arg(long, env = "ANTS_ANT")]
    ant: Option<UserId>,
    /// Config file listing the ants to serve (default without --ant: ~/.ants/config.toml, if it exists).
    #[arg(long, env = "ANTS_CONFIG")]
    config: Option<PathBuf>,
    #[command(flatten)]
    options: AntOptions,
}

/// The settings of an ant on the command line, of `ants daemon` and `ants add`.
#[derive(clap::Args, Debug, Default)]
pub struct AntOptions {
    /// Directory the harness works in (default: the current directory).
    #[arg(long)]
    workdir: Option<PathBuf>,
    /// The ant's own files (default: ~/.ants/<ant>).
    #[arg(long)]
    home: Option<PathBuf>,
    /// Role instructions shared in the roster and prepended to every prompt.
    #[arg(long)]
    whoami: Option<String>,
    /// Instructions for the memory tool available through the harness; prepended to every prompt.
    #[arg(long)]
    memory: Option<String>,
    /// Instructions for the task tool available through the harness; prepended to every prompt.
    #[arg(long)]
    tasks: Option<String>,
    /// Add NAME=VALUE to the harness environment (repeatable); overrides the file's entry for NAME.
    #[arg(long = "env", value_name = "NAME=VALUE", value_parser = env_pair)]
    env: Vec<(String, String)>,
    /// Agent harness: `copilot` (default), `claude`, `antigravity`, or a name the config file defines under
    /// `[harnesses.NAME]`.
    #[arg(long)]
    harness: Option<String>,
    /// Model ID advertised by the harness. Omitted: keep the harness default.
    #[arg(long)]
    model: Option<String>,
    /// Reasoning effort value advertised by the selected model.
    #[arg(long)]
    reasoning_effort: Option<String>,
    /// Provider-native context: Copilot default/long_context, Claude default/1m.
    #[arg(long)]
    context: Option<String>,
    /// Let the harness run tool requests matching RULE without asking: `*`, a tool kind such as `read`, or
    /// `<kind>:<pattern>` such as `execute:cargo test*` or `edit:src/*`. Repeatable, added to the file's `allow`.
    /// Other requests go to the human.
    #[arg(long = "allow", value_name = "RULE")]
    allow: Vec<String>,
    /// Allow every tool request (same as `--allow '*'`). Only for trusted, sandboxed workdirs.
    #[arg(long)]
    allow_tools: bool,
    /// How long a tool request outside the allowlist waits for the human's decision before it is rejected (a
    /// duration such as 90s, 10m or 1h; default: 5m).
    #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse)]
    approval_timeout: Option<Duration>,
    /// Most harness processes alive at once, one per active thread (default: 4).
    #[arg(long)]
    max_harnesses: Option<usize>,
    /// How long a thread's harness stays without a message before it stops; its session resumes on the next message
    /// (a duration such as 90s, 10m or 1h; default: 10m).
    #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse)]
    idle_timeout: Option<Duration>,
    /// How long a prompt may run, tool requests waiting for the human included, before it is cancelled and the thread
    /// goes to the human (a duration such as 90s, 10m or 1h; 0 for no limit; default: 30m).
    #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse)]
    prompt_timeout: Option<Duration>,
    /// Tool calls a prompt may start before it is cancelled and the thread goes to the human; 0 for no limit
    /// (default: 300).
    #[arg(long)]
    max_tool_calls: Option<usize>,
    /// Tokens a thread may spend between messages from the human, as the harness reports them (default: no cap).
    #[arg(long)]
    max_thread_tokens: Option<u64>,
    /// Cost a thread may spend between messages from the human, in the harness's unit such as USD (default: no cap).
    #[arg(long)]
    max_thread_cost: Option<f64>,
    /// Tokens the ant may spend in a UTC day (default: no cap).
    #[arg(long)]
    max_daily_tokens: Option<u64>,
    /// Cost the ant may spend in a UTC day (default: no cap).
    #[arg(long)]
    max_daily_cost: Option<f64>,
    /// Tag the ant is good for, listed in its roster entry for routing (repeatable, or comma-separated). Replaces
    /// the file's `capabilities`.
    #[arg(long = "capability", value_name = "TAG", value_delimiter = ',')]
    capabilities: Vec<String>,
}

impl AntOptions {
    /// Whether any setting is given.
    pub fn sets_any(&self) -> bool {
        self.workdir.is_some()
            || self.home.is_some()
            || self.whoami.is_some()
            || self.memory.is_some()
            || self.tasks.is_some()
            || !self.env.is_empty()
            || self.harness.is_some()
            || self.model.is_some()
            || self.reasoning_effort.is_some()
            || self.context.is_some()
            || !self.allow.is_empty()
            || self.allow_tools
            || self.approval_timeout.is_some()
            || self.max_harnesses.is_some()
            || self.idle_timeout.is_some()
            || self.prompt_timeout.is_some()
            || self.max_tool_calls.is_some()
            || self.max_thread_tokens.is_some()
            || self.max_thread_cost.is_some()
            || self.max_daily_tokens.is_some()
            || self.max_daily_cost.is_some()
            || !self.capabilities.is_empty()
    }

    /// The ant with the flags given on the command line in place of its own settings.
    pub fn apply(&self, mut p: Ant) -> Ant {
        if let Some(dir) = &self.workdir {
            p.workdir.clone_from(dir);
        }
        if let Some(home) = &self.home {
            p.home = Some(home.clone());
        }
        if let Some(whoami) = &self.whoami {
            p.whoami = Some(whoami.clone());
        }
        if let Some(memory) = &self.memory {
            p.memory = Some(memory.clone());
        }
        if let Some(tasks) = &self.tasks {
            p.tasks = Some(tasks.clone());
        }
        p.env.extend(self.env.iter().cloned());
        if let Some(harness) = &self.harness {
            p.harness = Some(harness.clone());
        }
        if let Some(model) = &self.model {
            p.model = Some(model.clone());
        }
        if let Some(effort) = &self.reasoning_effort {
            p.reasoning_effort = Some(effort.clone());
        }
        if let Some(context) = &self.context {
            p.context = Some(context.clone());
        }
        p.allow.extend(self.allow.iter().cloned());
        p.allow_tools |= self.allow_tools;
        if let Some(timeout) = self.approval_timeout {
            p.approval_timeout = timeout;
        }
        if let Some(n) = self.max_harnesses {
            p.max_harnesses = n;
        }
        if let Some(timeout) = self.idle_timeout {
            p.idle_timeout = timeout;
        }
        if let Some(timeout) = self.prompt_timeout {
            p.prompt_timeout = timeout;
        }
        if let Some(n) = self.max_tool_calls {
            p.max_tool_calls = n;
        }
        p.max_thread_tokens = self.max_thread_tokens.or(p.max_thread_tokens);
        p.max_thread_cost = self.max_thread_cost.or(p.max_thread_cost);
        p.max_daily_tokens = self.max_daily_tokens.or(p.max_daily_tokens);
        p.max_daily_cost = self.max_daily_cost.or(p.max_daily_cost);
        if !self.capabilities.is_empty() {
            p.capabilities.clone_from(&self.capabilities);
        }
        p
    }
}

fn env_pair(text: &str) -> Result<(String, String), String> {
    let (name, value) = text.split_once('=').ok_or("expected NAME=VALUE")?;
    if name.is_empty() || name.contains('\0') || value.contains('\0') {
        return Err("invalid environment variable".to_owned());
    }
    Ok((name.to_owned(), value.to_owned()))
}

#[derive(Debug, PartialEq)]
enum Source {
    Flags(UserId),
    File(PathBuf),
}

/// `--config` (or `ANTS_CONFIG`) names the file; otherwise `--ant` alone means the flags describe it, and with
/// neither the file at `default_path` is used.
fn source(args: &DaemonArgs, default_path: &Path) -> Result<Source> {
    Ok(match (&args.config, &args.ant) {
        (Some(path), _) => Source::File(path.clone()),
        (None, Some(ant)) => Source::Flags(ant.clone()),
        (None, None) if default_path.exists() => Source::File(default_path.to_owned()),
        (None, None) => bail!("no --ant given and no config file at {}", default_path.display()),
    })
}

/// The ants to serve, and the harnesses they may name: those of the config file, or for an ant described by flags
/// those of the file at `default_path` if it exists.
pub fn ants(args: &DaemonArgs, default_path: &Path) -> Result<(Vec<Ant>, Harnesses)> {
    match source(args, default_path)? {
        Source::Flags(name) => {
            let ant = args.options.apply(Ant::new(name, std::env::current_dir()?));
            Ok((vec![ant], Summary::load(default_path)?.harnesses))
        }
        Source::File(path) => {
            let (all, harnesses) = config::load(&path)?;
            Ok((pick(all, args, &path)?, harnesses))
        }
    }
}

fn pick(all: Vec<Ant>, args: &DaemonArgs, path: &Path) -> Result<Vec<Ant>> {
    match &args.ant {
        Some(name) => {
            let p = all
                .into_iter()
                .find(|p| &p.name == name)
                .with_context(|| format!("ant {name} is not in {}", path.display()))?;
            Ok(vec![args.options.apply(p)])
        }
        None if args.options.sets_any() => {
            bail!("ant flags such as --workdir apply to one ant; add --ant to pick it from {}", path.display())
        }
        None => Ok(all),
    }
}

/// Daemon configs, with their harnesses from `harnesses`, `nats` as the server, `js_domain` as the JetStream domain
/// and, for a lone ant, `creds` as the credentials where the ant sets none. Several ants cannot share one set of
/// credentials, so `creds` is ignored for them.
pub fn configs(
    ants: Vec<Ant>,
    harnesses: &Harnesses,
    nats: &str,
    creds: Option<PathBuf>,
    js_domain: Option<JsDomain>,
) -> Result<Vec<Config>> {
    let lone = ants.len() == 1;
    if !lone && creds.is_some() {
        tracing::warn!("ignoring --creds (ANTS_CREDS) while serving several ants; set creds per ant instead");
    }
    ants.into_iter()
        .map(|mut p| {
            if lone && p.creds.is_none() {
                p.creds.clone_from(&creds);
            }
            if p.js_domain.is_none() {
                p.js_domain.clone_from(&js_domain);
            }
            p.into_config(nats, harnesses)
        })
        .collect()
}

/// Serve every ant until Ctrl-C, each over its own NATS connection. An ant that fails is reported and the
/// others keep running; it is an error only when none is left.
pub async fn run(mut configs: Vec<Config>) -> Result<()> {
    if configs.len() == 1 {
        return ants_daemon::run(configs.remove(0)).await;
    }
    let total = configs.len();
    let runs = configs.into_iter().map(|config| {
        let span = tracing::info_span!("ant", name = %config.ant);
        async move {
            let result = ants_daemon::run(config).await;
            if let Err(e) = &result {
                tracing::error!(error = format!("{e:#}"), "ant stopped");
            }
            result.is_ok()
        }
        .instrument(span)
    });
    let served = futures::future::join_all(runs).await.into_iter().filter(|ok| *ok).count();
    if served == 0 {
        bail!("none of the {total} ants could be served");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Cmd {
        #[command(flatten)]
        args: DaemonArgs,
    }

    fn args(line: &str) -> DaemonArgs {
        Cmd::try_parse_from(std::iter::once("daemon").chain(line.split_whitespace())).unwrap().args
    }

    fn here() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    }

    fn ant(name: &str) -> Ant {
        let mut p = Ant::new(name.parse().unwrap(), here());
        p.nats = Some(format!("nats://{name}:4222"));
        p.creds = Some(PathBuf::from(format!("{name}.creds")));
        p
    }

    #[test]
    fn picks_the_source() {
        let existing = here().join("Cargo.toml");
        let missing = here().join("no-such-config.toml");
        assert_eq!(source(&args("--ant dev-a"), &existing).unwrap(), Source::Flags("dev-a".parse().unwrap()));
        assert_eq!(source(&args(""), &existing).unwrap(), Source::File(existing.clone()));
        assert_eq!(source(&args("--config c.toml --ant dev-a"), &missing).unwrap(), Source::File("c.toml".into()));
        let err = source(&args(""), &missing).unwrap_err().to_string();
        assert!(err.contains("no --ant given") && err.contains("no-such-config.toml"), "{err}");
    }

    #[test]
    fn flags_override_the_picked_ant() {
        let all = vec![ant("dev-a"), ant("dev-b")];
        let path = Path::new("config.toml");
        let picked = pick(
            all.clone(),
            &args(
                "--ant dev-b --max-harnesses 1 --allow-tools --allow read --capability rust,nats --capability acp \
                 --max-tool-calls 20 --max-daily-cost 7.5",
            ),
            path,
        )
        .unwrap();
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].name.as_str(), "dev-b");
        assert_eq!((picked[0].max_harnesses, picked[0].allow_tools), (1, true));
        assert_eq!(picked[0].allow, ["read"]);
        assert_eq!(picked[0].capabilities, ["rust", "nats", "acp"]);
        assert_eq!(picked[0].idle_timeout, config::DEFAULT_IDLE_TIMEOUT);
        assert_eq!((picked[0].max_tool_calls, picked[0].max_daily_cost), (20, Some(7.5)));
        assert_eq!(picked[0].prompt_timeout, config::DEFAULT_PROMPT_TIMEOUT);

        assert_eq!(pick(all.clone(), &args(""), path).unwrap(), all);
        let err = pick(all.clone(), &args("--ant dev-c"), path).unwrap_err().to_string();
        assert!(err.contains("ant dev-c is not in config.toml"), "{err}");
        let err = pick(all, &args("--workdir ."), path).unwrap_err().to_string();
        assert!(err.contains("add --ant"), "{err}");
    }

    #[test]
    fn the_file_wins_over_global_flags() {
        let mut bare = ant("dev-a");
        bare.nats = None;
        bare.creds = None;
        let cli_creds = Some(PathBuf::from("cli.creds"));
        let (cli_domain, own_domain): (JsDomain, JsDomain) = ("cli".parse().unwrap(), "own".parse().unwrap());

        let one = configs(
            vec![bare.clone()],
            &Harnesses::builtin(),
            "nats://cli:4222",
            cli_creds.clone(),
            Some(cli_domain.clone()),
        )
        .unwrap();
        assert_eq!((one[0].nats_url.as_str(), one[0].creds.clone()), ("nats://cli:4222", cli_creds.clone()));
        assert_eq!(one[0].js_domain, Some(cli_domain.clone()));

        let mut own = ant("dev-a");
        own.js_domain = Some(own_domain.clone());
        let one =
            configs(vec![own], &Harnesses::builtin(), "nats://cli:4222", cli_creds.clone(), Some(cli_domain)).unwrap();
        assert_eq!(one[0].nats_url, "nats://dev-a:4222");
        assert_eq!(one[0].creds, Some(PathBuf::from("dev-a.creds")));
        assert_eq!(one[0].js_domain, Some(own_domain));

        let two = configs(vec![bare, ant("dev-b")], &Harnesses::builtin(), "nats://cli:4222", cli_creds, None).unwrap();
        assert_eq!((two[0].nats_url.as_str(), two[0].creds.clone()), ("nats://cli:4222", None));
        assert_eq!(two[1].creds, Some(PathBuf::from("dev-b.creds")));
    }

    #[test]
    fn duration_flags_need_a_unit() {
        let p = args("--idle-timeout 90s --prompt-timeout 0 --approval-timeout 1h30m").options.apply(ant("dev-a"));
        assert_eq!(
            (p.idle_timeout, p.prompt_timeout, p.approval_timeout),
            (Duration::from_secs(90), Duration::ZERO, Duration::from_secs(5_400))
        );
        for line in ["--idle-timeout 90", "--idle-timeout-secs 90", "--prompt-timeout 1.5h"] {
            assert!(Cmd::try_parse_from(std::iter::once("daemon").chain(line.split_whitespace())).is_err(), "{line}");
        }
    }

    #[test]
    fn model_flags_override_only_the_selected_ant() {
        let mut p = ant("dev-a");
        p.model = Some("old-model".to_owned());
        p.reasoning_effort = Some("low".to_owned());
        p.context = Some("default".to_owned());
        let picked = pick(
            vec![p.clone(), ant("dev-b")],
            &args("--ant dev-a --model smart --reasoning-effort high --context long_context"),
            Path::new("config.toml"),
        )
        .unwrap();
        assert_eq!(picked[0].model.as_deref(), Some("smart"));
        assert_eq!(picked[0].reasoning_effort.as_deref(), Some("high"));
        assert_eq!(picked[0].context.as_deref(), Some("long_context"));
        let only_model = args("--model smart").options.apply(p);
        assert_eq!(only_model.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(only_model.context.as_deref(), Some("default"));
        for line in ["--model smart", "--reasoning-effort high", "--context long_context"] {
            assert!(pick(vec![ant("dev-a")], &args(line), Path::new("config.toml")).is_err());
        }
    }

    #[test]
    fn tool_flags_override_only_the_selected_settings() {
        let mut p = ant("dev-a");
        p.whoami = Some("file-role".to_owned());
        p.memory = Some("file-memory".to_owned());
        p.tasks = Some("file-tasks".to_owned());
        p.env.insert("TASK_DB".to_owned(), "file.db".to_owned());
        p.env.insert("KEEP".to_owned(), "keep".to_owned());
        let picked = pick(
            vec![p],
            &args("--ant dev-a --whoami custom-role --memory custom-memory --env TASK_DB=cli.db --env EXTRA=a=b"),
            Path::new("config.toml"),
        )
        .unwrap();
        assert_eq!(picked[0].whoami.as_deref(), Some("custom-role"));
        assert_eq!(picked[0].memory.as_deref(), Some("custom-memory"));
        assert_eq!(picked[0].tasks.as_deref(), Some("file-tasks"));
        assert_eq!(picked[0].env["TASK_DB"], "cli.db");
        assert_eq!(picked[0].env["KEEP"], "keep");
        assert_eq!(picked[0].env["EXTRA"], "a=b");
        let clear = Cmd::try_parse_from(["daemon", "--ant", "dev-a", "--whoami", "", "--tasks", "", "--env", "EMPTY="])
            .unwrap()
            .args;
        let cleared = clear.options.apply(picked[0].clone());
        assert_eq!(cleared.whoami.as_deref(), Some(""));
        assert_eq!(cleared.tasks.as_deref(), Some(""));
        assert_eq!(cleared.env["EMPTY"], "");
        assert!(Cmd::try_parse_from(["daemon", "--env", "=value"]).is_err());
        assert!(Cmd::try_parse_from(["daemon", "--env", "NAME"]).is_err());
        for line in ["--whoami role", "--memory notes", "--tasks tickets", "--env NAME=value"] {
            assert!(pick(vec![ant("dev-a")], &args(line), Path::new("config.toml")).is_err());
        }
    }
}
