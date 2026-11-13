//! `ants daemon`: which personas to serve, from the config file and the command line, and serving them together.

use std::path::{Path, PathBuf};

use ants_daemon::config::{self, Persona};
use ants_daemon::{Config, HarnessKind};
use ants_proto::{JsDomain, PersonaId};
use anyhow::{Context as _, Result, bail};
use tracing::Instrument as _;

#[derive(clap::Args, Debug)]
pub struct DaemonArgs {
    /// Serve only this persona: picked from the config file when one is given, described by the flags below otherwise.
    #[arg(long, env = "ANTS_PERSONA")]
    persona: Option<PersonaId>,
    /// Config file listing the personas to serve (default without --persona: ~/.ants/config.toml, if it exists).
    #[arg(long, env = "ANTS_CONFIG")]
    config: Option<PathBuf>,
    /// Directory the harness works in (default: the current directory).
    #[arg(long)]
    workdir: Option<PathBuf>,
    /// The persona's own files (default: ~/.ants/<persona>).
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
    /// Agent harness: `copilot` (default), `claude` or `antigravity`.
    #[arg(long)]
    harness: Option<HarnessKind>,
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
    /// Seconds a tool request outside the allowlist waits for the human's decision before it is rejected (default:
    /// 300).
    #[arg(long)]
    approval_timeout_secs: Option<u64>,
    /// Most harness processes alive at once, one per active thread (default: 4).
    #[arg(long)]
    max_harnesses: Option<usize>,
    /// Seconds without a message before a thread's harness stops; its session resumes on the next message (default:
    /// 600).
    #[arg(long)]
    idle_timeout_secs: Option<u64>,
    /// Seconds a prompt may run, tool requests waiting for the human included, before it is cancelled and the thread
    /// goes to the human; 0 for no limit (default: 1800).
    #[arg(long)]
    prompt_timeout_secs: Option<u64>,
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
    /// Tokens the persona may spend in a UTC day (default: no cap).
    #[arg(long)]
    max_daily_tokens: Option<u64>,
    /// Cost the persona may spend in a UTC day (default: no cap).
    #[arg(long)]
    max_daily_cost: Option<f64>,
    /// Tag the persona is good for, listed in its roster entry for routing (repeatable, or comma-separated). Replaces
    /// the file's `capabilities`.
    #[arg(long = "capability", value_name = "TAG", value_delimiter = ',')]
    capabilities: Vec<String>,
}

impl DaemonArgs {
    fn sets_persona_options(&self) -> bool {
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
            || self.approval_timeout_secs.is_some()
            || self.max_harnesses.is_some()
            || self.idle_timeout_secs.is_some()
            || self.prompt_timeout_secs.is_some()
            || self.max_tool_calls.is_some()
            || self.max_thread_tokens.is_some()
            || self.max_thread_cost.is_some()
            || self.max_daily_tokens.is_some()
            || self.max_daily_cost.is_some()
            || !self.capabilities.is_empty()
    }

    /// The persona with the flags given on the command line in place of its own settings.
    fn apply(&self, mut p: Persona) -> Persona {
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
        if let Some(harness) = self.harness {
            p.harness = harness;
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
        if let Some(secs) = self.approval_timeout_secs {
            p.approval_timeout_secs = secs;
        }
        if let Some(n) = self.max_harnesses {
            p.max_harnesses = n;
        }
        if let Some(secs) = self.idle_timeout_secs {
            p.idle_timeout_secs = secs;
        }
        if let Some(secs) = self.prompt_timeout_secs {
            p.prompt_timeout_secs = secs;
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
    Flags(PersonaId),
    File(PathBuf),
}

/// `--config` (or `ANTS_CONFIG`) names the file; otherwise `--persona` alone means the flags describe it, and with
/// neither the file at `default_path` is used.
fn source(args: &DaemonArgs, default_path: &Path) -> Result<Source> {
    Ok(match (&args.config, &args.persona) {
        (Some(path), _) => Source::File(path.clone()),
        (None, Some(persona)) => Source::Flags(persona.clone()),
        (None, None) if default_path.exists() => Source::File(default_path.to_owned()),
        (None, None) => bail!("no --persona given and no config file at {}", default_path.display()),
    })
}

/// The personas to serve.
pub fn personas(args: &DaemonArgs, default_path: &Path) -> Result<Vec<Persona>> {
    match source(args, default_path)? {
        Source::Flags(name) => Ok(vec![args.apply(Persona::new(name, std::env::current_dir()?))]),
        Source::File(path) => pick(config::load(&path)?, args, &path),
    }
}

fn pick(all: Vec<Persona>, args: &DaemonArgs, path: &Path) -> Result<Vec<Persona>> {
    match &args.persona {
        Some(name) => {
            let p = all
                .into_iter()
                .find(|p| &p.name == name)
                .with_context(|| format!("persona {name} is not in {}", path.display()))?;
            Ok(vec![args.apply(p)])
        }
        None if args.sets_persona_options() => {
            bail!(
                "persona flags such as --workdir apply to one persona; add --persona to pick it from {}",
                path.display()
            )
        }
        None => Ok(all),
    }
}

/// Daemon configs, with `nats` as the server, `js_domain` as the JetStream domain and, for a lone persona, `creds` as
/// the credentials where the persona sets none. Several personas cannot share one set of credentials, so `creds` is
/// ignored for them.
pub fn configs(
    personas: Vec<Persona>,
    nats: &str,
    creds: Option<PathBuf>,
    js_domain: Option<JsDomain>,
) -> Result<Vec<Config>> {
    let lone = personas.len() == 1;
    if !lone && creds.is_some() {
        tracing::warn!("ignoring --creds (ANTS_CREDS) while serving several personas; set creds per persona instead");
    }
    personas
        .into_iter()
        .map(|mut p| {
            if lone && p.creds.is_none() {
                p.creds.clone_from(&creds);
            }
            if p.js_domain.is_none() {
                p.js_domain.clone_from(&js_domain);
            }
            p.into_config(nats)
        })
        .collect()
}

/// Serve every persona until Ctrl-C, each over its own NATS connection. A persona that fails is reported and the
/// others keep running; it is an error only when none is left.
pub async fn run(mut configs: Vec<Config>) -> Result<()> {
    if configs.len() == 1 {
        return ants_daemon::run(configs.remove(0)).await;
    }
    let total = configs.len();
    let runs = configs.into_iter().map(|config| {
        let span = tracing::info_span!("persona", name = %config.persona);
        async move {
            let result = ants_daemon::run(config).await;
            if let Err(e) = &result {
                tracing::error!(error = format!("{e:#}"), "persona stopped");
            }
            result.is_ok()
        }
        .instrument(span)
    });
    let served = futures::future::join_all(runs).await.into_iter().filter(|ok| *ok).count();
    if served == 0 {
        bail!("none of the {total} personas could be served");
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

    fn persona(name: &str) -> Persona {
        let mut p = Persona::new(name.parse().unwrap(), here());
        p.nats = Some(format!("nats://{name}:4222"));
        p.creds = Some(PathBuf::from(format!("{name}.creds")));
        p
    }

    #[test]
    fn picks_the_source() {
        let existing = here().join("Cargo.toml");
        let missing = here().join("no-such-config.toml");
        assert_eq!(source(&args("--persona dev-a"), &existing).unwrap(), Source::Flags("dev-a".parse().unwrap()));
        assert_eq!(source(&args(""), &existing).unwrap(), Source::File(existing.clone()));
        assert_eq!(source(&args("--config c.toml --persona dev-a"), &missing).unwrap(), Source::File("c.toml".into()));
        let err = source(&args(""), &missing).unwrap_err().to_string();
        assert!(err.contains("no --persona given") && err.contains("no-such-config.toml"), "{err}");
    }

    #[test]
    fn flags_override_the_picked_persona() {
        let all = vec![persona("dev-a"), persona("dev-b")];
        let path = Path::new("config.toml");
        let picked = pick(
            all.clone(),
            &args(
                "--persona dev-b --max-harnesses 1 --allow-tools --allow read --capability rust,nats --capability acp \
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
        assert_eq!(picked[0].idle_timeout_secs, config::DEFAULT_IDLE_TIMEOUT_SECS);
        assert_eq!((picked[0].max_tool_calls, picked[0].max_daily_cost), (20, Some(7.5)));
        assert_eq!(picked[0].prompt_timeout_secs, config::DEFAULT_PROMPT_TIMEOUT_SECS);

        assert_eq!(pick(all.clone(), &args(""), path).unwrap(), all);
        let err = pick(all.clone(), &args("--persona dev-c"), path).unwrap_err().to_string();
        assert!(err.contains("persona dev-c is not in config.toml"), "{err}");
        let err = pick(all, &args("--workdir ."), path).unwrap_err().to_string();
        assert!(err.contains("add --persona"), "{err}");
    }

    #[test]
    fn the_file_wins_over_global_flags() {
        let mut bare = persona("dev-a");
        bare.nats = None;
        bare.creds = None;
        let cli_creds = Some(PathBuf::from("cli.creds"));
        let (cli_domain, own_domain): (JsDomain, JsDomain) = ("cli".parse().unwrap(), "own".parse().unwrap());

        let one = configs(vec![bare.clone()], "nats://cli:4222", cli_creds.clone(), Some(cli_domain.clone())).unwrap();
        assert_eq!((one[0].nats_url.as_str(), one[0].creds.clone()), ("nats://cli:4222", cli_creds.clone()));
        assert_eq!(one[0].js_domain, Some(cli_domain.clone()));

        let mut own = persona("dev-a");
        own.js_domain = Some(own_domain.clone());
        let one = configs(vec![own], "nats://cli:4222", cli_creds.clone(), Some(cli_domain)).unwrap();
        assert_eq!(one[0].nats_url, "nats://dev-a:4222");
        assert_eq!(one[0].creds, Some(PathBuf::from("dev-a.creds")));
        assert_eq!(one[0].js_domain, Some(own_domain));

        let two = configs(vec![bare, persona("dev-b")], "nats://cli:4222", cli_creds, None).unwrap();
        assert_eq!((two[0].nats_url.as_str(), two[0].creds.clone()), ("nats://cli:4222", None));
        assert_eq!(two[1].creds, Some(PathBuf::from("dev-b.creds")));
    }

    #[test]
    fn model_flags_override_only_the_selected_persona() {
        let mut p = persona("dev-a");
        p.model = Some("old-model".to_owned());
        p.reasoning_effort = Some("low".to_owned());
        p.context = Some("default".to_owned());
        let picked = pick(
            vec![p.clone(), persona("dev-b")],
            &args("--persona dev-a --model smart --reasoning-effort high --context long_context"),
            Path::new("config.toml"),
        )
        .unwrap();
        assert_eq!(picked[0].model.as_deref(), Some("smart"));
        assert_eq!(picked[0].reasoning_effort.as_deref(), Some("high"));
        assert_eq!(picked[0].context.as_deref(), Some("long_context"));
        let only_model = args("--model smart").apply(p);
        assert_eq!(only_model.reasoning_effort.as_deref(), Some("low"));
        assert_eq!(only_model.context.as_deref(), Some("default"));
        for line in ["--model smart", "--reasoning-effort high", "--context long_context"] {
            assert!(pick(vec![persona("dev-a")], &args(line), Path::new("config.toml")).is_err());
        }
    }

    #[test]
    fn tool_flags_override_only_the_selected_settings() {
        let mut p = persona("dev-a");
        p.whoami = Some("file-role".to_owned());
        p.memory = Some("file-memory".to_owned());
        p.tasks = Some("file-tasks".to_owned());
        p.env.insert("TASK_DB".to_owned(), "file.db".to_owned());
        p.env.insert("KEEP".to_owned(), "keep".to_owned());
        let picked = pick(
            vec![p],
            &args("--persona dev-a --whoami custom-role --memory custom-memory --env TASK_DB=cli.db --env EXTRA=a=b"),
            Path::new("config.toml"),
        )
        .unwrap();
        assert_eq!(picked[0].whoami.as_deref(), Some("custom-role"));
        assert_eq!(picked[0].memory.as_deref(), Some("custom-memory"));
        assert_eq!(picked[0].tasks.as_deref(), Some("file-tasks"));
        assert_eq!(picked[0].env["TASK_DB"], "cli.db");
        assert_eq!(picked[0].env["KEEP"], "keep");
        assert_eq!(picked[0].env["EXTRA"], "a=b");
        let clear =
            Cmd::try_parse_from(["daemon", "--persona", "dev-a", "--whoami", "", "--tasks", "", "--env", "EMPTY="])
                .unwrap()
                .args;
        let cleared = clear.apply(picked[0].clone());
        assert_eq!(cleared.whoami.as_deref(), Some(""));
        assert_eq!(cleared.tasks.as_deref(), Some(""));
        assert_eq!(cleared.env["EMPTY"], "");
        assert!(Cmd::try_parse_from(["daemon", "--env", "=value"]).is_err());
        assert!(Cmd::try_parse_from(["daemon", "--env", "NAME"]).is_err());
        for line in ["--whoami role", "--memory notes", "--tasks tickets", "--env NAME=value"] {
            assert!(pick(vec![persona("dev-a")], &args(line), Path::new("config.toml")).is_err());
        }
    }
}
