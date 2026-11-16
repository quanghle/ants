use std::path::{Path, PathBuf};

use ants_daemon::config::Ant;
use ants_daemon::{Harnesses, config};
use clap::Parser;

use super::selection::{Source, pick, source};
use super::*;

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
    p.harness = Some("copilot".to_owned());
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
fn the_file_names_the_machine_as_the_host_connection_would() {
    let dir = ants_test_support::scratch_dir("serve-machine").canonicalize().unwrap();
    let config = dir.join("config.toml");
    let listed = |text: &str, line: &str| {
        std::fs::write(&config, text).unwrap();
        ants(&args(&format!("--config {} {line}", config.display())), &config, "gen-4f2a.internal").unwrap()
    };
    let top = "harness = \"copilot\"\nmachine = \"dev-vm1\"\n";
    assert_eq!(listed(top, "--ant messenger").machine, Some("dev-vm1".parse().unwrap()));
    assert_eq!(listed("harness = \"copilot\"\nmachine = \"Dev VM\"\n", "").machine, Some("gen-4f2a".parse().unwrap()));
    let team = dir.join("team");
    crate::auth::init(&team).unwrap();
    crate::auth::issue_host(&team, &"m1".parse().unwrap(), None, &dir.join("h.creds")).unwrap();
    let with_creds = format!("{top}[host]\ncreds = \"h.creds\"\n");
    assert_eq!(listed(&with_creds, "").machine, Some("m1".parse().unwrap()), "the credentials' machine comes first");
    let flags = ants(&args("--ant dev-a"), &dir.join("none.toml"), "gen-4f2a.internal").unwrap();
    assert_eq!(flags.machine, None, "an ant described by flags goes by the host name");
    std::fs::remove_dir_all(&dir).unwrap();
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
    assert_eq!(picked[0].worktrees, None, "unset unless a flag sets it");
    let worktrees = |flag: &str| pick(all.clone(), &args(&format!("--ant dev-a {flag}")), path).unwrap()[0].worktrees;
    assert_eq!((worktrees("--worktrees"), worktrees("--no-worktrees")), (Some(true), Some(false)));

    assert_eq!(pick(all.clone(), &args(""), path).unwrap(), all);
    let err = pick(all.clone(), &args("--ant dev-c"), path).unwrap_err().to_string();
    assert!(err.contains("ant dev-c is not in config.toml"), "{err}");
    let err = pick(all, &args("--workdir ."), path).unwrap_err().to_string();
    assert!(err.contains("add --ant"), "{err}");

    let added = vec![Ant { template: Some("worker".to_owned()), ..ant("lead-a") }];
    assert_eq!(pick(added.clone(), &args("--ant lead-a"), path).unwrap(), added);
    let err = pick(added, &args("--ant lead-a --max-daily-cost 99"), path).unwrap_err().to_string();
    assert!(err.contains("ant lead-a was added from template worker"), "{err}");
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
        None,
    )
    .unwrap();
    assert_eq!((one[0].nats_url.as_str(), one[0].creds.clone()), ("nats://cli:4222", cli_creds.clone()));
    assert_eq!(one[0].js_domain, Some(cli_domain.clone()));

    let mut own = ant("dev-a");
    own.js_domain = Some(own_domain.clone());
    let one = configs(vec![own], &Harnesses::builtin(), "nats://cli:4222", cli_creds.clone(), Some(cli_domain), None)
        .unwrap();
    assert_eq!(one[0].nats_url, "nats://dev-a:4222");
    assert_eq!(one[0].creds, Some(PathBuf::from("dev-a.creds")));
    assert_eq!(one[0].js_domain, Some(own_domain));

    let two =
        configs(vec![bare, ant("dev-b")], &Harnesses::builtin(), "nats://cli:4222", cli_creds, None, None).unwrap();
    assert_eq!((two[0].nats_url.as_str(), two[0].creds.clone()), ("nats://cli:4222", None));
    assert_eq!(two[1].creds, Some(PathBuf::from("dev-b.creds")));
}

#[test]
fn a_reload_starts_new_ants_and_stops_or_restarts_the_others() {
    let config = |name: &str, model: Option<&str>| {
        let mut p = ant(name);
        p.model = model.map(str::to_owned);
        configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None, None).unwrap().remove(0)
    };
    let names = |start: Vec<(Config, Stop)>| start.into_iter().map(|(c, _)| c.ant.to_string()).collect::<Vec<_>>();
    let state = |running: &HashMap<UserId, Running>, name: &str| {
        let r = &running[&name.parse::<UserId>().unwrap()];
        (r.stopping, r.next.as_ref().map(|c| c.model_settings.model.clone()))
    };
    let mut running = HashMap::new();
    let (a, b) = (config("dev-a", None), config("dev-b", None));
    assert_eq!(names(reconcile(&mut running, vec![a.clone(), b.clone()])), ["dev-a", "dev-b"]);
    assert!(names(reconcile(&mut running, vec![a.clone(), b.clone()])).is_empty());
    assert_eq!((state(&running, "dev-a"), state(&running, "dev-b")), ((false, None), (false, None)));

    let changed = config("dev-a", Some("smart"));
    assert_eq!(names(reconcile(&mut running, vec![changed, config("dev-c", None)])), ["dev-c"]);
    assert_eq!(state(&running, "dev-a"), (true, Some(Some("smart".to_owned()))));
    assert_eq!(state(&running, "dev-b"), (true, None));
    assert_eq!(state(&running, "dev-c"), (false, None));

    // Listed again before it stopped, an ant is served again once it did.
    assert!(names(reconcile(&mut running, vec![a, b, config("dev-c", None)])).is_empty());
    assert_eq!((state(&running, "dev-a"), state(&running, "dev-b")), ((true, Some(None)), (true, Some(None))));
    assert_eq!(state(&running, "dev-c"), (false, None));
}

#[test]
fn a_reload_hands_a_running_ant_its_pool_s_new_members_without_a_restart() {
    use ants_daemon::caps::{Members, Pool};
    let member = |name: &str| name.parse::<UserId>().unwrap();
    let config = |members: &[&str]| {
        let pool = Pool {
            template: "worker".to_owned(),
            daily_tokens: Some(100),
            daily_cost: None,
            members: Members::new(members.iter().map(|m| member(m)).collect()),
        };
        let p = Ant { pool: Some(pool), ..ant("lead-a") };
        configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None, None).unwrap().remove(0)
    };
    let mut running = HashMap::new();
    let (served, _) = reconcile(&mut running, vec![config(&["lead-a"])]).remove(0);
    assert!(reconcile(&mut running, vec![config(&["lead-a", "lead-b"])]).is_empty());
    assert!(!running[&member("lead-a")].stopping, "only its pool's members changed");
    assert_eq!(served.pool.unwrap().members.get(), [member("lead-a"), member("lead-b")]);
}

#[test]
fn a_reload_pauses_and_resumes_a_running_ant_without_a_restart() {
    let ant_named = |paused: bool, model: Option<&str>| {
        let p = Ant { paused, model: model.map(str::to_owned), ..ant("dev-a") };
        configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None, None).unwrap().remove(0)
    };
    let dev_a: UserId = "dev-a".parse().unwrap();
    let mut running = HashMap::new();
    let (served, _) = reconcile(&mut running, vec![ant_named(false, None)]).remove(0);
    let paused = ant_named(true, None);
    paused.pause.credit(Some("alice".parse().unwrap()));
    assert!(reconcile(&mut running, vec![paused]).is_empty());
    assert!(!running[&dev_a].stopping, "only paused changed");
    let pause = served.pause.get().expect("the running ant is paused");
    assert_eq!(pause.by.as_ref().map(UserId::as_str), Some("alice"));
    assert!(reconcile(&mut running, vec![ant_named(true, None)]).is_empty());
    assert_eq!(served.pause.get(), Some(pause.clone()), "a pause on keeps its start and admin");

    assert!(reconcile(&mut running, vec![ant_named(true, Some("smart"))]).is_empty());
    let next = running[&dev_a].next.as_ref().expect("restarted for its model");
    assert_eq!(next.pause.get(), Some(pause), "carried over the restart");
    running.get_mut(&dev_a).unwrap().stopping = false;
    running.get_mut(&dev_a).unwrap().next = None;
    assert!(reconcile(&mut running, vec![ant_named(false, None)]).is_empty());
    assert!(!running[&dev_a].stopping, "only paused changed");
    assert_eq!(served.pause.get(), None, "resumed");
}

#[test]
fn a_change_of_drift_alone_restarts_no_ant() {
    let dev_a = || {
        let p = Ant { model: Some("gpt-4".to_owned()), ..ant("dev-a") };
        Ant { creds: None, ..p }
    };
    let shared = Shared::default();
    let record = |fields: serde_json::Value| {
        let fields = fields.as_object().unwrap().clone();
        ants_proto::SharedSettings::new("dev-a".parse().unwrap(), fields, "m1".parse().unwrap(), None)
    };
    shared.store(record(serde_json::json!({ "model": "gpt-5" })));
    let served = |ant: Ant| {
        shared.listed(std::slice::from_ref(&ant));
        let harnesses = Harnesses::builtin();
        configs(vec![ant], &harnesses, "nats://cli:4222", None, None, Some(&shared)).unwrap().remove(0)
    };
    let mut running = HashMap::new();
    let (config, _) = reconcile(&mut running, vec![served(dev_a())]).remove(0);
    assert_eq!((config.model_settings.model.as_deref(), config.drift.get()), (Some("gpt-5"), vec!["model".into()]));
    let drift = config.drift.subscribe();

    let mut same = dev_a();
    same.model = Some("gpt-5".to_owned());
    assert!(reconcile(&mut running, vec![served(same)]).is_empty());
    let dev_a_id: UserId = "dev-a".parse().unwrap();
    assert!(!running[&dev_a_id].stopping, "only the drift changed");
    assert!(drift.has_changed().unwrap(), "the running ant hears of it");
    assert_eq!(config.drift.get(), Vec::<String>::new());

    shared.store(record(serde_json::json!({ "model": "o3" })));
    assert!(reconcile(&mut running, vec![served(dev_a())]).is_empty());
    assert!(running[&dev_a_id].stopping, "the shared settings changed");
}

#[test]
fn a_reload_restarts_an_ant_whose_credentials_file_changed() {
    let dir = ants_test_support::scratch_dir("reload-creds");
    let creds = dir.join("dev-a.creds");
    std::fs::write(&creds, "issued").unwrap();
    let mut p = ant("dev-a");
    p.creds = Some(creds.clone());
    let config = configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None, None).unwrap().remove(0);
    let mut running = HashMap::new();
    assert_eq!(reconcile(&mut running, vec![config.clone()]).len(), 1);
    assert!(reconcile(&mut running, vec![config.clone()]).is_empty());
    assert!(!running[&config.ant].stopping, "same entry, same credentials");
    std::fs::write(&creds, "renewed").unwrap();
    assert!(reconcile(&mut running, vec![config.clone()]).is_empty());
    let r = &running[&config.ant];
    assert!(r.stopping && r.next.as_ref() == Some(&config), "served again with the renewed credentials");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_reload_starts_stops_or_restarts_schedules() {
    use std::cell::RefCell;
    let entry = |name: &str, text: &str| {
        let toml = format!("name = \"{name}\"\nevery = \"1h\"\nant = \"dev-a\"\ntext = \"{text}\"\n");
        Schedule::from_entry(toml::from_str(&toml).unwrap(), None, Path::to_path_buf).unwrap()
    };
    let started = RefCell::new(Vec::new());
    let spawn = |s: Schedule| {
        started.borrow_mut().push(s.name.to_string());
        tokio::spawn(std::future::pending())
    };
    let mut schedules = Schedules::default();
    schedules.reconcile(vec![entry("nightly", "a"), entry("pulse", "a")], spawn);
    schedules.reconcile(vec![entry("nightly", "a"), entry("pulse", "a")], spawn);
    assert_eq!(started.take(), ["nightly", "pulse"]);
    let pulse = schedules.0[&"pulse".parse::<UserId>().unwrap()].task.abort_handle();

    schedules.reconcile(vec![entry("nightly", "b"), entry("weekly", "a")], spawn);
    assert_eq!(started.take(), ["nightly", "weekly"]);
    tokio::task::yield_now().await;
    assert!(pulse.is_finished(), "a schedule no longer listed stops");
    assert_eq!(schedules.0.len(), 2);
}

#[test]
fn duration_flags_need_a_unit() {
    let p = args("--idle-timeout 90s --prompt-timeout 0 --approval-timeout 1h30m").options.apply(ant("dev-a"));
    assert_eq!(
        (p.idle_timeout, p.prompt_timeout, p.approval_timeout),
        (Duration::from_secs(90), Duration::ZERO, Duration::from_mins(90))
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
