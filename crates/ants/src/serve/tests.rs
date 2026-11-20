use std::path::{Path, PathBuf};

use ants_daemon::config::Ant;
use ants_daemon::config::builtins::Elsewhere;
use ants_daemon::{Harnesses, config};
use clap::Parser;

use super::selection::{Listed, Source, pick, source};
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

/// Whether this machine holds the credentials of the built-in ant `name`, which the config files of tests read.
fn builtin_creds_here(name: &str) -> bool {
    std::env::home_dir().is_some_and(|home| home.join(format!(".config/ants/auth/{name}.creds")).is_file())
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
    let listing = |text: &str, line: &str| {
        std::fs::write(&config, text).unwrap();
        ants(&args(&format!("--config {} {line}", config.display())), &config, "gen-4f2a.internal", None)
    };
    let listed = |text: &str, line: &str| listing(text, line).unwrap();
    let top = "harness = \"copilot\"\nmachine = \"dev-vm1\"\n";
    assert_eq!(listed(top, "--ant system-messenger").machine, Some("dev-vm1".parse().unwrap()));
    assert_eq!(listed("harness = \"copilot\"\nmachine = \"Dev VM\"\n", "").machine, Some("gen-4f2a".parse().unwrap()));
    let team = dir.join("team");
    crate::auth::init(&team).unwrap();
    crate::auth::issue_host(&team, &"m1".parse().unwrap(), None, &dir.join("h.creds")).unwrap();
    let with_creds = format!("{top}[host]\ncreds = \"h.creds\"\n");
    if !builtin_creds_here("system-messenger") {
        let err = format!("{:#}", listing(&with_creds, "").unwrap_err());
        assert!(
            err.contains("nothing to serve on this machine: no credentials of the built-in system-messenger at"),
            "{err}"
        );
        let err = format!("{:#}", listing(&with_creds, "--ant system-messenger").unwrap_err());
        assert!(
            err.contains("`ants auth builtin messenger`") && err.contains("or serve the system-messenger where"),
            "{err}"
        );
    }
    let with_creds = format!("{with_creds}[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n");
    let whole = listed(&with_creds, "");
    assert_eq!(whole.machine, Some("m1".parse().unwrap()), "the credentials' machine comes first");
    assert_eq!(whole.ants[0].name.as_str(), "dev-a");
    let one = listed(&with_creds, "--ant dev-a");
    assert_eq!(one.host, None, "one ant served makes no host connection");
    assert_eq!(
        one.sense_link.and_then(|s| s.creds),
        Some(dir.join("h.creds")),
        "sense links with the host's credentials"
    );
    let flags = ants(&args("--ant dev-a"), &dir.join("none.toml"), "gen-4f2a.internal", None).unwrap();
    assert_eq!(flags.machine, None, "an ant described by flags goes by the host name");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn without_a_host_connection_sense_links_with_the_host_credentials_where_the_ants_have_their_own() {
    use ants_daemon::provider::Setting;
    let config = |name: &str, creds: bool, provider: Setting| {
        let mut p = ant(name);
        p.creds = creds.then(|| PathBuf::from(format!("{name}.creds")));
        p.provider_setting = provider;
        configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None).unwrap().remove(0)
    };
    let link = |settings: Option<HostSettings>, configs: &[Config]| {
        sense_link(None, settings, configs, "nats://cli:4222", None, "box.local")
    };
    let creds = Some(PathBuf::from("host.creds"));
    let settings =
        HostSettings { creds: creds.clone(), nats: Some("nats://hub:4222".to_owned()), ..Default::default() };
    let authed = [config("dev-a", true, Setting::Sense), config("dev-b", true, Setting::None)];
    let Some(SenseLink::Own(own)) = link(Some(settings.clone()), &authed) else { panic!("a link of its own") };
    assert_eq!((&own.creds, own.nats.as_str()), (&creds, "nats://hub:4222"));
    let Some(SenseLink::Off(why)) = link(None, &authed) else { panic!("no link without host credentials") };
    assert!(why.contains("no host credentials on box") && why.contains("`ants auth host box`"), "{why}");
    let anonymous = [config("dev-a", false, Setting::Sense)];
    let Some(SenseLink::Own(own)) = link(None, &anonymous) else { panic!("a link as no one") };
    assert_eq!((own.creds, own.nats.as_str()), (None, "nats://cli:4222"), "no authentication: connected as no one");
    assert_eq!(link(Some(settings.clone()), &[config("dev-b", true, Setting::None)]), None, "no ant uses sense");
    let (host, _) = host(Some(settings.clone()), &authed, "nats://cli:4222", None, None).unwrap();
    let carried = sense_link(Some(&host), Some(settings), &authed, "nats://cli:4222", None, "box.local");
    assert_eq!(carried, None, "the host connection carries sense");
}

#[tokio::test]
async fn sense_s_own_link_comes_back_after_failing_without_a_reload() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let (creds, machine) = (server.dir().join("host.creds"), "m1".parse::<Machine>().unwrap());
    let nats = server.url.clone();
    let host = Host::quietly(Some(machine.clone()), None, nats, Some(creds.clone()), None, [true].into_iter()).unwrap();
    let served = Served {
        configs: vec![],
        schedules: vec![],
        metrics: None,
        host: None,
        sense: Some(SenseLink::Own(Box::new(host))),
        shared: None,
        machine: None,
    };
    let providers = Providers::default();
    let mut unkept = providers.sense().why_unkept();
    let failed = async |links: &mut Links, unkept: &mut tokio::sync::watch::Receiver<Option<String>>| {
        links.reconcile(None, Some(&served));
        let failed = tokio::time::timeout(Duration::from_secs(30), unkept.wait_for(Option::is_some));
        failed.await.expect("the link fails without its credentials").unwrap().clone().unwrap()
    };
    let mut links = Links::new(providers.clone());
    let why = failed(&mut links, &mut unkept).await;
    assert!(why.contains("sense's link with the host credentials stopped"), "{why}");
    links.reconcile(None, Some(&served));
    assert_eq!(unkept.borrow().as_ref(), Some(&why), "a reload keeps saying why while the link is down");
    drop(links);

    let mut links = Links::new(providers.clone());
    failed(&mut links, &mut unkept).await;
    crate::auth::issue_host(&server.dir().join("auth"), &machine, None, &creds).unwrap();
    let up = tokio::time::timeout(Duration::from_secs(30), unkept.wait_for(Option::is_none));
    up.await.expect("the link comes back without a reload").unwrap();
}

#[tokio::test]
async fn the_host_connection_comes_back_after_failing_without_a_reload() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let (creds, machine) = (server.dir().join("host.creds"), "m1".parse::<Machine>().unwrap());
    let nats = server.url.clone();
    let providers = Providers::default();
    let host = Host::quietly(Some(machine.clone()), None, nats, Some(creds.clone()), None, [true].into_iter()).unwrap();
    let host = Host { providers: providers.clone(), ..host };
    let mut unkept = providers.sense().why_unkept();
    let mut links = Links::new(providers.clone());
    links.reconcile(Some((host, vec![])), None);
    let failed = tokio::time::timeout(Duration::from_secs(30), unkept.wait_for(Option::is_some));
    let why = failed.await.expect("the connection fails without its credentials").unwrap().clone().unwrap();
    assert!(why.contains("the host connection stopped"), "{why}");
    crate::auth::issue_host(&server.dir().join("auth"), &machine, None, &creds).unwrap();
    let up = tokio::time::timeout(Duration::from_secs(30), unkept.wait_for(Option::is_none));
    up.await.expect("the connection comes back without a reload").unwrap();
}

#[test]
fn flags_override_the_picked_ant() {
    let all = vec![ant("dev-a"), ant("dev-b")];
    let path = Path::new("config.toml");
    let picked = pick(
        all.clone(),
        &[],
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
    let worktrees =
        |flag: &str| pick(all.clone(), &[], &args(&format!("--ant dev-a {flag}")), path).unwrap()[0].worktrees;
    assert_eq!((worktrees("--worktrees"), worktrees("--no-worktrees")), (Some(true), Some(false)));

    assert_eq!(pick(all.clone(), &[], &args(""), path).unwrap(), all);
    let err = pick(all.clone(), &[], &args("--ant dev-c"), path).unwrap_err().to_string();
    assert!(err.contains("ant dev-c is not in config.toml"), "{err}");
    let err = pick(all.clone(), &[], &args("--ant system-sage"), path).unwrap_err().to_string();
    assert!(err.contains("ant system-sage is not in config.toml"), "{err}");
    let sage = Ant { builtin: Some(ants_proto::BuiltinAnt::Sage), is_sage: true, ..Ant::new(UserId::sage(), here()) };
    let left = [Elsewhere { ant: sage, creds: PathBuf::from("auth/system-sage.creds") }];
    let err = pick(all.clone(), &left, &args("--ant system-sage"), path).unwrap_err().to_string();
    assert!(err.contains("no credentials of the built-in system-sage at auth/system-sage.creds"), "{err}");
    assert!(
        err.contains("`ants auth builtin sage`") && err.contains("pass them with --creds, or serve the system-sage"),
        "{err}"
    );
    assert_eq!(pick(all.clone(), &left, &args(""), path).unwrap(), all, "the others serve all the same");
    let err = pick(all, &[], &args("--workdir ."), path).unwrap_err().to_string();
    assert!(err.contains("add --ant"), "{err}");

    let added = vec![Ant { template: Some("worker".to_owned()), ..ant("lead-a") }];
    assert_eq!(pick(added.clone(), &[], &args("--ant lead-a"), path).unwrap(), added);
    let err = pick(added, &[], &args("--ant lead-a --max-daily-cost 99"), path).unwrap_err().to_string();
    assert!(err.contains("ant lead-a was added from template worker"), "{err}");
}

#[test]
fn the_file_wins_over_global_flags() {
    let mut bare = ant("dev-a");
    bare.nats = None;
    let (cli_domain, own_domain): (JsDomain, JsDomain) = ("cli".parse().unwrap(), "own".parse().unwrap());

    let one = configs(vec![bare], &Harnesses::builtin(), "nats://cli:4222", Some(cli_domain.clone()), None).unwrap();
    assert_eq!(one[0].connection.nats_url, "nats://cli:4222");
    assert_eq!(one[0].connection.js_domain, Some(cli_domain.clone()));

    let mut own = ant("dev-a");
    own.js_domain = Some(own_domain.clone());
    let one = configs(vec![own], &Harnesses::builtin(), "nats://cli:4222", Some(cli_domain), None).unwrap();
    assert_eq!(one[0].connection.nats_url, "nats://dev-a:4222");
    assert_eq!(one[0].connection.js_domain, Some(own_domain));
}

#[test]
fn creds_given_stand_for_the_one_ant_named_or_listed_never_a_builtin_ant_unnamed() {
    let dir = ants_test_support::scratch_dir("serve-creds").canonicalize().unwrap();
    let config = dir.join("config.toml");
    let cli = dir.join("cli.creds");
    let listing = |text: &str, line: &str| {
        std::fs::write(&config, format!("harness = \"copilot\"\n{text}")).unwrap();
        ants(&args(&format!("--config {} {line}", config.display())), &config, "box", Some(&cli))
    };
    let creds = |listed: &Listed| -> Vec<(String, Option<PathBuf>)> {
        listed.ants.iter().map(|ant| (ant.name.to_string(), ant.creds.clone())).collect()
    };
    let dev = |name: &str, more: &str| format!("[[ant]]\nname = \"{name}\"\nworkdir = \".\"\n{more}");
    let lone = listing(&dev("dev-a", ""), "").unwrap();
    assert_eq!(creds(&lone)[0], ("dev-a".to_owned(), Some(cli.clone())), "the file's one ant, with the sage it names");
    let builtin_here = |name: &str| builtin_creds_here(name).then(|| name.to_owned());
    let served: Vec<String> = creds(&lone).into_iter().skip(1).map(|(name, _)| name).collect();
    let expected: Vec<String> = ["system-messenger", "system-sage"].into_iter().filter_map(builtin_here).collect();
    assert_eq!(served, expected, "the built-in ants are left to another machine but where their own are");

    let two = format!("{}\n{}", dev("dev-a", ""), dev("dev-b", ""));
    let err = format!("{:#}", listing(&two, "").unwrap_err());
    assert!(err.contains("--creds (ANTS_CREDS) stands for one ant's credentials, and the file lists 2"), "{err}");
    let picked = listing(&two, "--ant dev-b").unwrap();
    assert_eq!(creds(&picked), [("dev-b".to_owned(), Some(cli.clone()))]);
    let own = listing(&dev("dev-a", "creds = \"own.creds\""), "--ant dev-a").unwrap();
    assert_eq!(creds(&own), [("dev-a".to_owned(), Some(dir.join("own.creds")))], "the file's own come first");
    if !builtin_creds_here("system-sage") {
        let sage = listing(&dev("dev-a", "creds = \"own.creds\""), "--ant system-sage").unwrap();
        assert_eq!(
            creds(&sage),
            [("system-sage".to_owned(), Some(cli.clone()))],
            "the sage left elsewhere, served here"
        );
    }
    let err = format!("{:#}", listing("", "").unwrap_err());
    assert!(err.contains("the file lists no [[ant]]"), "{err}");
    let flags = ants(&args("--ant dev-c"), &dir.join("none.toml"), "box", Some(&cli)).unwrap();
    assert_eq!(creds(&flags), [("dev-c".to_owned(), Some(cli.clone()))], "an ant described by flags");
    let flags = ants(&args("--ant system-messenger"), &dir.join("none.toml"), "box", Some(&cli)).unwrap_err();
    assert!(flags.to_string().contains("reserved for a built-in ant"), "{flags:#}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_reload_starts_new_ants_and_stops_or_restarts_the_others() {
    let config = |name: &str, model: Option<&str>| {
        let mut p = ant(name);
        p.model = model.map(str::to_owned);
        configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None).unwrap().remove(0)
    };
    let names = |start: Vec<(Config, Stop)>| start.into_iter().map(|(c, _)| c.ant.to_string()).collect::<Vec<_>>();
    let state = |running: &HashMap<UserId, Running>, name: &str| {
        let r = &running[&name.parse::<UserId>().unwrap()];
        (r.stopping, r.next.as_ref().map(|c| c.harness.model_settings.model.clone()))
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
    use ants_daemon::limits::caps::{Members, Pool};
    let member = |name: &str| name.parse::<UserId>().unwrap();
    let config = |members: &[&str]| {
        let pool = Pool {
            template: "worker".to_owned(),
            daily_tokens: Some(100),
            daily_cost: None,
            members: Members::new(members.iter().map(|m| member(m)).collect()),
        };
        let p = Ant { pool: Some(pool), ..ant("lead-a") };
        configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None).unwrap().remove(0)
    };
    let mut running = HashMap::new();
    let (served, _) = reconcile(&mut running, vec![config(&["lead-a"])]).remove(0);
    assert!(reconcile(&mut running, vec![config(&["lead-a", "lead-b"])]).is_empty());
    assert!(!running[&member("lead-a")].stopping, "only its pool's members changed");
    assert_eq!(served.spending.pool.unwrap().members.get(), [member("lead-a"), member("lead-b")]);
}

#[test]
fn a_reload_pauses_and_resumes_a_running_ant_without_a_restart() {
    let ant_named = |paused: bool, model: Option<&str>| {
        let p = Ant { paused, model: model.map(str::to_owned), ..ant("dev-a") };
        configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None).unwrap().remove(0)
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
        configs(vec![ant], &harnesses, "nats://cli:4222", None, Some(&shared)).unwrap().remove(0)
    };
    let mut running = HashMap::new();
    let (config, _) = reconcile(&mut running, vec![served(dev_a())]).remove(0);
    assert_eq!(
        (config.harness.model_settings.model.as_deref(), config.drift.get()),
        (Some("gpt-5"), vec!["model".into()])
    );
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
    let config = configs(vec![p], &Harnesses::builtin(), "nats://cli:4222", None, None).unwrap().remove(0);
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
        &[],
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
        assert!(pick(vec![ant("dev-a")], &[], &args(line), Path::new("config.toml")).is_err());
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
        &[],
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
        assert!(pick(vec![ant("dev-a")], &[], &args(line), Path::new("config.toml")).is_err());
    }
}
