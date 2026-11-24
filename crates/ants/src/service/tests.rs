//! Tests of the user services: launchd and systemd reloads and restarts, service states, the logrotate agent, the
//! units' restart policy, units reading back what was installed, installing, and the manual commands.

use super::*;
use std::sync::{Arc, Mutex};

use ants_daemon::sync::MutexExt as _;

/// Services of `manager` under `home`, answering commands with `answer` and noting them in the log returned.
fn fake(manager: Manager, home: &Path, answer: impl Fn(&str) -> Ran + 'static) -> (Services, Arc<Mutex<Vec<String>>>) {
    let log = Arc::new(Mutex::new(Vec::new()));
    let noted = log.clone();
    let run = move |args: &[&str]| {
        let command = args.join(" ");
        noted.locked().push(command.clone());
        Ok(answer(&command))
    };
    (Services { manager: Some(manager), home: home.to_owned(), run: Box::new(run) }, log)
}

fn ok(stdout: &str) -> Ran {
    Ran { ok: true, stdout: stdout.to_owned(), stderr: String::new() }
}

fn failed(stderr: &str) -> Ran {
    Ran { ok: false, stdout: String::new(), stderr: stderr.to_owned() }
}

fn install(path: PathBuf, text: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

#[test]
fn launchd_reloads_or_restarts_the_running_agent_of_the_file_only() {
    let home = ants_test_support::scratch_dir("service-launchd").canonicalize().unwrap();
    let config = home.join("a & b/config.toml");
    let (mut services, log) = fake(Manager::Launchd, &home, |_| ok(""));
    assert_eq!(services.apply(Service::Daemon, Action::Reload, &config), Outcome::Unmanaged, "not installed");
    assert!(log.locked().is_empty());

    let plist = |file: &str| format!("<array>\n\t<string>/bin/ants</string>\n\t<string>{file}</string>\n</array>");
    let agents = home.join("Library/LaunchAgents");
    install(agents.join("ants.daemon.plist"), &plist(&format!("{}.bak", home.join("a &amp; b/config.toml").display())));
    assert_eq!(services.apply(Service::Daemon, Action::Reload, &config), Outcome::Unmanaged, "another file");
    install(agents.join("ants.daemon.plist"), &plist(&home.join("a &amp; b/config.toml").display().to_string()));

    let (mut services, log) = services_answering(&home, "\tstate = running\n\tpid = 4242\n");
    assert_eq!(
        services.apply(Service::Daemon, Action::Reload, &config),
        Outcome::Done("launchctl kill SIGHUP gui/501/ants.daemon".to_owned())
    );
    assert_eq!(
        *log.locked(),
        ["id -u", "launchctl print gui/501/ants.daemon", "launchctl kill SIGHUP gui/501/ants.daemon"]
    );

    let leaf = home.join("leaf.conf");
    install(agents.join("ants.nats.plist"), &plist(&leaf.display().to_string()));
    assert_eq!(
        services.apply(Service::Nats, Action::Restart, &leaf),
        Outcome::Done("launchctl kickstart -k gui/501/ants.nats".to_owned())
    );

    let (mut services, log) = services_answering(&home, "\tstate = not running\n");
    assert_eq!(services.apply(Service::Nats, Action::Restart, &leaf), Outcome::NotRunning);
    assert_eq!(log.locked().last().unwrap(), "launchctl print gui/501/ants.nats", "nothing restarted");
    std::fs::remove_dir_all(&home).unwrap();

    fn services_answering(home: &Path, print: &'static str) -> (Services, Arc<Mutex<Vec<String>>>) {
        fake(Manager::Launchd, home, move |command| match command {
            "id -u" => ok("501\n"),
            c if c.starts_with("launchctl print") => ok(print),
            _ => ok(""),
        })
    }
}

#[test]
fn systemd_reloads_or_restarts_the_active_unit_of_the_file_and_reports_a_failure() {
    let home = ants_test_support::scratch_dir("service-systemd").canonicalize().unwrap();
    let config = home.join("100%/config.toml");
    let units = home.join(".config/systemd/user");
    let exec = format!("ExecStart=\"/bin/ants\" daemon --config \"{}\"\n", home.join("100%%/config.toml").display());
    install(units.join("ants-daemon.service"), &exec);

    let (mut services, log) = fake(Manager::Systemd, &home, |_| ok(""));
    assert_eq!(
        services.apply(Service::Daemon, Action::Reload, &config),
        Outcome::Done("systemctl --user reload ants-daemon".to_owned())
    );
    assert_eq!(
        *log.locked(),
        ["systemctl --user is-active --quiet ants-daemon", "systemctl --user reload ants-daemon"]
    );
    assert_eq!(services.apply(Service::Nats, Action::Restart, &config), Outcome::Unmanaged);

    let (mut services, _) = fake(Manager::Systemd, &home, |command| match command {
        "systemctl --user is-active --quiet ants-daemon" => ok(""),
        _ => failed("Job failed.\n"),
    });
    assert_eq!(
        services.apply(Service::Daemon, Action::Restart, &config),
        Outcome::Failed("systemctl --user restart ants-daemon".to_owned(), "Job failed.".to_owned())
    );
    let (mut services, log) = fake(Manager::Systemd, &home, |_| failed(""));
    assert_eq!(services.apply(Service::Daemon, Action::Reload, &config), Outcome::NotRunning);
    assert_eq!(log.locked().len(), 1);
    std::fs::remove_dir_all(&home).unwrap();
}

#[test]
fn the_state_of_a_service_names_the_command_starting_it() {
    let home = ants_test_support::scratch_dir("service-state").canonicalize().unwrap();
    let config = home.join("config.toml");
    let (mut services, _) = fake(Manager::Systemd, &home, |_| ok(""));
    assert_eq!(services.state(Service::Daemon, &config), State::NotInstalled);
    let units = home.join(".config/systemd/user");
    install(units.join("ants-daemon.service"), "ExecStart=\"/bin/ants\" daemon --config \"/elsewhere.toml\"\n");
    assert_eq!(services.state(Service::Daemon, &config), State::Other(units.join("ants-daemon.service")));
    let exec = format!("ExecStart=\"/bin/ants\" daemon --config \"{}\"\n", config.display());
    install(units.join("ants-daemon.service"), &exec);
    assert_eq!(services.state(Service::Daemon, &config), State::Running);
    let (mut services, _) = fake(Manager::Systemd, &home, |_| failed(""));
    assert_eq!(
        services.state(Service::Daemon, &config),
        State::Stopped("systemctl --user start ants-daemon".to_owned())
    );

    let plist = format!("<array>\n\t<string>/bin/ants</string>\n\t<string>{}</string>\n</array>", config.display());
    install(home.join("Library/LaunchAgents/ants.daemon.plist"), &plist);
    let (mut services, _) = fake(Manager::Launchd, &home, |command| match command {
        "id -u" => ok("501\n"),
        _ => failed("Could not find service"),
    });
    let bootstrap =
        format!("launchctl bootstrap gui/501 {}", home.join("Library/LaunchAgents/ants.daemon.plist").display());
    assert_eq!(services.state(Service::Daemon, &config), State::Stopped(bootstrap));
    std::fs::remove_dir_all(&home).unwrap();
}

#[test]
fn the_logrotate_agent_copies_and_truncates_logs_over_the_cap_in_place() {
    use std::io::Write;
    let plist = include_str!("../../../../contrib/launchd/ants.logrotate.plist");
    let script = plist.split("<string>-c</string>").nth(1).unwrap();
    let script = script.split_once("<string>").unwrap().1.split_once("</string>").unwrap().0;
    let script = script.replace("&gt;", ">").replace("&lt;", "<").replace("&amp;", "&");
    let logs = ants_test_support::scratch_dir("service-logrotate");
    let rotate = || {
        let mut sh = Command::new("sh");
        assert!(sh.args(["-c", &script, "ants-logrotate"]).arg(&logs).args(["100", "2"]).status().unwrap().success());
    };
    let gunzip = |name: &str| {
        let out = Command::new("gzip").arg("-dc").arg(logs.join(name)).output().unwrap();
        String::from_utf8(out.stdout).unwrap()
    };
    let read = |name: &str| std::fs::read_to_string(logs.join(name)).unwrap();
    // As launchd opens StandardOutPath: in append mode, held across rotations.
    let mut daemon = std::fs::OpenOptions::new().create(true).append(true).open(logs.join("daemon.log")).unwrap();
    std::fs::write(logs.join("nats.log"), "short\n").unwrap();
    std::fs::write(logs.join("notes.txt"), "x".repeat(200)).unwrap();

    for round in ["a", "b", "c"] {
        daemon.write_all(format!("{round}\n").repeat(60).as_bytes()).unwrap();
        rotate();
        daemon.write_all(format!("after {round}\n").as_bytes()).unwrap();
    }
    assert_eq!(read("daemon.log"), "after c\n", "written on through the same file, from its start");
    assert_eq!(gunzip("daemon.log.1.gz"), format!("after b\n{}", "c\n".repeat(60)));
    assert_eq!(gunzip("daemon.log.2.gz"), format!("after a\n{}", "b\n".repeat(60)));
    assert!(!logs.join("daemon.log.3.gz").exists(), "two copies kept");
    assert_eq!(read("nats.log"), "short\n");
    assert!(!logs.join("nats.log.1.gz").exists() && !logs.join("notes.txt.1.gz").exists());
    std::fs::remove_dir_all(&logs).unwrap();
}

#[test]
fn a_unit_reads_back_the_arguments_input_and_environment_install_units_wrote() {
    let systemd = "# ExecStart=\"/not/this\"\n[Service]\nExecStart=\"/opt/a b/ants\" bridge connect \\\n    \
                       --token-file \"/home/q/100%%/token\" \\\n    \"https://hub.example/bridge\"\n\
                       Environment=NO_COLOR=1\nEnvironment=\"SSL_CERT_FILE=/etc/ca b.pem\"\n\
                       StandardInput=file:/home/q/100%%/tunnel\n";
    let unit = systemd_unit(systemd);
    assert_eq!(
        unit.args,
        ["/opt/a b/ants", "bridge", "connect", "--token-file", "/home/q/100%/token", "https://hub.example/bridge"]
    );
    assert_eq!(unit.flag("--token-file"), Some("/home/q/100%/token"));
    assert_eq!((unit.env("NO_COLOR"), unit.env("SSL_CERT_FILE")), (Some("1"), Some("/etc/ca b.pem")));
    assert_eq!(unit.stdin.as_deref(), Some(Path::new("/home/q/100%/tunnel")));

    let plist = "<key>ProgramArguments</key>\n<array>\n\t<string>/bin/devtunnel</string>\n\t\
                     <string>connect</string>\n\t<string>a&amp;b</string>\n</array>\n<key>StandardInPath</key>\n\
                     <string>/t&lt;1&gt;</string>\n<key>EnvironmentVariables</key>\n<dict>\n\t<key>NO_COLOR</key>\n\t\
                     <string>1</string>\n</dict>\n<key>StandardOutPath</key>\n<string>/log</string>\n";
    let unit = plist_unit(plist);
    assert_eq!(unit.args, ["/bin/devtunnel", "connect", "a&b"]);
    assert_eq!(unit.stdin.as_deref(), Some(Path::new("/t<1>")));
    assert_eq!(unit.env, [("NO_COLOR".to_owned(), "1".to_owned())]);
}

#[test]
fn install_writes_and_starts_a_missing_unit_and_starts_or_keeps_an_installed_one() {
    let home = ants_test_support::scratch_dir("service-install").canonicalize().unwrap();
    let config = home.join("a & b/config.toml");
    let values =
        Values { ants: "/bin/ants".into(), nats_server: "/bin/nats-server".into(), path: "/bin:/usr/bin".into() };
    let launchd = |print: &'static str| {
        fake(Manager::Launchd, &home, move |command| match command {
            "id -u" => ok("501\n"),
            c if c.starts_with("launchctl print") => match print {
                "" => failed("Could not find service"),
                print => ok(print),
            },
            _ => ok(""),
        })
    };
    let plist = home.join("Library/LaunchAgents/ants.daemon.plist");
    let logrotate = home.join("Library/LaunchAgents/ants.logrotate.plist");
    let (mut services, log) = launchd("");
    let bootstrap = format!("launchctl bootstrap gui/501 {}", plist.display());
    assert_eq!(
        services.install(Service::Daemon, &config, &values, true),
        Installed::New(plist.clone(), bootstrap.clone())
    );
    assert!(!plist.exists() && !logrotate.exists(), "a dry run writes nothing");
    assert!(log.locked().iter().all(|c| !c.starts_with("launchctl bootstrap")), "nor starts anything");

    assert_eq!(
        services.install(Service::Daemon, &config, &values, false),
        Installed::New(plist.clone(), bootstrap.clone())
    );
    let text = std::fs::read_to_string(&plist).unwrap();
    assert!(text.contains("<string>/bin/ants</string>") && text.contains("<string>/bin:/usr/bin</string>"), "{text}");
    assert!(text.contains(&format!("<string>{}</string>", home.join("a &amp; b/config.toml").display())), "{text}");
    assert!(["@ANTS@", "@CONFIG@", "@PATH@", "@LOGS@"].iter().all(|p| !text.contains(p)), "filled in: {text}");
    assert!(
        std::fs::read_to_string(&logrotate).unwrap().contains(&home.join("Library/Logs/ants").display().to_string())
    );
    assert!(log.locked().contains(&bootstrap));
    assert!(log.locked().contains(&format!("launchctl bootstrap gui/501 {}", logrotate.display())));
    assert_eq!(services.state(Service::Daemon, &config), State::Stopped(bootstrap), "found as installed");

    let (mut services, log) = launchd("\tstate = not running\n");
    assert_eq!(
        services.install(Service::Daemon, &config, &values, false),
        Installed::Started("launchctl kickstart gui/501/ants.daemon".to_owned())
    );
    assert!(!log.locked().iter().any(|c| c.starts_with("launchctl bootstrap")), "logrotate kept");
    let (mut services, _) = launchd("\tpid = 7\n");
    assert_eq!(services.install(Service::Daemon, &config, &values, false), Installed::Running);
    assert_eq!(services.install(Service::Daemon, &home.join("other.toml"), &values, false), Installed::Other(plist));

    let (mut services, log) = fake(Manager::Systemd, &home, |command| match command {
        c if c.starts_with("systemctl --user is-active") => failed(""),
        _ => ok(""),
    });
    let conf = home.join("100%/nats.conf");
    let unit = home.join(".config/systemd/user/ants-nats.service");
    let start = "systemctl --user daemon-reload && systemctl --user enable --now ants-nats.service".to_owned();
    assert_eq!(services.install(Service::Nats, &conf, &values, false), Installed::New(unit.clone(), start));
    let text = std::fs::read_to_string(&unit).unwrap();
    assert!(
        text.contains(&format!("ExecStart=\"/bin/nats-server\" -c \"{}\"", home.join("100%%/nats.conf").display()))
    );
    assert_eq!(*log.locked(), ["systemctl --user daemon-reload", "systemctl --user enable --now ants-nats.service"]);
    assert_eq!(services.state(Service::Nats, &conf), State::Stopped("systemctl --user start ants-nats".to_owned()));
    std::fs::remove_dir_all(&home).unwrap();
}

#[test]
fn the_manual_commands_cover_both_managers() {
    assert_eq!(
        manual(Service::Nats, Action::Restart),
        "macOS: launchctl kickstart -k gui/$(id -u)/ants.nats, Linux: systemctl --user restart ants-nats"
    );
    assert_eq!(
        manual(Service::Daemon, Action::Reload),
        "macOS: launchctl kill SIGHUP gui/$(id -u)/ants.daemon, Linux: systemctl --user reload ants-daemon"
    );
}

/// The units of contrib/`dir` with `extension`, by file name.
fn contrib_units(dir: &str, extension: &str) -> Vec<(String, String)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../contrib").join(dir);
    let mut units: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == extension))
        .map(|path| (path.file_name().unwrap().to_string_lossy().into_owned(), std::fs::read_to_string(&path).unwrap()))
        .collect();
    units.sort();
    units
}

/// A stray SIGTERM, on which the services exit cleanly, must not leave one down: only a stop through the service
/// manager keeps it stopped.
#[test]
fn every_long_running_unit_starts_its_program_again_after_any_exit() {
    let plists = contrib_units("launchd", "plist");
    assert!(plists.iter().any(|(name, _)| name == "ants.nats.plist") && plists.len() > 5, "{plists:?}");
    for (name, plist) in &plists {
        let keep_alive = plist.split_once("<key>KeepAlive</key>").map(|(_, after)| after.trim_start());
        if *name == format!("{LOGROTATE}.plist") {
            assert_eq!(keep_alive, None, "{name} runs at load and hourly");
        } else {
            assert!(keep_alive.is_some_and(|v| v.starts_with("<true/>")), "{name}: KeepAlive true");
        }
    }
    let units = contrib_units("systemd", "service");
    assert!(units.iter().any(|(name, _)| name == "ants-nats.service") && units.len() > 5, "{units:?}");
    for (name, unit) in &units {
        let restart: Vec<_> = unit.lines().filter(|l| l.starts_with("Restart=")).collect();
        assert_eq!(restart, ["Restart=always"], "{name}");
    }
}
