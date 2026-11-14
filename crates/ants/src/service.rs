//! The user services `scripts/install-units.sh` installs from contrib/: launchd agents on macOS
//! (`~/Library/LaunchAgents/<label>.plist`, in the domain `gui/<uid>`) and systemd user units on Linux
//! (`~/.config/systemd/user/<unit>.service`). Reloads or restarts the one running a given file, when it is installed
//! and running; otherwise says how to do it by hand.

use std::path::{Path, PathBuf};
use std::process::Command;

/// A service of contrib/.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Service {
    /// `ants daemon --config FILE`.
    Daemon,
    /// `nats-server -c FILE`: the machine's leaf node, or the hub.
    Nats,
}

impl Service {
    fn label(self) -> &'static str {
        match self {
            Service::Daemon => "ants.daemon",
            Service::Nats => "ants.nats",
        }
    }

    fn unit(self) -> &'static str {
        match self {
            Service::Daemon => "ants-daemon",
            Service::Nats => "ants-nats",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// SIGHUP.
    Reload,
    /// Stop and start again.
    Restart,
    /// Start a service that is not running.
    Start,
}

/// Whether a service is installed to run a given file, and whether it runs ([`Services::state`]).
#[derive(Debug, PartialEq, Eq)]
pub enum State {
    /// The platform has no service manager ants installs units for.
    Unsupported,
    NotInstalled,
    /// Installed, from this unit file, to run another file.
    Other(PathBuf),
    /// Installed and not running; this command starts it.
    Stopped(String),
    Running,
    /// This command, finding where the service runs, failed for this reason.
    Unknown(String, String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Manager {
    Launchd,
    Systemd,
}

/// What came of [`Services::apply`].
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Done with this command.
    Done(String),
    /// The service of the file is installed but not running: it reads the file when it starts.
    NotRunning,
    /// No service of the file is installed.
    Unmanaged,
    /// This command failed, for this reason.
    Failed(String, String),
}

/// What a command run gave: whether it succeeded, its standard output and its standard error.
pub struct Ran {
    pub ok: bool,
    pub stdout: String,
    pub stderr: String,
}

type Runner = Box<dyn FnMut(&[&str]) -> std::io::Result<Ran>>;

/// The services of this machine and user.
pub struct Services {
    manager: Option<Manager>,
    home: PathBuf,
    run: Runner,
}

impl Services {
    /// The services of the current user, found under `$HOME` and driven with launchctl on macOS and systemctl on Linux;
    /// none elsewhere.
    pub fn local() -> Self {
        let manager = match std::env::consts::OS {
            "macos" => Some(Manager::Launchd),
            "linux" => Some(Manager::Systemd),
            _ => None,
        };
        let home = std::env::home_dir();
        Services { manager: manager.filter(|_| home.is_some()), home: home.unwrap_or_default(), run: Box::new(run) }
    }

    /// `action` on the installed `service` running `file`, when it runs; the service of another file is left alone.
    pub fn apply(&mut self, service: Service, action: Action, file: &Path) -> Outcome {
        let (manager, target) = match self.locate(service, file) {
            Ok(found) => found,
            Err(State::Unknown(command, e)) => return Outcome::Failed(command, e),
            Err(_) => return Outcome::Unmanaged,
        };
        if self.running(manager, &target) != Some(true) {
            return Outcome::NotRunning;
        }
        let command = command(manager, action, &target);
        let args: Vec<&str> = command.iter().map(String::as_str).collect();
        match self.output(&args) {
            Ok(_) => Outcome::Done(command.join(" ")),
            Err(e) => Outcome::Failed(command.join(" "), e),
        }
    }

    /// Whether `service` is installed to run `file`, and whether it runs.
    pub fn state(&mut self, service: Service, file: &Path) -> State {
        match self.locate(service, file) {
            Ok((manager, target)) => match self.running(manager, &target) {
                Some(true) => State::Running,
                Some(false) => State::Stopped(command(manager, Action::Start, &target).join(" ")),
                None => {
                    let unit = self.unit_file(manager, service);
                    State::Stopped(format!("launchctl bootstrap {} {}", launchd_domain(&target), unit.display()))
                }
            },
            Err(state) => state,
        }
    }

    fn unit_file(&self, manager: Manager, service: Service) -> PathBuf {
        match manager {
            Manager::Launchd => self.home.join("Library/LaunchAgents").join(format!("{}.plist", service.label())),
            Manager::Systemd => self.home.join(".config/systemd/user").join(format!("{}.service", service.unit())),
        }
    }

    /// The manager and target (`gui/<uid>/<label>`, or the unit) of `service` installed to run `file`, or the state
    /// of a service not installed so.
    fn locate(&mut self, service: Service, file: &Path) -> Result<(Manager, String), State> {
        let Some(manager) = self.manager else { return Err(State::Unsupported) };
        let unit_file = self.unit_file(manager, service);
        match std::fs::read_to_string(&unit_file) {
            Ok(text) if runs(manager, &text, file) => {}
            Ok(_) => return Err(State::Other(unit_file)),
            Err(_) => return Err(State::NotInstalled),
        }
        let target = match manager {
            Manager::Launchd => match self.output(&["id", "-u"]) {
                Ok(uid) => format!("gui/{}/{}", uid.trim(), service.label()),
                Err(e) => return Err(State::Unknown("id -u".to_owned(), e)),
            },
            Manager::Systemd => service.unit().to_owned(),
        };
        Ok((manager, target))
    }

    /// Whether the service `target` runs; None for a launchd agent not loaded at all.
    fn running(&mut self, manager: Manager, target: &str) -> Option<bool> {
        match manager {
            Manager::Launchd => match self.output(&["launchctl", "print", target]) {
                Ok(out) => Some(out.lines().any(|l| l.starts_with("\tpid = "))),
                Err(_) => None,
            },
            Manager::Systemd => Some(self.output(&["systemctl", "--user", "is-active", "--quiet", target]).is_ok()),
        }
    }

    /// The standard output of the command `args`, or why it failed.
    fn output(&mut self, args: &[&str]) -> Result<String, String> {
        match (self.run)(args) {
            Ok(ran) if ran.ok => Ok(ran.stdout),
            Ok(ran) if ran.stderr.trim().is_empty() => Err("failed".to_owned()),
            Ok(ran) => Err(ran.stderr.trim().to_owned()),
            Err(e) => Err(e.to_string()),
        }
    }
}

/// `outcome` of a failed command, as the start of a message.
pub fn failure(outcome: Outcome) -> String {
    match outcome {
        Outcome::Failed(command, error) => format!("`{command}` failed ({error}): "),
        _ => String::new(),
    }
}

/// The commands doing `action` on `service` by hand, on macOS and on Linux.
pub fn manual(service: Service, action: Action) -> String {
    format!(
        "macOS: {}, Linux: {}",
        command(Manager::Launchd, action, &format!("gui/$(id -u)/{}", service.label())).join(" "),
        command(Manager::Systemd, action, service.unit()).join(" ")
    )
}

/// The command doing `action` on the service `target`: `gui/<uid>/<label>` for launchd, the unit for systemd.
fn command(manager: Manager, action: Action, target: &str) -> Vec<String> {
    let args: &[&str] = match (manager, action) {
        (Manager::Launchd, Action::Reload) => &["launchctl", "kill", "SIGHUP"],
        (Manager::Launchd, Action::Restart) => &["launchctl", "kickstart", "-k"],
        (Manager::Systemd, Action::Reload) => &["systemctl", "--user", "reload"],
        (Manager::Systemd, Action::Restart) => &["systemctl", "--user", "restart"],
        (Manager::Launchd, Action::Start) => &["launchctl", "kickstart"],
        (Manager::Systemd, Action::Start) => &["systemctl", "--user", "start"],
    };
    args.iter().copied().chain([target]).map(str::to_owned).collect()
}

/// The launchd domain `gui/<uid>` of the service target `gui/<uid>/<label>`.
fn launchd_domain(target: &str) -> &str {
    target.rsplit_once('/').map_or(target, |(domain, _)| domain)
}

/// Whether the unit file `text`, as install-units.sh fills it in, runs `file`: names it as an argument of its own.
fn runs(manager: Manager, text: &str, file: &Path) -> bool {
    let names = [std::path::absolute(file).ok(), file.canonicalize().ok()];
    names.into_iter().flatten().filter_map(|p| p.to_str().map(str::to_owned)).any(|p| match manager {
        Manager::Launchd => {
            let p = p.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
            text.contains(&format!("<string>{p}</string>"))
        }
        Manager::Systemd => text.contains(&format!("\"{}\"", p.replace('%', "%%"))),
    })
}

fn run(args: &[&str]) -> std::io::Result<Ran> {
    let out = Command::new(args[0]).args(&args[1..]).stdin(std::process::Stdio::null()).output()?;
    Ok(Ran {
        ok: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// Services of `manager` under `home`, answering commands with `answer` and noting them in the log returned.
    fn fake(
        manager: Manager,
        home: &Path,
        answer: impl Fn(&str) -> Ran + 'static,
    ) -> (Services, Arc<Mutex<Vec<String>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let noted = log.clone();
        let run = move |args: &[&str]| {
            let command = args.join(" ");
            noted.lock().unwrap().push(command.clone());
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
        assert!(log.lock().unwrap().is_empty());

        let plist = |file: &str| format!("<array>\n\t<string>/bin/ants</string>\n\t<string>{file}</string>\n</array>");
        let agents = home.join("Library/LaunchAgents");
        install(
            agents.join("ants.daemon.plist"),
            &plist(&format!("{}.bak", home.join("a &amp; b/config.toml").display())),
        );
        assert_eq!(services.apply(Service::Daemon, Action::Reload, &config), Outcome::Unmanaged, "another file");
        install(agents.join("ants.daemon.plist"), &plist(&home.join("a &amp; b/config.toml").display().to_string()));

        let (mut services, log) = services_answering(&home, "\tstate = running\n\tpid = 4242\n");
        assert_eq!(
            services.apply(Service::Daemon, Action::Reload, &config),
            Outcome::Done("launchctl kill SIGHUP gui/501/ants.daemon".to_owned())
        );
        assert_eq!(
            *log.lock().unwrap(),
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
        assert_eq!(log.lock().unwrap().last().unwrap(), "launchctl print gui/501/ants.nats", "nothing restarted");
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
        let exec =
            format!("ExecStart=\"/bin/ants\" daemon --config \"{}\"\n", home.join("100%%/config.toml").display());
        install(units.join("ants-daemon.service"), &exec);

        let (mut services, log) = fake(Manager::Systemd, &home, |_| ok(""));
        assert_eq!(
            services.apply(Service::Daemon, Action::Reload, &config),
            Outcome::Done("systemctl --user reload ants-daemon".to_owned())
        );
        assert_eq!(
            *log.lock().unwrap(),
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
        assert_eq!(log.lock().unwrap().len(), 1);
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
        let plist = include_str!("../../../contrib/launchd/ants.logrotate.plist");
        let script = plist.split("<string>-c</string>").nth(1).unwrap();
        let script = script.split_once("<string>").unwrap().1.split_once("</string>").unwrap().0;
        let script = script.replace("&gt;", ">").replace("&lt;", "<").replace("&amp;", "&");
        let logs = ants_test_support::scratch_dir("service-logrotate");
        let rotate = || {
            let mut sh = Command::new("sh");
            assert!(
                sh.args(["-c", &script, "ants-logrotate"]).arg(&logs).args(["100", "2"]).status().unwrap().success()
            );
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
}
