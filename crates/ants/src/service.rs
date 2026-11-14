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
        let home = std::env::var_os("HOME").map(PathBuf::from);
        Services { manager: manager.filter(|_| home.is_some()), home: home.unwrap_or_default(), run: Box::new(run) }
    }

    /// `action` on the installed `service` running `file`, when it runs; the service of another file is left alone.
    pub fn apply(&mut self, service: Service, action: Action, file: &Path) -> Outcome {
        let Some(manager) = self.manager else { return Outcome::Unmanaged };
        let unit_file = match manager {
            Manager::Launchd => self.home.join("Library/LaunchAgents").join(format!("{}.plist", service.label())),
            Manager::Systemd => self.home.join(".config/systemd/user").join(format!("{}.service", service.unit())),
        };
        match std::fs::read_to_string(&unit_file) {
            Ok(text) if runs(manager, &text, file) => {}
            _ => return Outcome::Unmanaged,
        }
        let target = match manager {
            Manager::Launchd => match self.output(&["id", "-u"]) {
                Ok(uid) => format!("gui/{}/{}", uid.trim(), service.label()),
                Err(e) => return Outcome::Failed("id -u".to_owned(), e),
            },
            Manager::Systemd => service.unit().to_owned(),
        };
        let running = match manager {
            Manager::Launchd => self
                .output(&["launchctl", "print", &target])
                .is_ok_and(|out| out.lines().any(|l| l.starts_with("\tpid = "))),
            Manager::Systemd => self.output(&["systemctl", "--user", "is-active", "--quiet", &target]).is_ok(),
        };
        if !running {
            return Outcome::NotRunning;
        }
        let command = command(manager, action, &target);
        let args: Vec<&str> = command.iter().map(String::as_str).collect();
        match self.output(&args) {
            Ok(_) => Outcome::Done(command.join(" ")),
            Err(e) => Outcome::Failed(command.join(" "), e),
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
    };
    args.iter().copied().chain([target]).map(str::to_owned).collect()
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
