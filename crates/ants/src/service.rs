//! The user services `scripts/install-units.sh` installs from contrib/: launchd agents on macOS
//! (`~/Library/LaunchAgents/<label>.plist`, in the domain `gui/<uid>`) and systemd user units on Linux
//! (`~/.config/systemd/user/<unit>.service`). Reloads or restarts the one running a given file, when it is installed
//! and running; otherwise says how to do it by hand. Installs and starts the nats-server and daemon units for
//! `ants setup`, rendered from the same templates.

use std::path::{Path, PathBuf};
use std::process::Command;

/// A service of contrib/.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Service {
    /// `ants daemon --config FILE`.
    Daemon,
    /// `nats-server -c FILE`: the machine's leaf node, or the hub.
    Nats,
    /// `ants webhook serve --config FILE`.
    Webhook,
    /// `devtunnel connect TUNNEL`, a leaf node's uplink through a Dev Tunnel.
    DevTunnel,
    /// `devtunnel host TUNNEL`, on the hub machine.
    DevTunnelHost,
    /// `ants bridge connect URL`, a leaf node's uplink through the HTTPS fallback bridge.
    Bridge,
    /// `ants bridge serve`, on the hub machine.
    BridgeServe,
}

impl Service {
    fn label(self) -> &'static str {
        match self {
            Service::Daemon => "ants.daemon",
            Service::Nats => "ants.nats",
            Service::Webhook => "ants.webhook",
            Service::DevTunnel => "ants.devtunnel",
            Service::DevTunnelHost => "ants.devtunnel-host",
            Service::Bridge => "ants.bridge",
            Service::BridgeServe => "ants.bridge-serve",
        }
    }

    fn unit(self) -> &'static str {
        match self {
            Service::Daemon => "ants-daemon",
            Service::Nats => "ants-nats",
            Service::Webhook => "ants-webhook",
            Service::DevTunnel => "ants-devtunnel",
            Service::DevTunnelHost => "ants-devtunnel-host",
            Service::Bridge => "ants-bridge",
            Service::BridgeServe => "ants-bridge-serve",
        }
    }
}

/// What an installed unit runs, as [`Services::unit`] reads it.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Unit {
    /// The program and its arguments.
    pub args: Vec<String>,
    /// The file on its standard input.
    pub stdin: Option<PathBuf>,
    /// The environment it sets.
    pub env: Vec<(String, String)>,
}

impl Unit {
    /// The value following the argument `flag`.
    pub fn flag(&self, flag: &str) -> Option<&str> {
        self.args.iter().position(|a| a == flag).and_then(|i| self.args.get(i + 1)).map(String::as_str)
    }

    pub fn env(&self, name: &str) -> Option<&str> {
        self.env.iter().find(|(n, _)| n == name).map(|(_, v)| v.as_str())
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
        Self::under(std::env::home_dir())
    }

    /// The services of the current user whose units are under `home`.
    pub fn under(home: Option<PathBuf>) -> Self {
        let manager = match std::env::consts::OS {
            "macos" => Some(Manager::Launchd),
            "linux" => Some(Manager::Systemd),
            _ => None,
        };
        Services { manager: manager.filter(|_| home.is_some()), home: home.unwrap_or_default(), run: Box::new(run) }
    }

    /// The services under `home` of this platform's manager, answering commands with `answer`.
    #[cfg(all(test, unix))]
    pub(crate) fn answering(home: &Path, answer: impl Fn(&str) -> Ran + 'static) -> Self {
        let manager = if cfg!(target_os = "macos") { Manager::Launchd } else { Manager::Systemd };
        let run = move |args: &[&str]| Ok(answer(&args.join(" ")));
        Services { manager: Some(manager), home: home.to_owned(), run: Box::new(run) }
    }

    /// `action` on the installed `service` running `file`, when it runs; the service of another file is left alone.
    pub fn apply(&mut self, service: Service, action: Action, file: &Path) -> Outcome {
        let (manager, target) = match self.locate(service, Some(file)) {
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
        self.state_of(service, Some(file))
    }

    /// Whether `service` is installed, to run whatever file, and whether it runs.
    pub fn status(&mut self, service: Service) -> State {
        self.state_of(service, None)
    }

    /// What `service` runs, when installed.
    pub fn unit(&self, service: Service) -> Option<Unit> {
        let manager = self.manager?;
        let text = std::fs::read_to_string(self.unit_file(manager, service)).ok()?;
        Some(match manager {
            Manager::Launchd => plist_unit(&text),
            Manager::Systemd => systemd_unit(&text),
        })
    }

    /// The command doing `action` on the installed `service` by hand, on this machine.
    pub fn by_hand(&self, service: Service, action: Action) -> Option<String> {
        Some(
            match self.manager? {
                Manager::Launchd => command(Manager::Launchd, action, &format!("gui/$(id -u)/{}", service.label())),
                Manager::Systemd => command(Manager::Systemd, action, service.unit()),
            }
            .join(" "),
        )
    }

    fn state_of(&mut self, service: Service, file: Option<&Path>) -> State {
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

    /// The manager and target (`gui/<uid>/<label>`, or the unit) of `service` installed to run `file` (any file when
    /// None), or the state of a service not installed so.
    fn locate(&mut self, service: Service, file: Option<&Path>) -> Result<(Manager, String), State> {
        let Some(manager) = self.manager else { return Err(State::Unsupported) };
        let unit_file = self.unit_file(manager, service);
        match std::fs::read_to_string(&unit_file) {
            Ok(text) if file.is_none_or(|file| runs(manager, &text, file)) => {}
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

/// What a unit runs: the executables, the environment and where launchd agents log.
pub struct Values {
    pub ants: PathBuf,
    pub nats_server: PathBuf,
    /// The daemon's `PATH`, on which it finds harnesses and tools.
    pub path: String,
}

/// What came of [`Services::install`].
#[derive(Debug, PartialEq, Eq)]
pub enum Installed {
    /// The unit file was written there and the service started with this command; with a dry run, would be.
    New(PathBuf, String),
    /// Installed for the file already; it was not running and this command started it, or with a dry run would.
    Started(String),
    /// Installed for the file already, and running.
    Running,
    /// This unit file runs another file: left alone.
    Other(PathBuf),
    /// The platform has no service manager ants installs units for.
    Unsupported,
    /// This command failed, for this reason.
    Failed(String, String),
}

/// A launchd agent with no file of its own to run: the log rotation of the other agents.
const LOGROTATE: &str = "ants.logrotate";

impl Services {
    /// Install `service` to run `file`, as scripts/install-units.sh does, and start it, unless it is installed for a
    /// file already: then start it when it is not running, and leave the unit of another file alone. On macOS also
    /// install the agent rotating the logs, if missing. With `dry_run`, only say what would be done.
    pub fn install(&mut self, service: Service, file: &Path, values: &Values, dry_run: bool) -> Installed {
        let Some(manager) = self.manager else { return Installed::Unsupported };
        let file = std::path::absolute(file).unwrap_or_else(|_| file.to_owned());
        let installed = match self.locate(service, Some(&file)) {
            Ok((manager, target)) => match self.running(manager, &target) {
                Some(true) => return Installed::Running,
                Some(false) => self.started(command(manager, Action::Start, &target), dry_run),
                None => {
                    let unit = self.unit_file(manager, service);
                    let bootstrap = ["launchctl", "bootstrap", launchd_domain(&target), &unit.to_string_lossy()];
                    self.started(bootstrap.map(str::to_owned).to_vec(), dry_run)
                }
            },
            Err(State::NotInstalled) => {
                let Some(template) = template(manager, service) else { return Installed::Unsupported };
                let unit = self.unit_file(manager, service);
                let text = render(manager, template, &file, values, &self.logs());
                self.write_and_start(manager, &unit, &text, dry_run)
            }
            Err(State::Other(unit)) => Installed::Other(unit),
            Err(State::Unknown(command, e)) => Installed::Failed(command, e),
            Err(_) => Installed::Unsupported,
        };
        if manager == Manager::Launchd && !matches!(installed, Installed::Failed(..)) {
            let unit = self.home.join("Library/LaunchAgents").join(format!("{LOGROTATE}.plist"));
            if !unit.exists() {
                let text = render(
                    manager,
                    include_str!("../../../contrib/launchd/ants.logrotate.plist"),
                    &file,
                    values,
                    &self.logs(),
                );
                if let Installed::Failed(command, e) = self.write_and_start(manager, &unit, &text, dry_run) {
                    return Installed::Failed(command, e);
                }
            }
        }
        installed
    }

    /// Where launchd agents log.
    fn logs(&self) -> PathBuf {
        self.home.join("Library/Logs/ants")
    }

    fn started(&mut self, command: Vec<String>, dry_run: bool) -> Installed {
        let shown = command.join(" ");
        if dry_run {
            return Installed::Started(shown);
        }
        let args: Vec<&str> = command.iter().map(String::as_str).collect();
        match self.output(&args) {
            Ok(_) => Installed::Started(shown),
            Err(e) => Installed::Failed(shown, e),
        }
    }

    /// Write `text` to `unit` and start the service it defines: bootstrap a launchd agent, enable a systemd unit.
    fn write_and_start(&mut self, manager: Manager, unit: &Path, text: &str, dry_run: bool) -> Installed {
        let commands: Vec<Vec<String>> = match manager {
            Manager::Launchd => {
                let domain = match self.output(&["id", "-u"]) {
                    Ok(uid) => format!("gui/{}", uid.trim()),
                    Err(e) => return Installed::Failed("id -u".to_owned(), e),
                };
                vec![vec!["launchctl".into(), "bootstrap".into(), domain, unit.to_string_lossy().into_owned()]]
            }
            Manager::Systemd => {
                let name = unit.file_name().map_or_default(|n| n.to_string_lossy().into_owned());
                let reload = ["systemctl", "--user", "daemon-reload"];
                let enable = ["systemctl", "--user", "enable", "--now", &name];
                vec![reload.map(str::to_owned).to_vec(), enable.map(str::to_owned).to_vec()]
            }
        };
        let shown = commands.iter().map(|c| c.join(" ")).collect::<Vec<_>>().join(" && ");
        if dry_run {
            return Installed::New(unit.to_owned(), shown);
        }
        let written = (|| {
            std::fs::create_dir_all(unit.parent().unwrap_or(Path::new(".")))?;
            if manager == Manager::Launchd {
                std::fs::create_dir_all(self.logs())?;
            }
            std::fs::write(unit, text)
        })();
        if let Err(e) = written {
            return Installed::Failed(format!("writing {}", unit.display()), e.to_string());
        }
        for command in &commands {
            let args: Vec<&str> = command.iter().map(String::as_str).collect();
            if let Err(e) = self.output(&args) {
                return Installed::Failed(command.join(" "), e);
            }
        }
        Installed::New(unit.to_owned(), shown)
    }
}

/// The contrib/ template of `service` for `manager`, for the services [`Services::install`] installs.
fn template(manager: Manager, service: Service) -> Option<&'static str> {
    Some(match (manager, service) {
        (Manager::Launchd, Service::Daemon) => include_str!("../../../contrib/launchd/ants.daemon.plist"),
        (Manager::Launchd, Service::Nats) => include_str!("../../../contrib/launchd/ants.nats.plist"),
        (Manager::Systemd, Service::Daemon) => include_str!("../../../contrib/systemd/ants-daemon.service"),
        (Manager::Systemd, Service::Nats) => include_str!("../../../contrib/systemd/ants-nats.service"),
        _ => return None,
    })
}

/// `template` with its placeholders filled in as scripts/install-units.sh fills them, for a unit running `file`
/// (the daemon's config file, or nats-server's configuration).
fn render(manager: Manager, template: &str, file: &Path, values: &Values, logs: &Path) -> String {
    let escape = |value: &str| match manager {
        Manager::Launchd => value.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;"),
        Manager::Systemd => value.replace('%', "%%"),
    };
    let file = file.to_string_lossy();
    let values = [
        ("@ANTS@", values.ants.to_string_lossy()),
        ("@NATS_SERVER@", values.nats_server.to_string_lossy()),
        ("@NATS_CONF@", file.clone()),
        ("@CONFIG@", file),
        ("@PATH@", values.path.as_str().into()),
        ("@LOGS@", logs.to_string_lossy()),
    ];
    values.iter().fold(template.to_owned(), |text, (placeholder, value)| text.replace(placeholder, &escape(value)))
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

/// The text between `start` and the following `end` in `text`, after `from`.
fn between<'a>(text: &'a str, from: usize, start: &str, end: &str) -> Option<(&'a str, usize)> {
    let at = from + text[from..].find(start)? + start.len();
    let len = text[at..].find(end)?;
    Some((&text[at..at + len], at + len + end.len()))
}

fn xml_unescape(text: &str) -> String {
    text.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&apos;", "'").replace("&amp;", "&")
}

/// What a launchd property list runs: its `ProgramArguments`, `StandardInPath` and `EnvironmentVariables`.
fn plist_unit(text: &str) -> Unit {
    let mut unit = Unit::default();
    let value = |key: &str, open: &str, close: &str| {
        let at = text.find(&format!("<key>{key}</key>"))?;
        between(text, at, open, close).map(|(inner, _)| inner)
    };
    let strings = |inner: &str| {
        let mut found = Vec::new();
        let mut from = 0;
        while let Some((s, next)) = between(inner, from, "<string>", "</string>") {
            found.push(xml_unescape(s));
            from = next;
        }
        found
    };
    if let Some(array) = value("ProgramArguments", "<array>", "</array>") {
        unit.args = strings(array);
    }
    unit.stdin = value("StandardInPath", "<string>", "</string>").map(|p| PathBuf::from(xml_unescape(p)));
    if let Some(dict) = value("EnvironmentVariables", "<dict>", "</dict>") {
        let mut from = 0;
        while let Some((key, next)) = between(dict, from, "<key>", "</key>") {
            let Some((value, after)) = between(dict, next, "<string>", "</string>") else { break };
            unit.env.push((xml_unescape(key), xml_unescape(value)));
            from = after;
        }
    }
    unit
}

/// The words of a systemd command line or value: double quotes group, a backslash escapes, `%%` is `%`.
fn systemd_words(line: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word: Option<String> = None;
    let mut quoted = false;
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                quoted = !quoted;
                word.get_or_insert_default();
            }
            '\\' => word.get_or_insert_default().extend(chars.next()),
            c if c.is_whitespace() && !quoted => words.extend(word.take()),
            c => word.get_or_insert_default().push(c),
        }
    }
    words.extend(word);
    words.into_iter().map(|w| w.replace("%%", "%")).collect()
}

/// What a systemd unit runs: its `ExecStart`, `StandardInput=file:` and `Environment`.
fn systemd_unit(text: &str) -> Unit {
    let mut unit = Unit::default();
    let joined = text.replace("\\\n", " ");
    for line in joined.lines().map(str::trim) {
        if let Some(exec) = line.strip_prefix("ExecStart=") {
            unit.args = systemd_words(exec);
        } else if let Some(file) = line.strip_prefix("StandardInput=file:") {
            unit.stdin = Some(PathBuf::from(file.replace("%%", "%")));
        } else if let Some(env) = line.strip_prefix("Environment=") {
            for pair in systemd_words(env) {
                if let Some((name, value)) = pair.split_once('=') {
                    unit.env.push((name.to_owned(), value.to_owned()));
                }
            }
        }
    }
    unit
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
mod tests;
