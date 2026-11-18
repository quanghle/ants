//! The Dev Tunnel and HTTPS bridge services installed: the tunnel's login and token, and whether the bridge answers and
//! carries a connection to the hub's leafnode port.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::services::private_file;
use super::{Check, NETWORK_TIMEOUT, Status};
use crate::node::bridge::{self, Carried, Probe};
use crate::service::{Action, Service, Services, State, Unit};

/// A Dev Tunnel or HTTPS bridge service installed, read before any check waits.
pub(super) struct Transport {
    service: Service,
    state: State,
    unit: Unit,
    restart: String,
}

/// The Dev Tunnel and HTTPS bridge services installed.
pub(super) fn installed_transports(services: &mut Services) -> Vec<Transport> {
    [Service::DevTunnel, Service::DevTunnelHost, Service::Bridge, Service::BridgeServe]
        .into_iter()
        .filter_map(|service| {
            let unit = services.unit(service)?;
            let restart = services.by_hand(service, Action::Restart)?;
            Some(Transport { service, state: services.status(service), unit, restart })
        })
        .collect()
}

/// When a Dev Tunnel login has this little left, warn.
pub(super) const LOGIN_WARN: Duration = Duration::from_hours(24);
/// When a Dev Tunnel connect token has this little left, warn.
pub(super) const TOKEN_WARN: Duration = Duration::from_hours(6);
/// How long a Dev Tunnel connect token lasts, for a token file whose expiry cannot be read.
pub(super) const TOKEN_LIFE: Duration = Duration::from_hours(24);

/// The Dev Tunnel and HTTPS bridge services `transports`: whether each runs, the Dev Tunnel login or connect token,
/// and whether each bridge answers, takes its token and carries a connection to the hub's leafnode port. Nothing
/// without such services.
pub(super) async fn check_transports(transports: Vec<Transport>, now: u64) -> Vec<Check> {
    let mut checks = Vec::new();
    let mut login = None;
    for Transport { service, state, unit, restart } in &transports {
        let (check, what) = match service {
            Service::DevTunnel => ("devtunnel service", "devtunnel connect"),
            Service::DevTunnelHost => ("devtunnel host service", "devtunnel host"),
            Service::Bridge => ("bridge service", "ants bridge connect"),
            _ => ("bridge serve service", "ants bridge serve"),
        };
        checks.push(match state {
            State::Running => Check::ok(check, format!("running `{what}`")),
            State::Stopped(start) => {
                Check::fail(check, format!("`{what}` installed, not running"), vec![start.clone()])
            }
            State::Unknown(command, e) => Check::warn(check, format!("{command}: {e}"), Vec::new()),
            _ => Check::skip(check, "not installed"),
        });
        match service {
            Service::DevTunnel | Service::DevTunnelHost => {
                let program = unit.args.first().map_or_default(PathBuf::from);
                if !program.is_file() {
                    let detail = format!("no devtunnel at {}", program.display());
                    let fix = "scripts/install-units.sh again with --devtunnel-bin PATH".to_owned();
                    checks.push(Check::fail("devtunnel", detail, vec![fix]));
                    continue;
                }
                let tunnel = unit.args.get(2).map_or("TUNNEL", String::as_str);
                match (&unit.stdin, *service) {
                    (Some(token), Service::DevTunnel) => {
                        checks.push(check_devtunnel_token(token, tunnel, restart, now));
                    }
                    _ => {
                        login.get_or_insert((program, restart.clone()));
                    }
                }
            }
            _ => checks.extend(check_bridge(*service, unit, restart).await),
        }
    }
    if let Some((program, restart)) = login {
        checks.push(check_devtunnel_login(&program, &restart, now).await);
    }
    checks
}

/// The Dev Tunnel login of `devtunnel user show`, run with `program`: there, and not expiring within
/// [`LOGIN_WARN`].
pub(super) async fn check_devtunnel_login(program: &Path, restart: &str, now: u64) -> Check {
    let check = "devtunnel login";
    let fix =
        || vec![format!("devtunnel user login -g -d (GitHub, device code; -e for Microsoft Entra), then {restart}")];
    let mut command = tokio::process::Command::new(program);
    command.args(["user", "show", "--json"]).stdin(std::process::Stdio::null()).kill_on_drop(true);
    let output = match tokio::time::timeout(NETWORK_TIMEOUT, command.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => return Check::fail(check, format!("running {}: {e}", program.display()), fix()),
        Err(_) => {
            let detail = format!("`devtunnel user show` gave no answer within {}s", NETWORK_TIMEOUT.as_secs());
            return Check::warn(check, detail, Vec::new());
        }
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let shown: serde_json::Value = match serde_json::from_str(stdout.trim()) {
        Ok(shown) => shown,
        Err(_) => {
            let said = if stdout.trim().is_empty() { String::from_utf8_lossy(&output.stderr) } else { stdout };
            let detail = format!("`devtunnel user show` said: {}", said.trim());
            return Check::warn(check, detail, fix());
        }
    };
    let status = shown.get("status").and_then(|s| s.as_str()).unwrap_or_default();
    if !status.eq_ignore_ascii_case("logged in") {
        let status = if status.is_empty() { "no login" } else { status };
        return Check::fail(check, format!("{status}: the tunnel goes down at its next connection"), fix());
    }
    let user = ["username", "userName", "user", "name"]
        .iter()
        .find_map(|key| shown.get(key).and_then(|v| v.as_str()))
        .map_or(String::new(), |user| format!(" as {user}"));
    match expiry(&shown) {
        Some(expires) if expires <= now => Check::fail(check, format!("logged in{user}, expired"), fix()),
        Some(expires) if expires - now < LOGIN_WARN.as_secs() => {
            Check::warn(check, format!("logged in{user}, expiring in {}", left(expires - now)), fix())
        }
        Some(expires) => Check::ok(check, format!("logged in{user} for {}", left(expires - now))),
        None => Check::ok(check, format!("logged in{user}; a login lasts some days, then devtunnel user login again")),
    }
}

/// The connect token of `tunnel` in `file`, read by the devtunnel service at each start: there, private to its owner
/// and not expiring within [`TOKEN_WARN`] (its `exp`, or [`TOKEN_LIFE`] after it was written).
pub(super) fn check_devtunnel_token(file: &Path, tunnel: &str, restart: &str, now: u64) -> Check {
    let check = "devtunnel token";
    let shown = file.display();
    let renew =
        format!("devtunnel token {tunnel} --scopes connect, its token into {shown} (mode 0600), then {restart}");
    let metadata = match private_file(file, renew.clone()) {
        Ok(metadata) => metadata,
        Err(problem) => return problem.named(check, "the connect token", Status::Warn),
    };
    let token = std::fs::read_to_string(file).unwrap_or_default();
    let (expires, said) = match jwt_expiry(token.trim()) {
        Some(expires) => (expires, format!("expiring in {}", left(expires.saturating_sub(now)))),
        None => {
            let written = metadata.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok());
            let written = written.map_or(0, |t| t.as_secs());
            let age = left(now.saturating_sub(written));
            (written + TOKEN_LIFE.as_secs(), format!("written {age} ago, and a connect token lasts a day"))
        }
    };
    match expires.saturating_sub(now) {
        0 => Check::fail(check, format!("{shown}: expired"), vec![renew]),
        rest if rest < TOKEN_WARN.as_secs() => Check::warn(check, format!("{shown}: {said}"), vec![renew]),
        _ => Check::ok(check, format!("{shown}: {said}")),
    }
}

/// The `exp` of a JSON Web Token.
pub(super) fn jwt_expiry(token: &str) -> Option<u64> {
    let mut parts = token.split('.');
    let (_, payload, _) = (parts.next()?, parts.next()?, parts.next()?);
    let payload = data_encoding::BASE64URL_NOPAD.decode(payload.trim_end_matches('=').as_bytes()).ok()?;
    serde_json::from_slice::<serde_json::Value>(&payload).ok()?.get("exp")?.as_u64()
}

/// The time, as seconds since the epoch, of a field of `value` whose name speaks of expiry.
pub(super) fn expiry(value: &serde_json::Value) -> Option<u64> {
    let object = value.as_object()?;
    object.iter().find_map(|(key, value)| match value {
        serde_json::Value::Object(_) => expiry(value),
        _ if !key.to_ascii_lowercase().contains("expir") => None,
        serde_json::Value::String(text) => {
            chrono::DateTime::parse_from_rfc3339(text).ok().and_then(|t| u64::try_from(t.timestamp()).ok())
        }
        serde_json::Value::Number(n) => n.as_u64().map(|t| if t > 100_000_000_000 { t / 1000 } else { t }),
        _ => None,
    })
}

/// `seconds`, rounded down, as minutes, hours or days.
pub(super) fn left(seconds: u64) -> String {
    match seconds / 60 {
        minutes @ 0..60 => format!("{minutes}m"),
        minutes @ 60..2880 => format!("{}h", minutes / 60),
        minutes => format!("{} days", minutes / 1440),
    }
}

/// The bridge of `service`, `ants bridge connect` or `ants bridge serve` run as `unit`: its token file, whether the
/// bridge answers as one taking it, and whether it carries a connection to the hub's leafnode port ([`check_pipe`]).
pub(super) async fn check_bridge(service: Service, unit: &Unit, restart: &str) -> Vec<Check> {
    let serving = service == Service::BridgeServe;
    let (token_check, url_check, pipe_check) = if serving {
        ("bridge serve token", "bridge serve url", "bridge serve pipe")
    } else {
        ("bridge token", "bridge url", "bridge pipe")
    };
    let mut checks = Vec::new();
    let token = match unit.flag("--token-file") {
        Some(file) => {
            let file = Path::new(file);
            let write = if serving {
                format!("write a long random token into {} (mode 0600), then {restart}", file.display())
            } else {
                format!(
                    "the token of the hub's `ants bridge serve` into {} (mode 0600), then {restart}",
                    file.display()
                )
            };
            match private_file(file, write) {
                Ok(_) => checks.push(Check::ok(token_check, format!("{}, mode 0600", file.display()))),
                Err(problem) => checks.push(problem.named(token_check, "the bridge token", Status::Warn)),
            }
            bridge::token(None, Some(file)).ok().flatten()
        }
        None if serving => {
            let detail = "none: anyone reaching the bridge may carry a connection to the hub";
            checks.push(Check::warn(
                token_check,
                detail,
                vec!["scripts/install-units.sh with --bridge-token FILE".to_owned()],
            ));
            None
        }
        None => None,
    };
    let url = if serving {
        format!("http://{}", unit.flag("--listen").unwrap_or(bridge::DEFAULT_SERVE_LISTEN))
    } else {
        match bridge_url(&unit.args) {
            Some(url) => url.to_owned(),
            None => {
                checks.push(Check::fail(url_check, "the service names no bridge URL", Vec::new()));
                return checks;
            }
        }
    };
    let headers: Vec<String> =
        unit.args.windows(2).filter(|pair| pair[0] == "--header").map(|pair| pair[1].clone()).collect();
    let remote = bridge::Remote::new(&url, token.as_deref(), &headers, unit.flag("--proxy"));
    let remote = match (remote, unit.env("SSL_CERT_FILE")) {
        (Ok(remote), Some(ca)) => remote.trusting(Path::new(ca)),
        (remote, _) => remote,
    };
    let probed = match &remote {
        Ok(remote) => remote.probe(NETWORK_TIMEOUT).await,
        Err(e) => Err(anyhow::anyhow!("{e:#}")),
    };
    let unreachable = if serving {
        restart.to_owned()
    } else {
        format!("check that the hub's `ants bridge serve` runs and is reached at {url}, through the proxy if any")
    };
    let answers = matches!(probed, Ok(Probe::Answers));
    checks.push(match probed {
        Ok(Probe::Answers) => Check::ok(url_check, format!("{url} answers as a bridge taking the token")),
        Ok(Probe::Refused(answer)) => {
            let fix = match unit.flag("--token-file") {
                Some(file) => {
                    format!("the same token in {file} as the hub's `ants bridge serve` takes, then {restart}")
                }
                None => "scripts/install-units.sh with --bridge-token FILE holding the hub's bridge token".to_owned(),
            };
            Check::fail(url_check, format!("{url} refuses the token: {answer}"), vec![fix])
        }
        Ok(Probe::Other(status, answer)) => Check::fail(
            url_check,
            format!("{url} answered {status} {answer:?}, not as a bridge does"),
            vec![unreachable],
        ),
        Err(e) => Check::fail(url_check, format!("{url}: {e:#}"), vec![unreachable]),
    });
    checks.push(match remote {
        Ok(remote) if answers => {
            let end = if serving {
                End::Serve { upstream: unit.flag("--upstream").unwrap_or(bridge::DEFAULT_UPSTREAM) }
            } else {
                End::Connect { listen: unit.flag("--listen").unwrap_or(bridge::DEFAULT_CONNECT_LISTEN) }
            };
            check_pipe(pipe_check, &remote, &url, end, restart).await
        }
        _ => Check::skip(pipe_check, "the bridge does not answer as one taking the token"),
    });
    checks
}

/// Which end of the bridge [`check_pipe`] checks, with what it connects.
pub(super) enum End<'a> {
    /// `ants bridge connect`, listening on `listen` for the leaf node's uplink.
    Connect { listen: &'a str },
    /// `ants bridge serve`, carrying each session to `upstream`.
    Serve { upstream: &'a str },
}

/// Whether the bridge of `remote` at `url` carries a connection to the hub's leafnode port, as a leaf node's uplink
/// takes it: the INFO that port sends first comes down a session of the doctor's own and, at `ants bridge connect`,
/// down a connection to its listen address, as the leaf node's own uplink does. Neither sends anything up, so the hub
/// sees each connection come and go unauthenticated, and the leaf node's uplink is left alone: `ants bridge connect`
/// carries each local connection in a session of its own.
pub(super) async fn check_pipe(check: &str, remote: &bridge::Remote, url: &str, end: End<'_>, restart: &str) -> Check {
    let carried = match remote.first_line(NETWORK_TIMEOUT).await {
        Ok(carried) => carried,
        Err(e) => return Check::fail(check, format!("{url}: {e:#}"), vec![restart.to_owned()]),
    };
    let (unopened, elsewhere) = match end {
        End::Serve { upstream } => (
            format!(
                "start the hub's nats-server, whose leafnodes listen on {upstream}, or give `ants bridge serve` the \
                 --upstream of its leafnode port, then {restart}"
            ),
            format!("give `ants bridge serve` the --upstream of the hub's leafnode port, then {restart}"),
        ),
        End::Connect { .. } => (
            "on the hub, start nats-server, whose leafnodes listen where `ants bridge serve --upstream` points \
             (127.0.0.1:7422 by default); `ants doctor` there checks the bridge serve pipe"
                .to_owned(),
            "on the hub, give `ants bridge serve` the --upstream of its nats-server's leafnode port".to_owned(),
        ),
    };
    let server = match carried {
        Carried::Unopened(status, said) => {
            let detail = format!("{url} answers but opens no connection to the hub's leafnode port: {status} {said}");
            return Check::fail(check, detail, vec![unopened]);
        }
        Carried::Lost(status, said) => {
            let detail = format!("{url} loses the session it opened at the next request: {status} {said}");
            return Check::fail(
                check,
                detail,
                vec![format!("route every request under {url} to one `ants bridge serve`")],
            );
        }
        Carried::Line(line) => match leafnode_info(&line) {
            Ok(server) => server,
            Err(what) => {
                let detail = format!("{url} answers but carries {what}, not the INFO of the hub's leafnode port");
                return Check::fail(check, detail, vec![elsewhere]);
            }
        },
    };
    let listen = match end {
        End::Serve { upstream } => {
            return Check::ok(check, format!("{url} carries a session to the leafnode port of {server} at {upstream}"));
        }
        End::Connect { listen } => listen,
    };
    let detail = match first_line_at(listen, NETWORK_TIMEOUT).await.map(|line| leafnode_info(&line)) {
        Ok(Ok(_)) => {
            let detail =
                format!("{listen} carries the leaf node's uplink through {url} to the leafnode port of {server}");
            return Check::ok(check, detail);
        }
        Ok(Err(what)) => format!(
            "{listen} carries {what}, though {url} carries the hub's INFO: the running `ants bridge connect` took \
             another token, URL or proxy at its start"
        ),
        Err(e) => format!("nothing carries the leaf node's uplink at {listen}: {e}"),
    };
    Check::fail(check, detail, vec![restart.to_owned()])
}

/// The server whose leafnode port sent `line`, its first line, as a leaf node takes one: an INFO with a client id,
/// and leafnode URLs or `info_on_connect`. Else what `line` is.
pub(super) fn leafnode_info(line: &str) -> Result<String, String> {
    if line.trim().is_empty() {
        return Err("nothing".to_owned());
    }
    let info = line.strip_prefix("INFO ").and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok());
    let Some(info) = info else { return Err(format!("{:?}", crate::text::one_line(line, 80))) };
    let named = |key: &str| info.get(key).and_then(serde_json::Value::as_str).unwrap_or("?").to_owned();
    let server = format!("nats-server {} ({})", named("version"), named("server_name"));
    let client = info.get("client_id").and_then(serde_json::Value::as_u64).is_some_and(|id| id != 0);
    let leafnodes = info.get("leafnode_urls").is_some_and(serde_json::Value::is_array)
        || info.get("info_on_connect").and_then(serde_json::Value::as_bool) == Some(true);
    if client && leafnodes {
        Ok(server)
    } else {
        Err(format!("the INFO of {server} from another port than its leafnode port"))
    }
}

/// The first line, without its line end, that a connection to `addr` carries within `wait`, sending nothing: as much
/// of it as came when the connection ended or the wait passed first.
async fn first_line_at(addr: &str, wait: Duration) -> std::io::Result<String> {
    use tokio::io::AsyncReadExt as _;
    let connected = tokio::time::timeout(wait, tokio::net::TcpStream::connect(addr)).await;
    let mut stream =
        connected.map_err(|_| std::io::Error::other(format!("no connection within {}s", wait.as_secs())))??;
    let (mut line, mut buf) = (Vec::new(), [0; 4096]);
    let read = async {
        while !line.contains(&b'\n') && line.len() < 64 * 1024 {
            match stream.read(&mut buf).await? {
                0 => break,
                n => line.extend_from_slice(&buf[..n]),
            }
        }
        std::io::Result::Ok(())
    };
    if let Ok(read) = tokio::time::timeout(wait, read).await {
        read?;
    }
    let end = line.iter().position(|b| *b == b'\n').unwrap_or(line.len());
    Ok(String::from_utf8_lossy(&line[..end]).trim_end().to_owned())
}

/// The URL `ants bridge connect` runs with, among its arguments `args`.
pub(super) fn bridge_url(args: &[String]) -> Option<&str> {
    let start = args.iter().position(|a| a == "connect")? + 1;
    let mut rest = args[start..].iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--token-file" | "--token" | "--proxy" | "--header" | "--listen" => {
                rest.next();
            }
            a if a.starts_with('-') => {}
            url => return Some(url),
        }
    }
    None
}

/// Whether the NATS URL `url` names the host and port `listen`, a loopback address.
pub(super) fn same_address(url: &str, listen: &str) -> bool {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = rest.split(['/', '?', ',']).next().unwrap_or_default();
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) if !host.ends_with(']') || authority.starts_with('[') => (host, port),
        _ => (authority, "4222"),
    };
    let loopback = |h: &str| matches!(h, "localhost" | "127.0.0.1" | "[::1]" | "::1");
    let Some((listen_host, listen_port)) = listen.rsplit_once(':') else { return false };
    port == listen_port && (host == listen_host || loopback(host) && loopback(listen_host))
}
