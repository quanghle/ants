//! The Dev Tunnel and HTTPS bridge services installed: the tunnel's login and token, and whether the bridge forwards
//! to the server.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::services::private_file;
use super::{Check, NETWORK_TIMEOUT, Status};
use crate::node::bridge::{self, Probe};
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
/// and whether each bridge answers and takes its token. Nothing without such services.
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

/// The bridge of `service`, `ants bridge connect` or `ants bridge serve` run as `unit`: its token file, and whether
/// the bridge answers as one taking it.
pub(super) async fn check_bridge(service: Service, unit: &Unit, restart: &str) -> Vec<Check> {
    let serving = service == Service::BridgeServe;
    let (token_check, url_check) =
        if serving { ("bridge serve token", "bridge serve url") } else { ("bridge token", "bridge url") };
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
    let remote = bridge::Remote::new(&url, token.as_deref(), &[], unit.flag("--proxy"));
    let remote = match (remote, unit.env("SSL_CERT_FILE")) {
        (Ok(remote), Some(ca)) => remote.trusting(Path::new(ca)),
        (remote, _) => remote,
    };
    let probed = match remote {
        Ok(remote) => remote.probe(NETWORK_TIMEOUT).await,
        Err(e) => Err(e),
    };
    let unreachable = if serving {
        restart.to_owned()
    } else {
        format!("check that the hub's `ants bridge serve` runs and is reached at {url}, through the proxy if any")
    };
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
    checks
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
