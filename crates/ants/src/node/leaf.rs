//! `ants leaf config`: the nats-server configuration of a machine's leaf node.
//!
//! Every ants process on a machine connects to its leaf node on localhost; the leaf alone reaches the hub, through one
//! uplink: the hub's leafnode port over TCP (`nats-leaf://`, `tls://`, also through `devtunnel connect`) or its
//! websocket listener (`ws://`, `wss://`, also behind a reverse proxy). Switching the uplink rewrites this file and
//! reloads the leaf, through its service when one runs the file ([`take`]); nothing else on the machine changes.
//!
//! Clients reach the hub's JetStream through the hub's domain. The leaf runs a JetStream of its own, in a domain named
//! after the machine, only to keep the machine's outbox (`ants leaf outbox`) while the uplink is down; the hub sources
//! it from there. It trusts the team's operator through the same `server.conf` as the hub, so it checks local clients
//! against their own credentials, and its uplink carries the credentials of `ants auth leaf`.
//!
//! JetStream lives in the team's store account, on the leaf as on the hub, so a second uplink, to the same hub URLs,
//! joins the leaf's store account to the hub's with the store account credentials `ants auth leaf` writes beside the
//! uplink's: it carries only the hub's sourcing of the outbox, in the machine's domain, so a client's request to the
//! hub's domain takes the first uplink alone.
//!
//! A third uplink joins the leaf's system account to the hub's with the system account
//! credentials `ants auth leaf` writes beside the uplink's: allowed only to hear updates of the team's account and
//! answer them, it brings the leaf every account `ants auth push` sends the hub, revocations included. The leaf keeps
//! accounts in a `full` resolver of its own, so the last one pushed outlives restarts with an older `server.conf`.
//! That resolver also asks the hub's, if the hub runs a `full` one, for its accounts every [`RESOLVER_SYNC`], so a leaf
//! offline during a push catches up once it reconnects.

use std::fmt::Write as _;
use std::fs;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};

use ants_proto::{JsDomain, Machine};
use anyhow::{Context as _, Result, bail};
use data_encoding::BASE64URL_NOPAD;
use serde_json::Value;

use crate::service::{Action, Outcome, Service, failure, manual};

/// Where every ants process connects by default.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:4222";
/// nats-server's own default for a remote without a port; written out so the file says where it goes.
const LEAFNODE_PORT: u16 = 7422;

/// How often the leaf's resolver asks the hub's for accounts newer than its own: at most that long after reconnecting,
/// a leaf applies the pushes it missed. The hub answers with nothing while their accounts match, so this costs little.
const RESOLVER_SYNC: &str = "10s";

/// A machine's leaf node.
pub struct Leaf<'a> {
    /// Machine name, the leaf's `server_name` and JetStream domain.
    pub machine: &'a Machine,
    /// The hub's JetStream domain, which the machine's must differ from.
    pub hub_domain: Option<&'a JsDomain>,
    /// Hub URLs of the one remote, all of one scheme.
    pub uplinks: &'a [String],
    /// Uplink credentials from `ants auth leaf`.
    pub creds: &'a Path,
    /// System account credentials from `ants auth leaf`, for a second uplink bringing the hub's account updates.
    pub system_creds: Option<&'a Path>,
    /// The team's `server.conf` from `ants auth init`.
    pub server_conf: &'a Path,
    /// CA of a hub certificate the platform does not trust (`tls://` and `wss://` only).
    pub ca_file: Option<&'a Path>,
    /// HTTP CONNECT forward proxy in front of the uplink.
    pub proxy: Option<&'a str>,
    /// Local client address: loopback only.
    pub listen: &'a str,
}

/// Write the leaf node configuration of `leaf` to `out`, replacing any earlier one.
pub fn write(leaf: &Leaf, out: &Path) -> Result<()> {
    let out = normalize(out)?;
    let dir = out.parent().context("no directory for the leaf configuration")?;
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let text = render(leaf, dir)?;
    fs::write(&out, text).with_context(|| format!("writing {}", out.display()))
}

/// The machine name and uplink credentials of the leaf node configuration at `path`, as [`write()`] writes it.
pub fn read_uplink(path: &Path) -> Result<(Machine, PathBuf)> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let machine =
        value(&text, path, "server_name")?.parse().with_context(|| format!("{}: server_name", path.display()))?;
    Ok((machine, PathBuf::from(value(&text, path, "credentials")?)))
}

/// The address the leaf node of the configuration at `path`, as [`write()`] writes it, listens on for local clients.
pub fn read_listen(path: &Path) -> Result<String> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    value(&text, path, "listen")
}

/// The quoted value of the first `key: "..."` line of `text`, the configuration at `path`.
fn value(text: &str, path: &Path, key: &str) -> Result<String> {
    let line = text
        .lines()
        .find_map(|line| line.trim().strip_prefix(key)?.trim_start().strip_prefix(':'))
        .with_context(|| format!("{}: no {key} (written by `ants leaf config`?)", path.display()))?;
    serde_json::from_str(line.trim()).with_context(|| format!("{}: unreadable {key}", path.display()))
}

/// How a running leaf node takes the configuration `new` in place of `old`, both as [`write()`] writes them. A reload
/// (SIGHUP) adds and drops remotes, a remote being its URLs, account and credentials, and changes their TLS settings;
/// nats-server refuses it for any other change, the proxy of a kept remote included, which needs a restart, as does a
/// first configuration.
pub fn switch(old: Option<&str>, new: &str) -> Action {
    /// The lines naming a remote, and its proxy.
    type Remote<'a> = (Vec<&'a str>, Option<&'a str>);
    /// The settings outside `leafnodes`, and the remotes, comments left out.
    fn parse(text: &str) -> (Vec<&str>, Vec<Remote<'_>>) {
        let mut lines = text.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#'));
        let head = lines.by_ref().take_while(|l| *l != "leafnodes {").collect();
        let mut remotes = Vec::new();
        for line in lines {
            match line {
                "{" => remotes.push((Vec::new(), None)),
                l if l.starts_with("proxy") => {
                    if let Some(remote) = remotes.last_mut() {
                        remote.1 = Some(l);
                    }
                }
                l if ["urls:", "account:", "credentials:"].iter().any(|key| l.starts_with(key)) => {
                    if let Some(remote) = remotes.last_mut() {
                        remote.0.push(l);
                    }
                }
                _ => {}
            }
        }
        (head, remotes)
    }
    let Some(old) = old else { return Action::Restart };
    let ((old_head, old_remotes), (new_head, new_remotes)) = (parse(old), parse(new));
    let proxy_changed = old_remotes
        .iter()
        .any(|(id, proxy)| new_remotes.iter().any(|(new_id, new_proxy)| id == new_id && proxy != new_proxy));
    if old_head == new_head && !proxy_changed { Action::Reload } else { Action::Restart }
}

/// Has the leaf node of `out` take the configuration `new` just written there in place of `old` ([`switch`]), with
/// `apply` doing it on its service ([`crate::service::Services::apply`]), and says what came of it or how to do it by
/// hand.
pub fn take(out: &Path, old: Option<&str>, new: &str, apply: impl FnOnce(Action) -> Outcome) -> String {
    let action = switch(old, new);
    let out = out.display();
    match (apply(action), action) {
        (Outcome::Done(command), Action::Reload) => {
            format!("leaf node reloaded to take the uplinks of {out} ({command})")
        }
        (Outcome::Done(command), _) if old.is_none() => format!("leaf node restarted to read {out} ({command})"),
        (Outcome::Done(command), _) => {
            format!("leaf node restarted, as a reload takes uplink changes only ({command})")
        }
        (Outcome::NotRunning, _) => format!("the leaf node service is not running; it reads {out} once started"),
        (outcome, _) if old.is_none() => {
            format!(
                "{}start: nats-server -c {out} (scripts/install-units.sh installs it as a service)",
                failure(outcome)
            )
        }
        (outcome, Action::Reload) => format!(
            "{}switch the uplink: send the running leaf node SIGHUP (`nats-server --signal reload`), or reload its \
             service ({})",
            failure(outcome),
            manual(Service::Nats, Action::Reload)
        ),
        (outcome, _) => format!(
            "{}restart the running leaf node, as a reload takes uplink changes only (stop it and run \
             `nats-server -c {out}`), or restart its service ({})",
            failure(outcome),
            manual(Service::Nats, Action::Restart)
        ),
    }
}

/// Have the leaf node of the configuration at `path`, as [`write()`] writes it, connect its uplink again, to send the
/// uplink credentials just renewed in their file. nats-server reads that file at each connect, but a reload keeps the
/// connection of an unchanged remote, and a restart would cut every local connection too. So the remote's credentials
/// path switches between two spellings of the same file, with and without `./` (`.\` on Windows) before its name: a remote being its
/// URLs, account and credentials path, the reload drops it and adds it again, which reconnects the uplink alone.
/// `apply` reloads the leaf node's service ([`crate::service::Services::apply`]);
/// returns what came of it, or how to do it by hand.
pub fn reconnect_uplink(path: &Path, apply: impl FnOnce(Action) -> Outcome) -> Result<String> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let creds = value(&text, path, "credentials")?;
    let mut respelled = false;
    let mut new = String::with_capacity(text.len() + 2);
    for line in text.split_inclusive('\n') {
        let key = line.trim_start();
        if !respelled && key.starts_with("credentials") {
            let indent = &line[..line.len() - key.len()];
            let end = if line.ends_with('\n') { "\n" } else { "" };
            let _ = write!(new, "{indent}credentials: {}{end}", quote(&respell(&creds)));
            respelled = true;
        } else {
            new.push_str(line);
        }
    }
    fs::write(path, new).with_context(|| format!("writing {}", path.display()))?;
    Ok(match apply(Action::Reload) {
        Outcome::Done(command) => {
            format!("the leaf node reloaded to connect its uplink again with the renewed credentials ({command})")
        }
        Outcome::NotRunning => {
            "the leaf node service is not running; it connects with the renewed uplink credentials once started"
                .to_owned()
        }
        outcome => format!(
            "{}send the running leaf node SIGHUP (`nats-server --signal reload`), or reload its service ({}), to \
             connect its uplink again with the renewed credentials ({} names them anew for that); local connections \
             stay up",
            failure(outcome),
            manual(Service::Nats, Action::Reload),
            path.display()
        ),
    })
}

/// [`reconnect_uplink`], saying instead how to restart the leaf node by hand when the configuration at `path` cannot
/// be respelled.
pub fn use_renewed_uplink(path: &Path, apply: impl FnOnce(Action) -> Outcome) -> String {
    reconnect_uplink(path, apply).unwrap_or_else(|e| {
        format!(
            "{e:#}; restart the leaf node to use the renewed uplink credentials ({})",
            manual(Service::Nats, Action::Restart)
        )
    })
}

/// The other spelling of the file `path`: with `./` before its name (`.\` after a Windows directory), or without.
fn respell(path: &str) -> String {
    respell_at(path, std::path::is_separator)
}

/// [`respell`], with the separators of paths of the platform whose separators `is_separator` tells.
fn respell_at(path: &str, is_separator: impl Fn(char) -> bool) -> String {
    let Some(at) = path.rfind(&is_separator) else { return format!("./{path}") };
    let (dir, separator, name) = (&path[..at], &path[at..=at], &path[at + 1..]);
    if dir == "." {
        return name.to_owned();
    }
    match dir.strip_suffix('.').filter(|dir| dir.ends_with(&is_separator)) {
        Some(dir) => format!("{dir}{name}"),
        None => format!("{dir}{separator}.{separator}{name}"),
    }
}

/// The configuration of `leaf`, for a file in `dir`.
fn render(leaf: &Leaf, dir: &Path) -> Result<String> {
    let dir = normalize(dir)?;
    if leaf.hub_domain.is_some_and(|hub| hub.as_str() == leaf.machine.as_str()) {
        bail!("machine {} would take the hub's JetStream domain; name it otherwise", leaf.machine);
    }
    let listen: SocketAddr = leaf
        .listen
        .parse()
        .with_context(|| format!("--listen {}: expected an address such as {DEFAULT_LISTEN}", leaf.listen))?;
    if !listen.ip().is_loopback() {
        bail!("--listen {listen}: the leaf node serves this machine only; use a loopback address");
    }
    let uplinks = leaf.uplinks.iter().map(|u| Uplink::parse(u)).collect::<Result<Vec<_>>>()?;
    let Some(first) = uplinks.first() else { bail!("no uplink given") };
    if let Some(other) = uplinks.iter().find(|u| u.scheme != first.scheme) {
        bail!("uplinks {} and {} differ in scheme; give one scheme for all", first.url(), other.url());
    }
    let secure = matches!(first.scheme, Scheme::Tls | Scheme::Wss);
    if leaf.ca_file.is_some() && !secure {
        bail!("--ca-file needs a tls:// or wss:// uplink");
    }
    if let Some(proxy) = leaf.proxy {
        check_proxy(proxy)?;
    }

    let accounts = Accounts::read(leaf, first.scheme)?;
    // nats-server joins an include to the including file's directory, even an absolute one.
    let include = relative(&dir, &accounts.server_conf)?;

    let remotes = remotes(leaf, &uplinks, secure, &accounts)?;
    Ok(format!(
        "# Written by `ants leaf config {machine}`: the leaf node every ants process on {machine} connects to, and its\n\
         # uplink to the hub. To switch the uplink, run `ants leaf config` again: it reloads the leaf (SIGHUP).\n\
         server_name: {name}\n\
         listen: {listen}\n\
         # Notices a dead uplink in about a minute, and keeps proxies with idle timeouts from dropping it.\n\
         ping_interval: \"20s\"\n\
         include {include}\n\
         # Keeps the team's account as last pushed (`ants auth push`, through the hub and the system account's uplink)\n\
         # across restarts and reloads, even with an older server.conf, and every {sync} asks a hub with a full\n\
         # resolver for a newer one, missed while the uplink was down.\n\
         resolver {{ type: full, dir: {jwt}, interval: {sync_quoted} }}\n\
         # Only for the outbox, which keeps what the hub could not take while the uplink was down; clients reach the\n\
         # hub's JetStream through its domain.\n\
         jetstream {{\n  domain: {name}\n  store_dir: {store}\n  max_memory_store: 64MB\n  max_file_store: 1GB\n}}\n\
         leafnodes {{\n  remotes: [\n{remotes}  ]\n}}\n",
        machine = leaf.machine,
        name = quote(leaf.machine.as_str()),
        listen = quote(&listen.to_string()),
        include = quote(&include.to_string_lossy()),
        store = quote(&dir.join("jetstream").to_string_lossy()),
        jwt = quote(&dir.join("jwt").to_string_lossy()),
        sync = RESOLVER_SYNC,
        sync_quoted = quote(RESOLVER_SYNC),
    ))
}

/// The credentials of a leaf's uplinks and the accounts they are for, checked against the team's server.conf.
struct Accounts {
    creds: PathBuf,
    account: String,
    /// The store account's credentials and the account, for the second uplink.
    store: (PathBuf, String),
    server_conf: PathBuf,
    /// The system account's credentials and the account, for a second uplink.
    system: Option<(PathBuf, String)>,
}

impl Accounts {
    /// The credentials `leaf` names for uplinks of `scheme`, each checked to be for an account the team's server.conf
    /// trusts, the system account's for the system account.
    fn read(leaf: &Leaf, scheme: Scheme) -> Result<Self> {
        let creds = existing(leaf.creds, "uplink credentials (`ants auth leaf` writes them)")?;
        let account = leaf_account(&creds, scheme)?;
        let server_conf = existing(leaf.server_conf, "server.conf (copy it from the team directory)")?;
        let trust = fs::read_to_string(&server_conf).with_context(|| format!("reading {}", server_conf.display()))?;
        if !trust.contains(&account) {
            bail!(
                "{} does not trust the account of {}; use the team's server.conf",
                server_conf.display(),
                creds.display()
            );
        }
        let store = existing(
            &crate::auth::leaf_store_creds(&creds),
            "store account credentials (`ants auth leaf` writes them)",
        )?;
        let store_account = leaf_account(&store, scheme)?;
        if store_account == account || !trust.contains(&store_account) {
            bail!("{} are not for the store account of {}", store.display(), server_conf.display());
        }
        let system = match leaf.system_creds {
            Some(path) => {
                let path = existing(path, "system account credentials (`ants auth leaf` writes them)")?;
                let account = leaf_account(&path, scheme)?;
                if !trust
                    .lines()
                    .any(|line| line.trim().strip_prefix("system_account:").map(str::trim) == Some(&account))
                {
                    bail!("{} are not for the system account of {}", path.display(), server_conf.display());
                }
                Some((path, account))
            }
            None => None,
        };
        Ok(Self { creds, account, store: (store, store_account), server_conf, system })
    }
}

/// The `remotes` of the leaf node: the uplink to `uplinks` with `accounts`' credentials, then the store account's and
/// the system account's, with TLS when `secure`.
fn remotes(leaf: &Leaf, uplinks: &[Uplink], secure: bool, accounts: &Accounts) -> Result<String> {
    let urls: Vec<String> = uplinks.iter().map(|u| quote(&u.url())).collect();
    let mut transport = String::new();
    if secure {
        // Explicit, so the uplink never falls back to plain text; nats-server also wants it with a proxy.
        match leaf.ca_file {
            Some(ca) => {
                let ca = existing(ca, "CA file")?;
                let _ = writeln!(transport, "      tls {{ ca_file: {} }}", quote(&ca.to_string_lossy()));
            }
            None => transport += "      tls {}\n",
        }
    }
    if let Some(proxy) = leaf.proxy {
        let _ = writeln!(transport, "      proxy {{ url: {} }}", quote(proxy));
    }
    let remote = |account: &str, creds: &Path| {
        format!(
            "    {{\n      urls: [{}]\n      account: {}\n      credentials: {}\n{transport}    }}\n",
            urls.join(", "),
            quote(account),
            quote(&creds.to_string_lossy()),
        )
    };
    // The uplink comes first: `read_uplink` takes the first credentials.
    let mut remotes = remote(&accounts.account, &accounts.creds);
    remotes += &remote(&accounts.store.1, &accounts.store.0);
    if let Some((creds, account)) = &accounts.system {
        remotes += &remote(account, creds);
    }
    Ok(remotes)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Scheme {
    Tcp,
    Tls,
    Ws,
    Wss,
}

/// One hub URL of the leaf's remote, with the port spelled out: nats-server would put 7422 on any URL without one,
/// websocket ones included.
#[derive(Debug, PartialEq)]
struct Uplink {
    scheme: Scheme,
    host: String,
    port: u16,
    path: String,
}

impl Uplink {
    fn parse(raw: &str) -> Result<Self> {
        let bad = |why: &str| anyhow::anyhow!("uplink {raw:?}: {why}");
        let (scheme, rest) = raw.split_once("://").ok_or_else(|| bad("expected scheme://host[:port]"))?;
        let scheme = match scheme.to_ascii_lowercase().as_str() {
            "nats" | "nats-leaf" => Scheme::Tcp,
            "tls" => Scheme::Tls,
            "ws" => Scheme::Ws,
            "wss" => Scheme::Wss,
            _ => return Err(bad("use nats-leaf://, tls://, ws:// or wss://")),
        };
        if rest.contains(['?', '#']) {
            return Err(bad("a leaf node sends no query string or fragment"));
        }
        let (authority, path) = rest.find('/').map_or((rest, ""), |i| rest.split_at(i));
        if authority.contains('@') {
            return Err(bad("credentials go in --uplink-creds, not in the URL"));
        }
        let (host, port) = match authority.strip_prefix('[') {
            Some(v6) => {
                let (host, after) = v6.split_once(']').ok_or_else(|| bad("unclosed '['"))?;
                let port = match after {
                    "" => None,
                    after => Some(after.strip_prefix(':').ok_or_else(|| bad("invalid port"))?),
                };
                (format!("[{host}]"), port)
            }
            None => match authority.split_once(':') {
                Some((host, port)) => (host.to_owned(), Some(port)),
                None => (authority.to_owned(), None),
            },
        };
        if host.is_empty() || host == "[]" {
            return Err(bad("no host"));
        }
        let port = match port {
            Some(port) => port.parse().ok().filter(|p| *p != 0).ok_or_else(|| bad("invalid port"))?,
            None => match scheme {
                Scheme::Tcp | Scheme::Tls => LEAFNODE_PORT,
                Scheme::Ws => 80,
                Scheme::Wss => 443,
            },
        };
        let path = match scheme {
            Scheme::Ws | Scheme::Wss => path.trim_end_matches('/').to_owned(),
            _ if path.is_empty() || path == "/" => String::new(),
            _ => return Err(bad("only ws:// and wss:// uplinks have a path")),
        };
        Ok(Self { scheme, host, port, path })
    }

    fn url(&self) -> String {
        let scheme = match self.scheme {
            Scheme::Tcp => "nats-leaf",
            Scheme::Tls => "tls",
            Scheme::Ws => "ws",
            Scheme::Wss => "wss",
        };
        format!("{scheme}://{}:{}{}", self.host, self.port, self.path)
    }
}

fn check_proxy(proxy: &str) -> Result<()> {
    let Some((scheme, rest)) = proxy.split_once("://") else { bail!("--proxy {proxy:?}: expected http://host:port") };
    if !matches!(scheme, "http" | "https") {
        bail!("--proxy {proxy:?}: an HTTP CONNECT proxy, http:// or https://");
    }
    if rest.split('/').next().is_some_and(|authority| authority.contains('@')) {
        bail!("--proxy {proxy:?}: proxy credentials are not supported");
    }
    Ok(())
}

/// The team account of leaf uplink credentials, after checking they are ones (from `ants auth leaf`) that `scheme`
/// can use.
fn leaf_account(creds: &Path, scheme: Scheme) -> Result<String> {
    let text = fs::read_to_string(creds).with_context(|| format!("reading {}", creds.display()))?;
    let jwt = text
        .lines()
        .skip_while(|l| !l.contains("BEGIN NATS USER JWT"))
        .nth(1)
        .with_context(|| format!("{}: no user JWT", creds.display()))?;
    let claims: Value = jwt
        .split('.')
        .nth(1)
        .and_then(|payload| BASE64URL_NOPAD.decode(payload.trim().as_bytes()).ok())
        .and_then(|json| serde_json::from_slice(&json).ok())
        .with_context(|| format!("{}: unreadable user JWT", creds.display()))?;
    let needed = if matches!(scheme, Scheme::Ws | Scheme::Wss) { "LEAFNODE_WS" } else { "LEAFNODE" };
    let types = claims["nats"]["allowed_connection_types"].as_array();
    if !types.is_some_and(|types| types.iter().any(|t| t == needed)) {
        bail!("{} are not leaf uplink credentials; issue them with `ants auth leaf`", creds.display());
    }
    claims["nats"]["issuer_account"]
        .as_str()
        .or_else(|| claims["iss"].as_str())
        .map(str::to_owned)
        .with_context(|| format!("{}: no issuer in the user JWT", creds.display()))
}

/// `path` made absolute and checked to exist; `what` names it in the error.
fn existing(path: &Path, what: &str) -> Result<PathBuf> {
    let path = normalize(path)?;
    if !path.is_file() {
        bail!("{}: no such file; expected the {what}", path.display());
    }
    Ok(path)
}

/// `path` made absolute, with `.` and `..` resolved by name, as nats-server resolves includes (no symlinks followed).
fn normalize(path: &Path) -> Result<PathBuf> {
    let mut out = PathBuf::new();
    for component in std::path::absolute(path)?.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            c => out.push(c),
        }
    }
    Ok(out)
}

/// `target` relative to the directory `dir`, both absolute and normalized.
fn relative(dir: &Path, target: &Path) -> Result<PathBuf> {
    let common = dir.components().zip(target.components()).take_while(|(a, b)| a == b).count();
    if common == 0 {
        bail!("{} and {} share no root; nats-server needs a relative include", dir.display(), target.display());
    }
    let mut rel: PathBuf = dir.components().skip(common).map(|_| Component::ParentDir).collect();
    rel.extend(target.components().skip(common));
    Ok(rel)
}

/// A double-quoted string for the nats-server configuration, which takes JSON escapes.
fn quote(s: &str) -> String {
    serde_json::to_string(s).expect("a string serializes")
}

#[cfg(test)]
mod tests;
