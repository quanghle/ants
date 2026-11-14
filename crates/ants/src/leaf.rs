//! `ants leaf config`: the nats-server configuration of a machine's leaf node.
//!
//! Every ants process on a machine connects to its leaf node on localhost; the leaf alone reaches the hub, through one
//! uplink: the hub's leafnode port over TCP (`nats-leaf://`, `tls://`, also through `devtunnel connect`) or its
//! websocket listener (`ws://`, `wss://`, also behind a reverse proxy). Switching the uplink rewrites this file and
//! reloads the leaf; nothing else on the machine changes.
//!
//! Clients reach the hub's JetStream through the hub's domain. The leaf runs a JetStream of its own, in a domain named
//! after the machine, only to keep the machine's outbox (`ants leaf outbox`) while the uplink is down; the hub sources
//! it from there. It trusts the team's operator through the same `server.conf` as the hub, so it checks local clients
//! against their own credentials, and its uplink carries the credentials of `ants auth leaf`.
//!
//! A second uplink, to the same hub URLs, joins the leaf's system account to the hub's with the system account
//! credentials `ants auth leaf` writes beside the uplink's: allowed only to hear updates of the team's account and
//! answer them, it brings the leaf every account `ants auth push` sends the hub, revocations included. The leaf keeps
//! accounts in a `full` resolver of its own, so the last one pushed outlives restarts with an older `server.conf`.

use std::fs;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};

use ants_proto::{JsDomain, Machine};
use anyhow::{Context as _, Result, bail};
use data_encoding::BASE64URL_NOPAD;
use serde_json::Value;

/// Where every ants process connects by default.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:4222";
/// nats-server's own default for a remote without a port; written out so the file says where it goes.
const LEAFNODE_PORT: u16 = 7422;

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

/// The machine name and uplink credentials of the leaf node configuration at `path`, as [`write`] writes it.
pub fn read_uplink(path: &Path) -> Result<(Machine, PathBuf)> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let value = |key: &str| -> Result<String> {
        let line = text
            .lines()
            .find_map(|line| line.trim().strip_prefix(key)?.trim_start().strip_prefix(':'))
            .with_context(|| format!("{}: no {key} (written by `ants leaf config`?)", path.display()))?;
        serde_json::from_str(line.trim()).with_context(|| format!("{}: unreadable {key}", path.display()))
    };
    let machine = value("server_name")?.parse().with_context(|| format!("{}: server_name", path.display()))?;
    Ok((machine, PathBuf::from(value("credentials")?)))
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

    let creds = existing(leaf.creds, "uplink credentials (`ants auth leaf` writes them)")?;
    let account = leaf_account(&creds, first.scheme)?;
    let server_conf = existing(leaf.server_conf, "server.conf (copy it from the team directory)")?;
    let trust = fs::read_to_string(&server_conf).with_context(|| format!("reading {}", server_conf.display()))?;
    if !trust.contains(&account) {
        bail!(
            "{} does not trust the account of {}; use the team's server.conf",
            server_conf.display(),
            creds.display()
        );
    }
    let system = match leaf.system_creds {
        Some(path) => {
            let path = existing(path, "system account credentials (`ants auth leaf` writes them)")?;
            let account = leaf_account(&path, first.scheme)?;
            if !trust.lines().any(|line| line.trim().strip_prefix("system_account:").map(str::trim) == Some(&account)) {
                bail!("{} are not for the system account of {}", path.display(), server_conf.display());
            }
            Some((path, account))
        }
        None => None,
    };
    // nats-server joins an include to the including file's directory, even an absolute one.
    let include = relative(&dir, &server_conf)?;

    let urls: Vec<String> = uplinks.iter().map(|u| quote(&u.url())).collect();
    let mut transport = String::new();
    if secure {
        // Explicit, so the uplink never falls back to plain text; nats-server also wants it with a proxy.
        match leaf.ca_file {
            Some(ca) => {
                let ca = existing(ca, "CA file")?;
                transport += &format!("      tls {{ ca_file: {} }}\n", quote(&ca.to_string_lossy()));
            }
            None => transport += "      tls {}\n",
        }
    }
    if let Some(proxy) = leaf.proxy {
        transport += &format!("      proxy {{ url: {} }}\n", quote(proxy));
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
    let mut remotes = remote(&account, &creds);
    if let Some((creds, account)) = &system {
        remotes += &remote(account, creds);
    }
    Ok(format!(
        "# Written by `ants leaf config {machine}`: the leaf node every ants process on {machine} connects to, and its\n\
         # uplink to the hub. To switch the uplink, run `ants leaf config` again and reload the leaf (SIGHUP).\n\
         server_name: {name}\n\
         listen: {listen}\n\
         # Notices a dead uplink in about a minute, and keeps proxies with idle timeouts from dropping it.\n\
         ping_interval: \"20s\"\n\
         include {include}\n\
         # Keeps the team's account as last pushed (`ants auth push`, through the hub and the system account's uplink)\n\
         # across restarts and reloads, even with an older server.conf.\n\
         resolver {{ type: full, dir: {jwt} }}\n\
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
    ))
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
mod tests {
    use std::process::Command;
    use std::time::Duration;

    use ants_daemon::{bus, notes};
    use ants_proto::{ChannelPost, Envelope, NotesPost, Role, UserId};
    use ants_test_support::{NatsServer, scratch_dir};
    use async_nats::jetstream;
    use tokio::time::timeout;

    use super::*;
    use crate::auth;

    fn uplink(raw: &str) -> Result<String> {
        Uplink::parse(raw).map(|u| u.url())
    }

    #[test]
    fn uplinks_get_a_scheme_and_port_nats_server_reads_right() {
        assert_eq!(uplink("nats://127.0.0.1").unwrap(), "nats-leaf://127.0.0.1:7422");
        assert_eq!(uplink("nats-leaf://hub:7000/").unwrap(), "nats-leaf://hub:7000");
        assert_eq!(uplink("tls://hub.example.com").unwrap(), "tls://hub.example.com:7422");
        assert_eq!(uplink("wss://hub.example.com/nats/").unwrap(), "wss://hub.example.com:443/nats");
        assert_eq!(uplink("ws://[::1]:8080/nats").unwrap(), "ws://[::1]:8080/nats");
        assert_eq!(uplink("WS://hub").unwrap(), "ws://hub:80");
        for bad in [
            "hub:7422",
            "http://hub",
            "nats://hub:7422/path",
            "wss://user:pw@hub/nats",
            "wss://hub/nats?token=x",
            "nats://hub:0",
            "nats://hub:port",
            "wss:///nats",
        ] {
            assert!(uplink(bad).is_err(), "{bad} accepted");
        }
    }

    static M1: std::sync::LazyLock<Machine> = std::sync::LazyLock::new(|| "m1".parse().unwrap());

    /// A team with leaf credentials for m1, in a scratch directory.
    fn team() -> (PathBuf, PathBuf) {
        let dir = scratch_dir("leaf-team");
        auth::init(&dir).unwrap();
        let creds = dir.join("leaf-m1.creds");
        auth::issue_leaf(&dir, &M1, &[("dev-a".parse().unwrap(), Role::Ant)], &[], false, None, &creds).unwrap();
        (dir, creds)
    }

    fn leaf<'a>(uplinks: &'a [String], creds: &'a Path, server_conf: &'a Path) -> Leaf<'a> {
        Leaf {
            machine: &M1,
            hub_domain: None,
            uplinks,
            creds,
            system_creds: None,
            server_conf,
            ca_file: None,
            proxy: None,
            listen: DEFAULT_LISTEN,
        }
    }

    #[test]
    fn rejects_what_would_not_make_a_working_local_leaf() {
        let (dir, creds) = team();
        let server_conf = dir.join(auth::SERVER_CONF);
        let wss = ["wss://hub.example.com/nats".to_owned()];
        let tcp = ["nats-leaf://127.0.0.1:7422".to_owned()];
        let err = |leaf: Leaf| render(&leaf, &dir).unwrap_err().to_string();

        let text =
            render(&Leaf { proxy: Some("http://proxy:3128"), ..leaf(&wss, &creds, &server_conf) }, &dir).unwrap();
        assert!(text.contains("include \"server.conf\"\n"), "{text}");
        assert!(text.contains("listen: \"127.0.0.1:4222\"\n"), "{text}");
        assert!(text.contains("urls: [\"wss://hub.example.com:443/nats\"]"), "{text}");
        assert!(text.contains("tls {}\n") && text.contains("proxy { url: \"http://proxy:3128\" }"), "{text}");
        let store = normalize(&dir).unwrap().join("jetstream");
        let js = format!("jetstream {{\n  domain: \"m1\"\n  store_dir: {}\n", quote(&store.to_string_lossy()));
        assert!(text.contains(&js), "{text}");
        let conf = dir.join("leaf.conf");
        write(&leaf(&wss, &creds, &server_conf), &conf).unwrap();
        assert_eq!(read_uplink(&conf).unwrap(), (M1.clone(), normalize(&creds).unwrap()), "as `ants add` reads it");
        let hub: JsDomain = "m1".parse().unwrap();
        let taken = Leaf { hub_domain: Some(&hub), ..leaf(&wss, &creds, &server_conf) };
        assert!(err(taken).contains("hub's JetStream domain"));

        assert!(err(Leaf { listen: "0.0.0.0:4222", ..leaf(&wss, &creds, &server_conf) }).contains("loopback"));
        let mixed = [wss[0].clone(), tcp[0].clone()];
        assert!(err(leaf(&mixed, &creds, &server_conf)).contains("differ in scheme"));
        let ca = dir.join("ca.pem");
        assert!(err(Leaf { ca_file: Some(&ca), ..leaf(&tcp, &creds, &server_conf) }).contains("--ca-file needs"));
        let proxy = Some("socks5://proxy:1080");
        assert!(err(Leaf { proxy, ..leaf(&tcp, &creds, &server_conf) }).contains("HTTP CONNECT"));

        let system = auth::leaf_system_creds(&creds);
        let text = render(&Leaf { system_creds: Some(&system), ..leaf(&tcp, &creds, &server_conf) }, &dir).unwrap();
        assert_eq!(text.matches("urls: [\"nats-leaf://127.0.0.1:7422\"]").count(), 2, "{text}");
        let jwt = normalize(&dir).unwrap().join("jwt");
        assert!(text.contains(&format!("resolver {{ type: full, dir: {} }}", quote(&jwt.to_string_lossy()))), "{text}");
        let conf = dir.join("leaf.conf");
        fs::write(&conf, &text).unwrap();
        assert_eq!(read_uplink(&conf).unwrap(), (M1.clone(), normalize(&creds).unwrap()), "the team's uplink first");
        let wrong = |path: &Path| err(Leaf { system_creds: Some(path), ..leaf(&tcp, &creds, &server_conf) });
        assert!(wrong(&creds).contains("not for the system account"));

        let ant = dir.join("dev-a.creds");
        auth::issue(&dir, &"dev-a".parse().unwrap(), Role::Ant, None, &ant).unwrap();
        assert!(err(leaf(&tcp, &ant, &server_conf)).contains("not leaf uplink credentials"));
        assert!(wrong(&ant).contains("not leaf uplink credentials"));
        let other = scratch_dir("leaf-other-team");
        auth::init(&other).unwrap();
        assert!(err(leaf(&tcp, &creds, &other.join(auth::SERVER_CONF))).contains("does not trust"));
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&other);
    }

    #[test]
    fn includes_are_relative_to_the_configuration() {
        let rel = |dir: &str, target: &str| relative(Path::new(dir), Path::new(target)).unwrap();
        assert_eq!(
            rel("/home/me/.config/ants", "/home/me/.config/ants/auth/server.conf"),
            Path::new("auth/server.conf")
        );
        assert_eq!(rel("/home/me/leaf", "/home/me/auth/server.conf"), Path::new("../auth/server.conf"));
        assert_eq!(normalize(Path::new("/a/./b/../c")).unwrap(), Path::new("/a/c"));
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    async fn reaches_hub_stream(js: &jetstream::Context, user: &UserId, expected: bool) -> bool {
        for _ in 0..50 {
            if bus::open_stream(js, user).await.is_ok() == expected {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    /// A generated leaf node reaches a real hub over nats-leaf:// and ws://, and switching its uplink is a rewrite of
    /// its configuration and a reload, while the local client stays connected to it.
    #[tokio::test]
    async fn a_generated_leaf_reaches_the_hub_and_switches_its_uplink_on_reload() {
        let (leaf_port, ws_port, dead_port) = (free_port(), free_port(), free_port());
        let Some(hub) = NatsServer::spawn_with(|dir| {
            auth::init(&dir.join("auth")).unwrap();
            format!(
                "jetstream {{ store_dir: {:?}, domain: hub }}\ninclude \"auth/{}\"\n\
                 leafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n\
                 websocket {{ listen: \"127.0.0.1:{ws_port}\", no_tls: true }}\n",
                dir.join("js"),
                auth::SERVER_CONF
            )
        }) else {
            return;
        };
        let team = hub.dir().join("auth");
        let domain: JsDomain = "hub".parse().unwrap();
        let (human, dev_a) = (UserId::human(), "dev-a".parse::<UserId>().unwrap());
        let creds = |p: &UserId| {
            let out = hub.dir().join(format!("{p}.creds"));
            auth::issue(&team, p, Role::by_default(p), Some(&domain), &out).unwrap();
            out
        };
        let (_h, h_js) = bus::connect(&hub.url, Some(&creds(&human)), Some(&domain), &human, "test").await.unwrap();
        bus::init_stream(&h_js).await.unwrap();
        let leaf_creds = hub.dir().join("leaf-m1.creds");
        auth::issue_leaf(&team, &M1, &[(dev_a.clone(), Role::Ant)], &[], false, Some(&domain), &leaf_creds).unwrap();

        // The leaf's file includes the hub's server.conf from another directory, by a relative path.
        let server_conf = team.join(auth::SERVER_CONF);
        let listen = format!("127.0.0.1:{}", free_port());
        let configure = |dir: &Path, uplink: String| {
            let uplinks = [uplink];
            let leaf = Leaf { listen: &listen, ..leaf(&uplinks, &leaf_creds, &server_conf) };
            write(&leaf, &dir.join("leaf.conf")).unwrap();
        };
        let leaf_node = NatsServer::spawn_with(|dir| {
            configure(dir, format!("nats-leaf://127.0.0.1:{leaf_port}"));
            "include \"leaf.conf\"\n".to_owned()
        })
        .unwrap();
        assert_eq!(leaf_node.url, format!("nats://{listen}"));
        let (_a, mut a_js) =
            bus::connect(&leaf_node.url, Some(&creds(&dev_a)), Some(&domain), &dev_a, "test").await.unwrap();
        a_js.set_timeout(Duration::from_secs(1));
        assert!(reaches_hub_stream(&a_js, &dev_a, true).await, "no hub stream over nats-leaf://");
        // Only dev-a is on the leaf, so the uplink allows no inbox, notes or channel subject without a wildcard before
        // the sender. The hub's stream interest crosses all the same: the leaf has no outbox to fall back on.
        let to_human = Envelope::new(dev_a.clone(), human.clone(), "hi");
        assert_eq!(bus::publish(&a_js, &to_human).await.unwrap(), bus::Stored::Hub);
        let post = NotesPost::new("t1".parse().unwrap(), dev_a.clone(), "notes");
        assert!(notes::publish(&a_js, &post, Some(0)).await.unwrap(), "notes post not stored directly");
        let chan = ChannelPost::new("ops".parse().unwrap(), dev_a.clone(), "@human hi");
        let stored = bus::store(&a_js, chan.subject(), &chan, &chan.id.to_string()).await.unwrap();
        assert_eq!(stored, bus::Stored::Hub, "channel post not stored directly");
        // The interest widens nothing: dev-b, not on the uplink, still cannot reach the hub's stream from the leaf.
        let dev_b: UserId = "dev-b".parse().unwrap();
        let (_b, mut b_js) =
            bus::connect(&leaf_node.url, Some(&creds(&dev_b)), Some(&domain), &dev_b, "test").await.unwrap();
        b_js.set_timeout(Duration::from_secs(1));
        let stray = Envelope::new(dev_b.clone(), human.clone(), "stray");
        assert!(bus::publish(&b_js, &stray).await.is_err());
        let stray = NotesPost::new("t1".parse().unwrap(), dev_b.clone(), "stray");
        assert!(notes::publish(&b_js, &stray, Some(2)).await.is_err());
        let stray = ChannelPost::new("ops".parse().unwrap(), dev_b.clone(), "stray");
        assert!(bus::store(&b_js, stray.subject(), &stray, &stray.id.to_string()).await.is_err());
        let stored = h_js.get_stream(ants_proto::STREAM).await.unwrap().info().await.unwrap().state.messages;
        assert_eq!(stored, 3, "only dev-a's message and posts");

        let reload = |uplink: String| {
            configure(leaf_node.dir(), uplink);
            let status = Command::new("kill").arg("-HUP").arg(leaf_node.pid().to_string()).status().unwrap();
            assert!(status.success());
        };
        reload(format!("nats-leaf://127.0.0.1:{dead_port}"));
        assert!(reaches_hub_stream(&a_js, &dev_a, false).await, "the old uplink outlived the reload");
        reload(format!("ws://127.0.0.1:{ws_port}/nats"));
        assert!(reaches_hub_stream(&a_js, &dev_a, true).await, "no hub stream over ws://");
        let stream = bus::open_stream(&a_js, &dev_a).await.unwrap();
        bus::inbox_consumer(&stream, &dev_a, Duration::from_secs(5), 5, 2).await.unwrap();
    }

    /// A connection with `creds` to `url` that reports when the server closes it.
    async fn watched(
        url: &str,
        creds: &Path,
    ) -> Result<(async_nats::Client, tokio::sync::mpsc::UnboundedReceiver<()>)> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let client = async_nats::ConnectOptions::with_credentials_file(creds)
            .await?
            .event_callback(move |event| {
                let tx = tx.clone();
                async move {
                    if let async_nats::Event::Disconnected = event {
                        let _ = tx.send(());
                    }
                }
            })
            .connect(url)
            .await?;
        client.flush().await?;
        Ok((client, rx))
    }

    /// A revocation pushed to the hub reaches a leaf node through its system account uplink, with no server.conf
    /// copied: the leaf closes the revoked ant's connection and refuses it, also once restarted on its older
    /// server.conf.
    #[tokio::test]
    async fn a_revocation_pushed_to_the_hub_reaches_the_leaf_and_outlives_its_restart() {
        let leaf_port = free_port();
        let Some(hub) = NatsServer::spawn_with(|dir| {
            auth::init(&dir.join("auth")).unwrap();
            format!(
                "include \"auth/{}\"\nleafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n",
                auth::SERVER_CONF
            )
        }) else {
            return;
        };
        let team = hub.dir().join("auth");
        let (dev_a, dev_b) = ("dev-a".parse::<UserId>().unwrap(), "dev-b".parse::<UserId>().unwrap());
        let creds = |user: &UserId| {
            let out = hub.dir().join(format!("{user}.creds"));
            auth::issue(&team, user, Role::Ant, None, &out).unwrap();
            out
        };
        let (a, b) = (creds(&dev_a), creds(&dev_b));
        let leaf_creds = hub.dir().join("leaf-m1.creds");
        let members = [(dev_a.clone(), Role::Ant), (dev_b.clone(), Role::Ant)];
        auth::issue_leaf(&team, &M1, &members, &[], false, None, &leaf_creds).unwrap();

        // The leaf's files outlive its process, to restart it; it has its own copy of server.conf.
        let files = scratch_dir("leaf-files");
        let server_conf = files.join(auth::SERVER_CONF);
        fs::copy(team.join(auth::SERVER_CONF), &server_conf).unwrap();
        let (uplinks, listen) = ([format!("nats-leaf://127.0.0.1:{leaf_port}")], format!("127.0.0.1:{}", free_port()));
        let system_creds = auth::leaf_system_creds(&leaf_creds);
        let config =
            Leaf { system_creds: Some(&system_creds), listen: &listen, ..leaf(&uplinks, &leaf_creds, &server_conf) };
        write(&config, &files.join("leaf.conf")).unwrap();
        let start = || {
            NatsServer::spawn_with(|dir| {
                let include = relative(&normalize(dir).unwrap(), &normalize(&files.join("leaf.conf")).unwrap());
                format!("include {}\n", quote(&include.unwrap().to_string_lossy()))
            })
            .unwrap()
        };
        let leaf_node = start();
        let pushed_to_leaf = async || {
            for _ in 0..50 {
                let pushed = crate::revoke::push(&hub.url, &team).await.unwrap();
                if !pushed.others.is_empty() {
                    return pushed.others;
                }
            }
            panic!("the leaf never heard a push");
        };
        assert_eq!(pushed_to_leaf().await, [("m1".to_owned(), None)]);

        let (_a_client, mut a_closed) = watched(&leaf_node.url, &a).await.unwrap();
        let (b_client, mut b_closed) = watched(&leaf_node.url, &b).await.unwrap();
        crate::revoke::revoke(&team, Some("dev-a"), &[]).unwrap();
        let pushed = crate::revoke::push(&hub.url, &team).await.unwrap();
        assert_eq!(pushed.others, [("m1".to_owned(), None)]);
        timeout(Duration::from_secs(5), a_closed.recv()).await.expect("dev-a's connection to the leaf stays open");
        assert!(watched(&leaf_node.url, &a).await.is_err(), "the leaf still takes dev-a's credentials");
        b_client.flush().await.unwrap();
        assert!(b_closed.try_recv().is_err(), "dev-b's connection was closed");

        drop((b_client, leaf_node));
        let leaf_node = start();
        assert!(watched(&leaf_node.url, &b).await.is_ok(), "the restarted leaf refuses dev-b");
        assert!(watched(&leaf_node.url, &a).await.is_err(), "the restarted leaf forgot dev-a's revocation");
        let _ = fs::remove_dir_all(&files);
    }
}
