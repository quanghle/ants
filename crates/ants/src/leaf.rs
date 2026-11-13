//! `ants leaf config`: the nats-server configuration of a machine's leaf node.
//!
//! Every ants process on a machine connects to its leaf node on localhost; the leaf alone reaches the hub, through one
//! uplink: the hub's leafnode port over TCP (`nats-leaf://`, `tls://`, also through `devtunnel connect`) or its
//! websocket listener (`ws://`, `wss://`, also behind a reverse proxy). Switching the uplink rewrites this file and
//! reloads the leaf; nothing else on the machine changes.
//!
//! The leaf runs without JetStream: clients reach the hub's through its JetStream domain. It trusts the team's
//! operator through the same `server.conf` as the hub, so it checks local clients against their own credentials, and
//! its uplink carries the credentials of `ants auth leaf`.

use std::fs;
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use data_encoding::BASE64URL_NOPAD;
use serde_json::Value;

/// Where every ants process connects by default.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:4222";
/// nats-server's own default for a remote without a port; written out so the file says where it goes.
const LEAFNODE_PORT: u16 = 7422;

/// A machine's leaf node.
pub struct Leaf<'a> {
    /// Machine name, the leaf's `server_name`.
    pub machine: &'a str,
    /// Hub URLs of the one remote, all of one scheme.
    pub uplinks: &'a [String],
    /// Uplink credentials from `ants auth leaf`.
    pub creds: &'a Path,
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

/// The configuration of `leaf`, for a file in `dir`.
fn render(leaf: &Leaf, dir: &Path) -> Result<String> {
    let dir = normalize(dir)?;
    if leaf.machine.is_empty()
        || leaf.machine.starts_with('.')
        || !leaf.machine.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        bail!("invalid machine name {:?}: use letters, digits, '-', '_' and '.'", leaf.machine);
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
    // nats-server joins an include to the including file's directory, even an absolute one.
    let include = relative(&dir, &server_conf)?;

    let urls: Vec<String> = uplinks.iter().map(|u| quote(&u.url())).collect();
    let mut remote = format!(
        "      urls: [{}]\n      account: {}\n      credentials: {}\n",
        urls.join(", "),
        quote(&account),
        quote(&creds.to_string_lossy()),
    );
    if secure {
        // Explicit, so the uplink never falls back to plain text; nats-server also wants it with a proxy.
        match leaf.ca_file {
            Some(ca) => {
                let ca = existing(ca, "CA file")?;
                remote += &format!("      tls {{ ca_file: {} }}\n", quote(&ca.to_string_lossy()));
            }
            None => remote += "      tls {}\n",
        }
    }
    if let Some(proxy) = leaf.proxy {
        remote += &format!("      proxy {{ url: {} }}\n", quote(proxy));
    }
    Ok(format!(
        "# Written by `ants leaf config {machine}`: the leaf node every ants process on {machine} connects to, and its\n\
         # uplink to the hub. To switch the uplink, run `ants leaf config` again and reload the leaf (SIGHUP).\n\
         server_name: {name}\n\
         listen: {listen}\n\
         # Notices a dead uplink in about a minute, and keeps proxies with idle timeouts from dropping it.\n\
         ping_interval: \"20s\"\n\
         # Without JetStream: clients reach the hub's through its JetStream domain.\n\
         include {include}\n\
         leafnodes {{\n  remotes: [\n    {{\n{remote}    }}\n  ]\n}}\n",
        machine = leaf.machine,
        name = quote(leaf.machine),
        listen = quote(&listen.to_string()),
        include = quote(&include.to_string_lossy()),
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

    use ants_daemon::bus;
    use ants_proto::{JsDomain, PersonaId};
    use ants_test_support::{NatsServer, scratch_dir};
    use async_nats::jetstream;

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

    /// A team with leaf credentials for m1, in a scratch directory.
    fn team() -> (PathBuf, PathBuf) {
        let dir = scratch_dir("leaf-team");
        auth::init(&dir).unwrap();
        let creds = dir.join("leaf-m1.creds");
        auth::issue_leaf(&dir, "m1", &["dev-a".parse().unwrap()], None, &creds).unwrap();
        (dir, creds)
    }

    fn leaf<'a>(uplinks: &'a [String], creds: &'a Path, server_conf: &'a Path) -> Leaf<'a> {
        Leaf { machine: "m1", uplinks, creds, server_conf, ca_file: None, proxy: None, listen: DEFAULT_LISTEN }
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
        assert!(!text.contains("jetstream"), "{text}");

        assert!(err(Leaf { listen: "0.0.0.0:4222", ..leaf(&wss, &creds, &server_conf) }).contains("loopback"));
        let mixed = [wss[0].clone(), tcp[0].clone()];
        assert!(err(leaf(&mixed, &creds, &server_conf)).contains("differ in scheme"));
        let ca = dir.join("ca.pem");
        assert!(err(Leaf { ca_file: Some(&ca), ..leaf(&tcp, &creds, &server_conf) }).contains("--ca-file needs"));
        let proxy = Some("socks5://proxy:1080");
        assert!(err(Leaf { proxy, ..leaf(&tcp, &creds, &server_conf) }).contains("HTTP CONNECT"));

        let persona = dir.join("dev-a.creds");
        auth::issue(&dir, &"dev-a".parse().unwrap(), None, &persona).unwrap();
        assert!(err(leaf(&tcp, &persona, &server_conf)).contains("not leaf uplink credentials"));
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

    async fn reaches_hub_stream(js: &jetstream::Context, expected: bool) -> bool {
        for _ in 0..50 {
            if bus::ensure_stream(js).await.is_ok() == expected {
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
        let (human, dev_a) = (PersonaId::human(), "dev-a".parse::<PersonaId>().unwrap());
        let creds = |p: &PersonaId| {
            let out = hub.dir().join(format!("{p}.creds"));
            auth::issue(&team, p, Some(&domain), &out).unwrap();
            out
        };
        let (_h, h_js) = bus::connect(&hub.url, Some(&creds(&human)), Some(&domain), &human, "test").await.unwrap();
        bus::init_stream(&h_js).await.unwrap();
        let leaf_creds = hub.dir().join("leaf-m1.creds");
        auth::issue_leaf(&team, "m1", std::slice::from_ref(&dev_a), Some(&domain), &leaf_creds).unwrap();

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
        assert!(reaches_hub_stream(&a_js, true).await, "no hub stream over nats-leaf://");

        let reload = |uplink: String| {
            configure(leaf_node.dir(), uplink);
            let status = Command::new("kill").arg("-HUP").arg(leaf_node.pid().to_string()).status().unwrap();
            assert!(status.success());
        };
        reload(format!("nats-leaf://127.0.0.1:{dead_port}"));
        assert!(reaches_hub_stream(&a_js, false).await, "the old uplink outlived the reload");
        reload(format!("ws://127.0.0.1:{ws_port}/nats"));
        assert!(reaches_hub_stream(&a_js, true).await, "no hub stream over ws://");
        let stream = bus::ensure_stream(&a_js).await.unwrap();
        bus::inbox_consumer(&stream, &dev_a, Duration::from_secs(5), 5, 2).await.unwrap();
    }
}
