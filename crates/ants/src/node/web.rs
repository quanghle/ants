//! The WebSocket listener pages connect to: on a single machine's nats-server and on a leaf node's, an opt-in
//! loopback one taking pages of one origin only, the one `ants web` serves the page on ([`block`], [`add`]), and what
//! a nats-server configuration says of its listener ([`Listener::read`]), for `ants doctor`.
//!
//! nats-server checks the `Origin` header a browser sends with every WebSocket connection against the listener's
//! `allowed_origins`, and takes a connection without one, the CLI's or a leaf node's uplink, whatever they name. An
//! origin is a scheme, a host and a port, compared as written: `http://127.0.0.1:4280` and `http://localhost:4280`
//! are two origins.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;

use anyhow::{Context as _, Result, bail};

use super::conf::{entries, quote, uncommented, unquote};

/// The port `ants web` serves the page on by default: fixed, so that the page's origin, and what the browser keeps
/// for it, stay.
pub const PAGE_PORT: u16 = 4280;

/// The port of the loopback WebSocket listener `ants setup --web` and `ants leaf config --web` add.
pub const LISTENER_PORT: u16 = 4281;

/// The address `ants web` serves the page on by default.
pub const fn page_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), PAGE_PORT)
}

/// The origin of a page served over plain HTTP on `listen`, as a browser sends it for the page opened by that
/// address.
pub fn origin_of(listen: SocketAddr) -> String {
    format!("http://{listen}")
}

/// The origin `ants web` serves the page on by default, the only one the loopback listener takes.
pub fn origin() -> String {
    origin_of(page_listen())
}

/// The address of the loopback WebSocket listener of a server whose clients connect to `client_listen`: the same
/// loopback host, on [`LISTENER_PORT`].
pub fn listen(client_listen: &str) -> Result<SocketAddr> {
    let client: SocketAddr = client_listen
        .parse()
        .with_context(|| format!("{client_listen}: expected an address such as 127.0.0.1:4222"))?;
    if !client.ip().is_loopback() {
        bail!("{client}: pages are taken on a loopback address only");
    }
    Ok(SocketAddr::new(client.ip(), LISTENER_PORT))
}

/// The `websocket` block of a listener on `listen` taking pages of `origin` only.
pub fn block(listen: SocketAddr, origin: &str) -> String {
    format!(
        "# Written for `ants web`: the page connects here, and only pages of the origin named are taken. A client\n\
         # sending no Origin header, as the CLI does, connects whatever is named.\n\
         websocket {{\n  listen: {}\n  no_tls: true\n  allowed_origins: [{}]\n  compression: true\n}}\n",
        quote(&listen.to_string()),
        quote(origin)
    )
}

/// What [`add`] found or did.
#[derive(Debug, PartialEq, Eq)]
pub enum Added {
    /// The configuration with the listener added.
    New(String),
    /// The configuration has a listener taking the origin already.
    There,
    /// The configuration has a listener taking these origins and not the one asked for; none when it names no origin.
    Other(Vec<String>),
}

/// The nats-server configuration `text` with the listener of [`block`] at its end, unless it has a WebSocket listener
/// already.
pub fn add(text: &str, listen: SocketAddr, origin: &str) -> Added {
    match Listener::read(text) {
        Some(listener) if listener.takes(origin) => Added::There,
        Some(listener) => Added::Other(listener.origins),
        None => {
            let gap = if text.is_empty() || text.ends_with('\n') { "" } else { "\n" };
            Added::New(format!("{text}{gap}{}", block(listen, origin)))
        }
    }
}

/// The WebSocket listener of a nats-server configuration.
#[derive(Debug, PartialEq, Eq)]
pub struct Listener {
    /// Its address, as written; empty when the block gives a `port` or none.
    pub listen: String,
    /// The origins of `allowed_origins`.
    pub origins: Vec<String>,
}

impl Listener {
    /// The listener of the configuration `text`, None without a `websocket` block.
    pub fn read(text: &str) -> Option<Self> {
        let text = uncommented(text);
        let (_, body) = entries(&text).into_iter().find(|(key, _)| ["websocket", "ws"].contains(key))?;
        let mut listener = Self { listen: String::new(), origins: Vec::new() };
        for (key, value) in entries(body) {
            match key {
                "listen" => listener.listen = unquote(value).to_owned(),
                "allowed_origins" | "allowed_origin" => {
                    let origins = value.split([',', '\n', ' ', '\t']).map(unquote).filter(|o| !o.is_empty());
                    listener.origins = origins.map(str::to_owned).collect();
                }
                _ => {}
            }
        }
        Some(listener)
    }

    /// Whether pages of `origin` connect.
    pub fn takes(&self, origin: &str) -> bool {
        self.origins.iter().any(|o| o.eq_ignore_ascii_case(origin))
    }
}

/// Whether the server of the nats-server configuration at `path` asks its clients for credentials: it, or a file it
/// includes, names an operator, accounts or an authorization.
pub fn authenticated(path: &Path) -> bool {
    fn names_auth(path: &Path, depth: u8) -> bool {
        let Ok(text) = std::fs::read_to_string(path) else { return false };
        let text = uncommented(&text);
        entries(&text).into_iter().any(|(key, value)| match key {
            "operator" | "operators" | "trusted_keys" | "accounts" | "authorization" => true,
            "include" if depth > 0 => {
                // nats-server joins an include to the including file's directory, even an absolute one.
                let included = unquote(value).trim_start_matches(['/', '\\']);
                path.parent().is_some_and(|dir| names_auth(&dir.join(included), depth - 1))
            }
            _ => false,
        })
    }
    names_auth(path, 4)
}

#[cfg(test)]
mod tests {
    //! Tests of the WebSocket listener for pages: the block written and read back, a second addition changing
    //! nothing, the servers that ask for credentials, and a real nats-server taking the named origin only.

    use std::io::{Read as _, Write as _};
    use std::net::TcpStream;
    use std::time::Duration;

    use ants_test_support::{NatsServer, free_port, scratch_dir};

    use super::*;

    #[test]
    fn the_listener_is_on_the_clients_loopback_host_and_takes_the_page_of_ants_web() {
        assert_eq!(origin(), "http://127.0.0.1:4280");
        assert_eq!(origin_of("[::1]:8000".parse().unwrap()), "http://[::1]:8000");
        assert_eq!(listen("127.0.0.1:4222").unwrap().to_string(), "127.0.0.1:4281");
        assert_eq!(listen("[::1]:5000").unwrap().to_string(), "[::1]:4281");
        assert!(listen("0.0.0.0:4222").unwrap_err().to_string().contains("loopback"));
        assert!(listen("localhost").is_err());
    }

    #[test]
    fn a_second_addition_changes_nothing_and_another_listener_is_kept() {
        let at = listen("127.0.0.1:4222").unwrap();
        let Added::New(text) = add("listen: \"127.0.0.1:4222\"", at, &origin()) else { panic!("not added") };
        assert!(text.starts_with("listen: \"127.0.0.1:4222\"\n# Written for `ants web`"), "{text}");
        assert!(text.ends_with(
            "websocket {\n  listen: \"127.0.0.1:4281\"\n  no_tls: true\n  allowed_origins: \
             [\"http://127.0.0.1:4280\"]\n  compression: true\n}\n"
        ));
        assert_eq!(add(&text, at, &origin()), Added::There);
        assert_eq!(
            Listener::read(&text).unwrap(),
            Listener { listen: "127.0.0.1:4281".to_owned(), origins: vec![origin()] }
        );

        let open = "websocket { port: 8080, no_tls: true } # allowed_origins: [\"http://127.0.0.1:4280\"]\n";
        assert_eq!(add(open, at, &origin()), Added::Other(Vec::new()));
        let other = "ws {\n  listen: 127.0.0.1:8080\n  // allowed_origins: http://127.0.0.1:4280\n  \
                     allowed_origins = [\n    \"https://hub.example\",\n    'https://chat.example'\n  ]\n}\n";
        let named = vec!["https://hub.example".to_owned(), "https://chat.example".to_owned()];
        assert_eq!(add(other, at, &origin()), Added::Other(named.clone()));
        assert_eq!(Listener::read(other).unwrap(), Listener { listen: "127.0.0.1:8080".to_owned(), origins: named });
        let single = "jetstream { store_dir: \"/js\" }\nwebsocket { allowed_origins: \"HTTP://127.0.0.1:4280\" }\n";
        assert_eq!(add(single, at, &origin()), Added::There);
        assert_eq!(Listener::read("leafnodes {\n  websocket: 1\n}\nlisten: 1\n"), None, "not at the top level");
    }

    #[test]
    fn a_server_naming_an_operator_accounts_or_an_authorization_asks_for_credentials() {
        let dir = scratch_dir("web-authenticated");
        let conf = dir.join("nats.conf");
        let asks = |text: &str| {
            std::fs::write(&conf, text).unwrap();
            authenticated(&conf)
        };
        assert!(!asks("listen: 127.0.0.1:4222\njetstream {\n  store_dir: \"/js\"\n}\n"));
        assert!(!asks("# operator: x\nwebsocket {\n  authorization { token: x }\n}\n"), "the listener's own");
        assert!(asks("authorization {\n  token: x\n}\n"));
        assert!(asks("accounts: { A: { users: [] } }\n"));
        assert!(asks("operator: \"eyJ0\"\n"));
        std::fs::write(dir.join("server.conf"), "operator: \"eyJ0\"\nresolver: MEMORY\n").unwrap();
        assert!(asks("listen: 127.0.0.1:4222\ninclude \"server.conf\"\n"));
        assert!(asks("include '/server.conf'\n"), "joined to the configuration's directory");
        assert!(!asks("include \"missing.conf\"\n"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The first line of the server's answer to a WebSocket upgrade request sent to `addr` with the `origin` header.
    fn upgrade(addr: &str, origin: Option<&str>) -> String {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let origin = origin.map_or_default(|origin| format!("Origin: {origin}\r\n"));
        let request = format!(
            "GET / HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n{origin}\r\n"
        );
        stream.write_all(request.as_bytes()).unwrap();
        let mut answer = Vec::new();
        let mut byte = [0_u8; 1];
        while !answer.ends_with(b"\r\n") && stream.read(&mut byte).unwrap() == 1 {
            answer.push(byte[0]);
        }
        String::from_utf8_lossy(&answer).trim().to_owned()
    }

    #[test]
    fn a_server_takes_pages_of_the_named_origin_only_and_clients_sending_none() {
        let at = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), free_port());
        let conf = |dir: &Path| {
            let base = format!("jetstream {{ store_dir: {:?} }}\n", dir.join("js"));
            let Added::New(text) = add(&base, at, &origin()) else { panic!("not added") };
            text
        };
        let Some(_server) = NatsServer::spawn_with(conf) else { return };
        let addr = at.to_string();
        const TAKEN: &str = "HTTP/1.1 101 Switching Protocols";
        const REFUSED: &str = "HTTP/1.1 403 Forbidden";
        assert_eq!(upgrade(&addr, Some(&origin())), TAKEN, "the page of `ants web`");
        assert_eq!(upgrade(&addr, Some("http://localhost:4280")), REFUSED, "another name of the same address");
        assert_eq!(upgrade(&addr, Some("http://127.0.0.1:4281")), REFUSED, "another port");
        assert_eq!(upgrade(&addr, Some("https://evil.example")), REFUSED, "another site's page");
        assert_eq!(upgrade(&addr, None), TAKEN, "the CLI, or a leaf node's uplink");
    }
}
