use std::process::Command;
use std::time::Duration;

use ants_daemon::{bus, notes};
use ants_proto::{ChannelPost, Envelope, NotesPost, Permissions, Role, UserId};
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

    let text = render(&Leaf { proxy: Some("http://proxy:3128"), ..leaf(&wss, &creds, &server_conf) }, &dir).unwrap();
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
    let resolver = format!("resolver {{ type: full, dir: {}, interval: \"10s\" }}", quote(&jwt.to_string_lossy()));
    assert!(text.contains(&resolver), "{text}");
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
fn a_switched_uplink_takes_a_reload_and_anything_else_a_restart() {
    let (dir, creds) = team();
    let server_conf = dir.join(auth::SERVER_CONF);
    let system = auth::leaf_system_creds(&creds);
    let (tcp, ws) = (["nats-leaf://127.0.0.1:7422".to_owned()], ["ws://127.0.0.1:8080/nats".to_owned()]);
    let render = |leaf: Leaf| render(&leaf, &dir).unwrap();
    let first = render(leaf(&tcp, &creds, &server_conf));
    let proxied = |uplinks| render(Leaf { proxy: Some("http://proxy:3128"), ..leaf(uplinks, &creds, &server_conf) });
    let switch = |new: &str| switch(Some(&first), new);

    assert_eq!(super::switch(None, &first), Action::Restart, "a first configuration");
    assert_eq!(switch(&first), Action::Reload);
    assert_eq!(switch(&first.replace("# uplink to the hub.", "# the hub.")), Action::Reload, "comments");
    assert_eq!(switch(&render(leaf(&ws, &creds, &server_conf))), Action::Reload);
    assert_eq!(switch(&proxied(&ws)), Action::Reload, "the proxy of a new remote");
    assert_eq!(
        switch(&render(Leaf { system_creds: Some(&system), ..leaf(&tcp, &creds, &server_conf) })),
        Action::Reload
    );
    assert_eq!(switch(&proxied(&tcp)), Action::Restart, "the proxy of a kept remote");
    let listen = render(Leaf { listen: "127.0.0.1:4333", ..leaf(&tcp, &creds, &server_conf) });
    assert_eq!(switch(&listen), Action::Restart);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn taking_a_configuration_says_what_its_service_did_or_how_to_do_it() {
    let out = Path::new("/home/me/.config/ants/leaf.conf");
    let new = "listen: \"127.0.0.1:4222\"\nleafnodes {\n  remotes: [\n    {\n      urls: [\"ws://hub:80\"]\n";
    let old = new.replace("ws://hub:80", "nats-leaf://hub:7422");
    let reloaded = take(out, Some(&old), new, |action| {
        assert_eq!(action, Action::Reload);
        Outcome::Done("systemctl --user reload ants-nats".to_owned())
    });
    assert_eq!(
        reloaded,
        "leaf node reloaded to take the uplinks of /home/me/.config/ants/leaf.conf (systemctl --user reload ants-nats)"
    );
    let by_hand = take(out, Some(&old), new, |_| Outcome::Unmanaged);
    assert!(by_hand.starts_with("switch the uplink: send the running leaf node SIGHUP"), "{by_hand}");
    assert!(by_hand.contains("launchctl kill SIGHUP gui/$(id -u)/ants.nats"), "{by_hand}");

    let moved = new.replace("4222", "4333");
    let failed = take(out, Some(new), &moved, |action| {
        assert_eq!(action, Action::Restart);
        Outcome::Failed("systemctl --user restart ants-nats".to_owned(), "Job failed.".to_owned())
    });
    assert!(failed.starts_with("`systemctl --user restart ants-nats` failed (Job failed.): restart"), "{failed}");
    assert!(failed.contains("systemctl --user restart ants-nats)"), "{failed}");
    let stopped = take(out, Some(new), &moved, |_| Outcome::NotRunning);
    assert_eq!(stopped, "the leaf node service is not running; it reads /home/me/.config/ants/leaf.conf once started");
    let first = take(out, None, new, |_| Outcome::Unmanaged);
    assert!(first.starts_with("start: nats-server -c /home/me/.config/ants/leaf.conf"), "{first}");
}

#[test]
fn includes_are_relative_to_the_configuration() {
    let rel = |dir: &str, target: &str| relative(Path::new(dir), Path::new(target)).unwrap();
    assert_eq!(rel("/home/me/.config/ants", "/home/me/.config/ants/auth/server.conf"), Path::new("auth/server.conf"));
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
        let old = fs::read_to_string(leaf_node.dir().join("leaf.conf")).unwrap();
        configure(leaf_node.dir(), uplink);
        let new = fs::read_to_string(leaf_node.dir().join("leaf.conf")).unwrap();
        assert_eq!(switch(Some(&old), &new), Action::Reload);
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

/// Uplink credentials renewed to carry another ant take a reload that respells their path: a plain reload keeps
/// the uplink on the old ones, and the local connections of the leaf node stay up all along.
#[tokio::test]
async fn renewed_uplink_credentials_reconnect_the_uplink_alone_on_reload() {
    let leaf_port = free_port();
    let Some(hub) = NatsServer::spawn_with(|dir| {
        auth::init(&dir.join("auth")).unwrap();
        format!(
            "jetstream {{ store_dir: {:?}, domain: hub }}\ninclude \"auth/{}\"\n\
                 leafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n",
            dir.join("js"),
            auth::SERVER_CONF
        )
    }) else {
        return;
    };
    let team = hub.dir().join("auth");
    let domain: JsDomain = "hub".parse().unwrap();
    let (human, dev_a, dev_b) = (UserId::human(), "dev-a".parse::<UserId>().unwrap(), "dev-b".parse().unwrap());
    let creds = |p: &UserId| {
        let out = hub.dir().join(format!("{p}.creds"));
        auth::issue(&team, p, Role::by_default(p), Some(&domain), &out).unwrap();
        out
    };
    let (_h, h_js) = bus::connect(&hub.url, Some(&creds(&human)), Some(&domain), &human, "test").await.unwrap();
    bus::init_stream(&h_js).await.unwrap();
    let leaf_creds = hub.dir().join("leaf-m1.creds");
    auth::issue_leaf(&team, &M1, &[(dev_a.clone(), Role::Ant)], &[], false, Some(&domain), &leaf_creds).unwrap();
    let server_conf = team.join(auth::SERVER_CONF);
    let listen = format!("127.0.0.1:{}", free_port());
    let leaf_node = NatsServer::spawn_with(|dir| {
        let uplinks = [format!("nats-leaf://127.0.0.1:{leaf_port}")];
        write(&Leaf { listen: &listen, ..leaf(&uplinks, &leaf_creds, &server_conf) }, &dir.join("leaf.conf")).unwrap();
        "include \"leaf.conf\"\n".to_owned()
    })
    .unwrap();
    let conf = leaf_node.dir().join("leaf.conf");
    let connect = async |p: &UserId| {
        let (_, mut js) = bus::connect(&leaf_node.url, Some(&creds(p)), Some(&domain), p, "test").await.unwrap();
        js.set_timeout(Duration::from_secs(1));
        js
    };
    let (a_js, b_js) = (connect(&dev_a).await, connect(&dev_b).await);
    let (_watched, mut closed) = watched(&leaf_node.url, &hub.dir().join("dev-a.creds")).await.unwrap();
    assert!(reaches_hub_stream(&a_js, &dev_a, true).await, "no hub stream for dev-a");
    assert!(reaches_hub_stream(&b_js, &dev_b, false).await, "dev-b is not on the uplink yet");

    let (jwt, key) = auth::read_creds(&leaf_creds).unwrap();
    let more = Permissions::for_leaf(&M1, [(&dev_b, Role::Ant)]).with_js_domain(Some(&domain));
    let renewed = auth::extend_leaf(&team, &jwt, &M1, more).unwrap();
    auth::write_creds(&leaf_creds, &renewed, &key).unwrap();
    let hup = || Command::new("kill").arg("-HUP").arg(leaf_node.pid().to_string()).status().unwrap().success();
    assert!(hup());
    assert!(!reaches_hub_stream(&b_js, &dev_b, true).await, "a plain reload reconnected the uplink");

    let before = fs::read_to_string(&conf).unwrap();
    let said = reconnect_uplink(&conf, |action| {
        assert_eq!(action, Action::Reload);
        assert!(hup());
        Outcome::Done("kill -HUP".to_owned())
    })
    .unwrap();
    assert!(said.contains("reloaded to connect its uplink again"), "{said}");
    let after = fs::read_to_string(&conf).unwrap();
    assert_eq!(switch(Some(&before), &after), Action::Reload);
    assert_eq!(read_uplink(&conf).unwrap().1, normalize(&leaf_creds).unwrap(), "the same file");
    assert!(reaches_hub_stream(&b_js, &dev_b, true).await, "dev-b reaches no hub stream after the reconnect");
    assert!(reaches_hub_stream(&a_js, &dev_a, true).await, "dev-a lost the hub stream");
    assert!(closed.try_recv().is_err(), "a local connection was closed");

    reconnect_uplink(&conf, |_| Outcome::NotRunning).unwrap();
    assert_eq!(fs::read_to_string(&conf).unwrap(), before, "two respellings give the path back");
}

#[test]
fn a_respelled_path_names_the_same_file() {
    assert_eq!(respell("/home/me/.config/ants/leaf-m1.creds"), "/home/me/.config/ants/./leaf-m1.creds");
    assert_eq!(respell("/home/me/.config/ants/./leaf-m1.creds"), "/home/me/.config/ants/leaf-m1.creds");
    assert_eq!(respell("leaf-m1.creds"), "./leaf-m1.creds");
    assert_eq!(respell("./leaf-m1.creds"), "leaf-m1.creds");
}

/// A connection with `creds` to `url` that reports when the server closes it.
async fn watched(url: &str, creds: &Path) -> Result<(async_nats::Client, tokio::sync::mpsc::UnboundedReceiver<()>)> {
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

/// A hub, with a `full` resolver or not, the team's directory in it, credentials of dev-a and dev-b, and the files
/// of m1's leaf node joined to the hub's system account, which outlive the leaf's process, to restart it; the leaf
/// has its own copy of server.conf.
struct Revocable {
    hub: NatsServer,
    team: PathBuf,
    a: PathBuf,
    b: PathBuf,
    files: PathBuf,
}

impl Revocable {
    fn new(full_resolver: bool) -> Option<Self> {
        let leaf_port = free_port();
        let hub = NatsServer::spawn_with(|dir| {
            auth::init(&dir.join("auth")).unwrap();
            let resolver = match full_resolver {
                true => format!("resolver {{ type: full, dir: {} }}\n", quote(&dir.join("jwt").to_string_lossy())),
                false => String::new(),
            };
            format!(
                "include \"auth/{}\"\n{resolver}leafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n",
                auth::SERVER_CONF
            )
        })?;
        let team = hub.dir().join("auth");
        let (dev_a, dev_b) = ("dev-a".parse::<UserId>().unwrap(), "dev-b".parse::<UserId>().unwrap());
        let creds = |user: &UserId| {
            let out = hub.dir().join(format!("{user}.creds"));
            auth::issue(&team, user, Role::Ant, None, &out).unwrap();
            out
        };
        let (a, b) = (creds(&dev_a), creds(&dev_b));
        let leaf_creds = hub.dir().join("leaf-m1.creds");
        let members = [(dev_a, Role::Ant), (dev_b, Role::Ant)];
        auth::issue_leaf(&team, &M1, &members, &[], false, None, &leaf_creds).unwrap();

        let files = scratch_dir("leaf-files");
        let server_conf = files.join(auth::SERVER_CONF);
        fs::copy(team.join(auth::SERVER_CONF), &server_conf).unwrap();
        let (uplinks, listen) = ([format!("nats-leaf://127.0.0.1:{leaf_port}")], format!("127.0.0.1:{}", free_port()));
        let system_creds = auth::leaf_system_creds(&leaf_creds);
        let config =
            Leaf { system_creds: Some(&system_creds), listen: &listen, ..leaf(&uplinks, &leaf_creds, &server_conf) };
        write(&config, &files.join("leaf.conf")).unwrap();
        Some(Self { hub, team, a, b, files })
    }

    fn start_leaf(&self) -> NatsServer {
        NatsServer::spawn_with(|dir| {
            let include = relative(&normalize(dir).unwrap(), &normalize(&self.files.join("leaf.conf")).unwrap());
            format!("include {}\n", quote(&include.unwrap().to_string_lossy()))
        })
        .unwrap()
    }

    /// The leaf nodes that applied a push, once one has: their system account uplink is up.
    async fn pushed_to_leaf(&self) -> Vec<(String, Option<String>)> {
        for _ in 0..50 {
            let pushed = crate::revoke::push(&self.hub.url, &self.team).await.unwrap();
            if !pushed.others.is_empty() {
                return pushed.others;
            }
        }
        panic!("the leaf never heard a push");
    }
}

impl Drop for Revocable {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.files);
    }
}

/// A revocation pushed to the hub reaches a leaf node through its system account uplink, with no server.conf
/// copied: the leaf closes the revoked ant's connection and refuses it, also once restarted on its older
/// server.conf.
#[tokio::test]
async fn a_revocation_pushed_to_the_hub_reaches_the_leaf_and_outlives_its_restart() {
    let Some(team) = Revocable::new(false) else { return };
    let leaf_node = team.start_leaf();
    assert_eq!(team.pushed_to_leaf().await, [("m1".to_owned(), None)]);

    let (_a_client, mut a_closed) = watched(&leaf_node.url, &team.a).await.unwrap();
    let (b_client, mut b_closed) = watched(&leaf_node.url, &team.b).await.unwrap();
    crate::revoke::revoke(&team.team, Some("dev-a"), &[]).unwrap();
    let pushed = crate::revoke::push(&team.hub.url, &team.team).await.unwrap();
    assert_eq!(pushed.others, [("m1".to_owned(), None)]);
    timeout(Duration::from_secs(5), a_closed.recv()).await.expect("dev-a's connection to the leaf stays open");
    assert!(watched(&leaf_node.url, &team.a).await.is_err(), "the leaf still takes dev-a's credentials");
    b_client.flush().await.unwrap();
    assert!(b_closed.try_recv().is_err(), "dev-b's connection was closed");

    drop((b_client, leaf_node));
    let leaf_node = team.start_leaf();
    assert!(watched(&leaf_node.url, &team.b).await.is_ok(), "the restarted leaf refuses dev-b");
    assert!(watched(&leaf_node.url, &team.a).await.is_err(), "the restarted leaf forgot dev-a's revocation");
}

/// A leaf node down while a revocation is pushed to a hub with a `full` resolver catches up once back, with no
/// other push: its resolver gets the account from the hub's, closes the revoked ant's connection and refuses it.
#[tokio::test]
async fn a_leaf_offline_during_a_push_catches_up_from_a_hub_with_a_full_resolver() {
    let Some(team) = Revocable::new(true) else { return };
    let leaf_node = team.start_leaf();
    assert_eq!(team.pushed_to_leaf().await, [("m1".to_owned(), None)]);
    drop(leaf_node);

    crate::revoke::revoke(&team.team, Some("dev-a"), &[]).unwrap();
    let pushed = crate::revoke::push(&team.hub.url, &team.team).await.unwrap();
    assert_eq!(pushed.others, [], "the stopped leaf heard the push");

    let leaf_node = team.start_leaf();
    let (_a_client, mut a_closed) =
        watched(&leaf_node.url, &team.a).await.expect("the restarted leaf knew of the revocation already");
    let (b_client, mut b_closed) = watched(&leaf_node.url, &team.b).await.unwrap();
    timeout(Duration::from_secs(30), a_closed.recv()).await.expect("dev-a's connection to the leaf stays open");
    assert!(watched(&leaf_node.url, &team.a).await.is_err(), "the leaf still takes dev-a's credentials");
    b_client.flush().await.unwrap();
    assert!(b_closed.try_recv().is_err(), "dev-b's connection was closed");
}
