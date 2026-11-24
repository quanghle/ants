//! Tests of the doctor's transport checks: dev tunnels, HTTP bridges, the webhook config, the trails webhooks start
//! and exec sinks.

use super::*;

#[cfg(unix)]
fn private(path: &Path, text: &str) {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::write(path, text).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[cfg(unix)]
fn chmod(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

#[cfg(unix)]
async fn transports(home: &Path) -> Vec<Check> {
    let installed = installed_transports(&mut running(home));
    check_transports(installed, auth::now().unwrap()).await
}

#[cfg(unix)]
#[tokio::test]
async fn a_dev_tunnel_needs_a_login_or_a_connect_token_far_from_expiring() {
    let home = ants_test_support::scratch_dir("doctor-devtunnel").canonicalize().unwrap();
    assert!(transports(&home).await.is_empty(), "nothing without the services");
    let devtunnel = home.join("devtunnel");
    let user_show = |json: &str| {
        ants_test_support::executable(&devtunnel, &format!("#!/bin/sh\nprintf '%s\\n' '{json}'\n"));
    };
    user_show(r#"{"status": "Not logged in"}"#);
    let token = home.join("connect-token");
    let now = auth::now().unwrap();
    let jwt = |exp: u64| {
        let payload = data_encoding::BASE64URL_NOPAD.encode(format!("{{\"exp\":{exp}}}").as_bytes());
        format!("eyJhbGciOiJSUzI1NiJ9.{payload}.c2ln\n")
    };
    private(&token, &jwt(now + 3 * 3600 + 1800));
    let (bin, file) = (devtunnel.to_str().unwrap(), token.to_str().unwrap());
    let args = ["--devtunnel", "hub", "--devtunnel-bin", bin, "--devtunnel-token", file, "--devtunnel-host", "hub"];
    if !install_units(&home, &args) {
        return;
    }
    let restart = running(&home).by_hand(Service::DevTunnel, Action::Restart).unwrap();

    let checks = transports(&home).await;
    assert_eq!(one(&checks, "devtunnel service").status, Status::Ok, "{checks:#?}");
    assert_eq!(one(&checks, "devtunnel host service").status, Status::Ok, "{checks:#?}");
    let expiring = one(&checks, "devtunnel token");
    assert_eq!((expiring.status, expiring.detail.as_str()), (Status::Warn, &*format!("{file}: expiring in 3h")));
    let renew = format!("devtunnel token hub --scopes connect, its token into {file} (mode 0600), then {restart}");
    assert_eq!(expiring.fix, std::slice::from_ref(&renew));
    let login = one(&checks, "devtunnel login");
    assert_eq!(
        (login.status, login.detail.as_str()),
        (Status::Fail, "Not logged in: the tunnel goes down at its next connection")
    );
    assert!(login.fix[0].starts_with("devtunnel user login"), "{login}");

    let in_days = (chrono::Utc::now() + chrono::Duration::hours(80)).to_rfc3339();
    user_show(&format!(r#"{{"status": "Logged in", "username": "alice", "expiration": "{in_days}"}}"#));
    private(&token, &jwt(now + 10 * 3600));
    let checks = transports(&home).await;
    assert_eq!(one(&checks, "devtunnel token").status, Status::Ok, "{checks:#?}");
    let login = one(&checks, "devtunnel login");
    assert_eq!((login.status, login.detail.as_str()), (Status::Ok, "logged in as alice for 3 days"));
    let in_hours = (chrono::Utc::now() + chrono::Duration::hours(5)).to_rfc3339();
    user_show(&format!(r#"{{"status": "Logged in", "expiration": "{in_hours}"}}"#));
    assert_eq!(one(&transports(&home).await, "devtunnel login").status, Status::Warn);

    private(&token, &jwt(now - 1));
    assert_eq!(one(&transports(&home).await, "devtunnel token").status, Status::Fail);
    private(&token, "an-opaque-token\n");
    let written = one(&transports(&home).await, "devtunnel token").clone();
    assert_eq!(
        (written.status, written.detail),
        (Status::Ok, format!("{file}: written 0m ago, and a connect token lasts a day"))
    );
    chmod(&token, 0o644);
    let exposed = one(&transports(&home).await, "devtunnel token").clone();
    assert_eq!((exposed.status, exposed.fix), (Status::Warn, vec![format!("chmod 600 {file}")]));
    std::fs::remove_file(&token).unwrap();
    let missing = one(&transports(&home).await, "devtunnel token").clone();
    assert_eq!((missing.status, missing.fix), (Status::Fail, vec![renew]));
    std::fs::remove_dir_all(&home).unwrap();
}

#[cfg(unix)]
/// The `ants bridge serve` unit, with `serve`, else the `ants bridge connect` one, installed under `home`, run with
/// `flags` after its subcommand, as an edit of the unit by hand does: the first of a flag given twice is the one read.
fn bridge_flags(home: &Path, serve: bool, flags: &[&str]) {
    let sub = if serve { "serve" } else { "connect" };
    let (unit, from, to) = match std::env::consts::OS {
        "macos" => {
            let flags: String = flags.iter().map(|flag| format!("<string>{flag}</string>")).collect();
            let label = if serve { "ants.bridge-serve" } else { "ants.bridge" };
            let unit = home.join(format!("Library/LaunchAgents/{label}.plist"));
            (unit, format!("<string>{sub}</string>"), format!("<string>{sub}</string>{flags}"))
        }
        _ => {
            let name = if serve { "ants-bridge-serve" } else { "ants-bridge" };
            let unit = home.join(format!(".config/systemd/user/{name}.service"));
            (unit, format!("\" bridge {sub} "), format!("\" bridge {sub} {} ", flags.join(" ")))
        }
    };
    let text = std::fs::read_to_string(&unit).unwrap();
    assert!(text.contains(&from), "{text}");
    std::fs::write(&unit, text.replacen(&from, &to, 1)).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_bridge_must_answer_and_take_the_token_of_its_file() {
    let home = ants_test_support::scratch_dir("doctor-bridge").canonicalize().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(bridge::Server::new("127.0.0.1:1".to_owned(), Some("s3cret".to_owned())).run(listener));
    let token = home.join("bridge-token");
    private(&token, "s3cret\n");
    let url = format!("http://127.0.0.1:{port}/bridge");
    let file = token.to_str().unwrap();
    let args = ["--ants", "/bin/sh", "--bridge", &url, "--bridge-token", file, "--bridge-serve"];
    if !install_units(&home, &args) {
        return;
    }
    // The bridge this test serves, not the default port.
    bridge_flags(&home, true, &["--listen", &format!("127.0.0.1:{port}")]);
    let restart = running(&home).by_hand(Service::BridgeServe, Action::Restart).unwrap();

    let checks = transports(&home).await;
    for check in ["bridge service", "bridge token", "bridge serve service", "bridge serve token"] {
        assert_eq!(one(&checks, check).status, Status::Ok, "{checks:#?}");
    }
    assert_eq!(one(&checks, "bridge url").detail, format!("{url} answers as a bridge taking the token"));
    let serving = one(&checks, "bridge serve url");
    assert_eq!(serving.detail, format!("http://127.0.0.1:{port} answers as a bridge taking the token"));
    // The bridge answers, but its upstream is no hub's leafnode port.
    let pipe = one(&checks, "bridge pipe");
    let unopened =
        format!("{url} answers but opens no connection to the hub's leafnode port: 502 Bad Gateway upstream");
    assert!(pipe.status == Status::Fail && pipe.detail.starts_with(&unopened), "{pipe}");
    let hub = "on the hub, start nats-server, whose leafnodes listen where `ants bridge serve --upstream` points \
               (127.0.0.1:7422 by default); `ants doctor` there checks the bridge serve pipe";
    assert_eq!(pipe.fix, [hub]);
    let pipe = one(&checks, "bridge serve pipe");
    assert_eq!(pipe.status, Status::Fail, "{pipe}");
    let start = format!(
        "start the hub's nats-server, whose leafnodes listen on 127.0.0.1:7422, or give `ants bridge serve` the \
         --upstream of its leafnode port, then {restart}"
    );
    assert_eq!(pipe.fix, [start]);

    private(&token, "guess\n");
    let checks = transports(&home).await;
    let refused = one(&checks, "bridge url");
    assert_eq!(
        (refused.status, refused.detail.as_str()),
        (Status::Fail, &*format!("{url} refuses the token: missing or wrong bridge token"))
    );
    assert_eq!(one(&checks, "bridge pipe").status, Status::Skip, "{checks:#?}");
    chmod(&token, 0o644);
    let exposed = one(&transports(&home).await, "bridge token").clone();
    assert_eq!((exposed.status, exposed.fix), (Status::Warn, vec![format!("chmod 600 {file}")]));
    std::fs::remove_file(&token).unwrap();
    let checks = transports(&home).await;
    assert_eq!(one(&checks, "bridge token").status, Status::Fail, "{checks:#?}");
    assert_eq!(one(&checks, "bridge url").status, Status::Fail, "{checks:#?}");
    std::fs::remove_dir_all(&home).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_bridge_must_carry_the_leaf_node_s_uplink_to_the_hub_s_leafnode_port() {
    let leaf_port = ants_test_support::free_port();
    let Some(hub) = ants_test_support::NatsServer::spawn_with(|_| {
        format!("leafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n")
    }) else {
        return;
    };
    let home = ants_test_support::scratch_dir("doctor-bridge-pipe").canonicalize().unwrap();
    let local = async || {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    };
    let (serve, serve_port) = local().await;
    let upstream = format!("127.0.0.1:{leaf_port}");
    tokio::spawn(bridge::Server::new(upstream.clone(), Some("s3cret".to_owned())).run(serve));
    let url = format!("http://127.0.0.1:{serve_port}/bridge");
    // `ants bridge connect` as the unit runs it, and one started before the token it holds was replaced.
    let (connect, connect_port) = local().await;
    tokio::spawn(bridge::Remote::new(&url, Some("s3cret"), &[], None).unwrap().run(connect));
    let (stale, stale_port) = local().await;
    tokio::spawn(bridge::Remote::new(&url, Some("old"), &[], None).unwrap().run(stale));
    let token = home.join("bridge-token");
    private(&token, "s3cret\n");
    let args = ["--ants", "/bin/sh", "--bridge", &url, "--bridge-token", token.to_str().unwrap(), "--bridge-serve"];
    if !install_units(&home, &args) {
        return;
    }
    bridge_flags(&home, true, &["--listen", &format!("127.0.0.1:{serve_port}"), "--upstream", &upstream]);
    bridge_flags(&home, false, &["--listen", &format!("127.0.0.1:{connect_port}")]);
    let restart = running(&home).by_hand(Service::Bridge, Action::Restart).unwrap();

    let checks = transports(&home).await;
    let pipe = one(&checks, "bridge pipe");
    let carried = format!("127.0.0.1:{connect_port} carries the leaf node's uplink through {url} to the leafnode port");
    assert!(pipe.status == Status::Ok && pipe.detail.starts_with(&carried), "{pipe}");
    let pipe = one(&checks, "bridge serve pipe");
    let carried = format!("http://127.0.0.1:{serve_port} carries a session to the leafnode port of nats-server");
    assert!(pipe.status == Status::Ok && pipe.detail.starts_with(&carried), "{pipe}");
    assert!(pipe.detail.ends_with(&format!(" at {upstream}")), "{pipe}");

    bridge_flags(&home, false, &["--listen", &format!("127.0.0.1:{stale_port}")]);
    let pipe = one(&transports(&home).await, "bridge pipe").clone();
    let detail = format!(
        "127.0.0.1:{stale_port} carries nothing, though {url} carries the hub's INFO: the running `ants bridge \
         connect` took another token, URL or proxy at its start"
    );
    assert_eq!((pipe.status, pipe.detail, pipe.fix), (Status::Fail, detail, vec![restart.clone()]));
    let closed = ants_test_support::free_port();
    bridge_flags(&home, false, &["--listen", &format!("127.0.0.1:{closed}")]);
    let pipe = one(&transports(&home).await, "bridge pipe").clone();
    let nothing = format!("nothing carries the leaf node's uplink at 127.0.0.1:{closed}: ");
    assert!(pipe.status == Status::Fail && pipe.detail.starts_with(&nothing), "{pipe}");
    assert_eq!(pipe.fix, [restart]);

    // `ants bridge serve` whose upstream is the hub's client port.
    let (wrong, wrong_port) = local().await;
    let client = hub.url.trim_start_matches("nats://").to_owned();
    tokio::spawn(bridge::Server::new(client.clone(), Some("s3cret".to_owned())).run(wrong));
    bridge_flags(&home, true, &["--listen", &format!("127.0.0.1:{wrong_port}"), "--upstream", &client]);
    let pipe = one(&transports(&home).await, "bridge serve pipe").clone();
    let detail = format!("http://127.0.0.1:{wrong_port} answers but carries the INFO of nats-server ");
    assert!(pipe.status == Status::Fail && pipe.detail.starts_with(&detail), "{pipe}");
    assert!(
        pipe.detail.ends_with("from another port than its leafnode port, not the INFO of the hub's leafnode port"),
        "{pipe}"
    );
    std::fs::remove_dir_all(&home).unwrap();
}

#[test]
fn only_a_leafnode_port_s_info_is_taken_as_the_hub_s() {
    let leaf = r#"INFO {"server_id":"N1","server_name":"hub","version":"2.15.3","proto":3,"auth_required":true,"client_id":6,"nonce":"x","info_on_connect":true} "#;
    assert_eq!(leafnode_info(leaf), Ok("nats-server 2.15.3 (hub)".to_owned()));
    let routed = r#"INFO {"server_name":"hub","version":"2.16.0","client_id":3,"leafnode_urls":["10.0.0.1:7422"]}"#;
    assert_eq!(leafnode_info(routed), Ok("nats-server 2.16.0 (hub)".to_owned()));
    let client = r#"INFO {"server_id":"N1","server_name":"hub","version":"2.15.3","proto":1,"client_id":5,"client_ip":"127.0.0.1"} "#;
    let other = "the INFO of nats-server 2.15.3 (hub) from another port than its leafnode port".to_owned();
    assert_eq!(leafnode_info(client), Err(other));
    assert_eq!(leafnode_info(""), Err("nothing".to_owned()));
    assert_eq!(leafnode_info("HTTP/1.1 400 Bad Request"), Err("\"HTTP/1.1 400 Bad Request\"".to_owned()));
}

#[cfg(unix)]
#[tokio::test]
async fn the_webhook_config_needs_private_secrets_and_a_public_url_reaching_it() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let dir = ants_test_support::scratch_dir("doctor-webhooks").canonicalize().unwrap();
    let path = dir.join("webhooks.toml");
    assert!(check_webhooks(&path).await.is_empty(), "nothing without the file");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut head = [0; 4096];
            let n = stream.read(&mut head).await.unwrap_or_default();
            let page = "<h1>Bad link</h1><p>The link is incomplete.</p>";
            let answer = if head[..n].starts_with(b"GET /hooks/_approval HTTP/1.1") {
                format!("HTTP/1.1 400 Bad Request\r\ncontent-length: {}\r\n\r\n{page}", page.len())
            } else {
                "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n".to_owned()
            };
            let _ = stream.write_all(answer.as_bytes()).await;
        }
    });
    private(&dir.join("action"), "0123456789abcdef-secret\n");
    std::fs::write(dir.join("ntfy"), "tk_secret\n").unwrap();
    chmod(&dir.join("ntfy"), 0o644);
    let config = |public_url: &str| {
        let text = format!(
            "public_url = \"{public_url}\"\naction_secret_file = \"action\"\n[[human]]\nname = \"alice\"\n\
                 [[human.notify]]\nkind = \"ntfy\"\ntopic = \"a\"\nsecret_file = \"ntfy\"\n"
        );
        std::fs::write(&path, text).unwrap();
    };
    config(&format!("http://{addr}/hooks"));

    let checks = check_webhooks(&path).await;
    let exposed = one(&checks, "webhook secrets");
    let ntfy = dir.join("ntfy").display().to_string();
    assert_eq!(exposed.status, Status::Fail);
    assert_eq!(exposed.detail, format!("human alice notify #1: {ntfy} has mode 0644, readable by others"));
    assert_eq!(exposed.fix, [format!("chmod 600 {ntfy}")]);
    assert_eq!(one(&checks, "webhooks").status, Status::Skip);

    chmod(&dir.join("ntfy"), 0o600);
    let checks = check_webhooks(&path).await;
    assert!(checks.iter().all(|c| c.status == Status::Ok), "{checks:#?}");
    assert!(one(&checks, "webhooks").detail.contains("notifies alice"), "{checks:#?}");
    assert_eq!(
        one(&checks, "public_url").detail,
        format!("http://{addr}/hooks/_approval answers as `ants webhook serve` does")
    );

    config(&format!("http://{addr}/elsewhere"));
    assert_eq!(one(&check_webhooks(&path).await, "public_url").status, Status::Warn);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn the_programs_of_exec_sinks_must_be_there_to_run() {
    let dir = ants_test_support::scratch_dir("doctor-exec").canonicalize().unwrap();
    let path = dir.join("webhooks.toml");
    private(&dir.join("hook"), "0123456789abcdef-hook\n");
    std::fs::write(
        &path,
        "[[human]]\nname = \"alice\"\n\
             [[human.notify]]\nkind = \"exec\"\ncommand = [\"bin/notify\", \"--quiet\"]\n\
             [[human.notify]]\nkind = \"exec\"\ncommand = [\"no-such-program-of-ants\"]\n\
             [[human.notify]]\nkind = \"http\"\nurl = \"https://hooks.example/a\"\nsecret_file = \"hook\"\n",
    )
    .unwrap();
    let program = dir.join("bin/notify");
    let commands = |checks: &[Check]| -> Vec<(Status, String)> {
        checks.iter().filter(|c| c.check == "notify commands").map(|c| (c.status, c.detail.clone())).collect()
    };
    let checks = check_webhooks(&path).await;
    let missing = format!("human alice notify #1: {} is no executable file", program.display());
    let off_path = "human alice notify #2: no-such-program-of-ants is not on PATH here".to_owned();
    assert_eq!(commands(&checks), [(Status::Fail, missing), (Status::Warn, off_path.clone())]);
    assert_eq!(one(&checks, "webhook secrets").status, Status::Ok, "the http sink's secret file: {checks:#?}");

    std::fs::create_dir_all(dir.join("bin")).unwrap();
    std::fs::write(&program, "#!/bin/sh\ncat > /dev/null\n").unwrap();
    chmod(&program, 0o755);
    let ready = format!("human alice notify #1: {} is executable", program.display());
    assert_eq!(commands(&check_webhooks(&path).await), [(Status::Ok, ready), (Status::Warn, off_path)]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_webhook_starting_a_trail_needs_the_trail_saved_in_the_colony_or_on_this_machine() {
    let Some(server) = Server::start() else { return };
    let alice = ants_test_support::user("alice");
    let creds = server.creds_as(&alice, Role::Human);
    let (_, js) = server.connect_as(&alice, Role::Human).await;
    let rights = Rights::from_creds(Some(&creds)).unwrap();
    hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
    let connected = Connected { js: js.clone(), rights, colony: Ok(Vec::new()) };
    let dir = server.dir();
    let hook = |name: &str| {
        format!("[[webhook]]\nname = \"{name}\"\nauth = \"bearer\"\nsecret_env = \"S\"\nant = \"dev-a\"\n")
    };
    let text = format!("{}trail = \"triage\"\n{}", hook("triage"), hook("plain"));
    let config = webhook::Config::parse(&text, &dir.join("webhooks.toml"), dir, |_| Some("s".to_owned())).unwrap();
    let local = dir.join("trails");
    let shown = |checks: Vec<Check>| -> Vec<(String, Status, String, Vec<String>)> {
        checks.into_iter().map(|c| (c.check, c.status, c.detail, c.fix)).collect()
    };

    let missing =
        format!("starts trail triage, saved neither in the colony nor in {}: each event's run fails", local.display());
    let fix = vec!["ants trail save FILE --name triage".to_owned()];
    assert_eq!(
        shown(check_webhook_trails(&config.webhooks, Some(&connected), &local).await),
        [("webhook triage trail".to_owned(), Status::Fail, missing, fix)],
        "the plain webhook starts none"
    );
    let unread = check_webhook_trails(&config.webhooks, None, &local).await;
    assert_eq!(unread[0].status, Status::Skip, "{unread:#?}");

    std::fs::create_dir_all(&local).unwrap();
    let script = "meta = {\"name\": \"triage\", \"description\": \"Triage it\", \"phases\": []}\nagent(\"triage\")\n";
    let file = local.join("triage.py");
    std::fs::write(&file, script).unwrap();
    let here = format!("starts trail triage, saved in {}", file.display());
    for connected in [Some(&connected), None] {
        let checks = shown(check_webhook_trails(&config.webhooks, connected, &local).await);
        assert_eq!((checks[0].1, &checks[0].2), (Status::Ok, &here));
    }

    let saved = crate::trails::saved::read(&file, None, &alice, chrono::Utc::now()).unwrap();
    crate::trails::saved::put(&js, &saved).await.unwrap();
    std::fs::remove_file(&file).unwrap();
    let checks = shown(check_webhook_trails(&config.webhooks, Some(&connected), &local).await);
    assert_eq!((checks[0].1, checks[0].2.as_str()), (Status::Ok, "starts trail triage, saved in the colony"));
}
