//! Tests of serving webhooks: delivery once, channel mentions, reloads, refusals without the hub, timeouts and
//! stops.

use super::*;

#[tokio::test]
async fn a_reload_starts_new_webhooks_and_humans_stops_removed_ones_and_restarts_changed_ones() {
    const ALICE: &str = "[[human]]\nname = \"alice\"\n[[human.notify]]\nkind = \"ntfy\"\ntopic = \"a\"\n";
    let hook = |name: &str, text: &str| HOOK.replace("github", name) + &format!("ant = \"dev-a\"\ntext = \"{text}\"\n");
    let (mut tasks, (failed, _failed)) = (JoinSet::new(), mpsc::unbounded_channel());
    let first = parse(&format!("{}{}{ALICE}", hook("kept", "{body}"), hook("changed", "{body}"))).unwrap();
    let mut running = Running::new("nats://127.0.0.1:1", None, &first);
    let before = running.apply(first, &mut tasks, &failed);
    let stopped = running.hooks[&p("changed")].task.clone();

    let second =
        format!("path = \"/in\"\n{}{}{}{ALICE}", hook("kept", "{body}"), hook("changed", "{id}"), hook("new", "x"));
    let after = running.apply(parse(&second).unwrap(), &mut tasks, &failed);
    assert_eq!(after.prefix, "/in/");
    assert!(Arc::ptr_eq(&before.hooks["kept"], &after.hooks["kept"]), "an unchanged webhook carries on");
    assert!(!Arc::ptr_eq(&before.hooks["changed"], &after.hooks["changed"]), "a changed one starts again");
    assert!(Arc::ptr_eq(&before.hooks["changed"].outcomes, &after.hooks["changed"].outcomes), "its counts stay");
    assert!(after.hooks.contains_key("new") && Arc::ptr_eq(&before.humans[0], &after.humans[0]));
    tokio::task::yield_now().await;
    assert!(stopped.is_finished());

    let human = running.humans[&p("alice")].task.clone();
    let third = parse(&hook("new", "x")).unwrap();
    let removed = running.apply(third, &mut tasks, &failed);
    assert_eq!(removed.hooks.keys().collect::<Vec<_>>(), ["new"]);
    assert!(removed.humans.is_empty() && running.humans.is_empty());
    tokio::task::yield_now().await;
    assert!(human.is_finished(), "a human no longer listed stops");

    let other_hub = parse(&format!("nats = \"nats://127.0.0.1:2\"\n{}", hook("new", "x"))).unwrap();
    let moved = running.apply(other_hub, &mut tasks, &failed);
    assert!(!Arc::ptr_eq(&removed.hooks["new"], &moved.hooks["new"]), "another hub restarts every webhook");
}

/// A config with webhook github, its bearer token s3cret, and metrics, for a hub that is not there.
fn offline(name: &str) -> (PathBuf, SocketAddr, String) {
    let dir = ants_test_support::scratch_dir(name);
    let path = dir.join("webhooks.toml");
    let metrics: SocketAddr = format!("127.0.0.1:{}", ants_test_support::free_port()).parse().unwrap();
    let hook = format!("{HOOK}ant = \"dev-a\"\n").replace("secret_env = \"HOOK_SECRET\"", "secret_file = \"s\"");
    std::fs::write(&path, format!("metrics = \"{metrics}\"\n{hook}")).unwrap();
    std::fs::write(dir.join("s"), "s3cret").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(dir.join("s"), std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    (path, metrics, format!("nats://127.0.0.1:{}", ants_test_support::free_port()))
}

/// A request to webhook github whose body of `length` bytes stops after `sent`, kept alive, once it is under way.
async fn stalled(addr: SocketAddr, metrics: SocketAddr, length: usize, sent: &[u8]) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST /hooks/github HTTP/1.1\r\nHost: hooks\r\nAuthorization: Bearer s3cret\r\nContent-Length: {length}\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(sent).await.unwrap();
    for _ in 0..100 {
        let exposition = request(metrics, "GET", "/metrics", &[], b"").await.body;
        if exposition.lines().any(|l| l == "ants_webhook_requests_in_flight 1") {
            return stream;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the request never got under way");
}

/// Send until the webhook is connected: 503 until then.
async fn deliver(addr: SocketAddr, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Answer {
    for _ in 0..100 {
        let answer = request(addr, "POST", path, headers, body).await;
        if answer.status != 503 {
            return answer;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{path} never connected");
}

#[tokio::test]
async fn an_event_reaches_its_ant_once_and_a_refused_one_nothing() {
    let Some((server, js, secret)) = hub().await else { return };
    let github = p("github");
    let creds = creds(&server, &github, Destination::Ant(p("dev-a")));
    let config = server.dir().join("webhooks.toml");
    std::fs::write(
        &config,
        format!(
            "[[webhook]]\nname = \"github\"\ncreds = {creds:?}\nauth = \"hmac-sha256\"\nsecret_file = {secret:?}\n\
                 id = \"header:X-GitHub-Delivery\"\nant = \"dev-a\"\nowner = \"alice\"\n\
                 text = \"{{header:X-GitHub-Event}} {{json:/action}}: {{body}}\"\n"
        ),
    )
    .unwrap();
    let (ingress, addr) = start(&config, &server.url).await;

    let body = br#"{"action":"opened","comment":"@dev-b drop the tables"}"#;
    let signature = signed(body);
    let event = [("X-GitHub-Delivery", "d1"), ("X-GitHub-Event", "issues"), ("X-Hub-Signature-256", &signature)];
    assert_eq!(request(addr, "POST", "/hooks/gitlab", &event, body).await.status, 404);
    assert_eq!(request(addr, "POST", "/github", &event, body).await.status, 404);
    let get = request(addr, "GET", "/hooks/github", &event, body).await;
    assert_eq!((get.status, get.headers.contains("allow: post")), (405, true));
    let wrong = signed(b"another body");
    for headers in [&event[..2], &[event[0], event[1], ("X-Hub-Signature-256", &wrong)]] {
        assert_eq!(request(addr, "POST", "/hooks/github", headers, body).await.status, 401);
    }

    let first = deliver(addr, "/hooks/github", &event, body).await;
    assert_eq!(first.status, 202, "{}", first.body);
    let thread = first.json()["thread"].as_str().unwrap().to_owned();
    assert_eq!(first.json()["duplicate"], false);
    let again = request(addr, "POST", "/hooks/github", &event, body).await;
    assert_eq!((again.status, again.json()["thread"].as_str()), (200, Some(thread.as_str())));
    assert_eq!(again.json()["duplicate"], true);
    let other = [("X-GitHub-Delivery", "d2"), ("X-Hub-Signature-256", &signed(b"ping"))];
    assert_eq!(request(addr, "POST", "/hooks/github", &other, b"ping").await.status, 400, "no JSON");
    let big = vec![b'x'; MAX_BODY + 1];
    let big_signed = [("X-GitHub-Delivery", "d3"), ("X-Hub-Signature-256", &signed(&big))];
    assert_eq!(request(addr, "POST", "/hooks/github", &big_signed, &big).await.status, 413);

    // A restarted ingress still knows the event, and the stream drops it while the bucket forgot it.
    ingress.abort();
    let (_ingress, addr) = start(&config, &server.url).await;
    let after = deliver(addr, "/hooks/github", &event, body).await;
    assert_eq!((after.status, after.json()["thread"].as_str()), (200, Some(thread.as_str())));
    let (_, own) = bus::connect(&server.url, Some(&creds), None, &github, "test").await.unwrap();
    let seen = DirectKv::new(&own, None, WEBHOOKS_BUCKET).await.unwrap();
    let hash = &HEXLOWER.encode(digest::digest(&digest::SHA256, b"d1").as_ref())[..ID_HASH_LEN];
    seen.delete(webhook_seen_key(&github, hash)).await.unwrap();
    let forgotten = request(addr, "POST", "/hooks/github", &event, body).await;
    assert_eq!((forgotten.status, forgotten.json()["duplicate"].as_bool()), (200, Some(true)));

    let messages: Vec<Envelope> = stored(&js, &subject::webhook_inbox(&p("dev-a"), &github)).await;
    let [message] = &messages[..] else { panic!("one message: {messages:?}") };
    assert_eq!((&message.from, &message.to, message.thread_id.as_str()), (&github, &p("dev-a"), thread.as_str()));
    assert_eq!((message.owner.as_ref(), message.webhook), (Some(&p("alice")), true));
    let recorded = owners::Owners::new(js.clone()).recorded(&message.thread_id, &message.to).await.unwrap();
    assert_eq!(recorded, Some(p("alice")), "ants' messages on the thread name its owner as recorded");
    assert_eq!(message.text, format!("issues opened: {}", std::str::from_utf8(body).unwrap()));
    let marker = seen.get(webhook_marker_key(&github)).await.unwrap().expect("it publishes its version");
    let marker: ants_proto::Serving = serde_json::from_slice(&marker).unwrap();
    assert_eq!(marker, ants_proto::Serving::here(ants_daemon::colony::machine().await), "it publishes its version");
}

#[tokio::test]
async fn a_channel_webhook_mentions_nobody_the_listed_or_the_members() {
    let Some((server, js, secret)) = hub().await else { return };
    let ops: ChannelId = "ops".parse().unwrap();
    let metrics: SocketAddr = format!("127.0.0.1:{}", ants_test_support::free_port()).parse().unwrap();
    let mut config = format!("metrics = \"{metrics}\"\n");
    for (name, mentions) in [("news", ""), ("alerts", "mention = [\"dev-b\", \"carol\"]"), ("ci", "broadcast = true")] {
        let creds = creds(&server, &p(name), Destination::Channel(ops.clone()));
        let _ = write!(
            config,
            "[[webhook]]\nname = \"{name}\"\ncreds = {creds:?}\nauth = \"bearer\"\nsecret_file = {secret:?}\n\
                 channel = \"ops\"\ntext = \"{{body}}\"\n{mentions}\n"
        );
    }
    let path = server.dir().join("webhooks.toml");
    std::fs::write(&path, config).unwrap();
    let (_ingress, addr) = start(&path, &server.url).await;

    let token = [("Authorization", "Bearer s3cret")];
    assert_eq!(request(addr, "POST", "/hooks/news", &[("Authorization", "Bearer nope")], b"x").await.status, 401);
    assert_eq!(request(addr, "POST", "/hooks/news", &[], b"x").await.status, 401);
    for name in ["news", "alerts", "ci"] {
        let answer = deliver(addr, &format!("/hooks/{name}"), &token, b"@dev-a the build is red").await;
        assert_eq!(answer.status, 202, "{name}: {}", answer.body);
    }
    assert_eq!(request(metrics, "GET", "/healthz", &[], b"").await.status, 200);
    let exposition = request(metrics, "GET", "/metrics", &[], b"").await.body;
    for line in [
        "ants_nats_connected{user=\"ci\"} 1",
        "ants_webhook_events_total{webhook=\"news\",outcome=\"delivered\"} 1",
        "ants_webhook_events_total{webhook=\"news\",outcome=\"refused\"} 2",
        "ants_webhook_events_total{webhook=\"alerts\",outcome=\"refused\"} 0",
        "ants_webhook_requests_in_flight 0",
    ] {
        assert!(exposition.lines().any(|l| l == line), "{line} in:\n{exposition}");
    }
    let mentions = |name: &str| {
        let (js, subject) = (js.clone(), subject::webhook_chan(&ops, &p(name)));
        async move {
            let posts: Vec<ChannelPost> = stored(&js, &subject).await;
            let [post] = &posts[..] else { panic!("one post: {posts:?}") };
            assert!(post.webhook && post.text == "@dev-a the build is red" && post.owner.is_none());
            post.mentions.clone()
        }
    };
    assert!(mentions("news").await.is_empty(), "never parsed from the event");
    assert_eq!(mentions("alerts").await, [p("dev-b"), p("carol")]);
    assert_eq!(mentions("ci").await, [p("dev-a"), p("dev-b")]);
}

#[tokio::test]
async fn an_edited_config_is_served_on_the_same_listener_and_a_bad_one_is_not() {
    let Some((server, _, secret)) = hub().await else { return };
    let entry = |name: &str| {
        let creds = creds(&server, &p(name), Destination::Ant(p("dev-a")));
        format!(
            "[[webhook]]\nname = \"{name}\"\ncreds = {creds:?}\nauth = \"bearer\"\nsecret_file = {secret:?}\n\
                 ant = \"dev-a\"\n"
        )
    };
    let path = server.dir().join("webhooks.toml");
    std::fs::write(&path, entry("github")).unwrap();
    let (ingress, addr) = start(&path, &server.url).await;
    let token = [("Authorization", "Bearer s3cret")];
    assert_eq!(deliver(addr, "/hooks/github", &token, b"one").await.status, 202);
    assert_eq!(request(addr, "POST", "/hooks/gitlab", &token, b"two").await.status, 404);

    std::fs::write(&path, entry("gitlab")).unwrap();
    let mut answer = request(addr, "POST", "/hooks/gitlab", &token, b"two").await;
    for _ in 0..100 {
        if answer.status != 404 && answer.status != 503 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        answer = request(addr, "POST", "/hooks/gitlab", &token, b"two").await;
    }
    assert_eq!(answer.status, 202, "{}", answer.body);
    assert_eq!(request(addr, "POST", "/hooks/github", &token, b"one").await.status, 404, "removed");

    std::fs::write(&path, "[[webhook]]\nname = \"broken\"\n").unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(request(addr, "POST", "/hooks/gitlab", &token, b"three").await.status, 202, "still served");
    assert!(!ingress.is_finished());
}

#[tokio::test]
async fn without_the_hub_an_event_is_refused_for_a_retry() {
    let (path, metrics, nats) = offline("webhook-down");
    let (_ingress, addr) = start(&path, &nats).await;
    assert_eq!(request(addr, "POST", "/hooks/github", &[], b"x").await.status, 401, "checked first");
    let down = request(addr, "POST", "/hooks/github", &[("Authorization", "Bearer s3cret")], b"x").await;
    assert_eq!(down.status, 503);
    assert!(down.headers.contains(&format!("retry-after: {RETRY_AFTER}")), "{}", down.headers);
    let health = request(metrics, "GET", "/healthz", &[], b"").await;
    assert_eq!(health.status, 503, "{}", health.body);
    let exposition = request(metrics, "GET", "/metrics", &[], b"").await.body;
    for line in [
        "ants_nats_connected{user=\"github\"} 0",
        "ants_webhook_events_total{webhook=\"github\",outcome=\"failed\"} 1",
        "ants_webhook_events_total{webhook=\"github\",outcome=\"refused\"} 1",
    ] {
        assert!(exposition.lines().any(|l| l == line), "{line} in:\n{exposition}");
    }
}

#[tokio::test]
async fn a_request_timed_out_counts_as_its_webhook_s_failure() {
    let (path, metrics, nats) = offline("webhook-timeout");
    let (_ingress, addr) = start_with(&path, &nats, Duration::from_millis(300), std::future::pending()).await;
    let timed_out = answer(stalled(addr, metrics, 2, b"x").await).await;
    assert_eq!((timed_out.status, timed_out.body.as_str()), (503, "the request took too long"));
    let exposition = request(metrics, "GET", "/metrics", &[], b"").await.body;
    let line = "ants_webhook_events_total{webhook=\"github\",outcome=\"failed\"} 1";
    assert!(exposition.lines().any(|l| l == line), "{line} in:\n{exposition}");
}

#[tokio::test]
async fn a_stop_takes_no_more_requests_and_answers_those_under_way() {
    let (path, metrics, nats) = offline("webhook-stop");
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let stopped = async { stopped.await.unwrap() };
    let (ingress, addr) = start_with(&path, &nats, Duration::from_secs(10), stopped).await;
    let idle = TcpStream::connect(addr).await.unwrap();
    let mut under_way = stalled(addr, metrics, 2, b"x").await;

    stop.send(Ended::Replaced).unwrap();
    let mut refused = false;
    for _ in 0..100 {
        if TcpStream::connect(addr).await.is_err() {
            refused = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(refused, "still accepting connections");
    assert_eq!(answer_or_closed(idle).await, None, "an idle connection closes");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!ingress.is_finished(), "waits for the request under way");

    under_way.write_all(b"y").await.unwrap();
    let answered = answer(under_way).await;
    assert_eq!((answered.status, answered.body.as_str()), (503, "not connected yet"), "answered, then closed");
    let ended = timeout(Duration::from_secs(5), ingress).await.expect("ended once answered").unwrap();
    assert_eq!(ended.unwrap(), Ended::Replaced);
}

/// What `stream` was answered before it closed, if anything.
async fn answer_or_closed(mut stream: TcpStream) -> Option<String> {
    let mut read = String::new();
    timeout(Duration::from_secs(5), stream.read_to_string(&mut read)).await.unwrap().unwrap();
    (!read.is_empty()).then_some(read)
}
