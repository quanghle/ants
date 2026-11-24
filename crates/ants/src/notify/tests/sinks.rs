//! Tests of what sinks get: signed links, a button per verdict in each sink's form, and the notice as versioned
//! JSON to exec and http sinks, each failure reported.

use super::*;

#[test]
fn a_link_holds_only_as_signed_and_until_its_request_expires() {
    let (links, action) = (links(), action());
    let url = links.url(&action);
    let query = url.strip_prefix("https://hub.example/hooks/_approval?").expect(&url);
    assert_eq!(links.verify(query, Utc::now()), Ok(action.clone()));

    let later = DateTime::from_timestamp(action.expires, 0).unwrap();
    assert_eq!(links.verify(query, later), Err(LinkError::Expired));
    let other = Links::new("https://hub.example/hooks", "another-secret-of-length").unwrap();
    assert_eq!(other.verify(query, Utc::now()), Err(LinkError::Forged));
    for (from, to) in [
        ("verdict=allow_once", "verdict=allow_always"),
        ("by=alice", "by=bob"),
        ("seq=7", "seq=8"),
        ("ant=dev-a", "ant=dev-b"),
        (&format!("exp={}", action.expires), &format!("exp={}", action.expires + 3600)),
    ] {
        assert_eq!(links.verify(&query.replace(from, to), Utc::now()), Err(LinkError::Forged), "{to}");
    }
    let sig = query.rsplit_once("sig=").unwrap().1;
    assert_eq!(links.verify(&query.replace(sig, &sig.to_uppercase()), Utc::now()), Ok(action));
    for broken in [
        "",
        &query.replace("verdict=allow_once", "verdict=maybe"),
        &query.replace("&seq=7", ""),
        &format!("{query}&seq=7"),
        &format!("{query}&x=1"),
        &query.replace("sig=", "sig"),
    ] {
        assert_eq!(links.verify(broken, Utc::now()), Err(LinkError::Malformed), "{broken}");
    }
    assert!(Links::new("https://hub.example", "short").is_err());
    assert!(Links::new("hub.example", "0123456789abcdef-secret").is_err());
}

#[test]
fn a_notice_carries_a_button_per_verdict_offered_in_each_sink_s_form() {
    let (request, links) = (request(), links());
    let notice = approval_notice(&request, 9, &p("alice"), Some(&links), None, Utc::now());
    assert_eq!(notice.title, "dev-a asks to run a tool");
    assert!(notice.text.contains("rm -rf <build> (execute)\nfiles: src/a.rs\ninput: {\"command\""), "{}", notice.text);
    assert!(notice.text.contains(&format!("ants tools approve dev-a/{}", request.id)), "{}", notice.text);
    let verdicts: Vec<_> = notice.buttons.iter().map(|b| (b.label, b.verdict)).collect();
    assert_eq!(
        verdicts,
        [("Approve", Verdict::AllowOnce), ("Always allow", Verdict::AllowAlways), ("Reject", Verdict::Reject)]
    );
    for button in &notice.buttons {
        let query = button.url.split_once('?').unwrap().1;
        let action = links.verify(query, Utc::now()).unwrap();
        assert_eq!((action.seq, action.verdict, &action.by), (9, button.verdict, &p("alice")));
    }
    assert!(approval_notice(&request, 9, &p("alice"), None, None, Utc::now()).buttons.is_empty(), "no links");
    assert!(!notice.text.contains("--rule"), "{}", notice.text);
    let suggested = tools::Suggestion { rule: "execute:rm -rf build".to_owned(), approved_before: 2 };
    let suggesting = approval_notice(&request, 9, &p("alice"), None, Some(&suggested), Utc::now());
    assert!(
        suggesting.text.ends_with(&format!(
            "\nallowed 2 times before: `ants tools approve dev-a/{} --rule` also adds the allow rule \
                 \"execute:rm -rf build\" to dev-a",
            request.id
        )),
        "{}",
        suggesting.text
    );

    let ntfy = Sink::Ntfy { url: NTFY.to_owned(), topic: "t".to_owned(), token: Some("tk".to_owned()) };
    let (url, bearer, body) = posted(ntfy.request(&notice));
    assert_eq!((url.as_str(), bearer.as_deref()), (NTFY, Some("tk")));
    assert_eq!((body["topic"].as_str(), body["priority"].as_i64()), (Some("t"), Some(4)));
    assert_eq!(
        body["actions"][2],
        json!({
            "action": "http", "label": "Reject", "url": notice.buttons[2].url, "method": "POST", "clear": true
        })
    );

    let slack = Sink::Slack { url: "https://hooks.slack.com/s".to_owned() };
    let (url, bearer, body) = posted(slack.request(&notice));
    assert_eq!((url.as_str(), bearer), ("https://hooks.slack.com/s", None));
    let section = body["blocks"][0]["text"]["text"].as_str().unwrap();
    assert!(section.starts_with("*dev-a asks to run a tool*\nrm -rf &lt;build&gt;"), "{section}");
    let buttons = &body["blocks"][1]["elements"];
    assert_eq!(
        (buttons[0]["url"].as_str(), buttons[0]["style"].as_str()),
        (Some(&*notice.buttons[0].url), Some("primary"))
    );
    assert_eq!(buttons[2]["style"], "danger");

    let telegram = Sink::Telegram { api: TELEGRAM.to_owned(), token: "1:x".to_owned(), chat: "42".to_owned() };
    let (url, _, body) = posted(telegram.request(&notice));
    assert_eq!(url, "https://api.telegram.org/bot1:x/sendMessage");
    assert_eq!((body["chat_id"].as_str(), body["disable_web_page_preview"].as_bool()), (Some("42"), Some(true)));
    assert_eq!(
        body["reply_markup"]["inline_keyboard"][0][1],
        json!({ "text": "Always allow", "url": notice.buttons[1].url })
    );
    assert!(body["text"].as_str().unwrap().starts_with("dev-a asks to run a tool\n\nrm -rf <build>"));

    let plain = Envelope::new(p("dev-a"), p("alice"), "hello".to_owned());
    assert_eq!(escalation_notice(&plain), None);
    let escalation = Escalation::new(p("dev-a"), plain.thread_id.clone(), ants_proto::EscalationReason::TurnBudget);
    let escalated = plain.with_escalation(Some(escalation));
    let notice = escalation_notice(&escalated).unwrap();
    assert_eq!(notice.title, "dev-a escalates: turn_budget on dev-a");
    assert!(notice.text.starts_with("hello\n\nthread ") && notice.text.ends_with("@dev-a TEXT"), "{}", notice.text);
    let (_, _, body) = posted(telegram.request(&notice));
    assert!(body.get("reply_markup").is_none());

    // An expired tool request's notice, on a thread of its own, is answered on the request's thread.
    let expired = Envelope::new(p("dev-a"), p("alice"), "[ants] Tool request expired".to_owned())
        .with_escalation(Some(Escalation::of_request(&request)));
    let notice = escalation_notice(&expired).unwrap();
    assert_eq!(notice.title, "dev-a's tool request expired unanswered");
    let answer = format!("thread {0}; answer with ants ask --thread {0} @dev-a TEXT", request.thread_id);
    assert!(notice.text.ends_with(&answer), "{}", notice.text);
}

#[test]
fn exec_and_http_sinks_get_the_notice_as_versioned_json() {
    let (request, links) = (request(), links());
    let notice = approval_notice(&request, 9, &p("alice"), Some(&links), None, Utc::now());
    let now = Utc::now();
    let json = notice_json(&notice, now);
    let expires = request.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    assert_eq!(
        json,
        json!({
            "version": 1,
            "kind": "approval",
            "id": request.id,
            "human": "alice",
            "ant": "dev-a",
            "thread": request.thread_id,
            "title": notice.title,
            "text": notice.text,
            "urgent": true,
            "sent_at": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "request": format!("dev-a/{}", request.id),
            "seq": 9,
            "tool": {
                "kind": "execute", "title": "rm -rf <build>", "locations": ["src/a.rs"],
                "input": "{\"command\":\"rm -rf build\"}"
            },
            "options": ["allow_once", "allow_always", "reject"],
            "labels": {},
            "expires_at": expires,
            "links": [
                { "verdict": "allow_once", "label": "Approve", "url": notice.buttons[0].url },
                { "verdict": "allow_always", "label": "Always allow", "url": notice.buttons[1].url },
                { "verdict": "reject", "label": "Reject", "url": notice.buttons[2].url },
            ],
        })
    );
    let unlinked = notice_json(&approval_notice(&request, 9, &p("alice"), None, None, now), now);
    assert_eq!(unlinked["links"], json!([]));

    let escalated = Envelope::new(p("dev-b"), p("alice"), "stuck".to_owned()).with_escalation(Some(Escalation::new(
        p("dev-a"),
        ThreadId::generate(),
        ants_proto::EscalationReason::TurnBudget,
    )));
    let json = notice_json(&escalation_notice(&escalated).unwrap(), now);
    let thread = &escalated.escalation.as_ref().unwrap().thread_id;
    assert_eq!(
        (&json["kind"], &json["id"], &json["human"], &json["ant"], &json["thread"]),
        (&json!("escalation"), &json!(escalated.id), &json!("alice"), &json!("dev-b"), &json!(thread))
    );
    assert_eq!((&json["reason"], &json["stuck"], &json["level"]), (&json!("turn_budget"), &json!("dev-a"), &json!(0)));
    assert!(json.get("expires_at").is_none() && json["links"] == json!([]), "{json}");

    let (first, second) = (Uuid::now_v7(), Uuid::now_v7());
    let about = About::Unserved { ant: p("dev-c"), messages: vec![first, second] };
    let waiting = Notice {
        about,
        human: p("alice"),
        title: String::new(),
        text: String::new(),
        buttons: Vec::new(),
        urgent: false,
    };
    let json = notice_json(&waiting, now);
    assert_eq!(
        (&json["kind"], &json["id"], &json["ant"], &json["messages"]),
        (&json!("unserved"), &json!(first), &json!("dev-c"), &json!([first, second]))
    );

    let (thread, message) = (ThreadId::generate(), Uuid::now_v7());
    let due = at("2025-01-06T09:00:00Z");
    let reminder = ants_proto::Reminder { at: due, due, message: Some(message), note: "ship it".into(), pushed: false };
    let reminded = reminder_notice(&p("alice"), &thread, &reminder, due);
    assert_eq!(reminded.title, "reminder: ship it");
    assert!(!reminded.text.contains("late"), "{}", reminded.text);
    let late = reminder_notice(&p("alice"), &thread, &reminder, due + chrono::TimeDelta::hours(3));
    assert!(late.text.ends_with("(Sent late: due 2025-01-06 09:00 UTC.)"), "{}", late.text);
    let json = notice_json(&reminded, now);
    assert_eq!(
        (&json["kind"], &json["thread"], &json["message"], &json["due"]),
        (&json!("reminder"), &json!(thread), &json!(message), &json!("2025-01-06T09:00:00Z"))
    );

    let http = Sink::Http { url: "https://hooks.example/a".to_owned(), secret: "0123456789abcdef-hook".into() };
    let Delivery::Post { url, headers, body } = http.request(&notice) else { panic!("not a POST") };
    assert_eq!(url, "https://hooks.example/a");
    let header = |name: &str| headers.iter().find(|(n, _)| *n == name).map(|(_, v)| v.clone()).unwrap();
    let timestamp = header("x-ants-timestamp");
    let signed = format!("{timestamp}.{}", String::from_utf8(body.clone()).unwrap());
    let key = hmac::Key::new(hmac::HMAC_SHA256, b"0123456789abcdef-hook");
    let expected = HEXLOWER.encode(hmac::sign(&key, signed.as_bytes()).as_ref());
    assert_eq!(header("x-ants-signature"), format!("sha256={expected}"));
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap()["kind"], "approval");
}

/// An endpoint on loopback answering 200 to a `POST` signed with `secret` and 401 to others, handing over the
/// JSON of each.
#[cfg(unix)]
async fn endpoint(secret: &'static str) -> (std::net::SocketAddr, tokio::sync::mpsc::UnboundedReceiver<Value>) {
    use hyper::body::Incoming;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let tx = tx.clone();
            let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                let tx = tx.clone();
                async move {
                    let header = |name: &str| {
                        request.headers().get(name).and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned()
                    };
                    let (timestamp, signature) = (header("x-ants-timestamp"), header("x-ants-signature"));
                    let body = request.into_body().collect().await.unwrap().to_bytes();
                    let mut signed = format!("{timestamp}.").into_bytes();
                    signed.extend_from_slice(&body);
                    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
                    let sig = signature.strip_prefix("sha256=").and_then(|s| HEXLOWER.decode(s.as_bytes()).ok());
                    let fresh = timestamp.parse::<i64>().is_ok_and(|t| (Utc::now().timestamp() - t).abs() < 300);
                    let valid = fresh && sig.is_some_and(|sig| hmac::verify(&key, &signed, &sig).is_ok());
                    let status = if valid { StatusCode::OK } else { StatusCode::UNAUTHORIZED };
                    if valid {
                        let _ = tx.send(serde_json::from_slice::<Value>(&body).unwrap());
                    }
                    let mut response = Response::new(Full::new(Bytes::from_static(b"bad signature")));
                    *response.status_mut() = status;
                    Ok::<_, std::convert::Infallible>(response)
                }
            });
            tokio::spawn(hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service));
        }
    });
    (addr, rx)
}

#[cfg(unix)]
#[tokio::test]
async fn exec_and_http_sinks_take_the_notice_and_each_failure_is_reported() {
    let dir = ants_test_support::scratch_dir("notify-exec-http").canonicalize().unwrap();
    let script = |name: &str, body: &str| {
        let path = dir.join(name);
        ants_test_support::executable(&path, &format!("#!/bin/sh\n{body}\n"));
        path.display().to_string()
    };
    let taken = dir.join("taken.json");
    let pid = dir.join("pid");
    let take = script("take", &format!("cat > {}", taken.display()));
    let fail = script("fail", "cat > /dev/null; echo failing >&2; exit 3");
    let hang = script("hang", &format!("echo $$ > {}\nexec sleep 30", pid.display()));
    let (good, mut posts) = endpoint("0123456789abcdef-hook").await;
    let (forged, _) = endpoint("another-secret-0123456789").await;
    let alice = human(&format!(
        "name = \"alice\"\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"{take}\"]\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"{fail}\", \"an argument\"]\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"{hang}\"]\ntimeout = \"1s\"\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"{}/missing\"]\n\
             [[notify]]\nkind = \"http\"\nurl = \"http://{good}/notices\"\nsecret_env = \"HOOK_SECRET\"\n\
             [[notify]]\nkind = \"http\"\nurl = \"http://{forged}/notices\"\nsecret_env = \"HOOK_SECRET\"\n",
        dir.display()
    ))
    .unwrap();

    let started = std::time::Instant::now();
    let results = test(&alice, "laptop").await;
    assert!(started.elapsed() < Duration::from_secs(5), "the sinks run at once, the hanging one for 1s");
    let shown: Vec<(&str, String)> = results
        .iter()
        .map(|(sink, sent)| (sink.as_str(), sent.as_ref().map_or_else(|e| format!("{e:#}"), |()| "ok".into())))
        .collect();
    let missing = format!("exec {}/missing", dir.display());
    assert_eq!(
        shown,
        [
            (&*format!("exec {take}"), "ok".to_owned()),
            (&*format!("exec {fail}"), "exit status: 3".to_owned()),
            (&*format!("exec {hang}"), "no exit within 1s: killed".to_owned()),
            (&*missing, "starting the command: No such file or directory (os error 2)".to_owned()),
            ("http 127.0.0.1", "ok".to_owned()),
            ("http 127.0.0.1", "answered 401 Unauthorized: bad signature".to_owned()),
        ]
    );
    let json: Value = serde_json::from_slice(&std::fs::read(&taken).unwrap()).unwrap();
    assert_eq!((&json["version"], &json["kind"], &json["human"]), (&json!(1), &json!("test"), &json!("alice")));
    assert!(json["text"].as_str().unwrap().contains("on laptop"), "{json}");
    let posted = tokio::time::timeout(Duration::from_secs(5), posts.recv()).await.unwrap().unwrap();
    assert_eq!((&posted["kind"], &posted["id"]), (&json!("test"), &json["id"]), "one notice, one id");
    // Killed and reaped: no process, not even a zombie, answers to its pid.
    let pid = std::fs::read_to_string(&pid).unwrap();
    let alive = std::process::Command::new("kill")
        .args(["-0", pid.trim()])
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap();
    assert!(!alive.success(), "the hanging command {} is gone", pid.trim());
    std::fs::remove_dir_all(&dir).unwrap();
}
