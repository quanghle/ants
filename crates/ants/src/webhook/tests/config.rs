//! Tests of the webhook config: destinations and mentions, signatures, humans and their links, private secret
//! files, templates, and the trail each event starts.

use super::*;

fn error(text: &str) -> String {
    format!("{:#}", parse(text).err().expect("an error"))
}

#[test]
fn the_config_names_one_destination_and_its_mentions_per_webhook() {
    let config = parse(&format!(
        "path = \"/in/\"\n{HOOK}ant = \"dev-a\"\ncreds = \"~/github.creds\"\nid = \"header:X-GitHub-Delivery\"\n\
             [[webhook]]\nname = \"ci\"\nauth = \"hmac-sha256\"\nsecret_env = \"HOOK_SECRET\"\nchannel = \"ops\"\n\
             broadcast = true\ncreds = \"ci.creds\"\n\
             [[webhook]]\nname = \"alerts\"\nauth = \"bearer\"\nsecret_env = \"HOOK_SECRET\"\nchannel = \"ops\"\n\
             mention = [\"dev-b\"]\nid = \"json:/alert/id\"\nallow = [\"read:*\", \"execute:gh run view*\"]\n"
    ))
    .unwrap();
    assert_eq!(config.path, "/in");
    let [github, ci, alerts] = &config.webhooks[..] else { panic!("three webhooks") };
    assert_eq!(github.destination, Destination::Ant(p("dev-a")));
    assert_eq!(github.creds.as_deref(), Some(Path::new("/home/q/github.creds")));
    assert_eq!(ci.creds.as_deref(), Some(Path::new("/etc/ants/ci.creds")));
    assert_matches!(github.id, IdSource::Header(ref h) if h == "x-github-delivery");
    assert_matches!(ci.id, IdSource::BodySha256);
    assert_matches!(alerts.id, IdSource::Json(ref p) if p == "/alert/id");
    assert_eq!((&github.mentions, &ci.mentions), (&Mentions::Nobody, &Mentions::Members));
    assert_eq!(alerts.mentions, Mentions::Listed(vec![p("dev-b")]));
    assert_eq!((&github.allow, &alerts.allow), (&None, &Some(vec!["read:*".into(), "execute:gh run view*".into()])));

    for (extra, why) in [
        ("", "exactly one of ant and channel"),
        ("ant = \"dev-a\"\nchannel = \"ops\"", "exactly one of ant and channel"),
        ("ant = \"dev-a\"\nmention = [\"dev-b\"]", "with a channel only"),
        ("ant = \"dev-a\"\nbroadcast = true", "with a channel only"),
        ("channel = \"ops\"\nmention = []\nbroadcast = true", "exclude each other"),
        ("ant = \"dev-a\"\ntext = \"{payload}\"", "unknown placeholder {payload}"),
        ("ant = \"dev-a\"\ntext = \"{json:a}\"", "starts with /"),
        ("ant = \"dev-a\"\ntext = \"{body\"", "without its }"),
        ("ant = \"dev-a\"\nid = \"body\"", "expected header:NAME"),
        ("ant = \"dev-a\"\nsignature_header = \"X-Sig\"", "hmac-sha256\" only"),
        ("ant = \"dev-a\"\nsecret_file = \"s\"", "exactly one of secret_env and secret_file"),
        ("ant = \"dev-a\"\nurl = \"x\"", "unknown field `url`"),
        ("ant = \"dev-a\"\nallow = [\"write\"]", "unknown tool kind"),
    ] {
        let e = error(&format!("{HOOK}{extra}\n"));
        assert!(e.contains("webhook github") || e.contains("unknown field"), "{e}");
        assert!(e.contains(why), "{extra}: {e}");
    }
    let unset = HOOK.replace("HOOK_SECRET", "NOT_SET");
    assert!(error(&format!("{unset}ant = \"dev-a\"\n")).contains("NOT_SET is not set"));
    assert!(error(&format!("{HOOK}ant = \"dev-a\"\n{HOOK}ant = \"dev-b\"\n")).contains("listed twice"));
    assert!(error("nats = \"nats://hub:4222\"\n").contains("nothing to serve"));
    let err = error(&format!("nats = \"nats://dev:secret@hub\"\n{HOOK}ant = \"dev-a\"\n"));
    assert!(err.contains("NATS URL nats://***@hub") && !err.contains("secret"), "{err}");
}

#[test]
fn standard_webhooks_signatures_are_checked_with_the_signing_secret_within_the_tolerance() {
    const SIGNED: &str = "[[webhook]]\nname = \"gitlab\"\nauth = \"standard-webhooks\"\nsecret_env = \"SIGNING_SECRET\"\n\
                          ant = \"dev-a\"\n";
    let config = parse(SIGNED).unwrap();
    let hook = &config.webhooks[0];
    assert_matches!(hook.id, IdSource::Header(ref h) if h == "webhook-id");
    assert!(matches!(hook.auth, Auth::Standard { tolerance, .. } if tolerance == Duration::from_mins(5)));
    let tolerance = parse(&format!("{SIGNED}tolerance = \"90s\"\nid = \"body-sha256\"\n")).unwrap();
    assert!(
        matches!(tolerance.webhooks[0].auth, Auth::Standard { tolerance, .. } if tolerance == Duration::from_secs(90))
    );
    assert_matches!(tolerance.webhooks[0].id, IdSource::BodySha256);

    let body = br#"{"test": 2432232314}"#;
    let signature = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=";
    let headers = |signatures: &str| {
        let mut headers = HeaderMap::new();
        headers.insert(WEBHOOK_ID, HeaderValue::from_static("msg_p5jXN8AQM9LWM0D4loKWxJek"));
        headers.insert(WEBHOOK_TIMESTAMP, HeaderValue::from_static("1614265330"));
        headers.insert(WEBHOOK_SIGNATURE, signatures.parse().unwrap());
        headers
    };
    assert!(hook.authorized(&headers(signature), body));
    assert!(hook.authorized(&headers(&format!("v1a,AAAA v1,bm90IGl0 {signature}")), body), "any v1 one matches");
    assert!(!hook.authorized(&headers(&signature.replace("v1,", "v2,")), body));
    assert!(!hook.authorized(&headers("v1,bm90IGl0 v1,!!"), body));
    assert!(!hook.authorized(&headers(signature), br#"{"test": 2432232315}"#));
    let mut moved = headers(signature);
    moved.insert(WEBHOOK_TIMESTAMP, HeaderValue::from_static("1614265331"));
    assert!(!hook.authorized(&moved, body), "the timestamp is signed");
    let mut unsigned = headers(signature);
    unsigned.remove(WEBHOOK_SIGNATURE);
    assert!(!hook.authorized(&unsigned, body));

    let sent = SystemTime::UNIX_EPOCH + Duration::from_secs(1_614_265_330);
    let five = Duration::from_mins(5);
    assert!(recent(&headers(signature), five, sent + five) && recent(&headers(signature), five, sent - five));
    assert!(!recent(&headers(signature), five, sent + five + Duration::from_secs(1)), "too old");
    assert!(!recent(&headers(signature), five, sent - five - Duration::from_secs(1)), "too far ahead");
    let mut undated = headers(signature);
    undated.remove(WEBHOOK_TIMESTAMP);
    assert!(!recent(&undated, five, sent));
    assert!(!hook.authorized_before_body(&headers(signature)), "a stale request is refused before its body");
    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs().to_string();
    let mut fresh = headers(signature);
    fresh.insert(WEBHOOK_TIMESTAMP, now.parse().unwrap());
    assert!(hook.authorized_before_body(&fresh));
    fresh.remove(WEBHOOK_ID);
    assert!(!hook.authorized_before_body(&fresh));

    for (extra, why) in [
        ("tolerance = \"0\"", "from 1s to 1d"),
        ("tolerance = \"2d\"", "from 1s to 1d"),
        ("tolerance = \"5\"", "a number without a unit"),
    ] {
        let e = error(&format!("{SIGNED}{extra}\n"));
        assert!(e.contains(why), "{extra}: {e}");
    }
    let e = error(&SIGNED.replace("SIGNING_SECRET", "HOOK_SECRET"));
    assert!(e.contains("no Standard Webhooks signing secret"), "{e}");
    let e = error(&format!("{HOOK}ant = \"dev-a\"\ntolerance = \"1m\"\n"));
    assert!(e.contains("standard-webhooks\" only"), "{e}");
}

#[test]
fn humans_are_notified_and_answer_links_with_public_url_and_the_action_secret() {
    const ALICE: &str = "[[human]]\nname = \"alice\"\n[[human.notify]]\nkind = \"ntfy\"\ntopic = \"a\"\n";
    let env = |name: &str| match name {
        "ACTION_SECRET" => Some("0123456789abcdef-secret\n".to_owned()),
        "HOOK_SECRET" => Some("s3cret".to_owned()),
        _ => None,
    };
    let parse = |text: &str| Config::parse(text, Path::new("/etc/ants/webhooks.toml"), Path::new("/home/q"), env);
    let error = |text: &str| format!("{:#}", parse(text).err().expect("an error"));
    let config = parse(ALICE).unwrap();
    assert_eq!((config.webhooks.len(), config.humans.len(), config.links.is_some()), (0, 1, false));
    let linked = format!("public_url = \"https://hub.example/hooks\"\naction_secret_env = \"ACTION_SECRET\"\n{ALICE}");
    assert!(parse(&linked).unwrap().links.is_some());

    let url = "public_url = \"https://hub.example/hooks\"\n";
    let secret = "action_secret_env = \"ACTION_SECRET\"\n";
    for (text, why) in [
        (format!("{url}{ALICE}"), "public_url needs action_secret_file"),
        (format!("{secret}{ALICE}"), "the action secret needs public_url"),
        (format!("{url}{secret}{HOOK}ant = \"dev-a\"\n"), "go with [[human]] entries"),
        (format!("{url}{secret}action_secret_file = \"s\"\n{ALICE}"), "at most one of action_secret_env"),
        (format!("public_url = \"https://hub.example/hooks?x=1\"\n{secret}{ALICE}"), "no user, password or query"),
        (format!("{ALICE}{ALICE}"), "human alice is listed twice"),
        (format!("{HOOK}ant = \"dev-a\"\n{}", ALICE.replace("alice", "github")), "both a webhook and a human"),
        ("[[human]]\nname = \"alice\"\n".to_owned(), "nowhere to send"),
    ] {
        let e = error(&text);
        assert!(e.contains(why), "{text}: {e}");
    }
}

#[test]
fn a_secret_file_must_be_private() {
    let dir = ants_test_support::scratch_dir("webhook-secret");
    let secret = dir.join("github.secret");
    std::fs::write(&secret, "s3cret\n").unwrap();
    let text =
        format!("[[webhook]]\nname = \"github\"\nauth = \"bearer\"\nsecret_file = {secret:?}\nant = \"dev-a\"\n");
    let path = dir.join("webhooks.toml");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o644)).unwrap();
        let e = Config::parse(&text, &path, &dir, |_| None).err().unwrap();
        assert!(format!("{e:#}").contains("readable by others"), "{e:#}");
        std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let config = Config::parse(&text, &path, &dir, |_| None).unwrap();
    assert!(matches!(&config.webhooks[0].auth, Auth::Bearer(token) if token == "s3cret"), "trimmed");
}

#[test]
fn the_text_comes_from_the_template_and_the_event_id_from_its_source() {
    let config = parse(&format!(
        "{HOOK}ant = \"dev-a\"\nid = \"json:/id\"\n\
             text = \"{{{{{{webhook}}}}}} {{header:X-Event}} {{json:/repo/name}} {{json:/n}} {{json:/none}}|{{id}}\\n{{body}}\"\n"
    ))
    .unwrap();
    let hook = &config.webhooks[0];
    let mut headers = HeaderMap::new();
    headers.insert("x-event", HeaderValue::from_static("push"));
    let body = br#"{"id":42,"repo":{"name":"ants"},"n":[1]}"#;
    let event = hook.event(&headers, body).ok().unwrap();
    assert_eq!(event.text, format!("{{github}} push ants [1] |42\n{}", std::str::from_utf8(body).unwrap()));
    assert_eq!(event.hash.len(), ID_HASH_LEN);
    assert_eq!(hook.event(&headers, br#"{"id":42}"#).ok().unwrap().hash, event.hash, "the same id");

    let status = |body: &[u8]| hook.event(&headers, body).err().unwrap().0;
    assert_eq!(status(br#"{"repo":{}}"#), StatusCode::BAD_REQUEST, "no id");
    assert_eq!(status(b"not json"), StatusCode::BAD_REQUEST);
    let plain = parse(&format!("{HOOK}ant = \"dev-a\"\n")).unwrap();
    let event = plain.webhooks[0].event(&headers, b"ping").ok().unwrap();
    let id = HEXLOWER.encode(digest::digest(&digest::SHA256, b"ping").as_ref());
    assert_eq!(event.text, format!("[webhook github] {id}\n\nping"));
    assert_eq!(plain.webhooks[0].event(&headers, b"\xff").err().unwrap().0, StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[test]
fn a_webhook_naming_a_trail_starts_it_with_its_args_under_its_limits() {
    let config = parse(&format!(
        "{HOOK}ant = \"dev-a\"\ntrail = \"triage\"\nargs = {{ repo = \"app\" }}\nlimits = {{ steps = 100, wall = \"2d\" }}\n"
    ))
    .unwrap();
    let start = config.webhooks[0].trail.as_ref().expect("a trail");
    assert_eq!(start.trail.as_str(), "triage");
    assert_eq!(start.args, serde_json::json!({"repo": "app"}));
    let asked = ants_proto::TrailLimits { steps: 100, wall_secs: 2 * 86400, ..ants_proto::TrailLimits::DEFAULT };
    assert_eq!(start.limits, asked, "the limits given, the others their defaults");
    let plain = parse(&format!("{HOOK}ant = \"dev-a\"\ntrail = \"triage\"\n")).unwrap();
    let start = plain.webhooks[0].trail.as_ref().unwrap();
    assert_eq!((&start.args, start.limits), (&serde_json::json!({}), ants_proto::TrailLimits::DEFAULT));
    assert!(parse(&format!("{HOOK}ant = \"dev-a\"\n")).unwrap().webhooks[0].trail.is_none());

    for (extra, why) in [
        ("ant = \"dev-a\"\nargs = { a = 1 }", "args go with trail only"),
        ("ant = \"dev-a\"\nlimits = { steps = 9 }", "limits go with trail only"),
        ("channel = \"ops\"\ntrail = \"triage\"", "names the ant conducting its runs, not a channel"),
        ("ant = \"dev-a\"\ntrail = \"triage\"\nallow = [\"read\"]", "allow goes without trail"),
        ("ant = \"dev-a\"\ntrail = \"triage\"\nargs = { text = \"t\" }", "args.text is the event's text"),
        ("ant = \"dev-a\"\ntrail = \"Triage!\"", "trail"),
        (
            "ant = \"dev-a\"\ntrail = \"triage\"\nlimits = { steps = 501 }",
            "limits: steps is over the most a run may take, 500",
        ),
        (
            "ant = \"dev-a\"\ntrail = \"triage\"\nlimits = { wall = \"8d\" }",
            "limits: wall is over the most a run may take, 7d",
        ),
        ("ant = \"dev-a\"\ntrail = \"triage\"\nlimits = { stepz = 9 }", "unknown field `stepz`"),
    ] {
        let e = error(&format!("{HOOK}{extra}\n"));
        assert!(e.contains(why), "{extra}: {e}");
    }
}
