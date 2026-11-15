use std::assert_matches;

use ants_proto::{Escalation, ThreadId, ToolCall};

use super::*;

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

fn human(text: &str) -> Result<Human> {
    let entry: HumanEntry = toml::from_str(text)?;
    let env = |name: &str| match name {
        "SLACK_URL" => Some("https://hooks.slack.com/services/T0/B0/xyz\n".to_owned()),
        "BOT_TOKEN" => Some("123:abc".to_owned()),
        "HOOK_SECRET" => Some("0123456789abcdef-hook".to_owned()),
        _ => None,
    };
    Human::from_entry(entry, None, &|p: &Path| Path::new("/etc/ants").join(p), &env)
}

fn error(text: &str) -> String {
    format!("{:#}", human(text).err().expect("an error"))
}

const NTFY_SINK: &str = "[[notify]]\nkind = \"ntfy\"\ntopic = \"alice-ants\"\n";

#[test]
fn a_human_has_sinks_and_what_to_be_notified_of() {
    let phone = DeviceId::generate();
    let alice = human(&format!(
        "name = \"alice\"\ncreds = \"alice.creds\"\nants = [\"dev-a\"]\n{NTFY_SINK}\
             [[notify]]\nkind = \"slack\"\nsecret_env = \"SLACK_URL\"\n\
             [[notify]]\nkind = \"telegram\"\nchat = \"-10042\"\nsecret_env = \"BOT_TOKEN\"\ndevice = \"{phone}\"\n"
    ))
    .unwrap();
    assert_eq!(alice.creds.as_deref(), Some(Path::new("/etc/ants/alice.creds")));
    assert_eq!(alice.subjects(), ["ants.approvals.*", "ants.inbox.alice.*"]);
    assert!(alice.wants_request_of(&p("dev-a")) && !alice.wants_request_of(&p("dev-b")));
    let ntfy = Sink::Ntfy { url: NTFY.to_owned(), topic: "alice-ants".to_owned(), token: None };
    let slack = Sink::Slack { url: "https://hooks.slack.com/services/T0/B0/xyz".to_owned() };
    let telegram = Sink::Telegram { api: TELEGRAM.to_owned(), token: "123:abc".to_owned(), chat: "-10042".into() };
    let sinks: Vec<_> = alice.sinks.iter().map(|t| &t.sink).collect();
    assert_eq!(sinks, [&ntfy, &slack, &telegram]);
    assert_eq!(alice.devices().collect::<Vec<_>>(), [phone], "telegram is the phone's push target");
    assert_eq!(
        format!("{:?}", alice.sinks),
        format!("[ntfy topic alice-ants, slack, telegram chat -10042 of device {phone}]")
    );

    let quiet = human(&format!("name = \"bob\"\napprovals = false\n{NTFY_SINK}url = \"http://ntfy.lan/\"\n")).unwrap();
    assert_eq!(quiet.subjects(), ["ants.inbox.bob.*"]);
    assert!(!quiet.wants_request_of(&p("dev-a")));
    assert_matches!(&quiet.sinks[0].sink, Sink::Ntfy { url, .. } if url == "http://ntfy.lan");
    let every = human(&format!("name = \"bob\"\nescalations = false\n{NTFY_SINK}")).unwrap();
    assert_eq!(every.subjects(), ["ants.approvals.*"]);
    assert!(every.wants_request_of(&p("dev-b")));

    for (text, why) in [
        ("name = \"bob\"\n".to_owned(), "nowhere to send"),
        (format!("name = \"bob\"\napprovals = false\nescalations = false\n{NTFY_SINK}"), "nothing to notify"),
        (format!("name = \"bob\"\napprovals = false\nants = [\"dev-a\"]\n{NTFY_SINK}"), "approvals = true only"),
        (format!("name = \"bob\"\nants = []\n{NTFY_SINK}"), "give the ants"),
        ("name = \"bob\"\n[[notify]]\nkind = \"ntfy\"\n".to_owned(), "ntfy needs topic"),
        (format!("name = \"bob\"\n{NTFY_SINK}chat = \"1\"\n"), "chat does not go with kind = Ntfy"),
        ("name = \"bob\"\n[[notify]]\nkind = \"ntfy\"\ntopic = \"a/b\"\n".to_owned(), "letters, digits"),
        (format!("name = \"bob\"\n{NTFY_SINK}url = \"ftp://x\"\n"), "expected https://host"),
        (format!("name = \"bob\"\n{NTFY_SINK}url = \"https://u:p@x\"\n"), "no user, password or query"),
        (format!("name = \"bob\"\n{NTFY_SINK}secret_env = \"UNSET\"\n"), "UNSET is not set"),
        (format!("name = \"bob\"\n{NTFY_SINK}device = \"phone\"\n"), "device"),
        ("name = \"bob\"\n[[notify]]\nkind = \"slack\"\n".to_owned(), "slack needs its incoming webhook URL"),
        ("name = \"bob\"\n[[notify]]\nkind = \"slack\"\nurl = \"https://hooks.slack.com/x\"\n".to_owned(), "not url"),
        (
            "name = \"bob\"\n[[notify]]\nkind = \"slack\"\nsecret_env = \"BOT_TOKEN\"\n".to_owned(),
            "no incoming webhook",
        ),
        ("name = \"bob\"\n[[notify]]\nkind = \"telegram\"\nsecret_env = \"BOT_TOKEN\"\n".to_owned(), "needs chat"),
        ("name = \"bob\"\n[[notify]]\nkind = \"telegram\"\nchat = \"1\"\n".to_owned(), "needs the bot token"),
        ("name = \"bob\"\n[[notify]]\nkind = \"email\"\n".to_owned(), "unknown variant"),
    ] {
        assert!(error(&text).contains(why), "{text}: {}", error(&text));
    }
    let leaked = error("name = \"bob\"\n[[notify]]\nkind = \"slack\"\nsecret_env = \"BOT_TOKEN\"\n");
    assert!(!leaked.contains("123:abc"), "{leaked}");
}

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text).unwrap().with_timezone(&Utc)
}

#[test]
fn quiet_hours_read_the_wall_clock_of_their_time_zone_across_midnight() {
    let night = human(&format!("name = \"bob\"\nquiet = \"22:00-07:00\"\n{NTFY_SINK}")).unwrap();
    let quiet = night.quiet.as_ref().unwrap();
    for (time, held) in [("21:59", false), ("22:00", true), ("23:59", true), ("00:00", true), ("06:59", true)] {
        assert_eq!(quiet.contains(at(&format!("2026-03-10T{time}:30Z"))), held, "{time} UTC");
    }
    assert!(!quiet.contains(at("2026-03-10T07:00:00Z")), "the end is not in the window");
    let day = QuietHours::parse("09:30-17:00", TimeZone::UTC).unwrap();
    assert!(day.contains(at("2026-03-10T09:30:00Z")) && !day.contains(at("2026-03-10T17:00:00Z")));
    assert!(!day.contains(at("2026-03-10T03:00:00Z")));

    // Tokyo is UTC+9 all year; New York moves to UTC-4 on 2026-03-08.
    let tokyo =
        human(&format!("name = \"bob\"\nquiet = \"22:00-07:00\"\ntimezone = \"Asia/Tokyo\"\n{NTFY_SINK}")).unwrap();
    let tokyo = tokyo.quiet.as_ref().unwrap();
    assert!(tokyo.contains(at("2026-03-10T14:00:00Z")), "23:00 in Tokyo");
    assert!(!tokyo.contains(at("2026-03-10T23:00:00Z")), "08:00 in Tokyo");
    let entry: HumanEntry = toml::from_str(&format!("name = \"bob\"\nquiet = \"22:00-07:00\"\n{NTFY_SINK}")).unwrap();
    let york = ants_daemon::schedule::time_zone("America/New_York").unwrap();
    let york = Human::from_entry(entry, Some(&york), &Path::to_path_buf, &|_: &str| None).unwrap();
    assert_eq!(
        york.quiet_hours(at("2026-03-07T03:30:00Z")).unwrap(),
        "quiet hours 22:00-07:00 America/New_York (on now); quiet hours and a snooze let nothing through",
        "22:30 EST, in the file's time zone"
    );
    assert!(!york.quiet.as_ref().unwrap().contains(at("2026-03-09T11:30:00Z")), "07:30 EDT");
    assert!(york.quiet.as_ref().unwrap().contains(at("2026-03-09T10:30:00Z")), "06:30 EDT");

    for (text, why) in [
        ("quiet = \"22:00\"", "expected HH:MM-HH:MM"),
        ("quiet = \"24:00-07:00\"", "expected HH:MM-HH:MM"),
        ("quiet = \"7:00-9:00\"", "expected HH:MM-HH:MM"),
        ("quiet = \"08:00-08:00\"", "starts where it ends"),
        ("timezone = \"Asia/Tokyo\"", "timezone goes with quiet only"),
        ("quiet = \"22:00-07:00\"\ntimezone = \"Mars/Base\"", "unknown time zone"),
        ("bypass = [\"completions\"]", "unknown variant"),
    ] {
        let text = format!("name = \"bob\"\n{text}\n{NTFY_SINK}");
        assert!(error(&text).contains(why), "{text}: {}", error(&text));
    }
    let only =
        human(&format!("name = \"bob\"\napprovals = false\nescalations = false\ncompletions = true\n{NTFY_SINK}"));
    assert_eq!(only.unwrap().subjects(), ["ants.inbox.bob.*"], "completions read the inbox");
    let none = format!("name = \"bob\"\napprovals = false\nescalations = false\n{NTFY_SINK}");
    assert!(error(&none).contains("nothing to notify"), "{}", error(&none));
}

#[test]
fn a_mute_holds_back_everything_of_its_thread_and_a_snooze_or_quiet_hours_what_bypass_does_not_list() {
    let now = at("2026-03-10T12:00:00Z");
    let bob = human(&format!("name = \"bob\"\nbypass = [\"approvals\"]\n{NTFY_SINK}")).unwrap();
    let asked = request();
    let approval = About::Approval { request: Box::new(asked.clone()), seq: 1 };
    let other = ThreadId::generate();
    let escalation = About::Escalation {
        id: Uuid::now_v7(),
        from: p("dev-a"),
        escalation: Box::new(Escalation::new(p("dev-a"), other.clone(), ants_proto::EscalationReason::Asked)),
    };
    let completion = About::Completion { id: Uuid::now_v7(), from: p("dev-a"), thread: other };
    let mut prefs = NotifyPrefs::default();
    for about in [&approval, &escalation, &completion] {
        assert_eq!(held(&bob, about, &prefs, now), None, "nothing held");
    }

    prefs.snoozed_until = Some(now + TimeDelta::minutes(5));
    assert_eq!(held(&bob, &approval, &prefs, now), None, "approvals bypass");
    assert_eq!(held(&bob, &escalation, &prefs, now), Some("snoozed"));
    assert_eq!(held(&bob, &completion, &prefs, now), Some("snoozed"));
    let later = now + TimeDelta::minutes(5);
    assert_eq!(held(&bob, &completion, &prefs, later), None, "the snooze ended on its own");
    prefs.prune(later);
    assert_eq!(prefs, NotifyPrefs::default());

    prefs.muted.insert(asked.thread_id, now + TimeDelta::hours(1));
    assert_eq!(held(&bob, &approval, &prefs, now), Some("muted thread"), "whatever bypass says");
    assert_eq!(held(&bob, &completion, &prefs, now), None, "another thread");
    assert_eq!(held(&bob, &approval, &prefs, now + TimeDelta::hours(1)), None, "the mute ended");

    let night = human(&format!("name = \"bob\"\nquiet = \"11:00-13:00\"\nbypass = [\"escalations\"]\n{NTFY_SINK}"));
    let night = night.unwrap();
    let prefs = NotifyPrefs::default();
    assert_eq!(held(&night, &approval, &prefs, now), Some("quiet hours"));
    assert_eq!(held(&night, &escalation, &prefs, now), None, "escalations bypass");
    assert_eq!(held(&night, &completion, &prefs, now), Some("quiet hours"));
    assert_eq!(held(&night, &completion, &prefs, at("2026-03-10T13:00:00Z")), None);
}

#[test]
fn a_notice_held_from_a_human_away_is_relayed_to_its_stand_in_who_is_told_whom_to_answer() {
    let alice = human(&format!("name = \"alice\"\nstand_in = \"bob\"\n{NTFY_SINK}")).unwrap();
    assert!(error(&format!("name = \"bob\"\nstand_in = \"bob\"\n{NTFY_SINK}")).contains("the human itself"));
    let is_human = |user: &UserId| ["alice", "bob", "carol"].contains(&user.as_str());
    let now = Utc::now();
    let asked = request().for_human(Some(p("alice")), None);
    let notice = approval_notice(&asked, 9, &p("alice"), None, None, now);

    let relay = relayed(&alice, &notice, "quiet hours", is_human).unwrap();
    assert_eq!((&relay.from, &relay.to, &relay.thread_id), (&p("alice"), &p("bob"), &asked.thread_id));
    assert!(
        relay.text.starts_with("[ants] Relayed to bob, alice's stand-in, while alice is away (quiet hours)"),
        "{}",
        relay.text
    );
    assert!(relay.text.contains(&format!("ants tools approve dev-a/{}", asked.id)), "{}", relay.text);
    let escalation = relay.escalation.as_deref().unwrap();
    assert_eq!(escalation.away, Some(Away { human: p("alice"), ant: p("dev-a") }));
    assert_eq!((escalation.reason, escalation.request_id), (EscalationReason::Approval, Some(asked.id)));

    let shown = escalation_notice(&relay).unwrap();
    assert_eq!(
        (shown.title.as_str(), &shown.human, shown.urgent),
        ("for alice (away): dev-a asks to run a tool", &p("bob"), true)
    );
    let json = notice_json(&shown, now);
    assert_eq!((&json["ant"], &json["stand_in_for"]), (&json!("dev-a"), &json!("alice")));
    let bob = human(&format!("name = \"bob\"\n{NTFY_SINK}")).unwrap();
    let carol = human(&format!("name = \"carol\"\nants = [\"dev-b\"]\n{NTFY_SINK}")).unwrap();
    assert!(bob.gets_relayed_request(&shown.about), "bob is notified of dev-a's requests anyway");
    assert!(!carol.gets_relayed_request(&shown.about));
    assert!(relayed(&bob, &shown, "quiet hours", is_human).is_none(), "a relay is relayed no further");

    let named = asked.clone().for_human(Some(p("alice")), Some(p("carol")));
    let notice = approval_notice(&named, 9, &p("alice"), None, None, now);
    assert_eq!(relayed(&alice, &notice, "snoozed", is_human).unwrap().to, p("carol"), "the ant's stand-in");
    let named = asked.clone().for_human(Some(p("alice")), Some(p("dev-b")));
    let notice = approval_notice(&named, 9, &p("alice"), None, None, now);
    assert_eq!(relayed(&alice, &notice, "snoozed", is_human).unwrap().to, p("bob"), "an ant stands in for no one");
    assert!(relayed(&alice, &notice, "muted thread", is_human).is_none());
    assert!(relayed(&bob, &notice, "snoozed", is_human).is_none(), "the request waits for another human");
    let unnamed = human(&format!("name = \"alice\"\n{NTFY_SINK}")).unwrap();
    let notice = approval_notice(&asked, 9, &p("alice"), None, None, now);
    assert!(relayed(&unnamed, &notice, "snoozed", is_human).is_none(), "no stand-in");

    let thread = ThreadId::generate();
    let stuck = Escalation {
        stand_in: Some(p("carol")),
        ..Escalation::new(p("dev-a"), thread.clone(), EscalationReason::Asked)
    };
    let escalated = Envelope::new(p("dev-a"), p("alice"), "which branch?").with_escalation(Some(stuck));
    let relay = relayed(&alice, &escalation_notice(&escalated).unwrap(), "snoozed", is_human).unwrap();
    assert_eq!((&relay.to, &relay.thread_id), (&p("carol"), &thread));
    let shown = escalation_notice(&relay).unwrap();
    assert_eq!(shown.title, "for alice (away): dev-a escalates: asked on dev-a");
    assert!(shown.text.contains(&format!("ants ask --thread {thread} @dev-a TEXT")), "{}", shown.text);
    let forged = Envelope { from: p("dev-b"), ..relay };
    assert_eq!(escalation_notice(&forged).unwrap().title, "dev-b escalates: asked on dev-a", "relayed by no one away");
    let expired = escalated.with_escalation(Some(Escalation::of_request(&asked)));
    assert!(relayed(&alice, &escalation_notice(&expired).unwrap(), "snoozed", is_human).is_none(), "nothing to answer");
}

#[test]
fn a_reply_is_a_completion_and_nothing_else_is() {
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let asked = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    let done = asked.reply(dev_a.clone(), "built", ants_proto::Role::Human).unwrap();
    let notice = completion_notice(&done).unwrap();
    assert_eq!((notice.title.as_str(), &notice.human, notice.urgent), ("dev-a replied", &alice, false));
    let thread = &asked.thread_id;
    assert_eq!(notice.text, format!("built\n\nthread {thread}; answer with ants ask --thread {thread} @dev-a TEXT"));
    let json = notice_json(&notice, at("2026-03-10T12:00:00Z"));
    assert_eq!(
        (&json["version"], &json["kind"], &json["id"], &json["ant"], &json["thread"]),
        (&json!(1), &json!("completion"), &json!(done.id), &json!("dev-a"), &json!(thread))
    );
    assert_eq!(json["links"], json!([]));

    let started = Envelope::new(dev_a.clone(), alice, "hello");
    assert!(completion_notice(&started).is_none(), "no reply");
    let escalated =
        done.clone().with_escalation(Some(Escalation::new(dev_a, thread.clone(), ants_proto::EscalationReason::Asked)));
    assert!(completion_notice(&escalated).is_none(), "an escalation's notice is its own");
    let hooked = Envelope { webhook: true, ..done };
    assert!(completion_notice(&hooked).is_none());
}

/// Where a delivery `POST`s, its bearer token and its JSON body.
fn posted(delivery: Delivery) -> (String, Option<String>, Value) {
    let Delivery::Post { url, headers, body } = delivery else { panic!("not a POST") };
    let bearer = headers.iter().find(|(name, _)| *name == "authorization");
    let bearer = bearer.map(|(_, value)| value.strip_prefix("Bearer ").unwrap().to_owned());
    (url, bearer, serde_json::from_slice(&body).unwrap())
}

fn links() -> Links {
    Links::new("https://hub.example/hooks/", "0123456789abcdef-secret").unwrap()
}

fn action() -> Action {
    Action {
        ant: p("dev-a"),
        id: Uuid::now_v7(),
        seq: 7,
        verdict: Verdict::AllowOnce,
        by: p("alice"),
        expires: Utc::now().timestamp() + 60,
    }
}

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

fn request() -> ApprovalRequest {
    let tool = ToolCall {
        kind: "execute".to_owned(),
        title: "rm -rf <build>".to_owned(),
        locations: vec!["src/a.rs".to_owned()],
        input: Some("{\"command\":\"rm -rf build\"}".to_owned()),
    };
    let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
    ApprovalRequest::new(p("dev-a"), ThreadId::generate(), tool, options, Duration::from_secs(600))
}

#[test]
fn notices_are_redacted() {
    let secret = "ghp_0123456789abcdefghijABCDEFGHIJ";
    let mut request = request();
    request.tool.input = Some(format!("{{\"command\":\"git push https://x:{secret}@github.com/a/b\"}}"));
    let notice = approval_notice(&request, 9, &p("alice"), None, None, Utc::now());
    let json = notice_json(&notice, Utc::now());
    assert!(notice.text.contains("x:[redacted]@github.com"), "{}", notice.text);
    for shown in [notice.text, json.to_string(), details(&request)] {
        assert!(!shown.contains(secret), "{shown}");
    }

    let asked = Envelope::new(p("alice"), p("dev-a"), "build it");
    let done = asked.reply(p("dev-a"), format!("token {secret}"), ants_proto::Role::Human).unwrap();
    assert!(completion_notice(&done).unwrap().text.starts_with("token [redacted]\n"));
    let escalated =
        done.with_escalation(Some(Escalation::new(p("dev-a"), asked.thread_id, ants_proto::EscalationReason::Asked)));
    assert!(escalation_notice(&escalated).unwrap().text.starts_with("token [redacted]\n"));
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
fn texts_are_cut_and_escaped() {
    assert_eq!(cut("héllo", 3), "hél…");
    assert_eq!(cut("hé", 3), "hé");
    assert_eq!(html("<a href=\"x\">&'"), "&lt;a href=&quot;x&quot;&gt;&amp;&#39;");
    let old = Uuid::from_u128((1_000_000_000_u128 << 80) | (7 << 76) | (2 << 62));
    assert!(!is_recent(&old, Utc::now()) && is_recent(&Uuid::now_v7(), Utc::now()));
}

#[test]
fn an_old_style_config_sends_what_it_sent_before() {
    // The [[human]] example of the README before exec and http sinks, as it stood.
    let alice = human(
        "name = \"alice\"\ncreds = \"~/.config/ants/auth/alice.creds\"\nants = [\"dev-a\", \"dev-b\"]\n\
             approvals = true\nescalations = true\n\
             [[notify]]\nkind = \"ntfy\"\ntopic = \"alice-ants-7f3k\"\nsecret_env = \"BOT_TOKEN\"\n\
             [[notify]]\nkind = \"slack\"\nsecret_env = \"SLACK_URL\"\n\
             [[notify]]\nkind = \"telegram\"\nchat = \"123456789\"\nsecret_env = \"BOT_TOKEN\"\n\
             [[notify]]\nkind = \"ntfy\"\ntopic = \"alice-laptop-9c2e\"\ndevice = \"01968f7e-0000-7000-8000-000000000000\"\n",
    )
    .unwrap();
    assert!(alice.sinks.iter().all(|t| t.timeout == SEND_TIMEOUT));
    let notice = Notice {
        about: About::Test { id: Uuid::now_v7() },
        human: p("alice"),
        title: "dev-a asks".to_owned(),
        text: "a <b> & c".to_owned(),
        buttons: vec![Button { label: "Reject", url: "https://hub.example/x".to_owned(), verdict: Verdict::Reject }],
        urgent: true,
    };
    let sent: Vec<_> = alice.sinks.iter().map(|t| posted(t.sink.request(&notice))).collect();
    let reject =
        json!({ "action": "http", "label": "Reject", "url": "https://hub.example/x", "method": "POST", "clear": true });
    assert_eq!(
        sent,
        [
            (
                "https://ntfy.sh".to_owned(),
                Some("123:abc".to_owned()),
                json!({
                    "topic": "alice-ants-7f3k", "title": "dev-a asks", "message": "a <b> & c", "tags": ["ant"],
                    "priority": 4, "actions": [reject]
                })
            ),
            (
                "https://hooks.slack.com/services/T0/B0/xyz".to_owned(),
                None,
                json!({
                    "text": "dev-a asks",
                    "blocks": [
                        { "type": "section", "text": { "type": "mrkdwn", "text": "*dev-a asks*\na &lt;b&gt; &amp; c" } },
                        { "type": "actions", "elements": [{
                            "type": "button", "text": { "type": "plain_text", "text": "Reject" },
                            "url": "https://hub.example/x", "action_id": "ants-reject", "style": "danger"
                        }] }
                    ]
                })
            ),
            (
                "https://api.telegram.org/bot123:abc/sendMessage".to_owned(),
                None,
                json!({
                    "chat_id": "123456789", "text": "dev-a asks\n\na <b> & c", "disable_web_page_preview": true,
                    "reply_markup": { "inline_keyboard": [[{ "text": "Reject", "url": "https://hub.example/x" }]] }
                })
            ),
            (
                "https://ntfy.sh".to_owned(),
                None,
                json!({
                    "topic": "alice-laptop-9c2e", "title": "dev-a asks", "message": "a <b> & c", "tags": ["ant"],
                    "priority": 4, "actions": [reject]
                })
            ),
        ]
    );
}

#[test]
fn exec_and_http_sinks_are_checked() {
    let alice = human(
        "name = \"alice\"\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"bin/notify\", \"--to\", \"alice\"]\ntimeout = \"20s\"\n\
             [[notify]]\nkind = \"exec\"\ncommand = [\"notify-send\"]\n\
             [[notify]]\nkind = \"http\"\nurl = \"https://hooks.example/ants/\"\nsecret_env = \"HOOK_SECRET\"\n",
    )
    .unwrap();
    let sinks: Vec<_> = alice.sinks.iter().map(|t| (&t.sink, t.timeout)).collect();
    let exec = |command: &[&str]| Sink::Exec { command: command.iter().map(|&a| a.to_owned()).collect() };
    let http = Sink::Http { url: "https://hooks.example/ants/".to_owned(), secret: "0123456789abcdef-hook".into() };
    assert_eq!(
        sinks,
        [
            (&exec(&["/etc/ants/bin/notify", "--to", "alice"]), Duration::from_secs(20)),
            (&exec(&["notify-send"]), SEND_TIMEOUT),
            (&http, SEND_TIMEOUT),
        ]
    );
    assert_eq!(format!("{:?}", alice.sinks), "[exec /etc/ants/bin/notify, exec notify-send, http hooks.example]");
    for (text, why) in [
        ("[[notify]]\nkind = \"exec\"\n", "exec needs command"),
        ("[[notify]]\nkind = \"exec\"\ncommand = []\n", "give the program first"),
        ("[[notify]]\nkind = \"exec\"\ncommand = [\"x\"]\nurl = \"https://a\"\n", "url does not go with"),
        ("[[notify]]\nkind = \"exec\"\ncommand = [\"x\"]\nsecret_env = \"HOOK_SECRET\"\n", "reads its own"),
        ("[[notify]]\nkind = \"exec\"\ncommand = [\"x\"]\ntimeout = \"0\"\n", "from 1s to 30s"),
        ("[[notify]]\nkind = \"exec\"\ncommand = [\"x\"]\ntimeout = \"31s\"\n", "from 1s to 30s"),
        ("[[notify]]\nkind = \"exec\"\ncommand = [\"x\"]\ntimeout = \"10\"\n", "without a unit"),
        ("[[notify]]\nkind = \"ntfy\"\ntopic = \"t\"\ncommand = [\"x\"]\n", "command does not go with"),
        ("[[notify]]\nkind = \"http\"\nsecret_env = \"HOOK_SECRET\"\n", "http needs url"),
        ("[[notify]]\nkind = \"http\"\nurl = \"https://a/?k=v\"\nsecret_env = \"HOOK_SECRET\"\n", "no user"),
        ("[[notify]]\nkind = \"http\"\nurl = \"https://a\"\n", "needs the signing secret"),
        ("[[notify]]\nkind = \"http\"\nurl = \"https://a\"\nsecret_env = \"BOT_TOKEN\"\n", "too short"),
    ] {
        let text = format!("name = \"bob\"\n{text}");
        assert!(error(&text).contains(why), "{text}: {}", error(&text));
    }
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
    use std::os::unix::fs::PermissionsExt as _;
    let dir = ants_test_support::scratch_dir("notify-exec-http").canonicalize().unwrap();
    let script = |name: &str, body: &str| {
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
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
