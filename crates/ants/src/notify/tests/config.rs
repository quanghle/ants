//! Tests of the notify config: a human's sinks and what they are notified of, the old style, and exec and http
//! sinks checked.

use super::*;

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
    assert_eq!(alice.subjects(), ["ants.approvals.*", "ants.inbox.alice.>"]);
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
    assert_eq!(quiet.subjects(), ["ants.inbox.bob.>"]);
    assert!(!quiet.wants_request_of(&p("dev-a")));
    assert_matches!(&quiet.sinks[0].sink, Sink::Ntfy { url, .. } if url == "http://ntfy.lan");
    let off = "escalations = false\nmessages = false\nmentions = false\nruns = false\n";
    let every = human(&format!("name = \"bob\"\n{off}{NTFY_SINK}")).unwrap();
    assert_eq!(every.subjects(), ["ants.approvals.*"]);
    let asked = human(&format!("name = \"bob\"\n{off}questions = true\n{NTFY_SINK}")).unwrap();
    assert_eq!(asked.subjects(), ["ants.approvals.*", "ants.inbox.bob.>"], "questions follow escalations unless set");
    assert!(every.wants_request_of(&p("dev-b")));

    for (text, why) in [
        ("name = \"bob\"\n".to_owned(), "nowhere to send"),
        (format!("name = \"bob\"\napprovals = false\n{off}{NTFY_SINK}"), "nothing to notify"),
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
