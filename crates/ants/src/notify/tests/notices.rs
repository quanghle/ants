//! Tests of what a notice says: completions, several questions, sealed messages, redaction, what allow always
//! allows, and texts cut and escaped.

use super::*;

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
    let summarised = Envelope { summary: Some(ants_proto::SummaryRequest::thread(thread.clone(), 7)), ..done.clone() };
    assert!(completion_notice(&summarised).is_none(), "the CLI that asked for a summary takes it");
    let hooked = Envelope { webhook: true, ..done.clone() };
    assert!(completion_notice(&hooked).is_none());
    let spent = done.clone().as_notice(ants_proto::NoticeKind::Spend);
    assert!(completion_notice(&spent).is_none() && action_notice(&spent).is_none(), "a notice is no reply");
    let mcp = done.clone().as_notice(ants_proto::NoticeKind::McpSignIn);
    assert_eq!(action_notice(&mcp).unwrap().title, "dev-a: MCP sign-in needed");
    let signed_out = done.as_notice(ants_proto::NoticeKind::SignedOut);
    let notice = action_notice(&signed_out).unwrap();
    assert_eq!(notice.title, "dev-a: signed out");
    let json = notice_json(&notice, at("2026-03-10T12:00:00Z"));
    assert_eq!((&json["kind"], &json["notice"]), (&json!("notice"), &json!("signed_out")));
}

#[test]
fn a_notice_of_several_questions_lists_them_all_and_how_to_answer_each() {
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let options = ["eu".to_owned(), "us".to_owned()];
    let regions = ants_proto::Question::new("Which regions?", &options, false).unwrap().multi().headed("Regions");
    let ship = ants_proto::Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], true).unwrap();
    let questions = ants_proto::Questions::new(vec![regions, ship]).unwrap();
    let asking = Envelope::asking(ThreadId::generate(), dev_a, alice, questions);
    let notice = question_notice(&asking).unwrap();
    assert_eq!(notice.title, "dev-a asks 2 questions: Which regions?");
    assert!(notice.text.contains("Question 2 of 2: Ship it?"), "{}", notice.text);
    assert!(
        notice.text.contains(&format!("ants answer {} and an answer for each of the 2", asking.id)),
        "{}",
        notice.text
    );
    let json = notice_json(&notice, at("2026-03-10T12:00:00Z"));
    assert_eq!(
        (&json["questions"][0]["header"], &json["questions"][0]["multi_select"]),
        (&json!("Regions"), &json!(true))
    );
    assert_eq!(json["questions"][1]["question"], "Ship it?");
}

#[test]
fn a_notice_of_a_sealed_message_names_its_sender_and_thread_only() {
    let (alice, bob) = (p("alice"), p("bob"));
    let asked = Envelope::new(alice.clone(), bob.clone(), "lunch?");
    let mut reply = asked.reply(bob, "", ants_proto::Role::Human).unwrap();
    reply.title = Some("Lunch plans".to_owned());
    reply.sealed = Some(Box::new(ants_proto::Sealed {
        v: ants_proto::SEALED_VERSION,
        device: ants_proto::DeviceId::generate(),
        keys: Vec::new(),
        body: "c2VjcmV0".to_owned(),
    }));
    let notice = completion_notice(&reply).unwrap();
    let thread = &asked.thread_id;
    assert_eq!((notice.title.as_str(), &notice.human), ("bob replied", &alice));
    assert_eq!(
        notice.text,
        format!("a sealed message arrived\n\nthread {thread}; answer with ants ask --thread {thread} @bob TEXT")
    );
    let json = notice_json(&notice, at("2026-03-10T12:00:00Z")).to_string();
    assert!(!json.contains("Lunch") && !json.contains("c2VjcmV0"), "{json}");
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
fn an_approval_notice_names_what_allow_always_allows_redacted() {
    let secret = "ghp_0123456789abcdefghijABCDEFGHIJ";
    let always = format!("Yes, and don't ask again for git push https://x:{secret}@github.com");
    let request = request().labelled([(Verdict::AllowAlways, always)].into());
    let notice = approval_notice(&request, 9, &p("alice"), None, None, Utc::now());
    let shown = "Yes, and don't ask again for git push https://x:[redacted]@github.com";
    assert!(notice.text.contains(&format!("\nallow-always: {shown}\n")), "{}", notice.text);
    let json = notice_json(&notice, Utc::now());
    assert_eq!(json["labels"], json!({ "allow_always": shown }));
    assert!(details(&request).contains(&format!("<p>Allow always: {}</p>", html(shown))));
}

#[test]
fn texts_are_cut_and_escaped() {
    assert_eq!(cut("héllo", 3), "hél…");
    assert_eq!(cut("hé", 3), "hé");
    assert_eq!(html("<a href=\"x\">&'"), "&lt;a href=&quot;x&quot;&gt;&amp;&#39;");
    let old = Uuid::from_u128((1_000_000_000_u128 << 80) | (7 << 76) | (2 << 62));
    assert!(!is_recent(&old, Utc::now()) && is_recent(&Uuid::now_v7(), Utc::now()));
}
