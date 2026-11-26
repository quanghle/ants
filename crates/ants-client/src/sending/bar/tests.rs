//! Tests of the bar's routing: where each form of text goes from each view, what the bar's title says of it, the
//! message sent there, and whom a reply on a thread goes to.

use ants_proto::{Envelope, Team, TeamRefusal};
use chrono::DateTime;

use super::*;
use crate::error::ErrorKind;
use crate::p;
use crate::parser::Parser;

fn context(view: View) -> Context {
    Context { me: p("alice"), view, humans: vec![p("bob")], teams: None }
}

fn thread_view(thread: &ThreadId, to: Replied) -> View {
    View::Thread { thread: thread.clone(), title: "deploy the site".into(), channel: None, to, quoting: false }
}

/// What the bar's title says of `text` from `context`.
fn title(context: &Context, text: &str) -> String {
    describe_send(text, context).description
}

/// The message of `text` from `context`, or why none goes.
fn sent(context: &Context, text: &str) -> Result<Option<Outgoing>, String> {
    resolve_send(text, context).map_err(|e| {
        assert!(matches!(e.kind, ErrorKind::Invalid(_)), "{e:?}");
        e.to_string()
    })
}

fn out(target: Target, text: &str) -> Option<Outgoing> {
    Some(Outgoing { target, text: text.into(), queue: false, sealed: false })
}

#[test]
fn from_elsewhere_text_goes_to_the_assistant_an_at_user_asks_and_a_hash_channel_posts() {
    let home = context(View::Elsewhere);
    assert_eq!(describe_send("who owns billing?", &home).route, Route::Unaddressed);
    assert_eq!(title(&home, "who owns billing?"), "to the assistant or the messenger on a new thread");
    assert_eq!(sent(&home, "who owns billing?").unwrap(), out(Target::Unaddressed, "who owns billing?"));

    assert_eq!(describe_send("@dev-a fix it", &home).route, Route::Ask { to: p("dev-a"), thread: None });
    assert_eq!(title(&home, "@dev-a fix it"), "to dev-a on a new thread");
    assert_eq!(title(&home, "@dev-a"), "to dev-a on a new thread", "as typed");
    let ask = Target::Ask { to: p("dev-a"), team: None, thread: None };
    assert_eq!(sent(&home, " @dev-a fix\nit ").unwrap(), out(ask, "fix\nit"));
    assert_eq!(sent(&home, "@dev-a").unwrap_err(), "nothing to send to @dev-a");

    assert_eq!(title(&home, "@bob lunch?"), "to bob on a new thread, sealed");
    let sealed = sent(&home, "@bob lunch?").unwrap().unwrap();
    assert!(sealed.sealed, "to another human");
    assert_eq!(title(&home, "@alice note"), "to alice on a new thread", "to oneself, not sealed");

    assert_eq!(
        describe_send("#ops status?", &home).route,
        Route::Post { channel: "ops".parse().unwrap(), thread: None }
    );
    assert_eq!(title(&home, "#ops status?"), "new thread in #ops");
    let post = Target::Post { channel: "ops".parse().unwrap(), thread: None };
    assert_eq!(sent(&home, "#ops status?").unwrap(), out(post, "status?"));
    assert_eq!(sent(&home, "#ops  ").unwrap_err(), "nothing to post to #ops");

    for unnamed in ["@", "#", "@Dev A hi"] {
        assert_eq!(describe_send(unnamed, &home), Described { route: Route::Unnamed, description: String::new() });
        assert!(sent(&home, unnamed).is_err(), "{unnamed}");
    }
    assert_eq!(sent(&home, "   ").unwrap_err(), "nothing to send");
}

#[test]
fn slash_commands_are_the_client_s_but_later_and_a_double_slash_says_the_text() {
    let thread = ThreadId::generate();
    let on = context(thread_view(&thread, Replied::To { user: p("dev-b") }));
    assert_eq!(describe_send("/Done", &on).route, Route::Command { name: "done".into() });
    assert_eq!(title(&on, "/done"), "run /done");
    assert_eq!(sent(&on, "/done").unwrap(), None);
    assert_eq!(draft("/approve dev-a/1").unwrap(), Draft::Command { line: "approve dev-a/1".into() });

    let reply = Target::Reply { thread, to: Some(p("dev-b")) };
    assert_eq!(title(&on, "//etc/hosts is wrong"), "reply to dev-b on \"deploy the site\"");
    assert_eq!(sent(&on, "//etc/hosts is wrong").unwrap(), out(reply.clone(), "/etc/hosts is wrong"));

    assert_eq!(title(&on, "/later then the docs"), "reply to dev-b on \"deploy the site\"");
    let later = sent(&on, "/LATER  then the docs ").unwrap().unwrap();
    assert_eq!((later.target, later.text, later.queue), (reply, "then the docs".to_owned(), true));
    assert_eq!(sent(&on, "/later").unwrap_err(), "usage: /later TEXT");
    let home = context(View::Elsewhere);
    assert_eq!(sent(&home, "/later then").unwrap_err(), "/later replies on an open thread: open one first");
}

#[test]
fn on_a_thread_text_replies_to_whom_its_messages_say_and_an_at_user_answers_the_quote_there() {
    let thread = ThreadId::generate();
    let reading = context(thread_view(&thread, Replied::Reading));
    assert_eq!(title(&reading, "go on"), "reply on \"deploy the site\"", "whom to while the thread is read");
    assert_eq!(sent(&reading, "go on").unwrap(), out(Target::Reply { thread: thread.clone(), to: None }, "go on"));

    let to_bob = context(thread_view(&thread, Replied::To { user: p("bob") }));
    assert_eq!(title(&to_bob, "go on"), "reply to bob on \"deploy the site\", sealed");
    assert!(sent(&to_bob, "go on").unwrap().unwrap().sealed);

    let nobody = context(thread_view(&thread, Replied::Unaddressed));
    assert_eq!(title(&nobody, "go on"), "reply to the assistant or the messenger on \"deploy the site\"");
    assert_eq!(sent(&nobody, "go on").unwrap(), out(Target::Reply { thread: thread.clone(), to: None }, "go on"));

    assert_eq!(title(&nobody, "@dev-a look"), "to dev-a on a new thread", "no quote: a new thread");
    let mut quoting_view = nobody;
    if let View::Thread { quoting, title, .. } = &mut quoting_view.view {
        (*quoting, *title) = (true, "a title much longer than the forty characters shown".into());
    }
    assert_eq!(title(&quoting_view, "@dev-a look"), "to dev-a on \"a title much longer than the forty chara…\"");
    let ask = Target::Ask { to: p("dev-a"), team: None, thread: Some(thread) };
    assert_eq!(sent(&quoting_view, "@dev-a look").unwrap(), out(ask, "look"));
}

#[test]
fn on_a_channel_s_thread_or_feed_text_posts_there_an_at_user_with_it() {
    let (thread, ops): (ThreadId, ChannelId) = (ThreadId::generate(), "ops".parse().unwrap());
    let mut on_thread = context(thread_view(&thread, Replied::To { user: p("dev-b") }));
    if let View::Thread { channel, .. } = &mut on_thread.view {
        *channel = Some(ops.clone());
    }
    assert_eq!(title(&on_thread, "@dev-b look"), "post in #ops on \"deploy the site\"");
    let post = Target::Post { channel: ops.clone(), thread: Some(thread) };
    assert_eq!(sent(&on_thread, "@dev-b @dev-c look").unwrap(), out(post, "@dev-b @dev-c look"), "a post mentions any");

    let feed = context(View::Channel { channel: ops.clone() });
    assert_eq!(title(&feed, "@dev-b look"), "new thread in #ops");
    assert_eq!(title(&feed, "#web look"), "new thread in #web", "a #channel typed first changes it");
    let post = Target::Post { channel: ops, thread: None };
    assert_eq!(sent(&feed, "@dev-b").unwrap(), out(post, "@dev-b"));
}

#[test]
fn in_a_dm_text_goes_sealed_to_the_other_human() {
    let dm = context(View::Dm { with: p("bob") });
    assert_eq!(describe_send("lunch?", &dm).route, Route::Ask { to: p("bob"), thread: None });
    assert_eq!(title(&dm, "lunch?"), "to bob on a new thread, sealed");
    let sent = sent(&dm, "lunch?").unwrap().unwrap();
    assert_eq!((sent.target, sent.sealed), (Target::Ask { to: p("bob"), team: None, thread: None }, true));
}

#[test]
fn a_team_is_named_with_its_lead_and_one_without_any_lead_is_refused_before_sending() {
    let mut teams = TeamDirectory::default();
    teams.create(Team::new(p("eng"), p("boss"), p("alice")), None).unwrap();
    teams.create(Team::unled(p("backend"), p("alice")), Some(&p("eng"))).unwrap();
    teams.create(Team::new(p("web"), p("dev-w"), p("alice")), None).unwrap();
    teams.create(Team::unled(p("lone"), p("alice")), None).unwrap();
    let home = Context { teams: Some(teams), ..context(View::Elsewhere) };
    assert_eq!(title(&home, "@web fix"), "to team web (lead dev-w) on a new thread");
    assert_eq!(title(&home, "@backend fix"), "to team backend (lead boss, from team eng) on a new thread");
    assert_eq!(title(&home, "@lone fix"), "to team lone, which has no lead on a new thread");
    let to_lead = Target::Ask { to: p("boss"), team: Some(p("backend")), thread: None };
    assert_eq!(sent(&home, "@backend fix").unwrap(), out(to_lead, "fix"));
    let refused = sent(&home, "@lone fix").unwrap_err();
    assert_eq!(refused, TeamRefusal::NoLead { team: p("lone") }.to_string());
}

#[test]
fn a_message_to_one_recipient_mentioning_several_users_is_refused_before_sending() {
    let home = context(View::Elsewhere);
    let refused = sent(&home, "@dev-a @dev-b and @dev-c review it").unwrap_err();
    assert!(refused.starts_with("Not sent: the text mentions @dev-b and @dev-c"), "{refused}");
    assert!(sent(&home, "@dev-a ask @dev-b").is_ok());
    let thread = ThreadId::generate();
    let on = context(thread_view(&thread, Replied::To { user: p("dev-a") }));
    assert!(sent(&on, "ask @dev-b and @dev-c").unwrap_err().contains("one recipient, dev-a,"));
}

#[test]
fn the_route_and_context_serialize_as_a_binding_takes_them() {
    let thread = ThreadId::generate();
    let context = context(thread_view(&thread, Replied::To { user: p("dev-a") }));
    let json = serde_json::to_value(&context).unwrap();
    assert_eq!(json["view"]["kind"], "thread");
    assert_eq!(json["view"]["to"], serde_json::json!({"kind": "to", "user": "dev-a"}));
    assert_eq!(serde_json::from_value::<Context>(json).unwrap(), context);
    let described = serde_json::to_value(describe_send("@dev-b hi", &context)).unwrap();
    assert_eq!(described["route"], serde_json::json!({"kind": "ask", "to": "dev-b"}));
    assert_eq!(described["description"], "to dev-b on a new thread");
}

fn message(seq: u64, thread: &ThreadId, from: &str, to: &str) -> Entry {
    let envelope = Envelope { thread_id: thread.clone(), ..Envelope::new(p(from), p(to), "hi") };
    let subject = ants_proto::subject::inbox(&envelope.to, &envelope.from);
    let payload = serde_json::to_vec(&envelope).unwrap();
    let at = DateTime::from_timestamp(1_800_000_000 + i64::try_from(seq).unwrap(), 0).unwrap();
    Parser::default().entry(seq, at, &subject, &payload).unwrap()
}

#[test]
fn a_reply_goes_to_the_ant_that_last_wrote_else_the_one_last_written_to_else_the_other_human() {
    let (thread, me) = (ThreadId::generate(), p("alice"));
    let human = |u: &UserId| u.as_str() == "bob";
    let mut entries = vec![message(1, &thread, "alice", "dev-a")];
    assert_eq!(reply_to(&entries, &me, human), Some(p("dev-a")));
    entries.push(message(2, &thread, "dev-b", "alice"));
    entries.push(message(3, &thread, "bob", "alice"));
    assert_eq!(reply_to(&entries, &me, human), Some(p("dev-b")), "a human is no ant to reply to");
    assert_eq!(reply_to(&entries[2..], &me, human), None);
    assert_eq!(dm_with(&entries, &me, human), Some(p("bob")), "on a thread with no ant, the other human");
    assert_eq!(dm_with(&entries[..2], &me, human), None);
    assert_eq!(replied(&entries, &me, human), Replied::To { user: p("dev-b") });
    assert_eq!(replied(&entries[2..], &me, human), Replied::To { user: p("bob") });
    assert_eq!(replied(&entries[..0], &me, human), Replied::Unaddressed);
}
