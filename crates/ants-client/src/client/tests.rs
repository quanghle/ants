//! Tests of a client's reads and writes over a transport in memory ([`Server`]), which stores, acks and answers as
//! a nats-server with JetStream does for what a human's client asks: a window read page by page with what a reader
//! skips and refuses, a send with its owner's record and the stream's ack, the read marker moved by compare-and-set
//! against another device, and what is heard live.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::Duration;

use ants_proto::{NoticeKind, Role, StatusUpdate};
use data_encoding::BASE64URL_NOPAD;
use futures::executor::block_on;
use nkeys::KeyPair;

use super::*;
use crate::entry::Kind;
use crate::sealed::State;
use crate::transport::{Pending, TransportError};

/// A message a stream of the [`Server`] stored.
struct Kept {
    stream: String,
    seq: u64,
    subject: String,
    headers: Vec<(String, String)>,
    payload: Vec<u8>,
}

#[derive(Default)]
struct Inner {
    connected: Vec<Connect>,
    kept: Vec<Kept>,
    /// The subscriptions, each a subject and where its messages wait.
    subscribed: Vec<(String, Rc<RefCell<VecDeque<Message>>>)>,
    /// The subjects of every direct get and publish asked for, in order.
    asked: Vec<String>,
    /// What another device does to the read marker of `alice` right after it is next read.
    racing: VecDeque<ReadMarker>,
}

/// A server in memory: the `ANTS` stream over `ants.>`, a stream per bucket over `$KV.<bucket>.>`, of which the
/// owners' keeps the first message per subject, and core subscriptions. Its JetStream is in the domain `hub`.
#[derive(Clone, Default)]
struct Server(Rc<RefCell<Inner>>);

const API: &str = "$JS.hub.API.";

fn matches(pattern: &str, subject: &str) -> bool {
    let (mut pattern, mut subject) = (pattern.split('.'), subject.split('.'));
    loop {
        match (pattern.next(), subject.next()) {
            (Some(">"), Some(_)) => return true,
            (Some(want), Some(token)) if want == "*" || want == token => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

fn stream_of(subject: &str) -> Option<String> {
    if subject.starts_with("ants.") {
        return Some("ANTS".to_owned());
    }
    let bucket = subject.strip_prefix("$KV.")?.split('.').next()?;
    Some(format!("KV_{bucket}"))
}

fn refusal(err_code: u32, description: &str) -> Vec<u8> {
    serde_json::json!({ "error": { "code": 400, "err_code": err_code, "description": description } })
        .to_string()
        .into_bytes()
}

impl Inner {
    fn deliver(&self, message: &Message) {
        for (pattern, queue) in &self.subscribed {
            if matches(pattern, &message.subject) {
                queue.borrow_mut().push_back(message.clone());
            }
        }
    }

    fn last_seq(&self, stream: &str) -> u64 {
        self.kept.iter().filter(|k| k.stream == stream).map(|k| k.seq).max().unwrap_or(0)
    }

    /// Store `message` as its stream would, answering with the ack or the refusal; none when no stream captures it.
    fn store(&mut self, message: &Message) -> Option<Vec<u8>> {
        let subject = message.subject.strip_prefix(API).unwrap_or(&message.subject).to_owned();
        let stream = stream_of(&subject)?;
        let on_subject = |k: &&Kept| k.stream == stream && k.subject == subject;
        let last = self.kept.iter().filter(on_subject).map(|k| k.seq).max().unwrap_or(0);
        if let Some(expected) = message.header_value("Nats-Expected-Last-Subject-Sequence")
            && expected.parse() != Ok(last)
        {
            return Some(refusal(10071, &format!("wrong last sequence: {last}")));
        }
        let id = message.header_value("Nats-Msg-Id");
        let same_id = |k: &Kept| {
            k.stream == stream && id.is_some() && k.headers.iter().any(|(n, v)| n == "Nats-Msg-Id" && Some(&**v) == id)
        };
        if let Some(first) = self.kept.iter().find(|k| same_id(k)) {
            let ack = serde_json::json!({ "stream": stream, "domain": "hub", "seq": first.seq, "duplicate": true });
            return Some(ack.to_string().into_bytes());
        }
        if stream == "KV_ants_owners" && last > 0 {
            return Some(refusal(10077, "maximum messages per subject exceeded"));
        }
        let seq = self.last_seq(&stream) + 1;
        let (headers, payload) = (message.headers.clone(), message.payload.clone());
        self.kept.push(Kept { stream: stream.clone(), seq, subject, headers, payload });
        Some(serde_json::json!({ "stream": stream, "domain": "hub", "seq": seq }).to_string().into_bytes())
    }

    /// `kept` as a direct get answers with it on `reply`.
    fn got(kept: &Kept, reply: &str) -> Message {
        let mut answer = Message::new(reply, kept.payload.clone());
        answer.headers = kept.headers.clone();
        answer
            .header("Nats-Stream", kept.stream.clone())
            .header("Nats-Subject", kept.subject.clone())
            .header("Nats-Sequence", kept.seq.to_string())
            .header("Nats-Time-Stamp", format!("2026-11-20T10:00:{:02}Z", kept.seq % 60))
    }

    /// Answer a batched direct get of `ANTS` on its reply subject.
    fn batch(&self, get: &Message) {
        let request: serde_json::Value = serde_json::from_slice(&get.payload).unwrap();
        let reply = get.reply.as_deref().unwrap();
        let (from, filter) = (request["seq"].as_u64().unwrap_or(1), request["next_by_subj"].as_str().unwrap());
        let batch = request["batch"].as_u64().unwrap() as usize;
        let found: Vec<&Kept> =
            self.kept.iter().filter(|k| k.stream == "ANTS" && k.seq >= from && matches(filter, &k.subject)).collect();
        if found.is_empty() {
            self.deliver(&Message { status: Some(404), ..Message::new(reply, "") });
            return;
        }
        for kept in found.iter().take(batch) {
            self.deliver(&Self::got(kept, reply));
        }
        let end = Message { status: Some(204), description: Some("EOB".into()), ..Message::new(reply, "") };
        let pending = found.len().saturating_sub(batch);
        let end = end.header("Nats-Num-Pending", pending.to_string());
        self.deliver(&end.header("Nats-Last-Sequence", self.last_seq("ANTS").to_string()));
    }
}

impl Server {
    /// Store `payload` on `subject` as another client's publish.
    fn put(&self, subject: &str, payload: impl Into<Vec<u8>>) -> u64 {
        self.put_with(Message::new(subject, payload))
    }

    fn put_with(&self, message: Message) -> u64 {
        let ack = self.0.borrow_mut().store(&message).expect("a stream captures it");
        serde_json::from_slice::<serde_json::Value>(&ack).unwrap()["seq"].as_u64().expect("stored")
    }

    fn send(&self, envelope: &Envelope) -> u64 {
        self.put(&envelope.subject(), serde_json::to_vec(envelope).unwrap())
    }

    /// Publish `message` live, as another client does.
    fn say(&self, message: Message) {
        self.0.borrow().deliver(&message);
    }

    /// The value last put under `subject` of a bucket.
    fn value(&self, subject: &str) -> Option<String> {
        let inner = self.0.borrow();
        let kept = inner.kept.iter().rfind(|k| k.subject == subject)?;
        Some(String::from_utf8(kept.payload.clone()).unwrap())
    }

    fn asked(&self, what: &str) -> usize {
        self.0.borrow().asked.iter().filter(|subject| subject.contains(what)).count()
    }
}

impl Transport for Server {
    fn connect(&self, options: Connect) -> Pending<'_, Result<(), TransportError>> {
        self.0.borrow_mut().connected.push(options);
        Box::pin(std::future::ready(Ok(())))
    }

    fn publish(&self, message: Message) -> Pending<'_, Result<(), TransportError>> {
        let mut inner = self.0.borrow_mut();
        inner.asked.push(message.subject.clone());
        if message.subject == format!("{API}DIRECT.GET.ANTS") {
            inner.batch(&message);
        } else if let (Some(answer), Some(reply)) = (inner.store(&message), &message.reply) {
            inner.deliver(&Message::new(reply.clone(), answer));
        }
        inner.deliver(&message);
        Box::pin(std::future::ready(Ok(())))
    }

    fn subscribe(&self, subject: String) -> Pending<'_, Result<Box<dyn Subscription>, TransportError>> {
        let queue = Rc::new(RefCell::new(VecDeque::new()));
        self.0.borrow_mut().subscribed.push((subject, queue.clone()));
        Box::pin(std::future::ready(Ok(Box::new(Queue(queue)) as Box<dyn Subscription>)))
    }

    fn request(&self, message: Message, _wait: Duration) -> Pending<'_, Result<Message, TransportError>> {
        let mut inner = self.0.borrow_mut();
        inner.asked.push(message.subject.clone());
        let answer = match message.subject.strip_prefix(API).and_then(|rest| rest.strip_prefix("DIRECT.GET.KV_")) {
            Some(get) => {
                let subject = get.split_once('.').map_or("", |(_, subject)| subject);
                let found = inner.kept.iter().rfind(|k| k.subject == subject).map(|kept| Inner::got(kept, "reply"));
                if let (true, Some(marker)) = (subject == "$KV.ants_read.alice", inner.racing.pop_front()) {
                    let moved = Message::new(subject, serde_json::to_vec(&marker).unwrap());
                    inner.store(&moved);
                }
                Ok(found.unwrap_or_else(|| Message { status: Some(404), ..Message::default() }))
            }
            None => match inner.store(&message) {
                Some(answer) => Ok(Message::new("reply", answer)),
                None => Err(TransportError::NoResponders),
            },
        };
        Box::pin(std::future::ready(answer))
    }
}

struct Queue(Rc<RefCell<VecDeque<Message>>>);

impl Subscription for Queue {
    /// The next message waiting. With none, a wait runs out at once, and a subscription waited on for good is closed,
    /// so no test hangs.
    fn next(&mut self, wait: Option<Duration>) -> Pending<'_, Result<Message, TransportError>> {
        let closed = if wait.is_some() { TransportError::TimedOut } else { TransportError::Closed };
        Box::pin(std::future::ready(self.0.borrow_mut().pop_front().ok_or(closed)))
    }
}

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

fn alice_on(server: &Server, account: Option<String>) -> Client {
    let domain = Some("hub".parse().unwrap());
    let session = Session { human: p("alice"), domain, account, name: "ants-web".into() };
    Client::new(Rc::new(server.clone()), session)
}

fn alice() -> (Server, Client) {
    let server = Server::default();
    let client = alice_on(&server, None);
    (server, client)
}

fn ask(text: &str) -> Send {
    Send { text: text.into(), ..Send::default() }
}

#[test]
fn a_client_connects_under_the_reply_subjects_of_its_human() {
    let (server, client) = alice();
    block_on(client.connect()).unwrap();
    let expected = Connect { name: "ants-web".into(), inbox_prefix: "_INBOX.alice".into() };
    assert_eq!(server.0.borrow().connected, [expected]);
}

#[test]
fn a_send_records_the_thread_s_owner_and_is_sent_once_the_stream_acks_it() {
    let (server, client) = alice();
    let sent = block_on(client.send(Send { to: Some(p("dev-a")), ..ask("fix it") })).unwrap();
    let Sent { envelope, unaddressed, ack, owner_unrecorded } = &sent;
    assert_eq!((unaddressed, owner_unrecorded), (&None, &None));
    assert_eq!((ack.stream.as_str(), ack.seq, ack.duplicate), ("ANTS", 1, false));
    assert_eq!((envelope.owner.as_ref(), envelope.to.as_str()), (Some(&p("alice")), "dev-a"));
    let thread = &envelope.thread_id;
    assert_eq!(server.value(&format!("$KV.ants_owners.{thread}.dev-a")).as_deref(), Some("alice"));
    let asked = server.0.borrow().asked.clone();
    let expected = [format!("{API}$KV.ants_owners.{thread}.dev-a"), "ants.inbox.dev-a.alice".to_owned()];
    assert_eq!(asked, expected, "the owner first");
    let stored: Envelope = serde_json::from_str(&server.value("ants.inbox.dev-a.alice").unwrap()).unwrap();
    assert_eq!(&stored, envelope);

    // A reply waiting its turn on the thread: the owner's first record stays, and the stream keeps both messages.
    let later = Send { to: Some(p("dev-a")), thread: Some(thread.clone()), queue: true, ..ask("then the docs") };
    let later = block_on(client.send(later)).unwrap();
    assert_eq!((later.ack.seq, later.envelope.queue, &later.owner_unrecorded), (2, true, &None));
    assert_eq!(&later.envelope.thread_id, thread);

    // With no stream to store it, nothing is sent.
    let nowhere = Client::new(Rc::new(Nowhere), client.session().clone());
    let failed = block_on(nowhere.send(Send { to: Some(p("dev-a")), ..ask("anyone?") })).unwrap_err();
    let unanswered = Error::transport("sending the message", TransportError::NoResponders);
    assert_eq!(failed, unanswered);
    assert_eq!(failed.to_string(), "sending the message: nobody answers");
    assert!(matches!(block_on(client.send(ask(" "))), Err(Error::Invalid(_))), "no text");
}

/// A server with no JetStream.
struct Nowhere;

impl Transport for Nowhere {
    fn connect(&self, _: Connect) -> Pending<'_, Result<(), TransportError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn publish(&self, _: Message) -> Pending<'_, Result<(), TransportError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn subscribe(&self, _: String) -> Pending<'_, Result<Box<dyn Subscription>, TransportError>> {
        Box::pin(std::future::ready(Err(TransportError::Closed)))
    }

    fn request(&self, _: Message, _: Duration) -> Pending<'_, Result<Message, TransportError>> {
        Box::pin(std::future::ready(Err(TransportError::NoResponders)))
    }
}

/// A team's account and a device of `alice` its account signed, to sign her grants with.
struct Team {
    account: KeyPair,
    device: KeyPair,
}

impl Team {
    fn new() -> Self {
        Self { account: KeyPair::new_account(), device: KeyPair::new_user() }
    }

    fn grant(&self, assistant: Option<&str>) -> AssistantGrant {
        let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
        let device = ants_proto::DeviceId::generate();
        let claims = serde_json::json!({
            "sub": self.device.public_key(), "iss": self.account.public_key(), "name": "alice",
            "nats": { "type": "user", "tags": [device.tag()] },
        });
        let body = BASE64URL_NOPAD.encode(claims.to_string().as_bytes());
        let signature = BASE64URL_NOPAD.encode(&self.account.sign(format!("{header}.{body}").as_bytes()).unwrap());
        let jwt = format!("{header}.{body}.{signature}");
        let mut grant = AssistantGrant::new(p("alice"), assistant.map(p), Utc::now(), jwt);
        grant.signature = BASE64URL_NOPAD.encode(&self.device.sign(&grant.signed()).unwrap());
        grant
    }
}

#[test]
fn a_message_naming_no_one_goes_to_the_assistant_while_its_grant_stands_else_to_the_messenger() {
    let (server, team) = (Server::default(), Team::new());
    let client = alice_on(&server, Some(team.account.public_key()));
    let sent = block_on(client.send(ask("what is up?"))).unwrap();
    let messenger = Recipient { to: UserId::messenger(), assistant_of: None };
    assert_eq!(sent.unaddressed.as_ref(), Some(&messenger), "no grant");
    assert_eq!(sent.envelope.subject(), "ants.inbox.system-messenger.alice");

    let key = "$KV.ants_humans.alice.assistant";
    server.put(key, serde_json::to_vec(&team.grant(Some("pa"))).unwrap());
    let sent = block_on(client.send(ask("and now?"))).unwrap();
    let assistant = Recipient { to: p("pa"), assistant_of: Some(p("alice")) };
    assert_eq!(sent.unaddressed.as_ref(), Some(&assistant));
    assert_eq!(sent.envelope.to, p("pa"));
    let owner = format!("$KV.ants_owners.{}.pa", sent.envelope.thread_id);
    assert_eq!(server.value(&owner).as_deref(), Some("alice"));

    // A grant another team's account vouches for, one withdrawn, and a reader with no account to verify by.
    let stranger = alice_on(&server, Some(Team::new().account.public_key()));
    assert_eq!(block_on(stranger.assistant()), None);
    assert_eq!(block_on(alice_on(&server, None).assistant()), None);
    assert_eq!(block_on(client.assistant()), Some(p("pa")));
    server.put(key, serde_json::to_vec(&team.grant(None)).unwrap());
    assert_eq!(block_on(client.assistant()), None, "withdrawn");
    assert_eq!(block_on(client.send(ask("still there?"))).unwrap().unaddressed, Some(messenger));
}

#[test]
fn a_window_is_read_page_by_page_skipping_what_is_no_message_and_refusing_what_a_stream_relayed() {
    let (server, client) = alice();
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let asked = block_on(client.send(Send { to: Some(dev_a.clone()), ..ask("fix it") })).unwrap().envelope;
    let reply = asked.reply(dev_a.clone(), "done", Role::Human).unwrap();
    let reply_seq = server.send(&reply);
    // On the human's inbox: no envelope, an ack a stream relayed there, and a message stored as a direct get answered.
    server.put(&reply.subject(), "not json");
    server.put(&reply.subject(), r#"{"stream":"ANTS","seq":2}"#);
    let mut forged = Envelope::on_thread(asked.thread_id.clone(), dev_a.clone(), alice.clone(), "psst");
    forged.notice = Some(NoticeKind::GaveUp);
    let relayed = Message::new(forged.subject(), serde_json::to_vec(&forged).unwrap());
    server
        .put_with(relayed.header("Nats-Stream", "KV_x").header("Nats-Subject", "$KV.x.y").header("Nats-Sequence", "1"));
    let mut notice = Envelope::on_thread(asked.thread_id.clone(), dev_a.clone(), alice.clone(), "signed out");
    notice.notice = Some(NoticeKind::SignedOut);
    let notice_seq = server.send(&notice);
    let mut sealed = Envelope::new(p("bob"), alice.clone(), "");
    let device = ants_proto::DeviceId::generate();
    let body = ants_proto::Sealed { v: 1, device, keys: Vec::new(), body: "AAAA".into() };
    sealed.sealed = Some(Box::new(body));
    let sealed_seq = server.send(&sealed);
    // Outside the inboxes, and enough of the ant's chatter for a second batch.
    server.put("ants.notes.t1.dev-a", "{}");
    for i in 0..300 {
        server.send(&Envelope::on_thread(asked.thread_id.clone(), dev_a.clone(), p("dev-b"), format!("step {i}")));
    }

    let mut reader = client.reader(Window::default());
    assert_eq!(server.asked("DIRECT.GET.ANTS"), 0, "nothing is read before a page is asked for");
    let first = block_on(reader.next()).unwrap();
    assert_eq!((first.skipped, first.relayed, first.done), (1, 2, false));
    assert_eq!(first.entries.len(), 256 - 3);
    let texts: Vec<&str> = first.entries.iter().take(5).map(|e| e.text.as_str()).collect();
    assert_eq!(texts, ["fix it", "done", "signed out", "[sealed]", "step 0"]);
    let (shown, system) = (&first.entries[3], &first.entries[2]);
    assert_eq!((shown.seq, shown.sealed, shown.kind), (sealed_seq, Some(State::Unreadable), Kind::Message));
    assert_eq!((system.seq, system.notice), (notice_seq, Some(NoticeKind::SignedOut)), "a system line");
    assert_eq!(first.next_seq, first.entries.last().unwrap().seq + 1);

    let second = block_on(reader.next()).unwrap();
    assert_eq!((second.entries.len(), second.skipped, second.relayed, second.done), (51, 0, 0, true));
    assert_eq!(second.entries.last().unwrap().text, "step 299");
    let end = block_on(reader.next()).unwrap();
    assert!(end.done && end.entries.is_empty());
    assert_eq!(server.asked("DIRECT.GET.ANTS"), 2, "one get per page, and none once done");

    // What was stored since is read on from where the window ended, and what came before a sequence up to it.
    let more = Envelope::on_thread(asked.thread_id.clone(), dev_a, alice.clone(), "one more thing");
    let more_seq = server.send(&more);
    let mut on = client.reader(Window { from_seq: Some(end.next_seq), ..Window::default() });
    let page = block_on(on.next()).unwrap();
    assert_eq!((page.entries.len(), page.entries[0].seq, page.done), (1, more_seq, true));
    let mut before = client.reader(Window { until_seq: Some(reply_seq), ..Window::default() });
    let page = block_on(before.next()).unwrap();
    assert_eq!((page.entries.len(), page.done), (2, true));
    let mut everything = client.reader(Window { scope: Scope::Everything, until_seq: Some(9), ..Window::default() });
    let page = block_on(everything.next()).unwrap();
    assert_eq!((page.entries.len(), page.skipped, page.relayed), (5, 2, 2), "the note is no entry");
    let mut nothing = client.reader(Window { from_seq: Some(more_seq + 1), ..Window::default() });
    let page = block_on(nothing.next()).unwrap();
    assert!(page.done && page.entries.is_empty());

    // The human's unread messages by thread, and none once read.
    let unread = inbox::unread_by_thread(Some(&ReadMarker::default()), &alice, first.entries.iter());
    assert_eq!(unread.get(&asked.thread_id), Some(&vec![reply_seq, notice_seq]));
    assert_eq!(unread.get(&sealed.thread_id), Some(&vec![sealed_seq]));
}

#[test]
fn reading_marks_read_on_every_device_by_compare_and_set() {
    let (server, client) = alice();
    assert_eq!(block_on(client.marker()).unwrap(), Marked::default(), "no marker yet");
    let read = MarkRead { seqs: vec![2, 3], start: Some(1), inbox: vec![2, 3, 5] };
    let marked = block_on(client.mark_read(read)).unwrap();
    assert_eq!((marked.marker.below, marked.revision), (3, 1));
    assert_eq!(server.value("$KV.ants_read.alice").as_deref(), Some(r#"{"below":3}"#));
    assert_eq!(block_on(client.marker()).unwrap(), marked);

    // Another device marks message 9 read between this one's read of the marker and its put: both marks stay.
    let theirs = ReadMarker { below: 3, read: [9].into(), ..ReadMarker::default() };
    server.0.borrow_mut().racing.push_back(theirs);
    let marked = block_on(client.mark_read(MarkRead { seqs: vec![5], ..MarkRead::default() })).unwrap();
    assert_eq!(marked.marker.read.iter().copied().collect::<Vec<_>>(), [5, 9]);
    assert_eq!(marked.revision, 3);
    assert!(!inbox::unread(Some(&marked.marker), 9) && inbox::unread(Some(&marked.marker), 6));

    let unmarked = block_on(client.mark_unread(vec![2])).unwrap();
    assert!(inbox::unread(Some(&unmarked.marker), 2));
    assert_eq!(block_on(client.mark_read(MarkRead::default())).unwrap(), unmarked, "nothing to mark");

    // A marker that keeps moving is given up on.
    let moving = (0..inbox::CONFLICTS as u64).map(|n| ReadMarker { below: 100 + n, ..ReadMarker::default() });
    server.0.borrow_mut().racing.extend(moving);
    let failed = block_on(client.mark_read(MarkRead { seqs: vec![200], ..MarkRead::default() })).unwrap_err();
    assert_eq!(failed, Error::Conflict { what: "marking read".into() });
    // One that does not parse is started over from.
    server.put("$KV.ants_read.alice", "not json");
    assert_eq!(block_on(client.marker()).unwrap().marker, ReadMarker::default());
}

#[test]
fn live_updates_are_taken_only_as_their_subjects_name_them() {
    let (server, client) = alice();
    let (dev_a, thread): (UserId, ThreadId) = (p("dev-a"), "t1".parse().unwrap());
    let mut status = block_on(client.status(thread.clone())).unwrap();
    let mut inbox = block_on(client.inbox()).unwrap();
    let working =
        StatusEvent::new(dev_a.clone(), thread.clone(), StatusUpdate::State { state: ants_proto::State::Working });
    let idle = StatusEvent::new(dev_a.clone(), thread, StatusUpdate::State { state: ants_proto::State::Idle });
    let json = |event: &StatusEvent| serde_json::to_vec(event).unwrap();
    server.say(Message::new("ants.status.dev-b.t1", json(&working)));
    server.say(Message::new(working.subject(), json(&working)).header("Nats-Stream", "ANTS"));
    server.say(Message::new("ants.status.dev-a.t2", json(&working)));
    server.say(Message::new(working.subject(), json(&working)));
    server.say(Message::new(idle.subject(), json(&idle)));
    assert_eq!(block_on(status.next()).unwrap(), working, "the forged and the relayed ones are passed over");
    assert_eq!(block_on(status.next()).unwrap(), idle);
    let ended = Error::transport("following the thread's ants", TransportError::Closed);
    assert_eq!(block_on(status.next()).unwrap_err(), ended);

    let reply = Envelope::new(dev_a, p("alice"), "done");
    let payload = serde_json::to_vec(&reply).unwrap();
    server.say(Message::new("ants.inbox.alice.dev-b", payload.clone()));
    server.say(Message::new(reply.subject(), "not json"));
    server.say(Message::new("ants.inbox.bob.dev-a", payload.clone()));
    server.say(Message::new(reply.subject(), payload));
    assert_eq!(block_on(inbox.next()).unwrap(), reply);
    assert!(block_on(inbox.next()).is_err());
}
