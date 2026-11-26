//! Web credentials, a human's device that is a page of the web client: what the server lets them do, each by the
//! request of core NATS the page sends, and what it refuses; what `ants-client`'s client, which the page runs, does
//! with them over async-nats; and how they are issued, renewed and listed.

use std::rc::Rc;

use ants_client::client::{MarkRead, Session, Window};
use ants_client::device::{Capabilities, Credentials};
use ants_client::transport::{Message, Transport as _};
use ants_proto::{StatusEvent, StatusUpdate};
use ants_test_support::NatsTransport;
use async_nats::HeaderMap;

use super::*;
use crate::test_support::colony::entry as colony_entry;

/// Web credentials of `human` issued to `out`: the device.
fn issue_web(team: &Path, human: &UserId, out: &Path) -> ants_proto::DeviceId {
    let new = Registered { label: Some("browser"), web: true, ..Registered::default() };
    Issue::new(team, human, Kind::Human, Permissions::for_web(human)).with(&new).into_file(out).unwrap().unwrap()
}

/// What JetStream answers `payload` published on `subject` under `headers`: its ack, or its error.
async fn stored(client: &Client, subject: String, headers: HeaderMap, payload: &[u8]) -> serde_json::Value {
    let ack = client.request_with_headers(subject, headers, payload.to_vec().into());
    serde_json::from_slice(&timeout(WAIT, ack).await.expect("no ack").unwrap().payload).unwrap()
}

/// The last message stored on `subject` in `stream`, by a direct get naming it: its payload and sequence, or none.
async fn last(client: &Client, stream: &str, subject: &str) -> Option<(Vec<u8>, u64)> {
    let get = client.request(format!("$JS.API.DIRECT.GET.{stream}.{subject}"), "".into());
    let msg = timeout(WAIT, get).await.expect("no answer to a direct get").unwrap();
    let seq = msg.headers.as_ref()?.get("Nats-Sequence")?.as_str().parse().unwrap();
    Some((msg.payload.to_vec(), seq))
}

/// The messages of the `ANTS` stream from its first, read as the page reads a window: one batched direct get naming
/// no subject, its answers on one reply inbox until the one ending the batch.
async fn window(client: &Client) -> Vec<(String, Vec<u8>)> {
    let inbox = client.new_inbox();
    let mut answers = client.subscribe(inbox.clone()).await.unwrap();
    let request = json!({ "seq": 1, "next_by_subj": ">", "batch": 256, "max_bytes": 4 << 20 });
    let get = format!("$JS.API.DIRECT.GET.{STREAM}");
    client.publish_with_reply(get, inbox, request.to_string().into()).await.unwrap();
    let mut read = Vec::new();
    loop {
        let msg = timeout(WAIT, answers.next()).await.expect("no end of the batch").unwrap();
        if msg.status.is_some() {
            assert!(msg.headers.unwrap().get("Nats-Num-Pending").is_some(), "the batch's end");
            return read;
        }
        let subject = msg.headers.unwrap().get("Nats-Subject").unwrap().as_str().to_owned();
        read.push((subject, msg.payload.to_vec()));
    }
}

fn msg_id(id: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("Nats-Msg-Id", id);
    headers
}

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "every right of the page checked in turn against one server")]
async fn a_web_device_does_what_the_page_s_views_do_and_nothing_else() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let laptop = server.creds_as(&alice, Role::Human);
    let (_, h_js) = bus::connect(&server.url, Some(&laptop), None, &alice, "test").await.unwrap();
    let rights = crate::people::humans::Rights::from_creds(Some(&laptop)).unwrap();
    ants_daemon::hub::init(&h_js, |s| rights.may_publish(s)).await.unwrap();
    let (d_client, d_js) = server.connect(&dev_a).await;

    let out = server.dir().join("alice-web.creds");
    let device = issue_web(&team, &alice, &out);
    let (client, mut errors) = server.watched_with(&alice, &out).await;

    // It hears the messages to it, those sent under its name, every ant's status and the answers to its renewal.
    let mut inbox = client.subscribe(subject::inbox_of(&alice)).await.unwrap();
    let mut sent = client.subscribe(subject::inbox_from(&alice)).await.unwrap();
    let mut status = client.subscribe(subject::ALL_STATUS).await.unwrap();
    let _renewed = client.subscribe(subject::renew_reply("alice")).await.unwrap();

    // Its own entry among the humans, as a human's first connection puts it, and its assistant grant, which it has
    // none of.
    let humans = format!("KV_{}", ants_proto::HUMANS_BUCKET);
    assert_eq!(last(&client, &humans, &subject::human_entry(&alice)).await, None);
    let entry = serde_json::to_vec(&ants_proto::HumanEntry::new(alice.clone())).unwrap();
    let ack = stored(&client, subject::human_entry(&alice), HeaderMap::new(), &entry).await;
    assert_eq!(ack["stream"], humans.as_str(), "{ack}");
    assert_eq!(last(&client, &humans, &subject::human_entry(&alice)).await.unwrap().0, entry);
    let grant = format!("$KV.{}.{}", ants_proto::HUMANS_BUCKET, ants_proto::assistant_grant_key(&alice));
    assert_eq!(last(&client, &humans, &grant).await, None);

    // A send as `ants ask` makes it: the thread's owner recorded once, then the message under its id, acked by the
    // stream; a repeat of either is a duplicate.
    let ask = Envelope::new(alice.clone(), dev_a.clone(), "hi");
    let key = ants_proto::owner_key(&ask.thread_id, &dev_a);
    let owner = format!("$KV.{}.{key}", ants_proto::OWNERS_BUCKET);
    let recorded = msg_id(&ants_proto::owner_record_id(&key));
    let ack = stored(&client, owner.clone(), recorded.clone(), b"alice").await;
    assert_eq!((ack["stream"].as_str(), ack["duplicate"].as_bool()), (Some("KV_ants_owners"), None), "{ack}");
    let ack = stored(&client, owner.clone(), recorded, b"alice").await;
    assert_eq!(ack["duplicate"], true, "{ack}");
    let ack = stored(&client, owner, HeaderMap::new(), b"bob").await;
    assert!(ack["error"].is_object(), "the first record stays: {ack}");
    let seen = owners::Owners::new(d_js.clone()).recorded(&ask.thread_id, &dev_a).await.unwrap();
    assert_eq!(seen, Some(alice.clone()), "the ant's daemon reads the page's record");
    let payload = serde_json::to_vec(&ask).unwrap();
    let ack = stored(&client, ask.subject(), msg_id(&ask.id.to_string()), &payload).await;
    assert_eq!((ack["stream"].as_str(), ack["duplicate"].as_bool()), (Some(STREAM), None), "{ack}");
    let asked_at = ack["seq"].as_u64().unwrap();
    let again = stored(&client, ask.subject(), msg_id(&ask.id.to_string()), &payload).await;
    assert_eq!((again["seq"].clone(), again["duplicate"].as_bool()), (ack["seq"].clone(), Some(true)), "{again}");
    assert_eq!(&timeout(WAIT, sent.next()).await.unwrap().unwrap().payload[..], &payload[..]);

    // The ant's status and reply come live, and the window read back by direct gets holds both messages.
    let working = subject::status(&dev_a, &ask.thread_id);
    d_client.publish(working.clone(), "working".into()).await.unwrap();
    assert_eq!(timeout(WAIT, status.next()).await.unwrap().unwrap().subject.as_str(), working);
    let reply = ask.reply(dev_a.clone(), "hello", Role::Human).unwrap();
    bus::publish(&d_js, &reply).await.unwrap();
    let heard = timeout(WAIT, inbox.next()).await.unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&heard.payload).unwrap(), reply);
    let read: Vec<Envelope> = window(&client)
        .await
        .iter()
        .filter(|(subject, _)| subject::parse_inbox(subject).is_some())
        .map(|(_, payload)| serde_json::from_slice(payload).unwrap())
        .collect();
    assert_eq!(read, [ask.clone(), reply]);

    // Its read marker, moved by compare-and-set: a stale revision is refused.
    let (read, marker) = (format!("KV_{}", ants_proto::READ_BUCKET), subject::read_marker(&alice));
    assert_eq!(last(&client, &read, &marker).await, None);
    let expecting = |seq: u64| {
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Expected-Last-Subject-Sequence", seq.to_string().as_str());
        headers
    };
    let ack = stored(&client, marker.clone(), expecting(0), b"{\"v\":1}").await;
    assert_eq!(ack["stream"], read.as_str(), "{ack}");
    let (value, revision) = last(&client, &read, &marker).await.unwrap();
    assert_eq!((&value[..], Some(revision)), (&b"{\"v\":1}"[..], ack["seq"].as_u64()));
    let stale = stored(&client, marker.clone(), expecting(0), b"{\"v\":2}").await;
    assert_eq!(stale["error"]["err_code"], 10071, "{stale}");
    let ack = stored(&client, marker.clone(), expecting(revision), b"{\"v\":2}").await;
    assert!(ack["seq"].as_u64() > Some(revision), "{ack}");

    // Its presence, and the request for its renewal, which nobody answers here.
    let presence = ants_proto::presence_key(&alice, &device, ants_proto::Following::Inbox);
    let presence = format!("$KV.{}.{presence}", ants_proto::PRESENCE_BUCKET);
    let ack = stored(&client, presence, HeaderMap::new(), b"{}").await;
    assert_eq!(ack["stream"], "KV_ants_presence", "{ack}");
    client.publish(subject::renew("alice"), "{}".into()).await.unwrap();
    // A round trip: the server refused nothing so far, or its error would have come before this answer.
    assert!(last(&client, &read, &marker).await.is_some());
    assert_eq!(errors.try_recv().ok(), None, "a right the page uses was refused");

    let (m1, thread): (Machine, ThreadId) = ("m1".parse().unwrap(), "t1".parse().unwrap());
    let ops: ChannelId = "ops".parse().unwrap();
    let api = |request: &str| format!("$JS.API.{request}");
    let refused = [
        // No stream or bucket created, updated, purged, deleted from or looked up, and no consumer.
        api("STREAM.CREATE.ANTS"),
        api("STREAM.UPDATE.ANTS"),
        api("STREAM.DELETE.ANTS"),
        api("STREAM.PURGE.ANTS"),
        api("STREAM.MSG.DELETE.ANTS"),
        api("STREAM.MSG.GET.ANTS"),
        api("STREAM.INFO.ANTS"),
        api("INFO"),
        api("CONSUMER.CREATE.ANTS"),
        api(&format!("CONSUMER.CREATE.ANTS.x.{}", subject::inbox_of(&alice))),
        api("STREAM.CREATE.KV_ants_web"),
        api(&format!("STREAM.CREATE.{read}")),
        api(&format!("STREAM.UPDATE.{read}")),
        api(&format!("STREAM.PURGE.{read}")),
        api(&format!("STREAM.MSG.DELETE.{read}")),
        api(&format!("STREAM.UPDATE.{humans}")),
        api("STREAM.PURGE.KV_ants_presence"),
        api("STREAM.CREATE.OBJ_ants_files"),
        // No leaf outbox set up or published in.
        api(&format!("STREAM.CREATE.{}", ants_proto::OUTBOX_STREAM)),
        api(&format!("STREAM.UPDATE.{}", ants_proto::OUTBOX_STREAM)),
        format!("$JS.m1.API.STREAM.CREATE.{}", ants_proto::OUTBOX_STREAM),
        api(&format!("CONSUMER.CREATE.{}.{}", ants_proto::OUTBOX_STREAM, ants_proto::OUTBOX_CONSUMER)),
        api(&format!("STREAM.CREATE.{}", ants_proto::INTAKE_STREAM)),
        api(&format!("STREAM.UPDATE.{}", ants_proto::INTAKE_STREAM)),
        subject::outbox(&ask.subject()).unwrap(),
        ants_proto::owner_outbox(&thread, &dev_a),
        // No channel, human or machine record but its own entry, and no admin order.
        subject::channel_entry(&ops),
        subject::human_entry(&bob),
        grant,
        subject::machine_record(&m1),
        subject::admin_order(&m1, &dev_a),
        // Nothing else of a human's: no other sender, marker or presence, no notes or channel post, no decision.
        subject::inbox(&dev_a, &bob),
        subject::webhook_inbox(&dev_a, &alice),
        subject::notes(&thread, &alice),
        subject::chan(&ops, &alice),
        subject::read_marker(&bob),
        subject::channel_reads(&alice),
        api(&format!("DIRECT.GET.{read}.{}", subject::read_marker(&bob))),
        format!(
            "$KV.{}.{}",
            ants_proto::PRESENCE_BUCKET,
            ants_proto::presence_key(&bob, &device, ants_proto::Following::Inbox)
        ),
        subject::approval_decision(&dev_a, &ants_proto::Uuid::now_v7()),
        subject::cancel(&dev_a, &thread),
        subject::renew("bob"),
        subject::renew("leaf-m1"),
    ];
    for denied in refused {
        client.publish(denied.clone(), "{}".into()).await.unwrap();
        let v = next_violation(&client, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{denied}: {v}");
    }
    let unheard =
        [subject::ALL, ">", "$JS.API.>", "_INBOX.bob.>", "ants.inbox.bob.>", "ants.approvals.*", "ants.chan.>"];
    for denied in unheard {
        let _sub = client.subscribe(denied).await.unwrap();
        let v = next_violation(&client, &mut errors).await;
        assert!(v.contains(&format!("Subscription to \"{denied}\"")), "{denied}: {v}");
    }
    // A refusal ends no connection: the page reads on.
    assert_eq!(last(&client, &read, &marker).await.unwrap().0, b"{\"v\":2}");
    // And the human's other device sees what the page stored.
    let kept = h_js.get_stream_no_info(STREAM).await.unwrap().get_raw_message(asked_at).await.unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&kept.payload).unwrap(), ask);
}

/// The client of the web device of `alice` on `server`, connected as its credentials tell, with its transport and
/// device: the hub set up by another device of hers, whose JetStream context comes last.
async fn page(
    server: &Server,
    domain: Option<&JsDomain>,
) -> (ants_client::client::Client, Rc<NatsTransport>, ants_proto::DeviceId, async_nats::jetstream::Context) {
    let (team, alice) = (server.dir().join("auth"), p("alice"));
    let laptop = server.dir().join("alice.creds");
    let full = Permissions::for_user(&alice, Role::Human).with_js_domain(domain);
    issue_with(&team, &alice, Kind::Human, full, &laptop).unwrap();
    let h_js = connect_to(&server.url, &laptop, domain, &alice).await;
    let rights = crate::people::humans::Rights::from_creds(Some(&laptop)).unwrap();
    ants_daemon::hub::init(&h_js, |s| rights.may_publish(s)).await.unwrap();

    let out = server.dir().join("alice-web.creds");
    let new = Registered { web: true, ..Registered::default() };
    let web = Permissions::for_web(&alice).with_js_domain(domain);
    let device = Issue::new(&team, &alice, Kind::Human, web).with(&new).into_file(&out).unwrap().unwrap();
    let (jwt, key) = read_creds(&out).unwrap();
    let account = account_key(&team).unwrap();
    let credentials = Credentials::read(&jwt, &key.public_key(), Some(&account)).unwrap();
    assert_eq!((&credentials.human, credentials.device, credentials.domain.as_ref()), (&alice, Some(device), domain));
    let session = credentials.session();
    assert_eq!(
        session,
        Session {
            human: alice,
            domain: domain.cloned(),
            account: Some(account),
            name: "ants-web".into(),
            capabilities: Capabilities {
                memories_read: false,
                permissions_version: Some(ants_proto::PERMISSIONS_VERSION),
                ..Capabilities::all()
            },
        }
    );
    let transport = Rc::new(NatsTransport::new(&server.url, Some(&out)));
    let client = ants_client::client::Client::new(transport.clone(), session);
    client.connect().await.unwrap();
    (client, transport, device, h_js)
}

/// Every page of `window`, read to its end: the sequence, sender and text of each entry.
async fn read(client: &ants_client::client::Client, window: Window) -> Vec<(u64, String, String)> {
    let mut reader = client.reader(window);
    let mut entries = Vec::new();
    loop {
        let page = timeout(WAIT, reader.next()).await.expect("no page").unwrap();
        assert_eq!((page.skipped, page.relayed), (0, 0));
        entries.extend(page.entries.into_iter().map(|e| (e.seq, e.from.unwrap().to_string(), e.text)));
        if page.done {
            return entries;
        }
    }
}

/// The client lists the colony, keeps its presence, sends, hears, reads a window and marks read on `server`, whose JetStream is in `domain`, and the
/// server refuses none of it.
async fn the_client_s_reads_and_writes_pass(server: &Server, domain: Option<&JsDomain>) {
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let (client, transport, device, h_js) = page(server, domain).await;
    let ant = server.creds_in(&dev_a, domain);
    let (d_client, d_js) = bus::connect(&server.url, Some(&ant), domain, &dev_a, "test").await.unwrap();

    // The colony, listed by batched direct gets: nothing, then the entries of the ants serving, the last of each.
    assert_eq!(client.colony().await.unwrap(), []);
    bus::ensure_colony(&h_js, ants_proto::COLONY_TTL).await.unwrap();
    assert_eq!(client.colony().await.unwrap(), []);
    let colony = d_js.get_key_value(ants_proto::COLONY_BUCKET).await.unwrap();
    let now = chrono::Utc::now();
    let entries = [colony_entry("dev-a", 60, 5, now), colony_entry("dev-a", 60, 0, now)];
    for entry in &entries {
        colony.put("dev-a", serde_json::to_vec(entry).unwrap().into()).await.unwrap();
    }
    assert_eq!(client.colony().await.unwrap(), entries[1..]);

    // Its presence while open, read by the human's other devices, and gone when it closes.
    let present = crate::people::presence::Present::new(&h_js, domain).await.unwrap();
    let following = async || present.following(&alice, [device], ants_proto::Following::Inbox).await;
    let presence = client.presence(device);
    client.present(&presence).await.unwrap();
    assert_eq!(following().await, [device].into());
    client.absent(&presence).await.unwrap();
    assert_eq!(following().await, [].into());

    // A send to an ant: the thread's owner recorded, which the ant's daemon reads, and the stream's ack.
    let ask = |text: &str, to: Option<&UserId>| ants_client::sending::Send {
        to: to.cloned(),
        text: text.into(),
        ..ants_client::sending::Send::default()
    };
    let sent = client.send(ask("hi", Some(&dev_a))).await.unwrap();
    assert_eq!((&sent.owner_unrecorded, sent.ack.stream.as_str(), sent.ack.duplicate), (&None, STREAM, false));
    assert_eq!(sent.ack.domain.as_deref(), domain.map(JsDomain::as_str));
    let thread = sent.envelope.thread_id.clone();
    let seen = owners::Owners::new(d_js.clone()).recorded(&thread, &dev_a).await.unwrap();
    assert_eq!(seen, Some(alice.clone()));
    // One naming no one: the grant of an assistant read, there is none, so to the messenger.
    let unaddressed = client.send(ask("anyone?", None)).await.unwrap();
    assert_eq!(unaddressed.unaddressed, Some(UserId::messenger().into()));
    assert_eq!((&unaddressed.owner_unrecorded, unaddressed.ack.seq), (&None, sent.ack.seq + 1));

    // The ant's status and its reply, heard live. The marker's read is a round trip: the server has both
    // subscriptions by its answer.
    let mut status = client.status(thread.clone()).await.unwrap();
    let mut inbox = client.inbox().await.unwrap();
    assert_eq!(client.marker().await.unwrap().revision, 0, "no marker yet");
    let working = StatusEvent::new(dev_a.clone(), thread, StatusUpdate::State { state: ants_proto::State::Working });
    d_client.publish(working.subject(), serde_json::to_vec(&working).unwrap().into()).await.unwrap();
    assert_eq!(timeout(WAIT, status.next()).await.expect("no status").unwrap(), working);
    let reply = sent.envelope.reply(dev_a.clone(), "hello", Role::Human).unwrap();
    bus::publish(&d_js, &reply).await.unwrap();
    assert_eq!(timeout(WAIT, inbox.next()).await.expect("no message").unwrap(), reply);

    // The stream read back: all of it, what was stored in the last hour, and what came before the reply.
    let replied = sent.ack.seq + 2;
    let all = read(&client, Window::default()).await;
    let texts: Vec<(u64, &str, &str)> = all.iter().map(|(seq, from, text)| (*seq, &**from, &**text)).collect();
    let expected = [(sent.ack.seq, "alice", "hi"), (sent.ack.seq + 1, "alice", "anyone?"), (replied, "dev-a", "hello")];
    assert_eq!(texts, expected);
    let since = Some(chrono::Utc::now() - chrono::Duration::hours(1));
    assert_eq!(read(&client, Window { since, ..Window::default() }).await, all);
    let before = Window { until_seq: Some(replied - 1), ..Window::default() };
    assert_eq!(read(&client, before).await, all[..2]);
    let on = Window { from_seq: Some(replied), ..Window::default() };
    assert_eq!(read(&client, on).await, all[2..]);

    // The reply marked read by compare-and-set, as stored, and unread again.
    let marked = client.mark_read(MarkRead { seqs: vec![replied], ..MarkRead::default() }).await.unwrap();
    assert!(marked.revision > 0 && marked.marker.is_read(replied), "{marked:?}");
    assert_eq!(client.marker().await.unwrap(), marked);
    let unmarked = client.mark_unread(vec![replied]).await.unwrap();
    assert!(unmarked.revision > marked.revision && !unmarked.marker.is_read(replied), "{unmarked:?}");

    // A message stored with the stream's headers under other cases of their names, and a time under the stream's
    // own: read where and when the stream has it, from who its subject says.
    let decoyed = reply.reply(dev_a.clone(), "again", Role::Human).unwrap();
    let mut headers = msg_id(&decoyed.id.to_string());
    for (name, value) in [
        ("nats-stream", "KV_ants_read"),
        ("NATS-STREAM", "KV_ants_read"),
        ("nats-subject", "ants.inbox.alice.dev-b"),
        ("Nats-subject", "ants.inbox.alice.dev-b"),
        ("nats-sequence", "1"),
        ("NATS-Sequence", "2"),
        ("nats-time-stamp", "2020-01-01T00:00:00Z"),
        ("Nats-Time-Stamp", "2020-01-01T00:00:00Z"),
    ] {
        headers.append(name, value);
    }
    let payload = serde_json::to_vec(&decoyed).unwrap();
    let ack = d_js.publish_with_headers(decoyed.subject(), headers, payload.into()).await.unwrap().await.unwrap();
    let mut reader = client.reader(Window { from_seq: Some(ack.sequence), ..Window::default() });
    let page = timeout(WAIT, reader.next()).await.expect("no page").unwrap();
    assert_eq!((page.entries.len(), page.skipped, page.relayed, page.done), (1, 0, 0, true));
    let entry = &page.entries[0];
    assert_eq!((entry.seq, entry.from.as_ref(), entry.text.as_str()), (ack.sequence, Some(&dev_a), "again"));
    assert!(entry.stored_at > chrono::Utc::now() - chrono::Duration::hours(1), "{}", entry.stored_at);

    Box::pin(the_teams_pass(&client, &h_js, domain)).await;

    // The server answers a connection in order: the refusal of this publish is the first it sent, so it refused
    // nothing the client did.
    let denied = subject::read_marker(&bob);
    transport.publish(Message::new(denied.clone(), "{}")).await.unwrap();
    crate::test_support::eventually("the refusal", async || !transport.server_errors().is_empty()).await;
    let errors = transport.server_errors();
    assert!(errors.len() == 1 && errors[0].contains(&format!("Publish to \"{denied}\"")), "{errors:?}");
}

/// The client of a web device reads the team directory and creates, changes and deletes a team in it by
/// compare-and-set, refused as `ants team` refuses, with the rights of its credentials; `h_js` is its human's other
/// device, in `domain`.
async fn the_teams_pass(
    client: &ants_client::client::Client,
    h_js: &async_nats::jetstream::Context,
    domain: Option<&JsDomain>,
) {
    use ants_client::teams::TeamError;
    use ants_client::teams::change::{Change, Create};
    use ants_daemon::records::direct_kv::DirectKv;
    use ants_proto::{TEAMS_BUCKET, TeamDirectory, TeamRefusal};

    let (dev_a, dev_b) = (p("dev-a"), p("dev-b"));
    let caps = client.capabilities();
    assert!(caps.teams_read && caps.teams_write && !caps.stale_permissions, "{caps:?}");
    assert_eq!(client.teams().await.unwrap().directory, TeamDirectory::default(), "no directory yet");
    let identities = DirectKv::whole(h_js, domain, ants_proto::IDENTITIES_BUCKET).await.unwrap();
    for ant in [&dev_a, &dev_b] {
        identities.put(ant.as_str(), b"{}".to_vec()).await.unwrap();
    }
    let create = |team: &str, members: &[&str], owner: Option<&str>| Create {
        team: p(team),
        lead: members.first().map(|m| p(m)),
        members: members.iter().map(|m| p(m)).collect(),
        owner: owner.map(p),
        description: Some("on call".to_owned()),
    };

    let created = client.create_team(create("ops", &["dev-a"], Some("alice"))).await.unwrap();
    let ops = created.directory.directory.team(&p("ops")).unwrap();
    assert_eq!((&ops.members[..], ops.lead.as_ref()), (&[dev_a.clone()][..], Some(&dev_a)), "{:?}", created.lines);
    assert_eq!(client.teams().await.unwrap(), created.directory, "read back at the revision written");
    let teams = DirectKv::whole(h_js, domain, TEAMS_BUCKET).await.unwrap();
    let (stored, _) = ants_daemon::teams::read_checked(&teams).await.unwrap();
    assert_eq!(stored, created.directory.directory, "as the human's other devices read it");

    let refused = |e: TeamError| match e {
        TeamError::Refused(refusal) => refusal,
        other => panic!("not a refusal: {other}"),
    };
    let unknown = client.create_team(create("dev", &["dev-z"], None)).await.unwrap_err();
    assert_eq!(refused(unknown), TeamRefusal::UnknownAnt { name: p("dev-z") });
    let taken = client.create_team(create("dev-b", &[], None)).await.unwrap_err();
    assert_eq!(refused(taken), TeamRefusal::NameTaken { name: dev_b.clone() });
    let on_a_team = client.create_team(create("dev", &["dev-a"], None)).await.unwrap_err();
    assert_eq!(refused(on_a_team), TeamRefusal::OnTeam { member: dev_a.clone(), team: p("ops") });
    let a_human = client.create_team(create("dev", &["alice"], None)).await.unwrap_err();
    assert!(matches!(&a_human, TeamError::Invalid(why) if why.starts_with("alice is a human")), "{a_human}");
    let no_owner = client.create_team(create("dev", &[], Some("bob"))).await.unwrap_err();
    assert!(matches!(&no_owner, TeamError::Invalid(why) if why.starts_with("bob is no human")), "{no_owner}");

    let change = Change { add: vec![dev_b.clone()], lead: Some(Some(dev_b.clone())), ..Change::default() };
    let changed = client.change_team(p("ops"), change).await.unwrap();
    assert_eq!(&changed.added[..], &[dev_b.clone()][..]);
    assert_eq!(changed.lines, ["team ops: added dev-b, lead now dev-b"]);
    assert!(changed.directory.revision > created.directory.revision);
    let deleted = client.delete_team(p("ops")).await.unwrap();
    assert_eq!(deleted.directory.directory, TeamDirectory::default());
    assert_eq!(refused(client.delete_team(p("ops")).await.unwrap_err()), TeamRefusal::NoSuchTeam { team: p("ops") });

    // A directory breaking the rules, put by another writer: read as unreadable, and left as it is.
    let mut broken = TeamDirectory::default();
    for team in ["ops", "dev"] {
        let mut team = ants_proto::Team::unled(p(team), p("alice"));
        team.members.push(dev_a.clone());
        broken.teams.push(team);
    }
    teams.put(ants_proto::TEAM_DIRECTORY_KEY, serde_json::to_vec(&broken).unwrap()).await.unwrap();
    assert!(matches!(refused(client.teams().await.unwrap_err()), TeamRefusal::Unreadable { .. }));
    let blocked = client.change_team(p("ops"), Change { remove: vec![dev_a.clone()], ..Change::default() }).await;
    assert!(matches!(blocked.map_err(refused), Err(TeamRefusal::Unreadable { .. })));
    teams.delete(ants_proto::TEAM_DIRECTORY_KEY).await.unwrap();
    assert_eq!(client.teams().await.unwrap().directory, TeamDirectory::default(), "a deleted directory is empty");
}

#[tokio::test]
async fn the_client_sends_reads_marks_read_and_hears_with_web_credentials() {
    let Some(server) = Server::start() else { return };
    Box::pin(the_client_s_reads_and_writes_pass(&server, None)).await;
}

#[tokio::test]
async fn the_client_does_the_same_in_a_hub_s_domain() {
    let Some(server) = Server::start_with("domain: hub", "") else { return };
    Box::pin(the_client_s_reads_and_writes_pass(&server, Some(&"hub".parse().unwrap()))).await;
}

#[tokio::test]
async fn a_web_device_reaches_a_hub_s_domain_with_the_same_rights() {
    let Some(server) = Server::start_with("domain: hub", "") else { return };
    let team = server.dir().join("auth");
    let (alice, hub): (UserId, JsDomain) = (p("alice"), "hub".parse().unwrap());
    let laptop = server.dir().join("alice.creds");
    let full = Permissions::for_user(&alice, Role::Human).with_js_domain(Some(&hub));
    issue_with(&team, &alice, Kind::Human, full, &laptop).unwrap();
    let h_js = connect_to(&server.url, &laptop, Some(&hub), &alice).await;
    bus::ensure_stream(&h_js).await.unwrap();
    crate::messages::inbox::ensure_bucket(&h_js).await.unwrap();

    let out = server.dir().join("alice-web.creds");
    let new = Registered { web: true, ..Registered::default() };
    let web = Permissions::for_web(&alice).with_js_domain(Some(&hub));
    Issue::new(&team, &alice, Kind::Human, web).with(&new).into_file(&out).unwrap();
    let finding = Finding::from_creds(&std::fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!((finding.web, finding.js_domain.as_ref()), (true, Some(&hub)));
    let (client, mut errors) = server.watched_with(&alice, &out).await;

    let marker = subject::read_marker(&alice);
    let put = format!("$JS.hub.API.{marker}");
    let ack = stored(&client, put, HeaderMap::new(), b"{}").await;
    assert_eq!((ack["stream"].as_str(), ack["domain"].as_str()), (Some("KV_ants_read"), Some("hub")), "{ack}");
    let get = client.request(format!("$JS.hub.API.DIRECT.GET.KV_ants_read.{marker}"), "".into());
    assert_eq!(&timeout(WAIT, get).await.unwrap().unwrap().payload[..], b"{}");
    let ask = Envelope::new(alice.clone(), p("dev-a"), "hi");
    let ack = stored(&client, ask.subject(), msg_id(&ask.id.to_string()), &serde_json::to_vec(&ask).unwrap()).await;
    assert_eq!(ack["stream"], STREAM, "{ack}");
    assert_eq!(errors.try_recv().ok(), None);
    for denied in
        ["$JS.hub.API.STREAM.INFO.ANTS", "$JS.hub.API.STREAM.UPDATE.KV_ants_read", "$JS.hub.API.$KV.ants_read.bob"]
    {
        client.publish(denied, "{}".into()).await.unwrap();
        let v = next_violation(&client, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{denied}: {v}");
    }
}

#[test]
fn web_credentials_carry_the_tag_never_the_admin_role_nor_an_encryption_key_and_renewal_keeps_them() {
    use ants_daemon::secrets::creds;
    let dir = ants_test_support::scratch_dir("web-creds");
    let team = dir.join("auth");
    init(&team).unwrap();
    let alice = p("alice");
    let web = || Permissions::for_web(&alice);
    let new = || KeyPair::new_user().public_key();
    let out = dir.join("alice-web.creds");

    let device = issue_web(&team, &alice, &out);
    let (jwt, key) = read_creds(&out).unwrap();
    let body = verified_body(&jwt).unwrap().1;
    let tags: Vec<&str> = body["nats"]["tags"].as_array().unwrap().iter().filter_map(|t| t.as_str()).collect();
    assert_eq!(tags, [device.tag().as_str(), "ants-web", PERMISSIONS_VERSION.tag().as_str()]);
    assert_eq!(allowed(&body), web());
    let finding = creds::Finding::from_jwt(&jwt).unwrap();
    assert_eq!((&finding.kind, finding.web, finding.admin, &finding.xkey), (&CredsKind::Human, true, false, &None));
    assert_eq!(finding.holder(), "human alice (web device)");
    assert!(creds::xkey_of(&std::fs::read_to_string(&out).unwrap()).is_none(), "a web device holds no xkey");
    let record = issued::issued(&team).unwrap().pop().unwrap();
    assert_eq!((record.web, record.admin, record.xkey, record.device), (true, false, None, Some(device)));
    let account = account_key(&team).unwrap();
    let signature = BASE64URL_NOPAD.encode(&key.sign(b"an order").unwrap());
    let refused = ants_proto::verify_admin(&jwt, [account.as_str()], b"an order", &signature, now().unwrap());
    assert_eq!(refused.unwrap_err(), ants_proto::JwtError::NotAdmin(alice.clone()));

    // Never with the admin role, an encryption key, or for another than a human.
    let admin = Registered { web: true, admin: true, ..Registered::default() };
    let err = Issue::new(&team, &alice, Kind::Human, web()).with(&admin).for_key(&new(), None).unwrap_err().to_string();
    assert!(err.contains("never carry the admin role"), "{err}");
    let plain = Registered { web: true, ..Registered::default() };
    let xkey = device_key(&XKey::new()).unwrap();
    let err = Issue::new(&team, &alice, Kind::Human, web())
        .with(&plain)
        .for_key(&new(), Some(&xkey))
        .unwrap_err()
        .to_string();
    assert!(err.contains("bind no encryption key"), "{err}");
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    let err =
        Issue::new(&team, &p("dev-a"), Kind::Ant, ant).with(&plain).for_key(&new(), None).unwrap_err().to_string();
    assert!(err.contains("only a human's device"), "{err}");
    assert_eq!(issued::issued(&team).unwrap().len(), 1, "nothing refused was recorded");

    // A renewal keeps the device, its label, the tag and the rights, and binds no key.
    let err = renew(&team, &jwt, &new(), Some(&xkey)).unwrap_err().to_string();
    assert!(err.contains("a web device's, which bind no encryption key"), "{err}");
    let next = new();
    let renewed = renew(&team, &jwt, &next, None).unwrap();
    assert!(renewed.finding.web && !renewed.finding.admin && renewed.finding.xkey.is_none());
    assert!(finding.same_credentials(&renewed.finding));
    assert_eq!((renewed.finding.device, renewed.finding.key.as_str()), (Some(device), next.as_str()));
    assert_eq!(allowed(&verified_body(&renewed.jwt).unwrap().1), web());
    let record = issued::issued(&team).unwrap().pop().unwrap();
    assert_eq!((record.web, record.key.as_str(), record.device), (true, next.as_str(), Some(device)));
    assert_eq!((record.label.as_deref(), record.renewed_from), (Some("browser"), Some(key.public_key())));
    // And the lifetime: as long from now as the earlier credentials had from their issue.
    let week = 7 * 86_400;
    let expiring = Registered { web: true, expires_at: Some(now().unwrap() + week), ..Registered::default() };
    let (expiring, _) = Issue::new(&team, &alice, Kind::Human, web()).with(&expiring).for_key(&new(), None).unwrap();
    let phone = new();
    let exp = verified(&renew(&team, &expiring, &phone, None).unwrap().jwt).unwrap().exp;
    assert!(exp.is_some_and(|exp| exp.abs_diff(now().unwrap() + week) <= 2), "{exp:?}");

    // `ants auth devices` marks them, beside the human's other devices.
    let human = Permissions::for_user(&alice, Role::Human);
    Issue::new(&team, &alice, Kind::Human, human).labelled(Some("laptop")).into_file(&dir.join("alice.creds")).unwrap();
    let devices = devices::devices(&team, "alice").unwrap();
    let listed: Vec<(Option<&str>, bool)> = devices.iter().map(|d| (d.label.as_deref(), d.web)).collect();
    assert_eq!(listed, [(Some("browser"), true), (None, true), (Some("laptop"), false)]);
    assert_eq!(serde_json::to_value(&devices[0]).unwrap()["web"], true);
    assert!(serde_json::to_value(&devices[2]).unwrap().get("web").is_none());
    assert_eq!((devices::marks(&devices[0]), devices::marks(&devices[2])), ("  web".to_owned(), String::new()));
    let listing = crate::credentials::listing::listing(&team, now().unwrap()).unwrap();
    let expiring = listing.iter().find(|l| l.issued.key == phone).unwrap();
    assert!(expiring.expiring && expiring.issued.web, "{expiring:?}");
    assert_eq!(expiring.renew, ["ants auth user alice --human --web --public-key KEY [--js-domain DOMAIN]"]);
    std::fs::remove_dir_all(dir).unwrap();
}
