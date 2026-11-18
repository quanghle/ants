//! What was done with the ant's threads besides writing on them, for its review packets ([`Action`]): a thread's
//! marking done or explicitly undone, read by direct get as the ant's credentials allow, and the first message, note
//! or channel post stored after it was marked done ([`history::Active::reopenings`]), which reopened it; the ant's
//! tool requests rejected, from the timeline it reads back ([`history::Active::rejections`]); and the reactions to its
//! messages and channel posts, asked as `list_reactions` asks them ([`ReactionAsker`]). Each is read within [`TIMEOUT`]
//! in all, and left out when it fails.

use ants_proto::{
    DecidedBy, JsDomain, MergedRejection, ReactionsOutcome, ReactionsRequest, THREAD_UNDONE, THREADS_BUCKET,
    ThreadDone, ThreadId, UserId, done_key,
};
use ants_sense::dream::Action;
use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use tokio::time::{Duration, Instant, timeout_at};

use crate::mcp::ReactionAsker;
use crate::records::direct_kv::{DirectKv, Lookup};
#[cfg(doc)]
use crate::stream::history;
use crate::stream::history::{Reopening, Sent};

/// How long reading what was done with the threads of a packet may take in all.
const TIMEOUT: Duration = Duration::from_secs(30);
/// The most of the ant's latest messages and channel posts on a thread whose reactions are asked for.
const REACTED: usize = 8;

/// Reads what was done with each thread of a packet. A source that fails, answers nothing or runs out of time is read
/// no more: its lines are left out of the threads after as well.
pub(super) struct Actions<'a> {
    ant: &'a UserId,
    /// The threads' done states; none once a read failed.
    done: Option<DirectKv>,
    /// Asks for the reactions to a message; none once a request failed.
    reactions: Option<&'a ReactionAsker>,
    deadline: Instant,
}

impl<'a> Actions<'a> {
    /// Reading as `ant` on `js`, made for `js_domain`, and asking `reactions`.
    pub(super) async fn new(
        js: &jetstream::Context,
        js_domain: Option<&JsDomain>,
        ant: &'a UserId,
        reactions: Option<&'a ReactionAsker>,
    ) -> Self {
        let deadline = Instant::now() + TIMEOUT;
        let done = within(deadline, DirectKv::new(js, js_domain, THREADS_BUCKET)).await;
        let done = done.inspect_err(|e| tracing::warn!(error = e, "reading when threads were marked done")).ok();
        Self { ant, done, reactions, deadline }
    }

    /// What was done with thread `id` from `since` to `until` besides writing on it: the tool requests `rejections` of
    /// the ant rejected, the reactions to `sent`, its messages and channel posts on it, and its marking done, then the
    /// first of `reopenings`, the entries read back on it, stored after it.
    pub(super) async fn of(
        &mut self,
        id: &ThreadId,
        sent: &[Sent],
        rejections: &[MergedRejection],
        reopenings: &[Reopening],
        (since, until): (DateTime<Utc>, DateTime<Utc>),
    ) -> Vec<Action> {
        let between = |at: &DateTime<Utc>| (since..=until).contains(at);
        let mut actions: Vec<Action> = rejections.iter().filter(|r| between(&r.at)).map(rejected).collect();
        actions.extend(self.reactions(sent).await);
        if let Some(mark) = self.done(id).await {
            match mark {
                Mark::Done(done) if between(&done.at) => {
                    actions.push(done_by(&done));
                    if let Some(reopening) = reopenings.iter().find(|reopening| !done.holds(reopening.seq)) {
                        let article = if reopening.kind.starts_with("unreadable ") { "an" } else { "a" };
                        let by = format!("{article} {} from {}", reopening.kind, reopening.from);
                        actions.push(Action::Reopened { at: reopening.at.into(), by });
                    }
                }
                Mark::Undone(at) if between(&at) => actions.push(Action::Undone { at }),
                _ => {}
            }
        }
        actions
    }

    /// The reactions to the latest of `sent` but the ant's own, oldest first.
    async fn reactions(&mut self, sent: &[Sent]) -> Vec<Action> {
        let mut actions = Vec::new();
        for message in &sent[sent.len().saturating_sub(REACTED)..] {
            let Some(asker) = self.reactions else { break };
            let asked = within(self.deadline, asker(ReactionsRequest::new(self.ant.clone(), message.id))).await;
            let reactions = match asked {
                Ok(Some(ReactionsOutcome::Listed { reactions })) => reactions,
                Ok(Some(ReactionsOutcome::Unknown | ReactionsOutcome::Applied { .. })) => continue,
                Ok(None) => {
                    tracing::warn!("no answer about the reactions to the ant's messages: none in its review packet");
                    self.reactions = None;
                    continue;
                }
                Err(error) => {
                    tracing::warn!(error, "asking for the reactions to the ant's messages: none in its review packet");
                    self.reactions = None;
                    continue;
                }
            };
            for reacted in reactions {
                for (by, label) in reacted.by.iter().zip(reacted.labels()) {
                    if by != self.ant {
                        let (reaction, opening) = (reacted.reaction.to_string(), message.opening.clone());
                        actions.push(Action::Reaction { reaction, by: label, message: message.id, opening });
                    }
                }
            }
        }
        actions
    }

    /// The latest done or explicit undo mark of thread `id`, if it has one.
    async fn done(&mut self, id: &ThreadId) -> Option<Mark> {
        let kv = self.done.as_ref()?;
        match within(self.deadline, kv.lookup_marked(done_key(id), THREAD_UNDONE)).await {
            Ok((Lookup::Value(value), _)) => {
                serde_json::from_slice::<ThreadDone>(&value).ok().filter(|d| d.thread_id == *id).map(Mark::Done)
            }
            Ok((Lookup::NoValue | Lookup::NoBucket, undone)) => undone.map(|at| Mark::Undone(at.into())),
            Err(error) => {
                tracing::warn!(thread = %id, error, "reading when a thread was marked done: none in its review packet");
                self.done = None;
                None
            }
        }
    }
}

enum Mark {
    Done(ThreadDone),
    Undone(DateTime<Utc>),
}

/// What `read` comes to by `deadline`, its error or lateness as a reason.
async fn within<T>(deadline: Instant, read: impl Future<Output = Result<T>>) -> Result<T, String> {
    match timeout_at(deadline, read).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(format!("{e:#}")),
        Err(_) => Err("out of time".to_owned()),
    }
}

/// The ant's tool request `rejection` rejected, and by whom: none when it expired unanswered.
fn rejected(rejection: &MergedRejection) -> Action {
    let MergedRejection { rejection, at, .. } = rejection;
    let by = match (rejection.by, &rejection.decider, &rejection.via) {
        (DecidedBy::Expiry, _, _) => None,
        (DecidedBy::Human, Some(human), Some(assistant)) => Some(format!("human {human} via {assistant}")),
        (DecidedBy::Human, Some(human), None) => Some(format!("human {human}")),
        (DecidedBy::Human, None, _) => Some("a human".to_owned()),
        (DecidedBy::Chain, Some(ant), _) => Some(format!("ant {ant}")),
        (DecidedBy::Chain, None, _) => Some("an ant of its escalation chain".to_owned()),
    };
    let tool = match rejection.kind.as_str() {
        "" => rejection.title.clone(),
        kind => format!("{kind}: {}", rejection.title),
    };
    Action::Rejected { at: *at, tool: tool.replace(['\n', '\r'], " "), by }
}

/// The marking done `done`, by its marker, for the human an assistant claims to have marked it for.
fn done_by(done: &ThreadDone) -> Action {
    let by = match &done.on_behalf_of {
        Some(human) => format!("{} for {human}", done.by),
        None => done.by.to_string(),
    };
    Action::Done { at: done.at, by }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ants_proto::provider::{Origin, Starter};
    use ants_proto::{ChannelPost, Envelope, Reacted, SESSIONS_BUCKET, TimelineRejection, Uuid, subject};
    use ants_test_support::NatsServer;
    use chrono::TimeDelta;

    use super::*;
    use crate::bus;
    use crate::provider::origins::OriginStore;
    use crate::stream::history::{self, ReviewRecords};
    use crate::stream::reading::Reading;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn sent(n: u128, opening: &str) -> Sent {
        Sent { id: Uuid::from_u128(n), opening: opening.to_owned() }
    }

    fn rejection(ant: &UserId, thread: &ThreadId, at: DateTime<Utc>, by: DecidedBy) -> MergedRejection {
        let decider = (by != DecidedBy::Expiry).then(|| p("alice"));
        let (kind, title) = ("execute".to_owned(), "rm -rf\ntarget".to_owned());
        let rejection = TimelineRejection { request: Uuid::now_v7(), kind, title, by, decider, via: None };
        MergedRejection { ant: ant.clone(), thread_id: thread.clone(), message: Uuid::nil(), rejection, at }
    }

    #[tokio::test]
    async fn a_failing_reactions_request_leaves_out_the_reactions_and_keeps_the_rest() {
        let (ant, thread, now) = (p("dev-a"), ThreadId::generate(), Utc::now());
        let asked = Arc::new(AtomicUsize::new(0));
        let asker: ReactionAsker = Arc::new({
            let asked = asked.clone();
            move |request: ReactionsRequest| {
                asked.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    match request.message.as_u128() {
                        1 => {
                            let thumbs = Reacted::new("thumbsup".parse()?, vec![p("bob"), p("dev-a")]);
                            Ok(Some(ReactionsOutcome::Listed { reactions: vec![thumbs] }))
                        }
                        2 => Ok(Some(ReactionsOutcome::Unknown)),
                        _ => anyhow::bail!("hub unreachable"),
                    }
                })
            }
        });
        let mut actions =
            Actions { ant: &ant, done: None, reactions: Some(&asker), deadline: Instant::now() + TIMEOUT };
        let period = (now - TimeDelta::hours(1), now);
        let rejections = [
            rejection(&ant, &thread, now - TimeDelta::hours(2), DecidedBy::Human),
            rejection(&ant, &thread, now - TimeDelta::minutes(30), DecidedBy::Human),
            rejection(&ant, &thread, now - TimeDelta::minutes(20), DecidedBy::Expiry),
        ];
        let messages = [sent(1, "fixed the build"), sent(2, "a message gone"), sent(3, "lost"), sent(4, "never asked")];

        let got = actions.of(&thread, &messages, &rejections, &[], period).await;
        let tool = "execute: rm -rf target".to_owned();
        let expected = vec![
            Action::Rejected { at: rejections[1].at, tool: tool.clone(), by: Some("human alice".into()) },
            Action::Rejected { at: rejections[2].at, tool: tool.clone(), by: None },
            Action::Reaction {
                reaction: "thumbsup".into(),
                by: "bob".into(),
                message: Uuid::from_u128(1),
                opening: "fixed the build".into(),
            },
        ];
        assert_eq!(got, expected, "the ant's own reaction and the rejection before the period left out");
        assert_eq!(asked.load(Ordering::SeqCst), 3, "no request after the failing one");

        let got = actions.of(&thread, &[sent(1, "fixed the build")], &rejections[1..2], &[], period).await;
        assert_eq!(got, expected[..1], "the reactions asked no more once a request failed");
        assert_eq!(asked.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn a_rejection_names_who_rejected_the_tool_request() {
        let (ant, thread, now) = (p("dev-a"), ThreadId::generate(), Utc::now());
        let by = |by, via: Option<&str>| {
            let mut rejection = rejection(&ant, &thread, now, by);
            rejection.rejection.via = via.map(p);
            match super::rejected(&rejection) {
                Action::Rejected { by, .. } => by,
                other => panic!("{other:?}"),
            }
        };
        assert_eq!(by(DecidedBy::Human, Some("asst")).as_deref(), Some("human alice via asst"));
        assert_eq!(by(DecidedBy::Chain, None).as_deref(), Some("ant alice"));
        assert_eq!(by(DecidedBy::Expiry, None), None);
    }

    #[tokio::test]
    async fn a_thread_marked_done_in_the_period_shows_who_marked_it() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        js.create_stream(crate::hub::threads_config()).await.unwrap();
        let threads = js.get_key_value(THREADS_BUCKET).await.unwrap();
        let (ant, done, open) = (p("dev-a"), ThreadId::generate(), ThreadId::generate());
        let marked = ThreadDone::new(done.clone(), p("asst"), 7).on_behalf(Some(p("alice")));
        threads.put(done_key(&done), serde_json::to_vec(&marked).unwrap().into()).await.unwrap();
        let mut actions = Actions::new(&js, None, &ant, None).await;
        let period = (marked.at - TimeDelta::hours(1), marked.at + TimeDelta::minutes(1));

        let got = actions.of(&done, &[sent(1, "done")], &[], &[], period).await;
        let marked_done = Action::Done { at: marked.at, by: "asst for alice".into() };
        assert_eq!(got, std::slice::from_ref(&marked_done));
        assert_eq!(actions.of(&open, &[], &[], &[], period).await, []);
        let before = (period.0 - TimeDelta::hours(1), period.0);
        assert_eq!(actions.of(&done, &[], &[], &[], before).await, [], "marked done after the period");

        let reopening = |seq, minutes, from: &str, kind| Reopening {
            seq,
            at: (marked.at + TimeDelta::minutes(minutes)).into(),
            from: p(from),
            kind,
        };
        let seen = [reopening(7, -5, "dev-a", "message")];
        let got = actions.of(&done, &[], &[], &seen, period).await;
        assert_eq!(got, std::slice::from_ref(&marked_done), "seen when marked done");
        let reopened = [seen[0].clone(), reopening(8, 3, "bob", "channel post"), reopening(9, 4, "dev-a", "message")];
        let got = actions.of(&done, &[], &[], &reopened, period).await;
        let by = "a channel post from bob".to_owned();
        assert_eq!(got, [marked_done, Action::Reopened { at: marked.at + TimeDelta::minutes(3), by }]);
    }

    #[tokio::test]
    async fn review_distinguishes_explicit_undo_from_an_absent_or_pruned_done_mark() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        js.create_stream(crate::hub::threads_config()).await.unwrap();
        let kv = DirectKv::new(&js, None, THREADS_BUCKET).await.unwrap();
        let (ant, undone, pruned, never) =
            (p("dev-a"), ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let since = Utc::now();
        for id in [&undone, &pruned] {
            let key = done_key(id);
            let marked = ThreadDone::new(id.clone(), p("alice"), 7);
            kv.put(&key, serde_json::to_vec(&marked).unwrap()).await.unwrap();
            let (_, last) = kv.lookup_last(&key).await.unwrap();
            let deleted = if *id == undone {
                kv.delete_with_marker(&key, last, Some(bus::RETENTION), THREAD_UNDONE).await.unwrap()
            } else {
                kv.delete_expecting(&key, last, Some(Duration::from_secs(1))).await.unwrap()
            };
            assert!(deleted);
        }
        let mut actions = Actions::new(&js, None, &ant, None).await;
        let period = (since, Utc::now() + TimeDelta::seconds(1));
        let got = actions.of(&undone, &[], &[], &[], period).await;
        assert!(matches!(got.as_slice(), [Action::Undone { at }] if (period.0..=period.1).contains(at)));
        for id in [&pruned, &never] {
            assert_eq!(actions.of(id, &[], &[], &[], period).await, []);
        }
        assert_eq!(actions.of(&undone, &[], &[], &[], (since - TimeDelta::hours(1), since)).await, []);
    }

    #[tokio::test]
    async fn an_unreadable_entry_reopens_a_thread_only_with_matching_metadata() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        js.create_stream(crate::hub::stream_config()).await.unwrap();
        js.create_stream(crate::hub::threads_config()).await.unwrap();
        js.create_key_value(crate::hub::kept_config(SESSIONS_BUCKET)).await.unwrap();
        let (ant, alice, bob) = (p("dev-a"), p("alice"), p("bob"));
        let note_thread = Envelope::new(alice.clone(), ant.clone(), "initial inbox message for notes");
        let inbox = Envelope::new(alice.clone(), ant.clone(), "the original message before the review period");
        let channel = ChannelPost::new("ops".parse().unwrap(), ant.clone(), "initial channel post");
        let kv = DirectKv::new(&js, None, THREADS_BUCKET).await.unwrap();
        let put = async |subject, payload: Vec<u8>| {
            js.publish(subject, payload.into()).await.unwrap().await.unwrap().sequence
        };
        let origin = Origin { by: Starter::Human { user: alice.clone() }, channel: None };
        let origins = OriginStore::kv(DirectKv::new(&js, None, SESSIONS_BUCKET).await.unwrap(), ant.clone());
        origins.put(&inbox.thread_id, &origin).await.unwrap();
        let prior = put(inbox.subject(), serde_json::to_vec(&inbox).unwrap()).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let since = Utc::now();
        for (thread, seq) in [
            (&note_thread.thread_id, put(note_thread.subject(), serde_json::to_vec(&note_thread).unwrap()).await),
            (&inbox.thread_id, prior),
            (&channel.thread_id, put(channel.subject(), serde_json::to_vec(&channel).unwrap()).await),
        ] {
            let done = ThreadDone::new(thread.clone(), alice.clone(), seq);
            kv.put(done_key(thread), serde_json::to_vec(&done).unwrap()).await.unwrap();
        }
        put(inbox.subject(), b"opaque inbox bytes".to_vec()).await;
        put(channel.subject(), b"opaque channel bytes".to_vec()).await;
        let note_seq = put(subject::notes(&note_thread.thread_id, &bob), b"opaque note bytes".to_vec()).await;
        let inbox_meta = |from: &UserId, to: &UserId| {
            serde_json::to_vec(&serde_json::json!({"thread_id": inbox.thread_id, "from": from, "to": to})).unwrap()
        };
        put(inbox.subject(), inbox_meta(&bob, &ant)).await;
        put(inbox.subject(), inbox_meta(&alice, &bob)).await;
        let unknown = serde_json::json!({"thread_id": ThreadId::generate(), "from": alice, "to": ant});
        put(inbox.subject(), serde_json::to_vec(&unknown).unwrap()).await;
        let inbox_seq = put(inbox.subject(), inbox_meta(&alice, &ant)).await;
        let channel_meta = |from: &UserId, room: &str| {
            serde_json::to_vec(&serde_json::json!({"thread_id": channel.thread_id, "channel": room, "from": from}))
                .unwrap()
        };
        put(channel.subject(), channel_meta(&bob, "ops")).await;
        put(channel.subject(), channel_meta(&ant, "wrong-room")).await;
        let channel_seq = put(subject::chan(&channel.channel, &bob), channel_meta(&bob, "ops")).await;
        let until = Utc::now();
        let active = history::active(
            &js,
            &ant,
            Reading::Consumer,
            ReviewRecords { origins: Some(&origins), ..ReviewRecords::default() },
            (since.into(), until.into()),
            4_096,
        )
        .await
        .unwrap();
        assert_eq!(active.len(), 3);
        let mut actions = Actions::new(&js, None, &ant, None).await;
        for (id, seq, expected) in [
            (&note_thread.thread_id, note_seq, "an unreadable thread note from bob"),
            (&inbox.thread_id, inbox_seq, "an unreadable message from alice"),
            (&channel.thread_id, channel_seq, "an unreadable channel post from bob"),
        ] {
            let thread = active.iter().find(|t| t.thread == *id).unwrap();
            assert!(!thread.history.contains("opaque"));
            let got = actions.of(id, &[], &[], &thread.reopenings, (since, until)).await;
            let [Action::Done { .. }, Action::Reopened { at, by }] = got.as_slice() else { panic!("{got:?}") };
            assert_eq!(by, expected);
            assert_eq!(*at, DateTime::<Utc>::from(thread.reopenings.iter().find(|r| r.seq == seq).unwrap().at));
            let count = if *id == inbox.thread_id { 1 } else { 2 };
            assert_eq!(thread.reopenings.len(), count, "only the attributed unreadable entry: {:?}", thread.reopenings);
        }
    }
}
