//! The tools of an ant that is a human's assistant, standing while the grant names it.

use super::*;

/// A reaction: who reacted, to which message, with what, for whom.
type Reacted = (UserId, Uuid, Reaction, Option<UserId>);

/// What the assistant tools read and write, in memory.
#[derive(Default)]
struct MemoryHuman {
    messages: StdMutex<Vec<HumanMessage>>,
    requests: StdMutex<Vec<ants_proto::ApprovalRequest>>,
    titles: StdMutex<Vec<ThreadTitle>>,
    done: StdMutex<Vec<ants_proto::ThreadDone>>,
    reactions: StdMutex<Vec<Reacted>>,
    owners: StdMutex<Vec<(ThreadId, UserId, UserId, UserId)>>,
    /// The messages the ants' daemons hold, those awaiting redelivery or still in the stream, and the requests they
    /// answered.
    held: StdMutex<Vec<ants_proto::QueuedMessage>>,
    unheld: StdMutex<Vec<crate::queue::Waiting>>,
    cancels: StdMutex<Vec<ants_proto::CancelRequest>>,
    queues: StdMutex<Vec<ants_proto::QueueRequest>>,
    /// Whether no daemon of the ants serves, and the withdrawals recorded meanwhile, with the mark each one found.
    unserved: StdMutex<bool>,
    withdrawals: StdMutex<Vec<(UserId, Uuid, UserId, UserId)>>,
    found_mark: StdMutex<Option<ants_proto::MessageMark>>,
    orders: StdMutex<Vec<ants_proto::AdminOrder>>,
    /// The human's own marks and snoozes, and those its assistant set for it.
    own_marks: StdMutex<ants_proto::ThreadMarks>,
    own_snoozes: StdMutex<ants_proto::ThreadSnoozes>,
    marks: StdMutex<ants_proto::ThreadMarks>,
    snoozes: StdMutex<ants_proto::ThreadSnoozes>,
}

impl MemoryHuman {
    fn store(&self, envelope: Envelope) {
        let mut messages = self.messages.locked();
        let seq = messages.len() as u64 + 1;
        messages.push(HumanMessage { seq, envelope });
    }
}

impl HumanStore for MemoryHuman {
    fn messages(
        &self,
        human: UserId,
        assistant: UserId,
        _: std::time::SystemTime,
    ) -> BoxFuture<'_, Result<Vec<HumanMessage>>> {
        let involved = |e: &Envelope| {
            e.to == human || e.from == human || (e.from == assistant && e.on_behalf_of.as_ref() == Some(&human))
        };
        let found = self.messages.locked().iter().filter(|m| involved(&m.envelope)).cloned().collect();
        Box::pin(async move { Ok(found) })
    }

    fn approvals(
        &self,
        _: std::time::SystemTime,
    ) -> BoxFuture<'_, Result<(Vec<ants_proto::ApprovalRequest>, std::collections::HashSet<Uuid>)>> {
        let requests = self.requests.locked().clone();
        Box::pin(async move { Ok((requests, Default::default())) })
    }

    fn read_marker(&self, _: UserId) -> BoxFuture<'_, Result<ants_proto::ReadMarker>> {
        Box::pin(async { Ok(ants_proto::ReadMarker::default()) })
    }

    fn done(&self, thread: ThreadId) -> BoxFuture<'_, Result<Option<ants_proto::ThreadDone>>> {
        let done = self.done.locked().iter().rfind(|d| d.thread_id == thread).cloned();
        Box::pin(async move { Ok(done) })
    }

    fn last_sequence(&self) -> BoxFuture<'_, Result<u64>> {
        let last = self.messages.locked().len() as u64;
        Box::pin(async move { Ok(last) })
    }

    fn put_title(&self, title: ThreadTitle) -> BoxFuture<'_, Result<()>> {
        self.titles.locked().push(title);
        Box::pin(async { Ok(()) })
    }

    fn put_done(&self, done: ants_proto::ThreadDone) -> BoxFuture<'_, Result<()>> {
        self.done.locked().push(done);
        Box::pin(async { Ok(()) })
    }

    fn marks(&self, _: UserId) -> BoxFuture<'_, Result<ants_proto::ThreadMarks>> {
        let marks = self.own_marks.locked().clone();
        Box::pin(async move { Ok(marks) })
    }

    fn snoozes(&self, _: UserId) -> BoxFuture<'_, Result<ants_proto::ThreadSnoozes>> {
        let snoozes = self.own_snoozes.locked().clone();
        Box::pin(async move { Ok(snoozes) })
    }

    fn change_marks(
        &self,
        _: UserId,
        _: UserId,
        change: super::assist::Change<ants_proto::ThreadMarks>,
    ) -> BoxFuture<'_, Result<bool>> {
        let changed = change(&mut self.marks.locked());
        Box::pin(async move { Ok(changed) })
    }

    fn change_snoozes(
        &self,
        _: UserId,
        _: UserId,
        change: super::assist::Change<ants_proto::ThreadSnoozes>,
    ) -> BoxFuture<'_, Result<bool>> {
        let changed = change(&mut self.snoozes.locked());
        Box::pin(async move { Ok(changed) })
    }

    fn react(
        &self,
        by: UserId,
        message: Uuid,
        reaction: Reaction,
        remove: bool,
        human: UserId,
    ) -> BoxFuture<'_, Result<bool>> {
        let mut reactions = self.reactions.locked();
        reactions.retain(|(b, m, r, _)| !(*b == by && *m == message && *r == reaction));
        if !remove {
            reactions.push((by, message, reaction, Some(human)));
        }
        Box::pin(async { Ok(true) })
    }

    fn record_owner(
        &self,
        thread: ThreadId,
        to: UserId,
        human: UserId,
        assistant: UserId,
    ) -> BoxFuture<'_, Result<()>> {
        self.owners.locked().push((thread, to, human, assistant));
        Box::pin(async { Ok(()) })
    }

    /// A prompt runs on the thread of a held message marked running; a withdrawal takes a waiting message not running.
    fn cancel(&self, request: ants_proto::CancelRequest) -> BoxFuture<'_, Result<Vec<ants_proto::CancelReply>>> {
        let held = self.held.locked();
        let answer = match request.withdraw {
            Some(id) => request.answer(
                held.iter().any(|m| m.id == id && !m.running) || self.unheld.locked().iter().any(|m| m.id == id),
            ),
            None => request.answer(held.iter().any(|m| m.thread_id == request.thread_id && m.running)),
        };
        self.cancels.locked().push(request);
        let answers = if *self.unserved.locked() { vec![] } else { vec![answer] };
        Box::pin(async move { Ok(answers) })
    }

    /// Two more wait in the stream, unlisted.
    fn queue(
        &self,
        request: ants_proto::QueueRequest,
        _: ants_proto::Humans,
        _: usize,
    ) -> BoxFuture<'_, Result<Option<crate::queue::Listed>>> {
        let mut waiting: Vec<_> = self.held.locked().iter().cloned().map(crate::queue::held_waiting).collect();
        waiting.extend(self.unheld.locked().iter().cloned());
        self.queues.locked().push(request);
        let listed = (!*self.unserved.locked()).then_some((waiting, 2));
        Box::pin(async move { Ok(listed) })
    }

    fn record_withdrawal(
        &self,
        ant: UserId,
        message: Uuid,
        human: UserId,
        assistant: UserId,
    ) -> BoxFuture<'_, Result<crate::attempts::Recorded>> {
        self.withdrawals.locked().push((ant, message, human, assistant));
        let found = self.found_mark.locked().clone();
        Box::pin(async move { Ok(found.map_or(crate::attempts::Recorded::Holds, crate::attempts::Recorded::Marked)) })
    }

    fn send_order(&self, order: ants_proto::AdminOrder) -> BoxFuture<'_, Result<ants_proto::AdminOrder>> {
        self.orders.locked().push(order.clone());
        Box::pin(async { Ok(order) })
    }
}

/// Opens and seals as [`crate::sealing::AssistantSealing`] does as far as enrollment goes: only while the grant
/// standing enrolls the ant as a device of its human. The sealed body is the text itself.
struct EnrolledSealing(Registry);

impl EnrolledSealing {
    fn enrolled(&self, human: &UserId) -> Option<ants_proto::DeviceId> {
        self.0.grant(human)?.sealing.map(|s| s.device)
    }
}

impl HumanSealing for EnrolledSealing {
    fn open<'a>(&'a self, human: &'a UserId, envelope: &'a Envelope) -> BoxFuture<'a, Result<crate::sealing::Opening>> {
        use crate::sealing::Opening;
        let opening = match (self.enrolled(human), &envelope.sealed) {
            (Some(_), Some(sealed)) => Opening::Opened(ants_proto::SealedBody {
                from: envelope.from.clone(),
                to: envelope.to.clone(),
                thread_id: envelope.thread_id.clone(),
                id: envelope.id,
                text: sealed.body.clone(),
                title: None,
                attachments: Vec::new(),
            }),
            _ => Opening::Unreadable,
        };
        Box::pin(async move { Ok(opening) })
    }

    fn seal<'a>(&'a self, human: &'a UserId, envelope: &'a mut Envelope) -> BoxFuture<'a, Result<()>> {
        let sealed = self.enrolled(human).map(|device| {
            let body = std::mem::take(&mut envelope.text);
            envelope.sealed = Some(Box::new(ants_proto::Sealed { v: 1, device, keys: Vec::new(), body }));
        });
        Box::pin(async move { sealed.ok_or_else(|| anyhow::anyhow!("{human} has not enrolled the ant")) })
    }

    fn seal_title<'a>(
        &'a self,
        human: &'a UserId,
        other: &'a UserId,
        title: &'a mut ThreadTitle,
    ) -> BoxFuture<'a, Result<()>> {
        let sealed = self.enrolled(human).map(|device| {
            let body = format!("{other}: {}", std::mem::take(&mut title.title));
            title.sealed = Some(Box::new(ants_proto::Sealed { v: 1, device, keys: Vec::new(), body }));
        });
        Box::pin(async move { sealed.ok_or_else(|| anyhow::anyhow!("{human} has not enrolled the ant")) })
    }
}

/// Alice's grant naming `assistant`, or withdrawing hers with none, enrolling it as a device with `sealing`.
fn grant(humans: &Registry, device: &crate::humans::TestDevice, assistant: Option<&str>, sealing: bool) {
    let alice = user("alice");
    let sealing = sealing.then(|| ants_proto::SealingDevice {
        device: ants_proto::DeviceId::generate(),
        xkey: "XBLFEIK7UCHY5XKH7LGNQ4JZNVOZEWMIHPZH3LP4JUBJ6KQZDVBEWHWB".parse().unwrap(),
    });
    let mut grant =
        ants_proto::AssistantGrant::new(alice.clone(), assistant.map(user), chrono::Utc::now(), device.jwt(&alice))
            .with_sealing(sealing);
    grant.signature = device.sign(&grant.signed());
    humans.put_record(&grant.key(), Some(&serde_json::to_vec(&grant).unwrap()));
}

/// `dev-a` as the assistant of alice, its tools acting for her through the store and sealing returned.
async fn assistant(
    name: &str,
) -> (Mcp, mpsc::UnboundedReceiver<Envelope>, Registry, crate::humans::TestDevice, Arc<MemoryHuman>) {
    assistant_with(name, Arc::default()).await
}

/// [`assistant`], with `notes` as the thread notes.
async fn assistant_with(
    name: &str,
    notes: Arc<MemoryNotes>,
) -> (Mcp, mpsc::UnboundedReceiver<Envelope>, Registry, crate::humans::TestDevice, Arc<MemoryHuman>) {
    let device = crate::humans::TestDevice::default();
    let humans = Registry::with(UserId::human(), &["alice", "bob"]).verifying_grants(Some(device.account()));
    let (mcp, sent) = start_with(&home(name), Vec::new(), notes, humans.clone()).await;
    let store = Arc::new(MemoryHuman::default());
    mcp.with_assisting(user("alice"));
    mcp.with_human_store(store.clone());
    mcp.with_sealing(Arc::new(EnrolledSealing(humans.clone())));
    (mcp, sent, humans, device, store)
}

const ASSIST_TOOLS: [&str; 15] = [
    "human_queue",
    "answer_for_human",
    "send_for_human",
    "post_for_human",
    "notes_for_human",
    "react_for_human",
    "title_for_human",
    "done_for_human",
    "mark_for_human",
    "snooze_for_human",
    "decide_for_human",
    "queue_for_human",
    "cancel_for_human",
    "withdraw_for_human",
    "order_for_human",
];

#[tokio::test]
async fn the_assistant_tools_stand_only_while_the_grant_names_the_ant() {
    let (mcp, _sent, humans, device, _store) = assistant("assist-grant").await;
    let (url, token) = http_server(&mcp, &ThreadId::generate());
    let client = http_client(&url, &token).await;
    let tools = async || -> Vec<String> {
        client.list_all_tools().await.unwrap().into_iter().map(|t| t.name.to_string()).collect()
    };
    let offered = |tools: &[String]| ASSIST_TOOLS.iter().filter(|t| tools.iter().any(|n| n == *t)).count();

    let before = tools().await;
    assert_eq!((before.len(), offered(&before)), (18, 0), "no grant: {before:?}");
    let (ok, refused) = call(&client, "human_queue", serde_json::json!({})).await;
    assert!(!ok && refused.contains("no longer names dev-a"), "{refused}");

    grant(&humans, &device, Some("dev-a"), false);
    let granted = tools().await;
    assert_eq!((granted.len(), offered(&granted)), (33, 15), "{granted:?}");
    assert_eq!(call(&client, "human_queue", serde_json::json!({})).await, (true, "Nothing waits for alice.".into()));

    // The same connection loses them at once.
    grant(&humans, &device, None, false);
    let withdrawn = tools().await;
    assert_eq!((withdrawn.len(), offered(&withdrawn)), (18, 0), "{withdrawn:?}");
    let (ok, refused) = call(&client, "send_for_human", serde_json::json!({"to": "dev-b", "text": "hi"})).await;
    assert!(!ok && refused.contains("the grant of alice no longer names dev-a"), "{refused}");

    grant(&humans, &device, Some("pb"), false);
    assert_eq!(offered(&tools().await), 0, "the grant names another ant");
}

#[tokio::test]
async fn the_assistant_pins_archives_and_snoozes_for_its_human_naming_itself() {
    let (mcp, mut sent, humans, device, store) = assistant("assist-marks").await;
    grant(&humans, &device, Some("dev-a"), false);
    let alice = user("alice");
    let thread = ThreadId::generate();
    store.store(Envelope { thread_id: thread.clone(), ..Envelope::new(user("dev-b"), alice.clone(), "done") });
    let (url, token) = http_server(&mcp, &ThreadId::generate());
    let client = http_client(&url, &token).await;
    let (ok, refused) =
        call(&client, "mark_for_human", serde_json::json!({"thread": ThreadId::generate().to_string(), "mark": "pin"}))
            .await;
    assert!(!ok && refused.contains("alice has no message on thread"), "{refused}");

    let mark = async |mark: &str| {
        call(&client, "mark_for_human", serde_json::json!({"thread": thread.to_string(), "mark": mark})).await
    };
    let (ok, pinned) = mark("pin").await;
    assert!(ok && pinned.starts_with("Pinned thread"), "{pinned}");
    assert!(store.marks.locked().pinned.contains_key(&thread), "the pin is the assistant's");
    let notice = sent.try_recv().unwrap();
    assert!(notice.to == alice && notice.text.contains("as the assistant of alice, pinned thread"), "{}", notice.text);
    let (ok, again) = mark("pin").await;
    assert!(ok && again.contains("is pinned already") && sent.try_recv().is_err(), "{again}");
    let (ok, archived) = mark("archive").await;
    assert!(ok && archived.starts_with("Archived"), "{archived}");
    let marks = store.marks.locked().clone();
    assert!(marks.pinned.is_empty() && marks.archived.contains_key(&thread));
    sent.try_recv().unwrap();
    let (ok, refused) = mark("star").await;
    assert!(!ok && refused.contains("none of pin, unpin, archive and unarchive"), "{refused}");
    let snooze = async |duration: &str| {
        let args = serde_json::json!({"thread": thread.to_string(), "duration": duration});
        call(&client, "snooze_for_human", args).await
    };
    let (ok, snoozed) = snooze("30d").await;
    assert!(ok && snoozed.contains("on the home of alice until"), "{snoozed}");
    let snoozed = store.snoozes.locked().0[&thread];
    assert_eq!(snoozed.until - snoozed.at, ants_proto::SNOOZE_MAX, "cut to 7d");
    assert!(sent.try_recv().unwrap().text.contains("snoozed thread"));
    let (ok, woke) = snooze("off").await;
    assert!(ok && woke.starts_with("Ended the snooze") && store.snoozes.locked().0.is_empty(), "{woke}");
    sent.try_recv().unwrap();

    // The human's own marks and snoozes stay as they are: the assistant changes none of them.
    let now = chrono::Utc::now();
    store.own_marks.locked().pin(&thread);
    store.own_snoozes.locked().snooze(&thread, now, now + chrono::TimeDelta::hours(1));
    let before = (store.own_marks.locked().clone(), store.marks.locked().clone());
    for refused in [mark("unpin").await, mark("archive").await] {
        assert!(!refused.0 && refused.1.contains("alice pinned thread") && refused.1.contains("itself"), "{refused:?}");
    }
    let (ok, refused) = snooze("off").await;
    assert!(!ok && refused.contains("alice snoozed thread") && refused.contains("itself"), "{refused}");
    assert_eq!((store.own_marks.locked().clone(), store.marks.locked().clone()), before);
    assert!(store.own_snoozes.locked().get(&thread, now).is_some() && sent.try_recv().is_err());
}

#[tokio::test]
async fn what_the_assistant_does_for_its_human_carries_on_behalf_of() {
    let (mcp, mut sent, humans, device, store) = assistant("assist-acts").await;
    grant(&humans, &device, Some("dev-a"), false);
    let (alice, dev_a, dev_b) = (user("alice"), user("dev-a"), user("dev-b"));
    let thread = ThreadId::generate();
    let question = Question::new("Ship it?", &["yes".to_owned(), "no".to_owned()], false).unwrap();
    let asked =
        Envelope::asking(thread.clone(), dev_b.clone(), alice.clone(), question).with_owner(Some(alice.clone()));
    store.store(asked.clone());
    let mut request = ants_proto::ApprovalRequest::new(
        dev_b.clone(),
        thread.clone(),
        ants_proto::ToolCall { kind: "execute".into(), title: "git push".into(), ..Default::default() },
        vec![Verdict::AllowOnce],
        Duration::from_secs(600),
    )
    .for_human(Some(alice.clone()), None);
    store.requests.locked().push(request.clone());
    request.human = Some(user("bob"));
    store.requests.locked().push(request.clone());
    let (url, token) = http_server(&mcp, &ThreadId::generate());
    let client = http_client(&url, &token).await;

    let (ok, queue) = call(&client, "human_queue", serde_json::json!({})).await;
    assert!(ok, "{queue}");
    assert!(queue.contains(&format!("message {} on thread {thread}, dev-b to alice: Ship it?", asked.id)), "{queue}");
    assert!(queue.contains("[options: 1. yes, 2. no]"), "{queue}");
    assert!(queue.contains("Tool requests (1)") && queue.contains("dev-b/"), "bob's is not hers: {queue}");

    let (ok, answered) =
        call(&client, "answer_for_human", serde_json::json!({"message_id": asked.id.to_string(), "answer": "1"})).await;
    assert!(ok, "{answered}");
    let answer = sent.try_recv().unwrap();
    assert_eq!((&answer.from, &answer.to, answer.on_behalf_of.as_ref()), (&dev_a, &dev_b, Some(&alice)));
    assert_eq!(answer.answers_as(&asked, &alice), Some(ants_proto::Answered::Option(1, "yes")), "an answer for alice");
    assert!(humans.honours_assistant(&alice, &answer.from, Some(&alice)), "readers verify it against the grant");
    store.store(answer);
    let (_, queue) = call(&client, "human_queue", serde_json::json!({})).await;
    assert!(queue.contains("Questions to alice (0)"), "answered: {queue}");

    let (ok, decided) = call(
        &client,
        "decide_for_human",
        serde_json::json!({"request": format!("dev-b/{}", request.id), "verdict": "allow_once"}),
    )
    .await;
    assert!(ok && decided.contains("ants assistant confirm dev-b/"), "{decided}");
    let decision = sent.try_recv().unwrap();
    let approval = decision.approval.as_deref().unwrap();
    assert_eq!((&decision.to, decision.on_behalf_of.as_ref()), (&dev_b, Some(&alice)));
    assert_eq!((approval.verdict, approval.on_behalf_of.as_ref()), (Verdict::AllowOnce, Some(&alice)));

    let (ok, to_ant) = call(
        &client,
        "send_for_human",
        serde_json::json!({"to": "dev-b", "text": "ship it", "thread": thread.to_string()}),
    )
    .await;
    assert!(ok, "{to_ant}");
    let message = sent.try_recv().unwrap();
    assert_eq!((&message.to, &message.thread_id, message.on_behalf_of.as_ref()), (&dev_b, &thread, Some(&alice)));
    assert_eq!((message.owner.as_ref(), message.text.as_str()), (Some(&alice), "ship it"));
    assert!(sent.try_recv().is_err(), "dev-b's daemon tells alice");
    let recorded = store.owners.locked().clone();
    assert_eq!(recorded, [(thread.clone(), dev_b.clone(), alice.clone(), dev_a.clone())], "before the message");

    let (ok, titled) =
        call(&client, "title_for_human", serde_json::json!({"thread": thread.to_string(), "title": "Release"})).await;
    assert!(ok, "{titled}");
    let title = store.titles.locked().pop().unwrap();
    assert_eq!((title.title.as_str(), &title.by, title.on_behalf_of.as_ref()), ("Release", &dev_a, Some(&alice)));
    let notice = sent.try_recv().unwrap();
    assert_eq!((&notice.to, notice.on_behalf_of.as_ref()), (&alice, None), "alice is told: {}", notice.text);
    assert!(notice.text.contains("as the assistant of alice, titled thread"), "{}", notice.text);

    let (ok, _) =
        call(&client, "react_for_human", serde_json::json!({"message_id": asked.id.to_string(), "reaction": "check"}))
            .await;
    assert!(ok);
    assert_eq!(store.reactions.locked()[0].3.as_ref(), Some(&alice));
    sent.try_recv().unwrap();
    let (ok, _) = call(&client, "done_for_human", serde_json::json!({"thread": thread.to_string()})).await;
    assert!(ok);
    assert_eq!(store.done.locked()[0].on_behalf_of.as_ref(), Some(&alice));
    sent.try_recv().unwrap();

    let (ok, refused) =
        call(&client, "title_for_human", serde_json::json!({"thread": ThreadId::generate().to_string(), "title": "x"}))
            .await;
    assert!(!ok && refused.contains("alice has no message on thread"), "{refused}");

    let (ok, ordered) =
        call(&client, "order_for_human", serde_json::json!({"ant": "dev-b", "machine": "m1", "op": "restart-now"}))
            .await;
    assert!(ok && ordered.contains("until then it is held") && ordered.contains("dev-b/order:restart"), "{ordered}");
    let order = store.orders.locked().pop().unwrap();
    assert_eq!((&order.admin, order.on_behalf_of.as_ref()), (&dev_a, Some(&alice)));
    assert_eq!(
        (order.machine.as_str(), &order.ant, &order.op),
        ("m1", &dev_b, &ants_proto::AdminOp::Restart { now: true })
    );
    let notice = sent.try_recv().unwrap();
    assert_eq!(notice.to, alice);
    assert!(notice.text.contains(&format!("ants assistant confirm m1/dev-b/{}", order.id)), "{}", notice.text);
    let (ok, refused) =
        call(&client, "order_for_human", serde_json::json!({"ant": "dev-b", "machine": "m1", "op": "retire"})).await;
    assert!(!ok && refused.contains("unknown op retire"), "{refused}");
}

/// An order to set an ant's settings carries the changes as given, dotted fields split; one without changes is refused.
#[tokio::test]
async fn the_assistant_orders_settings_changed_for_its_human() {
    let (mcp, _sent, humans, device, store) = assistant("assist-set").await;
    grant(&humans, &device, Some("dev-a"), false);
    let (url, token) = http_server(&mcp, &ThreadId::generate());
    let client = http_client(&url, &token).await;
    let changes = serde_json::json!([
        {"field": "model", "value": "gpt-5", "expected": "o3"},
        {"field": "caps.daily", "value": null, "expected": 100},
    ]);
    let set = serde_json::json!({"ant": "dev-b", "machine": "m1", "op": "set", "changes": changes});
    let (ok, ordered) = call(&client, "order_for_human", set).await;
    assert!(ok && ordered.contains("dev-b/order:set"), "{ordered}");
    let order = store.orders.locked().pop().unwrap();
    let change = |field: &[&str], expected, value| ants_proto::FieldChange {
        field: field.iter().map(|f| (*f).to_owned()).collect(),
        expected: Some(expected),
        expected_digest: None,
        value,
    };
    assert_eq!(
        order.op,
        ants_proto::AdminOp::Set {
            changes: vec![
                change(&["model"], serde_json::json!("o3"), Some(serde_json::json!("gpt-5"))),
                change(&["caps", "daily"], serde_json::json!(100), None),
            ]
        }
    );
    for changes in [serde_json::json!(null), serde_json::json!([])] {
        let set = serde_json::json!({"ant": "dev-b", "machine": "m1", "op": "set", "changes": changes});
        let (ok, refused) = call(&client, "order_for_human", set).await;
        assert!(!ok && refused.contains("op set needs changes"), "{refused}");
    }
    let set = serde_json::json!({"ant": "dev-b", "machine": "m1", "op": "set", "changes": [{"field": "caps..daily"}]});
    let (ok, refused) = call(&client, "order_for_human", set).await;
    assert!(!ok && refused.contains("field caps..daily"), "{refused}");
}

#[tokio::test]
async fn the_assistant_posts_in_channels_and_notes_for_its_human() {
    let notes = Arc::new(MemoryNotes::default());
    let (mcp, mut sent, humans, device, _store) = assistant_with("assist-posts", notes.clone()).await;
    grant(&humans, &device, Some("dev-a"), false);
    let (alice, dev_a) = (user("alice"), user("dev-a"));
    let mut ops = ChannelEntry::new("ops".parse().unwrap());
    ops.add_member(alice.clone());
    ops.add_member(user("dev-b"));
    let dev = ChannelEntry::new("dev".parse().unwrap());
    let posted = Arc::new(StdMutex::new(Vec::new()));
    mcp.with_channels(channels(vec![ops, dev], posted.clone()));
    let (url, token) = http_server(&mcp, &ThreadId::generate());
    let client = http_client(&url, &token).await;

    let (ok, refused) = call(&client, "post_for_human", serde_json::json!({"channel": "dev", "text": "hi"})).await;
    assert!(!ok && refused.contains("alice is no member of #dev"), "{refused}");
    let (ok, result) =
        call(&client, "post_for_human", serde_json::json!({"channel": "#ops", "text": "@dev-b deploy friday"})).await;
    assert!(ok && result.contains("alice is told in its inbox"), "{result}");
    let post = posted.locked().pop().unwrap();
    assert_eq!((&post.from, post.on_behalf_of.as_ref(), post.owner.as_ref()), (&dev_a, Some(&alice), Some(&alice)));
    assert_eq!((post.mentions.clone(), post.turn_budget), (vec![user("dev-b")], ants_proto::DEFAULT_TURN_BUDGET));
    let notice = sent.try_recv().unwrap();
    assert_eq!((&notice.to, notice.on_behalf_of.as_ref()), (&alice, None));
    assert!(notice.text.contains("as the assistant of alice, posted to #ops on thread"), "{}", notice.text);

    let thread = post.thread_id.clone();
    let reply = serde_json::json!({"channel": "ops", "text": "done", "thread": thread.to_string()});
    let (ok, _) = call(&client, "post_for_human", reply).await;
    assert!(ok);
    let reply = posted.locked().pop().unwrap();
    assert_eq!(
        (&reply.thread_id, reply.in_reply_to, reply.on_behalf_of.as_ref()),
        (&thread, Some(post.id), Some(&alice))
    );
    sent.try_recv().unwrap();

    let spent = NotesPost::spend(thread.clone(), user("dev-b"), "stuck", 1, None).unwrap();
    notes.0.locked().insert(thread.clone(), vec![spent]);
    let (ok, result) =
        call(&client, "notes_for_human", serde_json::json!({"thread": thread.to_string(), "text": "go on"})).await;
    assert!(ok && result.contains(&format!("({} turns left)", ants_proto::DEFAULT_TURN_BUDGET)), "{result}");
    let noted = notes.0.locked()[&thread].last().cloned().unwrap();
    assert_eq!((&noted.from, noted.on_behalf_of.as_ref(), noted.text.as_str()), (&dev_a, Some(&alice), "go on"));
    let notice = sent.try_recv().unwrap();
    assert!(notice.text.contains("posted to the notes of thread"), "{}", notice.text);

    grant(&humans, &device, None, false);
    let (ok, refused) =
        call(&client, "notes_for_human", serde_json::json!({"thread": thread.to_string(), "text": "x"})).await;
    assert!(!ok && refused.contains("no longer names dev-a"), "{refused}");
}

#[tokio::test]
async fn the_assistant_lists_cancels_and_withdraws_for_its_human_on_requests_naming_it() {
    let (mcp, _sent, humans, device, store) = assistant("assist-queue").await;
    grant(&humans, &device, Some("dev-a"), false);
    let (alice, dev_a, dev_b) = (user("alice"), user("dev-a"), user("dev-b"));
    let (running, waiting) = (ThreadId::generate(), ThreadId::generate());
    let held = |thread: &ThreadId, seq, running| ants_proto::QueuedMessage {
        id: Uuid::now_v7(),
        thread_id: thread.clone(),
        from: alice.clone(),
        channel: None,
        sent_at: chrono::Utc::now(),
        seq,
        running,
        ahead: 0,
        redelivery: false,
    };
    let (prompted, queued) = (held(&running, 1, true), held(&waiting, 2, false));
    store.held.locked().extend([prompted.clone(), queued.clone()]);
    // Messages awaiting redelivery: started already in an attempt that did not end, awaiting a retry after a failed
    // attempt, or neither; and a channel post still in the stream.
    let unheld = |seq, channel: Option<&str>| crate::queue::Waiting {
        delivered: false,
        channel: channel.map(|c| c.parse().unwrap()),
        ..crate::queue::held_waiting(held(&ThreadId::generate(), seq, false))
    };
    let crashed = crate::queue::Waiting { redelivery: true, started: true, ..unheld(3, None) };
    let retried = crate::queue::Waiting { redelivery: true, retry: true, ..unheld(4, None) };
    let redelivered = crate::queue::Waiting { redelivery: true, ..unheld(5, None) };
    let posted = unheld(6, Some("ops"));
    store.unheld.locked().extend([crashed.clone(), retried.clone(), redelivered.clone(), posted.clone()]);
    let (url, token) = http_server(&mcp, &ThreadId::generate());
    let client = http_client(&url, &token).await;

    let (ok, listed) = call(&client, "queue_for_human", serde_json::json!({"ant": "dev-b"})).await;
    assert!(ok && listed.contains(&queued.id.to_string()) && listed.contains("2 more wait"), "{listed}");
    assert!(listed.contains(&format!("{}, sent", alice)), "{listed}");
    assert!(listed.contains("awaiting redelivery (started; not withdrawable)"), "{listed}");
    assert!(listed.contains(&format!("message {}", retried.id)), "{listed}");
    assert!(listed.contains("awaiting a retry after a failed attempt"), "{listed}");
    assert!(listed.contains(&format!("message {}", redelivered.id)) && listed.contains("awaiting redelivery"));
    assert!(listed.contains(&format!("message {}", posted.id)) && listed.contains("alice in #ops"), "{listed}");
    assert!(listed.contains("next, in the stream"), "{listed}");
    let (ok, cancelled) =
        call(&client, "cancel_for_human", serde_json::json!({"ant": "dev-b", "thread": running.to_string()})).await;
    assert!(ok && cancelled.contains("Cancelled the prompt of dev-b"), "{cancelled}");
    let (ok, refused) =
        call(&client, "cancel_for_human", serde_json::json!({"ant": "dev-b", "thread": waiting.to_string()})).await;
    assert!(!ok && refused.contains("no prompt running"), "{refused}");
    let withdraw = |id: Uuid| serde_json::json!({"ant": "dev-b", "message_id": id.to_string()});
    let (ok, refused) = call(&client, "withdraw_for_human", withdraw(prompted.id)).await;
    assert!(!ok && refused.contains("started on message"), "{refused}");
    let (ok, refused) = call(&client, "withdraw_for_human", withdraw(crashed.id)).await;
    assert!(!ok && refused.contains("awaits redelivery to finish it"), "{refused}");
    let (ok, withdrew) = call(&client, "withdraw_for_human", withdraw(queued.id)).await;
    assert!(ok && withdrew.contains("Withdrew message"), "{withdrew}");
    for unheld in [&retried, &redelivered, &posted] {
        let (ok, withdrew) = call(&client, "withdraw_for_human", withdraw(unheld.id)).await;
        assert!(ok && withdrew.contains("Withdrew message"), "{withdrew}");
    }
    let (ok, refused) = call(&client, "withdraw_for_human", withdraw(Uuid::now_v7())).await;
    assert!(!ok && refused.contains("neither waiting"), "{refused}");
    let (ok, refused) = call(&client, "queue_for_human", serde_json::json!({"ant": "bob"})).await;
    assert!(!ok && refused.contains("bob is a human"), "{refused}");

    // With no daemon of the ant serving, a message of the human's to it is withdrawn by a record naming the assistant.
    *store.unserved.locked() = true;
    let mut sent = Envelope::new(alice.clone(), dev_b.clone(), "later");
    sent.thread_id = waiting.clone();
    store.store(sent.clone());
    store.store(Envelope::new(alice.clone(), user("dev-c"), "elsewhere"));
    let elsewhere = store.messages.locked().last().unwrap().envelope.id;
    let (ok, recorded) = call(&client, "withdraw_for_human", withdraw(sent.id)).await;
    assert!(ok && recorded.contains("recorded the withdrawal of message"), "{recorded}");
    for id in [queued.id, elsewhere] {
        let (ok, refused) = call(&client, "withdraw_for_human", withdraw(id)).await;
        assert!(!ok && refused.contains("only a message of alice to dev-b"), "{refused}");
    }
    *store.found_mark.locked() =
        Some(ants_proto::MessageMark::Started { thread_id: waiting.clone(), at: chrono::Utc::now() });
    let (ok, refused) = call(&client, "withdraw_for_human", withdraw(sent.id)).await;
    assert!(!ok && refused.contains("started on message"), "{refused}");
    let withdrawals = store.withdrawals.locked().clone();
    assert_eq!(withdrawals, vec![(dev_b.clone(), sent.id, alice.clone(), dev_a.clone()); 2]);
    *store.unserved.locked() = false;

    // Every request names the assistant and the human, so they go on the subjects naming the assistant.
    let cancels = store.cancels.locked().clone();
    assert_eq!(cancels.len(), 6);
    for request in &cancels {
        assert_eq!((&request.ant, &request.by, request.on_behalf_of.as_ref()), (&dev_b, &dev_a, Some(&alice)));
        assert!(request.subject().ends_with(".dev-a"), "{}", request.subject());
    }
    assert_eq!(cancels[2].withdraw, Some(queued.id));
    assert_eq!(cancels[2].thread_id, waiting, "on the message's thread");
    assert_eq!((cancels[3].withdraw, &cancels[3].thread_id), (Some(retried.id), &retried.thread_id));
    assert_eq!((cancels[5].withdraw, &cancels[5].thread_id), (Some(posted.id), &posted.thread_id));
    let queues = store.queues.locked().clone();
    assert!(queues.iter().all(|q| q.subject() == "ants.queue.dev-b.dev-a" && q.on_behalf_of == Some(alice.clone())));
}

#[tokio::test]
async fn the_assistant_reads_and_sends_its_human_s_sealed_messages_only_when_enrolled() {
    let (mcp, mut sent, humans, device, store) = assistant("assist-sealed").await;
    grant(&humans, &device, Some("dev-a"), false);
    let (alice, bob) = (user("alice"), user("bob"));
    let mut sealed = Envelope::new(bob.clone(), alice.clone(), "");
    let device_id = ants_proto::DeviceId::generate();
    sealed.sealed =
        Some(Box::new(ants_proto::Sealed { v: 1, device: device_id, keys: Vec::new(), body: "lunch at one?".into() }));
    store.store(sealed.clone());
    let (url, token) = http_server(&mcp, &ThreadId::generate());
    let client = http_client(&url, &token).await;
    let read =
        async || call(&client, "human_queue", serde_json::json!({"thread": sealed.thread_id.to_string()})).await.1;
    let to_bob = serde_json::json!({"to": "bob", "text": "yes", "thread": sealed.thread_id.to_string()});

    assert!(read().await.contains("bob to alice: [sealed]"), "not enrolled");
    let (ok, refused) = call(&client, "send_for_human", to_bob.clone()).await;
    assert!(!ok && refused.contains("nothing sent: set plaintext"), "{refused}");
    assert!(sent.try_recv().is_err());

    grant(&humans, &device, Some("dev-a"), true);
    assert!(read().await.contains("bob to alice: lunch at one?"), "enrolled");
    let (ok, result) = call(&client, "send_for_human", to_bob.clone()).await;
    assert!(ok, "{result}");
    let reply = sent.try_recv().unwrap();
    assert_eq!((&reply.to, reply.on_behalf_of.as_ref(), reply.text.as_str()), (&bob, Some(&alice), ""));
    assert_eq!(reply.sealed.as_ref().unwrap().body, "yes");
    let notice = sent.try_recv().unwrap();
    assert!(notice.to == alice && notice.text.contains("sent bob a sealed message"), "{}", notice.text);
    assert!(!notice.text.contains("yes"), "the notice is in the clear, so it never quotes a sealed message");
    let title = serde_json::json!({"thread": sealed.thread_id.to_string(), "title": "Lunch"});
    let (ok, titled) = call(&client, "title_for_human", title.clone()).await;
    assert!(ok, "{titled}");
    let stored = store.titles.locked().pop().unwrap();
    assert_eq!((stored.title.as_str(), &stored.by, stored.on_behalf_of.as_ref()), ("", &user("dev-a"), Some(&alice)));
    assert_eq!(stored.sealed.as_ref().unwrap().body, "bob: Lunch", "sealed for alice's and bob's devices");
    let notice = sent.try_recv().unwrap();
    assert!(notice.text.contains("a sealed title") && !notice.text.contains("Lunch"), "{}", notice.text);

    grant(&humans, &device, Some("dev-a"), false);
    assert!(read().await.contains("bob to alice: [sealed]"), "enrollment ended");
    let (ok, refused) = call(&client, "title_for_human", title).await;
    assert!(!ok && refused.contains("not enrolled") && refused.contains("no title is set"), "{refused}");
    assert!(store.titles.locked().is_empty());
    let (ok, plain) =
        call(&client, "send_for_human", serde_json::json!({"to": "bob", "text": "yes", "plaintext": true})).await;
    assert!(ok, "{plain}");
    let reply = sent.try_recv().unwrap();
    assert!(reply.sealed.is_none() && reply.text == "yes");
}
