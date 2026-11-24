//! Caps and turn budgets: spend warnings, escalations up the chain, through the forest of teams, and to the owner, and
//! the leader's answers that renew a thread.

use super::*;

#[tokio::test]
async fn a_spent_cap_hands_the_thread_to_the_human_until_the_human_answers() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), Caps { thread_cost: Some(1.0), ..Caps::default() }));
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    bus.send_from(dev_b.clone(), &t1, "cost 2 stall").await;
    bus.send_from(dev_b.clone(), &t1, "more").await;
    bus.send(&t1, "go on").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let cap = "Thread cost cap reached (2.00 of 1.00 since a human's last message)";
        let reply = next_envelope(&mut to_dev_b).await;
        assert_eq!(reply.text, format!("partial\n\n[ants] {cap}; the prompt was cancelled."));
        let escalation = next_envelope(&mut to_human).await;
        assert_eq!(escalation.text, format!("{cap}; needs a decision. Last message from dev-b:\ncost 2 stall"));
        // The next message out of allowance finds the escalation waiting for the human: it is not sent again.
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "go on".to_owned()), "a fresh allowance");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    let log = log.locked();
    assert_eq!((log.cancelled, log.answered.as_slice()), (1, &["go on".to_owned()][..]));
}

#[tokio::test]
async fn spend_nearing_a_cap_warns_the_human_once_on_a_thread_of_its_own() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let caps = Caps { thread_cost: Some(1.0), ..Caps::default() };
    bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), caps).warn_at(80));
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let t1 = ThreadId::generate();
    bus.send_from(dev_b.clone(), &t1, "cost 0.85").await;
    bus.send_from(dev_b.clone(), &t1, "cost 0.9").await;
    bus.send(&t1, "done?").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let warning = next_envelope(&mut to_human).await;
        assert_eq!(warning.from, bus.dev);
        assert_ne!(warning.thread_id, t1, "a thread of its own, so a wait on the thread goes on");
        let start = format!("[ants] dev-a spent 0.85/1.00 (85%) of max_thread_cost on thread {t1} since");
        assert!(warning.text.starts_with(&start), "{}", warning.text);
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "done?".to_owned()), "no second warning");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn escalations_go_to_the_owner_and_a_registered_human_renews_the_allowance() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), Caps { thread_cost: Some(1.0), ..Caps::default() }));
    let alice: UserId = "alice".parse().unwrap();
    // The registry looks for the bucket before it exists, then finds it.
    let _watching = bus.humans.watch(bus.js.clone(), Duration::from_millis(50));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let config = jetstream::kv::Config { bucket: ants_proto::HUMANS_BUCKET.to_owned(), ..Default::default() };
    let humans = bus.js.create_key_value(config).await.unwrap();
    let entry = ants_proto::HumanEntry::new(alice.clone());
    humans.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    until("alice to be a human", || bus.humans.is_human(&alice)).await;

    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    let mut to_alice = bus.client.subscribe(subject::inbox_of(&alice)).await.unwrap();
    bus.client.flush().await.unwrap();
    let t1 = ThreadId::generate();
    bus::ensure_owners(&bus.js).await.unwrap();
    // alice asked both on the thread.
    for to in [&bus.dev, &dev_b] {
        crate::records::owners::record(&bus.js, None, &t1, to, &alice).await.unwrap();
    }
    let mut go_on = Envelope::on_thread(t1.clone(), alice.clone(), bus.dev.clone(), "go on");
    go_on.turn_budget = 0;
    for envelope in [
        Envelope::on_thread(t1.clone(), dev_b.clone(), bus.dev.clone(), "cost 2 stall").with_owner(Some(alice.clone())),
        go_on,
    ] {
        bus::publish_json(&bus.js, envelope.subject(), &envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let cap = "Thread cost cap reached (2.00 of 1.00 since a human's last message)";
        assert!(next_envelope(&mut to_dev_b).await.text.ends_with("the prompt was cancelled."));
        let escalation = next_envelope(&mut to_alice).await;
        assert_eq!((&escalation.to, &escalation.owner), (&alice, &Some(alice.clone())));
        assert_eq!(escalation.text, format!("{cap}; needs a decision. Last message from dev-b:\ncost 2 stall"));
        let reply = next_envelope(&mut to_alice).await;
        assert!(reply.text.ends_with(": go on"), "a fresh allowance: {}", reply.text);
        assert_eq!(reply.turn_budget, DEFAULT_TURN_BUDGET, "a human's message stamped 0 gets the fresh budget");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), to_human.next()).await.is_err(), "nothing went to human");
    assert_eq!(log.locked().answered, ["go on"]);
}

#[tokio::test]
async fn a_spent_budget_goes_up_the_chain_and_its_leader_renews_it_once_per_level() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let [lead, dev_b, boss, carol] = ants(["lead", "dev-b", "boss", "carol"]);
    let turn_budget = 3;
    bus.chain = Chain { escalate_to: vec![lead.clone(), dev_b.clone()], after: None, turn_budget, ..Chain::default() };
    bus.put_colony(&lead, std::slice::from_ref(&boss)).await;
    let [mut to_lead, mut to_dev_b, mut to_boss] = inboxes(&bus, &[lead.clone(), dev_b.clone(), boss.clone()]).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send_spent(&carol, &t1, "no turns", None).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let (first, copy) = (next_envelope(&mut to_lead).await, next_envelope(&mut to_dev_b).await);
        let one = first.escalation.as_deref().expect("the escalation").clone();
        let visited = vec![bus.dev.clone(), lead.clone(), dev_b.clone()];
        assert_eq!((one.level, &one.leader, &one.stuck, &one.visited), (1, &lead, &bus.dev, &visited));
        assert_eq!((copy.escalation, &copy.text), (first.escalation.clone(), &first.text), "every member gets it");
        let head = format!("[ants] Escalation on thread {t1}, level 1: dev-a is stuck (turn budget spent).");
        assert!(first.text.starts_with(&head), "{}", first.text);
        assert!(first.text.contains("Sent to lead, dev-b; lead leads."), "{}", first.text);
        assert!(first.text.ends_with("Turn budget spent; needs a decision. Last message from carol:\nno turns"));

        // The leader answers with no turns left either: a fresh budget, once at this level.
        bus.send_spent(&lead, &t1, "go on", Some(&one)).await;
        bus.send_spent(&lead, &t1, "and on", Some(&one)).await;
        let reply = next_envelope(&mut to_lead).await;
        assert!(reply.text.ends_with(": go on"), "{}", reply.text);
        assert_eq!(reply.turn_budget, turn_budget - 1, "the configured fresh budget, below the stamped 8");
        assert_eq!(reply.escalation, None, "the stuck ant's answer does not carry its escalation");

        // The next spend goes one level higher, to the leader's chain in the colony.
        let up = next_envelope(&mut to_boss).await;
        let two = up.escalation.as_deref().expect("the escalation").clone();
        assert_eq!((two.level, &two.leader, &two.stuck), (2, &boss, &bus.dev));
        assert!(up.text.contains("level 2: dev-a is stuck (turn budget spent). Sent to boss; boss leads."));
        assert!(up.text.ends_with("Last message from lead:\nand on"), "{}", up.text);
        bus.send_spent(&boss, &t1, "from boss", Some(&two)).await;
        let reply = next_envelope(&mut to_boss).await;
        assert_eq!((reply.text.ends_with(": from boss"), reply.turn_budget), (true, turn_budget - 1));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert_eq!(log.locked().answered, ["go on", "from boss"]);
    assert!(timeout(Duration::from_millis(200), to_human.next()).await.is_err(), "nothing went to the human");
    assert!(timeout(Duration::from_millis(50), to_dev_b.next()).await.is_err(), "dev-b got level 1 only");
    let pending = bus.ledger.pending(&t1).await.unwrap().expect("the escalation is kept");
    assert_eq!((pending.escalation.level, pending.to, pending.renewed_by.is_some()), (2, vec![boss], true));
}

#[tokio::test]
async fn the_leader_s_answer_to_a_channel_thread_out_of_turns_is_posted_back_with_fresh_turns() {
    for turn_budget in [DEFAULT_TURN_BUDGET, 3, 20] {
        Box::pin(a_leader_s_answer_resumes_a_channel_thread_with(turn_budget)).await;
    }
}

async fn a_leader_s_answer_resumes_a_channel_thread_with(turn_budget: u32) {
    let Some((mut bus, _)) = Bus::start().await else { return };
    let [lead, dev_b] = ants(["lead", "dev-b"]);
    bus.chain = Chain { escalate_to: vec![lead.clone()], after: None, turn_budget, ..Chain::default() };
    let [mut to_lead] = inboxes(&bus, std::slice::from_ref(&lead).try_into().unwrap()).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut posts) = bus.channel("ops", &["dev-a", "dev-b"]).await;
    let root = ChannelPost::new(ops.clone(), ants_test_support::human(), "deploy friday?");
    bus.post(&root).await;
    let mut spent = root.reply(dev_b.clone(), "@dev-a agree?", &[], &Humans::default()).unwrap();
    spent.turn_budget = 0;
    bus.post(&spent).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut to_lead).await;
        let one = notice.escalation.as_deref().expect("the escalation").clone();
        assert_eq!(notice.thread_id, root.thread_id);
        let mut answer = notice.reply(lead.clone(), "go on", Role::Ant).unwrap();
        answer = answer.with_escalation(Some(one));
        answer.turn_budget = 0;
        bus::publish_json(&bus.js, answer.subject(), &answer, None).await.unwrap();

        // Posted back to the thread with the leader's fresh budget, not sent to the leader.
        let resumed = next_post(&mut posts).await;
        assert_eq!((&resumed.channel, &resumed.thread_id), (&ops, &root.thread_id));
        assert_eq!((resumed.turn_budget, resumed.owner.as_ref()), (turn_budget - 1, Some(&root.from)));

        // Later posts on the thread go by the fresh budget: no escalation again. dev-b's post answers its own, as one
        // answering dev-a's would get no reply.
        let mut more = resumed.reply(dev_b.clone(), "@dev-a and then?", &[], &Humans::default()).unwrap();
        more.in_reply_to = Some(spent.id);
        bus.post(&more).await;
        let reply = next_post(&mut posts).await;
        assert_eq!((reply.in_reply_to, reply.turn_budget), (Some(more.id), turn_budget - 3));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), to_lead.next()).await.is_err(), "the leader gets no reply");
    let answered = log.locked().answered.clone();
    assert_eq!(answered.len(), 2, "{answered:?}");
    let note = format!("on that thread and spends one of its turns ({} left after it)", turn_budget - 1);
    assert!(answered[0].starts_with("go on\n\n[ants] The escalation's leader answered"), "{}", answered[0]);
    assert!(answered[0].contains(&note), "{}", answered[0]);
    let pending = bus.ledger.pending(&root.thread_id).await.unwrap().expect("the escalation is kept");
    assert_eq!((pending.channel.as_ref(), pending.renewed_by.is_some()), (Some(&ops), true));
}

#[tokio::test]
async fn past_the_hop_limit_the_owner_gets_it_and_a_human_starts_the_chain_over() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let [lead, boss, carol] = ants(["lead", "boss", "carol"]);
    bus.chain = Chain { escalate_to: vec![lead.clone()], max_hops: 1, after: None, ..Chain::default() };
    bus.put_colony(&lead, &[boss]).await;
    let [mut to_lead] = inboxes(&bus, std::slice::from_ref(&lead).try_into().unwrap()).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send_spent(&carol, &t1, "stuck", None).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let one = next_envelope(&mut to_lead).await.escalation.expect("the escalation");
        bus.send_spent(&lead, &t1, "go on", Some(&one)).await;
        assert!(next_envelope(&mut to_lead).await.text.ends_with(": go on"));
        bus.send_spent(&lead, &t1, "again", Some(&one)).await;
        let notice = next_envelope(&mut to_human).await;
        let text = "Turn budget spent; needs a decision. Last message from lead:\nagain\n\n[ants] The escalation of \
                        dev-a reached the hop limit (max_escalation_hops = 1).";
        assert_eq!(notice.text, text);
        assert_eq!(notice.escalation.as_deref(), Some(&*one), "the human sees how far it went");

        // A human's answer ends the escalation: the next spend starts at the ant's chain again.
        bus.send(&t1, "carry on").await;
        assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "carry on".to_owned()));
        bus.send_spent(&carol, &t1, "stuck again", None).await;
        let again = next_envelope(&mut to_lead).await.escalation.expect("the escalation");
        assert_eq!((again.level, &again.leader), (1, &lead));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert_eq!(log.locked().answered, ["go on", "carry on"]);
}

#[tokio::test]
async fn an_unanswered_escalation_goes_one_level_higher_after_escalate_after() {
    let Some((mut bus, _to_human)) = Bus::start().await else { return };
    let [lead, boss, carol] = ants(["lead", "boss", "carol"]);
    // Long enough that a slow machine hears of the escalation well before its deadline.
    bus.chain = Chain { escalate_to: vec![lead.clone()], after: Some(Duration::from_secs(2)), ..Chain::default() };
    bus.put_colony(&lead, std::slice::from_ref(&boss)).await;
    let [mut to_lead, mut to_boss] = inboxes(&bus, &[lead.clone(), boss.clone()]).await;
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send_spent(&carol, &t1, "stuck", None).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let one = next_envelope(&mut to_lead).await.escalation.expect("the escalation");
        assert!(timeout(Duration::from_millis(150), to_boss.next()).await.is_err(), "not before the deadline");
        let up = next_envelope(&mut to_boss).await;
        assert_eq!(up.escalation.as_deref().map(|e| (e.level, &e.leader)), Some((2, &boss)));
        let late =
            format!("[ants] No answer from lead on thread {t1} within 2 s; the escalation goes one level higher.");
        assert!(up.text.contains(&late), "{}", up.text);
        assert!(up.text.ends_with("Last message from carol:\nstuck"), "{}", up.text);
        assert!(timeout(Duration::from_millis(500), to_boss.next()).await.is_err(), "no deadline without one set");
        assert_eq!(one.level, 1);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn only_the_leader_answers_the_stuck_ant_and_members_answer_in_the_thread_notes() {
    let Some((bus, _to_human)) = Bus::start().await else { return };
    let [lead, carol, boss] = ants(["lead", "carol", "boss"]);
    bus.put_colony(&lead, std::slice::from_ref(&boss)).await;
    let [mut to_carol, mut to_lead, mut to_boss] = inboxes(&bus, &[carol.clone(), lead.clone(), boss.clone()]).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2, t3) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    // dev-a is a member of carol's escalation on t1 and t3, led by lead, and leads it on t2, one level up.
    let member = escalated(&carol, &t1, &[&[lead.clone(), bus.dev.clone()]]);
    let leader = escalated(&carol, &t2, &[std::slice::from_ref(&lead), std::slice::from_ref(&bus.dev)]);
    let spent = escalated(&carol, &t3, &[&[lead.clone(), bus.dev.clone()]]);
    let full = ants_proto::NotesPost::spend(t3.clone(), lead.clone(), "last turn", 1, None).unwrap();
    crate::stream::notes::publish(&bus.js, &full, None).await.unwrap();
    for (from, thread, text, escalation) in
        [(&carol, &t1, "help", member), (&lead, &t2, "help more", leader.clone()), (&carol, &t3, "x", spent)]
    {
        let envelope =
            Envelope::on_thread(thread.clone(), from.clone(), bus.dev.clone(), text).with_escalation(Some(escalation));
        bus::publish_json(&bus.js, envelope.subject(), &envelope, None).await.unwrap();
    }

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // t2 and t3 run side by side: their messages to carol come in either order.
        let (first, second) = (next_envelope(&mut to_carol).await, next_envelope(&mut to_carol).await);
        let (answer, notice) = if first.thread_id == t2 { (first, second) } else { (second, first) };
        assert_eq!((&answer.thread_id, answer.escalation.as_deref()), (&t2, Some(&leader)), "the leader's answer");
        assert!(answer.text.ends_with("The reply to this message goes to carol.\n\nhelp more"), "{}", answer.text);
        // Out of turns in the thread notes, the member's answer goes one level up with the escalation, and carol
        // hears.
        let up = next_envelope(&mut to_boss).await;
        assert_eq!(up.escalation.as_deref().map(|e| (e.level, &e.stuck)), Some((2, &carol)));
        assert!(up.text.contains("Unposted answer from dev-a:"), "{}", up.text);
        assert_eq!(notice.thread_id, t3);
        timeout(WAIT, async {
            while read_notes(&bus, &t1).await.is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the member's answer in the thread notes");
        assert!(notice.text.starts_with("[ants] dev-a escalated thread"), "{}", notice.text);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), to_carol.next()).await.is_err(), "a member does not answer carol");
    assert!(timeout(Duration::from_millis(50), to_lead.next()).await.is_err());
    let posts = read_notes(&bus, &t1).await;
    assert_eq!(posts.len(), 1, "the member's answer");
    assert_eq!((&posts[0].from, posts[0].turn_budget), (&bus.dev, DEFAULT_TURN_BUDGET - 1));
    assert!(posts[0].text.ends_with("not to carol.\n\nhelp"), "{}", posts[0].text);
    let prompt = log.locked().answered.iter().find(|p| p.ends_with("\n\nhelp")).cloned().unwrap();
    assert!(
        prompt.starts_with(&format!(
            "[ants] dev-a is part of an escalation group for carol's thread {t1}, level 1; members: lead, dev-a."
        )),
        "{prompt}"
    );
    assert!(prompt.contains("Only lead resolves it"), "{prompt}");
    let grant = read_notes(&bus, &t3).await.pop().unwrap();
    assert_eq!((&grant.from, grant.turn_budget), (&bus.dev, DEFAULT_TURN_BUDGET), "a fresh budget at level 2");
}

#[tokio::test]
async fn a_member_sending_the_escalation_higher_moves_the_stuck_thread_up_with_it() {
    let Some((mut bus, _to_human)) = Bus::start().await else { return };
    let [lead, boss, carol] = ants(["lead", "boss", "carol"]);
    let after = Duration::from_secs(600);
    bus.chain = Chain { escalate_to: vec![lead.clone()], after: Some(after), ..Chain::default() };
    bus.put_colony(&lead, std::slice::from_ref(&boss)).await;
    let [mut to_lead, mut to_boss] = inboxes(&bus, &[lead.clone(), boss.clone()]).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send_spent(&carol, &t1, "stuck", None).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let one = next_envelope(&mut to_lead).await.escalation.expect("the escalation");
        let grant = read_notes(&bus, &t1).await.pop().expect("the thread notes have a fresh budget");
        assert_eq!((&grant.from, grant.turn_budget), (&bus.dev, DEFAULT_TURN_BUDGET));
        assert!(grant.text.contains("level 1: sent to lead; lead leads"), "{}", grant.text);

        // lead sends it one level higher and says so: the thread waits for boss from then on.
        let two = escalated(&bus.dev, &t1, &[std::slice::from_ref(&lead), std::slice::from_ref(&boss)]);
        let climbed = Envelope::on_thread(t1.clone(), lead.clone(), bus.dev.clone(), "[ants] lead escalated")
            .with_escalation(Some(two.clone()));
        let seq = bus::publish_json(&bus.js, climbed.subject(), &climbed, None).await.unwrap();
        bus.until_acked(seq).await;
        let pending = bus.ledger.pending(&t1).await.unwrap().expect("the escalation");
        assert_eq!((pending.escalation.level, &pending.to, pending.renewed_by), (2, &vec![boss.clone()], None));
        assert!(pending.deadline.is_some(), "the stuck ant keeps the deadline");

        // Stuck again before boss answers: boss gets it again, not lead.
        bus.send_spent(&carol, &t1, "still stuck", None).await;
        let again = next_envelope(&mut to_boss).await;
        assert_eq!(again.escalation.as_deref(), Some(&two));
        bus.send_spent(&boss, &t1, "from boss", Some(&two)).await;
        let pending = timeout(WAIT, async {
            loop {
                let pending = bus.ledger.pending(&t1).await.unwrap().unwrap();
                if pending.renewed_by.is_some() {
                    break pending;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("boss renews the thread");
        assert_eq!(pending.escalation.level, 2);
        assert_eq!(one.level, 1);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(timeout(Duration::from_millis(100), to_lead.next()).await.is_err(), "lead got level 1 only");
    assert_eq!(log.locked().answered, ["from boss"], "the notice is the daemon's alone");
}

#[tokio::test]
async fn a_thread_a_human_started_stays_hers_and_goes_back_to_her_out_of_turns() {
    let Some((mut bus, _)) = Bus::start().await else { return };
    bus.humans = Registry::with(ants_test_support::human(), &["alice"]);
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    // dev-a is the channel's messenger and a member.
    let mut entry = ChannelEntry::new("ops".parse().unwrap());
    entry.add_member(bus.dev.clone());
    entry.add_member("dev-b".parse().unwrap());
    entry.messenger = Some(bus.dev.clone());
    bus.channels.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let mut replies = bus.client.subscribe(subject::chan(&entry.channel, &bus.dev)).await.unwrap();
    let alice: UserId = "alice".parse().unwrap();
    let mut alice_inbox = bus.client.subscribe(subject::inbox_of(&alice)).await.unwrap();
    bus.client.flush().await.unwrap();
    let root =
        ChannelPost::new(entry.channel.clone(), alice.clone(), "the build is red").with_owner(Some(alice.clone()));
    bus.post(&root).await;
    let bob = ChannelPost::new(entry.channel.clone(), "bob".parse().unwrap(), "no human, no triage");
    bus.post(&bob).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let answer = next_post(&mut replies).await;
        assert_eq!((&answer.thread_id, answer.in_reply_to), (&root.thread_id, Some(root.id)));
        assert_eq!((answer.owner.as_ref(), answer.turn_budget), (Some(&alice), DEFAULT_TURN_BUDGET - 1));
        // Answering alice's post: one answering dev-a's would be prompted, out of turns or not, with no reply.
        let mut spent = root.reply("dev-b".parse().unwrap(), "@dev-a over to you", &[], &bus.humans.humans()).unwrap();
        spent.turn_budget = 0;
        assert_eq!(spent.owner.as_ref(), Some(&alice), "ants' posts keep the owner");
        let seq = bus.post(&spent).await;
        let escalation = next_envelope(&mut alice_inbox).await;
        assert_eq!((&escalation.thread_id, escalation.in_reply_to), (&root.thread_id, Some(spent.id)));
        assert_eq!((&escalation.to, escalation.owner.as_ref()), (&alice, Some(&alice)));
        assert!(escalation.text.starts_with("Turn budget spent; needs a decision."), "{}", escalation.text);
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let answered = log.locked().answered.clone();
    assert_eq!(answered.len(), 1, "alice's post only: {answered:?}");
    assert!(answered[0].contains(channel::TRIAGE_RULES), "{}", answered[0]);
    assert!(answered[0].contains("Post from alice mentioning nobody:\nthe build is red"), "{}", answered[0]);
}

/// The leader's answer, carrying the escalation, is answered though it answers the stuck ant's message; that answer
/// does not carry the escalation, so the leader's turn on it sends nothing back and the two stop there, out of turns,
/// with nothing for the human (ants-iwqd6).
#[tokio::test]
async fn an_answered_escalation_ends_without_a_turn_budget_escalation_to_the_human() {
    use super::super::prompt::text::own_reply_note;
    use ants_proto::{NoReplyWhy, TimelineEntry};

    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let [lead, carol] = ants(["lead", "carol"]);
    bus.chain = Chain { escalate_to: vec![lead.clone()], after: None, ..Chain::default() };
    let [mut to_lead, mut to_carol] = inboxes(&bus, &[lead.clone(), carol.clone()]).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    bus.send_spent(&carol, &t1, "no turns", None).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        // dev-a is stuck on t1: its answer to its leader's answer does not carry the escalation.
        let notice = next_envelope(&mut to_lead).await;
        let one = notice.escalation.as_deref().expect("the escalation").clone();
        let mut answer = notice.reply(lead.clone(), "go on", Role::Ant).unwrap().with_escalation(Some(one.clone()));
        answer.turn_budget = 0;
        bus::publish_json(&bus.js, answer.subject(), &answer, None).await.unwrap();
        let reply = next_envelope(&mut to_lead).await;
        assert_eq!((reply.in_reply_to, reply.escalation.as_deref()), (Some(answer.id), None));
        assert!(reply.text.ends_with(": go on"), "{}", reply.text);

        // dev-a leads carol's escalation on t2: carol's answer to its answer, out of turns, goes no further.
        let leading = escalated(&carol, &t2, &[std::slice::from_ref(&bus.dev)]);
        let help = Envelope::on_thread(t2.clone(), carol.clone(), bus.dev.clone(), "help")
            .with_escalation(Some(leading.clone()));
        bus::publish_json(&bus.js, help.subject(), &help, None).await.unwrap();
        let answer = next_envelope(&mut to_carol).await;
        assert_eq!((answer.in_reply_to, answer.escalation.as_deref()), (Some(help.id), Some(&leading)));
        let mut thanks = answer.reply(carol.clone(), "thanks", Role::Ant).unwrap();
        thanks.turn_budget = 0;
        let seq = bus::publish_json(&bus.js, thanks.subject(), &thanks, None).await.unwrap();
        let (message, entry) = no_reply_on(&bus, &t2).await;
        assert_eq!(message, thanks.id);
        assert!(matches!(entry, TimelineEntry::NoReply { why: NoReplyWhy::ReplyToOwn, .. }), "{entry:?}");
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    let answered = log.locked().answered.clone();
    assert_eq!(answered.len(), 3, "{answered:?}");
    assert_eq!(answered[0], "go on", "prompted as an ordinary message");
    assert_eq!(answered[2], format!("{}\n\nthanks", own_reply_note(&carol, false, true)));
    let quiet = Duration::from_millis(200);
    assert!(timeout(quiet, to_carol.next()).await.is_err(), "nothing goes back to carol");
    assert!(timeout(quiet, to_lead.next()).await.is_err(), "nothing more goes to lead");
    assert!(timeout(quiet, to_human.next()).await.is_err(), "nothing went to the human");
}

/// The leader passing the escalation on to the human answers the stuck ant all the same; the stuck ant's answer to
/// that sends nothing back, and nothing more reaches the human (ants-iwqd6).
#[tokio::test]
async fn a_pass_on_to_the_human_is_not_followed_by_a_second_escalation() {
    use ants_proto::{NoReplyWhy, TimelineEntry};

    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let home = bus._server.dir().join("pass-on");
    std::fs::create_dir_all(&home).unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let escalator = Some(Arc::new(bus.escalator()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), escalator).await;
    bus.mcp = Some(mcp.unwrap());
    let [carol] = ants(["carol"]);
    let [mut to_carol] = inboxes(&bus, std::slice::from_ref(&carol).try_into().unwrap()).await;
    let t1 = ThreadId::generate();
    // dev-a leads carol's escalation, with no chain above it.
    let leading = escalated(&carol, &t1, &[std::slice::from_ref(&bus.dev)]);
    let help = Envelope::on_thread(t1.clone(), carol.clone(), bus.dev.clone(), "hold: help")
        .with_escalation(Some(leading.clone()));
    bus::publish_json(&bus.js, help.subject(), &help, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let passed = call_tool(bus.mcp.as_ref().unwrap(), &t1, "escalate", serde_json::json!({"issue": "not mine"}));
        let passed = passed.await;
        assert_ne!(passed.is_error, Some(true), "{passed:?}");
        let up = next_envelope(&mut to_human).await;
        let escalation = up.escalation.as_deref().expect("the escalation");
        assert_eq!((&escalation.stuck, &escalation.thread_id), (&carol, &t1));
        gate.add_permits(1);
        let answer = next_envelope(&mut to_carol).await;
        assert_eq!((answer.in_reply_to, answer.escalation.as_deref()), (Some(help.id), Some(&leading)));
        let mut waiting = answer.reply(carol.clone(), "awaiting her decision", Role::Ant).unwrap();
        waiting.turn_budget = 0;
        let seq = bus::publish_json(&bus.js, waiting.subject(), &waiting, None).await.unwrap();
        let (message, entry) = no_reply_on(&bus, &t1).await;
        assert_eq!(message, waiting.id);
        assert!(matches!(entry, TimelineEntry::NoReply { why: NoReplyWhy::ReplyToOwn, .. }), "{entry:?}");
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    let quiet = Duration::from_millis(200);
    assert!(timeout(quiet, to_carol.next()).await.is_err(), "nothing goes back to carol");
    assert!(timeout(quiet, to_human.next()).await.is_err(), "one escalation reached the human");
}

/// After a chain member's decide_approval, the member's answer and the requester's answer to it stop there: the
/// requester's does not carry the approval escalation, so the member's turn on it sends nothing back (ants-iwqd6).
#[tokio::test]
async fn a_chain_member_s_decision_ends_without_a_turn_budget_escalation_to_the_human() {
    use ants_proto::{ApprovalRequest, NoReplyWhy, TimelineEntry, ToolCall, Verdict};

    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let home = bus._server.dir().join("decide");
    std::fs::create_dir_all(&home).unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await;
    bus.mcp = Some(mcp.unwrap());
    let [lead, gate_ant] = ants(["lead", "gate-ant"]);
    let [mut to_lead, mut to_gate] = inboxes(&bus, &[lead.clone(), gate_ant.clone()]).await;
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let request_of = |ant: &UserId, thread: &ThreadId, chain: &[UserId]| {
        let tool =
            ToolCall { kind: "execute".to_owned(), title: "touch x".to_owned(), locations: Vec::new(), input: None };
        let verdicts = vec![Verdict::AllowOnce, Verdict::Reject];
        let request = ApprovalRequest::new(ant.clone(), thread.clone(), tool, verdicts, Duration::from_secs(60));
        let ants_proto::Hop::Chain { escalation, .. } = ants_proto::Escalation::of_request(&request).up(chain, 3)
        else {
            panic!("a chain hop")
        };
        *escalation
    };
    // dev-a, in gate-ant's chain, decides its request on t2, then answers it.
    let theirs = request_of(&gate_ant, &t2, std::slice::from_ref(&bus.dev));
    let asked = Envelope::on_thread(t2.clone(), gate_ant.clone(), bus.dev.clone(), "hold: may it?")
        .with_escalation(Some(theirs.clone()));
    bus::publish_json(&bus.js, asked.subject(), &asked, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        let verdict = serde_json::json!({"verdict": "allow_once"});
        let decided = call_tool(bus.mcp.as_ref().unwrap(), &t2, "decide_approval", verdict).await;
        assert_ne!(decided.is_error, Some(true), "{decided:?}");
        assert!(next_envelope(&mut to_gate).await.approval.is_some(), "the decision");
        gate.add_permits(1);
        let answer = next_envelope(&mut to_gate).await;
        assert_eq!((answer.in_reply_to, answer.escalation.as_deref()), (Some(asked.id), Some(&theirs)));
        let mut confirmed = answer.reply(gate_ant.clone(), "confirmed", Role::Ant).unwrap();
        confirmed.turn_budget = 0;
        let seq = bus::publish_json(&bus.js, confirmed.subject(), &confirmed, None).await.unwrap();
        let (message, entry) = no_reply_on(&bus, &t2).await;
        assert_eq!(message, confirmed.id);
        assert!(matches!(entry, TimelineEntry::NoReply { why: NoReplyWhy::ReplyToOwn, .. }), "{entry:?}");
        bus.until_acked(seq).await;

        // lead decided dev-a's own request on t1 and answers it: dev-a's answer does not carry the escalation.
        let ours = request_of(&bus.dev, &t1, std::slice::from_ref(&lead));
        let mut allowed =
            Envelope::on_thread(t1.clone(), lead.clone(), bus.dev.clone(), "allowed it").with_escalation(Some(ours));
        allowed.in_reply_to = Some(Uuid::now_v7());
        bus::publish_json(&bus.js, allowed.subject(), &allowed, None).await.unwrap();
        let reply = next_envelope(&mut to_lead).await;
        assert_eq!((reply.in_reply_to, reply.escalation.as_deref()), (Some(allowed.id), None));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    let quiet = Duration::from_millis(200);
    assert!(timeout(quiet, to_gate.next()).await.is_err(), "nothing goes back to gate-ant");
    assert!(timeout(quiet, to_human.next()).await.is_err(), "nothing went to the human");
}

/// A provider's wake brings turns of its own: prompted on a thread out of turns, and answered to the human.
#[tokio::test]
async fn a_provider_s_wake_is_prompted_on_a_thread_out_of_turns_and_answered_to_the_human() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [dev_b] = ants(["dev-b"]);
    let t1 = ThreadId::generate();
    bus.send_spent(&dev_b, &t1, "no turns", None).await;
    let wake = Envelope::on_thread(t1.clone(), bus.dev.clone(), bus.dev.clone(), "standup").woken_by("sense");
    bus::publish_json(&bus.js, wake.subject(), &wake, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let escalation = next_envelope(&mut to_human).await;
        assert!(escalation.text.contains("Turn budget spent"), "{}", escalation.text);
        let answer = next_envelope(&mut to_human).await;
        assert_eq!((&answer.thread_id, answer.in_reply_to, &answer.provider), (&t1, Some(wake.id), &None));
        assert!(answer.text.ends_with(": standup"), "{}", answer.text);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert_eq!(log.locked().answered, ["standup"]);
}

/// Store `directory` as the bus's team directory.
async fn put_teams(bus: &Bus, directory: &ants_proto::TeamDirectory) {
    bus.js.create_stream(crate::hub::teams_config()).await.unwrap();
    let kv = crate::teams::open(&bus.js, None).await.unwrap();
    assert!(crate::teams::write(&kv, directory, 0).await.unwrap());
}

/// A team in escalate_to stands for its lead; the lead, with no escalate_to, climbs to the lead above it, whose chain,
/// the top of the forest, goes to the owner.
#[tokio::test]
async fn a_team_in_escalate_to_reaches_its_lead_who_climbs_to_the_lead_above_then_the_owner() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let [lead, boss, carol, backend, eng] = ants(["lead", "boss", "carol", "backend", "eng"]);
    let mut directory = ants_proto::TeamDirectory::default();
    let by = ants_test_support::human();
    directory.create(ants_proto::Team::new(eng.clone(), boss.clone(), by.clone()), None).unwrap();
    directory.create(ants_proto::Team::new(backend.clone(), lead.clone(), by), Some(&eng)).unwrap();
    put_teams(&bus, &directory).await;
    bus.chain = Chain { escalate_to: vec![backend], after: None, ..Chain::default() };
    bus.put_colony(&lead, &[]).await;
    let [mut to_lead, mut to_boss] = inboxes(&bus, &[lead.clone(), boss.clone()]).await;
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send_spent(&carol, &t1, "no turns", None).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let first = next_envelope(&mut to_lead).await;
        let one = first.escalation.as_deref().expect("the escalation").clone();
        assert_eq!((one.level, &one.leader), (1, &lead), "the team's lead, in the team's stead");
        assert!(first.text.contains("Sent to lead; lead leads."), "{}", first.text);
        bus.send_spent(&lead, &t1, "go on", Some(&one)).await;
        assert!(next_envelope(&mut to_lead).await.text.ends_with(": go on"));

        // The lead has no escalate_to: the next spend goes to the lead above it.
        bus.send_spent(&lead, &t1, "and on", Some(&one)).await;
        let up = next_envelope(&mut to_boss).await;
        let two = up.escalation.as_deref().expect("the escalation").clone();
        assert_eq!((two.level, &two.leader, &two.stuck), (2, &boss, &bus.dev));
        bus.send_spent(&boss, &t1, "from boss", Some(&two)).await;
        assert!(next_envelope(&mut to_boss).await.text.ends_with(": from boss"));

        // boss leads the top team, so its chain goes to the owner.
        bus.send_spent(&boss, &t1, "again", Some(&two)).await;
        let notice = next_envelope(&mut to_human).await;
        assert!(notice.text.ends_with("boss has no escalate_to above it."), "{}", notice.text);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert_eq!(log.locked().answered, ["go on", "from boss"]);
}

/// An ant on a team with no escalate_to escalates to its team's lead before the owner; a lead, never to itself, to the
/// lead above it.
#[tokio::test]
async fn an_ant_on_a_team_with_no_escalate_to_escalates_to_the_lead_above_it_never_to_itself() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let [boss, carol, backend, eng] = ants(["boss", "carol", "backend", "eng"]);
    let mut directory = ants_proto::TeamDirectory::default();
    let by = ants_test_support::human();
    directory.create(ants_proto::Team::new(eng.clone(), boss.clone(), by.clone()), None).unwrap();
    directory.create(ants_proto::Team::new(backend, bus.dev.clone(), by), Some(&eng)).unwrap();
    put_teams(&bus, &directory).await;
    let [mut to_boss] = inboxes(&bus, std::slice::from_ref(&boss).try_into().unwrap()).await;
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let t1 = ThreadId::generate();
    bus.send_spent(&carol, &t1, "no turns", None).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let first = next_envelope(&mut to_boss).await;
        let one = first.escalation.as_deref().expect("the escalation").clone();
        let visited = vec![bus.dev.clone(), boss.clone()];
        assert_eq!((one.level, &one.leader, &one.visited), (1, &boss, &visited));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), to_human.next()).await.is_err(), "nothing went to the human");
}
