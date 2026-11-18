use std::ops::Range;

use ants_proto::SenseId;
use ants_proto::provider::{Origin, Starter};
use uuid::Uuid;

use super::*;
use crate::fork::INHERITED;
use crate::persona::{PERSONA, persona_id};
use crate::records::{Record, SELF_MODEL, record_id};
use crate::skill::{SKILL, skill_id};
use crate::thread::experience;
use crate::thread::record::record_event;

fn user(name: &str) -> UserId {
    name.parse().unwrap()
}

fn webhook() -> Starter {
    Starter::Webhook { user: user("hook") }
}

/// Note threads `threads` started by `by` in repository `repo`, one a day from `from`.
async fn started(items: &dyn Items, threads: Range<u8>, by: &Starter, repo: &str, from: DateTime<Utc>) {
    for n in threads.clone() {
        thread_started(items, &on(n).thread, by, repo, from + Duration::days(i64::from(n - threads.start))).await;
    }
}

/// Note `thread` started by `by` in repository `repo` at `at`.
async fn thread_started(items: &dyn Items, thread: &ThreadId, by: &Starter, repo: &str, at: DateTime<Utc>) {
    let origin = Origin { by: by.clone(), channel: None };
    let kind = EventKind::ThreadStarted { origin, repo: Some(repo.into()) };
    let thread = Some(thread.clone());
    let event = Event { id: Uuid::now_v7(), ant: user("dev-a"), identity: "0f1e".into(), thread, at, kind };
    record_event(items, &event).await.unwrap().unwrap();
}

/// The identity findings gathered now whose key starts with `kind`, by key with their counts.
async fn found(items: &dyn Items, kind: &str) -> Vec<(String, Option<u64>)> {
    let gathered = crate::dream::identity_findings::gather(items, &user("dev-a")).await.unwrap();
    gathered.into_iter().map(|found| (found.key(), found.count)).filter(|(key, _)| key.starts_with(kind)).collect()
}

fn keyed(found: &[(&str, u64)]) -> Vec<(String, Option<u64>)> {
    found.iter().map(|(key, count)| ((*key).to_owned(), Some(*count))).collect()
}

async fn identity(sense: &Sense, arguments: serde_json::Value) -> Answer {
    let answer = tool(sense, crate::identity::identity, arguments, context(human())).await;
    assert!(!answer.is_error, "{answer:?}");
    answer
}

#[tokio::test]
async fn no_persona_and_no_self_model_are_found_after_five_threads_until_proposed_or_written() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    assert!(found(&*items, "").await.is_empty(), "no thread yet");
    let from = Utc::now() - Duration::days(30);
    started(&*items, 20..24, &webhook(), "github.com/o/r", from).await;
    assert!(found(&*items, "").await.is_empty(), "4 threads");

    started(&*items, 24..25, &webhook(), "github.com/o/r", from).await;
    assert_eq!(found(&*items, "").await, keyed(&[("no_persona:persona", 5), ("no_self_model:self", 5)]));
    started(&*items, 25..29, &webhook(), "github.com/o/r", from).await;
    assert_eq!(found(&*items, "no_persona").await, keyed(&[("no_persona:persona", 5)]), "9 threads step down to 5");
    started(&*items, 29..30, &webhook(), "github.com/o/r", from).await;
    assert_eq!(found(&*items, "no_persona").await, keyed(&[("no_persona:persona", 10)]));

    identity(&sense, json!({"voice": "Plain.", "reason": "The threads ask for it."})).await;
    assert_eq!(found(&*items, "").await, keyed(&[("no_self_model:self", 10)]), "a persona is proposed");
    identity(&sense, json!({"self_model": "Careful.", "title": "Me"})).await;
    assert!(found(&*items, "").await.is_empty());
}

#[tokio::test]
async fn the_threads_of_dreams_and_reviews_count_toward_no_identity_finding() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let (now, repo) = (Utc::now(), "github.com/o/r");
    let (sage, writer) = (Starter::Ant { user: user("sage") }, on(9));
    started(&*items, 20..24, &webhook(), repo, now - Duration::days(30)).await;
    for n in 0..3 {
        let start = Start { recipient: Recipient::Sage(user("sage")), ..start(now + Duration::days(n), &writer) };
        let review = begin(&*items, start).await.unwrap().unwrap().review.unwrap().thread;
        thread_started(&*items, &review, &sage, repo, now + Duration::days(n)).await;
    }
    woken(&*items, 3, &on(30).thread).await.unwrap();
    started(&*items, 30..31, &Starter::Provider { name: "sense".into() }, repo, now).await;
    started(&*items, 31..32, &Starter::Review { user: user("dev-b") }, repo, now).await;
    let told = ReviewOutcomes { dream: 1, findings: Vec::new() };
    assert!(review_received(&*items, &user("dev-c"), &on(32).thread, &told, now).await.unwrap());
    started(&*items, 32..33, &Starter::Ant { user: user("dev-c") }, repo, now).await;
    assert_eq!(experience::experience(&*items, &user("dev-a"), 3).await.unwrap().unwrap().threads, 10);
    assert!(found(&*items, "").await.is_empty(), "4 threads of work, the sage started none of them");

    started(&*items, 24..25, &webhook(), repo, now).await;
    assert_eq!(found(&*items, "").await, keyed(&[("no_persona:persona", 5), ("no_self_model:self", 5)]));
}

#[tokio::test]
async fn a_relationship_record_is_asked_of_each_who_started_three_threads() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let (alice, bob) = (Starter::Human { user: user("alice") }, Starter::Human { user: user("bob") });
    let (dev_a, dev_b) = (Starter::Ant { user: user("dev-a") }, Starter::Ant { user: user("dev-b") });
    let from = Utc::now() - Duration::days(30);
    started(&*items, 20..24, &alice, "github.com/o/r", from).await;
    started(&*items, 30..32, &dev_b, "github.com/o/r", from).await;
    started(&*items, 40..42, &bob, "github.com/o/r", from).await;
    started(&*items, 50..53, &dev_a, "github.com/o/r", from).await;
    assert_eq!(found(&*items, "no_relationship").await, keyed(&[("no_relationship:human:alice", 3)]), "not itself");

    started(&*items, 32..33, &dev_b, "github.com/o/r", from).await;
    let both = keyed(&[("no_relationship:human:alice", 3), ("no_relationship:ant:dev-b", 3)]);
    assert_eq!(found(&*items, "no_relationship").await, both, "the most threads first");

    identity(&sense, json!({"relationship": "Brief asks.", "who": "human:alice", "title": "Alice"})).await;
    assert_eq!(found(&*items, "no_relationship").await, keyed(&[("no_relationship:ant:dev-b", 3)]));
}

#[tokio::test]
async fn a_self_model_unchanged_over_thirty_active_days_with_someone_new_is_found_stale() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let alice = Starter::Human { user: user("alice") };
    let now = Utc::now();
    started(&*items, 10..12, &alice, "github.com/o/r", now - Duration::days(2)).await;
    // The store's clock stamps the self-model now, so the threads after it are those started from tomorrow.
    let model = identity(&sense, json!({"self_model": "Careful.", "title": "Me"})).await.json["id"].clone();
    let (tomorrow, day) = (now + Duration::days(1), |n: i64| now + Duration::days(n));
    started(&*items, 20..49, &alice, "github.com/o/r", tomorrow).await;
    started(&*items, 60..61, &Starter::Human { user: user("bob") }, "github.com/o/r", day(2)).await;
    assert!(found(&*items, "stale").await.is_empty(), "29 active days, bob new");

    started(&*items, 49..50, &alice, "github.com/o/r", day(30)).await;
    let stale = crate::dream::identity_findings::gather(&*items, &user("dev-a")).await.unwrap();
    let stale: Vec<_> = stale.iter().filter(|found| found.kind == FindingKind::StaleSelfModel).collect();
    assert_eq!(stale.len(), 1);
    assert_eq!((stale[0].key(), stale[0].count), ("stale_self_model:self".into(), Some(1)));
    assert_eq!(json!(stale[0].items), json!([model]));
    assert!(
        stale[0].text.starts_with(
            "The self-model, revision 1, is unchanged over 30 active days in which the ant worked with human:bob for \
             the first time: revise it with sense_identity, giving self_model"
        ),
        "{}",
        stale[0].text
    );

    started(&*items, 70..71, &alice, "github.com/o/s", day(31)).await;
    assert_eq!(found(&*items, "stale").await, keyed(&[("stale_self_model:self", 2)]), "and a new repository");
}

#[tokio::test]
async fn a_self_model_unchanged_with_no_one_new_is_not_stale() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let alice = Starter::Human { user: user("alice") };
    let now = Utc::now();
    started(&*items, 10..11, &alice, "github.com/o/r", now - Duration::days(1)).await;
    started(&*items, 11..12, &Starter::Human { user: user("bob") }, "github.com/o/s", now - Duration::days(1)).await;
    identity(&sense, json!({"self_model": "Careful.", "title": "Me"})).await;
    started(&*items, 20..60, &alice, "github.com/o/s", now + Duration::days(1)).await;
    assert!(found(&*items, "stale").await.is_empty(), "40 active days, all known");
}

#[tokio::test]
async fn what_a_clone_inherited_is_found_after_five_threads_of_its_own_until_made_its_own() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let cloned = Event {
        id: Uuid::now_v7(),
        ant: user("dev-a"),
        identity: "0f1e".into(),
        thread: None,
        at: Utc::now() - Duration::days(40),
        kind: EventKind::AntCloned { from: user("dev-z"), from_identity: "9a8b".into() },
    };
    crate::identity::record_event(&*items, &cloned).await.unwrap();
    let inherited = |mut values: Values| {
        values.insert(INHERITED.into(), json!(SenseId::new()));
        values
    };
    let persona = inherited([("voice".to_owned(), json!("Plain."))].into_iter().collect());
    let model = Record {
        scope: Scope::Own,
        title: "Me".into(),
        body: "Careful.".into(),
        writer: context(human()),
        trust: memory::Trust::Own,
        inherited: Some(SenseId::new()),
    };
    let Value::Object(model) = json!(model) else { unreachable!() };
    let skill = |name: &str| {
        let content = format!("---\nname: {name}\ndescription: Do {name}.\n---\n# {name}\n");
        let values = json!({"name": name, "description": format!("Do {name}."), "files": {"SKILL.md": content}});
        let Value::Object(values) = values else { unreachable!() };
        Write::Create { id: skill_id(name), kind: SKILL.into(), values }
    };
    let Write::Create { id, kind, values } = skill("release") else { unreachable!() };
    let writes = [
        Write::Create { id: persona_id(), kind: PERSONA.into(), values: persona },
        Write::Create { id: record_id(&Scope::Own).unwrap(), kind: SELF_MODEL.into(), values: model },
        Write::Create { id, kind, values: inherited(values) },
        skill("notes"),
    ];
    items.write(&writes, "test").await.unwrap();
    let from = Utc::now() - Duration::days(30);
    started(&*items, 20..24, &webhook(), "github.com/o/r", from).await;
    assert!(found(&*items, "").await.is_empty(), "4 threads of its own");

    started(&*items, 24..25, &webhook(), "github.com/o/r", from).await;
    let all = keyed(&[("inherited:persona", 5), ("inherited:self", 5), ("inherited:skill:release", 5)]);
    assert_eq!(found(&*items, "").await, all);
    let gathered = crate::dream::identity_findings::gather(&*items, &user("dev-a")).await.unwrap();
    let ids: Vec<_> = gathered.iter().map(|found| found.items.clone()).collect();
    assert_eq!(ids, [vec![persona_id()], vec![record_id(&Scope::Own).unwrap()], vec![skill_id("release")]]);
    assert!(
        gathered[2].text.starts_with(
            "The skill release is still as inherited on clone from dev-z, after 5 threads of the ant's own: write it \
             anew as the ant's own with sense_skill, giving name release,"
        ),
        "{}",
        gathered[2].text
    );
    assert!(gathered[2].text.ends_with("or reject this finding to keep it as it is."), "{}", gathered[2].text);
    begin(&*items, start(Utc::now(), &on(9))).await.unwrap().unwrap();
    woken(&*items, 1, &on(1).thread).await.unwrap();
    let model = record_id(&Scope::Own).unwrap();
    let applied = |finding: &str| json!({"verdicts": [{"finding": finding, "verdict": "applied", "reason": "Mine.", "items": [model]}]});
    let refused = answer(&sense, 1, applied("1.3")).await;
    assert!(refused.text.starts_with("Finding 1.3 (inherited): applied names the skill proposal made;"), "{refused:?}");
    assert!(!answer(&sense, 1, applied("1.2")).await.is_error, "the self-model answers its own");

    identity(&sense, json!({"voice": "Plainer.", "reason": "Its own."})).await;
    identity(&sense, json!({"self_model": "Bolder.", "title": "Me", "revision": 1})).await;
    assert_eq!(found(&*items, "").await, keyed(&[("inherited:skill:release", 5)]), "proposed and written anew");
}

#[tokio::test]
async fn identity_findings_come_after_the_open_ones_within_the_cap() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    for n in 0..MAX_FINDINGS - 1 {
        store(&kept, &Memory { status: Status::NeedsReview, ..lesson(&format!("Doubted {n}")) }).await;
    }
    let now = Utc::now();
    let day = |n: i64| now + Duration::days(n);
    let first = begin(&*items, start(day(0), &on(9))).await.unwrap().unwrap();
    assert_eq!(first.findings.len(), MAX_FINDINGS - 1);

    started(&*items, 20..25, &webhook(), "github.com/o/r", now - Duration::days(10)).await;
    let second = begin(&*items, start(day(1), &on(9))).await.unwrap().unwrap();
    let kinds: Vec<_> = second.findings.iter().map(|finding| (finding.id.as_str(), finding.kind)).collect();
    assert_eq!(kinds.len(), MAX_FINDINGS);
    assert!(
        kinds[..MAX_FINDINGS - 1].iter().all(|(id, kind)| id.starts_with("1.") && *kind == FindingKind::NeedsReview)
    );
    assert_eq!(kinds[MAX_FINDINGS - 1], ("2.1", FindingKind::NoPersona), "no room for no_self_model");
    assert!(!second.findings[MAX_FINDINGS - 1].untrusted);

    started(&*items, 25..30, &webhook(), "github.com/o/r", now - Duration::days(5)).await;
    let third = begin(&*items, start(day(2), &on(9))).await.unwrap().unwrap();
    let again = &third.findings[MAX_FINDINGS - 1];
    assert_eq!((again.id.as_str(), again.count), ("2.1", Some(10)), "offered again as gathered now");
    assert!(again.text.starts_with("No persona after 10 threads"), "{}", again.text);
}

#[tokio::test]
async fn an_identity_finding_is_applied_naming_the_record_written_or_rests_when_rejected() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let day = |n: i64| now + Duration::days(n);
    started(&*items, 20..25, &webhook(), "github.com/o/r", now - Duration::days(10)).await;
    let first = begin(&*items, start(day(0), &on(9))).await.unwrap().unwrap();
    let kinds: Vec<_> = first.findings.iter().map(|finding| (finding.id.as_str(), finding.kind)).collect();
    assert_eq!(kinds, [("1.1", FindingKind::NoPersona), ("1.2", FindingKind::NoSelfModel)]);
    woken(&*items, 1, &on(1).thread).await.unwrap();
    let shown = answer(&sense, 1, json!({})).await;
    assert!(shown.text.contains("- 1.2 (no_self_model): No self-model after 5 threads: write one"), "{}", shown.text);

    let written = sense.call_tool(called("identity", 1, &json!({"self_model": "Careful.", "title": "Me"}))).await;
    let Output::Json(written) = written.unwrap().output else { panic!("not json") };
    let model = written["id"].clone();
    let verdicts = json!({"verdicts": [
        {"finding": "1.1", "verdict": "rejected", "reason": "Not yet."},
        {"finding": "1.2", "verdict": "applied", "reason": "Written.", "items": [model]},
    ]});
    let answered = answer(&sense, 1, verdicts).await;
    assert!(!answered.is_error, "{answered:?}");
    let (dream, _) = get(&*items, 1).await.unwrap().unwrap();
    assert_eq!(json!(dream.verdicts[1].items), json!([model]));
    assert_eq!(json!(dream.writes), json!([model]));

    let second = begin(&*items, start(day(1), &on(9))).await.unwrap().unwrap();
    assert!(second.findings.is_empty(), "the persona's rests, the self-model is written: {:?}", second.findings);
    started(&*items, 25..30, &webhook(), "github.com/o/r", now - Duration::days(5)).await;
    let third = begin(&*items, start(day(2), &on(9))).await.unwrap().unwrap();
    let kinds: Vec<_> = third.findings.iter().map(|finding| (finding.id.as_str(), finding.kind)).collect();
    assert_eq!(kinds, [("3.1", FindingKind::NoPersona)], "10 threads now");
}

#[tokio::test]
async fn an_identity_finding_applied_names_the_item_of_the_kind_it_asks_for() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    started(&*items, 20..25, &Starter::Human { user: user("alice") }, "github.com/o/r", now - Duration::days(10)).await;
    let first = begin(&*items, start(now, &on(9))).await.unwrap().unwrap();
    let kinds: Vec<_> = first.findings.iter().map(|finding| (finding.id.as_str(), finding.kind)).collect();
    let asked =
        [("1.1", FindingKind::NoPersona), ("1.2", FindingKind::NoSelfModel), ("1.3", FindingKind::NoRelationship)];
    assert_eq!(kinds, asked);
    woken(&*items, 1, &on(1).thread).await.unwrap();
    let applied = |finding: &str, item: &serde_json::Value| json!({"verdicts": [{"finding": finding, "verdict": "applied", "reason": "Written.", "items": [item]}]});
    let lesson = json!(store(&kept, &lesson("Alice asks for tests")).await);
    let bob = json!({"relationship": "Terse.", "who": "human:bob", "title": "Bob"});
    let bob = identity(&sense, bob).await.json["id"].clone();
    let model = identity(&sense, json!({"self_model": "Careful.", "title": "Me"})).await.json["id"].clone();
    for (finding, item, asks) in [
        ("1.1", &model, "Finding 1.1 (no_persona): applied names the persona proposal made;"),
        ("1.2", &lesson, "Finding 1.2 (no_self_model): applied names the self-model written;"),
        ("1.3", &bob, "Finding 1.3 (no_relationship): applied names the relationship record of human:alice written;"),
    ] {
        let refused = answer(&sense, 1, applied(finding, item)).await;
        assert_eq!(error(&refused), "invalid_arguments");
        assert!(refused.text.starts_with(asks), "{}", refused.text);
    }

    let proposed = json!({"voice": "Plain.", "reason": "The threads ask for it.", "finding": "1.1"});
    let proposal = identity(&sense, proposed).await.json["proposal"]["id"].clone();
    let alice = json!({"relationship": "Tests first.", "who": "human:alice", "title": "Alice"});
    let alice = identity(&sense, alice).await.json["id"].clone();
    for (finding, item) in [("1.1", &proposal), ("1.2", &model), ("1.3", &alice)] {
        let answered = answer(&sense, 1, applied(finding, item)).await;
        assert!(!answered.is_error, "{answered:?}");
    }
}
