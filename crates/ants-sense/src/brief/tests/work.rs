//! Tests of personal work in the brief: its place, open commitments, ready tasks and claims, expired claims, reminder
//! firings until acknowledged, and cuts within UTF-8 byte budgets.

use chrono::Duration;

use super::*;

async fn at(sense: &Sense, handler: Handler, thread: &str, time: &str, arguments: Value) -> Answer {
    let call = ToolCall {
        name: "tool".into(),
        arguments: arguments.as_object().unwrap().clone(),
        context: CallContext { at: time.parse().unwrap(), ..context(thread) },
        format: Format::Json,
    };
    let answer = handler(sense, &call).await;
    assert!(!answer.is_error, "{answer:?}");
    answer
}

async fn task(sense: &Sense, title: &str, priority: u8) -> ItemId {
    let answer = call(sense, task::task, THREAD, json!({"action":"create","title":title,"priority":priority})).await;
    answer.json["task"]["id"].as_str().unwrap().parse().unwrap()
}

async fn promise(sense: &Sense, title: &str, due: &str) -> ItemId {
    let answer = call(sense, promise::promise, OTHER, json!({"title":title,"promisee":"alice","due":due})).await;
    answer.json["promise"]["id"].as_str().unwrap().parse().unwrap()
}

async fn reminder(sense: &Sense, title: &str, trigger: Value) -> ItemId {
    let answer = call(sense, remind::remind, OTHER, json!({"title":title,"trigger":trigger})).await;
    answer.json["reminder"]["id"].as_str().unwrap().parse().unwrap()
}

fn section<'a>(brief: &'a str, heading: &str) -> &'a str {
    let start = position(brief, heading);
    let tail = &brief[start + heading.len()..];
    let end = tail.find("\n\n## ").unwrap_or(tail.len());
    &brief[start..start + heading.len() + end]
}

#[tokio::test]
async fn personal_work_follows_handoffs_before_memories_and_tools() {
    let (sense, _) = filled().await;
    let future = promise(&sense, "Send the release notes", "2025-01-03T12:00:00Z").await;
    let overdue = promise(&sense, "Deliver the parser", "2025-01-01T10:00:00Z").await;
    let oldest = promise(&sense, "Send the design", "2024-12-31T12:00:00Z").await;
    let closed = promise(&sense, "Already sent", "2024-12-30T12:00:00Z").await;
    call(&sense, promise::promise, OTHER, json!({"action":"keep","id":closed})).await;
    let fired = reminder(&sense, "Check the build", json!({"kind":"at","at":"2025-01-01T11:00:00Z"})).await;
    let items = sense.items(&context(THREAD).ant).await.unwrap();
    let firing = remind::prepare_firing(&*items, &fired, context(THREAD).at, "time due").await.unwrap().unwrap();
    let upcoming = reminder(&sense, "Review the result", json!({"kind":"after","seconds":3600})).await;
    let claimed = task(&sense, "Write parser tests", 1).await;
    let claim = call(&sense, task::task, THREAD, json!({"action":"claim","id":claimed})).await;
    let ready = task(&sense, "Prepare changelog", 2).await;
    let brief = session_brief(&sense, 12_000).await;
    let headings = [
        "## Identity",
        "## This thread",
        "## Unacknowledged handoffs",
        "## Open commitments",
        "## Fired reminders",
        "## Upcoming reminders",
        "## Claimed tasks (this thread)",
        "## Ready tasks",
        "## Memories",
        "## Core loop",
    ];
    let positions: Vec<_> = headings.iter().map(|heading| position(&brief, heading)).collect();
    assert!(positions.is_sorted(), "{brief}");
    let commitments = section(&brief, "## Open commitments");
    let promises: Vec<_> = [&oldest, &overdue, &future].iter().map(|id| position(commitments, id.as_str())).collect();
    assert!(promises.is_sorted(), "{commitments}");
    assert_eq!(commitments.matches(", overdue").count(), 2, "{commitments}");
    for id in [&oldest, &overdue, &future] {
        position(commitments, &format!("{id} (revision 1, open"));
    }
    position(commitments, &format!("source thread {OTHER}"));
    assert!(!brief.contains(closed.as_str()), "{brief}");
    let reminders = section(&brief, "## Fired reminders");
    position(reminders, &format!("{fired} (revision 2, fired)"));
    position(reminders, &format!("firing {}", firing.id));
    position(reminders, "delivery pending");
    position(section(&brief, "## Upcoming reminders"), upcoming.as_str());
    let claims = section(&brief, "## Claimed tasks (this thread)");
    position(claims, &format!("{claimed} P1 in_progress: Write parser tests (revision 2)"));
    position(claims, &format!("claim token {}", claim.json["token"]));
    position(section(&brief, "## Ready tasks"), ready.as_str());
    assert!(!brief.contains(MORE), "{brief}");
    assert_eq!(brief.matches(RECALLED_BEGIN).count(), 1);
    for tool in ["task", "promise", "remind"] {
        position(section(&brief, "## Core loop"), &format!("- sense_{tool}: "));
    }
    let other = BriefRequest { context: context(OTHER), budget: 12_000, reason: BriefReason::Started };
    let other = sense.brief(other).await.unwrap().text;
    assert_eq!(section(&other, "## Open commitments"), commitments, "commitments cross thread boundaries");
    assert!(!other.contains("## Claimed tasks"), "{other}");
}

#[tokio::test]
async fn every_open_commitment_is_included_without_a_memory_or_handoff_count_limit() {
    let (sense, _) = sense().await;
    let mut ids = Vec::new();
    for i in 0..12 {
        ids.push(promise(&sense, &format!("Deliver milestone {i}"), "2025-01-02T12:00:00Z").await);
    }
    let brief = session_brief(&sense, 20_000).await;
    let commitments = section(&brief, "## Open commitments");
    for id in ids {
        position(commitments, id.as_str());
    }
    assert!(!commitments.contains(MORE), "{commitments}");
}

#[tokio::test]
async fn only_ready_tasks_and_this_threads_claims_appear_with_usable_fences() {
    let (sense, _) = sense().await;
    let older = at(
        &sense,
        task::task,
        THREAD,
        "2025-01-01T10:00:00Z",
        json!({"action":"create","title":"Older low-priority task","priority":3}),
    )
    .await;
    let older = older.json["task"]["id"].as_str().unwrap();
    let prerequisite = task(&sense, "Unfinished prerequisite", 2).await;
    let blocked = call(
        &sense,
        task::task,
        THREAD,
        json!({"action":"create","title":"Dependent task","priority":0,
               "dependencies":[{"kind":"blocks","id":prerequisite}]}),
    )
    .await;
    let blocked = blocked.json["task"]["id"].as_str().unwrap();
    for status in ["blocked", "deferred"] {
        call(
            &sense,
            task::task,
            THREAD,
            json!({"action":"create","title":format!("Explicitly {status}"),"status":status}),
        )
        .await;
    }
    let own = call(
        &sense,
        task::task,
        THREAD,
        json!({"action":"create","title":"Implement the parser","priority":0,"external_ref":"bd:ants-123"}),
    )
    .await;
    let own = own.json["task"]["id"].as_str().unwrap();
    let claimed = call(&sense, task::task, THREAD, json!({"action":"claim","id":own})).await;
    let others = task(&sense, "Another thread's work", 0).await;
    call(&sense, task::task, OTHER, json!({"action":"claim","id":others})).await;
    let first = task(&sense, "First high-priority task", 1).await;
    let second = at(
        &sense,
        task::task,
        THREAD,
        "2025-01-01T12:01:00Z",
        json!({"action":"create","title":"Second high-priority task","priority":1}),
    )
    .await;
    let second = second.json["task"]["id"].as_str().unwrap();
    promise(&sense, "Not a task", "2025-01-02T12:00:00Z").await;
    let brief = session_brief(&sense, 12_000).await;
    let ready = section(&brief, "## Ready tasks");
    let ordered: Vec<_> =
        [first.as_str(), second, prerequisite.as_str(), older].iter().map(|id| position(ready, id)).collect();
    assert!(ordered.is_sorted(), "{ready}");
    for absent in [own, blocked, others.as_str(), "Explicitly blocked", "Explicitly deferred", "Not a task"] {
        assert!(!ready.contains(absent), "{absent} in {ready}");
    }
    assert!(!brief.contains(others.as_str()), "another thread's lease is not this thread's work: {brief}");
    let claims = section(&brief, "## Claimed tasks (this thread)");
    position(claims, "external bd:ants-123");
    position(claims, &format!("thread {THREAD}, lease until 2025-01-01T12:05:00+00:00"));
    let token: u64 = claims.split("claim token ").nth(1).unwrap().split_whitespace().next().unwrap().parse().unwrap();
    assert_eq!(Some(token), claimed.json["token"].as_u64());
    let heartbeat =
        at(&sense, task::task, THREAD, "2025-01-01T12:01:00Z", json!({"action":"heartbeat","id":own,"token":token}))
            .await;
    assert_eq!(heartbeat.json["task"]["revision"], 3);
    assert!(!brief.contains("## Fired reminders"), "{brief}");
}

#[tokio::test]
async fn expired_claims_are_not_live_or_ready_and_give_a_reclaim_action() {
    let (sense, _) = sense().await;
    let id = task(&sense, "Resume interrupted tests", 1).await;
    at(&sense, task::task, THREAD, "2025-01-01T11:55:00Z", json!({"action":"claim","id":id})).await;
    let brief = session_brief(&sense, 8_000).await;
    let claims = section(&brief, "## Claimed tasks (this thread)");
    position(claims, &format!("lease expired; claim again: sense_task action=claim id={id}"));
    assert!(!claims.contains("claim token"), "{claims}");
    assert!(!brief.contains("## Ready tasks"), "{brief}");
    let claim = call(&sense, task::task, THREAD, json!({"action":"claim","id":id})).await;
    let brief = session_brief(&sense, 8_000).await;
    let claims = section(&brief, "## Claimed tasks (this thread)");
    position(claims, &format!("claim token {}", claim.json["token"]));
    assert!(!claims.contains("expired"), "{claims}");
}

#[tokio::test]
async fn reminder_firings_stay_visible_after_delivery_until_acknowledged_or_postponed() {
    let (sense, items) = sense().await;
    let id = reminder(&sense, "Check release status", json!({"kind":"at","at":"2025-01-01T10:00:00Z"})).await;
    let now = context(THREAD).at;
    let firing = remind::prepare_firing(&*items, &id, now, "time due").await.unwrap().unwrap();
    remind::confirm_delivered(&*items, &id, &firing.id, now).await.unwrap();
    let brief = session_brief(&sense, 8_000).await;
    let fired = section(&brief, "## Fired reminders");
    position(fired, &format!("firing {} at 2025-01-01T12:00:00Z", firing.id));
    position(fired, "awaiting acknowledgement");
    assert!(!brief.contains("## Upcoming reminders"), "{brief}");

    call(&sense, remind::remind, OTHER, json!({"action":"postpone","id":id,"after_seconds":7200})).await;
    let brief = session_brief(&sense, 8_000).await;
    assert!(!brief.contains("## Fired reminders") && !brief.contains(&firing.id), "{brief}");
    position(section(&brief, "## Upcoming reminders"), "; next 2025-01-01T14:00:00Z");
    call(&sense, remind::remind, OTHER, json!({"action":"ack","id":id})).await;
    let brief = session_brief(&sense, 8_000).await;
    assert!(!brief.contains("## Upcoming reminders") && !brief.contains("## Fired reminders"), "{brief}");

    let exhausted = reminder(&sense, "Still needs acknowledgement", json!({"kind":"after","seconds":1})).await;
    for hour in 0..4 {
        let at = now + Duration::seconds(1) + Duration::hours(hour);
        let firing = remind::prepare_firing(&*items, &exhausted, at, "time due").await.unwrap().unwrap();
        remind::confirm_delivered(&*items, &exhausted, &firing.id, at).await.unwrap();
    }
    let brief = session_brief(&sense, 8_000).await;
    position(section(&brief, "## Fired reminders"), &format!("firing {exhausted}:4"));
}

#[tokio::test]
async fn work_sections_cut_with_real_read_calls_and_respect_utf8_byte_budgets() {
    let (sense, items) = filled().await;
    for i in 0..4 {
        promise(&sense, &format!("Commitment {i} {}", "日本語🦀 ".repeat(80)), "2025-01-02T12:00:00Z").await;
        let id = reminder(
            &sense,
            &format!("Fired reminder {i} {}", "日本語🦀 ".repeat(80)),
            json!({"kind":"at","at":"2025-01-01T11:00:00Z"}),
        )
        .await;
        remind::prepare_firing(&*items, &id, context(THREAD).at, "time due").await.unwrap().unwrap();
        let id = task(&sense, &format!("Claimed {i} {}", "日本語🦀".repeat(20)), 1).await;
        call(&sense, task::task, THREAD, json!({"action":"claim","id":id})).await;
        task(&sense, &format!("Ready {i} {}", "日本語🦀".repeat(20)), 2).await;
    }
    let sections = sections(&*items, &context(THREAD), None).await.unwrap();
    for (heading, hint, handler, arguments, key) in [
        (
            "## Open commitments",
            "sense_promise action=list",
            promise::promise as Handler,
            json!({"action":"list"}),
            "promises",
        ),
        (
            "## Fired reminders",
            "sense_remind action=list",
            remind::remind as Handler,
            json!({"action":"list"}),
            "reminders",
        ),
        (
            "## Upcoming reminders",
            "sense_remind action=list",
            remind::remind as Handler,
            json!({"action":"list"}),
            "reminders",
        ),
        (
            "## Claimed tasks (this thread)",
            "sense_task action=list limit=100",
            task::task as Handler,
            json!({"action":"list","limit":100}),
            "tasks",
        ),
        (
            "## Ready tasks",
            "sense_task action=ready limit=100",
            task::task as Handler,
            json!({"action":"ready","limit":100}),
            "tasks",
        ),
    ] {
        let section = sections.iter().find(|section| section.heading == heading).unwrap();
        let full = section.render();
        assert!(full.len() > 700, "{full}");
        let brief = fit(vec![section.clone()], 700);
        assert!(brief.len() <= 700 && brief.trim_end().ends_with(&format!("{MORE}{hint}")), "{brief}");
        let read = call(&sense, handler, THREAD, arguments).await;
        assert!(read.json[key].as_array().unwrap().len() >= 4, "{read:?}");
        let last = full.lines().last().unwrap().trim_start_matches("- ");
        assert!(!brief.contains(last), "the omitted work is available through the read call: {brief}");
        let omitted = last.split_whitespace().next().unwrap();
        assert!(read.json[key].as_array().unwrap().iter().any(|item| item["id"] == omitted), "{read:?}");
    }
    for budget in (0..5_000).step_by(97).chain([8_000]) {
        let brief = fit(sections.clone(), budget);
        assert!(brief.len() <= budget, "{} > {budget}", brief.len());
        if budget >= 3_000 {
            position(&brief, "## Core loop");
            assert_eq!(brief.matches(RECALLED_BEGIN).count(), brief.matches(RECALLED_END).count(), "{brief}");
            for section in sections.iter().filter(|section| section.more.is_some()) {
                position(&brief, &section.heading);
                if !brief.contains(&section.render()) {
                    position(&brief, &format!("{MORE}{}", section.more.as_ref().unwrap()));
                }
            }
        }
    }
}
