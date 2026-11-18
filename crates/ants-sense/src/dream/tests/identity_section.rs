use ants_proto::provider::{Origin, Starter};
use uuid::Uuid;

use super::*;
use crate::memory::remember;
use crate::persona::{PERSONA, persona, persona_id};
use crate::skill::{SKILL, skill_id};
use crate::thread::record::record_event;

fn at(hour: u32) -> DateTime<Utc> {
    format!("2025-01-02T{hour:02}:00:00Z").parse().unwrap()
}

fn user(name: &str) -> UserId {
    name.parse().unwrap()
}

/// Note thread `n` started by `by` at `hour` on January 2.
async fn started(items: &dyn Items, n: u8, by: Starter, hour: u32) {
    let kind = EventKind::ThreadStarted { origin: Origin { by, channel: None }, repo: Some("github.com/o/r".into()) };
    let thread = Some(on(n).thread);
    let event = Event { id: Uuid::now_v7(), ant: user("dev-a"), identity: "0f1e".into(), thread, at: at(hour), kind };
    record_event(items, &event).await.unwrap().unwrap();
}

/// The identity section of `packet`, up to its commits.
fn section(packet: &str) -> &str {
    let start = packet.find("\n## Identity\n").expect(packet);
    let end = packet.find("\n## Commits\n").unwrap_or(packet.len());
    &packet[start + 1..end]
}

#[tokio::test]
async fn the_identity_shows_the_persona_and_its_proposal_or_that_there_is_none() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let writer = on(9);
    let dream = begin(&*items, start(at(12), &writer)).await.unwrap().unwrap();
    let none = packet(&*items, &dream, &ant(), &[]).await.unwrap();
    assert_eq!(
        section(&none),
        "## Identity\n\n### Persona\n\nNo persona yet.\n\n### Self-model\n\nNo self-model yet.\n\n### Relationships \
         with the threads' starters\n\nNo human or other ant started the threads covered.\n\n### Skills\n\nNo skills \
         yet.\n\n### Experience since 2025-01-01 12:00 UTC\n\nNo thread active since then.\n"
    );

    let mut values = Values::new();
    values.insert("voice".into(), json!("Plain."));
    values.insert("boundaries".into(), json!("No force pushes."));
    items.write(&[Write::Create { id: persona_id(), kind: PERSONA.into(), values }], "test").await.unwrap();
    let alone = packet(&*items, &dream, &ant(), &[]).await.unwrap();
    assert!(
        section(&alone).contains("### Persona\n\nVoice: Plain.\nBoundaries: No force pushes.\n\n### Self"),
        "{alone}"
    );

    let change = json!({"voice": "Plain and brief.", "reason": "The threads ask for less."});
    let proposed = tool(&sense, persona, change, context(human())).await;
    assert!(!proposed.is_error, "{proposed:?}");
    let sage = Reader::Sage(user("dev-a"));
    let reviewed = packet(&*items, &dream, &sage, &[]).await.unwrap();
    let identity = section(&reviewed);
    assert!(identity.contains("Boundaries: No force pushes.\n\nProposed: Proposal "), "{identity}");
    assert!(identity.contains("(pending, made 2025-01-01T12:00:00Z"), "{identity}");
    assert!(
        identity.contains("The threads ask for less.\nVoice: Plain and brief.\nBoundaries: No force"),
        "{identity}"
    );
}

#[tokio::test]
async fn both_readers_see_the_records_of_the_threads_starters_the_skills_and_the_experience() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let starters = [
        Starter::Human { user: user("alice") },
        Starter::Human { user: user("bob") },
        Starter::Ant { user: user("dev-a") },
        Starter::Ant { user: user("dev-b") },
        Starter::Webhook { user: user("hook") },
        Starter::Human { user: user("alice") },
    ];
    for (n, by) in (1..).zip(starters) {
        started(&*items, n, by, u32::from(n)).await;
    }
    let remembered = [
        json!({"kind": "self_model", "title": "Careful", "body": "Tests first."}),
        json!({"kind": "relationship", "scope": "human:alice", "title": "Alice", "body": "Reviews fast."}),
    ];
    for arguments in remembered {
        let answer = tool(&sense, remember, arguments, context(human())).await;
        assert!(!answer.is_error, "{answer:?}");
    }
    let mut values = Values::new();
    values.insert("name".into(), json!("release"));
    values.insert("description".into(), json!("Cut a release."));
    values.insert("files".into(), json!({"SKILL.md": "---\nname: release\ndescription: Cut a release.\n---\n"}));
    items.write(&[Write::Create { id: skill_id("release"), kind: SKILL.into(), values }], "test").await.unwrap();
    let writer = on(9);
    let threads = (1..=6).map(|n| Covered { thread: on(n).thread, untrusted: n == 5 }).collect();
    let dream = begin(&*items, Start { threads, ..start(at(12), &writer) }).await.unwrap().unwrap();

    for reader in [ant(), Reader::Sage(user("dev-a"))] {
        let packet = packet(&*items, &dream, &reader, &[]).await.unwrap();
        let identity = section(&packet);
        let parts = [
            "### Self-model\n\n- ",
            " (self, revision 1): Careful\n  Tests first.\n\n### Relationships with the threads' starters\n\n- ",
            " (human:alice, revision 1): Alice\n  Reviews fast.\n- human:bob: no relationship record.\n- ant:dev-b: no \
             relationship record.\n\n### Skills\n\n- release: Cut a release.\n\n### Experience since 2025-01-01 12:00 \
             UTC\n\nExperience: 6 threads, 0 done, on 1 active day.\nMost threads with: human:alice (2, last \
             2025-01-02), ",
        ];
        let mut rest = identity;
        for part in parts {
            let found = rest.find(part).unwrap_or_else(|| panic!("{part:?} in {identity}"));
            rest = &rest[found + part.len()..];
        }
        assert!(rest.contains("Most threads in: repo:github.com/o/r (6, last 2025-01-02)."), "{identity}");
    }
}

#[test]
fn a_packet_over_its_budget_leaves_out_threads_before_it_cuts_the_identity_to_titles() {
    let dream = reviewing(3, 5);
    let threads = [thread(1, 3_000, false), thread(2, 3_000, true), thread(3, 3_000, false)];
    let identity = Identity {
        whole: format!("\n### Persona\n\n{}\n", "Voice: plain.\n".repeat(300)),
        titles: "\nCut to titles for room.\n\n### Persona\n\nFields: Voice.\n".into(),
    };
    let within = |budget| compose(&dream, None, &ant(), &identity, &threads, budget);
    let whole = within(PACKET_BUDGET);
    assert!(whole.contains(&identity.whole) && whole.contains("message 3"), "{whole}");
    assert!(whole.find("## Findings").unwrap() < whole.find("## Identity").unwrap(), "{whole}");
    assert!(whole.find("## Identity").unwrap() < whole.find("## Commits").unwrap(), "{whole}");

    let dropped = within(whole.len() - 6_400);
    assert!(dropped.contains("Left out for room") && dropped.contains("message 2"), "{dropped}");
    assert!(dropped.contains(&identity.whole) && !dropped.contains("Cut to titles"), "{dropped}");

    let bare = within(whole.len() - 8_500);
    assert!(!bare.contains("message ") && bare.contains(&identity.whole), "{bare}");
    assert!(bare.contains("Finding number 5"), "{bare}");

    let titles = within(bare.len() - 1);
    assert!(titles.len() < bare.len() && !titles.contains("Voice: plain."), "{titles}");
    assert!(titles.contains(&identity.titles) && titles.contains("Finding number 5"), "{titles}");

    let least = within(0);
    assert!(least.contains(&identity.titles) && least.contains("5 more findings left out"), "{least}");
}
