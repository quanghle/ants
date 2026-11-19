use ants_proto::TrailName;
use ants_proto::provider::{Origin, Starter};

use super::review::{fenced, sage, sent};
use super::*;

/// The outline of a saved trail drafting release notes, as a sage gives it.
fn outline() -> serde_json::Value {
    json!({
        "name": "Release Notes!",
        "description": "Draft the release notes of a repository from its merged pull requests",
        "args": {"repo": "github.com/o/r", "since": "v1.2.0"},
        "phases": ["List the pull requests merged since the tag", "Draft the notes by area"],
    })
}

/// A sage's trail finding stating `text`, citing thread `thread` and suggesting `outline`.
fn trail(text: &str, thread: u8, outline: serde_json::Value) -> serde_json::Value {
    json!({
        "kind": "trail",
        "finding": text,
        "evidence": {"threads": [on(thread).thread.to_string()]},
        "action": "Save it as a trail.",
        "trail": outline,
    })
}

fn named(name: &str) -> TrailName {
    name.parse().unwrap()
}

#[test]
fn a_trail_finding_suggests_a_saved_trail_its_outline_kept_or_refused_whole() {
    let parsed = parse(&fenced(&[trail("Notes by hand thrice", 1, outline())])).unwrap();
    let (text, advice) = &parsed.findings[0];
    assert_eq!((text.as_str(), advice.kind.as_str()), ("Notes by hand thrice", TRAIL));
    let suggested = advice.trail.as_ref().unwrap();
    assert_eq!((&suggested.name, suggested.args["since"].as_str()), (&named("release-notes"), "v1.2.0"));
    assert_eq!(suggested.phases.len(), 2);

    let with = |key: &str, value: serde_json::Value| {
        let mut outline = outline();
        outline[key] = value;
        trail("Broken", 1, outline)
    };
    let long = parse(&fenced(&[with("name", json!(format!("{} x", "a".repeat(63))))])).unwrap();
    assert_eq!(long.findings[0].1.trail.as_ref().unwrap().name, named(&"a".repeat(63)), "cut to 64, trimmed");

    let mut no_thread = trail("No thread", 1, outline());
    no_thread["evidence"] = json!({"files": ["src/lib.rs:3"]});
    let nine: serde_json::Map<_, _> = (1..=9).map(|n| (format!("arg{n}"), json!("x"))).collect();
    let broken = [
        trail("No outline", 1, json!(null)),
        no_thread,
        with("name", json!("!!!")),
        with("phases", json!(["Only one step"])),
        with("args", json!(nine)),
        with("args", json!({"Repo": "github.com/o/r"})),
        with("description", json!("x".repeat(201))),
    ];
    let parsed = parse(&fenced(&broken)).unwrap();
    assert_eq!((parsed.findings.len(), parsed.invalid), (0, broken.len()), "{parsed:?}");

    let mut lesson = trail("A lesson", 1, outline());
    lesson["kind"] = json!("lesson");
    let given: Vec<_> = (1..=4).map(|n| trail(&format!("Trail {n}"), 1, outline())).chain([lesson]).collect();
    let parsed = parse(&fenced(&given)).unwrap();
    assert_eq!((parsed.findings.len(), parsed.over), (4, 1), "at most {MAX_TRAIL_FINDINGS} trail findings");
    assert!(parsed.findings[3].1.trail.is_none(), "another kind keeps no outline");
}

#[tokio::test]
async fn a_trail_finding_shows_its_outline_and_is_untrusted_citing_an_untrusted_thread() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let thread = sent(&*items).await;
    let given = fenced(&[trail("Notes by hand", 1, outline()), trail("Deploys by hand", 2, outline())]);
    let reviewed = answered(&*items, &sage(), &thread, uuid::Uuid::now_v7(), Some(&given)).await.unwrap();
    let Some(Reviewed::Prompt(prompt)) = reviewed else { panic!("{reviewed:?}") };
    let shown = "\n  Suggested trail: release-notes: Draft the release notes of a repository from its merged pull \
                 requests; args repo (github.com/o/r), since (v1.2.0); phases List the pull requests merged since the \
                 tag; Draft the notes by area\n  Suggested action: Save it as a trail.";
    assert!(prompt.contains(shown), "{prompt}");
    assert!(prompt.contains("A trail finding is applied by writing the script it outlines"), "{prompt}");
    let (dream, _) = get(&*items, 1).await.unwrap().unwrap();
    let untrusted: Vec<_> = dream.findings.iter().take(2).map(|finding| finding.untrusted).collect();
    assert_eq!(untrusted, [false, true], "as any finding citing a webhook's thread");

    let on_dream = CallContext { thread, ..context(human()) };
    let refused = ground(&*items, &on_dream, Some("1.2"), true).await.unwrap().unwrap();
    assert_eq!(
        refused.text("trails are saved"),
        "Refused: finding 1.2 rests on untrusted text; trails are saved only on trusted ground."
    );
    assert_eq!(ground(&*items, &on_dream, Some("1.1"), true).await.unwrap(), None, "a trusted finding");
}

#[tokio::test]
async fn a_trail_is_saved_on_trusted_ground_and_named_in_the_verdict_applying_its_finding() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let thread = sent(&*items).await;
    let mut lesson = trail("Rebase first", 1, json!(null));
    lesson["kind"] = json!("lesson");
    let given = fenced(&[trail("Notes by hand", 1, outline()), trail("Deploys by hand", 1, outline()), lesson]);
    answered(&*items, &sage(), &thread, uuid::Uuid::now_v7(), Some(&given)).await.unwrap();
    let on_dream = CallContext { thread, ..context(human()) };

    assert_eq!(ground(&*items, &on_dream, Some("1.9"), true).await.unwrap(), Some(Refused::NotFound("1.9".into())));
    let refused = ground(&*items, &on_dream, None, true).await.unwrap().unwrap();
    let why = "Refused: the packet of dream 1 holds the history of an untrusted thread and no finding of it is named \
               in `finding`; trails are saved only on trusted ground.";
    assert_eq!((refused.code(), refused.text("trails are saved").as_str()), ("untrusted", why));
    let review = Origin { by: Starter::Review { user: sage() }, channel: None };
    let asked = ground(&*items, &CallContext { origin: review, ..on_dream.clone() }, Some("1.1"), true).await;
    assert_eq!(asked.unwrap().map(|refused| refused.code()), Some("untrusted"), "a thread asking a review");

    let saved = trail_saved(&*items, &on_dream, &named("release-notes"), Some("1.1")).await.unwrap();
    assert_eq!(saved, Some((1, 1)));
    assert_eq!(trail_saved(&*items, &on(5), &named("elsewhere"), None).await.unwrap(), None, "off the dream thread");
    assert_eq!(trail_saved(&*items, &on_dream, &named("deploy-check"), None).await.unwrap(), Some((1, 2)));

    let one = |verdict: serde_json::Value| json!({"verdicts": [verdict]});
    let refusals = [
        (json!({"finding": "1.1", "verdict": "applied", "reason": "Saved."}), "invalid_arguments"),
        (json!({"finding": "1.1", "verdict": "applied", "reason": "Saved.", "trails": ["other"]}), "not_found"),
        (json!({"finding": "1.2", "verdict": "applied", "reason": "Saved.", "trails": ["release-notes"]}), "not_found"),
        (
            json!({"finding": "1.1", "verdict": "rejected", "reason": "No.", "trails": ["release-notes"]}),
            "invalid_arguments",
        ),
        (
            json!({"finding": "1.1", "verdict": "applied", "reason": "Saved.", "trails": ["Release Notes"]}),
            "invalid_arguments",
        ),
    ];
    for (verdict, code) in refusals {
        assert_eq!(error(&tool(&sense, dream, one(verdict.clone()), on_dream.clone()).await), code, "{verdict}");
    }
    let applied = json!({"verdicts": [
        {"finding": "1.1", "verdict": "applied", "reason": "Saved it.", "trails": ["release-notes"]},
        {"finding": "1.3", "verdict": "applied", "reason": "A trail serves it.", "trails": ["deploy-check"]},
    ]});
    let answer = tool(&sense, dream, applied, on_dream.clone()).await;
    assert!(!answer.is_error, "{answer:?}");
    let shown = tool(&sense, dream, json!({}), on_dream.clone()).await;
    assert!(shown.text.contains(&format!("2 of {MAX_DREAM_WRITES} writes made")), "{}", shown.text);
    assert!(shown.text.contains("\n  applied: Saved it. (trails release-notes)\n"), "{}", shown.text);
    assert!(shown.text.contains("\nSaved on this thread: release-notes (for 1.1), deploy-check."), "{}", shown.text);
    assert!(!tool(&sense, dream, json!({"done": true}), on_dream.clone()).await.is_error);

    let (done, _) = get(&*items, 1).await.unwrap().unwrap();
    let diary = items.get(done.diary.as_ref().unwrap()).await.unwrap().unwrap();
    let body = Memory::of(&diary).unwrap().body;
    assert!(body.contains(&format!("2 of {MAX_DREAM_WRITES} writes made")), "{body}");
    assert!(body.contains("\n- 1.1 sage: applied: Saved it. (trails release-notes)\n"), "{body}");
    let next = Dream { number: 2, ..reviewing(0, 0) };
    let read = compose(&next, Earlier::Last(Some(&done)), &[], &ant(), &Identity::default(), &[], PACKET_BUDGET);
    assert!(read.contains("\n- 1.1 (sage): applied: Saved it. (trails release-notes)\n"), "{read}");
    assert!(!read.contains("A trail finding is applied"), "told only with a trail finding: {read}");
}

#[tokio::test]
async fn a_trail_saved_counts_as_a_dream_write_and_none_is_saved_past_the_cap() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    begin(&*items, start(Utc::now(), &on(9))).await.unwrap().unwrap();
    woken(&*items, 1, &on(1).thread).await.unwrap();
    for n in 1..MAX_DREAM_WRITES {
        assert!(!sense.call_tool(remember(1, n)).await.unwrap().is_error);
    }
    assert_eq!(ground(&*items, &on(1), None, true).await.unwrap(), None);
    let saved = trail_saved(&*items, &on(1), &named("release-notes"), None).await.unwrap();
    assert_eq!(saved, Some((1, MAX_DREAM_WRITES)));
    let refused = ground(&*items, &on(1), None, true).await.unwrap().unwrap();
    assert_eq!((refused.code(), refused.text("trails are saved")), ("dream_cap", capped(1)));
    assert_eq!(ground(&*items, &on(1), None, false).await.unwrap(), None, "a proposal is no counted write");
    assert_eq!(ground(&*items, &on(2), None, true).await.unwrap(), None, "other threads save freely");
    assert!(sense.call_tool(remember(1, MAX_DREAM_WRITES)).await.unwrap().is_error, "the trail took the last write");
}
