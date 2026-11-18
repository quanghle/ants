use std::sync::Arc;

use ants_proto::provider::{CallContext, Format, Origin, Output, Provider as _, Starter, ToolCall};
use serde_json::json;

use super::*;
use crate::dream::{Covered, FindingKind, Recipient, Start};
use crate::identity::identity;
use crate::memory::Trust;
use crate::memory::tests::{context, human, memory, sense, tool};
use crate::persona::{self, persona_id};
use crate::{Answer, Sense};

fn at(hours: i64) -> DateTime<Utc> {
    "2025-01-01T12:00:00Z".parse::<DateTime<Utc>>().unwrap() + Duration::hours(hours)
}

pub(crate) fn on(thread: u8, at: DateTime<Utc>) -> CallContext {
    let thread = format!("019a7d2c-0000-7000-8000-0000000000{thread:02}").parse().unwrap();
    CallContext { thread, at, ..context(human()) }
}

pub(crate) fn error(answer: &Answer) -> &str {
    assert!(answer.is_error, "{answer:?}");
    answer.json["error"].as_str().unwrap()
}

async fn propose(sense: &Sense, at: DateTime<Utc>, arguments: serde_json::Value) -> Answer {
    tool(sense, identity, arguments, on(2, at)).await
}

/// Dream begun at `at`, covering `threads`, woken on thread `thread`.
pub(crate) async fn dream_on(items: &dyn Items, at: DateTime<Utc>, thread: u8, threads: Vec<Covered>) -> Dream {
    let writer = on(9, at);
    let start = Start { at, recipient: Recipient::Ant, threads, commits: Vec::new(), writer: &writer };
    let begun = dream::begin(items, start).await.unwrap().unwrap();
    dream::woken(items, begun.number, &on(thread, at).thread).await.unwrap()
}

pub(crate) fn offered(dream: &Dream) -> Vec<&Finding> {
    dream.findings.iter().filter(|finding| finding.kind == FindingKind::Proposal).collect()
}

/// The answer of `sense_dream` on thread `thread` to `verdict` on `finding`.
pub(crate) async fn judge(sense: &Sense, thread: u8, finding: &str, verdict: &str) -> Answer {
    let verdicts = json!({"verdicts": [{"finding": finding, "verdict": verdict, "reason": "Judged."}]});
    tool(sense, dream::dream, verdicts, on(thread, Utc::now())).await
}

fn verdict(finding: &Finding, verdict: Judgement) -> Verdict {
    Verdict { finding: finding.id.clone(), verdict, reason: "Judged.".into(), items: vec![], at: Utc::now() }
}

pub(crate) async fn status(items: &dyn Items, id: &ItemId) -> Status {
    ProposalItem::of(items.get(id).await.unwrap().unwrap()).unwrap().proposal.status
}

pub(crate) fn proposal_id(answer: &Answer) -> ItemId {
    assert!(!answer.is_error, "{answer:?}");
    serde_json::from_value(answer.json["proposal"]["id"].clone()).unwrap()
}

#[tokio::test]
async fn a_persona_proposal_is_offered_after_twelve_hours_and_takes_effect_once_confirmed() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let made = propose(&sense, at(0), json!({"voice": "Plain and brief.", "reason": "Users skim."})).await;
    let id = proposal_id(&made);
    assert!(made.text.contains("from 2025-01-02T00:00:00Z"), "{}", made.text);
    assert!(persona::current(&*items).await.unwrap().is_none(), "a proposal changes nothing yet");

    let early = dream_on(&*items, at(11), 1, Vec::new()).await;
    assert!(offered(&early).is_empty(), "not offered before 12 hours");
    assert!(dream::proposal_due(&*items, Some(&early), at(12)).await.unwrap());
    assert!(!dream::proposal_due(&*items, Some(&early), at(11)).await.unwrap());

    let dream = dream_on(&*items, at(36), 3, Vec::new()).await;
    let [finding] = offered(&dream)[..] else { panic!("{:?}", dream.findings) };
    assert_eq!(finding.items, std::slice::from_ref(&id));
    assert!(finding.text.contains("Voice: Plain and brief.") && finding.text.contains("Users skim."));
    assert!(!dream::proposal_due(&*items, Some(&dream), at(48)).await.unwrap(), "offered already");
    let refused = decide(&*items, dream.number, finding, &verdict(finding, Judgement::Applied), at(11)).await;
    assert_eq!(error(&refused.unwrap_err()), "too_soon", "confirmed only 12 hours after");

    let confirmed = judge(&sense, 3, &finding.id, "applied").await;
    assert!(!confirmed.is_error, "{confirmed:?}");
    assert!(confirmed.text.contains(&format!("Proposal {id} (confirmed")), "{}", confirmed.text);
    let current = persona::current(&*items).await.unwrap().unwrap();
    assert_eq!(persona::text(&current.values), "Voice: Plain and brief.");
    assert_eq!((current.text("reason"), current.text("proposal")), ("Users skim.", id.to_string().as_str()));
    assert_eq!(status(&*items, &id).await, Status::Confirmed);
    let (dream, _) = dream::get(&*items, dream.number).await.unwrap().unwrap();
    assert_eq!(dream.verdicts[0].items, std::slice::from_ref(&id), "the verdict names the proposal");
    let finding = &offered(&dream)[0].id;
    assert!(!judge(&sense, 3, finding, "applied").await.is_error, "a repeated verdict changes nothing");
    assert_eq!(error(&judge(&sense, 3, finding, "rejected").await), "decided");
}

#[tokio::test]
async fn the_persona_changes_at_most_once_a_day_and_keeps_every_version_with_its_reason() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    propose(&sense, at(0), json!({"values": "Care.", "voice": "Warm.", "reason": "First."})).await;
    let dream = dream_on(&*items, at(12), 1, Vec::new()).await;
    assert!(!judge(&sense, 1, &offered(&dream)[0].id, "applied").await.is_error);

    let second = proposal_id(&propose(&sense, at(1), json!({"voice": "", "reason": "Second."})).await);
    let dream = dream_on(&*items, Utc::now(), 2, Vec::new()).await;
    let finding = offered(&dream)[0].clone();
    assert_eq!(error(&judge(&sense, 2, &finding.id, "applied").await), "too_soon", "a second change the same day");
    assert_eq!(status(&*items, &second).await, Status::Pending);
    let tomorrow = Utc::now() + Duration::hours(25);
    decide(&*items, dream.number, &finding, &verdict(&finding, Judgement::Applied), tomorrow).await.unwrap();
    let current = persona::current(&*items).await.unwrap().unwrap();
    assert_eq!(persona::text(&current.values), "Values: Care.", "an empty field is cleared");

    let history = propose(&sense, at(2), json!({"history": true})).await;
    let versions = history.json["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 2, "{history:?}");
    assert_eq!((versions[0]["reason"].as_str(), versions[0]["voice"].as_str()), (Some("Second."), None));
    assert_eq!((versions[1]["reason"].as_str(), versions[1]["voice"].as_str()), (Some("First."), Some("Warm.")));
}

#[tokio::test]
async fn a_confirmed_profile_is_published_until_the_persona_drops_it() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let good_for = "Good for Rust daemons and NATS.";
    propose(&sense, at(0), json!({"voice": "Plain.", "profile": good_for, "reason": "To be found."})).await;
    assert_eq!(persona::profile(&*items).await.unwrap(), None, "a proposal publishes nothing yet");
    let dream = dream_on(&*items, at(12), 1, Vec::new()).await;
    let finding = offered(&dream)[0].clone();
    assert!(finding.text.contains(&format!("Profile: {good_for}")), "{}", finding.text);
    assert!(finding.text.contains("the profile shows in the ant's colony entry"), "{}", finding.text);
    assert!(!judge(&sense, 1, &finding.id, "applied").await.is_error);
    assert_eq!(persona::profile(&*items).await.unwrap().as_deref(), Some(good_for));

    propose(&sense, at(1), json!({"profile": "", "reason": "Found already."})).await;
    let dream = dream_on(&*items, Utc::now(), 2, Vec::new()).await;
    let finding = offered(&dream)[0].clone();
    assert!(finding.text.contains("the profile leaves the ant's colony entry"), "{}", finding.text);
    let tomorrow = Utc::now() + Duration::hours(25);
    decide(&*items, dream.number, &finding, &verdict(&finding, Judgement::Applied), tomorrow).await.unwrap();
    assert_eq!(persona::profile(&*items).await.unwrap(), None);
    assert_eq!(persona::text(&persona::current(&*items).await.unwrap().unwrap().values), "Voice: Plain.");
}

#[tokio::test]
async fn a_rejected_proposal_is_discarded_and_a_newer_one_supersedes_a_pending_one() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let first = proposal_id(&propose(&sense, at(0), json!({"voice": "Terse.", "reason": "One."})).await);
    let second = propose(&sense, at(1), json!({"voice": "Formal.", "reason": "Two."})).await;
    assert_eq!(second.json["superseded"], json!(first));
    let second = proposal_id(&second);
    assert_eq!(status(&*items, &first).await, Status::Superseded);

    let dream = dream_on(&*items, at(13), 1, Vec::new()).await;
    let [finding] = offered(&dream)[..] else { panic!("{:?}", dream.findings) };
    assert_eq!(finding.items, std::slice::from_ref(&second), "only the pending proposal is offered");
    assert!(!judge(&sense, 1, &finding.id, "deferred").await.is_error);
    assert_eq!(status(&*items, &second).await, Status::Pending, "deferred keeps it");
    assert!(!judge(&sense, 1, &finding.id, "rejected").await.is_error);
    assert_eq!(status(&*items, &second).await, Status::Rejected);
    assert!(persona::current(&*items).await.unwrap().is_none());

    let next = dream_on(&*items, Utc::now(), 2, Vec::new()).await;
    assert!(offered(&next).is_empty(), "a rejected proposal is not offered again");
}

#[tokio::test]
async fn a_proposal_whose_target_changed_since_is_stale() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    propose(&sense, at(0), json!({"voice": "Terse.", "reason": "One."})).await;
    let mut values = Values::new();
    values.insert("voice".into(), json!("Imported."));
    let write = Write::Put { id: persona_id(), kind: persona::PERSONA.into(), values, expected: None };
    items.write(&[write], "test").await.unwrap();
    let dream = dream_on(&*items, at(13), 1, Vec::new()).await;
    assert_eq!(error(&judge(&sense, 1, &offered(&dream)[0].id, "applied").await), "stale");
}

#[tokio::test]
async fn proposals_resting_on_untrusted_text_are_refused() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let change = json!({"voice": "Loud.", "reason": "Asked to."});
    let answering = |finding: &str| json!({"voice": "Loud.", "reason": "Asked to.", "finding": finding});
    let webhook = Origin { by: Starter::Webhook { user: "hook".parse().unwrap() }, channel: None };
    let hooked = CallContext { origin: webhook, ..on(2, at(0)) };
    assert_eq!(error(&tool(&sense, identity, change.clone(), hooked.clone()).await), "untrusted", "a webhook thread");
    assert_eq!(error(&propose(&sense, at(0), answering("1.1")).await), "not_found");

    let first = dream_on(&*items, at(1), 1, Vec::new()).await;
    offer(&*items, &first, &[("1.1", false)]).await;
    let rejected = json!({"verdicts": [{"finding": "1.1", "verdict": "rejected", "reason": "Not now."}]});
    assert!(!tool(&sense, dream::dream, rejected, on(1, at(1))).await.is_error);
    let covered = vec![Covered { thread: on(7, at(0)).thread, untrusted: true }];
    let second = dream_on(&*items, at(25), 3, covered).await;
    offer(&*items, &second, &[("2.1", false), ("2.2", true)]).await;
    let dreaming = |arguments| async { tool(&sense, identity, arguments, on(3, at(26))).await };

    assert!(!dreaming(answering("2.1")).await.is_error, "in answer to a trusted finding of the dream");
    assert_eq!(error(&dreaming(answering("2.2")).await), "untrusted", "in answer to an untrusted finding");
    assert_eq!(error(&dreaming(answering("1.1")).await), "untrusted", "a finding the dream does not hold");
    assert_eq!(error(&dreaming(answering("2.7")).await), "not_found");
    assert_eq!(error(&dreaming(change.clone()).await), "untrusted", "naming no finding");
    let hooked = CallContext { thread: on(3, at(26)).thread, ..hooked };
    assert_eq!(error(&tool(&sense, identity, answering("2.1"), hooked).await), "untrusted", "a webhook thread");

    let remember = json!({"kind": "fact", "title": "Loud", "body": "Speak loudly.", "force": true});
    let call = ToolCall {
        name: "remember".into(),
        arguments: remember.as_object().unwrap().clone(),
        context: on(3, at(26)),
        format: Format::Json,
    };
    let written = sense.call_tool(call).await.unwrap();
    let Output::Json(written) = written.output else { panic!("json") };
    let id: ItemId = written["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(memory(&kept, &id).await.trust, Trust::Untrusted, "written while an untrusted finding is open");

    let answered = json!({"verdicts": [
        {"finding": "2.1", "verdict": "rejected", "reason": "Not now."},
        {"finding": "2.2", "verdict": "rejected", "reason": "Not now."},
    ]});
    assert!(!tool(&sense, dream::dream, answered, on(3, at(26))).await.is_error);
    dream_on(&*items, at(49), 4, Vec::new()).await;
    let trusted = |arguments| async { tool(&sense, identity, arguments, on(4, at(50))).await };
    assert_eq!(error(&trusted(answering("2.2")).await), "untrusted", "an untrusted finding of another dream");
    assert!(!trusted(change.clone()).await.is_error, "a dream thread whose packet holds no untrusted text");

    assert_eq!(error(&propose(&sense, at(26), answering("2.2")).await), "untrusted", "an untrusted finding");
    assert!(!propose(&sense, at(26), answering("1.1")).await.is_error, "elsewhere, any trusted finding");
    assert!(!propose(&sense, at(26), change).await.is_error, "elsewhere, the same proposal is taken");
}

/// Add to `dream` the findings of kind sage `offered`, each an id and whether it is untrusted.
async fn offer(items: &dyn Items, dream: &Dream, offered: &[(&str, bool)]) {
    let (current, revision) = dream::get(items, dream.number).await.unwrap().unwrap();
    let mut findings = current.findings;
    findings.extend(offered.iter().map(|&(id, untrusted)| Finding {
        id: id.into(),
        kind: FindingKind::Sage,
        text: "Shout.".into(),
        items: vec![],
        untrusted,
        key: None,
        count: None,
        advice: None,
    }));
    let mut values = Values::new();
    values.insert("findings".into(), json!(findings));
    let write = Write::Change { id: Dream::id(dream.number), values, expected: Some(revision) };
    items.write(&[write], "test").await.unwrap();
}
