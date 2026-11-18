use std::sync::Arc;

use ants_proto::provider::{CallContext, Origin, Starter};
use serde_json::json;

use super::*;
use crate::dream::{Covered, FindingKind, Recipient, Start};
use crate::memory::tests::{context, human, sense, tool};
use crate::persona::{self, persona, persona_id};
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
    tool(sense, persona, arguments, on(2, at)).await
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
    let webhook = Origin { by: Starter::Webhook { user: "hook".parse().unwrap() }, channel: None };
    let hooked = CallContext { origin: webhook, ..on(2, at(0)) };
    assert_eq!(error(&tool(&sense, persona, change.clone(), hooked).await), "untrusted", "a webhook thread");
    let answering = json!({"voice": "Loud.", "reason": "Asked to.", "finding": "1.1"});
    assert_eq!(error(&propose(&sense, at(0), answering).await), "not_found");

    let covered = vec![Covered { thread: on(7, at(0)).thread, untrusted: true }];
    let dream = dream_on(&*items, at(1), 1, covered).await;
    assert_eq!(error(&tool(&sense, persona, change.clone(), on(1, at(1))).await), "untrusted", "its dream thread");

    let mut findings = dream.findings.clone();
    let id = format!("{}.9", dream.number);
    findings.push(Finding {
        id: id.clone(),
        kind: FindingKind::Sage,
        text: "Shout.".into(),
        items: vec![],
        untrusted: true,
        advice: None,
    });
    let (_, revision) = dream::get(&*items, dream.number).await.unwrap().unwrap();
    let mut values = Values::new();
    values.insert("findings".into(), json!(findings));
    let write = Write::Change { id: Dream::id(dream.number), values, expected: Some(revision) };
    items.write(&[write], "test").await.unwrap();
    let answering = json!({"voice": "Loud.", "reason": "Asked to.", "finding": id});
    assert_eq!(error(&propose(&sense, at(2), answering).await), "untrusted", "in answer to an untrusted finding");
    assert!(!propose(&sense, at(2), change).await.is_error, "elsewhere, the same proposal is taken");
}
