//! Tool requests outside an ant's allowlist: the daemon's [`ApprovalRequest`] on `ants.approvals.<ant>`, which the
//! `ANTS` stream keeps for `ants tools list`, and the [`ApprovalDecision`] answering it, a human's on
//! `ants.approvals.<ant>.<request id>` or that of an ant of the requester's chain in an [`crate::Envelope`].

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use crate::questions::breaks_line;
#[cfg(doc)]
use crate::{Envelope, Escalation};
use crate::{Error, ThreadId, UserId, subject};

/// Most characters the label of a verdict has ([`ApprovalRequest::labels`]).
pub const MAX_LABEL: usize = 200;

/// What the human may answer to an approval request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Let the tool run this once.
    AllowOnce,
    /// Let the tool run, and let the harness remember that for the rest of its session.
    AllowAlways,
    /// Refuse the tool. Also the outcome when no decision arrives before the request expires.
    Reject,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AllowOnce => "allow_once",
            Self::AllowAlways => "allow_always",
            Self::Reject => "reject",
        }
    }
}

/// The tool call a harness asks permission for, as the human gets to see it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ToolCall {
    /// ACP tool kind: read, edit, delete, move, search, execute, think, fetch, switch_mode or other.
    pub kind: String,
    pub title: String,
    /// Files the call touches.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<String>,
    /// The harness's raw input for the tool (JSON), shortened if long.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<String>,
}

/// A tool request outside an ant's allowlist, published to `ants.approvals.<ant>`. It is pending until a
/// [`ApprovalDecision`] arrives on [`ApprovalRequest::decision_subject`] or `expires_at` passes; then the ant
/// rejects it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub id: Uuid,
    pub ant: UserId,
    pub thread_id: ThreadId,
    pub tool: ToolCall,
    /// The verdicts the harness offers. [`Verdict::Reject`] is always among them.
    pub options: Vec<Verdict>,
    /// What the harness names the verdicts it offers, for showing only: a decision gives a verdict, never a label.
    /// Each is one line of 1 to [`MAX_LABEL`] characters; a reader drops any other.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty", deserialize_with = "valid_labels")]
    pub labels: BTreeMap<Verdict, String>,
    pub requested_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    /// The human the request waits for: the owner of the thread the ant answers on, else the ant's default human.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub human: Option<UserId>,
    /// The ant's stand-in, who also gets the request while that human is away, in place of the human's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stand_in: Option<UserId>,
}

impl ApprovalRequest {
    pub fn new(
        ant: UserId,
        thread_id: ThreadId,
        tool: ToolCall,
        mut options: Vec<Verdict>,
        timeout: std::time::Duration,
    ) -> Self {
        if !options.contains(&Verdict::Reject) {
            options.push(Verdict::Reject);
        }
        let requested_at = Utc::now();
        let expires_at = chrono::TimeDelta::from_std(timeout)
            .ok()
            .and_then(|t| requested_at.checked_add_signed(t))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        Self {
            id: Uuid::now_v7(),
            ant,
            thread_id,
            tool,
            options,
            labels: BTreeMap::new(),
            requested_at,
            expires_at,
            human: None,
            stand_in: None,
        }
    }

    /// The same, with `labels`, the harness's names of the verdicts it offers, each made one line of at most
    /// [`MAX_LABEL`] characters: runs of blanks and control characters become a space, and an ellipsis ends one cut.
    /// Names of verdicts not offered, and names left empty, are dropped.
    #[must_use]
    pub fn labelled(self, labels: BTreeMap<Verdict, String>) -> Self {
        let labels = labels
            .into_iter()
            .filter(|(verdict, _)| self.options.contains(verdict))
            .filter_map(|(verdict, name)| Some((verdict, label(&name)?)))
            .collect();
        Self { labels, ..self }
    }

    /// The label of `verdict`, if the request offers it and the harness named it.
    pub fn label(&self, verdict: Verdict) -> Option<&str> {
        self.labels.get(&verdict).filter(|_| self.options.contains(&verdict)).map(String::as_str)
    }

    /// The same, waiting for `human`, with `stand_in` as the ant's stand-in.
    #[must_use]
    pub fn for_human(self, human: Option<UserId>, stand_in: Option<UserId>) -> Self {
        Self { human, stand_in, ..self }
    }

    pub fn subject(&self) -> String {
        subject::approvals(&self.ant)
    }

    pub fn decision_subject(&self) -> String {
        subject::approval_decision(&self.ant, &self.id)
    }

    /// Check that the request arrived on its own [`ApprovalRequest::subject`]. Credentials pin each ant to its own
    /// subject, while the body is whatever the publisher wrote, so a request naming another ant is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "approval request", user: self.ant.clone(), at: subject.to_owned() })
    }

    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        now >= self.expires_at
    }

    /// The human's answer. A verdict the harness did not offer counts as [`Verdict::Reject`].
    pub fn decide(&self, verdict: Verdict) -> ApprovalDecision {
        ApprovalDecision {
            request_id: self.id,
            ant: self.ant.clone(),
            verdict: if self.options.contains(&verdict) { verdict } else { Verdict::Reject },
            decided_at: Utc::now(),
            by: None,
            on_behalf_of: None,
        }
    }
}

/// `name` as the label of a verdict ([`ApprovalRequest::labelled`]); None when nothing is left.
fn label(name: &str) -> Option<String> {
    let words: Vec<&str> =
        name.split(|c: char| c.is_whitespace() || breaks_line(c)).filter(|w| !w.is_empty()).collect();
    let line = words.join(" ");
    if line.is_empty() {
        return None;
    }
    if line.chars().count() <= MAX_LABEL {
        return Some(line);
    }
    let mut cut: String = line.chars().take(MAX_LABEL - 1).collect();
    cut.truncate(cut.trim_end().len());
    cut.push('…');
    Some(cut)
}

fn is_label(text: &str) -> bool {
    let length = text.chars().count();
    (1..=MAX_LABEL).contains(&length) && text.trim() == text && !text.chars().any(breaks_line)
}

fn valid_labels<'de, D: Deserializer<'de>>(deserializer: D) -> Result<BTreeMap<Verdict, String>, D::Error> {
    let mut labels = BTreeMap::<Verdict, String>::deserialize(deserializer)?;
    labels.retain(|_, label| is_label(label));
    Ok(labels)
}

/// The answer to an [`ApprovalRequest`]: a human's, published to `ants.approvals.<ant>.<request id>`, or an ant's
/// of the ant's chain, on [`Envelope::approval`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalDecision {
    pub request_id: Uuid,
    pub ant: UserId,
    pub verdict: Verdict,
    pub decided_at: DateTime<Utc>,
    /// The human who decided, as the decider's client tells: the decision subject does not pin it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<UserId>,
    /// The human whose assistant, the sender, made the decision for it ([`crate::Acting`]). A requester never takes such
    /// a decision as the human's, nor applies it before the human confirms it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
}

impl ApprovalDecision {
    /// A decision on the request `request_id` of `ant`, made now, as an ant of its chain makes one from the
    /// [`Escalation`] alone. The requester maps a verdict it did not offer to [`Verdict::Reject`].
    pub fn new(request_id: Uuid, ant: UserId, verdict: Verdict) -> Self {
        Self { request_id, ant, verdict, decided_at: Utc::now(), by: None, on_behalf_of: None }
    }

    /// The same, made for `human` by its assistant ([`ApprovalDecision::on_behalf_of`]).
    #[must_use]
    pub fn on_behalf(self, human: Option<UserId>) -> Self {
        Self { on_behalf_of: human, ..self }
    }

    /// The same, decided by the human `by`.
    #[must_use]
    pub fn by(self, by: Option<UserId>) -> Self {
        Self { by, ..self }
    }

    pub fn subject(&self) -> String {
        subject::approval_decision(&self.ant, &self.request_id)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{Envelope, Escalation, EscalationReason, p};

    #[test]
    fn only_offered_verdicts_are_decided() {
        let tool = ToolCall { kind: "execute".into(), title: "rm -rf build".into(), ..Default::default() };
        let ask = ApprovalRequest::new(
            p("dev-a"),
            "t1".parse().unwrap(),
            tool,
            vec![Verdict::AllowOnce],
            Duration::from_secs(60),
        );
        assert_eq!(ask.options, [Verdict::AllowOnce, Verdict::Reject]);
        assert!(!ask.is_expired(ask.requested_at) && ask.is_expired(ask.expires_at));
        assert_eq!(ask.decide(Verdict::AllowOnce).verdict, Verdict::AllowOnce);
        let always = ask.decide(Verdict::AllowAlways);
        assert_eq!(always.verdict, Verdict::Reject, "allow_always was not offered");
        assert_eq!(always.subject(), ask.decision_subject());
        let json = serde_json::to_string(&ask).unwrap();
        assert!(json.contains(r#""options":["allow_once","reject"]"#), "{json}");
        assert_eq!(serde_json::from_str::<ApprovalRequest>(&json).unwrap(), ask);
        assert_eq!(ask.check_subject("ants.approvals.dev-a"), Ok(()));
        let forged = ask.check_subject("ants.approvals.dev-b").unwrap_err();
        assert_eq!(forged.to_string(), "approval request of dev-a found at ants.approvals.dev-b");
    }

    #[test]
    fn labels_name_offered_verdicts_on_one_bounded_line_and_readers_drop_others() {
        let tool = ToolCall { kind: "execute".into(), title: "git push".into(), ..Default::default() };
        let ask = ApprovalRequest::new(
            p("dev-a"),
            "t1".parse().unwrap(),
            tool,
            vec![Verdict::AllowOnce, Verdict::AllowAlways],
            Duration::from_secs(60),
        );
        let plain = serde_json::to_string(&ask).unwrap();
        assert!(!plain.contains("labels"), "{plain}");
        let always = "Yes, and don't ask again for git push commands in /work";
        let names = [
            (Verdict::AllowOnce, "  Yes\n\tonce \u{2028} "),
            (Verdict::AllowAlways, always),
            (Verdict::Reject, " \n "),
        ];
        let ask = ask.labelled(names.into_iter().map(|(v, n)| (v, n.to_owned())).collect());
        assert_eq!((ask.label(Verdict::AllowOnce), ask.label(Verdict::AllowAlways)), (Some("Yes once"), Some(always)));
        assert_eq!(ask.label(Verdict::Reject), None, "nothing left of it");
        let long = ask.clone().labelled([(Verdict::AllowAlways, "x ".repeat(MAX_LABEL))].into());
        let cut = long.label(Verdict::AllowAlways).unwrap();
        assert!(cut.chars().count() <= MAX_LABEL && cut.ends_with("x…"), "{cut}");
        let unoffered = ApprovalRequest { options: vec![Verdict::Reject], ..ask.clone() };
        assert_eq!(unoffered.label(Verdict::AllowAlways), None);
        assert_eq!(unoffered.labelled([(Verdict::AllowAlways, always.to_owned())].into()).labels, BTreeMap::new());

        let json = serde_json::to_string(&ask).unwrap();
        assert!(json.contains(&format!(r#""labels":{{"allow_once":"Yes once","allow_always":"{always}"}}"#)), "{json}");
        assert_eq!(serde_json::from_str::<ApprovalRequest>(&json).unwrap(), ask);
        let mut forged: serde_json::Value = serde_json::from_str(&json).unwrap();
        forged["labels"]["allow_once"] = "two\nlines".into();
        forged["labels"]["reject"] = "x".repeat(MAX_LABEL + 1).into();
        let read: ApprovalRequest = serde_json::from_value(forged).unwrap();
        assert_eq!(read.labels, [(Verdict::AllowAlways, always.to_owned())].into(), "invalid labels are dropped");
    }

    #[test]
    fn a_requester_takes_approval_decisions_only_from_its_chain() {
        let request = ApprovalRequest::new(
            p("dev-a"),
            "t1".parse().unwrap(),
            ToolCall::default(),
            vec![Verdict::AllowOnce],
            Duration::from_secs(60),
        );
        let escalation = Escalation::of_request(&request);
        assert_eq!((escalation.reason, escalation.request_id), (EscalationReason::Approval, Some(request.id)));
        let chain = [p("lead"), p("dev-b")];

        let verdict = ApprovalDecision::new(request.id, request.ant.clone(), Verdict::AllowOnce);
        let msg = Envelope::decision(request.thread_id.clone(), p("dev-b"), verdict);
        assert_eq!(msg.subject(), "ants.inbox.dev-a.dev-b", "the subject pins the decider");
        assert_eq!(msg.thread_id, request.thread_id);
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), msg);
        assert!(msg.is_from_chain(&chain));
        assert_eq!(msg.chain_decision(&request, &chain).map(|d| d.verdict), Some(Verdict::AllowOnce));

        assert_eq!(msg.chain_decision(&request, &[p("lead")]), None, "not in the chain");
        let other = ApprovalRequest { id: Uuid::now_v7(), ..request.clone() };
        assert_eq!(msg.chain_decision(&other, &chain), None, "another request");
        let foreign = ApprovalDecision::new(request.id, p("dev-c"), Verdict::AllowOnce);
        let foreign = Envelope { to: p("dev-a"), ..Envelope::decision(request.thread_id.clone(), p("lead"), foreign) };
        assert_eq!(foreign.chain_decision(&request, &chain), None, "a decision on another ant's request");
        assert_eq!(Envelope::new(p("lead"), p("dev-a"), "hi").chain_decision(&request, &chain), None);
        let held = Envelope { on_behalf_of: Some(p("alice")), ..msg };
        assert_eq!(held.chain_decision(&request, &chain), None, "a decision for a human waits for the human");

        let always = ApprovalDecision::new(request.id, request.ant.clone(), Verdict::AllowAlways);
        let always = Envelope::decision(request.thread_id.clone(), p("lead"), always);
        let decided = always.chain_decision(&request, &chain).unwrap();
        assert_eq!(decided.verdict, Verdict::Reject, "a verdict not offered rejects");
    }
}
