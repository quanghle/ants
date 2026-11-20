//! The ground a write in answer to a dream's finding stands on: refused when the finding is no dream's, it rests on
//! untrusted text ([`untrusted`]) or, for a trail saved, the dream it counts on made its writes ([`ground`]).

use ants_proto::provider::CallContext;
use serde_json::json;

use super::{Covered, DREAM, Dream, MAX_DREAM_WRITES, capped, finding, last};
use crate::items::ItemValues;
use crate::store::{Items, StoreError};

/// What a write checked against the ant's dreams makes ([`ground`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Making {
    /// A persona or skill proposal.
    Proposal,
    /// A trail saved for the colony: on a dream's thread, one of the dream's [`MAX_DREAM_WRITES`].
    Trail,
}

/// Why `making`, in `context`, in answer to `finding` if it names one, rests on untrusted text; none when it does not
/// (requirements ants-x39hk.6.5, ants-x39hk.12.5 and ants-x39hk.12.54). It does when the thread came from a webhook or
/// asks a review, or when the finding it names is untrusted ([`Finding::untrusted`](super::Finding::untrusted)). On the
/// thread of a dream whose packet holds untrusted text, an untrusted finding or the history of an untrusted thread, it
/// does too unless it names a finding of that dream, for a trail a [`TRAIL`](super::TRAIL) finding (decision
/// ants-1wq1r): one naming none, a finding the dream does not hold or, for a trail, a finding of another kind may
/// answer the untrusted text. The reviews a sage's dream covers hold none: their text is left out
/// ([`Covered::untrusted_text`]).
pub async fn untrusted(
    items: &dyn Items,
    context: &CallContext,
    finding: Option<&str>,
    making: Making,
) -> Result<Option<String>, StoreError> {
    if !context.trusted() {
        return Ok(Some("the thread came from a webhook or asks a review".into()));
    }
    let on = items.list_by(DREAM, "thread", Some(&json!(context.thread)), 1).await?;
    let dream = on.first().map(Dream::of).transpose()?;
    if let Some(id) = finding {
        let held = dream.as_ref().and_then(|dream| dream.findings.iter().find(|finding| finding.id == id));
        let untrusted = match held {
            Some(held) => held.untrusted,
            None => self::finding(items, id).await?.is_some_and(|finding| finding.untrusted),
        };
        if untrusted {
            return Ok(Some(format!("finding {id} rests on untrusted text")));
        }
        let answers = held.is_some_and(|held| making == Making::Proposal || held.suggests_trail());
        if answers || dream.is_none() {
            return Ok(None);
        }
    }
    let Some(dream) = dream else { return Ok(None) };
    let held = |id: &str| dream.findings.iter().any(|finding| finding.id == id);
    let answers = match finding {
        Some(id) if held(id) => format!("finding {id} is no trail finding"),
        Some(id) => format!("finding {id} is not one of its findings"),
        None => "no finding of it is named in `finding`".into(),
    };
    if let Some(finding) = dream.findings.iter().find(|finding| finding.untrusted) {
        return Ok(Some(format!("dream {} offers untrusted finding {} and {answers}", dream.number, finding.id)));
    }
    if dream.threads.iter().any(Covered::untrusted_text) {
        return Ok(Some(format!(
            "the packet of dream {} holds the history of an untrusted thread and {answers}",
            dream.number
        )));
    }
    Ok(None)
}

/// Why a write made in `context` in answer to a finding is refused ([`ground`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// No dream has the finding named.
    NotFound(String),
    /// It rests on untrusted text, as [`untrusted`] tells.
    Untrusted(String),
    /// The dream whose thread it is on, by number, made its [`MAX_DREAM_WRITES`].
    Cap(u64),
}

impl Refused {
    /// Its error code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound(_) => "not_found",
            Self::Untrusted(_) => "untrusted",
            Self::Cap(_) => "dream_cap",
        }
    }

    /// What it tells, `changed` naming what changes only on trusted ground, such as `trails are saved`.
    pub fn text(&self, changed: &str) -> String {
        match self {
            Self::NotFound(finding) => format!("No dream has a finding {finding:?}."),
            Self::Untrusted(why) => format!("Refused: {why}; {changed} only on trusted ground."),
            Self::Cap(number) => capped(*number),
        }
    }
}

/// Why `making`, in `context`, in answer to `finding` if it names one, is refused: the finding is no dream's, it rests
/// on untrusted text ([`untrusted`]) or, for a trail, one of the dream's writes, it is on the thread of the last dream,
/// which made its [`MAX_DREAM_WRITES`]. None when it is not.
pub async fn ground(
    items: &dyn Items,
    context: &CallContext,
    finding: Option<&str>,
    making: Making,
) -> Result<Option<Refused>, StoreError> {
    if let Some(id) = finding
        && self::finding(items, id).await?.is_none()
    {
        return Ok(Some(Refused::NotFound(id.to_owned())));
    }
    if let Some(why) = untrusted(items, context, finding, making).await? {
        return Ok(Some(Refused::Untrusted(why)));
    }
    if making == Making::Trail
        && let Some((dream, _)) = last(items).await?
        && dream.thread.as_ref() == Some(&context.thread)
        && dream.writes_made() >= MAX_DREAM_WRITES
    {
        return Ok(Some(Refused::Cap(dream.number)));
    }
    Ok(None)
}
