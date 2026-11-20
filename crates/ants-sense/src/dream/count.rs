//! The writes a dream makes on its thread, at most [`MAX_DREAM_WRITES`]: the memories and records written through
//! sense's tools ([`counted`]) and the trails saved ([`Sense::ground_trail`]), each counted on the dream. The ant's
//! writes on a dream thread take one turn from the check of the cap until counted, so concurrent writes cannot pass it
//! together.

use std::fmt::Write as _;
use std::sync::Arc;

use ants_proto::provider::{CallContext, ToolCall};
use ants_proto::{TrailName, UserId};
use serde_json::json;

use super::{Dream, MAX_DREAM_WRITES, Making, Refused, SavedTrail, ground, last, rewrite, update};
use crate::items::ItemId;
use crate::store::{Items, StoreError, read_modify_write};
use crate::tools::{Answer, ToolSpec};
use crate::{Ant, Sense};

/// Answer `call` of `spec`, a tool writing memories or records, counting it toward [`MAX_DREAM_WRITES`] when made on
/// the last dream's thread, refused past them. A memory or record written there while a finding resting on untrusted
/// text is open is stored as untrusted, as quoting outside content, when the tool takes quoting: the thread cannot
/// tell which finding a write answers.
pub(crate) async fn counted(sense: &Sense, spec: &ToolSpec, call: &ToolCall) -> Answer {
    let handler = spec.handler;
    let Some(counts) = spec.dream.filter(|counts| (counts.writes)(call)) else { return handler(sense, call).await };
    let thread = &call.context.thread;
    let Ok(items) = sense.items(&call.context.ant).await else { return handler(sense, call).await };
    let on_thread = |dream: &Dream| dream.thread.as_ref() == Some(thread);
    if !matches!(last(&*items).await, Ok(Some((dream, _))) if on_thread(&dream)) {
        return handler(sense, call).await;
    }
    let ant = sense.ant(&call.context.ant);
    let _turn = ant.dream_writes.lock().await;
    let dream = match last(&*items).await {
        Ok(Some((dream, _))) if on_thread(&dream) => dream,
        _ => return handler(sense, call).await,
    };
    if dream.writes_made() >= MAX_DREAM_WRITES {
        return Answer::error("dream_cap", capped(dream.number));
    }
    let untrusted: Vec<_> = dream.untrusted_open().map(|finding| finding.id.as_str()).collect();
    let mut answer = if !untrusted.is_empty() && counts.quotes && !call.arguments.contains_key("quotes") {
        let mut quoting = call.clone();
        quoting.arguments.insert("quotes".into(), json!("external"));
        let mut answer = handler(sense, &quoting).await;
        if !answer.is_error {
            let _ = write!(
                answer.text,
                "\nStored as untrusted: dream {} holds untrusted findings still open ({}).",
                dream.number,
                untrusted.join(", ")
            );
        }
        answer
    } else {
        handler(sense, call).await
    };
    if answer.is_error {
        return answer;
    }
    let Some(written) = answer.json["id"].as_str().and_then(|id| id.parse::<ItemId>().ok()) else { return answer };
    match count_write(&*items, dream.number, written).await {
        Ok(writes) => {
            let _ = write!(answer.text, "\n{}", made(dream.number, writes));
            answer.json["dream_writes"] = json!(writes);
        }
        Err(e) => {
            let _ = write!(answer.text, "\nWarning: this write did not count toward dream {}: {e}.", dream.number);
        }
    }
    answer
}

/// Count a write of `id` on the thread of dream `number`; returns the writes made ([`Dream::writes_made`]).
async fn count_write(items: &dyn Items, number: u64, id: ItemId) -> Result<usize, StoreError> {
    let (dream, ()) = update(items, number, |dream| dream.writes.push(id.clone())).await?;
    Ok(dream.writes_made())
}

/// Record trail `name` saved in `context`, in answer to `finding` if it names one, when on the thread of the last
/// dream: counted toward its [`MAX_DREAM_WRITES`]. Returns the dream's number and the writes it made; none on another
/// thread, recording nothing.
pub async fn trail_saved(
    items: &dyn Items,
    context: &CallContext,
    name: &TrailName,
    finding: Option<&str>,
) -> Result<Option<(u64, usize)>, StoreError> {
    read_modify_write(|| async move {
        let Some((before, revision)) = last(items).await? else { return Ok(None) };
        if before.thread.as_ref() != Some(&context.thread) {
            return Ok(None);
        }
        let mut dream = before.clone();
        let finding = finding.map(str::to_owned);
        dream.trails.push(SavedTrail { name: name.clone(), finding, at: context.at });
        rewrite(items, &before, &dream, revision).await?;
        Ok(Some((dream.number, dream.writes_made())))
    })
    .await
}

/// The refusal of a write on the thread of dream `number`, which made its [`MAX_DREAM_WRITES`].
pub(super) fn capped(number: u64) -> String {
    format!(
        "Dream {number} made its {MAX_DREAM_WRITES} writes, the most a dream makes: answer the findings left with \
         sense_dream as deferred, so the next dream offers them again."
    )
}

/// What a write counted on dream `number` tells of its writes, `writes` made.
fn made(number: u64, writes: usize) -> String {
    format!("Dream {number}: {writes} of {MAX_DREAM_WRITES} writes made.")
}

/// Why a trail is not saved ([`Sense::ground_trail`]).
#[derive(Debug)]
pub enum TrailRefused {
    /// [`ground`] refused it.
    Refused(Refused),
    /// The call's context cannot be known, for the reason given.
    NoContext(String),
    /// The ant's dreams cannot be read.
    Unreadable(StoreError),
}

impl TrailRefused {
    /// Its error code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Refused(refused) => refused.code(),
            Self::NoContext(_) | Self::Unreadable(_) => "unavailable",
        }
    }

    /// What it tells.
    pub fn text(&self) -> String {
        match self {
            Self::Refused(refused) => refused.text("trails are saved"),
            Self::NoContext(why) => format!(
                "the call's context cannot be known ({why}); a trail is saved once it is, checked against the ant's \
                 dreams"
            ),
            Self::Unreadable(e) => {
                format!("the ant's dreams cannot be read ({e}); a trail is saved once they can, checked against them")
            }
        }
    }
}

/// A trail save its ant's dreams ground ([`Sense::ground_trail`]): it holds the ant's turn of the writes on a dream
/// thread until recorded ([`TrailGround::saved`]) or dropped, so concurrent writes cannot pass a dream's cap together.
pub struct TrailGround {
    ant: Arc<Ant>,
    name: UserId,
    /// The ant's items and the call's context; none when sense keeps none of its data.
    kept: Option<(Arc<dyn Items>, CallContext)>,
    finding: Option<String>,
    /// The description of the trail the finding named suggests, when a sage's trail finding.
    pub suggested: Option<String>,
    _turn: tokio::sync::OwnedMutexGuard<()>,
}

impl Sense {
    /// Whether `ant` may save a trail in `context`, the call's context or why it is not known, in answer to `finding`
    /// if it names one ([`ground`]): refused when the finding is no dream's, rests on untrusted text, or the save is on
    /// the thread of a dream that made its writes. Sense keeping none of the ant's data, a save naming no finding
    /// passes; one naming a finding is refused as none is found.
    pub async fn ground_trail(
        &self,
        ant: &UserId,
        context: Result<CallContext, String>,
        finding: Option<&str>,
    ) -> Result<TrailGround, TrailRefused> {
        let kept = self.ant(ant);
        let turn = kept.dream_writes.clone().lock_owned().await;
        let items = match self.synced(ant).await {
            Ok(items) => Some(items),
            Err(StoreError::NotKept(_)) => None,
            Err(e) => return Err(TrailRefused::Unreadable(e)),
        };
        let held = match (items, finding) {
            (None, Some(id)) => return Err(TrailRefused::Refused(Refused::NotFound(id.to_owned()))),
            (None, None) => None,
            (Some(items), _) => {
                let context = context.map_err(TrailRefused::NoContext)?;
                let refused =
                    ground(&*items, &context, finding, Making::Trail).await.map_err(TrailRefused::Unreadable)?;
                if let Some(refused) = refused {
                    return Err(TrailRefused::Refused(refused));
                }
                Some((items, context))
            }
        };
        let suggested = match (&held, finding) {
            (Some((items, _)), Some(id)) => {
                let found = super::finding(&**items, id).await.map_err(TrailRefused::Unreadable)?;
                found.and_then(|finding| finding.advice?.trail).map(|outline| outline.description)
            }
            _ => None,
        };
        Ok(TrailGround {
            ant: kept,
            name: ant.clone(),
            kept: held,
            finding: finding.map(str::to_owned),
            suggested,
            _turn: turn,
        })
    }
}

impl TrailGround {
    /// Record trail `name`, saved, on the dream whose thread the save was on, counted as one of its writes: what the
    /// save's answer tells of it, with the verdict to name it in, none on another thread. A failure is logged and the
    /// save stands.
    pub async fn saved(self, name: &TrailName) -> Option<String> {
        let (items, context) = self.kept.as_ref()?;
        match trail_saved(&**items, context, name, self.finding.as_deref()).await {
            Ok(Some((number, writes))) => {
                self.ant.changed.send_replace(());
                let mut text = made(number, writes);
                if let Some(finding) = &self.finding {
                    let _ = write!(text, " Name {name} in the trails of finding {finding}'s verdict.");
                }
                Some(text)
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(ant = %self.name, trail = %name, error = %e, "recording a trail saved on its dream");
                Some(format!("Warning: this save was not recorded on the dream: {e}."))
            }
        }
    }
}
