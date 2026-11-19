//! An ant's dreams as `save_trail` reaches them: whether a trail saved, in answer to a dream finding or on a dream's
//! thread, stands on trusted ground within its dream's writes ([`Dreams::ground`]), and the trail saved recorded on the
//! dream, counted as one of its writes ([`Grounded::saved`]).

use std::fmt::Write as _;
use std::sync::Arc;

use ants_proto::provider::CallContext;
use ants_proto::{TrailName, UserId};
use ants_sense::dream::{self, MAX_DREAM_WRITES};
use ants_sense::store::{Items, StoreError};

/// The dreams of the ants a process's sense serves ([`super::Sense::dreams`]).
#[derive(Clone)]
pub struct Dreams {
    provider: Arc<ants_sense::Sense>,
}

/// Why a trail is not saved, by an error code and its text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub code: &'static str,
    pub text: String,
}

/// A trail save its ant's dreams ground ([`Dreams::ground`]): it holds the ant's turn of the writes on a dream thread
/// until recorded ([`Grounded::saved`]) or dropped, so concurrent writes cannot pass a dream's cap together.
pub struct Grounded {
    provider: Arc<ants_sense::Sense>,
    ant: UserId,
    /// The ant's items and the call's context; none when sense keeps none of its data.
    kept: Option<(Arc<dyn Items>, CallContext)>,
    finding: Option<String>,
    /// The description of the trail the finding named suggests, when a sage's trail finding.
    pub suggested: Option<String>,
    _turn: ants_sense::DreamTurn,
}

impl Dreams {
    pub(super) fn new(provider: Arc<ants_sense::Sense>) -> Self {
        Self { provider }
    }

    /// Whether `ant` may save a trail in `context`, the call's context or why it is not known, in answer to `finding`
    /// if it names one ([`dream::ground`]): refused when the finding is no dream's, rests on untrusted text, or the
    /// save is on the thread of a dream that made its writes. Sense keeping none of the ant's data, a save naming no
    /// finding passes; one naming a finding is refused as none is found.
    pub async fn ground(
        &self,
        ant: &UserId,
        context: Result<CallContext, String>,
        finding: Option<&str>,
    ) -> Result<Grounded, Refusal> {
        let turn = self.provider.dream_turn(ant).await;
        let items = match self.provider.synced(ant).await {
            Ok(items) => Some(items),
            Err(StoreError::NotKept(_)) => None,
            Err(e) => return Err(unreadable(&e)),
        };
        let kept = match (items, finding) {
            (None, Some(id)) => return Err(refusal(&dream::Refused::NotFound(id.to_owned()))),
            (None, None) => None,
            (Some(items), _) => {
                let context = context.map_err(|why| Refusal {
                    code: "unavailable",
                    text: format!(
                        "the call's context cannot be known ({why}); a trail is saved once it is, checked against \
                         the ant's dreams"
                    ),
                })?;
                if let Some(refused) =
                    dream::ground(&*items, &context, finding, true).await.map_err(|e| unreadable(&e))?
                {
                    return Err(refusal(&refused));
                }
                Some((items, context))
            }
        };
        let suggested = match (&kept, finding) {
            (Some((items, _)), Some(id)) => {
                let found = dream::finding(&**items, id).await.map_err(|e| unreadable(&e))?;
                found.and_then(|finding| finding.advice?.trail).map(|outline| outline.description)
            }
            _ => None,
        };
        Ok(Grounded {
            provider: self.provider.clone(),
            ant: ant.clone(),
            kept,
            finding: finding.map(str::to_owned),
            suggested,
            _turn: turn,
        })
    }
}

impl Grounded {
    /// Record trail `name`, saved, on the dream whose thread the save was on, counted as one of its writes: what the
    /// save's answer tells of it, with the verdict to name it in, none on another thread. A failure is logged and the
    /// save stands.
    pub async fn saved(self, name: &TrailName) -> Option<String> {
        let (items, context) = self.kept.as_ref()?;
        match dream::trail_saved(&**items, context, name, self.finding.as_deref()).await {
            Ok(Some((number, writes))) => {
                self.provider.changed(&self.ant);
                let mut text = format!("Dream {number}: {writes} of {MAX_DREAM_WRITES} writes made.");
                if let Some(finding) = &self.finding {
                    let _ = write!(text, " Name {name} in the trails of finding {finding}'s verdict.");
                }
                Some(text)
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(ant = %self.ant, trail = %name, error = %e, "recording a trail saved on its dream");
                Some(format!("Warning: this save was not recorded on the dream: {e}."))
            }
        }
    }
}

fn refusal(refused: &dream::Refused) -> Refusal {
    Refusal { code: refused.code(), text: refused.text("trails are saved") }
}

fn unreadable(e: &StoreError) -> Refusal {
    Refusal {
        code: "unavailable",
        text: format!("the ant's dreams cannot be read ({e}); a trail is saved once they can, checked against them"),
    }
}

#[cfg(test)]
impl super::Sense {
    /// Keep the data of `ant`, whose home is `home` and whose identity record's id is `identity`, in the hub `js`
    /// reaches, until the hosting returned drops.
    pub(crate) async fn keeping(
        &self,
        js: &async_nats::jetstream::Context,
        ant: &UserId,
        home: &std::path::Path,
        identity: &str,
    ) -> super::Hosting {
        use ants_proto::provider::{Event, EventKind, Provider as _};
        self.serve(ant, home, None);
        self.identify(ant, None);
        let hosting = self.hosting();
        hosting.up(super::Link::new(js.clone(), None, Vec::new(), ants_proto::Machine::from_hostname("studio")));
        let added = Event {
            id: uuid::Uuid::now_v7(),
            ant: ant.clone(),
            identity: identity.to_owned(),
            thread: None,
            at: chrono::Utc::now(),
            kind: EventKind::AntAdded,
        };
        self.provider.event(added).await.unwrap();
        hosting
    }

    /// Begin dream 1 of `ant`, reviewed by `sage`, its packet covering `threads`, each with whether it is untrusted,
    /// and take `answer` as the sage's: the thread the ant answers the findings on.
    pub(crate) async fn reviewed(
        &self,
        ant: &UserId,
        sage: &UserId,
        threads: &[(ants_proto::ThreadId, bool)],
        answer: &str,
    ) -> ants_proto::ThreadId {
        use ants_proto::provider::{Origin, Starter};
        use ants_sense::dream::{Covered, Recipient, Start};
        let items = self.provider.synced(ant).await.unwrap();
        let writer = CallContext {
            ant: ant.clone(),
            identity: String::new(),
            thread: threads[0].0.clone(),
            origin: Origin { by: Starter::Human { user: UserId::human() }, channel: None },
            harness: "test".to_owned(),
            model: None,
            machine: "m1".to_owned(),
            repo: None,
            at: chrono::Utc::now(),
        };
        let covered = |(thread, untrusted): &(ants_proto::ThreadId, bool)| Covered {
            thread: thread.clone(),
            untrusted: *untrusted,
            review: false,
        };
        let start = Start {
            at: writer.at,
            recipient: Recipient::Sage(sage.clone()),
            threads: threads.iter().map(covered).collect(),
            commits: Vec::new(),
            writer: &writer,
            vitals: Vec::new(),
            trails: &ants_sense::dream::ColonyTrails::read(Vec::new()),
        };
        let begun = dream::begin(&*items, start).await.unwrap().expect("dream 1 begins");
        let review = begun.review.expect("its review is asked").thread;
        dream::review_sent(&*items, begun.number).await.unwrap();
        let reviewed = dream::answered(&*items, sage, &review, uuid::Uuid::now_v7(), Some(answer)).await.unwrap();
        assert!(matches!(reviewed, Some(dream::Reviewed::Prompt(_))), "{reviewed:?}");
        review
    }
}
