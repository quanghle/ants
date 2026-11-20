//! An ant's dreams as `save_trail` reaches them: whether a trail saved, in answer to a dream finding or on a dream's
//! thread, stands on trusted ground within its dream's writes ([`Dreams::ground`]), and the trail saved recorded on the
//! dream, counted as one of its writes ([`Grounded::saved`]); sense decides both ([`ants_sense::Sense::ground_trail`]).

use std::sync::Arc;

use ants_proto::UserId;
use ants_proto::provider::CallContext;

/// A trail save its ant's dreams ground ([`Dreams::ground`]), holding the ant's turn of the writes on a dream thread
/// until recorded ([`Grounded::saved`]) or dropped.
pub type Grounded = ants_sense::dream::TrailGround;

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

impl Dreams {
    pub(super) fn new(provider: Arc<ants_sense::Sense>) -> Self {
        Self { provider }
    }

    /// Whether `ant` may save a trail in `context`, the call's context or why it is not known, in answer to `finding`
    /// if it names one ([`ants_sense::Sense::ground_trail`]).
    pub async fn ground(
        &self,
        ant: &UserId,
        context: Result<CallContext, String>,
        finding: Option<&str>,
    ) -> Result<Grounded, Refusal> {
        let refusal = |refused: ants_sense::dream::TrailRefused| Refusal { code: refused.code(), text: refused.text() };
        self.provider.ground_trail(ant, context, finding).await.map_err(refusal)
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
        use ants_sense::dream::{self, Covered, Recipient, Start};
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
