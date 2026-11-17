//! The ant's brief in the first prompt of a session: asked of its provider when a session of a thread starts, is
//! loaded or is rebuilt ([`BriefReason`]), within [`BRIEF_BUDGET`] bytes, and put after the ant's configured
//! instructions under `[ants brief from <provider>]`. A provider answering with nothing adds nothing; one not
//! answering within [`BRIEF_TIMEOUT`], or failing, leaves an `[ants]` line in its place.

use std::fmt::Write as _;

use ants_proto::ThreadId;
use ants_proto::provider::{BRIEF_BUDGET, BRIEF_TIMEOUT, BriefReason, BriefRequest, Capability, Connected, Origin};

use super::context::Caller;

/// What asks the ant's provider for its brief for a session of a thread.
#[derive(Clone)]
pub struct Briefing {
    provider: Connected,
    /// None when the ant's identity record cannot be read.
    caller: Option<Caller>,
    thread: ThreadId,
    origin: Origin,
    budget: u32,
}

impl Briefing {
    /// The briefing of sessions of `thread`, which `origin` started, by `provider`: none when the provider takes no
    /// brief requests, its context capability not agreed.
    pub fn new(provider: Connected, caller: Option<Caller>, thread: ThreadId, origin: Origin) -> Option<Self> {
        provider.has(Capability::Context).then_some(Self { provider, caller, thread, origin, budget: BRIEF_BUDGET })
    }

    /// The brief's part of the first prompt of a session opened for `reason` in `harness` running `model`: the brief
    /// under its heading, cut to the budget; an `[ants]` line when it is left out; empty for an empty brief.
    pub async fn section(&self, reason: BriefReason, harness: &str, model: Option<&str>) -> String {
        let (name, thread) = (self.provider.name(), &self.thread);
        let Some(caller) = &self.caller else {
            return left_out(name, "the ant's identity record cannot be read");
        };
        let context = caller.context(thread.clone(), self.origin.clone(), harness, model);
        let request = BriefRequest { context, budget: self.budget, reason };
        match tokio::time::timeout(BRIEF_TIMEOUT, self.provider.brief(request)).await {
            Ok(Ok(brief)) => shown(name, &brief.text, self.budget as usize),
            Ok(Err(e)) => {
                tracing::warn!(%thread, provider = name, error = %e, "the provider failed to brief the session");
                left_out(name, &format!("the provider failed (error {})", e.code))
            }
            Err(_) => {
                tracing::warn!(%thread, provider = name, "the provider sent no brief within {BRIEF_TIMEOUT:?}");
                left_out(name, &format!("no answer within {BRIEF_TIMEOUT:?}"))
            }
        }
    }
}

/// `text`, the brief of `provider`, under its heading, cut to `budget` bytes on a character boundary with a line
/// saying so; empty when `text` is blank.
fn shown(provider: &str, text: &str, budget: usize) -> String {
    let text = text.trim();
    if text.is_empty() {
        return String::new();
    }
    let cut = text.floor_char_boundary(budget);
    let mut shown = format!("[ants brief from {provider}]\n{}", &text[..cut]);
    if cut < text.len() {
        let _ = write!(shown, "\n[ants] The brief was cut to its budget of {budget} bytes.");
    }
    shown
}

/// The `[ants]` line telling that the brief of `provider` is left out, and `why`.
fn left_out(provider: &str, why: &str) -> String {
    format!("[ants] The {provider} brief is left out: {why}.")
}

/// A provider in process that takes only brief requests, answering each as told.
#[cfg(test)]
pub(crate) mod fake {
    use std::sync::{Arc, Mutex};

    use ants_proto::provider::{
        BoxFuture, Brief, BriefRequest, Capabilities, Connected, Host, Info, InitializeParams, InitializeResult,
        PROTOCOL_VERSION, Provider, RpcError,
    };

    use crate::sync::MutexExt as _;

    /// How the fake answers brief requests.
    #[derive(Clone)]
    pub(crate) enum Answer {
        Text(String),
        Never,
        Fail,
    }

    pub(crate) struct Fake {
        answer: Answer,
        context: bool,
        /// The requests it was asked, in order.
        pub(crate) asked: Mutex<Vec<BriefRequest>>,
    }

    impl Provider for Fake {
        fn initialize(
            &self,
            params: InitializeParams,
            _host: Arc<dyn Host>,
        ) -> BoxFuture<'_, Result<InitializeResult, RpcError>> {
            let info = Info { name: "fake".to_owned(), version: "1".to_owned() };
            let capabilities = Capabilities { context: self.context, ..Capabilities::NONE };
            Box::pin(std::future::ready(params.accept(&[PROTOCOL_VERSION], info, capabilities)))
        }

        fn brief(&self, request: BriefRequest) -> BoxFuture<'_, Result<Brief, RpcError>> {
            self.asked.locked().push(request);
            match self.answer.clone() {
                Answer::Text(text) => Box::pin(std::future::ready(Ok(Brief { text }))),
                Answer::Never => Box::pin(std::future::pending()),
                Answer::Fail => Box::pin(std::future::ready(Err(RpcError::new(RpcError::INTERNAL_ERROR, "broken")))),
            }
        }
    }

    /// The fake answering `answer`, connected, with the context capability unless `context` is false.
    pub(crate) async fn connected(answer: Answer, context: bool) -> (Connected, Arc<Fake>) {
        let fake = Arc::new(Fake { answer, context, asked: Mutex::default() });
        let connected = crate::provider::connect(fake.clone()).await.expect("the fake connects");
        (connected, fake)
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::UserId;
    use ants_proto::provider::Starter;

    use super::fake::{Answer, connected};
    use super::*;

    fn caller() -> Option<Caller> {
        Some(Caller { ant: "dev-a".parse().unwrap(), identity: "id-a".to_owned(), machine: "m1".to_owned() })
    }

    fn origin() -> Origin {
        Origin { by: Starter::Human { user: UserId::human() }, channel: None }
    }

    async fn briefing_by(answer: Answer) -> (Option<Briefing>, std::sync::Arc<fake::Fake>) {
        let (provider, fake) = connected(answer, true).await;
        (Briefing::new(provider, caller(), "t1".parse().unwrap(), origin()), fake)
    }

    #[tokio::test]
    async fn the_brief_comes_under_its_heading_asked_with_the_session_s_context() {
        let (briefing, fake) = briefing_by(Answer::Text("Working on the parser.\n".to_owned())).await;
        let section = briefing.unwrap().section(BriefReason::Loaded, "claude", Some("opus")).await;
        assert_eq!(section, "[ants brief from fake]\nWorking on the parser.");
        let asked = fake.asked.lock().unwrap();
        let [request] = asked.as_slice() else { panic!("{asked:?}") };
        assert_eq!((request.reason, request.budget), (BriefReason::Loaded, BRIEF_BUDGET));
        let context = &request.context;
        assert_eq!((context.ant.as_str(), context.identity.as_str(), context.thread.as_str()), ("dev-a", "id-a", "t1"));
        assert_eq!(
            (context.harness.as_str(), context.model.as_deref(), context.machine.as_str()),
            ("claude", Some("opus"), "m1")
        );
        assert_eq!(context.origin, origin());
    }

    #[tokio::test]
    async fn an_empty_brief_adds_nothing_and_a_provider_without_context_is_not_asked() {
        let (briefing, _) = briefing_by(Answer::Text(" \n".to_owned())).await;
        assert_eq!(briefing.unwrap().section(BriefReason::Started, "test", None).await, "");
        let (provider, _) = connected(Answer::Text("brief".to_owned()), false).await;
        assert!(Briefing::new(provider, caller(), "t1".parse().unwrap(), origin()).is_none());
    }

    #[tokio::test]
    async fn a_brief_over_its_budget_is_cut_on_a_character_boundary() {
        let text = format!("a{}", "é".repeat(4000));
        let (briefing, _) = briefing_by(Answer::Text(text)).await;
        let section = briefing.unwrap().section(BriefReason::Started, "test", None).await;
        let expected = format!(
            "[ants brief from fake]\na{}\n[ants] The brief was cut to its budget of 8000 bytes.",
            "é".repeat(3999)
        );
        assert_eq!(section, expected);
    }

    #[tokio::test(start_paused = true)]
    async fn a_brief_not_sent_in_time_is_left_out_with_a_line_saying_so() {
        let (briefing, _) = briefing_by(Answer::Never).await;
        let started = tokio::time::Instant::now();
        let section = briefing.unwrap().section(BriefReason::Rebuilt, "test", None).await;
        assert_eq!(section, "[ants] The fake brief is left out: no answer within 5s.");
        assert_eq!(started.elapsed(), BRIEF_TIMEOUT);
        let (briefing, _) = briefing_by(Answer::Fail).await;
        let section = briefing.unwrap().section(BriefReason::Started, "test", None).await;
        assert_eq!(section, "[ants] The fake brief is left out: the provider failed (error -32603).");
        let (provider, fake) = connected(Answer::Text("brief".to_owned()), true).await;
        let briefing = Briefing::new(provider, None, "t1".parse().unwrap(), origin()).unwrap();
        let section = briefing.section(BriefReason::Started, "test", None).await;
        assert_eq!(section, "[ants] The fake brief is left out: the ant's identity record cannot be read.");
        assert!(fake.asked.lock().unwrap().is_empty());
    }
}
