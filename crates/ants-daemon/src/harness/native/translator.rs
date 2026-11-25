//! The in-process ACP agent of a native protocol: one connection's requests answered with a [`Native`] and its
//! sessions.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use agent_client_protocol::v1::schema::{
    AgentCapabilities, AuthMethod, AuthMethodAgent, CancelNotification, CloseSessionRequest, CloseSessionResponse,
    ForkSessionRequest, ForkSessionResponse, Implementation, InitializeRequest, InitializeResponse, LoadSessionRequest,
    LoadSessionResponse, McpCapabilities, NewSessionRequest, NewSessionResponse, PromptCapabilities, PromptRequest,
    PromptResponse, SessionCapabilities, SessionCloseCapabilities, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelectOption, SessionForkCapabilities, SessionId, SessionMode, SessionModeState,
    SetSessionConfigOptionRequest, SetSessionConfigOptionResponse, SetSessionModeRequest, SetSessionModeResponse,
};
use agent_client_protocol::v1::{ConnectionTo, Responder};
use agent_client_protocol::{Agent, Client, DynConnectTo, Error, ErrorCode, JsonRpcResponse};

use super::{Cli, Credentials, Native, NativeSession, Open, Setting, Updates, failed, unsupported};
use crate::harness::{SteerRequest, SteerResponse, advertise_steering, background, skills};
use crate::sync::MutexExt as _;

/// Credentials a provider refused at a prompt, by where they are kept and their fingerprint: what the CLI said then.
static REFUSED: Mutex<BTreeMap<(String, u64), String>> = Mutex::new(BTreeMap::new());

/// An open session, with the model and reasoning effort it runs.
struct Live<S> {
    session: Arc<S>,
    settings: Mutex<BTreeMap<&'static str, String>>,
    /// The prompts running in it.
    prompts: AtomicUsize,
}

/// A prompt running in a session, counted until it ends, however it does.
struct Prompting<'a>(&'a AtomicUsize);

impl<'a> Prompting<'a> {
    fn new(prompts: &'a AtomicUsize) -> Self {
        prompts.fetch_add(1, Ordering::Relaxed);
        Self(prompts)
    }
}

impl Drop for Prompting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The ACP agent side of one connection.
pub(super) struct Translator<N: Native> {
    cli: Cli,
    native: tokio::sync::OnceCell<N>,
    sessions: Mutex<HashMap<SessionId, Arc<Live<N::Session>>>>,
}

/// The in-process ACP agent translating to protocol `N` for `cli`.
pub(super) fn connect<N: Native>(cli: Cli) -> DynConnectTo<Client> {
    let name = cli.name().to_owned();
    let t = Arc::new(Translator::<N>::new(cli));
    let [t1, t2, t3, t4, t5, t6, t7, t8, t9, t10] = [(); 10].map(|()| t.clone());
    DynConnectTo::new(
        Agent
            .v1()
            .name(name)
            .on_receive_request(
                async move |r: InitializeRequest, responder, cx| answer(&cx, responder, t1.clone().initialize(r)),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: NewSessionRequest, responder, cx| answer(&cx, responder, t2.clone().new_session(r)),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: LoadSessionRequest, responder, cx| answer(&cx, responder, t3.clone().load_session(r)),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: ForkSessionRequest, responder, cx| answer(&cx, responder, t4.clone().fork_session(r)),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: PromptRequest, responder, cx| {
                    let work = t5.clone().prompt(r, cx.clone());
                    answer(&cx, responder, work)
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_notification(
                async move |n: CancelNotification, cx| {
                    let live = t6.live(&n.session_id);
                    cx.spawn(async move {
                        match live {
                            Ok(live) => live.session.cancel().await,
                            Err(e) => tracing::debug!(error = %e, "session/cancel"),
                        }
                        Ok(())
                    })
                },
                agent_client_protocol::on_receive_notification!(),
            )
            .on_receive_request(
                async move |r: CloseSessionRequest, responder, cx| answer(&cx, responder, t9.clone().close_session(r)),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: SetSessionModeRequest, responder, cx| answer(&cx, responder, t7.clone().set_mode(r)),
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: SetSessionConfigOptionRequest, responder, cx| {
                    answer(&cx, responder, t8.clone().set_option(r))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |r: SteerRequest, responder, cx| answer(&cx, responder, t10.clone().steer(r)),
                agent_client_protocol::on_receive_request!(),
            ),
    )
}

/// Answer a request with what `work` comes to, off the dispatch loop so that it holds up no other message.
fn answer<R: JsonRpcResponse>(
    cx: &ConnectionTo<Client>,
    responder: Responder<R>,
    work: impl Future<Output = Result<R, Error>> + Send + 'static,
) -> Result<(), Error> {
    cx.spawn(async move { responder.respond_with_result(work.await) })
}

impl<N: Native> Translator<N> {
    pub(super) fn new(cli: Cli) -> Self {
        Self { cli, native: tokio::sync::OnceCell::new(), sessions: Mutex::default() }
    }

    pub(super) async fn initialize(self: Arc<Self>, init: InitializeRequest) -> Result<InitializeResponse, Error> {
        let native = N::start(self.cli.clone()).await?;
        let info = native.info();
        if self.native.set(native).is_err() {
            return Err(Error::new(ErrorCode::INVALID_REQUEST, "initialize was already called"));
        }
        let mut sessions = SessionCapabilities::new().close(SessionCloseCapabilities::new());
        if info.fork {
            sessions = sessions.fork(SessionForkCapabilities::new());
        }
        let capabilities = AgentCapabilities::new()
            .load_session(info.load)
            .session_capabilities(sessions)
            .prompt_capabilities(PromptCapabilities::new().image(info.images))
            .mcp_capabilities(McpCapabilities::new().http(info.http_mcp));
        let capabilities = if info.steer { advertise_steering(capabilities) } else { capabilities };
        let methods = self.cli.sign_in().map(|command| {
            AuthMethod::Agent(AuthMethodAgent::new("sign_in", "Sign in").description(command.to_owned()))
        });
        Ok(InitializeResponse::new(init.protocol_version)
            .agent_info(Implementation::new(info.name, info.version))
            .agent_capabilities(capabilities)
            .auth_methods(methods.into_iter().collect()))
    }

    fn native(&self) -> Result<&N, Error> {
        self.native.get().ok_or_else(|| Error::new(ErrorCode::INVALID_REQUEST, "initialize first"))
    }

    /// The CLI, to open a session with: signed out while its credentials are ones a provider refused at a prompt.
    fn opening(&self) -> Result<&N, Error> {
        let native = self.native()?;
        let Some(Credentials { place, fingerprint }) = native.credentials() else { return Ok(native) };
        match REFUSED.locked().get(&(place, fingerprint)) {
            Some(refused) => Err(Error::new(ErrorCode::AUTH_REQUIRED, refused.clone())),
            None => Ok(native),
        }
    }

    /// Note that the provider refused the CLI's credentials at a prompt, which said `e`.
    fn refused(&self, e: &Error) {
        let Some(credentials) = self.native.get().and_then(N::credentials) else { return };
        tracing::warn!(
            harness = self.cli.name(),
            credentials = credentials.place,
            "credentials refused at a prompt; the harness stays signed out until they change"
        );
        let detail = format!("{} (refused at a prompt; {} unchanged since)", e.message, credentials.place);
        REFUSED.locked().insert((credentials.place, credentials.fingerprint), detail);
    }

    fn live(&self, id: &SessionId) -> Result<Arc<Live<N::Session>>, Error> {
        let live = self.sessions.locked().get(id).cloned();
        live.ok_or_else(|| Error::new(ErrorCode::RESOURCE_NOT_FOUND, format!("no session {id}")))
    }

    /// Keep `session` open as `id`, with the ant's settings: the session modes and selectors to answer with.
    fn opened(
        &self,
        id: SessionId,
        session: N::Session,
    ) -> (SessionId, Option<SessionModeState>, Vec<SessionConfigOption>) {
        let mut settings = BTreeMap::new();
        for (setting, value) in [(Setting::Model, self.cli.model()), (Setting::ReasoningEffort, self.cli.effort())] {
            if let Some(value) = value {
                settings.insert(setting.id(), value.to_owned());
            }
        }
        let options = options(&settings);
        let live = Live { session: Arc::new(session), settings: Mutex::new(settings), prompts: AtomicUsize::new(0) };
        self.sessions.locked().insert(id.clone(), Arc::new(live));
        let modes = self.cli.mode().map(|mode| {
            SessionModeState::new(mode.to_owned(), vec![SessionMode::new(mode.to_owned(), mode.to_owned())])
        });
        (id, modes, options)
    }

    pub(super) async fn new_session(self: Arc<Self>, request: NewSessionRequest) -> Result<NewSessionResponse, Error> {
        let skills = skills::of_meta(request.meta.as_ref());
        let open = Open { cwd: request.cwd, mcp_servers: request.mcp_servers, sign_ins: self.cli.report(), skills };
        let session = self.opening()?.new_session(open).await?;
        let (id, modes, options) = self.opened(SessionId::new(session.id()), session);
        Ok(NewSessionResponse::new(id).modes(modes).config_options(options))
    }

    async fn load_session(self: Arc<Self>, request: LoadSessionRequest) -> Result<LoadSessionResponse, Error> {
        let skills = skills::of_meta(request.meta.as_ref());
        let open = Open { cwd: request.cwd, mcp_servers: request.mcp_servers, sign_ins: self.cli.report(), skills };
        let session = self.opening()?.load_session(&request.session_id.0, open).await?;
        let (_, modes, options) = self.opened(request.session_id, session);
        Ok(LoadSessionResponse::new().modes(modes).config_options(options))
    }

    async fn fork_session(self: Arc<Self>, request: ForkSessionRequest) -> Result<ForkSessionResponse, Error> {
        let native = self.opening()?;
        if !native.info().fork {
            return Err(unsupported("session/fork"));
        }
        let open = Open { cwd: request.cwd, mcp_servers: request.mcp_servers, sign_ins: None, skills: None };
        let session = native.fork_session(&request.session_id.0, open).await?;
        let (id, modes, options) = self.opened(SessionId::new(session.id()), session);
        Ok(ForkSessionResponse::new(id).modes(modes).config_options(options))
    }

    pub(super) async fn prompt(
        self: Arc<Self>,
        request: PromptRequest,
        cx: ConnectionTo<Client>,
    ) -> Result<PromptResponse, Error> {
        let live = self.live(&request.session_id)?;
        let updates = Updates::new(cx, request.session_id);
        let prompting = Prompting::new(&live.prompts);
        let done = live.session.prompt(request.prompt, updates).await;
        drop(prompting);
        if let Err(e) = &done
            && e.code == ErrorCode::AUTH_REQUIRED
        {
            self.refused(e);
        }
        // What the CLI runs is walked as each turn ends, so that its release ends it even if the CLI died first.
        let left = self.cli.runs().turn_ended().await;
        done.map(|response| background::with(response, &left))
    }

    pub(super) async fn steer(self: Arc<Self>, request: SteerRequest) -> Result<SteerResponse, Error> {
        let live = self.live(&request.session_id)?;
        let accepted = self.native()?.info().steer
            && live.prompts.load(Ordering::Relaxed) > 0
            && live.session.steer(request.prompt).await?;
        Ok(SteerResponse { accepted })
    }

    /// Forget a session the daemon abandoned, as on a context overflow, cancelling its prompt if one runs: dropped, it
    /// ends what runs for it alone, such as Claude Code's process or agy's and its private directory.
    async fn close_session(self: Arc<Self>, request: CloseSessionRequest) -> Result<CloseSessionResponse, Error> {
        let live = self.live(&request.session_id)?;
        self.sessions.locked().remove(&request.session_id);
        if live.prompts.load(Ordering::Relaxed) > 0 {
            live.session.cancel().await;
        }
        live.session.close().await;
        Ok(CloseSessionResponse::new())
    }

    async fn set_mode(self: Arc<Self>, request: SetSessionModeRequest) -> Result<SetSessionModeResponse, Error> {
        self.live(&request.session_id)?;
        match self.cli.mode() {
            Some(mode) if *request.mode_id.0 == *mode => Ok(SetSessionModeResponse::new()),
            _ => Err(failed(format!(
                "harness {}: the session mode is the definition's mode, the CLI's permission mode",
                self.cli.name()
            ))),
        }
    }

    async fn set_option(
        self: Arc<Self>,
        request: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, Error> {
        let live = self.live(&request.session_id)?;
        let setting = match &*request.config_id.0 {
            "model" => Setting::Model,
            "reasoning_effort" => Setting::ReasoningEffort,
            other => return Err(Error::new(ErrorCode::INVALID_PARAMS, format!("no session setting {other}"))),
        };
        let Some(value) = request.value.as_value_id().map(|value| value.0.to_string()) else {
            return Err(Error::new(ErrorCode::INVALID_PARAMS, format!("{} takes a value id", setting.id())));
        };
        let current = live.settings.locked().get(setting.id()).cloned();
        if current.as_deref() != Some(value.as_str()) {
            live.session.configure(setting, &value).await?;
            live.settings.locked().insert(setting.id(), value);
        }
        Ok(SetSessionConfigOptionResponse::new(options(&live.settings.locked())))
    }
}

/// The selectors of a session running `settings`, each offering the value it runs.
fn options(settings: &BTreeMap<&'static str, String>) -> Vec<SessionConfigOption> {
    settings
        .iter()
        .map(|(&id, value)| {
            let (name, category) = match id {
                "model" => ("Model", SessionConfigOptionCategory::Model),
                _ => ("Reasoning effort", SessionConfigOptionCategory::ThoughtLevel),
            };
            let offered = vec![SessionConfigSelectOption::new(value.clone(), value.clone())];
            SessionConfigOption::select(id, name, value.clone(), offered).category(category)
        })
        .collect()
}
