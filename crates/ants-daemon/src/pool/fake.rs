//! In-process fake harnesses for the daemon's and the pool's tests: what they were asked to do, and the switches
//! that make them fail as real ones do.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use agent_client_protocol::v1::schema::{
    AgentCapabilities, CancelNotification, ContentBlock, ContentChunk, Cost, ForkSessionRequest, ForkSessionResponse,
    Implementation, InitializeRequest, InitializeResponse, LoadSessionRequest, LoadSessionResponse, NewSessionRequest,
    NewSessionResponse, PermissionOption, PermissionOptionKind, PromptCapabilities, PromptRequest, PromptResponse,
    RequestPermissionOutcome, RequestPermissionRequest, SessionCapabilities, SessionForkCapabilities, SessionId,
    SessionNotification, SessionUpdate, StopReason, TextContent, ToolCall, ToolCallUpdate, ToolCallUpdateFields,
    ToolKind, Usage, UsageUpdate,
};
use agent_client_protocol::v1::{ConnectionTo, Responder};
use agent_client_protocol::{Agent, Client, DynConnectTo, Error};
use tokio::sync::Semaphore;

use crate::harness::{Launch, SteerRequest, SteerResponse, advertise_steering};
use crate::sync::MutexExt as _;

/// What tests set the fake harnesses to do, read as each request arrives.
#[derive(Default, Debug)]
pub struct Knobs {
    /// While set, opening a session or prompting fails with ACP's `auth_required`, as a harness that is not
    /// signed in does.
    pub signed_out: bool,
    /// While set, session/load fails, as with a harness whose session store was cleared.
    pub lose_sessions: bool,
    /// While set, prompts stream this text and fail with it, as Claude's adapter reports a provider limit.
    pub limited: Option<String>,
    /// Whether [`Knobs::limited`] ends the prompt as its reply instead, as Copilot CLI and Antigravity do.
    pub limited_reply: bool,
    /// While set, the handshake fails, as with a broken install.
    pub broken: bool,
    /// While set, the handshake fails with this error, as when the daemon runs out of open files.
    pub broken_with: Option<String>,
    /// While set, harnesses starting advertise that they take images in prompts.
    pub takes_images: bool,
    /// While set, harnesses starting advertise session/fork.
    pub forks: bool,
    /// While set, harnesses starting advertise `_ants/steer`.
    pub steers: bool,
}

/// What the fake harnesses were asked to do, across all their "processes", and what tests set them to do.
#[derive(Default, Debug)]
pub struct Log {
    pub starts: usize,
    pub new_sessions: usize,
    pub loaded: Vec<String>,
    /// Working directories of the sessions created or loaded, in order.
    pub cwds: Vec<PathBuf>,
    /// Prompts received, before any is held or answered.
    pub prompted: usize,
    /// Their texts, in order.
    pub prompts: Vec<String>,
    /// Prompts in the order they were answered.
    pub answered: Vec<String>,
    /// Prompts ended by session/cancel.
    pub cancelled: usize,
    /// Prompts with the word `doomed` that started, then failed.
    pub doomed: usize,
    pub doomed_failed: usize,
    flaky_failed: Vec<String>,
    /// What tests set the harnesses to do.
    pub knobs: Knobs,
    /// The images prompts carried after their text, as media type and base64 data, in order.
    pub images: Vec<(String, String)>,
    /// The sessions forked, each with the working directory of its fork, in order; fork `n` is session `f<n>`.
    pub forked: Vec<(String, PathBuf)>,
    /// The texts running prompts took as steers, in order.
    pub steered: Vec<String>,
}

/// In-process fake harnesses that answer each prompt with `<session>: <text>`. A prompt starting with `hold`, or
/// with a line starting with `hold:`, waits for a permit from the returned semaphore first; one starting with
/// `flaky` fails the first time; one with the word `doomed` waits for a permit too, then fails, every time. Words
/// in a prompt make it report spend and work: `tools N` starts N tool calls, `cost X` reports X as the session's
/// cost, `tokens N` reports N tokens in the response, and `stall` replies `partial` and waits for session/cancel.
/// `overflows` fails as a prompt that outgrew the context window, and so does `overflow` unless the session was
/// rebuilt for it. While [`Knobs::signed_out`] is set, sessions and prompts fail as with a harness that is not signed
/// in, and a prompt with the word `sign-out` starts its `tools N`, then sets it and fails so; while
/// [`Knobs::limited`] is, prompts start their `tools N`, then stream its text and fail with it, or end with it as
/// their reply; while [`Knobs::broken`] is, the handshake fails; while [`Knobs::takes_images`] is, harnesses starting
/// take images, which they log; while [`Knobs::steers`] is, harnesses starting take steers into a running prompt,
/// whose reply ends with a line `+ <steer>` for each, though they refuse a steer ending with `refused`. `ask TOOL`
/// asks permission to run the ants MCP tool TOOL, the reply ending with a line `asked TOOL: <option picked>`. They
/// can load stored sessions.
pub fn launch() -> (Launch, Arc<Mutex<Log>>, Arc<Semaphore>) {
    launch_with(Sessions::Loadable)
}

/// Whether fake harnesses can load a stored session, or only start new ones.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Sessions {
    Loadable,
    NewOnly,
}

/// [`launch`], with harnesses that load stored `sessions` or not.
pub fn launch_with(sessions: Sessions) -> (Launch, Arc<Mutex<Log>>, Arc<Semaphore>) {
    let log = Arc::new(Mutex::new(Log::default()));
    let gate = Arc::new(Semaphore::new(0));
    let (shared, held) = (log.clone(), gate.clone());
    let connect = Arc::new(move || {
        shared.locked().starts += 1;
        let fake = Fake {
            log: shared.clone(),
            gate: held.clone(),
            cancels: Arc::new(Semaphore::new(0)),
            sessions,
            running: Arc::default(),
        };
        let (on_cancel, on_init, on_new, on_load, on_fork, on_steer, on_prompt) =
            (fake.clone(), fake.clone(), fake.clone(), fake.clone(), fake.clone(), fake.clone(), fake);
        DynConnectTo::new(
            Agent
                .v1()
                .name("fake")
                .on_receive_notification(
                    async move |_: CancelNotification, _cx| {
                        on_cancel.cancels.add_permits(1);
                        Ok(())
                    },
                    agent_client_protocol::on_receive_notification!(),
                )
                .on_receive_request(
                    async move |init: InitializeRequest, responder, _cx| on_init.initialize(init, responder),
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |new: NewSessionRequest, responder, _cx| on_new.new_session(new, responder),
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |load: LoadSessionRequest, responder, cx| on_load.load_session(load, responder, &cx),
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |fork: ForkSessionRequest, responder, _cx| on_fork.fork_session(fork, responder),
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |steer: SteerRequest, responder, _cx| on_steer.steer(steer, responder),
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |prompt: PromptRequest, responder, cx| on_prompt.prompt(prompt, responder, &cx),
                    agent_client_protocol::on_receive_request!(),
                ),
        )
    });
    (Launch::with_transport(connect, PathBuf::from("/"), Default::default()), log, gate)
}

/// One fake harness "process": the shared log and gate, and the permits its session/cancel notifications leave.
#[derive(Clone)]
struct Fake {
    log: Arc<Mutex<Log>>,
    gate: Arc<Semaphore>,
    cancels: Arc<Semaphore>,
    sessions: Sessions,
    /// The sessions running a prompt, with the steers it took.
    running: Arc<Mutex<HashMap<SessionId, Vec<String>>>>,
}

impl Fake {
    fn initialize(&self, init: InitializeRequest, responder: Responder<InitializeResponse>) -> Result<(), Error> {
        let log = self.log.locked();
        if log.knobs.broken {
            return responder.respond_with_internal_error("broken install");
        }
        if let Some(error) = &log.knobs.broken_with {
            return responder.respond_with_internal_error(error.clone());
        }
        let prompts = PromptCapabilities::new().image(log.knobs.takes_images);
        let fork = log.knobs.forks.then(SessionForkCapabilities::new);
        let steers = log.knobs.steers;
        drop(log);
        let capabilities = AgentCapabilities::new()
            .load_session(self.sessions == Sessions::Loadable)
            .prompt_capabilities(prompts)
            .session_capabilities(SessionCapabilities::new().fork(fork));
        let capabilities = if steers { advertise_steering(capabilities) } else { capabilities };
        responder.respond(
            InitializeResponse::new(init.protocol_version)
                .agent_capabilities(capabilities)
                .agent_info(Implementation::new("fake", "1.0")),
        )
    }

    /// Take a steer into the prompt running in its session, if one is.
    fn steer(&self, steer: SteerRequest, responder: Responder<SteerResponse>) -> Result<(), Error> {
        let ContentBlock::Text(t) = &steer.prompt[0] else { unreachable!() };
        let mut running = self.running.locked();
        let accepted = match running.get_mut(&steer.session_id) {
            Some(_) if t.text.ends_with("refused") => false,
            Some(steers) => {
                steers.push(t.text.clone());
                self.log.locked().steered.push(t.text.clone());
                true
            }
            None => false,
        };
        responder.respond(SteerResponse { accepted })
    }

    fn new_session(&self, new: NewSessionRequest, responder: Responder<NewSessionResponse>) -> Result<(), Error> {
        let mut log = self.log.locked();
        if log.knobs.signed_out {
            return responder.respond_with_error(Error::auth_required());
        }
        log.new_sessions += 1;
        log.cwds.push(new.cwd);
        responder.respond(NewSessionResponse::new(format!("s{}", log.new_sessions)))
    }

    fn load_session(
        &self,
        load: LoadSessionRequest,
        responder: Responder<LoadSessionResponse>,
        cx: &ConnectionTo<Client>,
    ) -> Result<(), Error> {
        let mut log = self.log.locked();
        if log.knobs.signed_out {
            return responder.respond_with_error(Error::auth_required());
        }
        if log.knobs.lose_sessions {
            return responder.respond_with_internal_error("Session not found");
        }
        log.loaded.push(load.session_id.to_string());
        log.cwds.push(load.cwd.clone());
        drop(log);
        // Replayed history must not leak into the next reply.
        cx.send_notification(chunk(&load.session_id, "old history"))?;
        responder.respond(LoadSessionResponse::new())
    }

    fn fork_session(&self, fork: ForkSessionRequest, responder: Responder<ForkSessionResponse>) -> Result<(), Error> {
        let mut log = self.log.locked();
        log.forked.push((fork.session_id.to_string(), fork.cwd));
        responder.respond(ForkSessionResponse::new(format!("f{}", log.forked.len())))
    }

    fn prompt(
        &self,
        prompt: PromptRequest,
        responder: Responder<PromptResponse>,
        cx: &ConnectionTo<Client>,
    ) -> Result<(), Error> {
        let mut log = self.log.locked();
        if log.knobs.signed_out {
            return responder.respond_with_error(Error::auth_required());
        }
        log.prompted += 1;
        let ContentBlock::Text(t) = &prompt.prompt[0] else { unreachable!() };
        let words: Vec<&str> = t.text.split_whitespace().collect();
        let signs_out = words.contains(&"sign-out");
        if signs_out || log.knobs.limited.is_some() {
            // Tools run before the sign-out or the limit is met.
            let tools = words.windows(2).find(|w| w[0] == "tools").and_then(|w| w[1].parse().ok());
            for i in 0..tools.unwrap_or(0u64) {
                let call = SessionUpdate::ToolCall(ToolCall::new(format!("call-{i}"), "tool"));
                cx.send_notification(SessionNotification::new(prompt.session_id.clone(), call))?;
            }
        }
        if signs_out {
            log.knobs.signed_out = true;
            return responder.respond_with_error(Error::auth_required());
        }
        if let Some(limited) = &log.knobs.limited {
            cx.send_notification(chunk(&prompt.session_id, limited))?;
            if log.knobs.limited_reply {
                return responder.respond(PromptResponse::new(StopReason::EndTurn));
            }
            let error = Error::internal_error().data(limited.clone());
            return responder.respond_with_error(error);
        }
        log.prompts.push(t.text.clone());
        for block in &prompt.prompt[1..] {
            let ContentBlock::Image(image) = block else { unreachable!() };
            log.images.push((image.mime_type.clone(), image.data.clone()));
        }
        drop(log);
        self.running.locked().insert(prompt.session_id.clone(), Vec::new());
        let (fake, text, sender) = (self.clone(), t.text.clone(), cx.clone());
        // Answer from a task, so a held prompt does not stall the connection.
        cx.spawn(async move { fake.answer(text, prompt.session_id, responder, sender).await })
    }

    /// Answer a prompt with `text`, after the waits and failures its words ask for.
    async fn answer(
        self,
        text: String,
        session: SessionId,
        responder: Responder<PromptResponse>,
        sender: ConnectionTo<Client>,
    ) -> Result<(), Error> {
        let Fake { log, gate, cancels, running, .. } = self;
        if text.starts_with("hold") || text.lines().any(|line| line.starts_with("hold:")) {
            gate.acquire().await.unwrap().forget();
        }
        let steered = running.locked().remove(&session).unwrap_or_default();
        let text = steered.iter().fold(text, |text, steer| format!("{text}\n+ {steer}"));
        if text.split_whitespace().any(|w| w == "doomed") {
            log.locked().doomed += 1;
            gate.acquire().await.unwrap().forget();
            log.locked().doomed_failed += 1;
            return responder.respond_with_internal_error("doomed");
        }
        if text.starts_with("flaky") {
            let mut log = log.locked();
            if !log.flaky_failed.contains(&text) {
                log.flaky_failed.push(text);
                return responder.respond_with_internal_error("flaky");
            }
        }
        let session = &session;
        let words: Vec<&str> = text.split_whitespace().collect();
        let rebuilt = text.contains("[ants] The session of this thread outgrew");
        if words.contains(&"overflows") || words.contains(&"overflow") && !rebuilt {
            return responder.respond_with_internal_error("Prompt is too long");
        }
        // A word followed by a number, so prose such as the redelivery warning is no command.
        let arg = |word: &str| {
            let i = words.iter().position(|w| *w == word)?;
            words.get(i + 1)?.parse::<f64>().ok()
        };
        for i in 0..arg("tools").map_or(0, |n| n as u64) {
            let call = SessionUpdate::ToolCall(ToolCall::new(format!("call-{i}"), "tool"));
            sender.send_notification(SessionNotification::new(session.clone(), call))?;
        }
        if let Some(total) = arg("cost") {
            let cost = Cost::new(total, "USD");
            let usage = SessionUpdate::UsageUpdate(UsageUpdate::new(1, 100).cost(cost));
            sender.send_notification(SessionNotification::new(session.clone(), usage))?;
        }
        if words.contains(&"stall") {
            sender.send_notification(chunk(session, "partial"))?;
            cancels.acquire().await.unwrap().forget();
            log.locked().cancelled += 1;
            return responder.respond(PromptResponse::new(StopReason::Cancelled));
        }
        let mut reply = text.clone();
        if let Some(tool) = words.windows(2).find(|w| w[0] == "ask").map(|w| w[1]) {
            let picked = ask(&sender, session, tool).await?;
            let _ = write!(reply, "\nasked {tool}: {picked}");
        }
        log.locked().answered.push(text.clone());
        // In two chunks, as harnesses stream a reply.
        sender.send_notification(chunk(session, &format!("{session}: ")))?;
        sender.send_notification(chunk(session, &reply))?;
        let tokens = arg("tokens").map(|n| n as u64);
        let usage = tokens.map(|n| Usage::new(n, n, 0));
        responder.respond(PromptResponse::new(StopReason::EndTurn).usage(usage))
    }
}

/// Ask permission to run `tool` of the ants MCP server, as Claude Code titles it: the option the client picked.
async fn ask(sender: &ConnectionTo<Client>, session: &SessionId, tool: &str) -> Result<String, Error> {
    let fields = ToolCallUpdateFields::new().kind(ToolKind::Other).title(format!("mcp__ants__{tool}"));
    let options = vec![
        PermissionOption::new("allow", "Allow", PermissionOptionKind::AllowOnce),
        PermissionOption::new("reject", "Reject", PermissionOptionKind::RejectOnce),
    ];
    let request = RequestPermissionRequest::new(session.clone(), ToolCallUpdate::new("asked", fields), options);
    Ok(match sender.send_request(request).block_task().await?.outcome {
        RequestPermissionOutcome::Selected(selected) => selected.option_id.0.to_string(),
        _ => "cancelled".to_owned(),
    })
}

fn chunk(session: &SessionId, text: &str) -> SessionNotification {
    SessionNotification::new(
        session.clone(),
        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(text)))),
    )
}
