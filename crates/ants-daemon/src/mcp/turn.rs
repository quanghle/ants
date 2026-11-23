//! The turn of a thread's session: the message it handles ([`Mcp::begin`]), which what the session sends meanwhile
//! follows on from, and what the final answer of the turn carries besides its text: the title `suggest_thread_title`
//! suggests for the thread ([`Mcp::take_title`]) and the files `attach_to_reply` attaches ([`Mcp::take_reply_files`]).
//! `complete` ends the turn with no reply at all ([`Mcp::take_completed`]). The messages a thread handles tell where it
//! came from, which the calls of the provider's tools carry, settled before they are handled ([`Mcp::settle_origin`]).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ants_proto::{Attachment, AttachmentHandle, Envelope, ThreadId, ThreadTitle, Uuid};
use chrono::{DateTime, Utc};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use super::tools::{Tools, text};
use super::{Hub, Mcp, TurnState, attach};
use crate::sync::MutexExt as _;

/// Most characters the reason given to `complete` has.
const MAX_REASON: usize = 500;

/// The session of a thread called `complete` during its turn: the turn ends with no reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completed {
    /// Why no reply is needed, as the session said: a trimmed line of 1-500 characters.
    pub reason: Option<String>,
}

impl Mcp {
    /// Take the title the session of `thread` suggested for it during the turn, for the reply to carry.
    pub fn take_title(&self, thread: &ThreadId) -> Option<String> {
        self.hub.sessions.locked().turns.get_mut(thread)?.title.take()
    }

    /// Take whether the session of `thread` called `complete` during the turn: then its final answer goes to nobody.
    pub fn take_completed(&self, thread: &ThreadId) -> Option<Completed> {
        self.hub.sessions.locked().turns.get_mut(thread)?.completed.take()
    }

    /// Take the files the session of `thread` attached to the final answer of its turn (`attach_to_reply`), while the
    /// turn lasts: the directory the thread works in and their paths in it, for [`Mcp::store_reply_files`]. None when
    /// it attached none.
    pub fn take_reply_files(&self, thread: &ThreadId) -> Option<(PathBuf, Vec<String>)> {
        let paths = self.hub.sessions.locked().turns.get_mut(thread)?.reply_files.take()?;
        // Files are attached only where the attachments' store is available.
        let (_, dir) = self.hub.files(thread).ok()?;
        Some((dir, paths))
    }

    /// Store the files at `paths`, those a turn attached to its final answer ([`Mcp::take_reply_files`]), read again
    /// from `dir`, the directory its thread worked in, and confined to it as `attach` is, under the handles `handle_of`
    /// gives their ids, those of the answer, and return their references for it to carry. Fails, the files left out,
    /// when one cannot be read or stored now.
    pub async fn store_reply_files(
        &self,
        dir: &Path,
        paths: &[String],
        handle_of: impl Fn(Uuid) -> AttachmentHandle,
    ) -> Result<Vec<Attachment>, String> {
        let loaded = self.hub.load_from(dir, paths).await?;
        self.hub.store_files(loaded, handle_of).await
    }

    /// Mark `envelope` as the message its thread is handling until the guard drops. Messages the thread's session
    /// sends meanwhile follow on from it, and spend its turn budget.
    pub fn begin(&self, envelope: &Envelope) -> Turn {
        let mut sessions = self.hub.sessions.locked();
        let turn = sessions.turns.entry(envelope.thread_id.clone());
        turn.and_modify(|turn| turn.current = envelope.clone()).or_insert_with(|| TurnState::of(envelope.clone()));
        drop(sessions);
        Turn { hub: self.hub.clone(), thread: envelope.thread_id.clone() }
    }

    /// Make `envelope`, a message that joined the prompt running on its thread, the message the thread handles, as
    /// [`Mcp::begin`] does, while the turn of the message the prompt started with lasts; the session may read the
    /// files attached to either, which the hub stored at `stored`.
    pub fn steer_to(&self, envelope: &Envelope, handles: Vec<AttachmentHandle>, stored: Option<DateTime<Utc>>) {
        let thread = &envelope.thread_id;
        let mut sessions = self.hub.sessions.locked();
        let Some(turn) = sessions.turns.get_mut(thread) else { return };
        turn.current = envelope.clone();
        let Some(stored) = stored.filter(|_| !handles.is_empty()) else { return };
        let carried = turn.carried.get_or_insert_with(|| (Vec::new(), stored));
        carried.0.extend(handles);
        carried.1 = carried.1.max(stored);
    }
}

/// See [`Mcp::begin`].
pub struct Turn {
    hub: Arc<Hub>,
    thread: ThreadId,
}

impl Turn {
    /// With the handles of the files attached to the message, which the hub stored at `stored`: `read_attachment`
    /// waits for their chunks still on their way as long as that time says ([`crate::files::get_stored`]).
    #[must_use]
    pub fn carrying(self, handles: Vec<AttachmentHandle>, stored: Option<DateTime<Utc>>) -> Self {
        if let Some(stored) = stored.filter(|_| !handles.is_empty())
            && let Some(turn) = self.hub.sessions.locked().turns.get_mut(&self.thread)
        {
            turn.carried = Some((handles, stored));
        }
        self
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        self.hub.sessions.locked().turns.remove(&self.thread);
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SuggestThreadTitle {
    /// The title: one line of 1-120 characters, such as `Fix the flaky login test`.
    title: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct AttachToReply {
    /// Files the final answer carries: paths in the working directory, at most 8 files of up to 8 MiB each, secrets in
    /// text files redacted; empty for none.
    #[serde(default)]
    attach: Vec<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Complete {
    /// Why no reply is needed, kept on the thread's timeline: one line of up to 500 characters, such as `only confirms
    /// the fix`; omitted for none.
    #[serde(default)]
    reason: Option<String>,
}

/// `reason` trimmed, None when empty; an error when it is longer than [`MAX_REASON`] or several lines.
fn completion_reason(reason: Option<&str>) -> Result<Option<String>, String> {
    let Some(reason) = reason.map(str::trim).filter(|r| !r.is_empty()) else { return Ok(None) };
    if reason.contains(['\n', '\r']) {
        return Err("invalid reason: one line only; nothing recorded".to_owned());
    }
    if reason.chars().count() > MAX_REASON {
        return Err(format!("invalid reason: at most {MAX_REASON} characters; nothing recorded"));
    }
    Ok(Some(reason.to_owned()))
}

/// Why the message `current` may not go without a reply, if so.
fn always_answered(hub: &Hub, current: &Envelope) -> Option<&'static str> {
    if current.on_behalf_of.is_some() || hub.humans.sender_role(current).is_human() {
        Some("the message being handled is a human's, which is always answered; reply to it")
    } else if current.webhook {
        Some("the message being handled is a webhook's, whose answer goes to the thread's owner; reply to it")
    } else if current.escalation.is_some() {
        Some("the message being handled carries an escalation, which is always answered; answer it or escalate")
    } else if current.approval.is_some() {
        Some("the message being handled carries an approval request, which is always answered; answer it or escalate")
    } else {
        None
    }
}

impl Hub {
    /// End the turn on `thread` with no reply to the message it handles, for `reason`, which replaces any given earlier
    /// in the turn, then react `check` to that message. Refused, nothing recorded, for a message always answered.
    async fn complete(&self, thread: &ThreadId, reason: Option<&str>) -> Result<String, String> {
        let reason = completion_reason(reason)?;
        let current = self.sessions.locked().current(thread).cloned();
        let current =
            current.ok_or("no message is being handled on this thread; a turn is completed only while one is")?;
        if let Some(why) = always_answered(self, &current) {
            return Err(format!("{why}; nothing recorded"));
        }
        {
            let mut sessions = self.sessions.locked();
            let turn = sessions.turns.get_mut(thread).filter(|turn| turn.current.id == current.id);
            let turn = turn.ok_or("the turn ended before it was completed")?;
            turn.completed = Some(Completed { reason });
        }
        let reacted = self.react(thread, super::reactions::React::current("check")).await;
        let note = reacted.err().map_or_default(|e| format!(" The check reaction to the message was not made: {e}."));
        Ok(format!(
            "This turn ends with no reply once it finishes: its final answer goes to nobody. send_message and \
             post_channel still reach others; complete again replaces the reason.{note}"
        ))
    }

    /// Have the reply to the message `thread` is handling suggest `title` for the thread, in place of any title
    /// suggested earlier in the turn.
    fn suggest_title(&self, thread: &ThreadId, title: &str) -> Result<String, String> {
        let title = ThreadTitle::new(thread.clone(), title, self.ant.clone()).map_err(|e| e.to_string())?;
        let mut sessions = self.sessions.locked();
        let turn = sessions
            .turns
            .get_mut(thread)
            .ok_or("no message is being handled on this thread; a title is suggested only while one is")?;
        turn.title = Some(title.title.clone());
        Ok(format!(
            "The reply on thread {thread} suggests the title {:?}. It becomes the thread's title when a human lists or \
             shows the thread, unless the thread has or had another title.",
            title.title
        ))
    }

    /// Have the final answer of the turn on `thread` carry the files at `paths`, in place of any given earlier in the
    /// turn; none for no paths. They are checked now, and read again as the answer is sent ([`Mcp::store_reply_files`]).
    async fn attach_to_reply(&self, thread: &ThreadId, paths: Vec<String>) -> Result<String, String> {
        {
            let mut sessions = self.sessions.locked();
            let turn = sessions.turns.get_mut(thread).ok_or(
                "no message is being handled on this thread; files are attached to its answer only while one is",
            )?;
            if paths.is_empty() {
                turn.reply_files = None;
                return Ok("The final answer of this turn carries no files.".to_owned());
            }
        }
        let loaded = self.load_files(thread, &paths).await.map_err(|e| format!("{e}; nothing attached"))?;
        let mut sessions = self.sessions.locked();
        let turn = sessions.turns.get_mut(thread).ok_or("the turn ended before the files were attached")?;
        turn.reply_files = Some(paths);
        let named: Vec<String> = loaded.iter().map(|l| attach::describe(&l.attachment)).collect();
        Ok(format!(
            "The final answer of this turn carries {}. They are read again from the working directory as the answer is \
             sent; attach_to_reply again replaces them.",
            named.join(", ")
        ))
    }
}

#[tool_router(router = turn_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "Suggest a short title for the current thread, one line of 1-120 characters naming what \
        it is about, such as `Fix the flaky login test`. The reply to the message being handled carries it, whether \
        a message, a channel post or a thread notes post; calling again in the same turn replaces it. It \
        becomes the thread's title, shown to humans in their thread lists, only if the thread has never had one: a \
        title a human set, renamed or cleared always stays.")]
    async fn suggest_thread_title(
        &self,
        Parameters(args): Parameters<SuggestThreadTitle>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.suggest_title(&thread, &args.title)))
    }

    #[tool(description = "End this turn without replying, for a message needing no answer, such as an \
        acknowledgement or a reply that only confirms. The turn's final answer is then sent to nobody; it spends no \
        turn and wakes nobody, and the message gets a `check` reaction. `reason`, optional, says why. Refused for a \
        human's, a webhook's, an escalation's or an approval request's message, which are always answered.")]
    async fn complete(
        &self,
        Parameters(args): Parameters<Complete>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.complete(&thread, args.reason.as_deref()).await))
    }

    #[tool(description = "Attach files to the final answer of this turn, the reply sent when the turn ends (a \
        message, a channel post or a thread notes post), rather than sending them with send_message. `attach` lists \
        files in the working directory, as for send_message; calling again in the same turn replaces them, and an \
        empty list attaches none. They are checked now and read again as the answer is sent; one that cannot be \
        attached then leaves the files out, the answer saying why. Spends no turn.")]
    async fn attach_to_reply(
        &self,
        Parameters(args): Parameters<AttachToReply>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.attach_to_reply(&thread, args.attach).await))
    }
}
