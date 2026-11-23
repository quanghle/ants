//! The tools of the notes of a thread (see [`crate::stream::notes`]): `post_notes`, which spends a turn of the notes' budget
//! and escalates the thread with the last, and `read_notes`.

use std::fmt::Write as _;

use ants_proto::{AttachmentHandle, AttachmentScope, Envelope, EscalationReason, NotesPost, ThreadId};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use super::tools::{Tools, text};
use super::{Hub, attach};
use crate::escalation::{Escalated, Stuck};
use crate::sync::MutexExt as _;

/// Times a post is tried again after another post took the turn it was to spend.
pub(super) const POST_TRIES: usize = 5;
/// Most recent posts `read_notes` shows.
const SHOWN_POSTS: usize = 50;

#[derive(Deserialize, schemars::JsonSchema)]
struct PostNotes {
    /// The post.
    text: String,
    /// Files to attach: paths in the working directory, at most 8 files of up to 8 MiB each; secrets in text files are
    /// redacted.
    #[serde(default)]
    attach: Vec<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ReadNotes {
    /// Thread whose notes to read; omitted for the current one.
    #[serde(default)]
    thread: Option<String>,
}

impl Hub {
    /// Post `text` to the notes of `thread`, spending one of its turns. The post that spends the last one escalates
    /// the thread up its origin chain, in the end to its owner, else the default human.
    async fn post_notes(&self, thread: &ThreadId, text: String, attach: &[String]) -> Result<String, String> {
        let current = self.sessions.locked().current(thread).map(|e| {
            let carried = e.escalation.as_deref().cloned();
            let owner = (self.humans.human_owner(e.owner.as_ref()), e.creator.clone(), e.origin_thread.clone());
            (e.turn_budget, owner, self.humans.owner_of(e), carried, e.allow.clone())
        });
        let (budget, (owner, creator, origin_thread), human, carried, allow) =
            current.ok_or("no message is being handled on this thread; posts go out only while one is")?;
        let mut loaded = self.load(thread, attach).await?;
        let mut attachments = None;
        for _ in 0..POST_TRIES {
            let notes = self.notes.read(thread).await.map_err(|e| format!("cannot read the thread notes: {e:#}"))?;
            let mut post = NotesPost::spend(thread.clone(), self.ant.clone(), text.clone(), budget, notes.last())
                .map_err(|_| {
                    format!("the notes of thread {thread} have no turns left; the thread is {human}'s to decide")
                })?;
            // Stored once, before the first try: the handles name the thread, not the post.
            if attachments.is_none() {
                post.attachments = loaded.iter().map(|l| l.attachment.clone()).collect();
                self.check_fits(&post)?;
                let ant = self.ant.clone();
                let handle_of =
                    |id| AttachmentHandle { from: ant.clone(), scope: AttachmentScope::Notes(thread.clone()), id };
                attachments = Some(self.store(std::mem::take(&mut loaded), handle_of).await?);
            }
            post.attachments = attachments.clone().unwrap_or_default();
            if !self.notes.post(&post, notes.last_seq).await.map_err(|e| format!("not posted: {e:#}"))? {
                continue;
            }
            if post.turn_budget > 0 {
                return Ok(format!("Posted to the notes of thread {thread} ({} turns left).", post.turn_budget));
            }
            let notice = Envelope::on_thread(
                thread.clone(),
                self.ant.clone(),
                human.clone(),
                format!(
                    "The notes of thread {thread} spent their turn budget; the thread needs a decision. Last post \
                     from {}:\n{}\n`ants notes {thread}` shows the notes; `ants notes {thread} TEXT` posts there with \
                     a fresh budget.",
                    post.from, post.text
                ),
            )
            .with_owner(owner.clone())
            .with_creator(creator.clone())
            .with_origin_thread(origin_thread.clone())
            .with_allow(allow.clone());
            let Some(escalator) = &self.escalator else {
                tracing::info!(%thread, %human, "thread notes out of turns; handing the thread to its owner");
                self.send(&notice).await.map_err(|e| {
                    format!("posted the notes' last turn, but {human} was not told ({e}); message {human}")
                })?;
                return Ok(format!(
                    "Posted the last turn of the notes of thread {thread}; {human} has the thread now."
                ));
            };
            let stuck =
                Stuck { carried, ..Stuck::new(EscalationReason::Notes, notice, format!("notes-{}", post.id), None) };
            let escalated = escalator.escalate(stuck).await.map_err(|e| {
                format!("posted the notes' last turn, but the escalation was not sent ({e:#}); message {human}")
            })?;
            tracing::info!(%thread, %escalated, "thread notes out of turns; escalated");
            return Ok(match escalated {
                Escalated::Owner(human) => {
                    format!("Posted the last turn of the notes of thread {thread}; {human} has the thread now.")
                }
                Escalated::Chain { .. } | Escalated::Assistant { .. } => {
                    format!("Posted the last turn of the notes of thread {thread}; escalated to {escalated}.")
                }
            });
        }
        Err(format!("the notes of thread {thread} kept changing; read_notes and post again"))
    }
}

#[tool_router(router = notes_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "Post to the notes of the current thread: one shared board per thread where the ants on \
        it coordinate. A post notifies and wakes nobody; the others see it only when they call read_notes, so use \
        send_message or ask_ant to ask a named ant to read it. A post spends one turn of the thread's budget, \
        and the post that spends the last one escalates the thread: the notes then take no more ant posts until a \
        human posts there or the escalation goes up a level. `attach` lists files in the working directory to \
        attach, as for send_message.")]
    async fn post_notes(
        &self,
        Parameters(args): Parameters<PostNotes>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.post_notes(&thread, args.text, &args.attach).await))
    }

    #[tool(description = "Read the notes of the current thread, or of `thread`: who posted what, oldest first, and \
        how many turns the notes have left. Posts there notify nobody, so read the notes when asked to or before \
        posting. Reading spends no turn.")]
    async fn read_notes(
        &self,
        Parameters(args): Parameters<ReadNotes>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let current = self.thread(&cx)?;
        let result = async {
            let thread = match args.thread.as_deref().map(str::trim) {
                None | Some("") => current,
                Some(other) => other.parse().map_err(|e| format!("{e}"))?,
            };
            let notes =
                self.hub.notes.read(&thread).await.map_err(|e| format!("cannot read the thread notes: {e:#}"))?;
            Ok(describe_notes(&thread, &notes.posts))
        }
        .await;
        Ok(text(result))
    }
}

/// The most recent posts of a thread's notes, oldest first, with what its budget has left.
fn describe_notes(thread: &ThreadId, posts: &[NotesPost]) -> String {
    let Some(last) = posts.last() else { return format!("Thread {thread} has no notes yet.") };
    let shown = &posts[posts.len().saturating_sub(SHOWN_POSTS)..];
    let mut text = format!("Notes of thread {thread}: {} posts", posts.len());
    if shown.len() < posts.len() {
        let _ = write!(text, ", the last {} shown", shown.len());
    }
    let _ = write!(text, "; {} turns left.", last.turn_budget);
    for post in shown {
        let attachments = attach::listing(&post.attachments, |id| post.attachment_handle(id));
        let _ = write!(text, "\n\n{} at {}:\n{}{attachments}", post.from, post.sent_at.to_rfc3339(), post.text);
    }
    text
}
