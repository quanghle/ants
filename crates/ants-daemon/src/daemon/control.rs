//! What humans and admins ask of the running ant besides messages: cancelling its prompt (`ants cancel`), an
//! assistant's cancel or withdrawal for its human, restarting its harnesses, reading a snapshot of it for a clone and reading or signing in its MCP servers.

use crate::bus::relayed;
use std::collections::HashMap;

use ants_proto::{Acting, CancelReply, CancelRequest, MessageMark, ThreadId, UserId, subject};
use anyhow::Result;
use futures::stream::StreamExt as _;

use crate::admin::restart::Restarted;
use crate::bus;
use crate::sync::MutexExt as _;

use super::{CLONE_WAIT_CHECK, Inbox};

/// The prompt running on each thread, and where to send the human who cancels it.
#[derive(Default)]
pub(super) struct Cancels(std::sync::Mutex<HashMap<ThreadId, tokio::sync::mpsc::Sender<Cancel>>>);

/// A cancel of a running prompt: a human's, or a steering message's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Cancel {
    pub(super) by: UserId,
    pub(super) why: Why,
}

/// What a prompt is cancelled for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Why {
    /// `ants cancel`.
    Cancel,
    /// A cancel request of an assistant for this human, whose grant names it.
    CancelFor(UserId),
    /// A message steering the thread, which it takes next.
    Steer,
    /// An admin's order to restart the ant's harnesses now ([`crate::admin::restart`]).
    Restart,
}

impl Cancel {
    /// The `[ants]` line ending the cancelled prompt's reply.
    pub(super) fn notice(&self, ant: &UserId) -> String {
        let by = &self.by;
        match &self.why {
            Why::Cancel => format!("[ants] The prompt was cancelled by {by} (ants cancel)."),
            Why::CancelFor(human) => format!("[ants] The prompt was cancelled by {by}, the assistant of {human}."),
            Why::Steer => format!("[ants] {by} interrupted the prompt to steer {ant} with a new message."),
            Why::Restart => {
                format!(
                    "[ants] The prompt was cancelled by {by} to restart {ant}'s harnesses (ants ant restart --now)."
                )
            }
        }
    }
}

impl Cancels {
    /// Make the prompt about to run on `thread` cancellable until the guard drops; the cancels arrive on the receiver.
    pub(super) fn register<'a>(&'a self, thread: &ThreadId) -> (Cancellable<'a>, tokio::sync::mpsc::Receiver<Cancel>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        self.0.locked().insert(thread.clone(), tx);
        (Cancellable { cancels: self, thread: thread.clone() }, rx)
    }

    /// Cancel the prompt running on `thread`, if one is; one cancelled already stays so.
    pub(super) fn cancel(&self, thread: &ThreadId, cancel: Cancel) -> bool {
        use tokio::sync::mpsc::error::TrySendError;
        let cancels = self.0.locked();
        cancels.get(thread).is_some_and(|tx| !matches!(tx.try_send(cancel), Err(TrySendError::Closed(_))))
    }
}

pub(super) struct Cancellable<'a> {
    cancels: &'a Cancels,
    thread: ThreadId,
}

impl Drop for Cancellable<'_> {
    fn drop(&mut self) {
        self.cancels.0.locked().remove(&self.thread);
    }
}

impl Inbox<'_> {
    /// Answer the humans' cancel requests ([`CancelRequest`]) for as long as the daemon serves, cancelling the prompt
    /// running on the thread each one names, and the assistants' for their humans, which may withdraw a message instead.
    pub(super) async fn hear_cancels(&self) {
        let subjects = [subject::cancel_of(self.ant), subject::cancels_by_of(self.ant)];
        let mut requests = match relayed::subscribe_all(self.client, subjects).await {
            Ok(requests) => requests,
            Err(e) => {
                tracing::warn!(error = %e, "serving without ants cancel");
                return std::future::pending().await;
            }
        };
        while let Some(msg) = requests.next().await {
            let request: CancelRequest = match serde_json::from_slice(&msg.payload) {
                Ok(request) => request,
                Err(e) => {
                    tracing::warn!(subject = %msg.subject, error = %e, "ignoring a malformed cancel request");
                    continue;
                }
            };
            if let Err(e) = request.check_subject(&msg.subject) {
                tracing::warn!(error = %e, "ignoring a forged cancel request");
                continue;
            }
            let answer = if request.on_behalf_of.is_some() || request.withdraw.is_some() {
                let Some(human) = self.assisted(&request.by, request.on_behalf_of.as_ref(), "cancel request").await
                else {
                    continue;
                };
                match self.cancel_for(&request, &human).await {
                    Ok(answer) => answer,
                    Err(e) => {
                        tracing::warn!(by = %request.by, error = format!("{e:#}"), "withdrawing a message");
                        continue;
                    }
                }
            } else {
                let cancelled = self
                    .running
                    .cancels
                    .cancel(&request.thread_id, Cancel { by: request.by.clone(), why: Why::Cancel });
                if !cancelled {
                    tracing::info!(thread = %request.thread_id, by = %request.by, "no prompt to cancel");
                }
                request.answer(cancelled)
            };
            let payload = serde_json::to_vec(&answer).expect("a cancel answer serializes");
            if let Err(e) = self.client.publish(answer.subject(), payload.into()).await {
                tracing::warn!(error = %e, "answering a cancel request");
            }
        }
        std::future::pending::<()>().await;
    }

    /// The human `by`, an assistant, makes `what` for, when it claims `on_behalf_of` and the human's grant names it:
    /// a request on a subject naming its sender is an assistant's for its human, or none.
    pub(super) async fn assisted(&self, by: &UserId, on_behalf_of: Option<&UserId>, what: &str) -> Option<UserId> {
        match self.behalf.acting(self.humans, by, on_behalf_of).await {
            Acting::For(human) => Some(human),
            Acting::Forged(human) => {
                tracing::warn!(%by, %human, "ignoring a {what} for a human whose grant does not name its sender");
                None
            }
            Acting::Own => {
                tracing::warn!(%by, "ignoring a {what} naming its sender but no human it acts for");
                None
            }
        }
    }

    /// Cancel the prompt, or withdraw the message, `request` asks for, for `human` its assistant acts for, and tell the
    /// human when done: an assistant's withdrawal repeated is answered as done again, untold.
    async fn cancel_for(&self, request: &CancelRequest, human: &UserId) -> Result<CancelReply> {
        let (by, thread, ant) = (&request.by, &request.thread_id, self.ant);
        let (answer, text) = match request.withdraw {
            Some(message) => {
                let found = self.marks.attempts.withdraw(&message, by, Some(human)).await?;
                match found {
                    None => {
                        tracing::info!(%thread, %by, %human, %message, "withdrew a message for an assistant's human");
                        let text = format!(
                            "[ants] {by}, as the assistant of {human}, withdrew message {message} from the queue of \
                             {ant} on thread {thread} for {human}."
                        );
                        (request.answer(true), Some(text))
                    }
                    Some(MessageMark::Withdrawn { by: was, on_behalf_of: Some(for_human), .. })
                        if &was == by && &for_human == human =>
                    {
                        (request.answer(true), None)
                    }
                    Some(mark) => (request.found(mark), None),
                }
            }
            None => {
                let cancel = Cancel { by: by.clone(), why: Why::CancelFor(human.clone()) };
                let cancelled = self.running.cancels.cancel(thread, cancel);
                tracing::info!(%thread, %by, %human, cancelled, "cancel request of an assistant for its human");
                let text = format!(
                    "[ants] {by}, as the assistant of {human}, cancelled the prompt of {ant} on thread {thread} for \
                     {human}."
                );
                (request.answer(cancelled), cancelled.then_some(text))
            }
        };
        if let Some(text) = text {
            let notice = crate::people::behalf::report(ant.clone(), human, text);
            let id = format!("on-behalf-{}", request.id);
            if let Err(e) = bus::store(self.js, notice.subject(), &notice, &id).await {
                tracing::warn!(%human, error = format!("{e:#}"), "telling the human of its assistant's act");
            }
        }
        Ok(answer)
    }

    /// Restart the ant's harnesses on each admin order to ([`crate::admin::restart`]) for as long as the daemon serves:
    /// the idle ones stop at once, the working ones once their prompt ends, cancelled first on an order to restart now.
    pub(super) async fn hear_restarts(&self) {
        let mut listening = self.restarts.listen(self.ant, Some(self.client));
        loop {
            let request = listening.next().await;
            let restarting = self.pool.restart();
            let cancel = Cancel { by: request.by.clone(), why: Why::Restart };
            let cancelled = if request.now {
                restarting.working.iter().filter(|thread| self.running.cancels.cancel(thread, cancel.clone())).count()
            } else {
                0
            };
            let restarted =
                Restarted { stopped: restarting.stopped, after_prompt: restarting.working.len(), cancelled };
            tracing::info!(
                by = %request.by,
                now = request.now,
                stopped = restarted.stopped,
                after_prompt = restarted.after_prompt,
                cancelled,
                "restarting the harnesses"
            );
            let _ = request.done.send(restarted);
        }
    }

    /// Carry out the orders about the MCP servers of the ant's harness ([`crate::admin::mcp`]) for as long as the daemon
    /// serves, with sessions of their own on a thread of their own, whose MCP servers are those of any thread.
    pub(super) async fn hear_mcp(&self) {
        let probe = ThreadId::generate();
        crate::admin::mcp::serve(self.restarts.listen_mcp(self.ant), || self.pool.launch().for_thread(&probe)).await;
    }

    /// Read a snapshot of the ant for each clone of it asked ([`crate::clone`]) for as long as the daemon serves: once
    /// the ant is paused and handles no message, so the snapshot is of one point in time.
    pub(super) async fn hear_clones(&self) {
        let mut listening = self.restarts.listen_clones(self.ant);
        loop {
            let mut request = listening.next().await;
            let snapshot = async {
                while self.settings.pause.get().is_none() || self.workload.handling() > 0 {
                    tokio::time::sleep(CLONE_WAIT_CHECK).await;
                }
                tracing::info!(new = %request.new, "reading a snapshot of the ant for a clone");
                let source = crate::clone::Source {
                    js: self.js,
                    ant: self.ant,
                    home: self.settings.home,
                    store: self.pool.store(),
                    origins: self.mcp.and_then(crate::mcp::Mcp::origin_store),
                    identity: self.provider.identity,
                    launch: Some(self.pool.launch().clone()),
                    reading: self.reading,
                };
                let root = async |thread: &ThreadId| match self.threads.root(thread, u64::MAX).await {
                    Ok(channel) => channel,
                    Err(e) => {
                        tracing::warn!(%thread, error = format!("{e:#}"), "looking for the channel of a thread");
                        None
                    }
                };
                source.snapshot(&request.new, root).await
            };
            tokio::select! {
                snapshot = snapshot => {
                    let _ = request.done.send(snapshot);
                }
                () = request.done.closed() => {}
            }
        }
    }
}
