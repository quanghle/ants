//! Acting on the human's threads for it: `title_for_human` and `done_for_human` title them and mark them done, and
//! `mark_for_human` and `snooze_for_human` pin, archive and snooze them under keys naming the ant, apart from the
//! human's own marks and snoozes.

use std::time::Duration;

use ants_proto::{ThreadDone, ThreadId, ThreadMarks, ThreadTitle};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{ErrorData, schemars, tool, tool_router};
use serde::Deserialize;

use super::{Change, Messages};
use crate::mcp::Hub;
use crate::mcp::tools::{Tools, text};

/// How long `snooze_for_human` snoozes a thread for by default.
const SNOOZE: Duration = Duration::from_hours(1);

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct TitleForHuman {
    /// A thread of the human's.
    thread: String,
    /// One line of up to 120 characters.
    title: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct DoneForHuman {
    /// A thread of the human's.
    thread: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct MarkForHuman {
    /// A thread of the human's.
    thread: String,
    /// pin, unpin, archive or unarchive.
    mark: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct SnoozeForHuman {
    /// A thread of the human's.
    thread: String,
    /// How long: 30m, 2h, 1d, at most 7d (longer is cut to 7d); 1h by default; `off` ends the snooze.
    #[serde(default)]
    duration: Option<String>,
}

impl Hub {
    /// The thread `thread` names, if the human has a message on it.
    fn human_thread(&self, messages: &Messages, thread: &str) -> Result<ThreadId, String> {
        let thread: ThreadId = thread.trim().parse().map_err(|e| format!("{e}"))?;
        if messages.on(&thread).next().is_none() {
            return Err(format!("{} has no message on thread {thread} in the last 7 days", messages.human));
        }
        Ok(thread)
    }

    async fn title_for_human(&self, args: TitleForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let messages = self.human_messages(&human, store).await?;
        let thread = self.human_thread(&messages, &args.thread)?;
        let mut title = ThreadTitle::new(thread.clone(), &args.title, self.ant.clone())
            .map_err(|e| format!("{e}"))?
            .on_behalf(Some(human.clone()));
        let shown = title.title.clone();
        let sealed = messages.on(&thread).find(|m| m.envelope.sealed.is_some()).map(|m| &m.envelope);
        if let Some(sealed) = sealed {
            let ant = &self.ant;
            let other = [&sealed.from, &sealed.to].into_iter().find(|u| **u != human && *u != ant).cloned();
            let other = other.ok_or_else(|| format!("thread {thread} carries sealed messages with no other party"))?;
            let sealing = self.assist.sealing.get().ok_or_else(|| {
                format!("thread {thread} carries sealed messages, and {human} has not enrolled {ant} as one of its devices, so the title is not sealed; no title is set")
            })?;
            sealing.seal_title(&human, &other, &mut title).await.map_err(|e| format!("{e:#}; no title is set"))?;
        }
        store.put_title(title).await.map_err(|e| format!("{e:#}"))?;
        let what = if sealed.is_some() { "a sealed title".to_owned() } else { format!("{shown:?}") };
        let told = self.tell(&human, format!("titled thread {thread} {what} for {human}.")).await;
        Ok(format!("Titled thread {thread} {shown:?} for {human}.{told}"))
    }

    async fn done_for_human(&self, args: DoneForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let messages = self.human_messages(&human, store).await?;
        let thread = self.human_thread(&messages, &args.thread)?;
        let last = store.last_sequence().await.map_err(|e| format!("{e:#}"))?;
        let done = ThreadDone::new(thread.clone(), self.ant.clone(), last).on_behalf(Some(human.clone()));
        store.put_done(done).await.map_err(|e| format!("{e:#}"))?;
        let told = self.tell(&human, format!("marked thread {thread} done for {human}.")).await;
        Ok(format!("Marked thread {thread} done for {human}; a new message on it brings it back.{told}"))
    }

    async fn mark_for_human(&self, args: MarkForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let messages = self.human_messages(&human, store).await?;
        let thread = self.human_thread(&messages, &args.thread)?;
        let own = store.marks(human.clone()).await.map_err(|e| format!("{e:#}"))?;
        let marked = match own.mark(&thread, 0) {
            Some(ants_proto::Mark::Pinned) => Some("pinned"),
            Some(_) => Some("archived"),
            None => None,
        };
        if let Some(marked) = marked {
            return Err(format!(
                "{human} {marked} thread {thread} itself: only {human} changes its own marks; nothing changed"
            ));
        }
        let (change, (done, did), already): (Change<ThreadMarks>, _, _) = match args.mark.trim() {
            "pin" => {
                let t = thread.clone();
                (Box::new(move |m| m.pin(&t)), ("Pinned", "pinned"), "is pinned already")
            }
            "archive" => {
                let after = store.last_sequence().await.map_err(|e| format!("{e:#}"))?;
                let t = thread.clone();
                (Box::new(move |m| m.archive(&t, after)), ("Archived", "archived"), "is archived already")
            }
            "unpin" => {
                let t = thread.clone();
                (Box::new(move |m| m.unpin(&t)), ("Unpinned", "unpinned"), "is not pinned")
            }
            "unarchive" => {
                let t = thread.clone();
                (Box::new(move |m| m.unarchive(&t)), ("Unarchived", "unarchived"), "is not archived")
            }
            other => return Err(format!("mark: {other:?} is none of pin, unpin, archive and unarchive")),
        };
        let changed = store.change_marks(human.clone(), self.ant.clone(), change);
        if !changed.await.map_err(|e| format!("{e:#}"))? {
            return Ok(format!("Thread {thread} {already} for {human}; nothing changed."));
        }
        let told = self.tell(&human, format!("{did} thread {thread} for {human}.")).await;
        Ok(format!("{done} thread {thread} for {human}.{told}"))
    }

    async fn snooze_for_human(&self, args: SnoozeForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let messages = self.human_messages(&human, store).await?;
        let thread = self.human_thread(&messages, &args.thread)?;
        let now = chrono::Utc::now();
        let own = store.snoozes(human.clone()).await.map_err(|e| format!("{e:#}"))?;
        if let Some(snooze) = own.get(&thread, now) {
            let until = snooze.until.format("%Y-%m-%d %H:%M UTC");
            return Err(format!(
                "{human} snoozed thread {thread} itself until {until}: only {human} changes its own snoozes; nothing \
                 changed"
            ));
        }
        let t = thread.clone();
        if args.duration.as_deref().map(str::trim) == Some("off") {
            let woke = store.change_snoozes(human.clone(), self.ant.clone(), Box::new(move |s| s.wake(&t, now)));
            if !woke.await.map_err(|e| format!("{e:#}"))? {
                return Ok(format!("Thread {thread} is not snoozed for {human}; nothing changed."));
            }
            let told = self.tell(&human, format!("ended the snooze of thread {thread} for {human}.")).await;
            return Ok(format!("Ended the snooze of thread {thread} for {human}.{told}"));
        }
        let duration = match args.duration.as_deref().map(str::trim) {
            None | Some("") => SNOOZE,
            Some(text) => ants_proto::duration::parse(text).map_err(|e| format!("duration: {e}"))?,
        };
        let duration =
            chrono::TimeDelta::from_std(duration).unwrap_or(ants_proto::SNOOZE_MAX).min(ants_proto::SNOOZE_MAX);
        if duration <= chrono::TimeDelta::zero() {
            return Err("duration: a snooze lasts longer than 0".to_owned());
        }
        let until = now + duration;
        let snoozed = store.change_snoozes(
            human.clone(),
            self.ant.clone(),
            Box::new(move |s| {
                s.snooze(&t, now, until);
                true
            }),
        );
        snoozed.await.map_err(|e| format!("{e:#}"))?;
        let until = until.format("%Y-%m-%d %H:%M UTC");
        let told = self.tell(&human, format!("snoozed thread {thread} on the home of {human} until {until}.")).await;
        Ok(format!(
            "Snoozed thread {thread} on the home of {human} until {until}, or until something for {human} arrives on \
             it.{told}"
        ))
    }
}

#[tool_router(router = assist_threads_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "As the assistant of a human, set the title of one of its threads for it; the human is \
        told. On a thread carrying sealed messages the title is sealed for the human's and the other party's devices, \
        which needs the human to have enrolled this ant as one of its devices; the notice never quotes it.")]
    async fn title_for_human(&self, Parameters(args): Parameters<TitleForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.title_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, mark one of its threads done for it, which takes it out \
        of the human's home until a new message comes on it; the human is told.")]
    async fn done_for_human(&self, Parameters(args): Parameters<DoneForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.done_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, pin, unpin, archive or unarchive one of its threads for it \
        (`mark`), for all its devices. The marks set so are this ant's, kept apart from the human's own, and count \
        only while the human's grant names this ant and only on threads the human neither pinned nor archived itself; \
        a thread the human marked itself is refused, and the human marking a thread itself takes over. The human is \
        told.")]
    async fn mark_for_human(&self, Parameters(args): Parameters<MarkForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.mark_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, snooze one of its threads off its home for `duration` (1h \
        by default, at most 7d), or until something for the human arrives on it; `off` ends a snooze. The snoozes set \
        so are this ant's, kept apart from the human's own, and count only while the human's grant names this ant; a \
        thread the human snoozed itself is refused, and the human snoozing or waking a thread itself takes over. The \
        human is told.")]
    async fn snooze_for_human(
        &self,
        Parameters(args): Parameters<SnoozeForHuman>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.snooze_for_human(args).await))
    }
}
