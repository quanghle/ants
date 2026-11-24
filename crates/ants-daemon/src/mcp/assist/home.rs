//! What waits for the human in its home, and acting in it for the human: `human_queue` lists the questions, tool
//! requests, stuck threads, unread messages and mentions and due reminders waiting for it, less the threads done or
//! snoozed ([`ants_proto::ThreadSnoozes::holds`], as home leaves them out), or reads one of its threads; `answer_for_human`, `send_for_human`,
//! `post_for_human`, `notes_for_human`, `react_for_human` and `decide_for_human` answer, send, post, react and decide
//! tool requests for it.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::time::SystemTime;

use ants_client::sealed::State;
use ants_proto::{
    ApprovalRequest, ChannelId, ChannelPost, Envelope, NotesPost, Pick, Reaction, ThreadId, UserId, Uuid, Verdict,
};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{ErrorData, schemars, tool, tool_router};
use serde::Deserialize;

use super::{HumanMessage, HumanStore, LOOKBACK, Messages};
use crate::mcp::Hub;
use crate::mcp::messages::shown;
use crate::mcp::notes::POST_TRIES;
use crate::mcp::tools::{Tools, text};
use crate::people::behalf;
use crate::secrets::sealing::Opening;

/// The most entries of each kind `human_queue` lists, the latest ones.
const SHOWN: usize = 30;

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct HumanQueue {
    /// A thread of the human's to read whole instead of the queue.
    #[serde(default)]
    thread: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct AnswerForHuman {
    /// The id of the question's message, as human_queue lists it.
    message_id: String,
    /// One answer for each question of the message, in their order: the number of the option picked, numbers
    /// joined by commas (`1,3`) where the question takes several, or a typed answer where it takes one.
    answers: Vec<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct SendForHuman {
    /// The ant or human to send to, or a team, reaching its lead.
    to: String,
    text: String,
    /// The thread to send on, such as one human_queue lists; omitted for a new thread.
    #[serde(default)]
    thread: Option<String>,
    /// Send a message to a human unsealed. Without it the message is sealed, which needs the human to have
    /// enrolled this ant as one of its devices.
    #[serde(default)]
    plaintext: bool,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct ReactForHuman {
    /// The id of a message to or from the human.
    message_id: String,
    /// The reaction: 1-64 of lowercase letters, digits and '-', such as `eyes`, `check` or `thumbsup`.
    reaction: String,
    /// Remove the reaction instead of adding it.
    #[serde(default)]
    remove: bool,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct PostForHuman {
    /// A channel the human is a member of, with or without its `#`.
    channel: String,
    text: String,
    /// The thread to post on; none starts a new one.
    #[serde(default)]
    thread: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct NotesForHuman {
    /// The thread whose notes to post to.
    thread: String,
    text: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct DecideForHuman {
    /// The request, as human_queue lists it: `<ant>/<id>`, or its id alone.
    request: String,
    /// allow_once, allow_always or reject.
    verdict: String,
}

impl Hub {
    /// The tool requests waiting for `human`, or its stand-in, to decide, latest last.
    async fn waiting_requests(&self, human: &UserId, store: &dyn HumanStore) -> Result<Vec<ApprovalRequest>, String> {
        let since = SystemTime::now() - LOOKBACK;
        let (requests, decided) =
            store.approvals(since).await.map_err(|e| format!("cannot read the tool requests: {e:#}"))?;
        let now = chrono::Utc::now();
        let default = self.humans.default_human();
        Ok(requests
            .into_iter()
            .filter(|r| r.human.as_ref().unwrap_or(default) == human || r.stand_in.as_ref() == Some(human))
            .filter(|r| !r.is_expired(now) && !decided.contains(&r.id))
            .collect())
    }

    /// The text of `envelope` as the human's devices show it: a sealed one opened while the human enrolled the ant
    /// as one of its devices, else `[sealed]`, or `[forged]` when it does not open as from its sender.
    async fn shown_text(&self, human: &UserId, envelope: &Envelope) -> String {
        if envelope.sealed.is_none() {
            return shown(envelope);
        }
        let Some(sealing) = self.assist.sealing.get() else { return State::Unreadable.placeholder().to_owned() };
        match sealing.open(human, envelope).await {
            Ok(Opening::Opened(body)) => body.text,
            Ok(opening @ (Opening::Unreadable | Opening::Forged(_))) => opening.state().placeholder().to_owned(),
            Err(e) => format!("[sealed: {e:#}]"),
        }
    }

    /// Who sent `envelope`: `bob via pb` for a message an assistant sent for its human under a grant standing.
    fn sender(&self, envelope: &Envelope) -> String {
        match &envelope.on_behalf_of {
            Some(human) if self.humans.assistant(human).as_ref() == Some(&envelope.from) => {
                format!("{human} via {}", envelope.from)
            }
            Some(human) => format!("{} (a forged claim to act for {human})", envelope.from),
            None => envelope.from.to_string(),
        }
    }

    /// One line on `m`, with its text cut to its first line unless `whole`.
    async fn line(&self, human: &UserId, m: &HumanMessage, whole: bool) -> String {
        let e = &m.envelope;
        let shown = self.shown_text(human, e).await;
        let shown = if whole { shown } else { behalf::excerpt(&shown) };
        let mut line = format!("- message {} on thread {}, {} to {}: {shown}", e.id, e.thread_id, self.sender(e), e.to);
        if let Some(asked) = e.asks() {
            let several = asked.questions.len() > 1;
            for (n, q) in (1..).zip(&asked.questions) {
                let options: Vec<String> = q.option_lines().collect();
                let multi = if q.multi_select { "; several may be picked" } else { "" };
                let typed = if q.free_text { "; a typed answer is taken" } else { "" };
                let which =
                    if several { format!("question {n}, {}: ", behalf::excerpt(&q.text)) } else { String::new() };
                let _ = write!(line, " [{which}options: {}{multi}{typed}]", options.join(", "));
            }
        }
        line
    }

    /// The messages of `human` on `thread`, oldest first.
    async fn human_thread_text(&self, human: &UserId, messages: &Messages, thread: &str) -> Result<String, String> {
        let thread: ThreadId = thread.parse().map_err(|e| format!("{e}"))?;
        let on: Vec<&HumanMessage> = messages.on(&thread).collect();
        if on.is_empty() {
            return Err(format!("{human} has no message on thread {thread} in the last 7 days"));
        }
        let mut out = format!("Thread {thread} of {human}, oldest first:");
        for m in on.iter().skip(on.len().saturating_sub(SHOWN)) {
            let _ = write!(out, "\n{}", self.line(human, m, true).await);
        }
        Ok(out)
    }

    /// Write to `out` the heading of `listed`, how many there are between its two parts, then a line on each of the
    /// latest.
    async fn write_listed(
        &self,
        out: &mut String,
        human: &UserId,
        (what, how): (&str, &str),
        listed: &[&HumanMessage],
    ) {
        let _ = write!(out, "\n{what} ({}){how}", listed.len());
        for m in listed.iter().skip(listed.len().saturating_sub(SHOWN)) {
            let _ = write!(out, "\n{}", self.line(human, m, false).await);
        }
    }

    /// The threads of `messages` stuck on `human`, as home tells them: escalated to it with nothing said by a human, or
    /// this ant for it, since; the latest escalation of each, oldest first.
    fn stuck<'a>(&self, human: &UserId, messages: &'a Messages) -> Vec<&'a HumanMessage> {
        let mut by_human: HashMap<&ThreadId, u64> = HashMap::new();
        for m in &messages.stored {
            if self.humans.is_human(&m.envelope.from) || messages.by_human(&m.envelope) {
                by_human.insert(&m.envelope.thread_id, m.seq);
            }
        }
        let mut stuck: HashMap<&ThreadId, &HumanMessage> = HashMap::new();
        for m in messages.stored.iter().filter(|m| m.envelope.to == *human && m.envelope.escalation.is_some()) {
            if by_human.get(&m.envelope.thread_id).is_none_or(|said| *said < m.seq) {
                stuck.insert(&m.envelope.thread_id, m);
            }
        }
        let mut stuck: Vec<&HumanMessage> = stuck.into_values().collect();
        stuck.sort_by_key(|m| m.seq);
        stuck
    }

    /// What waits for the human in its home, or the messages of `thread`.
    async fn human_queue(&self, thread: Option<&str>) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let messages = self.human_messages(&human, store).await?;
        if let Some(thread) = thread.map(str::trim).filter(|t| !t.is_empty()) {
            return self.human_thread_text(&human, &messages, thread).await;
        }

        let now = chrono::Utc::now();
        let marker = store.read_marker(human.clone()).await.map_err(|e| format!("{e:#}"))?;
        let (reminders, assisted) =
            store.reminders(human.clone(), self.ant.clone()).await.map_err(|e| format!("{e:#}"))?;
        let snoozed = reminders.snoozed.clone().with_assisted(&assisted, now);
        let questions = messages.open_questions();
        let asked: HashSet<Uuid> = questions.iter().map(|q| q.envelope.id).collect();
        let mut last: HashMap<&ThreadId, u64> = HashMap::new();
        for m in &messages.stored {
            last.insert(&m.envelope.thread_id, m.seq);
        }
        let stuck = self.stuck(&human, &messages);
        let (mentions, unread): (Vec<&HumanMessage>, Vec<&HumanMessage>) = messages
            .stored
            .iter()
            .filter(|m| m.envelope.to == human && m.envelope.from != self.ant && !asked.contains(&m.envelope.id))
            .filter(|m| m.envelope.escalation.is_none() && !marker.is_read(m.seq))
            .partition(|m| m.envelope.mention.is_some());
        let mut done = HashSet::new();
        let listed = questions.iter().chain(&stuck).chain(&unread).chain(&mentions);
        for thread in listed.map(|m| &m.envelope.thread_id).collect::<HashSet<_>>() {
            let mark = store.done(thread.clone()).await.map_err(|e| format!("{e:#}"))?;
            if mark.is_some_and(|d| d.holds(last[thread])) {
                done.insert(thread.clone());
            }
        }
        let requests = self.waiting_requests(&human, store).await?;
        let due = reminders.due(now);
        // When the latest item for the human on each thread arrived, which ends a snooze set before it.
        let mut latest: HashMap<ThreadId, chrono::DateTime<chrono::Utc>> = HashMap::new();
        let mut arrived = |thread: &ThreadId, at| {
            let known = latest.entry(thread.clone()).or_insert(at);
            *known = (*known).max(at);
        };
        for m in questions.iter().chain(&stuck).chain(&unread).chain(&mentions) {
            arrived(&m.envelope.thread_id, m.envelope.sent_at);
        }
        for r in &requests {
            arrived(&r.thread_id, r.requested_at);
        }
        for (thread, reminder) in &due {
            arrived(thread, reminder.due);
        }
        let held = |thread: &ThreadId| snoozed.holds(thread, now, latest.get(thread).copied());
        let open = |m: &&HumanMessage| !done.contains(&m.envelope.thread_id) && !held(&m.envelope.thread_id);
        let [questions, stuck, unread, mentions] =
            [questions, stuck, unread, mentions].map(|of| of.into_iter().filter(open).collect::<Vec<_>>());
        let requests: Vec<ApprovalRequest> = requests.into_iter().filter(|r| !held(&r.thread_id)).collect();
        let due: Vec<_> = due.into_iter().filter(|(thread, _)| !held(thread)).collect();
        let held: HashSet<Uuid> = messages
            .stored
            .iter()
            .filter(|m| m.envelope.from == self.ant)
            .filter_map(|m| m.envelope.approval.as_ref().map(|d| d.request_id))
            .collect();

        if [&questions, &stuck, &unread, &mentions].iter().all(|of| of.is_empty())
            && requests.is_empty()
            && due.is_empty()
        {
            return Ok(format!("Nothing waits for {human}."));
        }
        let mut out = format!("What waits for {human}, latest last:");
        let asked = (&*format!("Questions to {human}"), ", answered with answer_for_human:");
        self.write_listed(&mut out, &human, asked, &questions).await;
        let _ = write!(out, "\nTool requests ({}), decided with decide_for_human:", requests.len());
        for r in requests.iter().skip(requests.len().saturating_sub(SHOWN)) {
            let options: Vec<&str> = r.options.iter().map(|v| v.as_str()).collect();
            let held = if held.contains(&r.id) { "; decided by this ant, held until confirmed" } else { "" };
            let _ = write!(
                out,
                "\n- {}/{} on thread {}: {} {} [options: {}; expires {}{held}]",
                r.ant,
                r.id,
                r.thread_id,
                r.tool.kind,
                behalf::excerpt(&r.tool.title),
                options.join(", "),
                r.expires_at.format("%Y-%m-%d %H:%M UTC"),
            );
        }
        let escalated = format!(", escalated to {human} with no human's word since:");
        self.write_listed(&mut out, &human, ("Stuck threads", &escalated), &stuck).await;
        self.write_listed(&mut out, &human, (&format!("Unread messages to {human}"), ":"), &unread).await;
        self.write_listed(&mut out, &human, (&format!("Unread mentions of {human}"), ":"), &mentions).await;
        let _ = write!(out, "\nDue reminders ({}):", due.len());
        for (thread, reminder) in due.iter().skip(due.len().saturating_sub(SHOWN)) {
            let due = reminder.due.format("%Y-%m-%d %H:%M UTC");
            let note = if reminder.note.is_empty() { String::new() } else { format!(": {}", reminder.note) };
            let _ = write!(out, "\n- thread {thread}, due {due}{note}");
        }
        Ok(out)
    }

    async fn answer_for_human(&self, args: AnswerForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let id: Uuid = args.message_id.trim().parse().map_err(|e| format!("message_id: {e}"))?;
        let messages = self.human_messages(&human, store).await?;
        let question =
            messages.open_questions().into_iter().find(|q| q.envelope.id == id).ok_or_else(|| {
                format!("no question to {human} waits as message {id}; human_queue lists those that do")
            })?;
        let question = &question.envelope;
        let picks = args.answers.iter().map(|a| Pick::parse(a)).collect();
        let answer = Envelope::answering(question, self.ant.clone(), picks)
            .map_err(|e| format!("{e}"))?
            .on_behalf(Some(human.clone()))
            .with_owner(question.owner.clone());
        self.send(&answer).await?;
        Ok(format!(
            "Answered {}'s question {id} for {human} on thread {} with {:?}; its daemon tells {human}.",
            question.from, question.thread_id, answer.text
        ))
    }

    async fn send_for_human(&self, args: SendForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let (to, team) = self.addressed(&args.to).await?;
        if to == human || to == self.ant {
            return Err(format!("{to} is {human} or this ant; send to someone else"));
        }
        let mut envelope = match args.thread.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
            Some(thread) => {
                let thread = thread.parse().map_err(|e| format!("{e}"))?;
                Envelope::on_thread(thread, self.ant.clone(), to.clone(), args.text)
            }
            None => Envelope::new(self.ant.clone(), to.clone(), args.text),
        }
        .on_behalf(Some(human.clone()))
        .with_owner(Some(human.clone()));
        envelope.team = team;
        let excerpt = behalf::excerpt(&envelope.text);
        if !self.humans.is_human(&to) {
            // Before the message, so the ant's own messages on the thread name the human as their owner.
            let thread = envelope.thread_id.clone();
            if let Err(e) = store.record_owner(thread.clone(), to.clone(), human.clone(), self.ant.clone()).await {
                tracing::warn!(%thread, %to, error = format!("{e:#}"), "recording the thread's owner for the human");
            }
            self.send(&envelope).await?;
            let through = envelope.team.as_ref().map_or_default(|team| format!(", the lead of team {team},"));
            return Ok(format!(
                "Sent {to}{through} message {} on thread {} for {human}; its daemon tells {human}.",
                envelope.id, envelope.thread_id
            ));
        }
        let sealed = !args.plaintext;
        if sealed {
            let sealing = self.assist.sealing.get().ok_or_else(|| {
                format!("{human} has not enrolled {} as one of its devices, so nothing is sealed; nothing sent: set plaintext to send it unsealed", self.ant)
            })?;
            sealing
                .seal(&human, &mut envelope)
                .await
                .map_err(|e| format!("{e:#}; nothing sent: set plaintext to send it unsealed"))?;
        }
        self.send(&envelope).await?;
        let what = if sealed { "a sealed message".to_owned() } else { format!("a message: {excerpt}") };
        let told = self.tell(&human, format!("sent {to} {what} for {human} on thread {}.", envelope.thread_id)).await;
        Ok(format!("Sent {to} message {} on thread {} for {human}.{told}", envelope.id, envelope.thread_id))
    }

    /// Post in a channel the human is a member of, starting a thread or on `thread`, as the human posts there: with a
    /// fresh turn budget.
    async fn post_for_human(&self, args: PostForHuman) -> Result<String, String> {
        let (human, _) = self.acting_for()?;
        let channel: ChannelId = args.channel.trim().trim_start_matches('#').parse().map_err(|e| format!("{e}"))?;
        let access = self.channels.get().ok_or("channels are not served here; post_for_human is unavailable")?;
        let entry = (access.entry)(channel.clone())
            .await
            .map_err(|e| format!("cannot read #{channel}: {e:#}"))?
            .ok_or_else(|| format!("there is no channel #{channel}"))?;
        if !entry.is_member(&human) {
            return Err(format!("{human} is no member of #{channel}; nothing posted"));
        }
        let post = match args.thread.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
            Some(thread) => {
                let thread: ThreadId = thread.parse().map_err(|e| format!("{e}"))?;
                let root = thread.as_str().parse::<Uuid>().ok();
                let mut post = ChannelPost::on_thread(channel.clone(), thread, self.ant.clone(), args.text);
                post.in_reply_to = root;
                post
            }
            None => ChannelPost::new(channel.clone(), self.ant.clone(), args.text),
        }
        .with_owner(Some(human.clone()))
        .on_behalf(Some(human.clone()));
        let post = self.through_teams(post, &entry).await?;
        let excerpt = behalf::excerpt(&post.text);
        (access.post)(post.clone(), None).await.map_err(|e| format!("not posted: {e:#}"))?;
        let thread = &post.thread_id;
        let told = self.tell(&human, format!("posted to #{channel} on thread {thread} for {human}: {excerpt}")).await;
        Ok(format!("Posted to #{channel} on thread {thread} for {human}.{told}"))
    }

    /// Post to the notes of `thread` as the human posts there: with a fresh turn budget.
    async fn notes_for_human(&self, args: NotesForHuman) -> Result<String, String> {
        let (human, _) = self.acting_for()?;
        let thread: ThreadId = args.thread.trim().parse().map_err(|e| format!("{e}"))?;
        let post = NotesPost::new(thread.clone(), self.ant.clone(), args.text).on_behalf(Some(human.clone()));
        let mut posted = false;
        for _ in 0..POST_TRIES {
            let notes = self.notes.read(&thread).await.map_err(|e| format!("cannot read the thread notes: {e:#}"))?;
            if self.notes.post(&post, notes.last_seq).await.map_err(|e| format!("not posted: {e:#}"))? {
                posted = true;
                break;
            }
        }
        if !posted {
            return Err(format!("the notes of thread {thread} kept changing; nothing posted"));
        }
        let excerpt = behalf::excerpt(&post.text);
        let told = self.tell(&human, format!("posted to the notes of thread {thread} for {human}: {excerpt}")).await;
        Ok(format!("Posted to the notes of thread {thread} for {human} ({} turns left).{told}", post.turn_budget))
    }

    async fn react_for_human(&self, args: ReactForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let reaction: Reaction = args.reaction.trim().parse().map_err(|e| format!("{e}"))?;
        let id: Uuid = args.message_id.trim().parse().map_err(|e| format!("message_id: {e}"))?;
        let messages = self.human_messages(&human, store).await?;
        let message = messages.find(id).ok_or_else(|| format!("{human} has no message {id} in the last 7 days"))?;
        let thread = message.envelope.thread_id.clone();
        let changed = store
            .react(self.ant.clone(), id, reaction.clone(), args.remove, human.clone())
            .await
            .map_err(|e| format!("{e:#}"))?;
        let (did, result) = match (args.remove, changed) {
            (true, false) => return Ok(format!("No reaction {reaction} to message {id} to remove.")),
            (false, false) => return Ok(format!("Already reacted {reaction} to message {id} for {human}.")),
            (true, true) => ("removed its reaction", format!("Removed the reaction {reaction} to message {id}.")),
            (false, true) => ("reacted", format!("Reacted {reaction} to message {id} for {human}.")),
        };
        let told = self.tell(&human, format!("{did} {reaction} to message {id} on thread {thread} for {human}.")).await;
        Ok(format!("{result}{told}"))
    }

    async fn decide_for_human(&self, args: DecideForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let verdict: Verdict = serde_json::from_value(serde_json::Value::String(args.verdict.trim().replace('-', "_")))
            .map_err(|_| format!("unknown verdict {}: allow_once, allow_always or reject", args.verdict))?;
        let named = args.request.trim();
        let (ant, id) = match named.split_once('/') {
            Some((ant, id)) => (Some(ant.parse::<UserId>().map_err(|e| format!("{e}"))?), id),
            None => (None, named),
        };
        let id: Uuid = id.parse().map_err(|e| format!("request: {e}"))?;
        let requests = self.waiting_requests(&human, store).await?;
        let request = requests
            .iter()
            .find(|r| r.id == id && ant.as_ref().is_none_or(|a| *a == r.ant))
            .ok_or_else(|| format!("no tool request {named} waits for {human}; human_queue lists those that do"))?;
        if verdict != Verdict::Reject && !request.options.contains(&verdict) {
            return Err(format!("{} is not offered for {}/{id}; reject always is", verdict.as_str(), request.ant));
        }
        let decision = request.decide(verdict).on_behalf(Some(human.clone()));
        let envelope =
            Envelope::decision(request.thread_id.clone(), self.ant.clone(), decision).with_owner(Some(human.clone()));
        self.send(&envelope).await?;
        let (ant, verdict) = (&request.ant, verdict.as_str());
        Ok(format!(
            "Sent {verdict} on tool request {ant}/{id} for {human}. It takes effect only once {human} confirms it (`o` \
             on the home of ants chat lists it, or `ants assistant confirm {ant}/{id}`) or a pre-allow rule of {human} \
             covers the call; until then it is held, and {ant}'s daemon tells {human}."
        ))
    }
}

#[tool_router(router = assist_home_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "As the assistant of a human, read what waits for it in its home in ants chat: the \
        questions ants asked it not yet answered, the tool requests waiting for its decision, the threads stuck \
        on it, its unread messages and mentions, each with its message id and thread, from the last 7 days, and its \
        due reminders; threads it or this ant snoozed stay out until something new arrives on them. With \
        `thread`, read that thread's messages to and from the human instead. A sealed message shows as [sealed] unless the human enrolled this \
        ant as one of its devices.")]
    async fn human_queue(&self, Parameters(args): Parameters<HumanQueue>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.human_queue(args.thread.as_deref()).await))
    }

    #[tool(description = "As the assistant of a human, answer a message of questions an ant asked it \
        (human_queue lists them) for it, all at once: `answers` has one answer for each question, in their order, \
        each the number of the option picked, numbers joined by commas where the question takes several, or a typed \
        answer where it takes one. The answer goes as this ant's, for the human, and the asking ant's daemon tells \
        the human.")]
    async fn answer_for_human(
        &self,
        Parameters(args): Parameters<AnswerForHuman>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.answer_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, send a message for it to an ant or another human, on \
        `thread` or a new one, as this ant's, for the human; the human is told. A team in `to` reaches its lead, \
        as with send_message. A message to a human is sealed for \
        the recipient's and the human's devices, which needs the human to have enrolled this ant as one of its \
        devices; `plaintext` sends it unsealed. Attachments are not sent for a human.")]
    async fn send_for_human(&self, Parameters(args): Parameters<SendForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.send_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, post for it in a channel it is a member of, on `thread` or \
        starting a new one, as this ant's, for the human, with the fresh turn budget a human's post has; an @mention \
        wakes that member as the human's would. The human is told.")]
    async fn post_for_human(&self, Parameters(args): Parameters<PostForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.post_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, post for it to the notes of a thread, as this ant's, for the \
        human, with the fresh turn budget a human's post gives the notes. The human is told.")]
    async fn notes_for_human(&self, Parameters(args): Parameters<NotesForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.notes_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, add or remove a reaction for it to a message to or from \
        it, shown as the human's via this ant; the human is told.")]
    async fn react_for_human(&self, Parameters(args): Parameters<ReactForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.react_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, decide a tool request waiting for it (human_queue lists \
        them): `verdict` is allow_once, allow_always or reject. The decision is held by the requesting ant's daemon \
        until the human confirms it or a pre-allow rule of the human covers the call; the human is told and may \
        still decide the request itself.")]
    async fn decide_for_human(
        &self,
        Parameters(args): Parameters<DecideForHuman>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.decide_for_human(args).await))
    }
}
