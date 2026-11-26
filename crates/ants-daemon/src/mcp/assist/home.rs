//! What waits for the human in its home, and acting in it for the human: `human_queue` lists what its home lists,
//! as ants-client derives it ([`ants_client::home`]) from the human's messages, the dead letters on its threads, its
//! read marker, done marks, reminders and snoozes and the tool requests waiting for it ([`Queue`]), or reads one of
//! its threads; `answer_for_human`, `send_for_human`, `post_for_human`, `notes_for_human`, `react_for_human` and
//! `decide_for_human` answer, send, post, react and decide tool requests for it.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::time::SystemTime;

use ants_client::facts::Facts;
use ants_client::home::{self, Item, Source, Why};
use ants_client::parser::{Lookups, Parser};
use ants_client::sealed::State;
use ants_proto::{
    Acting, ApprovalRequest, ChannelId, ChannelPost, Envelope, NotesPost, Pick, Reaction, ReadMarker, Reminders,
    ThreadId, ThreadSnoozes, UserId, Uuid, Verdict,
};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{ErrorData, schemars, tool, tool_router};
use serde::Deserialize;

use super::{HumanMessage, HumanStore, LOOKBACK, Messages, StoredLetter};
use crate::mcp::Hub;
use crate::mcp::messages::shown;
use crate::mcp::notes::POST_TRIES;
use crate::mcp::tools::{Tools, text};
use crate::people::behalf;
use crate::people::humans::Registry;
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

/// What the human's home derives from, as its assistant reads it for it.
struct Queue<'a> {
    humans: &'a Registry,
    human: UserId,
    facts: Facts,
    /// The threads of the human's messages.
    threads: Vec<ThreadId>,
    marker: ReadMarker,
    /// The threads with something waiting whose done mark holds.
    done: HashSet<ThreadId>,
    /// The tool requests waiting for the human, or its stand-in, to decide.
    requests: Vec<ApprovalRequest>,
    reminders: Reminders,
    /// The human's own snoozes, and those this ant set for it.
    snoozes: ThreadSnoozes,
}

impl Source for Queue<'_> {
    type Settled = ();

    fn me(&self) -> &UserId {
        &self.human
    }

    fn facts(&self) -> &Facts {
        &self.facts
    }

    fn threads(&self) -> impl Iterator<Item = &ThreadId> {
        self.threads.iter()
    }

    fn is_human(&self, user: &UserId) -> bool {
        self.humans.is_human(user)
    }

    fn unread(&self, seq: u64) -> bool {
        !self.marker.is_read(seq)
    }

    fn done(&self, thread: &ThreadId) -> bool {
        self.done.contains(thread)
    }

    fn requests(&self, _: chrono::DateTime<chrono::Utc>) -> Vec<(ApprovalRequest, Option<()>)> {
        self.requests.iter().map(|r| (r.clone(), None)).collect()
    }

    fn reminders(&self) -> &Reminders {
        &self.reminders
    }

    fn snoozes(&self) -> &ThreadSnoozes {
        &self.snoozes
    }
}

/// Honours a claim to act for a human made by the assistant its grant standing designates.
struct Grants<'a>(&'a Registry);

impl Lookups for Grants<'_> {
    fn acting(&self, from: &UserId, claim: Option<&UserId>) -> Acting {
        Acting::of(claim, |human| self.0.assistant(human).as_ref() == Some(from))
    }
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

    /// What the human's home derives from, of `messages` and every ant's dead letters `letters`: the facts of the
    /// messages and of the dead letters on the human's threads, read in stream order, and those threads, first
    /// messaged first.
    fn home_facts(&self, messages: &Messages, letters: Vec<StoredLetter>) -> (Facts, Vec<ThreadId>) {
        let mut threads: Vec<ThreadId> = Vec::new();
        for m in &messages.stored {
            if !threads.contains(&m.envelope.thread_id) {
                threads.push(m.envelope.thread_id.clone());
            }
        }
        let lookups = Grants(&self.humans);
        let mut parser = Parser::default();
        let mut read: Vec<(u64, Result<&HumanMessage, StoredLetter>)> =
            messages.stored.iter().map(|m| (m.seq, Ok(m))).collect();
        read.extend(letters.into_iter().map(|l| (l.seq, Err(l))));
        read.sort_by_key(|(seq, _)| *seq);
        let mut facts = Facts::default();
        for (_, stored) in read {
            let entry = match stored {
                Ok(m) => {
                    let e = m.envelope.clone();
                    Some(parser.envelope_with(&lookups, m.seq, m.stored_at, &e.subject(), e))
                }
                Err(l) => parser.entry_with(&lookups, l.seq, l.stored_at, &l.subject, &l.payload),
            };
            if let Some(entry) = entry.filter(|e| threads.contains(&e.thread_id)) {
                facts.add(&entry, &messages.human);
            }
        }
        (facts, threads)
    }

    /// What the home of `human`, whose messages are `messages`, derives from at `now`, read from `store`.
    async fn home_queue(
        &self,
        human: &UserId,
        store: &dyn HumanStore,
        messages: &Messages,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Queue<'_>, String> {
        let marker = store.read_marker(human.clone()).await.map_err(|e| format!("{e:#}"))?;
        let (reminders, assisted) =
            store.reminders(human.clone(), self.ant.clone()).await.map_err(|e| format!("{e:#}"))?;
        let snoozes = reminders.snoozed.clone().with_assisted(&assisted, now);
        let letters = store
            .dead_letters(SystemTime::now() - LOOKBACK)
            .await
            .map_err(|e| format!("cannot read the dead letters: {e:#}"))?;
        let (facts, threads) = self.home_facts(messages, letters);
        let requests = self.waiting_requests(human, store).await?;
        let mut queue = Queue {
            humans: &self.humans,
            human: human.clone(),
            facts,
            threads,
            marker,
            done: HashSet::new(),
            requests,
            reminders,
            snoozes,
        };
        // A done mark keeps a thread's messages off the home, not its tool requests or reminder.
        let marked = home::reasons(&queue, now).into_iter().filter(|i| !matches!(i.why, Why::Approval | Why::Reminder));
        for thread in marked.map(|i| i.thread).collect::<HashSet<_>>() {
            let last = queue.facts.get(&thread).map_or(0, |f| f.last_reopening);
            let mark = store.done(thread.clone()).await.map_err(|e| format!("{e:#}"))?;
            if mark.is_some_and(|d| d.holds(last)) {
                queue.done.insert(thread);
            }
        }
        Ok(queue)
    }

    /// What waits for the human in its home, as its home lists it ([`home::items`]), or the messages of `thread`.
    async fn human_queue(&self, thread: Option<&str>) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let messages = self.human_messages(&human, store).await?;
        if let Some(thread) = thread.map(str::trim).filter(|t| !t.is_empty()) {
            return self.human_thread_text(&human, &messages, thread).await;
        }
        let now = chrono::Utc::now();
        let queue = self.home_queue(&human, store, &messages, now).await?;
        let items = home::items(&queue, now);
        if items.is_empty() {
            return Ok(format!("Nothing waits for {human}."));
        }
        let held: HashSet<Uuid> = messages
            .stored
            .iter()
            .filter(|m| m.envelope.from == self.ant)
            .filter_map(|m| m.envelope.approval.as_ref().map(|d| d.request_id))
            .collect();

        let mut out = format!("What waits for {human}, as its home lists it, latest last:");
        for why in Why::ALL {
            let of: Vec<&Item<()>> = items.iter().filter(|i| i.why == why).collect();
            if why == Why::Approval {
                let requests: Vec<&ApprovalRequest> = of.iter().flat_map(|i| &i.requests).collect();
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
                continue;
            }
            let heading = match why {
                Why::Question => format!("Questions to {human} ({}), answered with answer_for_human:", of.len()),
                Why::Stuck => format!(
                    "Stuck threads ({}), escalated to {human}, or held by a message given up on or an ant unable to \
                     serve, with no human's word since:",
                    of.len()
                ),
                Why::Message => format!("Unread messages to {human} from other humans ({}):", of.len()),
                Why::Mention => format!("Unread mentions of {human} ({}):", of.len()),
                Why::Reminder => format!("Due reminders ({}):", of.len()),
                Why::Finished => {
                    format!("Finished threads ({}), an ant answered {human} on a thread it owns:", of.len())
                }
                Why::Approval => unreachable!("listed above"),
            };
            let _ = write!(out, "\n{heading}");
            for item in of.iter().skip(of.len().saturating_sub(SHOWN)) {
                let mut line = match (why, item.seq.and_then(|seq| messages.at(seq))) {
                    (Why::Reminder, _) => {
                        let reminder = queue.reminders.remind.get(&item.thread);
                        let due = item.at.format("%Y-%m-%d %H:%M UTC");
                        let note = reminder.filter(|r| !r.note.is_empty()).map_or_default(|r| format!(": {}", r.note));
                        format!("- thread {}, due {due}{note}", item.thread)
                    }
                    (_, Some(m)) => self.line(&human, m, false).await,
                    (_, None) => format!("- on thread {}: {}", item.thread, behalf::excerpt(&item.text)),
                };
                if item.count > 1 {
                    let _ = write!(line, " ({} unread)", item.count);
                }
                if !item.also.is_empty() {
                    let badges: Vec<String> = item.also.iter().map(|(why, n)| why.badge(*n)).collect();
                    let _ = write!(line, " (also: {})", badges.join(", "));
                }
                let _ = write!(out, "\n{line}");
            }
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
    #[tool(description = "As the assistant of a human, read what waits for it in its home in ants chat, as its \
        home lists it: the tool requests waiting for its decision, the questions ants asked it not yet answered, the \
        threads stuck on it, its unread messages from other humans and mentions, its due reminders and the threads \
        it owns an ant answered it on, each thread once under its most pressing reason, with message ids and \
        threads, from the last 7 days; threads it or this ant snoozed stay out until something new arrives on them, \
        and threads marked done keep only their tool requests and reminders. With \
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
