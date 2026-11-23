//! The text of a prompt: the message, or a channel post with its thread, and the notes the ant's `[ants]` lines
//! add to it.

use std::fmt::Write as _;

use ants_proto::{Envelope, Humans, SummaryOf, SummaryRequest, UserId};

use crate::channel::{self, Context};
use crate::escalation::{self};
use crate::provider::brief::Briefing;

use super::super::steering::{join_note, steer_note};
use super::super::{Inbox, Incoming, Posted, Steered};

impl Inbox<'_> {
    /// The prompt for `incoming`, given what was read of a post's thread; `own` when it is another ant's reply to the
    /// ant's own message ([`Inbox::replies_to_own`]), true if the thread has no turns left.
    pub(super) async fn prompt_for(
        &self,
        incoming: &Incoming,
        read: Option<&Context>,
        humans: &Humans,
        cut_off: bool,
        own: Option<bool>,
    ) -> String {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        let late = match self.mcp {
            Some(mcp) => mcp.late_on(thread).await,
            None => 0,
        };
        let text = match (&incoming.post, read) {
            (Some(posted), Some(read)) => self.channel_prompt(posted, read, humans, own.is_some()).await,
            _ => match (&envelope.summary, escalation::group_note(self.ant, envelope)) {
                (Some(summary), _) => format!("{}\n\n{}", summary_note(summary), envelope.text),
                (None, Some(note)) => format!("{note}\n\n{}", envelope.text),
                (None, None) => {
                    let text = channel::quoted(envelope.quote.as_ref(), &envelope.text);
                    let note = if self.asks_review(incoming) {
                        (incoming.steered != Steered::Joining).then_some(REVIEW_NOTE)
                    } else if self.settings.builtin == Some(ants_proto::BuiltinAnt::Messenger)
                        && incoming.post.is_none()
                        && incoming.resume.is_none()
                        && !envelope.webhook
                        && envelope.escalation.is_none()
                        && envelope.provider.is_none()
                        && envelope.trail_step.is_none()
                        && own.is_none()
                        // Such as the invitation to rate a trail run it conducted, not a request to route.
                        && envelope.from != *self.ant
                    {
                        Some(DISPATCH_NOTE)
                    } else {
                        None
                    };
                    match note {
                        Some(note) => format!("{note}\n\n{text}"),
                        None => text,
                    }
                }
            },
        };
        let text = match &incoming.resume {
            Some(channel) => {
                let left = envelope.turn_budget.saturating_sub(1);
                format!("{text}\n\n{}", channel::resume_note(channel, thread, left))
            }
            None => text,
        };
        let text = match self.answer_line(envelope, humans).await {
            Some(line) => format!("{line}\n\n{text}"),
            None if let Some(spent) = own => {
                let note = if incoming.late {
                    late_reply_note(&envelope.from, spent)
                } else {
                    own_reply_note(&envelope.from, incoming.post.is_some(), spent)
                };
                format!("{note}\n\n{text}")
            }
            None => text,
        };
        let text = prompt_text(&text, cut_off, late);
        match incoming.steered {
            Steered::Interrupted => format!("{}\n\n{text}", steer_note(&envelope.from)),
            Steered::Joining => format!("{}\n\n{text}", join_note(&envelope.from)),
            Steered::No | Steered::Ahead => text,
        }
    }

    /// The `[ants]` line telling that `envelope`, a human's message or one an assistant sent for its human, verified
    /// on arrival, answers a question the ant asked on the thread ([`crate::stream::questions::answer_line`]); none for any
    /// other message, which is prompted as an ordinary one.
    pub(super) async fn answer_line(&self, envelope: &Envelope, humans: &Humans) -> Option<String> {
        let human = envelope.on_behalf_of.is_some() || envelope.sender_role(humans).is_human();
        if envelope.answer.is_none() || !human {
            return None;
        }
        match crate::stream::questions::asked(self.js, self.ant, self.reading, envelope).await {
            Ok(question) => crate::stream::questions::answer_line(envelope, question.as_ref()?),
            Err(e) => {
                let thread = &envelope.thread_id;
                tracing::warn!(%thread, error = format!("{e:#}"), "reading back the question a message answers");
                None
            }
        }
    }

    /// What asks the ant's provider for its brief, should the prompt for `incoming` be the first of a session: none
    /// without a provider taking brief requests. Asked as the provider's tools are called ([`crate::mcp::Mcp::caller`],
    /// the thread's origin noted as the turn began).
    pub(super) async fn briefing(&self, incoming: &Incoming) -> Option<Briefing> {
        let mcp = self.mcp?;
        let provider = mcp.provider()?.clone();
        let thread = &incoming.envelope.thread_id;
        let origin = mcp.origin(thread)?;
        Briefing::new(provider, mcp.caller().await, thread.clone(), origin)
    }

    /// What the prompt for `incoming`, on a channel's thread, adds should it be the first of a session: the channel's
    /// description, as the post's entry said, or as stored now for the escalation leader's answer.
    pub(super) async fn channel_note(&self, incoming: &Incoming) -> Option<String> {
        let (channel, description) = match (&incoming.post, &incoming.resume) {
            (Some(posted), _) => (&posted.post.channel, posted.description.clone()),
            (None, Some(channel)) => (channel, self.stored_description(channel).await),
            (None, None) => return None,
        };
        description.map(|description| channel::description_note(channel, &description))
    }

    pub(super) async fn stored_description(&self, channel: &ants_proto::ChannelId) -> Option<String> {
        let read = async {
            let kv = self.js.get_key_value(ants_proto::CHANNELS_BUCKET).await?;
            anyhow::Ok(match channel::load(&kv, channel).await? {
                channel::Stored::Entry(entry, _) => entry.description,
                _ => None,
            })
        };
        read.await.unwrap_or_else(|e| {
            tracing::warn!(%channel, error = format!("{e:#}"), "reading the description of a channel");
            None
        })
    }

    /// The prompt for a channel post, with what was read of its thread: the posts not shown before, and the turns left,
    /// unless the answer is not posted, as for a post answering one of the ant's `own`.
    pub(super) async fn channel_prompt(&self, posted: &Posted, read: &Context, humans: &Humans, own: bool) -> String {
        let post = &posted.post;
        let left = post.turns_left(&read.later, humans).saturating_sub(1);
        let mut text = match &posted.triage {
            None => channel::prompt(self.ant, post, read, (!own).then_some(left)),
            Some(entry) => {
                let colony = (self.colony)().await.unwrap_or_else(|e| {
                    tracing::warn!(error = format!("{e:#}"), "reading the colony; members show as offline");
                    Vec::new()
                });
                channel::triage_prompt(post, entry, &colony, humans, read, left)
            }
        };
        if posted.owned {
            let _ = write!(text, "\n\n{}", channel::owner_note(&post.channel));
        }
        text
    }
}

/// Put before the text of a message whose earlier attempt started and stored no reply.
const CUT_OFF: &str = "[ants] An earlier attempt at this message was cut off before its reply was stored, so tools it \
ran may already have taken effect. The session history may show what ran. Check the current state before repeating \
any action.";

const DISPATCH_NOTE: &str = "[ants] Route this request: pick the capability it needs from list_ants, route it there \
    and reply with the answer. Split a request needing several capabilities into one route per capability. When no \
    ant has the needed capability, say so instead of answering directly. For a human's request starting a thread, \
    suggest a short title with suggest_thread_title.";

const REVIEW_NOTE: &str = "[ants] Review the work and identity in the packet from the ant under review. Check the \
    evidence and answer with one reply holding findings in the JSON block the packet asks for: cite the evidence and \
    suggest an action for each finding, or return an empty list if none is warranted. Judge work against identity \
    and identity against work. For a persona, self_model or relationship finding, cite the ant's own threads and \
    build on its existing identity. Check the earlier findings against the new work; for a finding that did not \
    hold, return a follow_up naming its id in \"follows\". For a multi-step approach repeated across the threads, \
    return a trail finding outlining a saved trail that takes the specifics as args. Only read and suggest: change \
    nothing and message no one. Read code only in the review checkouts of the commits named in the packet.";

/// The `[ants]` line that turns a human's message carrying `summary` into a request for a summary of the entries its
/// text carries.
fn summary_note(summary: &SummaryRequest) -> String {
    let what = match &summary.of {
        SummaryOf::Thread(thread) => format!(
            "a TL;DR of thread {thread} from the entries below: what was asked, decided and done, and what waits on whom"
        ),
        SummaryOf::Since(since) => format!(
            "a digest of the threads with entries since {} from the entries below: what finished, what is stuck, and \
             what waits on the human",
            since.format("%Y-%m-%d %H:%M UTC")
        ),
    };
    format!(
        "[ants] A human asks for {what}. Answer with at most 120 words of plain text; use no tools and message no \
         one: the reply is the summary."
    )
}

/// The `[ants]` line starting the prompt of `from`'s reply to a message, or a `post`, the ant sent: its answer is not
/// sent back.
pub(in crate::daemon) fn own_reply_note(from: &UserId, post: bool, spent: bool) -> String {
    let (what, going_on) = if post {
        ("a post this ant made", "post_channel on this thread (or send_message)")
    } else {
        ("a message this ant sent", "send_message (or post_channel on a channel)")
    };
    if spent {
        return format!(
            "[ants] {from} replied to {what}; the answer to this prompt is not sent back, and the thread has no turns \
             left: escalate hands it on when it needs more; complete ends the turn."
        );
    }
    format!(
        "[ants] {from} replied to {what}; the answer to this prompt is not sent back. {going_on} continues the \
         exchange; complete ends the turn."
    )
}

/// The `[ants]` line starting the prompt of `from`'s reply to an `ask_ant` or `route` call that gave up: its answer
/// is not sent back, and the ant passes the answer on to whoever asked on the thread.
pub(in crate::daemon) fn late_reply_note(from: &UserId, spent: bool) -> String {
    let going_on = if spent {
        "send_message to a human who asked, or escalate, as the thread has no turns left for ants"
    } else {
        "send_message (or post_channel on a channel's thread)"
    };
    format!(
        "[ants] {from} replied to an ask_ant or route call on this thread after the call gave up; the answer to this \
         prompt is not sent back. Pass the answer on to whoever asked on this thread with {going_on}; complete ends \
         the turn."
    )
}

/// The prompt for a message: warned when an earlier attempt at it was cut off, and noting the `late` replies to
/// `ask_ant` that wait on its thread for `check_inbox`.
pub(in crate::daemon) fn prompt_text(text: &str, cut_off: bool, late: usize) -> String {
    let late = match late {
        0 => None,
        1 => Some("[ants] 1 late reply to ask_ant is waiting on this thread; call check_inbox to read it.".to_owned()),
        n => Some(format!(
            "[ants] {n} late replies to ask_ant are waiting on this thread; call check_inbox to read them."
        )),
    };
    let cut_off = cut_off.then(|| CUT_OFF.to_owned());
    cut_off.into_iter().chain(late).chain([text.to_owned()]).collect::<Vec<_>>().join("\n\n")
}
