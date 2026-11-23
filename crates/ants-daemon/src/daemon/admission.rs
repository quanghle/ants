//! Admitting what arrives: inbox messages and channel posts checked, and what they claim of their sender, owner and
//! creator checked against stored state ([`Claims`]).

use ants_proto::{Acting, ChannelPost, Envelope, Humans, ThreadId, ThreadOrigin, UserId};
use anyhow::Result;
use async_nats::jetstream;

use crate::bus;
use crate::bus::relayed::{self, Read};
use crate::channel::{self, Channels, Threads, Wake};
use crate::mcp::Claim;
use crate::people::humans::Registry;

use super::{Inbox, Incoming, OWNER_LANDS_WITHIN, Posted, STREAM_SOURCE, Steered, stream_seq};

/// What stored state checks the claims of an inbox message against, for the ant's daemon and for the human's side
/// that buries what the daemon could not ([`dead::bury_undelivered`](crate::delivery::dead::bury_undelivered)).
pub struct Claims<'a> {
    /// Who is a human.
    pub humans: &'a Registry,
    /// The channel threads, whose posts say who started each.
    pub threads: &'a Threads,
}

impl Claims<'_> {
    /// Check what inbox message `envelope`, at stream sequence `seq`, says of its sender and thread: its sender wrote
    /// it, and its subject says whether it is a webhook's ([`Envelope::check_subject`]). A human's owner and creator hold, and a
    /// webhook's owner, the human its config names. From an ant, a thread a channel post started has the owner and
    /// creator its posts say ([`Threads::origin_of`]); any other has the owner a human's or a webhook's message on it to
    /// the sender, or to an ant that passed the link to it on to the sender, named ([`Threads::owner_for`]), else that
    /// of the thread it was spun off ([`Envelope::origin_thread`]): a channel thread's under the same check as its
    /// creator, another's under the same as its own; else, for an assistant's message for its human naming the human,
    /// the human; else none, whatever owner the ant names.
    pub async fn sender_claims(&self, envelope: &mut Envelope, seq: u64) {
        if self.humans.sender_role(envelope).is_human() {
            return;
        }
        if envelope.webhook {
            envelope.creator = None;
            envelope.origin_thread = None;
            return;
        }
        if envelope.owner.is_none() && envelope.creator.is_none() {
            envelope.origin_thread = None;
            return;
        }
        let humans = self.humans.humans();
        match self.threads.origin_of(&envelope.thread_id, seq, &humans).await {
            Ok(Some(origin)) => {
                envelope.owner = origin.owner;
                envelope.creator = origin.creator;
                envelope.origin_thread = None;
            }
            Ok(None) => self.spun_off(envelope, seq, &humans).await,
            Err(e) => {
                tracing::warn!(thread = %envelope.thread_id, error = format!("{e:#}"), "reading the thread's origin");
                if let Some(named) = envelope.owner.take() {
                    let recorded = self.recorded_owner(&envelope.thread_id, &envelope.from, Some(&named)).await;
                    envelope.owner = recorded.or_else(|| self.assisted_owner(envelope, named));
                }
                envelope.creator = None;
                envelope.origin_thread = None;
            }
        }
    }

    /// Check the owner and creator `envelope`, an ant's message at stream sequence `seq` on a thread no channel post
    /// started, names: its owner is the one recorded for its thread and sender, else the one of the thread it links to
    /// ([`Claims::linked`]); its creator that of the channel thread it links to. It keeps the link only when it gives
    /// either.
    async fn spun_off(&self, envelope: &mut Envelope, seq: u64, humans: &Humans) {
        let (named_owner, named_creator) = (envelope.owner.take(), envelope.creator.take());
        let recorded = match &named_owner {
            Some(named) => self.recorded_owner(&envelope.thread_id, &envelope.from, Some(named)).await,
            None => None,
        };
        let (linked_owner, linked_creator) = match envelope.origin_thread.take() {
            Some(origin) if (named_owner.is_some() && recorded.is_none()) || named_creator.is_some() => {
                let (owner, creator) = self.linked(&origin, &envelope.from, named_owner.as_ref(), seq, humans).await;
                let owner = owner.filter(|_| named_owner.is_some() && recorded.is_none());
                let creator = creator.filter(|_| named_creator.is_some());
                if owner.is_some() || creator.is_some() {
                    envelope.origin_thread = Some(origin);
                }
                (owner, creator)
            }
            _ => (None, None),
        };
        let assisted = named_owner.clone().and_then(|named| self.assisted_owner(envelope, named));
        envelope.owner = recorded.or(linked_owner).or(assisted);
        envelope.creator = linked_creator;
        if named_owner.is_some() && envelope.owner.is_none() || named_creator.is_some() && envelope.creator.is_none() {
            tracing::debug!(
                thread = %envelope.thread_id,
                sender = %envelope.from,
                "dropping the message's owner or creator: no human or webhook recorded the thread's owner for the \
                 sender, and it links to no thread that gives one"
            );
        }
    }

    /// The owner and creator of `origin`, the thread an ant's message from `sender` at stream sequence `seq` links to,
    /// when `sender` has a link to it: a channel thread's ([`Threads::origin_for`]); else the owner recorded for it
    /// ([`Threads::owner_for`]) and no creator.
    async fn linked(
        &self,
        origin: &ThreadId,
        sender: &UserId,
        named: Option<&UserId>,
        seq: u64,
        humans: &Humans,
    ) -> (Option<UserId>, Option<UserId>) {
        let read = async {
            if self.threads.is_channel_thread(origin, seq).await? {
                let origin = self.threads.origin_for(origin, sender, seq, humans).await?;
                return anyhow::Ok(origin.map_or((None, None), |o| (o.owner, o.creator)));
            }
            let owner = self.recorded_owner_of(origin, sender, named).await?;
            Ok((owner, None))
        };
        read.await.unwrap_or_else(|e| {
            tracing::warn!(thread = %origin, error = format!("{e:#}"), "reading the origin thread");
            (None, None)
        })
    }

    /// The owner recorded for `thread` and `sender`, when a human ([`Threads::owner_for`]), or `named`, the owner the
    /// message names, as its assistant recorded it.
    async fn recorded_owner(&self, thread: &ThreadId, sender: &UserId, named: Option<&UserId>) -> Option<UserId> {
        self.recorded_owner_of(thread, sender, named).await.unwrap_or_else(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "reading the thread's owner");
            None
        })
    }

    async fn recorded_owner_of(
        &self,
        thread: &ThreadId,
        sender: &UserId,
        named: Option<&UserId>,
    ) -> Result<Option<UserId>> {
        let assistant = named.and_then(|human| self.humans.assistant(human));
        let owner = self.threads.owner_for(thread, sender, named.zip(assistant.as_ref())).await?;
        Ok(owner.filter(|o| self.humans.is_human(o)))
    }

    /// `named`, the owner `envelope` names, when the message is its assistant's for it: its sender is the assistant
    /// the human's grant names now, and it claims to act for the human.
    fn assisted_owner(&self, envelope: &Envelope, named: UserId) -> Option<UserId> {
        let assistant = self.humans.assistant(&named);
        (envelope.on_behalf_of.as_ref() == Some(&named) && assistant.as_ref() == Some(&envelope.from)).then_some(named)
    }
}

impl Inbox<'_> {
    /// The inbox message `msg` to answer, if it is one: a malformed or forged one is buried, a reply to an `ask_ant`
    /// call is handed over, and so is an approval decision, which is acked and never prompted with
    /// ([`NatsApprover::offer`](crate::approvals::NatsApprover::offer)). An owner that is no human is dropped
    /// ([`Registry::human_owner`]), so neither the escalations nor the messages that follow on from this one name it,
    /// and so is a creator that is a human.
    pub(super) async fn admit(&self, msg: &jetstream::Message) -> Option<Incoming> {
        // Its subject pins no sender when a stream relayed it there ([`relayed`]).
        if relayed::stored_relayed(msg.headers.as_ref(), &msg.payload, Read::Consumer) {
            self.bury_unanswerable(msg, "relayed by a stream, not published by its sender").await;
            return None;
        }
        let mut envelope: Envelope = match serde_json::from_slice(&msg.payload) {
            Ok(e) => e,
            Err(e) => {
                self.bury_unanswerable(msg, &format!("not an envelope: {e}")).await;
                return None;
            }
        };
        // A forged sender is buried without its envelope, so a replay cannot pass it on as genuine.
        if let Err(e) = envelope.check_subject(&msg.subject) {
            self.bury_unanswerable(msg, &format!("sender mismatch: {e}")).await;
            return None;
        }
        self.check_behalf(&mut envelope).await;
        if envelope.approval.is_some() {
            if let Some(approvals) = self.approvals {
                approvals.offer(&envelope);
            }
            if let Err(e) = bus::double_ack(self.client, msg).await {
                tracing::warn!(error = %e, "acking an approval decision");
            }
            return None;
        }
        envelope.owner = self.humans.human_owner(envelope.owner.as_ref());
        envelope.creator = self.humans.ant_creator(envelope.creator.as_ref());
        let late = match self.claim(&envelope, msg).await {
            Some(late) => late,
            None => return None,
        };
        Some(Incoming { envelope, post: None, resume: None, steered: Steered::No, late })
    }

    /// Check the `on_behalf_of` of `envelope`, which arrived on its sender's subject: a forged one is cleared, so the
    /// message counts as the sender's own; for an honoured one the human is told what its assistant did for it, as of
    /// a stand-in's decision. Failing to tell, it only warns.
    async fn check_behalf(&self, envelope: &mut Envelope) {
        let acting = self.behalf.acting(self.humans, &envelope.from, envelope.on_behalf_of.as_ref()).await;
        let notice = match &acting {
            Acting::Own => return,
            Acting::Forged(human) => {
                tracing::warn!(from = %envelope.from, %human, "ignoring a forged on_behalf_of; taking the message as its sender's own");
                envelope.on_behalf_of = None;
                return;
            }
            Acting::For(human) => match envelope.approval.as_deref() {
                Some(decision) => crate::people::behalf::held_notice(envelope, decision, human),
                None => crate::people::behalf::sent_notice(envelope, human),
            },
        };
        let id = format!("on-behalf-{}", envelope.id);
        match bus::store(self.js, notice.subject(), &notice, &id).await {
            Ok(_) => tracing::info!(from = %envelope.from, to = %notice.to, "told the human of its assistant's act"),
            Err(e) => tracing::warn!(
                from = %envelope.from,
                to = %notice.to,
                error = format!("{e:#}"),
                "telling the human of its assistant's act"
            ),
        }
    }

    /// The channel post `msg` to answer, if it wakes the ant ([`channel::wake`]). Any other is acked at once: the
    /// ant's own, one in a channel it is not a member of, one that does not @mention it and is not for it to
    /// triage. A malformed or forged one is dropped, with no dead letter: every daemon sees it, and one whose `webhook`
    /// disagrees with its subject is forged. A post is for a human only from its assistant while the grant stands: a
    /// forged `on_behalf_of` is cleared. No daemon tells the human of an honoured one, as every daemon sees it. A top-level post kept names its thread's owner, a human, and its
    /// creator, an ant ([`ThreadOrigin`]); any other names none until [`Inbox::handle`] reads its thread's.
    pub(super) async fn admit_post(&self, msg: &jetstream::Message, channels: &mut Channels) -> Option<Incoming> {
        let post = if relayed::stored_relayed(msg.headers.as_ref(), &msg.payload, Read::Consumer) {
            Err("relayed by a stream, not published by its sender".to_owned())
        } else {
            serde_json::from_slice::<ChannelPost>(&msg.payload)
                .map_err(|e| e.to_string())
                .and_then(|post| post.check_subject(&msg.subject).map(|()| post).map_err(|e| e.to_string()))
        };
        let mut post = match post {
            Ok(post) => post,
            Err(error) => {
                tracing::warn!(subject = %msg.subject, error, "dropping a channel post");
                bus::term(msg).await;
                return None;
            }
        };
        if let Acting::Forged(human) = self.behalf.acting(self.humans, &post.from, post.on_behalf_of.as_ref()).await {
            tracing::warn!(
                channel = %post.channel,
                sender = %post.from,
                %human,
                "ignoring a forged on_behalf_of; taking the post as its sender's own"
            );
            post.on_behalf_of = None;
        }
        let humans = self.humans.humans();
        if (post.from == *self.ant || post.webhook) && !self.notify_mentioned(msg, &post, channels, &humans).await {
            return None;
        }
        let mut wake = channel::wake(self.ant, channels.get(&post.channel), &post, &humans);
        let unseen = channels.get(&post.channel).is_none() && post.to_triage(&humans);
        if wake == Wake::Skip && (post.mentions(self.ant) && post.from != *self.ant || unseen) {
            // The post may have overtaken the change that made the ant a member, or created the channel.
            channels.refresh(&post.channel).await;
            wake = channel::wake(self.ant, channels.get(&post.channel), &post, &humans);
        }
        if wake == Wake::Skip {
            if let Err(e) = msg.ack().await {
                tracing::warn!(error = %e, "acking a channel post");
            }
            return None;
        }
        let triage = (wake == Wake::Triage).then(|| channels.get(&post.channel).cloned()).flatten();
        let owned = channels.get(&post.channel).is_some_and(|e| e.owner.as_ref() == Some(self.ant));
        let description = channels.get(&post.channel).and_then(|e| e.description.clone());
        let origin = if post.starts_thread() { ThreadOrigin::of_root(&post, &humans) } else { ThreadOrigin::default() };
        origin.apply(&mut post);
        let envelope = channel::as_envelope(&post, self.ant);
        let posted = Some(Box::new(Posted { post, triage, owned, description }));
        Some(Incoming { envelope, post: posted, resume: None, steered: Steered::No, late: false })
    }

    /// Notify the human members `post`, in `msg`, @mentions ([`Threads::notify_mentioned`]) before the post is acked,
    /// when the ant sends its notices: its own post, or a webhook's, in a channel
    /// the ant is the messenger of. False when that failed: the post is naked, to be tried again.
    async fn notify_mentioned(
        &self,
        msg: &jetstream::Message,
        post: &ChannelPost,
        channels: &mut Channels,
        humans: &Humans,
    ) -> bool {
        let mut mentioned = post.mentions.iter().filter(|m| humans.contains(m)).peekable();
        if mentioned.peek().is_none() {
            return true;
        }
        if channels.get(&post.channel).is_none_or(|entry| mentioned.any(|m| !entry.is_member(m))) {
            // The post may have overtaken the change that made the human a member, or the ant the messenger.
            channels.refresh(&post.channel).await;
        }
        let Some(seq) = stream_seq(msg) else {
            tracing::warn!(channel = %post.channel, "notifying no human of a post without its stream sequence");
            return true;
        };
        match self.threads.notify_mentioned(post, seq, channels.get(&post.channel), humans).await {
            Ok(notified) => {
                if !notified.is_empty() {
                    tracing::info!(channel = %post.channel, thread = %post.thread_id, ?notified, "notified humans");
                }
                true
            }
            Err(e) => {
                tracing::warn!(channel = %post.channel, error = format!("{e:#}"), "notifying the humans mentioned");
                bus::nak(msg, Some(self.pacing.retry_delay)).await;
                false
            }
        }
    }

    /// Name the owner and creator of the thread of the channel post in `incoming`, at stream sequence `seq`, as its
    /// thread's posts say ([`Threads::origin`]), not as the post does: its author chose those. Unread, the thread has
    /// neither.
    pub(super) async fn thread_origin(&self, incoming: &mut Incoming, seq: u64) {
        let Some(posted) = incoming.post.as_mut().filter(|posted| !posted.post.starts_thread()) else { return };
        let origin = match self.threads.origin(&posted.post, seq, &self.humans.humans()).await {
            Ok(origin) => origin,
            Err(e) => {
                tracing::warn!(thread = %posted.post.thread_id, error = format!("{e:#}"), "reading the thread's origin");
                ThreadOrigin::default()
            }
        };
        origin.apply(&mut posted.post);
        incoming.envelope.owner.clone_from(&posted.post.owner);
        incoming.envelope.creator.clone_from(&posted.post.creator);
    }

    /// Check what the inbox message in `incoming`, at stream sequence `seq`, says of its sender and thread
    /// ([`Claims::sender_claims`]).
    pub(super) async fn sender_claims(&self, incoming: &mut Incoming, seq: u64) {
        if incoming.post.is_some() {
            return;
        }
        let claims = Claims { humans: self.humans, threads: self.threads };
        claims.sender_claims(&mut incoming.envelope, seq).await;
    }

    /// Wait, up to [`OWNER_LANDS_WITHIN`], for the owner a human's message in `incoming` names to be recorded for its
    /// thread and the ant when the message came from a machine's outbox: the owners bucket sources the record from the
    /// same outbox, but apart from the hub's stream, and the ant's own messages on or from the thread take their owner
    /// from it.
    pub(super) async fn owner_landed(&self, incoming: &Incoming, msg: &jetstream::Message) {
        let envelope = &incoming.envelope;
        let sourced = msg.headers.as_ref().is_some_and(|h| h.get(STREAM_SOURCE).is_some());
        if !sourced || incoming.post.is_some() || envelope.owner.is_none() || !self.humans.is_human(&envelope.from) {
            return;
        }
        match self.threads.awaited_owner(&envelope.thread_id, self.ant, OWNER_LANDS_WITHIN).await {
            Ok(Some(_)) => {}
            Ok(None) => tracing::debug!(thread = %envelope.thread_id, "no owner recorded for the thread"),
            Err(e) => {
                tracing::warn!(thread = %envelope.thread_id, error = format!("{e:#}"), "reading the thread's owner");
            }
        }
    }

    /// What `envelope` is to the `ask_ant` calls: `None` for a reply handed over and acked, which must not wait in its
    /// thread's queue, as the prompt that asked is in flight on that thread; else whether it is a late reply, kept for
    /// `check_inbox` and prompted on its thread when its turn comes there.
    async fn claim(&self, envelope: &Envelope, msg: &jetstream::Message) -> Option<bool> {
        let Some(mcp) = self.mcp else { return Some(false) };
        match mcp.claim(envelope).await {
            Ok(Claim::No) => Some(false),
            Ok(Claim::Late) => Some(true),
            Ok(Claim::Taken) => {
                match bus::double_ack(self.client, msg).await {
                    // Finished, not left waiting to the watcher of its sender ([`unserved`]).
                    Ok(()) => self.marks.attempts.finish(envelope).await,
                    Err(e) => tracing::warn!(error = %e, "acking a reply to ask_ant"),
                }
                None
            }
            Err(e) => {
                tracing::warn!(error = format!("{e:#}"), "keeping a reply to ask_ant");
                bus::nak(msg, Some(self.pacing.retry_delay)).await;
                None
            }
        }
    }
}
