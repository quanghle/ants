use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::{CHANNELS_BUCKET, HUMANS_BUCKET, WEBHOOK_ALLOW, WEBHOOKS_BUCKET, webhook_marker_key};
use crate::{
    ChannelId, ChannelOp, DEFAULT_TURN_BUDGET, Envelope, Error, Humans, Mention, Refusal, ThreadId, UserId,
    parse_mentions, subject,
};

/// A channel's entry in the [`CHANNELS_BUCKET`], under the key of its channel id. Humans write it, and the channel
/// manager on its owner's requests ([`ChannelEntry::apply`]).
///
/// Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelEntry {
    pub channel: ChannelId,
    /// The users in the channel, ants and humans, in the order they were added.
    #[serde(default)]
    pub members: Vec<UserId>,
    /// The ant that triages humans' posts that @mention nobody, when not the default `messenger`
    /// ([`ChannelEntry::messenger`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messenger: Option<UserId>,
    /// The ant that may change the members and the messenger through the channel manager. Only a human sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
}

impl ChannelEntry {
    /// A channel without members, messenger or owner.
    pub fn new(channel: ChannelId) -> Self {
        Self { channel, members: Vec::new(), messenger: None, owner: None }
    }

    /// The KV key of the entry.
    pub fn key(&self) -> &str {
        self.channel.as_str()
    }

    /// Check that the entry was read from its own key; a reader skips one naming another channel.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignChannel { kind: "channel entry", channel: self.channel.clone(), at: format!("key {key}") })
    }

    pub fn is_member(&self, user: &UserId) -> bool {
        self.members.contains(user)
    }

    /// The ant that triages humans' posts that @mention nobody: the one assigned, or else the default
    /// [`UserId::messenger`]. It need not be a member.
    pub fn messenger(&self) -> UserId {
        self.messenger.clone().unwrap_or_else(UserId::messenger)
    }

    /// Add `user`, an ant or a human, after the other members; false when it already is one.
    pub fn add_member(&mut self, user: UserId) -> bool {
        if self.is_member(&user) {
            return false;
        }
        self.members.push(user);
        true
    }

    /// Remove `user` from the members; false when it was none.
    pub fn remove_member(&mut self, user: &UserId) -> bool {
        let before = self.members.len();
        self.members.retain(|m| m != user);
        self.members.len() != before
    }

    /// The ants `post` wakes: the members it @mentions, except its author and the `humans`; mentions of anyone
    /// else wake nobody. A post of a human or a webhook that mentions nobody wakes the channel's
    /// [`ChannelEntry::messenger`] instead, to triage it ([`ChannelPost::to_triage`]); an agent's post that mentions
    /// nobody wakes nobody. A post to another channel wakes nobody here.
    pub fn woken(&self, post: &ChannelPost, humans: &Humans) -> Vec<UserId> {
        if post.channel != self.channel {
            return Vec::new();
        }
        if post.mentions.is_empty() {
            return if post.to_triage(humans) { vec![self.messenger()] } else { Vec::new() };
        }
        post.mentions.iter().filter(|m| self.is_member(m) && **m != post.from && !humans.contains(m)).cloned().collect()
    }

    /// Who sends the mention notices of `post` ([`ChannelEntry::notified`]): its author, or for a webhook's post the
    /// channel's [`ChannelEntry::messenger`], since a webhook's credentials publish only to its destination
    /// ([`ChannelPost::relayed_notice`]). A reader checks [`ChannelPost::webhook`] against the webhook's marker first.
    pub fn notifier(&self, post: &ChannelPost) -> UserId {
        if post.webhook { self.messenger() } else { post.from.clone() }
    }

    /// The `humans` `post` notifies ([`ChannelPost::mention_notice`]): the members among them it @mentions, except its
    /// author. They are never woken: a mention of a human wakes no agent, and keeps the post from triage. A post to
    /// another channel notifies nobody here.
    pub fn notified(&self, post: &ChannelPost, humans: &Humans) -> Vec<UserId> {
        if post.channel != self.channel {
            return Vec::new();
        }
        post.mentions.iter().filter(|m| self.is_member(m) && **m != post.from && humans.contains(m)).cloned().collect()
    }

    /// Apply the `ops` of `requester`, which must be the channel's owner, leaving the owner as it is. Members may be
    /// ants or `humans`, but none may make a human the messenger: it is an ant. Returns what changed, in order, empty
    /// when nothing did; on a refusal the entry is unchanged.
    pub fn apply(&mut self, requester: &UserId, ops: &[ChannelOp], humans: &Humans) -> Result<Vec<String>, Refusal> {
        if self.owner.as_ref() != Some(requester) {
            return Err(Refusal::NotOwner { owner: self.owner.clone() });
        }
        if let Some(human) = ops.iter().find_map(|op| match op {
            ChannelOp::SetMessenger { ant } if humans.contains(ant) => Some(ant),
            _ => None,
        }) {
            return Err(Refusal::Human(human.clone()));
        }
        let mut changed = Vec::new();
        for op in ops {
            match op {
                ChannelOp::AddMember { ant } => {
                    if self.add_member(ant.clone()) {
                        changed.push(format!("added {ant}"));
                    }
                }
                ChannelOp::RemoveMember { ant } => {
                    if self.remove_member(ant) {
                        changed.push(format!("removed {ant}"));
                    }
                }
                ChannelOp::SetMessenger { ant } => {
                    if self.messenger() != *ant {
                        changed.push(format!("messenger {ant}"));
                    }
                    self.messenger = (*ant != UserId::messenger()).then(|| ant.clone());
                }
                ChannelOp::ClearMessenger => {
                    if self.messenger.take().is_some() {
                        changed.push(format!("messenger back to the default {}", UserId::messenger()));
                    }
                }
            }
        }
        Ok(changed)
    }
}

/// A post to a channel, on [`subject::chan`]: like a Slack message, either a top-level post that starts a thread or a
/// post on an existing thread.
///
/// Posts spend the thread's turn budget like messages between agents do: every agent's post spends a turn of what the
/// thread has left ([`ChannelPost::turns_left`]), and at zero the thread goes to its owner, the human whose post started
/// it, else the default human; any human's post gives the thread a fresh budget. Each post carries the turns the thread
/// has left after it, the thread's owner when known, and the ant that created the thread when an ant did.
///
/// Who is a human is the caller's to say, from the [`HUMANS_BUCKET`], as [`Humans`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelPost {
    pub id: Uuid,
    pub channel: ChannelId,
    /// The thread the post is on. A top-level post starts a thread with its own id ([`ChannelPost::starts_thread`]).
    pub thread_id: ThreadId,
    pub from: UserId,
    pub text: String,
    /// The users the post @mentions, in order ([`parse_mentions`] of the text, when built here).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<UserId>,
    /// Agent-to-agent turns left on the thread after this post.
    pub turn_budget: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<Uuid>,
    /// The human who started the thread. Replies keep it ([`ChannelPost::reply`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// The ant whose top-level post started the thread, never a human. Replies keep it ([`ChannelPost::reply`]); a
    /// reader takes it from a top-level post's author only ([`ChannelPost::thread_creator`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<UserId>,
    /// A webhook's post, as `ants webhook serve` marks it: one that mentions nobody goes to the channel's messenger, as
    /// a human's does ([`ChannelPost::to_triage`]). It is the poster's claim: a daemon takes it only from a sender whose
    /// marker is in [`WEBHOOKS_BUCKET`] ([`webhook_marker_key`]) or a human, so no ant names a thread's owner with it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub webhook: bool,
    /// The tool rules the thread is narrowed to, as [`Envelope::allow`]: a webhook's post without them stands for
    /// [`WEBHOOK_ALLOW`], an agent's reply keeps those of the post it answers ([`ChannelPost::reply`]) and a human's has
    /// none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow: Option<Vec<String>>,
    pub sent_at: DateTime<Utc>,
}

impl ChannelPost {
    /// A top-level post with a fresh budget and no owner: it starts a new thread, whose id is the post's own.
    pub fn new(channel: ChannelId, from: UserId, text: impl Into<String>) -> Self {
        let id = Uuid::now_v7();
        Self::build(id, channel, ThreadId::of_post(id), from, text.into())
    }

    /// A post on an existing thread, with a fresh budget and no owner, as a human posts.
    pub fn on_thread(channel: ChannelId, thread_id: ThreadId, from: UserId, text: impl Into<String>) -> Self {
        Self::build(Uuid::now_v7(), channel, thread_id, from, text.into())
    }

    fn build(id: Uuid, channel: ChannelId, thread_id: ThreadId, from: UserId, text: String) -> Self {
        Self {
            id,
            channel,
            thread_id,
            mentions: parse_mentions(&text),
            from,
            text,
            turn_budget: DEFAULT_TURN_BUDGET,
            in_reply_to: None,
            owner: None,
            creator: None,
            webhook: false,
            allow: None,
            sent_at: Utc::now(),
        }
    }

    /// The post with `owner` as the human who started its thread.
    #[must_use]
    pub fn with_owner(self, owner: Option<UserId>) -> Self {
        Self { owner, ..self }
    }

    /// The post with `creator` as the ant that started its thread.
    #[must_use]
    pub fn with_creator(self, creator: Option<UserId>) -> Self {
        Self { creator, ..self }
    }

    /// The post narrowed to the tool rules `allow` ([`ChannelPost::allow`]).
    #[must_use]
    pub fn with_allow(self, allow: Option<Vec<String>>) -> Self {
        Self { allow, ..self }
    }

    /// The ant that created the post's thread, none of the `humans`, no webhook and not the channel manager: of a
    /// top-level post its author, whatever the post says, so only the author's credentials make an ant a thread's
    /// creator; of a later post the one it carries, which its author chose: a reader takes a thread's creator from
    /// its stored posts instead ([`ThreadOrigin`]). Trust it only once the post passed [`ChannelPost::check_subject`].
    pub fn thread_creator(&self, humans: &Humans) -> Option<UserId> {
        let creator = if self.starts_thread() { (!self.webhook).then_some(&self.from) } else { self.creator.as_ref() };
        creator.filter(|c| !humans.contains(c) && **c != UserId::channel_manager()).cloned()
    }

    /// The post as a webhook's, mentioning exactly `mentions`, whatever its text says.
    #[must_use]
    pub fn from_webhook(self, mentions: Vec<UserId>) -> Self {
        Self { mentions, webhook: true, ..self }
    }

    /// Whether the post goes to the channel's messenger to triage: it mentions nobody and is a post of one of the
    /// `humans` or a webhook's.
    pub fn to_triage(&self, humans: &Humans) -> bool {
        self.mentions.is_empty() && (self.webhook || humans.contains(&self.from))
    }

    /// Whether the post is a human's: its author is one of the `humans` and it is no webhook's, so a webhook a human's
    /// name was registered for never posts as that human.
    pub fn by_human(&self, humans: &Humans) -> bool {
        !self.webhook && humans.contains(&self.from)
    }

    /// The owner of the post's thread, one of the `humans`: the one it carries, else its author when a human
    /// ([`ChannelPost::by_human`]), as for a post stored before posts carried owners. An owner that is no human is
    /// ignored: only a human owns a thread. The owner a post carries is its author's choice: a reader takes a thread's
    /// owner from its stored posts instead ([`ThreadOrigin`]).
    pub fn thread_owner(&self, humans: &Humans) -> Option<UserId> {
        let carried = self.owner.as_ref().filter(|owner| humans.contains(owner));
        carried.cloned().or_else(|| self.by_human(humans).then(|| self.from.clone()))
    }

    /// The post as an agent's, with what is left of `budget`, the turns its thread has left: one turn less, whoever it
    /// wakes, which fails when none are left.
    pub fn spend(self, budget: u32) -> Result<Self, Error> {
        let turn_budget = budget.checked_sub(1).ok_or_else(|| Error::BudgetExhausted(self.thread_id.clone()))?;
        Ok(Self { turn_budget, ..self })
    }

    /// The turns the thread has left for an answer to this post, given the posts stored on the thread `later` than it,
    /// in order: a post of one of the `humans` gives the thread a fresh budget, and any other leaves the lower of what
    /// was left and what it carries. So the agents answering one post spend from the thread's turns together, not each
    /// from the post's.
    pub fn turns_left(&self, later: &[ChannelPost], humans: &Humans) -> u32 {
        later.iter().filter(|post| post.thread_id == self.thread_id).fold(self.turn_budget, |left, post| {
            if post.by_human(humans) { post.turn_budget } else { left.min(post.turn_budget) }
        })
    }

    /// A reply on the same channel and thread, after the posts stored on it `later` than this one, keeping the
    /// thread's owner ([`ChannelPost::thread_owner`]) and creator ([`ChannelPost::thread_creator`]); a human's reply on
    /// a thread with no known owner takes it. A human's reply has a fresh budget and no tool rules; an agent's spends
    /// one of the [`ChannelPost::turns_left`] and keeps this post's tool rules.
    pub fn reply(
        &self,
        from: UserId,
        text: impl Into<String>,
        later: &[ChannelPost],
        humans: &Humans,
    ) -> Result<Self, Error> {
        let mut reply = Self::on_thread(self.channel.clone(), self.thread_id.clone(), from, text);
        reply.in_reply_to = Some(self.id);
        reply.owner = self.thread_owner(humans).or_else(|| reply.thread_owner(humans));
        reply.creator = self.thread_creator(humans);
        if humans.contains(&reply.from) {
            return Ok(reply);
        }
        reply.allow.clone_from(&self.allow);
        reply.spend(self.turns_left(later, humans))
    }

    /// Whether the post is a top-level one, which started its thread.
    pub fn starts_thread(&self) -> bool {
        self.thread_id.as_str() == self.id.to_string()
    }

    /// Whether the post @mentions `user`.
    pub fn mentions(&self, user: &UserId) -> bool {
        self.mentions.contains(user)
    }

    /// The message telling `human`, a human member of the channel it @mentions ([`ChannelEntry::notified`]), of the
    /// post: from its author, on its thread, with its text, and the channel and post in [`Envelope::mention`]. It
    /// spends no turn and names no owner or creator. Sent with [`ChannelPost::mention_msg_id`].
    pub fn mention_notice(&self, human: &UserId) -> Envelope {
        Envelope {
            in_reply_to: None,
            turn_budget: self.turn_budget,
            webhook: self.webhook,
            mention: Some(Mention { channel: self.channel.clone(), post: self.id, author: None }),
            sent_at: self.sent_at,
            ..Envelope::on_thread(self.thread_id.clone(), self.from.clone(), human.clone(), self.text.clone())
        }
    }

    /// The [`ChannelPost::mention_notice`] to `human` as `relay` sends it for the post's author, a webhook
    /// ([`ChannelEntry::notifier`]): from `relay`, no webhook's, naming the author in [`Mention::author`].
    pub fn relayed_notice(&self, human: &UserId, relay: &UserId) -> Envelope {
        let notice = self.mention_notice(human);
        let mention = notice.mention.map(|mention| Mention { author: Some(self.from.clone()), ..mention });
        Envelope { from: relay.clone(), webhook: false, mention, ..notice }
    }

    /// The JetStream msg id of the [`ChannelPost::mention_notice`] to `human`, so a notice sent again is stored once.
    pub fn mention_msg_id(&self, human: &UserId) -> String {
        format!("mention.{}.{human}", self.id)
    }

    /// Whether the post @mentions an ant other than its author and the `humans`: an agent it would wake.
    pub fn wakes_agents(&self, humans: &Humans) -> bool {
        self.mentions.iter().any(|m| *m != self.from && !humans.contains(m))
    }

    pub fn subject(&self) -> String {
        subject::chan(&self.channel, &self.from)
    }

    /// Check that the post arrived on its own [`ChannelPost::subject`]. Credentials pin the sender token of a channel
    /// subject, so a `from` that disagrees with it is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::WrongChannelSubject {
            subject: subject.to_owned(),
            from: self.from.clone(),
            channel: self.channel.clone(),
        })
    }
}

/// Who started a channel thread, as its stored posts say. Readers go by it rather than by the `owner` and `creator`
/// a later post carries, which its author chose: an ant could otherwise name any creator, and so the chain the
/// thread's escalations climb, or any owner.
///
/// Fed the posts stored on one thread in stream order ([`ThreadOrigin::push`]), each checked against its subject
/// ([`ChannelPost::check_subject`]):
/// - the thread's top-level post is the first one read, when it starts the thread: a later post with the thread's id
///   only claims to. Its author is the creator when an ant ([`ChannelPost::thread_creator`]);
/// - the owner is the first human to post on the thread ([`ChannelPost::by_human`]), the author of the top-level post
///   when a human; a webhook's top-level post may name the owner, a human, as its config does.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ThreadOrigin {
    /// Whether the thread's top-level post was read.
    pub root: bool,
    /// Whether any post on the thread was read.
    seen: bool,
    pub owner: Option<UserId>,
    pub creator: Option<UserId>,
}

impl ThreadOrigin {
    /// The origin of the thread that `root`, a top-level post, starts, from that post alone.
    pub fn of_root(root: &ChannelPost, humans: &Humans) -> Self {
        let mut origin = Self::default();
        origin.push(root, humans);
        origin
    }

    /// Take `post`, the next post stored on the thread.
    pub fn push(&mut self, post: &ChannelPost, humans: &Humans) {
        let root = !self.seen && post.starts_thread();
        self.seen = true;
        if root {
            self.root = true;
            self.creator = post.thread_creator(humans);
        }
        if self.owner.is_none() {
            self.owner = if post.by_human(humans) {
                Some(post.from.clone())
            } else if root && post.webhook {
                post.owner.clone().filter(|owner| humans.contains(owner))
            } else {
                None
            };
        }
    }

    /// Whether later posts change nothing: the top-level post was read and the thread has its owner.
    pub fn settled(&self) -> bool {
        self.root && self.owner.is_some()
    }

    /// Put the thread's owner and creator on `post`, a post on the thread, in place of those it carries.
    pub fn apply(&self, post: &mut ChannelPost) {
        post.owner.clone_from(&self.owner);
        post.creator.clone_from(&self.creator);
    }
}

#[cfg(test)]
mod tests;
