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
    pub fn with_owner(self, owner: Option<UserId>) -> Self {
        Self { owner, ..self }
    }

    /// The post with `creator` as the ant that started its thread.
    pub fn with_creator(self, creator: Option<UserId>) -> Self {
        Self { creator, ..self }
    }

    /// The post narrowed to the tool rules `allow` ([`ChannelPost::allow`]).
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
mod tests {
    use super::*;
    use crate::{HumanEntry, Role, mention_key, p};

    #[test]
    fn channel_posts_start_threads_and_are_checked_against_their_subject() {
        let c: ChannelId = "ops".parse().unwrap();
        let top = ChannelPost::new(c.clone(), UserId::human(), "@dev-a, @dev-b: deploy? cc @dev-a");
        assert!(top.starts_thread());
        assert_eq!(top.thread_id.as_str(), top.id.to_string());
        assert_eq!(top.mentions, [p("dev-a"), p("dev-b")]);
        assert!(top.mentions(&p("dev-b")) && !top.mentions(&p("dev-c")));
        assert_eq!((top.turn_budget, top.in_reply_to), (DEFAULT_TURN_BUDGET, None));
        assert_eq!(top.subject(), "ants.chan.ops.human");
        assert_eq!(subject::parse_chan(&top.subject()), Some((c.clone(), UserId::human())));
        assert_eq!(top.check_subject("ants.chan.ops.human"), Ok(()));
        let err = top.check_subject("ants.chan.ops.dev-a").unwrap_err();
        assert_eq!(err.to_string(), "channel post from human in channel ops arrived on ants.chan.ops.dev-a");
        assert!(top.check_subject("ants.chan.dev.human").is_err(), "posted to another channel");

        let two = ChannelPost::new(c.clone(), UserId::human(), "and this");
        assert_ne!(two.thread_id, top.thread_id, "each top-level post starts its own thread");
        assert!(two.mentions.is_empty());
        let on = ChannelPost::on_thread(c, top.thread_id.clone(), UserId::human(), "more");
        assert!(!on.starts_thread());
        assert_eq!(on.thread_id, top.thread_id);

        let json = serde_json::to_string(&two).unwrap();
        assert!(!json.contains("mentions") && !json.contains("in_reply_to"), "{json}");
        assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), two);
        let json = serde_json::to_string(&top).unwrap();
        assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), top);
    }

    #[test]
    fn every_agent_channel_post_spends_a_turn() {
        let humans = Humans::default();
        let c: ChannelId = "ops".parse().unwrap();
        let ask = ChannelPost::new(c.clone(), UserId::human(), "@dev-a deploy?");
        let answer = ask.reply(p("dev-a"), "done, @human", &[], &humans).unwrap();
        assert_eq!((answer.channel.clone(), answer.thread_id.clone()), (c.clone(), ask.thread_id.clone()));
        assert_eq!(answer.in_reply_to, Some(ask.id));
        assert_eq!(answer.turn_budget, DEFAULT_TURN_BUDGET - 1, "waking nobody spends a turn too");
        let mut handoff = ask.reply(p("dev-a"), "@dev-b check it; @dev-a will wait", &[], &humans).unwrap();
        assert_eq!(handoff.turn_budget, DEFAULT_TURN_BUDGET - 1);
        handoff.turn_budget = 1;
        let back = handoff.reply(p("dev-b"), "@dev-a fine", &[], &humans).unwrap();
        assert_eq!(back.turn_budget, 0);
        let spent = Err(Error::BudgetExhausted(ask.thread_id));
        assert_eq!(back.reply(p("dev-a"), "@dev-b again", &[], &humans), spent);
        assert_eq!(back.reply(p("dev-a"), "@human over to you", &[], &humans), spent);
        let human = back.reply(UserId::human(), "@dev-a @dev-b go on", &[], &humans).unwrap();
        assert_eq!(human.turn_budget, DEFAULT_TURN_BUDGET, "the human's post gives a fresh budget");
        assert_eq!(
            handoff.reply(p("dev-c"), "late", &[back.clone(), human], &humans).unwrap().turn_budget,
            7,
            "reset since"
        );
        assert_eq!(
            handoff.reply(p("dev-c"), "late", &[back], &humans),
            spent,
            "an older post spends the thread's turns"
        );
        let own = ChannelPost::new(c, p("dev-a"), "@dev-b status?").spend(3).unwrap();
        assert!(own.starts_thread());
        assert_eq!(own.turn_budget, 2);
    }

    #[test]
    fn agents_woken_by_one_post_spend_the_thread_s_turns_together() {
        let humans = Humans::default();
        let c: ChannelId = "ops".parse().unwrap();
        let mut ask = ChannelPost::new(c.clone(), UserId::human(), "@dev-a @dev-b @dev-c review");
        ask.turn_budget = 4;
        let a = ask.reply(p("dev-a"), "@dev-b @dev-c fine", &[], &humans).unwrap();
        let b = ask.reply(p("dev-b"), "@dev-a @dev-c fine", std::slice::from_ref(&a), &humans).unwrap();
        let other = ChannelPost::on_thread(c, "other".parse().unwrap(), p("dev-z"), "elsewhere");
        let c_ = ask.reply(p("dev-c"), "@dev-a @dev-b fine", &[a.clone(), other, b.clone()], &humans).unwrap();
        assert_eq!([a.turn_budget, b.turn_budget, c_.turn_budget], [3, 2, 1], "one budget, run down across branches");
        let a2 = a.reply(p("dev-a"), "@dev-b again", &[b.clone(), c_.clone()], &humans).unwrap();
        assert_eq!(a2.turn_budget, 0);
        // Every post of the thread is spent out: no branch gets another turn.
        let thread = [ask, a, b, c_, a2];
        for (i, post) in thread.iter().enumerate() {
            let spent = Err(Error::BudgetExhausted(post.thread_id.clone()));
            assert_eq!(post.reply(p("dev-b"), "more", &thread[i + 1..], &humans), spent, "after {}", post.text);
        }
    }

    #[test]
    fn channel_entries_list_members_and_wake_those_mentioned() {
        let humans = Humans::default();
        let c: ChannelId = "ops".parse().unwrap();
        let mut entry = ChannelEntry::new(c.clone());
        assert!(entry.add_member(p("dev-a")) && entry.add_member(p("dev-b")) && entry.add_member(p("dev-c")));
        assert!(!entry.add_member(p("dev-a")), "already a member");
        assert!(entry.remove_member(&p("dev-c")) && !entry.remove_member(&p("dev-c")));
        assert_eq!(entry.members, [p("dev-a"), p("dev-b")]);
        assert!(entry.is_member(&p("dev-a")) && !entry.is_member(&p("dev-c")));
        assert_eq!(entry.key(), "ops");
        assert_eq!(entry.check_key("ops"), Ok(()));
        assert_eq!(entry.check_key("dev").unwrap_err().to_string(), "channel entry of channel ops found at key dev");

        let json = serde_json::to_string(&entry).unwrap();
        assert!(!json.contains("messenger"), "{json}");
        assert_eq!(serde_json::from_str::<ChannelEntry>(&json).unwrap(), entry);
        let minimal: ChannelEntry = serde_json::from_str(r#"{"channel":"ops","topic":"new"}"#).unwrap();
        assert_eq!(minimal, ChannelEntry::new(c.clone()));

        let human = UserId::human();
        let untagged = ChannelPost::new(c.clone(), human.clone(), "anyone?");
        assert_eq!(entry.woken(&untagged, &humans), [UserId::messenger()], "the default messenger triages");
        entry.messenger = Some(p("dev-c"));
        assert_eq!(entry.woken(&untagged, &humans), [p("dev-c")], "the assigned one, member or not");
        assert!(
            entry.woken(&ChannelPost::new(c.clone(), p("dev-a"), "anyone?"), &humans).is_empty(),
            "agents never fan out"
        );
        let tagged = ChannelPost::new(c, p("dev-a"), "@dev-b @dev-c @dev-a @human look");
        assert_eq!(entry.woken(&tagged, &humans), [p("dev-b")], "members only, not the author or the human");
        assert!(entry.woken(&ChannelPost::new("dev".parse().unwrap(), human, "@dev-a"), &humans).is_empty());
    }

    #[test]
    fn a_mention_notifies_human_members_and_wakes_ant_members() {
        let (c, alice, bob) = ("ops".parse::<ChannelId>().unwrap(), p("alice"), p("bob"));
        let mut humans = Humans::default();
        for h in [&alice, &bob] {
            humans.add(h.as_str(), HumanEntry::new(h.clone())).unwrap();
        }
        let mut entry = ChannelEntry::new(c.clone());
        for member in [p("dev-a"), p("dev-b"), alice.clone()] {
            entry.add_member(member);
        }
        let mixed =
            ChannelPost::new(c.clone(), p("dev-a"), "@alice @bob @dev-b @dev-c look").with_owner(Some(bob.clone()));
        assert_eq!(entry.woken(&mixed, &humans), [p("dev-b")]);
        assert_eq!(entry.notified(&mixed, &humans), std::slice::from_ref(&alice), "members only: bob is none");
        let human_only = ChannelPost::new(c.clone(), p("dev-a"), "@alice see");
        assert!(entry.woken(&human_only, &humans).is_empty(), "no agent, nor the messenger");
        let by_alice = ChannelPost::new(c.clone(), alice.clone(), "@alice @dev-b note");
        assert!(entry.notified(&by_alice, &humans).is_empty(), "not its author");
        assert_eq!(entry.woken(&by_alice, &humans), [p("dev-b")]);
        assert!(entry.notified(&ChannelPost::new("dev".parse().unwrap(), p("dev-a"), "@alice"), &humans).is_empty());

        let notice = mixed.mention_notice(&alice);
        assert_eq!((&notice.from, &notice.to, &notice.thread_id), (&p("dev-a"), &alice, &mixed.thread_id));
        assert_eq!((notice.text.as_str(), notice.turn_budget), (mixed.text.as_str(), mixed.turn_budget));
        assert_eq!((&notice.owner, &notice.creator), (&None, &None), "a notice claims no thread");
        assert_eq!(notice.mention, Some(Mention { channel: c, post: mixed.id, author: None }));
        assert_eq!(notice.subject(), subject::inbox(&alice, &p("dev-a")));
        assert_eq!(mixed.mention_msg_id(&alice), format!("mention.{}.alice", mixed.id));
        assert_eq!(mention_key(&p("dev-a"), 7, &alice), "dev-a.mention.7.alice");

        let json = serde_json::to_value(&notice).unwrap();
        assert_eq!(json["mention"]["channel"], "ops");
        let mut old = json;
        old.as_object_mut().unwrap().remove("mention");
        let read: Envelope = serde_json::from_value(old).unwrap();
        assert_eq!(read, Envelope { mention: None, ..notice }, "an envelope from before mentions reads as a message");
        let plain = serde_json::to_string(&Envelope::new(p("dev-a"), alice, "hi")).unwrap();
        assert!(!plain.contains("mention"), "{plain}");
    }

    #[test]
    fn the_messenger_relays_the_mention_notices_of_a_webhook_s_post() {
        let (c, alice) = ("ops".parse::<ChannelId>().unwrap(), p("alice"));
        let mut humans = Humans::default();
        humans.add("alice", HumanEntry::new(alice.clone())).unwrap();
        let mut entry = ChannelEntry::new(c.clone());
        entry.add_member(alice.clone());
        let hook = ChannelPost::new(c.clone(), p("github"), "deploy failed").from_webhook(vec![alice.clone()]);
        assert_eq!(entry.notified(&hook, &humans), std::slice::from_ref(&alice));
        assert_eq!(entry.notifier(&hook), UserId::messenger());
        entry.messenger = Some(p("dev-a"));
        assert_eq!(entry.notifier(&hook), p("dev-a"));
        let own = ChannelPost::new(c.clone(), p("dev-b"), "@alice look");
        assert_eq!(entry.notifier(&own), p("dev-b"), "an ant's or a human's post: its author");

        let notice = hook.relayed_notice(&alice, &p("dev-a"));
        assert_eq!((&notice.from, &notice.to, &notice.thread_id), (&p("dev-a"), &alice, &hook.thread_id));
        assert_eq!((notice.text.as_str(), notice.webhook), (hook.text.as_str(), false));
        assert_eq!(notice.mention, Some(Mention { channel: c, post: hook.id, author: Some(p("github")) }));
        assert_eq!(notice.subject(), subject::inbox(&alice, &p("dev-a")));
        let json = serde_json::to_value(&notice).unwrap();
        assert_eq!(json["mention"]["author"], "github");
        assert!(serde_json::to_value(hook.mention_notice(&alice)).unwrap()["mention"].get("author").is_none());
    }

    #[test]
    fn any_human_s_posts_count_as_human_and_threads_keep_the_owner_who_started_them() {
        let (c, alice, bob) = ("ops".parse::<ChannelId>().unwrap(), p("alice"), p("bob"));
        let mut humans = Humans::default();
        for h in [&alice, &bob] {
            humans.add(h.as_str(), HumanEntry::new(h.clone())).unwrap();
        }
        let mut entry = ChannelEntry::new(c.clone());
        entry.add_member(p("dev-a"));
        let top = ChannelPost::new(c.clone(), alice.clone(), "the build is red").with_owner(Some(alice.clone()));
        assert_eq!(entry.woken(&top, &humans), [UserId::messenger()], "alice's post goes to the messenger");
        assert!(entry.woken(&top, &Humans::default()).is_empty(), "unless alice is no human");
        let at_alice = ChannelPost::new(c.clone(), p("dev-a"), "@alice @dev-a look");
        assert!(entry.woken(&at_alice, &humans).is_empty() && !at_alice.wakes_agents(&humans));

        let answer = top.reply(p("dev-a"), "@dev-a on it", &[], &humans).unwrap();
        assert_eq!((answer.owner.as_ref(), answer.turn_budget), (Some(&alice), DEFAULT_TURN_BUDGET - 1));
        let mut spent = answer.reply(p("dev-b"), "@dev-a done", &[], &humans).unwrap();
        spent.turn_budget = 1;
        let by_bob = spent.reply(bob.clone(), "@dev-a again", &[], &humans).unwrap();
        assert_eq!((by_bob.owner.as_ref(), by_bob.turn_budget), (Some(&alice), DEFAULT_TURN_BUDGET), "fresh, alice's");
        assert_eq!(spent.turns_left(std::slice::from_ref(&by_bob), &humans), DEFAULT_TURN_BUDGET);

        // A thread from before owners: its human author owns it, else the first human to answer on it.
        let old = ChannelPost::new(c.clone(), alice.clone(), "old");
        assert_eq!(old.reply(p("dev-a"), "a", &[], &humans).unwrap().owner, Some(alice.clone()));
        let agents = ChannelPost::new(c, p("dev-a"), "@dev-b status?");
        assert_eq!(agents.reply(p("dev-b"), "b", &[], &humans).unwrap().owner, None);
        assert_eq!(agents.reply(bob.clone(), "b", &[], &humans).unwrap().owner, Some(bob));
        // Only a human owns a thread: an owner an ant names is ignored.
        let named = ChannelPost::new("ops".parse().unwrap(), p("dev-a"), "@dev-b go").with_owner(Some(p("dev-a")));
        assert_eq!(named.thread_owner(&humans), None);
        assert_eq!(named.reply(p("dev-b"), "b", &[], &humans).unwrap().owner, None);

        let json = serde_json::to_string(&old).unwrap();
        assert!(!json.contains("owner"), "{json}");
        assert!(serde_json::to_string(&top).unwrap().contains(r#""owner":"alice""#));
        assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), old, "posts without an owner still read");
    }

    #[test]
    fn a_webhook_s_post_that_mentions_nobody_is_triaged_and_its_mentions_come_from_the_ingress() {
        let humans = Humans::default();
        let c: ChannelId = "ops".parse().unwrap();
        let mut entry = ChannelEntry::new(c.clone());
        entry.add_member(p("dev-a"));
        let hook = p("github");
        let untagged = ChannelPost::new(c.clone(), hook.clone(), "@dev-a push to main").from_webhook(Vec::new());
        assert!(untagged.mentions.is_empty(), "never parsed from the text");
        assert!(untagged.to_triage(&humans));
        assert_eq!(entry.woken(&untagged, &humans), [UserId::messenger()]);
        let tagged = ChannelPost::new(c, hook, "push").from_webhook(vec![p("dev-a"), p("dev-b")]);
        assert_eq!(entry.woken(&tagged, &humans), [p("dev-a")], "members only");

        let json = serde_json::to_string(&untagged).unwrap();
        assert!(json.contains(r#""webhook":true"#), "{json}");
        assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), untagged);
        let reply = untagged.reply(p("dev-a"), "on it", &[], &humans).unwrap();
        assert!(
            !reply.webhook && !serde_json::to_string(&reply).unwrap().contains("webhook"),
            "replies are no webhook's"
        );
    }

    #[test]
    fn a_webhook_under_a_human_s_name_is_no_human() {
        let (c, bob) = ("ops".parse::<ChannelId>().unwrap(), p("bob"));
        let mut humans = Humans::default();
        humans.add(bob.as_str(), HumanEntry::new(bob.clone())).unwrap();
        let hook = ChannelPost::new(c.clone(), bob.clone(), "red").from_webhook(Vec::new());
        assert!(!hook.by_human(&humans) && ChannelPost::new(c.clone(), bob.clone(), "hi").by_human(&humans));
        assert_eq!(hook.thread_owner(&humans), None, "it owns no thread");
        let mut spent = hook.clone();
        spent.turn_budget = 1;
        let again = ChannelPost::on_thread(c, hook.thread_id, bob.clone(), "red").from_webhook(Vec::new());
        assert_eq!(spent.turns_left(&[again], &humans), 1, "nor renews its turns");

        let message = Envelope::new(bob.clone(), p("dev-a"), "red");
        assert_eq!(
            (message.sender_role(&humans), message.clone().from_webhook().sender_role(&humans)),
            (Role::Human, Role::Ant)
        );
        assert!(!serde_json::to_string(&message).unwrap().contains("webhook"));
        let json = serde_json::to_string(&message.clone().from_webhook()).unwrap();
        assert!(json.contains(r#""webhook":true"#), "{json}");
        assert!(serde_json::from_str::<Envelope>(&json).unwrap().webhook);
        let reply = message.from_webhook().reply(p("dev-a"), "on it", Role::Human).unwrap();
        assert!(!reply.webhook, "replies are no webhook's");
    }

    #[test]
    fn a_thread_s_origin_comes_from_its_posts_not_from_what_a_later_post_carries() {
        let (c, alice, bob) = ("ops".parse::<ChannelId>().unwrap(), p("alice"), p("bob"));
        let mut humans = Humans::default();
        for h in [&alice, &bob] {
            humans.add(h.as_str(), HumanEntry::new(h.clone())).unwrap();
        }
        let on =
            |from: &UserId, thread: &ThreadId| ChannelPost::on_thread(c.clone(), thread.clone(), from.clone(), "x");
        let root = ChannelPost::new(c.clone(), p("lead"), "@dev-a fix it").with_owner(Some(bob.clone()));
        let thread = root.thread_id.clone();
        let forged = on(&p("dev-a"), &thread).with_owner(Some(bob.clone())).with_creator(Some(p("boss")));
        let mut origin = ThreadOrigin::default();
        origin.push(&root, &humans);
        assert_eq!((&origin.owner, &origin.creator, origin.root), (&None, &Some(p("lead")), true), "an ant owns none");
        origin.push(&forged, &humans);
        // A later post with the thread's id as its own only claims to start it.
        let mut fake = on(&p("dev-b"), &thread);
        fake.id = root.id;
        origin.push(&fake, &humans);
        assert_eq!((&origin.owner, &origin.creator, origin.settled()), (&None, &Some(p("lead")), false));
        // The first human to post owns the thread.
        origin.push(&on(&alice, &thread), &humans);
        origin.push(&on(&bob, &thread), &humans);
        assert_eq!((&origin.owner, origin.settled()), (&Some(alice.clone()), true));
        let mut post = forged.clone();
        origin.apply(&mut post);
        assert_eq!((post.owner, post.creator), (Some(alice.clone()), Some(p("lead"))));

        // With its top-level post no longer kept, the thread has no creator.
        let mut lost = ThreadOrigin::default();
        lost.push(&forged, &humans);
        assert_eq!((&lost.owner, &lost.creator, lost.root), (&None, &None, false));
        // A webhook's top-level post names the owner its config does, a human; a human's owns its thread.
        let hook = ChannelPost::new(c.clone(), p("ci"), "red").from_webhook(Vec::new());
        let of = |post: ChannelPost| {
            let origin = ThreadOrigin::of_root(&post, &humans);
            (origin.owner, origin.creator)
        };
        assert_eq!(of(hook.clone().with_owner(Some(alice.clone()))), (Some(alice.clone()), None));
        assert_eq!(of(hook.with_owner(Some(p("dev-a")))), (None, None));
        assert_eq!(of(ChannelPost::new(c, bob.clone(), "hi").with_owner(Some(alice))), (Some(bob), None));
    }

    #[test]
    fn threads_keep_the_ant_that_created_them_and_only_its_own_post_makes_it_so() {
        let (c, alice) = ("ops".parse::<ChannelId>().unwrap(), p("alice"));
        let mut humans = Humans::default();
        humans.add(alice.as_str(), HumanEntry::new(alice.clone())).unwrap();
        // A top-level post's creator is its author, whatever it says.
        let top = ChannelPost::new(c.clone(), p("lead"), "@dev-a fix it").with_creator(Some(p("boss")));
        assert_eq!(top.thread_creator(&humans), Some(p("lead")));
        assert_eq!(ChannelPost::new(c.clone(), alice.clone(), "hi").thread_creator(&humans), None, "humans own");
        let hook = ChannelPost::new(c.clone(), p("ci"), "red").from_webhook(vec![p("dev-a")]);
        assert_eq!(hook.thread_creator(&humans), None, "a webhook is no ant");
        let manager = ChannelPost::new(c.clone(), UserId::channel_manager(), "dev-b joined");
        assert_eq!(manager.thread_creator(&humans), None, "nor is the channel manager");
        let answer = top.reply(p("dev-a"), "@dev-b help", &[], &humans).unwrap();
        assert_eq!((answer.creator.as_ref(), answer.owner.as_ref()), (Some(&p("lead")), None));
        let by_alice = answer.reply(alice.clone(), "@dev-a go on", &[], &humans).unwrap();
        assert_eq!((by_alice.creator.as_ref(), by_alice.owner.as_ref()), (Some(&p("lead")), Some(&alice)));
        assert_eq!(by_alice.reply(p("dev-b"), "ok", &[], &humans).unwrap().creator, Some(p("lead")));
        // A later post keeps the creator it carries, unless a human.
        let named = ChannelPost::on_thread(c, top.thread_id.clone(), p("dev-b"), "x").with_creator(Some(alice.clone()));
        assert_eq!(named.thread_creator(&humans), None);

        // Messages on or from the thread keep it too.
        let msg = Envelope::new(p("lead"), p("dev-a"), "q").with_creator(Some(p("lead")));
        assert_eq!(msg.reply(p("dev-a"), "a", Role::Ant).unwrap().creator, Some(p("lead")));
        assert_eq!(msg.follow_up(p("dev-a"), p("human"), "n").creator, Some(p("lead")));

        let json = serde_json::to_string(&ChannelPost::new("ops".parse().unwrap(), p("lead"), "x")).unwrap();
        assert!(!json.contains("creator"), "{json}");
        assert!(serde_json::to_string(&top).unwrap().contains(r#""creator":"boss""#));
        assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap().creator, None, "older posts read");
        let older = r#"{"id":"0190a5c4-1d2e-7f00-8000-000000000001","thread_id":"t1","from":"human","to":"dev-a",
            "text":"hi","turn_budget":8,"sent_at":"2026-01-01T00:00:00Z"}"#;
        assert_eq!(serde_json::from_str::<Envelope>(older).unwrap().creator, None);
    }
}
