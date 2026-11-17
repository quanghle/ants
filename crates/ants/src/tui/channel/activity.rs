//! What happened lately in each channel, for the channel view's activity pane: new threads and replies at the times
//! the stream stored them, and what comparing successive reads tells, at the time it is noticed: reactions to the
//! posts watched, changes to the channel's members, messenger, owner and description, and threads coming to be done,
//! stuck or waiting on the human.

use std::collections::{BTreeSet, HashMap};

use ants_daemon::records::reactions::Reactions;
use ants_proto::{ChannelEntry, ChannelId, Reaction, ThreadId, UserId, Uuid};
use chrono::{DateTime, TimeDelta, Utc};

use super::Feeds;
use crate::threads::thread::{Entry, Kind};
use crate::tui::model::Data;
use crate::tui::work::ThreadState;

/// The most events kept of each channel.
const KEPT: usize = 50;
/// How long after the threads' states are first looked at their changes only tell how things stood: what `ants chat`
/// reads as it starts (the stream, the read marker, the done marks) comes in meanwhile.
const SETTLE: TimeDelta = TimeDelta::seconds(15);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum What {
    /// A new thread, by its opening post.
    Posted(ThreadId),
    Replied(ThreadId),
    /// A reaction to a thread's opening post.
    Reacted(ThreadId, Reaction),
    /// A thread came to this state.
    State(ThreadId, ThreadState),
    /// Members added, and members removed.
    Members(Vec<UserId>, Vec<UserId>),
    /// The messenger set, or with none back to the default.
    Messenger(Option<UserId>),
    /// The owner set, or with none cleared.
    Owner(Option<UserId>),
    Description,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub at: DateTime<Utc>,
    /// Who did it, when known: never for what the channels' entries and the threads' states tell.
    pub by: Option<String>,
    pub what: What,
}

#[derive(Debug, Default)]
pub struct Activity {
    /// Each channel's latest events, oldest first.
    events: HashMap<ChannelId, Vec<Event>>,
    /// Who reacted how to each post watched, as last read.
    reacted: HashMap<Uuid, BTreeSet<(Reaction, UserId)>>,
    /// Each channel thread's state, as last looked at.
    states: HashMap<ThreadId, ThreadState>,
    /// When the threads' states were first looked at.
    first_look: Option<DateTime<Utc>>,
}

impl Activity {
    /// The channel's latest events, newest first.
    pub fn of(&self, channel: &ChannelId) -> impl Iterator<Item = &Event> {
        self.events.get(channel).into_iter().flat_map(|e| e.iter().rev())
    }

    fn push(&mut self, channel: &ChannelId, event: Event) {
        let events = self.events.entry(channel.clone()).or_default();
        let at = events.partition_point(|e| e.at <= event.at);
        events.insert(at, event);
        if events.len() > KEPT {
            events.remove(0);
        }
    }

    /// Take a message read, once `feeds` took it: on a channel's thread, a new thread or a reply.
    pub fn stored(&mut self, entry: &Entry, feeds: &Feeds) {
        if !matches!(entry.kind, Kind::Message | Kind::Post) {
            return;
        }
        let Some(channel) = feeds.channel(&entry.thread_id) else { return };
        let thread = entry.thread_id.clone();
        let what = if Feeds::opens(entry) { What::Posted(thread) } else { What::Replied(thread) };
        self.push(channel, Event { at: entry.stored_at, by: Some(entry.sender()), what });
    }

    /// Take the channels read at `now` after `before`: what changed in those in both.
    pub fn directory(&mut self, before: &[ChannelEntry], after: &[ChannelEntry], now: DateTime<Utc>) {
        for new in after {
            let Some(old) = before.iter().find(|c| c.channel == new.channel) else { continue };
            let added: Vec<UserId> = new.members.iter().filter(|m| !old.members.contains(m)).cloned().collect();
            let removed: Vec<UserId> = old.members.iter().filter(|m| !new.members.contains(m)).cloned().collect();
            let mut whats = Vec::new();
            if !added.is_empty() || !removed.is_empty() {
                whats.push(What::Members(added, removed));
            }
            if old.messenger != new.messenger {
                whats.push(What::Messenger(new.messenger.clone()));
            }
            if old.owner != new.owner {
                whats.push(What::Owner(new.owner.clone()));
            }
            if old.description != new.description {
                whats.push(What::Description);
            }
            for what in whats {
                self.push(&new.channel, Event { at: now, by: None, what });
            }
        }
    }

    /// Take the reactions read at `now` to the messages `watched`: those to a channel's opening posts that the last
    /// read of them did not have, the first read of a post only telling how it stood.
    pub fn reactions(&mut self, watched: &[Uuid], read: &Reactions, feeds: &Feeds, now: DateTime<Utc>) {
        for id in watched {
            let Some(thread) = id.to_string().parse::<ThreadId>().ok() else { continue };
            // Only an opening post's id is that of a thread.
            let Some(channel) = feeds.channel(&thread) else { continue };
            let by: BTreeSet<(Reaction, UserId)> = read
                .get(id)
                .into_iter()
                .flatten()
                .flat_map(|r| r.by.iter().map(|by| (r.reaction.clone(), by.clone())))
                .collect();
            let new: Vec<(Reaction, UserId)> = match self.reacted.get(id) {
                Some(old) => by.difference(old).cloned().collect(),
                None => Vec::new(),
            };
            for (reaction, by) in new {
                let what = What::Reacted(thread.clone(), reaction);
                self.push(channel, Event { at: now, by: Some(by.to_string()), what });
            }
            self.reacted.insert(*id, by);
        }
    }

    /// Look at `now` at the states of the threads of the human's channels: those that came to be done, stuck or
    /// waiting on the human since the last look, but while the first look settles.
    pub fn look(&mut self, data: &Data, now: DateTime<Utc>) {
        let settled = now - *self.first_look.get_or_insert(now) >= SETTLE;
        for channel in &data.channels {
            for post in data.feeds.posts(&channel.channel) {
                let state = data.state(&post.thread);
                let before = match self.states.get_mut(&post.thread) {
                    Some(before) if *before == state => continue,
                    Some(before) => Some(std::mem::replace(before, state)),
                    None => self.states.insert(post.thread.clone(), state),
                };
                let told = matches!(state, ThreadState::Done | ThreadState::Stuck | ThreadState::WaitingOnMe);
                if settled && before.is_some() && told {
                    let what = What::State(post.thread.clone(), state);
                    self.push(&channel.channel, Event { at: now, by: None, what });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{ChannelPost, HumanEntry, Humans, Reacted};

    use super::*;
    use crate::test_support::live::{at, message, user};
    use crate::tui::model::Model;
    use crate::tui::reads::Read;
    use crate::tui::test_support::{model, post};

    type Told = (DateTime<Utc>, Option<String>, What);

    fn told(m: &Model, channel: &ChannelId) -> Vec<Told> {
        m.data.activity.of(channel).map(|e| (e.at, e.by.clone(), e.what.clone())).collect()
    }

    fn humans() -> Humans {
        Humans::from_entries(["alice", "bob"].map(|h| (h, HumanEntry::new(user(h)))))
    }

    #[test]
    fn new_threads_and_replies_come_newest_first_at_their_stored_times() {
        let ops: ChannelId = "ops".parse().unwrap();
        let first = ChannelPost::new(ops.clone(), user("bob"), "the site is down");
        let reply = ChannelPost::on_thread(ops.clone(), first.thread_id.clone(), user("dev-a"), "looking");
        let answer = message(3, &first.thread_id, "dev-a", "bob", "fixed");
        let elsewhere = message(4, &ThreadId::generate(), "dev-a", "bob", "not on a channel");
        let mut m = model();
        m.stored(vec![post(1, &first), post(2, &reply), answer, elsewhere], at(10));
        let thread = first.thread_id;
        assert_eq!(
            told(&m, &ops),
            [
                (at(3), Some("dev-a".to_owned()), What::Replied(thread.clone())),
                (at(2), Some("dev-a".to_owned()), What::Replied(thread.clone())),
                (at(1), Some("bob".to_owned()), What::Posted(thread)),
            ]
        );
    }

    #[test]
    fn changes_to_a_channels_entry_tell_at_the_read_that_notices_them_with_no_author() {
        let ops: ChannelId = "ops".parse().unwrap();
        let entry =
            |members: &[&str], messenger: Option<&str>, owner: Option<&str>, description: Option<&str>| ChannelEntry {
                members: members.iter().map(|m| user(m)).collect(),
                messenger: messenger.map(user),
                owner: owner.map(user),
                description: description.map(str::to_owned),
                ..ChannelEntry::new(ops.clone())
            };
        let mut m = model();
        let first = entry(&["alice", "carol"], None, None, None);
        m.read(Read::Directory(Ok((vec![first.clone()], humans()))), at(1));
        m.read(Read::Directory(Ok((vec![first], humans()))), at(2));
        assert_eq!(told(&m, &ops), [], "the first read, then nothing changed");
        let changed = entry(&["alice", "dev-a"], Some("dev-m"), Some("dev-a"), Some("ops"));
        m.read(Read::Directory(Ok((vec![changed], humans()))), at(3));
        assert_eq!(
            told(&m, &ops),
            [
                (at(3), None, What::Description),
                (at(3), None, What::Owner(Some(user("dev-a")))),
                (at(3), None, What::Messenger(Some(user("dev-m")))),
                (at(3), None, What::Members(vec![user("dev-a")], vec![user("carol")])),
            ]
        );
    }

    #[test]
    fn reactions_to_opening_posts_tell_who_reacted_since_the_last_read_of_them() {
        let ops: ChannelId = "ops".parse().unwrap();
        let first = ChannelPost::new(ops.clone(), user("bob"), "the site is down");
        let reply = ChannelPost::on_thread(ops.clone(), first.thread_id.clone(), user("dev-a"), "looking");
        let mut m = model();
        m.stored(vec![post(1, &first), post(2, &reply)], at(2));
        let eyes: Reaction = "eyes".parse().unwrap();
        let read = |by: &[&str]| -> Reactions {
            let reacted = Reacted::new(eyes.clone(), by.iter().map(|b| user(b)).collect());
            [(first.id, vec![reacted.clone()]), (reply.id, vec![reacted])].into()
        };
        let watched = vec![first.id, reply.id];
        m.read(Read::Reactions(watched.clone(), read(&["bob"])), at(3));
        m.read(Read::Reactions(watched.clone(), read(&["bob", "dev-a"])), at(4));
        m.read(Read::Reactions(watched, read(&["dev-a"])), at(5));
        let mut reacted = told(&m, &ops);
        reacted.retain(|(_, _, what)| matches!(what, What::Reacted(..)));
        assert_eq!(
            reacted,
            [(at(4), Some("dev-a".to_owned()), What::Reacted(first.thread_id, eyes))],
            "bob's as it stood, and none to a reply"
        );
    }

    #[test]
    fn threads_coming_to_be_done_stuck_or_waiting_on_the_human_tell_once_the_first_look_settles() {
        let ops: ChannelId = "ops".parse().unwrap();
        let mut m = model();
        let member = ChannelEntry { members: vec![user("alice")], ..ChannelEntry::new(ops.clone()) };
        m.read(Read::Directory(Ok((vec![member], humans()))), at(0));
        let early = ChannelPost::new(ops.clone(), user("bob"), "deploy at noon");
        let late = ChannelPost::new(ops.clone(), user("bob"), "the site is down");
        m.stored(vec![post(1, &early), post(2, &late)], at(2));
        m.look(at(2));
        m.stored(vec![message(3, &early.thread_id, "dev-a", "alice", "deployed")], at(3));
        m.look(at(3));
        let states = |m: &Model| {
            let mut said = told(m, &ops);
            said.retain(|(_, _, what)| matches!(what, What::State(..)));
            said
        };
        assert_eq!(states(&m), [], "settling");
        m.stored(vec![message(4, &late.thread_id, "dev-a", "alice", "fixed")], at(20));
        m.look(at(20));
        m.data.marker.as_mut().unwrap().mark(4);
        m.look(at(21));
        assert_eq!(
            states(&m),
            [(at(20), None, What::State(late.thread_id, ThreadState::WaitingOnMe))],
            "out of waiting on the human tells nothing"
        );
    }
}
