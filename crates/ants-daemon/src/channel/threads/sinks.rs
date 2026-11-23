//! What a read of a channel's posts, in stream order, is folded into: the context of a prompt, a thread's origin, a
//! post's author, a thread's channel, or whether a sender takes part in a thread.

use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::time::SystemTime;

use ants_proto::{ChannelEntry, ChannelId, ChannelPost, Humans, ThreadId, ThreadOrigin, UserId, Uuid};

use super::{Context, MAX_CONTEXT};

/// Picks the context of a prompt from the posts of a channel, read in stream order.
pub(super) struct Collect<'a> {
    ant: &'a UserId,
    humans: &'a Humans,
    thread: &'a ThreadId,
    /// Stream sequence of the post being answered: the context ends before it, and the later posts start after it.
    at: u64,
    /// The post the one being answered answers.
    answering: Option<Uuid>,
    posts: VecDeque<ChannelPost>,
    skipped: usize,
    later: Vec<ChannelPost>,
    /// Whether the post the one being answered answers is the ant's, once read.
    pub(super) answered_own: Option<bool>,
}

impl<'a> Collect<'a> {
    pub(super) fn new(
        ant: &'a UserId,
        humans: &'a Humans,
        thread: &'a ThreadId,
        at: u64,
        answering: Option<Uuid>,
    ) -> Self {
        let (posts, later) = (VecDeque::new(), Vec::new());
        Self { ant, humans, thread, at, answering, posts, skipped: 0, later, answered_own: None }
    }

    /// Take the post at stream sequence `seq`.
    fn push(&mut self, seq: u64, post: ChannelPost) {
        if post.thread_id != *self.thread || seq == self.at {
            return;
        }
        if seq > self.at {
            // A human's post gives the thread a fresh budget: the ones before it no longer count.
            if post.by_human(self.humans) {
                self.later.clear();
            }
            self.later.push(post);
            return;
        }
        if self.answering == Some(post.id) {
            self.answered_own = Some(post.from == *self.ant);
        }
        if post.from == *self.ant {
            return;
        }
        if self.posts.len() == MAX_CONTEXT {
            self.posts.pop_front();
            self.skipped += 1;
        }
        self.posts.push_back(post);
    }

    pub(super) fn done(self) -> Context {
        let answers_own = self.answered_own == Some(true);
        Context { posts: self.posts.into(), skipped: self.skipped, later: self.later, answers_own, last_seq: 0 }
    }
}

/// Takes the posts of a channel, read in stream order.
pub(super) trait Sink {
    /// Take the post at stream sequence `seq`, stored at `stored`; a break ends the read.
    fn take(&mut self, seq: u64, stored: SystemTime, post: ChannelPost) -> ControlFlow<()>;
}

impl Sink for Collect<'_> {
    fn take(&mut self, seq: u64, _: SystemTime, post: ChannelPost) -> ControlFlow<()> {
        self.push(seq, post);
        ControlFlow::Continue(())
    }
}

/// Folds the posts of one thread, up to stream sequence `upto`, into its [`ThreadOrigin`].
pub(super) struct Origin<'a> {
    pub(super) thread: &'a ThreadId,
    pub(super) humans: &'a Humans,
    pub(super) upto: u64,
    pub(super) fold: ThreadOrigin,
}

impl Sink for Origin<'_> {
    fn take(&mut self, seq: u64, _: SystemTime, post: ChannelPost) -> ControlFlow<()> {
        if seq > self.upto {
            return ControlFlow::Break(());
        }
        if post.thread_id == *self.thread {
            self.fold.push(&post, self.humans);
        }
        if self.fold.settled() { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
    }
}

/// Finds the author of post `id` on `thread`, among the posts stored before stream sequence `before`.
pub(super) struct Author<'a> {
    pub(super) thread: &'a ThreadId,
    pub(super) id: Uuid,
    pub(super) before: u64,
    pub(super) from: Option<UserId>,
}

impl Sink for Author<'_> {
    fn take(&mut self, seq: u64, _: SystemTime, post: ChannelPost) -> ControlFlow<()> {
        if seq >= self.before {
            return ControlFlow::Break(());
        }
        if post.thread_id == *self.thread && post.id == self.id {
            self.from = Some(post.from);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// Finds the channel of the top-level post that starts `thread`, among the posts of every channel stored up to stream
/// sequence `upto` and before `until`.
pub(super) struct Root<'a> {
    pub(super) thread: &'a ThreadId,
    pub(super) upto: u64,
    pub(super) until: SystemTime,
    pub(super) channel: Option<ChannelId>,
}

impl Sink for Root<'_> {
    fn take(&mut self, seq: u64, stored: SystemTime, post: ChannelPost) -> ControlFlow<()> {
        if seq > self.upto || stored > self.until {
            return ControlFlow::Break(());
        }
        if post.thread_id == *self.thread && post.starts_thread() {
            self.channel = Some(post.channel);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// Finds whether a post on `thread`, stored up to stream sequence `upto`, is by `sender` or wakes it, a member or the
/// messenger of the thread's channel, whose `entry` it is.
#[derive(Clone, Copy)]
pub(super) struct Part<'a> {
    pub(super) thread: &'a ThreadId,
    pub(super) sender: &'a UserId,
    pub(super) entry: &'a ChannelEntry,
    pub(super) humans: &'a Humans,
    pub(super) upto: u64,
    pub(super) found: bool,
}

impl Sink for Part<'_> {
    fn take(&mut self, seq: u64, _: SystemTime, post: ChannelPost) -> ControlFlow<()> {
        if seq > self.upto {
            return ControlFlow::Break(());
        }
        if post.thread_id == *self.thread
            && (post.from == *self.sender || self.entry.woken(&post, self.humans).contains(self.sender))
        {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::tests::{humans, p, post};

    #[test]
    fn the_context_is_the_latest_posts_of_the_thread_by_others_before_the_post() {
        let (a, root, humans) = (p("dev-a"), post("human", "deploy"), humans());
        let on = |from: &str, text: &str| {
            ChannelPost::on_thread(root.channel.clone(), root.thread_id.clone(), p(from), text)
        };
        let mut collect = Collect::new(&a, &humans, &root.thread_id, 1000, None);
        collect.push(1, root.clone());
        collect.push(2, post("human", "another thread"));
        collect.push(3, on("dev-a", "its own"));
        for i in 0..MAX_CONTEXT {
            collect.push(4 + i as u64, on("dev-b", &format!("note {i}")));
        }
        collect.push(1000, on("human", "@dev-a the post itself"));
        let context = collect.done();
        assert_eq!(context.skipped, 1, "the root is the oldest, left out");
        assert_eq!(context.posts.len(), MAX_CONTEXT);
        assert_eq!(context.posts[0].text, "note 0");
        assert!(context.posts.iter().all(|p| p.thread_id == root.thread_id && p.from.as_str() == "dev-b"));
        assert!(context.later.is_empty());
    }

    #[test]
    fn the_later_posts_are_the_thread_s_since_the_post_or_the_human_s_last() {
        let (a, root, humans) = (p("dev-a"), post("human", "deploy"), humans());
        let on = |from: &str, text: &str| {
            ChannelPost::on_thread(root.channel.clone(), root.thread_id.clone(), p(from), text)
        };
        let mut collect = Collect::new(&a, &humans, &root.thread_id, 5, None);
        collect.push(5, root.clone());
        collect.push(6, on("dev-b", "first"));
        collect.push(7, post("human", "another thread"));
        collect.push(8, on("dev-a", "its own counts too"));
        let later = |c: &Collect| c.later.iter().map(|p| p.text.clone()).collect::<Vec<_>>();
        assert_eq!(later(&collect), ["first", "its own counts too"]);
        collect.push(9, on("human", "go on"));
        collect.push(10, on("dev-c", "then"));
        assert_eq!(later(&collect), ["go on", "then"], "the human's post resets the budget");
        collect.push(11, on("alice", "and on"));
        assert_eq!(later(&collect), ["and on"], "any human's does");
        assert!(collect.done().posts.is_empty());
    }

    #[test]
    fn the_context_tells_whether_the_post_answers_one_of_the_ant_s_own() {
        let (a, root, humans) = (p("dev-a"), post("human", "deploy"), humans());
        let on = |from: &str| ChannelPost::on_thread(root.channel.clone(), root.thread_id.clone(), p(from), "x");
        let (own, other) = (on("dev-a"), on("dev-b"));
        let answers = |answering: &ChannelPost, upto: u64| {
            let mut collect = Collect::new(&a, &humans, &root.thread_id, 10, Some(answering.id));
            for (seq, post) in [(1, &root), (2, &own), (3, &other)].into_iter().filter(|(seq, _)| *seq <= upto) {
                collect.push(seq, post.clone());
            }
            collect.push(11, on("dev-a"));
            collect.done().answers_own
        };
        assert!(answers(&own, 3));
        assert!(!answers(&other, 3), "another's post");
        assert!(!answers(&root, 3), "a human's post");
        assert!(!answers(&own, 1), "the ant's post not read: looked up by `Threads::context`");
    }
}
