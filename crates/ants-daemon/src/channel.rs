//! Channels as a member's daemon sees them.
//!
//! The daemon watches the channels bucket ([`Channels`]) and takes every channel post through its consumer
//! `chan-<ant>`. A post wakes the ant only when [`wake`] says so; the rest are acked without a prompt. A prompt
//! on a channel thread shows the posts on the thread the ant has not been shown yet ([`Threads::context`]): the
//! stream sequence of the last post shown is kept per thread in the sessions bucket, so a restarted daemon goes on from
//! there. The reply is posted on the same thread, spending one of its turns.
//!
//! A post of a human or a webhook that @mentions nobody goes to the channel's messenger instead, to triage: its answer @mentions
//! the members the post is relevant to, which wakes them ([`triage_prompt`]). Who is a human is the daemon's
//! [`crate::people::humans::Registry`]'s to say.

mod entries;
mod prompt;
mod threads;

use ants_proto::{ChannelEntry, ChannelPost, Envelope, Humans, UserId, Uuid};

pub use entries::{Channels, Stored, list, load};
pub use prompt::{TRIAGE_RULES, description_note, owner_note, prompt, quoted, resume_note, triage_prompt};
pub(crate) use threads::from_start;
pub use threads::{CLOCK_SLACK, Context, Earlier, Threads, read_back, thread_start};

pub use crate::stream::reading::Reading;

/// What a channel post asks of an ant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wake {
    /// Nothing: it is acked without a prompt, and shown with the ant's next prompt on its thread.
    Skip,
    /// An answer: the post @mentions the ant, a member.
    Mentioned,
    /// A triage: the post is a human's or a webhook's, mentions nobody, and the ant is the channel's messenger.
    Triage,
}

/// What `post` asks of `ant`, given the entry of its channel and the `humans`: a prompt only when it @mentions the
/// ant, a member, or when the ant is the channel's messenger and it is a post of a human or a webhook that mentions
/// nobody;
/// never for the ant's own.
pub fn wake(ant: &UserId, entry: Option<&ChannelEntry>, post: &ChannelPost, humans: &Humans) -> Wake {
    let Some(entry) = entry else { return Wake::Skip };
    if post.from == *ant || !entry.woken(post, humans).contains(ant) {
        Wake::Skip
    } else if post.mentions.is_empty() {
        Wake::Triage
    } else {
        Wake::Mentioned
    }
}

/// `post` as an inbox message from its author to `ant`, owned by the thread's owner the post carries, and created by
/// its creator, which the daemon took from the thread's posts ([`Threads::origin`]): what the attempt markers, caps,
/// escalations and tools of the thread go by. A webhook's post stays a webhook's ([`Envelope::webhook`]), and the post's
/// tool rules stay its ([`Envelope::allow`]).
pub fn as_envelope(post: &ChannelPost, ant: &UserId) -> Envelope {
    Envelope {
        id: post.id,
        thread_id: post.thread_id.clone(),
        from: post.from.clone(),
        to: ant.clone(),
        text: post.text.clone(),
        turn_budget: post.turn_budget,
        in_reply_to: None,
        quote: post.quote.clone(),
        owner: post.owner.clone(),
        creator: post.creator.clone(),
        origin_thread: None,
        escalation: None,
        approval: None,
        on_behalf_of: post.on_behalf_of.clone(),
        webhook: post.webhook,
        provider: None,
        queue: post.queue,
        allow: post.allow.clone(),
        mention: None,
        title: None,
        summary: None,
        question: None,
        answer: None,
        // Stored under the post's channel handles, which an inbox message's would not name.
        attachments: Vec::new(),
        sealed: None,
        notice: None,
        review_commits: Vec::new(),
        trail_step: None,
        trail_spent: None,
        trail_start: None,
        rating_of: None,
        review_outcomes: None,
        sent_at: post.sent_at,
    }
}

/// The inbox thread a top-level MCP post came from, in the author's own sessions keys, not in the channel post.
pub(crate) fn review_source_key(ant: &UserId, post: Uuid) -> String {
    format!("{ant}.{post}.review_source")
}

#[cfg(test)]
mod tests {
    use super::*;
    pub(super) use ants_test_support::user as p;

    pub(super) fn ops() -> ChannelEntry {
        let mut entry = ChannelEntry::new("ops".parse().unwrap());
        entry.add_member(p("dev-a"));
        entry.add_member(p("dev-b"));
        entry
    }

    /// The humans: `human`, and alice, from the humans bucket.
    pub(super) fn humans() -> Humans {
        Humans::from_entries([
            ("alice", ants_proto::HumanEntry::new(p("alice"))),
            ("human", ants_proto::HumanEntry::new(p("human"))),
        ])
    }

    pub(super) fn post(from: &str, text: &str) -> ChannelPost {
        ChannelPost::new("ops".parse().unwrap(), p(from), text)
    }

    fn wakes(ant: &UserId, entry: Option<&ChannelEntry>, post: &ChannelPost) -> Wake {
        wake(ant, entry, post, &humans())
    }

    #[test]
    fn only_a_post_mentioning_a_member_wakes_it() {
        let (entry, a) = (ops(), p("dev-a"));
        assert_eq!(wakes(&a, Some(&entry), &post("human", "@dev-a deploy")), Wake::Mentioned);
        assert_eq!(wakes(&a, Some(&entry), &post("dev-b", "@dev-b @dev-a check")), Wake::Mentioned, "ants too");
        assert_eq!(wakes(&a, Some(&entry), &post("human", "@dev-b deploy")), Wake::Skip, "another member");
        assert_eq!(wakes(&a, Some(&entry), &post("human", "deploy")), Wake::Skip, "nobody mentioned");
        assert_eq!(wakes(&a, Some(&entry), &post("dev-a", "@dev-a note to self")), Wake::Skip, "its own post");
        assert_eq!(wakes(&a, None, &post("human", "@dev-a deploy")), Wake::Skip, "no such channel");
        assert_eq!(wakes(&p("dev-c"), Some(&entry), &post("human", "@dev-c deploy")), Wake::Skip, "no member");
        let other = ChannelPost::new("dev".parse().unwrap(), ants_test_support::human(), "@dev-a deploy");
        assert_eq!(wakes(&a, Some(&entry), &other), Wake::Skip, "another channel's entry");
    }

    #[test]
    fn the_messenger_triages_the_human_posts_that_mention_nobody() {
        let (mut entry, messenger) = (ops(), UserId::messenger());
        let untagged = post("human", "the build is red");
        assert_eq!(wakes(&messenger, Some(&entry), &untagged), Wake::Triage, "the default messenger, no member");
        assert_eq!(wakes(&p("dev-a"), Some(&entry), &untagged), Wake::Skip, "members wait for its mentions");
        assert_eq!(wakes(&messenger, Some(&entry), &post("dev-a", "the build is red")), Wake::Skip, "ants' posts");
        assert_eq!(wakes(&messenger, Some(&entry), &post("human", "@dev-a the build")), Wake::Skip, "mentions");
        assert_eq!(wakes(&messenger, Some(&entry), &post("human", "@human the build")), Wake::Skip, "mentions");
        entry.messenger = Some(p("dev-b"));
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &untagged), Wake::Triage, "the assigned one");
        assert_eq!(wakes(&messenger, Some(&entry), &untagged), Wake::Skip, "not the default then");
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("human", "@dev-b look")), Wake::Mentioned);
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("alice", "the build")), Wake::Triage, "any human's");
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("bob", "the build")), Wake::Skip, "bob is no human");
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("dev-a", "@alice the build")), Wake::Skip, "a human");
        let hook = post("github", "@dev-a push").from_webhook(Vec::new());
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &hook), Wake::Triage, "a webhook's");
        assert_eq!(wakes(&p("dev-a"), Some(&entry), &hook), Wake::Skip, "its text mentions nobody");
    }

    #[test]
    fn a_post_reads_as_a_message_from_its_author_on_its_thread() {
        let mut ask = post("dev-b", "@dev-a check");
        ask.turn_budget = 3;
        let envelope = as_envelope(&ask, &p("dev-a"));
        assert_eq!((envelope.id, &envelope.thread_id, envelope.turn_budget), (ask.id, &ask.thread_id, 3));
        assert_eq!((envelope.from, envelope.to, envelope.owner), (p("dev-b"), p("dev-a"), None));
        assert_eq!(envelope.text, ask.text);
        let owned = ask.with_owner(Some(p("alice")));
        assert_eq!(as_envelope(&owned, &p("dev-a")).owner, Some(p("alice")), "the thread's owner gets its escalations");
    }
}
