//! A channel's feed: its top-level posts, newest first, each with its replies' count and latest time and the reactions
//! to it, as `ants channel read` shows them; and which of the channels' posts the human has not read.

use std::collections::HashMap;

use ants_proto::{ChannelId, ReadMarker, ThreadId, UserId, Uuid};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph};

use super::attach;
use super::exec::Job;
use super::frame::{self, ago};
use super::model::{Action, Data, Key, To};
use super::select::Selection;
use super::work::Filter;
use crate::attachments::Listed;
use crate::channel::reads::Unread;
use crate::thread::{Entry, Kind};

/// The most posts of a channel whose reactions are read.
const WATCHED: usize = 200;

/// A top-level post.
#[derive(Debug, Clone)]
pub struct Post {
    pub id: Uuid,
    pub thread: ThreadId,
    pub from: String,
    /// Who posted it, an assistant's post for its human naming both ([`Entry::sender`]).
    pub by: String,
    pub at: DateTime<Utc>,
    pub text: String,
    pub attachments: Vec<Listed>,
}

/// The replies on a post's thread: how many, and the latest's time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Replies {
    pub count: usize,
    pub last_at: DateTime<Utc>,
}

/// A channel post of any kind, as far as its being read goes.
#[derive(Debug, Clone)]
struct Seen {
    seq: u64,
    thread: ThreadId,
    from: UserId,
    mentions: Vec<UserId>,
}

/// The channels' posts read from the stream.
#[derive(Debug, Default)]
pub struct Feeds {
    posts: HashMap<ChannelId, Vec<Post>>,
    replies: HashMap<ThreadId, Replies>,
    /// Every post of each channel, in the order stored.
    seen: HashMap<ChannelId, Vec<Seen>>,
    /// The channel of each thread posted on.
    channels: HashMap<ThreadId, ChannelId>,
}

impl Feeds {
    /// Take a message read: whether it is a top-level post.
    pub fn add(&mut self, entry: &Entry) -> bool {
        if let (Kind::Post, Some(channel), Some(from)) = (entry.kind, &entry.channel, &entry.from) {
            let thread = entry.thread_id.clone();
            let seen =
                Seen { seq: entry.seq, thread: thread.clone(), from: from.clone(), mentions: entry.mentions.clone() };
            self.seen.entry(channel.clone()).or_default().push(seen);
            self.channels.insert(thread, channel.clone());
        }
        let top = entry.kind == Kind::Post && entry.id.is_some_and(|id| id.to_string() == entry.thread_id.as_str());
        if let (true, Some(id), Some(channel)) = (top, entry.id, &entry.channel) {
            let from = entry.from.as_ref().map_or_else(String::new, ToString::to_string);
            let post = Post {
                id,
                thread: entry.thread_id.clone(),
                from,
                by: entry.sender(),
                at: entry.stored_at,
                text: entry.text.clone(),
                attachments: entry.attachments.clone(),
            };
            self.posts.entry(channel.clone()).or_default().push(post);
            return true;
        }
        if matches!(entry.kind, Kind::Message | Kind::Post) {
            let replies =
                self.replies.entry(entry.thread_id.clone()).or_insert(Replies { count: 0, last_at: entry.stored_at });
            replies.count += 1;
            replies.last_at = entry.stored_at;
        }
        false
    }

    /// The channel's top-level posts, newest first.
    pub fn posts(&self, channel: &ChannelId) -> impl Iterator<Item = &Post> {
        self.posts.get(channel).into_iter().flat_map(|p| p.iter().rev())
    }

    pub fn replies(&self, thread: &ThreadId) -> Option<Replies> {
        self.replies.get(thread).copied()
    }

    /// The sequence of the channel's newest post read.
    pub fn newest(&self, channel: &ChannelId) -> Option<u64> {
        self.seen.get(channel)?.last().map(|s| s.seq)
    }

    /// The channel of the posts on `thread`, and their sequences.
    pub fn thread_posts(&self, thread: &ThreadId) -> Option<(&ChannelId, Vec<u64>)> {
        let channel = self.channels.get(thread)?;
        let seqs = self.seen.get(channel)?.iter().filter(|s| s.thread == *thread).map(|s| s.seq).collect();
        Some((channel, seqs))
    }

    /// The channel's posts read that `marker` (none: nothing read) leaves unread for `me`, `is_human` telling a
    /// human author from an ant.
    pub fn unread(
        &self,
        channel: &ChannelId,
        marker: Option<&ReadMarker>,
        me: &UserId,
        is_human: impl Fn(&UserId) -> bool,
    ) -> Unread {
        let posts = self.seen.get(channel).into_iter().flatten();
        Unread::of(posts.map(|s| (s.seq, &s.from, s.mentions.as_slice())), marker, me, is_human)
    }

    /// The posts of the channel whose reactions to read: the newest.
    pub fn watched(&self, channel: &ChannelId) -> Vec<Uuid> {
        self.posts(channel).take(WATCHED).map(|p| p.id).collect()
    }
}

#[derive(Debug)]
pub struct Page {
    pub channel: ChannelId,
    /// The selected post, kept on it as posts arrive.
    selected: Selection<Uuid>,
    /// The attachment `o` opens of the post with them, by its id.
    mark: attach::Mark<Uuid>,
}

impl Page {
    pub fn new(channel: ChannelId) -> Self {
        Self { channel, selected: Selection::default(), mark: attach::Mark::default() }
    }

    pub const KEYS: &str =
        "↑↓ post · Enter open its thread · w its threads in work · o open file · ] next file · Esc nav";

    /// The selected post.
    pub fn post<'d>(&self, data: &'d Data) -> Option<&'d Post> {
        data.feeds.posts(&self.channel).nth(self.at(data))
    }

    fn ids(&self, data: &Data) -> Vec<Uuid> {
        data.feeds.posts(&self.channel).map(|p| p.id).collect()
    }

    /// Where the selected post is among the channel's.
    fn at(&self, data: &Data) -> usize {
        self.selected.at(&self.ids(data))
    }

    /// The attachment `o` opens of the selected post.
    pub fn attachment<'d>(&self, data: &'d Data) -> Option<&'d Listed> {
        let post = self.post(data)?;
        post.attachments.get(self.mark.at(post.id, post.attachments.len()))
    }

    pub fn key(&mut self, key: Key, data: &Data) -> Option<To> {
        let at = self.at(data);
        let to = match key {
            Key::Char('o') => {
                let listed = self.attachment(data)?.clone();
                return Some(To::Do(Action::Run(Job::Open(Box::new(listed)), String::new())));
            }
            Key::Char(']') => {
                let post = self.post(data)?;
                self.mark.next(post.id, post.attachments.len());
                return None;
            }
            Key::Enter => return self.post(data).map(|p| To::Thread(p.thread.clone())),
            Key::Char('w') => {
                return Some(To::Work(Filter { channel: Some(self.channel.clone()), ..Filter::default() }));
            }
            Key::Up | Key::Char('k') => at.saturating_sub(1),
            Key::Down | Key::Char('j') => at + 1,
            Key::PageUp => at.saturating_sub(10),
            Key::PageDown => at + 10,
            Key::Home => 0,
            Key::End => usize::MAX,
            _ => return None,
        };
        self.selected.select(&self.ids(data), to);
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
        let posts: Vec<&Post> = data.feeds.posts(&self.channel).collect();
        let block = frame::block(format!(" #{} ({} posts) ", self.channel, posts.len()), focused);
        if posts.is_empty() {
            let text = if data.live.stored_at.is_some() { "no posts read" } else { "reading the stream…" };
            frame.render_widget(Paragraph::new(text).block(block), area);
            return;
        }
        let selected = self.at(data);
        let items: Vec<ListItem> = posts
            .iter()
            .enumerate()
            .map(|(at, p)| {
                let marked = (at == selected).then(|| self.mark.at(p.id, p.attachments.len()));
                ListItem::new(post_lines(p, data, now, marked))
            })
            .collect();
        let list = List::new(items).block(block).highlight_style(frame::highlight(focused));
        frame.render_stateful_widget(list, area, &mut ListState::default().with_selected(Some(selected)));
    }
}

/// A post: its author and age, its first line, its attachments, `marked` the one `o` opens, then its replies and
/// reactions.
fn post_lines(post: &Post, data: &Data, now: DateTime<Utc>, marked: Option<usize>) -> Vec<Line<'static>> {
    let first = post.text.lines().next().unwrap_or_default();
    let face = post.from.parse().ok().and_then(|from| data.face(&from));
    let mut head: Vec<Span<'static>> = face.into_iter().flatten().collect();
    head.extend([
        Span::styled(post.by.clone(), Style::new().fg(Color::Blue).bold()),
        Span::styled(format!(" {} ", ago(post.at, now)), frame::dim()),
        Span::raw(crate::excerpt(first, 200)),
    ]);
    let mut lines = vec![Line::from(head)];
    lines.extend(attach::lines(&post.attachments, marked, "  "));
    let mut under = Vec::new();
    if let Some(r) = data.feeds.replies(&post.thread) {
        let replies = if r.count == 1 { "reply" } else { "replies" };
        under.push(format!("↳ {} {replies} · last {}", r.count, ago(r.last_at, now)));
    }
    for r in data.reactions.get(&post.id).into_iter().flatten() {
        under.push(format!("{} {}", super::react::glyph(r.reaction.as_str()), r.by.len()));
    }
    if !under.is_empty() {
        lines.push(Line::styled(format!("  {}", under.join("  ")), frame::dim()));
    }
    lines
}

#[cfg(test)]
pub mod tests {
    use ants_proto::{ChannelPost, Envelope, Reacted, subject};

    use super::*;
    use crate::live::tests::{at, user};
    use crate::thread::Parser;
    use crate::tui::model::tests::model;
    use crate::tui::model::{Action, Page as View};
    use crate::tui::nav;

    /// The entry of `post`, stored at sequence `seq`.
    pub fn post(seq: u64, post: &ChannelPost) -> Entry {
        let payload = serde_json::to_vec(post).unwrap();
        let subject = subject::chan(&post.channel, &post.from);
        Parser::default().entry(seq, at(seq.cast_signed()), &subject, &payload).unwrap()
    }

    #[test]
    fn the_feed_lists_top_level_posts_newest_first_with_their_replies_and_reactions() {
        let ops: ChannelId = "ops".parse().unwrap();
        let first = ChannelPost::new(ops.clone(), user("bob"), "the site is down");
        let second = ChannelPost::new(ops.clone(), user("alice"), "deploy at noon");
        let reply = ChannelPost::on_thread(ops.clone(), first.thread_id.clone(), user("dev-a"), "looking");
        let answer =
            Envelope { thread_id: first.thread_id.clone(), ..Envelope::new(user("dev-a"), user("bob"), "fixed") };
        let answer = Parser::default()
            .entry(4, at(4), &subject::inbox(&answer.to, &answer.from), &serde_json::to_vec(&answer).unwrap())
            .unwrap();
        let mut m = model();
        m.select(nav::Item::Channel(ops.clone()));
        let actions = m.stored(vec![post(1, &first), post(2, &second), post(3, &reply), answer], at(4));
        assert_eq!(actions, [Action::Reactions(vec![second.id, first.id])], "a shown channel's reactions are read");
        let threads: Vec<_> = m.data.feeds.posts(&ops).map(|p| p.thread.clone()).collect();
        assert_eq!(threads, [second.thread_id.clone(), first.thread_id.clone()]);
        assert_eq!(m.data.feeds.replies(&first.thread_id), Some(Replies { count: 2, last_at: at(4) }));
        assert_eq!(m.data.feeds.replies(&second.thread_id), None);
        let reacted = Reacted::new("eyes".parse().unwrap(), vec![user("bob")]);
        m.read(super::super::reads::Read::Reactions([(second.id, vec![reacted])].into()), at(4));
        let View::Channel(page) = m.page() else { panic!("not the channel") };
        let shown: Vec<String> = post_lines(m.data.feeds.posts(&ops).nth(1).unwrap(), &m.data, at(64), None)
            .into_iter()
            .chain(post_lines(m.data.feeds.posts(&ops).next().unwrap(), &m.data, at(64), None))
            .map(|l| l.to_string())
            .collect();
        assert_eq!(
            shown,
            ["bob 1m ago the site is down", "  ↳ 2 replies · last 1m ago", "alice 1m ago deploy at noon", "  👀 1"]
        );
        assert_eq!(page.channel, ops);

        m.key(super::super::model::Key::Down, at(5));
        assert_eq!(m.key(super::super::model::Key::Enter, at(5)), Some(Action::Open(first.thread_id)));
        m.key(super::super::model::Key::Esc, at(5));
        m.key(super::super::model::Key::Char('w'), at(5));
        let View::Work(work) = m.page() else { panic!("not the work view") };
        assert_eq!(work.filter, Filter { channel: Some(ops), ..Filter::default() });
    }
}
