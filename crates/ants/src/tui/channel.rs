//! A channel's view: a header with the channel, its members and its latest activity ([`header`], [`activity`]) over
//! its threads under tabs by state ([`tabs`]): their top-level posts, newest first, each with its thread's state, its
//! replies' count and latest time and the reactions to it, as `ants channel read` shows them, those of the threads
//! the human archived or left idle under their own tab; and which of the channels' posts the human has not read.

use std::collections::HashMap;

use ants_proto::{ChannelId, ChannelNotify, ReadMarker, ThreadId, UserId, Uuid};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{ListState, Paragraph};

use super::attach;
use super::command::Conversation;
use super::exec::Job;
use super::model::{Action, Data, Key, To};
use super::select::Selection;
use super::style::{self, ago};
use crate::channel::reads::Unread;
use crate::messages::attachments::Listed;
use crate::threads::thread::{Entry, Kind};

mod activity;
mod header;
mod members;
mod tabs;

pub use activity::Activity;
pub use tabs::{Tab, keep as keep_tab, kept as kept_tabs};

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
        if let (true, Some(id), Some(channel)) = (Self::opens(entry), entry.id, &entry.channel) {
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

    /// Whether `entry` is the top-level post opening its thread.
    fn opens(entry: &Entry) -> bool {
        entry.kind == Kind::Post && entry.id.is_some_and(|id| id.to_string() == entry.thread_id.as_str())
    }

    /// The channel of the posts on `thread`.
    pub fn channel(&self, thread: &ThreadId) -> Option<&ChannelId> {
        self.channels.get(thread)
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

    /// The channel's posts read that `marker` (none: nothing read) leaves unread for `me` at notify level `level`,
    /// `is_human` telling a human author from an ant, but for those on the threads `hidden` leaves out.
    pub fn unread(
        &self,
        channel: &ChannelId,
        marker: Option<&ReadMarker>,
        me: &UserId,
        level: ChannelNotify,
        is_human: impl Fn(&UserId) -> bool,
        hidden: impl Fn(&ThreadId) -> bool,
    ) -> Unread {
        let posts = self.seen.get(channel).into_iter().flatten().filter(|s| !hidden(&s.thread));
        Unread::of(posts.map(|s| (s.seq, &s.from, s.mentions.as_slice())), marker, me, level, is_human)
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
    /// The members editor `M` opens.
    editor: Option<members::Editor>,
}

impl Page {
    pub fn new(channel: ChannelId) -> Self {
        Self { channel, selected: Selection::default(), mark: attach::Mark::default(), editor: None }
    }

    /// The keys of the members editor, which takes every key, when it is open.
    pub fn prompt(&self) -> Option<&'static str> {
        self.editor.as_ref().map(members::Editor::keys)
    }

    /// Take `key` for the members editor when it is open: what to do, if anything.
    pub fn write(&mut self, key: Key, data: &Data) -> Option<Option<To>> {
        let editor = self.editor.as_mut()?;
        Some(match editor.key(key, &self.channel, data) {
            members::Edited::Kept => None,
            members::Edited::Closed => {
                self.editor = None;
                None
            }
            members::Edited::To(to) => Some(*to),
        })
    }

    /// Take `line` pasted for the members editor when it is open, into the row typed in if any.
    pub fn paste(&mut self, line: &str) -> bool {
        self.editor.as_mut().is_some_and(|editor| editor.paste(line))
    }

    /// The tab chosen, as kept on the device.
    pub fn tab(&self, data: &Data) -> Tab {
        data.channel_tabs.get(&self.channel).copied().unwrap_or_default()
    }

    /// The channel's top-level posts shown, newest first: those of the threads the tab chosen lists.
    fn posts<'d>(&self, data: &'d Data) -> impl Iterator<Item = &'d Post> {
        let tab = self.tab(data);
        data.feeds.posts(&self.channel).filter(move |p| tab.lists(&p.thread, data))
    }

    /// The selected post.
    pub fn post<'d>(&self, data: &'d Data) -> Option<&'d Post> {
        self.posts(data).nth(self.at(data))
    }

    fn ids(&self, data: &Data) -> Vec<Uuid> {
        self.posts(data).map(|p| p.id).collect()
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
            Key::Left | Key::Right => {
                let tab = self.tab(data).moved(if key == Key::Left { -1 } else { 1 });
                self.selected = Selection::default();
                return Some(To::Do(Action::KeepChannelTab(self.channel.clone(), tab)));
            }
            Key::Char('M') => {
                if !data.channels.iter().any(|c| c.channel == self.channel) {
                    return Some(To::Refused(format!("#{} is not read yet", self.channel)));
                }
                self.editor = Some(members::Editor::default());
                return None;
            }
            Key::Char('n') => {
                let level = next_level(data.level(&self.channel));
                return Some(To::Do(Action::Run(
                    Job::ChannelNotify { channel: self.channel.clone(), level },
                    String::new(),
                )));
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
        let rows = header::rows(frame.area());
        let [head, tabs_row, area] =
            Layout::vertical([Constraint::Length(rows), Constraint::Length(1), Constraint::Min(1)]).areas(area);
        header::draw(frame, head, data, &self.channel, now);
        let tab = self.tab(data);
        let counts = tabs::counts(data.feeds.posts(&self.channel).map(|p| &p.thread), data);
        frame.render_widget(Paragraph::new(tabs::line(tab, &counts, usize::from(tabs_row.width), focused)), tabs_row);
        let posts: Vec<&Post> = self.posts(data).collect();
        let block = style::block(format!(" #{} · {} ", self.channel, tab.label()), focused);
        if posts.is_empty() {
            let text = match (data.live.stored_at, tab) {
                (None, _) => "reading the stream…".to_owned(),
                (Some(_), Tab::All) if counts.contains_key(&Tab::Archived) => {
                    "only posts on archived threads".to_owned()
                }
                (Some(_), Tab::All) => "no posts read".to_owned(),
                (Some(_), tab) => format!("no thread is {}", tab.label()),
            };
            frame.render_widget(Paragraph::new(text).block(block), area);
            self.draw_editor(frame, head, tabs_row.union(area), data, now);
            return;
        }
        let selected = self.at(data);
        let items: Vec<Text> = posts
            .iter()
            .enumerate()
            .map(|(at, p)| {
                let marked = (at == selected).then(|| self.mark.at(p.id, p.attachments.len()));
                let inner = usize::from(area.width.saturating_sub(3));
                Text::from(
                    post_lines(p, data, now, marked).into_iter().map(|l| style::clip(l, inner)).collect::<Vec<_>>(),
                )
            })
            .collect();
        let list = style::list(items, Some(selected), focused).block(block);
        frame.render_stateful_widget(list, area, &mut ListState::default().with_selected(Some(selected)));
        self.draw_editor(frame, head, tabs_row.union(area), data, now);
    }

    /// The members editor, if open: over the members pane of the header `head`, as wide as it needs; over the top
    /// of `view` under a header in one line.
    fn draw_editor(&self, frame: &mut Frame, head: Rect, view: Rect, data: &Data, now: DateTime<Utc>) {
        let Some(editor) = &self.editor else { return };
        let Some(entry) = data.channels.iter().find(|c| c.channel == self.channel) else { return };
        let bottom = view.bottom();
        let (x, y, width) = match header::panes(head) {
            Some([_, members, _]) => (members.x, members.y, members.width.max(44)),
            None => (view.x, view.y, view.width.min(60)),
        };
        let width = width.min(view.right().saturating_sub(x));
        let height = editor.height(entry, data).min(bottom.saturating_sub(y));
        editor.draw(frame, Rect { x, y, width, height }, entry, data, now);
    }
}

/// The notify level after `level`, as `n` steps through them.
fn next_level(level: ChannelNotify) -> ChannelNotify {
    let at = ChannelNotify::ALL.iter().position(|l| *l == level).unwrap_or(0);
    ChannelNotify::ALL[(at + 1) % ChannelNotify::ALL.len()]
}

/// A post: its author and age, its first line, its attachments, `marked` the one `o` opens, then its thread's state,
/// its replies and reactions.
fn post_lines(post: &Post, data: &Data, now: DateTime<Utc>, marked: Option<usize>) -> Vec<Line<'static>> {
    let first = post.text.lines().next().unwrap_or_default();
    let face = post.from.parse().ok().and_then(|from| data.face(&from));
    let mut head: Vec<Span<'static>> = face.into_iter().flatten().collect();
    head.extend([
        Span::styled(post.by.clone(), Style::new().fg(Color::Blue).bold()),
        Span::styled(format!(" {} ", ago(post.at, now)), style::dim()),
        Span::raw(crate::text::excerpt(first, 200)),
    ]);
    head.extend(data.drafts.mark(&Conversation::Thread(post.thread.clone())));
    let mut lines = vec![Line::from(head)];
    lines.extend(attach::lines(&post.attachments, marked, "  "));
    let state = data.state(&post.thread);
    let mut under = Vec::new();
    if let Some(r) = data.feeds.replies(&post.thread) {
        let replies = if r.count == 1 { "reply" } else { "replies" };
        under.push(format!("↳ {} {replies} · last {}", r.count, ago(r.last_at, now)));
    }
    for r in data.reactions.get(&post.id).into_iter().flatten() {
        under.push(format!("{} {}", super::react::glyph(r.reaction.as_str()), r.by.len()));
    }
    let mut badge = vec![Span::raw("  "), Span::styled(state.label(), Style::new().fg(state.color()))];
    if !under.is_empty() {
        badge.push(Span::styled(format!("  {}", under.join("  ")), style::dim()));
    }
    lines.push(Line::from(badge));
    lines
}

#[cfg(test)]
mod tests {
    use ants_proto::{ChannelPost, Envelope, Reacted, subject};

    use super::*;
    use crate::test_support::live::{at, message, user};
    use crate::test_support::render::rendered;
    use crate::threads::thread::Parser;
    use crate::tui::model::{Action, Key, Model, Page as View};
    use crate::tui::nav;
    use crate::tui::test_support::{model, post};
    use crate::tui::work::ThreadState;

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
        let watched = vec![second.id, first.id];
        m.read(super::super::reads::Read::Reactions(watched, [(second.id, vec![reacted])].into()), at(4));
        let View::Channel(page) = m.page() else { panic!("not the channel") };
        let shown: Vec<String> = post_lines(m.data.feeds.posts(&ops).nth(1).unwrap(), &m.data, at(64), None)
            .into_iter()
            .chain(post_lines(m.data.feeds.posts(&ops).next().unwrap(), &m.data, at(64), None))
            .map(|l| l.to_string())
            .collect();
        assert_eq!(
            shown,
            [
                "bob 1m ago the site is down",
                "  idle  ↳ 2 replies · last 1m ago",
                "alice 1m ago deploy at noon",
                "  idle  👀 1"
            ]
        );
        assert_eq!(page.channel, ops);

        m.key(Key::Down, at(5));
        assert_eq!(m.key(Key::Enter, at(5)), Some(Action::Open(first.thread_id)));
        m.key(Key::Esc, at(5));
        assert_eq!(m.key(Key::Char('w'), at(5)), None);
        assert!(matches!(m.page(), View::Channel(_)), "w no longer leads to work");
    }

    #[test]
    fn left_and_right_cycle_the_tabs_by_state_each_counted_and_the_archived_threads_under_their_own() {
        let ops: ChannelId = "ops".parse().unwrap();
        let idle = ChannelPost::new(ops.clone(), user("bob"), "lunch?");
        let asked = ChannelPost::new(ops.clone(), user("bob"), "deploy at noon");
        let archived = ChannelPost::new(ops.clone(), user("bob"), "fixed");
        let answer = message(4, &asked.thread_id, "dev-a", "alice", "deployed");
        let mut m = model();
        m.data.live.marks.archive(&archived.thread_id, 3);
        m.stored(vec![post(1, &idle), post(2, &asked), post(3, &archived), answer], at(4));
        m.select(nav::Item::Channel(ops.clone()));
        let page = |m: &Model| {
            let View::Channel(page) = m.page() else { panic!("not the channel") };
            (page.tab(&m.data), page.posts(&m.data).map(|p| p.thread.clone()).collect::<Vec<_>>())
        };
        let screen = |m: &Model| {
            let View::Channel(page) = m.page() else { panic!("not the channel") };
            rendered(130, 30, |f| page.draw(f, f.area(), &m.data, at(4), true))
        };
        assert_eq!(page(&m), (Tab::All, vec![asked.thread_id.clone(), idle.thread_id]));
        let shown = screen(&m);
        let tabs =
            "‹ all 2 · waiting on me 1 · working 0 · waiting on ant 0 · stuck 0 · idle 1 · done 0 · archived 1 ›";
        assert!(shown.lines().any(|l| l.trim_end() == tabs), "{shown}");
        assert!(shown.contains("#ops · all"), "{shown}");

        let kept = |tab| Some(Action::KeepChannelTab(ops.clone(), tab));
        assert_eq!(m.key(Key::Right, at(4)), kept(Tab::WaitingOnMe));
        assert_eq!(page(&m), (Tab::WaitingOnMe, vec![asked.thread_id]));
        let badge = post_lines(m.data.feeds.posts(&ops).nth(1).unwrap(), &m.data, at(4), None).remove(1);
        assert_eq!(badge.to_string(), "  waiting on me  ↳ 1 reply · last 0s ago");
        assert_eq!(badge.spans[1].style.fg, Some(ThreadState::WaitingOnMe.color()), "coloured by its state");

        assert_eq!(m.key(Key::Left, at(4)), kept(Tab::All));
        assert!(m.data.channel_tabs.is_empty(), "the default is kept as none");
        assert_eq!(m.key(Key::Left, at(4)), kept(Tab::Archived), "round from the first tab to the last");
        assert_eq!(page(&m), (Tab::Archived, vec![archived.thread_id.clone()]));
        m.select(nav::Item::Home);
        m.select(nav::Item::Channel(ops.clone()));
        assert_eq!(page(&m).0, Tab::Archived, "kept for the channel");
        assert!(screen(&m).contains("fixed"));
        assert_eq!(m.key(Key::Left, at(4)), kept(Tab::Done));
        assert!(screen(&m).contains("no thread is done"));
        assert_eq!(m.key(Key::Right, at(4)), kept(Tab::Archived));
        assert_eq!(m.key(Key::Right, at(4)), kept(Tab::All), "round from the last tab to the first");
        let counts = tabs::counts(m.data.feeds.posts(&ops).map(|p| &p.thread), &m.data);
        let narrow = tabs::line(Tab::Stuck, &counts, 60, true).to_string();
        assert_eq!(
            narrow, "‹ all 2 · waiting on me 1 · stuck 0 · idle 1 · archived 1 ›",
            "those listing nothing left out"
        );
    }

    #[test]
    fn posts_on_archived_threads_leave_the_feed_and_its_unread_count_until_one_comes_on_them() {
        let ops: ChannelId = "ops".parse().unwrap();
        let done = ChannelPost::new(ops.clone(), user("bob"), "fixed");
        let open = ChannelPost::new(ops.clone(), user("bob"), "deploy at noon");
        let mut m = model();
        m.data.live.marks.archive(&done.thread_id, 2);
        m.stored(vec![post(1, &done), post(2, &open)], at(2));
        m.select(nav::Item::Channel(ops.clone()));
        let shown = |m: &crate::tui::model::Model| {
            let View::Channel(page) = m.page() else { panic!("not the channel") };
            page.posts(&m.data).map(|p| p.thread.clone()).collect::<Vec<_>>()
        };
        let unread = |m: &crate::tui::model::Model| {
            let hidden = |t: &ThreadId| m.data.live.hidden(t);
            m.data.feeds.unread(&ops, None, &user("alice"), ChannelNotify::All, |_| true, hidden).posts
        };
        assert_eq!(shown(&m), std::slice::from_ref(&open.thread_id));
        assert_eq!(unread(&m), 1);
        let reply = ChannelPost::on_thread(ops.clone(), done.thread_id.clone(), user("bob"), "broke again");
        m.stored(vec![post(3, &reply)], at(3));
        assert_eq!(shown(&m), [open.thread_id, done.thread_id], "posted on since it was archived");
        assert_eq!(unread(&m), 3);
    }

    #[test]
    fn n_in_a_channel_steps_its_notify_level_at_once_and_on_all_devices() {
        let ops: ChannelId = "ops".parse().unwrap();
        let mut m = model();
        m.select(nav::Item::Channel(ops.clone()));
        let set = |level| Some(Action::Run(Job::ChannelNotify { channel: ops.clone(), level }, String::new()));
        m.key(Key::Up, at(1));
        assert_eq!(m.key(Key::Char('n'), at(1)), set(ChannelNotify::Mentions));
        assert_eq!(m.data.level(&ops), ChannelNotify::Mentions, "shown before it is stored");
        assert_eq!(m.key(Key::Char('n'), at(1)), set(ChannelNotify::All));
        assert_eq!(m.key(Key::Char('n'), at(1)), set(ChannelNotify::Humans));
        assert!(m.data.channel_levels.is_empty(), "the default is kept as none");
    }
}
