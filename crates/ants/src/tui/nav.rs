//! The nav: home, work with a count per state, the human's channels with their unread posts and @mentions, the ants
//! with their state, and the team's other humans, online or not. Each entry selects a view.

use ants_proto::{Activity, ChannelId, Humans, RosterEntry, UserId};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::frame;
use super::model::Data;
use super::work::ThreadState;
use crate::channel::reads::Unread;
use crate::roster;
use crate::thread::Kind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    Home,
    /// Every thread, or those in a state.
    Work(Option<ThreadState>),
    Channel(ChannelId),
    /// An ant's threads, until an ant view comes.
    Ant(UserId),
    /// Another human: the direct messages with it.
    Person(UserId),
}

impl Item {
    /// The heading of the section the entry is in, if any.
    fn section(&self) -> Option<&'static str> {
        match self {
            Self::Home | Self::Work(_) => None,
            Self::Channel(_) => Some("CHANNELS"),
            Self::Ant(_) => Some("ANTS"),
            Self::Person(_) => Some("PEOPLE"),
        }
    }
}

/// The nav's entries, in order.
pub fn items(data: &Data, _now: DateTime<Utc>) -> Vec<Item> {
    let mut items = vec![Item::Home, Item::Work(None)];
    items.extend(ThreadState::ALL.map(|s| Item::Work(Some(s))));
    items.extend(data.channels.iter().map(|c| Item::Channel(c.channel.clone())));
    let mut ants: Vec<&UserId> = data.live.roster.iter().map(|e| &e.ant).collect();
    ants.sort();
    ants.dedup();
    items.extend(ants.into_iter().map(|a| Item::Ant(a.clone())));
    let mut people: Vec<&UserId> =
        data.humans.iter().flat_map(Humans::iter).filter(|h| *h != data.me() && **h != UserId::human()).collect();
    people.sort();
    items.extend(people.into_iter().map(|p| Item::Person(p.clone())));
    items
}

/// The ant's state at a glance: whether its daemon beats, else what holds it back, else whether it is busy.
pub fn marker(entries: &[&RosterEntry], now: DateTime<Utc>) -> (&'static str, Color) {
    let live: Vec<&&RosterEntry> =
        entries.iter().filter(|e| roster::State::of(e, now) == roster::State::Live).collect();
    let Some(entry) = live.first() else { return ("○", Color::DarkGray) };
    if entry.signed_out.is_some() || entry.failing.is_some() {
        return ("✕", Color::Red);
    }
    if entry.limited.is_some() || entry.fallback.is_some() {
        return ("◌", Color::Yellow);
    }
    if live.iter().any(|e| e.activity == Activity::Working) {
        return ("◐", Color::Green);
    }
    ("●", Color::Green)
}

/// The channel's unread posts and @mentions of the human among those read, once which are read is known; else, without
/// the credentials to know, only its unread @mentions as its inbox tells of them.
fn unread(data: &Data, channel: &ChannelId) -> Unread {
    if let Some(markers) = &data.channel_reads {
        return data.feeds.unread(channel, markers.channels.get(channel), data.me());
    }
    let mentions = data
        .live
        .order
        .iter()
        .flat_map(|t| data.unread_on(t))
        .filter(|i| i.kind == Kind::Mention && i.channel.as_ref() == Some(channel))
        .count();
    Unread { posts: 0, mentions }
}

fn line(
    item: &Item,
    data: &Data,
    counts: &std::collections::HashMap<ThreadState, usize>,
    now: DateTime<Utc>,
) -> Line<'static> {
    let count = |n: usize| if n == 0 { String::new() } else { format!(" {n}") };
    match item {
        Item::Home => Line::raw(format!("Home{}", count(super::home::waiting(data, now).len()))),
        Item::Work(None) => Line::raw(format!("Work{}", count(data.live.order.len()))),
        Item::Work(Some(state)) => {
            let n = counts.get(state).copied().unwrap_or(0);
            let style = if n == 0 { frame::dim() } else { Style::new().fg(state.color()) };
            Line::styled(format!("  {}{}", state.label(), count(n)), style)
        }
        Item::Channel(channel) => {
            let unread = unread(data, channel);
            let style = if unread.posts > 0 { Style::new().add_modifier(Modifier::BOLD) } else { Style::new() };
            let mut spans = vec![Span::styled(format!("#{channel}"), style)];
            if unread.posts > 0 {
                spans.push(Span::raw(format!(" {}", unread.posts)));
            }
            if unread.mentions > 0 {
                spans.push(Span::styled(format!(" @{}", unread.mentions), Style::new().fg(Color::Magenta).bold()));
            }
            Line::from(spans)
        }
        Item::Ant(ant) => {
            let entries: Vec<&RosterEntry> = data.live.roster.iter().filter(|e| e.ant == *ant).collect();
            let (mark, color) = marker(&entries, now);
            Line::from(vec![Span::styled(mark, Style::new().fg(color)), Span::raw(format!(" {ant}"))])
        }
        Item::Person(person) => super::people::nav_line(data, person),
    }
}

pub fn draw(frame: &mut Frame, area: Rect, data: &Data, selected: &Item, now: DateTime<Utc>, focused: bool) {
    let counts = data.counts();
    let mut lines = Vec::new();
    let mut section = None;
    let mut at = 0;
    for item in items(data, now) {
        if item.section() != section {
            section = item.section();
            lines.push(Line::raw(""));
            lines.push(Line::styled(section.unwrap_or_default(), frame::dim().add_modifier(Modifier::BOLD)));
        }
        let mut line = line(&item, data, &counts, now);
        if item == *selected {
            at = lines.len();
            line = line.patch_style(frame::highlight(focused));
        }
        lines.push(line);
    }
    let height = usize::from(area.height.saturating_sub(2));
    let top = (at + 1).saturating_sub(height);
    let paragraph = Paragraph::new(lines)
        .block(frame::block(String::new(), focused))
        .scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
    frame.render_widget(paragraph, area);
}

#[cfg(test)]
mod tests {
    use ants_proto::{ChannelEntry, ChannelPost, ChannelReadMarkers, HumanEntry, ThreadId};

    use super::*;
    use crate::live::tests::{at, message, user};
    use crate::roster::tests::entry;
    use crate::tui::channel::tests::post;
    use crate::tui::model::tests::model;
    use crate::tui::reads::Read;

    #[test]
    fn the_nav_lists_states_channels_ants_and_other_humans() {
        let mut m = model();
        let ops = ChannelEntry { members: vec![user("alice")], ..ChannelEntry::new("ops".parse().unwrap()) };
        let humans = Humans::from_entries(["alice", "carol"].map(|h| (h, HumanEntry::new(user(h)))));
        m.read(Read::Directory(Ok((vec![ops], humans))), at(0));
        m.data.live.roster(vec![entry("dev-b", 60, 5, at(0)), entry("dev-a", 60, 5, at(0))], at(0));
        let items = items(&m.data, at(0));
        assert_eq!(items[..2], [Item::Home, Item::Work(None)]);
        assert_eq!(
            items[8..],
            [
                Item::Channel("ops".parse().unwrap()),
                Item::Ant(user("dev-a")),
                Item::Ant(user("dev-b")),
                Item::Person(user("carol")),
            ]
        );
    }

    #[test]
    fn an_ants_marker_tells_whether_it_beats_is_held_back_or_busy() {
        let now = at(0);
        let mut busy = entry("dev-a", 60, 5, now);
        busy.activity = Activity::Working;
        assert_eq!(marker(&[&entry("dev-a", 60, 5, now)], now).0, "●");
        assert_eq!(marker(&[&entry("dev-a", 600, 300, now)], now).0, "○");
        assert_eq!(marker(&[&entry("dev-a", 600, 300, now), &busy], now).0, "◐");
        let mut out = entry("dev-a", 60, 5, now);
        out.signed_out = Some(ants_proto::SignInNeeded { since: now, sign_in: None });
        assert_eq!(marker(&[&out], now).0, "✕");
    }

    #[test]
    fn a_channel_shows_its_unread_posts_and_mentions() {
        let thread = ThreadId::generate();
        let mut m = model();
        let ops: ChannelId = "ops".parse().unwrap();
        let mut mention = message(1, &thread, "dev-a", "alice", "@alice look");
        mention.kind = Kind::Mention;
        mention.channel = Some(ops.clone());
        m.stored(vec![mention], at(1));
        assert_eq!(unread(&m.data, &ops), Unread { posts: 0, mentions: 1 }, "without channel markers, the inbox's");
        m.data.marker.as_mut().unwrap().mark(1);
        assert_eq!(unread(&m.data, &ops).mentions, 0);

        m.data.channel_reads = Some(ChannelReadMarkers::default());
        let look = ChannelPost::new(ops.clone(), user("dev-a"), "@alice look");
        let mine = ChannelPost::new(ops.clone(), user("alice"), "on it");
        let other = ChannelPost::new("dev".parse().unwrap(), user("dev-a"), "@alice elsewhere");
        m.stored(vec![post(2, &look), post(3, &mine), post(4, &other)], at(2));
        assert_eq!(unread(&m.data, &ops), Unread { posts: 1, mentions: 1 }, "the human's own posts are read");
        m.data.channel_reads.as_mut().unwrap().mark(&ops, 2);
        assert_eq!(unread(&m.data, &ops), Unread::default());
    }
}
