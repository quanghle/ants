//! The nav: home, the assistant's activity once there is any, work with a count per state (shown while work or a state
//! is selected), the human's channels with their unread posts and @mentions, the teams as a forest (each indented
//! under the team holding it, with how many ants it holds down through its member teams), the ants with their state,
//! and the team's other humans, online or not. Each entry selects a view.

use ants_client::entry::Kind;
use ants_proto::{Activity, ChannelId, ColonyEntry, Humans, ThreadId, UserId};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::command::Conversation;
use super::model::{Data, Ui};
use super::style;
use super::work::ThreadState;
use crate::ants::colony;
use crate::channel::reads::Unread;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    Home,
    /// The threads reporting an assistant's acts to the human, one per day ([`ants_proto::ThreadId::is_activity`]).
    Activity,
    /// Every thread, or those in a state.
    Work(Option<ThreadState>),
    Channel(ChannelId),
    /// A team: its lead, owner and description, and its members as a tree.
    Team(UserId),
    /// An ant: where and how it runs, what holds it back, its activity, threads, queue and spend.
    Ant(UserId),
    /// Another human: the direct messages with it.
    Person(UserId),
}

impl Item {
    /// The heading of the section the entry is in, if any.
    fn section(&self) -> Option<&'static str> {
        match self {
            Self::Home | Self::Activity | Self::Work(_) => None,
            Self::Channel(_) => Some("CHANNELS"),
            Self::Team(_) => Some("TEAMS"),
            Self::Ant(_) => Some("ANTS"),
            Self::Person(_) => Some("PEOPLE"),
        }
    }
}

/// The nav's entries, in order, the states under Work only while `selected` is Work or one of them.
pub fn items(data: &Data, selected: &Item, _now: DateTime<Utc>) -> Vec<Item> {
    let mut items = vec![Item::Home];
    if data.live.order.iter().any(ThreadId::is_activity) {
        items.push(Item::Activity);
    }
    items.push(Item::Work(None));
    if matches!(selected, Item::Work(_)) {
        items.extend(ThreadState::ALL.map(|s| Item::Work(Some(s))));
    }
    items.extend(data.channels.iter().map(|c| Item::Channel(c.channel.clone())));
    if let Some(teams) = &data.teams {
        items.extend(crate::teams::forest(teams).into_iter().map(|(_, t)| Item::Team(t.team.clone())));
    }
    let mut ants: Vec<&UserId> = data.live.colony.iter().map(|e| &e.ant).collect();
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
pub fn marker(entries: &[&ColonyEntry], now: DateTime<Utc>) -> (&'static str, Color) {
    let live: Vec<&&ColonyEntry> =
        entries.iter().filter(|e| colony::State::of(e, now) == colony::State::Live).collect();
    let Some(entry) = live.first() else { return ("○", Color::DarkGray) };
    if entry.unserved.is_some() || entry.signed_out.is_some() || entry.failing.is_some() {
        return ("✕", style::ERROR);
    }
    if entry.paused.is_some() {
        return ("‖", style::WARNING);
    }
    if entry.limited.is_some() || entry.fallback.is_some() {
        return ("◌", style::WARNING);
    }
    if live.iter().any(|e| e.activity == Activity::Working) {
        return ("◐", style::CONNECTED);
    }
    ("●", style::CONNECTED)
}

/// The channel's unread posts its notify level counts, @mentions of the human and the other posts among those read
/// but for those on archived and idle threads, once which are read is known; else, without the credentials to know, only its unread @mentions as its inbox tells
/// of them.
pub(super) fn unread(data: &Data, channel: &ChannelId) -> Unread {
    if let Some(markers) = &data.channel_reads {
        let (marker, level) = (markers.channels.get(channel), data.level(channel));
        return data.feeds.unread(channel, marker, data.me(), level, |u| data.is_human(u), |t| data.live.hidden(t));
    }
    let mentions = data
        .live
        .order
        .iter()
        .flat_map(|t| data.unread_on(t))
        .filter(|i| i.kind == Kind::Mention && i.channel.as_ref() == Some(channel))
        .count();
    Unread { posts: 0, mentions, other: 0 }
}

fn line(
    item: &Item,
    data: &Data,
    ui: &Ui,
    counts: &std::collections::HashMap<ThreadState, usize>,
    now: DateTime<Utc>,
) -> Line<'static> {
    let count = |n: usize| if n == 0 { String::new() } else { format!(" {n}") };
    match item {
        Item::Home => Line::raw(format!("Home{}", count(super::home::waiting(data, now).len()))),
        Item::Activity => {
            let days = data.live.order.iter().filter(|t| t.is_activity()).count();
            Line::styled(format!("  assistant activity{}", count(days)), style::dim())
        }
        Item::Work(None) => Line::raw(format!("Work{}", count(data.work().count()))),
        Item::Work(Some(state)) => {
            let n = counts.get(state).copied().unwrap_or(0);
            let style = if n == 0 { style::dim() } else { Style::new().fg(state.color()) };
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
            if unread.other > 0 {
                spans.push(Span::styled(format!(" ·{}", unread.other), style::dim()));
            }
            spans.extend(ui.drafts.mark(&Conversation::Channel(channel.clone())));
            Line::from(spans)
        }
        Item::Team(team) => {
            let Some(teams) = &data.teams else { return Line::raw(team.to_string()) };
            let indent = "  ".repeat(teams.ancestors(team).len());
            let mut spans = vec![Span::raw(format!("{indent}{team}"))];
            spans.push(Span::styled(count(teams.ants_under(team).len()), style::dim()));
            if teams.team(team).is_some_and(ants_proto::Team::leaderless) {
                spans.push(Span::styled(" ✕ no lead", Style::new().fg(style::ERROR)));
            }
            Line::from(spans)
        }
        Item::Ant(ant) => {
            let entries: Vec<&ColonyEntry> = data.live.colony.iter().filter(|e| e.ant == *ant).collect();
            let (mark, color) = marker(&entries, now);
            Line::from(vec![
                Span::styled(mark, Style::new().fg(color)),
                Span::raw(" "),
                data.looks.face(ant),
                Span::raw(format!(" {ant}")),
            ])
        }
        Item::Person(person) => {
            let mut line = super::people::nav_line(data, person);
            line.spans.extend(ui.drafts.mark(&Conversation::Person(person.clone())));
            line
        }
    }
}

pub fn draw(frame: &mut Frame, area: Rect, data: &Data, ui: &Ui, selected: &Item, now: DateTime<Utc>, focused: bool) {
    ui.mouse.add(area, super::mouse::Target::Nav(selected.clone()));
    let counts = data.counts();
    let mut lines = Vec::new();
    let mut section = None;
    let mut at = 0;
    let mut hits = Vec::new();
    for item in items(data, selected, now) {
        if item.section() != section {
            section = item.section();
            lines.push(Line::raw(""));
            lines.push(Line::styled(section.unwrap_or_default(), style::dim().add_modifier(Modifier::BOLD)));
        }
        let mut line = line(&item, data, ui, &counts, now);
        if item == *selected {
            at = lines.len();
            line = line.patch_style(style::highlight(focused));
        }
        hits.push((lines.len(), item));
        lines.push(line);
    }
    let inner = style::block(String::new(), focused).inner(area);
    let top = ui.mouse.top(super::mouse::Viewport::Nav, inner, at, at, at + 1, lines.len());
    ui.mouse.rows(inner, top, hits.into_iter().map(|(row, item)| (row, super::mouse::Target::Nav(item))));
    let paragraph = Paragraph::new(lines)
        .block(style::block(String::new(), focused))
        .scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
    frame.render_widget(paragraph, area);
}

#[cfg(test)]
mod tests {
    use ants_proto::{ChannelEntry, ChannelPost, ChannelReadMarkers, HumanEntry, ThreadId};

    use super::*;
    use crate::test_support::colony::entry;
    use crate::test_support::live::{at, message, user};
    use crate::tui::reads::Read;
    use crate::tui::test_support::model;
    use crate::tui::test_support::post;

    #[test]
    fn the_nav_lists_states_channels_ants_and_other_humans() {
        let mut m = model();
        let ops = ChannelEntry { members: vec![user("alice")], ..ChannelEntry::new("ops".parse().unwrap()) };
        let humans = Humans::from_entries(["alice", "carol"].map(|h| (h, HumanEntry::new(user(h)))));
        m.read(Read::Directory(Ok((vec![ops], humans))), at(0));
        m.data.live.colony(vec![entry("dev-b", 60, 5, at(0)), entry("dev-a", 60, 5, at(0))], at(0));
        let at_work = items(&m.data, &Item::Work(Some(ThreadState::Idle)), at(0));
        assert_eq!(at_work[2..8], ThreadState::ALL.map(|s| Item::Work(Some(s))), "the states show at work");
        let items = items(&m.data, &Item::Home, at(0));
        assert_eq!(items[..2], [Item::Home, Item::Work(None)]);
        assert_eq!(
            items[2..],
            [
                Item::Channel("ops".parse().unwrap()),
                Item::Ant(user("dev-a")),
                Item::Ant(user("dev-b")),
                Item::Person(user("carol")),
            ]
        );
    }

    #[test]
    fn with_home_selected_the_states_fold_and_the_first_channel_is_two_rows_under_work() {
        let mut m = model();
        let ops = ChannelEntry { members: vec![user("alice")], ..ChannelEntry::new("ops".parse().unwrap()) };
        m.read(Read::Directory(Ok((vec![ops], Humans::default()))), at(0));
        let rows = |selected: &Item| {
            let shown = crate::test_support::render::rendered(30, 20, |f| {
                draw(f, f.area(), &m.data, &m.ui, selected, at(0), true);
            });
            let rows: Vec<String> =
                shown.lines().map(|l| l.trim_matches(|c: char| c == '│' || c == ' ').to_owned()).collect();
            let row = |name: &str| rows.iter().position(|r| r == name).unwrap();
            (row("Work"), row("#ops"), rows.iter().any(|r| r == "idle"))
        };
        let (work, ops, states) = rows(&Item::Home);
        assert_eq!((ops - work, states), (3, false), "Work, a blank line and CHANNELS, then #ops");
        let (work, ops, states) = rows(&Item::Work(None));
        assert_eq!((ops - work, states), (9, true), "the six states unfold under Work");
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
        let mut paused = entry("dev-a", 60, 5, now);
        paused.paused = Some(ants_proto::Paused { since: now, by: None });
        assert_eq!(marker(&[&paused], now).0, "‖");
    }

    #[test]
    fn a_channel_shows_its_unread_posts_and_mentions_apart_from_ants_chatter() {
        let thread = ThreadId::generate();
        let mut m = model();
        let ops: ChannelId = "ops".parse().unwrap();
        let mut mention = message(1, &thread, "dev-a", "alice", "@alice look");
        mention.kind = Kind::Mention;
        mention.channel = Some(ops.clone());
        m.stored(vec![mention], at(1));
        let inbox = Unread { posts: 0, mentions: 1, other: 0 };
        assert_eq!(unread(&m.data, &ops), inbox, "without channel markers, the inbox's");
        m.data.marker.as_mut().unwrap().mark(1);
        assert_eq!(unread(&m.data, &ops).mentions, 0);

        m.data.channel_reads = Some(ChannelReadMarkers::default());
        let look = ChannelPost::new(ops.clone(), user("dev-a"), "@alice look");
        let mine = ChannelPost::new(ops.clone(), user("alice"), "on it");
        let other = ChannelPost::new("dev".parse().unwrap(), user("dev-a"), "@alice elsewhere");
        m.stored(vec![post(2, &look), post(3, &mine), post(4, &other)], at(2));
        assert_eq!(unread(&m.data, &ops), Unread { posts: 1, mentions: 1, other: 0 }, "the human's own posts are read");
        m.data.channel_reads.as_mut().unwrap().mark(&ops, 2);
        assert_eq!(unread(&m.data, &ops), Unread::default());

        m.data.humans = Some(ants_proto::Humans::from_entries([("bob", ants_proto::HumanEntry::new(user("bob")))]));
        let chatter = ChannelPost::new(ops.clone(), user("dev-a"), "@dev-b done?");
        let asked = ChannelPost::new(ops.clone(), user("bob"), "status?");
        m.stored(vec![post(5, &chatter), post(6, &asked)], at(3));
        assert_eq!(unread(&m.data, &ops), Unread { posts: 1, mentions: 0, other: 1 }, "an ant's post counts apart");
        m.data.channel_levels.insert(ops.clone(), ants_proto::ChannelNotify::Mentions);
        assert_eq!(unread(&m.data, &ops), Unread { posts: 0, mentions: 0, other: 2 }, "bob's post too at mentions");
        m.data.channel_levels.insert(ops.clone(), ants_proto::ChannelNotify::All);
        assert_eq!(unread(&m.data, &ops), Unread { posts: 2, mentions: 0, other: 0 }, "every post at all");
    }
}
