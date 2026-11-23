//! The channel view's header: three panes side by side, of the channel (its notify level, description, owner and
//! messenger), of its members (each ant with its face and its state as the nav marks it, the humans by name, the
//! owner and the messenger marked) and of its latest activity ([`super::activity`]); on a small screen, one line
//! summing them up.

use ants_proto::{ChannelEntry, ChannelId, ColonyEntry, UserId};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::activity::{Event, What};
use crate::colony;
use crate::text::excerpt;
use crate::tui::model::Data;
use crate::tui::{nav, react, style};

/// How tall the header is with its panes.
const ROWS: u16 = 5;
/// The screen narrower than which, or shorter than which, the header is one line.
const NARROW: u16 = 100;
const SHORT: u16 = 24;
/// The marks of the owner and the messenger among the members.
const OWNER: &str = "★";
const MESSENGER: &str = "✉";

/// How tall the header is on `screen`.
pub fn rows(screen: Rect) -> u16 {
    if screen.width < NARROW || screen.height < SHORT { 1 } else { ROWS }
}

pub fn draw(frame: &mut Frame, area: Rect, data: &Data, channel: &ChannelId, now: DateTime<Utc>) {
    let entry = data.channels.iter().find(|c| c.channel == *channel);
    let level = data.level(channel);
    let Some([info, members, activity]) = panes(area) else {
        let line = style::clip(summary(entry, channel, data, now), usize::from(area.width));
        frame.render_widget(Paragraph::new(line), area);
        return;
    };
    let inner = |r: Rect| (usize::from(r.width.saturating_sub(2)), usize::from(r.height.saturating_sub(2)));
    let (width, rows) = inner(info);
    let manage = Line::styled(" M manage ", style::dim()).right_aligned();
    let block = style::block(format!(" #{channel} · notify {level} "), false).title_bottom(manage);
    frame.render_widget(Paragraph::new(about(entry, width, rows)).block(block), info);
    let (width, rows) = inner(members);
    let (title, lines) = match entry {
        Some(entry) => {
            let items: Vec<Vec<Span<'static>>> = entry.members.iter().map(|m| member(m, entry, data, now)).collect();
            (format!(" members {} ", entry.members.len()), pack(&items, width, rows))
        }
        None => (" members ".to_owned(), vec![Line::styled("reading the channels…", style::dim())]),
    };
    frame.render_widget(Paragraph::new(lines).block(style::block(title, false)), members);
    let (width, rows) = inner(activity);
    let mut lines: Vec<Line> =
        data.activity.of(channel).take(rows).map(|e| style::clip(told(e, data, now), width)).collect();
    if lines.is_empty() {
        lines.push(Line::styled("nothing yet", style::dim()));
    }
    frame.render_widget(Paragraph::new(lines).block(style::block(" activity ".to_owned(), false)), activity);
}

/// The panes of the header in `area`: the channel's, its members' and its activity's; none when it is one line.
pub fn panes(area: Rect) -> Option<[Rect; 3]> {
    (area.height >= ROWS)
        .then(|| Layout::horizontal([Constraint::Fill(2), Constraint::Fill(2), Constraint::Fill(3)]).areas(area))
}

/// The header in one line: the channel, its notify level, how many members it has and its latest activity.
fn summary(entry: Option<&ChannelEntry>, channel: &ChannelId, data: &Data, now: DateTime<Utc>) -> Line<'static> {
    let members = entry.map(|e| e.members.len()).map_or_else(String::new, |n| {
        let members = if n == 1 { "member" } else { "members" };
        format!(" · {n} {members}")
    });
    let about = format!(" · notify {}{members} · ", data.level(channel));
    let mut spans = vec![Span::styled(format!("#{channel}"), Style::new().bold()), Span::styled(about, style::dim())];
    match data.activity.of(channel).next() {
        Some(event) => spans.extend(told(event, data, now).spans),
        None => spans.push(Span::styled("no activity yet", style::dim())),
    }
    Line::from(spans)
}

/// The channel's description, wrapped and cut to fit the `rows` but one of `width`, then its owner and messenger.
fn about(entry: Option<&ChannelEntry>, width: usize, rows: usize) -> Vec<Line<'static>> {
    let Some(entry) = entry else { return vec![Line::styled("reading the channels…", style::dim())] };
    let room = rows.saturating_sub(1).max(1);
    let mut lines = match &entry.description {
        Some(text) => {
            let mut wrapped = style::rows(text, width);
            if wrapped.len() > room {
                wrapped.truncate(room);
                let last = wrapped.last_mut().expect("a line");
                if last.chars().count() >= width {
                    last.pop();
                }
                last.push('…');
            }
            wrapped.into_iter().map(Line::raw).collect()
        }
        None => vec![Line::styled("no description", style::dim())],
    };
    let owner = entry.owner.as_ref().map_or_else(|| "none".to_owned(), ToString::to_string);
    lines.push(Line::from(vec![
        Span::styled("owner ", style::dim()),
        Span::raw(owner),
        Span::styled(" · messenger ", style::dim()),
        Span::raw(entry.messenger().to_string()),
    ]));
    lines
}

/// A member: an ant with its state's mark and its face, a human by name; then a mark if it owns the channel, and
/// another if it is its messenger.
pub fn member(user: &UserId, entry: &ChannelEntry, data: &Data, now: DateTime<Utc>) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    if !data.is_human(user) {
        let entries: Vec<&ColonyEntry> = data.live.colony.iter().filter(|e| e.ant == *user).collect();
        let (mark, color) = nav::marker(&entries, now);
        spans.extend([Span::styled(mark, Style::new().fg(color)), Span::raw(" "), data.looks.face(user)]);
        spans.push(Span::raw(" "));
    }
    spans.push(Span::raw(user.to_string()));
    if entry.owner.as_ref() == Some(user) {
        spans.push(Span::styled(format!(" {OWNER}"), Style::new().fg(style::WARNING)));
    }
    if entry.messenger() == *user {
        spans.push(Span::styled(format!(" {MESSENGER}"), Style::new().fg(style::INFO)));
    }
    spans
}

/// `items` side by side in lines `width` wide, at most `rows` of them, the last ending with how many more did not fit.
fn pack(items: &[Vec<Span<'static>>], width: usize, rows: usize) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    let mut line: Vec<Span<'static>> = Vec::new();
    let mut used = 0;
    let mut at = 0;
    while let Some(item) = items.get(at) {
        let wide: usize = item.iter().map(Span::width).sum();
        let last = lines.len() + 1 >= rows;
        let left = items.len() - at - 1;
        let more = if last && left > 0 { format!("  +{left}").len() } else { 0 };
        if line.is_empty() || used + 2 + wide + more <= width {
            if !line.is_empty() {
                line.push(Span::raw("  "));
                used += 2;
            }
            line.extend(item.iter().cloned());
            used += wide;
            at += 1;
        } else if last {
            line.push(Span::styled(format!("  +{}", items.len() - at), style::dim()));
            break;
        } else {
            lines.push(Line::from(std::mem::take(&mut line)));
            used = 0;
        }
    }
    if !line.is_empty() {
        lines.push(Line::from(line));
    }
    lines
}

/// An event in one line: how long ago, who did it if known, and what.
fn told(event: &Event, data: &Data, now: DateTime<Utc>) -> Line<'static> {
    let mut spans = vec![Span::styled(format!("{} ", colony::short(colony::since(event.at, now))), style::dim())];
    if let Some(by) = &event.by {
        spans.push(Span::styled(format!("{by} "), Style::new().fg(Color::Blue).bold()));
    }
    let title = |thread| excerpt(&data.title(thread), 60);
    match &event.what {
        What::Posted(thread) => spans.extend([Span::styled("new thread ", style::dim()), Span::raw(title(thread))]),
        What::Replied(thread) => spans.extend([Span::styled("replied on ", style::dim()), Span::raw(title(thread))]),
        What::Reacted(thread, reaction) => spans.extend([
            Span::styled(format!("reacted {} on ", react::glyph(reaction.as_str())), style::dim()),
            Span::raw(title(thread)),
        ]),
        What::State(thread, state) => spans.extend([
            Span::raw(title(thread)),
            Span::styled(" now ", style::dim()),
            Span::styled(state.label(), Style::new().fg(state.color())),
        ]),
        What::Members(added, removed) => {
            let changes = added.iter().map(|m| format!("+{m}")).chain(removed.iter().map(|m| format!("−{m}")));
            spans.extend([Span::styled("members ", style::dim()), Span::raw(changes.collect::<Vec<_>>().join(" "))]);
        }
        What::Messenger(Some(messenger)) => {
            spans.extend([Span::styled("messenger now ", style::dim()), Span::raw(messenger.to_string())]);
        }
        What::Messenger(None) => spans.push(Span::styled("messenger back to the default", style::dim())),
        What::Owner(Some(owner)) => {
            spans.extend([Span::styled("owner now ", style::dim()), Span::raw(owner.to_string())]);
        }
        What::Owner(None) => spans.push(Span::styled("owner cleared", style::dim())),
        What::Description => spans.push(Span::styled("description changed", style::dim())),
    }
    Line::from(spans)
}

#[cfg(test)]
mod tests {
    use ants_proto::ChannelPost;

    use super::*;
    use crate::test_support::live::{at, user};
    use crate::tui::model::Model;
    use crate::tui::test_support::{channel_screen, post};

    /// Alice's model showing #ops, dev-a in the colony, after bob opened a thread on it.
    fn ops() -> Model {
        let description = "Deploys, outages and the on-call rota of the production site.";
        let mut m = crate::tui::test_support::ops(description, &["dev-a"]);
        let ops: ChannelId = "ops".parse().unwrap();
        m.stored(vec![post(1, &ChannelPost::new(ops, user("bob"), "the site is down"))], at(1));
        m
    }

    #[test]
    fn the_header_shows_the_channel_its_members_and_its_activity_side_by_side() {
        let m = ops();
        let shown = channel_screen(&m, 130, 30);
        let lines: Vec<&str> = shown.lines().collect();
        assert!(lines[0].contains("#ops · notify humans") && lines[0].contains("members 4"), "{shown}");
        assert!(lines[0].contains("activity"), "{shown}");
        assert!(lines[1].contains("Deploys, outages and the on-call"), "{shown}");
        assert!(lines[3].contains("owner dev-a · messenger dev-m"), "{shown}");
        assert!(shown.contains("1m bob new thread the site is down"), "{shown}");
        assert!(lines[5].starts_with("‹ all 1 · waiting on me 0"), "the tabs under it: {shown}");
    }

    #[test]
    fn the_header_is_one_line_on_a_narrow_or_short_screen() {
        let m = ops();
        for (width, height) in [(99, 30), (130, 23)] {
            let shown = channel_screen(&m, width, height);
            let first = shown.lines().next().unwrap();
            assert_eq!(first, "#ops · notify humans · 4 members · 1m bob new thread the site is down", "{shown}");
            assert!(shown.lines().nth(1).unwrap().starts_with("‹ all 1"), "{shown}");
        }
    }

    #[test]
    fn members_show_ants_with_their_state_and_face_humans_by_name_and_the_owner_and_messenger_marked() {
        let m = ops();
        let entry = &m.data.channels[0];
        let text = |user: &str| -> String {
            member(&self::user(user), entry, &m.data, at(1)).iter().map(|s| s.content.as_ref()).collect()
        };
        let face = |user: &str| m.data.looks.face(&self::user(user)).content.into_owned();
        assert_eq!(text("alice"), "alice");
        assert_eq!(text("dev-a"), format!("● {} dev-a {OWNER}", face("dev-a")), "live, and the owner");
        assert_eq!(text("dev-m"), format!("○ {} dev-m {MESSENGER}", face("dev-m")), "not in the colony");
        let items: Vec<Vec<Span<'static>>> =
            ["alice", "bob", "carol", "dave", "erin"].map(|u| vec![Span::raw(u)]).to_vec();
        let packed: Vec<String> = pack(&items, 14, 2).iter().map(ToString::to_string).collect();
        assert_eq!(packed, ["alice  bob", "carol  +2"], "those that do not fit counted");
    }
}
