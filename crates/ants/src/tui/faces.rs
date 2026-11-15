//! The ants' avatars in ants chat ([`ants_proto::Avatar`]): each ant's one-line face before its name in the nav, home,
//! thread and channel views, and its head in the ant view, moving with what the ant does ([`Mood`]). [`Io`] reads the
//! identity records again every while; only the shown ant's head moves, and the screen is drawn again only when its
//! frame changes.

use std::time::Duration;

use ants_proto::{AntIdentity, Avatar, Frame, Mood, Motion, Palette, Rgb, RosterEntry, Theme, UserId};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};

use super::model::Data;
use crate::avatars::{Depth, Identities};
use crate::live::maybe;
use crate::progress::Stand;
use crate::roster;

/// How often the identity records are read again, for avatars changed and prompts counted meanwhile.
const READ_EVERY: Duration = Duration::from_secs(30);

/// How the ants look: their identity records, the colours and theme drawn in, and the shown ant's head as it moves.
#[derive(Debug)]
pub struct Looks {
    identities: Identities,
    depth: Depth,
    theme: Theme,
    /// Whether heads move (`[cli] animate`).
    animate: bool,
    head: Option<Head>,
}

/// The shown ant's head: its motion in its mood, and the frame last drawn.
#[derive(Debug)]
struct Head {
    ant: UserId,
    seed: String,
    motion: Motion,
    frame: Frame,
}

impl Default for Looks {
    fn default() -> Self {
        Self::new(Depth::Plain, Theme::Dark, true)
    }
}

impl Looks {
    pub fn new(depth: Depth, theme: Theme, animate: bool) -> Self {
        Self { identities: Identities::new(), depth, theme, animate, head: None }
    }

    /// The looks of a terminal of `depth`, in the theme and with the motion `[cli]` sets.
    pub fn of_terminal() -> Self {
        let depth = depth(
            std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty()),
            std::env::var("COLORTERM").ok().as_deref(),
        );
        let theme = if depth == Depth::Plain { Theme::Dark } else { crate::avatars::theme() };
        let animate = crate::defaults::cli().map_or(true, |cli| cli.animate);
        Self::new(depth, theme, animate)
    }

    pub fn set(&mut self, identities: Identities) {
        self.identities = identities;
    }

    pub fn identity(&self, ant: &UserId) -> Option<&AntIdentity> {
        self.identities.get(ant)
    }

    /// Whether `user` has an identity record, as only ants do.
    pub fn known(&self, user: &UserId) -> bool {
        self.identities.contains_key(user)
    }

    fn avatar(&self, ant: &UserId) -> Avatar {
        Avatar::of(ant, self.identity(ant))
    }

    fn style(&self, color: Rgb) -> Style {
        match self.depth {
            Depth::Plain => Style::new(),
            Depth::Palette => Style::new().fg(Color::Indexed(color.ansi256())),
            Depth::True => Style::new().fg(Color::Rgb(color.0, color.1, color.2)),
        }
    }

    fn color(&self, color: Option<Rgb>) -> Option<Color> {
        let color = color?;
        match self.depth {
            Depth::Plain => None,
            Depth::Palette => Some(Color::Indexed(color.ansi256())),
            Depth::True => Some(Color::Rgb(color.0, color.1, color.2)),
        }
    }

    /// The ant's face, such as `(ovo)`, in its colour.
    pub fn face(&self, ant: &UserId) -> Span<'static> {
        let avatar = self.avatar(ant);
        Span::styled(avatar.face(), self.style(avatar.color(self.theme)))
    }

    /// The ant's head in its frame now, in half-blocks in its colours; none without colour, where its face stands for
    /// it.
    pub fn head(&self, ant: &UserId) -> Option<Vec<Line<'static>>> {
        if self.depth == Depth::Plain {
            return None;
        }
        let avatar = self.avatar(ant);
        let stage = self.identity(ant).map_or_else(|| ants_proto::Stage::of(0), AntIdentity::stage);
        let frame = self.head.as_ref().filter(|h| h.ant == *ant).map_or_else(Frame::default, |h| h.frame);
        let palette = Palette::new(avatar.hue, self.theme, stage, frame.grey, frame.alarm);
        let grid = avatar.head(stage, &frame);
        let lines = grid
            .halfblocks(&palette)
            .into_iter()
            .map(|row| {
                let spans: Vec<Span<'static>> = row
                    .into_iter()
                    .map(|cell| {
                        let mut style = Style::new();
                        if let Some(fg) = self.color(cell.fg) {
                            style = style.fg(fg);
                        }
                        if let Some(bg) = self.color(cell.bg) {
                            style = style.bg(bg);
                        }
                        Span::styled(cell.ch.to_string(), style)
                    })
                    .collect();
                Line::from(spans)
            })
            .collect();
        Some(lines)
    }

    /// Move the head of the ant shown, in its mood, to second `t` of a clock that only goes forward; with None, let it
    /// go. Whether the head's frame changed, so that the screen is to be drawn again.
    pub fn tick(&mut self, shown: Option<(UserId, Mood)>, t: f64) -> bool {
        let Some((ant, mood)) = shown else {
            return self.head.take().is_some();
        };
        let seed = self.identity(&ant).map_or_else(|| ant.to_string(), |record| record.seed().to_owned());
        let same = self.head.as_ref().is_some_and(|h| h.ant == ant && h.seed == seed && h.motion.mood() == mood);
        let before = self.head.as_ref().filter(|_| same).map(|h| h.frame);
        let animate = self.animate;
        let head = match &mut self.head {
            Some(head) if same => head,
            head => head.insert(Head { ant, motion: Motion::new(&seed, mood, t), seed, frame: Frame::default() }),
        };
        head.frame = if animate {
            head.motion.frame(t)
        } else {
            Frame {
                closed: matches!(mood, Mood::Paused | Mood::Offline),
                grey: mood == Mood::Offline,
                ..Frame::default()
            }
        };
        before != Some(head.frame)
    }
}

/// How long after a message from an ant arrives its head nods.
const NODS_FOR: Duration = Duration::from_secs(4);

/// What `ant` does at `now`, as its head shows it: offline when no daemon serving it beats or its harness is signed
/// out; stuck when its harness keeps failing to start or a prompt of it failed; paused while a provider limit holds
/// it back; waiting on a human for a tool request or a question; nodding a few seconds after a message from it
/// arrived; chewing on a tool call; thinking; else idle.
pub fn mood(data: &Data, ant: &UserId, now: DateTime<Utc>) -> Mood {
    let entries: Vec<&RosterEntry> =
        data.live.roster.iter().filter(|e| e.ant == *ant && roster::State::of(e, now) == roster::State::Live).collect();
    let stands: Vec<(&Stand, bool)> = data.live.progress.values().filter_map(|p| p.status.stand(ant)).collect();
    if entries.is_empty() || entries.iter().any(|e| e.signed_out.is_some()) || stands.iter().any(|(_, out)| *out) {
        return Mood::Offline;
    }
    if entries.iter().any(|e| e.failing.is_some()) || stands.iter().any(|(s, _)| **s == Stand::Failed) {
        return Mood::Stuck;
    }
    if entries.iter().any(|e| e.limited.is_some()) {
        return Mood::Paused;
    }
    let asks = data.live.order.iter().any(|t| data.question(t).is_some_and(|q| q.from == *ant));
    if asks
        || data.live.approvals.iter().any(|r| r.ant == *ant)
        || stands.iter().any(|(s, _)| matches!(s, Stand::Approval(_)))
    {
        return Mood::Waiting;
    }
    let lately = now - chrono::Duration::from_std(NODS_FOR).unwrap_or_default();
    let arrived = data.live.order.iter().filter_map(|t| data.facts.get(t)).flat_map(|f| &f.inbox);
    if arrived.into_iter().any(|i| i.from == *ant && i.at > lately && i.at <= now) {
        return Mood::Message;
    }
    if stands.iter().any(|(s, _)| matches!(s, Stand::Tool { .. })) {
        return Mood::Tool;
    }
    if stands.iter().any(|(s, _)| matches!(s, Stand::Thinking | Stand::Working | Stand::Writing)) {
        return Mood::Thinking;
    }
    Mood::Idle
}

/// The colours a terminal shows: none with `NO_COLOR`, else 24-bit colour as `COLORTERM` tells, else the 256 palette.
fn depth(no_color: bool, colorterm: Option<&str>) -> Depth {
    match colorterm {
        _ if no_color => Depth::Plain,
        Some("truecolor" | "24bit") => Depth::True,
        _ => Depth::Palette,
    }
}

/// The identity records, read now and every while.
pub struct Io {
    js: jetstream::Context,
    every: tokio::time::Interval,
    read: Option<LocalBoxFuture<'static, Identities>>,
}

impl Io {
    pub fn new(js: jetstream::Context) -> Self {
        Self { js, every: tokio::time::interval(READ_EVERY), read: None }
    }

    /// The records next read. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Identities {
        loop {
            tokio::select! {
                _ = self.every.tick() => {
                    if self.read.is_none() {
                        self.refresh();
                    }
                }
                identities = maybe(&mut self.read) => {
                    self.read = None;
                    return identities;
                }
            }
        }
    }

    /// Read them again now.
    pub fn refresh(&mut self) {
        let js = self.js.clone();
        self.read = Some(async move { crate::avatars::read_or_none(&js).await }.boxed_local());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(s: &str) -> UserId {
        s.parse().unwrap()
    }

    #[test]
    fn colour_follows_no_color_and_colorterm() {
        assert_eq!(depth(true, Some("truecolor")), Depth::Plain);
        assert_eq!(depth(false, Some("24bit")), Depth::True);
        assert_eq!(depth(false, None), Depth::Palette);
    }

    #[test]
    fn the_shown_head_moves_with_its_mood_and_redraws_only_on_a_new_frame() {
        let dev = user("dev-a");
        let mut looks = Looks::new(Depth::True, Theme::Dark, true);
        assert!(looks.tick(Some((dev.clone(), Mood::Offline)), 0.0), "a head shown is drawn");
        assert!(!looks.tick(Some((dev.clone(), Mood::Offline)), 5.0), "an offline head stays still");
        let grey = looks.head(&dev).unwrap();
        assert_eq!(grey.len(), ants_proto::HEAD_ROWS / 2);
        assert!(looks.tick(Some((dev.clone(), Mood::Tool)), 5.0));
        let changes =
            (1..40).filter(|n| looks.tick(Some((dev.clone(), Mood::Tool)), 5.0 + f64::from(*n) * 0.05)).count();
        assert!((3..40).contains(&changes), "chewing changes some frames, not every tick: {changes}");
        assert_ne!(looks.head(&dev).unwrap(), grey);
        assert!(looks.tick(None, 8.0) && !looks.tick(None, 9.0), "a head no longer shown stops");

        let mut still = Looks::new(Depth::True, Theme::Dark, false);
        assert!(still.tick(Some((dev.clone(), Mood::Tool)), 0.0));
        let changes = (1..100).filter(|n| still.tick(Some((dev.clone(), Mood::Tool)), f64::from(*n) * 0.05)).count();
        assert_eq!(changes, 0, "animate = false keeps heads still");
    }

    #[test]
    fn the_mood_follows_the_roster_the_threads_and_the_inbox() {
        use ants_proto::{SignInNeeded, StatusEvent, StatusUpdate, ThreadId};

        use crate::live::tests::{at, message};
        use crate::roster::tests::entry;

        let (dev, thread) = (user("dev-a"), ThreadId::generate());
        let mut m = crate::tui::model::tests::model();
        assert_eq!(mood(&m.data, &dev, at(2)), Mood::Offline, "not in the roster");
        m.data.live.roster(vec![entry("dev-a", 3600, 2, at(2))], at(2));
        assert_eq!(mood(&m.data, &dev, at(2)), Mood::Idle);
        m.stored(vec![message(1, &thread, "dev-a", "alice", "done")], at(2));
        assert_eq!(mood(&m.data, &dev, at(2)), Mood::Message, "a message from it just arrived");
        assert_eq!(mood(&m.data, &dev, at(9)), Mood::Idle, "nodding for a few seconds");
        let tool = StatusUpdate::Tool { id: "t1".to_owned(), title: None, tool_kind: None, status: None, paths: None };
        m.take(crate::live::Event::Status(StatusEvent::new(dev.clone(), thread, tool)), at(9));
        assert_eq!(mood(&m.data, &dev, at(9)), Mood::Tool);
        let mut out = entry("dev-a", 3600, 2, at(2));
        out.signed_out = Some(SignInNeeded { since: at(1), sign_in: None });
        m.data.live.roster(vec![out], at(9));
        assert_eq!(mood(&m.data, &dev, at(9)), Mood::Offline, "signed out");
    }

    #[test]
    fn in_colour_the_ant_view_draws_the_head() {
        let mut m = crate::tui::model::tests::model();
        m.data.live.roster(
            vec![crate::roster::tests::entry("dev-a", 3600, 2, crate::live::tests::at(2))],
            crate::live::tests::at(2),
        );
        m.data.looks = Looks::new(Depth::True, Theme::Dark, true);
        m.select(crate::tui::nav::Item::Ant(user("dev-a")));
        let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(140, 36)).unwrap();
        terminal.draw(|f| crate::tui::frame::draw(f, &m, crate::live::tests::at(3))).unwrap();
        let cells = terminal.backend().buffer().content.iter().filter(|c| matches!(c.symbol(), "▀" | "▄")).count();
        assert!(cells > 20, "{cells} half-blocks");
    }

    #[test]
    fn without_colour_the_face_stands_for_the_head() {
        let looks = Looks::default();
        let dev = user("dev-a");
        assert_eq!(looks.head(&dev), None);
        assert_eq!(looks.face(&dev), Span::raw("{.~.}"));
    }
}
