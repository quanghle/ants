//! An open thread: its title, channels, state and the ants' progress on it, then its messages as `ants thread show`
//! renders them, their text as markdown, the latest at the bottom unless scrolled up, or opened from a search at
//! the message found. The timeline (ants-ofmi.5) is to replace the plain rendering.

use std::cell::Cell;

use ants_proto::Uuid;
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::frame::{self, ago};
use super::model::{Data, Key, To};
use super::{markdown, work};
use crate::live::History;
use crate::thread::{Entry, Kind};

/// How far a message's text is indented under its heading.
const INDENT: &str = "    ";

#[derive(Debug)]
pub struct Page {
    pub history: History,
    /// How many lines up from the latest the view is scrolled.
    pub scroll: usize,
    /// How far up the view scrolls at most, as last drawn.
    top: Cell<Option<usize>>,
    /// The message the thread was opened at, highlighted.
    at: Option<Uuid>,
    /// Whether the view holds `at` at its top, until scrolled.
    held: bool,
    /// How far up the view was scrolled, as last drawn.
    drawn: Cell<Option<usize>>,
}

impl Page {
    pub fn new(thread: ants_proto::ThreadId) -> Self {
        Self {
            history: History::new(thread),
            scroll: 0,
            top: Cell::new(None),
            at: None,
            held: false,
            drawn: Cell::new(None),
        }
    }

    /// The thread, shown from `message` on once read.
    pub fn at(thread: ants_proto::ThreadId, message: Uuid) -> Self {
        Self { at: Some(message), held: true, ..Self::new(thread) }
    }

    pub const KEYS: &str = "↑↓ PgUp/PgDn scroll · End latest · Esc back";

    pub fn key(&mut self, key: Key) -> Option<To> {
        let top = self.top.get().unwrap_or(usize::MAX);
        let scroll = if self.held { self.drawn.get().unwrap_or(self.scroll) } else { self.scroll };
        self.scroll = match key {
            Key::Up | Key::Char('k') => scroll + 1,
            Key::Down | Key::Char('j') => scroll.saturating_sub(1),
            Key::PageUp => scroll + 10,
            Key::PageDown => scroll.saturating_sub(10),
            Key::Home => top,
            Key::End => 0,
            _ => return None,
        }
        .min(top);
        self.held = false;
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
        let thread = &self.history.thread;
        let summary = data.live.summary(thread);
        let mut title = format!(" {} ", data.title(thread));
        if let Some(s) = summary.filter(|s| !s.channels.is_empty()) {
            let channels: Vec<String> = s.channels.iter().map(|c| format!("#{c}")).collect();
            title = format!("{title}· {} ", channels.join(" "));
        }
        let width = usize::from(area.width.saturating_sub(2));
        let mut lines = vec![work::why(data, thread)];
        if let Some((line, at)) = data.live.progress_of(thread, width.saturating_sub(16)) {
            lines.push(Line::styled(format!("▶ {line} ({})", ago(at, now)), Style::new().fg(Color::Green)));
        }
        if self.history.loading {
            lines.push(Line::styled("reading the thread…", frame::dim()));
        }
        let (shown, at) = messages(&self.history.entries, width, self.at);
        let at = at.map(|i| i + lines.len()).filter(|_| self.held);
        lines.extend(shown);
        let height = usize::from(area.height.saturating_sub(2));
        let most = lines.len().saturating_sub(height);
        self.top.set(Some(most));
        let top = at.map_or_else(|| most.saturating_sub(self.scroll), |at| at.min(most));
        self.drawn.set(Some(most - top));
        let paragraph = Paragraph::new(lines)
            .block(frame::block(title, focused))
            .scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
        frame.render_widget(paragraph, area);
    }
}

/// The messages as `ants thread show` renders them, wrapped at `width`: each one's heading, then its text indented,
/// as markdown for a message, post or note, and the reactions to it. The heading of message `at` is highlighted and
/// its line returned.
fn messages(entries: &[Entry], width: usize, at: Option<Uuid>) -> (Vec<Line<'static>>, Option<usize>) {
    let inner = width.saturating_sub(INDENT.len());
    let mut lines = Vec::new();
    let mut found = None;
    for e in entries {
        let mut style = Style::new().fg(Color::Blue);
        if at.is_some() && e.id == at {
            found = Some(lines.len());
            style = style.add_modifier(Modifier::REVERSED);
        }
        let heading = crate::thread::heading(e);
        lines.extend(frame::wrap(&heading, width).into_iter().map(|l| Line::styled(l, style)));
        let text = crate::thread::body(e, entries);
        let body = match e.kind {
            Kind::Message | Kind::Escalation | Kind::Mention | Kind::Post | Kind::Note if e.question.is_none() => {
                markdown::render(&text, inner)
            }
            _ => text.lines().flat_map(|l| frame::wrap(l, inner)).map(Line::raw).collect(),
        };
        let reactions = crate::inbox::render_reactions(&e.reactions, "");
        let reactions = reactions.lines().map(|l| Line::styled(l.to_owned(), frame::dim()));
        for line in body.into_iter().chain(reactions) {
            if line.width() == 0 {
                lines.push(line);
                continue;
            }
            let mut spans = vec![Span::raw(INDENT)];
            spans.extend(line.spans);
            lines.push(Line::from(spans).style(line.style));
        }
    }
    (lines, found)
}

#[cfg(test)]
mod tests {
    use ants_proto::ThreadId;

    use super::*;
    use crate::live::tests::message;

    #[test]
    fn scrolling_goes_up_from_the_latest_and_back() {
        let mut page = Page::new(ThreadId::generate());
        page.key(Key::PageUp);
        page.key(Key::Up);
        assert_eq!(page.scroll, 11);
        page.key(Key::PageDown);
        page.key(Key::Down);
        page.key(Key::Down);
        assert_eq!(page.scroll, 0, "not below the latest");
        page.key(Key::Home);
        page.key(Key::End);
        assert_eq!(page.scroll, 0);
        assert_eq!(page.key(Key::Enter), None);
    }

    #[test]
    fn opened_at_a_message_it_shows_from_there_until_scrolled() {
        let thread = ThreadId::generate();
        let entries: Vec<Entry> =
            (1..=20).map(|n| message(n, &thread, "dev-a", "alice", &format!("line {n}"))).collect();
        let (id, sixteenth) = (entries[4].id.unwrap(), entries[15].id.unwrap());
        let mut page = Page::at(thread, id);
        page.history.entries = entries;
        page.history.loading = false;
        let data = &crate::tui::model::tests::model().data;
        let first = |page: &Page| {
            let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(100, 12)).unwrap();
            terminal.draw(|f| page.draw(f, f.area(), data, Utc::now(), true)).unwrap();
            let row: String = (1..99).map(|x| terminal.backend().buffer()[(x, 1)].symbol().to_owned()).collect();
            row.trim_end().to_owned()
        };
        assert!(first(&page).ends_with(&format!("(message {id})")), "{}", first(&page));
        page.key(Key::Up);
        assert_eq!(first(&page), "    line 4", "scrolling starts from the message");
        page.key(Key::End);
        assert!(first(&page).ends_with(&format!("(message {sixteenth})")), "the latest at the bottom");
    }

    #[test]
    fn a_message_s_text_shows_as_markdown_under_its_heading() {
        let thread = ThreadId::generate();
        let mut tool = message(2, &thread, "dev-a", "alice", "ls *.rs *.md");
        tool.kind = Kind::Dead;
        let entries = [message(1, &thread, "dev-a", "alice", "**done**:\n\n- one\n- two"), tool];
        let (lines, at) = messages(&entries, 120, None);
        assert_eq!(at, None);
        let shown: Vec<String> = lines.iter().map(ToString::to_string).collect();
        let id = entries[0].id.unwrap();
        assert!(shown[0].ends_with(&format!("dev-a -> alice (message {id})")), "{}", shown[0]);
        assert_eq!(shown[1..5], ["    done:", "", "    • one", "    • two"]);
        assert_eq!(shown[6], "    ls *.rs *.md", "only messages, posts and notes are markdown");
        let done = lines[1].spans.iter().find(|s| s.content == "done").unwrap();
        assert_eq!(done.style, Style::new().add_modifier(Modifier::BOLD));
    }
}
