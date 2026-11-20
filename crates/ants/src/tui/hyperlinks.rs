//! OSC 8 is applied after drawing: escape sequences never enter the text measured by ratatui.

use std::io::{self, Write};

use pulldown_cmark::{Event, Parser, Tag, TagEnd};
use ratatui::backend::{Backend as _, CrosstermBackend};
use ratatui::buffer::{Buffer, Cell};
use ratatui::crossterm::cursor::{RestorePosition, SavePosition};
use ratatui::crossterm::queue;
use ratatui::layout::Position;

use super::model::{Model, Page};

pub fn supported() -> bool {
    known(
        &std::env::var("TERM").unwrap_or_default(),
        &std::env::var("TERM_PROGRAM").unwrap_or_default(),
        ["VTE_VERSION", "WT_SESSION", "KONSOLE_VERSION"].iter().any(|key| std::env::var_os(key).is_some()),
    )
}

fn known(term: &str, program: &str, other: bool) -> bool {
    other
        || ["kitty", "foot", "wezterm", "ghostty"].iter().any(|name| term.contains(name))
        || ["iTerm.app", "WezTerm", "vscode", "ghostty", "contour"].contains(&program)
}

fn safe(url: &str) -> bool {
    ["https://", "http://", "mailto:"].iter().any(|scheme| url.starts_with(scheme))
        && !url.chars().any(|c| c.is_control() || c.is_whitespace())
}

fn bare(text: &str) -> Vec<String> {
    let mut urls = Vec::new();
    for (at, _) in text.match_indices("http") {
        let tail = &text[at..];
        if !(tail.starts_with("https://") || tail.starts_with("http://")) {
            continue;
        }
        let tail = tail
            .split(|c: char| {
                c.is_whitespace()
                    || c.is_control()
                    || matches!(c, '<' | '>' | '"' | '\'' | '\u{2502}' | '\u{2510}' | '\u{2518}')
            })
            .next()
            .unwrap_or_default();
        let tail = tail.trim_end_matches(['.', ',', ';', ':', '!', '?', ']', '}']);
        let mut tail = tail;
        while tail.ends_with(')')
            && tail.chars().filter(|&c| c == ')').count() > tail.chars().filter(|&c| c == '(').count()
        {
            tail = &tail[..tail.len() - 1];
        }
        if safe(tail) {
            urls.push(tail.to_owned());
        }
    }
    urls
}

fn links(text: &str) -> Vec<(String, String)> {
    let mut links = Vec::new();
    let mut current = None;
    for event in Parser::new(text) {
        match event {
            Event::Start(Tag::Link { dest_url, .. }) if safe(&dest_url) => {
                current = Some((String::new(), dest_url.to_string()));
            }
            Event::Text(text) | Event::Code(text) if current.is_some() => {
                current.as_mut().expect("a link").0.extend(text.chars().filter(|c| !c.is_control()));
            }
            Event::End(TagEnd::Link) => {
                if let Some(link) = current.take().filter(|(label, _)| !label.is_empty()) {
                    links.push(link);
                }
            }
            _ => {}
        }
    }
    links.extend(bare(text).into_iter().map(|url| (url.clone(), url)));
    links
}

pub struct Link {
    url: String,
    cells: Vec<(u16, u16, Cell)>,
}

pub fn find(buffer: &Buffer, model: &Model) -> Vec<Link> {
    let mut found = Vec::new();
    let mut destinations = Vec::new();
    if let Page::Thread(page) = model.page() {
        let visible = model.ui.mouse.visible_messages();
        for entry in page.history.entries.iter().filter(|e| visible.contains(&e.seq)) {
            let candidates = links(&entry.text);
            destinations.extend(candidates.iter().map(|(_, url)| url.clone()));
            let mut cells = Vec::new();
            for y in buffer.area.y..buffer.area.bottom() {
                for x in buffer.area.x..buffer.area.right() {
                    if model.ui.mouse.message_at(Position::new(x, y)) == Some(entry.seq) {
                        cells.push((x, y, buffer[(x, y)].clone()));
                    }
                }
            }
            find_message(&cells, candidates, &mut found);
        }
    }
    for y in buffer.area.y..buffer.area.bottom() {
        let mut text = String::new();
        let mut offsets = Vec::new();
        for x in buffer.area.x..buffer.area.right() {
            offsets.push((text.len(), x));
            text.push_str(buffer[(x, y)].symbol());
        }
        for url in bare(&text) {
            if destinations.iter().any(|full| full.len() > url.len() && full.starts_with(&url)) {
                continue;
            }
            for (at, _) in text.match_indices(&url) {
                let cells = offsets
                    .iter()
                    .filter(|(offset, _)| *offset >= at && *offset < at + url.len())
                    .map(|&(_, x)| (x, y, buffer[(x, y)].clone()))
                    .collect();
                found.push(Link { url: url.clone(), cells });
            }
        }
    }
    found
}

fn find_message(cells: &[(u16, u16, Cell)], candidates: Vec<(String, String)>, found: &mut Vec<Link>) {
    let mut text = String::new();
    let mut offsets = Vec::new();
    for (at, (_, _, cell)) in cells.iter().enumerate() {
        let symbol: String = cell.symbol().chars().filter(|c| !c.is_whitespace()).collect();
        if !symbol.is_empty() {
            offsets.push((text.len(), at));
            text.push_str(&symbol);
        }
    }
    for (label, url) in candidates {
        let label: String = label.chars().filter(|c| !c.is_whitespace()).collect();
        if label.is_empty() {
            continue;
        }
        for (at, _) in text.match_indices(&label) {
            let mut matched = offsets.iter().filter(|(offset, _)| *offset >= at && *offset < at + label.len());
            if let Some(&(_, start)) = matched.next() {
                let end = matched.next_back().map_or(start, |&(_, at)| at);
                found.push(Link { url: url.clone(), cells: cells[start..=end].to_vec() });
            }
        }
    }
}

pub fn draw(links: &[Link], out: &mut impl Write) -> io::Result<()> {
    if links.is_empty() {
        return Ok(());
    }
    queue!(out, SavePosition)?;
    for link in links {
        write!(out, "\x1b]8;;{}\x1b\\", link.url)?;
        let mut backend = CrosstermBackend::new(&mut *out);
        backend.draw(link.cells.iter().map(|(x, y, cell)| (*x, *y, cell)))?;
        out.write_all(b"\x1b]8;;\x1b\\")?;
    }
    queue!(out, RestorePosition)?;
    out.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::{at, message};
    use crate::tui::{frame, test_support, thread};

    #[test]
    fn links_are_emitted_without_passing_message_controls_to_the_terminal() {
        assert!(known("xterm-kitty", "", false));
        assert!(known("xterm-256color", "iTerm.app", false));
        assert!(!known("xterm", "Apple_Terminal", false));
        assert!(!safe("https://host/\x1b]0;injected"));
        assert_eq!(bare("See https://example.org/a_(b)."), ["https://example.org/a_(b)"]);
        let mut model = test_support::model();
        let id = ants_proto::ThreadId::generate();
        let mut page = thread::Page::new(id.clone());
        let entry = message(
            1,
            &id,
            "alice",
            "dev-a",
            "[documentation](https://example.org/docs) and https://example.org/plain",
        );
        page.history.loaded(&id, Ok(vec![entry])).unwrap();
        model.pages.push(Page::Thread(page));
        let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(120, 25)).unwrap();
        let frame = terminal.draw(|f| frame::draw(f, &model, at(1))).unwrap();
        let links = find(frame.buffer, &model);
        let mut out = Vec::new();
        draw(&links, &mut out).unwrap();
        let out = String::from_utf8(out).unwrap();
        assert!(out.contains("\x1b]8;;https://example.org/docs\x1b\\"), "{out:?}");
        assert!(out.contains("\x1b]8;;https://example.org/plain\x1b\\"), "{out:?}");
        assert!(out.contains("\x1b]8;;\x1b\\"), "{out:?}");
    }

    #[test]
    fn wrapped_links_keep_the_full_destination_and_identical_labels_stay_on_their_messages() {
        let mut model = test_support::model();
        let id = ants_proto::ThreadId::generate();
        let label = "documentation with a descriptive label spanning several terminal rows";
        let url = "https://example.org/a/long/path/that/spans/several/terminal/rows";
        let entries = vec![
            message(1, &id, "alice", "dev-a", &format!("[{label}]({url})")),
            message(2, &id, "dev-a", "alice", &format!("[{label}](https://other.example.org/)")),
        ];
        let mut page = thread::Page::new(id.clone());
        page.history.loaded(&id, Ok(entries)).unwrap();
        model.pages.push(Page::Thread(page));
        let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(52, 40)).unwrap();
        let drawn = terminal.draw(|f| frame::draw(f, &model, at(1))).unwrap();
        let links = find(drawn.buffer, &model);
        for (seq, destination) in [(1, url), (2, "https://other.example.org/")] {
            let link = links
                .iter()
                .find(|link| {
                    link.url == destination
                        && link
                            .cells
                            .iter()
                            .map(|(_, _, cell)| cell.symbol())
                            .collect::<String>()
                            .contains("documentation")
                })
                .unwrap_or_else(|| panic!("no label link for message {seq}"));
            assert!(link.cells.first().unwrap().1 < link.cells.last().unwrap().1, "the label wrapped");
            assert!(link.cells.iter().all(|(x, y, _)| model.ui.mouse.message_at(Position::new(*x, *y)) == Some(seq)));
        }
        model.overlay(crate::tui::model::Overlay::Help(super::super::help::Help::default()));
        let drawn = terminal.draw(|f| frame::draw(f, &model, at(1))).unwrap();
        assert!(find(drawn.buffer, &model).is_empty(), "hidden message labels must not link the help");
    }
}
