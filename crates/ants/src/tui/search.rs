//! Search from `ants chat`: `/` opens a prompt whose words `from:`, `to:`, `channel:`, `type:`, `since:` and `until:`
//! are the filters of `ants thread search` and whose other words its text; Enter runs it as `ants thread search` does
//! ([`crate::search::Search`]) and lists the latest matches newest first, each with where, who, how long ago and its
//! matching line. Enter on a match opens its thread at it; Esc closes the list.

use ants_proto::ChannelId;
use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use clap::ValueEnum as _;
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph};

use super::frame::{self, ago};
use super::model::{Data, Key, To};
use crate::live::maybe;
use crate::search::{self, Search};
use crate::thread::{Entry, Kind};

/// How many matches a search lists, the latest.
pub const LIMIT: usize = 50;
/// How wide the place and the sender of a match show.
const PLACE: usize = 24;
const SENDER: usize = 12;

/// The search `query` asks for at `now`: its filter words, each `key:value`, and its other words, the text to find.
pub fn parse(query: &str, now: DateTime<Utc>) -> Result<Search, String> {
    let mut search = Search { limit: LIMIT, ..Search::default() };
    let (mut since, mut until, mut text) = (None, None, Vec::new());
    for word in query.split_whitespace() {
        let Some((key, value)) = word.split_once(':').filter(|(_, value)| !value.is_empty()) else {
            text.push(word);
            continue;
        };
        let user = |value: &str| value.trim_start_matches('@').parse().map_err(|e| format!("{key}:{value}: {e}"));
        match key {
            "from" => search.from = Some(user(value)?),
            "to" => search.to = Some(user(value)?),
            "channel" => {
                let channel: ChannelId =
                    value.trim_start_matches('#').parse().map_err(|e| format!("channel:{value}: {e}"))?;
                search.channel = Some(channel);
            }
            "type" => {
                for kind in value.split(',') {
                    search.kinds.insert(Kind::from_str(kind, true).map_err(|_| unknown_type(kind))?);
                }
            }
            "since" => since = Some(search::parse_when(value)?),
            "until" => until = Some(search::parse_when(value)?),
            _ => text.push(word),
        }
    }
    if !text.is_empty() {
        search.grep = Some(text.join(" "));
    }
    (search.since, search.until) = search::bounds(since, until, now)
        .map_err(|e| e.to_string().replace("--since", "since:").replace("--until", "until:"))?;
    Ok(search)
}

fn unknown_type(kind: &str) -> String {
    let kinds: Vec<String> =
        Kind::value_variants().iter().filter_map(|k| Some(k.to_possible_value()?.get_name().to_owned())).collect();
    format!("type:{kind} is none of {}", kinds.join(", "))
}

/// The search prompt being typed.
#[derive(Debug, Default)]
pub struct Prompt {
    pub text: String,
    /// Why the query typed was refused.
    pub error: Option<String>,
}

/// What a key does to the prompt.
#[derive(Debug, PartialEq, Eq)]
pub enum Edit {
    Typing,
    Close,
    Run(String),
}

impl Prompt {
    pub fn key(&mut self, key: Key) -> Edit {
        self.error = None;
        match key {
            Key::Esc => return Edit::Close,
            Key::Enter => return Edit::Run(self.text.clone()),
            Key::Char(c) => self.text.push(c),
            Key::Backspace => {
                self.text.pop();
            }
            Key::Ctrl('u') => self.text.clear(),
            Key::Ctrl('w') => {
                let kept = self.text.trim_end().rfind(' ').map_or(0, |at| at + 1);
                self.text.truncate(kept);
            }
            _ => {}
        }
        Edit::Typing
    }

    /// The prompt as the footer shows it, with why the query was refused.
    pub fn line(&self) -> Line<'static> {
        let mut spans = vec![
            Span::styled(" /search ", Style::new().fg(Color::Cyan).bold()),
            Span::raw(self.text.clone()),
            Span::raw("▏"),
        ];
        match &self.error {
            Some(error) => spans.push(Span::styled(format!("  {error}"), Style::new().fg(Color::Red))),
            None => spans.push(Span::styled(
                "  from: to: channel: type: since: until: and text · Enter search · Esc close",
                frame::dim(),
            )),
        }
        Line::from(spans)
    }
}

/// A search's matches, as listed.
#[derive(Debug)]
pub struct Page {
    pub query: String,
    /// The matches newest first and how many earlier ones were left out, or why the search failed; none while it runs.
    pub found: Option<Result<(Vec<Entry>, usize), String>>,
    grep: Option<String>,
    selected: usize,
}

impl Page {
    pub fn new(query: String, search: &Search) -> Self {
        Self { query, found: None, grep: search.grep.clone(), selected: 0 }
    }

    pub const KEYS: &str = "↑↓ match · Enter open its thread at it · Esc close";

    /// Take the matches found, given oldest first.
    pub fn found(&mut self, found: Found) {
        self.found = Some(match found {
            Ok((mut entries, omitted)) => {
                entries.reverse();
                Ok((entries, omitted))
            }
            Err(e) => Err(format!("{e:#}")),
        });
    }

    fn matches(&self) -> &[Entry] {
        match &self.found {
            Some(Ok((entries, _))) => entries,
            _ => &[],
        }
    }

    pub fn key(&mut self, key: Key) -> Option<To> {
        let count = self.matches().len();
        self.selected = match key {
            Key::Enter => {
                let e = self.matches().get(self.selected)?;
                return Some(match e.id {
                    Some(id) => To::Message(e.thread_id.clone(), id),
                    None => To::Thread(e.thread_id.clone()),
                });
            }
            Key::Up | Key::Char('k') => self.selected.saturating_sub(1),
            Key::Down | Key::Char('j') => self.selected + 1,
            Key::PageUp => self.selected.saturating_sub(10),
            Key::PageDown => self.selected + 10,
            Key::Home => 0,
            Key::End => usize::MAX,
            _ => return None,
        }
        .min(count.saturating_sub(1));
        None
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, data: &Data, now: DateTime<Utc>, focused: bool) {
        let title = match &self.found {
            Some(Ok((entries, omitted))) if *omitted > 0 => {
                format!(" search {} · latest {} matches, {omitted} earlier left out ", self.query, entries.len())
            }
            Some(Ok((entries, _))) => format!(" search {} · {} matches ", self.query, entries.len()),
            _ => format!(" search {} ", self.query),
        };
        let block = frame::block(title, focused);
        let text = match &self.found {
            None => Some(Line::styled("searching…", frame::dim())),
            Some(Err(e)) => Some(Line::styled(format!("searching failed: {e}"), Style::new().fg(Color::Red))),
            Some(Ok((entries, _))) if entries.is_empty() => Some(Line::raw("no matching messages")),
            Some(Ok(_)) => None,
        };
        if let Some(text) = text {
            frame.render_widget(Paragraph::new(text).block(block), area);
            return;
        }
        let items: Vec<ListItem> = self.matches().iter().map(|e| ListItem::new(self.row(e, data, now))).collect();
        let list = List::new(items).block(block).highlight_style(frame::highlight(focused));
        frame.render_stateful_widget(list, area, &mut ListState::default().with_selected(Some(self.selected)));
    }

    /// A match: where it was, who sent it, how long ago, and its line matching the text, else its first.
    fn row(&self, e: &Entry, data: &Data, now: DateTime<Utc>) -> Line<'static> {
        let place = match &e.channel {
            Some(channel) => format!("#{channel}"),
            None => data.title(&e.thread_id),
        };
        let from = e.from.as_ref().map_or_else(|| "(a human)".to_owned(), ToString::to_string);
        let line = self.grep.as_deref().and_then(|grep| search::matched(&e.text, grep)).unwrap_or(&e.text);
        Line::from(vec![
            Span::styled(format!("{:<PLACE$} ", crate::excerpt(&place, PLACE - 1)), Style::new().bold()),
            Span::styled(format!("{:<SENDER$} ", crate::excerpt(&from, SENDER)), Style::new().fg(Color::Blue)),
            Span::styled(format!("{:>8}  ", ago(e.stored_at, now)), frame::dim()),
            Span::raw(crate::excerpt(line.trim(), 200)),
        ])
    }
}

/// The matches of a search, oldest first, and how many earlier ones were left out.
type Found = Result<(Vec<Entry>, usize)>;

/// The search running, at most one: a new one replaces it.
pub struct Searches {
    js: jetstream::Context,
    running: Option<LocalBoxFuture<'static, Found>>,
}

impl Searches {
    pub fn new(js: jetstream::Context) -> Self {
        Self { js, running: None }
    }

    /// Run `search`, dropping the one running.
    pub fn start(&mut self, search: Search) {
        let js = self.js.clone();
        self.running = Some(async move { search.run(&js).await }.boxed_local());
    }

    /// The matches of the search running, once found. Cancel-safe.
    pub async fn next(&mut self) -> Found {
        let found = maybe(&mut self.running).await;
        self.running = None;
        found
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::ThreadId;

    use super::*;
    use crate::live::tests::{at, message, user};

    #[test]
    fn a_query_takes_filter_words_and_the_rest_as_text() {
        let now = DateTime::parse_from_rfc3339("2026-11-15T12:00:00Z").unwrap().to_utc();
        let utc = |text: &str| Some(DateTime::parse_from_rfc3339(text).unwrap().to_utc());
        let s = parse("deploy from:@dev-a to:alice channel:#ops type:post,mention failed since:2h", now).unwrap();
        assert_eq!((s.from, s.to), (Some(user("dev-a")), Some(user("alice"))));
        assert_eq!(s.channel, Some("ops".parse().unwrap()));
        assert_eq!(s.kinds, [Kind::Post, Kind::Mention].into());
        assert_eq!(s.grep.as_deref(), Some("deploy failed"));
        assert_eq!((s.since, s.until, s.limit), (utc("2026-11-15T10:00:00Z"), None, LIMIT));

        let s = parse("until:2026-11-14 http://x.example/a:b TYPE:post", now).unwrap();
        assert_eq!((s.since, s.until), (utc("2026-11-13T00:00:00Z"), utc("2026-11-14T00:00:00Z")));
        assert_eq!(s.grep.as_deref(), Some("http://x.example/a:b TYPE:post"), "only the filter words are filters");
        let s = parse("", now).unwrap();
        assert_eq!((s.grep, s.since), (None, utc("2026-11-14T12:00:00Z")), "the last 24h, as `ants thread search`");

        assert!(parse("type:tool", now).unwrap_err().starts_with("type:tool is none of message, escalation"));
        assert!(parse("from:Bad_Name", now).unwrap_err().starts_with("from:Bad_Name: "));
        assert!(parse("since:yesterday", now).unwrap_err().starts_with("invalid time \"yesterday\""));
        assert_eq!(
            parse("since:1h until:2h", now).unwrap_err(),
            "since: 2026-11-15 11:00:00 is not before until: 2026-11-15 10:00:00"
        );
    }

    #[test]
    fn the_prompt_edits_and_runs_its_text() {
        let mut prompt = Prompt::default();
        for c in "from:dev-a deploy".chars() {
            assert_eq!(prompt.key(Key::Char(c)), Edit::Typing);
        }
        prompt.key(Key::Ctrl('w'));
        assert_eq!(prompt.text, "from:dev-a ");
        prompt.key(Key::Backspace);
        prompt.key(Key::Char('q'));
        assert_eq!(prompt.key(Key::Enter), Edit::Run("from:dev-aq".to_owned()));
        prompt.key(Key::Ctrl('u'));
        assert_eq!(prompt.text, "");
        assert_eq!(prompt.key(Key::Esc), Edit::Close);
    }

    #[test]
    fn matches_list_newest_first_and_open_their_thread_at_them() {
        let (thread, other) = (ThreadId::generate(), ThreadId::generate());
        let search = parse("deploy", at(10)).unwrap();
        let mut page = Page::new("deploy".to_owned(), &search);
        assert_eq!(page.key(Key::Enter), None, "nothing to open while searching");
        let first = message(1, &thread, "alice", "dev-a", "Please\ndeploy the API");
        let second = message(2, &other, "dev-a", "alice", "deployed");
        page.found(Ok((vec![first.clone(), second.clone()], 0)));
        let data = &super::super::model::tests::model().data;
        let row = page.row(&page.matches()[1], data, at(70)).to_string();
        assert!(row.starts_with(&format!("{:<24} alice", thread.to_string()[..23].to_owned() + "…")), "{row}");
        assert!(row.ends_with("1m ago  deploy the API"), "{row}");
        assert_eq!(page.key(Key::Enter), Some(To::Message(other, second.id.unwrap())));
        page.key(Key::End);
        assert_eq!(page.key(Key::Enter), Some(To::Message(thread, first.id.unwrap())));
    }
}
