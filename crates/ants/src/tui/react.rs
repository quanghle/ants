//! Reactions in `ants chat`: under each message, grouped by name with a count, those of the human set apart, drawn as
//! emoji for the names of [`GLYPHS`] and as `:name:` otherwise; and the picker `+` opens on a message, of the
//! [`COMMON`] names and any valid one typed, which reacts as `ants react` does, or takes the reaction back when the
//! human already reacted so.

use ants_daemon::reactions::Reacted;
use ants_proto::{Reaction, UserId, Uuid};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};

use super::frame;
use super::model::Key;

/// The names the picker offers, in order.
pub const COMMON: [&str; 10] =
    ["thumbsup", "heart", "eyes", "check", "x", "rocket", "tada", "thinking", "fire", "pray"];

/// The emoji of the common names; any other name shows as `:name:`.
const GLYPHS: &[(&str, &str)] = &[
    ("thumbsup", "👍"),
    ("thumbsdown", "👎"),
    ("heart", "♥"),
    ("eyes", "👀"),
    ("check", "✅"),
    ("white-check-mark", "✅"),
    ("x", "❌"),
    ("rocket", "🚀"),
    ("tada", "🎉"),
    ("thinking", "🤔"),
    ("fire", "🔥"),
    ("pray", "🙏"),
    ("clap", "👏"),
    ("smile", "😄"),
    ("laughing", "😆"),
    ("ok", "👌"),
    ("wave", "👋"),
    ("bug", "🐛"),
    ("sparkles", "✨"),
    ("hourglass", "⏳"),
    ("question", "❓"),
    ("100", "💯"),
];

/// How `reaction` is drawn: its emoji, else `:reaction:`.
pub fn glyph(reaction: &str) -> String {
    GLYPHS.iter().find(|(name, _)| *name == reaction).map_or_else(|| format!(":{reaction}:"), |(_, g)| (*g).to_owned())
}

/// The line under a message reacted to: each reaction with how many reacted so, those `me` reacted with highlighted.
pub fn line(reacted: &[Reacted], me: &UserId) -> Option<Line<'static>> {
    let mut spans = Vec::new();
    for r in reacted.iter().filter(|r| !r.by.is_empty()) {
        if !spans.is_empty() {
            spans.push(Span::raw("  "));
        }
        let style = if r.by.contains(me) { Style::new().fg(Color::Cyan).bold() } else { frame::dim() };
        spans.push(Span::styled(format!("{} {}", glyph(r.reaction.as_str()), r.by.len()), style));
    }
    (!spans.is_empty()).then(|| Line::from(spans))
}

/// Whether `me` reacted `reaction` to the message reacted to as `reacted`.
pub fn reacted(reacted: &[Reacted], me: &UserId, reaction: &Reaction) -> bool {
    reacted.iter().any(|r| r.reaction == *reaction && r.by.contains(me))
}

/// What the human did, as `ants react` says it.
pub fn done_line(message: Uuid, reaction: &Reaction, remove: bool, changed: bool) -> String {
    match (remove, changed) {
        (true, true) => format!("removed {reaction} from message {message}"),
        (true, false) => format!("no {reaction} reaction to message {message} to remove"),
        (false, true) => format!("reacted {reaction} to message {message}"),
        (false, false) => format!("already reacted {reaction} to message {message}"),
    }
}

/// The picker of a reaction to a message, in the footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Picker {
    pub message: Uuid,
    /// What is typed: the start of a common name, or a name of its own.
    typed: String,
    /// The choice selected, of [`Picker::choices`].
    selected: usize,
    error: Option<String>,
}

/// What a key does to the picker.
#[derive(Debug, PartialEq, Eq)]
pub enum Pick {
    Typing,
    Close,
    /// React to the message so, or take the reaction back.
    React(Uuid, Reaction),
}

impl Picker {
    pub fn new(message: Uuid) -> Self {
        Self { message, typed: String::new(), selected: 0, error: None }
    }

    /// The names to choose from: the common ones starting with what is typed, then what is typed when it is no
    /// common name.
    fn choices(&self) -> Vec<String> {
        let mut choices: Vec<String> =
            COMMON.iter().filter(|c| c.starts_with(&self.typed)).map(|c| (*c).to_owned()).collect();
        if !self.typed.is_empty() && !COMMON.contains(&self.typed.as_str()) {
            choices.push(self.typed.clone());
        }
        choices
    }

    pub fn key(&mut self, key: Key) -> Pick {
        self.error = None;
        let count = self.choices().len();
        match key {
            Key::Esc => return Pick::Close,
            Key::Enter => {
                let Some(choice) = self.choices().into_iter().nth(self.selected) else {
                    self.error = Some("type a name, or Esc to close".to_owned());
                    return Pick::Typing;
                };
                return match choice.parse() {
                    Ok(reaction) => Pick::React(self.message, reaction),
                    Err(_) => {
                        self.error = Some("a reaction is 1 to 64 of a-z, 0-9 and -, not starting with -".to_owned());
                        Pick::Typing
                    }
                };
            }
            Key::Down | Key::Tab => self.selected = (self.selected + 1) % count.max(1),
            Key::Up => self.selected = (self.selected + count.max(1) - 1) % count.max(1),
            Key::Char(c) => {
                self.typed.push(c);
                self.selected = 0;
            }
            Key::Backspace => {
                self.typed.pop();
                self.selected = 0;
            }
            Key::Ctrl('u') => {
                self.typed.clear();
                self.selected = 0;
            }
            _ => {}
        }
        Pick::Typing
    }

    /// The footer while it is open: what is typed, the choices with the selected one marked, and the keys or why a
    /// choice was refused.
    pub fn line(&self) -> Line<'static> {
        let mut spans = vec![
            Span::styled(" react ", Style::new().fg(Color::Cyan).bold()),
            Span::raw(self.typed.clone()),
            Span::raw("▏ "),
        ];
        for (at, choice) in self.choices().into_iter().enumerate() {
            let text = format!(" {} {choice} ", glyph(&choice));
            let style = if at == self.selected { Style::new().fg(Color::Black).bg(Color::Cyan) } else { Style::new() };
            spans.push(Span::styled(text, style));
        }
        match &self.error {
            Some(error) => spans.push(Span::styled(format!("  {error}"), Style::new().fg(Color::Red))),
            None => spans.push(Span::styled("  ↑↓ choose · Enter react or take back · Esc close", frame::dim())),
        }
        Line::from(spans)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::user;

    fn reacted(reaction: &str, by: &[&str]) -> Reacted {
        Reacted::new(reaction.parse().unwrap(), by.iter().map(|b| user(b)).collect())
    }

    #[test]
    fn reactions_show_as_emoji_or_names_with_counts_the_humans_own_set_apart() {
        let line = line(&[reacted("eyes", &["dev-a", "alice"]), reacted("shipit", &["bob"])], &user("alice")).unwrap();
        assert_eq!(line.to_string(), "👀 2  :shipit: 1");
        assert_eq!(line.spans[0].style, Style::new().fg(Color::Cyan).bold(), "alice reacted so");
        assert_eq!(line.spans[2].style, frame::dim());
        assert_eq!(super::line(&[], &user("alice")), None);
    }

    #[test]
    fn the_picker_narrows_the_common_names_and_takes_a_valid_name_of_its_own() {
        let message = Uuid::now_v7();
        let mut picker = Picker::new(message);
        assert_eq!(picker.key(Key::Up), Pick::Typing);
        assert_eq!(picker.key(Key::Enter), Pick::React(message, "pray".parse().unwrap()), "Up wraps to the last");

        let mut picker = Picker::new(message);
        picker.key(Key::Char('t'));
        assert_eq!(picker.choices(), ["thumbsup", "tada", "thinking", "t"]);
        picker.key(Key::Tab);
        assert!(picker.line().to_string().contains(" 🎉 tada "), "{}", picker.line());
        assert_eq!(picker.key(Key::Enter), Pick::React(message, "tada".parse().unwrap()));

        for c in "hip".chars() {
            picker.key(Key::Char(c));
        }
        assert_eq!(picker.choices(), ["thip"]);
        assert_eq!(picker.key(Key::Enter), Pick::React(message, "thip".parse().unwrap()));
        picker.key(Key::Char('!'));
        assert_eq!(picker.key(Key::Enter), Pick::Typing);
        assert!(picker.line().to_string().contains("a reaction is 1 to 64 of a-z"), "{}", picker.line());
        assert_eq!(picker.key(Key::Esc), Pick::Close);
    }
}
