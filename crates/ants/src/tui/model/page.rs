//! What each view does for the model: takes its keys, what is typed or pasted into what it holds and the keys of its
//! focused question card, draws itself and its footer, tells what covers it, and drops what it asked to confirm.

use std::borrow::Cow;

use ants_proto::Uuid;
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;

use super::{Data, Key, Model, Page, To, Ui};
use crate::tui::{keymap, style};

impl Page {
    /// Take `key`: where it leads, if anywhere.
    pub fn key(&mut self, key: Key, data: &Data, ui: &Ui, now: DateTime<Utc>) -> Option<To> {
        match self {
            Self::Home(page) => page.key(key, data, now),
            Self::Work(page) => page.key(key, data, now),
            Self::Channel(page) => page.key(key, data, ui, now),
            Self::Thread(page) => page.key(key, data, now),
            Self::Dm(page) => page.key(key, data, now),
            Self::Search(page) => page.key(key, data, now),
            Self::Tree(page) => page.key(key, data, now),
            Self::Ant(page) => page.key(key, data, now),
            Self::Credentials(page) => page.key(key, data, now),
            Self::Held(page) => page.key(key, data, now),
        }
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, model: &Model, now: DateTime<Utc>, focused: bool) {
        match self {
            Self::Home(page) => page.draw(frame, area, model, now, focused),
            Self::Work(page) => page.draw(frame, area, model, now, focused),
            Self::Channel(page) => page.draw(frame, area, model, now, focused),
            Self::Thread(page) => page.draw(frame, area, model, now, focused),
            Self::Dm(page) => page.draw(frame, area, model, now, focused),
            Self::Search(page) => page.draw(frame, area, model, now, focused),
            Self::Tree(page) => page.draw(frame, area, model, now, focused),
            Self::Ant(page) => page.draw(frame, area, model, now, focused),
            Self::Credentials(page) => page.draw(frame, area, model, now, focused),
            Self::Held(page) => page.draw(frame, area, model, now, focused),
        }
    }

    /// Take `key` if it is typed into what the view holds: the text found in a thread, an ant's setting or sign-in
    /// address being written, or a channel's members editor. Where it leads, if anywhere.
    pub fn typed(&mut self, key: Key, data: &Data) -> Option<Option<To>> {
        match self {
            Self::Thread(page) => page.find_key(key).then_some(None),
            Self::Ant(page) => page.write(key).map(|action| action.map(To::Do)),
            Self::Channel(page) => page.write(key, data),
            _ => None,
        }
    }

    /// Type `line` pasted into the focused question card or into what the view has being written, if any: whether it
    /// took it.
    pub fn paste(&mut self, line: &str, data: &Data, now: DateTime<Utc>) -> bool {
        match self {
            Self::Home(page) => page.card_paste(line, data, now),
            Self::Thread(page) => {
                page.card_paste(line, data, now) || line.chars().all(|c| page.finding() && page.find_key(Key::Char(c)))
            }
            Self::Channel(page) => page.paste(line),
            Self::Ant(page) => line.chars().all(|c| page.write(Key::Char(c)).is_some()),
            _ => false,
        }
    }

    /// Take `key` on the view's focused question card, or on the list covering it, if any: where it leads, if
    /// anywhere.
    pub fn card_key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<Option<To>> {
        match self {
            Self::Home(page) => page.card_key(key, data, now),
            Self::Thread(page) => page.card_key(key, data, now),
            _ => None,
        }
    }

    /// Move to the row `at` of the card of the message `question`, focusing it, as a click on it does: whether the
    /// view shows that card.
    pub fn mouse_question(&mut self, question: Uuid, at: usize, data: &Data, now: DateTime<Utc>) -> bool {
        match self {
            Self::Home(page) => page.mouse_question(question, at, data, now),
            Self::Thread(page) => page.mouse_question(question, at, data, now),
            _ => false,
        }
    }

    /// Whether PgUp, PgDn, Ctrl-Home and Ctrl-End move the view while the bar is written in: a thread's conversation.
    pub fn scrolls_while_writing(&self) -> bool {
        matches!(self, Self::Thread(_))
    }

    /// The view's own footer, at most `width` wide: the text being found in a thread, the keys of a focused question
    /// card, else those of what the view asks to confirm or has being written.
    pub fn footer(&self, data: &Data, now: DateTime<Utc>, width: u16) -> Option<Line<'static>> {
        let card = match self {
            Self::Thread(page) => match page.find_footer() {
                Some(footer) => return Some(style::clip(footer, usize::from(width))),
                None => page.card(data, now),
            },
            Self::Home(page) => page.card(data, now).map(|(cursor, _)| cursor),
            _ => None,
        };
        if let Some(cursor) = card {
            return Some(keymap::prompt(cursor.keys(), width, false));
        }
        self.prompt().map(|keys| keymap::prompt(&keys, width, self.armed()))
    }

    /// The keys of what the view asks to confirm, or of what is being written in it, for the footer.
    pub fn prompt(&self) -> Option<Cow<'static, str>> {
        match self {
            Self::Home(page) => page.prompt().map(Cow::Owned),
            Self::Channel(page) => page.prompt().map(Cow::Borrowed),
            Self::Tree(page) => page.prompt().map(Cow::Borrowed),
            Self::Ant(page) => page.prompt().map(Cow::Borrowed),
            Self::Credentials(page) => page.prompt().map(Cow::Borrowed),
            Self::Held(page) => page.prompt().map(Cow::Borrowed),
            Self::Work(_) | Self::Thread(_) | Self::Dm(_) | Self::Search(_) => None,
        }
    }

    /// Whether a list of the view's own covers it, taking its keys: a thread's waiting cards.
    pub fn covers(&self) -> bool {
        matches!(self, Self::Thread(page) if page.listing())
    }

    /// Show the list covering the view, or close it: whether the view has one.
    pub fn toggle_cover(&mut self) -> bool {
        let Self::Thread(page) = self else { return false };
        page.toggle_waiting();
        true
    }

    /// Whether the view waits for the `y` confirming what one of its keys asked for.
    pub fn armed(&self) -> bool {
        match self {
            Self::Ant(page) => page.armed().is_some(),
            Self::Home(_) | Self::Tree(_) | Self::Credentials(_) | Self::Held(_) => self.prompt().is_some(),
            Self::Work(_) | Self::Channel(_) | Self::Thread(_) | Self::Dm(_) | Self::Search(_) => false,
        }
    }

    /// Drop the confirmation the view asked for, if any: whether there was one.
    pub fn disarm(&mut self) -> bool {
        match self {
            Self::Home(page) => page.disarm(),
            Self::Tree(page) => page.disarm(),
            Self::Ant(page) => page.disarm(),
            Self::Credentials(page) => page.disarm(),
            Self::Held(page) => page.disarm(),
            Self::Work(_) | Self::Channel(_) | Self::Thread(_) | Self::Dm(_) | Self::Search(_) => false,
        }
    }

    /// Close what the view finds or has being written, else drop what it asked to confirm: whether anything was.
    pub fn close(&mut self, data: &Data) -> bool {
        self.typed(Key::Esc, data).is_some() || self.disarm()
    }
}
