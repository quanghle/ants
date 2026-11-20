//! What each view does for the model: takes its keys, draws itself, tells the footer what it asks, and drops what it
//! asked to confirm.

use std::borrow::Cow;

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::Rect;

use super::{Data, Key, Model, Page, To};

impl Page {
    /// Take `key`: where it leads, if anywhere.
    pub fn key(&mut self, key: Key, data: &Data, now: DateTime<Utc>) -> Option<To> {
        match self {
            Self::Home(page) => page.key(key, data, now),
            Self::Work(page) => page.key(key, data, now),
            Self::Channel(page) => page.key(key, data, now),
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
        let closed = match self {
            Self::Thread(page) => page.find_key(Key::Esc),
            Self::Ant(page) => page.write(Key::Esc).is_some(),
            Self::Channel(page) => page.write(Key::Esc, data).is_some(),
            _ => false,
        };
        closed || self.disarm()
    }
}
