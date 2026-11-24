//! What folds away to give the view the width: the nav under [`NAV_FROM`] columns, shown then only while it has the
//! focus, over the view; the thread view's side panel under [`PANEL_FROM`], its state, ants, turns and spend then one
//! line under the title. Ctrl-O folds or shows the nav and Ctrl-\ the panel at any width, again to follow the width,
//! the choice kept on the device.

use std::cell::Cell;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::model::{Action, Key, Level, Model, Page};

/// From how many columns the nav shows beside the view.
const NAV_FROM: u16 = 100;
/// From how many columns the thread view's side panel shows.
const PANEL_FROM: u16 = 140;
/// The key that folds or shows the nav.
pub const NAV_KEY: Key = Key::Ctrl('o');
/// The key that folds or shows the thread view's side panel.
pub const PANEL_KEY: Key = Key::Ctrl('\\');

/// Whether the nav and the side panel show, chosen by key; None follows the width.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Folds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nav: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub panel: Option<bool>,
}

/// What folds, and the width of the screen drawn last, which the keys fold or show at.
#[derive(Debug, Default)]
pub struct Fold {
    pub folds: Folds,
    width: Cell<u16>,
}

impl Fold {
    /// Note the width of the screen drawn.
    pub fn drawn(&self, width: u16) {
        self.width.set(width);
    }

    /// Whether the nav shows beside the view on a screen `width` columns wide.
    pub fn nav(&self, width: u16) -> bool {
        self.folds.nav.unwrap_or(width >= NAV_FROM)
    }

    /// Whether the side panel shows beside a thread on a screen `width` columns wide.
    pub fn panel(&self, width: u16) -> bool {
        self.folds.panel.unwrap_or(width >= PANEL_FROM)
    }
}

/// Show what is folded, or fold what shows, at the width drawn last: the choice, None where it is what the width does.
fn flip(chosen: Option<bool>, width: u16, from: u16) -> Option<bool> {
    let shown = !chosen.unwrap_or(width >= from);
    (shown != (width >= from)).then_some(shown)
}

impl Model {
    /// Take `key` if it folds or shows the nav, or the side panel on a thread: the choice to keep.
    pub(in crate::tui) fn fold_key(&mut self, key: Key, now: DateTime<Utc>) -> Option<Action> {
        let width = self.ui.fold.width.get();
        let what = match key {
            NAV_KEY => {
                self.ui.fold.folds.nav = flip(self.ui.fold.folds.nav, width, NAV_FROM);
                if self.ui.fold.nav(width) { "the nav shows" } else { "the nav folds: Tab shows it" }
            }
            PANEL_KEY if matches!(self.page(), Page::Thread(_)) => {
                self.ui.fold.folds.panel = flip(self.ui.fold.folds.panel, width, PANEL_FROM);
                if self.ui.fold.panel(width) { "the side panel shows" } else { "the side panel folds" }
            }
            _ => return None,
        };
        let follows =
            if key == NAV_KEY { self.ui.fold.folds.nav.is_none() } else { self.ui.fold.folds.panel.is_none() };
        let what = if follows { format!("{what}, as the width has it") } else { what.to_owned() };
        self.notify(what, Level::Info, now);
        Some(Action::KeepFolds(self.ui.fold.folds.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_flips_what_shows_and_again_follows_the_width() {
        assert_eq!(flip(None, 80, NAV_FROM), Some(true));
        assert_eq!(flip(Some(true), 80, NAV_FROM), None);
        assert_eq!(flip(None, 120, NAV_FROM), Some(false));
        assert_eq!(flip(Some(false), 120, NAV_FROM), None);
        assert_eq!(flip(Some(false), 80, NAV_FROM), Some(true), "a choice made wider");
    }
}
