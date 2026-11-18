//! The terminal a live view draws on full screen, and the log lines held meanwhile.

use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context as _, Result};
use ratatui::DefaultTerminal;
use ratatui::crossterm::event::{
    DisableBracketedPaste, EnableBracketedPaste, KeyboardEnhancementFlags, PopKeyboardEnhancementFlags,
    PushKeyboardEnhancementFlags,
};
use ratatui::crossterm::execute;

use crate::messages::progress;

/// Whether the screen asked the terminal for the kitty keyboard protocol ([`Screen::open_telling_keys_apart`]).
static KEYBOARD: AtomicBool = AtomicBool::new(false);

/// The terminal, drawn on full screen while this lives; log lines are kept meanwhile and written once it is restored.
pub struct Screen {
    pub terminal: DefaultTerminal,
}

impl Screen {
    pub fn open() -> Result<Self> {
        progress::hold_logs();
        match ratatui::try_init() {
            Ok(terminal) => {
                bracket_pastes(true);
                Ok(Self { terminal })
            }
            Err(e) => {
                ratatui::restore();
                release_logs();
                Err(e).context("setting up the terminal")
            }
        }
    }

    /// [`Screen::open`], the terminal asked to tell keys with modifiers from those without, such as Shift-Enter and
    /// Alt-Enter from Enter, by the kitty keyboard protocol where it answers the query for it, until the screen is
    /// dropped or the process panics.
    pub fn open_telling_keys_apart() -> Result<Self> {
        let supported = crate::tty::kitty_keyboard();
        let screen = Self::open()?;
        if supported && !KEYBOARD.swap(true, Ordering::Relaxed) {
            let hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                stop_telling_keys_apart();
                hook(info);
            }));
            keyboard(true);
        }
        Ok(screen)
    }
}

impl Drop for Screen {
    fn drop(&mut self) {
        stop_telling_keys_apart();
        bracket_pastes(false);
        ratatui::restore();
        release_logs();
    }
}

/// Log lines held while this lives, from before a [`Screen`] opens, and written once it is dropped (or the screen
/// restored).
pub struct HeldLogs(());

impl HeldLogs {
    pub fn hold() -> Self {
        progress::hold_logs();
        Self(())
    }
}

impl Drop for HeldLogs {
    fn drop(&mut self) {
        release_logs();
    }
}

/// Have the terminal mark what is pasted, so that a paste comes as one, rather than as keys with Enter at each line
/// break; a terminal that cannot leaves pastes as keys. Not on Windows, whose console input crossterm reads as key
/// records with no pastes among them, so that the markers would come as keys.
pub fn bracket_pastes(on: bool) {
    if cfg!(windows) {
        return;
    }
    let set = if on {
        execute!(std::io::stdout(), EnableBracketedPaste)
    } else {
        execute!(std::io::stdout(), DisableBracketedPaste)
    };
    if let Err(e) = set {
        tracing::debug!("bracketed paste: {e}");
    }
}

/// Push the kitty keyboard protocol's disambiguation, or pop it, when the screen asked for it; on the alternate screen,
/// whose flags the terminal keeps apart from the main screen's.
pub fn keyboard(on: bool) {
    if !KEYBOARD.load(Ordering::Relaxed) {
        return;
    }
    let set = if on {
        execute!(std::io::stdout(), PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES))
    } else {
        execute!(std::io::stdout(), PopKeyboardEnhancementFlags)
    };
    if let Err(e) = set {
        tracing::debug!("kitty keyboard protocol: {e}");
    }
}

/// Pop the kitty keyboard protocol's disambiguation for good, once.
fn stop_telling_keys_apart() {
    keyboard(false);
    KEYBOARD.store(false, Ordering::Relaxed);
}

fn release_logs() {
    for line in progress::release_logs() {
        eprintln!("{line}");
    }
}

/// `text` without its terminal escape sequences, such as a log line's colours.
fn plain(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // A CSI sequence ends with a letter.
            if chars.next() == Some('[') {
                chars.by_ref().find(char::is_ascii_alphabetic);
            }
        } else if !c.is_control() {
            out.push(c);
        }
    }
    out
}

/// A log line worth showing on the screen, a warning or an error, without its colours.
pub fn warning(line: &str) -> Option<String> {
    let line = plain(line);
    line.split_whitespace().take(2).any(|word| word == "WARN" || word == "ERROR").then_some(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warnings_logged_show_without_their_colours() {
        let warn = "2026-11-14T22:08:00Z \x1b[33m WARN\x1b[0m ants: lost\r";
        assert_eq!(warning(warn).as_deref(), Some("2026-11-14T22:08:00Z  WARN ants: lost"));
        assert_eq!(warning("2026-11-14T22:08:00Z \x1b[32m INFO\x1b[0m async_nats: event: connected"), None);
    }
}
