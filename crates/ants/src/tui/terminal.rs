//! Terminal modes and announcements used only by chat, not by the other live views.

use std::collections::HashSet;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};

use ants_daemon::config::Notify;
use ratatui::crossterm::event::{DisableFocusChange, DisableMouseCapture, EnableFocusChange, EnableMouseCapture};
use ratatui::crossterm::{execute, terminal};

use super::home::Item;

static ACTIVE: AtomicBool = AtomicBool::new(false);
static MOUSE: AtomicBool = AtomicBool::new(false);

pub struct Session;

impl Session {
    pub fn open(mouse: bool) -> Self {
        MOUSE.store(mouse, Ordering::Relaxed);
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            pause();
            hook(info);
        }));
        resume();
        Self
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        pause();
    }
}

fn modes(out: &mut impl Write, on: bool, mouse: bool) -> io::Result<()> {
    if on {
        // Xterm's title stack restores the title without needing to read it first.
        out.write_all(b"\x1b[22;0t")?;
        execute!(out, EnableFocusChange)?;
        if mouse {
            execute!(out, EnableMouseCapture)?;
        }
    } else {
        if mouse {
            execute!(out, DisableMouseCapture)?;
        }
        execute!(out, DisableFocusChange)?;
        out.write_all(b"\x1b[23;0t")?;
    }
    out.flush()
}

pub fn pause() {
    if ACTIVE.swap(false, Ordering::Relaxed)
        && let Err(e) = modes(&mut io::stdout(), false, MOUSE.load(Ordering::Relaxed))
    {
        tracing::debug!("restoring chat terminal modes: {e}");
    }
}

pub fn resume() {
    if !ACTIVE.swap(true, Ordering::Relaxed)
        && let Err(e) = modes(&mut io::stdout(), true, MOUSE.load(Ordering::Relaxed))
    {
        tracing::debug!("setting chat terminal modes: {e}");
    }
}

pub fn mouse() -> bool {
    MOUSE.load(Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Protocol {
    Osc9,
    Osc99,
    Osc777,
}

impl Protocol {
    fn detect(term: &str) -> Self {
        if term.contains("kitty") {
            Self::Osc99
        } else if term.contains("rxvt") {
            Self::Osc777
        } else {
            Self::Osc9
        }
    }

    fn notification(self, count: usize) -> String {
        let body = if count == 1 { "1 item waits on home".to_owned() } else { format!("{count} items wait on home") };
        match self {
            Self::Osc9 => format!("\x1b]9;ants: {body}\x1b\\"),
            Self::Osc99 => format!("\x1b]99;i=ants:d=0;ants\x1b\\\x1b]99;i=ants:p=body;{body}\x1b\\"),
            Self::Osc777 => format!("\x1b]777;notify;ants;{body}\x1b\\"),
        }
    }
}

/// Track identities, not counts: a new request can arrive while another leaves.
pub struct Attention {
    focused: bool,
    notify: Notify,
    protocol: Protocol,
    seen: HashSet<String>,
    count: Option<usize>,
}

impl Attention {
    pub fn new(notify: Notify) -> Self {
        Self {
            focused: true,
            notify,
            protocol: Protocol::detect(&std::env::var("TERM").unwrap_or_default()),
            seen: HashSet::new(),
            count: None,
        }
    }

    pub fn focus(&mut self, focused: bool) {
        self.focused = focused;
    }

    pub fn resumed(&mut self) {
        self.count = None;
        self.focused = true;
    }

    pub fn update(&mut self, items: &[Item], out: &mut impl Write) -> io::Result<()> {
        let mut added = 0;
        for item in items {
            let ids = if item.requests.is_empty() {
                let id = match &item.question {
                    Some((id, _)) => format!("question:{id}"),
                    None => format!("{:?}:{}:{}", item.why, item.thread, item.at),
                };
                vec![id]
            } else {
                item.requests.iter().map(|r| format!("request:{}:{}", r.ant, r.id)).collect()
            };
            for id in ids {
                added += usize::from(self.seen.insert(id));
            }
        }
        if !self.focused {
            for _ in 0..added {
                match self.notify {
                    Notify::Bell => out.write_all(b"\x07")?,
                    Notify::Terminal => out.write_all(self.protocol.notification(items.len()).as_bytes())?,
                    Notify::Off => {}
                }
            }
        }
        if self.count != Some(items.len()) {
            // ST, not BEL, terminates titles so a title update never sounds an extra bell.
            write!(out, "\x1b]0;ants ({})\x1b\\", items.len())?;
            self.count = Some(items.len());
        }
        out.flush()
    }
}

/// Stop the whole foreground job, resuming here when the shell brings it back.
#[cfg(unix)]
pub fn suspend() -> io::Result<()> {
    // SAFETY: SIGTSTP is delivered to this process, after the terminal has been restored.
    if unsafe { libc::raise(libc::SIGTSTP) } == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

#[cfg(not(unix))]
pub fn suspend() -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "suspending requires Unix job control"))
}

pub fn restore_raw() -> anyhow::Result<()> {
    terminal::enable_raw_mode()?;
    execute!(io::stdout(), terminal::EnterAlternateScreen)?;
    resume();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::{at, request};
    use crate::tui::{home, test_support};

    #[test]
    fn disabled_mouse_never_emits_mouse_modes_and_modes_are_paired() {
        let mut out = Vec::new();
        modes(&mut out, true, false).unwrap();
        modes(&mut out, false, false).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\x1b[?1004h") && text.contains("\x1b[?1004l"));
        assert!(text.contains("\x1b[22;0t") && text.contains("\x1b[23;0t"));
        assert!(!text.contains("1000") && !text.contains("1006"));
        let mut out = Vec::new();
        modes(&mut out, true, true).unwrap();
        modes(&mut out, false, true).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\x1b[?1006h") && text.contains("\x1b[?1006l"));
    }

    #[test]
    fn unfocused_requests_ring_once_even_when_grouped_and_titles_have_no_bel() {
        let mut m = test_support::model();
        let mut attention = Attention::new(Notify::Bell);
        let mut out = Vec::new();
        attention.update(&[], &mut out).unwrap();
        attention.focus(false);
        m.data.live.approvals.push(request("dev-a", "one", vec![]));
        attention.update(&home::waiting(&m.data, at(0)), &mut out).unwrap();
        attention.update(&home::waiting(&m.data, at(0)), &mut out).unwrap();
        assert_eq!(out.iter().filter(|&&b| b == 7).count(), 1);
        assert!(String::from_utf8_lossy(&out).contains("ants (1)"));
        m.data.live.approvals.push(request("dev-a", "two", vec![]));
        attention.update(&home::waiting(&m.data, at(0)), &mut out).unwrap();
        assert_eq!(out.iter().filter(|&&b| b == 7).count(), 2);
    }

    #[test]
    fn focused_and_disabled_notifications_are_silent() {
        for (notify, focused) in [(Notify::Off, false), (Notify::Bell, true)] {
            let mut m = test_support::model();
            let mut attention = Attention::new(notify);
            attention.focus(focused);
            m.data.live.approvals.push(request("dev-a", "one", vec![]));
            let mut out = Vec::new();
            attention.update(&home::waiting(&m.data, at(0)), &mut out).unwrap();
            attention.focus(false);
            attention.update(&home::waiting(&m.data, at(0)), &mut out).unwrap();
            assert!(!out.contains(&7));
        }
    }

    #[test]
    fn terminal_notifications_are_once_per_item_and_cannot_include_tool_escape_sequences() {
        let mut m = test_support::model();
        let mut request = request("dev-a", "one", vec![]);
        request.tool.title = "\x1b]0;injected\x07".to_owned();
        m.data.live.approvals.push(request);
        for protocol in [Protocol::Osc9, Protocol::Osc99, Protocol::Osc777] {
            let mut attention = Attention::new(Notify::Terminal);
            attention.protocol = protocol;
            attention.focus(false);
            let items = home::waiting(&m.data, at(0));
            let mut out = Vec::new();
            attention.update(&items, &mut out).unwrap();
            let first = out.len();
            attention.update(&items, &mut out).unwrap();
            assert_eq!(out.len(), first);
            assert!(!out.contains(&7));
            assert!(!String::from_utf8_lossy(&out).contains("injected"));
            assert!(String::from_utf8_lossy(&out).contains("1 item waits on home"));
        }
    }

    #[test]
    fn suspend_is_global_and_keeps_composition_and_overlays() {
        let mut model = test_support::model();
        model.bar.focused = true;
        model.bar.set_text("unsent");
        model.overlay(crate::tui::model::Overlay::Help(super::super::help::Help::default()));
        assert_eq!(model.key(super::super::model::Key::Ctrl('z'), at(1)), Some(super::super::model::Action::Suspend));
        assert_eq!(model.bar.text(), "unsent");
        assert!(model.help().is_some());
    }
}
