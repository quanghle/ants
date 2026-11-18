//! Questions put to the terminal with escape sequences ([`ask`]), each answered within 200ms or taken as unanswered,
//! and the keys typed while waiting for the answers, kept for the UI ([`typed_keys`]).

use std::sync::Mutex;
use std::time::Duration;

use ratatui::crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// How long the terminal has to answer.
#[cfg_attr(not(unix), allow(dead_code, reason = "the terminal is asked on unix alone"))]
const WAIT: Duration = Duration::from_millis(200);

/// The primary device attributes query, which about every terminal answers, after the questions: its answer coming
/// last tells that the terminal has answered all it will.
#[cfg_attr(not(unix), allow(dead_code, reason = "the terminal is asked on unix alone"))]
const ATTRIBUTES: &[u8] = b"\x1b[c";

/// The bytes typed while the terminal was asked, which were no answer.
static TYPED: Mutex<Vec<u8>> = Mutex::new(Vec::new());

/// The terminal's answers to `question`, when standard input and output are terminals and it answers within 200ms:
/// the OSC strings and the CSI sequences with a `?` it sent, the answer to the attributes query that follows
/// `question` among them. What else is read was typed meanwhile, and is kept for [`typed_keys`].
pub fn ask(question: &[u8]) -> Option<Vec<u8>> {
    let read = unix::exchange(question)?;
    let read = split(&read);
    TYPED.lock().unwrap_or_else(std::sync::PoisonError::into_inner).extend_from_slice(&read.typed);
    (!read.answers.is_empty()).then_some(read.answers)
}

/// The keys typed while the terminal was asked, as crossterm would have read them, taken once.
pub fn typed_keys() -> Vec<KeyEvent> {
    keys(&std::mem::take(&mut *TYPED.lock().unwrap_or_else(std::sync::PoisonError::into_inner)))
}

/// What was read from the terminal while asking it.
#[derive(Debug, Default, PartialEq, Eq)]
struct Read {
    /// The terminal's answers, one after the other.
    answers: Vec<u8>,
    /// The rest, typed meanwhile.
    typed: Vec<u8>,
    /// Whether the answer to the attributes query, the last to come, is in.
    done: bool,
}

/// `read` split into the terminal's answers (OSC strings and CSI sequences with a `?`, an unfinished one dropped) and
/// what was typed.
fn split(read: &[u8]) -> Read {
    let mut out = Read::default();
    let mut at = 0;
    while at < read.len() {
        let rest = &read[at..];
        let end = if rest.starts_with(b"\x1b]") {
            let bel = rest.iter().position(|b| *b == 0x07).map(|end| end + 1);
            let st = rest.windows(2).position(|w| w == b"\x1b\\").map(|end| end + 2);
            bel.into_iter().chain(st).min()
        } else if rest.starts_with(b"\x1b[?") {
            rest[3..].iter().position(|b| (0x40..=0x7e).contains(b)).map(|end| end + 4)
        } else {
            out.typed.push(rest[0]);
            at += 1;
            continue;
        };
        let Some(end) = end else { break };
        out.done |= rest[..end].ends_with(b"c") && rest.starts_with(b"\x1b[?");
        out.answers.extend_from_slice(&rest[..end]);
        at += end;
    }
    out
}

/// The keys `bytes` typed on a terminal in raw mode stand for, as crossterm reads them: characters, Enter, Tab,
/// Backspace, Ctrl and a letter, Esc, Alt and a key as Esc before it, the arrows, Home, End, Delete, PgUp and PgDn.
/// Other sequences are dropped.
fn keys(bytes: &[u8]) -> Vec<KeyEvent> {
    let text = String::from_utf8_lossy(bytes);
    let mut chars = text.chars();
    let mut keys = Vec::new();
    while let Some(c) = chars.next() {
        let key = if c == '\x1b' {
            match chars.next() {
                None | Some('\x1b') => Some(KeyEvent::from(KeyCode::Esc)),
                Some('[' | 'O') => {
                    let mut params = String::new();
                    let end = chars.by_ref().find(|c| {
                        params.push(*c);
                        ('@'..='~').contains(c)
                    });
                    let params = params.trim_end_matches(|c: char| ('@'..='~').contains(&c));
                    let code = match (end, params.split(';').next().unwrap_or_default()) {
                        (Some('A'), _) => Some(KeyCode::Up),
                        (Some('B'), _) => Some(KeyCode::Down),
                        (Some('C'), _) => Some(KeyCode::Right),
                        (Some('D'), _) => Some(KeyCode::Left),
                        (Some('H'), _) | (Some('~'), "1" | "7") => Some(KeyCode::Home),
                        (Some('F'), _) | (Some('~'), "4" | "8") => Some(KeyCode::End),
                        (Some('~'), "3") => Some(KeyCode::Delete),
                        (Some('~'), "5") => Some(KeyCode::PageUp),
                        (Some('~'), "6") => Some(KeyCode::PageDown),
                        _ => None,
                    };
                    code.map(KeyEvent::from)
                }
                Some(c) => plain(c).map(|key| KeyEvent::new(key.code, key.modifiers | KeyModifiers::ALT)),
            }
        } else {
            plain(c)
        };
        keys.extend(key);
    }
    keys
}

/// The key the character `c` typed alone stands for.
fn plain(c: char) -> Option<KeyEvent> {
    Some(match c {
        '\r' => KeyCode::Enter.into(),
        '\t' => KeyCode::Tab.into(),
        '\x7f' => KeyCode::Backspace.into(),
        '\x01'..='\x1a' => {
            let letter = char::from(b'a' + (u8::try_from(c).ok()? - 1));
            KeyEvent::new(KeyCode::Char(letter), KeyModifiers::CONTROL)
        }
        c if c.is_control() => return None,
        c => KeyCode::Char(c).into(),
    })
}

#[cfg(unix)]
mod unix {
    use std::io::IsTerminal as _;
    use std::os::fd::RawFd;
    use std::time::Instant;

    use super::{ATTRIBUTES, WAIT};

    /// Write `question` and the attributes query to standard output and read standard input in raw mode until the
    /// attributes' answer is in or 200ms have passed; what was read.
    pub(super) fn exchange(question: &[u8]) -> Option<Vec<u8>> {
        if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
            return None;
        }
        let fd = libc::STDIN_FILENO;
        // SAFETY: termios is plain data, filled by tcgetattr before use.
        let mut saved: libc::termios = unsafe { std::mem::zeroed() };
        // SAFETY: fd is the terminal and saved a valid termios.
        if unsafe { libc::tcgetattr(fd, &raw mut saved) } != 0 {
            return None;
        }
        let mut raw = saved;
        // SAFETY: raw is a valid termios, copied from the terminal's.
        unsafe { libc::cfmakeraw(&raw mut raw) };
        // SAFETY: as above.
        if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &raw const raw) } != 0 {
            return None;
        }
        let read = read_answers(fd, &mut std::io::stdout().lock(), question);
        // SAFETY: restores the settings read above.
        unsafe { libc::tcsetattr(fd, libc::TCSANOW, &raw const saved) };
        Some(read)
    }

    /// Write `question` and the attributes query to `out`, then read `fd` until the attributes' answer is in or 200ms
    /// have passed: only once `fd` has input, giving up on an error, a hang-up or a descriptor poll cannot wait on
    /// (as /dev/tty on macOS).
    pub(super) fn read_answers(fd: RawFd, out: &mut impl std::io::Write, question: &[u8]) -> Vec<u8> {
        let mut read = Vec::new();
        if out.write_all(question).and_then(|()| out.write_all(ATTRIBUTES)).and_then(|()| out.flush()).is_err() {
            return read;
        }
        let deadline = Instant::now() + WAIT;
        let mut buf = [0u8; 256];
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let ms = libc::c_int::try_from(left.as_millis()).unwrap_or(0);
            if ms == 0 {
                return read;
            }
            let mut poll = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
            // SAFETY: poll is one valid pollfd.
            let ready = unsafe { libc::poll(&raw mut poll, 1, ms) };
            if ready < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            if ready <= 0 || poll.revents & libc::POLLIN == 0 {
                return read;
            }
            // SAFETY: buf is valid for writes of its length.
            let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
            let Ok(n @ 1..) = usize::try_from(n) else { return read };
            read.extend_from_slice(&buf[..n]);
            if super::split(&read).done {
                return read;
            }
        }
    }
}

#[cfg(not(unix))]
mod unix {
    pub(super) fn exchange(_: &[u8]) -> Option<Vec<u8>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn answers_are_told_from_what_was_typed() {
        let read = split(b"hi\x1b]11;rgb:ffff/ffff/ffff\x07\r\x1b[?1u\x1b[A\x1b[?62;22c");
        assert_eq!(read.answers, b"\x1b]11;rgb:ffff/ffff/ffff\x07\x1b[?1u\x1b[?62;22c");
        assert_eq!(read.typed, b"hi\r\x1b[A");
        assert!(read.done);
        let unfinished = split(b"q\x1b]11;rgb:ff");
        assert_eq!((unfinished.answers, unfinished.typed, unfinished.done), (vec![], b"q".to_vec(), false));
    }

    #[test]
    fn what_was_typed_comes_as_keys() {
        let key = |code, modifiers| KeyEvent::new(code, modifiers);
        let none = KeyModifiers::NONE;
        assert_eq!(
            keys("aé\r\x7f\t\x0b\n\x1b\x1b\x1bb\x1b[A\x1b[1;5C\x1bOH\x1b[3~\x1b[5~\x1b[200~".as_bytes()),
            [
                key(KeyCode::Char('a'), none),
                key(KeyCode::Char('é'), none),
                key(KeyCode::Enter, none),
                key(KeyCode::Backspace, none),
                key(KeyCode::Tab, none),
                key(KeyCode::Char('k'), KeyModifiers::CONTROL),
                key(KeyCode::Char('j'), KeyModifiers::CONTROL),
                key(KeyCode::Esc, none),
                key(KeyCode::Char('b'), KeyModifiers::ALT),
                key(KeyCode::Up, none),
                key(KeyCode::Right, none),
                key(KeyCode::Home, none),
                key(KeyCode::Delete, none),
                key(KeyCode::PageUp, none),
            ]
        );
        assert_eq!(keys(b"\x1b"), [key(KeyCode::Esc, none)]);
    }

    #[cfg(unix)]
    #[test]
    fn a_terminal_that_answers_nothing_is_given_up_on_and_what_is_typed_kept() {
        use std::io::Write as _;
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        use std::time::Instant;

        let (mut master, mut slave) = (0, 0);
        // SAFETY: openpty fills the two descriptors; no name, termios or size is asked for.
        let opened = unsafe {
            libc::openpty(
                &raw mut master,
                &raw mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(opened, 0);
        // SAFETY: openpty opened both, and nothing else owns them.
        let (mut master, mut slave) =
            unsafe { (std::fs::File::from_raw_fd(master), std::fs::File::from_raw_fd(slave)) };
        // SAFETY: termios is plain data, filled by tcgetattr.
        let mut raw: libc::termios = unsafe { std::mem::zeroed() };
        // SAFETY: slave is a terminal and raw a valid termios.
        unsafe {
            libc::tcgetattr(slave.as_raw_fd(), &raw mut raw);
            libc::cfmakeraw(&raw mut raw);
            libc::tcsetattr(slave.as_raw_fd(), libc::TCSANOW, &raw const raw);
        }
        master.write_all(b"j").unwrap();
        let started = Instant::now();
        let fd = slave.as_raw_fd();
        let read = unix::read_answers(fd, &mut slave, b"\x1b]11;?\x07");
        let waited = started.elapsed();
        assert_eq!(read, b"j", "the key typed is read, not waited on");
        assert!(waited >= WAIT - Duration::from_millis(20) && waited < Duration::from_secs(1), "{waited:?}");

        master.write_all(b"\x1b]11;rgb:0/0/0\x07\x1b[?62c").unwrap();
        let started = Instant::now();
        let read = unix::read_answers(fd, &mut slave, b"\x1b]11;?\x07");
        assert!(started.elapsed() < WAIT, "the attributes' answer ends the wait");
        assert_eq!(split(&read).answers, b"\x1b]11;rgb:0/0/0\x07\x1b[?62c");
    }
}
