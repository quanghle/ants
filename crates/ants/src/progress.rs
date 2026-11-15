//! The status line `ants ask` and `ants post` keep on stderr while they wait for replies on a terminal: where each
//! ant stands on the thread, from its live status updates ([`StatusEvent`]). Other output clears it first, so what
//! is printed stays clean; off a terminal nothing is shown.

use std::io::{IsTerminal as _, Write as _};
use std::sync::Mutex;

use ants_daemon::sync::MutexExt as _;
use ants_proto::{State, StatusEvent, StatusUpdate, UserId, Uuid};
use futures::StreamExt as _;

const CLEAR: &str = "\r\x1b[2K";

/// The status line: its text, and whether it is on the screen now.
static LINE: Mutex<(Option<String>, bool)> = Mutex::new((None, false));

/// Whether a status line suits this run: stderr is a terminal, and the credentials are a human's, which may read
/// every ant's status.
pub fn wanted(human: bool) -> bool {
    human && std::io::stderr().is_terminal()
}

fn show(text: String) {
    let mut line = LINE.locked();
    if text.is_empty() {
        drop(line);
        return clear();
    }
    let mut stderr = std::io::stderr().lock();
    _ = write!(stderr, "{CLEAR}{text}");
    _ = stderr.flush();
    *line = (Some(text), true);
}

/// Take the status line off the screen until the next update, before printing something else.
pub fn clear() {
    let mut line = LINE.locked();
    if line.1 {
        let mut stderr = std::io::stderr().lock();
        _ = write!(stderr, "{CLEAR}");
        _ = stderr.flush();
    }
    *line = (None, false);
}

/// Log lines kept instead of written while `ants dash` holds the terminal ([`hold_logs`]).
static HELD: Mutex<Option<Vec<String>>> = Mutex::new(None);
/// How many of the latest log lines are kept.
const HELD_MAX: usize = 100;

/// Keep log lines instead of writing them to stderr, until [`release_logs`].
pub fn hold_logs() {
    *HELD.locked() = Some(Vec::new());
}

/// Write log lines to stderr again; returns those kept meanwhile.
pub fn release_logs() -> Vec<String> {
    HELD.locked().take().unwrap_or_default()
}

/// The latest log line kept, if any.
pub fn last_held_log() -> Option<String> {
    HELD.locked().as_ref().and_then(|held| held.last().cloned())
}

/// Stderr for log lines: the status line goes off the screen before one and comes back after it.
pub fn log_writer() -> LogWriter {
    LogWriter { hid: false }
}

pub struct LogWriter {
    hid: bool,
}

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Some(held) = HELD.locked().as_mut() {
            held.extend(String::from_utf8_lossy(buf).lines().map(str::to_owned));
            let over = held.len().saturating_sub(HELD_MAX);
            held.drain(..over);
            return Ok(buf.len());
        }
        if !self.hid {
            self.hid = true;
            let mut line = LINE.locked();
            if line.1 {
                _ = write!(std::io::stderr(), "{CLEAR}");
                line.1 = false;
            }
        }
        std::io::stderr().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stderr().flush()
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        let mut line = LINE.locked();
        if let (Some(text), false) = &*line {
            let mut stderr = std::io::stderr().lock();
            _ = write!(stderr, "{CLEAR}{text}");
            _ = stderr.flush();
            line.1 = true;
        }
    }
}

/// Where an ant stands on the thread, as far as its updates tell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stand {
    /// Nothing heard yet.
    Waiting,
    /// No daemon serves the ant now, as the roster tells.
    Unserved,
    /// The message sent waits on the thread behind this many others.
    Queued(usize),
    Working,
    Thinking,
    Writing,
    Tool {
        id: String,
        title: String,
    },
    /// A tool request waits for a decision: its id, when the daemon tells it.
    Approval(Option<Uuid>),
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct View {
    ant: UserId,
    stand: Stand,
    /// The harness is signed out: the thread's messages wait for a sign-in.
    signed_out: bool,
}

/// What the status line shows: each ant heard from on the thread, until it goes idle.
#[derive(Debug, Default)]
pub struct Status {
    /// The messages sent, whose queue positions count.
    mine: Vec<Uuid>,
    ants: Vec<View>,
}

impl Status {
    pub fn new(mine: Vec<Uuid>) -> Self {
        Self { mine, ants: Vec::new() }
    }

    /// The ants at work on the thread now: those heard working, thinking, writing, calling a tool or waiting for a
    /// tool's approval.
    pub fn busy(&self) -> impl Iterator<Item = &UserId> {
        self.ants
            .iter()
            .filter(|v| {
                matches!(
                    v.stand,
                    Stand::Working | Stand::Thinking | Stand::Writing | Stand::Tool { .. } | Stand::Approval(_)
                )
            })
            .map(|v| &v.ant)
    }

    /// Start with `ant` standing at `stand`, before any update.
    pub fn with(mut self, ant: UserId, stand: Stand) -> Self {
        self.ants.push(View { ant, stand, signed_out: false });
        self
    }

    pub fn apply(&mut self, event: &StatusEvent) {
        let at = match self.ants.iter().position(|v| v.ant == event.ant) {
            Some(at) => at,
            None => {
                self.ants.push(View { ant: event.ant.clone(), stand: Stand::Waiting, signed_out: false });
                self.ants.len() - 1
            }
        };
        let view = &mut self.ants[at];
        if let StatusUpdate::Queued { message, ahead } = &event.update {
            if self.mine.contains(message) {
                view.stand = if *ahead == 0 { Stand::Working } else { Stand::Queued(*ahead) };
            }
            return;
        }
        if let StatusUpdate::State { state } = &event.update {
            view.signed_out = *state == State::SignedOut;
        }
        // Until its turn comes, what happens on the thread is about the messages before it.
        if let Stand::Queued(_) = view.stand {
            return;
        }
        let stand = match &event.update {
            StatusUpdate::State { state } => match state {
                State::Working => Stand::Working,
                State::WaitingApproval if matches!(view.stand, Stand::Approval(_)) => return,
                State::WaitingApproval => Stand::Approval(None),
                State::Failed => Stand::Failed,
                State::SignedOut => return,
                State::Idle => {
                    self.ants.remove(at);
                    return;
                }
            },
            StatusUpdate::Approval { request_id } => Stand::Approval(Some(*request_id)),
            StatusUpdate::Message { .. } => Stand::Writing,
            StatusUpdate::Thought { .. } => Stand::Thinking,
            StatusUpdate::Tool { id, title, tool_kind, status, .. } => {
                let done = matches!(status.as_deref(), Some("completed" | "failed"));
                match &view.stand {
                    Stand::Tool { id: current, .. } if current == id && done => Stand::Working,
                    _ if done => return,
                    Stand::Tool { id: current, title: was } if current == id => {
                        Stand::Tool { id: id.clone(), title: title.as_deref().map_or_else(|| was.clone(), one_line) }
                    }
                    _ => Stand::Tool {
                        id: id.clone(),
                        title: title.as_deref().or(tool_kind.as_deref()).map_or_else(|| "a tool".to_owned(), one_line),
                    },
                }
            }
            StatusUpdate::Plan { .. }
            | StatusUpdate::Queued { .. }
            | StatusUpdate::Escalated { .. }
            | StatusUpdate::Decided { .. } => return,
        };
        view.stand = stand;
    }

    /// Where `ant` stands, as the line tells it after the ant's name; None when it is not shown.
    pub fn of(&self, ant: &UserId) -> Option<String> {
        let view = self.ants.iter().find(|v| v.ant == *ant)?;
        let line = describe(view);
        Some(line.strip_prefix(&format!("{ant}: ")).map_or_else(|| line.clone(), str::to_owned))
    }

    /// The line, cut to `width` characters; empty when no ant is to show.
    pub fn line(&self, width: usize) -> String {
        let line = self.ants.iter().map(describe).collect::<Vec<_>>().join(" | ");
        if line.chars().count() <= width {
            return line;
        }
        let mut cut: String = line.chars().take(width.saturating_sub(1)).collect();
        cut.push('…');
        cut
    }
}

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn describe(view: &View) -> String {
    let ant = &view.ant;
    let stand = match &view.stand {
        Stand::Waiting => "waiting".to_owned(),
        Stand::Unserved => "not served: the message waits for a daemon".to_owned(),
        Stand::Queued(ahead) => format!("queued behind {ahead}"),
        Stand::Working => "working".to_owned(),
        Stand::Thinking => "thinking".to_owned(),
        Stand::Writing => "writing".to_owned(),
        Stand::Tool { title, .. } => format!("running {title}"),
        Stand::Approval(Some(id)) => {
            let short = &id.simple().to_string()[..8];
            format!("waiting for approval: ants tools approve {ant}/{short} (or reject)")
        }
        Stand::Approval(None) => "waiting for approval: `ants tools list` shows the request".to_owned(),
        Stand::Failed => "attempt failed, may be retried".to_owned(),
    };
    match (view.signed_out, &view.stand) {
        (true, Stand::Queued(_)) => format!("{ant}: harness signed out, {stand}"),
        (true, _) => format!("{ant}: harness signed out: the messages wait for a sign-in (see `ants roster`)"),
        (false, _) => format!("{ant}: {stand}"),
    }
}

/// Columns of the terminal, as `COLUMNS` tells, else 80.
fn width() -> usize {
    std::env::var("COLUMNS").ok().and_then(|c| c.parse().ok()).filter(|&c| c > 1).unwrap_or(80) - 1
}

/// The status line, shown while it lives; dropping it takes the line off the screen.
pub struct Shown(tokio::task::JoinHandle<()>);

impl Drop for Shown {
    fn drop(&mut self) {
        self.0.abort();
        clear();
    }
}

/// Show `status` and keep it current with the updates `sub` brings, until the returned [`Shown`] drops. With
/// `interrupt`, Ctrl-C takes the line off before ending the process as interrupted.
pub fn start(mut sub: async_nats::Subscriber, mut status: Status, interrupt: bool) -> Shown {
    show(status.line(width()));
    Shown(tokio::spawn(async move {
        let ctrl_c = async {
            if interrupt {
                _ = tokio::signal::ctrl_c().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(ctrl_c);
        loop {
            let msg = tokio::select! {
                () = &mut ctrl_c => {
                    clear();
                    std::process::exit(130);
                }
                msg = sub.next() => match msg {
                    Some(msg) => msg,
                    None => return,
                },
            };
            let Ok(event) = serde_json::from_slice::<StatusEvent>(&msg.payload) else { continue };
            if event.check_subject(&msg.subject).is_ok() {
                status.apply(&event);
                show(status.line(width()));
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(status: &mut Status, updates: impl IntoIterator<Item = StatusUpdate>) -> String {
        let (ant, thread): (UserId, ants_proto::ThreadId) = ("dev-b".parse().unwrap(), "t1".parse().unwrap());
        for update in updates {
            status.apply(&StatusEvent::new(ant.clone(), thread.clone(), update));
        }
        status.line(200)
    }

    fn state(state: State) -> StatusUpdate {
        StatusUpdate::State { state }
    }

    fn tool(title: Option<&str>, status: &str) -> StatusUpdate {
        let title = title.map(str::to_owned);
        StatusUpdate::Tool { id: "c1".to_owned(), title, tool_kind: None, status: Some(status.to_owned()), paths: None }
    }

    #[test]
    fn the_line_follows_the_ant_through_its_prompt() {
        let dev_b: UserId = "dev-b".parse().unwrap();
        let mut status = Status::new(vec![]).with(dev_b.clone(), Stand::Waiting);
        assert_eq!(line(&mut status, []), "dev-b: waiting");
        assert_eq!(line(&mut status, [state(State::Working)]), "dev-b: working");
        assert_eq!(line(&mut status, [tool(Some("cargo\n  test"), "pending")]), "dev-b: running cargo test");
        assert_eq!(line(&mut status, [tool(None, "in_progress")]), "dev-b: running cargo test");
        assert_eq!(line(&mut status, [tool(None, "completed")]), "dev-b: working");
        let id: Uuid = "1a2b3c4d-0000-0000-0000-000000000000".parse().unwrap();
        let asks = [StatusUpdate::Approval { request_id: id }, state(State::WaitingApproval)];
        assert_eq!(
            line(&mut status, asks),
            "dev-b: waiting for approval: ants tools approve dev-b/1a2b3c4d (or reject)"
        );
        assert_eq!(line(&mut status, [StatusUpdate::Message { text: "ok".to_owned() }]), "dev-b: writing");
        assert_eq!(line(&mut status, [state(State::SignedOut)]).split(':').nth(1), Some(" harness signed out"));
        assert_eq!(line(&mut status, [state(State::Working)]), "dev-b: working");
        assert_eq!(line(&mut status, [state(State::Idle)]), "", "an idle ant leaves the line");

        let unserved = Status::new(vec![]).with(dev_b, Stand::Unserved);
        assert_eq!(unserved.line(200), "dev-b: not served: the message waits for a daemon");
        assert_eq!(unserved.line(10), "dev-b: no…");
    }

    #[test]
    fn a_queued_message_ignores_the_prompts_before_it_until_its_turn() {
        let (mine, other) = (Uuid::now_v7(), Uuid::now_v7());
        let queued = |message, ahead| StatusUpdate::Queued { message, ahead };
        let mut status = Status::new(vec![mine]);
        assert_eq!(line(&mut status, [state(State::Working), queued(other, 1)]), "dev-b: working");
        assert_eq!(line(&mut status, [queued(mine, 2)]), "dev-b: queued behind 2");
        let before = [tool(Some("make"), "pending"), state(State::Idle), state(State::Working), queued(mine, 1)];
        assert_eq!(line(&mut status, before), "dev-b: queued behind 1");
        assert_eq!(line(&mut status, [state(State::SignedOut)]), "dev-b: harness signed out, queued behind 1");
        assert_eq!(line(&mut status, [state(State::Working)]), "dev-b: queued behind 1");
        assert_eq!(line(&mut status, [queued(mine, 0)]), "dev-b: working");
        assert_eq!(line(&mut status, [state(State::Failed)]), "dev-b: attempt failed, may be retried");
    }
}
