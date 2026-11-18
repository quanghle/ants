//! Catch-up summaries in `ants chat` ([`crate::threads::summary`]): home's first line, counting what changed since the
//! previous `ants chat` on the device ended (kept in [`STATE_FILE`] beside the config file), whose `s` opens the
//! digest of that span in an overlay, as `/digest [SINCE]` does of another; and the TL;DRs that `S` and `/tldr` ask
//! for, which the thread and work views show. With `[cli]` `summaries = "ahead"`, the TL;DRs of the threads work lists
//! are also asked for ahead of time, one at a time, until the human's summaries spent `summary_daily_tokens` that UTC
//! day.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::config::{Summaries, SummaryConfig};
use ants_proto::{ChannelId, JsDomain, SummaryRequest, ThreadId, ThreadSummary, UserId};
use anyhow::{Result, anyhow};
use async_nats::jetstream;
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};
use serde::{Deserialize, Serialize};

use super::command::Tone;
use super::home::{self, Why};
use super::markdown;
use super::model::{Action, Data, Key, Level, Model};
use super::style::{self, ago};
use super::timeline::Tools;
use super::work;
use crate::cmd::send;
use crate::colony;
use crate::people::humans;
use crate::threads::summary;
use crate::threads::thread_meta::Meta;

/// The file beside the config file that keeps when the last `ants chat` on the device ended, how its thread view
/// shows tool calls, the tab chosen in each channel's view, and whether the nav and the side panel show.
pub const STATE_FILE: &str = "chat-state.json";
/// How far back home's first line counts when no `ants chat` ended on the device before.
const BEFORE: TimeDelta = TimeDelta::hours(24);
/// How long a summary request waits for its reply.
const WAIT: Duration = Duration::from_secs(600);
/// How many lines of a TL;DR the thread view shows.
const TLDR_LINES: usize = 3;

/// What [`STATE_FILE`] keeps.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ChatState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended: Option<DateTime<Utc>>,
    #[serde(default)]
    pub tools: Tools,
    #[serde(default, skip_serializing_if = "HashMap::is_empty", deserialize_with = "super::channel::kept_tabs")]
    pub channel_tabs: HashMap<ChannelId, super::channel::Tab>,
    /// Whether the nav and the thread's side panel show, as last chosen by key.
    #[serde(default)]
    pub folds: super::fold::Folds,
}

/// [`STATE_FILE`] beside the config file.
pub fn state_path() -> Result<PathBuf> {
    Ok(crate::defaults::path()?.with_file_name(STATE_FILE))
}

/// What the file at `path` keeps; the defaults when it keeps nothing readable.
pub fn load(path: &Path) -> ChatState {
    let Ok(text) = std::fs::read(path) else { return ChatState::default() };
    serde_json::from_slice(&text).unwrap_or_else(|e| {
        tracing::debug!("ignoring {}: {e}", path.display());
        ChatState::default()
    })
}

/// Change what the file at `path` keeps by `change`.
pub fn keep(path: &Path, change: impl FnOnce(&mut ChatState)) -> Result<()> {
    let mut state = load(path);
    change(&mut state);
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(&state)?)?;
    Ok(())
}

/// What a summary is asked of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ask {
    /// The TL;DR of a thread, asked ahead of time or on demand.
    Tldr { thread: ThreadId, ahead: bool },
    /// A digest of the threads with entries since then.
    Digest(DateTime<Utc>),
}

/// What a summary request brought.
#[derive(Debug)]
pub enum Got {
    Tldr(ThreadSummary),
    Digest {
        by: UserId,
        text: String,
        tokens: u64,
    },
    /// Nothing to summarise.
    Empty,
    /// The human's summaries spent this many tokens that UTC day, no fewer than `summary_daily_tokens`: no TL;DR is
    /// asked for ahead of time until the next.
    Spent(u64),
}

/// How a summary request went, with the warnings to tell.
#[derive(Debug)]
pub struct Summarised {
    pub ask: Ask,
    pub result: Result<Got>,
    pub warnings: Vec<String>,
}

/// The digest overlay.
#[derive(Debug)]
pub struct Digest {
    pub since: DateTime<Utc>,
    /// The digest once in: the ant that wrote it, its text and tokens; or why there is none.
    pub got: Option<Result<(UserId, String, u64), String>>,
    pub shown: bool,
    pub top: u16,
}

/// What the model keeps of the catch-up summaries.
#[derive(Debug, Default)]
pub struct Catchup {
    /// When the span home's first line counts starts: when the last `ants chat` on the device ended.
    pub since: Option<DateTime<Utc>>,
    /// Whether TL;DRs are asked for ahead of time.
    pub ahead: bool,
    /// The threads whose TL;DR is being asked for.
    pub asking: HashSet<ThreadId>,
    /// The sequence each thread was at when its TL;DR was last asked for ahead of time, so it is not asked again
    /// before a newer entry.
    tried: HashMap<ThreadId, u64>,
    /// The UTC day TL;DRs stopped being asked for ahead of time, the day's tokens spent.
    stopped: Option<NaiveDate>,
    pub digest: Option<Digest>,
}

impl Catchup {
    /// When home's first line counts from, at `now`.
    pub fn from(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        self.since.unwrap_or(now - BEFORE)
    }

    /// Whether the digest overlay shows.
    pub fn shown(&self) -> bool {
        self.digest.as_ref().is_some_and(|d| d.shown)
    }
}

/// How many threads finished, got stuck and wait on the human since `since`, as home lists them at `now`.
pub fn counts(data: &Data, since: DateTime<Utc>, now: DateTime<Utc>) -> (usize, usize, usize) {
    let items = home::items(data, now);
    let threads = |of: &dyn Fn(Why) -> bool| {
        let mut threads: Vec<&ThreadId> =
            items.iter().filter(|i| of(i.why) && i.at >= since).map(|i| &i.thread).collect();
        threads.sort();
        threads.dedup();
        threads.len()
    };
    (
        threads(&|w| w == Why::Finished),
        threads(&|w| w == Why::Stuck),
        threads(&|w| !matches!(w, Why::Finished | Why::Stuck)),
    )
}

/// Home's first line: what changed since the last `ants chat` on the device ended, and the ants' spend that UTC day.
pub fn since_line(model: &Model, now: DateTime<Utc>) -> Line<'static> {
    let data = &model.data;
    let since = model.catchup.from(now);
    let (finished, stuck, waiting) = counts(data, since, now);
    let spent = data.cards.today().map_or_else(|| "$?".to_owned(), |s| work::dollars(s.cost));
    let at = since.with_timezone(&chrono::Local);
    let at = if now - since < TimeDelta::hours(24) { at.format("%H:%M") } else { at.format("%b %-d %H:%M") };
    let digest = match &model.catchup.digest {
        Some(d) if d.got.is_none() => "asking for a digest…",
        _ => "s opens the digest",
    };
    Line::from(vec![
        Span::styled(format!("Since {at} ({}): ", ago(since, now)), Style::new().bold()),
        Span::raw(format!("{finished} finished · {stuck} stuck · {waiting} waiting · {spent} spent today · ")),
        Span::styled(digest, style::dim()),
    ])
}

/// The TL;DR of `thread` as the thread view shows it, in lines of `width`: who wrote it and how long ago, stale once
/// a newer entry is stored, then its first lines; or that one is being asked for. None when there is neither.
pub fn tldr_lines(model: &Model, thread: &ThreadId, now: DateTime<Utc>, width: usize) -> Vec<Line<'static>> {
    let data = &model.data;
    let asking = model.catchup.asking.contains(thread);
    let Some(tldr) = data.cards.tldr.get(thread) else {
        return if asking { vec![Line::styled("TL;DR  asking for one…", style::dim())] } else { Vec::new() };
    };
    let mut about = format!("TL;DR ({}, {}", tldr.by, ago(tldr.at, now));
    if !current(data, thread, tldr) {
        about.push_str(", stale");
    }
    about.push_str(if asking { ", asking for a new one…)" } else { ", S asks for a new one)" });
    let mut lines = vec![Line::styled(about, style::dim())];
    let text = markdown::render(&tldr.text, width.max(10));
    let more = text.len() > TLDR_LINES;
    lines.extend(text.into_iter().take(TLDR_LINES));
    if more && let Some(last) = lines.last_mut() {
        last.spans.push(Span::styled(" …", style::dim()));
    }
    lines
}

/// Draw the TL;DR of `thread` at the top of `area`, in at most a third of it, with a rule under it: the area left.
pub fn draw_tldr(frame: &mut Frame, area: Rect, model: &Model, thread: &ThreadId, now: DateTime<Utc>) -> Rect {
    let lines = tldr_lines(model, thread, now, usize::from(area.width));
    if lines.is_empty() || area.height < 9 {
        return area;
    }
    let height = u16::try_from(lines.len()).unwrap_or(u16::MAX).min(area.height / 3);
    let [top, rule, rest] =
        Layout::vertical([Constraint::Length(height), Constraint::Length(1), Constraint::Min(0)]).areas(area);
    frame.render_widget(Paragraph::new(lines), top);
    frame.render_widget(Paragraph::new("─".repeat(usize::from(rule.width))).style(style::dim()), rule);
    rest
}

/// Whether the TL;DR `tldr` of `thread` covers its last entry.
fn current(data: &Data, thread: &ThreadId, tldr: &ThreadSummary) -> bool {
    tldr.is_current(data.live.summary(thread).map_or(0, |s| s.last_reopening))
}

/// Draw the digest overlay, if shown.
pub fn draw_digest(frame: &mut Frame, model: &Model, now: DateTime<Utc>) {
    let Some(digest) = model.catchup.digest.as_ref().filter(|d| d.shown) else { return };
    let area = frame.area();
    let (width, height) = (area.width.saturating_sub(8).min(100), area.height.saturating_sub(4));
    let area = Rect { x: area.x + (area.width - width) / 2, y: area.y + (area.height - height) / 2, width, height };
    let inner = usize::from(width.saturating_sub(2));
    let since =
        format!("{} ({})", digest.since.with_timezone(&chrono::Local).format("%b %-d %H:%M"), ago(digest.since, now));
    let (title, mut lines) = match &digest.got {
        None => (format!(" Digest since {since} "), vec![Line::styled("asking for a digest…", style::dim())]),
        Some(Err(e)) => {
            (format!(" Digest since {since} "), vec![Line::styled(e.clone(), Style::new().fg(style::ERROR))])
        }
        Some(Ok((by, text, tokens))) => {
            (format!(" Digest since {since} · {by} · {tokens} tokens "), markdown::render(text, inner.max(10)))
        }
    };
    lines.extend([Line::raw(""), Line::styled("j/k scroll · s asks again · any other key closes", style::dim())]);
    frame.render_widget(Clear, area);
    let paragraph = Paragraph::new(lines).block(style::block(title, true)).scroll((digest.top, 0));
    frame.render_widget(paragraph, area);
}

impl Model {
    /// Ask for a new TL;DR of `thread`.
    pub fn tldr(&mut self, thread: ThreadId) -> Option<Action> {
        if !self.catchup.asking.insert(thread.clone()) {
            self.bar.set_status(format!("a TL;DR of thread {thread} is being asked for"), Tone::Busy);
            return None;
        }
        self.bar.set_status(format!("asking for a TL;DR of thread {thread}…"), Tone::Busy);
        Some(Action::Summarise(Ask::Tldr { thread, ahead: false }))
    }

    /// Show the digest of the span home's first line counts: the one kept if it is of that span, else a new one.
    pub fn home_digest(&mut self, now: DateTime<Utc>) -> Option<Action> {
        let since = self.catchup.from(now);
        if let Some(digest) =
            self.catchup.digest.as_mut().filter(|d| d.since == since && !matches!(d.got, Some(Err(_))))
        {
            digest.shown = true;
            return None;
        }
        Some(self.ask_digest(since))
    }

    /// Ask for a digest of the threads with entries within `since`, a duration such as `90m` (0 for everything the
    /// stream keeps), else since the span home's first line counts starts.
    pub fn digest(&mut self, since: Option<&str>, now: DateTime<Utc>) -> Result<Action, String> {
        let since = match since {
            None => self.catchup.from(now),
            Some(text) => match ants_proto::duration::parse(text).map_err(|e| e.to_string())? {
                d if d.is_zero() => DateTime::<Utc>::UNIX_EPOCH,
                d => now - TimeDelta::from_std(d).map_err(|e| format!("invalid duration {text:?}: {e}"))?,
            },
        };
        Ok(self.ask_digest(since))
    }

    fn ask_digest(&mut self, since: DateTime<Utc>) -> Action {
        self.catchup.digest = Some(Digest { since, got: None, shown: true, top: 0 });
        Action::Summarise(Ask::Digest(since))
    }

    /// The keys while the digest shows: j/k scroll, s asks again, any other closes it.
    pub fn digest_key(&mut self, key: Key) -> Option<Action> {
        let digest = self.catchup.digest.as_mut()?;
        match key {
            Key::Down | Key::Char('j') => digest.top = digest.top.saturating_add(1),
            Key::Up | Key::Char('k') => digest.top = digest.top.saturating_sub(1),
            Key::PageDown => digest.top = digest.top.saturating_add(10),
            Key::PageUp => digest.top = digest.top.saturating_sub(10),
            Key::Home => digest.top = 0,
            Key::Char('s') if digest.got.is_some() => {
                let since = digest.since;
                return Some(self.ask_digest(since));
            }
            _ => digest.shown = false,
        }
        None
    }

    /// Take how a summary request went.
    pub fn summarised(&mut self, summarised: Summarised, now: DateTime<Utc>) {
        let Summarised { ask, result, warnings } = summarised;
        let warned = |line: String| {
            if warnings.is_empty() {
                (line, Tone::Done)
            } else {
                (std::iter::once(line).chain(warnings.iter().cloned()).collect::<Vec<_>>().join(" · "), Tone::Warn)
            }
        };
        match ask {
            Ask::Tldr { thread, ahead } => {
                self.catchup.asking.remove(&thread);
                let told = match result {
                    Ok(Got::Tldr(tldr)) => {
                        let line = format!("TL;DR of thread {thread} by {}, {} tokens", tldr.by, tldr.tokens);
                        self.data.cards.tldr.insert(thread, tldr);
                        Some(warned(line))
                    }
                    Ok(Got::Empty) => Some((format!("no messages on thread {thread} to summarise"), Tone::Warn)),
                    Ok(Got::Spent(spent)) => {
                        self.catchup.stopped = Some(now.date_naive());
                        let text = format!("TL;DRs ahead of time stop for the UTC day: summaries spent {spent} tokens");
                        self.notify(text, Level::Info, now);
                        None
                    }
                    Ok(Got::Digest { .. }) => None,
                    Err(e) if ahead => {
                        self.notify(format!("asking ahead for the TL;DR of thread {thread}: {e:#}"), Level::Error, now);
                        None
                    }
                    Err(e) => Some((format!("{e:#}"), Tone::Error)),
                };
                if let Some((text, tone)) = told.filter(|_| !ahead) {
                    self.bar.set_status(text, tone);
                }
            }
            Ask::Digest(since) => {
                let Some(digest) = self.catchup.digest.as_mut().filter(|d| d.since == since && d.got.is_none()) else {
                    return;
                };
                digest.got = Some(match result {
                    Ok(Got::Digest { by, text, tokens }) => Ok((by, text, tokens)),
                    Ok(Got::Empty) => Err(format!("no entries since {}", crate::threads::thread::time(since))),
                    Ok(_) => Err("no digest came".to_owned()),
                    Err(e) => Err(format!("{e:#}")),
                });
                let shown = digest.shown;
                if !warnings.is_empty() {
                    self.notify(warnings.join(" · "), Level::Error, now);
                }
                if !shown {
                    self.notify("the digest is in: s on home shows it", Level::Info, now);
                }
            }
        }
    }

    /// With summaries ahead of time, the next TL;DR to ask for: of the first thread work lists whose TL;DR is missing
    /// or stale and was not asked for at its last entry, while none is being asked for and the day's tokens last.
    pub fn ahead(&mut self, now: DateTime<Utc>) -> Option<Action> {
        let catchup = &self.catchup;
        let data = &self.data;
        if !catchup.ahead
            || !catchup.asking.is_empty()
            || catchup.stopped == Some(now.date_naive())
            || !data.cards.tldr_read
        {
            return None;
        }
        let rows = work::Page::new(work::Filter::default()).rows(data);
        let thread = rows.into_iter().map(|(t, _)| t).find(|t| {
            let last = data.live.summary(t).map_or(0, |s| s.last_reopening);
            last > 0
                && catchup.tried.get(t) != Some(&last)
                && data.cards.tldr.get(t).is_none_or(|tldr| !current(data, t, tldr))
        })?;
        let last = data.live.summary(&thread).map_or(0, |s| s.last_reopening);
        self.catchup.tried.insert(thread.clone(), last);
        self.catchup.asking.insert(thread.clone());
        Some(Action::Summarise(Ask::Tldr { thread, ahead: true }))
    }
}

/// What a summary request needs: the UI's connection, its human, the summary settings and where the TL;DRs are kept.
struct Ctx {
    client: async_nats::Client,
    js: jetstream::Context,
    me: humans::Me,
    domain: Option<JsDomain>,
    config: Result<SummaryConfig, String>,
    meta: Option<Meta>,
}

/// The summary requests running.
pub struct Io {
    ctx: Rc<Ctx>,
    jobs: FuturesUnordered<LocalBoxFuture<'static, Summarised>>,
}

impl Io {
    pub async fn open(nats: &crate::connection::Nats<'_>, client: async_nats::Client, js: jetstream::Context) -> Self {
        let config = crate::defaults::summaries().map_err(|e| format!("{e:#}"));
        let meta = crate::threads::thread_meta::open(nats, &js)
            .await
            .inspect_err(|e| tracing::debug!("not keeping TL;DRs: {e:#}"))
            .ok();
        let ctx = Ctx { client, js, me: nats.me.clone(), domain: nats.domain.cloned(), config, meta };
        Self { ctx: Rc::new(ctx), jobs: FuturesUnordered::new() }
    }

    /// Whether TL;DRs are asked for ahead of time.
    pub fn ahead(&self) -> bool {
        self.ctx.config.as_ref().is_ok_and(|c| c.mode == Summaries::Ahead)
    }

    pub fn ask(&mut self, ask: Ask) {
        let ctx = Rc::clone(&self.ctx);
        self.jobs.push(
            async move {
                let mut warnings = Vec::new();
                let result = ctx.run(&ask, &mut warnings).await;
                Summarised { ask, result, warnings }
            }
            .boxed_local(),
        );
    }

    /// The next request to end; pending while none runs.
    pub async fn next(&mut self) -> Summarised {
        match self.jobs.next().await {
            Some(summarised) => summarised,
            None => std::future::pending().await,
        }
    }
}

impl Ctx {
    async fn run(&self, ask: &Ask, warnings: &mut Vec<String>) -> Result<Got> {
        let config = self.config.as_ref().map_err(|e| anyhow!("{e}"))?;
        match ask {
            Ask::Tldr { thread, ahead } => {
                if *ahead {
                    let spent = summary::spent_today(&self.js, &self.me.id, Utc::now()).await?;
                    if spent >= config.daily_tokens {
                        return Ok(Got::Spent(spent));
                    }
                }
                let Some((text, request)) = summary::tldr_request(&self.js, thread).await? else {
                    return Ok(Got::Empty);
                };
                let reply = self.ask(config, text, request.clone(), warnings).await?;
                let tldr = summary::tldr_of(&request, &reply, &self.me.id)
                    .ok_or_else(|| anyhow!("a TL;DR request with no thread"))?;
                if let Some(meta) = &self.meta
                    && let Err(e) = meta.put_summary(&tldr).await
                {
                    warnings.push(format!("not keeping the TL;DR: {e:#}"));
                }
                Ok(Got::Tldr(tldr))
            }
            Ask::Digest(since) => {
                let Some((text, request)) = summary::digest_request(&self.js, *since).await? else {
                    return Ok(Got::Empty);
                };
                let reply = self.ask(config, text, request, warnings).await?;
                let tokens = reply.summary.as_ref().and_then(|s| s.tokens).unwrap_or_default();
                Ok(Got::Digest { by: reply.from, text: reply.text.trim().to_owned(), tokens })
            }
        }
    }

    /// Ask `config`'s ant for `request` as `ants thread tldr` and `ants digest` do, and wait for its reply, marked
    /// read.
    async fn ask(
        &self,
        config: &SummaryConfig,
        text: String,
        request: SummaryRequest,
        warnings: &mut Vec<String>,
    ) -> Result<ants_proto::Envelope> {
        let envelope = summary::request_envelope(&self.me.id, config.ant.clone(), text, request);
        // Subscribe first so a fast reply is not missed.
        let mut replies = bus::caught_up_inbox(&self.client, &self.js, &self.me.id, self.me.reading()).await?;
        warnings.extend(colony::unserved_warnings(&self.js, &[&envelope.to]).await);
        if send::deliver(&self.js, self.domain.as_ref(), &envelope).await? == bus::Stored::Outbox {
            warnings.push(send::OUTBOX.to_owned());
        }
        let wait = summary::reply(&mut replies, &envelope);
        let reply = tokio::time::timeout(WAIT, wait)
            .await
            .map_err(|_| anyhow!("{} gave no summary within {}m", envelope.to, WAIT.as_secs() / 60))??;
        summary::taken(&self.js, &self.me.id, reply).await
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test_support::live::{at, message, user};
    use crate::tui::model::Page;
    use crate::tui::test_support::model;
    use crate::tui::thread;

    #[test]
    fn the_device_keeps_when_ants_chat_ended_and_how_tool_calls_show_each_without_losing_the_other() {
        let dir = ants_test_support::scratch_dir("chat-state");
        let path = dir.join("sub").join(STATE_FILE);
        assert_eq!((load(&path).ended, load(&path).tools), (None, Tools::Collapsed), "nothing kept yet");
        keep(&path, |kept| kept.tools = Tools::None).unwrap();
        keep(&path, |kept| kept.ended = Some(at(5))).unwrap();
        assert_eq!((load(&path).ended, load(&path).tools), (Some(at(5)), Tools::None));
        std::fs::write(&path, "not json").unwrap();
        assert_eq!(load(&path).tools, Tools::Collapsed, "an unreadable file keeps nothing");
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn shown(m: &Model, now: DateTime<Utc>) -> String {
        crate::test_support::render::rendered(140, 30, |f| crate::tui::frame::draw(f, m, now))
    }

    /// A thread alice gave dev-a, which dev-a answered at `seq` + 1.
    fn finished(m: &mut Model, seq: u64) -> ThreadId {
        let thread = ThreadId::generate();
        let mut opening = message(seq, &thread, "alice", "dev-a", "deploy");
        opening.owner = Some(user("alice"));
        m.stored(vec![opening, message(seq + 1, &thread, "dev-a", "alice", "deployed")], at(seq.cast_signed() + 1));
        thread
    }

    #[test]
    fn home_counts_what_changed_since_the_last_chat_and_s_opens_the_digest_of_that_span() {
        let mut m = model();
        finished(&mut m, 1);
        finished(&mut m, 20);
        m.catchup.since = Some(at(10));
        let line: String = since_line(&m, at(30)).spans.iter().map(|s| s.content.as_ref()).collect();
        assert!(line.contains("1 finished · 0 stuck · 0 waiting · $? spent today · s opens the digest"), "{line}");

        assert_eq!(m.key(Key::Char('s'), at(30)), Some(Action::Summarise(Ask::Digest(at(10)))));
        assert!(shown(&m, at(30)).contains("asking for a digest…"));
        let got = Got::Digest { by: user("messenger"), text: "Two deploys **landed**.".into(), tokens: 42 };
        m.summarised(Summarised { ask: Ask::Digest(at(10)), result: Ok(got), warnings: Vec::new() }, at(31));
        let screen = shown(&m, at(31));
        assert!(screen.contains("messenger · 42 tokens") && screen.contains("Two deploys landed."), "{screen}");
        assert_eq!(m.key(Key::Char('j'), at(31)), None);
        assert!(m.catchup.shown(), "j scrolls");
        assert_eq!(m.key(Key::Esc, at(31)), None);
        assert!(!m.catchup.shown() && matches!(m.page(), Page::Home(_)), "Esc closes the digest only");
        assert_eq!(m.key(Key::Char('s'), at(32)), None, "the digest of the span is kept");
        assert!(m.catchup.shown());
        m.key(Key::Esc, at(32));

        let action = m.digest(Some("2h"), at(7200)).unwrap();
        assert_eq!(action, Action::Summarise(Ask::Digest(at(0))));
        assert!(m.digest(Some("soon"), at(7200)).is_err());
    }

    #[test]
    fn a_tldr_asked_for_shows_in_the_thread_and_turns_stale_with_a_newer_entry() {
        let mut m = model();
        let thread = finished(&mut m, 1);
        m.pages.push(Page::Thread(thread::Page::new(thread.clone())));
        let asked = Action::Summarise(Ask::Tldr { thread: thread.clone(), ahead: false });
        m.key(Key::Up, at(3));
        assert_eq!(m.key(Key::Char('S'), at(3)), Some(asked));
        assert_eq!(m.key(Key::Char('S'), at(3)), None, "asked for once at a time");
        assert!(shown(&m, at(3)).contains("TL;DR  asking for one…"));
        let tldr = ThreadSummary {
            thread_id: thread.clone(),
            text: "Deployed to staging.".into(),
            by: user("messenger"),
            asker: user("alice"),
            at: at(4),
            upto: 2,
            tokens: 9,
        };
        let ask = Ask::Tldr { thread: thread.clone(), ahead: false };
        m.summarised(Summarised { ask, result: Ok(Got::Tldr(tldr)), warnings: Vec::new() }, at(4));
        let screen = shown(&m, at(64));
        assert!(screen.contains("TL;DR (messenger, 1m ago, S asks for a new one)"), "{screen}");
        assert!(screen.contains("Deployed to staging."), "{screen}");
        m.stored(vec![message(3, &thread, "dev-a", "alice", "and to prod")], at(65));
        assert!(shown(&m, at(65)).contains("TL;DR (messenger, 1m ago, stale, S asks"));
    }

    #[test]
    fn ahead_of_time_one_tldr_is_asked_for_at_a_time_until_the_day_is_spent() {
        let mut m = model();
        let (old, new) = (finished(&mut m, 1), finished(&mut m, 10));
        assert_eq!(m.ahead(at(20)), None, "on demand");
        m.catchup.ahead = true;
        assert_eq!(m.ahead(at(20)), None, "not before the TL;DRs kept are read");
        m.data.cards.tldr_read = true;
        let ask = |thread: &ThreadId| Ask::Tldr { thread: thread.clone(), ahead: true };
        assert_eq!(m.ahead(at(20)), Some(Action::Summarise(ask(&new))));
        assert_eq!(m.ahead(at(20)), None, "one at a time");
        let failed = Summarised { ask: ask(&new), result: Err(anyhow!("no reply")), warnings: Vec::new() };
        m.summarised(failed, at(21));
        assert_eq!(m.ahead(at(21)), Some(Action::Summarise(ask(&old))), "a failed one waits for a newer entry");
        m.summarised(Summarised { ask: ask(&old), result: Ok(Got::Spent(500)), warnings: Vec::new() }, at(22));
        m.stored(vec![message(30, &new, "dev-a", "alice", "more")], at(30));
        assert_eq!(m.ahead(at(30)), None, "stopped for the UTC day");
        assert!(m.ahead(at(30) + TimeDelta::days(1)).is_some());
    }
}
