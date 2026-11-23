//! `/trail`: the saved trails, their tips and the runs of trails as `ants trail` lists, shows, starts, stops and
//! resumes them, with the CLI's own code. `/trail list`, `/trail show`, `/trail tips` and `/trail runs` show what `ants
//! trail list`, `ants trail show`, `ants trail tips` and `ants trail runs` print in the pager, the runs followed by the
//! top tips of their trails; `/trail tips --retire` retires a tip as `ants trail tips --retire` does; `/trail run`
//! shows what `ants trail run` prints of the run before it starts, the trail and what the run may spend, over the view,
//! until `y` starts it, on a new thread then shown, or another key leaves it unstarted; `/trail stop` and `/trail
//! resume` ask the run's conductor as `ants trail stop` and `ants trail resume` do.

use ants_proto::{ThreadId, TipBoard, TrailLimits, TrailName, UserId};
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::{Clear, Paragraph};
use serde_json::Value;

use super::command::Tone;
use super::exec::{self, Ctx, Done};
use super::help::{Help, centered};
use super::keymap::{self, View};
use super::model::{Action, Key, Model, Overlay};
use super::style::{block, dim};
use crate::messages::send;
use crate::trails::runs::{self, Pick};
use crate::trails::{self as cli, LimitArgs, Shown, TipsArgs, control, saved, start, tips as cli_tips};

/// How wide the summary of a run to start is at most.
const WIDTH: u16 = 100;

/// How many trails of the runs listed, the latest runs' first, `/trail runs` shows the top tips of.
const TIPPED_TRAILS: usize = 8;

/// How many of a trail's live tips `/trail runs` shows.
const TOP_TIPS: usize = 3;

/// How many characters of a tip's text `/trail runs` shows.
const TIP_EXCERPT: usize = 80;

/// A run of a saved trail `/trail run` planned, waiting for `y` to start: what `ants trail run` shows of it first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Planned {
    pub summary: String,
    pub start: start::Start,
}

impl Planned {
    /// The footer asking for `y`.
    pub fn line(&self) -> Line<'static> {
        let keys = keymap::entries(View::Start).iter().filter(|e| e.rank != keymap::HELP_ONLY);
        let keys: Vec<String> = keys.map(keymap::Entry::hint).collect();
        Line::styled(format!(" start the run? {}", keys.join(" · ")), Style::new().fg(Color::Yellow))
    }

    /// Draw the summary over the screen between the header and the footer.
    pub fn draw(&self, frame: &mut Frame) {
        let screen = frame.area();
        let between = Rect { y: screen.y + 1, height: screen.height.saturating_sub(2), ..screen };
        let room = usize::from(between.height.saturating_sub(3));
        let rows: Vec<Line> = fit(&self.summary, room).into_iter().map(|line| Line::raw(format!(" {line}"))).collect();
        let tall = u16::try_from(rows.len()).unwrap_or(u16::MAX).saturating_add(3);
        let area = centered(between, WIDTH.min(between.width), tall);
        let outer = block(" Start a run of a trail? ".to_owned(), true);
        let [body, keys] = Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(outer.inner(area));
        frame.render_widget(Clear, area);
        frame.render_widget(outer, area);
        frame.render_widget(Paragraph::new(rows), body);
        let line = " y starts it · another key leaves it unstarted";
        frame.render_widget(Paragraph::new(Line::styled(line, dim())), keys);
    }
}

/// The rows of `summary` that fit in `room`: its first row, its description, its phases, then the conductor, the
/// thread and the limits, the description cut first, to a mark saying how many of its rows are not shown, then the
/// first row and the phases, so the conductor, the thread and the limits are the last to go.
fn fit(summary: &str, room: usize) -> Vec<String> {
    let rows: Vec<&str> = summary.lines().collect();
    if rows.len() <= room {
        return rows.into_iter().map(str::to_owned).collect();
    }
    let mut tail = rows.iter().rposition(|row| row.starts_with("conductor ")).unwrap_or(rows.len());
    if tail > 1 && rows[tail - 1].starts_with("  phases: ") {
        tail -= 1;
    }
    let head = usize::from(tail > 0);
    let (described, kept) = (tail - head, rows.len() - tail);
    let shown = room.saturating_sub(head + kept + 1);
    let mut out: Vec<String> = Vec::with_capacity(room);
    if room > head + kept {
        out.extend(rows[..head + shown].iter().map(|row| (*row).to_owned()));
        let cut = described - shown;
        out.push(format!("  … {cut} more {} of the description", if cut == 1 { "line" } else { "lines" }));
    }
    let from = rows.len() - (room - out.len()).min(kept);
    out.extend(rows[from..].iter().map(|row| (*row).to_owned()));
    out
}

impl Planned {
    /// Take `key` while the run waits: `y` starts it, `?` shows the help and another key leaves it unstarted. The run
    /// still planned, if it waits on.
    pub fn key(self, key: Key, model: &mut Model) -> (Option<Self>, Option<Action>) {
        if key == Key::Char('?') {
            model.overlay(Overlay::Help(Help::default()));
            return (Some(self), None);
        }
        if key != Key::Char('y') {
            model.ui.bar.set_status("the run is not started", Tone::Done);
            return (None, None);
        }
        model.ui.bar.set_status(format!("asking {} to start the run…", self.start.conductor), Tone::Busy);
        (None, Some(Action::Run(exec::Job::Trail(Job::Start(Box::new(self.start))), String::new())))
    }
}

/// What `/trail` asks of the saved trails and the runs of trails over the connection, as `ants trail` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Job {
    List,
    /// Show the saved trail, else the run, named.
    Show(Shown),
    /// List a trail's tips, or retire one.
    Tips(TipsArgs),
    Runs,
    /// Plan a run of the saved trail `trail` with `args`, a JSON value, on `conductor`, else whom a message naming no
    /// one goes to, under `limits`, reporting on `thread`, else a new one, as `ants trail run` sums it up before
    /// asking to start it ([`Done::planned`]).
    Plan {
        trail: TrailName,
        conductor: Option<UserId>,
        args: Option<Value>,
        limits: TrailLimits,
        thread: Option<ThreadId>,
    },
    /// Ask the conductor to start the run planned, as `ants trail run` does once confirmed.
    Start(Box<start::Start>),
    Stop(Pick),
    /// Resume the run `run`, under its own limits with those `limits` gives instead.
    Resume {
        run: Pick,
        limits: LimitArgs,
    },
}

impl Job {
    pub(super) async fn run(self, ctx: &Ctx) -> Result<Done> {
        match self {
            Self::List => trails(ctx).await,
            Self::Show(what) => shown(ctx, what).await,
            Self::Tips(args) => tips(ctx, &args).await,
            Self::Runs => listed(ctx).await,
            Self::Plan { trail, conductor, args, limits, thread } => {
                planned(ctx, &trail, conductor, args, limits, thread).await
            }
            Self::Start(start) => started(ctx, &start).await,
            Self::Stop(run) => stopped(ctx, &run).await,
            Self::Resume { run, limits } => resumed(ctx, &run, limits).await,
        }
    }
}

/// The saved trails, as `ants trail list` lists them, for the pager.
async fn trails(ctx: &Ctx) -> Result<Done> {
    let listed = cli::saved_trails(&ctx.js).await?;
    let line = match listed.len() {
        0 => return Ok(Done { line: "no saved trails".to_owned(), ..Done::default() }),
        1 => "1 saved trail: the pager lists it".to_owned(),
        n => format!("{n} saved trails: the pager lists them"),
    };
    Ok(Done { line, page: Some(saved::table(&listed, Utc::now())), ..Done::default() })
}

/// The saved trail `what` names, else the run, as `ants trail show` shows it, for the pager.
async fn shown(ctx: &Ctx, what: Shown) -> Result<Done> {
    let now = Utc::now();
    let (page, shows) = match cli::find(&ctx.js, what, ctx.me.role.is_human()).await? {
        cli::Found::Trail(found) => (saved::render(&found, now), "its meta and script"),
        cli::Found::Run(run) => (cli::run_shown(&ctx.js, &run, now).await?, "its record and journal"),
    };
    let line = format!("{}: the pager shows {shows}", page.lines().next().unwrap_or_default());
    Ok(Done { line, page: Some(page), ..Done::default() })
}

/// The tips of a trail, as `ants trail tips` lists them, for the pager; or the tip retired, as `ants trail tips
/// --retire` retires it.
async fn tips(ctx: &Ctx, args: &TipsArgs) -> Result<Done> {
    let now = Utc::now();
    if let Some(id) = &args.retire {
        if !ctx.me.role.is_human() {
            bail!("retiring a tip needs a human's credentials; those of {} are an ant's", ctx.me.id);
        }
        let retired = cli_tips::retire(&ctx.js, &args.name, id, args.writer.as_ref(), &ctx.me.id, now).await?;
        return Ok(Done { line: cli_tips::retired(&retired, now), ..Done::default() });
    }
    let (board, script) = cli_tips::listing(&ctx.js, &args.name).await?;
    let line = cli_tips::summary(&board, args, script.as_deref(), now);
    if cli_tips::listed(&board, args, script.as_deref(), now).is_empty() {
        return Ok(Done { line, ..Done::default() });
    }
    Ok(Done { line, page: Some(cli_tips::render(&board, args, script.as_deref(), now)), ..Done::default() })
}

/// The runs of trails conductors recorded, as `ants trail runs` lists them, then the top tips of their trails, for the
/// pager.
async fn listed(ctx: &Ctx) -> Result<Done> {
    let listed = runs::list(&ctx.js, None).await?;
    if listed.is_empty() {
        return Ok(Done { line: "no runs of trails".to_owned(), ..Done::default() });
    }
    let now = Utc::now();
    let going = listed.iter().filter(|run| !run.finished()).count();
    let recorded = if listed.len() == 1 { "1 run".to_owned() } else { format!("{} runs", listed.len()) };
    let line = format!("{recorded} recorded, {going} not over: the pager lists them");
    let rated = crate::trails::ratings::of_runs(&crate::trails::ratings::read_or_none(&ctx.js, None).await);
    let mut page = runs::table(&listed, &rated, now);
    let mut trails: Vec<&TrailName> = Vec::new();
    for trail in listed.iter().rev().filter_map(|run| run.trail()) {
        if !trails.contains(&trail) && trails.len() < TIPPED_TRAILS {
            trails.push(trail);
        }
    }
    for trail in trails {
        match cli_tips::read(&ctx.js, trail).await {
            Ok((board, script)) => page.push_str(&top_tips(&board, script.as_deref(), now)),
            Err(e) => {
                tracing::warn!(%trail, "not showing the tips of a trail: {e:#}");
                break;
            }
        }
    }
    Ok(Done { line, page: Some(page), ..Done::default() })
}

/// The live tips of `board` in brief, the top [`TOP_TIPS`] in rank order for the current script `script`, after a
/// blank line: nothing when it has none.
fn top_tips(board: &TipBoard, script: Option<&str>, now: DateTime<Utc>) -> String {
    let live: Vec<_> = board.ranked(now, script).into_iter().filter(|r| r.status.is_live()).collect();
    if live.is_empty() {
        return String::new();
    }
    let trail = board.trail();
    let count = if live.len() == 1 { "1 live tip".to_owned() } else { format!("{} live tips", live.len()) };
    let rows: Vec<[String; 4]> = live
        .iter()
        .take(TOP_TIPS)
        .map(|ranked| {
            let mut votes = cli_tips::votes(ranked);
            if ranked.older_script {
                votes.push_str(", older script");
            }
            let text = crate::text::one_line(&ranked.tip.text, TIP_EXCERPT);
            [ranked.tip.id.to_string(), cli_tips::key(ranked.tip), votes, text]
        })
        .collect();
    let mut out = format!("\n\ntrail {trail}: {count}, which /trail tips {trail} lists");
    for row in cli::columns(&rows).lines() {
        out.push_str("\n  ");
        out.push_str(row);
    }
    out
}

/// Plan a run of the saved trail `name` with `args` on `conductor`, else whom a message naming no one goes to, under
/// `limits`, reporting on `thread`, else a new one: summed up as `ants trail run` sums it up, for `y` to start.
async fn planned(
    ctx: &Ctx,
    name: &TrailName,
    conductor: Option<UserId>,
    args: Option<Value>,
    limits: TrailLimits,
    thread: Option<ThreadId>,
) -> Result<Done> {
    let args = start::sized(args, "ARGS")?;
    let plan = start::saved_plan(&ctx.js, name).await?;
    let conductor = match conductor {
        Some(conductor) => conductor,
        None => send::unaddressed(&ctx.js, &ctx.me, ctx.account.as_deref()).await.to,
    };
    let summary = start::summary(&plan, &conductor, thread.as_ref(), &limits);
    let on = thread.as_ref().map_or_default(|thread| format!(", reporting on thread {thread}"));
    let line = format!("trail {} on {conductor}{on}, at most {}: y starts it", plan.name, start::limits_line(&limits));
    let thread = thread.unwrap_or_else(ThreadId::generate);
    let start = start::Start { source: plan.source, args, conductor, thread, limits };
    Ok(Done { line, planned: Some(Box::new(Planned { summary, start })), ..Done::default() })
}

/// Ask the conductor to start the run `start`, showing its thread then.
async fn started(ctx: &Ctx, start: &start::Start) -> Result<Done> {
    let run = start.ask(&ctx.client, &ctx.js, ctx.domain.as_ref(), &ctx.me.id).await?;
    Ok(Done { line: start.started(&run), open: Some(start.thread.clone()), ..Done::default() })
}

/// Stop the run `pick` names, as `ants trail stop` does: its first line, or, when steps are left waiting in other
/// ants' queues, a count of them, the pager showing what `ants trail stop` prints, with how to withdraw each.
async fn stopped(ctx: &Ctx, pick: &Pick) -> Result<Done> {
    let run = runs::pick(&ctx.js, pick).await?;
    let stop = control::stop(&ctx.client, &ctx.me.id, &run).await?;
    let said = stop.said(&run);
    if let control::Stop::Stopped { waiting } = &stop
        && !waiting.is_empty()
    {
        let steps = match waiting.len() {
            1 => "1 step".to_owned(),
            n => format!("{n} steps"),
        };
        let line = format!(
            "stopped run {} on {}, {steps} still waiting in other ants' queues: the pager lists them",
            run.run, run.conductor
        );
        return Ok(Done { line, page: Some(said), ..Done::default() });
    }
    Ok(Done { line: said.lines().next().unwrap_or_default().to_owned(), ..Done::default() })
}

/// Resume the run `pick` names under its own limits with those `limits` gives instead, as `ants trail resume` does.
async fn resumed(ctx: &Ctx, pick: &Pick, limits: LimitArgs) -> Result<Done> {
    let run = runs::pick(&ctx.js, pick).await?;
    let limits = if limits.given() { Some(limits.over(run.limits)?) } else { None };
    Ok(Done { line: control::resume(&ctx.client, &ctx.me.id, &run, limits).await?, ..Done::default() })
}

#[cfg(test)]
mod tests;
