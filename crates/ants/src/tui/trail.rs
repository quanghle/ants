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
use super::exec::{Ctx, Done, Job};
use super::help::{Help, centered};
use super::keymap::{self, View};
use super::model::{Action, Key, Model};
use super::style::{block, dim};
use crate::cmd::send;
use crate::cmd::trail::runs::{self, Pick};
use crate::cmd::trail::{self as cli, LimitArgs, Shown, TipsArgs, control, saved, start, tips as cli_tips};

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

impl Model {
    /// Take `key` while a run planned waits: `y` starts it, `?` shows the help and another key leaves it unstarted.
    pub fn start_key(&mut self, key: Key) -> Option<Action> {
        if key == Key::Char('?') {
            self.help = Some(Help::default());
            return None;
        }
        let planned = self.starting.take()?;
        if key != Key::Char('y') {
            self.bar.set_status("the run is not started", Tone::Done);
            return None;
        }
        self.bar.set_status(format!("asking {} to start the run…", planned.start.conductor), Tone::Busy);
        Some(Action::Run(Job::TrailStart(Box::new(planned.start)), String::new()))
    }
}

/// The saved trails, as `ants trail list` lists them, for the pager.
pub async fn trails(ctx: &Ctx) -> Result<Done> {
    let listed = cli::saved_trails(&ctx.js).await?;
    let line = match listed.len() {
        0 => return Ok(Done { line: "no saved trails".to_owned(), ..Done::default() }),
        1 => "1 saved trail: the pager lists it".to_owned(),
        n => format!("{n} saved trails: the pager lists them"),
    };
    Ok(Done { line, page: Some(saved::table(&listed, Utc::now())), ..Done::default() })
}

/// The saved trail `what` names, else the run, as `ants trail show` shows it, for the pager.
pub async fn shown(ctx: &Ctx, what: Shown) -> Result<Done> {
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
pub async fn tips(ctx: &Ctx, args: &TipsArgs) -> Result<Done> {
    let now = Utc::now();
    if let Some(id) = &args.retire {
        if !ctx.me.role.is_human() {
            bail!("retiring a tip needs a human's credentials; those of {} are an ant's", ctx.me.id);
        }
        let retired = cli_tips::retire(&ctx.js, &args.name, id, &ctx.me.id, now).await?;
        return Ok(Done { line: cli_tips::retired(&retired, now), ..Done::default() });
    }
    let (board, script) = cli_tips::read(&ctx.js, &args.name).await?;
    let line = cli_tips::summary(&board, args, script.as_deref(), now);
    if cli_tips::listed(&board, args, script.as_deref(), now).is_empty() {
        return Ok(Done { line, ..Done::default() });
    }
    Ok(Done { line, page: Some(cli_tips::render(&board, args, script.as_deref(), now)), ..Done::default() })
}

/// The runs of trails conductors recorded, as `ants trail runs` lists them, then the top tips of their trails, for the
/// pager.
pub async fn listed(ctx: &Ctx) -> Result<Done> {
    let listed = runs::list(&ctx.js, None).await?;
    if listed.is_empty() {
        return Ok(Done { line: "no runs of trails".to_owned(), ..Done::default() });
    }
    let now = Utc::now();
    let going = listed.iter().filter(|run| !run.finished()).count();
    let recorded = if listed.len() == 1 { "1 run".to_owned() } else { format!("{} runs", listed.len()) };
    let line = format!("{recorded} recorded, {going} not over: the pager lists them");
    let rated = crate::cmd::trail::ratings::of_runs(&crate::cmd::trail::ratings::read_or_none(&ctx.js, None).await);
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
pub async fn planned(
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
pub async fn started(ctx: &Ctx, start: &start::Start) -> Result<Done> {
    let run = start.ask(&ctx.client, &ctx.js, ctx.domain.as_ref(), &ctx.me.id).await?;
    Ok(Done { line: start.started(&run), open: Some(start.thread.clone()), ..Done::default() })
}

/// Stop the run `pick` names, as `ants trail stop` does: its first line, then the steps left waiting as warnings.
pub async fn stopped(ctx: &Ctx, pick: &Pick) -> Result<Done> {
    let run = runs::pick(&ctx.js, pick).await?;
    let said = control::stop(&ctx.client, &ctx.me.id, &run).await?;
    let mut lines = said.lines().map(str::trim);
    let line = lines.next().unwrap_or_default().to_owned();
    Ok(Done { line, warnings: lines.map(str::to_owned).collect(), ..Done::default() })
}

/// Resume the run `pick` names under its own limits with those `limits` gives instead, as `ants trail resume` does.
pub async fn resumed(ctx: &Ctx, pick: &Pick, limits: LimitArgs) -> Result<Done> {
    let run = runs::pick(&ctx.js, pick).await?;
    let limits = if limits.given() { Some(limits.over(run.limits)?) } else { None };
    Ok(Done { line: control::resume(&ctx.client, &ctx.me.id, &run, limits).await?, ..Done::default() })
}

#[cfg(test)]
mod tests {
    use ants_proto::provider::{Origin, Starter};
    use ants_proto::{
        Role, RunId, RunState, SavedTrail, Saver, TRAIL_RUNS_BUCKET, TRAIL_TIPS_BUCKET, TRAILS_BUCKET, TrailAction,
        TrailLeg, TrailOutcome, TrailRequest, TrailRun, TrailSource, TrailTip, Uuid, WaitingStep, subject,
    };
    use futures::StreamExt as _;
    use serde_json::json;

    use super::*;
    use crate::connection::Nats;
    use crate::test_support::live::{at, message, user};
    use crate::tui::exec::Outcome;
    use crate::tui::model::{Focus, Page};
    use crate::tui::test_support::model;

    fn screen(m: &Model) -> String {
        crate::test_support::render::rendered(110, 30, |f| crate::tui::frame::draw(f, m, at(2)))
    }

    fn done(result: Done) -> Outcome {
        Outcome { result: Ok(result), text: String::new(), quoting: None }
    }

    /// The leg starting a run of `trail` with `script`.
    fn first_leg(trail: &TrailName, script: &str) -> TrailLeg {
        TrailLeg {
            trail: Some(trail.clone()),
            local: false,
            at: None,
            script: script.to_owned(),
            args: json!(null),
            first_call: 0,
            started_at: Utc::now(),
            steps: 0,
        }
    }

    /// Send `text` from the bar of the view shown.
    fn run(m: &mut Model, text: &str) -> Option<Action> {
        m.follow_drafts();
        m.bar.focused = true;
        m.bar.set_text(text);
        m.key(Key::Enter, at(1))
    }

    #[test]
    fn trail_run_shows_what_the_run_may_spend_until_y_starts_it_and_another_key_leaves_it() {
        let mut m = model();
        let Some(Action::Run(Job::TrailPlan { trail, conductor, args, limits, thread }, _)) =
            run(&mut m, r#"/trail run review @lead {"files": ["a.rs"]}"#)
        else {
            panic!("no plan")
        };
        assert_eq!((trail.as_str(), conductor, args), ("review", Some(user("lead")), Some(json!({"files": ["a.rs"]}))));
        assert_eq!((limits, thread), (TrailLimits::DEFAULT, None), "the default limits, on a new thread");
        let plan = start::Plan {
            name: "review".to_owned(),
            from: "saved for the colony by alice 1m ago".to_owned(),
            description: "Reviews each file".to_owned(),
            phases: vec!["review".to_owned()],
            source: TrailSource::Saved(trail),
        };
        let limits = TrailLimits::DEFAULT;
        let thread = ThreadId::generate();
        let start =
            start::Start { source: plan.source.clone(), args: json!(null), conductor: user("lead"), thread, limits };
        let planned = Planned { summary: start::summary(&plan, &user("lead"), None, &limits), start: start.clone() };
        let plan_done =
            || Done { line: "trail review".to_owned(), planned: Some(Box::new(planned.clone())), ..Done::default() };

        assert_eq!(m.outcome(done(plan_done())), None);
        let shown = screen(&m);
        for line in
            ["Start a run of a trail?", "trail review, saved for the colony by alice", "the run may spend at most:"]
        {
            assert!(shown.contains(line), "{line}: {shown}");
        }
        assert!(shown.contains(" start the run? y start · Esc not started"), "{shown}");
        assert_eq!(m.key(Key::Char('x'), at(2)), None);
        assert!(m.starting.is_none() && !screen(&m).contains("Start a run"), "another key leaves it unstarted");

        m.outcome(done(plan_done()));
        assert_eq!(m.key(Key::Char('?'), at(2)), None);
        assert!(
            screen(&m).contains("start the run its summary shows, then show its thread"),
            "the help lists its keys"
        );
        m.key(Key::Esc, at(2));
        assert!(m.help.is_none() && m.starting.is_some(), "the run waits on after the help");
        let Some(Action::Run(Job::TrailStart(asked), _)) = m.key(Key::Char('y'), at(2)) else { panic!("no start") };
        assert_eq!(*asked, start);
        let started = Done { line: "started run".to_owned(), open: Some(start.thread.clone()), ..Done::default() };
        assert_eq!(m.outcome(done(started)), Some(Action::Open(start.thread.clone())), "its thread shows");
        assert!(matches!(m.page(), Page::Thread(page) if page.history.thread == start.thread));

        m.focus = Focus::View;
        assert_eq!(run(&mut m, "/trail runs"), Some(Action::Run(Job::TrailRuns, "/trail runs".to_owned())));
        let Some(Action::Run(Job::TrailTips(args), _)) = run(&mut m, "/trail tips review --all") else {
            panic!("no tips")
        };
        assert_eq!((args.name.as_str(), args.all), ("review", true));
        let listed = Done { line: "1 run".to_owned(), page: Some("RUN  TRAIL".to_owned()), ..Done::default() };
        assert_eq!(m.outcome(done(listed)), Some(Action::Page("RUN  TRAIL".to_owned())));
        let Some(Action::Run(Job::TrailStop(pick), _)) = run(&mut m, "/trail stop 0190ab") else { panic!("no stop") };
        assert_eq!(pick.to_string(), "0190ab");
        let Some(Action::Run(Job::TrailResume { run: pick, limits }, _)) =
            run(&mut m, "/trail resume 0190ab --steps 80")
        else {
            panic!("no resume")
        };
        assert_eq!((pick.to_string(), limits.steps), ("0190ab".to_owned(), Some(80)));
    }

    /// A plan of a run of the saved trail `review` described by `description`, in two phases.
    fn planned_review(description: &str) -> Planned {
        let plan = start::Plan {
            name: "review".to_owned(),
            from: "saved for the colony by alice 1m ago".to_owned(),
            description: description.to_owned(),
            phases: vec!["read".to_owned(), "review".to_owned()],
            source: TrailSource::Saved("review".parse().unwrap()),
        };
        let (limits, thread) = (TrailLimits::DEFAULT, ThreadId::generate());
        let start =
            start::Start { source: plan.source.clone(), args: json!(null), conductor: user("lead"), thread, limits };
        Planned { summary: start::summary(&plan, &user("lead"), None, &limits), start }
    }

    #[test]
    fn the_box_of_a_run_to_start_cuts_a_long_description_and_keeps_the_conductor_thread_and_limits() {
        // A saved description is one line; one written over many is shown on one row all the same.
        let description: Vec<String> = (1..=20).map(|n| format!("line {n} of what the trail does")).collect();
        let planned = planned_review(&description.join("\n"));
        let shown = crate::test_support::render::rendered(100, 24, |f| planned.draw(f));
        for line in [
            "trail review, saved for the colony by alice",
            "line 1 of what the trail does line 2 of",
            "phases: read, review",
            "conductor lead, reporting on a new thread",
            "steps:",
            "at once:",
            "turns:",
            "wall time:",
            "compute:",
            "y starts it",
        ] {
            assert!(shown.contains(line), "{line}: {shown}");
        }
        assert!(!shown.contains("of the description"), "{shown}");

        let short = planned_review("Reviews each file");
        let shown = crate::test_support::render::rendered(100, 24, |f| short.draw(f));
        assert!(shown.contains("Reviews each file") && !shown.contains("of the description"), "{shown}");
        let shown = crate::test_support::render::rendered(100, 12, |f| short.draw(f));
        for line in ["conductor lead", "turns:", "wall time:", "compute:"] {
            assert!(shown.contains(line), "{line} at 12 rows: {shown}");
        }
        let shown = crate::test_support::render::rendered(100, 9, |f| short.draw(f));
        assert!(shown.contains("compute:") && !shown.contains("conductor"), "the limits go last: {shown}");
    }

    #[test]
    fn trail_run_reports_on_a_thread_read_the_latest_or_the_one_open() {
        let (older, newer) = ("0190aaaa-0000-7000-8000-000000000001", "0190bbbb-0000-7000-8000-000000000002");
        let (older, newer): (ThreadId, ThreadId) = (older.parse().unwrap(), newer.parse().unwrap());
        let mut m = model();
        m.stored(vec![message(1, &older, "alice", "dev-a", "a"), message(2, &newer, "alice", "dev-a", "b")], at(1));
        let thread = |m: &mut Model, text: &str| match run(m, text) {
            Some(Action::Run(Job::TrailPlan { thread, limits, .. }, _)) => Ok((thread, limits.steps)),
            _ => Err(m.bar.status.as_ref().unwrap().text.clone()),
        };
        assert_eq!(thread(&mut m, "/trail run review --thread 0190a --steps 10"), Ok((Some(older.clone()), 10)));
        assert_eq!(thread(&mut m, "/trail run review --thread last"), Ok((Some(newer), 50)));
        assert_eq!(
            thread(&mut m, "/trail run review --thread 0190"),
            Err("2 threads start 0190: give more of the id".into())
        );
        assert_eq!(thread(&mut m, "/trail run review --thread ffff"), Err("no thread ffff read".into()));
        assert_eq!(thread(&mut m, "/trail run review --thread here"), Err("no thread open or selected".into()));
        m.pages.push(Page::Thread(crate::tui::thread::Page::new(older.clone())));
        assert_eq!(thread(&mut m, "/trail run review --thread here"), Ok((Some(older), 50)));
        let error = thread(&mut m, "/trail run review --steps 501").unwrap_err();
        assert_eq!(error, "--steps is over the most a run may take, 500");
    }

    /// Answer each request on `ant`'s control subject as its daemon does: a run started, stopped with a step left
    /// waiting, or resumed.
    async fn conductor(server: &crate::test_support::Server, ant: &UserId) {
        let (client, js) = server.connect(ant).await;
        let mut requests = client.subscribe(subject::trail(ant)).await.unwrap();
        js.query_account().await.unwrap();
        tokio::spawn(async move {
            while let Some(message) = requests.next().await {
                let request: TrailRequest = serde_json::from_slice(&message.payload).unwrap();
                let outcome = match &request.action {
                    TrailAction::Start { .. } => TrailOutcome::Started { run: RunId::generate() },
                    TrailAction::Stop { run } => {
                        let waiting = vec![WaitingStep { to: "dev-a".parse().unwrap(), message: Uuid::nil() }];
                        TrailOutcome::Stopped { run: run.clone(), waiting }
                    }
                    TrailAction::Resume { run, .. } => TrailOutcome::Resumed { run: run.clone() },
                };
                let reply = request.answer(outcome);
                client.publish(reply.subject(), serde_json::to_vec(&reply).unwrap().into()).await.unwrap();
            }
        });
    }

    /// The jobs' context, as `me`, a human.
    async fn human(server: &crate::test_support::Server, me: &UserId) -> Ctx {
        let creds = server.creds_as(me, Role::Human);
        let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
        let (client, js) = nats.connect("test").await.unwrap();
        let (me, sealing) = (nats.me.clone(), crate::people::sealed::Sealing::of(&nats));
        Ctx {
            client,
            js,
            me,
            domain: None,
            meta: Err("unused".into()),
            sealing,
            account: None,
            keyring: None,
            admin: Err("unused".into()),
        }
    }

    const SCRIPT: &str = "meta = {\"name\": \"review\", \"description\": \"Reviews\", \"phases\": []}\nagent(\"go\")\n";

    /// Save [`SCRIPT`] for the colony as `name`, by the human of `ctx`.
    async fn save(ctx: &Ctx, name: &TrailName) -> SavedTrail {
        let saved = SavedTrail {
            name: name.clone(),
            description: "Reviews".to_owned(),
            phases: Vec::new(),
            waypoints: Vec::new(),
            script: SCRIPT.to_owned(),
            saved_by: Saver::Human { user: ctx.me.id.clone() },
            saved_from: None,
            saved_at: Utc::now(),
        };
        let payload = serde_json::to_vec(&saved).unwrap();
        ctx.js.publish(format!("$KV.{TRAILS_BUCKET}.{name}"), payload.into()).await.unwrap().await.unwrap();
        saved
    }

    /// Record a run of `trail` that `lead` conducts for `owner` on `thread`, as `lead`'s daemon does.
    async fn record(
        server: &crate::test_support::Server,
        lead: &UserId,
        owner: &UserId,
        trail: &TrailName,
        thread: ThreadId,
    ) -> TrailRun {
        let record = TrailRun {
            run: "0190aaaa-0000-7000-8000-000000000001".parse().unwrap(),
            conductor: lead.clone(),
            route: vec![first_leg(trail, SCRIPT)],
            owner: Some(owner.clone()),
            thread,
            parent: None,
            spends_from: None,
            origin: Origin { by: Starter::Human { user: owner.clone() }, channel: None },
            limits: TrailLimits::DEFAULT,
            state: RunState::Running,
            phase: None,
            steps: 1,
            turns: 2,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            ended_at: None,
        };
        let (_, lead_js) = server.connect(lead).await;
        let payload = serde_json::to_vec(&record).unwrap();
        let subject = format!("$KV.{TRAIL_RUNS_BUCKET}.{}", record.key());
        lead_js.publish(subject, payload.into()).await.unwrap().await.unwrap();
        record
    }

    #[tokio::test]
    async fn the_trail_jobs_list_and_show_saved_trails_and_runs_as_ants_trail_does() {
        let Some(server) = crate::test_support::Server::start() else { return };
        let (alice, lead) = (user("alice"), user("lead"));
        let ctx = human(&server, &alice).await;
        // A name no machine's ~/.ants/trails holds, as this one's are looked in too.
        let name: TrailName = "chat-test-review".parse().unwrap();
        let error = shown(&ctx, Shown::Trail(name.clone())).await.unwrap_err().to_string();
        assert!(error.starts_with("no trail chat-test-review"), "{error}");
        save(&ctx, &name).await;

        let listed = trails(&ctx).await.unwrap();
        assert!(listed.line.contains("saved trail") && listed.line.contains(": the pager lists"), "{}", listed.line);
        let table = listed.page.unwrap();
        let row = table.lines().find(|row| row.starts_with(name.as_str())).unwrap_or_else(|| panic!("{table}"));
        assert!(row.contains("colony") && row.contains("alice") && row.ends_with("Reviews"), "{row}");

        let trail = shown(&ctx, Shown::Trail(name.clone())).await.unwrap();
        assert_eq!(trail.line, format!("trail {name}: the pager shows its meta and script"));
        let page = trail.page.unwrap();
        assert!(page.contains("saved for the colony by alice") && page.ends_with(SCRIPT), "{page}");

        let record = record(&server, &lead, &alice, &name, ThreadId::generate()).await;
        let heading = format!("run {} of trail {name}", record.run);
        for what in [Shown::Run(record.run.clone()), Shown::Trail("0190aaaa".parse().unwrap())] {
            let run = shown(&ctx, what).await.unwrap();
            assert_eq!(run.line, format!("{heading}: the pager shows its record and journal"));
            let page = run.page.unwrap();
            assert!(page.starts_with(&heading) && page.contains("limits: steps 1 of 50, turns 2 of 200"), "{page}");
        }
        let error = shown(&ctx, Shown::Trail("0190c".parse().unwrap())).await.unwrap_err().to_string();
        assert!(error.starts_with("no trail 0190c"), "{error}");
    }

    #[tokio::test]
    async fn the_trail_jobs_list_plan_start_stop_and_resume_runs_as_ants_trail_does() {
        let Some(server) = crate::test_support::Server::start() else { return };
        let alice = user("alice");
        let ctx = human(&server, &alice).await;
        let lead = user("lead");
        conductor(&server, &lead).await;

        assert_eq!(listed(&ctx).await.unwrap().line, "no runs of trails");
        // A name no machine's ~/.ants/trails holds, as this one's are looked in too.
        let name: TrailName = "chat-test-review".parse().unwrap();
        let error = planned(&ctx, &name, None, None, TrailLimits::DEFAULT, None).await.unwrap_err().to_string();
        assert!(error.starts_with("no trail chat-test-review"), "{error}");
        let saved = save(&ctx, &name).await;
        let big = Some(json!("x".repeat(ants_proto::MAX_TRAIL_ARGS)));
        let error = planned(&ctx, &saved.name, None, big, TrailLimits::DEFAULT, None).await.unwrap_err().to_string();
        assert!(error.starts_with("ARGS has 65538 bytes of JSON"), "{error}");
        let plan = planned(&ctx, &saved.name, None, Some(json!({"files": ["a.rs"]})), TrailLimits::DEFAULT, None)
            .await
            .unwrap();
        let Planned { summary, start } = *plan.planned.unwrap();
        let messenger = UserId::messenger();
        assert_eq!(
            plan.line,
            format!(
                "trail {name} on {messenger}, at most 50 steps, 4 at once, 200 turns, 1d of wall time: y starts it"
            )
        );
        assert!(summary.starts_with(&format!("trail {name}, saved for the colony by alice")), "{summary}");
        assert!(summary.contains(&format!("conductor {messenger}, reporting on a new thread")), "{summary}");
        assert_eq!((start.source, start.args), (TrailSource::Saved(saved.name.clone()), json!({"files": ["a.rs"]})));

        let limits = TrailLimits { steps: 10, wall_secs: 7200, ..TrailLimits::DEFAULT };
        let thread = ThreadId::generate();
        let plan = planned(&ctx, &saved.name, Some(lead.clone()), None, limits, Some(thread.clone())).await.unwrap();
        assert_eq!(
            plan.line,
            format!(
                "trail {name} on lead, reporting on thread {thread}, at most 10 steps, 4 at once, 200 turns, 2h of wall \
                 time: y starts it"
            )
        );
        let Planned { summary, start } = *plan.planned.unwrap();
        assert!(summary.contains(&format!("conductor lead, reporting on thread {thread}")), "{summary}");
        assert!(summary.contains("steps:      10,") && summary.contains("wall time:  2h,"), "{summary}");
        assert_eq!((&start.thread, start.limits), (&thread, limits));
        let started = started(&ctx, &start).await.unwrap();
        assert_eq!(started.open.as_ref(), Some(&start.thread));
        assert!(
            started.line.starts_with("started run ")
                && started.line.ends_with(&format!("on lead, reporting on thread {}", start.thread)),
            "{}",
            started.line
        );

        let record = record(&server, &lead, &alice, &saved.name, start.thread.clone()).await;
        let runs = listed(&ctx).await.unwrap();
        assert_eq!(runs.line, "1 run recorded, 1 not over: the pager lists them");
        assert!(runs.page.unwrap().contains(record.run.as_str()));
        let pick: Pick = "0190AAAA".parse().unwrap();
        let stop = stopped(&ctx, &pick).await.unwrap();
        assert!(stop.line.starts_with(&format!("stopped run {} on lead: ", record.run)), "{}", stop.line);
        assert_eq!(stop.warnings.last().unwrap(), &format!("dev-a message {0}: ants withdraw dev-a {0}", Uuid::nil()));
        let own = LimitArgs::default();
        assert_eq!(resumed(&ctx, &pick, own).await.unwrap().line, format!("resumed run {} on lead", record.run));
        let raised = LimitArgs { turns: Some(400), wall: Some(std::time::Duration::from_hours(48)), ..own };
        assert_eq!(
            resumed(&ctx, &pick, raised).await.unwrap().line,
            format!("resumed run {} on lead under 50 steps, 4 at once, 400 turns, 2d of wall time", record.run)
        );
        let error = stopped(&ctx, &"0190c".parse().unwrap()).await.unwrap_err().to_string();
        assert!(error.starts_with("no run's id starts with 0190c"), "{error}");
    }

    /// Put the tip `id` of `writer` on `trail`, left on [`SCRIPT`] or another, as its daemon does.
    async fn tip(ctx: &Ctx, trail: &TrailName, writer: &str, id: &str, text: &str, current: bool) -> TrailTip {
        let script = if current { ants_proto::script_sha256(SCRIPT) } else { "old".to_owned() };
        let tip = TrailTip {
            trail: trail.clone(),
            id: id.parse().unwrap(),
            writer: user(writer),
            text: text.to_owned(),
            tip_key: None,
            run: RunId::generate(),
            script,
            written_at: Utc::now(),
            retired: None,
        };
        let payload = serde_json::to_vec(&tip).unwrap();
        ctx.js.publish(format!("$KV.{TRAIL_TIPS_BUCKET}.{}", tip.key()), payload.into()).await.unwrap().await.unwrap();
        tip
    }

    #[tokio::test]
    async fn the_trail_jobs_list_and_retire_tips_and_show_the_top_ones_beside_the_runs() {
        let Some(server) = crate::test_support::Server::start() else { return };
        let (alice, lead) = (user("alice"), user("lead"));
        let ctx = human(&server, &alice).await;
        let name: TrailName = "chat-test-review".parse().unwrap();
        let args = TipsArgs { name: name.clone(), key: None, all: false, retire: None };
        let none = tips(&ctx, &args).await.unwrap();
        assert_eq!((none.line.as_str(), none.page), ("no tips on trail chat-test-review", None));

        save(&ctx, &name).await;
        let pinned = tip(&ctx, &name, "dev-a", "aaaaaaaa", "pin the base image", true).await;
        tip(&ctx, &name, "dev-b", "bbbbbbbb", "run the migrations\nfirst", false).await;
        tip(&ctx, &name, "dev-c", "cccccccc", "cache the crates", true).await;
        tip(&ctx, &name, "dev-c", "dddddddd", "a fourth tip", true).await;
        let listed_tips = tips(&ctx, &args).await.unwrap();
        assert_eq!(listed_tips.line, "trail chat-test-review: 4 live tips");
        let page = listed_tips.page.unwrap();
        assert!(page.contains("bbbbbbbb  trail-wide  dev-b   +0 ~0 !0  live, older script"), "{page}");
        assert!(page.contains("\n  run the migrations\n  first\n"), "{page}");

        let retire = TipsArgs { retire: Some(pinned.id.clone()), ..args.clone() };
        let retired = tips(&ctx, &retire).await.unwrap();
        assert!(
            retired.line.starts_with("retired tip aaaaaaaa of trail chat-test-review, left by dev-a "),
            "{retired:?}"
        );
        let error = tips(&ctx, &retire).await.unwrap_err().to_string();
        assert!(error.starts_with("tip aaaaaaaa of trail chat-test-review was retired by alice"), "{error}");
        let listed_tips = tips(&ctx, &args).await.unwrap();
        assert_eq!(listed_tips.line, "trail chat-test-review: 3 live tips, 1 retired, which --all lists");

        record(&server, &lead, &alice, &name, ThreadId::generate()).await;
        let page = listed(&ctx).await.unwrap().page.unwrap();
        let (runs, top) = page.split_once("\n\n").unwrap_or_else(|| panic!("{page}"));
        assert!(runs.starts_with("RUN") && runs.contains("chat-test-review"), "{page}");
        let top: Vec<&str> = top.lines().collect();
        assert_eq!(top[0], "trail chat-test-review: 3 live tips, which /trail tips chat-test-review lists", "{page}");
        assert_eq!(top.len(), 4, "the top 3: {page}");
        assert!(top[1].starts_with("  ") && !page.contains("aaaaaaaa"), "{page}");
        assert!(page.contains("bbbbbbbb  trail-wide  +0 ~0 !0, older script  run the migrations first"), "{page}");
    }
}
