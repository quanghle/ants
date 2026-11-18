//! `/trail`: the runs of trails as `ants trail` lists, starts, stops and resumes them, with the CLI's own code. `/trail
//! runs` shows the CLI's table of runs in the pager; `/trail run` shows what `ants trail run` prints of the run before
//! it starts, the trail and what the run may spend, over the view, until `y` starts it, on a new thread then shown, or
//! another key leaves it unstarted; `/trail stop` and `/trail resume` ask the run's conductor as `ants trail stop` and
//! `ants trail resume` do.

use ants_proto::{ThreadId, TrailLimits, TrailName, UserId};
use anyhow::Result;
use chrono::Utc;
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
use crate::cmd::trail::{control, start};

/// How wide the summary of a run to start is at most.
const WIDTH: u16 = 100;

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
        let rows: Vec<Line> = self.summary.lines().map(|line| Line::raw(format!(" {line}"))).collect();
        let tall = u16::try_from(rows.len()).unwrap_or(u16::MAX).saturating_add(3);
        let area = centered(between, WIDTH.min(between.width), tall);
        let outer = block(" Start a run of a trail? ".to_owned(), true);
        let [body, keys] = Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(outer.inner(area));
        frame.render_widget(Clear, area);
        frame.render_widget(outer, area);
        frame.render_widget(Paragraph::new(rows), body);
        let line = " y starts it, on a new thread · another key leaves it unstarted";
        frame.render_widget(Paragraph::new(Line::styled(line, dim())), keys);
    }
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

/// The runs of trails conductors recorded, as `ants trail runs` lists them, for the pager.
pub async fn listed(ctx: &Ctx) -> Result<Done> {
    let listed = runs::list(&ctx.js, None).await?;
    if listed.is_empty() {
        return Ok(Done { line: "no runs of trails".to_owned(), ..Done::default() });
    }
    let going = listed.iter().filter(|run| !run.finished()).count();
    let recorded = if listed.len() == 1 { "1 run".to_owned() } else { format!("{} runs", listed.len()) };
    let line = format!("{recorded} recorded, {going} not over: the pager lists them");
    Ok(Done { line, page: Some(runs::table(&listed, Utc::now())), ..Done::default() })
}

/// Plan a run of the saved trail `name` with `args` on `conductor`, else whom a message naming no one goes to, under
/// the default limits, reporting on a new thread: summed up as `ants trail run` sums it up, for `y` to start.
pub async fn planned(ctx: &Ctx, name: &TrailName, conductor: Option<UserId>, args: Option<Value>) -> Result<Done> {
    let args = start::sized(args, "ARGS")?;
    let plan = start::saved_plan(&ctx.js, name).await?;
    let conductor = match conductor {
        Some(conductor) => conductor,
        None => send::unaddressed(&ctx.js, &ctx.me, ctx.account.as_deref()).await.to,
    };
    let limits = TrailLimits::DEFAULT;
    let summary = start::summary(&plan, &conductor, None, &limits);
    let line = format!("trail {} on {conductor}, at most {}: y starts it", plan.name, start::limits_line(&limits));
    let start = start::Start { source: plan.source, args, conductor, thread: ThreadId::generate(), limits };
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

/// Resume the run `pick` names under its own limits, as `ants trail resume` does.
pub async fn resumed(ctx: &Ctx, pick: &Pick) -> Result<Done> {
    let run = runs::pick(&ctx.js, pick).await?;
    Ok(Done { line: control::resume(&ctx.client, &ctx.me.id, &run, None).await?, ..Done::default() })
}

#[cfg(test)]
mod tests {
    use ants_proto::provider::{Origin, Starter};
    use ants_proto::{
        Role, RunId, RunState, SavedTrail, TRAIL_RUNS_BUCKET, TRAILS_BUCKET, TrailAction, TrailOutcome, TrailRequest,
        TrailRun, TrailSource, Uuid, WaitingStep, subject,
    };
    use futures::StreamExt as _;
    use serde_json::json;

    use super::*;
    use crate::connection::Nats;
    use crate::test_support::live::{at, user};
    use crate::tui::exec::Outcome;
    use crate::tui::model::{Focus, Page};
    use crate::tui::test_support::model;

    fn screen(m: &Model) -> String {
        crate::test_support::render::rendered(110, 30, |f| crate::tui::frame::draw(f, m, at(2)))
    }

    fn done(result: Done) -> Outcome {
        Outcome { result: Ok(result), text: String::new(), quoting: None }
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
        let Some(Action::Run(Job::TrailPlan { trail, conductor, args }, _)) =
            run(&mut m, r#"/trail run review @lead {"files": ["a.rs"]}"#)
        else {
            panic!("no plan")
        };
        assert_eq!((trail.as_str(), conductor, args), ("review", Some(user("lead")), Some(json!({"files": ["a.rs"]}))));
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
        assert!(screen(&m).contains("start the run its summary shows, on a new thread"), "the help lists its keys");
        m.key(Key::Esc, at(2));
        assert!(m.help.is_none() && m.starting.is_some(), "the run waits on after the help");
        let Some(Action::Run(Job::TrailStart(asked), _)) = m.key(Key::Char('y'), at(2)) else { panic!("no start") };
        assert_eq!(*asked, start);
        let started = Done { line: "started run".to_owned(), open: Some(start.thread.clone()), ..Done::default() };
        assert_eq!(m.outcome(done(started)), Some(Action::Open(start.thread.clone())), "its thread shows");
        assert!(matches!(m.page(), Page::Thread(page) if page.history.thread == start.thread));

        m.focus = Focus::View;
        assert_eq!(run(&mut m, "/trail runs"), Some(Action::Run(Job::TrailRuns, "/trail runs".to_owned())));
        let listed = Done { line: "1 run".to_owned(), page: Some("RUN  TRAIL".to_owned()), ..Done::default() };
        assert_eq!(m.outcome(done(listed)), Some(Action::Page("RUN  TRAIL".to_owned())));
        let Some(Action::Run(Job::TrailStop(pick), _)) = run(&mut m, "/trail stop 0190ab") else { panic!("no stop") };
        assert_eq!(pick.to_string(), "0190ab");
        assert!(matches!(run(&mut m, "/trail resume 0190ab"), Some(Action::Run(Job::TrailResume(_), _))));
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

    #[tokio::test]
    async fn the_trail_jobs_list_plan_start_stop_and_resume_runs_as_ants_trail_does() {
        let Some(server) = crate::test_support::Server::start() else { return };
        let alice = user("alice");
        let creds = server.creds_as(&alice, Role::Human);
        let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
        let (client, js) = nats.connect("test").await.unwrap();
        let (me, sealing) = (nats.me.clone(), crate::people::sealed::Sealing::of(&nats));
        let ctx = Ctx {
            client,
            js: js.clone(),
            me,
            domain: None,
            meta: Err("unused".into()),
            sealing,
            account: None,
            keyring: None,
            admin: Err("unused".into()),
        };
        let lead = user("lead");
        conductor(&server, &lead).await;

        assert_eq!(listed(&ctx).await.unwrap().line, "no runs of trails");
        // A name no machine's ~/.ants/trails holds, as this one's are looked in too.
        let name: TrailName = "chat-test-review".parse().unwrap();
        let error = planned(&ctx, &name, None, None).await.unwrap_err().to_string();
        assert!(error.starts_with("no trail chat-test-review"), "{error}");
        let script = "meta = {\"name\": \"review\", \"description\": \"Reviews\", \"phases\": []}\nagent(\"go\")\n";
        let saved = SavedTrail {
            name: name.clone(),
            description: "Reviews".to_owned(),
            phases: Vec::new(),
            waypoints: Vec::new(),
            script: script.to_owned(),
            saved_by: alice.clone(),
            saved_at: Utc::now(),
        };
        let payload = serde_json::to_vec(&saved).unwrap();
        js.publish(format!("$KV.{TRAILS_BUCKET}.{name}"), payload.into()).await.unwrap().await.unwrap();
        let big = Some(json!("x".repeat(ants_proto::MAX_TRAIL_ARGS)));
        let error = planned(&ctx, &saved.name, None, big).await.unwrap_err().to_string();
        assert!(error.starts_with("ARGS has 65538 bytes of JSON"), "{error}");
        let plan = planned(&ctx, &saved.name, None, Some(json!({"files": ["a.rs"]}))).await.unwrap();
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

        let start = start::Start {
            conductor: lead.clone(),
            ..planned(&ctx, &saved.name, Some(lead.clone()), None).await.unwrap().planned.unwrap().start
        };
        let started = started(&ctx, &start).await.unwrap();
        assert_eq!(started.open.as_ref(), Some(&start.thread));
        assert!(
            started.line.starts_with("started run ")
                && started.line.ends_with(&format!("on lead, reporting on thread {}", start.thread)),
            "{}",
            started.line
        );

        let (_, lead_js) = server.connect(&lead).await;
        let record = TrailRun {
            run: "0190aaaa-0000-7000-8000-000000000001".parse().unwrap(),
            conductor: lead.clone(),
            trail: Some(saved.name.clone()),
            script: script.to_owned(),
            args: json!(null),
            owner: Some(alice.clone()),
            thread: start.thread.clone(),
            parent: None,
            origin: Origin { by: Starter::Human { user: alice.clone() }, channel: None },
            limits: TrailLimits::DEFAULT,
            state: RunState::Running,
            phase: None,
            steps: 1,
            turns: 2,
            started_at: Utc::now(),
            updated_at: Utc::now(),
            ended_at: None,
        };
        let payload = serde_json::to_vec(&record).unwrap();
        lead_js
            .publish(format!("$KV.{TRAIL_RUNS_BUCKET}.{}", record.key()), payload.into())
            .await
            .unwrap()
            .await
            .unwrap();
        let runs = listed(&ctx).await.unwrap();
        assert_eq!(runs.line, "1 run recorded, 1 not over: the pager lists them");
        assert!(runs.page.unwrap().contains(record.run.as_str()));
        let pick: Pick = "0190AAAA".parse().unwrap();
        let stop = stopped(&ctx, &pick).await.unwrap();
        assert!(stop.line.starts_with(&format!("stopped run {} on lead: ", record.run)), "{}", stop.line);
        assert_eq!(stop.warnings.last().unwrap(), &format!("dev-a message {0}: ants withdraw dev-a {0}", Uuid::nil()));
        assert_eq!(resumed(&ctx, &pick).await.unwrap().line, format!("resumed run {} on lead", record.run));
        let error = stopped(&ctx, &"0190c".parse().unwrap()).await.unwrap_err().to_string();
        assert!(error.starts_with("no run's id starts with 0190c"), "{error}");
    }
}
