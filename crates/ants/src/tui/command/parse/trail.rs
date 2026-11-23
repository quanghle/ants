//! What `/trail` asks of the saved trails and the runs of trails, read from the words after it as `ants trail` reads
//! its arguments.

use ants_proto::{ThreadId, TrailLimits, TrailName, UserId};
use serde_json::Value;

use super::{first_word, flags};
use crate::cmd::trail::runs::Pick;
use crate::cmd::trail::{LimitArgs, Shown, TipsArgs};

/// What `/trail` asks of the saved trails and the runs of trails, as `ants trail` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trail {
    List,
    /// Show the saved trail, else the run, named.
    Show(Shown),
    /// List a trail's tips, or retire one.
    Tips(TipsArgs),
    Runs,
    /// Start a run of the saved trail `trail` with `args`, a JSON value, on `conductor`, else whom a message naming no
    /// one goes to, under `limits`, reporting on `thread`, else a new one, once confirmed.
    Run {
        trail: TrailName,
        conductor: Option<UserId>,
        args: Option<Value>,
        limits: TrailLimits,
        thread: Option<OnThread>,
    },
    Stop(Pick),
    /// Resume the run `run`, under its own limits with those `limits` gives instead.
    Resume {
        run: Pick,
        limits: LimitArgs,
    },
}

/// The thread `/trail run --thread` names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OnThread {
    /// A thread, by its id or a start of one, as `/goto` takes it.
    Id(String),
    /// The thread most recently active among those read: `last`.
    Last,
    /// The thread slash commands act on, the open one, else the one selected: `here`.
    Here,
}

impl std::str::FromStr for OnThread {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "last" => Ok(Self::Last),
            "here" => Ok(Self::Here),
            id if id.parse::<ThreadId>().is_ok() => Ok(Self::Id(id.to_owned())),
            id => Err(format!("{id} is no thread id, start of one, `last` or `here`")),
        }
    }
}

/// What `/trail`'s `args` ask, or why not: None for its usage.
pub(super) fn read(args: &str) -> Result<Trail, Option<String>> {
    let error = |e: &dyn std::fmt::Display| Some(e.to_string());
    match first_word(args) {
        ("list", "") => Ok(Trail::List),
        ("show", what) if !what.is_empty() && !what.contains(char::is_whitespace) => {
            what.to_ascii_lowercase().parse().map(Trail::Show).map_err(|e| error(&e))
        }
        ("tips", rest) => flags("/trail tips", rest).map(Trail::Tips),
        ("runs", "") => Ok(Trail::Runs),
        ("run", rest) if !rest.is_empty() => trail_run(rest),
        ("stop", run) if !run.is_empty() && !run.contains(char::is_whitespace) => {
            run.parse().map(Trail::Stop).map_err(|e: String| Some(e))
        }
        ("resume", rest) if !rest.is_empty() => trail_resume(rest),
        _ => Err(None),
    }
}

/// The run `/trail run`'s `args` ask for: the trail's name, then the conductor and the options of `ants trail run` in
/// any order, then the script's args; None for its usage.
fn trail_run(args: &str) -> Result<Trail, Option<String>> {
    let error = |e: &dyn std::fmt::Display| Some(e.to_string());
    let (name, rest) = first_word(args);
    let trail = name.parse().map_err(|e| error(&e))?;
    let (TrailOptions { conductor, limits, thread }, rest) = trail_options(rest, true)?;
    let limits = limits.over(TrailLimits::DEFAULT).map_err(|e| error(&e))?;
    let args = match rest {
        "" => None,
        json => Some(crate::cmd::trail::start::json_args(json).map_err(|e| Some(format!("ARGS {e}")))?),
    };
    Ok(Trail::Run { trail, conductor, args, limits, thread })
}

/// The resume `/trail resume`'s `args` ask for: the run, then the options of `ants trail resume` naming limits; None
/// for its usage.
fn trail_resume(args: &str) -> Result<Trail, Option<String>> {
    let (run, rest) = first_word(args);
    let run = run.parse().map_err(|e: String| Some(e))?;
    let (TrailOptions { limits, .. }, rest) = trail_options(rest, false)?;
    if !rest.is_empty() {
        return Err(None);
    }
    limits.over(TrailLimits::DEFAULT).map_err(|e| Some(e.to_string()))?;
    Ok(Trail::Resume { run, limits })
}

/// The options of `/trail run`, else of `/trail resume`.
#[derive(Default)]
struct TrailOptions {
    conductor: Option<UserId>,
    limits: LimitArgs,
    thread: Option<OnThread>,
}

/// The options at the start of `rest`, in any order, and the text after them: the limits `ants trail run` and `ants
/// trail resume` take, and for `/trail run` (`run`) its conductor and thread too.
fn trail_options(mut rest: &str, run: bool) -> Result<(TrailOptions, &str), Option<String>> {
    let error = |e: &dyn std::fmt::Display| Some(e.to_string());
    let mut options = TrailOptions::default();
    loop {
        let (word, after) = first_word(rest);
        if let Some(ant) = word.strip_prefix('@').filter(|_| run) {
            if options.conductor.is_some() {
                return Err(None);
            }
            options.conductor = Some(ant.parse().map_err(|e| error(&e))?);
            rest = after;
            continue;
        }
        let Some(option) = word.strip_prefix("--") else { break };
        let (option, value, after) = match option.split_once('=') {
            Some((option, value)) => (option, value, after),
            None => {
                let (value, after) = first_word(after);
                (option, value, after)
            }
        };
        let placeholder = match option {
            "steps" | "at-once" | "turns" => "N",
            "wall" => "DUR",
            "thread" if run => "THREAD",
            _ if run => return Err(Some(format!("no option --{option}: /trail run takes {RUN_OPTIONS}"))),
            _ => return Err(Some(format!("no option --{option}: /trail resume takes {RESUME_OPTIONS}"))),
        };
        let shown = format!("'--{option} <{placeholder}>'");
        if value.is_empty() {
            return Err(Some(format!("a value is required for {shown} but none was supplied")));
        }
        let invalid = |e: &dyn std::fmt::Display| Some(format!("invalid value '{value}' for {shown}: {e}"));
        let limits = &mut options.limits;
        let given = match option {
            "steps" => once(&mut limits.steps, value.parse().map_err(|e| invalid(&e))?),
            "at-once" => once(&mut limits.at_once, value.parse().map_err(|e| invalid(&e))?),
            "turns" => once(&mut limits.turns, value.parse().map_err(|e| invalid(&e))?),
            "wall" => once(&mut limits.wall, ants_proto::duration::parse(value).map_err(|e| invalid(&e))?),
            _ => once(&mut options.thread, value.parse().map_err(|e| invalid(&e))?),
        };
        if !given {
            return Err(Some(format!("{shown} is given twice")));
        }
        rest = after;
    }
    Ok((options, rest))
}

/// The options `/trail run` takes, as `ants trail run` names them.
const RUN_OPTIONS: &str = "--steps, --at-once, --turns, --wall and --thread";

/// The options `/trail resume` takes, as `ants trail resume` names them.
const RESUME_OPTIONS: &str = "--steps, --at-once, --turns and --wall";

/// Set `slot` to `value` unless set already: whether it was not.
fn once<T>(slot: &mut Option<T>, value: T) -> bool {
    slot.replace(value).is_none()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::test_support::live::user;
    use crate::tui::command::parse::{Command, Slash, parse};

    #[test]
    fn trail_lists_and_shows_trails_and_lists_starts_stops_and_resumes_runs() {
        let trail = |text: &str| match parse(text) {
            Ok(Command::Slash(Slash::Trail(trail))) => trail,
            other => panic!("{text}: {other:?}"),
        };
        assert_eq!(trail("/trail list"), Trail::List);
        assert_eq!(trail("/trail show review"), Trail::Show(Shown::Trail("review".parse().unwrap())));
        assert_eq!(trail("/trail show 0190AB"), Trail::Show(Shown::Trail("0190ab".parse().unwrap())));
        let run = "0190aaaa-0000-7000-8000-000000000001";
        assert_eq!(trail(&format!("/trail show {run}")), Trail::Show(Shown::Run(run.parse().unwrap())));
        assert_eq!(trail("/trail runs"), Trail::Runs);
        let run = |conductor: Option<&str>, args| Trail::Run {
            trail: "review".parse().unwrap(),
            conductor: conductor.map(user),
            args,
            limits: TrailLimits::DEFAULT,
            thread: None,
        };
        assert_eq!(trail("/trail run review"), run(None, None));
        assert_eq!(trail("/TRAIL run review @lead"), run(Some("lead"), None));
        let args = Some(serde_json::json!({"files": ["a.rs", "b.rs"]}));
        assert_eq!(trail(r#"/trail run review @lead {"files": ["a.rs", "b.rs"]}"#), run(Some("lead"), args.clone()));
        assert_eq!(trail(r#"/trail run review {"files": ["a.rs", "b.rs"]}"#), run(None, args));
        assert_eq!(trail("/trail stop 0190AB"), Trail::Stop("0190ab".parse().unwrap()));
        let resume = Trail::Resume { run: "0190ab".parse().unwrap(), limits: LimitArgs::default() };
        assert_eq!(trail("/trail resume 0190ab"), resume);
        let usage = "usage: /trail list | show NAME|RUN | tips NAME [--key KEY] [--all] | tips NAME --retire ID \
                     [--writer ANT] | runs | run NAME [@ANT] [--steps N] [--at-once N] [--turns N] [--wall DUR] \
                     [--thread THREAD] [ARGS] | stop RUN | resume RUN [--steps N] [--at-once N] [--turns N] [--wall \
                     DUR]";
        for refused in [
            "/trail",
            "/trail tips",
            "/trail runs all",
            "/trail run",
            "/trail stop",
            "/trail stop a b",
            "/trail resume",
            "/trail resume 0190ab now",
            "/trail resume 0190ab @lead",
            "/trail list all",
            "/trail show",
        ] {
            assert_eq!(parse(refused).unwrap_err(), usage, "{refused}");
        }
        assert_eq!(parse("/trail show a b").unwrap_err(), usage);
        assert!(parse("/trail show Not_A_Name").unwrap_err().contains("trail name"));
        assert!(parse("/trail run Not_A_Name").unwrap_err().contains("trail name"));
        assert!(parse("/trail run review {files").unwrap_err().starts_with("ARGS not JSON"));
        assert!(parse("/trail run review @Lead").is_err());
        assert!(parse("/trail stop review").unwrap_err().contains("hex digits"));

        let tips =
            |name: &str| TipsArgs { name: name.parse().unwrap(), key: None, all: false, retire: None, writer: None };
        assert_eq!(trail("/trail tips deploy"), Trail::Tips(tips("deploy")));
        let keyed = TipsArgs { key: Some("build image".to_owned()), all: true, ..tips("deploy") };
        assert_eq!(trail(r#"/trail tips deploy --key "build image" --all"#), Trail::Tips(keyed));
        let retire = TipsArgs { retire: Some("abcdefgh".parse().unwrap()), ..tips("deploy") };
        assert_eq!(trail("/trail tips deploy --retire abcdefgh"), Trail::Tips(retire));
        assert!(parse("/trail tips deploy --retire abcdefgh --all").unwrap_err().contains("cannot be used with"));
        assert!(parse("/trail tips deploy --json").unwrap_err().contains("--json"));
        assert!(parse("/trail tips Not_A_Name").unwrap_err().contains("trail name"));
    }

    #[test]
    fn trail_run_takes_the_limits_and_thread_of_ants_trail_run_before_its_args() {
        let trail = |text: &str| match parse(text) {
            Ok(Command::Slash(Slash::Trail(trail))) => trail,
            other => panic!("{text}: {other:?}"),
        };
        let limits = TrailLimits { steps: 10, concurrency: 2, turns: 2000, wall_secs: 7200, ..TrailLimits::DEFAULT };
        assert_eq!(
            trail(r#"/trail run review --steps 10 --at-once=2 @lead --turns 2000 --wall 2h --thread 0190AB [1, -2]"#),
            Trail::Run {
                trail: "review".parse().unwrap(),
                conductor: Some(user("lead")),
                args: Some(serde_json::json!([1, -2])),
                limits,
                thread: Some(OnThread::Id("0190ab".into())),
            }
        );
        let thread = |text: &str| match trail(text) {
            Trail::Run { thread, limits, .. } => (thread, limits.steps),
            other => panic!("{text}: {other:?}"),
        };
        assert_eq!(thread("/trail run review --thread last"), (Some(OnThread::Last), 50));
        assert_eq!(thread("/trail run review --thread=HERE --steps 500"), (Some(OnThread::Here), 500));
        for (refused, why) in [
            ("/trail run review --steps 0", "--steps must be more than 0"),
            ("/trail run review --at-once 17", "--at-once is over the most a run may take, 16"),
            ("/trail run review --turns 2001", "--turns is over the most a run may take, 2000"),
            ("/trail run review --wall 8d", "--wall is over the most a run may take, 7d"),
            ("/trail run review --steps ten", "invalid value 'ten' for '--steps <N>': invalid digit found in string"),
            ("/trail run review --wall", "a value is required for '--wall <DUR>' but none was supplied"),
            ("/trail run review --steps 2 --steps=3", "'--steps <N>' is given twice"),
            (
                "/trail run review --thread a.b",
                "invalid value 'a.b' for '--thread <THREAD>': a.b is no thread id, start of one, `last` or `here`",
            ),
            (
                "/trail run review --yes",
                "no option --yes: /trail run takes --steps, --at-once, --turns, --wall and --thread",
            ),
        ] {
            assert_eq!(parse(refused).unwrap_err(), why, "{refused}");
        }
        assert!(parse("/trail run review @lead @dev-a").unwrap_err().starts_with("usage: /trail"));
        assert!(parse("/trail run review {} --steps 2").unwrap_err().starts_with("ARGS not JSON"));
    }

    #[test]
    fn trail_resume_takes_the_limits_of_ants_trail_resume_after_its_run() {
        let resume = |text: &str| match parse(text) {
            Ok(Command::Slash(Slash::Trail(Trail::Resume { run, limits }))) => (run.to_string(), limits),
            other => panic!("{text}: {other:?}"),
        };
        let limits =
            LimitArgs { steps: Some(100), at_once: Some(2), turns: Some(400), wall: Some(Duration::from_hours(48)) };
        assert_eq!(
            resume("/trail resume 0190AB --turns 400 --wall=2d --steps 100 --at-once 2"),
            ("0190ab".into(), limits)
        );
        let turns = LimitArgs { turns: Some(2000), ..LimitArgs::default() };
        assert_eq!(resume("/trail resume 0190ab --turns=2000"), ("0190ab".into(), turns));
        for (refused, why) in [
            ("/trail resume 0190ab --steps 0", "--steps must be more than 0"),
            ("/trail resume 0190ab --steps 501", "--steps is over the most a run may take, 500"),
            ("/trail resume 0190ab --wall 8d", "--wall is over the most a run may take, 7d"),
            ("/trail resume 0190ab --turns", "a value is required for '--turns <N>' but none was supplied"),
            ("/trail resume 0190ab --wall 2d --wall=3d", "'--wall <DUR>' is given twice"),
            (
                "/trail resume 0190ab --thread last",
                "no option --thread: /trail resume takes --steps, --at-once, --turns and --wall",
            ),
            (
                "/trail resume 0190ab --follow",
                "no option --follow: /trail resume takes --steps, --at-once, --turns and --wall",
            ),
        ] {
            assert_eq!(parse(refused).unwrap_err(), why, "{refused}");
        }
        assert!(parse("/trail resume --steps 2").unwrap_err().contains("hex digits"));
    }
}
