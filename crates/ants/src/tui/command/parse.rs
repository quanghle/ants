//! What the command bar's text says to do: a message to start a thread with a user, a post to a channel, text to
//! say on the open thread, or a slash command ([`COMMANDS`]).

use std::time::Duration;

use ants_proto::{ChannelId, ChannelNotify, Reaction, ThreadId, UserId};
use chrono::TimeDelta;

use crate::admin::orders::{add, clone};
use crate::threads::thread_meta::Marking;
use crate::tui::exec::{AddAnt, CloneAnt};
use crate::tui::{notify, snooze};

mod trail;

pub use trail::{OnThread, Trail};

/// A slash command as the list shows it, with what it needs to act.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Row {
    pub name: &'static str,
    /// Its arguments: those of a command of several forms, between ` | `.
    pub args: &'static str,
    /// What it does.
    pub about: &'static str,
    /// What it needs of the view shown, given no argument naming another target.
    pub needs: Needs,
}

/// What a slash command needs of the view shown to act there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Needs {
    Nothing,
    /// A thread open or selected.
    Thread,
    /// A tool request waiting on the thread open or selected.
    Request,
    /// An ant at work on the thread open or selected.
    BusyAnt,
    /// A thread open, not only selected.
    OpenThread,
    /// A question to the human waiting for an answer on the thread open or selected.
    Question,
    /// An attachment marked on the message selected.
    Attachment,
    /// A dead letter selected, or on the thread open or selected.
    Letter,
    /// A message selected on the thread open, else a TL;DR of the thread open or selected.
    Forwardable,
}

const fn row(name: &'static str, args: &'static str, about: &'static str, needs: Needs) -> Row {
    Row { name, args, about, needs }
}

/// The slash commands.
pub const COMMANDS: &[Row] = &[
    row("approve", "[ID] [--always]", "allow a tool request, once or for the session", Needs::Request),
    row("reject", "[ID]", "refuse a tool request", Needs::Request),
    row("grant", "[TEXT]", "fresh turns for the stuck ant", Needs::Thread),
    row("cancel", "", "stop the prompt an ant runs", Needs::BusyAnt),
    row("later", "TEXT", "reply without interrupting the ant at work", Needs::OpenThread),
    row("answer", "PICK...", "answer the questions asked: N, N,M or TEXT, one each", Needs::Question),
    row("done", "", "mark the thread done", Needs::Thread),
    row("undone", "", "mark the thread not done", Needs::Thread),
    row("pin", "", "pin the thread", Needs::Thread),
    row("unpin", "", "unpin the thread", Needs::Thread),
    row("mute", "[DURATION]", "hold back the thread's notifications", Needs::Thread),
    row("unmute", "", "notify of the thread again", Needs::Thread),
    row(
        "notify",
        "all|humans|mentions | snooze DURATION | resume | status",
        "which unread posts of the channel count; snooze or resume push notifications, or show what holds them",
        Needs::Nothing,
    ),
    row("snooze", "[DURATION]", "keep the thread off home, 1h unless given", Needs::Thread),
    row("unsnooze", "", "bring the thread back on home", Needs::Thread),
    row(
        "remind",
        "DURATION|TIME [NOTE]",
        "remind of the thread (or selected message) on home and by push then",
        Needs::Thread,
    ),
    row("unremind", "", "clear the thread's reminder", Needs::Thread),
    row("reminders", "", "every reminder, the soonest due first, in the pager", Needs::Nothing),
    row("schedules", "", "the machine's schedules, their next and last firings, in the pager", Needs::Nothing),
    row("archive", "", "archive the thread", Needs::Thread),
    row("unarchive", "", "unarchive the thread", Needs::Thread),
    row("title", "TEXT", "title the thread", Needs::Thread),
    row("notes", "TEXT", "post to the thread's notes: wakes nobody, renews their turn budget", Needs::Thread),
    row("react", "NAME", "react to the latest message", Needs::Thread),
    row("goto", "THREAD|#channel|@user", "show a thread, channel or user", Needs::Nothing),
    row("search", "QUERY", "search the threads", Needs::Nothing),
    row("digest", "[SINCE]", "catch-up summary", Needs::Nothing),
    row("tldr", "", "summary of the thread", Needs::Thread),
    row("tree", "", "the thread's delegation tree", Needs::Thread),
    row(
        "forward",
        "@ANT [TEXT]",
        "hand the selected message, else the thread's TL;DR, to an ant on a new thread",
        Needs::Forwardable,
    ),
    row("attach", "PATH", "attach a file to the next message", Needs::Nothing),
    row("detach", "", "drop the files attached", Needs::Nothing),
    row("save", "[PATH]", "save the marked attachment", Needs::Attachment),
    row(
        "add",
        "NAME --on MACHINE [FLAGS]",
        "add an ant by admin order, with the flags of ants ant add",
        Needs::Nothing,
    ),
    row(
        "clone",
        "ANT NEW [FLAGS]",
        "clone the ant as NEW by admin order, with the flags of ants ant clone",
        Needs::Nothing,
    ),
    row("retire", "ANT", "retire the ant by admin order, its home kept", Needs::Nothing),
    row(
        "dead",
        "replay [SEQ] | drop [SEQ]",
        "send a dead letter (the one selected, else the thread's latest) to its ant again, or drop it once y confirms",
        Needs::Letter,
    ),
    row(
        "trail",
        "list | show NAME|RUN | tips NAME [--key KEY] [--all] | tips NAME --retire ID [--writer ANT] | runs | run \
         NAME [@ANT] [--steps N] [--at-once N] [--turns N] [--wall DUR] [--thread THREAD] [ARGS] | stop RUN | resume \
         RUN [--steps N] [--at-once N] [--turns N] [--wall DUR]",
        "list or show saved trails and runs, list or retire a trail's tips, start a run of a saved trail once y \
         confirms it, stop or resume a run",
        Needs::Nothing,
    ),
    row("quit", "", "leave ants chat", Needs::Nothing),
];

/// What the bar's text says to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Start a thread with the user.
    Ask {
        to: UserId,
        text: String,
    },
    /// Post a new thread to the channel.
    Post {
        channel: ChannelId,
        text: String,
    },
    /// Reply on the open thread, else ask the human's assistant, else the messenger.
    Say(String),
    /// Reply on the open thread, waiting behind the prompt running there rather than interrupting it.
    Later(String),
    Slash(Slash),
}

/// A slash command and its arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Slash {
    Approve {
        id: Option<String>,
        always: bool,
    },
    Reject {
        id: Option<String>,
    },
    Grant(Option<String>),
    Cancel,
    /// The text answering the questions asked: a pick, or one per question ([`quoted_words`]).
    Answer(String),
    Done(bool),
    Mark(Marking),
    Mute(Option<Duration>),
    Unmute,
    Notify(Notify),
    /// Keep the thread off home this long.
    Snooze(TimeDelta),
    Unsnooze,
    /// Remind of the thread as `DURATION|TIME [NOTE]` says.
    Remind(String),
    Unremind,
    /// List every reminder, as `ants remind list` does.
    Reminders,
    /// List the machine's schedules, as `ants schedule list` does.
    Schedules,
    Title(String),
    /// Post the text to the thread's notes.
    Notes(String),
    React(Reaction),
    Goto(Goto),
    Search(String),
    Digest(Option<String>),
    Tldr,
    Tree,
    /// Hand the selected message, else the thread's TL;DR, to the user on a new thread, with the note.
    Forward {
        to: UserId,
        note: String,
    },
    /// Stage the file at the path for the next message.
    Attach(String),
    Detach,
    /// Save the marked attachment of the selected message, to the path if given.
    Save(Option<String>),
    Add(AddAnt),
    Clone(CloneAnt),
    /// Retire the ant by admin order.
    Retire(UserId),
    Dead(Dead),
    Trail(Trail),
    Quit,
}

/// What `/notify` asks: the notify level of the channel on view, as `ants channel notify` sets it, or of the human's
/// push notifications, as `ants notify` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Notify {
    Level(ChannelNotify),
    Push(notify::Job),
}

/// What `/dead` does with a dead letter, as `ants dead replay|drop` does: the one at the stream sequence given, else
/// the one selected or the thread's latest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dead {
    Replay(Option<u64>),
    /// Drop it unreplayed, once `y` confirms.
    Drop(Option<u64>),
}

/// Where `/goto` leads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Goto {
    /// A thread, by its id or a start of one.
    Thread(String),
    Channel(ChannelId),
    User(UserId),
}

/// `text`'s first word and the rest, trimmed.
fn first_word(text: &str) -> (&str, &str) {
    text.split_once(char::is_whitespace).map_or((text, ""), |(word, rest)| (word, rest.trim()))
}

/// What the bar's `text` says to do, or why it says nothing that can be done.
/// `text` split at whitespace into words, a run quoted with `'` or `"` at the start of a word, up to its closing quote,
/// being one word without its quotes; a quote with none closing it is kept as typed.
pub fn quoted_words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut rest = text.trim_start();
    while !rest.is_empty() {
        let quoted = rest.strip_prefix(['\'', '"']).and_then(|inner| {
            let quote = rest.chars().next()?;
            let end = inner.find(quote)?;
            Some((inner[..end].to_owned(), &inner[end + 1..]))
        });
        let (word, after) = quoted.unwrap_or_else(|| {
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            (rest[..end].to_owned(), &rest[end..])
        });
        words.push(word);
        rest = after.trim_start();
    }
    words
}

pub fn parse(text: &str) -> Result<Command, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("nothing to send".to_owned());
    }
    if let Some(literal) = text.strip_prefix("//") {
        return Ok(Command::Say(format!("/{literal}")));
    }
    if let Some(command) = text.strip_prefix('/') {
        return match first_word(command) {
            (name, "") if name.eq_ignore_ascii_case("later") => Err("usage: /later TEXT".to_owned()),
            (name, later) if name.eq_ignore_ascii_case("later") => Ok(Command::Later(later.to_owned())),
            _ => slash(command).map(Command::Slash),
        };
    }
    if let Some(rest) = text.strip_prefix('@') {
        let (name, text) = first_word(rest);
        let to: UserId = name.parse().map_err(|e| format!("{e}"))?;
        if text.is_empty() {
            return Err(format!("nothing to send to @{to}"));
        }
        return Ok(Command::Ask { to, text: text.to_owned() });
    }
    if let Some(rest) = text.strip_prefix('#') {
        let (name, text) = first_word(rest);
        let channel: ChannelId = name.parse().map_err(|e| format!("{e}"))?;
        if text.is_empty() {
            return Err(format!("nothing to post to #{channel}"));
        }
        return Ok(Command::Post { channel, text: text.to_owned() });
    }
    Ok(Command::Say(text.to_owned()))
}

/// The slash command `text`, after its `/`.
fn slash(text: &str) -> Result<Slash, String> {
    let (name, args) = first_word(text);
    let name = name.to_ascii_lowercase();
    let usage = || {
        let args = COMMANDS.iter().find(|row| row.name == name).map_or("", |row| row.args);
        format!("usage: /{name} {args}").trim_end().to_owned()
    };
    let bare = |command: Slash| if args.is_empty() { Ok(command) } else { Err(usage()) };
    let text = || if args.is_empty() { Err(usage()) } else { Ok(args.to_owned()) };
    let optional = || (!args.is_empty()).then(|| args.to_owned());
    match name.as_str() {
        "approve" => {
            let (mut id, mut always) = (None, false);
            for word in args.split_whitespace() {
                match word {
                    "--always" if !always => always = true,
                    word if id.is_none() && !word.starts_with('-') => id = Some(word.to_owned()),
                    _ => return Err(usage()),
                }
            }
            Ok(Slash::Approve { id, always })
        }
        "reject" if args.split_whitespace().count() <= 1 && !args.starts_with('-') => {
            Ok(Slash::Reject { id: optional() })
        }
        "grant" => Ok(Slash::Grant(optional())),
        "cancel" => bare(Slash::Cancel),
        "answer" => text().map(Slash::Answer),
        "done" => bare(Slash::Done(true)),
        "undone" => bare(Slash::Done(false)),
        "pin" => bare(Slash::Mark(Marking::Pin)),
        "unpin" => bare(Slash::Mark(Marking::Unpin)),
        "archive" => bare(Slash::Mark(Marking::Archive)),
        "unarchive" => bare(Slash::Mark(Marking::Unarchive)),
        "mute" if args.is_empty() => Ok(Slash::Mute(None)),
        "mute" => ants_proto::duration::parse(args).map(|hold| Slash::Mute(Some(hold))).map_err(|e| e.to_string()),
        "unmute" => bare(Slash::Unmute),
        "notify" => notify(args).map(Slash::Notify).map_err(|why| why.unwrap_or_else(usage)),
        "snooze" => snooze::duration(args).map(Slash::Snooze),
        "unsnooze" => bare(Slash::Unsnooze),
        "remind" => text().map(Slash::Remind),
        "unremind" => bare(Slash::Unremind),
        "reminders" => bare(Slash::Reminders),
        "schedules" => bare(Slash::Schedules),
        "title" => text().map(Slash::Title),
        "notes" => text().map(Slash::Notes),
        "react" => text()?.parse().map(Slash::React).map_err(|e| e.to_string()),
        "goto" => goto(&text()?).map(Slash::Goto),
        "search" => text().map(Slash::Search),
        "digest" => Ok(Slash::Digest(optional())),
        "tldr" => bare(Slash::Tldr),
        "tree" => bare(Slash::Tree),
        "forward" => {
            let (name, note) = first_word(args);
            let to = name.strip_prefix('@').ok_or_else(usage)?.parse().map_err(|e| format!("{e}"))?;
            Ok(Slash::Forward { to, note: note.to_owned() })
        }
        "attach" => text().map(Slash::Attach),
        "detach" => bare(Slash::Detach),
        "save" => Ok(Slash::Save(optional())),
        "add" => {
            let args: add::Args = flags("/add", args).map_err(|why| why.unwrap_or_else(usage))?;
            let settings = add::settings(&args.name, &args.options).map_err(|e| format!("{e:#}"))?;
            Ok(Slash::Add(AddAnt { name: args.name, machine: args.on, settings, wait: args.wait }))
        }
        "clone" => {
            let args: clone::Args = flags("/clone", args).map_err(|why| why.unwrap_or_else(usage))?;
            let settings = add::settings(&args.new, &args.options).map_err(|e| format!("{e:#}"))?;
            Ok(Slash::Clone(CloneAnt {
                src: args.src,
                new: args.new,
                machine: args.machine,
                settings,
                wait: args.wait,
            }))
        }
        "retire" => {
            let ant = text()?;
            ant.strip_prefix('@').unwrap_or(&ant).parse().map(Slash::Retire).map_err(|e| format!("{e}"))
        }
        "dead" => dead(args).map(Slash::Dead).map_err(|why| why.unwrap_or_else(usage)),
        "trail" => trail::read(args).map(Slash::Trail).map_err(|why| why.unwrap_or_else(usage)),
        "quit" => bare(Slash::Quit),
        _ if COMMANDS.iter().any(|row| row.name == name) => Err(usage()),
        _ => Err(format!("no command /{name}: the palette lists them")),
    }
}

/// What `/notify`'s `args` ask, or why not: None for its usage.
fn notify(args: &str) -> Result<Notify, Option<String>> {
    match first_word(args) {
        ("", _) | ("snooze", "") => Err(None),
        ("snooze", hold) => ants_proto::duration::parse(hold)
            .map(|hold| Notify::Push(notify::Job::Snooze(hold)))
            .map_err(|e| Some(e.to_string())),
        ("resume", "") => Ok(Notify::Push(notify::Job::Resume)),
        ("status", "") => Ok(Notify::Push(notify::Job::Status)),
        ("resume" | "status", _) => Err(None),
        _ => args.parse().map(Notify::Level).map_err(Some),
    }
}

/// What `/dead`'s `args` ask, or why not: None for its usage.
fn dead(args: &str) -> Result<Dead, Option<String>> {
    let (verb, seq) = first_word(args);
    let dead = match verb {
        "replay" => Dead::Replay,
        "drop" => Dead::Drop,
        _ => return Err(None),
    };
    match seq {
        "" => Ok(dead(None)),
        seq => seq.parse().map(|seq| dead(Some(seq))).map_err(|_| Some(format!("SEQ not a number: {seq}"))),
    }
}

/// `args` read as the command line reads the arguments `T` of its command: words as [`quoted_words`] splits them,
/// those before the first flag without a leading `@`. Or why not: clap's error in a line, None when none was given.
fn flags<T: clap::Args>(command: &'static str, args: &str) -> Result<T, Option<String>> {
    let mut words = quoted_words(args);
    if words.is_empty() {
        return Err(None);
    }
    for word in words.iter_mut().take_while(|word| !word.starts_with('-')) {
        if let Some(ant) = word.strip_prefix('@') {
            *word = ant.to_owned();
        }
    }
    let command = clap::Command::new(command).no_binary_name(true).disable_help_flag(true);
    let told = |e: clap::Error| {
        let text = e.to_string();
        let first = text.split("\n\n").next().unwrap_or_default();
        Some(first.trim_start_matches("error: ").split_whitespace().collect::<Vec<_>>().join(" "))
    };
    let matches = T::augment_args(command).try_get_matches_from(words).map_err(told)?;
    T::from_arg_matches(&matches).map_err(told)
}

fn goto(to: &str) -> Result<Goto, String> {
    let error = |e: ants_proto::Error| format!("{e}");
    if let Some(channel) = to.strip_prefix('#') {
        return channel.parse().map(Goto::Channel).map_err(error);
    }
    if let Some(user) = to.strip_prefix('@') {
        return user.parse().map(Goto::User).map_err(error);
    }
    let thread = to.to_ascii_lowercase();
    thread.parse::<ThreadId>().map_err(error)?;
    Ok(Goto::Thread(thread))
}

#[cfg(test)]
pub(super) mod tests {
    use ants_proto::Machine;
    use serde_json::{Value, json};

    use super::*;
    use crate::test_support::live::user;

    #[test]
    fn text_asks_a_user_posts_to_a_channel_or_is_said_on_the_thread() {
        let ask = parse(" @dev-a check\nthe build ").unwrap();
        assert_eq!(ask, Command::Ask { to: user("dev-a"), text: "check\nthe build".into() });
        assert_eq!(
            parse("#ops deploy?").unwrap(),
            Command::Post { channel: "ops".parse().unwrap(), text: "deploy?".into() }
        );
        assert_eq!(parse("looks good").unwrap(), Command::Say("looks good".into()));
        assert_eq!(parse("//not a command").unwrap(), Command::Say("/not a command".into()));
        assert_eq!(parse("@dev-a").unwrap_err(), "nothing to send to @dev-a");
        assert_eq!(parse("#ops  ").unwrap_err(), "nothing to post to #ops");
        assert!(parse("@Dev A hi").is_err());
        assert_eq!(parse("/later  then the docs ").unwrap(), Command::Later("then the docs".into()));
        assert_eq!(parse("/later").unwrap_err(), "usage: /later TEXT");
    }

    #[test]
    fn quoted_words_keep_a_quoted_run_together() {
        assert_eq!(
            quoted_words(" 2  1,3 'after lunch' \"it's\" don't 'open"),
            ["2", "1,3", "after lunch", "it's", "don't", "'open"]
        );
    }

    #[test]
    fn slash_commands_take_their_arguments_and_refuse_others() {
        let ok = |text: &str| match parse(text) {
            Ok(Command::Slash(slash)) => slash,
            other => panic!("{text}: {other:?}"),
        };
        assert_eq!(ok("/approve"), Slash::Approve { id: None, always: false });
        assert_eq!(ok("/APPROVE dev-a/019a --always"), Slash::Approve { id: Some("dev-a/019a".into()), always: true });
        assert_eq!(ok("/reject 019a"), Slash::Reject { id: Some("019a".into()) });
        assert_eq!(ok("/grant"), Slash::Grant(None));
        assert_eq!(ok("/answer ship it"), Slash::Answer("ship it".into()));
        assert_eq!(ok("/mute 2h"), Slash::Mute(Some(Duration::from_hours(2))));
        assert_eq!(ok("/notify mentions"), Slash::Notify(Notify::Level(ChannelNotify::Mentions)));
        assert_eq!(ok("/notify snooze 90m"), Slash::Notify(Notify::Push(notify::Job::Snooze(Duration::from_mins(90)))));
        assert_eq!(ok("/NOTIFY resume"), Slash::Notify(Notify::Push(notify::Job::Resume)));
        assert_eq!(ok("/notify status"), Slash::Notify(Notify::Push(notify::Job::Status)));
        let usage = "usage: /notify all|humans|mentions | snooze DURATION | resume | status";
        for refused in ["/notify", "/notify snooze", "/notify resume now", "/notify status --json"] {
            assert_eq!(parse(refused).unwrap_err(), usage, "{refused}");
        }
        assert!(parse("/notify snooze soon").is_err());
        assert!(parse("/notify some").unwrap_err().starts_with("unknown notify level"));
        assert_eq!(ok("/react eyes"), Slash::React("eyes".parse().unwrap()));
        assert_eq!(ok("/notes split  the work"), Slash::Notes("split  the work".into()));
        assert_eq!(parse("/notes").unwrap_err(), "usage: /notes TEXT");
        assert_eq!(ok("/goto #ops"), Slash::Goto(Goto::Channel("ops".parse().unwrap())));
        assert_eq!(ok("/goto @bob"), Slash::Goto(Goto::User(user("bob"))));
        assert_eq!(ok("/retire @dev-a"), Slash::Retire(user("dev-a")));
        assert_eq!(ok("/goto 019A"), Slash::Goto(Goto::Thread("019a".into())));
        assert_eq!(ok("/archive"), Slash::Mark(Marking::Archive));
        assert_eq!(ok("/snooze"), Slash::Snooze(TimeDelta::hours(1)));
        assert_eq!(ok("/snooze 2d"), Slash::Snooze(TimeDelta::days(2)));
        assert_eq!(ok("/unsnooze"), Slash::Unsnooze);
        assert_eq!(ok("/remind 2h look again"), Slash::Remind("2h look again".into()));
        assert_eq!(ok("/unremind"), Slash::Unremind);
        assert_eq!(ok("/reminders"), Slash::Reminders);
        assert_eq!(parse("/reminders soon").unwrap_err(), "usage: /reminders");
        assert_eq!(ok("/schedules"), Slash::Schedules);
        assert_eq!(parse("/remind").unwrap_err(), "usage: /remind DURATION|TIME [NOTE]");
        assert_eq!(ok("/forward @dev-b"), Slash::Forward { to: user("dev-b"), note: String::new() });
        assert_eq!(ok("/forward @dev-b take it"), Slash::Forward { to: user("dev-b"), note: "take it".into() });
        assert_eq!(parse("/forward dev-b").unwrap_err(), "usage: /forward @ANT [TEXT]");
        assert_eq!(parse("/snooze 8d").unwrap_err(), "too long: at most 7d");
        assert!(parse("/steer left").unwrap_err().starts_with("no command"));
        assert_eq!(parse("/done now").unwrap_err(), "usage: /done");
        assert_eq!(parse("/approve a b").unwrap_err(), "usage: /approve [ID] [--always]");
        assert_eq!(parse("/frobnicate").unwrap_err(), "no command /frobnicate: the palette lists them");
        assert!(parse("/mute forever").is_err());
        assert!(parse("/react Thumbs Up").is_err());
    }

    #[test]
    fn dead_replays_or_drops_a_dead_letter() {
        let dead = |text: &str| match parse(text) {
            Ok(Command::Slash(Slash::Dead(dead))) => dead,
            other => panic!("{text}: {other:?}"),
        };
        assert_eq!(dead("/dead replay"), Dead::Replay(None));
        assert_eq!(dead("/DEAD drop  12"), Dead::Drop(Some(12)));
        let usage = "usage: /dead replay [SEQ] | drop [SEQ]";
        for refused in ["/dead", "/dead list", "/dead 12"] {
            assert_eq!(parse(refused).unwrap_err(), usage, "{refused}");
        }
        assert_eq!(parse("/dead drop 12 13").unwrap_err(), "SEQ not a number: 12 13");
        assert_eq!(parse("/dead replay -1").unwrap_err(), "SEQ not a number: -1");
    }

    #[test]
    fn add_and_clone_take_the_arguments_of_ants_ant_add_and_clone() {
        let ok = |text: &str| match parse(text) {
            Ok(Command::Slash(slash)) => slash,
            other => panic!("{text}: {other:?}"),
        };
        let settings = |value: Value| match value {
            Value::Object(settings) => settings,
            other => panic!("{other}"),
        };
        let m1: Machine = "m1".parse().unwrap();
        let add = ok("/add @dev-c --on m1 --model gpt-5 --whoami 'Reviews PRs' --capability rust,docs --wait 10m");
        let expected = json!({ "model": "gpt-5", "whoami": "Reviews PRs", "capabilities": ["rust", "docs"] });
        let wait = Duration::from_mins(10);
        assert_eq!(
            add,
            Slash::Add(AddAnt { name: user("dev-c"), machine: m1.clone(), settings: settings(expected), wait })
        );
        let clone = ok("/CLONE dev-a @dev-c --machine m1 --home /srv/dev-c");
        let (src, new, machine) = (user("dev-a"), user("dev-c"), Some(m1));
        let settings = settings(json!({ "home": "/srv/dev-c" }));
        assert_eq!(clone, Slash::Clone(CloneAnt { src, new, machine, settings, wait: Duration::from_mins(40) }));
        let Slash::Add(AddAnt { wait, .. }) = ok("/add dev-c --on m1") else { panic!("an add") };
        assert_eq!(wait, Duration::from_mins(3), "the wait of ants ant add");

        assert_eq!(parse("/add").unwrap_err(), "usage: /add NAME --on MACHINE [FLAGS]");
        assert_eq!(parse("/clone").unwrap_err(), "usage: /clone ANT NEW [FLAGS]");
        assert_eq!(
            parse("/add dev-c").unwrap_err(),
            "the following required arguments were not provided: --on <MACHINE>"
        );
        assert_eq!(parse("/clone dev-a").unwrap_err(), "the following required arguments were not provided: <NEW>");
        let unknown = parse("/add dev-c --on m1 --colour red").unwrap_err();
        assert!(unknown.starts_with("unexpected argument '--colour' found"), "{unknown}");
        assert!(!unknown.contains('\n'), "one line: {unknown}");
        assert!(parse("/add dev-c --on m1 --help").unwrap_err().starts_with("unexpected argument '--help'"));
        assert!(parse("/clone dev-a 'dev c'").unwrap_err().starts_with("invalid value 'dev c' for '<NEW>'"));
    }

    /// A sample of an argument [`COMMANDS`] names; a new placeholder needs one here.
    fn sample(arg: &str) -> &str {
        match arg {
            "THREAD" | "RUN" => "019a",
            "ID" => "k3f7q2ab",
            "KEY" => "build",
            "SEQ" => "12",
            "TEXT" | "NOTE" | "QUERY" => "ship it",
            "N" => "2",
            "PICK..." => "2 'after lunch'",
            "DURATION" | "DUR" => "2h",
            "TIME" => "17:00",
            "SINCE" => "1d",
            "NAME" => "eyes",
            "#channel" => "#ops",
            "@user" | "@ANT" => "@dev-b",
            "ANT" => "dev-a",
            "NEW" => "dev-c",
            "MACHINE" => "m1",
            "FLAGS" => "--model gpt-5 --allow 'execute:cargo test*'",
            "PATH" => "notes.txt",
            "ARGS" => r#"{"files": ["a.rs"]}"#,
            literal if literal.starts_with("--") || literal.chars().all(|c| c.is_ascii_lowercase()) => literal,
            other => panic!("no sample for {other}"),
        }
    }

    /// The arguments of `form`: its words, those of a bracketed run, as `[--steps N]`, one.
    fn arguments(form: &str) -> Vec<String> {
        let mut args: Vec<String> = Vec::new();
        let mut open = false;
        for word in form.split_whitespace() {
            match args.last_mut() {
                Some(last) if open => *last = format!("{last} {word}"),
                _ => args.push(word.to_owned()),
            }
            open = (open || word.starts_with('[')) && !word.ends_with(']');
        }
        args
    }

    /// The command `name` typed in its `form` with a sample of each argument it requires and no other.
    pub(in crate::tui::command) fn bare(name: &str, form: &str) -> String {
        let required = arguments(form).into_iter().filter(|arg| !arg.starts_with('['));
        let samples = required.map(|arg| {
            let first = arg.split('|').next().unwrap_or_default();
            format!(" {}", first.split_whitespace().map(sample).collect::<Vec<_>>().join(" "))
        });
        format!("/{name}{}", samples.collect::<String>())
    }

    #[test]
    fn every_listed_command_parses_with_its_arguments() {
        for (name, form) in COMMANDS.iter().flat_map(|row| row.args.split(" | ").map(move |f| (row.name, f))) {
            let mut lines = vec![format!("/{name}")];
            for arg in arguments(form) {
                let (alternatives, optional) = match arg.strip_prefix('[').and_then(|a| a.strip_suffix(']')) {
                    Some(arg) => (arg, true),
                    None => (arg.as_str(), false),
                };
                let samples: Vec<String> = alternatives
                    .split('|')
                    .map(|words| words.split_whitespace().map(sample).collect::<Vec<_>>().join(" "))
                    .collect();
                lines = lines
                    .iter()
                    .flat_map(|line| {
                        let given = samples.iter().map(move |sample| format!("{line} {sample}"));
                        optional.then(|| line.clone()).into_iter().chain(given)
                    })
                    .collect();
            }
            for line in lines {
                assert!(parse(&line).is_ok(), "{line}: {:?}", parse(&line));
            }
        }
    }
}
