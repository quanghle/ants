//! What the command bar's text says to do: a message to start a thread with a user, a post to a channel, text to
//! say on the open thread, or a slash command ([`COMMANDS`]).

use std::time::Duration;

use ants_proto::{ChannelId, ChannelNotify, Choice, Reaction, ThreadId, UserId};
use chrono::TimeDelta;

use super::snooze;
use crate::thread_meta::Marking;

/// The slash commands: name, arguments and what each does, as the list shows them.
pub const COMMANDS: &[(&str, &str, &str)] = &[
    ("approve", "[ID] [--always]", "allow a tool request, once or for the session"),
    ("reject", "[ID]", "refuse a tool request"),
    ("grant", "[TEXT]", "fresh turns for the stuck ant"),
    ("cancel", "", "stop the prompt an ant runs"),
    ("later", "TEXT", "reply without interrupting the ant at work"),
    ("answer", "N|TEXT", "answer the question asked"),
    ("done", "", "mark the thread done"),
    ("undone", "", "mark the thread not done"),
    ("pin", "", "pin the thread"),
    ("unpin", "", "unpin the thread"),
    ("mute", "[DURATION]", "hold back the thread's notifications"),
    ("unmute", "", "notify of the thread again"),
    (
        "notify",
        "all|humans|mentions",
        "which unread posts of the channel count: all, by humans and @mentions, @mentions",
    ),
    ("snooze", "[DURATION]", "keep the thread off home, 1h unless given"),
    ("unsnooze", "", "bring the thread back on home"),
    ("remind", "DURATION|TIME [NOTE]", "remind of the thread (or selected message) on home and by push then"),
    ("unremind", "", "clear the thread's reminder"),
    ("archive", "", "archive the thread"),
    ("unarchive", "", "unarchive the thread"),
    ("title", "TEXT", "title the thread"),
    ("react", "NAME", "react to the latest message"),
    ("goto", "THREAD|#channel|@user", "show a thread, channel or user"),
    ("search", "QUERY", "search the threads"),
    ("digest", "[SINCE]", "catch-up summary"),
    ("tldr", "", "summary of the thread"),
    ("tree", "", "the thread's delegation tree"),
    ("forward", "@ANT [TEXT]", "hand the selected message, else the thread's TL;DR, to an ant on a new thread"),
    ("attach", "PATH", "attach a file to the next message"),
    ("detach", "", "drop the files attached"),
    ("save", "[PATH]", "save the marked attachment"),
    ("retire", "ANT", "retire the ant by admin order, its home kept"),
    ("quit", "", "leave ants chat"),
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
    Answer(Choice),
    Done(bool),
    Mark(Marking),
    Mute(Option<Duration>),
    Unmute,
    /// Set the notify level of the channel on view.
    Notify(ChannelNotify),
    /// Keep the thread off home this long.
    Snooze(TimeDelta),
    Unsnooze,
    /// Remind of the thread as `DURATION|TIME [NOTE]` says.
    Remind(String),
    Unremind,
    Title(String),
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
    /// Retire the ant by admin order.
    Retire(UserId),
    Quit,
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
        let args = COMMANDS.iter().find(|(n, ..)| *n == name).map_or("", |(_, args, _)| *args);
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
        "answer" => text().map(|answer| Slash::Answer(Choice::parse(&answer))),
        "done" => bare(Slash::Done(true)),
        "undone" => bare(Slash::Done(false)),
        "pin" => bare(Slash::Mark(Marking::Pin)),
        "unpin" => bare(Slash::Mark(Marking::Unpin)),
        "archive" => bare(Slash::Mark(Marking::Archive)),
        "unarchive" => bare(Slash::Mark(Marking::Unarchive)),
        "mute" if args.is_empty() => Ok(Slash::Mute(None)),
        "mute" => ants_proto::duration::parse(args).map(|hold| Slash::Mute(Some(hold))).map_err(|e| e.to_string()),
        "unmute" => bare(Slash::Unmute),
        "notify" => text()?.parse().map(Slash::Notify),
        "snooze" => snooze::duration(args).map(Slash::Snooze),
        "unsnooze" => bare(Slash::Unsnooze),
        "remind" => text().map(Slash::Remind),
        "unremind" => bare(Slash::Unremind),
        "title" => text().map(Slash::Title),
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
        "retire" => {
            let ant = text()?;
            ant.strip_prefix('@').unwrap_or(&ant).parse().map(Slash::Retire).map_err(|e| format!("{e}"))
        }
        "quit" => bare(Slash::Quit),
        _ if COMMANDS.iter().any(|(n, ..)| *n == name) => Err(usage()),
        _ => Err(format!("no command /{name}")),
    }
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
mod tests {
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
    fn slash_commands_take_their_arguments_and_refuse_others() {
        let ok = |text: &str| match parse(text) {
            Ok(Command::Slash(slash)) => slash,
            other => panic!("{text}: {other:?}"),
        };
        assert_eq!(ok("/approve"), Slash::Approve { id: None, always: false });
        assert_eq!(ok("/APPROVE dev-a/019a --always"), Slash::Approve { id: Some("dev-a/019a".into()), always: true });
        assert_eq!(ok("/reject 019a"), Slash::Reject { id: Some("019a".into()) });
        assert_eq!(ok("/grant"), Slash::Grant(None));
        assert_eq!(ok("/answer 2"), Slash::Answer(Choice::Option(2)));
        assert_eq!(ok("/answer ship it"), Slash::Answer(Choice::Typed("ship it".into())));
        assert_eq!(ok("/mute 2h"), Slash::Mute(Some(Duration::from_hours(2))));
        assert_eq!(ok("/notify mentions"), Slash::Notify(ChannelNotify::Mentions));
        assert!(parse("/notify").unwrap_err().starts_with("usage: /notify all|humans|mentions"));
        assert!(parse("/notify some").unwrap_err().starts_with("unknown notify level"));
        assert_eq!(ok("/react eyes"), Slash::React("eyes".parse().unwrap()));
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
        assert_eq!(ok("/forward @dev-b"), Slash::Forward { to: user("dev-b"), note: String::new() });
        assert_eq!(ok("/forward @dev-b take it"), Slash::Forward { to: user("dev-b"), note: "take it".into() });
        assert_eq!(parse("/forward dev-b").unwrap_err(), "usage: /forward @ANT [TEXT]");
        assert_eq!(parse("/snooze 8d").unwrap_err(), "too long: at most 7d");
        assert!(parse("/steer left").unwrap_err().starts_with("no command"));
        assert_eq!(parse("/done now").unwrap_err(), "usage: /done");
        assert_eq!(parse("/approve a b").unwrap_err(), "usage: /approve [ID] [--always]");
        assert_eq!(parse("/frobnicate").unwrap_err(), "no command /frobnicate");
        assert!(parse("/mute forever").is_err());
        assert!(parse("/react Thumbs Up").is_err());
    }

    /// A sample of an argument [`COMMANDS`] names; a new placeholder needs one here.
    fn sample(arg: &str) -> &str {
        match arg {
            "ID" | "THREAD" => "019a",
            "TEXT" | "NOTE" | "QUERY" => "ship it",
            "N" => "2",
            "DURATION" => "2h",
            "TIME" => "17:00",
            "SINCE" => "1d",
            "NAME" => "eyes",
            "#channel" => "#ops",
            "@user" | "@ANT" => "@dev-b",
            "ANT" => "dev-a",
            "PATH" => "notes.txt",
            literal if literal.starts_with("--") || literal.chars().all(|c| c.is_ascii_lowercase()) => literal,
            other => panic!("no sample for {other}"),
        }
    }

    #[test]
    fn every_listed_command_parses_with_its_arguments() {
        for (name, args, _) in COMMANDS {
            let mut lines = vec![format!("/{name}")];
            for arg in args.split_whitespace() {
                let (alternatives, optional) = match arg.strip_prefix('[').and_then(|a| a.strip_suffix(']')) {
                    Some(arg) => (arg, true),
                    None => (arg, false),
                };
                let samples: Vec<_> = alternatives.split('|').map(sample).collect();
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
