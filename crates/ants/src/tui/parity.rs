//! Parity of ants chat with the command line and the MCP tools. [`CLI`] maps each command of `ants` ([`Cli`]), and
//! [`MCP`] each MCP tool of ants' own ([`ants_daemon::mcp::own_names`]), to where ants chat does or shows the same,
//! or to why it has none; the tests fail on a command or tool without a row, on a row naming none, and on a slash
//! command or key ants chat lacks. As every key a view takes has a row in the key table ([`super::keymap`]), a command
//! or tool added comes with its place in ants chat, the reason it has none, or the bd issue of the gap.
//!
//! A tool is mapped to where a human does or sees in ants chat what it does for an ant: `ask_human` to answering
//! questions. The tools of an ant's provider are left out: sense asks nothing of humans (requirement ants-x39hk.1).

use clap::CommandFactory as _;

use super::command::COMMANDS;
use super::keymap::{self, View};
use super::model::Key;
use crate::cli::Cli;

use Chat::{Exempt, Missing, Press, Shown, Slash};

/// Where ants chat does or shows what a command or tool does, or why it does not.
#[derive(Debug, Clone, Copy)]
enum Chat {
    /// Slash commands of [`COMMANDS`], without their `/`, separated by spaces.
    Slash(&'static str),
    /// A key of a view: the row of [`keymap::entries`] taking it, by the row's short text.
    Press(View, Key, &'static str),
    /// The view, pane or place doing or showing it, in words.
    Shown(&'static str),
    /// Why ants chat has none.
    Exempt(&'static str),
    /// A gap to close: the bd issue tracking it.
    Missing(&'static str),
}

const SERVICE: &str = "a service, run on its own";
const CREDENTIALS: &str = "the credentials files and keys of the machine running it";
const MACHINE: &str = "sets up or checks the machine running it";
const MOVING: &str = "moves an ant between machines: files, config file and provider data of the machine running it";
const ASSISTANT: &str = "ants-waemb";
const SEND: &str = "the command bar: `@user TEXT` starts a thread, a text alone replies on the open one";
const POST: &str = "the command bar: `#channel TEXT` posts to a channel";

/// Each command of `ants` by its words (`trail run`), or the commands starting with the same words (`auth`), and where
/// ants chat has it: a command takes the row of the most words it starts with.
const CLI: &[(&str, Chat)] = &[
    ("ask", Shown("the command bar: `@user TEXT`, a text alone going to the assistant or the messenger; /forward")),
    ("inbox", Shown("home: the replies not read yet, read once their thread shows; `u` marks them unread")),
    ("colony", Shown("the nav's ants with their states, and the ant view of each")),
    ("ant show", Shown("the ant view, which an ant of the nav opens")),
    ("ant orders", Shown("the ant view's settings pane, listing the latest admin orders to the ant")),
    ("ant add", Slash("add")),
    ("ant clone", Slash("clone")),
    ("ant retire", Slash("retire")),
    ("ant set", Press(View::Settings, Key::Enter, "change")),
    ("ant avatar", Shown("the ant view's avatar head; its seed and colour change from the command line")),
    ("ant sense-id", Exempt("settles the sense id of an ant's home, on the machine whose config file lists the ant")),
    ("ant memories", Shown("the ant view's memories pane")),
    ("ant mcp", Shown("the ant view's MCP pane, where `s` signs in to a server")),
    ("ant restart", Press(View::Ant, Key::Char('r'), "restart")),
    ("ant pause", Press(View::Ant, Key::Char('p'), "(un)pause")),
    ("ant resume", Press(View::Ant, Key::Char('p'), "(un)pause")),
    ("ant stop-trail", Shown("/trail stop RUN, by request while the conductor's daemon is up rather than by order")),
    (
        "ant resume-trail",
        Shown("/trail resume RUN, by request while the conductor's daemon is up rather than by order"),
    ),
    ("ant export", Exempt(MOVING)),
    ("ant import", Exempt(MOVING)),
    ("ant unseal", Exempt(MOVING)),
    ("machines", Exempt("an inventory of the machines' daemons for operators; the ant view names an ant's machine")),
    ("spend", Shown("home's summary line and the ant view that UTC day; work's SPEND and the thread's side panel")),
    ("post", Shown("the command bar: `#channel TEXT`, or a text alone in a channel's view")),
    ("channel create", Missing("ants-njfyx")),
    ("channel set", Press(View::Channel, Key::Char('M'), "members")),
    ("channel add", Press(View::Channel, Key::Char('M'), "members")),
    ("channel remove", Press(View::Channel, Key::Char('M'), "members")),
    ("channel list", Shown("the nav's channels, with their unread posts and @mentions")),
    ("channel notify", Slash("notify")),
    ("channel read", Shown("a channel's view, which a channel of the nav opens, marking its posts read")),
    ("channel delete", Missing("ants-njfyx")),
    ("channel serve", Exempt(SERVICE)),
    ("notes", Slash("notes")),
    ("react", Slash("react")),
    ("answer", Slash("answer")),
    ("thread list", Shown("work: the threads by state, which the nav's work opens")),
    ("thread show", Shown("the thread view")),
    ("thread search", Slash("search")),
    ("thread title", Slash("title")),
    ("thread tldr", Slash("tldr")),
    ("thread pin", Slash("pin")),
    ("thread unpin", Slash("unpin")),
    ("thread archive", Slash("archive")),
    ("thread unarchive", Slash("unarchive")),
    ("thread done", Slash("done")),
    ("thread undone", Slash("undone")),
    ("thread mute", Slash("mute")),
    ("thread unmute", Slash("unmute")),
    ("trail save", Exempt("saves a trail from a .py file of the machine running it")),
    ("trail rm", Exempt("removes a saved trail, as the command line saves them")),
    ("trail list", Slash("trail")),
    ("trail show", Slash("trail")),
    ("trail tips", Slash("trail")),
    ("trail run", Slash("trail")),
    ("trail runs", Slash("trail")),
    ("trail stop", Slash("trail")),
    ("trail resume", Slash("trail")),
    ("digest", Slash("digest")),
    ("attachment get", Slash("save")),
    ("notify", Slash("notify")),
    ("remind set", Slash("remind")),
    ("remind list", Slash("reminders")),
    ("remind clear", Slash("unremind")),
    ("assistant enable", Missing("ants-waemb")),
    ("assistant disable", Missing("ants-waemb")),
    ("assistant show", Missing("ants-waemb")),
    ("assistant set", Missing(ASSISTANT)),
    ("assistant clear", Missing(ASSISTANT)),
    ("assistant allow", Missing(ASSISTANT)),
    ("assistant confirm", Press(View::Held, Key::Char('a'), "confirm")),
    ("assistant seal", Exempt("the assistant's enrollment as a device of the human, with a key of this machine")),
    ("assistant key", Exempt("prints or makes the encryption key of an ant of this machine's config file")),
    ("watch", Shown("the ant view's now column, as `ants watch ANT` follows an ant, and a thread's live tool calls")),
    ("chat", Exempt("ants chat itself")),
    ("status", Shown("home's summary line and the nav's counts: what waits, the ants busy, the spend that UTC day")),
    ("dash", Exempt("another terminal UI, whose threads, progress, ants and tool requests ants chat shows too")),
    ("cancel", Slash("cancel")),
    ("queue", Shown("the ant view's queue pane")),
    ("withdraw", Press(View::Thread, Key::Char('W'), "withdraw")),
    ("init", Exempt("creates the hub's stream and buckets, as any connection with a human's credentials does")),
    ("tools list", Shown("home's tool request cards; on a thread, Ctrl-L lists those waiting there")),
    ("tools show", Press(View::Home, Key::Enter, "open")),
    ("tools approve", Slash("approve")),
    ("tools reject", Slash("reject")),
    ("dead list", Shown("the thread view's `gave up` lines and home's stuck threads")),
    ("dead replay", Slash("dead")),
    ("dead drop", Slash("dead")),
    ("dead watch", Exempt(SERVICE)),
    ("daemon", Exempt(SERVICE)),
    ("add", Exempt("run on the ant's machine, where its config file and key stay")),
    ("retire", Slash("retire")),
    ("enroll list", Press(View::Home, Key::Char('c'), "creds")),
    ("enroll approve", Press(View::Credentials, Key::Char('a'), "approve")),
    ("enroll renewals", Press(View::Home, Key::Char('c'), "creds")),
    ("enroll renew", Press(View::Credentials, Key::Char('a'), "approve")),
    ("enroll reject", Press(View::Credentials, Key::Char('r'), "reject")),
    ("signer", Exempt(SERVICE)),
    ("login", Exempt("registers this device, before ants chat has credentials to connect with")),
    ("renew", Exempt(CREDENTIALS)),
    ("keys", Exempt(CREDENTIALS)),
    ("mcp", Exempt("the stdio bridge a daemon starts for its harness")),
    ("harness", Exempt("talks to a harness of this machine, without NATS")),
    ("doctor", Exempt(MACHINE)),
    ("setup", Exempt(MACHINE)),
    ("completions", Exempt("shell completions")),
    ("update", Exempt("replaces this executable")),
    ("auth", Exempt("issues and revokes credentials with the team's keys, on the machine holding them")),
    ("leaf", Exempt("the leaf node of the machine running it")),
    ("bridge", Exempt(SERVICE)),
    (
        "web",
        Exempt("serves the web client's page, another client of the human, or writes it for a hub's reverse proxy"),
    ),
    ("webhook", Exempt("the webhooks service, its push sinks and webhooks, where its config file and credentials are")),
    ("hook", Exempt(SERVICE)),
];

/// Each MCP tool of ants' own, and where a human does or sees in ants chat what it does for an ant.
const MCP: &[(&str, Chat)] = &[
    ("send_message", Shown(SEND)),
    ("handoff", Exempt("an ant's own turn: it hands work to itself on another thread")),
    ("ask_human", Slash("answer")),
    ("ask_ant", Shown(SEND)),
    ("check_inbox", Shown("home: the replies not read yet")),
    ("list_ants", Shown("the nav's ants")),
    ("route", Exempt("finds an ant by colony capability; a human picks one among the nav's ants, which show theirs")),
    ("react", Slash("react")),
    ("list_reactions", Shown("the reactions under each message of the thread view")),
    ("suggest_thread_title", Slash("title")),
    ("complete", Exempt("an ant's own turn: it ends without a reply")),
    ("attach_to_reply", Slash("attach")),
    ("post_notes", Slash("notes")),
    ("read_notes", Shown("the thread view, the notes among the messages")),
    ("read_attachment", Press(View::Thread, Key::Char('o'), "open file")),
    ("post_channel", Shown(POST)),
    ("manage_channel", Press(View::Channel, Key::Char('M'), "members")),
    ("list_teams", Missing("ants-n0szm.5")),
    ("manage_team", Missing("ants-n0szm.5")),
    ("escalate", Press(View::Home, Key::Char('g'), "grant")),
    ("decide_approval", Slash("approve reject")),
    ("request_ant", Slash("add")),
    ("retire_ant", Slash("retire")),
    ("run_trail", Slash("trail")),
    ("trail_status", Shown("/trail runs, and the steps a run's thread lists")),
    ("stop_trail", Slash("trail")),
    ("leave_tip", Shown("a trail's tips, beside its runs: /trail tips NAME")),
    ("vote_tip", Shown("the votes of a trail's tips, beside its runs: /trail tips NAME")),
    ("trail_tips", Slash("trail")),
    ("save_trail", Shown("who saved a trail, and from which run: /trail list, /trail show NAME")),
    ("remove_trail", Shown("the saved trails: /trail list")),
    ("rate_trail", Shown("a trail's ratings, beside its runs: /trail show NAME, /trail runs")),
    ("find_trails", Shown("the trails with their ratings: /trail list")),
    ("human_queue", Shown("home: what waits on the human")),
    ("answer_for_human", Slash("answer")),
    ("send_for_human", Shown(SEND)),
    ("post_for_human", Shown(POST)),
    ("notes_for_human", Slash("notes")),
    ("react_for_human", Slash("react")),
    ("decide_for_human", Slash("approve reject")),
    ("queue_for_human", Shown("the ant view's queue pane")),
    ("cancel_for_human", Slash("cancel")),
    ("withdraw_for_human", Press(View::Thread, Key::Char('W'), "withdraw")),
    ("order_for_human", Shown("the ant view: `r` restarts, `p` (un)pauses; its memories and settings panes")),
    ("describe_channel_for_human", Press(View::Channel, Key::Char('M'), "members")),
    ("title_for_human", Slash("title")),
    ("done_for_human", Slash("done")),
    ("mark_for_human", Slash("pin unpin archive unarchive")),
    ("snooze_for_human", Slash("snooze")),
];

/// The visible commands of `ants` that take no subcommand, by their words after `ants`.
fn commands() -> Vec<String> {
    fn walk(command: &clap::Command, words: &mut Vec<String>, found: &mut Vec<String>) {
        for sub in command.get_subcommands().filter(|c| !c.is_hide_set() && c.get_name() != "help") {
            words.push(sub.get_name().to_owned());
            if sub.has_subcommands() {
                walk(sub, words, found);
            } else {
                found.push(words.join(" "));
            }
            words.pop();
        }
    }
    let mut found = Vec::new();
    walk(&Cli::command(), &mut Vec::new(), &mut found);
    found
}

/// The row of [`CLI`] `command` takes: that of the most words it starts with.
fn row(command: &str) -> Option<&'static str> {
    let starts = |words: &str| command.strip_prefix(words).is_some_and(|rest| rest.is_empty() || rest.starts_with(' '));
    CLI.iter().map(|(words, _)| *words).filter(|words| starts(words)).max_by_key(|words| words.len())
}

/// Fail listing the `problems` found, if any, with how to write a row.
fn fail_on(problems: &[String]) {
    assert!(
        problems.is_empty(),
        "ants chat parity (crates/ants/src/tui/parity.rs):\n{}\n\nA row says where ants chat has it: Slash(\"name\") \
         for slash commands of COMMANDS (crates/ants/src/tui/command/parse.rs), Press(View, Key, \"short\") for a key \
         of the key table (crates/ants/src/tui/keymap/table.rs), Shown(\"…\") naming the view or pane, Exempt(\"why ants \
         chat has none\"), or Missing(\"ants-…\") naming the bd issue of the gap.",
        problems.join("\n")
    );
}

#[test]
fn every_command_has_a_row() {
    let commands = commands();
    assert!(commands.iter().any(|c| c == "trail run"), "the commands are read: {commands:?}");
    let problems: Vec<_> = commands
        .iter()
        .filter(|c| row(c).is_none())
        .map(|c| format!("no row in CLI for `ants {c}`: add (\"{c}\", …), or a row for the words it starts with"))
        .collect();
    fail_on(&problems);
}

#[test]
fn every_row_is_taken_by_a_command() {
    let commands = commands();
    let mut problems = Vec::new();
    for (at, (words, _)) in CLI.iter().enumerate() {
        if CLI[..at].iter().any(|(earlier, _)| earlier == words) {
            problems.push(format!("two rows for \"{words}\" in CLI: remove one"));
        } else if !commands.iter().any(|c| row(c) == Some(*words)) {
            problems.push(format!(
                "the row \"{words}\" of CLI names no command of `ants`, nor words starting one that no longer row \
                 takes: remove it, or rename it after the command"
            ));
        }
    }
    fail_on(&problems);
}

#[test]
fn every_tool_has_a_row_and_every_row_a_tool() {
    let tools = ants_daemon::mcp::own_names();
    let mut missing: Vec<_> = tools.iter().filter(|t| !MCP.iter().any(|(name, _)| name == t)).collect();
    missing.sort();
    let mut problems: Vec<_> = missing
        .into_iter()
        .map(|t| {
            format!(
                "no row in MCP for the tool `{t}`: add (\"{t}\", …), naming where a human does or sees in ants chat \
                 what it does for an ant"
            )
        })
        .collect();
    for (at, (name, _)) in MCP.iter().enumerate() {
        if MCP[..at].iter().any(|(earlier, _)| earlier == name) {
            problems.push(format!("two rows for \"{name}\" in MCP: remove one"));
        } else if !tools.contains(*name) {
            problems.push(format!(
                "the row \"{name}\" of MCP names no tool of ants_daemon::mcp::own_names: remove it, or rename it \
                 after the tool"
            ));
        }
    }
    fail_on(&problems);
}

#[test]
fn every_place_named_is_in_ants_chat() {
    let rows = CLI.iter().map(|(words, chat)| ("CLI", words, chat)).chain(MCP.iter().map(|(n, c)| ("MCP", n, c)));
    let mut problems = Vec::new();
    for (table, name, chat) in rows {
        let at = format!("the row \"{name}\" of {table}");
        match *chat {
            Slash(names) if names.trim().is_empty() => problems.push(format!("{at} names no slash command")),
            Slash(names) => problems.extend(
                names
                    .split_whitespace()
                    .filter(|slash| !COMMANDS.iter().any(|row| row.name == *slash))
                    .map(|slash| format!("{at}: no /{slash} in COMMANDS: name a command listed there")),
            ),
            Press(view, key, short) => {
                if !keymap::entries(view).iter().any(|e| e.keys.contains(&key) && e.short == short) {
                    problems.push(format!(
                        "{at}: no row of View::{view:?} in the key table takes {key:?} with the short text \
                         \"{short}\": name the key and short text of a row there"
                    ));
                }
            }
            Shown(text) | Exempt(text) if text.trim().is_empty() => problems.push(format!("{at} says nothing")),
            Shown(_) | Exempt(_) => {}
            Missing(gap) => {
                if !gap.strip_prefix("ants-").is_some_and(|id| !id.is_empty() && !id.contains(' ')) {
                    problems.push(format!("{at}: Missing names the bd issue of the gap (\"ants-…\")"));
                }
            }
        }
    }
    fail_on(&problems);
}
