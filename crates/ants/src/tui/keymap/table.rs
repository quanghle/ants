//! The table's rows: everywhere's and each view's.

use super::Entry;
use super::HELP_ONLY;
use super::when::*;
use crate::tui::model::Key;

const UP_DOWN: &[Key] = &[Key::Up, Key::Down, Key::Char('k'), Key::Char('j')];
const PAGES: &[Key] = &[Key::PageUp, Key::PageDown];
const ENDS: &[Key] = &[Key::Home, Key::End];
const DIGITS: &[Key] = &[
    Key::Char('1'),
    Key::Char('2'),
    Key::Char('3'),
    Key::Char('4'),
    Key::Char('5'),
    Key::Char('6'),
    Key::Char('7'),
    Key::Char('8'),
    Key::Char('9'),
];

/// The rows that hold everywhere, under the views'.
pub const GLOBAL: &[Entry] = &[
    Entry::new(
        &[Key::Ctrl('z')],
        "Ctrl-Z",
        "suspend",
        "restore the terminal and suspend; fg resumes and redraws (Unix)",
        HELP_ONLY,
        always,
    ),
    Entry::new(
        &[],
        "Mouse",
        "scroll/select",
        "wheel scrolls under the pointer by line; click selects, a second click opens; Shift, or Option in iTerm2, keeps native terminal selection; [cli] mouse = false disables reporting",
        HELP_ONLY,
        always,
    ),
    Entry::new(&[Key::Esc], "Esc", "back", "back to the previous view, else to the nav", 9, not_selecting),
    Entry::new(&[Key::Tab], "Tab", "nav/view", "move between the nav and the view", 40, always),
    Entry::new(
        &[Key::Ctrl('k'), Key::Char('/')],
        "Ctrl-K /",
        "commands",
        "the palette: the /commands, the view's keys and the places (channels, ants, people, threads), narrowed as typed, those that cannot act here dim with the reason",
        45,
        always,
    ),
    Entry::new(
        &[Key::Ctrl('f')],
        "Ctrl-F",
        "search",
        "the bar on /search, as in /search deploy from:dev-a",
        50,
        no_thread_open,
    ),
    Entry::new(
        &[Key::Ctrl('t')],
        "Ctrl-T",
        "next unread",
        "the next channel, person or thread with something unread (the bar focused too)",
        55,
        always,
    ),
    Entry::new(
        &[Key::Ctrl('o')],
        "Ctrl-O",
        "nav",
        "fold or show the nav; again, as the width has it (the bar focused too)",
        60,
        always,
    ),
    Entry::new(
        &[Key::Char('@'), Key::Char('#')],
        "@ #",
        "ask/post",
        "the command bar: @ant asks an ant, #channel posts to a channel",
        70,
        on_view,
    ),
    Entry::new(
        &[Key::Char('R')],
        "R",
        "read again",
        "read everything again, on an ant its memories, MCP servers and settings too",
        80,
        always,
    ),
    Entry::new(
        &[Key::Char('q')],
        "q",
        "quit",
        "quit, the bar empty; soon after q closed something, nothing",
        90,
        bar_empty,
    ),
    Entry::new(&[Key::Char('?')], "?", "help", "this help", HELP_ONLY, always),
    Entry::new(
        &[Key::Char('N')],
        "N",
        "notices",
        "the notices of the run, the latest first, each in full with its time",
        85,
        not_finding,
    ),
    Entry::new(
        &[Key::Interrupt],
        "Ctrl-C",
        "close",
        "close what is open, else empty the bar, else quit on a second Ctrl-C within a second",
        HELP_ONLY,
        always,
    ),
];

pub(super) const NAV: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "select", "move the selection", 10, always),
    Entry::new(&[Key::Enter, Key::Esc], "Enter", "view", "focus the selected view (Esc too)", 5, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const HOME: &[Entry] = &[
    Entry::new(&[Key::Char('a')], "a", "allow", "allow the selected tool request once", 1, home_pending),
    Entry::new(&[Key::Char('A')], "A", "always", "allow the selected tool request always", 2, home_pending),
    Entry::new(&[Key::Char('r')], "r", "reject", "reject the selected tool request at once", 3, home_pending),
    Entry::new(
        &[Key::Char('n')],
        "n",
        "note",
        "reject the selected tool request with a note to the ant",
        4,
        home_pending,
    ),
    Entry::new(
        &[Key::Char('l')],
        "l",
        "allow all",
        "allow once each request of the selected group of the oldest's tool kind, once y confirms",
        4,
        home_group,
    ),
    Entry::new(&[Key::Char('y')], "y", "confirm", "confirm the allow-all l asked for", 1, home_armed),
    Entry::new(DIGITS, "1-9", "answer", "answer the selected question by number", 5, home_question),
    Entry::new(
        &[Key::Enter],
        "Enter",
        "open",
        "open the selection: a tool request in full, a question's card, a group of requests, else its thread",
        6,
        home_selected,
    ),
    Entry::new(UP_DOWN, "↑↓", "select", "move the selection", 10, always),
    Entry::new(&[Key::Char('r')], "r", "reply", "reply on the selected thread", 12, home_reply),
    Entry::new(&[Key::Char('g')], "g", "grant", "grant the stuck thread selected more turns", 13, home_stuck),
    Entry::new(
        &[Key::Char('d')],
        "d",
        "done",
        "mark the selected thread done; on a reminder, clear it",
        14,
        home_selected,
    ),
    Entry::new(&[Key::Char('z')], "z", "snooze", "snooze the selected thread", 15, home_selected),
    Entry::new(&[Key::Char('x')], "x", "cancel", "cancel the prompt running on the selected thread", 16, home_selected),
    Entry::new(&[Key::Char('f')], "f", "forward", "forward the selected thread", 17, home_selected),
    Entry::new(&[Key::Char('b')], "b", "remind", "remind of the selected thread later", 18, home_selected),
    Entry::new(&[Key::Char('u')], "u", "unread", "mark the selected thread unread", 19, home_selected),
    Entry::new(&[Key::Char('t')], "t", "tree", "the delegation tree of the selected thread", 20, home_selected),
    Entry::new(&[Key::Char('s')], "s", "digest", "the digest of what came while away", 21, always),
    Entry::new(&[Key::Char('c')], "c", "creds", "the credentials asked for", 22, always),
    Entry::new(
        &[Key::Char('o')],
        "o",
        "held",
        "the admin orders and tool decisions of the assistant held for confirmation",
        23,
        always,
    ),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const THREAD: &[Entry] = &[
    Entry::new(
        &[Key::Ctrl('l')],
        "Ctrl-L",
        "waiting",
        "list every waiting tool request and question on the open thread; Enter opens the selection",
        8,
        thread_waiting,
    ),
    Entry::new(&[Key::Char('a')], "a", "allow", "allow the oldest tool request waiting once", 1, thread_pending),
    Entry::new(&[Key::Char('A')], "A", "always", "allow the oldest tool request waiting always", 2, thread_pending),
    Entry::new(&[Key::Char('r')], "r", "reject", "reject the oldest tool request waiting at once", 3, thread_pending),
    Entry::new(
        &[Key::Char('n')],
        "n",
        "note",
        "reject the oldest tool request waiting with a note to the ant",
        4,
        thread_note,
    ),
    Entry::new(&[Key::Enter], "Enter", "details", "the oldest tool request waiting in full", 5, thread_pending_enter),
    Entry::new(DIGITS, "1-9", "answer", "answer the question waiting by number", 6, thread_question),
    Entry::new(&[Key::Enter], "Enter", "answer", "focus the card of the question waiting", 7, thread_question_enter),
    Entry::new(
        &[Key::Enter],
        "Enter",
        "step thread",
        "open the thread of the selected step of a trail run",
        4,
        thread_step,
    ),
    Entry::new(&[Key::Char('i')], "i", "write", "back to writing in the bar (End and Esc too)", 8, selecting),
    Entry::new(
        &[Key::CtrlHome, Key::CtrlEnd],
        "Ctrl-Home",
        "first/latest",
        "show the first message; Ctrl-End follows the latest, while writing too, as PgUp and PgDn scroll",
        HELP_ONLY,
        always,
    ),
    Entry::new(
        &[Key::Char('n'), Key::Char('N')],
        "n N",
        "next/previous",
        "step to the next or the previous match found",
        0,
        thread_finding,
    ),
    Entry::new(&[Key::Esc], "Esc", "end find", "stop finding, the matches no longer marked", 0, thread_finding),
    Entry::new(
        &[Key::Enter, Key::Newline, Key::Backspace],
        "Enter",
        "next match",
        "while typing the text to find: the next match (Shift-Enter the previous); Backspace takes back a character",
        HELP_ONLY,
        thread_finding,
    ),
    Entry::new(
        UP_DOWN,
        "↑↓",
        "select or scroll",
        "select a message or a trail run's step, the latest at the bottom; through a message taller than the view, scroll by line",
        10,
        always,
    ),
    Entry::new(
        &[Key::Ctrl('f')],
        "Ctrl-F",
        "find",
        "find text in the conversation: type it, Enter steps to a match, n and N then step on",
        19,
        always,
    ),
    Entry::new(&[Key::Char('v')], "v", "pager", "the selected message in the pager ($PAGER)", 30, thread_message),
    Entry::new(
        &[Key::Ctrl('\\')],
        "Ctrl-\\",
        "panel",
        "fold or show the side panel; again, as the width has it",
        60,
        always,
    ),
    Entry::new(
        &[Key::Char('>')],
        ">",
        "quote",
        "reply to the selected message, quoting it and @mentioning its author when an ant",
        11,
        thread_message,
    ),
    Entry::new(&[Key::Char('y')], "y", "copy", "copy the selected message", 12, thread_message),
    Entry::new(&[Key::Char('+')], "+", "react", "react to the selected message", 13, thread_message),
    Entry::new(
        &[Key::Char('x'), Key::Ctrl('x')],
        "x",
        "cancel",
        "cancel the prompt running on the thread (Ctrl-X too)",
        14,
        always,
    ),
    Entry::new(&[Key::Char('d')], "d", "done", "mark the thread done, again to undo", 15, always),
    Entry::new(&[Key::Char('z')], "z", "snooze", "snooze the thread", 16, always),
    Entry::new(&[Key::Char('o')], "o", "open file", "open the marked attachment", 17, thread_attachment),
    Entry::new(&[Key::Char(']')], "]", "next file", "mark the next attachment", 18, thread_attachments),
    Entry::new(&[Key::Char('p')], "p", "pin", "pin the thread, again to undo", 20, always),
    Entry::new(&[Key::Char('m')], "m", "mute", "mute the thread, again to undo", 21, always),
    Entry::new(&[Key::Char('e')], "e", "archive", "archive the thread, again to undo", 22, always),
    Entry::new(&[Key::Char('f')], "f", "forward", "forward the thread", 23, always),
    Entry::new(&[Key::Char('b')], "b", "remind", "remind of the thread, at the selected message, later", 24, always),
    Entry::new(&[Key::Char('u')], "u", "unread", "mark the thread unread from the selected message", 25, always),
    Entry::new(&[Key::Char('t')], "t", "tree", "the thread's delegation tree", 26, always),
    Entry::new(&[Key::Char('S')], "S", "TL;DR", "a TL;DR of the thread", 27, always),
    Entry::new(&[Key::Char('T')], "T", "tools", "show or hide the tool calls", 28, always),
    Entry::new(&[Key::Char('W')], "W", "withdraw", "withdraw a message or post still waiting", 29, always),
    Entry::new(&[Key::Char('E')], "E", "edit", "withdraw a message or post still waiting, to edit it", 31, always),
    Entry::new(PAGES, "PgUp PgDn", "scroll", "scroll half a view", 92, always),
    Entry::new(ENDS, "Home End", "first/latest", "select the first message, or follow the latest", 95, always),
];

pub(super) const PENDING: &[Entry] = &[
    Entry::new(
        &[Key::Enter],
        "Enter",
        "open",
        "open the selected tool request in full or focus the selected question's card",
        0,
        always,
    ),
    Entry::new(UP_DOWN, "↑↓ j k", "select", "select a waiting tool request or question", 1, always),
    Entry::new(
        &[Key::Esc, Key::Ctrl('l'), Key::Char('q')],
        "Esc Ctrl-L q",
        "close",
        "close the list and return to the thread, keeping the draft",
        2,
        always,
    ),
    Entry::new(PAGES, "PgUp PgDn", "page", "select half a view back or ahead", 90, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last waiting card", 95, always),
    Entry::new(&[Key::Char('?')], "?", "help", "the keys of this list", HELP_ONLY, always),
    Entry::new(&[Key::Ctrl('k')], "Ctrl-K", "commands", "open the palette", HELP_ONLY, always),
    Entry::new(&[Key::Tab], "Tab", "nav", "move to the nav", HELP_ONLY, always),
];

pub(super) const WORK: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "thread", "select a thread", 10, always),
    Entry::new(&[Key::Enter], "Enter", "open", "open the selected thread", 5, thread_selected),
    Entry::new(
        &[Key::Char('x'), Key::Ctrl('x')],
        "x",
        "cancel",
        "cancel the prompt running on the selected thread (Ctrl-X too)",
        14,
        thread_selected,
    ),
    Entry::new(&[Key::Char('z')], "z", "snooze", "snooze the selected thread", 15, thread_selected),
    Entry::new(&[Key::Char('S')], "S", "TL;DR", "a TL;DR of the selected thread", 16, thread_selected),
    Entry::new(&[Key::Char('u')], "u", "unread", "mark the selected thread unread", 17, thread_selected),
    Entry::new(&[Key::Char('b')], "b", "remind", "remind of the selected thread later", 18, thread_selected),
    Entry::new(&[Key::Char('f')], "f", "forward", "forward the selected thread", 19, thread_selected),
    Entry::new(PAGES, "PgUp PgDn", "page", "move the selection further", 92, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const TYPING: &[Entry] = &[
    Entry::new(
        &[],
        "a-z",
        "write",
        "a printable key is written in the bar, Enter sending it here; ? on an empty bar shows this help",
        0,
        always,
    ),
    Entry::new(
        &[Key::Up],
        "↑",
        "select",
        "select the latest message or post: the keys of the view then act on it, i, End or Esc back to writing",
        1,
        always,
    ),
];

pub(super) const CHANNEL: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "post", "select a post", 10, always),
    Entry::new(&[Key::Char('i')], "i", "write", "back to writing in the bar (End and Esc too)", 8, selecting),
    Entry::new(&[Key::Enter], "Enter", "open", "open the selected post's thread", 5, post_selected),
    Entry::new(&[Key::Left, Key::Right], "←→", "tab", "the channel's state tab", 11, always),
    Entry::new(
        &[Key::Char('n')],
        "n",
        "notify",
        "the next notify level: all posts, posts by humans and @mentions, @mentions only",
        12,
        always,
    ),
    Entry::new(&[Key::Char('M')], "M", "members", "manage the channel's members and description", 13, always),
    Entry::new(&[Key::Char('o')], "o", "open file", "open the marked attachment", 14, post_attachment),
    Entry::new(&[Key::Char(']')], "]", "next file", "mark the next attachment", 15, post_attachments),
    Entry::new(PAGES, "PgUp PgDn", "page", "move the selection further", 92, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const DM: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "thread", "select a thread", 10, always),
    Entry::new(&[Key::Enter], "Enter", "open", "open the selected thread", 5, always),
    Entry::new(&[Key::Char('n')], "n", "write", "write to the person in the command bar", 6, always),
    Entry::new(&[Key::Char('i')], "i", "write", "back to writing in the bar (End and Esc too)", 8, selecting),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const SEARCH: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "match", "select a match", 10, always),
    Entry::new(&[Key::Enter], "Enter", "open", "open the match's thread at it", 5, always),
    Entry::new(PAGES, "PgUp PgDn", "page", "move the selection further", 92, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const TREE: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "select", "select a thread of the tree", 10, always),
    Entry::new(&[Key::Enter], "Enter", "open", "open the selected thread", 5, always),
    Entry::new(&[Key::Char('x')], "x", "cancel subtree", "cancel every prompt in the selected subtree", 11, always),
    Entry::new(&[Key::Char('y')], "y", "confirm", "confirm the cancel x asked for", 1, tree_armed),
    Entry::new(PAGES, "PgUp PgDn", "page", "move the selection further", 92, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const ANT: &[Entry] = &[
    Entry::new(&[Key::Left, Key::Right], "←→", "pane", "the pane: threads, queue, memories, MCP, settings", 20, always),
    Entry::new(&[Key::Char('a')], "a", "ask", "ask the ant in the command bar", 21, always),
    Entry::new(&[Key::Char('w')], "w", "work", "the ant's threads in work", 22, always),
    Entry::new(&[Key::Char('r')], "r", "restart", "restart the ant's harnesses, once y or ! confirms", 23, always),
    Entry::new(&[Key::Char('p')], "p", "(un)pause", "pause the ant, or resume it, once y confirms", 24, always),
    Entry::new(
        &[Key::Char('y')],
        "y",
        "confirm",
        "confirm what r or p asked for: a restart after the prompts running, the pause or the resume",
        1,
        ant_armed,
    ),
    Entry::new(
        &[Key::Char('!')],
        "!",
        "restart now",
        "confirm the restart r asked for at once, cancelling the prompts running",
        2,
        ant_restart_armed,
    ),
    Entry::new(&[Key::Char('C')], "C", "clone", "clone the ant by admin order, in the command bar", 25, always),
    Entry::new(&[Key::Char('X')], "X", "retire", "retire the ant, in the command bar", 26, always),
    Entry::new(&[Key::Char('c')], "c", "creds", "the credentials asked for", 27, always),
    Entry::new(&[Key::Char('['), Key::Char(']')], "[ ]", "scroll", "scroll the about or now column", 28, always),
    Entry::new(&[Key::Char('\\')], "\\", "column", "switch between the about and now columns", 29, always),
];

pub(super) const ANT_QUEUE: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "message", "select a message waiting for the ant", 10, always),
    Entry::new(&[Key::Enter], "Enter", "open", "open the selected message's thread", 5, always),
    Entry::new(PAGES, "PgUp PgDn", "page", "move the selection further", 92, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const MEMORIES: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "scroll", "scroll the memories", 10, always),
    Entry::new(PAGES, "PgUp PgDn", "page", "scroll further", 92, always),
    Entry::new(ENDS, "Home End", "top/bottom", "scroll to the top or the bottom", 95, always),
];

pub(super) const MCP: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "server", "select a server", 10, always),
    Entry::new(&[Key::Char('s'), Key::Enter], "s", "sign in", "sign in to the selected server (Enter too)", 5, always),
    Entry::new(&[Key::Char('y')], "y", "copy address", "copy the address the sign-in gave", 3, mcp_signing),
    Entry::new(&[Key::Char('v')], "v", "hand back", "hand back the address the browser came back to", 4, mcp_signing),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const SETTINGS: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "field", "select a field", 10, always),
    Entry::new(&[Key::Enter], "Enter", "change", "change the selected field", 5, always),
    Entry::new(&[Key::Char('n')], "n", "new field", "set a field not listed", 6, always),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const CREDENTIALS: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "select", "select a request", 10, always),
    Entry::new(&[Key::Char('a')], "a", "approve", "approve or renew the selected request, once y confirms", 5, always),
    Entry::new(&[Key::Char('r')], "r", "reject", "reject the selected request, once y confirms", 6, always),
    Entry::new(&[Key::Char('y')], "y", "confirm", "confirm the order a or r asked for", 1, page_armed),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const HELD: &[Entry] = &[
    Entry::new(UP_DOWN, "↑↓", "select", "select an order or a decision", 10, always),
    Entry::new(&[Key::Char('a')], "a", "confirm", "confirm the selected order or decision, once y confirms", 5, always),
    Entry::new(&[Key::Char('r')], "r", "deny", "deny the selected order or decision, once y confirms", 6, always),
    Entry::new(&[Key::Char('y')], "y", "confirm", "give the answer a or r asked for", 1, page_armed),
    Entry::new(ENDS, "Home End", "first/last", "select the first or the last", 95, always),
];

pub(super) const BAR: &[Entry] = &[
    Entry::new(
        &[Key::Enter],
        "Enter",
        "send",
        "send the text where the bar's title says: a reply, an ask, a post, or a /command",
        1,
        always,
    ),
    Entry::new(
        &[Key::Newline],
        "Ctrl-J",
        "new line",
        "a new line (Shift-Enter, Alt-Enter, or \\ then Enter, too)",
        2,
        always,
    ),
    Entry::new(
        &[Key::Ctrl('g'), Key::Ctrl('x')],
        "Ctrl-G",
        "editor",
        "the text in $EDITOR (Ctrl-X Ctrl-E too)",
        3,
        always,
    ),
    Entry::new(
        &[Key::Esc],
        "Esc",
        "leave",
        "leave the bar, the text kept as the draft of the conversation shown; Esc again soon empties it into the history",
        4,
        always,
    ),
    Entry::new(
        &[Key::Ctrl('p'), Key::Ctrl('n')],
        "Ctrl-P Ctrl-N",
        "history",
        "the texts sent, earlier and later",
        5,
        always,
    ),
    Entry::new(
        &[Key::Ctrl('r')],
        "Ctrl-R",
        "search history",
        "search the texts sent: typing narrows it, Ctrl-R goes earlier, Enter takes the text, Esc or Ctrl-G puts back the one before",
        6,
        always,
    ),
    Entry::new(
        &[Key::Up, Key::Down, Key::Tab],
        "↑↓ Tab",
        "complete",
        "pick an @ant, #channel or /command listed and complete it (Enter too); with no list, ↑↓ move by line",
        7,
        always,
    ),
    Entry::new(&[Key::Left, Key::Right], "←→", "move", "move the cursor", 8, always),
    Entry::new(
        &[Key::Home, Key::End, Key::Ctrl('a'), Key::Ctrl('e')],
        "Ctrl-A Ctrl-E",
        "line start/end",
        "to the start or the end of the line (Home, End too)",
        9,
        always,
    ),
    Entry::new(
        &[Key::Alt('b'), Key::Alt('B'), Key::Alt('f'), Key::Alt('F'), Key::WordLeft, Key::WordRight],
        "Alt-B Alt-F",
        "word",
        "back or on a word (Ctrl-← Ctrl-→ too)",
        10,
        always,
    ),
    Entry::new(
        &[Key::Backspace, Key::Delete],
        "Backspace",
        "delete",
        "delete the character before the cursor (Delete: the one under it)",
        11,
        always,
    ),
    Entry::new(
        &[Key::Ctrl('k'), Key::Ctrl('u')],
        "Ctrl-K Ctrl-U",
        "kill line",
        "kill to the end or the start of the line",
        12,
        always,
    ),
    Entry::new(
        &[Key::Ctrl('w'), Key::AltBackspace, Key::Alt('d'), Key::Alt('D')],
        "Ctrl-W Alt-D",
        "kill word",
        "kill the word before the cursor (Alt-Backspace too) or after it",
        13,
        always,
    ),
    Entry::new(&[Key::Ctrl('y')], "Ctrl-Y", "yank", "put back the text killed last", 14, always),
];

pub(super) const START: &[Entry] = &[
    Entry::new(&[Key::Char('y')], "y", "start", "start the run its summary shows, then show its thread", 1, always),
    Entry::new(&[Key::Esc], "Esc", "not started", "leave the run unstarted, as any key but y and ? does", 2, always),
    Entry::new(&[Key::Char('?')], "?", "help", "the keys of the run to start", HELP_ONLY, always),
];

pub(super) const REQUEST: &[Entry] = &[
    Entry::new(&[Key::Char('a')], "a", "allow", "allow the tool request once", 1, always),
    Entry::new(&[Key::Char('A')], "A", "always", "allow the tool request always", 2, always),
    Entry::new(&[Key::Char('r')], "r", "reject", "reject the tool request at once", 3, always),
    Entry::new(&[Key::Char('n')], "n", "note", "reject the tool request with a note to the ant", 4, always),
    Entry::new(&[Key::Enter], "Enter", "thread", "open the tool request's thread", 5, always),
    Entry::new(&[Key::Esc, Key::Char('q')], "Esc", "close", "close the tool request, still waiting (q too)", 6, always),
    Entry::new(UP_DOWN, "↑↓", "scroll", "scroll the tool request by line", 10, always),
    Entry::new(PAGES, "PgUp PgDn", "page", "scroll the tool request by a page", 92, always),
    Entry::new(ENDS, "Home End", "top/bottom", "scroll to the top or the bottom", 95, always),
];
