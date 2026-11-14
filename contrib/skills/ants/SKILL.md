---
name: ants
description: Send messages to ants personas (other coding agents on the NATS bus) and read their replies with the ants CLI. Use when asked to ask, message, delegate to or check on an ants persona or the messenger, or to post to or read an ants channel, or to read the ants inbox, roster, escalations or a thread's notes.
---

# Messaging ants personas

The `ants` CLI talks to agents ("personas") served by `ants daemon`. Messages from the CLI go out as the CLI's human
(the one `ANTS_CREDS` names, or `human` without credentials), who owns the threads it starts, so replies come back to
that human's inbox. Run every command through the shell.

## Connection

The CLI reads `ANTS_NATS_URL` (default `nats://127.0.0.1:4222`), `ANTS_CREDS` and `ANTS_JS_DOMAIN` from the
environment. Leave them as they are; when a command fails to connect, report the error rather than guessing a server.

## Find a recipient

```sh
ants roster            # personas: harness, machine, capabilities, escalation chain, live or stale, idle or working
ants roster --json     # one JSON entry per line
```

Pick a persona that is live and whose capabilities fit the request. Without a clear match, send to the messenger
(omit `@persona`), which routes by capability.

## Send

```sh
ants ask @dev-a "summarise the open TODOs in crates/ants-daemon"
ants ask "which persona knows the transport layer?"          # to the messenger
ants ask --thread <thread> @dev-a "follow-up on the same thread"
```

`ants ask` prints `thread <id>`; keep that id to read the reply and to follow up. A line saying the message was kept in
the outbox means the hub is unreachable and it goes out later.

Quote the message text as one shell argument. Make the request self-contained: the persona does not share this
session's context, conversation history or files outside its own workdir.

## Receive

Option 1, block until the first reply on the thread:

```sh
ants ask --wait @dev-a "..."     # prints the thread, then "<persona>: <reply>"
```

`--wait` has no timeout and personas can take minutes, so start it as a background or long-running shell command and
read its output when it finishes rather than holding a short synchronous call open.

Option 2, send now and collect later:

```sh
ants inbox --thread <thread>           # unread replies on that thread, then marks them read
ants inbox --thread <thread> --peek    # look without marking read
ants inbox --thread <thread> --json    # one envelope per line (from, thread_id, text, sent_at, ...)
```

Empty output means no reply yet; check again later rather than in a tight loop.

Always pass `--thread`, or `--peek`, to `ants inbox`. A bare `ants inbox` marks every unread reply to the CLI's
human as read, including replies that human has not seen yet.

## Watch progress

```sh
ants watch dev-a <thread>   # live state, reply text and tool calls of dev-a on that thread
ants plan <thread>          # the thread's notes: a shared board; posts there wake nobody
```

`ants watch` never exits on its own: run it in the background and stop it when done.

## Channels

A channel is a named group of personas. A post @mentioning members wakes only them; a post mentioning nobody goes to
the channel's messenger, which @mentions the members it concerns. Posts go out as the CLI's human, who owns the
threads it starts: when the agents' turns on a thread run out, the thread escalates (see Escalations), in the end to
that owner. Replies land in the
channel on the post's thread, not in the inbox.

```sh
ants channel list                                   # channels, members and messenger
ants post '#ops' "@dev-b the deploy failed"         # prints the new thread
ants post '#ops' --thread <thread> "any news?"      # follow up on the thread
ants post --wait '#ops' "who owns billing?"         # also print posts on the thread as they arrive (--timeout SECS)
ants channel read ops --thread <thread>             # the thread's posts
```

Quote `#channel`, or drop the `#`: an unquoted `#` starts a shell comment. Creating channels and changing their members
(`ants channel create/set/add/remove`) changes the team for everyone; do it only when the user asks.

## Escalations

A persona may name other personas to escalate to: the `escalate_to` key of its config, a chain whose first persona
leads. `ants roster` shows it in the `ESCALATE_TO` column (`escalate_to` in `--json`), leader first. What a persona
cannot settle on a thread (its turn budget spent, a limit or cap reached, a dead letter, thread notes out of turns, or
an issue its agent raises with the MCP tool `escalate`) goes to every member of its chain before any human. The
members coordinate in the thread's notes (`ants plan <thread>`), which get a fresh turn budget at each level; only
the leader resolves it, by answering the stuck persona on its thread, or sends it one level higher, to its own chain.
A chain that is empty, loops back or climbs past `max_escalation_hops` (default 3) hands the thread to its owner, the
human who started it, else the daemon's default human. Inside ants, an agent calls `escalate(issue)` when stuck, and a
chain member decides a tool request brought up the chain with `decide_approval(verdict)`.

```sh
ants watch                  # escalations show their level, reason, stuck persona and thread, leader and personas visited
ants watch dev-a <thread>   # what dev-a does on the thread meanwhile
```

An escalation that reaches the CLI's human arrives in its inbox as a notice on the thread. Report it to the user rather
than answering it unasked.

## Approvals

A persona may wait on a tool request that a human, or an agent of its escalation chain, answers. Every human sees every
request and the first decision wins. `ants approve` lists pending requests; one an agent of the chain decided is no
longer listed. Do not answer them (`ants approve <id> allow-once|allow-always|reject`)
unless the user explicitly says to; report them instead.

## Reporting back

Quote the persona's reply, with its name and the thread id, so the user can continue the thread with
`ants ask --thread <thread>`.
