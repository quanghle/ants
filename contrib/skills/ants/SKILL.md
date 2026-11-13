---
name: ants
description: Send messages to ants personas (other coding agents on the NATS bus) and read their replies with the ants CLI. Use when asked to ask, message, delegate to or check on an ants persona or the messenger, or to post to or read an ants channel, or to read the ants inbox, roster or a thread's plan room.
---

# Messaging ants personas

The `ants` CLI talks to agents ("personas") served by `ants daemon`. Messages from the CLI go out as the human, so
replies come back to the human's inbox. Run every command through the shell.

## Connection

The CLI reads `ANTS_NATS_URL` (default `nats://127.0.0.1:4222`), `ANTS_CREDS` and `ANTS_JS_DOMAIN` from the
environment. Leave them as they are; when a command fails to connect, report the error rather than guessing a server.

## Find a recipient

```sh
ants roster            # personas: harness, machine, capabilities, live or stale, idle or working
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

Always pass `--thread`, or `--peek`, to `ants inbox`. A bare `ants inbox` marks every unread reply to the human as
read, including replies the human has not seen yet.

## Watch progress

```sh
ants watch dev-a <thread>   # live state, reply text and tool calls of dev-a on that thread
ants plan <thread>          # the thread's plan room
```

`ants watch` never exits on its own: run it in the background and stop it when done.

## Channels

A channel is a named group of personas. A post @mentioning members wakes only them; a post mentioning nobody goes to
the channel's messenger, which @mentions the members it concerns. Replies land in the channel on the post's thread,
not in the inbox.

```sh
ants channel list                                   # channels, members and messenger
ants post '#ops' "@dev-b the deploy failed"         # prints the new thread
ants post '#ops' --thread <thread> "any news?"      # follow up on the thread
ants post --wait '#ops' "who owns billing?"         # also print posts on the thread as they arrive (--timeout SECS)
ants channel read ops --thread <thread>             # the thread's posts
```

Quote `#channel`, or drop the `#`: an unquoted `#` starts a shell comment. Creating channels and changing their members
(`ants channel create/set/add/remove`) changes the team for everyone; do it only when the user asks.

## Approvals

A persona may wait on a tool request only the human can answer. `ants approve` lists pending requests. Do not answer
them (`ants approve <id> allow-once|allow-always|reject`) unless the user explicitly says to; report them instead.

## Reporting back

Quote the persona's reply, with its name and the thread id, so the user can continue the thread with
`ants ask --thread <thread>`.
