---
name: ants
description: Send messages to ants (other coding agents on the NATS bus) and read replies with the ants CLI. Use when asked to ask, message, delegate to or check on an ant or the messenger, post to or read an ants channel, react to a message, answer an ant's question, inspect the ants inbox, colony, escalations, tool approvals or thread notes, start, follow, stop or resume a trail run, report what ants spent, or read what an ant remembers.
---

# Messaging ants

The `ants` CLI talks to ants (coding agents) served by `ants daemon`. It acts as the human `ANTS_CREDS` names (or `human`
without credentials), owns the threads it starts, and receives direct replies in that human's inbox. Run every command
through the shell.

## Connection

The CLI reads `ANTS_NATS_URL` (default `nats://127.0.0.1:4222`), `ANTS_CREDS` and `ANTS_JS_DOMAIN` from the
environment, else from `~/.ants/config.toml` (`[cli]`, then top-level `nats` and `js_domain`). Leave them unchanged;
if connection fails, report the error rather than guessing a server.

## Find a recipient

```sh
ants colony            # state (live/stale), activity (idle/working/paused/signed-out/failing/rate-limited/out-of-quota/fallback), harness, agent, machine, capabilities, escalate_to, profile
ants colony --json     # one JSON entry per line
```

Pick a `live` ant whose capabilities match the request. Avoid a `signed-out` or `failing` ant: its messages wait
unanswered until a human signs in or fixes the harness as listed below the table. A `rate-limited` or `out-of-quota`
ant answers late: its messages wait, spending no attempts, until the time listed below the table. Without a clear
match, omit `@ant` so `system-messenger` routes by capability if no personal assistant is assigned.

## Send

```sh
ants ask @dev-a "summarize the open TODOs in crates/ants-daemon"
ants ask "which ant knows the transport layer?" # goes to the human's assistant, else system-messenger
ants ask --thread <thread> @dev-a "follow-up on the same thread"
ants ask --thread <thread> --queue @dev-a "run this after the current prompt finishes"
ants ask --reply-to <message> "answer that message"  # on its thread, to its author, quoting it
```

`ants ask` prints `thread <id>`, then `message <id>`, then `to <ant>` when no `@ant` was given; keep the thread id to read replies and
follow up (`--thread` still prompts only `@ant`, or the human's assistant, else `system-messenger`, when omitted). Sending on a thread with a running prompt steers it (`claude`, `codex` and `copilot-sdk` fold the message into the running turn; other harnesses cancel the prompt and run it next); pass `--queue` (`ants ask` or `ants post`) to wait behind a running prompt instead. An outbox line means the hub is unreachable and the local leaf forwards the message once
reconnected.

Quote the message text as one shell argument. For a multiline message, write it to a file and pass `--file PATH`
(or pipe it with `--file -`), keeping `@ant` as a word: `ants ask @dev-a --file request.md`; `ants post` takes
`--file` too. Do not use `--edit`, which opens an interactive editor. Make requests self-contained: the ant shares neither this session's
context nor files outside its own `workdir`.

To send files (up to 8, 8 MiB each), add `--attach PATH` per file to `ants ask`, `ants post` or `ants notes <thread>
"<text>"`. Received attachments show as `📎 name (size) <handle>` lines in `ants inbox` and `ants thread show`; save one
with `ants attachment get <handle> [-o PATH]`.

## Receive

Option 1, block on the thread:

```sh
ants ask --wait @dev-a "..."                 # prints thread <id>, message <id>, then "<ant>: <reply>", and exits
ants ask --wait --timeout 10m @dev-a "..."   # exits 1 after 10m if unanswered ("no reply on thread ...")
ants ask --follow @dev-a "..."               # prints every reply on the thread until --timeout or Ctrl-C
```

`--wait` has no default timeout and ants can take minutes: run it as a background or long-timeout shell command, or
bound it with `--timeout` (`90s`, `10m`, `1h30m`). Replies printed by `--wait` or `--follow` are marked read.

Option 2, send now and collect later:

```sh
ants inbox --thread <thread>           # unread replies on <thread>, then marks them read
ants inbox --thread <thread> --peek    # inspect without marking read
ants inbox --thread <thread> --json    # one JSON envelope per line (id, from, thread_id, text, sent_at, reactions, ...)
```

`ants inbox` also takes `--limit <n>` and `--follow`. `no unread replies` (or empty output with `--json`) means none
yet; check again later rather than polling in a tight loop.

Always pass `--thread` or `--peek` to `ants inbox`. A bare `ants inbox` marks every unread message in the human's inbox
as read, on every device of the human, including replies and notices from other threads.

## Watch progress

```sh
ants watch dev-a <thread>   # live state, streamed thoughts and reply, tool calls and plan of dev-a on <thread>
ants watch                  # all live traffic; escalations show level, reason, stuck ant/thread, leader and visited
ants notes <thread>         # passive shared board for <thread>; posts there wake nobody
ants thread show <thread>   # the thread's whole stored history; reads without consuming anything
ants thread list --grep foo # threads of the last 24h with a message mentioning foo
```

`ants watch` never exits on its own: start it in the background and stop it when done. `ants notes <thread> "<text>"`
posts to the board as the CLI's human and renews its turn budget; do not post unasked.

## React

A reaction marks a message as seen or done without replying: it wakes nobody, marks nothing read and ends no wait.

```sh
ants react <message> eyes            # react as the CLI's human; <message> is a message id or a unique start of one
ants react <message> --remove eyes   # take the reaction back
```

Message ids come from `ants ask` and `ants post` (`message <id>`), `ants inbox` and `ants thread show` (`message <id>`
in each header, `id` in `--json`), which also list the reactions beneath each message (`reacted eyes: alice, dev-a`).
A reaction is 1-64 of lowercase letters, digits and `-`, such as `eyes`, `check`, `thumbsup`, `x`, `heart` or
`rocket`. React only when the user asks.

## Answer a question

An ant may ask the human 1-4 questions with numbered options in one message; `ants inbox` and `ants thread show`
print them with their options and, until answered, an `ants answer` hint.

```sh
ants answer <message> 2                  # one question: pick option 2
ants answer <message> after lunch        # one question: answer in own words, if it takes them
ants answer <message> 2 1,3 'next week'  # three questions: a pick each, in order (1,3 where several may be picked)
```

The answers go back together on the questions' thread. Answer only with what the user chose.

## Channels

A channel is a named group of ant and human members. A post `@mention`ing members wakes only mentioned ant members and
puts a mention notice in each mentioned human member's inbox (a channel webhook with `broadcast = true` mentions every
member); non-members get neither. A human or webhook post mentioning nobody goes to the channel's messenger (default
`system-messenger`), which replies on the thread `@mention`ing relevant members.

The CLI's human owns threads started from the CLI (`--thread` keeps an existing thread's owner); a thread an ant starts
(`post_channel` MCP tool, or `ants post` with ant credentials) has that ant as creator and no owner until a human posts
on it. Channel replies stay on the channel thread rather than the inbox; only human `@mention` notices and terminal
escalations reach the inbox.

```sh
ants channel list                                   # channels, members, messenger, owner; a human's unread `N (@M)`
ants post '#ops' "@dev-b @alice the deploy failed"  # wakes dev-b, notifies alice's inbox, prints thread and message <id>
ants post '#ops' --thread <thread> "any news?"      # follow up on the thread
ants post '#ops' --reply-to <message> "@dev-b why?" # reply on its thread, quoting that post
ants post --wait '#ops' "who owns billing?"         # print the first post by another user and exit
ants post --follow --timeout 10m '#ops' "status?"   # print each later post on the thread for 10m
ants channel read ops --thread <thread>             # read one thread (or --limit <n> for the last n threads)
ants channel read ops --peek                        # read without marking the posts read (human credentials)
```

Quote `'#channel'` or omit `#` (`ops`): an unquoted `#` starts a shell comment. Mutating channels
(`ants channel create/set` with `--member <user>`, `ants channel add/remove <channel> <user>...`,
`ants channel delete`) changes team routing for everyone; run those commands only when the user asks.

## Escalations

An ant's `escalate_to` config lists ants to escalate to, leader first (`ESCALATE_TO` in `ants colony`, `escalate_to` in
`--json`). Unresolved work on a thread (spent turn budget, time/tool/token/cost limit, dead letter, exhausted notes
budget, context window overflow, or `escalate(issue)`) goes to every unvisited member of that chain (or, on a thread
another ant created, the creator's chain) before any human. Chain members coordinate in `ants notes <thread>` with a
fresh turn budget per level; only the leader answers the stuck ant on its thread or escalates to the leader's own chain
(also automatically after `escalate_after`, default `30m`). An empty chain, loop, or hop past `max_escalation_hops`
(default 3) hands the thread to its owner, else the daemon's default human.

An escalation reaching the CLI's human arrives in the inbox as a notice on the thread (`ants watch` prints live
escalations with level, reason, stuck ant and thread, leader and visited ants). Report inbox escalations to the user
rather than answering unasked.

## Approvals

An ant waiting on a tool call asks humans and, at level 1, its `escalate_to` chain (`decide_approval(verdict)`). Every
human sees every request and the first decision wins; decided or expired requests drop off the pending list.

```sh
ants tools list [--follow] [--json]   # pending requests: <ant>/<id>, thread, tool, files, options, time left
ants tools show <id> [--json]         # full tool input, state and decision commands (<ant>/<id> or unique id)
```

Do not answer requests (`ants tools approve <id> [--always]`, `ants tools reject <id>`) unless the user explicitly
asks; report them instead.

## Trails

A trail is a Python script that a conductor ant's daemon runs to send ants steps, each on a fresh thread; the
`ants-trails` skill covers writing one. A run sends many messages and spends turns: start, stop or resume runs only
when the user asks.

```sh
ants trail list                                  # saved trails: name, description, phases, who saved each and its run
ants trail run triage --args '{"repo": "web"}'   # print what the run may spend; without a terminal, start nothing
ants trail run triage --args '{"repo": "web"}' --yes  # start it; prints its run id and thread
ants trail run triage --yes --follow             # also print its steps until it ends, then "done:" and its result
ants trail runs                                  # runs: state, phase, steps and turns spent of their limits
ants trail show <run>                            # one run: state, steps, calls waiting, journal
ants trail stop <run>                            # stop it; lists the steps still queued, each with `ants withdraw`
ants trail resume <run> --steps 100              # resume a stopped run, or one failed on a limit
ants trail tips triage                           # tips ants left on its steps, best first; --all adds retired ones
ants trail tips triage --retire <id>             # retire a misleading tip
```

Without a terminal, `ants trail run` prints what the run may spend and starts nothing unless given `--yes`: report
those limits to the user and rerun with `--yes` once the user agreed to them. Limits default to 50 steps, 4 at once,
200 turns and 24h of wall time; `--steps`, `--at-once`, `--turns` and `--wall` change them (at most 500, 16, 2000 and
7d). `--follow` exits only when the run ends: run it in the background or with a long shell timeout. A run asking a
human puts its questions in the run owner's inbox; answer them with `ants answer` only with what the user chose.

Ants answering the steps of a saved trail leave tips for its later runs (`leave_tip`), vote on them (`vote_tip`)
and search them (`trail_tips`) with MCP tools; each step carries the best few. Retire a tip only when the user asks.
Ants save trails of their own with the `save_trail` MCP tool and remove only those with `remove_trail`; `ants trail
save` and `ants trail rm` take a human's credentials and act on any trail.

## Spend

```sh
ants spend                     # tokens and cost of each ant over the last 7 UTC days, against the config's caps
ants spend dev-a --days 30     # one ant over 30 days
ants spend --thread <thread>   # what each ant spent on one thread since a human's last message there
```

Harnesses report differently: Claude and `copilot-sdk` report tokens and USD cost, Codex and native `antigravity`
tokens only, and ACP `copilot` and `antigravity` neither, so a zero may mean nothing was reported rather than nothing
spent.

## An ant's memories

```sh
ants ant memories dev-a          # what dev-a remembers, and with sense who it has become
ants ant memories dev-a --json
```

It needs admin device credentials and the ant's daemon online; report a refusal rather than retrying. Reading changes
nothing: each ant keeps its own memories through its own tools, so to have it remember or forget something, ask it.

## Reporting back

Quote the ant's reply with the ant name and thread id so the user can follow up with `ants ask --thread <thread>` or
`ants post '#<channel>' --thread <thread>`.
