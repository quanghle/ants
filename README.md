# ants

[![CI](https://github.com/quanghle/ants/actions/workflows/ci.yml/badge.svg)](https://github.com/quanghle/ants/actions/workflows/ci.yml)

Headless agent teams across machines, communicating with each other and with humans at the terminal over NATS.

- **Backbone:** NATS + JetStream for durable inboxes, channels, thread notes, approvals and dead letters, plus NATS KV for roster, session, attempt, channel, human, chain, link, owner and webhook state.
- **Transport:** `wss://`/`ws://` behind a reverse proxy, `tls://`, a Dev Tunnel, or the plain-HTTPS fallback bridge (`ants bridge`), optionally with a local store-and-forward leaf node per machine. See [Transport options](#transport-options) and [docs/transport.md](docs/transport.md).
- **Harnesses:** ACP (Agent Client Protocol) agents over stdio: Copilot CLI (`copilot --acp`, default), Claude Code (`claude-agent-acp`), Google Antigravity CLI (`agy acp`), or custom `[harnesses.NAME]` definitions such as Codex (`codex-acp`). See [Harnesses](#harnesses).
- **Footprint:** one Rust binary, `ants`, serves as the CLI, ant daemon, stdio MCP bridge, channel manager, webhook ingress and HTTPS bridge. An idle daemon stays under 20 MB RSS.

## Architecture

```text
 machine "macbook"                      hub machine                       machine "desk"
+-------------------------------+      +--------------------------+      +--------------------------+
| ants daemon                   |      | nats-server (hub)        |      | ants CLI (human)         |
|   dev-a -> harness per thread |      |   JetStream domain "hub" |      |   ask, post, inbox,      |
|   dev-b -> harness per thread |      |   stream ANTS            |      |   tools, watch           |
|   MCP server for sessions     |      |   KV ants_roster,        |      |            |             |
|            |                  |      |      ants_sessions,      |      |            |             |
|            |                  |      |      ants_attempts,      |      |            |             |
|            |                  |      |      ants_channels,      |      |            |             |
|            |                  |      |      ants_humans,        |      |            |             |
|            |                  |      |      ants_chains,        |      |            |             |
|            |                  |      |      ants_links,         |      |            |             |
|            |                  |      |      ants_owners,        |      |            |             |
|            |                  |      |      ants_read,          |      |            |             |
|            |                  |      |      ants_presence,      |      |            |             |
|            |                  |      |      ants_reactions,     |      |            |             |
|            |                  |      |      ants_threads,       |      |            |             |
|            |                  |      |      ants_notify,        |      |            |             |
| nats-server (leaf, outbox)  --+----->|      ants_webhooks       |<-----+-- nats-server (leaf)     |
+-------------------------------+      +--------------------------+      +--------------------------+
                        uplink: wss://, tls://, Dev Tunnel, or ants bridge
```

- **Hub:** one `nats-server` with JetStream holding stream `ANTS` and fourteen KV buckets (`ants init`). With `ants auth`, every ant, human, webhook, leaf uplink and channel manager connects using role-scoped NKey/JWT credentials.
- **Leaf:** optional per-machine `nats-server` (`ants leaf config`) on `127.0.0.1:4222` with a store-and-forward outbox (`ants leaf outbox`). Local `ants` processes target `127.0.0.1:4222`; only the leaf uplink crosses the network. Single-machine setups point `ants` straight at the hub.
- **Daemon:** `ants daemon` serves ants over per-ant NATS connections and durable consumers, runs up to `max_harnesses` ACP processes per ant, enforces tool allowlists, limits and spending caps, refreshes roster heartbeats and exposes an MCP server to each session.
- **Ants and humans:** ants are named agents (`dev-a`, `reviewer`, `messenger`) with a workdir, harness, capabilities and optional `whoami`. Humans (`human` by default, or named users from `ants auth user NAME --human`) own threads they start and approve tools. Unaddressed `ants ask` requests go to `messenger` for capability routing. Agent-to-agent messages share a per-thread turn budget (default 8).

| Store / subject | Storage / TTL | Purpose |
|---|---|---|
| Stream `ANTS` | File, 30d | Captures `ants.inbox.>`, `ants.notes.>`, `ants.approvals.>`, `ants.dead.>`, `ants.chan.>`; enables direct gets; republishes `ants.inbox.*.*` to `_INBOX.<to>.stored.<from>`; takes up to 10000 consumers. |
| KV `ants_roster` | Memory, 30s | Live `RosterEntry` per ant (`<ant>`), refreshed every 10s; an expired entry leaves a marker that watchers see; takes up to 10000 consumers (watches). |
| KV `ants_sessions` | File, 30d | Thread ACP session IDs (`<ant>.<thread>`), spend/escalation state and reminders, channel seen cursors, mention markers. |
| KV `ants_attempts` | File, 30d | Started delivery markers and saved replies (`<ant>.<seq>`) before consumer ack; what became of each message (`<ant>.msg.<id>`: started, finished or withdrawn, see [Queue](#queue-and-withdrawal)). |
| KV `ants_channels` | File, none | `ChannelEntry` (`<channel>`): members, optional messenger, optional owner ant; takes up to 10000 consumers (watches). |
| KV `ants_humans` | File, none | Registered humans (`<human>`), written automatically on CLI connection; takes up to 10000 consumers (watches). |
| KV `ants_chains` | File, none | Persistent `escalate_to` chains (`<ant>`), readable after roster expiry. |
| KV `ants_links` | File, 30d | Verified `origin_thread` handoffs (`<from>.<thread>.<to>`), from spin-offs and from messages on a thread with a `creator` or `owner`. |
| KV `ants_owners` | File, 30d | Thread `owner` (`<thread>.<recipient>`) recorded by a human (`ants ask`) or webhook for each ant it messages on the thread; the first record of a key stays: a second is refused, or dropped as a repeat of its `Nats-Msg-Id` (no delete or purge). |
| KV `ants_read` | File, none | Each human's read marker (`<human>`): the `ANTS` sequences of its inbox messages read on any of its devices (`below`, plus a `read` set above it); each human reads and writes only its own key. |
| KV `ants_presence` | Memory, 30s | Devices following (`<human>.<device>.<inbox|approvals>`), put every 10s while `ants inbox -f`, `ants tools list -f` or `ants dash` runs on that device and deleted when it stops; each human reads by direct get and writes only its own keys. |
| KV `ants_notify` | File, none | Each human's notification holds (`<human>`): a snooze deadline and muted threads with theirs, shared by all its devices (see [Notifications](#notifications)); each human reads by direct get and writes only its own key. |
| KV `ants_reactions` | File, 30d | Reactions to messages (`<reactor>.<message id>.<reaction>`, see [Reactions](#reactions)); each user puts and deletes only its own keys, humans read them all; no purge, rollup or bucket-wide delete. |
| KV `ants_threads` | File, none | Thread titles (`title.<thread>`), shared by all humans, and each human's pinned and archived threads (`marks.<human>`), shared by its devices (see [Titles, pins and archives](#titles-pins-and-archives)); any human writes titles, each human reads and writes only its own marks; ants read and write none. |
| KV `ants_webhooks` | File, 7d | Delivered webhook event IDs (`<webhook>.<hash>`), sent schedule firings (`<schedule>.<unix time>`), hourly `<webhook>.webhook` markers of webhooks and schedules, and the channel manager's hourly `channel-manager.serve`, each holding the version of ants it runs. |
| Core NATS | Unstored | `ants.status.<ant>.<thread>` (progress), `ants.cancel.<ant>.<thread>` and `ants.cancelled.<ant>.<thread>` (`ants cancel` and the daemons' answers), `ants.queue.<ant>` and `ants.queued.<ant>` (`ants queue` and the daemons' answers), `ants.chanadm.<channel>.<ant>` (owner ops), `ants.enroll.<ant>[.reply]` (enrollment, carried by leaf uplinks), `ants.antreq.<manager>[.reply]` (a manager ant's request for a new ant and the signer's answer, carried by leaf uplinks). |

User, channel, thread and JetStream domain IDs use 1–64 characters of `a-z`, `0-9` and `-`, starting with a letter or digit (`channel-manager` is reserved). Machine and harness names also allow `_` (machine names allow uppercase ASCII and must differ from `js_domain`). Durations require whole-number units ordered largest first (`s`, `m`, `h`, `d`: `90s`, `10m`, `1h30m`) or `0`; bare non-zero numbers are rejected.

## Install

Requirements:

- Rust 1.100 or later.
- [nats-server](https://github.com/nats-io/nats-server/releases) 2.11 or later (tested with 2.15.3).
- A signed-in harness CLI on machines running `ants daemon` (see [Harnesses](#harnesses)).

```sh
cargo install --locked --git https://github.com/quanghle/ants ants   # ~/.cargo/bin/ants
ants --help
```

From a checkout: `cargo install --locked --path crates/ants`.

### Windows

ants runs on macOS, Linux and Windows. Build on Windows with the MSVC toolchain (Visual Studio Build Tools), or cross-build `--target x86_64-pc-windows-gnu` with mingw-w64 on `PATH` (for its linker and the C compiler of `ring`). On Windows:

- **MCP for stdio harnesses:** the daemon serves the tools for harnesses without HTTP MCP on a named pipe, `\\.\pipe\ants-mcp-<random>`, instead of `<home>/mcp.sock`. Its access list grants access to the daemon's user only, so other local users cannot connect, and it refuses remote clients; each connection still starts with the session's token, as on the socket. See [MCP tools](#mcp-tools).
- **Harness commands:** a bare command name is looked up on `PATH` as an `.exe`, `.cmd` or `.bat` file, so harnesses installed by npm (`copilot.cmd`) start.
- **No service units:** `scripts/install-units.sh`, and the service reloads and restarts of `ants add`, `ants renew`, `ants leaf config`, `request_ant` and `ants tools approve --rule`, cover launchd and systemd only; they print the commands to run instead. Run `ants daemon` and `nats-server` in a console, or as a Task Scheduler task at logon (`schtasks /Create /SC ONLOGON /TN ants-daemon /TR "C:\Users\NAME\.cargo\bin\ants.exe daemon --config C:\Users\NAME\.ants\config.toml"`), for the account whose Credential Manager holds any stored keys (see [Private keys in the OS credential store](#private-keys-in-the-os-credential-store)). Such a task does not restart on the upgrade exit (status 75): pass `--upgrade-check 0`, or start it again after an upgrade.
- **Signals:** `ants daemon` stops as on `SIGTERM` on Ctrl-C, Ctrl-Break, the console closing or the system shutting down. There is no `SIGHUP`: `ants daemon` and `ants webhook serve` reload an edited config file by themselves within seconds (see [Config file](#config-file)).
- **File permissions:** the files ants restricts to their owner elsewhere (mode `0600`: credentials, seeds, secrets) get no mode on Windows: they inherit the access list of their directory. `~` is `%USERPROFILE%`, whose default access list admits the user, SYSTEM and administrators only; keep credentials and the team directory under it.

### Upgrade

Running `cargo install` again replaces `~/.cargo/bin/ants`; the services move onto the new executable by themselves. `ants daemon`, `ants channel serve`, `ants webhook serve`, `ants bridge serve` and `ants bridge connect` check every 30s whether another file replaced their executable at the path they started from (or the file was rewritten in place). Once the new file is executable, non-empty and unchanged across two checks, the process exits with status 75 for the service manager to start the new one: `ants daemon` first stops its ants as on `SIGTERM` (prompts in flight finish, waiting messages go back for redelivery) and its schedules at once; the others exit at once, as on `SIGTERM`. The units of `scripts/install-units.sh` restart on status 75 (systemd `RestartForceExitStatus=75`; launchd `KeepAlive`, as on any failed exit); systemd units installed before also restart on it, as on a failure. A process started by hand logs the reason and exits: start it again. `--upgrade-check DURATION` (`ANTS_UPGRADE_CHECK`) on those commands sets the period; `0` turns the check off.

## Quick start

Single machine without credentials:

```sh
ants setup --ant dev-a --workdir ~/src/some-repo    # nats-server, config file, services, completions, doctor
ants ask --wait @dev-a "summarise the open TODOs"
ants ask @dev-a --file request.md                   # multiline message from a file (--file - reads stdin)
ants ask @dev-a --edit                              # write it in $VISUAL / $EDITOR
ants inbox                                          # unread replies
ants roster                                         # active ants
ants tools list                                     # pending tool approval requests
ants notes <thread>                                 # passive thread notes
ants thread list                                    # threads active in the last 24h
ants thread list --escalated --since 7d             # escalations no human answered yet
ants thread show <thread>                           # one thread's whole history
ants thread search --from dev-a --grep deploy       # messages dev-a sent mentioning deploy, last 24h
ants thread search --type escalation,dead --since 7d   # escalations and dead letters of the past week
ants thread title <thread> Release 1.2              # title a thread for every human
ants thread pin <thread>                            # pin it (archive, unpin, unarchive alike)
ants thread list --archived                         # the threads archived
ants react <message> eyes                           # react to a message (ids from ask, post, inbox, thread show)
ants channel create ops --member dev-a,dev-b
ants post '#ops' "@dev-a the build is red"          # wakes dev-a; prints thread ID
ants channel read ops
ants watch                                          # live traffic on ants.>
ants watch dev-a [<thread>]                         # streamed harness state, text, tools, plan
ants cancel dev-a <thread>                          # stop the prompt dev-a runs on the thread
ants queue dev-a                                    # messages waiting for dev-a, and where each stands
ants withdraw dev-a <message>                       # take one back before dev-a is prompted with it
ants dash                                           # interactive dashboard: threads, progress, tool requests
```

`ants setup` brings the machine up as [Setup](#setup) describes; by hand, the same is `nats-server -js &` (local hub), `ants harness check` (ACP handshake with Copilot CLI) and `ants daemon --ant dev-a --workdir ~/src/some-repo &`.

An empty `~/.ants/config.toml` (or `harness = "claude"`) lets `ants daemon` with no flags serve the built-in `messenger` alone (see [Messenger](#messenger)). Multi-machine deployments add [Credentials](#credentials), [Leaf nodes](#leaf-nodes), a [Config file](#config-file) and [Services](#services).

## CLI

| Command | Description |
|---|---|
| `ants ask [--thread THREAD] [--wait \| -f] [--timeout DUR] [@ant] TEXT \| --file PATH \| --edit` | Send to `@ant` (`messenger` if omitted) and print the thread ID (`thread <id>`), then the message ID (`message <id>`). `--thread` continues an existing thread. The message is TEXT, or comes from a file or an editor (see [Composing messages](#composing-messages)). Warns on stderr, but sends anyway, when no daemon serves the ant now (not in `ants_roster`, stale, its harness signed out, or held back by a provider limit), hinting at a misspelt name when `ants_chains` keeps no chain for it either. |
| `ants inbox [--peek] [--thread THREAD] [--limit N] [-f] [--json]` | Print unread replies, each with its message ID and the reactions to it so far (`reactions` in `--json`), and mark read on every device of the human (`--peek` keeps unread; `-f`/`--follow` streams new replies, on every device following, and watches advisories; without `--thread` and `--json`, it also prints pending tool requests with the commands answering them, new ones, and how each ends (answered, by whom if an agent, or expired); without `--thread`, the device counts as reading its escalations, and without `--json` its tool requests too, see [Notifications](#notifications)). |
| `ants roster [--json]` | List ants (`live`/`stale`, `idle`/`working`/`signed-out`/`failing`/`rate-limited`/`out-of-quota`), the ants and permissions versions of their daemons, harnesses and the agent each runs (`AGENT`, `name@version` as the latest harness start named it), capabilities, `escalate_to` chains and workdirs; below the table, a note when live daemons run mixed versions, one when live ants run a harness agent at different versions on different machines, one line per signed-out harness with its sign-in command and machine, one per harness failing to start with its last error and the `ants harness check` to run, and one per harness held back by a provider limit with when its messages are tried again. |
| `ants spend [ANT] [--thread THREAD \| --days N] [--json] [--config FILE]` | Report tokens and cost spent (see [Spend report](#spend-report)). |
| `ants notes THREAD [TEXT]` | Show thread notes, or post `TEXT` as the CLI human (resets budget to 8 turns; wakes nobody). |
| `ants react MESSAGE REACTION [--remove]` | React to a message as the CLI's user, or remove that reaction (see [Reactions](#reactions)); reacting again or removing a reaction not there changes nothing. Wakes nobody, marks nothing read and ends no `--wait`. |
| `ants post [--thread THREAD] [--wait \| -f] [--timeout DUR] CHANNEL TEXT \| --file PATH \| --edit` | Post to `ops` or `'#ops'` and print the thread ID, then the message ID (`@ant` wakes members; `@human` notifies members; no mention triages). TEXT may come from a file or an editor, as for `ants ask`. |
| `ants channel create \| set \| add \| remove \| list \| delete` | Manage channels: `--member USER,...`, `--messenger ANT` (`--default-messenger` on `set`), `--owner ANT` (`--no-owner` on `set`). Warns about ants named that no daemon serves now, as `ants ask` does. |
| `ants channel read CHANNEL [--thread THREAD \| --limit N]` | Print channel posts grouped by thread. |
| `ants channel serve` | Run the channel manager (`ants auth channel-manager` creds) to apply owner `manage_channel` requests; it puts the version of ants it runs at `channel-manager.serve` in `ants_webhooks` hourly. |
| `ants thread list [--ant USER] [--owner HUMAN] [--grep TEXT] [--escalated] [--pinned \| --archived] [--since DUR] [--limit N] [--json]` | List threads with messages stored within `--since` (default `24h`, `0` for all kept): id, owner, users and channels, first/last time, message count, state and title (columns shown only when a listed thread has one), opening line; most recently active last, the human's pinned threads after the others, up to `--limit` (default 50, pinned threads kept first). Archived threads are left out unless a message was stored on one since it was archived (state `archived+new`); `--pinned` or `--archived` lists only those, read by default from the earliest one's start (see [Titles, pins and archives](#titles-pins-and-archives)). `--json` adds `title`, `pinned`, `archived` and `new_activity` when set. `--grep` keeps threads with a message containing `TEXT` (case-insensitive) and prints the matching line. `--escalated` keeps threads whose escalation (other than a tool request's) reached a human with no human message on the thread since, and prints when it first did, to whom, and the escalation. |
| `ants thread show THREAD [--since DUR] [--json]` | Print a thread's history in stored order: inbox messages both ways, channel posts, notes, escalations, tool requests and decisions, dead letters, with message IDs and the reactions to each message (`id` and `reactions` in `--json`; see [Thread overview](#thread-overview)); the text output starts with the thread's title and whether the human pinned or archived it. |
| `ants thread search [--grep TEXT] [--from USER] [--to USER] [--channel NAME] [--type KIND,...] [--since TIME] [--until TIME] [--limit N] [--json]` | Print the messages stored on any thread that match every filter given, the latest `--limit` (default 50) of them, oldest first: time, thread, kind, sender and recipient, message ID and the first line matching `--grep` (else the first line). `--since` (inclusive, default 24h before `--until`) and `--until` (exclusive, default now) take a duration back from now, an RFC 3339 time, or a UTC date or date and time (see [History search](#history-search)). `--json` prints one entry per line, as `thread show --json`. |
| `ants thread title THREAD [TEXT...] [--clear]` | Title a thread `TEXT` (words joined by spaces; one line of 1–120 characters, trimmed) for every human, replacing any title, print its title without `TEXT`, or remove it with `--clear`. |
| `ants thread pin\|unpin\|archive\|unarchive THREAD` | Pin or archive a thread for every device of the human, or undo it. Pinning unarchives, archiving unpins; archiving deletes nothing and cancels no work. |
| `ants thread mute THREAD [--for DUR]` / `ants thread unmute THREAD` | Hold back every notification of a thread (`THREAD`: an id, a unique start of one, or `last`) to the CLI's human for `--for` (default `30d`), on all its devices, or end that (see [Notifications](#notifications)). |
| `ants notify snooze DUR` / `ants notify resume` / `ants notify status [--json]` | Hold back the CLI's human's notifications for `DUR` (at most `365d`), on all its devices, except kinds its `bypass` lets through; end the snooze; show the snooze and muted threads (see [Notifications](#notifications)). |
| `ants watch [ANT [THREAD] \| SUBJECT]` | Stream traffic (default `ants.>`), escalations and decisions, or live harness progress for `ANT`. With human credentials, watching `ants.>` or `>` also prints reactions added and removed. |
| `ants dash [--since DUR]` | Interactive terminal dashboard of the recent threads and their history, the ants' live progress and the roster, and the pending tool requests, answered after a confirmation (see [Dashboard](#dashboard)). Requires human credentials and a terminal. |
| `ants cancel ANT THREAD` | Stop the prompt `ANT` runs on `THREAD` (see [Prompt limits](#prompt-limits-and-spending-caps)); fails when no daemon of `ANT` runs one there or none answers within 5s. Requires human credentials. |
| `ants queue ANT [--json]` | List the messages waiting for `ANT`, oldest first: message ID, thread, sender (and channel), age and state (`running`, `next`, `N ahead` on its thread, `, in the stream` when the daemon has not taken it yet, or `withdrawn by HUMAN`); see [Queue and withdrawal](#queue-and-withdrawal). Fails when no daemon of `ANT` answers within 5s. Requires human credentials. |
| `ants withdraw ANT MESSAGE` | Withdraw a message `ANT` has not started on, so it is never prompted with it, leaving the prompt running and the other messages be (see [Queue and withdrawal](#queue-and-withdrawal)); fails when `ANT` started or finished it, or, when a daemon of `ANT` answers, when the message does not wait for `ANT`. Requires human credentials. |
| `ants tools list [-f] [--json]` | List pending tool requests (`<ant>/<id>`, thread, tool, files, options, time remaining; `-f` streams them, and the device counts as reading tool requests, see [Notifications](#notifications)). |
| `ants tools show ID [--json]` | Show full request, tool input, status and answer commands for `<ant>/<id>` (unique prefix accepted). |
| `ants tools approve ID [--always] [--rule [--config FILE]]` / `reject ID` | Allow once, allow for the session (`--always`, if offered), or reject a tool request; `--rule` also adds the allow rule fitting the request exactly to the ant's `allow` in the config file and reloads the daemon (not for an ant added from a template, whose rules only its template sets). |
| `ants dead list` / `replay SEQ` / `drop SEQ` / `watch` | Inspect, replay or delete dead letters; `watch` turns `MAX_DELIVERIES` advisories into dead letters. |
| `ants init` | Create or update stream `ANTS` and all fourteen KV buckets (requires human credentials; other commands with human credentials do the same on connecting). |
| `ants daemon [--ant NAME] [--config FILE] [--metrics ADDR] [ant-flags...]` | Serve config-file ants, or one ant selected/defined by `--ant` (`ANTS_ANT`); `--metrics` serves health and metrics (see [Monitoring](#monitoring)). |
| `ants add NAME [--config FILE] [--out FILE] [--store-key] [--jwt JWT \| --enroll CREDS [--hub URL] [--leaf-config FILE]] [ant-flags...]` | Add an ant to `config.toml` and generate its user NKey locally; `--store-key` keeps its seed in the OS credential store (see [Adding an ant](#adding-an-ant)). |
| `ants enroll list` / `approve NAME` / `reject NAME [--reason TEXT]` | List, sign or refuse `ants add --enroll` requests with the account key (`--public-key KEY` disambiguates; approval through a leaf node also renews its uplink credentials and outbox admission); `approve NAME --webhook --to ANT --channel CH` signs a webhook's for `ants webhook add --enroll`. |
| `ants signer [--config FILE] [--approval-timeout DUR] [--dir DIR]` | On the machine holding the account key, sign the credentials of the ants manager ants ask for with `request_ant`, until stopped: at once within the policy of the file's `[[ant_template]]` tables, else on a human's approval (default `15m` to decide); revoke those of the ants they retire with `retire_ant` (see [Manager ants](#manager-ants)). |
| `ants retire NAME [--config FILE] [--dir DIR] [--no-push]` | Retire an ant added from a template, whichever manager added it: unlist it, wait up to `5m` for the daemon to stop it, revoke its credentials (when `--dir` holds the account key, else print the `ants auth revoke --key` to run there) and remove their file, keeping its home (see [Retiring an added ant](#retiring-an-added-ant)). |
| `ants enroll renewals` / `renew NAME [--public-key KEY]` / `renew --auto` | List the `ants renew` requests heard, renew the credentials `NAME` asks for, or keep renewing every valid, unrevoked request as it arrives until stopped (see [Renewing over NATS](#renewing-over-nats)). |
| `ants renew [FILE] [--config FILE] [--leaf-config FILE]` | Renew the credentials of `FILE` (default `--creds`) over NATS for a key generated here, wait for `ants enroll renew`, replace the file and reload the daemon or restart the leaf node using it; a leaf uplink's request goes over a human's `--creds`. |
| `ants keys store` / `unstore` / `remove [FILE...]` | Move the NKey seed of each credentials file (default `--creds`) into the OS credential store once it authenticates at `--nats`, back into the file, or delete the file and its stored seed (see [Private keys in the OS credential store](#private-keys-in-the-os-credential-store)). |
| `ants harness check` / `prompt TEXT` | Check the harness is installed and signed in (ACP handshake and `session/new` in the current directory; names the sign-in command when signed out) or run one prompt without NATS (`--harness NAME`, `--config FILE`; rejects tools). |
| `ants setup [--ant NAME --workdir DIR] [--harness NAME] [--config FILE] [--nats-server PATH] [--no-services] [--no-harness] [--no-completions] [--dry-run]` | Bring up ants on this machine, each step left alone when already done, then run `ants doctor` (see [Setup](#setup)). |
| `ants completions SHELL` / `--install [SHELL]` | Print the script registering completions for `bash`, `zsh` or `fish`, or write it where the shell loads it from (see [Shell completions](#shell-completions)). |
| `ants doctor [--config FILE] [--leaf-config FILE] [--dir DIR] [--webhook-config FILE] [--no-harness] [--json]` | Check the machine's setup end to end and print the command fixing each problem; exits with status 1 when a check fails (see [Doctor](#doctor)). |
| `ants auth init` / `user NAME` / `enroll NAME` / `channel-manager` / `leaf MACHINE` | Create team keys and `server.conf`, or issue `.creds` (`user` and `channel-manager`: `--store-key` keeps the new seed in the OS credential store; `user`: `--human`, `--webhook --to ANT --channel CH`, `--observer` (read-only, for [hooks](docs/hooks.md)), `--label LABEL` for a human's device; `enroll`: `--ttl DUR`, default `1h`, `0` for no expiry; `leaf`: `--ant`, `--human`, `--webhook` name the machine's members, `--add-*`/`--remove-*` change those of the last run, which a run without them reissues, `--enroll-approver` (`--no-enroll-approver`) for the account-key machine, and writes the leaf's system account credentials beside the uplink's). |
| `ants auth revoke NAME` / `revoked` / `push` | Revoke every key issued to `NAME` (or `--key U...`, or only the human's `--device DEVICE`), rewrite `server.conf` and push the account to the `--nats` server and the leaf nodes joined to its system account (`--no-push` to skip); list revoked keys; push the account again. |
| `ants auth issued [--json]` | List the keys issued (key, name, kind, issue time, expiry, permissions version, revoked or reissued), with the command renewing those in use that are outdated or expire within 7 days. |
| `ants auth backup FILE` / `restore FILE [--force]` | Write the team keys, `server.conf`, `issued.jsonl` and `revoked.json` to `FILE` encrypted with a passphrase (`--passphrase-file FILE`, `ANTS_BACKUP_PASSPHRASE` or the terminal), or restore them into the team directory (see [Backing up the team keys](#backing-up-the-team-keys)). |
| `ants auth devices HUMAN [--json]` / `rename HUMAN DEVICE LABEL` | List a human's devices (id, label, key, issue time, revoked); relabel one. `DEVICE` is an id, a key, or a label no other device of the human has. |
| `ants leaf config MACHINE --uplink URL ...` / `outbox MACHINE ...` | Write `~/.config/ants/leaf.conf`; configure or `--remove` store-and-forward outbox sourcing (`outbox`: members from `--ant`/`--human`, or those the uplink credentials of `--leaf-config FILE` carry, changed by `--add-*`/`--remove-*`). |
| `ants bridge serve` / `connect URL` | Tunnel a leaf uplink over plain HTTPS `POST`/`GET` requests (`--token-file FILE` reads the bearer token from a file). |
| `ants webhook serve [--config FILE] [--listen ADDR]` | Serve HTTP webhook ingress, and push tool requests, escalations and, when asked for, replies to humans' ntfy, Slack, Telegram, commands or signed HTTP endpoints with approve/reject links (see [Webhooks](#webhooks) and [Notifications](#notifications)). |
| `ants webhook notify-test HUMAN [--config FILE]` | Send a test notice through each sink of `HUMAN` in the webhooks' config file and print `ok` or `fail` with the error per sink; exits with status 1 when one fails (see [Notifications](#notifications)). |
| `ants webhook add NAME (--to ANT \| --channel CH) [--preset github\|gitlab\|grafana] [--config FILE] [--dir DIR] [--out FILE] [--enroll CREDS [--hub URL]]` | Issue a webhook's credentials (or ask for them over NATS), write a new secret file, append its `[[webhook]]` entry, reload the webhook service and print the URL and secret for the sender (see [Adding a webhook](#adding-a-webhook)). |
| `ants hook run --on EVENT [--concurrency N] [--timeout DUR] -- CMD [ARGS...]` | Run `CMD` once per tool request, verdict, dead letter, escalation, status update or roster entry, with its JSON document on stdin, using observer credentials (see [Hooks](#hooks)). |
| `ants mcp --socket PATH --token TOK` | Internal stdio-to-socket MCP bridge launched by `ants daemon` (a named pipe on Windows). |

`THREAD` in `ask`, `inbox`, `post`, `notes`, `thread show`, `thread title`, `thread pin` (and `unpin`, `archive`, `unarchive`), `cancel`, `watch`, `spend --thread` and `channel read --thread` is a whole thread ID (a UUID, used without any lookup), a unique start of one, or `last`. A start matches the threads with messages the CLI human sent or received (inbox both ways, own channel and notes posts) stored in the last 7 days; when none of those starts with it, every thread with a message stored in the last 7 days, such as one between ants. For a start of a time-ordered ID (as generated ones are), that second lookup reads only the messages stored while such an ID was made, give or take 5 minutes of clock skew and up to 10 minutes after, so a longer start reads less. An exact ID among the threads matched wins; a start matching several is refused with them listed (ID, latest message time, route and first line), and one matching none with the human's own threads listed. `last` is the thread of the latest message the human sent or received, however old. Both read the stream by message gets (see `ants thread` below) and need human credentials.

### Identity and human registration

Every command accepts `--nats`, `--creds` and `--js-domain`. Where neither the flag nor its environment variable sets one, it comes from the config file (`ANTS_CONFIG`, default `~/.ants/config.toml`, read only if it exists): `[cli]` `nats`, `creds` and `js_domain`, else the top-level `nats` and `js_domain`, else the built-in default; an unreadable file or a malformed `[cli]`, `nats` or `js_domain` fails the command. `ants daemon` and `ants add` take none of these, as their flags describe ants and they read the file's top-level keys themselves. A NATS URL carrying a user and password or a token (`nats://user:pass@host`), on the command line or in a config file, is refused: credentials come only from `.creds` files. The CLI acts as the user named in the `--creds` JWT, or `human` without credentials. With human credentials (`ants auth user NAME --human`, or `human`), connecting ensures `ants_humans` exists and registers `<name>` (direct get first, put if absent; `human` needs no entry; warns and continues on failure, though daemons treat unregistered senders as agents until registered), and the first connection also brings `ANTS` and the KV buckets up to date as `ants init` does (see [Credentials](#credentials)). With ant credentials, the CLI acts as that ant, skips `ants_humans`, warns, refuses `ants inbox` and leaves replies unread during `ants ask --wait` or `--follow`.

### Setup

`ants setup` brings up a single machine: a local hub, the config file and the services, step by step. It prints one line per step, `done`, `exists` (done before, kept as it is), `skip` or `fail`, and never overwrites a file, so running it again is safe and reports what already exists. `--dry-run` prints `would` for what each step would do and writes, starts and checks nothing. In order:

- **nats-server config**: `~/.config/ants/nats.conf`, JetStream in `~/.ants/jetstream`, listening where `--nats` (default `nats://127.0.0.1:4222`) points, on this machine only. Skipped when `--nats` is no loopback `nats://` address or a leaf node configuration (`~/.config/ants/leaf.conf`) exists: the machine then joins a hub as [Leaf nodes](#leaf-nodes) describes.
- **nats-server service**: the `ants.nats` / `ants-nats` unit of [Services](#services) running that file, installed and started; skipped when a server already listens there. With `--no-services`, the command starting nats-server is printed instead.
- **ants init**: stream `ANTS` and the KV buckets, once the server listens.
- **config**: the config file (`--config`, `ANTS_CONFIG`, default `~/.ants/config.toml`) serving the built-in messenger (`messenger = true`), with `--harness` and a `nats` other than the default, and the ant of `--ant NAME --workdir DIR`. An existing file is kept; only an `--ant` it does not list is appended.
- **harness**: `ants harness check` with the config file's harness (`--harness`, else its top-level `harness`, else `copilot`; `--no-harness` skips it).
- **daemon service**: the `ants.daemon` / `ants-daemon` unit running the config file, installed and started, or reloaded when running and the config file changed. With `--no-services`, the command starting the daemon is printed instead.
- **completions**: `ants completions --install` for the shell `$SHELL` names (`--no-completions` skips it).
- **doctor**: `ants doctor` without the harness checks.

Where no service manager ants installs units for runs, the service steps print the command to start by hand. Units come from the templates in `contrib/` with the paths of `ants`, `nats-server` (`--nats-server`, default the one on `PATH`) and the current `PATH`, as `scripts/install-units.sh` renders them; `scripts/install-units.sh --nats-conf ~/.config/ants/nats.conf` renders them again. It exits with status 1 when a step or a doctor check fails.

### Shell completions

`ants completions bash|zsh|fish` prints a script registering completions; `ants completions --install [SHELL]` writes it where the shell loads it from (`$SHELL` names the default shell): `~/.local/share/bash-completion/completions/ants` (bash-completion 2 loads it; otherwise `source` it from `~/.bashrc`), `~/.local/share/zsh/site-functions/_ants` (add the directory to `fpath` before `compinit` in `~/.zshrc`) or `~/.config/fish/completions/ants.fish` (`XDG_DATA_HOME` and `XDG_CONFIG_HOME` move them). `ants setup` and `scripts/install-units.sh` install them too.

The script runs `ants` with `ANTS_COMPLETE` set for each completion. Besides commands and flags, it completes values looked up over NATS as the command line being completed would connect (its `--nats`, `--creds`, `--js-domain` and `--config`, else as other commands do): ant names (`@ant` in `ask` and `post`) from `ants_roster`, the config file and `messenger`; channels; `last` and the threads the CLI human sent or received on in the last 7 days, with their first line; pending tool requests; dead letter sequence numbers. A lookup gives up after 2 seconds and completes nothing on any error; it never prompts, so credentials whose seed is in the OS credential store complete no values.

### Doctor

`ants doctor` checks the machine's setup end to end and changes nothing. It prints one line per check, `ok`, `warn`, `fail` or `skip`, with a short detail, and under each warning or failure the command fixing it (`--json`: one object per check, with `check`, `status`, `detail` and `fix`). It exits with status 1 when a check fails. In order:

- **config**: the config file (`--config`, `ANTS_CONFIG`, default `~/.ants/config.toml`) loads as `ants daemon` loads it.
- **credentials**: the CLI's `--creds` (or `[cli]` `creds`): holder, permissions version and expiry, as `ants auth issued` reports them, with the renewing command.
- **credentials key** and **ant NAME key**: for credentials whose seed is in the OS credential store (see [Private keys in the OS credential store](#private-keys-in-the-os-credential-store)), the store answers, holds the entry, and the seed stored is the key of their user JWT; a failure tells a locked or unavailable store (fix: unlock it) from a missing or mismatched entry (fix: restore it from a backup of the store, or issue the credentials again), and never shows the seed. Without the seed, **server** is skipped.
- **server** and **jetstream**: the `--nats` server accepts the credentials, and JetStream answers in the `--js-domain` they were issued for; through a leaf node, the hub's JetStream rather than the leaf's own.
- **hub**: stream `ANTS` and the KV buckets exist and match this version (fix: `ants init`); skipped with an ant's credentials, which get no stream info.
- **consumers**: the consumers of stream `ANTS`, and of the streams of the buckets every daemon, channel manager or standby daemon watches (`KV_ants_roster`, `KV_ants_channels`, `KV_ants_humans`), each against the most it takes, its `max_consumers` (10000 since `ants init` sets it), else the 1000 that nats-server 2.15 and later give a stream without one: a warning from four fifths of it, as once it is full no ant can open a consumer to read its inbox, or no daemon can watch the bucket (fix: `ants init`, or `nats stream edit STREAM --max-consumers` with a higher limit). `ANTS` is skipped with an ant's credentials.
- **versions**: the ants and permissions versions every live daemon, `ants webhook serve`, schedule and `ants channel serve` publishes, and whether renewing credentials with this ants suits them all (see [Credentials across upgrades](#credentials-across-upgrades)): a warning names the components on an older permissions version, or publishing none (fix: upgrade and restart them before renewing), or on a newer one (fix: upgrade ants here); mixed ants versions, and what could not be read, show in the detail. Skipped with an ant's credentials.
- **harness versions**: the harness agents live ants run, as their roster entries name them (`agent`); a warning names each agent running at different versions on different machines, with which ants run which version (fix: upgrade the older ones; an entry shows the new version once the ant's harness starts again). Different versions on one machine only, a harness upgraded there while an older one still runs, pass.
- **storage**: the JetStream usage of each store (file, and memory when used or limited) of the hub, and of the leaf node the CLI connects through, against its limit (see [JetStream storage](#jetstream-storage)): a warning past `warn_percent` of `[storage]` (default 80%), naming the streams using most, with the setting to raise; an unknown limit names the `[storage]` setting giving it. Skipped with credentials that may not read the account's JetStream info.
- **ant NAME credentials** and **ant NAME harness**: each config-file ant's credentials are an ant's of its name, for its JetStream domain; its harness is installed and signed in (the ACP handshake of `ants harness check`, one per harness and environment, all at once; `--no-harness` skips them).
- **ant NAME workdir**: a warning for each ant with `max_harnesses` above 1 and `worktrees` off whose workdir is inside a git work tree (a `.git` directory or file in it or above it), where threads running at once may clobber each other's edits (fix: `worktrees = true` in its `[[ant]]`).
- **hub resolver**: on the machine holding the team keys (`--dir`, `ANTS_AUTH_DIR`, default `~/.config/ants/auth`), the `--nats` server, asked as a user of the team's system account, keeps pushed accounts across restarts (a `full` resolver); a warning otherwise, with the `resolver` line to add, and when the server refuses the system account (a `server.conf` from before revocations or of another team). Skipped without the team keys and when the CLI connects through the leaf node.
- **team keys backup**: on the machine holding the team keys, a warning when no `ants auth backup` of them is recorded, or when keys were issued, renewed or relabelled (`issued.jsonl` grew) or revocations changed since the last one (fix: `ants auth backup FILE`). No line without the team keys.
- **leaf**, **leaf uplink**, **leaf system uplink**, **leaf node**, **outbox**: with a leaf node configuration (`--leaf-config`, default `~/.config/ants/leaf.conf`), its uplink credentials carry the machine's ants and the CLI's human that connect through it, the system account uplink is configured, the leaf node listens where the CLI connects, and, with human credentials, the hub sources the machine's outbox for every member.
- **daemon service** and **leaf node service**: the units of `scripts/install-units.sh` are installed for the config file and the leaf node configuration, and running (see [Services](#services)); with the webhooks' config file, **webhook service** too.
- **webhook secrets**, **webhooks** and **public_url**: with the webhooks' config file (`--webhook-config`, default `~/.ants/webhooks.toml`; no lines without it), each `secret_file` it names (webhooks, sinks, the action secret) exists with mode 0600, as `ants webhook serve` refuses one readable by others (fix: `chmod 600 FILE`, or write the file); a `secret_env` unset in the shell running `ants doctor` warns, as only the service's environment may hold it. The file then reads as `ants webhook serve` reads it, without checking credentials; the program of each `exec` sink (**notify commands**) must be an executable file at its path, or, for a bare name, on the `PATH` of the shell running `ants doctor` (a warning otherwise, as the service's `PATH` may differ: give the full path); and `GET <public_url>/_approval` must answer as `ants webhook serve` does (`400`, incomplete link), within 5 seconds: a warning otherwise, as approval links would not open (the machine may also fail to reach its own public address). Sinks themselves are tried with `ants webhook notify-test HUMAN`.
- **devtunnel service**, **devtunnel host service**, **devtunnel login**, **devtunnel token**: with the Dev Tunnel units of `scripts/install-units.sh` installed (`--devtunnel`, `--devtunnel-host`; no lines without them), each is running and its devtunnel binary is there. `devtunnel connect` with a token file (`--devtunnel-token`) needs the file present, mode 0600 (a warning otherwise), and its connect token not expired, a warning within 6 hours of its `exp` (a token that is no JWT counts as expiring a day after the file was written; fix: `devtunnel token TUNNEL --scopes connect` into the file, then restart the service). Otherwise, and for `devtunnel host`, `devtunnel user show` must report a login (fix: `devtunnel user login`, then restart the service), a warning within a day of its expiry when it shows one.
- **bridge service**, **bridge token**, **bridge url**, and **bridge serve service**, **bridge serve token**, **bridge serve url**: with the units of `ants bridge connect` or `ants bridge serve` installed (`--bridge`, `--bridge-serve`; no lines without them), each is running, its `--token-file` exists with mode 0600 (a warning for other modes; `ants bridge serve` without a token warns), and its bridge answers within 5 seconds as a bridge taking the token: `ants bridge connect`'s URL, through its `--proxy` and trusting its unit's `SSL_CERT_FILE`, and `ants bridge serve`'s listen address. A refused token fails with the file to correct; no answer names what to restart or reach.

A check that cannot run, such as one needing a file that does not exist or rights the credentials lack, is `skip` with the reason, not a failure. Every network check gives up within 10 seconds (each request after 5), a harness handshake after 60.

### Environment variables

| Variable | Flag / Scope | Default / Behavior |
|---|---|---|
| `ANTS_NATS_URL` | `--nats` (global) | Config file's `[cli]` or top-level `nats`, else `nats://127.0.0.1:4222` |
| `ANTS_CREDS` | `--creds` (global) | Path to `.creds` file, else the config file's `[cli]` `creds`; omitted connects unauthenticated as `human` |
| `ANTS_JS_DOMAIN` | `--js-domain` (global) | Hub JetStream domain (`$JS.<domain>.API`), else the config file's `[cli]` or top-level `js_domain` |
| `ANTS_CONFIG` | `--config` (`daemon`, `add`, `harness`, `spend`, `setup`); the global connection defaults | `~/.ants/config.toml` |
| `ANTS_COMPLETE` | Set by the completion scripts | Print completions for the shell it names instead of running a command (see [Shell completions](#shell-completions)) |
| `ANTS_ANT` | `--ant` (`ants daemon`) | Serve only the named ant (from config file if `--config` is set, else from CLI flags) |
| `ANTS_METRICS` | `--metrics` (`ants daemon`) | Address for `/healthz` and `/metrics`, else the config file's `metrics`; off by default (see [Monitoring](#monitoring)) |
| `ANTS_AUTH_DIR` | `--dir` (`auth`, `enroll`, `leaf config`, `doctor`) | `~/.config/ants/auth` |
| `ANTS_BACKUP_PASSPHRASE` | The passphrase of `ants auth backup` and `restore` when no `--passphrase-file` is given | Unset: read from the terminal without echo |
| `ANTS_BRIDGE_TOKEN` | `--token` (`bridge serve`, `bridge connect`) | Optional bearer token gating bridge sessions; `--token-file FILE` reads it from a file instead |
| `ANTS_MCP_TOKEN` | `--token` (`ants mcp`) | Per-thread session token passed automatically by `ants daemon` |
| `ANTS_UPGRADE_CHECK` | `--upgrade-check` (`daemon`, `channel serve`, `webhook serve`, `bridge serve`, `bridge connect`) | `30s`: how often to check for a replaced executable and exit with status 75 ([Upgrade](#upgrade)); `0` never checks |
| `ANTS_LOG` | All commands | `tracing` stderr filter, default `info,rmcp=warn` |
| `SSL_CERT_FILE` | TLS / `ants bridge connect` | PEM CA bundle trusted by rustls for private CAs |
| `ANTS_REQUIRE_TOOLS` | `cargo test` | When `1`, tests fail instead of skipping if `nats-server` is missing on `PATH` |
| `ANTS_TEST_JS_ACK_FC_V2` | `cargo test` | When `1`, test servers run with `feature_flags { js_ack_fc_v2: true }`, the v2 ack and flow control subjects of nats-server 2.16 (needs nats-server 2.14 or later) |

### Agent skill

`contrib/skills/ants/SKILL.md` teaches an external coding agent to message ants through the CLI (`ants roster`, `ants ask`, `ants inbox --thread`, `ants watch`). Install with `copilot skill add contrib/skills/ants/SKILL.md` or copy into another agent's skills directory.

## Delivery

### Composing messages

`ants ask` and `ants post` take the message as the words after `@ant` (or after the channel), joined by spaces, or from one of:

- `--file PATH`: the file's text, its lines kept as they are (inner blank lines, indentation, `\r\n`) but for the trailing line breaks and a leading byte order mark. `--file -` reads stdin to its end; stdin is read only then, so a script piping nothing, or a service whose stdin never closes, neither hangs nor sends anything unasked.
- `--edit`: opens an editor on a new file in the temporary directory (mode `0600`, removed afterwards) and sends what it holds once the editor exits. The editor is the command line in `VISUAL`, else `EDITOR`, else `vi` (`notepad` on Windows), run as git runs it: through `sh -c` (`cmd /S /C` on Windows) with the file's path appended, so `EDITOR="code --wait"` works.

A leading `@ant` word still picks the recipient of `ants ask` with `--file` or `--edit`; any other word with them is refused. Nothing is sent, with an error and a non-zero exit, when the message is empty or blank, when both `--file` and `--edit` are given, when the editor exits non-zero or is not found, or when the file or stdin is unreadable or not UTF-8. Saving an empty file in the editor cancels the message the same way.

```sh
ants ask @dev-a --file plan.md                         # recipient from the words, message from the file
git diff --stat | ants post ops --file -               # stdin, only when asked for
EDITOR="code --wait" ants ask --wait --edit @dev-a     # write in VS Code, then wait for the reply
ants post ops --thread last --edit                     # follow up on a channel thread from the editor
```

### Waiting for replies

`ants ask` and `ants post` print the thread ID and exit immediately unless `--wait` or `-f`/`--follow` is set (mutually exclusive):

- `--wait` prints the first reply on the thread (to the CLI human for `ants ask`; by another author for `ants post`) and exits `0`.
- `-f`, `--follow` prints every arriving reply or post on the thread until `--timeout` or Ctrl-C, then exits `0`.
- `--timeout DURATION` (requires `--wait` or `--follow`) bounds the wait. If `--wait` times out, `ants` prints `no reply on thread <thread> within <timeout>` to stderr and exits `1`. Ctrl-C during `--wait` exits immediately as interrupted (`130`). `ants ask --wait` and `--follow` mark only printed replies read, and warn up front that the wait may not end while no daemon serves the ant.
- With human credentials and stderr on a terminal, `--wait` and `--follow` keep one status line on stderr, rewritten in place from `ants.status.<ant>.<thread>` (every ant on the thread for `ants post`) and cleared before any reply, log line or error is printed. It shows `not served` while no daemon serves the ant, `queued behind N` while earlier messages of the thread go first, `working`, `thinking`, `writing`, `running <tool>`, `waiting for approval` with the `ants tools approve <ant>/<id>` command answering the request, and `harness signed out` while the messages wait for a sign-in. An idle ant drops off the line. Off a terminal, or with ant credentials, nothing is shown.

### Inbox consumers, pool and retries

Human replies stay in `ANTS` (30 d) and count as unread until the human's read marker in `ants_read` says otherwise: every device of the human reads them all by sequence, and reading on one marks read on all. Each ant maintains up to `max_harnesses` (default 4) ACP processes, one per active thread: concurrent across threads, sequential per thread, retrying failures in order. Idle harnesses stop after `idle_timeout` (`10m`) and resume via ACP `session/load` using `<ant>.<thread>` in `ants_sessions`; busy harnesses are never evicted. A session that cannot be resumed (see [Lost sessions and the context window](#lost-sessions-and-the-context-window)) starts over from the thread's history. First Ctrl-C or `SIGTERM` drains prompts in flight; a second Ctrl-C exits immediately for redelivery.

Threads of one ant share its workdir unless `worktrees = true` (or `--worktrees`) gives each thread its own git worktree of the workdir's repository: `<home>/worktrees/<thread>` on branch `ants/<ant>/<thread>`, started from the repository's `HEAD` (uncommitted changes in the workdir are not carried over). Sessions of the thread work at the workdir's place inside it, and allowlist paths resolve against it. The worktree is created on the thread's first prompt, reused when its harness starts again (`session/load` sees the same directory), and removed with `git worktree remove --force` once `<ant>.<thread>` has expired from `ants_sessions` (checked at start and hourly, for threads with no live harness), deleting the branch unless it holds commits not merged into the repository's `HEAD`. A reply whose prompt ends with such commits on the thread's branch carries an `[ants] Branch ants/<ant>/<thread> holds N commits not in the repository's HEAD.` line, and a branch kept at removal is told in an `[ants]` message on the thread to its owner, else the default human. Worktrees left under `<home>/worktrees` after `worktrees` is turned off are removed the same way (their repository found from the worktrees themselves), while the threads share the workdir. A workdir outside a git work tree, or a missing `git`, is logged once and stays shared; a worktree that cannot be created (an empty repository, the branch checked out elsewhere) logs a warning and the thread works in the shared workdir. With `worktrees` off, `max_harnesses` above 1 and a workdir inside a git work tree, the daemon logs a warning at start, as threads running at once may clobber each other's edits there; `ants doctor` warns too.

Held messages send 20s progress acks against a 60s `ack_wait` (ignoring duplicate deliveries while held). Delivery is at-least-once and acks only after storing the reply:

1. Before prompting, the daemon writes `<ant>.<seq>` in `ants_attempts`. On redelivery after a cut-off, finding that marker resumes the session and prepends a warning that prior tool calls may have run.
   Before that, as it takes a message up, the daemon creates `<ant>.msg.<id>` (`started`) unless the key has a value: a `withdrawn` value there means a human withdrew the message first (see [Queue and withdrawal](#queue-and-withdrawal)). Once the message is acked or dead-lettered, the value becomes `finished`; it expires with the bucket's 30 d.
2. When the harness finishes, the reply is saved in `<ant>.<seq>` before publishing with dedup ID `reply-<ant>-<envelope_id>` (24 h window), so a post-generation cut-off republishes without re-prompting.
3. Stored replies are also noted in `<home>/answered` until acked, avoiding re-prompts when an offline leaf buffers replies in its outbox. Markers clear on ack or dead-lettering (30d TTL).

During prompts, the daemon streams unpersisted `StatusEvent` JSON to `ants.status.<ant>.<thread>` (`working`, `waiting_approval`, `idle`, `failed`, `signed_out`, 250ms batched text/reasoning, tool calls, execution plan), dropping backpressured frames. A message arriving while earlier ones of its thread are in flight or waiting gets `queued` with its envelope id and the number `ahead` of it, again each time one starts, and `ahead: 0` when its own turn comes; `approval` carries the request id just before `waiting_approval`; `signed_out` comes when a message arrives while the harness is signed out, or its prompt finds the harness signed out; `escalated` names the recipient and carries the escalation of each escalation message the ant's daemon stores (see [Escalations](#escalations)). `ants watch <ant>` prints them all.

## Config file

`~/.ants/config.toml` (`--config` / `ANTS_CONFIG`) configures multi-ant daemons, shared defaults and custom harnesses. An empty file is valid and serves the built-in `messenger` on `copilot`.

```toml
harness = "copilot"                        # default for every ant and built-in messenger
messenger = true                           # default: serve built-in messenger unless [[ant]] names "messenger"
nats = "wss://hub.example/nats"            # default NATS URL for ants without `nats`
js_domain = "hub"                          # default hub JetStream domain; also settable per ant
human = "alice"                            # fallback human for ownerless escalations (default "human"); also per ant
metrics = "127.0.0.1:9464"                 # serve /healthz and /metrics (default: off); see Monitoring
timezone = "America/New_York"              # default time zone of schedules' cron (default UTC); see Schedules
redact = true                              # redact secrets from what ants publish (default true); see Redaction

[cli]                                      # the CLI's connection where no flag or environment variable sets it
creds = "~/.config/ants/auth/alice.creds"  # the human's credentials (an ant's never apply)
nats = "nats://127.0.0.1:4222"             # default: the top-level nats
js_domain = "hub"                          # default: the top-level js_domain

[storage]                                  # see JetStream storage
warn_percent = 80                          # warn the humans past this share of a store's limit (default 80; 0: never)
max_file_store = "100GB"                   # the hub's max_file_store (default: unknown); bytes, or K/M/G/T(B), 1024-based
max_memory_store = "1GB"                   # the hub's max_memory_store (default: unknown)

[[schedule]]                               # see Schedules
name = "nightly"
creds = "~/.config/ants/auth/nightly.creds"
cron = "0 3 * * 1-5"
ant = "dev-a"
text = "Triage the failing nightly tests."
allow = ["read:*", "execute:cargo test*"]  # optional: narrows dev-a's allow on its threads; default read-only

[[ant]]
name = "dev-a"
workdir = "~/src/repo-a"                   # must exist
creds = "~/.config/ants/auth/dev-a.creds"
harness = "claude"                         # overrides top-level harness
model = "claude-sonnet-4-5"                # optional exact ACP model ID
reasoning_effort = "high"                  # optional effort ID offered by the model
context = "1m"                             # optional key from the harness's context table
history_budget = 131072                    # optional bytes of thread history seeding a rebuilt session
home = "~/.ants/dev-a"                     # default: ~/.ants/<name>
whoami = "Maintain Rust backend services." # published in roster and prepended to prompts
memory = "Use notes MCP for durable memory."
tasks = "Use bd for task tracking."
env = { BD_DB = "/srv/ants/dev-a/bd.db" }  # literal environment overrides for harness + subprocesses
allow = ["read", "search", "execute:cargo test*"]
allow_tools = false                        # true equals allow = ["*"]
approval_timeout = "5m"                    # default
capabilities = ["rust", "backend"]         # routing tags published in roster
max_harnesses = 4                          # default
worktrees = false                          # default; true gives each thread its own git worktree
idle_timeout = "10m"                       # default
prompt_timeout = "30m"                     # default; "0" disables
max_tool_calls = 300                       # default; 0 disables
max_thread_cost = 5.0                      # optional; also max_thread_tokens
max_daily_cost = 50.0                      # optional; also max_daily_tokens
spend_warn_percent = 80                    # default; warn once past this share of a cap; 0 disables
escalate_to = ["lead", "dev-b"]            # optional escalation chain; first ant leads
max_escalation_hops = 3                    # default
escalate_after = "30m"                     # default; "0" waits indefinitely
escalation_reminder = "4h"                 # default; remind a human of an unanswered escalation; "0" disables

[[ant]]
name = "dev-b"
nats = "nats://127.0.0.1:4222"
workdir = "~/src/repo-b"
creds = "~/.config/ants/auth/dev-b.creds"

[[ant_template]]                           # see Ant templates
name = "worker"
managers = ["lead"]
max_ants = 3
workdirs = ["~/src/repo-a"]
models = { "claude-sonnet-4-5" = "high" }

[[ant]]                                    # added from a template: other settings come from it
name = "lead-docs"
template = "worker"
added_by = "lead"
creds = "~/.config/ants/auth/lead-docs.creds"
model = "claude-sonnet-4-5"
```

`~` expands to the home directory; other relative paths resolve against the config file's directory (`env` values remain literal). Unknown keys, duplicate ant, template or schedule names, a schedule named after an ant, or `messenger = false` with no `[[ant]]` fail config loading. Unreachable NATS or JetStream endpoints retry with exponential backoff (`1s` to `30s`); credential or permission errors fail that ant immediately while remaining ants keep running, exiting non-zero only if none start.

Flag and file precedence on `ants daemon`:

- `ants daemon`: serves all ants in `~/.ants/config.toml` (plus built-in `messenger` when enabled) and runs its schedules.
- `ants daemon --ant dev-a` (`ANTS_ANT=dev-a`) without `--config`: ignores `[[ant]]` entries in `~/.ants/config.toml` and configures `dev-a` from CLI flags, while still loading `[harnesses.NAME]` definitions from `~/.ants/config.toml` if present and from `~/.ants/harnesses.d/` (see [Drop-in harness files](#drop-in-harness-files)).
- `ants daemon --config FILE --ant dev-a`: serves only `dev-a` from `FILE` (`--ant messenger` selects the built-in messenger), with CLI ant flags overriding `FILE`; an ant added from a template refuses ant flags. With `--ant`, no schedule runs.
- Per-ant, per-schedule and top-level `nats`, `js_domain` and `creds` in the file take precedence over `--nats`, `--js-domain` and `--creds`, which fill in omitted values (`--creds` applies only when serving a single ant, never to schedules).

`SIGHUP` reloads the config file without a restart, and so does a change to the file itself or to its drop-in harness files: the daemon checks the file's modification time, length and inode, and those of the `*.toml` files in `harnesses.d` beside it (a file added, removed, replaced or edited), every 3 s and reloads once a change stayed the same across two checks (so within about 6 s of the last write; a `SIGHUP` reload counts as having read it). On a reload, ants newly listed start, ants no longer listed stop as on Ctrl-C (prompts in flight finish, waiting messages go back for redelivery), and ants whose resolved settings changed (including a `[harnesses.NAME]` they use, a top-level default or the `[[ant_template]]` they were added from) stop the same way and start again with the new settings; unchanged ants keep running untouched (a template's pool taking in or losing an ant changes no member's settings). An ant that failed earlier and is still listed starts again. Schedules newly listed start, schedules no longer listed stop at once, and schedules whose entry changed or that stopped start again. A file that fails to load is logged and the ants served and schedules run stay as they were.

### Ant templates

An `[[ant_template]]` bounds the worker ants a manager ant may add: each such ant is an `[[ant]]` entry naming the template (`template`) and the manager that added it (`added_by`), and setting only `name`, `creds`, `nats`, `js_domain` and the keys a request may choose below; any other key fails config loading. Its other settings come from the template each time the file loads, and on every load and `SIGHUP` reload each such ant is checked against the template as it is then: a setting outside its bounds fails loading with an error naming the bound. Its escalations go to its manager first (`escalate_to = [manager]`). Added ants never add ants: neither a template's `managers` nor an `added_by` may name an ant added from a template.

```toml
[[ant_template]]
name = "worker"                            # lowercase letters, digits, '-' and '_'
managers = ["lead"]                        # ants that may add ants from it, by exact name; default none
name_pattern = "<manager>-*"               # default; <manager> is the manager's name, * any text
max_ants = 3                               # required; ants the file may list from it
workdirs = ["~/src/repo-a", "~/src/repo-b"] # required; allowed workdirs, the first the default
harnesses = ["claude"]                     # allowed harnesses, the first the default; default: the top-level harness only
models = { "claude-sonnet-4-5" = "high", "claude-haiku-4-5" = "max" } # model ID = highest reasoning_effort
contexts = ["1m"]                          # allowed context values; default: the harness default only
max_thread_cost = 2.0                      # ceilings and defaults: max_thread_tokens/cost, max_daily_tokens/cost
max_daily_cost = 10.0
max_harnesses = 2                          # ceiling and default; default 4
prompt_timeout = "20m"                     # ceiling and default; default "30m"; "0": no ceiling
max_tool_calls = 300                       # ceiling and default; default 300; 0: no ceiling
allow = ["read", "search"]                 # tool rules every ant from it has
allow_sets = { build = ["execute:cargo build*", "execute:cargo test*"] } # named rule sets an ant may pick
pool_daily_cost = 25.0                     # optional; all its ants together per UTC day; also pool_daily_tokens
whoami = "Implement what the manager assigns." # defaults an ant may replace: whoami, memory, tasks, capabilities
capabilities = ["rust"]
env = { CARGO_TARGET_DIR = "target" }      # fixed for its ants: env, worktrees, idle_timeout,
worktrees = true                           # approval_timeout, spend_warn_percent (defaults as for [[ant]])
```

An ant added from a template may set, within the template's bounds: `workdir` (one of `workdirs`), `harness` (one of `harnesses`), `model` (one of `models`, required when the template lists any, else not allowed), `reasoning_effort` (ranked `low` < `medium` < `high` < `xhigh` < `max`, at most the model's), `context` (one of `contexts`), `allow_sets` (names from `allow_sets`, added to `allow`), `whoami`, `memory`, `tasks`, `capabilities`, and `max_thread_tokens`, `max_thread_cost`, `max_daily_tokens`, `max_daily_cost`, `max_harnesses`, `prompt_timeout` and `max_tool_calls`, each no higher than the template's (a cap the template leaves unset has no ceiling; a `prompt_timeout` or `max_tool_calls` of `0`, no limit, only where the template's is `0`). Its name must fit `name_pattern`, and the file may list at most `max_ants` ants from the template. Removing a manager from `managers` stops it adding ants; the ants it added stay.

### Manager ants

A manager ant, an ant named in a template's `managers` and not itself added from one, gets the MCP tool `request_ant(name, template, reason, overrides?, timeout?)` (allowlist `other:request_ant`) when its daemon serves the whole config file (not `--ant`): `overrides` holds the settings an added ant may choose, as above (`prompt_timeout` as a duration such as `10m`). The daemon:

1. checks the request against the template of its own config file, refusing at once, with nothing asked or signed, a name, setting or count out of the template's bounds;
2. generates the ant's user NKey on this machine, as `ants add` does (`~/.config/ants/auth/<name>.nk`, `0600`; the seed never leaves the machine), refusing a name whose `<name>.creds` exists;
3. asks `ants signer`, on the machine holding the account key, for the ant's credentials on `ants.antreq.<manager>`, every 5 s, signed by the manager's credentials, until an answer for the key arrives or `timeout` passes (default `15m`, at most `30m`, a human's approval included);
4. writes `<name>.creds` beside the seed (and removes the seed), appends the ant's `[[ant]]` entry with `template`, `added_by` and the settings chosen to the config file, and returns once the daemon, rereading the file within seconds, lists the ant in `ants_roster` (or after a minute, saying the ant is added but not yet listed).

One request runs at a time per daemon process, so two cannot take a template's last place. Behind a leaf node, the signer also renews the leaf's uplink credentials to carry the new ant and the daemon writes them into place, as `ants add --enroll` does, then reconnects the uplink alone, without restarting the leaf node: it rewrites the remote's credentials path in `leaf.conf` to another spelling of the same file and reloads the leaf node service, so the machine's other ants stay connected (nats-server 2.14 or later on the leaf node; without a service to reload, the tool's answer names the command, and until it runs the ant reaches only the ants of its machine). Each ant grows the uplink credentials (about 8 KB with a JetStream domain, 4 KB without): the signer refuses at once, before asking a human or signing anything, an ant the leaf's uplink has no room for, and the tool's result says so; such ants go behind another leaf node. See [docs/transport.md](docs/transport.md#enrollment-behind-a-leaf-node).

`ants signer` trusts only what it checks itself: the request's signature by credentials of the team's account, and the template as its own config file (`--config`/`ANTS_CONFIG`, default `~/.ants/config.toml`, reread for each request; only its `[[ant_template]]` tables count) defines it, never the manager machine's copy. A request is within the policy when the signer's template of that name lists the manager, has fewer than `max_ants` live keys issued from it (neither revoked nor expired; `ants auth revoke NAME` frees a place), and bounds the name and settings; the signer then signs at once a user JWT for the key, named for the ant, with the ant's permissions and the JetStream domain of the manager's credentials, recording `template` and `added_by` in `issued.jsonl`. A request outside the policy goes to the humans as a tool request of the manager on its thread (`request_ant: add ant NAME from template T (why)`), shown by `ants tools`, `ants inbox` and `ants webhook serve`'s ntfy, Slack and Telegram notices, with allow once and reject only; the signer signs on approval and refuses on rejection or once `--approval-timeout` (default `15m`, at most until the manager stops waiting) passes. It refuses outright: a forged or unreadable request, a name of a human, of the manager itself or reserved, a name another live key holds, a manager that was itself added from a template, and more than 3 requests of one manager waiting for humans at once. Requests waiting for a human are forgotten when the signer restarts, and the manager's next repeat asks again.

```sh
ants signer --config ~/.ants/config.toml   # where the team keys are; or scripts/install-units.sh --signer
```

#### Retiring an added ant

A manager ant also gets `retire_ant(name)` (allowlist `other:retire_ant`) for an ant it added (its entry's `added_by`), once its work is done. The daemon, one request at a time with `request_ant`:

1. removes the ant's `[[ant]]` table from the config file, leaving the rest of the file as it is, so the daemon, rereading the file, stops the ant as on any reload: its prompts in flight end first and its roster entry goes;
2. waits up to `5m` for the ant to leave the roster, then carries on, the revocation cutting off prompts still running;
3. asks `ants signer` on `ants.antreq.<manager>` to retire the ant, with the key of its `<name>.creds`, every 5 s for up to `2m`; the signer refuses unless `issued.jsonl` records that key as issued from a template with `added_by` the manager, revokes the key and the keys of the name issued before it (renewed ones) as `ants auth revoke` does, and pushes the account to the hub (its answer says when the push failed: `ants auth push` then);
4. removes `<name>.creds` (and a seed in the OS credential store) and answers with what was done.

The ant's home (`~/.ants/<name>`: memory, task database, worktrees with their unmerged branches) stays: a later `request_ant` of the same name from the same template brings the ant back with it. Once revoked, the ant no longer counts toward the template's `max_ants` nor holds its name, and its spend leaves the pool's total for the rest of the day (and comes back with the ant added again the same day). A `retire_ant` cut off after the entry left the file carries on from `<name>.creds` when asked again. Behind a leaf node the revocation reaches the leaf through its system account uplink, as any push does; the leaf's uplink credentials keep listing the ant until reissued (`ants auth leaf MACHINE --remove-ant NAME`).

A human retires any ant added from a template with `ants retire NAME` on the manager's machine: the same steps, the revocation done in place when `--dir` (default `~/.config/ants/auth`) holds the account key, and otherwise printed as the `ants auth revoke --key U...` to run where it is. Other ants are refused: remove their `[[ant]]` table by hand and run `ants auth revoke NAME`.

```sh
ants retire lead-a   # unlist, stop, revoke and remove the credentials of lead-a; ~/.ants/lead-a stays
```

## Harnesses

A harness definition specifies an ACP stdio executable, arguments, environment additions, required ACP session mode, valid `context` options and the command that signs it in. Built-in definitions live in `crates/ants-daemon/src/harnesses.toml`:

| Name | Command | Forced ACP mode | Sign-in |
|---|---|---|---|
| `copilot` (default) | `copilot --acp` | none | `copilot login` |
| `claude` | `claude-agent-acp` (`npm install -g @agentclientprotocol/claude-agent-acp`) | `default` | `claude login` or `ANTHROPIC_API_KEY` |
| `antigravity` | `agy acp --disable-telemetry` | `default` | `agy` (or `agy acp --login`) or Gemini API key |

Add new harnesses or replace a built-in whole under `[harnesses.NAME]`, in the config file or in a [drop-in file](#drop-in-harness-files):

```toml
[harnesses.custom-acp]
command = "~/.local/bin/custom-acp"            # path if containing '/', else resolved on PATH
args = ["--stdio", "--log-level", "warn"]
env = { CUSTOM_ACP_TELEMETRY = "off" }         # layered: daemon env < harness env < ant env < --env
mode = "default"                               # session fails if harness lacks this mode
sign_in = "custom-acp login"                   # named when the harness reports it is signed out
context_overflow = ["context too long"]        # prompt errors meaning the session outgrew the context window
rate_limited = ["slow down"]                   # error phrases of a rate limit, besides the common ones
quota_exhausted = ["monthly cap reached"]      # error phrases of an exhausted quota, besides the common ones
window = 200000                                # optional context window in tokens; sizes the history seed
history_budget = 65536                         # optional bytes of thread history seeding a rebuilt session

[harnesses.custom-acp.context]
default = {}
large = { args = ["--context-window", "1m"], window = 1000000 }  # appended to launch args; its window
huge = { model_suffix = "-1m", history_budget = 262144 }         # appended to explicit `model`; its budget
```

Codex runs through its ACP adapter as a custom definition (`npm install -g @agentclientprotocol/codex-acp`; sign in with `codex-acp login`, or set `OPENAI_API_KEY` or `CODEX_API_KEY` together with `DEFAULT_AUTH_REQUEST`):

```toml
[harnesses.codex]
command = "codex-acp"
mode = "read-only"                             # default `agent` lets Codex's auto-reviewer approve actions
sign_in = "codex-acp login"
# env = { DEFAULT_AUTH_REQUEST = '{"methodId":"api-key"}' }  # sign in with the API key from the environment
```

- **Claude Code:** forced to `default` mode (`auto` approves tools internally). Built-in read-only tools/commands (`Read`, `Grep`, `ls`, `echo`) and `~/.claude/settings.json` / `.claude/settings*.json` rules still run unasked; loads `CLAUDE.md` and hooks.
- **Codex:** `read-only` asks before edits, network access and commands that need to leave Codex's read-only sandbox; commands that only read run in that sandbox unasked (`workspace-write` also edits the workdir unasked, `agent-full-access` never asks). Reports MCP tool calls as `execute` titled `mcp.<server>.<tool>` (matched by `other:` rules). Loads `~/.codex/config.toml` and `AGENTS.md`.
- **Antigravity CLI:** forced to `default` mode (`agentMode` `auto` runs edits/commands unasked). Workspace reads/edits and `~/.gemini/antigravity-cli/settings.json` rules still apply first; loads `GEMINI.md` and `AGENTS.md`. Keychain sign-in requires services to run as the signed-in OS user (logs: `~/.gemini/antigravity-cli/log/`).

### Drop-in harness files

A harness may also ship as one TOML file in the `harnesses.d` directory beside the config file: `~/.ants/harnesses.d/` for `~/.ants/config.toml`, `DIR/harnesses.d/` for `--config DIR/FILE` or `ANTS_CONFIG=DIR/FILE`. The daemon, `ants doctor`, `ants harness check` and `ants harness prompt` load its `*.toml` files in file-name order, skipping hidden files (`.NAME`), directories and other files; a missing directory defines nothing. Each file holds `[harnesses.NAME]` tables, exactly as in the config file, so a definition moves between them unchanged; any other top-level key fails. A drop-in definition replaces a built-in of the same name whole, as one in the config file does, and a relative `command` with a `/` resolves against `harnesses.d` (`~` expands to the home directory). A name defined in two drop-in files, or in one and the config file, an unknown key or an invalid definition fails the config load, naming the files. Harnesses remain ACP executables: a drop-in file only describes how to start one.

```toml
# ~/.ants/harnesses.d/codex.toml
[harnesses.codex]
command = "codex-acp"
mode = "read-only"
sign_in = "codex-acp login"
```

### Sign-in

A harness is signed out when it answers `session/new`, `session/load` or a prompt with ACP's `auth_required` error (`-32000`), or when its error or stderr says so in so many words (`No authentication information found`, `not logged in`, `please run /login` and the like; a proxy's or an MCP server's authentication does not count). The daemon then:

- holds the message in hand, with progress acks, first on its thread: the attempt is not counted (its `ants_attempts` marker is cleared unless the prompt had already started a tool call, so the retry carries no cut-off warning), its delivery count does not grow, and no message is dead-lettered for it;
- starts no further prompts; messages already delivered wait their turn, and once `max_harnesses × 2` are in hand the consumer delivers no more;
- tells the thread's owner, else the default human, once per signed-out period, on the thread, with the sign-in command (`sign_in`, else the terminal auth method the harness offers in its `initialize` response) and the machine to run it on; the notice goes to the human directly, not up the ant's chain;
- shows the harness as signed out in the ant's roster entry (`signed_out`: `since`, `sign_in`), so `ants roster` and peers' `list_ants` show why the ant is not answering, and `route` prefers ants whose harness is signed in;
- checks the harness every minute with a handshake (start it, `initialize`, `session/new` in the workdir, stop it); once one succeeds, clears `signed_out` from the roster entry and answers the held messages. The next sign-out after a message got past sign-in tells the human again.

Stopping the daemon while signed out hands the held messages back, each counting one more delivery. `ants harness check` runs the same handshake on demand.

### Provider limits

A harness error reporting a provider limit is classified, after sign-in, as either:

- a **rate limit** or overloaded provider: HTTP `429` or `529` after a word such as `status`, `error` or `code`, or phrases such as `rate limit`, `too many requests`, `overloaded`, `resource_exhausted` or `exhausted your capacity`, plus the definition's `rate_limited` phrases;
- an **exhausted quota**: phrases such as `insufficient_quota`, `quota exceeded`, `usage limit`, `spending limit` or `credit balance is too low`, plus the definition's `quota_exhausted` phrases (the built-ins add Copilot's premium requests, AI credits and weekly rate limit, Claude's plan limits such as `You've hit your 5-hour limit` and billing errors, and Antigravity's daily quota and AI credits). A quota said to be back within 5 minutes counts as a rate limit.

Either way the daemon holds the message in hand exactly as for [sign-in](#sign-in) (no attempt counted, no delivery count growth, no dead letter, no further prompts meanwhile), then tries again after a backoff: 30 s doubling to 10 min for a rate limit, 15 min doubling to 2 h for an exhausted quota. A wait the error names (`retry after 30s`, `try again in 4 days 20 hours`, `resets in 2h27m3s`, `retryDelay`, an epoch after `|`) replaces the doubling, within the same cap. A rate limit passes quietly (logged); an exhausted quota tells the thread's owner, else the default human, once, on the thread, with the harness's words, the machine and when messages are tried again. The roster entry shows the limit (`limited`: `kind` `rate_limit` or `quota`, `since`, `until`) until the wait passes, so `ants roster` and peers' `list_ants` show it and `route` prefers ants without one; the thread's status goes back to `idle`. A message getting past starts the backoff over and lets the next exhausted quota be told again. A limit reported after the prompt started a tool call keeps the attempt marker, so the retry carries the cut-off warning, but still spends no attempt.

Claude's adapter reports a limit as an error of the prompt; Copilot CLI and Antigravity end the prompt with a reply of error lines instead (`Error: Failed to get response from the AI model; ... Last error: 429 ...`, `⚠ API error (attempt 1): Error 429, ...`). A reply whose every streamed piece starts with `Error:`, `API Error:`, `Info:` or `⚠` and is at most 4096 characters is read as such an error: one that reports a limit is classified the same way and never sent as an answer; any other stays the reply.

### Harness failures

Any other failure to start a thread's harness (spawning it, `initialize`, `session/new` or `session/load`, applying the ant's model, mode or other settings), such as a missing or broken install, a bad `[harnesses.NAME]` definition or bad ant settings, is retried like a failed prompt, spending an attempt. A failure on one thread alone stays that thread's: its message is dead-lettered after 5 attempts. Once the harness fails to start 3 times within 10 minutes, on at least 2 threads, with no start succeeding in between, the daemon holds the messages instead, as for [sign-in](#sign-in) (no attempt counted from then on, no dead letter, no further prompts meanwhile), and:

- tells the thread's owner, else the default human, once, on the thread, with the error, the machine, and `ants harness check --harness NAME` to run there; the notice goes to the human directly, carrying a level-0 `harness` escalation, so `ants webhook serve` pushes it;
- shows the harness as failing in the ant's roster entry (`failing`: `since`, `error`, the last error), so `ants roster` and peers' `list_ants` show why the ant is not answering, and `route` ranks the ant with the signed-out ones;
- sets the thread's status back to `idle` and starts the harness every minute as for a thread, with the ant's settings, stopping it again; once a start succeeds (or finds the harness signed out or held back by a provider limit, which then take over), clears `failing` from the roster entry and lets the held messages go on. Until a message then gets past the start, one more failure holds the messages again at once, without telling the human again.

### Lost sessions and the context window

When `session/load` fails (the harness was upgraded, its session store cleared, or it cannot load sessions), the daemon starts a new session, logs a warning and stores the new session ID. Its first prompt starts with an `[ants]` line saying the earlier session could not be resumed, followed by the thread's history read back from `ANTS` with the ant's own credentials, oldest first:

- messages to the ant on the thread received before the one being answered, the ant's own messages on the thread (through consumer `sentread-<ant>`), the thread's notes, and the posts of the channel thread;
- the most recent entries within the history budget, each cut to an eighth of it, after an `[ants]` line telling what older ones were left out: how many of each kind (messages, thread notes, channel posts), their senders (up to 8 named) and the time span they cover;
- an `[ants]` line instead when part or all of the history cannot be read (credentials older than permissions version 3 cannot read the ant's own messages back).

A prompt error that says the session outgrew the model's context window (`context_length_exceeded`, `maximum context length`, `prompt is too long`, `exceeds the context window` and the like, plus the definition's `context_overflow` phrases, matched case-insensitively) moves the harness to a new session at once: the prompt runs again there, seeded the same way with an `[ants]` line saying why, without spending a delivery attempt. A prompt that overflows a new session too (a retried one, or the first after a lost session) is not retried: the daemon escalates `context_window` on the thread with the harness's error, acks the message and dead-letters nothing. A session rebuilt after an overflow is seeded with half the history budget, leaving room for the prompt.

The history budget, in bytes, is the ant's `history_budget`; else that of the ant's `context` value, or one derived from that value's `window`; else the harness's `history_budget`, or one derived from its `window` (the model's context window in tokens); else 64 KiB. A budget derived from a window is a twelfth of it at 4 bytes a token (about 65 KB for 200k tokens, 333 KB for 1m), and no budget exceeds a quarter of the window (window × 1 byte). The built-in Claude definition sets a window of 200k tokens, and 1m for `context = "1m"`; Copilot and Antigravity set none, so their ants get 64 KiB unless `history_budget` says otherwise. `history_budget = 0` seeds only the line about what was left out.

### Model, reasoning effort and context

Set `model`, `reasoning_effort` and `context` per ant or via `--model`, `--reasoning-effort` and `--context`. The daemon sets `model` before `reasoning_effort` via ACP selectors on new and resumed sessions; unsupported IDs or mismatched read-backs fail the session and list valid options.

- **Copilot:** `context` takes `default` or `long_context` (launch flag `--context ...`).
- **Claude:** `context` takes `default`, or `1m` with an explicit `model` (selects exact `<model>[1m]` variant). A `model` already ending in `[1m]` must omit or match `context`.
- **Antigravity:** no `context` table; pick a model with the desired window.

`context` selects a provider-native tier/window; `ants` never truncates or compacts a live session's history (a rebuilt session's seed follows the [history budget](#lost-sessions-and-the-context-window)).

### Prompt limits and spending caps

Per-prompt limits default to `prompt_timeout = "30m"` (includes approval wait; `"0"` disables) and `max_tool_calls = 300` (`0` disables). Optional spending caps bound tokens or cost per thread (`max_thread_tokens`, `max_thread_cost`) and per ant per UTC day (`max_daily_tokens`, `max_daily_cost`).

Crossing a limit or cap cancels the prompt via ACP `session/cancel` (restarting the harness after 30s if hung), returns partial output plus an `[ants]` notice, and escalates agent-originated threads (reason `cap`). Further agent messages on a capped thread escalate immediately without prompting.

- Per-prompt limits reset every prompt.

A human stops a running prompt with `ants cancel ANT THREAD`: a `CancelRequest` on `ants.cancel.<ant>.<thread>` (core NATS, so only daemons connected at the time hear it). The daemon running the prompt cancels it the same way, replies with the partial output and `[ants] The prompt was cancelled by <human> (ants cancel).`, and acks the message: no retry, dead letter or escalation. Every daemon serving the ant answers with a `CancelReply` on `ants.cancelled.<ant>.<thread>` saying whether it cancelled one; the CLI succeeds on the first that did. Messages waiting behind the cancelled one on the thread still run. A message not started yet is withdrawn instead (see [Queue and withdrawal](#queue-and-withdrawal)).

- Thread allowances reset on any human message on the thread, and once per escalation level when the level's leader replies.
- Daily allowances reset at 00:00 UTC (or on daemon restart with a higher cap).
- The ants added from an `[[ant_template]]` with `pool_daily_tokens` or `pool_daily_cost` share that daily cap, on top of their own: reaching it, by the sum of the daily spend of every ant the config file lists from the template, read from `ants_sessions` (so it holds across daemons and restarts), cancels and escalates as a daily cap does, first to the manager that added the ant. Its warning goes once per UTC day to the default human of the ant that passes `spend_warn_percent` of it, noted in that ant's daily spend. A member's spend that cannot be read lets prompts run rather than block them.
- Spend first passing `spend_warn_percent` (default 80, `0` disables, below 100) of a cap, short of it, sends an `[ants]` warning on a new thread: of a thread cap to the thread's owner, else the default human; of a daily or pool cap to the default human. It shows the spend as `ants spend` does (`spent/cap` and the percentage), names the config key to raise (then reload the daemon with `SIGHUP`) and the `ants spend` command. Each cap warns once per thread allowance or UTC day, recorded with the spend in `ants_sessions`, so restarts and later prompts do not repeat it.
- Spending uses ACP `usage_update` cost and prompt token totals stored at `<ant>.<thread>.spend` and `<ant>.day.<YYYY-MM-DD>` in `ants_sessions`, with or without caps. Claude's adapter reports both at turn end; Codex's adapter reports tokens only; Copilot CLI reports neither (so only prompt limits trigger); Antigravity reporting is unverified.

### Queue and withdrawal

A message an ant has not started on yet is taken back with `ants withdraw ANT MESSAGE` rather than cancelled: the ant is never prompted with it, the prompt running goes on, and so do the other messages, those of the same thread included. `ants queue ANT` lists the messages that wait, with the IDs to withdraw:

- `ants queue` sends a `QueueRequest` on `ants.queue.<ant>` (core NATS); the daemon serving the ant answers on `ants.queued.<ant>` with the messages in its hands, by stream sequence: the one running on each thread, those waiting their turn behind it, and those held while the harness is signed out or a provider limit holds. The answer also gives the stream sequence the ant's inbox consumer delivered last, and how many messages it has not delivered yet: the CLI reads those from the stream (up to 100) and counts their place on their threads after the ones in hand. A standby daemon does not answer; channel posts not yet delivered, and messages awaiting redelivery after a failed attempt, are not listed.
- `ants withdraw` creates `<ant>.msg.<id>` in `ants_attempts` as `withdrawn` (by the human, and when); the daemon creates the same key as `started` as it takes the message up, before anything runs for it. A key is created only when it has no value, so exactly one of the two wins: a withdrawal after the start fails, naming `ants cancel` for the running prompt, as does one after the message is `finished`; a second withdrawal reports the first.
- When a withdrawn message's turn comes, on its first delivery or a redelivery, the daemon stores an `[ants] Withdrawn by <human> before <ant> was prompted with it (ants withdraw).` notice on its thread, then acks it: no prompt, retry, dead letter or escalation. The notice answers the sender as a reply would (`in_reply_to` the message), so `ants thread show` and an `ask_agent` waiting for the reply see it; that of a webhook's message goes to the thread's owner, else to the human who withdrew it, as does one to an ant out of turns or to a channel post.
- A message the harness turned away for want of signing in, or for a provider limit, before any tool ran, is no longer started: its `started` value is deleted while it waits, so it can be withdrawn meanwhile.

`ants withdraw` checks that the message waits for the ant with a queue request first; when no daemon of the ant answers, as while it is stopped, the withdrawal is written anyway and takes effect when a daemon takes the message up. Any human may withdraw any message, as any human may cancel any prompt.

### Spend report

`ants spend` (human credentials) reads those keys:

- `ants spend`: one row per ant that spent anything in the last `--days` UTC days (default 7, today included, at most 366; `ants_sessions` keeps keys 30 days after their last write): today's tokens and cost, and their totals over the days.
- `ants spend ANT`: each day and the total, then each thread that spent anything since a human's last message on it, most costly first.
- `ants spend [ANT] --thread THREAD`: what the ant, or each ant on the thread, spent on it since a human's last message there.

Today's and each day's figures show as `spent/cap` against `max_daily_tokens` and `max_daily_cost`, and threads' against `max_thread_tokens` and `max_thread_cost`, for the ants the local config file (`--config`, `ANTS_CONFIG`, default `~/.ants/config.toml`) lists with caps; ants served from other machines show spend alone. Costs are in the harness's unit (USD for Claude), with two decimals. `--json` prints one object per line: per ant `{ant, days: [{day, tokens, cost}], total: {tokens, cost}, threads?, caps?}` (`threads` only for one `ANT`), or with `--thread` `{ant, thread, tokens, cost, caps?}`; `caps` holds the caps set (`thread_tokens`, `thread_cost`, `daily_tokens`, `daily_cost`).

## Roster, identity and memory

Each daemon publishes a `RosterEntry` per ant at `<ant>` in `ants_roster` (`machine`, daemon `instance`, `harness`, the `agent` it runs (`name` and `version`, as the ACP `initialize` of the daemon's latest harness start named it; absent until a harness started or when the agent names none), `workdir`, `capabilities`, `whoami`, `escalate_to`, ants `version` and permissions version `perms`, start/heartbeat timestamps, `idle`/`working` activity, in-flight message count, live harness count, while the harness is signed out a `signed_out` object with `since` and the `sign_in` command, and while a provider limit holds its messages a `limited` object with `kind` (`rate_limit` or `quota`), `since` and `until`, and while the harness keeps failing to start a `failing` object with `since` and the last `error`), refreshing every 10s and on activity, agent, sign-in, limit or failing transitions, and deleting on clean exit. Entries expire after 30s, each leaving a delete marker (`Nats-Marker-Reason: MaxAge`, kept 30s) that watchers of the bucket see as a purge; `ants roster` marks entries without a heartbeat for 20s as `stale`.

### Identity, memory and task tracking

- `whoami` (`--whoami TEXT`, `--whoami ""` clears): role description and boundaries prepended to every prompt (including resumed sessions) and published in `ants_roster` and `list_ants` so peers can pick recipients. Visible to all ants; keep secrets out. `route` matches `capabilities` tags, not `whoami`.
- `memory` and `tasks` (`--memory TEXT`, `--tasks TEXT`): optional tool-agnostic instructions prepended to every prompt and kept private (not published in the roster). `ants` initializes no external tool state automatically.
- `env` (`--env NAME=VALUE`, repeatable): literal environment overrides passed to the harness, hooks and tool subprocesses. Use absolute paths to isolate per-ant state.

Example using `bd` (`mkdir -p ~/.ants/dev-a && bd init -C ~/.ants/dev-a --prefix dev-a`):

```toml
[[ant]]
name = "dev-a"
workdir = "~/src/some-repo"
memory = "Run bd prime at session start; use bd remember for durable notes."
tasks = "Use bd ready, bd claim, bd create and bd close for task tracking."
env = { BD_DB = "/Users/alice/.ants/dev-a/.bd/bd.db", BD_REMOTE = "" }
```

## Tool permissions

Tool calls requested by a harness are checked against the ant's allowlist (`allow = [...]` or repeatable `--allow RULE`; `--allow-tools` equals `*`, for sandboxed workdirs only):

| Rule | Matches |
|---|---|
| `*` | Every tool call |
| `<kind>` | ACP tool kind: `read`, `edit`, `delete`, `move`, `search`, `execute`, `think`, `fetch`, `switch_mode`, `other` |
| `execute:cargo test*` | Commands matching glob; commands containing `;`, `&`, `\|`, `<`, `>`, `` ` `` or `$(` never match |
| `edit:src/*` | Relative paths inside `<workdir>/src` (never outside `<workdir>`; with `worktrees = true`, the thread's worktree) |
| `edit:/srv/notes/*` | Exact absolute path glob |
| `fetch:https://docs.rs/*` | Matching URLs |
| `other:send_message` / `other:bd-*` | MCP and `other` tools by title, or by the tool in Claude Code's `mcp__<server>__<tool>` and Codex's `mcp.<server>.<tool>` titles |

Matched calls auto-approve with `allow_once`. Unmatched calls set `waiting_approval`, publish an `ApprovalRequest` on `ants.approvals.<ant>`, and wait `approval_timeout` (default `5m`) for `allow_once`, `allow_always` or `reject` before rejecting:

- **Human decisions:** `ants tools approve|reject <ant>/<id>` stores an `ApprovalDecision` on `ants.approvals.<ant>.<id>` using `Nats-Expected-Last-Subject-Sequence` so the first human answer wins; the daemon verifies the stored decision via direct get.
- **Off the terminal:** `ants webhook serve` pushes pending requests to each configured human's ntfy, Slack, Telegram, command or HTTP endpoint, with signed approve/reject links that store the same `ApprovalDecision` (see [Notifications](#notifications)).
- **Suggested rules:** once 3 requests of an ant for the same exact call (`execute:<command>`, `<kind>:<absolute path>` for a single file, `fetch:<url>`, else `<kind>:<title>`) are approved by humans, counting the one at hand and the earlier ones still in the stream, `ants tools approve` prints that allow rule and approval notices mention it. `ants tools approve ID --rule` approves and appends the rule to the ant's `allow` in `~/.ants/config.toml` (`--config`/`ANTS_CONFIG`), leaving the rest of the file as it is, then reloads the daemon service (the ant restarts once its prompts in flight finish). For an ant added from a template, whose `allow` only the template sets (an `allow` key in its entry fails config loading), `--rule` approves once and prints the rule for a human to add to the template's `allow` or `allow_sets` instead. No rule is suggested for commands with shell control characters or `*`, or for calls touching several files.
- **Chain decisions:** ants with `escalate_to` also send an `[ants]` notice (`approval` escalation) to every ant in `escalate_to` on the thread. Members handling that notice may call `decide_approval(verdict)` (`ants.inbox.<ant>.<decider>`); `escalate` is refused on approval notices. Earliest stored decision wins (human or chain member), and `ants tools` inspects both.
- **Webhook and schedule threads:** a thread a webhook or schedule starts is narrowed to the `allow` of its `[[webhook]]` or `[[schedule]]` entry: a call auto-approves only when both that list and the ant's allowlist match it, and goes to the humans and chain otherwise. Without `allow`, the narrowing is read-only: `read:*` (the workdir), `search`, `think` and the MCP tools `list_ants`, `check_inbox`, `read_notes`, `post_notes`, `send_message`, `ask_agent`, `route`, `escalate` and `react` (no `post_channel`, `decide_approval`, `manage_channel`, `request_ant` or `retire_ant`). `allow = ["*"]` opts out, leaving the ant's allowlist as it is; `allow = []` sends every call to the humans. The rules travel on the thread's messages and posts (`allow`), so replies, `send_message`/`ask_agent` (spin-offs included), channel posts, escalations and approval notices carry the rules to every ant reached (an ant of the chain asked to decide one of the thread's approvals is held to them too, so its `decide_approval` goes to its own approvers unless the rules allow `other:decide_approval`); each daemon keeps them per thread in `ants_sessions` (`<ant>.<thread>.spend`), across restarts, until a human's message to the ant on the thread, or a human's post on the channel thread that wakes it, lifts them. The rules only ever narrow an allowlist, never widen it.
- **Expiry:** a request left undecided for `approval_timeout` leaves `ants tools list` and is rejected; the thread's owner (else the default human) gets an `[ants]` notice, on a thread of its own so that `ants ask --wait` still waits for the reply, naming the ant, request, tool and thread, with how to retry (`ants ask --thread`), raise `approval_timeout` or add an `allow` rule. The notice carries the request's `approval` escalation, so `ants webhook serve` pushes it to that human's sinks. Each ant of `escalate_to` that was asked gets an `[ants]` message on the request's thread saying the request expired and needs no answer; a human among them, already told, gets no second notice.

```sh
ants tools list -f                         # stream pending requests (--json: JSON lines)
ants tools show dev-a/019a7d2e --json      # full request + tool input (unique ID prefix accepted)
ants tools approve dev-a/019a7d2e --always # allow-always when offered by the harness
ants tools approve dev-a/019a7d2e --rule   # also add the exact allow rule to dev-a in config.toml
ants tools reject dev-a/019a7d2e
```

### Redaction

Tool requests, status streams, replies (stored in `ANTS` for 30 days), escalations, thread notes, the messages and channel posts sent with MCP tools, and a failing harness's last error (in `ants_roster` and the notice telling a human) are redacted before the daemon publishes them: each secret found becomes `[redacted]`. Secrets found:

- **Environment values:** the values of the ant's `env`, of its harness's `env` and of the daemon's own environment whose variable name has a part such as `TOKEN`, `SECRET`, `PASSWORD`, `KEY`, `AUTH` or `CREDENTIALS`, and the other values of the ant's and harness's `env` that look like a token (letters and digits, without spaces, slashes or a file extension). Values shorter than 8 characters and paths (starting with `/`, `~` or `.`) stay, so `1`, `true`, a file name, a host or a directory never mangle the text.
- **Patterns:** PEM private keys, `Bearer` and `Basic` credentials, GitHub, GitLab, Slack, OpenAI, Anthropic, AWS, Google, Stripe, npm, Hugging Face and Telegram bot tokens, Slack and Discord webhook URLs, NKey seeds, JWTs, URL passwords (`scheme://user:password@host`), and values with a digit given to such a secret name (`API_KEY=...`, `"password": "..."`, `--token ...`).

A tool request's input is redacted before it is cut to its 2000-byte excerpt, and streamed text is held back until a secret cannot be split across two status updates. Top-level `redact = false` turns this off for every ant of the config file. Notices of `ants webhook serve` (ntfy topics are public unless the server requires a token) are redacted regardless, by pattern and by the secret-named variables of its own environment. Local logs are not redacted.

## MCP tools

Each session gets the `ants` MCP server over streamable HTTP (`127.0.0.1:<port>/mcp` with per-thread bearer token) or, without HTTP MCP support, via `ants mcp --socket <home>/mcp.sock` (`0600`, `ANTS_MCP_TOKEN`; on Windows a named pipe only the daemon's user may open, see [Windows](#windows)). Tools run on the active thread during a prompt (allowlist: `other:<tool>`):

| Tool | Description |
|---|---|
| `send_message(to, text, thread?)` | Send to an ant or human (`thread` omitted: current; `"new"`: fresh; else ID) and return once stored, with the message ID; warns when no daemon serves the ant now or a provider limit holds its messages. |
| `ask_agent(to, text, timeout?)` | Ask an ant on the current thread and wait for its reply (`timeout` a duration such as `90s`: default `5m`, max `30m`; refuses humans, and at once, sending nothing, an ant no daemon serves now or whose provider limit outlasts the wait). |
| `check_inbox(all_threads?)` | Drain late `ask_agent` replies arriving after timeout (current thread unless `all_threads`). |
| `react(reaction, message_id?, remove?)` | Add the ant's reaction to a message (default: the message being handled on the thread), or remove it with `remove: true` (see [Reactions](#reactions)); 0 turns, wakes nobody. `ask_agent`, `route` and `check_inbox` results give each reply's message ID. |
| `list_ants(capability?)` | List peer ants from `ants_roster` (`whoami`, capabilities, harness and the agent it runs with its version, machine, activity, or a signed-out harness and its sign-in command, or a harness failing to start and its last error, or a provider limit and until when). |
| `route(capability, text, wait?, timeout?)` | Pick a peer with `capability` (signed in and starting > signed out or failing to start, no provider limit > limited, `live` > `stale`, `idle` > unknown > `working`, fewest messages, name); waits unless `wait: false`. |
| `post_notes(text)` | Append to the current thread's notes (1 turn; wakes nobody). |
| `read_notes(thread?)` | Read up to 50 notes posts (oldest first) and turns left on the current or named thread (0 turns). |
| `decide_approval(verdict)` | Answer an incoming chain approval notice (`allow_once`, `allow_always`, `reject`). |
| `post_channel(channel, text)` | Start a channel thread as `creator` (member/messenger only; inherits turns minus 1 for agent messages). |
| `manage_channel(channel?, add?, remove?, messenger?, clear_messenger?)` | List owned channels, show one, or change members/messenger via `ants channel serve`. |
| `escalate(issue)` | Escalate the thread one level up its origin chain (or to the owner) with reason `asked`. |
| `request_ant(name, template, reason, overrides?, timeout?)` | Managers of an `[[ant_template]]` only: add a worker ant from the template, signed by `ants signer`, and return its name once it is in the roster, or why it was refused (see [Manager ants](#manager-ants)). |
| `retire_ant(name)` | Managers of an `[[ant_template]]` only: retire an ant the caller added, unlisting and stopping it and revoking its credentials while keeping its home for a later `request_ant` of the name (see [Retiring an added ant](#retiring-an-added-ant)). |

Human traffic costs 0 turns; agent-to-agent sends/replies cost 1 turn (`ask_agent` requires 1 turn left for the reply), and self-messaging is refused. Before an ant gets a message, its `ants_roster` entry is read (one key, giving up after 3s): when no daemon serves it now (no entry, a `stale` one, or a harness signed out or failing to start), `ask_agent` (and `route` waiting on a reply) is refused at once with nothing sent, and `send_message` still sends, the message waiting in the ant's inbox, with a warning; both say why and name ants to message instead (the closest names in the roster, by edit distance or prefix, or for a stale, signed-out or failing ant the served ants sharing a capability with that ant). Humans and the channel manager are never looked up, and a roster that cannot be read holds nothing back. `ask_agent` hears replies immediately on live inbox and `_INBOX.<ant>.stored.*` subscriptions without waiting behind the active prompt; late replies persist in `<home>/mcp.json` and prepend a count on later prompts until `check_inbox` drains them.

## Messenger

`ants ask` without `@ant` routes through `messenger` (`list_ants` + `route`); channels also use `messenger` by default to triage unmentioned human and webhook posts.

Unless `messenger = false` or an `[[ant]]` is named `messenger`, each daemon serves a built-in `messenger` (`~/.ants/messenger`, `~/.config/ants/auth/messenger.creds` if present, top-level `harness`). Built-in instances coordinate via `ants_roster`: one daemon holds the `messenger` entry while others watch it and take over within about a second of its deletion on exit or its 30s TTL expiry (looking again every minute in case the watch misses it, and every 10s on a hub older than nats-server 2.11, whose roster leaves no expiry markers), yielding immediately if a configured `[[ant]]` named `messenger` claims the entry. Define `[[ant]] name = "messenger"` with an `AGENTS.md` in `workdir` for custom instructions and `allow` rules:

```toml
[[ant]]
name = "messenger"
workdir = "~/.ants/messenger-work"
creds = "~/.config/ants/auth/messenger.creds"
allow = ["other:list_ants", "other:route", "other:ask_agent", "other:send_message", "other:check_inbox"]
```

## Thread notes

Thread notes (`ants.notes.<thread>.<from>`) provide one passive shared board per thread for multi-agent coordination, separate from live execution plans (`ants watch <ant>`) and wake-on-post [Channels](#channels). Posting wakes nobody; agents inspect notes only via `read_notes`.

Agent posts spend 1 turn from the lower of the active message's budget and the last note's remaining turns, serialized with `Nats-Expected-Last-Subject-Sequence` (losers re-read and retry). Spending the final turn escalates the thread (`notes`) and blocks further agent note posts until a human runs `ants notes <thread> TEXT` (8 fresh turns) or an escalation climbs a chain level. Daemons read via ephemeral consumer `notes-<ant>`; CLI runs with ant credentials use spare consumers `notes-<ant>_1..3`; humans read directly by message gets, as `ants thread` does.

## Thread overview

`ants thread list` and `ants thread show` read the `ANTS` stream back with human credentials by message gets (as `ants channel read`, `ants notes`, `ants inbox` and the `--wait` polls do), so they consume, ack and mark read nothing and create no consumer; ant credentials are refused. Message gets are batched direct gets (`$JS.API.DIRECT.GET.ANTS`, nats-server 2.11 or later), one request per 256 messages; on a connection whose credentials or leaf uplink predate permissions version 3 (the request goes unanswered for 2 s), or whose server lacks batches, reads take one `STREAM.MSG.GET` per message instead, for the rest of that connection. Human credentials that themselves predate version 3 (by the `ants-perms` tag of their user JWT) go straight to `STREAM.MSG.GET`, without the 2 s wait. Only notes subjects name their thread, so each read goes through every message stored in its window: `--since` bounds it (`list` defaults to `24h`; `show` starts at the thread's start when its id is a time-ordered UUID, as generated ids are, else reads all the stream keeps). Entries are kinds `message`, `escalation` (with level, reason, leader and ants visited), `mention`, `post`, `note`, `approval`, `decision` (a human's, which names no human, or a chain agent's) and `dead`; a human's decision is placed on its request's thread only when the request is in the window. A message stored on a subject naming another sender is marked `(forged)`. A thread's owner is the one its first message naming one gives; `list` counts only what was stored within `--since`, so a thread older than that shows its first message in the window as the opening.

### History search

`ants thread search` finds single messages across every thread, reading the stream as `ants thread list` does (human credentials, message gets, nothing consumed, acked or marked read). Filters combine: a message is printed only when it matches all of them.

- `--from USER`: the ant or human that sent it (a dead letter's sender is its message's; a human's decision on a tool request names no sender, so `--from` never finds one).
- `--to USER`: the recipient of an inbox message, or the ant a decision or dead letter is for; posts, notes and tool requests have none.
- `--channel NAME`: posts to the channel and the mentions telling human members of them.
- `--type KIND` (repeat or separate with commas): `message` (inbox messages both ways, replies included), `escalation`, `mention`, `post`, `note`, `approval` (a tool request), `decision` and `dead`, the kinds `ants thread show` prints. Reactions are no messages: they live in the `ants_reactions` bucket and `ants thread show` prints them under each message.
- `--grep TEXT`: the message's text contains `TEXT`, whatever the case, as with `ants thread list --grep`.
- `--since TIME` and `--until TIME`: stored at or after `--since` and before `--until`. Each takes a duration back from now (`90m`, `2h`, `3d`; `0` for no bound, so `--since 0` reads all the stream keeps), an RFC 3339 time (`2026-11-14T09:30:00+09:00`), or a date or date and time without an offset (`2026-11-14`, `2026-11-14 09:30`), taken in UTC as `ants thread` prints times, whatever the machine's or the config file's `timezone`. A date is its midnight, so `--since 2026-11-14 --until 2026-11-15` is that UTC day. Without `--since`, the 24h before `--until` (or now).
- `--limit N` (default 50) keeps the latest N matches, printed oldest first; how many earlier ones it left out goes to stderr.

Each match carries its thread ID, kind, stream sequence, message ID (none for a tool request or dead letter) and stored time; with `--json` one object per line with the fields of `ants thread show --json` (`seq`, `id`, `stored_at`, `kind`, `thread_id`, `from`, `to`, `channel`, `owner`, `note`, `text`, `forged`), without reactions.

Cost: the filters pick the subjects read (`ants.inbox.<to>.<from>`, `ants.chan.<channel>.<from>`, `ants.notes.*.<from>`, `ants.approvals.<ant>[.<request>]`, `ants.dead.<ant>`), each read once from `--since` (a start time, so earlier messages are skipped by the server) to `--until`, one batched direct get per 256 messages on it. The server skips the messages on other subjects, so `--from`, `--to` and `--type` narrow what is read, while `--grep` and the `--channel` of a mention are matched on the messages read: `--channel ops` alone reads every inbox message in the window to find mentions, `--channel ops --type post` only the channel's posts. A human's decision is found only when its request is in the window, as with `ants thread show`.

```sh
ants thread search --from dev-a --grep "deploy failed"                # what dev-a said about it, last 24h
ants thread search --to alice --type message --since 2026-11-14 --until 2026-11-15
ants thread search --channel ops --type post --since 3d --json | jq -r .thread_id | sort -u
ants thread search --type escalation,dead --since 0 --limit 200      # every escalation and dead letter kept
```

### Titles, pins and archives

A thread keeps its ID; a title names it for people. `ants thread title THREAD Release 1.2` sets it, again renames it, `--clear` removes it, and without text the command prints it. Titles are shared: one per thread, the same for every human and device, since a thread is a team conversation every human lists, and any human sets or renames any thread's title (a thread may have no owner, and credentials cannot tie a key to a thread's owner). The title is one line of 1–120 characters, trimmed, kept under `title.<thread>` in KV `ants_threads` as `{thread_id, title, by, at}` (`by` the human who set it), and never expires. `ants thread list` shows it in a TITLE column (`title` in `--json`), `ants thread show` on a `title:` line under its header, and shell completion of `THREAD` as the description of each recent thread.

Pins and archives are per human, shared by all that human's devices and invisible to other humans: `ants thread pin THREAD`, `unpin`, `archive` and `unarchive`. A thread is pinned or archived, never both: pinning unarchives it and archiving unpins it. `ants thread list` puts pinned threads after the others (nearest the prompt), and `--limit` drops other threads before pinned ones; it leaves archived threads out. Archiving deletes no message and cancels no work: replies still reach the inbox, notifications and escalations still go out. Archiving records the last sequence of `ANTS`; once a message is stored on the thread after it, the thread comes back in `ants thread list`, flagged `archived+new` (`new_activity` in `--json`), and stays archived: `archive` again hides it until the next message, `unarchive` brings it back for good. `--pinned` and `--archived` list only the threads so marked; without `--since`, they read from the start of the earliest such thread (everything kept when its ID is not time-ordered). The STATE column (`pinned`, `archived`, `archived+new`) shows when a listed thread has one, and `ants thread show` prints `pinned`, `archived` or `archived, with messages since` under its header. A human's marks are one key, `marks.<human>` in `ants_threads`, holding `{pinned: {thread: at}, archived: {thread: {at, after}}}`, changed with `Nats-Expected-Last-Subject-Sequence` (a device losing a race re-reads and retries), so marks from several devices at once all stay. Marks of threads whose messages expired stay in the key.

Credentials let every human direct-get and put any `$KV.ants_threads.title.*` key, but only its own `$KV.ants_threads.marks.<self>`; ants, observers, webhooks and the channel manager read and write neither. The bucket has no outbox form: titling, pinning and archiving behind a leaf node need the hub reachable. `ants init` creates the bucket, as the first connection of any command with human credentials does. On a hub without it, `ants thread list` warns and lists threads without titles or marks, and the commands that need it fail naming `ants init`; human credentials from before these rights get the same treatment, with the command that renews them.

## Dashboard

`ants dash [--since DUR]` opens an interactive terminal dashboard with human credentials. It reads over the CLI's own connection as `ants thread`, `ants watch` and `ants tools list -f` do: no extra service, and nothing consumed or marked read.

- **Threads** (left): threads with messages stored within `--since` (default `24h`, `0` for all kept), most recently active first, each with `▶` while an ant works on it, the start of its id, its last time (UTC), users and channels, and its opening line. The most recent one is selected at start.
- **Thread** (right): the selected thread's owner, the live progress of the ants on it (from `ants.status.>`, with the age of the last update), then its history as `ants thread show` prints it, the latest at the bottom. Messages stored later are appended.
- **Ants**: the roster (`live`/`stale`, activity, machine), read every 10s.
- **Tool requests**: the pending requests (`<ant>/<id>`, tool, time left), followed as `ants tools list -f` does, so the device counts as reading tool requests (see [Notifications](#notifications)). `Enter` opens one in full, as `ants tools show` prints it. `a` (allow once), `A` (allow always) or `r` (reject), among the verdicts offered, then `y` to confirm, stores the decision as `ants tools approve|reject` does: the first decision wins, and one stored meanwhile by another human or an agent is reported instead. A request answered or expired elsewhere leaves the list, and an opened one says so. After an approval, the notice names the allow rule that `ants tools approve` would suggest.
- **Header**: the connection (`● connected`, or `○ disconnected 12s, reconnecting` in red while the client reconnects on its own), and how long ago the stream and the roster were read; the stream's age turns yellow past 15s or while disconnected. Once reconnected, the stream is read from where reading stopped, the tool requests are followed anew, and the roster and the selected thread are read again. The stream is also read every 5s, for messages stored without being heard (such as those a leaf node's outbox forwards).
- **Footer**: the keys, or for 15s the latest notice: a decision stored, an error, a warning logged. Log lines are held while the dashboard runs and printed to stderr when it exits.

| Key | Action |
|---|---|
| `Tab` | Switch between threads and tool requests. |
| `↑`/`↓`, `k`/`j`, `Home`/`End` | Select a thread or request. |
| `Enter` | Read the selected thread again, or open the selected request. |
| `PgUp`/`PgDn` | Scroll the thread's history. |
| `a`, `A`, `r`, then `y` | On an opened request: allow once, allow always or reject, then confirm; any key other than `y` cancels. |
| `Esc` | Close an opened request. |
| `R` | Read everything again. |
| `q` | Quit, or close an opened request. |
| `Ctrl-C` | Quit. |

Without a terminal on stdin and stdout, `ants dash` exits with status 1 naming the commands for scripts: `ants thread list`, `ants thread show THREAD`, `ants roster`, `ants watch ANT`, `ants tools list -f`, `ants tools show ID`, `ants tools approve ID` and `ants tools reject ID`.

## Reactions

A reaction marks a message as seen, done and so on without a reply: `ants react MESSAGE eyes` (`--remove` takes it back) or the MCP tool `react`. A message's ID is its envelope's `id` (a channel post's `id`), a UUID that stays the same on every leaf node and the hub, unlike stream sequences; `ants ask` and `ants post` print it, and `ants inbox`, `ants thread show` and the MCP results show it. A reaction is a token of 1–64 characters of `a-z`, `0-9` and `-`, starting with a letter or digit (a valid subject token), such as `eyes`, `check`, `thumbsup`, `x`, `heart` or `rocket`, which shell completion suggests.

Reactions live in KV `ants_reactions`, apart from messages: the key `<reactor>.<message id>.<reaction>` holds `{message, by, reaction, at}`, and readers take who reacted how to what from the key. Reacting puts the key (again: no change), removing deletes it (absent: no change), and a key expires after 30 days, as the messages in `ANTS` do. Being outside `ANTS`, a reaction reaches no inbox, wakes no ant, marks nothing read and ends no `ants ask --wait`, `-f`, `ask_agent` or `route` wait.

The reactor is the user the credentials name, so every device of a human reacts as that human. Credentials allow each user to put, delete and direct-get only `$KV.ants_reactions.<self>.>`: nobody can react as another or remove another's reaction. Humans, who read every thread, read all reactions (direct gets and ordered consumers of `KV_ants_reactions`) and see them grouped beneath each message in `ants inbox` and `ants thread show` (`reacted eyes: alice, dev-a`) and live in `ants watch`; ants, observers, webhooks and the channel manager read none. A subject cannot tie a message ID to the right to read that message, so an ant can react under any ID, even of a message it never got; only humans see the reaction. The bucket has no outbox form: reacting behind a leaf node needs the hub reachable. `ants init` creates the bucket.

## Channels

Channels (`ants.chan.<channel>.<from>`, metadata in `ants_channels`) are named groups of ants and humans:

```sh
ants channel create ops --member dev-a,dev-b --messenger triage --owner lead
ants channel add ops dev-c alice               # ant and human members
ants channel remove ops dev-a
ants channel set ops --default-messenger       # revert triage to `messenger`; --no-owner clears owner
ants channel list
ants channel delete ops                        # removes entry; stored posts remain in ANTS

ants post '#ops' "@dev-b deploy failed"        # wakes dev-b; prints new thread ID
ants post ops "who owns billing?"              # no @mention -> channel messenger triages
ants post -f --timeout 10m ops --thread <id> "status?"
ants post ops --file notes.md                  # multiline post from a file
ants channel read ops --limit 10
```

- **Mentions and triage:** `@ant` wakes mentioned channel members only; `@human` sends an inbox notice to mentioned human members (excluding the author) without waking ants. Human or webhook posts with zero mentions prompt the channel messenger (`--messenger`, else `messenger`, member or not) to post a short `@mention` triage reply (1 turn). Agent posts with zero mentions wake nobody.
- **Threads, owners and creators:** omitting `--thread` starts a thread named after the post ID. A human starter, first human poster, or webhook `owner` becomes thread `owner`. An ant starting a thread (`post_channel` or `ants post`) becomes its `creator` (ownerless until a human posts); spin-off messages via `send_message` carry `origin_thread` and propagate `creator` across `ants_links` (see [Credentials](#credentials)). On threads no channel post started, the `owner` of an ant's message is the one in `ants_owners` for the thread, else the one recorded for its `origin_thread` (spin-offs via `send_message` with `on: new` or a thread ID), each only when a human or webhook messaged the ant on that thread, or an ant they did passed the link on (`ants_links`), else none: the owner an ant names there is never taken as is.
- **Turn budget:** each agent post spends 1 turn from the thread's shared budget (minimum turns remaining across later posts on the thread, so fan-out never multiplies turns). Exhausted posts/answers are withheld and escalated (`turn_budget`). Human posts, or an escalation leader's reply (posted to the channel with 7 turns left), renew the budget.

### Channel owners

`--owner ANT` lets that ant call `manage_channel` to add/remove members and set/clear the channel messenger; only humans create/delete channels or change owners. Owner daemons send requests on `ants.chanadm.<channel>.<ant>` (retried every 2s for 10s; 60s TTL) to `ants channel serve` (`ants auth channel-manager` creds, queue-grouped with CAS on `ants_channels`), which checks ownership and roster entries (humans cannot be messenger or owner) and announces changes on `ants.chan.<channel>.channel-manager`.

### Channel delivery and human members

Each ant reads `ants.chan.>` from creation onward via durable consumer `chan-<ant>`, prepending up to 50 unseen thread posts tracked at `<ant>.<thread>.seen` in `ants_sessions`, with the same attempt, ack-after-store and dead-letter guarantees as inboxes. Mentioned human members receive an `Envelope` (`mention: {channel, post[, author]}`) on `ants.inbox.<human>.<sender>` (dedup msg ID `mention.<post>.<human>`, marker `<sender>.mention.<seq>.<human>`), sent by the author's daemon or `ants post`, or relayed by the channel messenger's daemon for verified webhooks.

## Escalations

Unresolved thread conditions — `turn_budget`, `cap`, `notes`, `dead_letter`, `context_window` (see [Lost sessions and the context window](#lost-sessions-and-the-context-window)), `asked` (`escalate` tool), plus single-hop `approval` (see [Tool permissions](#tool-permissions)) — climb the thread's origin chain before falling back to thread `owner`, else daemon `human` (default `human`).

- **Origin chain:** the stuck ant's `escalate_to` (persisted in `ants_chains`), or on ant-created channel threads and verified `origin_thread` spin-offs, level 1 of the `creator` ant's `escalate_to` (both stuck ant and creator marked visited).
- **Group coordination:** all unvisited ants in `escalate_to` receive an `[ants]` inbox notice on the thread, and thread notes get 8 fresh turns for that level. The first listed ant leads: only the leader may answer the stuck ant (renewing turn budget and spending allowance once per level) or call `escalate`. Non-leaders reply into thread notes and cannot message the stuck ant directly on the thread.
- **Advancement and reset:** after `escalate_after` (`30m`; `"0"` waits indefinitely) without a leader reply, or if a member gets stuck, escalation climbs to the leader's `escalate_to`. Empty chains, visited leaders or `max_escalation_hops` (default 3) hand off to the human owner. Only a human message on the thread resets escalation to level 0.
- **Reaching a human:** the notice to the owner, else the default human, carries the escalation at every level, so `ants webhook serve` pushes it (see [Notifications](#notifications)). Without a human message to the ant on the thread within `escalation_reminder` (default `4h`, `"0"` disables), that human gets an `[ants] Reminder:` on the thread, carrying the escalation (so pushed too), naming the stuck ant, reason and waiting time, and the `ants ask --thread` command that ends it, then one daily for 30 days after the escalation first reached a human. The reminder is kept with the thread's escalation in `ants_sessions` and listed with its deadlines, so only the daemon serving the ant sends it, once, across restarts and hand-overs. `ants thread list --escalated` lists the escalations still open within `--since`.
- **Watching:** once a daemon stores a message carrying an escalation (up a chain, to a human, a reminder, a tool request sent up the chain or expired), it publishes an `escalated` status update naming the recipient and carrying the escalation, without the message's text, on the sender's `ants.status.<ant>.<thread>`; observer credentials hear it (see [Hooks](#hooks)).

## Dead letters

Inbox and channel messages get 5 prompt attempts (`max_deliver = 6`); a harness that is signed out, held back by a provider limit or held for failing to start spends none (see [Sign-in](#sign-in), [Provider limits](#provider-limits) and [Harness failures](#harness-failures)), nor does a prompt retried in a new session after outgrowing the context window. When attempt 5 fails — or on delivery 6 if attempt 5 crashed without recording a reply — the daemon writes a `DeadLetter` to `ants.dead.<ant>`, escalates a `dead_letter` notice on the thread, and terminates the consumer delivery. Unparseable or subject-mismatched payloads are dead-lettered immediately without envelopes so they cannot be replayed.

```sh
ants dead list          # seq, ant, thread, from, deliveries, time, error, text
ants dead replay 42     # republish original message to its inbox and delete dead letter (human creds)
ants dead drop 42       # delete dead letter without replaying (human creds)
ants dead watch         # convert MAX_DELIVERIES advisories into dead letters until Ctrl-C
```

If NATS errors prevent storing the dead letter on delivery 6, JetStream emits unpersisted advisory `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ANTS.<consumer>`. `ants dead watch` (and `ants inbox --follow`) captures that advisory, writes the dead letter and notifies the thread owner (or default human) directly, past the escalation chain; the notice carries a level-0 `dead_letter` escalation, so `ants webhook serve` pushes it and `ants thread list --escalated` lists it, but no reminder follows, as human credentials cannot keep the escalation in `ants_sessions`. A daemon finishing a burial from its outbox on the last delivery sends the same notice under the same message id. Offline leaf daemons note burials in `<home>/buried` and check `deadread-<ant>` on reconnect to avoid duplicates.

## JetStream storage

A full JetStream store refuses new messages: webhooks answer `503`, ants keep messages in their outbox or retry them, and dead letters cannot be stored. Every 5 minutes (first 30 seconds after start), one ant of each daemon (per NATS server it serves ants on) reads its account's JetStream usage on the hub and, through a leaf node, on the leaf node, and reports it in `ants_jetstream_bytes` (see [Monitoring](#monitoring)). Past `warn_percent` of a store's limit (`[storage]` of the config file, default 80), the daemon sends the default human (`human`) a notice naming the streams using most and the setting to raise, carrying a level-0 `storage` escalation, so `ants webhook serve` pushes it; no reminder follows. One notice goes out per crossing, whichever daemons see it and however often they restart: the first to create the note `_storage.<domain>.<store>.warned` in `ants_sessions` (`_` without a domain) sends it, and the note goes once usage falls more than 5 points below the share, so that the next crossing warns again.

A store's limit is the account's when it has one. Accounts that `ants auth` issues have none, so the hub's limits, its `max_file_store` and `max_memory_store` (by default 75% of the disk and of the memory), come from `max_file_store` and `max_memory_store` of `[storage]`; a leaf node's are those `ants leaf config` writes (1 GB of file, 64 MB of memory). Without a known limit the usage is only reported. `ants doctor` reports the same (**storage**).

## Webhooks

`ants webhook serve` listens on plain HTTP (`127.0.0.1:8424` by default, behind a TLS reverse proxy) and turns `POST <path>/<webhook>` requests into new inbox or channel threads using per-webhook NATS credentials (`~/.ants/webhooks.toml` or `--config FILE`):

```toml
listen = "127.0.0.1:8424"                # overridden by --listen
metrics = "127.0.0.1:9465"               # serve /healthz and /metrics (default: off); overridden by --metrics
path = "/hooks"                          # POST /hooks/<name>
nats = "nats://127.0.0.1:4222"           # overridden by --nats / ANTS_NATS_URL
js_domain = "hub"                        # overridden by --js-domain / ANTS_JS_DOMAIN

[[webhook]]
name = "github"
creds = "~/.config/ants/auth/github.creds"    # ants auth user github --webhook --to dev-a
auth = "hmac-sha256"                          # signature_header defaults to X-Hub-Signature-256 ([sha256=]<hex>)
secret_file = "~/.ants/github.secret"         # mode 0600 (owner-readable only); or secret_env = "VAR"
id = "header:X-GitHub-Delivery"               # or "json:/pointer"; default "body-sha256"
ant = "dev-a"
owner = "alice"                               # optional human owner for replies and escalations
text = "GitHub {header:X-GitHub-Event} on {json:/repository/full_name}\n\n{body}"
allow = ["read:*", "execute:gh pr view*"]     # optional tool rules of its threads; default read-only, ["*"] opts out

[[webhook]]
name = "alerts"                               # ants auth user alerts --webhook --channel ops
creds = "alerts.creds"
auth = "bearer"                               # Authorization: Bearer <secret>
secret_env = "ANTS_ALERTS_TOKEN"
id = "json:/alert/id"
channel = "ops"
mention = ["dev-a"]                           # optional; or broadcast = true for all current members
```

- **Auth and limits:** `hmac-sha256` (the hex of the body's HMAC-SHA256 in `signature_header`, after `sha256=` or alone), `bearer` (`Authorization: Bearer <secret>`) and `token` (the secret alone in `token_header`, such as GitLab's `X-Gitlab-Token`) check secrets in constant time (`401` on failure). Max body `256 KiB` (`413`), timeout `15s`, max `64` concurrent requests (`503` with `Retry-After: 5` when overloaded or NATS cannot store immediately; nothing buffers locally). Bad JSON/ID returns `400`, non-UTF-8 `{body}` `415`, unknown hook `404`, non-`POST` `405`.
- **Deduplication and marker:** stored events return `202` (`{"thread": "...", "duplicate": false}`); repeats within 7 days match `<webhook>.<hash>` in `ants_webhooks` (plus 24 h `Nats-Msg-Id` `webhook.<webhook>.<hash>`) and return `200` (`"duplicate": true`). `ants webhook serve` puts `<webhook>.webhook` in `ants_webhooks` on connecting and hourly, holding the machine, ants version and permissions version it runs (`{"machine": "...", "version": "...", "perms": 2}`; before versions were published, the webhook's name, put daily on delivery).
- **Templates and routing:** `text` substitutes `{body}`, `{id}`, `{webhook}`, `{header:Name}` and `{json:/pointer}` (`{{` and `}}` escape braces). Payloads are untrusted input (prompt injection): the threads a webhook starts are narrowed to its `allow` (default read-only; `["*"]` opts out) until a human messages the ant on the thread (see [Tool permissions](#tool-permissions)). Mentions come strictly from config (`mention` or `broadcast`). Inbox webhook replies go to `owner` if set, else are logged; webhook posts never reset turn budgets.
- **Reload:** `SIGHUP` reloads the config file without a restart, and so does a change to the file itself (checked every 3 s, acted on once unchanged across two checks). Webhooks and `[[human]]`s newly listed start, those no longer listed stop, and those whose entry or credentials file changed start again (every one of them for another `nats` or `js_domain`, every human for another `public_url` or action secret); the others and the HTTP listener carry on untouched, and so do requests under way. A file that fails to load is logged and the config served stays. `listen` and `metrics` take a restart.

### Adding a webhook

`ants webhook add` does the steps above in one go, on the machine running `ants webhook serve`:

```sh
ants webhook add github --to dev-a --preset github --js-domain hub
ants webhook add alerts --channel ops --preset grafana
```

It issues the webhook's credentials with the account key, as `ants auth user NAME --webhook --to ANT` (or `--channel CH`) does, into `<dir>/<name>.creds` (`--dir`, default `~/.config/ants/auth`; `--out FILE`), for `--js-domain` or else the file's `js_domain`. Away from the account key, `--enroll CREDS [--hub URL]` asks for them over NATS with enrollment credentials from `ants auth enroll NAME`, as `ants add --enroll` does, and waits for `ants enroll approve NAME --webhook --to ANT` (or `--channel CH`) there; a request through a leaf node is refused, as the ingress connects to the hub. It writes a new random secret (64 hex digits) to `<name>.secret` beside the config file, mode `0600`, appends the `[[webhook]]` entry to the config file (created when missing; the text already there, comments included, stays as it is, and a name already listed is refused), reloads the webhook service serving that file when it runs (a running ingress also picks the change up by itself within seconds), and prints the URL to give the sender (`public_url` of the file, else the address the ingress listens on, for the reverse proxy to map), the secret, and where they go. `--preset` sets the entry up for a sender:

| Preset | `auth` | Header | `id` | `text` |
|---|---|---|---|---|
| (none) | `bearer` | `Authorization: Bearer <secret>` | `body-sha256` | the default |
| `github` | `hmac-sha256` | `X-Hub-Signature-256` | `header:X-GitHub-Delivery` | `GitHub {header:X-GitHub-Event} on {json:/repository/full_name}`, then the body |
| `gitlab` | `token` | `X-Gitlab-Token` | `header:Idempotency-Key` (GitLab 17.4 and later; the same across retries) | `GitLab {header:X-Gitlab-Event} on {json:/project/path_with_namespace}`, then the body |
| `grafana` | `hmac-sha256` | `X-Grafana-Alerting-Signature` (Grafana's HMAC signature, with no timestamp header) | `body-sha256` | `Grafana: {json:/title}`, then `{json:/message}` |

GitHub needs the content type `application/json`. GitLab's newer signing token (`webhook-signature`) is not supported: give the secret as the secret token.

### Notifications

`[[human]]` entries in the same file push tool requests (`ants.approvals.<ant>`), escalations reaching the human's inbox and, when asked for, ants' replies to phones and chats, so approvals need no terminal running `ants tools list -f` or `ants inbox -f`:

```toml
public_url = "https://hub.example/hooks"      # what the reverse proxy maps to `path`; enables approve/reject links
action_secret_file = "~/.ants/action.secret"  # HMAC key of the links, 16+ characters, mode 0600; or action_secret_env
timezone = "Europe/London"                    # optional: the zone of quiet hours (default UTC)

[[human]]
name = "alice"
creds = "~/.config/ants/auth/alice.creds"     # ants auth user alice --human
ants = ["dev-a", "dev-b"]                     # optional: only these ants' tool requests (default: every ant)
approvals = true                              # tool requests (default true)
escalations = true                            # escalations reaching alice's inbox (default true)
completions = true                            # replies reaching alice's inbox (default false)
quiet = "22:00-07:00"                         # optional quiet hours, local time, start before end or across midnight
timezone = "America/New_York"                 # optional: the zone of alice's quiet hours (default the top-level one)
bypass = ["approvals"]                        # optional: what quiet hours and a snooze let through: approvals, escalations

[[human.notify]]
kind = "ntfy"
topic = "alice-ants-7f3k"                     # url defaults to https://ntfy.sh
secret_file = "~/.ants/ntfy.token"            # optional access token (bearer)

[[human.notify]]
kind = "slack"
secret_file = "~/.ants/slack.url"             # the incoming webhook URL, a secret

[[human.notify]]
kind = "telegram"
chat = "123456789"                            # chat id; url defaults to https://api.telegram.org
secret_env = "ANTS_TELEGRAM_TOKEN"            # the bot token

[[human.notify]]
kind = "ntfy"
topic = "alice-laptop-9c2e"
device = "01968f7e-..."                       # optional: a device id (ants auth devices alice)

[[human.notify]]
kind = "exec"
command = ["~/bin/ants-notice", "--to", "alice"]  # program and arguments, no shell; a bare name is found on PATH
timeout = "20s"                               # optional, any kind: 1s to 30s (default 10s)

[[human.notify]]
kind = "http"
url = "https://hooks.example/ants"            # receives the notice as signed JSON
secret_file = "~/.ants/notice.secret"         # HMAC-SHA256 key, 16+ characters, mode 0600; or secret_env
```

- **Test:** `ants webhook notify-test HUMAN [--config FILE]` sends a test notice, naming the machine, through each sink of `HUMAN` at once, those of a device too whatever its presence, and needs no NATS. It prints `ok    SINK` or `fail  SINK: ERROR` per sink (the HTTP status and the start of the answer, a command's exit status, or why none came within the sink's `timeout`; never URLs, tokens or command arguments) and exits with status 1 when one fails. With quiet hours, it also prints them, their zone, whether they hold now and what `bypass` lets through.
- **One notice per event:** each human is its own NATS user and reads durable consumer `notify-<human>` on `ANTS` (deliver new, explicit ack), so several `ants webhook serve` processes listing the same human share it and each request, escalation or reply is sent once. Requests already decided or expired, unreadable or forged ones, and escalations older than a day are skipped. Sinks run concurrently, each within its `timeout`; failures are logged (never with URLs or tokens) and not retried.
- **Per device:** a sink with `device` is that device's push target and gets no notice while the device follows what the notice is about: `ants tools list -f`, `ants dash`, or `ants inbox -f` without `--thread` and `--json` for tool requests, `ants inbox -f` without `--thread` for escalations. The human's other devices still get it. While following, the CLI puts the device's presence in `ants_presence` every 10s and deletes it when it stops; a device that stops without deleting it (crash, lost connection) counts as away within 30s. A sink without `device` gets every notice, as before. Unreadable presence counts as away, so a failure sends a notice too many, never one too few. Legacy devices (credentials without a device id) record no presence.
- **Quiet hours, snooze, muted threads:** a notice is held back while the thread it is about is muted, else, unless its kind (`approvals` for tool requests, `escalations`) is in `bypass`, while the human is snoozed or within quiet hours. A held notice is dropped (logged at info level as `held back a notice`, with why and its title), not sent later; it marks nothing read and decides nothing, so a held tool request stays pending in `ants tools list` and the inbox keeps the held messages unread. `bypass` (default empty) never lets a muted thread through. Test notices are never held back.
  - **Quiet hours** are configuration: `quiet = "HH:MM-HH:MM"` per human, read on the wall clock of the human's `timezone`, else the file's top-level `timezone`, else UTC, daylight saving time included (an IANA name such as `Europe/London`; an unknown name, a malformed window or one starting where it ends fails loading the file, as does `timezone` without `quiet`). A window from `22:00` to `07:00` crosses midnight; the start is held back, the end is not.
  - **Snooze and muted threads** are the human's runtime state, shared by all its devices: `ants notify snooze DURATION` (`30m`, `1h`, `2d`; at most `365d`), `ants notify resume`, `ants notify status [--json]`, and `ants thread mute THREAD [--for DURATION]` (default `30d`, as long as `ANTS` keeps a thread's messages) and `ants thread unmute THREAD`, with human credentials. They live in KV `ants_notify` as one JSON key per human (`{"snoozed_until": ..., "muted": {"<thread>": ...}}`), changed by compare-and-set so devices changing them at once keep each other's changes. Each hold stores its deadline and ends there on its own; ended holds are pruned on the next change. `ants webhook serve` reads the key by direct get for each notice, and an unreadable or malformed key holds nothing back.
- **Completions:** with `completions = true` (default false), a reply (a message answering another, not an escalation, mention or webhook message) reaching the human's inbox is pushed as `<ant> replied`, with the text and the `ants ask --thread` command to answer on the thread; the shared `notify-<human>` consumer sends each once, whatever the number of services and devices. A device following `ants inbox -f` without `--thread` gets none.
- **Content:** approval notices carry the ant, tool title and kind, files, an input excerpt, thread, time left and the `ants tools approve|reject <ant>/<id>` commands; a request whose call was approved twice before carries the allow rule `ants tools approve --rule` would add; escalation notices carry the reason, the stuck ant, the text and the `ants ask --thread` command to answer on the escalation's thread (an expired tool request's notice is titled as such and answers on the request's thread); `[ants] Reminder:` messages of an unanswered escalation are pushed the same way. Plain messages, and replies without `completions = true`, are not notified.
- **Approve/reject links:** with `public_url` and the action secret, approval notices carry one link per offered verdict (`Approve`, `Always allow`, `Reject`): `<public_url>/_approval?ant=&id=&seq=&verdict=&by=&exp=&sig=`, HMAC-SHA256 over every field, valid until the request expires. `GET` shows the request, its state and a confirm button; `POST` stores the decision with the human's credentials exactly as `ants tools approve|reject` does (first answer wins). Answers: `200` stored or shown, `400` incomplete link, `403` wrong signature, `404` unknown human or request, `405` not `GET`/`POST`, `409` already answered or expired, `410` past expiry, `503` hub unreachable. Without `public_url`, notices carry only the commands. Anyone holding a link can decide that one request, so keep sinks private (ntfy topics are public unless the server requires a token).

| Sink | Notice | Actions |
|---|---|---|
| `ntfy` | JSON publish, priority high for tool requests | HTTP action buttons that `POST` the link: one tap decides |
| `slack` | Incoming webhook, `mrkdwn` section | Link buttons opening the confirm page (Slack may flag them without an app interactivity URL; harmless) |
| `telegram` | Bot API `sendMessage`, link previews off | Inline URL buttons opening the confirm page |
| `exec` | The [JSON notice](#json-notice) on the command's standard input | The `links` of the JSON |
| `http` | The [JSON notice](#json-notice) in a signed `POST` | The `links` of the JSON |

Every kind goes through the same path: one notice per event and human, the per-device filter, all sinks at once, each within its `timeout` (default `10s`, at most `30s`), and a failure logged by the sink's label (`ntfy topic T`, `slack`, `telegram chat C`, `exec PROGRAM`, `http HOST`), never stopping the others nor naming URLs, tokens, secrets or command arguments.

- **exec:** runs `command` directly (no shell), relative paths and `~` resolved like the file's other paths, with the environment and working directory of `ants webhook serve`. The JSON notice is written to its standard input as one line, which then closes; its standard output is dropped and its standard error goes to that of `ants webhook serve` (the service's log). It succeeds by exiting with status 0 within `timeout`; another status fails it (`exit status: 3`), and past `timeout` it is killed (`SIGKILL`, the command itself, not processes it started) and reaped (`no exit within 20s: killed`). It takes no `url`, `secret_file` or `secret_env`: the command reads its own secrets.
- **http:** `POST`s the JSON notice to `url` (no user, password or query) with `Content-Type: application/json`, `X-Ants-Timestamp: <Unix seconds>` and `X-Ants-Signature: sha256=<hex>`, the lowercase hex HMAC-SHA256, keyed with the secret, of the timestamp's digits, a `.` and the raw body (`<timestamp>.<body>`). Any `2xx` answer succeeds; another fails it (`answered 401 Unauthorized: ...`). A receiver should recompute the signature over the raw body, compare it in constant time, refuse a timestamp more than 5 minutes from its clock, and drop a notice whose `kind` and `id` it has already taken.

#### JSON notice

`exec` and `http` sinks get each notice as one JSON object, version 1 of this schema. Later versions only add fields and kinds (`completion` came later than the others); a change that breaks readers of version 1 raises `version`. A reader should ignore an unknown `kind`.

```json
{
  "version": 1,
  "kind": "approval",
  "id": "01968f8a-…",
  "human": "alice",
  "ant": "dev-a",
  "thread": "01968f89-…",
  "title": "dev-a asks to run a tool",
  "text": "cargo publish (execute)\nthread …, request dev-a/01968f8a-…, rejected in 9m59s unless answered\n…",
  "urgent": true,
  "sent_at": "2026-11-15T05:18:05Z",
  "request": "dev-a/01968f8a-…",
  "seq": 4211,
  "tool": { "kind": "execute", "title": "cargo publish", "locations": ["Cargo.toml"], "input": "{…}" },
  "options": ["allow_once", "allow_always", "reject"],
  "expires_at": "2026-11-15T05:28:05Z",
  "links": [
    { "verdict": "allow_once", "label": "Approve", "url": "https://hub.example/hooks/_approval?…" },
    { "verdict": "allow_always", "label": "Always allow", "url": "https://hub.example/hooks/_approval?…" },
    { "verdict": "reject", "label": "Reject", "url": "https://hub.example/hooks/_approval?…" }
  ]
}
```

| Field | Kinds | Meaning |
|---|---|---|
| `version` | all | `1` |
| `kind` | all | `approval` (a pending tool request), `escalation` (an escalation reaching the human's inbox), `completion` (a reply reaching it, with `completions = true`) or `test` (`ants webhook notify-test`) |
| `id` | all | The tool request's id, the escalation or reply message's id, or a fresh one per test; with `kind`, what to deduplicate on |
| `human` | all | The human notified |
| `title`, `text` | all | The notice as the other sinks show it: a title and plain text with the commands answering it |
| `urgent` | all | `true` for tool requests |
| `sent_at` | all | When the notice was formatted, RFC 3339 UTC |
| `links` | all | One `{verdict, label, url}` per offered verdict, as described under approve/reject links; empty without `public_url`, and for escalations, completions and tests |
| `ant`, `thread` | approval, escalation, completion | The ant asking, the ant whose message escalates, or the one replying; the thread to answer on |
| `request` | approval | `<ant>/<id>`, as `ants tools approve\|reject` take it |
| `seq` | approval | The request's sequence on stream `ANTS` |
| `tool` | approval | `kind`, `title`, and `locations` and `input` when the request has them |
| `options` | approval | The verdicts offered: `allow_once`, `allow_always`, `reject` |
| `expires_at` | approval | When the ant rejects the request unless answered, RFC 3339 UTC |
| `reason`, `stuck`, `level` | escalation | Why (`turn_budget`, `approval`, …), the stuck ant, and the escalation level (0: the stuck ant itself) |

Human credentials issued before notifications lack the `notify-<human>` consumer permissions; connecting with them warns and names the command that renews them ([Credentials across upgrades](#credentials-across-upgrades)). Those issued before presence lack `ants_presence`: `ants webhook serve` refuses them for a human with device sinks, and following on such a device warns and records no presence, so its sinks keep getting every notice; renew them (and the leaf uplinks where they run) and run `ants init`. Those issued before snoozes and muted threads lack `ants_notify`: `ants webhook serve` warns, naming the command that renews them, and sends every notice its quiet hours let through, and `ants notify` and `ants thread mute` fail for them.

## Schedules

`[[schedule]]` entries of the config file send a prompt on a timetable: each firing sends `text`, literally, as a new thread to an ant's inbox or a channel. `ants daemon` runs them when serving the whole file (not with `--ant`), each over its own NATS connection with its own credentials:

```toml
[[schedule]]
name = "nightly"                              # ants auth user nightly --webhook --to dev-a
creds = "~/.config/ants/auth/nightly.creds"
cron = "0 3 * * 1-5"                          # minute hour day-of-month month day-of-week
timezone = "Europe/London"                    # optional: the zone cron reads in; default the top-level timezone, then UTC
ant = "dev-a"
owner = "alice"                               # optional human owner for replies and escalations
text = "Triage the failing nightly tests and summarise them."
allow = ["read:*", "execute:cargo test*"]     # optional tool rules of its threads; default read-only, ["*"] opts out
catch_up = true                               # optional: send the latest missed firing late (see Missed firings)

[[schedule]]
name = "standup"                              # ants auth user standup --webhook --channel ops
creds = "standup.creds"
every = "4h"                                  # at whole multiples since 1970-01-01 00:00 UTC
channel = "ops"
broadcast = true                              # optional; or mention = ["dev-a"]; else the channel messenger triages
text = "Post what changed since the last standup."
nats = "nats://127.0.0.1:4222"                # optional, as js_domain: default the top-level values, then the flags
```

- **Timetable:** exactly one of `cron` and `every`. `cron` takes 5 fields with `*`, lists, ranges, steps (`*/15`, `1-5/2`, `10/20`), month and weekday names (`jan`, `mon`; day of week `0` and `7` are Sunday) or `@yearly`, `@monthly`, `@weekly`, `@daily` and `@hourly`; when both day fields are restricted, a day matching either fires (as cron does). `every` is a duration of at least `1m` (`15m`, `1h30m`, `1d`), anchored at the Unix epoch whatever the time zone, so `every = "1d"` fires at 00:00 UTC.
- **Time zone:** `cron` reads in the IANA time zone `timezone` names (`Europe/London`, `Asia/Tokyo`), else the file's top-level `timezone`, else UTC; `timezone` goes with `cron` only, and an unknown name fails loading the file. Daylight saving time applies: a local time skipped when the clocks go forward fires once, at the instant they jump (`30 2 * * *` fires at 03:00 on a night going from 02:00 to 03:00, and `*/15 2 * * *` once then), and a local time repeated when they go back fires once, at its first occurrence, so `0 * * * *` skips the repeated hour; `every` keeps a steady interval. Zones come from the system's time zone database (`/usr/share/zoneinfo`, or the directory `TZDIR` names), and on Windows from one built into `ants`.
- **Destination:** exactly one of `ant` and `channel`; `mention` and `broadcast` (all members at the firing) only with `channel`. `owner` works as for webhooks.
- **Identity:** a schedule is a webhook-role user (`ants auth user NAME --webhook --to ANT` or `--channel CH`, and `--webhook NAME` in `ants auth leaf`), checked at load: its credentials must name it, be no human's and allow its destination. Its messages carry `webhook: true` and it puts `<schedule>.webhook` in `ants_webhooks` hourly, as a webhook does, so replies, triage and the narrowing of its threads to its `allow` (default read-only, `["*"]` opts out; see [Tool permissions](#tool-permissions)) work as for a [webhook](#webhooks).
- **Once per firing:** several daemons running the same schedule, or one restarted, send each firing once: its key `<schedule>.<unix time>` in `ants_webhooks` (7 days) and its 24 h `Nats-Msg-Id` `schedule.<schedule>.<unix time>` drop a repeat.
- **Missed firings:** a firing goes out up to 1 minute late, retried every 5s while the hub does not store it, and fires after a restart within that minute. A firing missed by more (no daemon running, hub unreachable, machine asleep) is logged and skipped, unless the schedule sets `catch_up = true` (default `false`): then, once a daemon runs and reaches the hub again (at start, or on waking), the latest firing missed within the last 7 days goes out late, once, with `(Sent late: due YYYY-MM-DD HH:MM UTC.)` appended to `text`; older missed firings are skipped, never sent as a backlog, and none is made up when a later firing is due within the minute. It keeps the firing's own key `<schedule>.<unix time>`, so several daemons, restarts or an earlier on-time send never repeat it, and a firing the hub does not take is retried every 5s until the next one is due.

## Credentials

`ants auth` manages decentralized NATS NKey/JWT credentials in `~/.config/ants/auth` (`--dir` / `ANTS_AUTH_DIR`; keep `*.nk` seeds private):

```sh
ants auth init                                          # writes operator.nk, account.nk, system.nk, server.conf
ants auth user human --js-domain hub                    # human.creds ("human" always gets Role::Human)
ants auth user alice --human --js-domain hub            # alice.creds (human role)
ants auth user dev-a --js-domain hub                    # dev-a.creds (ant role)
ants auth user github --webhook --to dev-a --js-domain hub
ants auth user nightly --webhook --to dev-a --js-domain hub  # a schedule sends as a webhook
ants auth channel-manager --js-domain hub               # channel-manager.creds
ants auth user audit --observer --js-domain hub         # audit.creds (read-only, for hooks)
ants auth user alice --human --store-key --js-domain hub  # alice.creds naming a seed in the OS credential store
```

Every key issued or signed here (`ants auth user`, `enroll`, `channel-manager`, `leaf`, `enroll approve`) is recorded in `issued.jsonl` (key, name, kind, issue time, expiry, permissions version, and a human's device id and label). Credentials stay valid until they expire, unless revoked:

```sh
ants auth revoke alice --nats wss://hub.example/nats        # every key issued to alice, on all devices
ants auth revoke --key UABC... --nats wss://hub.example/nats # one key
ants auth revoke leaf-macbook --no-push                     # a leaf uplink; enroll-<ant>, channel-manager alike
ants auth revoked                                           # key, name, revoked at, expiry
ants auth issued                                            # every key, and how to renew outdated or expiring ones
ants auth push --nats wss://hub.example/nats                # push the account again
```

`ants auth revoke NAME` takes the keys recorded for `NAME` plus those of `.creds` files in the team directory carrying that name (credentials issued before the registry), and fails when it knows none: revoke those by `--key`. Revocations go to `revoked.json` and into the account JWT of `server.conf` (NATS account revocations: JWTs of the key issued at or before the revocation are refused; new credentials for the name stay valid). The account is then pushed to the `--nats` server (the hub) as a short-lived system account user (`$SYS.REQ.ACCOUNT.<account>.CLAIMS.UPDATE`), which closes the revoked users' connections at once. Leaf nodes joined to the hub's system account by `ants leaf config` (with the `leaf-<machine>.system.creds` that `ants auth leaf` writes) apply the push too, closing the revoked users' local connections, keep it across restarts, and are listed in its output; a leaf node offline during the push catches up within 10 s of reconnecting when the hub runs a `full` resolver (below), and otherwise gets it from the next `ants auth push`. When the hub is out of reach the revocations stay recorded; push them later with `ants auth push`, or copy the new `server.conf` to the hub and reload it (`SIGHUP`). A hub with a `full` resolver (below) stores each account pushed to it and keeps it across restarts and reloads with an older `server.conf`, while a newer `server.conf` still replaces it; the push says whether the hub keeps it (only a `full` resolver answers `$SYS.REQ.CLAIMS.LIST`), and so does `ants doctor`. Copy `server.conf` to such a hub only for what a push does not carry: its operator, system account and `max_control_line` (a new team, or an ants version writing them differently). A hub without a `full` resolver keeps pushed accounts in memory only: copy the new `server.conf` to it and reload it after every revocation, as a restart otherwise brings back the old account. Leaf nodes without the system account uplink authenticate their local clients against their own `server.conf`, so a revoked ant behind one keeps working there until it reloads a new `server.conf` (or until `ants auth leaf MACHINE --remove-ant ANT` reissues the uplink without the ant); give them the uplink with `ants auth leaf` and `ants leaf config` and restart them, once the hub runs a `server.conf` written since (its system account then admits leaf nodes). A hub on a `server.conf` from before revocations accepts no system account connection, so the first push fails until it runs on a new one. Revocations of credentials that have all expired are dropped on the next `revoke` or `push`.

### Backing up the team keys

Without the operator and account seeds no credentials can be issued, renewed, signed or revoked again: keep a backup of the team directory off the machine holding it.

```sh
ants auth backup /media/usb/ants-team.age               # asks for a passphrase twice
ANTS_BACKUP_PASSPHRASE=... ants auth backup team.age    # or --passphrase-file FILE, for scripts
ants auth restore team.age --dir ~/.config/ants/auth    # into a directory without team files; --force replaces them
```

The backup holds `operator.nk`, `account.nk`, `system.nk`, `server.conf`, `issued.jsonl` and `revoked.json`; `.creds` files stay out, as `ants auth` issues them again. The team keys always live in these files, never in the OS credential store, so a restore writes them back as files, readable by their owner only. The file, mode 0600, is ASCII-armored [age](https://age-encryption.org) encrypted with the passphrase (scrypt, work factor 18, then ChaCha20-Poly1305), so `age -d` decrypts it too and a file of `age -p` restores; an empty passphrase is refused, and a wrong passphrase or a damaged or altered file restores nothing. The passphrase comes from the first line of `--passphrase-file`, else `ANTS_BACKUP_PASSPHRASE`, else the terminal, without echo. `backup.json` in the team directory records the last backup (time, file, account, entries of `issued.jsonl`, digest of `revoked.json`), and a restore records the backup restored; `ants doctor` warns until a backup holds every key issued and revocation made. A restore brings back the directory as it was at the backup: keys issued since are missing from `issued.jsonl`, and revocations made since are lost, so revoke those keys again and push the account with `ants auth push`.

### Devices

Each `ants auth user NAME --human` (and each signed `--public-key` of a human) issues credentials for a new device of the human: a generated id (a UUIDv7, never chosen and never reused), carried in the user JWT as the tag `ants-device:<id>` and in the connection name of the CLI, plus an optional `--label` for display, which need not be unique. Credentials issued before device ids are a legacy device.

```sh
ants auth user alice --human --label "Alice's laptop"      # prints alice.creds, and the device id on stderr
ants auth devices alice                                   # id (or legacy), label, key, issued, revoked
ants auth rename alice "Alice's laptop" "Work laptop"     # by label, id or key
ants auth revoke alice --device 01968f7e-...              # that device only
```

A label naming more than one device is refused, with the ids of those devices; give an id then. Every device of a human reads the same inbox: `ants inbox -f` on each shows every new reply, and what one device reads (`ants inbox`, `ants ask --wait`) is read on all, kept in the human's read marker in `ants_read` (updated with compare-and-set, so devices reading at once keep each other's marks). The first `ants inbox` with new credentials moves the read state of the old shared consumer `inbox-<name>` into the marker and deletes the consumer; a CLI from before would create it again, and show every reply anew, so upgrade the CLI on every device. Push targets are per device too: a `[[human.notify]]` sink with `device = "<id>"` skips notices while that device follows them (see [Notifications](#notifications)). Human credentials (and leaf uplinks carrying humans) issued before the read marker lack its permissions: `ants inbox` refuses them and `ants ask --wait` leaves its reply unread until they are renewed ([Credentials across upgrades](#credentials-across-upgrades)).

Copy `server.conf` next to the hub `nats-server` config and add `include "server.conf"` (`include` resolves relative to the config file even with a leading `/`; `server.conf` sets `max_control_line: 16384`), followed by `resolver { type: full, dir: "/ABSOLUTE/DIR" }` so the hub keeps pushed accounts in that directory across restarts and leaf nodes catch up on pushes they missed while offline (see [docs/transport.md](docs/transport.md#hub)). The `dir` is relative to the working directory of `nats-server`, not to the configuration file, hence absolute; a hub switching resolvers needs a restart, not a reload. `server.conf` itself keeps a `MEMORY` resolver, which the line after the include replaces: leaf nodes include it too, with their own `full` resolver, and a hub reloading a new `server.conf` would not switch resolvers. Run `ants init` once with human credentials to create `ANTS` and all fourteen KV buckets. The first connection of every other CLI command with human credentials (or none) does the same: in one round of concurrent stream info requests, it creates missing buckets and adds what an older version lacks (stream subjects, direct gets, inbox republishing, the 24 h duplicate window of `ANTS` and its room for 10000 consumers, the `ants_owners` duplicate window, the markers of expired `ants_roster` entries), keeping other settings such as outbox sources, and logs each change. It skips objects whose stream info the credentials do not allow (`ants_presence` for every human, which `ants init` and following create without it), warns that `ants init` is needed for changes they cannot make, and ant and webhook credentials check nothing. So a publish on a subject an outdated `ANTS` does not capture, from a daemon, webhook or channel manager too, gets no stream while the hub's JetStream still answers: it fails naming `ants init` with human credentials as the fix, or, on a leaf node with an outbox, waits there with a warning saying the same, once per process for each kind of subject (`ants.chan`, `ants.dead` and so on). When machine members change, run `ants auth leaf MACHINE` with `--add-*`/`--remove-*` and then `ants leaf outbox MACHINE` again; when role permissions change, renew credentials as below.

Two settings depend on the hub's nats-server version. An `ants_roster` entry that expires leaves a marker that watchers see (nats-server 2.11 and later; adding them to an existing bucket needs human credentials of permissions version 3 or later): a hub older than that gets the bucket without them, and daemons waiting for an entry look again every 10 s instead. Since nats-server 2.15, a stream without a `max_consumers` of its own takes at most 1000 consumers (`default_max_consumers` under `jetstream { limits { ... } }` in the server configuration), and `-1` does not lift it. Each ant holds two durable consumers on `ANTS` and briefly up to about 15 more while reading, each human following notifications one, so `ants init` gives `ANTS` room for 10000 and keeps a higher limit set by hand; an account limit on consumers below that makes the change fail. Every watch of a KV bucket is a consumer of that bucket's stream, too: each daemon, channel manager and dead-letter reader watches `ants_humans`, each daemon `ants_channels` and each standby daemon its `ants_roster` entry, so `ants init` gives those three buckets room for 10000 as well, keeping a higher limit (a KV bucket takes it by a stream update after its creation, which needs human credentials of permissions version 4 or later).

### Credentials across upgrades

Each user JWT that `ants auth` issues carries the permissions version of the issuing ants as the tag `ants-perms:<n>`; the version goes up with every change to any role's permissions (cancel, spend, owners, notifications, inbox spares, the read marker, presence, ...); version 3 adds humans' batched direct gets of `ANTS` (`$JS.API.DIRECT.GET.ANTS`), humans' `STREAM.UPDATE` of the roster stream (`KV_ants_roster`, for the markers of expired entries) and the ants' `sentread-<ant>` consumer, which reads back the messages an ant sent; version 4 adds humans' `STREAM.UPDATE` of `KV_ants_channels` and `KV_ants_humans`, for their room for 10000 consumers, and ants' direct gets of every ant's daily spend (`$KV.ants_sessions.*.day.*`), which pool caps add up, ants' requests for new ants, and for retiring them, on `ants.antreq.<self>` and their answers on `ants.antreq.<self>.reply`, and the enrollment approver's hearing of every such request and answering it, with its tool requests to humans and the direct gets of their decisions; every user's put, delete and direct get of its own reactions (`$KV.ants_reactions.<self>.>`), and humans' creation, stream info, direct gets and ordered consumers of `KV_ants_reactions`; humans' creation and stream info of `KV_ants_threads`, their puts and direct gets of every thread title (`$KV.ants_threads.title.*`) and of their own pinned and archived threads (`$KV.ants_threads.marks.<self>`); humans' creation and stream info of `KV_ants_notify` and put and direct get of their own snooze and muted threads (`$KV.ants_notify.<human>`); humans' queue requests (`ants.queue.*`), creation and direct gets of what became of messages to ants (`$KV.ants_attempts.*.msg.*`, for `ants withdraw` and `ants queue`), and ants' hearing of their own queue requests (`ants.queue.<self>`) and answering on `ants.queued.<self>`; every user's listing of streams (`$JS.API.STREAM.LIST`, which shows no subjects stored), and ants' reading of the account's JetStream info (`$JS.API.INFO`) and their creation, direct gets and deletion of the storage warning notes (`$KV.ants_sessions._storage.*.*.warned`), for [JetStream storage](#jetstream-storage); and, for every role, the v2 form of JetStream ack and flow control subjects beside the v1 one: `$JS.ACK.<domain>.<account hash>.<stream>.<consumer>.…` and `$JS.FC.<domain>.<account hash>.<stream>.…` (domain `_` without one), which nats-server 2.16 uses by default (feature flag `js_ack_fc_v2`). On connecting, the CLI, the daemon, `ants webhook serve`, `ants channel serve` and `ants enroll` compare the version of the credentials in use with their own and log a warning, without failing, naming the command that renews them, worked out from the credentials themselves:

```text
WARN the credentials of ant dev-a carry permissions version 1, older than version 2 of this ants: requests their role allows now may be refused; renew them on the machine holding the team keys with `ants auth user dev-a --js-domain hub` and put the new file in their place, once `ants doctor` run as a human finds every daemon, `ants webhook serve` and `ants channel serve` at this permissions version (its `versions` check)
```

The command is `ants auth user NAME` (with `--human`, `--webhook --to ANT --channel CH` or `--observer`, and `--js-domain D` as issued), `ants auth channel-manager`, or for a leaf node's uplink `ants auth leaf MACHINE` (members as last issued; on credentials from before the team directory kept them, `--ant ANT --human HUMAN --webhook WEBHOOK [--enroll-approver]`) followed by `ants leaf outbox MACHINE`. Credentials without the tag predate versions and count as outdated; credentials newer than the ants in use ask for an upgrade of ants. nats-server, not ants, uses a leaf node's uplink credentials: `ants leaf config` checks those it writes into `leaf.conf`, the system account uplink's `leaf-<machine>.system.creds` included (renewed with the same `ants auth leaf`), and the leaf node needs a restart once they are replaced (`SIGHUP` does not reread credentials).

Credentials that expire within 7 days, or have expired, get a warning naming the same command; the daemon checks again daily. Enrollment credentials, short-lived by design, warn only once expired. `ants auth issued` lists every key of `issued.jsonl` once (`--json`: one object per line) and, under each one in use (neither revoked, reissued since for the same name, expired nor an enrollment's) that is outdated or expires within 7 days, the commands renewing it: from its `.creds` file in the team directory when there is one, else with placeholders for what only that file tells (`--to ANT...`, `[--js-domain DOMAIN]`). Keys recorded before versions show `perms -`.

Upgrades that withdraw rights older versions still use (stream info of `ants_sessions`, `ants_attempts`, `ants_owners`, `ANTS` and `ants_webhooks`) need renewed ant, webhook, channel manager and leaf credentials only after every daemon, `ants webhook serve` and `ants channel serve` runs the new version; until then, old credentials keep working. Each of them publishes the ants and permissions versions it runs: daemons in their roster entries (`ants roster` shows them and flags mixed versions), webhooks and schedules in their `<webhook>.webhook` marker, and the channel manager at `channel-manager.serve` in `ants_webhooks`, both put hourly. `ants doctor` run as a human reads them all (check **versions**) and tells whether every one runs this permissions version, so that renewing is safe; the warnings of outdated ant, webhook, channel manager and leaf credentials point to it. Components from before version publishing (permissions version 1) publish none and count as behind; humans' credentials from before version 2 cannot read the channel manager's entry, and its channel manager credentials cannot write it; humans' credentials and leaf uplinks from before version 3 read the `ANTS` stream back one message at a time instead of in batches. Ants' credentials and leaf uplinks from before version 4 cannot read other ants' daily spend, so pool caps do not hold back the ants using them (each prompt logs a warning). Until credentials are renewed after an upgrade adding rights, requests needing them are refused or unheard (`ants cancel`, `ants queue`, `ants withdraw`, `ants spend`, `ants inbox`), and ants' messages on threads no channel post started carry no owner, so their escalations go to the daemon's default human; renew the leaf uplinks of machines where such clients run too, and run `ants leaf outbox` again. Owner records keyed by thread alone, from before the recipient key, are not read.

Renew all credentials from before permissions version 4, of every kind (ants, humans, webhooks, observers, the channel manager and leaf uplinks), before the hub or any leaf node moves to nats-server 2.16 or sets `feature_flags { js_ack_fc_v2: true }`: they allow only the v1 ack and flow control subjects (`$JS.ACK.<stream>.<consumer>.…`, `$JS.FC.<stream>.…`), so on such a server every ack they send is denied, messages are delivered again until they become dead letters, and KV watches (the roster, channels, humans) stall. `ants renew FILE`, or the `ants auth` commands above, renew them; `ants auth issued` lists those still outdated.

### Renewing over NATS

Credentials of permissions version 2 or later (ants, humans, webhooks, observers, the channel manager and leaf uplinks) renew over NATS while still valid, without copying files from the machine holding the team keys:

```sh
ants renew ~/.ants/dev-a.creds                          # where dev-a runs: waits for the approval
ants enroll renewals --js-domain hub                    # auth machine: list the requests heard
ants enroll renew dev-a --js-domain hub                 # sign and answer it
ants renew ~/.config/ants/auth/leaf-macbook.creds --creds ~/.config/ants/auth/alice.creds  # a leaf uplink
```

`ants renew` generates a new user NKey, whose seed never leaves the machine, and asks on `ants.renew.<name>` every 5 s, with the JWT of the credentials renewed and a signature of the request by their key, until an answer for the new key arrives, the credentials expire or Ctrl-C. Uplink credentials connect only as a leaf node, so a human's credentials (`--creds`) carry the request of `leaf-<machine>`. `ants enroll renew NAME` checks that the request proves credentials of the team's account named `NAME` by that signature, neither expired nor revoked, and signs a user JWT for the new key with the same name, kind, device, JetStream domain and members (a webhook's destinations, a leaf node's ants, humans, webhooks and enrollment approver), the permissions this ants gives that kind (so an upgrade's rights apply), and as long a lifetime as the earlier credentials had. Enrollment and leaf system credentials, expired, revoked or older ones, and requests whose name, kind or signature do not match are refused: renew those with `ants auth` as above. `issued.jsonl` records the new key as renewed from the earlier one (`renewed_from`), with the human's device and label, or the leaf's members, so a later `ants auth leaf MACHINE` keeps them; the earlier key stays valid until it expires, and `ants enroll renew` prints the `ants auth revoke --key` retiring it.

The holder checks that the answer comes from the same account for the new key with the same name and kind, then replaces the file at once (written beside it, then renamed over it; a seed in the OS credential store stays there, see [Private keys in the OS credential store](#private-keys-in-the-os-credential-store)) and reloads what uses it: the daemon service of `--config` (default `~/.ants/config.toml`) when one of its ants uses the file, which restarts those ants, or the leaf node service of `--leaf-config` (default `~/.config/ants/leaf.conf`) when the file is its uplink's (see [Services](#services)); otherwise it names what to restart. A daemon reload also restarts the ants and schedules whose credentials file changed. Requests and answers are core NATS messages, carried through leaf nodes whose uplink is of version 2 or later; an answer holds only a JWT, useless without the seed. Warnings and `ants doctor` name `ants renew FILE` and `ants enroll renew NAME` for credentials that may renew over NATS.

`ants enroll renew --auto` approves every request as it arrives, without an approval each, for as long as it runs on the machine holding the account key; it is opt-in, since the signing key must be online for it. It reconnects every 5 minutes with fresh credentials it issues itself, rereads the revocations then, and answers a repeated request with the same JWT.

### Private keys in the OS credential store

A `.creds` file holds a user JWT, which is public, and the NKey seed signing the server's challenge, which is not. `ants keys store` moves the seed into the OS credential store: the macOS keychain (login keychain, service `ants`), on Linux the Secret Service (GNOME Keyring, KDE Wallet, KeePassXC) through `secret-tool` (package `libsecret-tools` or `libsecret`), or the Windows Credential Manager (a generic Windows credential `ants:<public key>`, kept for the user on this machine only). On other platforms seeds stay in the file.

```sh
ants keys store ~/.config/ants/auth/alice.creds ~/.ants/dev-a.creds   # default: --creds
ants keys unstore ~/.ants/dev-a.creds                                  # back into the file
ants keys remove ~/.ants/dev-a.creds                                   # delete the file and its stored seed
```

Each seed is stored under its public key, so every device and every ant has an entry of its own and stays revocable on its own. `ants keys store` stores the seed, reads it back, and authenticates at `--nats` with the file as it is to be before replacing the file at once; the file then keeps the JWT and, in place of the seed, the public key it is stored under. Any failure (store locked, write refused, server refusing the key) leaves the file as it was and deletes what was stored. Every ants process using the file (CLI, daemon, `ants webhook serve`, `ants channel serve`, `ants renew`) reads the seed from the store when it connects and keeps it in memory to sign the challenge again on each reconnect. `ants renew` keeps the new seed where the earlier one was: stored, read back and proven by authenticating before the file is replaced, the earlier seed deleted only then. A file naming a stored key never falls back to a seed elsewhere: when the store is locked, unreachable or lacks the entry, connecting fails with what to do, and no message carries a seed. Other NATS tools (`nats` CLI) cannot read such a file; `ants keys unstore` makes it plain again.

- **Headless machines:** a launchd agent reads the login keychain once the user has logged in; a locked keychain fails the connection (error -25308). The daemon, `ants webhook serve`, `ants channel serve` and `ants notify` wait at start for a store that is locked or not available yet, as a service started before its user logs in, retrying with backoff (1 s, doubling up to 30 s) and logging what they wait for; a store lacking the entry, or holding another key's seed, stops them at once. macOS asks again for access after `ants upgrade` replaces the binary: answer **Always Allow** in a session, then restart the service. Where no store is reachable or unlocked (below), keep the seed in the file, readable by its owner only (`ants keys unstore`, or never store it): the file-backed form is the explicit compatibility mode, and nothing moves a seed between the two on its own. `ants doctor` checks every stored key the CLI and the config file's ants use.
- **Linux desktops:** a desktop login unlocks the login keyring. A locked keyring asks for its password in a desktop prompt; `secret-tool` gets 60 s for an answer, then the attempt fails as a locked store does. A locked keyring never reads as a missing entry: reading fails (services wait, as above), and `ants keys unstore` and `ants keys remove` fail and leave the file as it was. A collection listing no entries while locked, as a locked KeePassXC database, counts as holding the entry; ants reads the collections over D-Bus with `busctl`, `gdbus` or `dbus-send`, whichever is installed, and without any of them such an entry reads as missing. Errors name what is missing: `secret-tool`, a D-Bus session bus, a Secret Service, a default keyring, or an unlocked one.
- **KDE Wallet:** `kwalletd6` (`kwalletd5` from KDE Frameworks 5.97) provides the Secret Service while it runs: a Plasma session starts it, and elsewhere D-Bus may not start it on demand, so start it before ants. It needs the Qt cryptography plugins (package `libqca-qt6-plugins` on Debian), without which every call fails with `createDLGroup failed`. The first store asks to create a wallet and its password. A closed wallet asks for its password on each read; a delete fails as locked, without asking, until the wallet is opened (for instance in KWalletManager).
- **KeePassXC:** KeePassXC must be running with **Secret Service Integration** on (in its settings) and a group exposed to it in each database's settings; entries go into that group. By default KeePassXC asks to confirm each read by a new process, and each ants or `secret-tool` run is one: turn off **Confirm when passwords are retrieved by clients** for unattended reads, or `ants keys unstore` and `ants keys remove` cannot delete the entry (reported as locked). A locked database lists no entries: with **Prompt to unlock database before searching** on (the default), a read asks to unlock one when none is unlocked, and a read or delete in a locked database fails as locked (above). A deleted entry moves to the database's recycle bin, which keeps the seed until emptied; without a recycle bin, KeePassXC asks to confirm each delete unless **Confirm when clients request entry deletion** is off.
- **Headless Linux and systemd:** a systemd user service (and an SSH session) reaches the user's session bus at `$XDG_RUNTIME_DIR/bus` (package `dbus-user-session` on Debian), where D-Bus starts GNOME Keyring on demand; a system service, even with `User=`, a cron job or `sudo` has no session bus and fails. Without a desktop no prompt can create or unlock a keyring: create or unlock the login keyring as the service's user with `printf '%s' "$PASSWORD" | gnome-keyring-daemon --replace --unlock --components=secrets` (the password on standard input; a running service waits for it), for instance over SSH after each boot. Unlocking it unattended needs the password stored on the machine, which protects the seed no better than the file: keep the seed in the file there instead.
- **WSL:** a WSL 2 distribution often has neither a D-Bus session bus nor a Secret Service: the bus needs systemd (`[boot] systemd=true` in `/etc/wsl.conf`, the default of recent Ubuntu images), and the keyring is then installed and unlocked as on headless Linux. ants in WSL cannot reach the Windows Credential Manager of the host. Otherwise keep the seed in the file there.
- **Windows:** credentials are encrypted for the user's logon. A logon without a credential set, as an SSH session authenticated by key, a service running as another account or a scheduled task not storing the password, cannot reach them (error 1312): keep the seed in the file there.
- **Backup and recovery:** a stored seed is backed up with the store (an encrypted keychain or keyring backup), or exported with `ants keys unstore` and stored again later. A lost seed cannot be recovered: issue the credentials again (`ants auth user`, `ants enroll`) and revoke the old key.
- **Removal and revocation:** `ants keys remove` deletes a machine's copy only; the credentials stay valid until they expire. Revoke them with `ants auth revoke --key KEY` on the machine holding the team keys.
- **New credentials:** `ants add --store-key` (with `--jwt` or `--enroll`), `ants auth user --store-key` and `ants auth channel-manager --store-key` put the new seed straight into the store, so it never touches a disk: the seed is stored and read back, then the file naming it is written once it authenticates at the server (`--hub` or `--nats`); when no server answers there, the file is written with a warning; a server refusing it, or any other failure, leaves no file and deletes what was stored. `ants add --store-key` stores the seed when it generates the key: the pending `<ant>.nk` then holds only the public key, and a seed already pending in it moves into the store. Without `--store-key`, the seed goes into the file as before, since a service may not reach the store (above); store it afterwards with `ants keys store`. A failed `ants add --enroll --store-key` prints the approved JWT, to complete the credentials later with `--jwt`.
- **Not covered:** leaf uplink credentials (`leaf-<machine>.creds`, `.system.creds`), which nats-server reads (`ants keys store` refuses them); the team keys `operator.nk`, `account.nk` and `system.nk`; the pending `<ant>.nk` of `ants add` without `--store-key`; credentials `ants auth` writes for another machine, such as those of `ants auth enroll` (move them there, then store them); and the short-lived system account and approver credentials `ants auth revoke`, `push` and `ants enroll` build in memory.

### Wire ACLs and verification invariants

- **Subject-pinned senders:** ants and webhooks publish only under their own sender token (`ants.inbox.*.<self>`, `ants.notes.*.<self>`, `ants.chan.*.<self>`, `ants.dead.<self>`, `ants.chanadm.*.<self>`, plus `ants.out.*` on leaves). Readers verify payload `from`/target fields against the NATS subject (`ants watch` marks mismatches forged). Only humans publish under arbitrary inbox senders (for `ants dead replay`), write `ants.approvals.<ant>.<id>`, publish cancel requests (`ants.cancel.<ant>.<thread>`, heard only by `<ant>`, which answers only on `ants.cancelled.<ant>.*`) and queue requests (`ants.queue.<ant>`, heard only by `<ant>`, which answers only on `ants.queued.<ant>`), write `ants_humans` and `ants_channels` (alongside `channel-manager`), or call `STREAM.MSG.GET` and batched direct gets of `ANTS` (`$JS.API.DIRECT.GET.ANTS`), which take any sequence. Each human may create and read its own notification consumer `notify-<human>` only.
- **Scoped ant reads:** without `STREAM.MSG.GET`, ant credentials pin consumer filters (`inbox-<ant>[_1..3]`, `chan-<ant>`, `chanread-<ant>[_1..3]`, `notes-<ant>[_1..3]`, `deadread-<ant>`, `sentread-<ant>` filtered to `ants.inbox.*.<ant>`) and restrict direct gets to `ants.approvals.<ant>.*`, their own `$KV.ants_sessions.<ant>.>` and `$KV.ants_attempts.<ant>.>`, `$KV.ants_humans.*`, `$KV.ants_chains.*`, `$KV.ants_owners.>` and `$KV.ants_webhooks.*.webhook`; humans also direct-get `$KV.ants_webhooks.channel-manager.serve`, the one key of `ants_webhooks` besides its own events the channel manager writes. Users write only their own keys in `ants_roster`, `ants_chains`, `ants_links`, `ants_webhooks` and `ants_reactions`, and humans only their own `marks.<human>` key in `ants_threads` (every human writes its `title.*` keys). Only humans and webhooks with an ant destination write `ants_owners`, webhooks only `<thread>.<ant>` keys of those ants (humans also through a leaf outbox), and its stream refuses a second record of a key (`discard_new_per_subject`, deletes and purges denied), but drops one repeating the first's `Nats-Msg-Id` (`owner.<key>`, which every writer sets) within the 30 d retention, so a record sourced from an outbox never stalls the source. `ants init` and `ants leaf outbox` widen the duplicate window of a stream from before.
- **Private keys:** stream info with a subjects filter lists every key of a bucket, so only human credentials get stream info of `ants_sessions`, `ants_attempts` and `ants_owners` (to create them, and for `ants spend` to find spend keys). Of `ants_attempts`, humans write (by a publish expecting no value) and direct-get only the marks of messages (`$KV.ants_attempts.*.msg.*`), not ants' attempt markers or the replies they saved. Humans read and write only their own read marker (`$KV.ants_read.<human>`, by direct get), their own devices' presence (`$KV.ants_presence.<human>.>`, by direct get, without stream info of `ants_presence`) and their own snooze and muted threads (`$KV.ants_notify.<human>`, by direct get; stream info of `ants_notify` names humans, not threads), and may delete only their own legacy `inbox-<human>` consumer. Humans direct-get only the spend keys of `ants_sessions` (`$KV.ants_sessions.*.*.spend` and `$KV.ants_sessions.*.day.*`), not ants' sessions. Daemons read their own keys there, every ant's daily spend (`$KV.ants_sessions.*.day.*`, for pool caps) and owners by direct get and write their own keys by plain publish, and webhooks record owners by plain publish, so neither ants nor webhooks can list other ants' thread ids. Likewise stream info of `ANTS` lists every subject stored (who messages whom, approvals, dead letters) and that of `ants_webhooks` every webhook's events, so only human credentials get them: ants, webhooks and the channel manager find JetStream through the info of their own inbox consumer, ants open `ANTS` without its info and read back from the last message through their own reader, and a webhook records each event it delivered by a publish expecting no earlier record of it and reads it back by direct get.
- **Read-only observers:** observer credentials publish nothing but JetStream requests on their own consumers `observe-requests-<observer>`, `observe-decisions-<observer>` and `observe-dead-<observer>` (filters `ants.approvals.*`, `ants.approvals.*.*` and `ants.dead.>` pinned in the create subject), reads and watches of `ants_roster`, and their own renewal request; they subscribe to those subjects, `ants.status.*.*` and their own reply inboxes only (see [docs/hooks.md](docs/hooks.md)).
- **Metadata trust:** daemons accept `owner` only for `human` or entries in `ants_humans`, and honour `webhook: true` only with `<sender>.webhook` in `ants_webhooks` (or from a human). Channel thread `owner` and `creator` come from stored root/human posts in `ANTS`; an ant's non-channel `owner` comes from `ants_owners` for its thread or its `origin_thread`, else is dropped: a channel `origin_thread` gives the owner its posts say, under the same participation check as `creator`; a message thread, its own or its `origin_thread`, gives the owner of its `<thread>.<sender>` record, else of the record of an ant `ants_links` traces the sender's link back to; non-channel `creator` requires `origin_thread` backed by participation or `<from>.<thread>.<to>` links in `ants_links` (max 32 hops).

### Adding an ant

`ants add` generates the ant's user NKey on the target machine (keeping `<ant>.nk` at mode `0600` until signed, never copying the seed elsewhere; with `--store-key`, the seed goes into the OS credential store at once and `<ant>.nk` holds only its public key, see [Private keys in the OS credential store](#private-keys-in-the-os-credential-store)) and appends `[[ant]]` to `config.toml`:

```sh
# Out of band
ants add dev-c --workdir ~/src/app              # target machine: appends config, prints public key U...
ants auth user dev-c --public-key U... --js-domain hub   # auth machine: prints signed JWT
ants add dev-c --jwt eyJ...                     # target machine: writes dev-c.creds (accepts JWT, file or '-')

# Over NATS (direct, or through leaf nodes)
ants auth enroll dev-c                          # auth machine: writes enroll-dev-c.creds, valid 1h (--ttl; 0: no expiry)
ants add dev-c --workdir ~/src/app --enroll enroll-dev-c.creds   # target machine: waits for approval
ants enroll list --js-domain hub                # auth machine: inspect pending requests
ants enroll approve dev-c --js-domain hub
```

Requests prove the enrollment credentials by signature; `--hub URL` sends them to another server than `--nats`. Behind a leaf node, `ants add --enroll` names the machine's leaf (from `~/.config/ants/leaf.conf` or `--leaf-config FILE`) with a signature by its uplink key, and approval renews the leaf's uplink credentials for the ant and admits it to the outbox sourcing set by `ants leaf outbox`; `ants add` then restarts the leaf node service of that `leaf.conf` when it runs (`SIGHUP` does not reread credentials; otherwise it prints the restart command, see [Services](#services)). Later `ants auth leaf` and `ants leaf outbox` runs keep the ant: approval records it with the leaf's members in the team directory, and the renewed uplink credentials carry it. `ants enroll` behind a leaf node needs uplink credentials issued with `ants auth leaf --enroll-approver`. Leaf uplink credentials issued before enrollment crossed leaves need `ants auth leaf` once more ([Credentials across upgrades](#credentials-across-upgrades)). Uplink credentials grow by about 8 KB per ant with a JetStream domain (4 KB without); `ants auth leaf`, approval and `ants signer` refuse, before writing or answering anything, uplink JWTs over 252 KiB, the most a hub takes in a leaf's `CONNECT` (16 × `max_control_line`), about 30 ants per leaf with a domain (60 without): run more behind another leaf node under another machine name. See [docs/transport.md](docs/transport.md#enrollment-behind-a-leaf-node).

Once the credentials are complete, `ants add` reloads the daemon service serving the config file (see [Services](#services)) when it runs, and otherwise prints the command to send `ants daemon` `SIGHUP` by hand; after `--jwt` behind a leaf node, also run `ants auth leaf MACHINE --add-ant NAME` and, with the new uplink credentials, `ants leaf outbox MACHINE`.

## Leaf nodes

Running a local `nats-server` leaf per machine isolates local `ants` processes on `127.0.0.1:4222` and buffers outbound traffic while offline. Configure `jetstream { domain: hub }` and `feature_flags { js_api_reply_v2: true }` on the hub, pass `--js-domain hub` (`ANTS_JS_DOMAIN=hub`, or `js_domain = "hub"` in `~/.ants/config.toml` for the CLI and the ants it lists) everywhere, and issue leaf credentials scoping all ants, humans and webhooks on that machine:

```sh
# Auth machine (issue user/webhook .creds first)
ants auth leaf macbook --ant dev-a --ant messenger --human alice --webhook github --js-domain hub

# Worker machine ("macbook", with server.conf, leaf-macbook.creds and leaf-macbook.system.creds in ~/.config/ants/auth)
ants leaf config macbook --uplink wss://hub.example/nats       # writes ~/.config/ants/leaf.conf
nats-server -c ~/.config/ants/leaf.conf &
ants leaf outbox macbook --ant dev-a --ant messenger --human alice

# Later: the auth machine remembers the members, the worker reads them from its uplink credentials
ants auth leaf macbook --add-ant dev-b --js-domain hub
ants leaf outbox macbook                                         # after copying the new leaf-macbook.creds and restarting the leaf
```

- Without `--ant`, `--human` or `--webhook`, `ants auth leaf` reissues the uplink for the members of its last run on the machine, which `issued.jsonl` in the team directory keeps with the ants enrolled through the leaf since (for uplinks issued before, the members their `.creds` file carries); `--add-ant`, `--add-human`, `--add-webhook` and `--remove-*` change that set, and `--enroll-approver` stays until `--no-enroll-approver`. Named lists replace the set. Without members to keep or name, it fails.
- Without `--ant` or `--human`, `ants leaf outbox` admits the ants and humans the uplink credentials of the machine's leaf node carry (`--leaf-config FILE`, default `~/.config/ants/leaf.conf`), changed by `--add-ant`, `--add-human`, `--remove-ant` and `--remove-human`.

`ants leaf config` accepts `nats-leaf://` (or `nats://`), `tls://`, `ws://` and `wss://` uplinks, `--ca-file FILE` and `--proxy http://host:port`; re-run it to switch uplinks live: it reloads the leaf node service running that `leaf.conf` (see [Services](#services)), and otherwise prints the `SIGHUP` to send `nats-server` by hand. A reload adds and drops uplinks and changes their TLS settings; any other change (`--listen`, the machine name, `--server-conf`, the `--proxy` of an unchanged uplink) takes a restart, which it does instead. A second uplink joins the hub's system account with `leaf-<machine>.system.creds` (`--system-creds`), so revocations pushed to the hub reach the leaf, those pushed while it was offline within 10 s of reconnecting to a hub with a `full` resolver; a leaf configured before it needs `ants auth leaf`, `ants leaf config` and a restart. While offline, outbound inbox messages, replies, notes, channel posts, dead letters, `ants_links` entries and the `ants_owners` records of `ants ask` queue in local stream `ANTS_OUT` and drain via hub stream `ANTS_IN` into `ANTS`, `ants_links` and `ants_owners` on reconnect (`ants leaf outbox --remove` removes sourcing); a daemon holds a human's message from an outbox up to 10 s until its thread's owner lands. `ANTS` republishes stored inbox messages to `_INBOX.<to>.stored.<from>`; `ants ask --wait`/`--follow` polls stored replies every 5s (humans by message gets, ants via spare consumers `inbox-<ant>_1..3`, which ant credentials and leaf uplinks issued before them lack: see [Credentials across upgrades](#credentials-across-upgrades)), and `ants post --wait`/`--follow` (human or ant) polls the channel's stored posts after its own every 5s, so replies stored during an outage or sourced from an outbox show once. Full hub, proxy, leaf, outbox, Dev Tunnel and keepalive details live in [docs/transport.md](docs/transport.md) (`Hub`, `Reverse proxy`, `Leaf nodes`, `Store and forward`, `HTTPS fallback bridge`, `Dev Tunnels`, `Keepalive and limits`).

### Transport options

| Leaf uplink | Use case |
|---|---|
| `wss://host/path` | Standard TLS WebSocket uplink behind a reverse proxy (supports `--proxy` HTTP `CONNECT`). |
| `tls://host:7422` | Direct TLS connection to the hub leafnode port. |
| `nats-leaf://127.0.0.1:7422` via `devtunnel connect` | Private Dev Tunnel forwarding the hub leafnode port (`--devtunnel` in `install-units.sh` on workers, `--devtunnel-host` on the hub). |
| `nats-leaf://127.0.0.1:7422` via `ants bridge connect` | Networks whose proxies block WebSocket; tunnels leaf traffic over plain HTTPS `POST`/long-polled `GET` (`--bridge URL` in `install-units.sh` on workers, `--bridge-serve` on the hub). |

For the HTTPS fallback bridge, run `ants bridge serve` (`127.0.0.1:8423` -> `127.0.0.1:7422`, optional `ANTS_BRIDGE_TOKEN` or `--token-file FILE`) behind the hub reverse proxy, and `ants bridge connect https://hub.example/bridge [--proxy URL] [--header 'Name: val']` (`127.0.0.1:7422`) on the worker. Either end takes `--metrics ADDR` (see [Monitoring](#monitoring)). Run both as services with `scripts/install-units.sh --bridge-serve` on the hub and `--bridge URL` on the worker (see [Services](#services)). See [docs/transport.md](docs/transport.md#https-fallback-bridge).

## Services

Windows has no units here: see [Windows](#windows). `ants setup` installs the nats-server and daemon units of a single machine (see [Setup](#setup)). For the others, and leaf nodes, `scripts/install-units.sh` renders templates from `contrib/launchd/` (macOS `~/Library/LaunchAgents`) or `contrib/systemd/` (Linux/WSL `~/.config/systemd/user`) with resolved binary paths and current `PATH`, then enables and starts them:

```sh
scripts/install-units.sh                                           # nats + daemon
scripts/install-units.sh --devtunnel ants-hub.usw2                 # + devtunnel connect (--devtunnel-token FILE)
scripts/install-units.sh --no-daemon --channel-manager ~/.config/ants/auth/channel-manager.creds --webhook
scripts/install-units.sh --no-daemon --devtunnel-host ants-hub    # hub: + devtunnel host (needs devtunnel user login)
scripts/install-units.sh --bridge https://hub.example/bridge --bridge-token ~/.config/ants/bridge.token   # + ants bridge connect
scripts/install-units.sh --no-daemon --bridge-serve --bridge-token ~/.config/ants/bridge.token           # hub: + ants bridge serve
scripts/install-units.sh --no-daemon --signer                     # account-key machine: + ants signer (--config's templates)
scripts/install-units.sh --out ./rendered-units                    # render only; install nothing
```

Options: `--ants PATH`, `--nats-server PATH`, `--nats-conf FILE` (default `~/.config/ants/leaf.conf`), `--config FILE` (default `~/.ants/config.toml`), `--no-daemon`, `--channel-manager CREDS`, `--signer`, `--webhook`, `--webhook-config FILE` (default `~/.ants/webhooks.toml`), `--devtunnel TUNNEL`, `--devtunnel-host TUNNEL`, `--devtunnel-bin PATH`, `--devtunnel-token FILE`, `--bridge URL`, `--bridge-serve`, `--bridge-token FILE` (either end), `--bridge-proxy URL`, `--bridge-ca FILE` (`SSL_CERT_FILE` for `--bridge`), `--out DIR`. `--devtunnel` and `--bridge` exclude each other: both listen on `127.0.0.1:7422`. Unless `--out`, it also runs `ants completions --install` where it installs an ants service.

| Service | macOS (launchd / `~/Library/Logs/ants/`) | Linux & WSL (`systemd --user`) | Installed when |
|---|---|---|---|
| NATS (`nats-server`) | `ants.nats` (`nats.log`) | `ants-nats.service` | Always |
| Ant daemon (`ants daemon`) | `ants.daemon` (`daemon.log`) | `ants-daemon.service` | Default (omit with `--no-daemon`) |
| Channel manager (`ants channel serve`) | `ants.channels` (`channels.log`) | `ants-channels.service` | `--channel-manager CREDS` |
| Webhook ingress (`ants webhook serve`) | `ants.webhook` (`webhook.log`) | `ants-webhook.service` | `--webhook` |
| Dev Tunnel (`devtunnel connect`) | `ants.devtunnel` (`devtunnel.log`) | `ants-devtunnel.service` | `--devtunnel TUNNEL` |
| Dev Tunnel host (`devtunnel host`, hub) | `ants.devtunnel-host` (`devtunnel-host.log`) | `ants-devtunnel-host.service` | `--devtunnel-host TUNNEL` |
| HTTPS bridge (`ants bridge connect`) | `ants.bridge` (`bridge.log`) | `ants-bridge.service` | `--bridge URL` |
| Signer (`ants signer`, account-key machine) | `ants.signer` (`signer.log`) | `ants-signer.service` | `--signer` |
| HTTPS bridge server (`ants bridge serve`, hub) | `ants.bridge-serve` (`bridge-serve.log`) | `ants-bridge-serve.service` | `--bridge-serve` |
| Log rotation (macOS) | `ants.logrotate` (`logrotate.log`) | — (the journal rotates) | Always on macOS |

- **Reload leaf uplink:** `launchctl kill SIGHUP gui/$(id -u)/ants.nats` (macOS) or `systemctl --user reload ants-nats` (Linux); `ants leaf config` does it itself.
- **Reload daemon config:** the daemon reloads an edited config file by itself within seconds; at once: `launchctl kill SIGHUP gui/$(id -u)/ants.daemon` (macOS) or `systemctl --user reload ants-daemon` (Linux).
- **Reload webhook config:** the webhook ingress reloads an edited config file by itself within seconds, and `ants webhook add` reloads its service at once; by hand: `launchctl kill SIGHUP gui/$(id -u)/ants.webhook` (macOS) or `systemctl --user reload ants-webhook` (Linux; units installed before need `scripts/install-units.sh --webhook` again for `ExecReload`).
- **Managed by `ants add`:** completed credentials reload `ants.daemon` / `ants-daemon` when its unit (`~/Library/LaunchAgents/ants.daemon.plist`, `~/.config/systemd/user/ants-daemon.service`) runs the config file of `ants add` and the service is running; an enrollment that renewed the leaf's uplink credentials then restarts `ants.nats` / `ants-nats` (`launchctl kickstart -k`, `systemctl --user restart`) when its unit runs that `leaf.conf`. `ants renew` reloads the daemon service in the same way when one of its ants uses the renewed file, and restarts `ants.nats` / `ants-nats` when the file is the uplink of the `leaf.conf` its unit runs. `ants leaf config` reloads `ants.nats` / `ants-nats` (`launchctl kill SIGHUP`, `systemctl --user reload`) when its unit runs the `leaf.conf` written and the service is running, or restarts it for changes a reload does not take. A manager ant's daemon reloads it in the same way after `request_ant` renewed the uplink credentials of that `leaf.conf`. A unit of another file, a stopped service or another system gets the commands printed instead.
- **Start order:** systemd starts `ants-nats` after `ants-devtunnel` or `ants-bridge`, and `ants-devtunnel-host`, `ants-bridge-serve`, the daemon, channel manager, signer and webhook ingress after `ants-nats`. launchd has no start order: the leaf node retries its uplink until the tunnel or bridge listens, and the other services retry their connections.
- **Restart daemon:** `launchctl kickstart -k gui/$(id -u)/ants.daemon` (macOS) or `systemctl --user restart ants-daemon` (Linux; 120s graceful `SIGTERM` stop).
- **Upgrade:** the ants services restart onto a new `ants` executable by themselves, exiting with status 75 ([Upgrade](#upgrade)); run `scripts/install-units.sh` again so systemd units installed before treat that exit as a restart rather than a failure.
- **Logs (macOS):** launchd never rotates `~/Library/Logs/ants/*.log`, so `ants.logrotate` checks them at load and hourly: a log over 10 MiB is copied to `<name>.log.1`, gzipped, and truncated in place (copy-truncate, as launchd keeps the file open in append mode; no restart, signal or root needed), keeping 5 copies (`<name>.log.5.gz`). Lines written between the copy and the truncation are lost. Rotate now: `launchctl kickstart gui/$(id -u)/ants.logrotate`; the cap and count are the last two `ProgramArguments` of its plist.
- **Remove units:** `launchctl bootout gui/$(id -u)/<label>` or `systemctl --user disable --now <unit>`, then delete the unit files.
- **Persistence:** Linux user units require `loginctl enable-linger $USER` to run without an active login session; WSL requires `[boot] systemd=true` in `/etc/wsl.conf`.

## Monitoring

`ants daemon`, `ants webhook serve`, `ants bridge serve` and `ants bridge connect` serve health and metrics over plain HTTP when given an address: `--metrics ADDR` (`ANTS_METRICS` for the daemon), or `metrics` in the daemon's or the webhooks' config file. It is off by default. The address is meant to be loopback: any other address is served too, with a warning, as the endpoints have no authentication. A daemon reads the address at start; a reload (`SIGHUP`) does not move it.

- `GET /healthz`: `200 ok` when healthy, else `503`. A daemon is healthy while it serves at least one ant and every ant it serves is connected to NATS; the webhook ingress while every webhook and human is connected; a bridge while it runs.
- `GET /metrics`: the Prometheus text format (`text/plain; version=0.0.4`). Other paths answer `404`, other methods `405`.

| Metric | Type | Labels | From |
|---|---|---|---|
| `ants_nats_connected` | gauge | `ant` (daemon), `user` (webhook) | Whether the NATS connection is up (1) or not (0) |
| `ants_harnesses` | gauge | `ant` | Harness processes alive |
| `ants_messages_in_hand` | gauge | `ant` | Messages being handled: prompting, or waiting for their thread or a harness |
| `ants_prompts_in_flight` | gauge | `ant` | Prompts running in a harness |
| `ants_prompts_total` | counter | `ant` | Prompts started |
| `ants_retries_total` | counter | `ant` | Messages handed back for redelivery after a failure |
| `ants_dead_letters_total` | counter | `ant` | Messages buried as dead letters |
| `ants_spend_tokens_total`, `ants_spend_cost_total` | counter | `ant` | Tokens, and cost in the harness's currency (usually USD), the harnesses reported (see [Spend report](#spend-report)) |
| `ants_jetstream_bytes`, `ants_jetstream_limit_bytes` | gauge | `domain`, `store` (`file`, `memory`) | JetStream storage used by the account, and the store's limit when known (see [JetStream storage](#jetstream-storage)); `domain` empty without one |
| `ants_webhook_requests_in_flight` | gauge | | Webhook requests being answered |
| `ants_webhook_events_total` | counter | `webhook`, `outcome` | Requests to a webhook: `delivered`, `duplicate`, `refused` (4xx) or `failed` (503) |
| `ants_bridge_sessions`, `ants_bridge_sessions_total` | gauge, counter | | Bridge sessions open, and opened |
| `ants_bridge_failures_total` | counter | | Upstream unreachable (`serve`), or a session not opened or broken (`connect`) |
| `ants_bridge_bytes_total` | counter | `direction` (`up`, `down`) | Bytes carried towards the hub's upstream, and back |

A daemon's counters start at zero when it starts; an ant's series appear while it is served.

```sh
ants daemon --metrics 127.0.0.1:9464 &
curl -s http://127.0.0.1:9464/healthz
curl -s http://127.0.0.1:9464/metrics
```

## Hooks

Programs outside ants act on its events with read-only observer credentials (`ants auth user NAME --observer`): they read every tool request, verdict and dead letter through consumers of their own, and hear every ant's status (escalations included) and roster entry, but send nothing, answer no request, write no bucket and read no inbox, notes or channel. `ants hook run --on EVENT -- CMD [ARGS...]` runs a command once per event with its JSON document on stdin (`--concurrency`, default 4; `--timeout`, default `60s`); stored events resume where the hook's consumer left off. [docs/hooks.md](docs/hooks.md) documents the subjects and documents a hook may rely on, the observer's rights, and what may change between versions.

```sh
ants auth user audit --observer --js-domain hub
ants hook run --creds audit.creds --on approval-request -- ./page-on-call.sh
```

## Footprint

`scripts/footprint.sh` starts a temporary `nats-server -js` and `ants daemon`, samples resident memory with `ps`, and fails at or above `--max-mb` (default `20` MB). Pass `--ask "reply with ok"` to measure with one live harness session, or `--metrics 127.0.0.1:9464` to measure with health and metrics served and scraped before each sample (about 0.3 MB more on macOS arm64).

```sh
scripts/footprint.sh --port 14722
scripts/footprint.sh --port 14722 --ask "reply with ok"
scripts/footprint.sh --port 14722 --metrics 127.0.0.1:9464
```

Measured with `ants` 0.1.0 (release) and `nats-server` 2.15.3:

| Scenario | Linux x86_64 | macOS arm64 |
|---|---|---|
| Idle daemon | 5.9 MB | 8.5 MB |
| Idle daemon with MCP server | not measured | 10.5 MB |
| Daemon with one Copilot session | not measured | 10.5 MB |
| Copilot CLI harness processes | not measured | 255 MB |

## Development

```sh
cargo build --release
cargo fmt --all --check
CARGO_BUILD_WARNINGS=deny cargo clippy --all-targets
cargo test
cargo deny check        # licenses, advisories, bans, sources (deny.toml)
```

CI sets `CARGO_BUILD_WARNINGS=deny`, so any lint warning (rustc or clippy) fails `cargo clippy`, `cargo test` and `cargo build` there. `.cargo/config.toml` sets a 7-day dependency cooldown (`registry.global-min-publish-age`): `cargo update` and `cargo add` resolve only to crate versions published at least 7 days ago, unless `Cargo.lock` already holds them; builds and `cargo install --path` from the committed `Cargo.lock` are unaffected. `CARGO_RESOLVER_INCOMPATIBLE_PUBLISH_AGE=allow cargo update -p CRATE --precise VERSION` takes a newer release, such as a security fix.

CI also lints and builds the Windows target on `windows-latest` and runs the MCP named pipe test there. To lint it from macOS or Linux: `rustup target add x86_64-pc-windows-gnu`, then `CARGO_BUILD_WARNINGS=deny cargo clippy --all-targets --target x86_64-pc-windows-gnu` with mingw-w64 on `PATH`.

Integration tests requiring `nats-server` on `PATH` skip when absent unless `ANTS_REQUIRE_TOOLS=1` is set (as in `.github/workflows/ci.yml`, which runs the suite a second time with `ANTS_TEST_JS_ACK_FC_V2=1`). `crates/ants/tests/e2e.rs` exercises `ants ask --wait` against `ants daemon` using the test binary as a stub ACP harness.

## Layout

| Path | Purpose |
|---|---|
| `crates/ants-proto` | Wire contract: IDs, subjects, envelopes, turn budgets, NATS permissions (no I/O) |
| `crates/ants-daemon` | NATS/JetStream bus, ACP harness pool, delivery loop, approvals, escalations, MCP server |
| `crates/ants` | `ants` binary: CLI, `daemon` and its schedules, `auth`, `enroll`, `leaf`, `bridge`, `channel`, `webhook`, `hook`, `mcp` |
| `crates/ants-test-support` | Dev-only test helpers: `PATH` checks and throwaway `nats-server` |
| `contrib/` | `launchd` plists, `systemd` user units and CLI agent skill (`contrib/skills/ants/SKILL.md`) |
| `scripts/` | `install-units.sh` (service installer) and `footprint.sh` (RSS budget check) |
| `docs/transport.md` | Transport reference: hub, reverse proxy, leaf nodes, store-and-forward, HTTPS bridge, Dev Tunnels |
| `docs/hooks.md` | Hook contract: observer credentials, the subjects and JSON documents hooks rely on, versioning |

Work items and specifications live in `bd` (`.bd/`): `bd ready`, `bd spec list`, `bd spec show <area>`. The database stays local; `.bd/issues.jsonl` is a committed snapshot (`bd export -o .bd/issues.jsonl`), loaded into a fresh workspace with `bd init --prefix ants` and `bd import .bd/issues.jsonl`.

## Security

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
