# ants

[![CI](https://github.com/quanghle/ants/actions/workflows/ci.yml/badge.svg)](https://github.com/quanghle/ants/actions/workflows/ci.yml)

Headless agent teams across machines, communicating with each other and with humans at the terminal over NATS.

- **Backbone:** NATS + JetStream stream `ANTS` for durable inboxes, channels, thread notes, approvals, dead letters and admin orders, plus 16 NATS KV buckets for roster, sessions, attempts, channels, humans, chains, links, owners, read markers, presence, notifications, reactions, thread metadata, webhooks, human devices' encryption keys and the ants each machine serves.
- **Transport:** `wss://`/`ws://` behind a reverse proxy, `tls://`, a Dev Tunnel, or the plain-HTTPS fallback bridge (`ants bridge`), optionally with a local store-and-forward leaf node per machine. See [docs/transport.md](docs/transport.md).
- **Harnesses:** ACP (Agent Client Protocol) agents over stdio. Four built-ins, none a default: `copilot` (Copilot CLI), `claude` (Claude Code via `claude-agent-acp`), `codex` (Codex via `codex-acp`) and `antigravity` (Google Antigravity CLI, `agy acp`), plus custom `[harnesses.NAME]` definitions. Built-ins run in their most permissive setting: they run their own tools unasked, so allow rules, tool approvals and webhook/schedule narrowing do not reach those tools (`ants daemon` and `ants doctor` warn) unless the built-in is redefined to ask. See [docs/config.md](docs/config.md#harnesses).
- **Footprint:** one Rust binary, `ants`, serves as the CLI, ant daemon, signer, stdio MCP bridge, channel manager, webhook ingress and HTTPS bridge. An idle daemon stays under 20 MB RSS; when system memory runs low, idle harnesses stop at once and fewer run ([memory pressure](docs/config.md#memory-pressure)).

## Architecture

```text
 machine "macbook"                      hub machine                       machine "desk"
+-------------------------------+      +--------------------------+      +--------------------------+
| ants daemon                   |      | nats-server (hub)        |      | ants CLI (human)         |
|   dev-a -> harness per thread |      |   JetStream domain "hub" |      |   ask, post, inbox,      |
|   dev-b -> harness per thread |      |   stream ANTS            |      |   tools, watch, dash     |
|   MCP server for sessions     |      |   16 KV buckets          |      |            |             |
|            |                  |      |                          |      |            |             |
| nats-server (leaf, outbox)  --+----->|                          |<-----+-- nats-server (leaf)     |
+-------------------------------+      +--------------------------+      +--------------------------+
                        uplink: wss://, tls://, Dev Tunnel, or ants bridge
```

- **Hub:** one `nats-server` with JetStream holding stream `ANTS`, 16 KV buckets and the attachment store `ants_files` (`ants init`). With `ants auth`, every ant, human, webhook, observer, leaf uplink, machine host and channel manager connects using role-scoped NKey/JWT credentials ([docs/auth.md](docs/auth.md)).
- **Leaf:** optional per-machine `nats-server` (`ants leaf config`) on `127.0.0.1:4222` with a store-and-forward outbox (`ants leaf outbox`). Local `ants` processes target `127.0.0.1:4222`; only the leaf uplink crosses the network.
- **Daemon:** `ants daemon` serves ants over per-ant NATS connections and durable consumers, runs up to `max_harnesses` ACP processes per ant, enforces tool allowlists, limits and spending caps, refreshes roster heartbeats and exposes an MCP server to each session.
- **Ants and humans:** ants are named agents (`dev-a`, `reviewer`, `messenger`) with a workdir, harness, capabilities and optional `whoami`. Humans (`human` by default, or named users from `ants auth user NAME --human`) own threads they start and approve tools. Unaddressed `ants ask` requests go to `messenger` for capability routing. Agent-to-agent messages share a per-thread turn budget (default 8).

| Store / subject | Storage / TTL | Purpose |
|---|---|---|
| Stream `ANTS` | File, 30d | Captures `ants.inbox.>`, `ants.notes.>`, `ants.approvals.>`, `ants.dead.>`, `ants.chan.>`, `ants.timeline.>`, `ants.admin.>`; direct gets enabled; republishes `ants.inbox.*.*` to `_INBOX.<to>.stored.<from>`; `max_consumers` 10000. |
| KV `ants_roster` | Memory, 30s | Live `RosterEntry` per ant (`<ant>`), refreshed every 10s; expiry markers enabled; `max_consumers` 10000. |
| KV `ants_sessions` | File, 30d | Thread ACP session IDs with the harness each ran in (`<ant>.<thread>`), spend/escalation/narrowing state, channel seen cursors, mention markers, `_storage.*.*.warned` notes. |
| KV `ants_attempts` | File, 30d | Started delivery markers and saved replies (`<ant>.<seq>`), message marks (`<ant>.msg.<id>`), unserved-ant markers (`<sender>.unserved.<id>`). |
| KV `ants_channels` | File, none | `ChannelEntry` (`<channel>`): members, optional messenger, optional owner ant; `max_consumers` 10000. |
| KV `ants_humans` | File, none | Registered humans (`<human>`), written on CLI connection, each human's signed assistant grant (`<human>.assistant`), enrolling the assistant as a device when `ants assistant seal on` did, and its signed consent to the assistant's decisions (`<human>.confirmed`, `<human>.preallow`), written only by that human; `max_consumers` 10000. |
| KV `ants_chains` | File, none | Persistent `escalate_to` chains (`<ant>`), readable after roster expiry. |
| KV `ants_machines` | File, none | Per-machine `MachineRecord` (`<machine>`): the ants its daemon serves (built-in messenger left out), host name, ants version and time, put on start and on each change with the machine's host credentials (`ants auth host`); never deleted, so an ant keeps its machine after its roster entry expires (`ants machines`). Also the outcome of each admin order to the machine's ants (`<machine>.<ant>.<id>`: applied or refused and why, [docs/auth.md](docs/auth.md#admin-orders)). Each host writes only its own keys; humans read all. |
| KV `ants_links` | File, 30d | Verified `origin_thread` handoffs (`<from>.<thread>.<to>`). |
| KV `ants_owners` | File, 30d | Thread `owner` (`<thread>.<recipient>`); first record wins (`discard_new_per_subject`, 30d dedup window, no delete/purge). |
| KV `ants_read` | File, none | Per-human inbox read marker (`<human>`: `below` + `read` set) and channel read markers (`<human>.chan`: one `below` + `read` set per channel), each shared across devices. |
| KV `ants_presence` | Memory, 30s | Per-device follow presence (`<human>.<device>.<inbox\|approvals>`), refreshed every 10s; each human writes its own keys and reads every human's (`ants chat` shows who is online), ants read none. |
| KV `ants_devices` | File, none | Directory of human devices' encryption keys: each device's user JWT binding its `ants-xkey:` (`<human>.<device>`), put by its CLI on connecting, revoked keys of humans' credentials (`revoked.<U...>`), put by `ants auth revoke`/`push`, and each assistant's user JWT (`<assistant>.assistant`), put by its daemon, against which readers check what it does for its human; humans read all and put only their own devices', an assistant only its own record, and ants direct-get the assistants' records and the revocations. |
| KV `ants_notify` | File, none | Per-human snooze deadline and muted threads (`<human>`), and notices held back for a digest (`held.<human>`). |
| KV `ants_reactions` | File, 30d | Message reactions (`<reactor>.<message_id>.<reaction>`); humans write/delete their own keys and read all; ants neither read nor write, asking `ants signer` instead. |
| KV `ants_threads` | File, none (per-message TTLs allowed) | Shared thread titles (`title.<thread>`), TL;DRs (`summary.<thread>`) and done states (`done.<thread>`), and per-human pin/archive/keep marks (`marks.<human>`) and threads snoozed from home in `ants chat` (`snoozed.<human>`); `ants thread list` prunes those of expired threads. |
| Store `ants_files` (stream `OBJ_ants_files`) | File, 30d | Attachment bytes as chunks of up to 128 KiB on `$O.ants_files.C.<from>.<inbox\|notes\|chan>.<target>.<id>.<n>` (the handle follows the subject of the message carrying the reference `{id, name, mime, size, sha256}`; chunk 0 carries it in header `Ants-Attachment`); up to 8 attachments of 8 MiB each per message; each chunk stored once (copies from a leaf outbox dropped within a 24 h duplicate window), no delete/purge; direct gets. Senders write their own, ants read those of messages they read, humans all. |
| KV `ants_webhooks` | File, 7d | Webhook event hashes (`<webhook>.<hash>`), schedule firings (`<schedule>.<unix_time>`), and hourly `<webhook>.webhook` / `channel-manager.serve` version markers. |
| Core NATS | Unstored | `ants.status.<ant>.<thread>`, `ants.cancel[led].<ant>.<thread>`, `ants.queue[d].<ant>`, `ants.chanadm.<channel>.<ant>`, `ants.enroll.<ant>[.reply]`, `ants.renew.<name>[.reply]`, `ants.antreq.<manager>[.reply]`. |

User, channel, thread and JetStream domain IDs use 1–64 characters of `a-z`, `0-9` and `-`, starting with a letter or digit (`bootstrap`, `channel-manager`, `revoked` and `signer` are reserved: no ant, human, webhook or device takes them). Machine and harness names also allow `_` (machine names allow uppercase ASCII and must differ from `js_domain`). Durations require whole-number units ordered largest first (`s`, `m`, `h`, `d`: `90s`, `10m`, `1h30m`) or `0`; bare non-zero numbers are rejected.

## Install

Requirements: Rust 1.100+, [nats-server](https://github.com/nats-io/nats-server/releases) 2.15+ (tested with 2.15.3), and a signed-in harness CLI on machines running `ants daemon`.

```sh
cargo install --locked --git https://github.com/quanghle/ants ants   # or --path crates/ants
```

- **Windows:** build with MSVC or cross-build `--target x86_64-pc-windows-gnu` (mingw-w64 on `PATH`). Stdio MCP uses a current-user named pipe (`\\.\pipe\ants-mcp-<random>`); bare harness names resolve `.exe`/`.cmd`/`.bat` on `PATH`; `ants daemon` handles Ctrl-C/Ctrl-Break/close/shutdown as `SIGTERM` and auto-reloads edited config files (`webhooks.toml` too). `scripts/install-units.sh` and service actions in `ants setup`, `ants add`, `ants webhook add`, `ants renew`, `ants leaf config`, `request_ant` and `ants tools approve --rule` cover launchd/systemd only (printing manual commands on Windows; pass `--upgrade-check 0` under Task Scheduler). Secret files inherit `%USERPROFILE%` ACLs instead of Unix `0600`.
- **Upgrade:** `ants daemon`, `ants channel serve`, `ants webhook serve`, `ants signer`, `ants bridge serve` and `ants bridge connect` check every `30s` (`--upgrade-check DURATION` / `ANTS_UPGRADE_CHECK`, `0` disables) whether their executable was replaced by a settled, non-empty executable file, then exit with status `75` (`ants daemon` drains prompts in flight first). Installed launchd/systemd units restart automatically on status `75`.

## Quick start

```sh
ants setup --ant dev-a --workdir ~/src/some-repo    # local nats-server, config, services, completions, doctor
ants ask --wait @dev-a "summarise the open TODOs"
ants ask @dev-a --file request.md                   # or --file - (stdin), or --edit ($VISUAL / $EDITOR)
ants ask @dev-a --attach shot.png "what breaks?"    # attach files (up to 8, 8 MiB each)
ants attachment get <handle> -o ~/Downloads         # save an attachment `ants inbox` / `ants thread show` list
ants inbox                                          # unread replies (marks read across devices)
ants roster                                         # active ants and harness status
ants tools list                                     # pending tool approval requests
ants thread list                                    # threads active in the last 24h (--escalated, --pinned, --archived)
ants thread show <thread>                           # full stored thread history
ants thread search --from dev-a --grep deploy       # cross-thread message search
ants thread title <thread> Release 1.2              # shared title (also: pin, unpin, archive, unarchive, done, undone, mute)
ants thread tldr <thread>                           # TL;DR of a thread by the messenger, stored for every human
ants react <message> eyes                           # react to a message (id or unique start of one)
ants answer <message> 2                             # answer an ant's question with an option (or in own words)
ants channel create ops --member dev-a,dev-b
ants post '#ops' "@dev-a the build is red"
ants watch [dev-a [<thread>]]                       # live bus traffic or one ant's streamed progress
ants queue dev-a                                    # waiting messages (ants withdraw / ants cancel)
ants steer dev-a last go left instead             # interrupt dev-a's prompt on a thread and redirect it
ants                                                # full-screen chat UI on a terminal (also: ants chat)
ants --compact                                      # the same as a two-line pane, as for a tmux split
ants status --tmux                                  # one line for tmux status-right: ⚑3 ◐2 $4.12
ants dash                                           # interactive terminal dashboard
```

Without `ants setup`: `nats-server -js &`, `ants harness check --harness claude`, and `ants daemon --ant dev-a --harness claude --workdir ~/src/some-repo &`. A `~/.ants/config.toml` holding only `harness = "claude"` serves the built-in `messenger` alone. Every ant needs a harness: a config without a top-level `harness` whose ants name none fails to load, naming the built-ins.

Joining a team's hub as a human, with the bootstrap credentials the team shares (`ants auth bootstrap`) and an `ants signer` whose config has a [`[registration]`](docs/config.md#human-registration) table:

```sh
ants login --hub wss://hub.example/nats --name alice --bootstrap bootstrap.creds   # GitHub sign-in in a browser
ants inbox                                                                         # uses the [cli] creds and hub it set
ants renew                                                                         # before the credentials expire (30 d by default)
```

A member of a listed GitHub organization or team, or a user with a verified email address in a listed domain, is signed at once; anyone else waits for a human's approval. See [docs/auth.md](docs/auth.md#registering-humans).

## CLI

| Command | Description |
|---|---|
| `ants ask [--thread THREAD [--steer]] [--plaintext] [--wait \| -f] [--timeout DUR] [--attach PATH]... [@ant] TEXT \| --file PATH \| --edit` | Send to `@ant`, else to the human's assistant while its [grant](docs/auth.md#assistant-grant) stands, else to `messenger`; prints `thread <id>`, `message <id>`, `to <ant>` without `@ant` (`to pa (assistant of alice)`) and `attachment <handle>` per `--attach` file (see Attachments below). From a human to `@HUMAN`, seals the text and attached files for both humans' devices, or refuses without sendable keys unless `--plaintext` ([docs/auth.md](docs/auth.md#sealed-messages-between-humans)). `--steer` (human creds) cancels the prompt `@ant` runs on the thread and prompts the same session with the message next, ahead of the queue ([docs/config.md](docs/config.md)). Warns on stderr if `@ant` is missing from `ants_roster`, `stale`, `signed-out`, `failing`, or provider-limited with no fallback answering. |
| `ants inbox [--peek] [--thread THREAD] [--limit N] [-f] [--json]` | Print unread replies (with message IDs and reactions) and mark read across all devices (`--peek` keeps unread; `-f` streams replies, reactions added/removed to shown messages (human creds; `--json`: `{message, by, reaction, removed}`), `MAX_DELIVERIES` advisories and, without `--thread`/`--json`, tool requests; records presence without `--thread`). Attachments show as `📎 name (size) handle` lines; `--json` gives each as its reference with `handle`. |
| `ants roster [--json]` | List ants (`live`/`stale`, `idle`/`working`/`signed-out`/`failing`/`rate-limited`/`out-of-quota`/`fallback`), daemon/perms versions, harness and `AGENT` (`name@version`), capabilities, `escalate_to` and workdirs, plus version-drift, recovery and memory-pressure notes. |
| `ants ant show ANT [--json] [--config FILE]` | One ant at a glance, as the ant view of `ants chat` shows it: its roster entry (machine, harness, model, workdir, capabilities, version), what holds it back, the messages waiting for it, its spend that UTC day and its latest 10 threads. |
| `ants ant memories ANT [--json] [--machine M]` | Show an ant's memories, read by its machine's daemon through a `read_memories` admin order while the daemon is online: its core memories (`whoami`, `memory`, `tasks`, which a `set` order edits) and, read-only and size-capped, its harness's memory files (`memory_files`, [docs/config.md](docs/config.md#memory-files)) and the bd memories of `BD_DB` in its environment. Human device's admin creds. |
| `ants ant orders ANT [--json]` | List the admin orders to an ant on every machine, oldest first: time sent, admin, machine, order and where it stands (`pending` until the machine's daemon takes it, then `applied` or `refused` with the reason) ([docs/auth.md](docs/auth.md#admin-orders)). Human creds. |
| `ants ant restart ANT [--now] [--machine M] [--json]` | Order the daemon serving ANT (on `--machine`, else the machine that last recorded serving it) to restart its harnesses: idle ones stop at once, working ones once their current prompt ends, or with `--now` their prompts are cancelled as `ants cancel` does; each thread's next prompt resumes its session in a new harness. Prints what it stopped once the daemon applied the order, or that it is pending after 10 s, and fails with the reason of a refusal ([docs/auth.md](docs/auth.md#admin-orders)). A human device's admin creds. |
| `ants machines [ANT] [--json]` | List the machines whose daemons recorded the ants they serve in `ants_machines` (machine, host name, version, time recorded, ants), kept after an ant goes offline; with `ANT`, the machines serving it or that last did, latest first (`--json`: `{ant, machines}`). Human creds. |
| `ants spend [ANT] [--thread THREAD \| --days N] [--json] [--config FILE]` | Report token and cost spend over UTC days (default 7, max 366) or on a thread against local config caps ([docs/config.md](docs/config.md#spend-report)). |
| `ants notes THREAD [--attach PATH]... [TEXT]` | Show thread notes with their attachments, or post `TEXT` (with any `--attach` files) as the CLI human (resets budget to 8 turns; wakes nobody). |
| `ants react MESSAGE REACTION [--remove]` | Add or remove a message reaction (`1–64` of `a-z0-9-`); wakes nobody, marks nothing read, ends no wait. `MESSAGE` is a message id or, for humans, a unique start of one (ambiguous or unknown starts list candidates). With ant credentials, `ants signer` makes the change, only for a message the ant reads. |
| `ants answer MESSAGE N\|TEXT...` | Answer a question an ant asked the human (MCP `ask_human`) with option `N` or, if the question takes them, own words; prints `thread <id>` and `message <id>`. Human creds only; `MESSAGE` resolves as for `ants react`. |
| `ants post [--thread THREAD] [--wait \| -f] [--timeout DUR] [--attach PATH]... CHANNEL TEXT \| --file PATH \| --edit` | Post to `ops` or `'#ops'`; prints `thread <id>`, `message <id>` and `attachment <handle>` per `--attach` file (`@ant` wakes members, `@human` notifies members, no mention triages). |
| `ants channel create \| set \| add \| remove \| list \| delete` | Manage channels (`--member USER,...`, `--messenger ANT`, `--default-messenger`, `--owner ANT`, `--no-owner`). For a human member of any channel, `list` adds UNREAD: unread posts of each channel it is a member of (own posts count as read), with unread @mentions as `N (@M)`. |
| `ants channel read CHANNEL [--thread THREAD \| --limit N] [--peek]` | Print channel posts grouped by thread, with their attachments; for a human, marks the printed posts read on every device unless `--peek`. |
| `ants channel serve` | Run the channel manager (`ants auth channel-manager` creds) to apply owner `manage_channel` requests. |
| `ants thread list [--ant USER] [--owner HUMAN] [--grep TEXT] [--escalated] [--pinned \| --archived] [--since DUR] [--limit N] [--json]` | List active threads within `--since` (default `24h`, `0` all; limit 50), pinned last, archived and `auto-archived` hidden unless `archived+new`, `done` in STATE (`done` with `by`/`at` in `--json`). |
| `ants thread show THREAD [--since DUR] [--json] [--no-tools]` | Print a thread's stored messages, posts, notes, escalations, tool requests/decisions, dead letters, attachments (`📎 name (size) handle`; `attachments` with `handle` in `--json`) and reactions, under a header with its title, `done by <human> <time>` and the CLI human's mark; questions show their options and the answers given, and the tool calls its ants made show as entries of kind `tool` (time, ant, tool kind, title, status, duration; `--no-tools` leaves them out). |
| `ants thread search [--grep TEXT] [--from USER] [--to USER] [--channel NAME] [--type KIND,...] [--since TIME] [--until TIME] [--limit N] [--json]` | Search stored messages across threads matching all filters (latest `--limit`, default 50, oldest first). |
| `ants thread title THREAD [TEXT...] [--clear]` | Print, set (1–120 chars, shared across humans) or clear a thread title; sealed for both humans' devices on a thread of sealed messages between them. |
| `ants thread done \| undone THREAD` | Mark a thread done, or clear its done state, for every human, printing whether anything changed; a later message, post or note reopens it. |
| `ants thread pin \| unpin \| archive \| unarchive THREAD` | Pin or archive a thread for the CLI human across devices (mutually exclusive; archiving cancels nothing; unarchiving an `auto-archived` thread keeps it another `auto_archive` period). |
| `ants thread tldr THREAD [--new] [--json]` | Print the thread's stored TL;DR while current, else ask `[cli]` `summary_ant` for one (at most 120 words) and store it for every human ([docs/config.md](docs/config.md#catch-up-summaries)). |
| `ants digest [--since DUR]` | Ask `[cli]` `summary_ant` for a digest of the threads with entries within `--since` (default `24h`, `0` all); stored nowhere. |
| `ants thread mute \| unmute THREAD [--for DUR]` | Mute push notifications for `THREAD` across devices (default `30d`, max `365d`), or unmute ([docs/webhooks.md](docs/webhooks.md#quiet-hours-snooze-and-muted-threads)). |
| `ants attachment get HANDLE [-o PATH]` | Save an attachment to `PATH` (into it under its name when a directory) or `./name`, after checking its size and SHA-256; refuses to overwrite a file and writes nothing on a mismatch. One of a sealed message is opened with the device's key first, and refused where the device does not open the message ([docs/auth.md](docs/auth.md#sealed-messages-between-humans)). Missing chunks, perhaps still on their way from a leaf's outbox, are waited for up to 60 s when the attachment was made within the hour or a message stored on its handle's subject within the hour carries it, looked up by message gets with a human's credentials and through a spare of the ant's inbox, notes or channel reader with an ant's (not for a message the ant sent, which only its daemon reads back) ([docs/transport.md](docs/transport.md)). |
| `ants assistant enable` / `disable` | Create the CLI's human's dedicated assistant ant in one step where the team's account key is (default name `<human>-assistant`, `--name`; harness and model flags as `ants add`): its credentials naming the human, an `[[ant]]` working in its own home with an assistant's whoami and no `fetch` or `execute` rules, and the signed grant; run again, it keeps what is in place. `disable` withdraws the grant, ending any enrollment as a device, and offers to retire the dedicated ant (`--retire`): unlisted, credentials and encryption key seed removed and the credentials revoked, home kept. The recommended path ([docs/auth.md](docs/auth.md#dedicated-assistant)). |
| `ants assistant set ANT` / `clear` / `show [HUMAN]` | Designate an existing ant as the one acting for the CLI's human (one at most), withdraw it at once, or show a human's assistant once its device-signed grant verifies; `set` refuses an ant added from a template and warns of a workdir or tools allowed without asking ([docs/auth.md](docs/auth.md#assistant-grant)). What the assistant sends for its human reads `<human> via <assistant>` once verified, else `<assistant> (forged: for <human>)`; the human is told in its inbox, and a decision made for it is held for the human ([docs/auth.md](docs/auth.md#acting-for-its-human)). |
| `ants assistant confirm [ACT] [--deny]` / `allow [RULE...] [--remove \| --clear]` | List the assistant's decisions on tool requests held for the CLI's human, or confirm the one on `ACT` (`<ant>/<id>`) so it takes effect as the human's, or drop it (`--deny`); `allow` lists, adds or removes the human's pre-allow rules (`<ant>/<rule>` or `*/<rule>`, an allow rule), under which the assistant's decisions take effect unconfirmed but `allow_always`. Signed by the human's device ([docs/auth.md](docs/auth.md#confirming-an-assistants-acts)). |
| `ants assistant seal on [--key KEY]` / `seal off` / `key ANT` | Enroll the human's assistant as one of its devices, so messages sealed for the human are sealed for it too and it seals what it sends for the human, or end that at once (`clear` and `disable` do too); off unless enrolled. `key ANT` prints the encryption key of an ant on this machine, made if missing, for `--key` on the human's machine ([docs/auth.md](docs/auth.md#assistant-as-a-device)). |
| `ants notify snooze DUR` / `resume` / `status [--json]` | Snooze push notifications (`1s`–`365d`, subject to `bypass`), resume, or inspect active holds. |
| `ants watch [ANT [THREAD] \| SUBJECT]` | Stream traffic (default `ants.>`), escalations/decisions, reactions (human creds), or live harness progress for `ANT`. |
| `ants` / `ants chat [--since DUR] [--compact]` | Full-screen chat UI (human creds + TTY; threads of the last 7 d by default): nav of home, work by state, the human's channels, ants and people (online or away) beside the selected view; a person opens the direct messages with that human, where `n` writes a new one. `--compact` (also bare `ants --compact`) draws a two-line pane instead, as for a tmux split. Bare `ants` opens it on a terminal and prints the usage (exit 2) without one; with no server reachable it points at `ants setup` and `ants login`. |
| `ants status [--tmux \| --json]` | One line, no TTY needed (human creds): home items by kind, the ants working (roster) and the ants' spend that UTC day; `--tmux` prints `⚑<items> ◐<working> $<spend>` for tmux `status-right`, `--json` one object. Reads for at most 3 s; what is not read in time prints `?` (`null` in JSON). |
| `ants dash [--since DUR]` | Interactive TUI for recent threads (archived and `auto-archived` hidden unless `archived+new`), live progress, roster and tool approvals (human creds + TTY). |
| `ants cancel ANT THREAD` | Cancel the prompt `ANT` runs on `THREAD` (5s timeout; human creds). |
| `ants steer ANT THREAD [--wait \| -f] [--timeout DUR] [--attach PATH]... TEXT \| --file PATH \| --edit` | Shorthand for `ants ask --steer --thread THREAD @ANT`: cancels the prompt `ANT` runs on `THREAD` and prompts the same session with the message next (human creds). |
| `ants queue ANT [--json]` / `ants withdraw ANT MESSAGE` | Inspect waiting messages for `ANT`, or withdraw an unstarted message (human creds). |
| `ants tools list [-f] [--json]` / `show ID [--json]` | List or inspect pending tool requests (`<ant>/<id>`, unique prefix accepted; `-f` records approval presence). |
| `ants tools approve ID [--always] [--rule [--config FILE]]` / `reject ID` | Decide a pending tool request; `--rule` appends the exact allow rule to `config.toml` and reloads the daemon. |
| `ants dead list` / `replay SEQ` / `drop SEQ` / `watch` | Inspect, replay or drop dead letters; `watch` converts `MAX_DELIVERIES` advisories into dead letters. |
| `ants init` | Create whichever of stream `ANTS`, the 16 KV buckets and store `ants_files` is missing, leaving existing ones as they are except for adding the subjects `ANTS` lacks and widening a duplicate window of `ANTS` or `OBJ_ants_files` shorter than 24 h, in place (human creds; also runs automatically on first human CLI connect). |
| `ants daemon [--ant NAME] [--config FILE] [--metrics ADDR] [ant-flags...]` | Serve config-file ants and schedules, or a single `--ant` ([docs/config.md](docs/config.md)). |
| `ants add NAME [--config FILE] [--out FILE] [--store-key] [--jwt JWT \| --enroll CREDS [--hub URL] [--leaf-config FILE]] [ant-flags...]` | Generate local NKey, append `[[ant]]` to `config.toml`, and complete credentials ([docs/auth.md](docs/auth.md#adding-an-ant)). |
| `ants retire NAME [--config FILE] [--dir DIR] [--no-push]` | Unlist, drain, revoke and delete credentials of a templated ant while keeping `~/.ants/<name>` ([docs/config.md](docs/config.md#retiring-an-added-ant)). |
| `ants signer [--config FILE] [--approval-timeout DUR] [--dir DIR]` | Sign or human-gate `request_ant` requests, process `retire_ant` revocations, answer ants' reaction requests ([docs/config.md](docs/config.md#manager-ants)) and register humans' devices after a GitHub sign-in ([`[registration]`](docs/config.md#human-registration)), renewing them while their grant holds and revoking those whose GitHub users left the policy. |
| `ants enroll list` / `approve NAME [...]` / `reject NAME [--reason TEXT]` | List, sign (`--webhook --to/--channel` for webhooks) or reject `ants add --enroll` requests. |
| `ants enroll renewals` / `renew NAME [--public-key KEY]` / `renew --auto` | List or sign over-NATS `ants renew` requests, except those of devices registered with `ants login`, which `ants signer` renews ([docs/auth.md](docs/auth.md#renewing-over-nats)). |
| `ants login --hub URL --name NAME [--label TEXT] [--store-key] [--bootstrap FILE]` | Register this machine as a device of human `NAME` after a GitHub sign-in answered by `ants signer`, writing `~/.config/ants/auth/NAME.creds` and setting `[cli]` when it names no credentials ([docs/auth.md](docs/auth.md#registering-humans)). |
| `ants renew [FILE] [--config FILE] [--leaf-config FILE]` | Rotate `.creds` over NATS (signed by `ants enroll renew`, or by `ants signer` for devices registered with `ants login`) and reload the daemon, or reconnect the leaf node's uplink ([docs/auth.md](docs/auth.md#renewing-over-nats)). |
| `ants keys store` / `unstore` / `remove [FILE...]` | Move NKey seeds (and a human device's encryption key seed) into/out of the OS credential store, or delete local credentials ([docs/auth.md](docs/auth.md#private-keys-in-the-os-credential-store)). |
| `ants harness check` / `prompt TEXT` | Verify harness install and sign-in, or run one standalone prompt (`--harness NAME`, `--config FILE`; tool requests rejected, though built-ins run their tools unasked). |
| `ants setup [--ant NAME --workdir DIR] [--harness NAME] [--config FILE] [--nats-server PATH] [--no-services] [--no-harness] [--no-completions] [--dry-run]` | Idempotently set up a single machine and run `ants doctor`. |
| `ants completions SHELL` / `--install [SHELL]` | Print or install dynamic completions for `bash`, `zsh` or `fish`. |
| `ants doctor [--config FILE] [--leaf-config FILE] [--dir DIR] [--webhook-config FILE] [--no-harness] [--json]` | Audit local config, keys, hub/leaf/outbox, versions, storage, services, webhooks and tunnels (`exit 1` on `fail`). |
| `ants auth init` / `user` / `enroll` / `bootstrap` / `channel-manager` / `leaf` / `revoke` / `revoked` / `issued` / `push` / `backup` / `restore` / `devices` / `rename` | Manage team NKeys, role `.creds`, revocations, age-encrypted backups and human devices ([docs/auth.md](docs/auth.md)). |
| `ants leaf config MACHINE --uplink URL ...` / `outbox MACHINE ...` | Write `~/.config/ants/leaf.conf` or configure store-and-forward outbox sourcing ([docs/transport.md](docs/transport.md)). |
| `ants bridge serve` / `connect URL` | Tunnel a leaf uplink over plain HTTPS `POST`/`GET` requests ([docs/transport.md](docs/transport.md#https-fallback-bridge)). |
| `ants webhook serve [--config FILE] [--listen ADDR] [--metrics ADDR]` / `notify-test HUMAN` / `add NAME ...` | Serve HTTP webhooks and human push notifications, test sinks, or provision a webhook ([docs/webhooks.md](docs/webhooks.md)). |
| `ants hook run --on EVENT [--concurrency N] [--timeout DUR] -- CMD [ARGS...]` | Run `CMD` per bus event using `--observer` credentials ([docs/hooks.md](docs/hooks.md)). |
| `ants mcp --socket PATH --token TOK` | Internal stdio-to-socket/pipe MCP bridge spawned by `ants daemon`. |

`THREAD` in `ask`, `inbox`, `post`, `notes`, `thread show`, `thread title`, `thread pin`/`unpin`/`archive`/`unarchive`, `thread done`/`undone`, `thread mute`/`unmute`, `cancel`, `watch`, `spend --thread` and `channel read --thread` accepts a hyphenated UUID (used without lookup), a unique prefix (or non-UUID thread ID), or `last` (latest thread the human sent or received on). Prefix lookup (human creds only) checks the human's own threads from the last 7 d first, then all threads in the last 7 d (bounded to `[-5m, +10m]` around the timestamp encoded in a UUIDv7 prefix); exact matches win, and ambiguous or missing prefixes list candidates.

### Identity, setup, completions and doctor

- **Connection & identity:** every command accepts `--nats`, `--creds` and `--js-domain`, falling back to `[cli]` then top-level keys in `ANTS_CONFIG` (`~/.ants/config.toml`), except `ants daemon` and `ants add` (which read top-level config directly). NATS URLs containing credentials/tokens are rejected. Connecting with human credentials ensures `ants_humans` contains `<name>` (`human` needs no entry) and runs an `ants init` pass creating missing objects. Ant credentials skip `ants_humans`, warn, refuse `ants inbox`, and leave `--wait`/`-f` replies unread.
- **`ants setup`:** without `--harness` and a config file, first chooses the harness of a new config by looking up each built-in's command on `PATH` (no handshake): the only one installed is picked and reported, several are offered in a numbered prompt on a terminal (failing without one), and none installed fails with each built-in's install and sign-in commands ([docs/config.md](docs/config.md#choosing-a-harness-in-ants-setup)). It then idempotently writes `~/.config/ants/nats.conf` (JetStream in `~/.ants/jetstream`, skipped for non-loopback `--nats` or existing `leaf.conf`), installs/starts `ants.nats` / `ants-nats`, runs `ants init`, creates/appends `~/.ants/config.toml`, runs `ants harness check`, issues the machine's host credentials when its ants have credentials and the team keys are on the machine ([docs/auth.md](docs/auth.md#machine-host-credentials)), installs/starts/reloads `ants.daemon` / `ants-daemon`, installs shell completions, and runs `ants doctor --no-harness` (`--dry-run` previews without changes).
- **Shell completions:** `ants completions --install` writes to `~/.local/share/bash-completion/completions/ants`, `~/.local/share/zsh/site-functions/_ants` (add to `fpath` before `compinit`), or `~/.config/fish/completions/ants.fish` (`XDG_DATA_HOME`/`XDG_CONFIG_HOME` honoured). Dynamic lookups (`ANTS_COMPLETE`, 2 s timeout, silent on error, skipped for OS-keystore seeds) complete `@ant` / ant names, channels, `last` + recent 7 d threads (with thread `title`, else latest line), the latest 50 messages to or from the CLI's human over 7 d matching the typed start (with route and first line) and suggested reactions on `ants react`, pending tool requests, and dead-letter sequences.
- **`ants doctor`:** read-only end-to-end check (`ok`, `warn`, `fail`, `skip`, with `fix` commands; 5 s per request, 10 s per network check, 60 s per harness handshake). Checks `config`, CLI `credentials` + OS `credentials key` + a human device's `credentials xkey`, `server` (failing below `nats-server` 2.15) + `jetstream`, `hub` objects, `consumers` (`≥ 80%` of `max_consumers`, raised with `nats stream edit`), team `versions` + cross-machine `harness versions`, JetStream `storage`, the machine's `host credentials` (warning when its ants have credentials and the host none), per-ant `credentials` / `harness` / shared git `workdir` / `reactions` (asked as the ant: `ants signer` or `ants webhook serve` answering, warning when nothing answers `react` or either; a daemon without credentials answers itself) / `tool permissions` (allow rules, approvals or webhook/schedule narrowing a harness running its tools unasked skips), `hub resolver` (`full` resolver) + `team keys backup` (when `--dir` holds team keys), `leaf` / `leaf uplink` / `leaf system uplink` / `leaf node` / `outbox`, installed `daemon` / `leaf node` / `hub` (the `ants setup` nats-server of `~/.config/ants/nats.conf`, without `leaf.conf`) / `webhook` services, `webhooks.toml` secrets (`0600`) / `exec` commands / `public_url` `/_approval` probe, and installed `devtunnel` / `bridge` services, tokens and reachability.

### Environment variables

| Variable | Flag / Scope | Default / Behavior |
|---|---|---|
| `ANTS_NATS_URL` | `--nats` (global) | `[cli]` or top-level `nats`, else `nats://127.0.0.1:4222` |
| `ANTS_CREDS` | `--creds` (global) | `[cli]` `creds`; unset connects unauthenticated as `human` |
| `ANTS_JS_DOMAIN` | `--js-domain` (global) | `[cli]` or top-level `js_domain` (`$JS.<domain>.API`) |
| `ANTS_CONFIG` | `--config` (`daemon`, `add`, `retire`, `signer`, `renew`, `harness`, `spend`, `setup`, `doctor`, `tools approve`); global `[cli]` defaults | `~/.ants/config.toml` |
| `ANTS_COMPLETE` | Completion scripts | Emit shell completions and exit |
| `ANTS_ANT` | `--ant` (`ants daemon`) | Serve only the named ant |
| `ANTS_METRICS` | `--metrics` (`ants daemon`) | `/healthz` and `/metrics` address, overriding config `metrics` |
| `ANTS_AUTH_DIR` | `--dir` (`auth`, `enroll`, `signer`, `retire`, `leaf config`, `webhook add`, `doctor`) | `~/.config/ants/auth` |
| `ANTS_BACKUP_PASSPHRASE` | `ants auth backup` / `restore` | Fallback when `--passphrase-file` is omitted (else terminal prompt) |
| `ANTS_BRIDGE_TOKEN` | `--token` (`bridge serve`, `bridge connect`) | Optional bearer token (`--token-file FILE` preferred) |
| `ANTS_MCP_TOKEN` | `--token` (`ants mcp`) | Per-thread session token passed by `ants daemon` |
| `ANTS_UPGRADE_CHECK` | `--upgrade-check` (`daemon`, `channel serve`, `webhook serve`, `signer`, `bridge serve`, `bridge connect`) | `30s` (`0` disables) |
| `ANTS_LOG` | All commands | `tracing` stderr filter, default `info,rmcp=warn` |
| `SSL_CERT_FILE` | TLS / `ants bridge connect` | PEM CA bundle trusted by `rustls` |
| `ANTS_REQUIRE_TOOLS` | `cargo test` | When `1`, fail instead of skipping when `nats-server` is missing |
| `ANTS_TEST_JS_ACK_FC_V2` | `cargo test` | When `1`, enable `js_ack_fc_v2: true` on test `nats-server`s |

`contrib/skills/ants/SKILL.md` teaches external coding agents to use the `ants` CLI (`copilot skill add contrib/skills/ants/SKILL.md`).

## Delivery

### Composing and waiting

- **Input:** `ants ask` and `ants post` take trailing words joined by spaces, `--file PATH` (`--file -` reads stdin; preserves internal formatting, strips BOM and trailing newlines), or `--edit` (`$VISUAL` -> `$EDITOR` -> `vi`/`notepad` on a `0600` temp file; empty/blank messages abort).
- **Attachments:** `--attach PATH` (repeatable; `ants ask`, `ants post`, and `ants notes` with `TEXT`) attaches up to 8 regular files of at most 8 MiB each, named by their base name, typed by extension (`application/octet-stream` otherwise). Files are read and stored in `ants_files` before the message is sent; any failure sends nothing. Each attachment is addressed by a handle `<sender>.<inbox|notes|chan>.<target>.<id>` that `ants attachment get` takes; readers of a message can fetch its attachments.
- **Sealed messages:** a message between two humans is sealed end to end for their devices' keys in `ants_devices`, and for an assistant either enrolled as a device (`ants assistant seal on`) (`--plaintext` sends it in the clear). Readers print what the device opens, looking up a sending device enrolled since they started, else `[sealed]` (no key for the device) or `[forged]` (does not open); `--grep` matches opened text only, and `ants watch` and dead letters show `[sealed, N bytes]` ([docs/auth.md](docs/auth.md#sealed-messages-between-humans)).
- **Waiting:** `--wait` prints the first reply and exits `0` (`--timeout` exits `1` on timeout; Ctrl-C exits `130`); `-f`/`--follow` streams replies until `--timeout` or Ctrl-C (`0`), marking printed replies read, plus reactions added/removed to the sent message and printed replies (human creds; `dev-a reacted eyes to message <id> (alice: <opening>)`). On a terminal with human credentials, stderr displays a live in-place status line (`not served`, `queued behind N`, `working`, `thinking`, `writing`, `running <tool>`, `waiting for approval` with `ants tools approve <ant>/<id>`, `harness signed out`).

### Pool, worktrees and at-least-once retries

Each ant runs up to `max_harnesses` (default 4) ACP processes, concurrent across threads and sequential per thread. Idle harnesses stop after `idle_timeout` (`10m`), or at once while system memory is low ([`[memory_pressure]`](docs/config.md#memory-pressure)), and resume via ACP `session/load` (`<ant>.<thread>` in `ants_sessions`), rebuilding from stored thread history if `session/load` fails ([docs/config.md](docs/config.md#lost-sessions-and-context-window)). First Ctrl-C/`SIGTERM` drains active prompts; second Ctrl-C exits immediately.

With worktrees on, each thread gets its own git worktree at `<home>/worktrees/<thread>` on branch `ants/<ant>/<thread>` from `HEAD`, pruned once `<ant>.<thread>` expires from `ants_sessions` (preserving branches with unmerged commits and appending an `[ants]` notice). Worktrees are on by default for a workdir inside a git work tree, and not ignored by it, with `max_harnesses > 1` (logged at daemon start); `worktrees = true` (`--worktrees`) forces them on and `worktrees = false` (`--no-worktrees`) keeps every thread in the shared workdir. Trade-offs: one checkout per thread (plus its build outputs) on disk, a branch per thread to merge or delete, and threads not seeing uncommitted changes of the workdir; see [docs/config.md](docs/config.md#worktrees). `ants daemon` and `ants doctor` warn when `worktrees = false` leaves `max_harnesses > 1` sharing a git workdir.

With `worktree_base = "upstream"` (default `"head"`), a thread's new branch starts from the default branch of the repository's default remote (the current branch's upstream remote, else `origin`): `git fetch` runs first (60 s timeout, no credential prompt), then the branch starts from `refs/remotes/<remote>/HEAD`, else from the current branch's upstream. Without a remote, or when the fetch fails or neither ref is known, the branch starts from `HEAD` with a logged warning. Branch commits already in that remote branch count as merged when pruning and in the `[ants]` notice.

With `publish = "draft-pr"` (default `"off"`) and worktrees on (set or by default), a prompt that ends with new commits on the thread's branch pushes `ants/<ant>/<thread>` to the default remote and opens a draft pull request with `gh` (title: the thread's title when the ant's credentials can read `ants_threads`, else the first line of the thread's first prompted message; body: the ant and thread id), or finds the open one, which the push updated. The push needs an allow rule such as `execute:git push origin ants/<ant>/*` or an approval, like a tool call ([docs/config.md](docs/config.md#tool-permissions)). The reply carries an `[ants]` line with the pull request URL, or with what was rejected or failed; nothing is retried until the branch has commits the remote lacks. A branch the remote already has as it is, or a prompt cancelled by a human, publishes nothing. `git push` and `gh` run without prompts (`GIT_TERMINAL_PROMPT=0`, `GH_PROMPT_DISABLED=1`), bounded by timeouts (120 s, 60 s), with `gh` authenticated on the machine.

Attachments of a prompted message (an inbox message, or a channel post read under its `chan` handles) end the prompt's text with one line each: `[ants] Attached: "NAME" (MIME, SIZE bytes), handle HANDLE`, then how to read it with the `read_attachment` MCP tool. When the harness's ACP `initialize` advertises `promptCapabilities.image`, each `image/png`, `image/jpeg`, `image/gif` or `image/webp` attachment is read from `ants_files` with the ant's credentials, checked against its size and SHA-256 (chunks missing of an attachment made within the hour, or carried by a message the hub stored within the hour, perhaps still on their way from a leaf's outbox, waited for up to 60 s; likewise for `read_attachment` on the handles of the message being handled), and handed after the text as an ACP image content block, its line saying so. Other attachments, all of them for harnesses without image support, and those that cannot be read (invalid reference, bytes missing or not matching, with the reason in the line) stay lines only; the prompt runs regardless. The earlier channel posts a prompt shows as the thread's context, and the entries of a rebuilt session's history, list their attachments the same way, one line each under the post or message, with the handle its subject gives and nothing read from the store; in a rebuilt session these lines count toward the history budget.

In-hand messages send 20 s progress acks against a 60 s `ack_wait`, acking only after storing the reply:

1. Claims `<ant>.msg.<id>` = `started` in `ants_attempts` (unless already `withdrawn`), then writes `<ant>.<seq>` before prompting so redeliveries after a crash prepend a prior-tool-call warning.
2. Saves the finished reply in `<ant>.<seq>` and `<home>/answered` before publishing with 24 h dedup ID `reply-<ant>-<envelope_id>`, preventing re-prompts across post-generation disconnects or offline leaf outboxes, then marks `<ant>.msg.<id>` = `finished`.
3. Streams unpersisted `StatusEvent` JSON on `ants.status.<ant>.<thread>` (`state`, 250 ms batched `message`/`thought`, `tool`, `plan`, `queued`, `approval`, `escalated`, `decided`).
4. Stores the prompt's tool calls and plan as compact `Timeline` records on `ants.timeline.<thread>.<ant>` (at most one every 30 s while calls changed, the rest when the prompt ends; ≤ 50 entries each, ≤ 200 calls per prompt with the rest counted; titles and paths redacted as status is, never inputs, outputs, text or thoughts). A failed store is logged and dropped; credentials older than permissions version 2 store none, with one warning ([docs/hooks.md](docs/hooks.md#timeline-on-antstimelinethreadant)).

### Queue and withdrawal

- `ants queue ANT` queries `ants.queue.<ant>` (5 s timeout), combining messages held by the active daemon with up to 100 undelivered inbox messages from `ANTS`.
- `ants withdraw ANT MESSAGE` atomically creates `<ant>.msg.<id>` = `withdrawn` in `ants_attempts` (losing to `started` if prompting already began; sign-out and provider-limit holds before tool execution clear `started` so messages stay withdrawable). When its turn arrives, the daemon replies `[ants] Withdrawn by <human> before <ant> was prompted with it (ants withdraw).` and acks without prompting.
- **Unserved-ant notices:** daemons (via `sentread-<ant>`), `ants webhook serve` (with `escalations = true`) and `ants inbox -f` check sent messages every minute. If a message waits ≥ 15 m for an ant absent from `ants_roster` ≥ 15 m, the first watcher to claim `<sender>.unserved.<id>` in `ants_attempts` notifies the owner/sender once with `ants withdraw` instructions and up to 5 capability-matching live peers.

## Config file

`~/.ants/config.toml` (`--config` / `ANTS_CONFIG`) configures multi-ant daemons, CLI defaults, custom harnesses, worker templates and schedules, reloading automatically on edit (~6 s) or `SIGHUP`:

```toml
harness = "copilot"
nats = "nats://127.0.0.1:4222"
js_domain = "hub"

[cli]
creds = "~/.config/ants/auth/alice.creds"

[[ant]]
name = "dev-a"
workdir = "~/src/repo-a"
creds = "~/.config/ants/auth/dev-a.creds"
harness = "claude"
whoami = "Maintain Rust backend services."
capabilities = ["rust", "backend"]
allow = ["read", "search", "execute:cargo test*"]
escalate_to = ["lead"]
```

Full reference in [docs/config.md](docs/config.md):

- [Precedence and reload](docs/config.md#precedence-and-reload)
- [Ant templates](docs/config.md#ant-templates), [Manager ants](docs/config.md#manager-ants) (`request_ant`, `ants signer`) and [Retiring an added ant](docs/config.md#retiring-an-added-ant) (`retire_ant`, `ants retire`)
- [Human registration](docs/config.md#human-registration) (`[registration]`, GitHub sign-in policy)
- [Harnesses](docs/config.md#harnesses) (`copilot`, `claude`, `codex`, `antigravity`, custom `[harnesses.NAME]` and `harnesses.d` drop-ins, sign-in/rate-limit/quota/failure holds, [fallbacks](docs/config.md#fallbacks), session rebuilds and context windows)
- [Prompt limits and spending caps](docs/config.md#prompt-limits-and-spending-caps) and [Spend report](docs/config.md#spend-report)
- [Tool permissions](docs/config.md#tool-permissions) (allowlist rules, `--rule` suggestions, chain approvals, webhook/schedule narrowing, all for harnesses that ask before running tools) and [Redaction](docs/config.md#redaction)
- [Schedules](docs/config.md#schedules) (`[[schedule]]` `cron` / `every`, time zones, `catch_up`)

## Roster, identity and memory

Each daemon publishes a `RosterEntry` at `<ant>` in `ants_roster` every 10 s (and immediately on state transitions), deleting it on clean exit (`30s` TTL with `MaxAge` delete markers; `ants roster` marks entries without a heartbeat for 20 s as `stale`):

- **Identity & runtime:** `machine`, daemon `instance`, `harness`, `agent` (`{name, version}` from ACP `initialize`), `workdir`, `capabilities`, `whoami`, `escalate_to`, `version`, `perms`, `started_at`, `heartbeat_at`, `activity` (`idle` / `working`), `handling` count, live `harnesses` count.
- **Hold states:** `signed_out` (`{since, sign_in?}`), `limited` (`{kind: "rate_limit" | "quota", since, until}`), and `failing` (`{since, error}`), plus `fallback` (`{name, since}`) while one of the ant's `fallback` harnesses or models answers instead.
- **Memory pressure:** `memory_low` (`{since, available, max_harnesses}`) while the machine's available memory is below `[memory_pressure]` `min_available` ([docs/config.md](docs/config.md#memory-pressure)).

Per-ant prompt instructions and state isolation:

- `whoami` (`--whoami TEXT`): role description prepended to every prompt and published in `ants_roster` (keep secrets out; `route` matches `capabilities`, not `whoami`).
- `memory` and `tasks` (`--memory`, `--tasks`): private instructions prepended to every prompt.
- `env` (`--env NAME=VALUE`): literal environment overrides passed to the harness and tool subprocesses (e.g. `BD_DB = "/Users/alice/.ants/dev-a/.bd/bd.db"`).

## MCP tools

Each session gets the `ants` MCP server over streamable HTTP (`127.0.0.1:<port>/mcp` with per-thread bearer token) or via `ants mcp --socket <home>/mcp.sock` (`0600`, `ANTS_MCP_TOKEN`; current-user named pipe on Windows). Allowlist rules match `other:<tool>`:

| Tool | Description |
|---|---|
| `send_message(to, text, thread?, attach?)` | Send to an ant or human (`thread` omitted: current; `"new"`: fresh; else ID); warns when unserved or limited. |
| `ask_ant(to, text, timeout?, attach?)` | Ask an ant on the current thread and wait (`timeout` default `5m`, max `30m`; refuses humans, unserved ants, or limits outlasting `timeout`). |
| `check_inbox(all_threads?)` | Drain late `ask_ant` replies arriving after timeout (persisted in `<home>/mcp.json`). |
| `ask_human(question, options, free_text?)` | Ask the thread's owner (else the default human) a question with 2–9 options (question ≤ 500 chars, options ≤ 200; `free_text` default `true` also takes typed answers) and return at once; 0 turns. |
| `react(reaction, message_id?, remove?)` | Add or remove the ant's reaction on a message it reads (default: active message), made by `ants signer`; 0 turns, wakes nobody. |
| `list_reactions(message_id?)` | List reactions and reactors on a message the ant reads (default: active message), answered by `ants signer` or `ants webhook serve` with a `[[human]]` entry. |
| `list_ants(capability?)` | List peer ants from `ants_roster` with capabilities, `agent`, activity and hold states. |
| `route(capability, text, wait?, timeout?, attach?)` | Route to the best peer advertising `capability` (healthy > on a fallback > held, `live` > `stale`, `idle` > `working`, fewest messages); waits unless `wait: false`. |
| `post_notes(text, attach?)` / `read_notes(thread?)` | Append to (1 turn) or read (up to 50 posts, 0 turns) passive thread notes; wakes nobody. |
| `read_attachment(handle)` | Read a file attached to a message the ant reads, by the handle listed with it (`<from>.<inbox\|notes\|chan>.<target>.<id>`): PNG, JPEG, GIF or WebP as image content, text or JSON up to 128 KiB as text, any other saved into the working directory under its name (`name-2`… beside a different file) and its path returned; 0 turns. |
| `attach_to_reply(attach)` | Attach files from the working directory to the turn's final answer (the message, channel post or thread notes post sent when the turn ends), as `attach` does for `send_message`; a later call in the same turn replaces them, `[]` attaches none. Checked when called and read again as the answer is sent: a file that cannot be attached then leaves them all out and ends the answer with an `[ants]` line saying why; 0 turns. |
| `decide_approval(verdict)` | Answer an incoming chain approval notice (`allow_once`, `allow_always`, `reject`). |
| `post_channel(channel, text, attach?)` | Start a channel thread as `creator` (member or channel messenger only; inherits turns minus 1 for ant messages). |
| `manage_channel(channel?, add?, remove?, messenger?, clear_messenger?)` | List owned channels, inspect one, or modify members/messenger via `ants channel serve`. |
| `escalate(issue)` | Escalate the thread one level up its origin chain (or to the owner) with reason `asked`. |
| `suggest_thread_title(title)` | Suggest a title for the current thread (1–120 trimmed single-line chars); the turn's reply carries it, whether a message, a channel post or a thread notes post, and a human's `ants thread list` or `show` stores it only if the thread never had a title. |
| `request_ant(...)` / `retire_ant(name)` | Template managers only: add or retire a templated worker ant via `ants signer` ([docs/config.md](docs/config.md#manager-ants)). |

`attach` lists up to 8 files of up to 8 MiB each, by path relative to the directory the thread's session works in (the ant's `workdir`, or the thread's worktree): a path whose real location (symlinks and `..` resolved) leaves it, a non-regular file, or a file too big fails the call with nothing sent. Each file goes to the `ants_files` store under the message's handle, named by its base name, with a media type guessed from its extension; a file whose bytes are UTF-8 text has its secrets replaced with `[redacted]` first, as message text has ([Redaction](docs/config.md#redaction)). Replies, check_inbox and read_notes list a message's attachments with their handles.

Human traffic costs 0 turns; ant-to-ant messages cost 1 turn (`ask_ant` requires 1 turn left for the reply), and self-messaging is refused. A 3 s `ants_roster` lookup runs before messaging ants, suggesting closest names or capability peers when the target is unserved.

## Messenger and thread notes

- **Messenger:** unaddressed `ants ask` (from a human with no assistant, see [docs/auth.md](docs/auth.md#assistant-grant)) and unmentioned human/webhook channel posts route through `messenger`. Unless `messenger = false` or `[[ant]]` defines `messenger`, daemons run a built-in `messenger` (`~/.ants/messenger`, `~/.config/ants/auth/messenger.creds` if present) with automatic standby failover via `ants_roster` (~1 s on exit or 30 s TTL expiry), yielding immediately to an explicit `[[ant]] name = "messenger"`. It suggests a short title (`suggest_thread_title`) for a thread a human's request starts.
- **Thread notes:** `ants.notes.<thread>.<from>` provides a passive shared board per thread (wakes nobody). An ant's `post_notes` spends 1 turn via CAS from `min(message_budget, last_note_budget)`; exhausting turns escalates `notes` until a human runs `ants notes <thread> TEXT` (8 fresh turns) or escalation climbs a level.
- **Questions (`ask_human` / `ants answer`):** a question is a message to the human carrying `question` (text, numbered options, `free_text`); `ants inbox` and `ants thread show` print its options and how to answer it, and `thread show` lists the answers stored with it. `ants answer MESSAGE N|TEXT` finds the question among messages to the human, checks the option is one of its numbers (typed words only if `free_text`), and replies on the thread with the option's text (or the words) and `answer` (question id, option or none). An answer prompts the ant with an `[ants]` line naming the question and the option picked, or saying the answer is typed; an answer to no question the ant sent the human is an ordinary message. Only ants ask, so answers go to ants in the clear; a question from a human (no CLI sends one) is refused, pointing at `ants ask @HUMAN`. A stored question is told on the ant's status as `asked` (`ants hook run --on question`) and pushed by `ants webhook serve` to the human's sinks (`questions` per `[[human]]`, [docs/webhooks.md](docs/webhooks.md#notifications)).

## Threads, search, titles, done states and marks

`ants thread list`, `show` and `search` read `ANTS` with human credentials via batched direct gets (`$JS.API.DIRECT.GET.ANTS`, 256 messages/request), consuming and marking read nothing. Entry kinds: `message`, `escalation`, `mention`, `post`, `note`, `approval`, `decision`, `dead` (subject mismatches marked `(forged)`).

- **Search (`ants thread search`):** combines `--from`, `--to`, `--channel`, `--type`, `--grep` (case-insensitive), `--since` (inclusive, default 24 h before `--until`), `--until` (exclusive, default now; accepts durations, RFC 3339, or UTC `YYYY-MM-DD[ HH:MM]`), and `--limit` (default 50). `--from`, `--to` and `--type` prune scanned subjects server-side.
- **Titles (`ants thread title`):** 1–120 trimmed single-line chars stored at `title.<thread>` in `ants_threads`, shared across all humans and shown in `thread list`, `thread show` and tab completion. A title an ant suggests (`suggest_thread_title`, carried as `title` on its reply: an inbox message, channel post or notes post, ignored when forged) is stored by the next `ants thread list` or `thread show` of a human, by compare-and-set and only if `title.<thread>` has no entry, not even a cleared title's delete marker, with `by` naming the ant: a title a human set, renamed or cleared wins while its thread has messages. `thread show` marks it `(suggested by <ant>)`. On a thread holding sealed messages between two humans, `ants thread title` (only by one of them) seals the title for both humans' devices (`title` empty, `sealed` set), readers show it where the device opens it and `[sealed]` elsewhere, and no ant's suggestion is stored ([docs/auth.md](docs/auth.md#sealed-messages-between-humans)).
- **Done (`ants thread done|undone`):** one state per thread at `done.<thread>` in `ants_threads` (`ThreadDone {thread_id, by, at, after}`), shared across all humans: any human sets or clears it, ants neither read nor write it, and it changes no ant's work, reply or notification. `after` is the `ANTS` sequence of the thread's last message, channel post or note when marked; one stored above it reopens the thread at read time, without a write (tool requests, decisions and dead letters do not). `ants thread undone` leaves a delete marker with a 1 s per-message TTL. `thread list` shows `done` in STATE and `thread show` prints `done by <human> <time>`. Credentials issued before permissions version 2 lack the right: the CLI warns once and shows no done state.
- **Pins & archives (`ants thread pin|unpin|archive|unarchive`):** per-human CAS state at `marks.<human>` in `ants_threads` (mutually exclusive). Pinned threads sort last (nearest the prompt) and survive `--limit` truncation; archived threads hide from `ants thread list` and `ants dash` until new messages arrive (`archived+new`). Requires hub reachability.
- **Auto-archive (`auto_archive` in `[cli]`):** off by default; with e.g. `auto_archive = "14d"`, a thread neither pinned nor archived with no message, post, note or other entry stored for that long counts as archived (`auto-archived` in STATE, `auto_archived` in `--json`) and hides from `ants thread list` and `ants dash`, read-time only (nothing is written). Any new entry on it makes it active again. `ants thread list --archived` then reads all `ANTS` keeps and lists them with the archived ones; `ants thread unarchive` stores a `kept` time in `marks.<human>`, idling it again only after another `auto_archive` without activity; pinning or archiving drops it.
- **TL;DRs and digests (`ants thread tldr`, `ants digest`):** asked of `[cli]` `summary_ant` on threads of their own, hidden from thread lists, the inbox and push notifications; see [docs/config.md](docs/config.md#catch-up-summaries).
- **Pruning:** `ants_threads` never expires, so `ants thread list` forgets marks, titles, TL;DRs and done states of threads with no message left in `ANTS` (30 d retention). After reading back to a thread's start (UUIDv7 start minus 5 min clock slack; the whole stream for `--since 0` or non-time-ordered IDs) and finding none of its messages, the list drops the human's pin, archive or `kept` time placed more than 30 d ago (CAS, retrying on conflicting devices) and, when the read reached back 30 d or more, deletes titles, TL;DRs and done states set and the delete markers of those cleared more than 30 d ago, conditioned on their revision so a concurrently re-set one survives, leaving a delete marker with a 1 s per-message TTL, so no key remains. The 30 d grace covers messages still delayed in a leaf outbox. On the same CAS write it drops `kept` times of threads it read a message on stored at or after them, which no longer change when they idle. `--pinned`/`--archived` reads back to the earliest marked thread, so expired marks clear there; `--since 0` clears all expired titles.

## Dashboard and reactions

- **`ants chat [--since DUR]` (bare `ants` on a terminal):** full-screen chat UI (human creds). The header shows the human, the server and the connection; the nav lists home (count of what waits on the human), work (all threads, then a count per state: `waiting on me`, `working`, `waiting on ant`, `stuck`, `idle`, `done`), the human's channels (unread posts in bold with their count, `@N` unread @mentions), the ants (`●` live, `◐` busy, `◌` limited or on a fallback, `✕` signed out or failing, `○` stale) and the other humans (`●` online: a device records presence; `○` away; `·` not known) with the count of unread messages from each. Every view names a thread by the title a human set (`ants thread title` or the bar's `/title`, read from `ants_threads` every 10 s and opened where sealed, else `[sealed]`), else the title an ant suggested, else its opening line, as `ants thread list` does. Home's first line counts, without asking any ant, what changed since the previous `ants chat` on the device ended (kept in `chat-state.json` beside the config file, else the last 24 h): the threads finished, stuck and waiting on the human since then, and the ants' spend that UTC day; `s` opens the digest of that span in an overlay (`j`/`k` scroll, `s` asks again, any other key closes it), asked of `[cli]` `summary_ant` as `ants digest` does, and kept while the span holds. Home is the triage queue of what waits on the human, most pressing first and oldest first within a kind: pending tool requests (those waiting before those settled lately), unanswered questions to the human, stuck threads (an escalation to a human or a dead letter no human answered since), unread messages from other humans, unread mentions and finished threads (an ant answered on a thread the human owns, not done, with nothing running or queued). Each item shows its keys: a question shows as a card with its numbered options, `1`-`9` answers with that option and `r` opens the command bar on `/answer` for a typed answer where the question allows one, as `ants answer` does; `g` grants a stuck thread's ant fresh turns (a `Carry on` message, as `/grant` does), `r` replies and `f` follows up (the thread opens with the command bar), `x` cancels the prompt running there (as in a thread), `d` marks the thread done (and the human's inbox messages on it read, as marking it done anywhere does), `t` shows its delegation tree, `Enter` opens it, and `z` snoozes it from home on all the human's devices: the footer asks for how long (`1h` unless edited, at most `7d`), and the thread stays off home until then or until an item for the human arrives on it (`snoozed.<human>` in `ants_threads`; credentials issued before permissions version 2 lack the right, which `z` then names along with `ants renew`). `z` on a thread in work or a thread view snoozes it the same way, and on a thread a snooze holds off home wakes it back onto home; work marks such threads `snoozed`, as a thread's details do under HELD. A done state, a snooze and the order of home change no ant's work, reply or notification. Each tool request shows as a card, on home and at the bottom of its thread, with the ant, the tool and its input: `a`/`A`/`r` then `y` answers it `allow_once`/`allow_always`/`reject` as `ants tools approve`/`reject` do (on a thread, its oldest pending request; the first decision wins), `Enter` shows it in full as `ants dash` does, and a request settled while the UI runs keeps its card 2 min, saying how and by whom. A question to the human shows as a card at the bottom of its thread too, answered there by the same keys (`r` only while no tool request waits there), and keeps its card 2 min once answered, saying who answered and with what. Work lists the threads by state, then latest activity: a thread is `done` while its shared done state holds, else `stuck`, else `waiting on me` (a tool request, an unanswered question to the human, an unread message or finished work), else `working`, `waiting on ant` or `idle`; its columns are the ants, the owner, the `TURNS` left as the latest message carries, the `SPEND` since a human's last message (as `ants spend` reads it), the age and what happens now (`approval`, `question` or the ants' progress), and the selected thread's card shows its TL;DR with the ant that wrote it and its age (`stale` once newer messages came; `S` asks for a new one) and its delegation chain, who first messaged whom from the owner on, with each ant's spend; done states, TL;DRs and spend are read every 10 s. `--compact` draws two lines instead: the counts of home items by kind, the ants working and on what, the ants' spend that UTC day, then the top home item with its keys, which act on it as in home; `Enter` opens it in the full view, as replying does, and `q` quits. A channel shows its top-level posts newest first with their attachments, reply counts and reactions (`w` lists its threads in work), and showing it marks every post of the channel read on every device, also those arriving while it shows; `Enter` opens a thread, which marks the human's inbox messages and the channel posts on it read on every device, also those arriving while it is open; its messages, posts and notes render as markdown (headings, bold, italic, strikethrough, inline code, fenced code blocks dimmed without highlighting or reflow, nested lists, block quotes, links with their URL, simple tables), with control characters dropped. A thread shows its conversation as a timeline: messages with the ants' tool calls between them (kind, title, `ok`/`failed` with the time taken, or a spinner while running; earlier prompts' calls from the stored timeline, the running prompt's from status), a message from one ant to another nested under the sender with the receiver's calls and reply under it, questions with their options and answers, and under each message its attachments as `📎 name (size) type` (an invalid reference shows `unavailable` with why; those of a sealed message opened on this device list from its sealed body, sized as the file before sealing); with `inline_images = true` in `[cli]`, the thread's latest 12 PNG, JPEG, GIF and WebP images also draw under their lines, at most 48 by 12 cells and only while whole on screen, through the Kitty, iTerm2 or Sixel graphics protocol the terminal answers to at start, else as halfblocks (under tmux only with passthrough on); one that fails to fetch or decode, or does not fit the width, keeps its line alone. Over it shows the thread's TL;DR, its ant, age and whether it is stale, its first 3 lines; `S` asks for a new one as `ants thread tldr --new` does. With `[cli]` `summaries = "ahead"`, `ants chat` also asks for the TL;DR of each thread work lists whose TL;DR is missing or stale, one at a time, until the human's summaries spent `summary_daily_tokens` that UTC day. Beside it (from 96 columns) a panel shows the thread's state, owner, ants (with what each is doing), turns left, spend, age, origin (`ask by` a human, `#channel post`, `webhook` source; schedules send as webhooks), held approvals and the files touched. In a thread, `↑↓` selects a message, `y` copies it (OSC 52), `i` focuses the command bar to reply on the thread, `p`/`m`/`e`/`d` pin, mute (as `ants mute` without `--for`), archive or mark the thread done, each again to undo, `End` follows the latest, `o` opens an attachment of the selected message (following the latest: of the latest message with attachments), fetching it into a new private directory under the system's temporary one (one sealed opened as `ants attachment get` opens it) and opening it with the system's viewer (`open` on macOS, `wslview` under WSL, else `xdg-open`), `]` marks the next of its attachments (`◂` marks the one `o` opens), as in a channel for the selected post, and `t` shows the thread's delegation tree: the owner, then each ant messaged on the thread, on the threads spun off it or by a link passed on (`ants_links`), nested under the ant that messaged it first, each with the thread's title or its opening line, turns left, spend since a human's last message and state, then the totals of ants, turns used, spend and time elapsed (links and spend read again every 10 s); `Enter` opens a node's thread, and `x` then `y` cancels every prompt running in the selected node's subtree, as `ants cancel` does for each ant. In a thread, or on the thread selected in work, `s` opens the command bar on `/steer` for the ant at work there, and `x` or `Ctrl-X` cancels the prompt running there as `ants cancel` does (asking the ants at work, else every ant on the thread), each asking for `y` first while an ant there runs a tool call; the bar's status line then tells whether a prompt was interrupted or none was running. Under each message of a thread show its reactions, live, grouped by name with a count (an emoji for common names, else `:name:`; those of the human highlighted); `+` on the selected message opens a picker in the footer of common names and any valid name typed (`↑↓` chooses), which reacts as `ants react` does, or takes the reaction back when the human already reacted so. `/` opens the command bar on `/search`, whose words `from:`, `to:`, `channel:` (`@` and `#` optional), `type:` (comma-separated), `since:` and `until:` filter as the options of `ants thread search` do (the last 24 h by default), the other words are the text to find; `Enter` lists the latest 50 matches newest first with where, who, how long ago and the matching line, `Enter` on a match opens its thread with the match highlighted at the top, and `Esc` closes the list. An ant shows the ant view, the same as `ants ant show ANT`: where and how it runs as its roster entry tells (machine, harness and agent, model, reasoning effort and context, workdir, capabilities, escalation targets, harnesses, version, uptime and whoami), what holds it back (a signed-out or failing harness, a rate limit or quota, a fallback, memory pressure, a stale or missing entry), what it does now on each thread as `ants watch ANT` follows it, and its spend that UTC day; under them `←`/`→` switches between its threads, with the keys of work, the messages waiting for it as `ants queue ANT` lists them, asked every 10 s, where `Enter` opens a message's thread, and its memories as `ants ant memories ANT` shows them (read-only, asked when the pane first shows, again on `u`, with the chat's admin credentials); `a` opens the command bar on `@ANT`, `w` lists its threads in work and `r` restarts its harnesses as `ants ant restart` does (admin credentials), asking for `y` (after their prompts) or `!` (now, cancelling them) first. On the machine holding the account key (the team directory of `ants auth init`), `ants chat` also listens every 15 s for pending `ants add --enroll` requests and `ants renew` requests: home then names them in a yellow line under its first, an ant's view lists its own, and `c` on either opens the credentials page, which shows each request (name, host, leaf node, new key, age) with its keys: `a` approves an enrollment or signs a renewal, `r` rejects an enrollment, each asking for `y` first, as `ants enroll approve`/`reject`/`renew` do (a webhook's enrollment goes through `ants enroll approve --webhook`); a renewal's notice names the `ants auth revoke --key` that retires the earlier key. Elsewhere the page says the requests are answered on the machine holding the account key, as listing them takes it. A person shows the threads holding messages between the two humans, newest first with unread counts (`📎N`: the latest message carries N files), `Enter` opens one, and `n` writes a message that starts a new thread, as `ants ask @human` does. Sealed messages between two humans open with the device's key there, in threads and in search, marked with 🔒 as are their threads' titles; one sent before this device had a key reads `[sealed]`, one that does not open `[forged]` ([docs/auth.md](docs/auth.md#sealed-messages-between-humans)). Credentials issued before permissions version 2 show no one online, saying so once (`ants renew`). `Tab` moves between nav and view, `Esc` goes back, `R` refreshes, `?` shows the keys, `q`/`Ctrl-C` quits. Updates live from the stream, status events and the roster; records inbox and approval presence while open. Credentials issued before permissions version 2 lack the channel read markers: the nav then counts only the unread @mentions in the inbox, and nothing marks channel posts read, with one warning. Without a TTY, `ants chat` fails naming the scripting commands; without a reachable server (5 s) it points at `ants setup` and `ants login`.
- **The `ants chat` command bar** (under the view; `Ctrl-K`, or `@`/`#` typed in the view, focuses it, `Esc` leaves it keeping the text): `@ant TEXT` asks an ant on a new thread, which opens, and `#channel TEXT` posts to a channel; other text replies on the open thread (a post on a channel thread; else to the ant that last wrote, or with no ant there to the other human) or, with no thread open, asks the person of a DM view on a new thread, else the human's assistant while its grant stands, else the messenger (the result names it: `sent to pa (assistant of alice) on thread …`), and `//TEXT` sends a text starting with `/`. `Enter` sends, `Alt-Enter` adds a line, `Ctrl-E` edits the text in `$VISUAL`/`$EDITOR`, `Ctrl-K` lists the commands. Typing `@` lists the members of the channel on view (also on a channel thread), else the ants of the roster and the registered humans, `#` the channels and `/` at the start the commands with their arguments, narrowed as typed (prefixes first, then substrings, ignoring case): `Up`/`Down` select, `Tab` completes, as does `Enter` unless the word is already whole, and `Esc` closes the list. A slash command acts on the open thread, else the one selected: `/approve [ID] [--always]`, `/reject [ID]` (the thread's first pending tool request without an ID), `/grant [TEXT]` (a message to the stuck ant, starting its turns afresh), `/cancel`, `/steer TEXT` (as `ants ask --steer`, to the ant at work), `/answer N|TEXT`, `/done`, `/undone`, `/pin`, `/unpin`, `/mute [DURATION]`, `/unmute`, `/snooze [DURATION]` (as `z` without asking, `1h` unless given, at most `7d`), `/unsnooze`, `/archive`, `/unarchive`, `/title TEXT`, `/react NAME` (the selected post, else the latest message from another), `/goto THREAD|#channel|@user`, `/search QUERY` (as `/` opens it), `/tree` (as `t` in a thread), `/save [PATH]` (the attachment `o` would open, saved as `ants attachment get -o PATH` does, else under its name in the current directory), `/tldr` (a new TL;DR, as `S`), `/digest [SINCE]` (a digest of the threads with entries within `SINCE`, such as `2h`, `0` for all, else since the span home's first line counts; shown in an overlay as `s` on home shows it) and `/quit`. `/attach PATH` (paths complete) attaches a file to the next message the bar sends (text, `@ant`, `#channel`, `/steer` or `/grant`), within the limits of `--attach`, stored before the message as `ants ask --attach` stores it; the bar's title lists the files attached, and `/detach` drops them. A message to another human, from the bar or the DM view's `n`, is sealed as `ants ask @human` seals it, with the files `/attach` added, and `/title` on a thread between two humans whose messages are sealed seals the title as `ants thread title` does; a message that cannot be sealed is not sent, the error pointing at `ants ask --plaintext`. The result shows under the bar; a failed send puts its text and files back to edit.
- **`ants dash [--since DUR]`:** full-screen terminal UI (human creds) showing active threads (`▶` while busy, `done` after the users on a thread whose shared done state holds, read every 10 s from `ants_threads` as `ants thread list` marks it), selected thread live progress and history, 10 s roster refresh, and live pending tool requests (`Enter` opens details; `a`/`A`/`r` then `y` confirms `allow_once`/`allow_always`/`reject`; `Tab` switches panes, `R` refreshes, `q`/`Ctrl-C` quits). Records approval presence while open.
- **Reactions (`ants react MESSAGE REACTION [--remove]` / MCP `react`):** stores `<reactor>.<message_id>.<reaction>` in `ants_reactions` (30 d TTL; requires hub reachability). Humans write/delete only their own keys and read all reactions in `ants inbox`, `ants thread show` and `ants watch`, and follow them live in `ants inbox -f`, `ants ask -f` and `ants post -f`; reactions never end a wait or mark anything read. A human's `MESSAGE` may be a unique start of an id: matched first among messages to or from the human stored in the last 7 d, then among any stored then (narrowed by the time in a time-ordered id); several or no matches are refused with a list of candidates. Ant credentials need the whole id. Ant credentials neither read nor write `ants_reactions`: an ant reads the reactions to, and adds or removes its own on, only messages it reads (its own inbox, messages it sent, thread notes and channel posts), via MCP `list_reactions` and `react` (or `ants react` with its credentials). `ants signer` answers both on `_INBOX.<ant>.reactions` after finding the message's first copy in `ANTS` on such a subject, writing the reaction under the ant's key; each `[[human]]` connection of `ants webhook serve` also answers listings ([docs/webhooks.md](docs/webhooks.md#notifications)). A daemon without credentials (an open server) answers its ants itself, the same way.

## Channels

Channels (`ants.chan.<channel>.<from>`, metadata in `ants_channels`) group ants and humans:

- **Mentions & triage:** `@ant` wakes mentioned ant members; `@human` sends an inbox mention notice to mentioned human members; human/webhook posts with no mentions wake the channel messenger (`--messenger`, default `messenger`) for a 1-turn `@mention` triage reply; unmentioned agent posts wake nobody.
- **Ownership & budget:** human starters/posters or webhook `owner` set thread `owner`; ant starters set `creator` (propagated across spin-offs via `origin_thread` and `ants_links`). Ant posts share the thread's turn budget (minimum remaining across later posts), escalating `turn_budget` when exhausted.
- **Channel owners:** `--owner ANT` lets that ant call `manage_channel` via `ants.chanadm.<channel>.<ant>` to `ants channel serve` (`ants auth channel-manager` creds), which validates ownership/roster and announces changes on `ants.chan.<channel>.channel-manager`.
- **Delivery:** ants read `ants.chan.>` via durable consumer `chan-<ant>`, prepending up to 50 unseen thread posts (`<ant>.<thread>.seen` in `ants_sessions`).

## Escalations, dead letters and storage

### Escalations

Unresolved conditions (`turn_budget`, `cap`, `notes`, `dead_letter`, `context_window`, `asked`, plus single-hop `approval`) climb `escalate_to` (`ants_chains`; or `creator`'s chain on ant-created threads) before falling back to thread `owner`, else daemon `human`:

- All unvisited chain ants receive an `[ants]` notice and 8 fresh thread-note turns; only the first listed ant (leader) may answer on the thread or `escalate` further.
- Escalation climbs to the leader's `escalate_to` after `escalate_after` (`30m`; `"0"` disables) or if a member gets stuck, up to `max_escalation_hops` (default 3). Only a human message on the thread resets escalation to level 0.
- Human-bound escalations repeat an `[ants] Reminder:` after `escalation_reminder` (default `4h`, `"0"` disables) and then daily for 30 d until answered.
- **Status updates:** chain escalations, human escalations, reminders, and chain/expired tool approvals also emit `StatusEvent` (`type: "escalated"`) on `ants.status.<ant>.<thread>` (direct `harness`, `storage`, and `ants dead watch` notices do not; see [docs/hooks.md](docs/hooks.md#escalations)). Every settled tool request, whether by a human, a chain member's `decide_approval` or expiry, emits `type: "decided"` on the requester's status ([docs/hooks.md](docs/hooks.md#decisions)).

### Dead letters

Inbox and channel messages get 5 prompt attempts (`max_deliver = 6`; sign-out, provider-limit and startup-breaker holds, and first context-window session rebuilds, spend none). Failure on attempt 5 (or delivery 6 after a crash) writes a `DeadLetter` to `ants.dead.<ant>`, escalates `dead_letter`, and terminates delivery (`ants dead list | replay SEQ | drop SEQ | watch`). If NATS fails on delivery 6, `ants dead watch` and `ants inbox -f` convert `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ANTS.*` advisories into dead letters and notify the owner directly.

### JetStream storage

Every 5 m (first check at 30 s), one ant per daemon/server queries hub and local leaf JetStream usage, exporting `ants_jetstream_bytes` and warning the default human (`storage` escalation) once usage reaches `[storage]` `warn_percent` (default `80%`), naming the top 3 streams and fix. Shared KV lock `_storage.<domain>.<store>.warned` in `ants_sessions` deduplicates warnings across daemons until usage drops > 5 percentage points below `warn_percent`. Hub limits come from `[storage]` `max_file_store` / `max_memory_store` (unless set on the NATS account); leaf limits default to `1 GB` file / `64 MB` memory.

## Webhooks

`ants webhook serve` (`~/.ants/webhooks.toml`) authenticates incoming HTTP `POST <path>/<webhook>` requests (`hmac-sha256`, `bearer`, `token`, `standard-webhooks` such as GitLab's signing token), deduplicates events via `ants_webhooks`, and starts narrowed inbox or channel threads (`ants webhook add` provisions credentials, secrets and presets for GitHub, GitLab and Grafana, or takes a secret given, such as GitLab's signing token). `[[human]]` entries in the same file push tool approvals (with signed `/_approval` buttons), escalations, ants' questions (`ask_human`), unserved-ant notices and optional completions to `ntfy`, `slack`, `telegram`, `exec` and `http` sinks, respecting per-device presence, quiet hours, snoozes and muted threads, with an optional digest of what quiet hours or a snooze held back once they end. While a human is away (held by quiet hours or a snooze, no device present), its tool requests and escalations are relayed to an optional stand-in (`stand_in` per `[[human]]` or `[[ant]]`), whose answers the human is then told of. See [docs/webhooks.md](docs/webhooks.md) and its [stand-in](docs/webhooks.md#stand-in) section.

## Credentials

`ants auth` manages operator, account and system NKeys (`~/.config/ants/auth`), role-scoped `.creds` (ants, humans with per-device IDs and encryption keys, admins (`--admin`, `ants auth admin list` / `drop`), assistants of humans (`--assistant-of`), webhooks, observers, `channel-manager`, leaf uplinks, machine hosts), account revocations (`ants auth revoke` / `push`), and passphrase-encrypted `age` backups (`ants auth backup` / `restore`). `ants auth bootstrap` and `ants login` let humans register their own devices after a GitHub sign-in, checked by `ants signer`. `ants add`, `ants renew` / `ants enroll`, and `ants keys store` handle local key generation, over-NATS enrollment/renewal, and OS credential stores (macOS Keychain, Linux Secret Service, Windows Credential Manager). See [docs/auth.md](docs/auth.md).

## Leaf nodes

Running a local `nats-server` leaf per machine (`127.0.0.1:4222`, `--js-domain hub`) isolates local processes and buffers offline traffic in `ANTS_OUT`, draining through hub `ANTS_IN` on reconnect:

```sh
ants auth leaf macbook --ant dev-a --ant messenger --human alice --js-domain hub   # auth machine
ants leaf config macbook --uplink wss://hub.example/nats                           # worker: ~/.config/ants/leaf.conf
nats-server -c ~/.config/ants/leaf.conf &
ants leaf outbox macbook                                                           # configure store-and-forward
```

| Leaf uplink | Use case |
|---|---|
| `wss://host/path` | TLS WebSocket uplink behind a reverse proxy (supports `--proxy` HTTP `CONNECT`). |
| `tls://host:7422` | Direct TLS connection to the hub leafnode port. |
| `nats-leaf://127.0.0.1:7422` via `devtunnel connect` | Private Dev Tunnel forwarding `:7422` (`--devtunnel` / `--devtunnel-host` in `install-units.sh`). |
| `nats-leaf://127.0.0.1:7422` via `ants bridge connect` | Plain HTTPS `POST`/long-polled `GET` tunnel when proxies block WebSocket (`--bridge` / `--bridge-serve`). |

Full hub, reverse proxy, revocation sync, leaf, store-and-forward, HTTPS bridge and Dev Tunnel details live in [docs/transport.md](docs/transport.md).

## Services

`ants setup` installs single-machine `nats` and `daemon` units. For multi-machine hubs, workers and tunnels, `scripts/install-units.sh` renders and starts `contrib/launchd/` (`~/Library/LaunchAgents`) or `contrib/systemd/` (`~/.config/systemd/user`) units:

```sh
scripts/install-units.sh                                                                    # nats + daemon
scripts/install-units.sh --devtunnel ants-hub.usw2                                          # + devtunnel connect
scripts/install-units.sh --no-daemon --channel-manager ~/.config/ants/auth/channel-manager.creds --webhook --signer
scripts/install-units.sh --bridge https://hub.example/bridge --bridge-token ~/.config/ants/bridge.token
scripts/install-units.sh --out ./rendered-units                                             # render only
```

Options: `--ants PATH`, `--nats-server PATH`, `--nats-conf FILE` (default `~/.config/ants/leaf.conf`), `--config FILE` (default `~/.ants/config.toml`), `--no-daemon`, `--channel-manager CREDS`, `--signer`, `--webhook`, `--webhook-config FILE` (default `~/.ants/webhooks.toml`), `--devtunnel TUNNEL`, `--devtunnel-host TUNNEL`, `--devtunnel-bin PATH`, `--devtunnel-token FILE`, `--bridge URL`, `--bridge-serve`, `--bridge-token FILE`, `--bridge-proxy URL`, `--bridge-ca FILE` (`SSL_CERT_FILE`), `--out DIR`. `--devtunnel` and `--bridge` are mutually exclusive (`127.0.0.1:7422`). Installs shell completions unless `--out` is set.

| Service | macOS (`~/Library/Logs/ants/`) | Linux & WSL (`systemd --user`) | Installed when |
|---|---|---|---|
| NATS (`nats-server`) | `ants.nats` (`nats.log`) | `ants-nats.service` | Always |
| Ant daemon (`ants daemon`) | `ants.daemon` (`daemon.log`) | `ants-daemon.service` | Default (omit with `--no-daemon`) |
| Channel manager (`ants channel serve`) | `ants.channels` (`channels.log`) | `ants-channels.service` | `--channel-manager CREDS` |
| Webhook ingress (`ants webhook serve`) | `ants.webhook` (`webhook.log`) | `ants-webhook.service` | `--webhook` |
| Signer (`ants signer`) | `ants.signer` (`signer.log`) | `ants-signer.service` | `--signer` |
| Dev Tunnel (`devtunnel connect` / `host`) | `ants.devtunnel[-host]` | `ants-devtunnel[-host].service` | `--devtunnel` / `--devtunnel-host` |
| HTTPS bridge (`ants bridge connect` / `serve`) | `ants.bridge[-serve]` | `ants-bridge[-serve].service` | `--bridge URL` / `--bridge-serve` |
| Log rotation (macOS) | `ants.logrotate` (`logrotate.log`) | — (`journald` rotates) | Always on macOS |

- **Reload & restart:** `ants.daemon` and `ants.webhook` auto-reload edited config files (~6 s) and accept `SIGHUP` (`launchctl kill SIGHUP gui/$(id -u)/<label>`, `systemctl --user reload <unit>`); `ants add`, `ants webhook add`, `ants renew`, `ants leaf config`, `request_ant` and `ants tools approve --rule` reload or restart matching running units automatically. For renewed leaf uplink credentials (`ants add --enroll`, `ants renew`, `request_ant`), `ants.nats` gets a reload that reconnects only the uplink, keeping local connections.
- **Logs & linger:** on macOS, `ants.logrotate` copy-truncates logs > 10 MiB hourly (keeping `.1`..`.5.gz`). On Linux, enable `loginctl enable-linger $USER` (`[boot] systemd=true` in `/etc/wsl.conf` on WSL 2). Remove units with `launchctl bootout gui/$(id -u)/<label>` or `systemctl --user disable --now <unit>`.

## Monitoring

`ants daemon`, `ants webhook serve`, `ants bridge serve` and `ants bridge connect` expose unauthenticated loopback HTTP health and Prometheus metrics when `--metrics ADDR` (`ANTS_METRICS` on daemon) or config `metrics` is set (read at startup):

- `GET /healthz`: `200 ok` when healthy (daemon: ≥ 1 ant served and all NATS connections up; webhook: all webhook and human connections up; bridge: running), else `503`.
- `GET /metrics`: Prometheus text format (`text/plain; version=0.0.4`).

| Metric | Type | Labels | Description |
|---|---|---|---|
| `ants_nats_connected` | gauge | `ant` / `user` | NATS connection state (`1` up, `0` down) |
| `ants_harnesses` | gauge | `ant` | Live harness processes |
| `ants_memory_low` | gauge | `ant` | `1` while memory is low and the ant runs fewer harnesses ([memory pressure](docs/config.md#memory-pressure)), else `0` |
| `ants_memory_available_bytes` | gauge | | System memory available at the last pool check (absent until read) |
| `ants_messages_in_hand`, `ants_prompts_in_flight` | gauge | `ant` | Messages held by daemon; active harness prompts |
| `ants_prompts_total`, `ants_retries_total`, `ants_dead_letters_total` | counter | `ant` | Prompts started, redelivery retries, dead letters |
| `ants_spend_tokens_total`, `ants_spend_cost_total` | counter | `ant` | Reported tokens and cost ([docs/config.md](docs/config.md#spend-report)) |
| `ants_jetstream_bytes`, `ants_jetstream_limit_bytes` | gauge | `domain`, `store` | Account JetStream storage used and limit ([JetStream storage](#jetstream-storage)) |
| `ants_webhook_requests_in_flight` | gauge | | Active HTTP webhook requests |
| `ants_webhook_events_total` | counter | `webhook`, `outcome` | `delivered`, `duplicate`, `refused` (4xx), `failed` (503) |
| `ants_bridge_sessions`, `ants_bridge_sessions_total` | gauge, counter | | Open and cumulative bridge sessions |
| `ants_bridge_failures_total` | counter | | Upstream/session failures |
| `ants_bridge_bytes_total` | counter | `direction` (`up`, `down`) | Bridge bytes transferred |

## Hooks

`ants hook run --on EVENT [--concurrency N] [--timeout DUR] -- CMD [ARGS...]` runs `CMD` once per `approval-request`, `approval-decision`, `dead-letter`, `escalation`, `question`, `decided`, `status` or `roster` event with its JSON document on stdin (`--observer` credentials; default concurrency `4`, timeout `60s`). Stored feeds resume from durable consumers. See [docs/hooks.md](docs/hooks.md).

## Footprint

`scripts/footprint.sh` starts a throwaway `nats-server -js` and `ants daemon`, samples RSS via `ps`, and fails at or above `--max-mb` (default `20` MB; `--ask TEXT` measures a live session; `--metrics ADDR` scrapes `/healthz` and `/metrics`):

| Scenario (`ants` 0.1.0 release, `nats-server` 2.15.3) | Linux x86_64 | macOS arm64 |
|---|---|---|
| Idle daemon | 5.9 MB | 8.5 MB |
| Idle daemon with MCP server / one Copilot session | — | 10.5 MB |
| Copilot CLI harness processes | — | 255 MB |

Harness processes dominate memory use. Below `[memory_pressure]` `min_available` (default `1GB`), each ant stops its idle harnesses at once and runs at most one until memory recovers ([docs/config.md](docs/config.md#memory-pressure)).

## Development

```sh
cargo build --release
scripts/check.sh        # the checks of CI's check and deny jobs, below
cargo fmt --all --check
CARGO_BUILD_WARNINGS=deny cargo clippy --all-targets
cargo test
cargo deny check        # licenses, advisories, bans, sources (deny.toml)
```

- **Local CI:** `scripts/check.sh` runs formatting, lints, both test passes and `cargo deny --locked check` with `CARGO_BUILD_WARNINGS=deny`, stopping at the first failure; without `nats-server` or cargo-deny it skips what needs them and lists the skips.
- **Toolchain:** `rust-toolchain.toml` pins Rust 1.100 (rustfmt, clippy), installed by rustup on the first `cargo` command; a bump changes it, `rust-version` in `Cargo.toml` and every `toolchain:` in `.github/workflows/ci.yml` together.
- **Warnings & Windows:** CI sets `CARGO_BUILD_WARNINGS=deny` and cross-checks `windows-latest` (`rustup target add x86_64-pc-windows-gnu` + mingw-w64 locally).
- **Dependency cooldown:** `.cargo/config.toml` sets a 7-day `registry.global-min-publish-age` (`CARGO_RESOLVER_INCOMPATIBLE_PUBLISH_AGE=allow cargo update -p CRATE --precise VERSION` overrides for security fixes).
- **Tests:** NATS integration tests skip without `nats-server` on `PATH` unless `ANTS_REQUIRE_TOOLS=1`; CI runs `cargo test` both with default acks and with `ANTS_TEST_JS_ACK_FC_V2=1`.
- **Wire snapshots:** `crates/ants-proto/tests/wire_snapshots.rs` pins the JSON form of every wire type in [insta](https://insta.rs) snapshots, so a renamed field or variant fails `cargo test`; after an intended wire change, review them with `cargo insta review` (or `INSTA_UPDATE=always`): a changed snapshot is a wire change, which follows the [versioning rules](docs/hooks.md#versioning).

## Layout

| Path | Purpose |
|---|---|
| `crates/ants-proto` | Wire contract: IDs, subjects, envelopes, turn budgets, NATS permissions (no I/O) |
| `crates/ants-daemon` | NATS/JetStream bus, ACP harness pool, delivery loop, approvals, escalations, MCP server |
| `crates/ants` | `ants` binary: CLI, `daemon`, `signer`, `auth`, `enroll`, `leaf`, `bridge`, `channel`, `webhook`, `hook`, `mcp` |
| `crates/ants-test-support` | Dev-only test helpers: `PATH` checks and throwaway `nats-server` |
| `contrib/` | `launchd` plists, `systemd` user units and CLI agent skill (`contrib/skills/ants/SKILL.md`) |
| `scripts/` | `check.sh` (CI checks run locally), `install-units.sh` (service installer) and `footprint.sh` (RSS budget check) |
| `docs/config.md` | Daemon config, `[[ant_template]]`, manager ants & signer, harnesses, caps, tool permissions, redaction, schedules |
| `docs/auth.md` | NATS credentials, revocation, age backups, devices, over-NATS enrollment/renewal, permissions versions, OS keystores, wire ACLs |
| `docs/webhooks.md` | HTTP webhook ingress, `ants webhook add` presets, human notification sinks, quiet hours/snooze/mute/digest, stand-ins, `/_approval` links, JSON notice v1 |
| `docs/transport.md` | Hub, reverse proxy, revocation sync, leaf nodes, store-and-forward, HTTPS bridge, Dev Tunnels, keepalive |
| `docs/hooks.md` | Hook contract: observer credentials, feed subjects and JSON schemas, versioning |

Work items and specifications live in `bd` (`.bd/`): `bd ready`, `bd spec list`, `bd spec show <area>`. The database stays local; `.bd/issues.jsonl` is a committed snapshot (`bd export -o .bd/issues.jsonl`), loaded into a fresh workspace with `bd init --prefix ants` and `bd import .bd/issues.jsonl`.

## Security

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
