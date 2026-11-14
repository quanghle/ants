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
| nats-server (leaf, outbox)  --+----->|      ants_webhooks       |<-----+-- nats-server (leaf)     |
+-------------------------------+      +--------------------------+      +--------------------------+
                        uplink: wss://, tls://, Dev Tunnel, or ants bridge
```

- **Hub:** one `nats-server` with JetStream holding stream `ANTS` and eleven KV buckets (`ants init`). With `ants auth`, every ant, human, webhook, leaf uplink and channel manager connects using role-scoped NKey/JWT credentials.
- **Leaf:** optional per-machine `nats-server` (`ants leaf config`) on `127.0.0.1:4222` with a store-and-forward outbox (`ants leaf outbox`). Local `ants` processes target `127.0.0.1:4222`; only the leaf uplink crosses the network. Single-machine setups point `ants` straight at the hub.
- **Daemon:** `ants daemon` serves ants over per-ant NATS connections and durable consumers, runs up to `max_harnesses` ACP processes per ant, enforces tool allowlists, limits and spending caps, refreshes roster heartbeats and exposes an MCP server to each session.
- **Ants and humans:** ants are named agents (`dev-a`, `reviewer`, `messenger`) with a workdir, harness, capabilities and optional `whoami`. Humans (`human` by default, or named users from `ants auth user NAME --human`) own threads they start and approve tools. Unaddressed `ants ask` requests go to `messenger` for capability routing. Agent-to-agent messages share a per-thread turn budget (default 8).

| Store / subject | Storage / TTL | Purpose |
|---|---|---|
| Stream `ANTS` | File, 30d | Captures `ants.inbox.>`, `ants.notes.>`, `ants.approvals.>`, `ants.dead.>`, `ants.chan.>`; enables direct gets; republishes `ants.inbox.*.*` to `_INBOX.<to>.stored.<from>`. |
| KV `ants_roster` | Memory, 30s | Live `RosterEntry` per ant (`<ant>`), refreshed every 10s. |
| KV `ants_sessions` | File, 30d | Thread ACP session IDs (`<ant>.<thread>`), spend/escalation state, channel seen cursors, mention markers. |
| KV `ants_attempts` | File, 30d | Started delivery markers and saved replies (`<ant>.<seq>`) before consumer ack. |
| KV `ants_channels` | File, none | `ChannelEntry` (`<channel>`): members, optional messenger, optional owner ant. |
| KV `ants_humans` | File, none | Registered humans (`<human>`), written automatically on CLI connection. |
| KV `ants_chains` | File, none | Persistent `escalate_to` chains (`<ant>`), readable after roster expiry. |
| KV `ants_links` | File, 30d | Verified `origin_thread` handoffs (`<from>.<thread>.<to>`), from spin-offs and from messages on a thread with a `creator` or `owner`. |
| KV `ants_owners` | File, 30d | Thread `owner` (`<thread>.<recipient>`) recorded by a human (`ants ask`) or webhook for each ant it messages on the thread; the first record of a key stays: a second is refused, or dropped as a repeat of its `Nats-Msg-Id` (no delete or purge). |
| KV `ants_read` | File, none | Each human's read marker (`<human>`): the `ANTS` sequences of its inbox messages read on any of its devices (`below`, plus a `read` set above it); each human reads and writes only its own key. |
| KV `ants_presence` | Memory, 30s | Devices following (`<human>.<device>.<inbox|approvals>`), put every 10s while `ants inbox -f` or `ants tools list -f` runs on that device and deleted when it stops; each human reads by direct get and writes only its own keys. |
| KV `ants_webhooks` | File, 7d | Delivered webhook event IDs (`<webhook>.<hash>`), sent schedule firings (`<schedule>.<unix time>`), hourly `<webhook>.webhook` markers of webhooks and schedules, and the channel manager's hourly `channel-manager.serve`, each holding the version of ants it runs. |
| Core NATS | Unstored | `ants.status.<ant>.<thread>` (progress), `ants.cancel.<ant>.<thread>` and `ants.cancelled.<ant>.<thread>` (`ants cancel` and the daemons' answers), `ants.chanadm.<channel>.<ant>` (owner ops), `ants.enroll.<ant>[.reply]` (enrollment, carried by leaf uplinks). |

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

### Upgrade

Running `cargo install` again replaces `~/.cargo/bin/ants`; the services move onto the new executable by themselves. `ants daemon`, `ants channel serve`, `ants webhook serve`, `ants bridge serve` and `ants bridge connect` check every 30s whether another file replaced their executable at the path they started from (or the file was rewritten in place). Once the new file is executable, non-empty and unchanged across two checks, the process exits with status 75 for the service manager to start the new one: `ants daemon` first stops its ants as on `SIGTERM` (prompts in flight finish, waiting messages go back for redelivery) and its schedules at once; the others exit at once, as on `SIGTERM`. The units of `scripts/install-units.sh` restart on status 75 (systemd `RestartForceExitStatus=75`; launchd `KeepAlive`, as on any failed exit); systemd units installed before also restart on it, as on a failure. A process started by hand logs the reason and exits: start it again. `--upgrade-check DURATION` (`ANTS_UPGRADE_CHECK`) on those commands sets the period; `0` turns the check off.

## Quick start

Single machine without credentials:

```sh
nats-server -js &                                   # local hub
ants harness check                                  # ACP handshake with Copilot CLI
ants daemon --ant dev-a --workdir ~/src/some-repo &
ants ask --wait @dev-a "summarise the open TODOs"
ants inbox                                          # unread replies
ants roster                                         # active ants
ants tools list                                     # pending tool approval requests
ants notes <thread>                                 # passive thread notes
ants thread list                                    # threads active in the last 24h
ants thread show <thread>                           # one thread's whole history
ants channel create ops --member dev-a,dev-b
ants post '#ops' "@dev-a the build is red"          # wakes dev-a; prints thread ID
ants channel read ops
ants watch                                          # live traffic on ants.>
ants watch dev-a [<thread>]                         # streamed harness state, text, tools, plan
ants cancel dev-a <thread>                          # stop the prompt dev-a runs on the thread
```

An empty `~/.ants/config.toml` (or `harness = "claude"`) lets `ants daemon` with no flags serve the built-in `messenger` alone (see [Messenger](#messenger)). Multi-machine deployments add [Credentials](#credentials), [Leaf nodes](#leaf-nodes), a [Config file](#config-file) and [Services](#services).

## CLI

| Command | Description |
|---|---|
| `ants ask [--thread THREAD] [--wait \| -f] [--timeout DUR] [@ant] TEXT` | Send to `@ant` (`messenger` if omitted) and print the thread ID. `--thread` continues an existing thread. Warns on stderr, but sends anyway, when no daemon serves the ant now (not in `ants_roster`, stale, or its harness signed out), hinting at a misspelt name when `ants_chains` keeps no chain for it either. |
| `ants inbox [--peek] [--thread THREAD] [--limit N] [-f] [--json]` | Print unread replies and mark read on every device of the human (`--peek` keeps unread; `-f`/`--follow` streams new replies, on every device following, and watches advisories; without `--thread` and `--json`, it also prints pending tool requests with the commands answering them, new ones, and how each ends (answered, by whom if an agent, or expired); without `--thread`, the device counts as reading its escalations, and without `--json` its tool requests too, see [Notifications](#notifications)). |
| `ants roster [--json]` | List ants (`live`/`stale`, `idle`/`working`/`signed-out`), the ants and permissions versions of their daemons, harnesses, capabilities, `escalate_to` chains and workdirs; below the table, a note when live daemons run mixed versions, and one line per signed-out harness with its sign-in command and machine. |
| `ants spend [ANT] [--thread THREAD \| --days N] [--json] [--config FILE]` | Report tokens and cost spent (see [Spend report](#spend-report)). |
| `ants notes THREAD [TEXT]` | Show thread notes, or post `TEXT` as the CLI human (resets budget to 8 turns; wakes nobody). |
| `ants post [--thread THREAD] [--wait \| -f] [--timeout DUR] CHANNEL TEXT` | Post to `ops` or `'#ops'` and print the thread ID (`@ant` wakes members; `@human` notifies members; no mention triages). |
| `ants channel create \| set \| add \| remove \| list \| delete` | Manage channels: `--member USER,...`, `--messenger ANT` (`--default-messenger` on `set`), `--owner ANT` (`--no-owner` on `set`). Warns about ants named that no daemon serves now, as `ants ask` does. |
| `ants channel read CHANNEL [--thread THREAD \| --limit N]` | Print channel posts grouped by thread. |
| `ants channel serve` | Run the channel manager (`ants auth channel-manager` creds) to apply owner `manage_channel` requests; it puts the version of ants it runs at `channel-manager.serve` in `ants_webhooks` hourly. |
| `ants thread list [--ant USER] [--owner HUMAN] [--grep TEXT] [--since DUR] [--limit N] [--json]` | List threads with messages stored within `--since` (default `24h`, `0` for all kept): id, owner, users and channels, first/last time, message count, opening line; most recently active last, up to `--limit` (default 50). `--grep` keeps threads with a message containing `TEXT` (case-insensitive) and prints the matching line. |
| `ants thread show THREAD [--since DUR] [--json]` | Print a thread's history in stored order: inbox messages both ways, channel posts, notes, escalations, tool requests and decisions, dead letters (see [Thread overview](#thread-overview)). |
| `ants watch [ANT [THREAD] \| SUBJECT]` | Stream traffic (default `ants.>`), escalations and decisions, or live harness progress for `ANT`. |
| `ants cancel ANT THREAD` | Stop the prompt `ANT` runs on `THREAD` (see [Prompt limits](#prompt-limits-and-spending-caps)); fails when no daemon of `ANT` runs one there or none answers within 5s. Requires human credentials. |
| `ants tools list [-f] [--json]` | List pending tool requests (`<ant>/<id>`, thread, tool, files, options, time remaining; `-f` streams them, and the device counts as reading tool requests, see [Notifications](#notifications)). |
| `ants tools show ID [--json]` | Show full request, tool input, status and answer commands for `<ant>/<id>` (unique prefix accepted). |
| `ants tools approve ID [--always] [--rule [--config FILE]]` / `reject ID` | Allow once, allow for the session (`--always`, if offered), or reject a tool request; `--rule` also adds the allow rule fitting the request exactly to the ant's `allow` in the config file and reloads the daemon. |
| `ants dead list` / `replay SEQ` / `drop SEQ` / `watch` | Inspect, replay or delete dead letters; `watch` turns `MAX_DELIVERIES` advisories into dead letters. |
| `ants init` | Create or update stream `ANTS` and all eleven KV buckets (requires human credentials; other commands with human credentials do the same on connecting). |
| `ants daemon [--ant NAME] [--config FILE] [--metrics ADDR] [ant-flags...]` | Serve config-file ants, or one ant selected/defined by `--ant` (`ANTS_ANT`); `--metrics` serves health and metrics (see [Monitoring](#monitoring)). |
| `ants add NAME [--config FILE] [--out FILE] [--jwt JWT \| --enroll CREDS [--hub URL] [--leaf-config FILE]] [ant-flags...]` | Add an ant to `config.toml` and generate its user NKey locally (see [Adding an ant](#adding-an-ant)). |
| `ants enroll list` / `approve NAME` / `reject NAME [--reason TEXT]` | List, sign or refuse `ants add --enroll` requests with the account key (`--public-key KEY` disambiguates; approval through a leaf node also renews its uplink credentials and outbox admission). |
| `ants enroll renewals` / `renew NAME [--public-key KEY]` / `renew --auto` | List the `ants renew` requests heard, renew the credentials `NAME` asks for, or keep renewing every valid, unrevoked request as it arrives until stopped (see [Renewing over NATS](#renewing-over-nats)). |
| `ants renew [FILE] [--config FILE] [--leaf-config FILE]` | Renew the credentials of `FILE` (default `--creds`) over NATS for a key generated here, wait for `ants enroll renew`, replace the file and reload the daemon or restart the leaf node using it; a leaf uplink's request goes over a human's `--creds`. |
| `ants keys store` / `unstore` / `remove [FILE...]` | Move the NKey seed of each credentials file (default `--creds`) into the OS credential store once it authenticates at `--nats`, back into the file, or delete the file and its stored seed (see [Private keys in the OS credential store](#private-keys-in-the-os-credential-store)). |
| `ants harness check` / `prompt TEXT` | Check the harness is installed and signed in (ACP handshake and `session/new` in the current directory; names the sign-in command when signed out) or run one prompt without NATS (`--harness NAME`, `--config FILE`; rejects tools). |
| `ants doctor [--config FILE] [--leaf-config FILE] [--dir DIR] [--no-harness] [--json]` | Check the machine's setup end to end and print the command fixing each problem; exits with status 1 when a check fails (see [Doctor](#doctor)). |
| `ants auth init` / `user NAME` / `enroll NAME` / `channel-manager` / `leaf MACHINE` | Create team keys and `server.conf`, or issue `.creds` (`user`: `--human`, `--webhook --to ANT --channel CH`, `--label LABEL` for a human's device; `enroll`: `--ttl DUR`, default `1h`, `0` for no expiry; `leaf`: `--ant`, `--human`, `--webhook` name the machine's members, `--add-*`/`--remove-*` change those of the last run, which a run without them reissues, `--enroll-approver` (`--no-enroll-approver`) for the account-key machine, and writes the leaf's system account credentials beside the uplink's). |
| `ants auth revoke NAME` / `revoked` / `push` | Revoke every key issued to `NAME` (or `--key U...`, or only the human's `--device DEVICE`), rewrite `server.conf` and push the account to the `--nats` server and the leaf nodes joined to its system account (`--no-push` to skip); list revoked keys; push the account again. |
| `ants auth issued [--json]` | List the keys issued (key, name, kind, issue time, expiry, permissions version, revoked or reissued), with the command renewing those in use that are outdated or expire within 7 days. |
| `ants auth devices HUMAN [--json]` / `rename HUMAN DEVICE LABEL` | List a human's devices (id, label, key, issue time, revoked); relabel one. `DEVICE` is an id, a key, or a label no other device of the human has. |
| `ants leaf config MACHINE --uplink URL ...` / `outbox MACHINE ...` | Write `~/.config/ants/leaf.conf`; configure or `--remove` store-and-forward outbox sourcing (`outbox`: members from `--ant`/`--human`, or those the uplink credentials of `--leaf-config FILE` carry, changed by `--add-*`/`--remove-*`). |
| `ants bridge serve` / `connect URL` | Tunnel a leaf uplink over plain HTTPS `POST`/`GET` requests (`--token-file FILE` reads the bearer token from a file). |
| `ants webhook serve [--config FILE] [--listen ADDR]` | Serve HTTP webhook ingress, and push tool requests and escalations to humans' ntfy, Slack or Telegram with approve/reject links (see [Webhooks](#webhooks) and [Notifications](#notifications)). |
| `ants mcp --socket PATH --token TOK` | Internal stdio-to-socket MCP bridge launched by `ants daemon`. |

`THREAD` in `ask`, `inbox`, `post`, `notes`, `thread show`, `cancel`, `watch`, `spend --thread` and `channel read --thread` is a whole thread ID (a UUID, used without any lookup), a unique start of one, or `last`. A start matches the threads with messages the CLI human sent or received (inbox both ways, own channel and notes posts) stored in the last 7 days; when none of those starts with it, every thread with a message stored in the last 7 days, such as one between ants. For a start of a time-ordered ID (as generated ones are), that second lookup reads only the messages stored while such an ID was made, give or take 5 minutes of clock skew and up to 10 minutes after, so a longer start reads less. An exact ID among the threads matched wins; a start matching several is refused with them listed (ID, latest message time, route and first line), and one matching none with the human's own threads listed. `last` is the thread of the latest message the human sent or received, however old. Both read the stream by message gets (see `ants thread` below) and need human credentials.

### Identity and human registration

Every command accepts `--nats`, `--creds` and `--js-domain`. Where neither the flag nor its environment variable sets one, it comes from the config file (`ANTS_CONFIG`, default `~/.ants/config.toml`, read only if it exists): `[cli]` `nats`, `creds` and `js_domain`, else the top-level `nats` and `js_domain`, else the built-in default; an unreadable file or a malformed `[cli]`, `nats` or `js_domain` fails the command. `ants daemon` and `ants add` take none of these, as their flags describe ants and they read the file's top-level keys themselves. A NATS URL carrying a user and password or a token (`nats://user:pass@host`), on the command line or in a config file, is refused: credentials come only from `.creds` files. The CLI acts as the user named in the `--creds` JWT, or `human` without credentials. With human credentials (`ants auth user NAME --human`, or `human`), connecting ensures `ants_humans` exists and registers `<name>` (direct get first, put if absent; `human` needs no entry; warns and continues on failure, though daemons treat unregistered senders as agents until registered), and the first connection also brings `ANTS` and the KV buckets up to date as `ants init` does (see [Credentials](#credentials)). With ant credentials, the CLI acts as that ant, skips `ants_humans`, warns, refuses `ants inbox` and leaves replies unread during `ants ask --wait` or `--follow`.

### Doctor

`ants doctor` checks the machine's setup end to end and changes nothing. It prints one line per check, `ok`, `warn`, `fail` or `skip`, with a short detail, and under each warning or failure the command fixing it (`--json`: one object per check, with `check`, `status`, `detail` and `fix`). It exits with status 1 when a check fails. In order:

- **config**: the config file (`--config`, `ANTS_CONFIG`, default `~/.ants/config.toml`) loads as `ants daemon` loads it.
- **credentials**: the CLI's `--creds` (or `[cli]` `creds`): holder, permissions version and expiry, as `ants auth issued` reports them, with the renewing command.
- **server** and **jetstream**: the `--nats` server accepts the credentials, and JetStream answers in the `--js-domain` they were issued for; through a leaf node, the hub's JetStream rather than the leaf's own.
- **hub**: stream `ANTS` and the KV buckets exist and match this version (fix: `ants init`); skipped with an ant's credentials, which get no stream info.
- **versions**: the ants and permissions versions every live daemon, `ants webhook serve`, schedule and `ants channel serve` publishes, and whether renewing credentials with this ants suits them all (see [Credentials across upgrades](#credentials-across-upgrades)): a warning names the components on an older permissions version, or publishing none (fix: upgrade and restart them before renewing), or on a newer one (fix: upgrade ants here); mixed ants versions, and what could not be read, show in the detail. Skipped with an ant's credentials.
- **ant NAME credentials** and **ant NAME harness**: each config-file ant's credentials are an ant's of its name, for its JetStream domain; its harness is installed and signed in (the ACP handshake of `ants harness check`, one per harness and environment, all at once; `--no-harness` skips them).
- **ant NAME workdir**: a warning for each ant with `max_harnesses` above 1 and `worktrees` off whose workdir is inside a git work tree (a `.git` directory or file in it or above it), where threads running at once may clobber each other's edits (fix: `worktrees = true` in its `[[ant]]`).
- **hub resolver**: on the machine holding the team keys (`--dir`, `ANTS_AUTH_DIR`, default `~/.config/ants/auth`), the `--nats` server, asked as a user of the team's system account, keeps pushed accounts across restarts (a `full` resolver); a warning otherwise, with the `resolver` line to add, and when the server refuses the system account (a `server.conf` from before revocations or of another team). Skipped without the team keys and when the CLI connects through the leaf node.
- **leaf**, **leaf uplink**, **leaf system uplink**, **leaf node**, **outbox**: with a leaf node configuration (`--leaf-config`, default `~/.config/ants/leaf.conf`), its uplink credentials carry the machine's ants and the CLI's human that connect through it, the system account uplink is configured, the leaf node listens where the CLI connects, and, with human credentials, the hub sources the machine's outbox for every member.
- **daemon service** and **leaf node service**: the units of `scripts/install-units.sh` are installed for the config file and the leaf node configuration, and running (see [Services](#services)).

A check that cannot run, such as one needing a file that does not exist or rights the credentials lack, is `skip` with the reason, not a failure. Every network check gives up within 10 seconds (each request after 5), a harness handshake after 60.

### Environment variables

| Variable | Flag / Scope | Default / Behavior |
|---|---|---|
| `ANTS_NATS_URL` | `--nats` (global) | Config file's `[cli]` or top-level `nats`, else `nats://127.0.0.1:4222` |
| `ANTS_CREDS` | `--creds` (global) | Path to `.creds` file, else the config file's `[cli]` `creds`; omitted connects unauthenticated as `human` |
| `ANTS_JS_DOMAIN` | `--js-domain` (global) | Hub JetStream domain (`$JS.<domain>.API`), else the config file's `[cli]` or top-level `js_domain` |
| `ANTS_CONFIG` | `--config` (`daemon`, `add`, `harness`, `spend`); the global connection defaults | `~/.ants/config.toml` |
| `ANTS_ANT` | `--ant` (`ants daemon`) | Serve only the named ant (from config file if `--config` is set, else from CLI flags) |
| `ANTS_METRICS` | `--metrics` (`ants daemon`) | Address for `/healthz` and `/metrics`, else the config file's `metrics`; off by default (see [Monitoring](#monitoring)) |
| `ANTS_AUTH_DIR` | `--dir` (`auth`, `enroll`, `leaf config`, `doctor`) | `~/.config/ants/auth` |
| `ANTS_BRIDGE_TOKEN` | `--token` (`bridge serve`, `bridge connect`) | Optional bearer token gating bridge sessions; `--token-file FILE` reads it from a file instead |
| `ANTS_MCP_TOKEN` | `--token` (`ants mcp`) | Per-thread session token passed automatically by `ants daemon` |
| `ANTS_UPGRADE_CHECK` | `--upgrade-check` (`daemon`, `channel serve`, `webhook serve`, `bridge serve`, `bridge connect`) | `30s`: how often to check for a replaced executable and exit with status 75 ([Upgrade](#upgrade)); `0` never checks |
| `ANTS_LOG` | All commands | `tracing` stderr filter, default `info,rmcp=warn` |
| `SSL_CERT_FILE` | TLS / `ants bridge connect` | PEM CA bundle trusted by rustls for private CAs |
| `ANTS_REQUIRE_TOOLS` | `cargo test` | When `1`, tests fail instead of skipping if `nats-server` is missing on `PATH` |

### Agent skill

`contrib/skills/ants/SKILL.md` teaches an external coding agent to message ants through the CLI (`ants roster`, `ants ask`, `ants inbox --thread`, `ants watch`). Install with `copilot skill add contrib/skills/ants/SKILL.md` or copy into another agent's skills directory.

## Delivery

### Waiting for replies

`ants ask` and `ants post` print the thread ID and exit immediately unless `--wait` or `-f`/`--follow` is set (mutually exclusive):

- `--wait` prints the first reply on the thread (to the CLI human for `ants ask`; by another author for `ants post`) and exits `0`.
- `-f`, `--follow` prints every arriving reply or post on the thread until `--timeout` or Ctrl-C, then exits `0`.
- `--timeout DURATION` (requires `--wait` or `--follow`) bounds the wait. If `--wait` times out, `ants` prints `no reply on thread <thread> within <timeout>` to stderr and exits `1`. Ctrl-C during `--wait` exits immediately as interrupted (`130`). `ants ask --wait` and `--follow` mark only printed replies read, and warn up front that the wait may not end while no daemon serves the ant.

### Inbox consumers, pool and retries

Human replies stay in `ANTS` (30 d) and count as unread until the human's read marker in `ants_read` says otherwise: every device of the human reads them all by sequence, and reading on one marks read on all. Each ant maintains up to `max_harnesses` (default 4) ACP processes, one per active thread: concurrent across threads, sequential per thread, retrying failures in order. Idle harnesses stop after `idle_timeout` (`10m`) and resume via ACP `session/load` using `<ant>.<thread>` in `ants_sessions`; busy harnesses are never evicted. First Ctrl-C or `SIGTERM` drains prompts in flight; a second Ctrl-C exits immediately for redelivery.

Threads of one ant share its workdir unless `worktrees = true` (or `--worktrees`) gives each thread its own git worktree of the workdir's repository: `<home>/worktrees/<thread>` on branch `ants/<ant>/<thread>`, started from the repository's `HEAD` (uncommitted changes in the workdir are not carried over). Sessions of the thread work at the workdir's place inside it, and allowlist paths resolve against it. The worktree is created on the thread's first prompt, reused when its harness starts again (`session/load` sees the same directory), and removed with `git worktree remove --force` once `<ant>.<thread>` has expired from `ants_sessions` (checked at start and hourly, for threads with no live harness), deleting the branch unless it holds commits not merged into the repository's `HEAD`. A reply whose prompt ends with such commits on the thread's branch carries an `[ants] Branch ants/<ant>/<thread> holds N commits not in the repository's HEAD.` line, and a branch kept at removal is told in an `[ants]` message on the thread to its owner, else the default human. Worktrees left under `<home>/worktrees` after `worktrees` is turned off are removed the same way (their repository found from the worktrees themselves), while the threads share the workdir. A workdir outside a git work tree, or a missing `git`, is logged once and stays shared; a worktree that cannot be created (an empty repository, the branch checked out elsewhere) logs a warning and the thread works in the shared workdir. With `worktrees` off, `max_harnesses` above 1 and a workdir inside a git work tree, the daemon logs a warning at start, as threads running at once may clobber each other's edits there; `ants doctor` warns too.

Held messages send 20s progress acks against a 60s `ack_wait` (ignoring duplicate deliveries while held). Delivery is at-least-once and acks only after storing the reply:

1. Before prompting, the daemon writes `<ant>.<seq>` in `ants_attempts`. On redelivery after a cut-off, finding that marker resumes the session and prepends a warning that prior tool calls may have run.
2. When the harness finishes, the reply is saved in `<ant>.<seq>` before publishing with dedup ID `reply-<ant>-<envelope_id>` (24 h window), so a post-generation cut-off republishes without re-prompting.
3. Stored replies are also noted in `<home>/answered` until acked, avoiding re-prompts when an offline leaf buffers replies in its outbox. Markers clear on ack or dead-lettering (30d TTL).

During prompts, the daemon streams unpersisted `StatusEvent` JSON to `ants.status.<ant>.<thread>` (`working`, `waiting_approval`, `idle`, `failed`, 250ms batched text/reasoning, tool calls, execution plan), dropping backpressured frames.

## Config file

`~/.ants/config.toml` (`--config` / `ANTS_CONFIG`) configures multi-ant daemons, shared defaults and custom harnesses. An empty file is valid and serves the built-in `messenger` on `copilot`.

```toml
harness = "copilot"                        # default for every ant and built-in messenger
messenger = true                           # default: serve built-in messenger unless [[ant]] names "messenger"
nats = "wss://hub.example/nats"            # default NATS URL for ants without `nats`
js_domain = "hub"                          # default hub JetStream domain; also settable per ant
human = "alice"                            # fallback human for ownerless escalations (default "human"); also per ant
metrics = "127.0.0.1:9464"                 # serve /healthz and /metrics (default: off); see Monitoring

[cli]                                      # the CLI's connection where no flag or environment variable sets it
creds = "~/.config/ants/auth/alice.creds"  # the human's credentials (an ant's never apply)
nats = "nats://127.0.0.1:4222"             # default: the top-level nats
js_domain = "hub"                          # default: the top-level js_domain

[[schedule]]                               # see Schedules
name = "nightly"
creds = "~/.config/ants/auth/nightly.creds"
cron = "0 3 * * 1-5"
ant = "dev-a"
text = "Triage the failing nightly tests."

[[ant]]
name = "dev-a"
workdir = "~/src/repo-a"                   # must exist
creds = "~/.config/ants/auth/dev-a.creds"
harness = "claude"                         # overrides top-level harness
model = "claude-sonnet-4-5"                # optional exact ACP model ID
reasoning_effort = "high"                  # optional effort ID offered by the model
context = "1m"                             # optional key from the harness's context table
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

[[ant]]
name = "dev-b"
nats = "nats://127.0.0.1:4222"
workdir = "~/src/repo-b"
creds = "~/.config/ants/auth/dev-b.creds"
```

`~` expands to the home directory; other relative paths resolve against the config file's directory (`env` values remain literal). Unknown keys, duplicate ant or schedule names, a schedule named after an ant, or `messenger = false` with no `[[ant]]` fail config loading. Unreachable NATS or JetStream endpoints retry with exponential backoff (`1s` to `30s`); credential or permission errors fail that ant immediately while remaining ants keep running, exiting non-zero only if none start.

Flag and file precedence on `ants daemon`:

- `ants daemon`: serves all ants in `~/.ants/config.toml` (plus built-in `messenger` when enabled) and runs its schedules.
- `ants daemon --ant dev-a` (`ANTS_ANT=dev-a`) without `--config`: ignores `[[ant]]` entries in `~/.ants/config.toml` and configures `dev-a` from CLI flags, while still loading `[harnesses.NAME]` definitions from `~/.ants/config.toml` if present.
- `ants daemon --config FILE --ant dev-a`: serves only `dev-a` from `FILE` (`--ant messenger` selects the built-in messenger), with CLI ant flags overriding `FILE`. With `--ant`, no schedule runs.
- Per-ant, per-schedule and top-level `nats`, `js_domain` and `creds` in the file take precedence over `--nats`, `--js-domain` and `--creds`, which fill in omitted values (`--creds` applies only when serving a single ant, never to schedules).

`SIGHUP` reloads the config file without a restart: ants newly listed start, ants no longer listed stop as on Ctrl-C (prompts in flight finish, waiting messages go back for redelivery), and ants whose resolved settings changed (including a `[harnesses.NAME]` they use or a top-level default) stop the same way and start again with the new settings; unchanged ants keep running untouched. An ant that failed earlier and is still listed starts again. Schedules newly listed start, schedules no longer listed stop at once, and schedules whose entry changed or that stopped start again. A file that fails to load is logged and the ants served and schedules run stay as they were.

## Harnesses

A harness definition specifies an ACP stdio executable, arguments, environment additions, required ACP session mode, valid `context` options and the command that signs it in. Built-in definitions live in `crates/ants-daemon/src/harnesses.toml`:

| Name | Command | Forced ACP mode | Sign-in |
|---|---|---|---|
| `copilot` (default) | `copilot --acp` | none | `copilot login` |
| `claude` | `claude-agent-acp` (`npm install -g @agentclientprotocol/claude-agent-acp`) | `default` | `claude login` or `ANTHROPIC_API_KEY` |
| `antigravity` | `agy acp --disable-telemetry` | `default` | `agy` (or `agy acp --login`) or Gemini API key |

Add new harnesses or replace a built-in whole under `[harnesses.NAME]`:

```toml
[harnesses.custom-acp]
command = "~/.local/bin/custom-acp"            # path if containing '/', else resolved on PATH
args = ["--stdio", "--log-level", "warn"]
env = { CUSTOM_ACP_TELEMETRY = "off" }         # layered: daemon env < harness env < ant env < --env
mode = "default"                               # session fails if harness lacks this mode
sign_in = "custom-acp login"                   # named when the harness reports it is signed out

[harnesses.custom-acp.context]
default = {}
large = { args = ["--context-window", "1m"] }  # appended to launch args
huge = { model_suffix = "-1m" }                # appended to explicit `model`
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

### Sign-in

A harness is signed out when it answers `session/new`, `session/load` or a prompt with ACP's `auth_required` error (`-32000`), or when its error or stderr says so in so many words (`No authentication information found`, `not logged in`, `please run /login` and the like; a proxy's or an MCP server's authentication does not count). The daemon then:

- holds the message in hand, with progress acks, first on its thread: the attempt is not counted (its `ants_attempts` marker is cleared unless the prompt had already streamed work, so the retry carries no cut-off warning), its delivery count does not grow, and no message is dead-lettered for it;
- starts no further prompts; messages already delivered wait their turn, and once `max_harnesses × 2` are in hand the consumer delivers no more;
- tells the thread's owner, else the default human, once per signed-out period, on the thread, with the sign-in command (`sign_in`, else the terminal auth method the harness offers in its `initialize` response) and the machine to run it on; the notice goes to the human directly, not up the ant's chain;
- shows the harness as signed out in the ant's roster entry (`signed_out`: `since`, `sign_in`), so `ants roster` and peers' `list_ants` show why the ant is not answering, and `route` prefers ants whose harness is signed in;
- checks the harness every minute with a handshake (start it, `initialize`, `session/new` in the workdir, stop it); once one succeeds, clears `signed_out` from the roster entry and answers the held messages. The next sign-out after a message got past sign-in tells the human again.

Stopping the daemon while signed out hands the held messages back, each counting one more delivery. `ants harness check` runs the same handshake on demand.

### Model, reasoning effort and context

Set `model`, `reasoning_effort` and `context` per ant or via `--model`, `--reasoning-effort` and `--context`. The daemon sets `model` before `reasoning_effort` via ACP selectors on new and resumed sessions; unsupported IDs or mismatched read-backs fail the session and list valid options.

- **Copilot:** `context` takes `default` or `long_context` (launch flag `--context ...`).
- **Claude:** `context` takes `default`, or `1m` with an explicit `model` (selects exact `<model>[1m]` variant). A `model` already ending in `[1m]` must omit or match `context`.
- **Antigravity:** no `context` table; pick a model with the desired window.

`context` selects a provider-native tier/window; `ants` never truncates or compacts history.

### Prompt limits and spending caps

Per-prompt limits default to `prompt_timeout = "30m"` (includes approval wait; `"0"` disables) and `max_tool_calls = 300` (`0` disables). Optional spending caps bound tokens or cost per thread (`max_thread_tokens`, `max_thread_cost`) and per ant per UTC day (`max_daily_tokens`, `max_daily_cost`).

Crossing a limit or cap cancels the prompt via ACP `session/cancel` (restarting the harness after 30s if hung), returns partial output plus an `[ants]` notice, and escalates agent-originated threads (reason `cap`). Further agent messages on a capped thread escalate immediately without prompting.

- Per-prompt limits reset every prompt.

A human stops a running prompt with `ants cancel ANT THREAD`: a `CancelRequest` on `ants.cancel.<ant>.<thread>` (core NATS, so only daemons connected at the time hear it). The daemon running the prompt cancels it the same way, replies with the partial output and `[ants] The prompt was cancelled by <human> (ants cancel).`, and acks the message: no retry, dead letter or escalation. Every daemon serving the ant answers with a `CancelReply` on `ants.cancelled.<ant>.<thread>` saying whether it cancelled one; the CLI succeeds on the first that did. Messages waiting behind the cancelled one on the thread still run.

- Thread allowances reset on any human message on the thread, and once per escalation level when the level's leader replies.
- Daily allowances reset at 00:00 UTC (or on daemon restart with a higher cap).
- Spend first passing `spend_warn_percent` (default 80, `0` disables, below 100) of a cap, short of it, sends an `[ants]` warning on a new thread: of a thread cap to the thread's owner, else the default human; of a daily cap to the default human. It shows the spend as `ants spend` does (`spent/cap` and the percentage), names the config key to raise (then reload the daemon with `SIGHUP`) and the `ants spend` command. Each cap warns once per thread allowance or UTC day, recorded with the spend in `ants_sessions`, so restarts and later prompts do not repeat it.
- Spending uses ACP `usage_update` cost and prompt token totals stored at `<ant>.<thread>.spend` and `<ant>.day.<YYYY-MM-DD>` in `ants_sessions`, with or without caps. Claude's adapter reports both at turn end; Codex's adapter reports tokens only; Copilot CLI reports neither (so only prompt limits trigger); Antigravity reporting is unverified.

### Spend report

`ants spend` (human credentials) reads those keys:

- `ants spend`: one row per ant that spent anything in the last `--days` UTC days (default 7, today included, at most 366; `ants_sessions` keeps keys 30 days after their last write): today's tokens and cost, and their totals over the days.
- `ants spend ANT`: each day and the total, then each thread that spent anything since a human's last message on it, most costly first.
- `ants spend [ANT] --thread THREAD`: what the ant, or each ant on the thread, spent on it since a human's last message there.

Today's and each day's figures show as `spent/cap` against `max_daily_tokens` and `max_daily_cost`, and threads' against `max_thread_tokens` and `max_thread_cost`, for the ants the local config file (`--config`, `ANTS_CONFIG`, default `~/.ants/config.toml`) lists with caps; ants served from other machines show spend alone. Costs are in the harness's unit (USD for Claude), with two decimals. `--json` prints one object per line: per ant `{ant, days: [{day, tokens, cost}], total: {tokens, cost}, threads?, caps?}` (`threads` only for one `ANT`), or with `--thread` `{ant, thread, tokens, cost, caps?}`; `caps` holds the caps set (`thread_tokens`, `thread_cost`, `daily_tokens`, `daily_cost`).

## Roster, identity and memory

Each daemon publishes a `RosterEntry` per ant at `<ant>` in `ants_roster` (`machine`, daemon `instance`, `harness`, `workdir`, `capabilities`, `whoami`, `escalate_to`, ants `version` and permissions version `perms`, start/heartbeat timestamps, `idle`/`working` activity, in-flight message count, live harness count, and while the harness is signed out a `signed_out` object with `since` and the `sign_in` command), refreshing every 10s and on activity or sign-in transitions, and deleting on clean exit. Entries expire after 30s; `ants roster` marks entries without a heartbeat for 20s as `stale`.

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
- **Off the terminal:** `ants webhook serve` pushes pending requests to each configured human's ntfy, Slack or Telegram, with signed approve/reject links that store the same `ApprovalDecision` (see [Notifications](#notifications)).
- **Suggested rules:** once 3 requests of an ant for the same exact call (`execute:<command>`, `<kind>:<absolute path>` for a single file, `fetch:<url>`, else `<kind>:<title>`) are approved by humans, counting the one at hand and the earlier ones still in the stream, `ants tools approve` prints that allow rule and approval notices mention it. `ants tools approve ID --rule` approves and appends the rule to the ant's `allow` in `~/.ants/config.toml` (`--config`/`ANTS_CONFIG`), leaving the rest of the file as it is, then reloads the daemon service (the ant restarts once its prompts in flight finish). No rule is suggested for commands with shell control characters or `*`, or for calls touching several files.
- **Chain decisions:** ants with `escalate_to` also send an `[ants]` notice (`approval` escalation) to every ant in `escalate_to` on the thread. Members handling that notice may call `decide_approval(verdict)` (`ants.inbox.<ant>.<decider>`); `escalate` is refused on approval notices. Earliest stored decision wins (human or chain member), and `ants tools` inspects both.
- **Expiry:** a request left undecided for `approval_timeout` leaves `ants tools list` and is rejected; the thread's owner (else the default human) gets an `[ants]` notice, on a thread of its own so that `ants ask --wait` still waits for the reply, naming the ant, request, tool and thread, with how to retry (`ants ask --thread`), raise `approval_timeout` or add an `allow` rule.

```sh
ants tools list -f                         # stream pending requests (--json: JSON lines)
ants tools show dev-a/019a7d2e --json      # full request + tool input (unique ID prefix accepted)
ants tools approve dev-a/019a7d2e --always # allow-always when offered by the harness
ants tools approve dev-a/019a7d2e --rule   # also add the exact allow rule to dev-a in config.toml
ants tools reject dev-a/019a7d2e
```

## MCP tools

Each session gets the `ants` MCP server over streamable HTTP (`127.0.0.1:<port>/mcp` with per-thread bearer token) or, without HTTP MCP support, via `ants mcp --socket <home>/mcp.sock` (`0600`, `ANTS_MCP_TOKEN`). Tools run on the active thread during a prompt (allowlist: `other:<tool>`):

| Tool | Description |
|---|---|
| `send_message(to, text, thread?)` | Send to an ant or human (`thread` omitted: current; `"new"`: fresh; else ID) and return once stored; warns when no daemon serves the ant now. |
| `ask_agent(to, text, timeout?)` | Ask an ant on the current thread and wait for its reply (`timeout` a duration such as `90s`: default `5m`, max `30m`; refuses humans, and an ant no daemon serves now at once, sending nothing). |
| `check_inbox(all_threads?)` | Drain late `ask_agent` replies arriving after timeout (current thread unless `all_threads`). |
| `list_ants(capability?)` | List peer ants from `ants_roster` (`whoami`, capabilities, harness, machine, activity, or a signed-out harness and its sign-in command). |
| `route(capability, text, wait?, timeout?)` | Pick a peer with `capability` (signed in > signed out, `live` > `stale`, `idle` > unknown > `working`, fewest messages, name); waits unless `wait: false`. |
| `post_notes(text)` | Append to the current thread's notes (1 turn; wakes nobody). |
| `read_notes(thread?)` | Read up to 50 notes posts (oldest first) and turns left on the current or named thread (0 turns). |
| `decide_approval(verdict)` | Answer an incoming chain approval notice (`allow_once`, `allow_always`, `reject`). |
| `post_channel(channel, text)` | Start a channel thread as `creator` (member/messenger only; inherits turns minus 1 for agent messages). |
| `manage_channel(channel?, add?, remove?, messenger?, clear_messenger?)` | List owned channels, show one, or change members/messenger via `ants channel serve`. |
| `escalate(issue)` | Escalate the thread one level up its origin chain (or to the owner) with reason `asked`. |

Human traffic costs 0 turns; agent-to-agent sends/replies cost 1 turn (`ask_agent` requires 1 turn left for the reply), and self-messaging is refused. Before an ant gets a message, its `ants_roster` entry is read (one key, giving up after 3s): when no daemon serves it now (no entry, a `stale` one, or a signed-out harness), `ask_agent` (and `route` waiting on a reply) is refused at once with nothing sent, and `send_message` still sends, the message waiting in the ant's inbox, with a warning; both say why and name ants to message instead (the closest names in the roster, by edit distance or prefix, or for a stale or signed-out ant the served ants sharing a capability with that ant). Humans and the channel manager are never looked up, and a roster that cannot be read holds nothing back. `ask_agent` hears replies immediately on live inbox and `_INBOX.<ant>.stored.*` subscriptions without waiting behind the active prompt; late replies persist in `<home>/mcp.json` and prepend a count on later prompts until `check_inbox` drains them.

## Messenger

`ants ask` without `@ant` routes through `messenger` (`list_ants` + `route`); channels also use `messenger` by default to triage unmentioned human and webhook posts.

Unless `messenger = false` or an `[[ant]]` is named `messenger`, each daemon serves a built-in `messenger` (`~/.ants/messenger`, `~/.config/ants/auth/messenger.creds` if present, top-level `harness`). Built-in instances coordinate via `ants_roster`: one daemon holds the `messenger` entry while others poll every 10s to take over on exit or 30s TTL expiry, yielding immediately if a configured `[[ant]]` named `messenger` claims the entry. Define `[[ant]] name = "messenger"` with an `AGENTS.md` in `workdir` for custom instructions and `allow` rules:

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

`ants thread list` and `ants thread show` read the `ANTS` stream back with human credentials by message gets (as `ants channel read`, `ants notes`, `ants inbox` and the `--wait` polls do), so they consume, ack and mark read nothing and create no consumer; ant credentials are refused. Message gets are batched direct gets (`$JS.API.DIRECT.GET.ANTS`, nats-server 2.11 or later), one request per 256 messages; on a connection whose credentials or leaf uplink predate permissions version 3 (the request goes unanswered for 2 s), or whose server lacks batches, reads take one `STREAM.MSG.GET` per message instead, for the rest of that connection. Only notes subjects name their thread, so each read goes through every message stored in its window: `--since` bounds it (`list` defaults to `24h`; `show` starts at the thread's start when its id is a time-ordered UUID, as generated ids are, else reads all the stream keeps). Entries are kinds `message`, `escalation` (with level, reason, leader and ants visited), `mention`, `post`, `note`, `approval`, `decision` (a human's, which names no human, or a chain agent's) and `dead`; a human's decision is placed on its request's thread only when the request is in the window. A message stored on a subject naming another sender is marked `(forged)`. A thread's owner is the one its first message naming one gives; `list` counts only what was stored within `--since`, so a thread older than that shows its first message in the window as the opening.

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

Unresolved thread conditions — `turn_budget`, `cap`, `notes`, `dead_letter`, `asked` (`escalate` tool), plus single-hop `approval` (see [Tool permissions](#tool-permissions)) — climb the thread's origin chain before falling back to thread `owner`, else daemon `human` (default `human`).

- **Origin chain:** the stuck ant's `escalate_to` (persisted in `ants_chains`), or on ant-created channel threads and verified `origin_thread` spin-offs, level 1 of the `creator` ant's `escalate_to` (both stuck ant and creator marked visited).
- **Group coordination:** all unvisited ants in `escalate_to` receive an `[ants]` inbox notice on the thread, and thread notes get 8 fresh turns for that level. The first listed ant leads: only the leader may answer the stuck ant (renewing turn budget and spending allowance once per level) or call `escalate`. Non-leaders reply into thread notes and cannot message the stuck ant directly on the thread.
- **Advancement and reset:** after `escalate_after` (`30m`; `"0"` waits indefinitely) without a leader reply, or if a member gets stuck, escalation climbs to the leader's `escalate_to`. Empty chains, visited leaders or `max_escalation_hops` (default 3) hand off to the human owner. Only a human message on the thread resets escalation to level 0.

## Dead letters

Inbox and channel messages get 5 prompt attempts (`max_deliver = 6`); a harness that is signed out spends none (see [Sign-in](#sign-in)). When attempt 5 fails — or on delivery 6 if attempt 5 crashed without recording a reply — the daemon writes a `DeadLetter` to `ants.dead.<ant>`, escalates a `dead_letter` notice on the thread, and terminates the consumer delivery. Unparseable or subject-mismatched payloads are dead-lettered immediately without envelopes so they cannot be replayed.

```sh
ants dead list          # seq, ant, thread, from, deliveries, time, error, text
ants dead replay 42     # republish original message to its inbox and delete dead letter (human creds)
ants dead drop 42       # delete dead letter without replaying (human creds)
ants dead watch         # convert MAX_DELIVERIES advisories into dead letters until Ctrl-C
```

If NATS errors prevent storing the dead letter on delivery 6, JetStream emits unpersisted advisory `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ANTS.<consumer>`. `ants dead watch` (and `ants inbox --follow`) captures that advisory, writes the dead letter and notifies the thread owner (or default human) directly. Offline leaf daemons note burials in `<home>/buried` and check `deadread-<ant>` on reconnect to avoid duplicates.

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
auth = "hmac-sha256"                          # signature_header defaults to X-Hub-Signature-256 (sha256=<hex>)
secret_file = "~/.ants/github.secret"         # mode 0600 (owner-readable only); or secret_env = "VAR"
id = "header:X-GitHub-Delivery"               # or "json:/pointer"; default "body-sha256"
ant = "dev-a"
owner = "alice"                               # optional human owner for replies and escalations
text = "GitHub {header:X-GitHub-Event} on {json:/repository/full_name}\n\n{body}"

[[webhook]]
name = "alerts"                               # ants auth user alerts --webhook --channel ops
creds = "alerts.creds"
auth = "bearer"                               # Authorization: Bearer <secret>
secret_env = "ANTS_ALERTS_TOKEN"
id = "json:/alert/id"
channel = "ops"
mention = ["dev-a"]                           # optional; or broadcast = true for all current members
```

- **Auth and limits:** `hmac-sha256` and `bearer` check secrets in constant time (`401` on failure). Max body `256 KiB` (`413`), timeout `15s`, max `64` concurrent requests (`503` with `Retry-After: 5` when overloaded or NATS cannot store immediately; nothing buffers locally). Bad JSON/ID returns `400`, non-UTF-8 `{body}` `415`, unknown hook `404`, non-`POST` `405`.
- **Deduplication and marker:** stored events return `202` (`{"thread": "...", "duplicate": false}`); repeats within 7 days match `<webhook>.<hash>` in `ants_webhooks` (plus 24 h `Nats-Msg-Id` `webhook.<webhook>.<hash>`) and return `200` (`"duplicate": true`). `ants webhook serve` puts `<webhook>.webhook` in `ants_webhooks` on connecting and hourly, holding the machine, ants version and permissions version it runs (`{"machine": "...", "version": "...", "perms": 2}`; before versions were published, the webhook's name, put daily on delivery).
- **Templates and routing:** `text` substitutes `{body}`, `{id}`, `{webhook}`, `{header:Name}` and `{json:/pointer}` (`{{` and `}}` escape braces). Treat payloads as untrusted input when setting target `allow` rules. Mentions come strictly from config (`mention` or `broadcast`). Inbox webhook replies go to `owner` if set, else are logged; webhook posts never reset turn budgets.

### Notifications

`[[human]]` entries in the same file push tool requests (`ants.approvals.<ant>`) and escalations reaching the human's inbox to phones and chats, so approvals need no terminal running `ants tools list -f` or `ants inbox -f`:

```toml
public_url = "https://hub.example/hooks"      # what the reverse proxy maps to `path`; enables approve/reject links
action_secret_file = "~/.ants/action.secret"  # HMAC key of the links, 16+ characters, mode 0600; or action_secret_env

[[human]]
name = "alice"
creds = "~/.config/ants/auth/alice.creds"     # ants auth user alice --human
ants = ["dev-a", "dev-b"]                     # optional: only these ants' tool requests (default: every ant)
approvals = true                              # tool requests (default true)
escalations = true                            # escalations reaching alice's inbox (default true)

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
```

- **One notice per event:** each human is its own NATS user and reads durable consumer `notify-<human>` on `ANTS` (deliver new, explicit ack), so several `ants webhook serve` processes listing the same human share it and each request or escalation is sent once. Requests already decided or expired, unreadable or forged ones, and escalations older than a day are skipped. Sinks run concurrently with a `10s` timeout; failures are logged (never with URLs or tokens) and not retried.
- **Per device:** a sink with `device` is that device's push target and gets no notice while the device follows what the notice is about: `ants tools list -f` or `ants inbox -f` without `--thread` and `--json` for tool requests, `ants inbox -f` without `--thread` for escalations. The human's other devices still get it. While following, the CLI puts the device's presence in `ants_presence` every 10s and deletes it when it stops; a device that stops without deleting it (crash, lost connection) counts as away within 30s. A sink without `device` gets every notice, as before. Unreadable presence counts as away, so a failure sends a notice too many, never one too few. Legacy devices (credentials without a device id) record no presence.
- **Content:** approval notices carry the ant, tool title and kind, files, an input excerpt, thread, time left and the `ants tools approve|reject <ant>/<id>` commands; a request whose call was approved twice before carries the allow rule `ants tools approve --rule` would add; escalation notices carry the reason, the stuck ant, the text and the `ants ask --thread` command to answer. Plain messages are not notified.
- **Approve/reject links:** with `public_url` and the action secret, approval notices carry one link per offered verdict (`Approve`, `Always allow`, `Reject`): `<public_url>/_approval?ant=&id=&seq=&verdict=&by=&exp=&sig=`, HMAC-SHA256 over every field, valid until the request expires. `GET` shows the request, its state and a confirm button; `POST` stores the decision with the human's credentials exactly as `ants tools approve|reject` does (first answer wins). Answers: `200` stored or shown, `400` incomplete link, `403` wrong signature, `404` unknown human or request, `405` not `GET`/`POST`, `409` already answered or expired, `410` past expiry, `503` hub unreachable. Without `public_url`, notices carry only the commands. Anyone holding a link can decide that one request, so keep sinks private (ntfy topics are public unless the server requires a token).

| Sink | Notice | Actions |
|---|---|---|
| `ntfy` | JSON publish, priority high for tool requests | HTTP action buttons that `POST` the link: one tap decides |
| `slack` | Incoming webhook, `mrkdwn` section | Link buttons opening the confirm page (Slack may flag them without an app interactivity URL; harmless) |
| `telegram` | Bot API `sendMessage`, link previews off | Inline URL buttons opening the confirm page |

Human credentials issued before notifications lack the `notify-<human>` consumer permissions; connecting with them warns and names the command that renews them ([Credentials across upgrades](#credentials-across-upgrades)). Those issued before presence lack `ants_presence`: `ants webhook serve` refuses them for a human with device sinks, and following on such a device warns and records no presence, so its sinks keep getting every notice; renew them (and the leaf uplinks where they run) and run `ants init`.

## Schedules

`[[schedule]]` entries of the config file send a prompt on a timetable: each firing sends `text`, literally, as a new thread to an ant's inbox or a channel. `ants daemon` runs them when serving the whole file (not with `--ant`), each over its own NATS connection with its own credentials:

```toml
[[schedule]]
name = "nightly"                              # ants auth user nightly --webhook --to dev-a
creds = "~/.config/ants/auth/nightly.creds"
cron = "0 3 * * 1-5"                          # minute hour day-of-month month day-of-week, in UTC
ant = "dev-a"
owner = "alice"                               # optional human owner for replies and escalations
text = "Triage the failing nightly tests and summarise them."
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

- **Timetable:** exactly one of `cron` and `every`. `cron` takes 5 fields with `*`, lists, ranges, steps (`*/15`, `1-5/2`, `10/20`), month and weekday names (`jan`, `mon`; day of week `0` and `7` are Sunday) or `@yearly`, `@monthly`, `@weekly`, `@daily` and `@hourly`; when both day fields are restricted, a day matching either fires (as cron does). `every` is a duration of at least `1m` (`15m`, `1h30m`, `1d`), so `every = "1d"` fires at 00:00 UTC. All times are UTC.
- **Destination:** exactly one of `ant` and `channel`; `mention` and `broadcast` (all members at the firing) only with `channel`. `owner` works as for webhooks.
- **Identity:** a schedule is a webhook-role user (`ants auth user NAME --webhook --to ANT` or `--channel CH`, and `--webhook NAME` in `ants auth leaf`), checked at load: its credentials must name it, be no human's and allow its destination. Its messages carry `webhook: true` and it puts `<schedule>.webhook` in `ants_webhooks` hourly, as a webhook does, so replies and triage work as for a [webhook](#webhooks).
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

### Devices

Each `ants auth user NAME --human` (and each signed `--public-key` of a human) issues credentials for a new device of the human: a generated id (a UUIDv7, never chosen and never reused), carried in the user JWT as the tag `ants-device:<id>` and in the connection name of the CLI, plus an optional `--label` for display, which need not be unique. Credentials issued before device ids are a legacy device.

```sh
ants auth user alice --human --label "Alice's laptop"      # prints alice.creds, and the device id on stderr
ants auth devices alice                                   # id (or legacy), label, key, issued, revoked
ants auth rename alice "Alice's laptop" "Work laptop"     # by label, id or key
ants auth revoke alice --device 01968f7e-...              # that device only
```

A label naming more than one device is refused, with the ids of those devices; give an id then. Every device of a human reads the same inbox: `ants inbox -f` on each shows every new reply, and what one device reads (`ants inbox`, `ants ask --wait`) is read on all, kept in the human's read marker in `ants_read` (updated with compare-and-set, so devices reading at once keep each other's marks). The first `ants inbox` with new credentials moves the read state of the old shared consumer `inbox-<name>` into the marker and deletes the consumer; a CLI from before would create it again, and show every reply anew, so upgrade the CLI on every device. Push targets are per device too: a `[[human.notify]]` sink with `device = "<id>"` skips notices while that device follows them (see [Notifications](#notifications)). Human credentials (and leaf uplinks carrying humans) issued before the read marker lack its permissions: `ants inbox` refuses them and `ants ask --wait` leaves its reply unread until they are renewed ([Credentials across upgrades](#credentials-across-upgrades)).

Copy `server.conf` next to the hub `nats-server` config and add `include "server.conf"` (`include` resolves relative to the config file even with a leading `/`; `server.conf` sets `max_control_line: 16384`), followed by `resolver { type: full, dir: "/ABSOLUTE/DIR" }` so the hub keeps pushed accounts in that directory across restarts and leaf nodes catch up on pushes they missed while offline (see [docs/transport.md](docs/transport.md#hub)). The `dir` is relative to the working directory of `nats-server`, not to the configuration file, hence absolute; a hub switching resolvers needs a restart, not a reload. `server.conf` itself keeps a `MEMORY` resolver, which the line after the include replaces: leaf nodes include it too, with their own `full` resolver, and a hub reloading a new `server.conf` would not switch resolvers. Run `ants init` once with human credentials to create `ANTS` and all eleven KV buckets. The first connection of every other CLI command with human credentials (or none) does the same: in one round of concurrent stream info requests, it creates missing buckets and adds what an older version lacks (stream subjects, direct gets, inbox republishing, the 24 h duplicate window of `ANTS`, the `ants_owners` duplicate window), keeping other settings such as outbox sources, and logs each change. It skips objects whose stream info the credentials do not allow (`ants_presence` for every human, which `ants init` and following create without it), warns that `ants init` is needed for changes they cannot make, and ant and webhook credentials check nothing. So a publish on a subject an outdated `ANTS` does not capture, from a daemon, webhook or channel manager too, gets no stream while the hub's JetStream still answers: it fails naming `ants init` with human credentials as the fix, or, on a leaf node with an outbox, waits there with a warning saying the same, once per process for each kind of subject (`ants.chan`, `ants.dead` and so on). When machine members change, run `ants auth leaf MACHINE` with `--add-*`/`--remove-*` and then `ants leaf outbox MACHINE` again; when role permissions change, renew credentials as below.

### Credentials across upgrades

Each user JWT that `ants auth` issues carries the permissions version of the issuing ants as the tag `ants-perms:<n>`; the version goes up with every change to any role's permissions (cancel, spend, owners, notifications, inbox spares, the read marker, presence, ...). On connecting, the CLI, the daemon, `ants webhook serve`, `ants channel serve` and `ants enroll` compare the version of the credentials in use with their own and log a warning, without failing, naming the command that renews them, worked out from the credentials themselves:

```text
WARN the credentials of ant dev-a carry permissions version 1, older than version 2 of this ants: requests their role allows now may be refused; renew them on the machine holding the team keys with `ants auth user dev-a --js-domain hub` and put the new file in their place, once `ants doctor` run as a human finds every daemon, `ants webhook serve` and `ants channel serve` at this permissions version (its `versions` check)
```

The command is `ants auth user NAME` (with `--human`, or `--webhook --to ANT --channel CH`, and `--js-domain D` as issued), `ants auth channel-manager`, or for a leaf node's uplink `ants auth leaf MACHINE` (members as last issued; on credentials from before the team directory kept them, `--ant ANT --human HUMAN --webhook WEBHOOK [--enroll-approver]`) followed by `ants leaf outbox MACHINE`. Credentials without the tag predate versions and count as outdated; credentials newer than the ants in use ask for an upgrade of ants. nats-server, not ants, uses a leaf node's uplink credentials: `ants leaf config` checks those it writes into `leaf.conf`, the system account uplink's `leaf-<machine>.system.creds` included (renewed with the same `ants auth leaf`), and the leaf node needs a restart once they are replaced (`SIGHUP` does not reread credentials).

Credentials that expire within 7 days, or have expired, get a warning naming the same command; the daemon checks again daily. Enrollment credentials, short-lived by design, warn only once expired. `ants auth issued` lists every key of `issued.jsonl` once (`--json`: one object per line) and, under each one in use (neither revoked, reissued since for the same name, expired nor an enrollment's) that is outdated or expires within 7 days, the commands renewing it: from its `.creds` file in the team directory when there is one, else with placeholders for what only that file tells (`--to ANT...`, `[--js-domain DOMAIN]`). Keys recorded before versions show `perms -`.

Upgrades that withdraw rights older versions still use (stream info of `ants_sessions`, `ants_attempts`, `ants_owners`, `ANTS` and `ants_webhooks`) need renewed ant, webhook, channel manager and leaf credentials only after every daemon, `ants webhook serve` and `ants channel serve` runs the new version; until then, old credentials keep working. Each of them publishes the ants and permissions versions it runs: daemons in their roster entries (`ants roster` shows them and flags mixed versions), webhooks and schedules in their `<webhook>.webhook` marker, and the channel manager at `channel-manager.serve` in `ants_webhooks`, both put hourly. `ants doctor` run as a human reads them all (check **versions**) and tells whether every one runs this permissions version, so that renewing is safe; the warnings of outdated ant, webhook, channel manager and leaf credentials point to it. Components from before version publishing (permissions version 1) publish none and count as behind; humans' credentials from before version 2 cannot read the channel manager's entry, and its channel manager credentials cannot write it; humans' credentials and leaf uplinks from before version 3 read the `ANTS` stream back one message at a time instead of in batches. Until credentials are renewed after an upgrade adding rights, requests needing them are refused or unheard (`ants cancel`, `ants spend`, `ants inbox`), and ants' messages on threads no channel post started carry no owner, so their escalations go to the daemon's default human; renew the leaf uplinks of machines where such clients run too, and run `ants leaf outbox` again. Owner records keyed by thread alone, from before the recipient key, are not read.

### Renewing over NATS

Credentials of permissions version 2 or later (ants, humans, webhooks, the channel manager and leaf uplinks) renew over NATS while still valid, without copying files from the machine holding the team keys:

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

A `.creds` file holds a user JWT, which is public, and the NKey seed signing the server's challenge, which is not. `ants keys store` moves the seed into the OS credential store: the macOS keychain (login keychain, service `ants`) or, on Linux, the Secret Service (GNOME Keyring, KDE Wallet, KeePassXC) through `secret-tool` (package `libsecret-tools` or `libsecret`). Windows Credential Manager is not supported; neither is any other platform, where seeds stay in the file.

```sh
ants keys store ~/.config/ants/auth/alice.creds ~/.ants/dev-a.creds   # default: --creds
ants keys unstore ~/.ants/dev-a.creds                                  # back into the file
ants keys remove ~/.ants/dev-a.creds                                   # delete the file and its stored seed
```

Each seed is stored under its public key, so every device and every ant has an entry of its own and stays revocable on its own. `ants keys store` stores the seed, reads it back, and authenticates at `--nats` with the file as it is to be before replacing the file at once; the file then keeps the JWT and, in place of the seed, the public key it is stored under. Any failure (store locked, write refused, server refusing the key) leaves the file as it was and deletes what was stored. Every ants process using the file (CLI, daemon, `ants webhook serve`, `ants channel serve`, `ants renew`) reads the seed from the store when it connects and keeps it in memory to sign the challenge again on each reconnect. `ants renew` keeps the new seed where the earlier one was: stored, read back and proven by authenticating before the file is replaced, the earlier seed deleted only then. A file naming a stored key never falls back to a seed elsewhere: when the store is locked, unreachable or lacks the entry, connecting fails with what to do, and no message carries a seed. Other NATS tools (`nats` CLI) cannot read such a file; `ants keys unstore` makes it plain again.

- **Headless machines:** a launchd agent reads the login keychain once the user has logged in; a locked keychain fails the connection (error -25308). macOS asks again for access after `ants upgrade` replaces the binary: answer **Always Allow** in a session, then restart the service. On Linux a systemd user service, an SSH session or WSL often has no Secret Service running or unlocked. On such machines keep the seed in the file, readable by its owner only (`ants keys unstore`, or never store it): the file-backed form is the explicit compatibility mode, and nothing moves a seed between the two on its own.
- **Backup and recovery:** a stored seed is backed up with the store (an encrypted keychain or keyring backup), or exported with `ants keys unstore` and stored again later. A lost seed cannot be recovered: issue the credentials again (`ants auth user`, `ants enroll`) and revoke the old key.
- **Removal and revocation:** `ants keys remove` deletes a machine's copy only; the credentials stay valid until they expire. Revoke them with `ants auth revoke --key KEY` on the machine holding the team keys.
- **Not covered:** leaf uplink credentials (`leaf-<machine>.creds`, `.system.creds`), which nats-server reads (`ants keys store` refuses them); the team keys `operator.nk`, `account.nk` and `system.nk`; the pending `<ant>.nk` of `ants add`; credentials `ants auth` writes for another machine (move them there, then store them); and the short-lived system account and approver credentials `ants auth revoke`, `push` and `ants enroll` build in memory. `ants add` writes new credentials with the seed in the file; store it afterwards.

### Wire ACLs and verification invariants

- **Subject-pinned senders:** ants and webhooks publish only under their own sender token (`ants.inbox.*.<self>`, `ants.notes.*.<self>`, `ants.chan.*.<self>`, `ants.dead.<self>`, `ants.chanadm.*.<self>`, plus `ants.out.*` on leaves). Readers verify payload `from`/target fields against the NATS subject (`ants watch` marks mismatches forged). Only humans publish under arbitrary inbox senders (for `ants dead replay`), write `ants.approvals.<ant>.<id>`, publish cancel requests (`ants.cancel.<ant>.<thread>`, heard only by `<ant>`, which answers only on `ants.cancelled.<ant>.*`), write `ants_humans` and `ants_channels` (alongside `channel-manager`), or call `STREAM.MSG.GET` and batched direct gets of `ANTS` (`$JS.API.DIRECT.GET.ANTS`), which take any sequence. Each human may create and read its own notification consumer `notify-<human>` only.
- **Scoped ant reads:** without `STREAM.MSG.GET`, ant credentials pin consumer filters (`inbox-<ant>[_1..3]`, `chan-<ant>`, `chanread-<ant>[_1..3]`, `notes-<ant>[_1..3]`, `deadread-<ant>`) and restrict direct gets to `ants.approvals.<ant>.*`, their own `$KV.ants_sessions.<ant>.>` and `$KV.ants_attempts.<ant>.>`, `$KV.ants_humans.*`, `$KV.ants_chains.*`, `$KV.ants_owners.>` and `$KV.ants_webhooks.*.webhook`; humans also direct-get `$KV.ants_webhooks.channel-manager.serve`, the one key of `ants_webhooks` besides its own events the channel manager writes. Users write only their own keys in `ants_roster`, `ants_chains`, `ants_links` and `ants_webhooks`. Only humans and webhooks with an ant destination write `ants_owners`, webhooks only `<thread>.<ant>` keys of those ants (humans also through a leaf outbox), and its stream refuses a second record of a key (`discard_new_per_subject`, deletes and purges denied), but drops one repeating the first's `Nats-Msg-Id` (`owner.<key>`, which every writer sets) within the 30 d retention, so a record sourced from an outbox never stalls the source. `ants init` and `ants leaf outbox` widen the duplicate window of a stream from before.
- **Private keys:** stream info with a subjects filter lists every key of a bucket, so only human credentials get stream info of `ants_sessions`, `ants_attempts` and `ants_owners` (to create them, and for `ants spend` to find spend keys). Humans read and write only their own read marker (`$KV.ants_read.<human>`, by direct get) and their own devices' presence (`$KV.ants_presence.<human>.>`, by direct get, without stream info of `ants_presence`) and may delete only their own legacy `inbox-<human>` consumer. Humans direct-get only the spend keys of `ants_sessions` (`$KV.ants_sessions.*.*.spend` and `$KV.ants_sessions.*.day.*`), not ants' sessions. Daemons read their own keys there and owners by direct get and write their own keys by plain publish, and webhooks record owners by plain publish, so neither ants nor webhooks can list other ants' thread ids. Likewise stream info of `ANTS` lists every subject stored (who messages whom, approvals, dead letters) and that of `ants_webhooks` every webhook's events, so only human credentials get them: ants, webhooks and the channel manager find JetStream through the info of their own inbox consumer, ants open `ANTS` without its info and read back from the last message through their own reader, and a webhook records each event it delivered by a publish expecting no earlier record of it and reads it back by direct get.
- **Metadata trust:** daemons accept `owner` only for `human` or entries in `ants_humans`, and honour `webhook: true` only with `<sender>.webhook` in `ants_webhooks` (or from a human). Channel thread `owner` and `creator` come from stored root/human posts in `ANTS`; an ant's non-channel `owner` comes from `ants_owners` for its thread or its `origin_thread`, else is dropped: a channel `origin_thread` gives the owner its posts say, under the same participation check as `creator`; a message thread, its own or its `origin_thread`, gives the owner of its `<thread>.<sender>` record, else of the record of an ant `ants_links` traces the sender's link back to; non-channel `creator` requires `origin_thread` backed by participation or `<from>.<thread>.<to>` links in `ants_links` (max 32 hops).

### Adding an ant

`ants add` generates the ant's user NKey on the target machine (keeping `<ant>.nk` at mode `0600` until signed, never copying the seed elsewhere) and appends `[[ant]]` to `config.toml`:

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

Requests prove the enrollment credentials by signature; `--hub URL` sends them to another server than `--nats`. Behind a leaf node, `ants add --enroll` names the machine's leaf (from `~/.config/ants/leaf.conf` or `--leaf-config FILE`) with a signature by its uplink key, and approval renews the leaf's uplink credentials for the ant and admits it to the outbox sourcing set by `ants leaf outbox`; `ants add` then restarts the leaf node service of that `leaf.conf` when it runs (`SIGHUP` does not reread credentials; otherwise it prints the restart command, see [Services](#services)). Later `ants auth leaf` and `ants leaf outbox` runs keep the ant: approval records it with the leaf's members in the team directory, and the renewed uplink credentials carry it. `ants enroll` behind a leaf node needs uplink credentials issued with `ants auth leaf --enroll-approver`. Leaf uplink credentials issued before enrollment crossed leaves need `ants auth leaf` once more ([Credentials across upgrades](#credentials-across-upgrades)). Uplink credentials grow by about 7.5 KB per ant with a JetStream domain (4 KB without); `ants auth leaf` and approval refuse, before writing or answering anything, uplink JWTs over 252 KiB, the most a hub takes in a leaf's `CONNECT` (16 × `max_control_line`), about 34 ants per leaf with a domain (66 without): run more behind another leaf node under another machine name. See [docs/transport.md](docs/transport.md#enrollment-behind-a-leaf-node).

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

`scripts/install-units.sh` renders templates from `contrib/launchd/` (macOS `~/Library/LaunchAgents`) or `contrib/systemd/` (Linux/WSL `~/.config/systemd/user`) with resolved binary paths and current `PATH`, then enables and starts them:

```sh
scripts/install-units.sh                                           # nats + daemon
scripts/install-units.sh --devtunnel ants-hub.usw2                 # + devtunnel connect (--devtunnel-token FILE)
scripts/install-units.sh --no-daemon --channel-manager ~/.config/ants/auth/channel-manager.creds --webhook
scripts/install-units.sh --no-daemon --devtunnel-host ants-hub    # hub: + devtunnel host (needs devtunnel user login)
scripts/install-units.sh --bridge https://hub.example/bridge --bridge-token ~/.config/ants/bridge.token   # + ants bridge connect
scripts/install-units.sh --no-daemon --bridge-serve --bridge-token ~/.config/ants/bridge.token           # hub: + ants bridge serve
scripts/install-units.sh --out ./rendered-units                    # render only; install nothing
```

Options: `--ants PATH`, `--nats-server PATH`, `--nats-conf FILE` (default `~/.config/ants/leaf.conf`), `--config FILE` (default `~/.ants/config.toml`), `--no-daemon`, `--channel-manager CREDS`, `--webhook`, `--webhook-config FILE` (default `~/.ants/webhooks.toml`), `--devtunnel TUNNEL`, `--devtunnel-host TUNNEL`, `--devtunnel-bin PATH`, `--devtunnel-token FILE`, `--bridge URL`, `--bridge-serve`, `--bridge-token FILE` (either end), `--bridge-proxy URL`, `--bridge-ca FILE` (`SSL_CERT_FILE` for `--bridge`), `--out DIR`. `--devtunnel` and `--bridge` exclude each other: both listen on `127.0.0.1:7422`.

| Service | macOS (launchd / `~/Library/Logs/ants/`) | Linux & WSL (`systemd --user`) | Installed when |
|---|---|---|---|
| NATS (`nats-server`) | `ants.nats` (`nats.log`) | `ants-nats.service` | Always |
| Ant daemon (`ants daemon`) | `ants.daemon` (`daemon.log`) | `ants-daemon.service` | Default (omit with `--no-daemon`) |
| Channel manager (`ants channel serve`) | `ants.channels` (`channels.log`) | `ants-channels.service` | `--channel-manager CREDS` |
| Webhook ingress (`ants webhook serve`) | `ants.webhook` (`webhook.log`) | `ants-webhook.service` | `--webhook` |
| Dev Tunnel (`devtunnel connect`) | `ants.devtunnel` (`devtunnel.log`) | `ants-devtunnel.service` | `--devtunnel TUNNEL` |
| Dev Tunnel host (`devtunnel host`, hub) | `ants.devtunnel-host` (`devtunnel-host.log`) | `ants-devtunnel-host.service` | `--devtunnel-host TUNNEL` |
| HTTPS bridge (`ants bridge connect`) | `ants.bridge` (`bridge.log`) | `ants-bridge.service` | `--bridge URL` |
| HTTPS bridge server (`ants bridge serve`, hub) | `ants.bridge-serve` (`bridge-serve.log`) | `ants-bridge-serve.service` | `--bridge-serve` |
| Log rotation (macOS) | `ants.logrotate` (`logrotate.log`) | — (the journal rotates) | Always on macOS |

- **Reload leaf uplink:** `launchctl kill SIGHUP gui/$(id -u)/ants.nats` (macOS) or `systemctl --user reload ants-nats` (Linux); `ants leaf config` does it itself.
- **Reload daemon config:** `launchctl kill SIGHUP gui/$(id -u)/ants.daemon` (macOS) or `systemctl --user reload ants-daemon` (Linux).
- **Managed by `ants add`:** completed credentials reload `ants.daemon` / `ants-daemon` when its unit (`~/Library/LaunchAgents/ants.daemon.plist`, `~/.config/systemd/user/ants-daemon.service`) runs the config file of `ants add` and the service is running; an enrollment that renewed the leaf's uplink credentials then restarts `ants.nats` / `ants-nats` (`launchctl kickstart -k`, `systemctl --user restart`) when its unit runs that `leaf.conf`. `ants renew` reloads the daemon service in the same way when one of its ants uses the renewed file, and restarts `ants.nats` / `ants-nats` when the file is the uplink of the `leaf.conf` its unit runs. `ants leaf config` reloads `ants.nats` / `ants-nats` (`launchctl kill SIGHUP`, `systemctl --user reload`) when its unit runs the `leaf.conf` written and the service is running, or restarts it for changes a reload does not take. A unit of another file, a stopped service or another system gets the commands printed instead.
- **Start order:** systemd starts `ants-nats` after `ants-devtunnel` or `ants-bridge`, and `ants-devtunnel-host`, `ants-bridge-serve`, the daemon, channel manager and webhook ingress after `ants-nats`. launchd has no start order: the leaf node retries its uplink until the tunnel or bridge listens, and the other services retry their connections.
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

Integration tests requiring `nats-server` on `PATH` skip when absent unless `ANTS_REQUIRE_TOOLS=1` is set (as in `.github/workflows/ci.yml`). `crates/ants/tests/e2e.rs` exercises `ants ask --wait` against `ants daemon` using the test binary as a stub ACP harness.

## Layout

| Path | Purpose |
|---|---|
| `crates/ants-proto` | Wire contract: IDs, subjects, envelopes, turn budgets, NATS permissions (no I/O) |
| `crates/ants-daemon` | NATS/JetStream bus, ACP harness pool, delivery loop, approvals, escalations, MCP server |
| `crates/ants` | `ants` binary: CLI, `daemon` and its schedules, `auth`, `enroll`, `leaf`, `bridge`, `channel`, `webhook`, `mcp` |
| `crates/ants-test-support` | Dev-only test helpers: `PATH` checks and throwaway `nats-server` |
| `contrib/` | `launchd` plists, `systemd` user units and CLI agent skill (`contrib/skills/ants/SKILL.md`) |
| `scripts/` | `install-units.sh` (service installer) and `footprint.sh` (RSS budget check) |
| `docs/transport.md` | Transport reference: hub, reverse proxy, leaf nodes, store-and-forward, HTTPS bridge, Dev Tunnels |

Work items and specifications live in `bd` (`.bd/`): `bd ready`, `bd spec list`, `bd spec show <area>`. The database stays local; `.bd/issues.jsonl` is a committed snapshot (`bd export -o .bd/issues.jsonl`), loaded into a fresh workspace with `bd init --prefix ants` and `bd import .bd/issues.jsonl`.

## Security

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
