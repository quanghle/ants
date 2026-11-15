# ants

[![CI](https://github.com/quanghle/ants/actions/workflows/ci.yml/badge.svg)](https://github.com/quanghle/ants/actions/workflows/ci.yml)

Headless agent teams across machines, communicating with each other and with humans at the terminal over NATS.

- **Backbone:** NATS + JetStream stream `ANTS` for durable inboxes, channels, thread notes, approvals and dead letters, plus 14 NATS KV buckets for roster, sessions, attempts, channels, humans, chains, links, owners, read markers, presence, notifications, reactions, thread metadata and webhooks.
- **Transport:** `wss://`/`ws://` behind a reverse proxy, `tls://`, a Dev Tunnel, or the plain-HTTPS fallback bridge (`ants bridge`), optionally with a local store-and-forward leaf node per machine. See [docs/transport.md](docs/transport.md).
- **Harnesses:** ACP (Agent Client Protocol) agents over stdio: Copilot CLI (`copilot --acp`, default), Claude Code (`claude-agent-acp`), Google Antigravity CLI (`agy acp`), or custom `[harnesses.NAME]` definitions such as Codex (`codex-acp`). See [docs/config.md](docs/config.md#harnesses).
- **Footprint:** one Rust binary, `ants`, serves as the CLI, ant daemon, signer, stdio MCP bridge, channel manager, webhook ingress and HTTPS bridge. An idle daemon stays under 20 MB RSS.

## Architecture

```text
 machine "macbook"                      hub machine                       machine "desk"
+-------------------------------+      +--------------------------+      +--------------------------+
| ants daemon                   |      | nats-server (hub)        |      | ants CLI (human)         |
|   dev-a -> harness per thread |      |   JetStream domain "hub" |      |   ask, post, inbox,      |
|   dev-b -> harness per thread |      |   stream ANTS            |      |   tools, watch, dash     |
|   MCP server for sessions     |      |   14 KV buckets          |      |            |             |
|            |                  |      |                          |      |            |             |
| nats-server (leaf, outbox)  --+----->|                          |<-----+-- nats-server (leaf)     |
+-------------------------------+      +--------------------------+      +--------------------------+
                        uplink: wss://, tls://, Dev Tunnel, or ants bridge
```

- **Hub:** one `nats-server` with JetStream holding stream `ANTS` and 14 KV buckets (`ants init`). With `ants auth`, every ant, human, webhook, observer, leaf uplink and channel manager connects using role-scoped NKey/JWT credentials ([docs/auth.md](docs/auth.md)).
- **Leaf:** optional per-machine `nats-server` (`ants leaf config`) on `127.0.0.1:4222` with a store-and-forward outbox (`ants leaf outbox`). Local `ants` processes target `127.0.0.1:4222`; only the leaf uplink crosses the network.
- **Daemon:** `ants daemon` serves ants over per-ant NATS connections and durable consumers, runs up to `max_harnesses` ACP processes per ant, enforces tool allowlists, limits and spending caps, refreshes roster heartbeats and exposes an MCP server to each session.
- **Ants and humans:** ants are named agents (`dev-a`, `reviewer`, `messenger`) with a workdir, harness, capabilities and optional `whoami`. Humans (`human` by default, or named users from `ants auth user NAME --human`) own threads they start and approve tools. Unaddressed `ants ask` requests go to `messenger` for capability routing. Agent-to-agent messages share a per-thread turn budget (default 8).

| Store / subject | Storage / TTL | Purpose |
|---|---|---|
| Stream `ANTS` | File, 30d | Captures `ants.inbox.>`, `ants.notes.>`, `ants.approvals.>`, `ants.dead.>`, `ants.chan.>`; direct gets enabled; republishes `ants.inbox.*.*` to `_INBOX.<to>.stored.<from>`; `max_consumers` 10000. |
| KV `ants_roster` | Memory, 30s | Live `RosterEntry` per ant (`<ant>`), refreshed every 10s; expiry markers enabled; `max_consumers` 10000. |
| KV `ants_sessions` | File, 30d | Thread ACP session IDs (`<ant>.<thread>`), spend/escalation/narrowing state, channel seen cursors, mention markers, `_storage.*.*.warned` notes. |
| KV `ants_attempts` | File, 30d | Started delivery markers and saved replies (`<ant>.<seq>`), message marks (`<ant>.msg.<id>`), unserved-ant markers (`<sender>.unserved.<id>`). |
| KV `ants_channels` | File, none | `ChannelEntry` (`<channel>`): members, optional messenger, optional owner ant; `max_consumers` 10000. |
| KV `ants_humans` | File, none | Registered humans (`<human>`), written on CLI connection; `max_consumers` 10000. |
| KV `ants_chains` | File, none | Persistent `escalate_to` chains (`<ant>`), readable after roster expiry. |
| KV `ants_links` | File, 30d | Verified `origin_thread` handoffs (`<from>.<thread>.<to>`). |
| KV `ants_owners` | File, 30d | Thread `owner` (`<thread>.<recipient>`); first record wins (`discard_new_per_subject`, 30d dedup window, no delete/purge). |
| KV `ants_read` | File, none | Per-human inbox read marker (`<human>`: `below` + `read` set), shared across devices. |
| KV `ants_presence` | Memory, 30s | Per-device follow presence (`<human>.<device>.<inbox\|approvals>`), refreshed every 10s. |
| KV `ants_notify` | File, none | Per-human snooze deadline and muted threads (`<human>`), and notices held back for a digest (`held.<human>`). |
| KV `ants_reactions` | File, 30d | Message reactions (`<reactor>.<message_id>.<reaction>`); self-scoped write/delete, human-wide read. |
| KV `ants_threads` | File, none | Shared thread titles (`title.<thread>`) and per-human pin/archive/keep marks (`marks.<human>`); `ants thread list` prunes those of expired threads. |
| KV `ants_webhooks` | File, 7d | Webhook event hashes (`<webhook>.<hash>`), schedule firings (`<schedule>.<unix_time>`), and hourly `<webhook>.webhook` / `channel-manager.serve` version markers. |
| Core NATS | Unstored | `ants.status.<ant>.<thread>`, `ants.cancel[led].<ant>.<thread>`, `ants.queue[d].<ant>`, `ants.chanadm.<channel>.<ant>`, `ants.enroll.<ant>[.reply]`, `ants.renew.<name>[.reply]`, `ants.antreq.<manager>[.reply]`. |

User, channel, thread and JetStream domain IDs use 1–64 characters of `a-z`, `0-9` and `-`, starting with a letter or digit (`channel-manager` is reserved). Machine and harness names also allow `_` (machine names allow uppercase ASCII and must differ from `js_domain`). Durations require whole-number units ordered largest first (`s`, `m`, `h`, `d`: `90s`, `10m`, `1h30m`) or `0`; bare non-zero numbers are rejected.

## Install

Requirements: Rust 1.100+, [nats-server](https://github.com/nats-io/nats-server/releases) 2.11+ (tested with 2.15.3), and a signed-in harness CLI on machines running `ants daemon`.

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
ants inbox                                          # unread replies (marks read across devices)
ants roster                                         # active ants and harness status
ants tools list                                     # pending tool approval requests
ants thread list                                    # threads active in the last 24h (--escalated, --pinned, --archived)
ants thread show <thread>                           # full stored thread history
ants thread search --from dev-a --grep deploy       # cross-thread message search
ants thread title <thread> Release 1.2              # shared title (also: pin, unpin, archive, unarchive, mute)
ants react <message> eyes                           # react to a message (id or unique start of one)
ants channel create ops --member dev-a,dev-b
ants post '#ops' "@dev-a the build is red"
ants watch [dev-a [<thread>]]                       # live bus traffic or one ant's streamed progress
ants queue dev-a                                    # waiting messages (ants withdraw / ants cancel)
ants dash                                           # interactive terminal dashboard
```

Without `ants setup`: `nats-server -js &`, `ants harness check`, and `ants daemon --ant dev-a --workdir ~/src/some-repo &`. An empty `~/.ants/config.toml` serves the built-in `messenger` alone.

## CLI

| Command | Description |
|---|---|
| `ants ask [--thread THREAD] [--wait \| -f] [--timeout DUR] [@ant] TEXT \| --file PATH \| --edit` | Send to `@ant` (`messenger` if omitted); prints `thread <id>` and `message <id>`. Warns on stderr if `@ant` is missing from `ants_roster`, `stale`, `signed-out`, `failing`, or provider-limited. |
| `ants inbox [--peek] [--thread THREAD] [--limit N] [-f] [--json]` | Print unread replies (with message IDs and reactions) and mark read across all devices (`--peek` keeps unread; `-f` streams replies, reactions added/removed to shown messages (human creds; `--json`: `{message, by, reaction, removed}`), `MAX_DELIVERIES` advisories and, without `--thread`/`--json`, tool requests; records presence without `--thread`). |
| `ants roster [--json]` | List ants (`live`/`stale`, `idle`/`working`/`signed-out`/`failing`/`rate-limited`/`out-of-quota`), daemon/perms versions, harness and `AGENT` (`name@version`), capabilities, `escalate_to` and workdirs, plus version-drift and recovery notes. |
| `ants spend [ANT] [--thread THREAD \| --days N] [--json] [--config FILE]` | Report token and cost spend over UTC days (default 7, max 366) or on a thread against local config caps ([docs/config.md](docs/config.md#spend-report)). |
| `ants notes THREAD [TEXT]` | Show thread notes, or post `TEXT` as the CLI human (resets budget to 8 turns; wakes nobody). |
| `ants react MESSAGE REACTION [--remove]` | Add or remove a message reaction (`1–64` of `a-z0-9-`); wakes nobody, marks nothing read, ends no wait. `MESSAGE` is a message id or, for humans, a unique start of one (ambiguous or unknown starts list candidates). |
| `ants post [--thread THREAD] [--wait \| -f] [--timeout DUR] CHANNEL TEXT \| --file PATH \| --edit` | Post to `ops` or `'#ops'`; prints `thread <id>` and `message <id>` (`@ant` wakes members, `@human` notifies members, no mention triages). |
| `ants channel create \| set \| add \| remove \| list \| delete` | Manage channels (`--member USER,...`, `--messenger ANT`, `--default-messenger`, `--owner ANT`, `--no-owner`). |
| `ants channel read CHANNEL [--thread THREAD \| --limit N]` | Print channel posts grouped by thread. |
| `ants channel serve` | Run the channel manager (`ants auth channel-manager` creds) to apply owner `manage_channel` requests. |
| `ants thread list [--ant USER] [--owner HUMAN] [--grep TEXT] [--escalated] [--pinned \| --archived] [--since DUR] [--limit N] [--json]` | List active threads within `--since` (default `24h`, `0` all; limit 50), pinned last, archived and `auto-archived` hidden unless `archived+new`. |
| `ants thread show THREAD [--since DUR] [--json]` | Print a thread's stored messages, posts, notes, escalations, tool requests/decisions, dead letters and reactions. |
| `ants thread search [--grep TEXT] [--from USER] [--to USER] [--channel NAME] [--type KIND,...] [--since TIME] [--until TIME] [--limit N] [--json]` | Search stored messages across threads matching all filters (latest `--limit`, default 50, oldest first). |
| `ants thread title THREAD [TEXT...] [--clear]` | Print, set (1–120 chars, shared across humans) or clear a thread title. |
| `ants thread pin \| unpin \| archive \| unarchive THREAD` | Pin or archive a thread for the CLI human across devices (mutually exclusive; archiving cancels nothing; unarchiving an `auto-archived` thread keeps it another `auto_archive` period). |
| `ants thread mute \| unmute THREAD [--for DUR]` | Mute push notifications for `THREAD` across devices (default `30d`, max `365d`), or unmute ([docs/webhooks.md](docs/webhooks.md#quiet-hours-snooze-and-muted-threads)). |
| `ants notify snooze DUR` / `resume` / `status [--json]` | Snooze push notifications (`1s`–`365d`, subject to `bypass`), resume, or inspect active holds. |
| `ants watch [ANT [THREAD] \| SUBJECT]` | Stream traffic (default `ants.>`), escalations/decisions, reactions (human creds), or live harness progress for `ANT`. |
| `ants dash [--since DUR]` | Interactive TUI for recent threads (archived and `auto-archived` hidden unless `archived+new`), live progress, roster and tool approvals (human creds + TTY). |
| `ants cancel ANT THREAD` | Cancel the prompt `ANT` runs on `THREAD` (5s timeout; human creds). |
| `ants queue ANT [--json]` / `ants withdraw ANT MESSAGE` | Inspect waiting messages for `ANT`, or withdraw an unstarted message (human creds). |
| `ants tools list [-f] [--json]` / `show ID [--json]` | List or inspect pending tool requests (`<ant>/<id>`, unique prefix accepted; `-f` records approval presence). |
| `ants tools approve ID [--always] [--rule [--config FILE]]` / `reject ID` | Decide a pending tool request; `--rule` appends the exact allow rule to `config.toml` and reloads the daemon. |
| `ants dead list` / `replay SEQ` / `drop SEQ` / `watch` | Inspect, replay or drop dead letters; `watch` converts `MAX_DELIVERIES` advisories into dead letters. |
| `ants init` | Create or upgrade stream `ANTS` and all 14 KV buckets (human creds; also runs automatically on first human CLI connect). |
| `ants daemon [--ant NAME] [--config FILE] [--metrics ADDR] [ant-flags...]` | Serve config-file ants and schedules, or a single `--ant` ([docs/config.md](docs/config.md)). |
| `ants add NAME [--config FILE] [--out FILE] [--store-key] [--jwt JWT \| --enroll CREDS [--hub URL] [--leaf-config FILE]] [ant-flags...]` | Generate local NKey, append `[[ant]]` to `config.toml`, and complete credentials ([docs/auth.md](docs/auth.md#adding-an-ant)). |
| `ants retire NAME [--config FILE] [--dir DIR] [--no-push]` | Unlist, drain, revoke and delete credentials of a templated ant while keeping `~/.ants/<name>` ([docs/config.md](docs/config.md#retiring-an-added-ant)). |
| `ants signer [--config FILE] [--approval-timeout DUR] [--dir DIR]` | Sign or human-gate `request_ant` requests and process `retire_ant` revocations ([docs/config.md](docs/config.md#manager-ants)). |
| `ants enroll list` / `approve NAME [...]` / `reject NAME [--reason TEXT]` | List, sign (`--webhook --to/--channel` for webhooks) or reject `ants add --enroll` requests. |
| `ants enroll renewals` / `renew NAME [--public-key KEY]` / `renew --auto` | List or sign over-NATS `ants renew` requests ([docs/auth.md](docs/auth.md#renewing-over-nats)). |
| `ants renew [FILE] [--config FILE] [--leaf-config FILE]` | Rotate `.creds` over NATS and reload the daemon, or reconnect the leaf node's uplink. |
| `ants keys store` / `unstore` / `remove [FILE...]` | Move NKey seeds into/out of the OS credential store, or delete local credentials ([docs/auth.md](docs/auth.md#private-keys-in-the-os-credential-store)). |
| `ants harness check` / `prompt TEXT` | Verify harness install and sign-in, or run one standalone prompt (`--harness NAME`, `--config FILE`). |
| `ants setup [--ant NAME --workdir DIR] [--harness NAME] [--config FILE] [--nats-server PATH] [--no-services] [--no-harness] [--no-completions] [--dry-run]` | Idempotently set up a single machine and run `ants doctor`. |
| `ants completions SHELL` / `--install [SHELL]` | Print or install dynamic completions for `bash`, `zsh` or `fish`. |
| `ants doctor [--config FILE] [--leaf-config FILE] [--dir DIR] [--webhook-config FILE] [--no-harness] [--json]` | Audit local config, keys, hub/leaf/outbox, versions, storage, services, webhooks and tunnels (`exit 1` on `fail`). |
| `ants auth init` / `user` / `enroll` / `channel-manager` / `leaf` / `revoke` / `revoked` / `issued` / `push` / `backup` / `restore` / `devices` / `rename` | Manage team NKeys, role `.creds`, revocations, age-encrypted backups and human devices ([docs/auth.md](docs/auth.md)). |
| `ants leaf config MACHINE --uplink URL ...` / `outbox MACHINE ...` | Write `~/.config/ants/leaf.conf` or configure store-and-forward outbox sourcing ([docs/transport.md](docs/transport.md)). |
| `ants bridge serve` / `connect URL` | Tunnel a leaf uplink over plain HTTPS `POST`/`GET` requests ([docs/transport.md](docs/transport.md#https-fallback-bridge)). |
| `ants webhook serve [--config FILE] [--listen ADDR] [--metrics ADDR]` / `notify-test HUMAN` / `add NAME ...` | Serve HTTP webhooks and human push notifications, test sinks, or provision a webhook ([docs/webhooks.md](docs/webhooks.md)). |
| `ants hook run --on EVENT [--concurrency N] [--timeout DUR] -- CMD [ARGS...]` | Run `CMD` per bus event using `--observer` credentials ([docs/hooks.md](docs/hooks.md)). |
| `ants mcp --socket PATH --token TOK` | Internal stdio-to-socket/pipe MCP bridge spawned by `ants daemon`. |

`THREAD` in `ask`, `inbox`, `post`, `notes`, `thread show`, `thread title`, `thread pin`/`unpin`/`archive`/`unarchive`, `thread mute`/`unmute`, `cancel`, `watch`, `spend --thread` and `channel read --thread` accepts a hyphenated UUID (used without lookup), a unique prefix (or non-UUID thread ID), or `last` (latest thread the human sent or received on). Prefix lookup (human creds only) checks the human's own threads from the last 7 d first, then all threads in the last 7 d (bounded to `[-5m, +10m]` around the timestamp encoded in a UUIDv7 prefix); exact matches win, and ambiguous or missing prefixes list candidates.

### Identity, setup, completions and doctor

- **Connection & identity:** every command accepts `--nats`, `--creds` and `--js-domain`, falling back to `[cli]` then top-level keys in `ANTS_CONFIG` (`~/.ants/config.toml`), except `ants daemon` and `ants add` (which read top-level config directly). NATS URLs containing credentials/tokens are rejected. Connecting with human credentials ensures `ants_humans` contains `<name>` (`human` needs no entry) and runs an `ants init` check/upgrade pass. Ant credentials skip `ants_humans`, warn, refuse `ants inbox`, and leave `--wait`/`-f` replies unread.
- **`ants setup`:** idempotently writes `~/.config/ants/nats.conf` (JetStream in `~/.ants/jetstream`, skipped for non-loopback `--nats` or existing `leaf.conf`), installs/starts `ants.nats` / `ants-nats`, runs `ants init`, creates/appends `~/.ants/config.toml`, runs `ants harness check`, installs/starts/reloads `ants.daemon` / `ants-daemon`, installs shell completions, and runs `ants doctor --no-harness` (`--dry-run` previews without changes).
- **Shell completions:** `ants completions --install` writes to `~/.local/share/bash-completion/completions/ants`, `~/.local/share/zsh/site-functions/_ants` (add to `fpath` before `compinit`), or `~/.config/fish/completions/ants.fish` (`XDG_DATA_HOME`/`XDG_CONFIG_HOME` honoured). Dynamic lookups (`ANTS_COMPLETE`, 2 s timeout, silent on error, skipped for OS-keystore seeds) complete `@ant` / ant names, channels, `last` + recent 7 d threads (with thread `title`, else latest line), the latest 50 messages to or from the CLI's human over 7 d matching the typed start (with route and first line) and suggested reactions on `ants react`, pending tool requests, and dead-letter sequences.
- **`ants doctor`:** read-only end-to-end check (`ok`, `warn`, `fail`, `skip`, with `fix` commands; 5 s per request, 10 s per network check, 60 s per harness handshake). Checks `config`, CLI `credentials` + OS `credentials key`, `server` + `jetstream`, `hub` objects, `consumers` (`≥ 80%` of `max_consumers`), team `versions` + cross-machine `harness versions`, JetStream `storage`, per-ant `credentials` / `harness` / shared git `workdir`, `hub resolver` (`full` resolver) + `team keys backup` (when `--dir` holds team keys), `leaf` / `leaf uplink` / `leaf system uplink` / `leaf node` / `outbox`, installed `daemon` / `leaf node` / `hub` (the `ants setup` nats-server of `~/.config/ants/nats.conf`, without `leaf.conf`) / `webhook` services, `webhooks.toml` secrets (`0600`) / `exec` commands / `public_url` `/_approval` probe, and installed `devtunnel` / `bridge` services, tokens and reachability.

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
| `ANTS_TEST_JS_ACK_FC_V2` | `cargo test` | When `1`, enable `js_ack_fc_v2: true` on test `nats-server`s (2.14+) |

`contrib/skills/ants/SKILL.md` teaches external coding agents to use the `ants` CLI (`copilot skill add contrib/skills/ants/SKILL.md`).

## Delivery

### Composing and waiting

- **Input:** `ants ask` and `ants post` take trailing words joined by spaces, `--file PATH` (`--file -` reads stdin; preserves internal formatting, strips BOM and trailing newlines), or `--edit` (`$VISUAL` -> `$EDITOR` -> `vi`/`notepad` on a `0600` temp file; empty/blank messages abort).
- **Waiting:** `--wait` prints the first reply and exits `0` (`--timeout` exits `1` on timeout; Ctrl-C exits `130`); `-f`/`--follow` streams replies until `--timeout` or Ctrl-C (`0`), marking printed replies read, plus reactions added/removed to the sent message and printed replies (human creds; `dev-a reacted eyes to message <id> (alice: <opening>)`). On a terminal with human credentials, stderr displays a live in-place status line (`not served`, `queued behind N`, `working`, `thinking`, `writing`, `running <tool>`, `waiting for approval` with `ants tools approve <ant>/<id>`, `harness signed out`).

### Pool, worktrees and at-least-once retries

Each ant runs up to `max_harnesses` (default 4) ACP processes, concurrent across threads and sequential per thread. Idle harnesses stop after `idle_timeout` (`10m`) and resume via ACP `session/load` (`<ant>.<thread>` in `ants_sessions`), rebuilding from stored thread history if `session/load` fails ([docs/config.md](docs/config.md#lost-sessions-and-context-window)). First Ctrl-C/`SIGTERM` drains active prompts; second Ctrl-C exits immediately.

With worktrees on, each thread gets its own git worktree at `<home>/worktrees/<thread>` on branch `ants/<ant>/<thread>` from `HEAD`, pruned once `<ant>.<thread>` expires from `ants_sessions` (preserving branches with unmerged commits and appending an `[ants]` notice). Worktrees are on by default for a workdir inside a git work tree, and not ignored by it, with `max_harnesses > 1` (logged at daemon start); `worktrees = true` (`--worktrees`) forces them on and `worktrees = false` (`--no-worktrees`) keeps every thread in the shared workdir. Trade-offs: one checkout per thread (plus its build outputs) on disk, a branch per thread to merge or delete, and threads not seeing uncommitted changes of the workdir; see [docs/config.md](docs/config.md#worktrees). `ants daemon` and `ants doctor` warn when `worktrees = false` leaves `max_harnesses > 1` sharing a git workdir.

In-hand messages send 20 s progress acks against a 60 s `ack_wait`, acking only after storing the reply:

1. Claims `<ant>.msg.<id>` = `started` in `ants_attempts` (unless already `withdrawn`), then writes `<ant>.<seq>` before prompting so redeliveries after a crash prepend a prior-tool-call warning.
2. Saves the finished reply in `<ant>.<seq>` and `<home>/answered` before publishing with 24 h dedup ID `reply-<ant>-<envelope_id>`, preventing re-prompts across post-generation disconnects or offline leaf outboxes, then marks `<ant>.msg.<id>` = `finished`.
3. Streams unpersisted `StatusEvent` JSON on `ants.status.<ant>.<thread>` (`state`, 250 ms batched `message`/`thought`, `tool`, `plan`, `queued`, `approval`, `escalated`, `decided`).

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
- [Harnesses](docs/config.md#harnesses) (`copilot`, `claude`, `antigravity`, Codex, `harnesses.d` drop-ins, sign-in/rate-limit/quota/failure holds, session rebuilds and context windows)
- [Prompt limits and spending caps](docs/config.md#prompt-limits-and-spending-caps) and [Spend report](docs/config.md#spend-report)
- [Tool permissions](docs/config.md#tool-permissions) (allowlist rules, `--rule` suggestions, chain approvals, webhook/schedule narrowing) and [Redaction](docs/config.md#redaction)
- [Schedules](docs/config.md#schedules) (`[[schedule]]` `cron` / `every`, time zones, `catch_up`)

## Roster, identity and memory

Each daemon publishes a `RosterEntry` at `<ant>` in `ants_roster` every 10 s (and immediately on state transitions), deleting it on clean exit (`30s` TTL with `MaxAge` delete markers; `ants roster` marks entries without a heartbeat for 20 s as `stale`):

- **Identity & runtime:** `machine`, daemon `instance`, `harness`, `agent` (`{name, version}` from ACP `initialize`), `workdir`, `capabilities`, `whoami`, `escalate_to`, `version`, `perms`, `started_at`, `heartbeat_at`, `activity` (`idle` / `working`), `handling` count, live `harnesses` count.
- **Hold states:** `signed_out` (`{since, sign_in?}`), `limited` (`{kind: "rate_limit" | "quota", since, until}`), and `failing` (`{since, error}`).

Per-ant prompt instructions and state isolation:

- `whoami` (`--whoami TEXT`): role description prepended to every prompt and published in `ants_roster` (keep secrets out; `route` matches `capabilities`, not `whoami`).
- `memory` and `tasks` (`--memory`, `--tasks`): private instructions prepended to every prompt.
- `env` (`--env NAME=VALUE`): literal environment overrides passed to the harness and tool subprocesses (e.g. `BD_DB = "/Users/alice/.ants/dev-a/.bd/bd.db"`).

## MCP tools

Each session gets the `ants` MCP server over streamable HTTP (`127.0.0.1:<port>/mcp` with per-thread bearer token) or via `ants mcp --socket <home>/mcp.sock` (`0600`, `ANTS_MCP_TOKEN`; current-user named pipe on Windows). Allowlist rules match `other:<tool>`:

| Tool | Description |
|---|---|
| `send_message(to, text, thread?)` | Send to an ant or human (`thread` omitted: current; `"new"`: fresh; else ID); warns when unserved or limited. |
| `ask_ant(to, text, timeout?)` | Ask an ant on the current thread and wait (`timeout` default `5m`, max `30m`; refuses humans, unserved ants, or limits outlasting `timeout`). |
| `check_inbox(all_threads?)` | Drain late `ask_ant` replies arriving after timeout (persisted in `<home>/mcp.json`). |
| `react(reaction, message_id?, remove?)` | Add or remove the ant's reaction on a message (default: active message); 0 turns, wakes nobody. |
| `list_ants(capability?)` | List peer ants from `ants_roster` with capabilities, `agent`, activity and hold states. |
| `route(capability, text, wait?, timeout?)` | Route to the best peer advertising `capability` (healthy > held, `live` > `stale`, `idle` > `working`, fewest messages); waits unless `wait: false`. |
| `post_notes(text)` / `read_notes(thread?)` | Append to (1 turn) or read (up to 50 posts, 0 turns) passive thread notes; wakes nobody. |
| `decide_approval(verdict)` | Answer an incoming chain approval notice (`allow_once`, `allow_always`, `reject`). |
| `post_channel(channel, text)` | Start a channel thread as `creator` (member or channel messenger only; inherits turns minus 1 for ant messages). |
| `manage_channel(channel?, add?, remove?, messenger?, clear_messenger?)` | List owned channels, inspect one, or modify members/messenger via `ants channel serve`. |
| `escalate(issue)` | Escalate the thread one level up its origin chain (or to the owner) with reason `asked`. |
| `suggest_thread_title(title)` | Suggest a title for the current thread (1–120 trimmed single-line chars); the turn's reply carries it, and a human's `ants thread list` or `show` stores it only if the thread never had a title. |
| `request_ant(...)` / `retire_ant(name)` | Template managers only: add or retire a templated worker ant via `ants signer` ([docs/config.md](docs/config.md#manager-ants)). |

Human traffic costs 0 turns; ant-to-ant messages cost 1 turn (`ask_ant` requires 1 turn left for the reply), and self-messaging is refused. A 3 s `ants_roster` lookup runs before messaging ants, suggesting closest names or capability peers when the target is unserved.

## Messenger and thread notes

- **Messenger:** unaddressed `ants ask` and unmentioned human/webhook channel posts route through `messenger`. Unless `messenger = false` or `[[ant]]` defines `messenger`, daemons run a built-in `messenger` (`~/.ants/messenger`, `~/.config/ants/auth/messenger.creds` if present) with automatic standby failover via `ants_roster` (~1 s on exit or 30 s TTL expiry), yielding immediately to an explicit `[[ant]] name = "messenger"`. It suggests a short title (`suggest_thread_title`) for a thread a human's request starts.
- **Thread notes:** `ants.notes.<thread>.<from>` provides a passive shared board per thread (wakes nobody). An ant's `post_notes` spends 1 turn via CAS from `min(message_budget, last_note_budget)`; exhausting turns escalates `notes` until a human runs `ants notes <thread> TEXT` (8 fresh turns) or escalation climbs a level.

## Threads, search, titles and marks

`ants thread list`, `show` and `search` read `ANTS` with human credentials via batched direct gets (`$JS.API.DIRECT.GET.ANTS`, 256 messages/request; falling back to `STREAM.MSG.GET` on pre-v3 credentials or older servers), consuming and marking read nothing. Entry kinds: `message`, `escalation`, `mention`, `post`, `note`, `approval`, `decision`, `dead` (subject mismatches marked `(forged)`).

- **Search (`ants thread search`):** combines `--from`, `--to`, `--channel`, `--type`, `--grep` (case-insensitive), `--since` (inclusive, default 24 h before `--until`), `--until` (exclusive, default now; accepts durations, RFC 3339, or UTC `YYYY-MM-DD[ HH:MM]`), and `--limit` (default 50). `--from`, `--to` and `--type` prune scanned subjects server-side.
- **Titles (`ants thread title`):** 1–120 trimmed single-line chars stored at `title.<thread>` in `ants_threads`, shared across all humans and shown in `thread list`, `thread show` and tab completion. A title an ant suggests (`suggest_thread_title`, carried as `title` on its reply) is stored by the next `ants thread list` or `thread show` of a human, by compare-and-set and only if `title.<thread>` never had an entry, with `by` naming the ant: a title a human set, renamed or cleared always wins. `thread show` marks it `(suggested by <ant>)`.
- **Pins & archives (`ants thread pin|unpin|archive|unarchive`):** per-human CAS state at `marks.<human>` in `ants_threads` (mutually exclusive). Pinned threads sort last (nearest the prompt) and survive `--limit` truncation; archived threads hide from `ants thread list` and `ants dash` until new messages arrive (`archived+new`). Requires hub reachability.
- **Auto-archive (`auto_archive` in `[cli]`):** off by default; with e.g. `auto_archive = "14d"`, a thread neither pinned nor archived with no message, post, note or other entry stored for that long counts as archived (`auto-archived` in STATE, `auto_archived` in `--json`) and hides from `ants thread list` and `ants dash`, read-time only (nothing is written). Any new entry on it makes it active again. `ants thread list --archived` then reads all `ANTS` keeps and lists them with the archived ones; `ants thread unarchive` stores a `kept` time in `marks.<human>`, idling it again only after another `auto_archive` without activity; pinning or archiving drops it.
- **Pruning:** `ants_threads` never expires, so `ants thread list` forgets marks and titles of threads with no message left in `ANTS` (30 d retention). After reading back to a thread's start (UUIDv7 start minus 5 min clock slack; the whole stream for `--since 0` or non-time-ordered IDs) and finding none of its messages, the list drops the human's pin, archive or `kept` time placed more than 30 d ago (CAS, retrying on conflicting devices) and, when the read reached back 30 d or more, deletes titles set more than 30 d ago, conditioned on their revision so a concurrently re-set title survives. The 30 d grace covers messages still delayed in a leaf outbox. On the same CAS write it drops `kept` times of threads it read a message on stored at or after them, which no longer change when they idle. `--pinned`/`--archived` reads back to the earliest marked thread, so expired marks clear there; `--since 0` clears all expired titles.

## Dashboard and reactions

- **`ants dash [--since DUR]`:** full-screen terminal UI (human creds) showing active threads (`▶` while busy), selected thread live progress and history, 10 s roster refresh, and live pending tool requests (`Enter` opens details; `a`/`A`/`r` then `y` confirms `allow_once`/`allow_always`/`reject`; `Tab` switches panes, `R` refreshes, `q`/`Ctrl-C` quits). Records approval presence while open.
- **Reactions (`ants react MESSAGE REACTION [--remove]` / MCP `react`):** stores `<reactor>.<message_id>.<reaction>` in `ants_reactions` (30 d TTL; requires hub reachability). Users write/delete only their own keys; humans read all reactions in `ants inbox`, `ants thread show` and `ants watch`, and follow them live in `ants inbox -f`, `ants ask -f` and `ants post -f`; reactions never end a wait or mark anything read. A human's `MESSAGE` may be a unique start of an id: matched first among messages to or from the human stored in the last 7 d, then among any stored then (narrowed by the time in a time-ordered id); several or no matches are refused with a list of candidates. Ant credentials need the whole id.

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

`ants webhook serve` (`~/.ants/webhooks.toml`) authenticates incoming HTTP `POST <path>/<webhook>` requests (`hmac-sha256`, `bearer`, `token`, `standard-webhooks` such as GitLab's signing token), deduplicates events via `ants_webhooks`, and starts narrowed inbox or channel threads (`ants webhook add` provisions credentials, secrets and presets for GitHub, GitLab and Grafana, or takes a secret given, such as GitLab's signing token). `[[human]]` entries in the same file push tool approvals (with signed `/_approval` buttons), escalations, unserved-ant notices and optional completions to `ntfy`, `slack`, `telegram`, `exec` and `http` sinks, respecting per-device presence, quiet hours, snoozes and muted threads, with an optional digest of what quiet hours or a snooze held back once they end. See [docs/webhooks.md](docs/webhooks.md).

## Credentials

`ants auth` manages operator, account and system NKeys (`~/.config/ants/auth`), role-scoped `.creds` (ants, humans with per-device IDs, webhooks, observers, `channel-manager`, leaf uplinks), account revocations (`ants auth revoke` / `push`), and passphrase-encrypted `age` backups (`ants auth backup` / `restore`). `ants add`, `ants renew` / `ants enroll`, and `ants keys store` handle local key generation, over-NATS enrollment/renewal, and OS credential stores (macOS Keychain, Linux Secret Service, Windows Credential Manager). Renew all credentials to permissions version `4` before upgrading any server to `nats-server` 2.16 (`js_ack_fc_v2`). See [docs/auth.md](docs/auth.md).

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

- **Reload & restart:** `ants.daemon` and `ants.webhook` auto-reload edited config files (~6 s) and accept `SIGHUP` (`launchctl kill SIGHUP gui/$(id -u)/<label>`, `systemctl --user reload <unit>`); `ants add`, `ants webhook add`, `ants renew`, `ants leaf config`, `request_ant` and `ants tools approve --rule` reload or restart matching running units automatically. For renewed leaf uplink credentials (`ants add --enroll`, `ants renew`, `request_ant`), `ants.nats` gets a reload that reconnects only the uplink, keeping local connections (`nats-server` 2.14+, as the running leaf node reports; `ants add` and `ants renew` restart older ones).
- **Logs & linger:** on macOS, `ants.logrotate` copy-truncates logs > 10 MiB hourly (keeping `.1`..`.5.gz`). On Linux, enable `loginctl enable-linger $USER` (`[boot] systemd=true` in `/etc/wsl.conf` on WSL 2). Remove units with `launchctl bootout gui/$(id -u)/<label>` or `systemctl --user disable --now <unit>`.

## Monitoring

`ants daemon`, `ants webhook serve`, `ants bridge serve` and `ants bridge connect` expose unauthenticated loopback HTTP health and Prometheus metrics when `--metrics ADDR` (`ANTS_METRICS` on daemon) or config `metrics` is set (read at startup):

- `GET /healthz`: `200 ok` when healthy (daemon: ≥ 1 ant served and all NATS connections up; webhook: all webhook and human connections up; bridge: running), else `503`.
- `GET /metrics`: Prometheus text format (`text/plain; version=0.0.4`).

| Metric | Type | Labels | Description |
|---|---|---|---|
| `ants_nats_connected` | gauge | `ant` / `user` | NATS connection state (`1` up, `0` down) |
| `ants_harnesses` | gauge | `ant` | Live harness processes |
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

`ants hook run --on EVENT [--concurrency N] [--timeout DUR] -- CMD [ARGS...]` runs `CMD` once per `approval-request`, `approval-decision`, `dead-letter`, `escalation`, `decided`, `status` or `roster` event with its JSON document on stdin (`--observer` credentials; default concurrency `4`, timeout `60s`). Stored feeds resume from durable consumers. See [docs/hooks.md](docs/hooks.md).

## Footprint

`scripts/footprint.sh` starts a throwaway `nats-server -js` and `ants daemon`, samples RSS via `ps`, and fails at or above `--max-mb` (default `20` MB; `--ask TEXT` measures a live session; `--metrics ADDR` scrapes `/healthz` and `/metrics`):

| Scenario (`ants` 0.1.0 release, `nats-server` 2.15.3) | Linux x86_64 | macOS arm64 |
|---|---|---|
| Idle daemon | 5.9 MB | 8.5 MB |
| Idle daemon with MCP server / one Copilot session | — | 10.5 MB |
| Copilot CLI harness processes | — | 255 MB |

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
- **Wire snapshots:** `crates/ants-proto/tests/wire_snapshots.rs` pins the JSON form of every wire type in [insta](https://insta.rs) snapshots, so a renamed field or variant fails `cargo test`; after an intended wire change, review them with `cargo insta review` (or `INSTA_UPDATE=always`) and keep older peers reading the new form.

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
| `docs/webhooks.md` | HTTP webhook ingress, `ants webhook add` presets, human notification sinks, quiet hours/snooze/mute/digest, `/_approval` links, JSON notice v1 |
| `docs/transport.md` | Hub, reverse proxy, revocation sync, leaf nodes, store-and-forward, HTTPS bridge, Dev Tunnels, keepalive |
| `docs/hooks.md` | Hook contract: observer credentials, feed subjects and JSON schemas, versioning |

Work items and specifications live in `bd` (`.bd/`): `bd ready`, `bd spec list`, `bd spec show <area>`. The database stays local; `.bd/issues.jsonl` is a committed snapshot (`bd export -o .bd/issues.jsonl`), loaded into a fresh workspace with `bd init --prefix ants` and `bd import .bd/issues.jsonl`.

## Security

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
