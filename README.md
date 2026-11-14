# ants

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
| nats-server (leaf, outbox)  --+----->|      ants_webhooks       |<-----+-- nats-server (leaf)     |
+-------------------------------+      +--------------------------+      +--------------------------+
                        uplink: wss://, tls://, Dev Tunnel, or ants bridge
```

- **Hub:** one `nats-server` with JetStream holding stream `ANTS` and nine KV buckets (`ants init`). With `ants auth`, every ant, human, webhook, leaf uplink and channel manager connects using role-scoped NKey/JWT credentials.
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
| KV `ants_webhooks` | File, 7d | Delivered webhook event IDs (`<webhook>.<hash>`), sent schedule firings (`<schedule>.<unix time>`) and daily `<webhook>.webhook` markers of webhooks and schedules. |
| Core NATS | Unstored | `ants.status.<ant>.<thread>` (progress), `ants.cancel.<ant>.<thread>` and `ants.cancelled.<ant>.<thread>` (`ants cancel` and the daemons' answers), `ants.chanadm.<channel>.<ant>` (owner ops), `ants.enroll.<ant>[.reply]` (enrollment). |

User, channel, thread and JetStream domain IDs use 1–64 characters of `a-z`, `0-9` and `-`, starting with a letter or digit (`channel-manager` is reserved). Machine and harness names also allow `_` (machine names allow uppercase ASCII and must differ from `js_domain`). Durations require whole-number units ordered largest first (`s`, `m`, `h`, `d`: `90s`, `10m`, `1h30m`) or `0`; bare non-zero numbers are rejected.

## Install

Requirements:

- Rust 1.100 or later.
- [nats-server](https://github.com/nats-io/nats-server/releases) 2.11 or later (tested with 2.15.3).
- A signed-in harness CLI on machines running `ants daemon` (see [Harnesses](#harnesses)).

```sh
cargo install --path crates/ants        # ~/.cargo/bin/ants
ants --help
```

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
| `ants ask [--thread ID] [--wait \| -f] [--timeout DUR] [@ant] TEXT` | Send to `@ant` (`messenger` if omitted) and print the thread ID. `--thread` continues an existing thread. |
| `ants inbox [--peek] [--thread ID] [--limit N] [-f] [--json]` | Print unread replies and mark read (`--peek` keeps unread; `-f`/`--follow` streams replies and watches advisories). |
| `ants roster [--json]` | List ants (`live`/`stale`, `idle`/`working`), harnesses, capabilities, `escalate_to` chains and workdirs. |
| `ants spend [ANT] [--thread ID \| --days N] [--json] [--config FILE]` | Report tokens and cost spent (see [Spend report](#spend-report)). |
| `ants notes THREAD [TEXT]` | Show thread notes, or post `TEXT` as the CLI human (resets budget to 8 turns; wakes nobody). |
| `ants post [--thread ID] [--wait \| -f] [--timeout DUR] CHANNEL TEXT` | Post to `ops` or `'#ops'` and print the thread ID (`@ant` wakes members; `@human` notifies members; no mention triages). |
| `ants channel create \| set \| add \| remove \| list \| delete` | Manage channels: `--member USER,...`, `--messenger ANT` (`--default-messenger` on `set`), `--owner ANT` (`--no-owner` on `set`). |
| `ants channel read CHANNEL [--thread ID \| --limit N]` | Print channel posts grouped by thread. |
| `ants channel serve` | Run the channel manager (`ants auth channel-manager` creds) to apply owner `manage_channel` requests. |
| `ants thread list [--ant USER] [--owner HUMAN] [--grep TEXT] [--since DUR] [--limit N] [--json]` | List threads with messages stored within `--since` (default `24h`, `0` for all kept): id, owner, users and channels, first/last time, message count, opening line; most recently active last, up to `--limit` (default 50). `--grep` keeps threads with a message containing `TEXT` (case-insensitive) and prints the matching line. |
| `ants thread show THREAD [--since DUR] [--json]` | Print a thread's history in stored order: inbox messages both ways, channel posts, notes, escalations, tool requests and decisions, dead letters (see [Thread overview](#thread-overview)). |
| `ants watch [ANT [THREAD] \| SUBJECT]` | Stream traffic (default `ants.>`), escalations and decisions, or live harness progress for `ANT`. |
| `ants cancel ANT THREAD` | Stop the prompt `ANT` runs on `THREAD` (see [Prompt limits](#prompt-limits-and-spending-caps)); fails when no daemon of `ANT` runs one there or none answers within 5s. Requires human credentials. |
| `ants tools list [-f] [--json]` | List pending tool requests (`<ant>/<id>`, thread, tool, files, options, time remaining). |
| `ants tools show ID [--json]` | Show full request, tool input, status and answer commands for `<ant>/<id>` (unique prefix accepted). |
| `ants tools approve ID [--always]` / `reject ID` | Allow once, allow for the session (`--always`, if offered), or reject a tool request. |
| `ants dead list` / `replay SEQ` / `drop SEQ` / `watch` | Inspect, replay or delete dead letters; `watch` turns `MAX_DELIVERIES` advisories into dead letters. |
| `ants init` | Create or update stream `ANTS` and all nine KV buckets (requires human credentials). |
| `ants daemon [--ant NAME] [--config FILE] [ant-flags...]` | Serve config-file ants, or one ant selected/defined by `--ant` (`ANTS_ANT`). |
| `ants add NAME [--config FILE] [--out FILE] [--jwt JWT \| --enroll CREDS [--hub URL]] [ant-flags...]` | Add an ant to `config.toml` and generate its user NKey locally (see [Adding an ant](#adding-an-ant)). |
| `ants enroll list` / `approve NAME` / `reject NAME [--reason TEXT]` | List, sign or refuse `ants add --enroll` requests with the account key (`--public-key KEY` disambiguates). |
| `ants harness check` / `prompt TEXT` | Test ACP handshake or run one prompt without NATS (`--harness NAME`, `--config FILE`; rejects tools). |
| `ants auth init` / `user NAME` / `enroll NAME` / `channel-manager` / `leaf MACHINE` | Create team keys and `server.conf`, or issue `.creds` (`user`: `--human`, `--webhook --to ANT --channel CH`; `enroll`: `--ttl DUR`, default `1h`, `0` for no expiry). |
| `ants auth revoke NAME` / `revoked` / `push` | Revoke every key issued to `NAME` (or `--key U...`), rewrite `server.conf` and push the account to the `--nats` server (`--no-push` to skip); list revoked keys; push the account again. |
| `ants leaf config MACHINE --uplink URL ...` / `outbox MACHINE ...` | Write `~/.config/ants/leaf.conf`; configure or `--remove` store-and-forward outbox sourcing. |
| `ants bridge serve` / `connect URL` | Tunnel a leaf uplink over plain HTTPS `POST`/`GET` requests (`--token-file FILE` reads the bearer token from a file). |
| `ants webhook serve [--config FILE] [--listen ADDR]` | Serve HTTP webhook ingress, and push tool requests and escalations to humans' ntfy, Slack or Telegram with approve/reject links (see [Webhooks](#webhooks) and [Notifications](#notifications)). |
| `ants mcp --socket PATH --token TOK` | Internal stdio-to-socket MCP bridge launched by `ants daemon`. |

### Identity and human registration

Every command accepts `--nats`, `--creds` and `--js-domain`. A NATS URL carrying a user and password or a token (`nats://user:pass@host`), on the command line or in a config file, is refused: credentials come only from `.creds` files. The CLI acts as the user named in the `--creds` JWT, or `human` without credentials. With human credentials (`ants auth user NAME --human`, or `human`), connecting ensures `ants_humans` exists and registers `<name>` (direct get first, put if absent; `human` needs no entry; warns and continues on failure, though daemons treat unregistered senders as agents until registered). With ant credentials, the CLI acts as that ant, skips `ants_humans`, warns, refuses `ants inbox` and leaves replies unread during `ants ask --wait` or `--follow`.

### Environment variables

| Variable | Flag / Scope | Default / Behavior |
|---|---|---|
| `ANTS_NATS_URL` | `--nats` (global) | `nats://127.0.0.1:4222` |
| `ANTS_CREDS` | `--creds` (global) | Path to `.creds` file; omitted connects unauthenticated as `human` |
| `ANTS_JS_DOMAIN` | `--js-domain` (global) | Hub JetStream domain (`$JS.<domain>.API`) |
| `ANTS_CONFIG` | `--config` (`daemon`, `add`, `harness`, `spend`) | `~/.ants/config.toml` |
| `ANTS_ANT` | `--ant` (`ants daemon`) | Serve only the named ant (from config file if `--config` is set, else from CLI flags) |
| `ANTS_AUTH_DIR` | `--dir` (`auth`, `enroll`, `leaf config`) | `~/.config/ants/auth` |
| `ANTS_BRIDGE_TOKEN` | `--token` (`bridge serve`, `bridge connect`) | Optional bearer token gating bridge sessions; `--token-file FILE` reads it from a file instead |
| `ANTS_MCP_TOKEN` | `--token` (`ants mcp`) | Per-thread session token passed automatically by `ants daemon` |
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
- `--timeout DURATION` (requires `--wait` or `--follow`) bounds the wait. If `--wait` times out, `ants` prints `no reply on thread <thread> within <timeout>` to stderr and exits `1`. Ctrl-C during `--wait` exits immediately as interrupted (`130`). `ants ask --wait` and `--follow` mark only printed replies read.

### Inbox consumers, pool and retries

Human replies wait in durable consumer `inbox-<name>` (`inbox-human` for `human`) until read. Each ant maintains up to `max_harnesses` (default 4) ACP processes, one per active thread: concurrent across threads, sequential per thread, retrying failures in order. Idle harnesses stop after `idle_timeout` (`10m`) and resume via ACP `session/load` using `<ant>.<thread>` in `ants_sessions`; busy harnesses are never evicted. First Ctrl-C or `SIGTERM` drains prompts in flight; a second Ctrl-C exits immediately for redelivery.

Threads of one ant share its workdir unless `worktrees = true` (or `--worktrees`) gives each thread its own git worktree of the workdir's repository: `<home>/worktrees/<thread>` on branch `ants/<ant>/<thread>`, started from the repository's `HEAD` (uncommitted changes in the workdir are not carried over). Sessions of the thread work at the workdir's place inside it, and allowlist paths resolve against it. The worktree is created on the thread's first prompt, reused when its harness starts again (`session/load` sees the same directory), and removed with `git worktree remove --force` once `<ant>.<thread>` has expired from `ants_sessions` (checked at start and hourly, for threads with no live harness), deleting the branch unless it holds commits not merged into the repository's `HEAD`. A workdir outside a git work tree, or a missing `git`, is logged once and stays shared; a worktree that cannot be created (an empty repository, the branch checked out elsewhere) logs a warning and the thread works in the shared workdir.

Held messages send 20s progress acks against a 60s `ack_wait` (ignoring duplicate deliveries while held). Delivery is at-least-once and acks only after storing the reply:

1. Before prompting, the daemon writes `<ant>.<seq>` in `ants_attempts`. On redelivery after a cut-off, finding that marker resumes the session and prepends a warning that prior tool calls may have run.
2. When the harness finishes, the reply is saved in `<ant>.<seq>` before publishing with dedup ID `<ant>/<envelope_id>` (2m window), so a post-generation cut-off republishes without re-prompting.
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

A harness definition specifies an ACP stdio executable, arguments, environment additions, required ACP session mode and valid `context` options. Built-in definitions live in `crates/ants-daemon/src/harnesses.toml`:

| Name | Command | Forced ACP mode | Sign-in |
|---|---|---|---|
| `copilot` (default) | `copilot --acp` | none | `copilot login` |
| `claude` | `claude-agent-acp` (`npm install -g @agentclientprotocol/claude-agent-acp`) | `default` | `claude login` or `ANTHROPIC_API_KEY` |
| `antigravity` | `agy acp --disable-telemetry` | `default` | `agy` / `agy acp --login` or Gemini API key |

Add new harnesses or replace a built-in whole under `[harnesses.NAME]`:

```toml
[harnesses.custom-acp]
command = "~/.local/bin/custom-acp"            # path if containing '/', else resolved on PATH
args = ["--stdio", "--log-level", "warn"]
env = { CUSTOM_ACP_TELEMETRY = "off" }         # layered: daemon env < harness env < ant env < --env
mode = "default"                               # session fails if harness lacks this mode

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
# env = { DEFAULT_AUTH_REQUEST = '{"methodId":"api-key"}' }  # sign in with the API key from the environment
```

- **Claude Code:** forced to `default` mode (`auto` approves tools internally). Built-in read-only tools/commands (`Read`, `Grep`, `ls`, `echo`) and `~/.claude/settings.json` / `.claude/settings*.json` rules still run unasked; loads `CLAUDE.md` and hooks.
- **Codex:** `read-only` asks before edits, network access and commands that need to leave Codex's read-only sandbox; commands that only read run in that sandbox unasked (`workspace-write` also edits the workdir unasked, `agent-full-access` never asks). Reports MCP tool calls as `execute` titled `mcp.<server>.<tool>` (matched by `other:` rules). Loads `~/.codex/config.toml` and `AGENTS.md`.
- **Antigravity CLI:** forced to `default` mode (`agentMode` `auto` runs edits/commands unasked). Workspace reads/edits and `~/.gemini/antigravity-cli/settings.json` rules still apply first; loads `GEMINI.md` and `AGENTS.md`. Keychain sign-in requires services to run as the signed-in OS user (logs: `~/.gemini/antigravity-cli/log/`).

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
- Spending uses ACP `usage_update` cost and prompt token totals stored at `<ant>.<thread>.spend` and `<ant>.day.<YYYY-MM-DD>` in `ants_sessions`, with or without caps. Claude's adapter reports both at turn end; Codex's adapter reports tokens only; Copilot CLI reports neither (so only prompt limits trigger); Antigravity reporting is unverified.

### Spend report

`ants spend` (human credentials) reads those keys:

- `ants spend`: one row per ant that spent anything in the last `--days` UTC days (default 7, today included, at most 366; `ants_sessions` keeps keys 30 days after their last write): today's tokens and cost, and their totals over the days.
- `ants spend ANT`: each day and the total, then each thread that spent anything since a human's last message on it, most costly first.
- `ants spend [ANT] --thread ID`: what the ant, or each ant on the thread, spent on it since a human's last message there.

Today's and each day's figures show as `spent/cap` against `max_daily_tokens` and `max_daily_cost`, and threads' against `max_thread_tokens` and `max_thread_cost`, for the ants the local config file (`--config`, `ANTS_CONFIG`, default `~/.ants/config.toml`) lists with caps; ants served from other machines show spend alone. Costs are in the harness's unit (USD for Claude), with two decimals. `--json` prints one object per line: per ant `{ant, days: [{day, tokens, cost}], total: {tokens, cost}, threads?, caps?}` (`threads` only for one `ANT`), or with `--thread` `{ant, thread, tokens, cost, caps?}`; `caps` holds the caps set (`thread_tokens`, `thread_cost`, `daily_tokens`, `daily_cost`).

## Roster, identity and memory

Each daemon publishes a `RosterEntry` per ant at `<ant>` in `ants_roster` (`machine`, daemon `instance`, `harness`, `workdir`, `capabilities`, `whoami`, `escalate_to`, start/heartbeat timestamps, `idle`/`working` activity, in-flight message count, live harness count), refreshing every 10s and on activity transitions, and deleting on clean exit. Entries expire after 30s; `ants roster` marks entries without a heartbeat for 20s as `stale`.

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
- **Chain decisions:** ants with `escalate_to` also send an `[ants]` notice (`approval` escalation) to every ant in `escalate_to` on the thread. Members handling that notice may call `decide_approval(verdict)` (`ants.inbox.<ant>.<decider>`); `escalate` is refused on approval notices. Earliest stored decision wins (human or chain member), and `ants tools` inspects both.

```sh
ants tools list -f                         # stream pending requests (--json: JSON lines)
ants tools show dev-a/019a7d2e --json      # full request + tool input (unique ID prefix accepted)
ants tools approve dev-a/019a7d2e --always # allow-always when offered by the harness
ants tools reject dev-a/019a7d2e
```

## MCP tools

Each session gets the `ants` MCP server over streamable HTTP (`127.0.0.1:<port>/mcp` with per-thread bearer token) or, without HTTP MCP support, via `ants mcp --socket <home>/mcp.sock` (`0600`, `ANTS_MCP_TOKEN`). Tools run on the active thread during a prompt (allowlist: `other:<tool>`):

| Tool | Description |
|---|---|
| `send_message(to, text, thread?)` | Send to an ant or human (`thread` omitted: current; `"new"`: fresh; else ID) and return once stored. |
| `ask_agent(to, text, timeout?)` | Ask an ant on the current thread and wait for its reply (`timeout` a duration such as `90s`: default `5m`, max `30m`; refuses humans). |
| `check_inbox(all_threads?)` | Drain late `ask_agent` replies arriving after timeout (current thread unless `all_threads`). |
| `list_ants(capability?)` | List peer ants from `ants_roster` (`whoami`, capabilities, harness, machine, activity). |
| `route(capability, text, wait?, timeout?)` | Pick a peer with `capability` (`live` > `stale`, `idle` > unknown > `working`, fewest messages, name); waits unless `wait: false`. |
| `post_notes(text)` | Append to the current thread's notes (1 turn; wakes nobody). |
| `read_notes(thread?)` | Read up to 50 notes posts (oldest first) and turns left on the current or named thread (0 turns). |
| `decide_approval(verdict)` | Answer an incoming chain approval notice (`allow_once`, `allow_always`, `reject`). |
| `post_channel(channel, text)` | Start a channel thread as `creator` (member/messenger only; inherits turns minus 1 for agent messages). |
| `manage_channel(channel?, add?, remove?, messenger?, clear_messenger?)` | List owned channels, show one, or change members/messenger via `ants channel serve`. |
| `escalate(issue)` | Escalate the thread one level up its origin chain (or to the owner) with reason `asked`. |

Human traffic costs 0 turns; agent-to-agent sends/replies cost 1 turn (`ask_agent` requires 1 turn left for the reply), and self-messaging is refused. `ask_agent` hears replies immediately on live inbox and `_INBOX.<ant>.stored.*` subscriptions without waiting behind the active prompt; late replies persist in `<home>/mcp.json` and prepend a count on later prompts until `check_inbox` drains them.

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

Agent posts spend 1 turn from the lower of the active message's budget and the last note's remaining turns, serialized with `Nats-Expected-Last-Subject-Sequence` (losers re-read and retry). Spending the final turn escalates the thread (`notes`) and blocks further agent note posts until a human runs `ants notes <thread> TEXT` (8 fresh turns) or an escalation climbs a chain level. Daemons read via ephemeral consumer `notes-<ant>`; CLI runs with ant credentials use spare consumers `notes-<ant>_1..3`; humans read directly via `STREAM.MSG.GET`.

## Thread overview

`ants thread list` and `ants thread show` read the `ANTS` stream back with human credentials by `STREAM.MSG.GET` (as `ants channel read` and `ants notes` do), so they consume, ack and mark read nothing and create no consumer; ant credentials are refused. Only notes subjects name their thread, so each read goes through every message stored in its window: `--since` bounds it (`list` defaults to `24h`; `show` starts at the thread's start when its id is a time-ordered UUID, as generated ids are, else reads all the stream keeps). Entries are kinds `message`, `escalation` (with level, reason, leader and ants visited), `mention`, `post`, `note`, `approval`, `decision` (a human's, which names no human, or a chain agent's) and `dead`; a human's decision is placed on its request's thread only when the request is in the window. A message stored on a subject naming another sender is marked `(forged)`. A thread's owner is the one its first message naming one gives; `list` counts only what was stored within `--since`, so a thread older than that shows its first message in the window as the opening.

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

Inbox and channel messages get 5 prompt attempts (`max_deliver = 6`). When attempt 5 fails — or on delivery 6 if attempt 5 crashed without recording a reply — the daemon writes a `DeadLetter` to `ants.dead.<ant>`, escalates a `dead_letter` notice on the thread, and terminates the consumer delivery. Unparseable or subject-mismatched payloads are dead-lettered immediately without envelopes so they cannot be replayed.

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
- **Deduplication and marker:** stored events return `202` (`{"thread": "...", "duplicate": false}`); repeats within 7 days match `<webhook>.<hash>` in `ants_webhooks` (plus 2m `Nats-Msg-Id` `webhook.<webhook>.<hash>`) and return `200` (`"duplicate": true`). `ants webhook serve` refreshes `<webhook>.webhook` in `ants_webhooks` daily.
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
```

- **One notice per event:** each human is its own NATS user and reads durable consumer `notify-<human>` on `ANTS` (deliver new, explicit ack), so several `ants webhook serve` processes listing the same human share it and each request or escalation is sent once. Requests already decided or expired, unreadable or forged ones, and escalations older than a day are skipped. Sinks run concurrently with a `10s` timeout; failures are logged (never with URLs or tokens) and not retried.
- **Content:** approval notices carry the ant, tool title and kind, files, an input excerpt, thread, time left and the `ants tools approve|reject <ant>/<id>` commands; escalation notices carry the reason, the stuck ant, the text and the `ants ask --thread` command to answer. Plain messages are not notified.
- **Approve/reject links:** with `public_url` and the action secret, approval notices carry one link per offered verdict (`Approve`, `Always allow`, `Reject`): `<public_url>/_approval?ant=&id=&seq=&verdict=&by=&exp=&sig=`, HMAC-SHA256 over every field, valid until the request expires. `GET` shows the request, its state and a confirm button; `POST` stores the decision with the human's credentials exactly as `ants tools approve|reject` does (first answer wins). Answers: `200` stored or shown, `400` incomplete link, `403` wrong signature, `404` unknown human or request, `405` not `GET`/`POST`, `409` already answered or expired, `410` past expiry, `503` hub unreachable. Without `public_url`, notices carry only the commands. Anyone holding a link can decide that one request, so keep sinks private (ntfy topics are public unless the server requires a token).

| Sink | Notice | Actions |
|---|---|---|
| `ntfy` | JSON publish, priority high for tool requests | HTTP action buttons that `POST` the link: one tap decides |
| `slack` | Incoming webhook, `mrkdwn` section | Link buttons opening the confirm page (Slack may flag them without an app interactivity URL; harmless) |
| `telegram` | Bot API `sendMessage`, link previews off | Inline URL buttons opening the confirm page |

Human credentials issued before notifications lack the `notify-<human>` consumer permissions: re-issue them with `ants auth user NAME --human`.

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
- **Identity:** a schedule is a webhook-role user (`ants auth user NAME --webhook --to ANT` or `--channel CH`, and `--webhook NAME` in `ants auth leaf`), checked at load: its credentials must name it, be no human's and allow its destination. Its messages carry `webhook: true` and it refreshes `<schedule>.webhook` in `ants_webhooks` daily, so replies and triage work as for a [webhook](#webhooks).
- **Once per firing:** several daemons running the same schedule, or one restarted, send each firing once: its key `<schedule>.<unix time>` in `ants_webhooks` (7 days) and its 2m `Nats-Msg-Id` `schedule.<schedule>.<unix time>` drop a repeat.
- **Missed firings:** a firing goes out up to 1 minute late, retried every 5s while the hub does not store it, and fires after a restart within that minute. A firing missed by more (no daemon running, hub unreachable, machine asleep) is logged and skipped, never made up.

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

Every key issued or signed here (`ants auth user`, `enroll`, `channel-manager`, `leaf`, `enroll approve`) is recorded in `issued.jsonl` (key, name, kind, issue time, expiry). Credentials stay valid until they expire, unless revoked:

```sh
ants auth revoke alice --nats wss://hub.example/nats        # every key issued to alice, on all devices
ants auth revoke --key UABC... --nats wss://hub.example/nats # one key
ants auth revoke leaf-macbook --no-push                     # a leaf uplink; enroll-<ant>, channel-manager alike
ants auth revoked                                           # key, name, revoked at, expiry
ants auth push --nats wss://hub.example/nats                # push the account again
```

`ants auth revoke NAME` takes the keys recorded for `NAME` plus those of `.creds` files in the team directory carrying that name (credentials issued before the registry), and fails when it knows none: revoke those by `--key`. Revocations go to `revoked.json` and into the account JWT of `server.conf` (NATS account revocations: JWTs of the key issued at or before the revocation are refused; new credentials for the name stay valid). The account is then pushed to the `--nats` server (the hub) as a short-lived system account user (`$SYS.REQ.ACCOUNT.<account>.CLAIMS.UPDATE`), which closes the revoked users' connections at once. When the hub is out of reach the revocations stay recorded; push them later with `ants auth push`. Either way, copy the new `server.conf` to the hub and every leaf node and reload them (`SIGHUP`): restarts otherwise bring back the old account, and leaf nodes authenticate their local clients themselves, so a revoked ant behind a leaf keeps working there until its leaf has it (or until `ants auth leaf` is issued again without it). A hub on a `server.conf` from before revocations accepts no system account connection, so the first push fails until it runs on a new one. Revocations of credentials that have all expired are dropped on the next `revoke` or `push`.

Copy `server.conf` next to the hub `nats-server` config and add `include "server.conf"` (`include` resolves relative to the config file even with a leading `/`; `server.conf` sets `max_control_line: 16384`). Run `ants init` once with human credentials to create `ANTS` and all nine KV buckets. When stream subjects, buckets, direct gets, role permissions or machine members change, re-run `ants init` and re-issue `.creds` (`ants auth user`, `ants auth leaf`, `ants leaf outbox`). Upgrades that withdraw stream info of `ants_sessions`, `ants_attempts` and `ants_owners` from ants and webhooks need new ant, webhook and leaf credentials only after every daemon and `ants webhook serve` runs the new version (older ones need that stream info); until then, old credentials keep working and keep the listing open. The same holds for upgrades that withdraw stream info of `ANTS` and `ants_webhooks` from ants, webhooks and the channel manager: those credentials then find JetStream through their own inbox consumer's info instead. Upgrades that add `ants_owners`, have it take records from leaf outboxes, or key it by recipient, need both for every ant, human and webhook, and `ants leaf outbox` again per machine: until then, ants' messages on threads no channel post started carry no owner, and their escalations go to the daemon's default human. Records keyed by thread alone, from before the recipient key, are not read. `ants spend` needs human and leaf credentials issued with its reads of spend keys. `ants cancel` needs new human and ant credentials, and new leaf credentials where they run on a leaf; until then the request is refused or unheard.

### Wire ACLs and verification invariants

- **Subject-pinned senders:** ants and webhooks publish only under their own sender token (`ants.inbox.*.<self>`, `ants.notes.*.<self>`, `ants.chan.*.<self>`, `ants.dead.<self>`, `ants.chanadm.*.<self>`, plus `ants.out.*` on leaves). Readers verify payload `from`/target fields against the NATS subject (`ants watch` marks mismatches forged). Only humans publish under arbitrary inbox senders (for `ants dead replay`), write `ants.approvals.<ant>.<id>`, publish cancel requests (`ants.cancel.<ant>.<thread>`, heard only by `<ant>`, which answers only on `ants.cancelled.<ant>.*`), write `ants_humans` and `ants_channels` (alongside `channel-manager`), or call `STREAM.MSG.GET`. Each human may create and read its own notification consumer `notify-<human>` only.
- **Scoped ant reads:** without `STREAM.MSG.GET`, ant credentials pin consumer filters (`inbox-<ant>[_1..3]`, `chan-<ant>`, `chanread-<ant>[_1..3]`, `notes-<ant>[_1..3]`, `deadread-<ant>`) and restrict direct gets to `ants.approvals.<ant>.*`, their own `$KV.ants_sessions.<ant>.>` and `$KV.ants_attempts.<ant>.>`, `$KV.ants_humans.*`, `$KV.ants_chains.*`, `$KV.ants_owners.>` and `$KV.ants_webhooks.*.webhook`. Users write only their own keys in `ants_roster`, `ants_chains`, `ants_links` and `ants_webhooks`. Only humans and webhooks with an ant destination write `ants_owners`, webhooks only `<thread>.<ant>` keys of those ants (humans also through a leaf outbox), and its stream refuses a second record of a key (`discard_new_per_subject`, deletes and purges denied), but drops one repeating the first's `Nats-Msg-Id` (`owner.<key>`, which every writer sets) within the 30 d retention, so a record sourced from an outbox never stalls the source. `ants init` and `ants leaf outbox` widen the duplicate window of a stream from before.
- **Private keys:** stream info with a subjects filter lists every key of a bucket, so only human credentials get stream info of `ants_sessions`, `ants_attempts` and `ants_owners` (to create them, and for `ants spend` to find spend keys). Humans direct-get only the spend keys of `ants_sessions` (`$KV.ants_sessions.*.*.spend` and `$KV.ants_sessions.*.day.*`), not ants' sessions. Daemons read their own keys there and owners by direct get and write their own keys by plain publish, and webhooks record owners by plain publish, so neither ants nor webhooks can list other ants' thread ids. Likewise stream info of `ANTS` lists every subject stored (who messages whom, approvals, dead letters) and that of `ants_webhooks` every webhook's events, so only human credentials get them: ants, webhooks and the channel manager find JetStream through the info of their own inbox consumer, ants open `ANTS` without its info and read back from the last message through their own reader, and a webhook records each event it delivered by a publish expecting no earlier record of it and reads it back by direct get.
- **Metadata trust:** daemons accept `owner` only for `human` or entries in `ants_humans`, and honour `webhook: true` only with `<sender>.webhook` in `ants_webhooks` (or from a human). Channel thread `owner` and `creator` come from stored root/human posts in `ANTS`; an ant's non-channel `owner` comes from `ants_owners` for its thread or its `origin_thread`, else is dropped: a channel `origin_thread` gives the owner its posts say, under the same participation check as `creator`; a message thread, its own or its `origin_thread`, gives the owner of its `<thread>.<sender>` record, else of the record of an ant `ants_links` traces the sender's link back to; non-channel `creator` requires `origin_thread` backed by participation or `<from>.<thread>.<to>` links in `ants_links` (max 32 hops).

### Adding an ant

`ants add` generates the ant's user NKey on the target machine (keeping `<ant>.nk` at mode `0600` until signed, never copying the seed elsewhere) and appends `[[ant]]` to `config.toml`:

```sh
# Out of band
ants add dev-c --workdir ~/src/app              # target machine: appends config, prints public key U...
ants auth user dev-c --public-key U... --js-domain hub   # auth machine: prints signed JWT
ants add dev-c --jwt eyJ...                     # target machine: writes dev-c.creds (accepts JWT, file or '-')

# Over NATS (direct to hub; leaf uplinks do not carry ants.enroll.*)
ants auth enroll dev-c                          # auth machine: writes enroll-dev-c.creds, valid 1h (--ttl; 0: no expiry)
ants add dev-c --workdir ~/src/app --enroll enroll-dev-c.creds --hub wss://hub.example/nats
ants enroll list --nats wss://hub.example/nats  # auth machine: inspect pending requests
ants enroll approve dev-c --nats wss://hub.example/nats --js-domain hub
```

Reload `ants daemon` (`SIGHUP`, see [Services](#services)) after adding an ant, and update `ants auth leaf` + `ants leaf outbox` if running behind a leaf node.

## Leaf nodes

Running a local `nats-server` leaf per machine isolates local `ants` processes on `127.0.0.1:4222` and buffers outbound traffic while offline. Configure `jetstream { domain: hub }` and `feature_flags { js_api_reply_v2: true }` on the hub, pass `--js-domain hub` (`ANTS_JS_DOMAIN=hub`) everywhere, and issue leaf credentials scoping all ants, humans and webhooks on that machine:

```sh
# Auth machine (issue user/webhook .creds first)
ants auth leaf macbook --ant dev-a --ant messenger --human alice --webhook github --js-domain hub

# Worker machine ("macbook", with server.conf and leaf-macbook.creds in ~/.config/ants/auth)
ants leaf config macbook --uplink wss://hub.example/nats       # writes ~/.config/ants/leaf.conf
nats-server -c ~/.config/ants/leaf.conf &
ants leaf outbox macbook --ant dev-a --ant messenger --human alice
```

`ants leaf config` accepts `nats-leaf://` (or `nats://`), `tls://`, `ws://` and `wss://` uplinks, `--ca-file FILE` and `--proxy http://host:port`; re-run it and send `SIGHUP` to `nats-server` to switch uplinks live. While offline, outbound inbox messages, replies, notes, channel posts, dead letters, `ants_links` entries and the `ants_owners` records of `ants ask` queue in local stream `ANTS_OUT` and drain via hub stream `ANTS_IN` into `ANTS`, `ants_links` and `ants_owners` on reconnect (`ants leaf outbox --remove` removes sourcing); a daemon holds a human's message from an outbox up to 10 s until its thread's owner lands. `ANTS` republishes stored inbox messages to `_INBOX.<to>.stored.<from>`; `ants ask --wait`/`--follow` polls stored replies every 5s (humans via `STREAM.MSG.GET`, ants via spare consumers `inbox-<ant>_1..3`, so ant credentials and leaf uplinks issued before need `ants auth user` and `ants auth leaf` again), and `ants post --wait`/`--follow` (human or ant) polls the channel's stored posts after its own every 5s, so replies stored during an outage or sourced from an outbox show once. Full hub, proxy, leaf, outbox, Dev Tunnel and keepalive details live in [docs/transport.md](docs/transport.md) (`Hub`, `Reverse proxy`, `Leaf nodes`, `Store and forward`, `HTTPS fallback bridge`, `Dev Tunnels`, `Keepalive and limits`).

### Transport options

| Leaf uplink | Use case |
|---|---|
| `wss://host/path` | Standard TLS WebSocket uplink behind a reverse proxy (supports `--proxy` HTTP `CONNECT`). |
| `tls://host:7422` | Direct TLS connection to the hub leafnode port. |
| `nats-leaf://127.0.0.1:7422` via `devtunnel connect` | Private Dev Tunnel forwarding the hub leafnode port (`--devtunnel` in `install-units.sh` on workers, `--devtunnel-host` on the hub). |
| `nats-leaf://127.0.0.1:7422` via `ants bridge connect` | Networks whose proxies block WebSocket; tunnels leaf traffic over plain HTTPS `POST`/long-polled `GET` (`--bridge URL` in `install-units.sh` on workers, `--bridge-serve` on the hub). |

For the HTTPS fallback bridge, run `ants bridge serve` (`127.0.0.1:8423` -> `127.0.0.1:7422`, optional `ANTS_BRIDGE_TOKEN` or `--token-file FILE`) behind the hub reverse proxy, and `ants bridge connect https://hub.example/bridge [--proxy URL] [--header 'Name: val']` (`127.0.0.1:7422`) on the worker. Run both as services with `scripts/install-units.sh --bridge-serve` on the hub and `--bridge URL` on the worker (see [Services](#services)). See [docs/transport.md](docs/transport.md#https-fallback-bridge).

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

- **Reload leaf uplink:** `launchctl kill SIGHUP gui/$(id -u)/ants.nats` (macOS) or `systemctl --user reload ants-nats` (Linux).
- **Reload daemon config (`ants add`):** `launchctl kill SIGHUP gui/$(id -u)/ants.daemon` (macOS) or `systemctl --user reload ants-daemon` (Linux).
- **Start order:** systemd starts `ants-nats` after `ants-devtunnel` or `ants-bridge`, and `ants-devtunnel-host`, `ants-bridge-serve`, the daemon, channel manager and webhook ingress after `ants-nats`. launchd has no start order: the leaf node retries its uplink until the tunnel or bridge listens, and the other services retry their connections.
- **Restart daemon:** `launchctl kickstart -k gui/$(id -u)/ants.daemon` (macOS) or `systemctl --user restart ants-daemon` (Linux; 120s graceful `SIGTERM` stop).
- **Remove units:** `launchctl bootout gui/$(id -u)/<label>` or `systemctl --user disable --now <unit>`, then delete the unit files.
- **Persistence:** Linux user units require `loginctl enable-linger $USER` to run without an active login session; WSL requires `[boot] systemd=true` in `/etc/wsl.conf`.

## Footprint

`scripts/footprint.sh` starts a temporary `nats-server -js` and `ants daemon`, samples resident memory with `ps`, and fails at or above `--max-mb` (default `20` MB). Pass `--ask "reply with ok"` to measure with one live harness session.

```sh
scripts/footprint.sh --port 14722
scripts/footprint.sh --port 14722 --ask "reply with ok"
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
cargo clippy --all-targets -- -D warnings
cargo test
cargo deny check        # licenses, advisories, bans, sources (deny.toml)
```

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

Work items and specifications live in `bd` (`.bd/`): `bd ready`, `bd spec list`, `bd spec show <area>`.
