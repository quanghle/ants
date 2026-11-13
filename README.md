# ants

Headless agent teams on several machines, talking to each other and to a human at the terminal over NATS.

- **Backbone:** NATS + JetStream for durable inboxes with acks and redelivery, and NATS KV for the roster, sessions and
  attempt markers.
- **Transport:** any uplink that carries NATS: `wss://`/`ws://` through a reverse proxy, `tls://`, or a Dev Tunnel to
  the hub's ports, optionally through an HTTP `CONNECT` proxy, or the HTTPS fallback bridge (`ants bridge`). See [Transport options](#transport-options).
- **Harnesses:** agents run behind ACP (Agent Client Protocol) over stdio: Copilot CLI (`copilot --acp`, the default),
  Claude Code through its ACP adapter (`claude-agent-acp`), or Google Antigravity CLI (`agy acp`). See
  [Harnesses](#harnesses).
- **Footprint:** one Rust binary, `ants`, runs as the CLI, the persona daemon and its MCP server.

## Architecture

```text
 machine "macbook"                      hub machine                       machine "desk"
+-------------------------------+      +--------------------------+      +--------------------------+
| ants daemon                   |      | nats-server (hub)        |      | ants CLI (the human)     |
|   dev-a -> harness per thread |      |   JetStream domain "hub" |      |   ask, inbox, approve    |
|   dev-b -> harness per thread |      |   stream ANTS            |      |            |             |
|   MCP server for the sessions |      |   KV ants_roster,        |      |            |             |
|            |                  |      |      ants_sessions,      |      |            |             |
|            |                  |      |      ants_attempts,      |      |            |             |
| nats-server (leaf, outbox)  --+----->|      ants_channels       |<-----+-- nats-server (leaf)     |
+-------------------------------+      +--------------------------+      +--------------------------+
                          uplink: wss://, tls:// or a Dev Tunnel
```

- **Hub:** one nats-server with JetStream, reachable by every machine. It holds the stream `ANTS` (inboxes, plan rooms,
  channels, approvals, dead letters) and the KV buckets for the roster, the thread sessions, attempt markers and
  channels. In operator
  mode (`ants auth`), every persona and the human connect with their own NKey/JWT credentials, limited to the subjects
  of their role.
- **Leaf:** each machine runs its own nats-server as a leaf node of the hub (`ants leaf config`). Every `ants` process
  on the machine connects to it on `127.0.0.1:4222`; only the leaf's uplink crosses the network, and switching the
  uplink touches nothing else. While the uplink is down, what the machine's personas send waits in its outbox.
  A single machine can skip the leaf and point `ants` at the hub directly.
- **Daemon:** `ants daemon` serves the personas of a machine, each over its own NATS connection with its own durable
  inbox consumer. It runs one harness process per active thread, answers tool permission requests from an allowlist
  or the human, enforces limits and spending caps, keeps the persona's roster entry fresh, and gives every harness
  session an MCP server for messaging other personas.
- **Personas:** named agents (`dev-a`, `reviewer`, ...), each with a workdir, a harness, capabilities and a `whoami`
  role. The CLI acts as a human: the one its credentials name, or `human` without credentials. The persona
  `messenger` gets every `ants ask` without `@persona` and routes it by capability. Messages go on
  `ants.inbox.<to>.<from>`, grouped into threads with a turn budget for agent-to-agent traffic.
- **Harnesses:** the agent CLIs the daemon drives over ACP on stdio: `copilot`, `claude` or `antigravity`.

## Install

Requirements:

- Rust 1.100 or later, to build `ants`.
- [nats-server](https://github.com/nats-io/nats-server/releases) 2.11 or later (tested with 2.15.3), for the hub and
  each leaf.
- On machines that serve personas, a harness CLI signed in (see [Harnesses](#harnesses)), for example
  `copilot login`.

```sh
cargo install --path crates/ants        # ~/.cargo/bin/ants
ants --help
```

The same binary runs everywhere: the CLI, `ants daemon`, and the stdio MCP bridge `ants mcp`. To run the hub, the
leaf and the daemon as user services, see [Services](#services).

## Quick start

On one machine, with no credentials:

```sh
nats-server -js &                                   # local hub
ants harness check                                  # ACP handshake with Copilot CLI
ants daemon --persona dev-a --workdir ~/src/some-repo &
ants ask --wait @dev-a "summarise the open TODOs"
ants inbox                                          # replies not read yet
ants roster                                         # personas being served
ants approve                                        # tool requests waiting for an answer
ants plan <thread>                                  # a thread's plan room
ants channel create ops --member dev-a,dev-b        # a channel (see Channels)
ants post '#ops' "@dev-a the build is red"          # wakes dev-a only; prints the thread
ants channel read ops                               # the channel's posts, by thread
ants watch                                          # live traffic
ants watch dev-a [<thread>]                         # what dev-a is doing: state, reply text, tool calls, plan
```

The minimum config file is one line, `provider = "copilot"` (or `"claude"`, `"antigravity"`) in `~/.ants/config.toml`:
`ants daemon` with no flags then serves the built-in messenger, which takes `ants ask` without `@persona` (see
[Messenger](#messenger)).

`ANTS_NATS_URL` (or `--nats`) selects the server, for example `wss://hub.example/nats`. A team across machines adds
credentials ([Credentials](#credentials)), a leaf node per machine ([Leaf nodes](#leaf-nodes)), a config file listing
the personas ([Config file](#config-file)) and services ([Services](#services)).

## CLI

| Command | Does |
|---|---|
| `ants ask [@persona] TEXT` | Sends a message as the CLI's human, the thread's owner (with a persona's credentials, a thread without owner); without `@persona` it goes to the messenger. `--thread` continues a thread, `--wait` prints the first reply. |
| `ants inbox` | Prints unread replies to the CLI's human and marks them read (`--peek`, `--thread`, `--limit`, `--follow`, `--json`). |
| `ants roster` | Lists the personas being served, live or stale, idle or working (`--json`). |
| `ants plan THREAD [TEXT]` | Shows a thread's plan room, or posts to it as the CLI's human. |
| `ants post CHANNEL TEXT` | Posts to a channel (`ops` or `'#ops'`) as the CLI's human and prints the thread; `@persona` wakes that member only. `--thread` posts on a thread, `--wait` prints the posts on the thread as they arrive (`--timeout`). |
| `ants channel create` / `set` / `add` / `remove` / `delete` / `list` | Manages channels: members (`--member`) and messenger (`--messenger`, `--no-messenger`). |
| `ants channel read CHANNEL` | Prints a channel's posts by thread (`--thread`, `--limit`). |
| `ants watch [PERSONA [THREAD] \| SUBJECT]` | Prints traffic as it happens (default `ants.>`); with a persona, its streamed harness progress. |
| `ants approve [ID [VERDICT]]` | Lists pending tool requests, shows one, or answers it (`allow-once`, `allow-always`, `reject`); every human sees every request, and the first decision wins. |
| `ants dead list` / `replay SEQ` / `drop SEQ` / `watch` | Inspects, replays or drops dead letters; `watch` turns max-deliveries advisories into dead letters. |
| `ants init` | Creates the stream and buckets on the hub, or adds the stream subjects and direct gets an older version lacks. Needs a human's credentials. |
| `ants daemon` | Serves the personas of the config file, or one persona described by flags. |
| `ants harness check` / `prompt TEXT` | Talks to a harness directly, without NATS. |
| `ants auth init` / `user NAME [--human]` / `leaf MACHINE` | Issues the team's operator and account, persona or human credentials, and leaf uplink credentials. |
| `ants leaf config MACHINE` / `outbox MACHINE` | Writes the machine's leaf node configuration; sets up its store-and-forward outbox. |
| `ants bridge serve` / `connect URL` | Carries a leaf uplink over plain HTTPS where WebSocket is blocked. |
| `ants mcp` | Bridges stdio to a daemon's MCP socket; the daemon configures it for harnesses without HTTP MCP. |

Every command takes `--nats` (`ANTS_NATS_URL`, default `nats://127.0.0.1:4222`), `--creds` (`ANTS_CREDS`) and
`--js-domain` (`ANTS_JS_DOMAIN`). `ants <command> --help` lists the rest.

The CLI acts as the human its credentials name (the `name` of their user JWT, as `ants auth user NAME --human` sets
it), or as `human` without credentials: it sends and posts under that name, reads that human's inbox and plan room
consumers, and connects with that human's reply prefix. On connecting, it registers that human in the KV bucket
`ants_humans`, creating the bucket when missing: one direct get when the entry is there already, a put otherwise
(again after the entry was removed). `human` needs no entry. When registering fails, the command warns and goes on;
until a later connection registers the human, daemons take its messages for an agent's. Credentials issued without
`--human` are a persona's: the CLI then acts as that persona, registers nothing and warns, `ants inbox` refuses (the
persona's daemon reads its inbox), and `ants ask --wait` marks nothing read.

### Agent skill

`contrib/skills/ants/SKILL.md` teaches a coding agent outside ants (an interactive Copilot CLI session, say) to
message personas as the CLI's human through this CLI: `ants roster`, `ants ask`, `ants inbox --thread`,
`ants watch`. Install it as a personal skill with `copilot skill add contrib/skills/ants/SKILL.md`, or copy the
directory into another agent's skills directory.

## Delivery

Replies to a human wait in its own durable JetStream consumer, `inbox-<name>` (`inbox-human` for `human`), until read,
so those that arrive while no CLI runs are kept. `ants inbox` prints the unread ones and marks them read; `--peek`
leaves them unread, `--thread` and `--limit` pick some and leave the rest unread, `--follow` keeps waiting for new
ones (and dead-letters what JetStream stops delivering, see [Dead letters](#dead-letters)) and `--json` prints one
envelope per line.
`ants ask --wait` marks only the reply it prints read.

The daemon keeps one harness process per active thread, at most `--max-harnesses` (default 4). Different threads are
prompted concurrently; messages on one thread are handled one at a time, in order, and a failed one is retried before
the rest of its thread. A thread's harness stops after `--idle-timeout-secs` (default 600) without a message, and its
session resumes with ACP `session/load` on the next one. A harness with a prompt in flight is never stopped: when all
are busy at the cap, a new thread waits for one to finish. Ctrl-C lets prompts in flight finish (a second Ctrl-C stops
at once, and their messages are redelivered later). SIGTERM, as service managers send, stops it like Ctrl-C.

Every message in hand, waiting for its thread or a harness or being prompted (a tool request waiting for the human
included), gets a progress ack every 20 s. A message held by a daemon that crashed or lost its link is redelivered
after the 60 s ack wait; a copy redelivered to a daemon still holding the message is ignored. The daemon updates its
inbox consumer to these settings when it starts.

Delivery is at least once: a daemon acks a message only after its reply is stored, so an attempt cut off in between
(a crash, a lost link, a second Ctrl-C, a failed prompt) is redelivered and its tools may run again. Before prompting,
the daemon marks the attempt as started in the KV bucket `ants_attempts` under `<persona>.<stream sequence>`. A later
attempt that finds the mark gets this warning before the message text, and resumes the thread's session, whose history
may show what ran:

> [ants] An earlier attempt at this message was cut off before its reply was stored, so tools it ran may already have
> taken effect. The session history may show what ran. Check the current state before repeating any action.

Once the harness has answered, the reply is recorded with the mark before it goes to the stream, so an attempt cut off
after that sends the recorded reply without prompting again. Replies carry a message id made of the persona and the
message's envelope id, so JetStream keeps one copy of a reply sent twice within its duplicate window (2 min). A mark is
removed when the message is acked or dead-lettered; one left behind expires after 30 days. Once its reply is stored, a
message is also noted in `<home>/answered` until its ack is confirmed: while a leaf node's uplink is down the reply
waits in the machine's outbox and the ack fails, so the redelivery, even to a restarted daemon, is acked without being
answered again. A note left behind is removed after 30 days. A daemon needs the bucket: `ants init`
as the human creates it, and persona and leaf credentials issued before it existed must be issued again.

While prompting, the daemon streams progress to `ants.status.<persona>.<thread>` as JSON `StatusEvent`s: the thread's
state (`working`, `waiting_approval`, `idle`, `failed`), the reply text and reasoning as the harness streams them,
joined for up to 250 ms, tool calls as they start or change status, and the harness's plan. Status is core NATS only:
nothing keeps it, so `ants watch dev-a` shows only what happens while it runs, and an update the daemon cannot send at
once is dropped rather than hold up the harness.

Which session each thread runs in is kept in the KV bucket `ants_sessions` under `<persona>.<thread>`, so a restarted
daemon resumes it too. `ants init` (or a daemon without credentials) creates the bucket. An entry is rewritten whenever the thread's harness starts and expires
30 days later, like the stream's messages.

## Config file

One daemon process can serve several personas, each over its own NATS connection with its own credentials. List them
in `~/.ants/config.toml` (or the file named by `--config` / `ANTS_CONFIG`) and run `ants daemon`. The minimum file
names a provider and serves the built-in messenger alone (see [Messenger](#messenger)):

```toml
provider = "copilot"                       # or "claude", "antigravity"
```

A fuller file:

```toml
provider = "copilot"                       # default harness of every persona; serves the built-in messenger
nats = "wss://hub.example/nats"            # server for every persona that sets none
js_domain = "hub"                          # the hub's JetStream domain, if it has one (see Leaf nodes)
human = "alice"                            # default human, see Escalations; default "human"; also per persona

[[persona]]
name = "dev-a"
workdir = "~/src/repo-a"                   # must exist
creds = "~/.config/ants/auth/dev-a.creds"
harness = "claude"                         # default: provider, else "copilot"; or "antigravity"
model = "gpt-6.1-sol"                      # optional; exact model ID offered by the harness
reasoning_effort = "high"                  # optional; a value offered by the selected model
context = "long_context"                  # optional; provider-native tier/window, not a token cap
home = "~/.ants/dev-a"                     # default: ~/.ants/<name>
whoami = "Maintain Rust services and review backend changes." # optional; shared in the roster
memory = "Use the notes MCP server for durable memory."  # optional, any tool available to the harness
tasks = "Use the tracker CLI for task tracking."         # optional, independent of memory
env = { TRACKER_DB = "/srv/ants/dev-a/tasks.db" }         # optional harness environment overrides
allow = ["read", "search"]                 # tool allowlist, see Tool permissions
approval_timeout_secs = 300                # default
capabilities = ["rust", "review"]          # advertised in the roster
max_harnesses = 4                          # default
idle_timeout_secs = 600                    # default
allow_tools = false                        # default
prompt_timeout_secs = 1800                 # default; 0 for no limit (see Limits and caps)
max_tool_calls = 300                       # default; 0 for no limit
max_thread_cost = 5.0                      # default: no cap; also max_thread_tokens
max_daily_cost = 50.0                      # default: no cap; also max_daily_tokens

[[persona]]
name = "dev-b"
nats = "nats://127.0.0.1:4222"             # this persona's own server
workdir = "~/src/repo-b"
creds = "~/.config/ants/auth/dev-b.creds"
```

`~` expands to the home directory; other relative paths are relative to the file. Names must be unique and unknown
keys are errors. A file needs `provider` or at least one `[[persona]]`. A persona that fails to start (a missing credentials file, credentials the server rejects) is logged and
the others keep running; the daemon exits with an error only when none could start. A persona whose server cannot be
reached yet waits for it: the connection is retried after 1 s, then twice as long each time up to 30 s, with a warning
naming the URL. Ctrl-C stops them all, waiting or not.

How flags and the file combine:

- `ants daemon` serves every persona in `~/.ants/config.toml`.
- `ants daemon --persona dev-a` ignores that default file and takes the persona from the flags (`--workdir`, `--home`,
  `--harness`, `--max-harnesses`, ...), as before.
- `ants daemon --config FILE --persona dev-a` serves only `dev-a` from `FILE`; persona flags given override its entries.
- `ants daemon --config FILE --persona messenger` serves only the built-in messenger of a file with `provider`.
- In the file, `nats`, `js_domain` and `creds` win over `--nats` / `ANTS_NATS_URL`, `--js-domain` / `ANTS_JS_DOMAIN` and
  `--creds` / `ANTS_CREDS`, which fill in what the file leaves out. `--creds` applies only when one persona is served.
  `nats`, `js_domain` and `human` may also be set per persona.

### Escalations

What needs a human goes to the thread's owner (the envelope's `owner`, the human who started the thread), else to the
daemon's default human, the config file key `human` (default `human`; top-level or per persona): a spent turn budget, a
reached limit or cap, a dead letter's notice, a plan room out of turns. The daemon tells humans by the KV bucket
`ants_humans` (see Credentials): it reads the bucket at start and watches it, and while the bucket does not exist it
looks again every 30 s, knowing only `human` and the default human meanwhile. Only a human owns a thread: the daemon
ignores an `owner` that is no human, as a persona may name any, and neither escalates to it nor passes it on. A reply
to any human spends no turn, a message from any human renews the thread's allowance, and `send_message` to a human
spends no turn either; `ask_agent` refuses a human.

## Limits and caps

### Model, reasoning effort and context

Each persona may set `model`, `reasoning_effort` and `context`. The matching flags are `--model`,
`--reasoning-effort` and `--context`; with a config file, select one persona using `--persona` before overriding
settings. Omitted fields keep the harness's defaults, including saved session settings on resume.

The daemon selects the model through ACP before setting reasoning effort, since available effort levels depend on
the selected model. Settings are reapplied to new and resumed sessions. Values must match the harness's advertised
option IDs; unsupported values, missing selectors, rejected changes or a selector that reports a different value
fail the session before a prompt runs. Errors list available choices for unsupported values.

| Harness | Model and reasoning effort | Native `context` selection |
|---|---|---|
| Copilot | ACP model and reasoning-effort selectors | `default` or `long_context`, passed to `copilot --acp --context ...`; support depends on the selected model and account |
| Claude | ACP model and effort selectors | `default`, or `1m` with an explicit `model`; `1m` selects the exact advertised `<model>[1m]` variant, without falling back to another model |
| Antigravity | ACP model and thinking-level selectors | No separate context selector; omit `context` and select a model with the required native window |

For Claude, a model ID already ending in `[1m]` can be used without a separate `context`; `context = "default"`
with such an ID is a configuration error. Other model IDs must match the advertised IDs exactly; aliases work only
when the harness lists the alias. A configured context is a native tier/window, not a custom numeric token limit
or a spending cap. ants does not truncate or compact history to enforce a context size. Copilot's context tier is
applied at process launch; ACP does not expose a context selector for read-back. Actual context capacity remains
model- and account-dependent.

These settings are separate from `whoami`; the roster's role description does not claim an effective model or
context capacity. Provider settings and credentials remain managed by the harness and persona `env`.

### Prompt limits and spending caps

Every prompt has a time limit, `--prompt-timeout-secs` (default 1800, tool requests waiting for the human included),
and a limit on the tool calls it starts, `--max-tool-calls` (default 300). Spending can be capped per thread,
`--max-thread-tokens` and `--max-thread-cost`, and per persona per UTC day, `--max-daily-tokens` and
`--max-daily-cost`; there are no spending caps by default. The config file keys have the same names with underscores.

A prompt that passes a limit or a cap is cancelled with ACP `session/cancel` (a harness that does not stop within 30 s
is restarted, and its session resumes on the next message). The reply carries what the harness wrote so far and an
`[ants]` notice naming the limit. When the message came from another agent, the thread then goes to its owner (see
Escalations) like a spent turn budget: the owner gets the notice, "needs a decision", and the last message. While a cap
is reached, later messages on the thread are not prompted; each goes to the owner the same way.

- Per-prompt limits start afresh with every prompt.
- A thread's allowance starts afresh with every message from a human on it.
- A persona's daily allowance starts afresh at 00:00 UTC (or when the daemon restarts with a higher cap); a message
  from a human does not lift it.

Spending counts what the harness reports: the session cost of ACP `usage_update` and the tokens of each prompt
response. Claude's ACP adapter reports both, at the end of each turn, so a cap usually takes effect after the turn that
crossed it. Copilot CLI reports neither, so spending caps never trigger for it; the per-prompt limits still apply.
Whether Antigravity CLI reports either has not been verified, so its spending caps may never trigger either.
Spending is kept in the KV bucket `ants_sessions`, under `<persona>.<thread>.spend` and `<persona>.day.<YYYY-MM-DD>`,
so it survives daemon and harness restarts; entries expire 30 days after their last update.

## Services

Each machine runs two user services: its nats-server (the leaf node, or the hub on the hub machine) and the persona
daemon, `ants daemon --config ~/.ants/config.toml`. Templates are in `contrib/`: launchd agents on macOS
(`contrib/launchd/ants.nats.plist`, `ants.daemon.plist`), systemd user units on Linux and WSL
(`contrib/systemd/ants-nats.service`, `ants-daemon.service`). Both restart on failure; the daemon starts after
nats-server under systemd, and under launchd, which has no start order, it waits until nats-server listens (see Config
file). The units name files only: credentials stay in the files the config names.

Setting up a machine (`macbook`, personas `dev-a` and `dev-b`, hub `wss://hub.example.com/nats` with
`jetstream { domain: hub }`):

```sh
# on the machine holding the team directory
ants auth user dev-a --js-domain hub && ants auth user dev-b --js-domain hub && ants auth user human --js-domain hub
ants auth user messenger --js-domain hub                                  # for the built-in messenger
ants auth leaf macbook --persona dev-a --persona dev-b --persona messenger --persona human --js-domain hub
# copy server.conf, leaf-macbook.creds and the persona and human .creds to ~/.config/ants/auth on macbook (not the .nk seeds)

# on macbook
cargo install --path crates/ants                                          # ~/.cargo/bin/ants
ants leaf config macbook --uplink wss://hub.example.com/nats             # ~/.config/ants/leaf.conf
$EDITOR ~/.ants/config.toml                                               # provider, js_domain = "hub", the personas; see Config file
scripts/install-units.sh                                                  # installs and starts both services
export ANTS_JS_DOMAIN=hub ANTS_CREDS=~/.config/ants/auth/human.creds      # for the CLI, in the shell profile
ants leaf outbox macbook --persona dev-a --persona dev-b --persona messenger --persona human  # store and forward, see Leaf nodes
ants roster
```

`scripts/install-units.sh` fills in the absolute paths of `ants` and `nats-server` (found on `PATH`, or `--ants`,
`--nats-server`), the nats-server configuration (`--nats-conf`, default `~/.config/ants/leaf.conf`; the hub machine
gives its hub configuration), the daemon config file (`--config`) and the current `PATH`, on which the daemon finds the
harness and configured tools. It installs them in `~/Library/LaunchAgents` or `~/.config/systemd/user` and (re)starts them.
`--out DIR` only writes the filled-in units; `--no-daemon` installs nats-server alone. Run it again after moving a
binary or changing `PATH`.

| | macOS (launchd) | Linux, WSL (systemd) |
|---|---|---|
| Logs | `~/Library/Logs/ants/nats.log`, `daemon.log` | `journalctl --user -u ants-nats -u ants-daemon` |
| Switch the uplink | `ants leaf config ...`, then `launchctl kill SIGHUP gui/$(id -u)/ants.nats` | `ants leaf config ...`, then `systemctl --user reload ants-nats` |
| Restart the daemon | `launchctl kickstart -k gui/$(id -u)/ants.daemon` | `systemctl --user restart ants-daemon` |
| Remove | `launchctl bootout gui/$(id -u)/ants.daemon` (and `ants.nats`, `ants.devtunnel`), delete the plists | `systemctl --user disable --now ants-daemon ants-nats` (and `ants-devtunnel`), delete the units |

On stop the daemon gets SIGTERM and lets prompts in flight finish for up to 120 s; messages cut off are redelivered.
launchd agents run while the user is logged in. systemd user units outlive the login session only with
`loginctl enable-linger $USER`; on WSL, systemd must be enabled (`[boot] systemd=true` in `/etc/wsl.conf`).
An uplink through a private Dev Tunnel (`--uplink nats-leaf://127.0.0.1:7422`) also needs `devtunnel connect`:
`--devtunnel ants-hub.<region>` adds a service keeping it running (`ants.devtunnel`, `ants-devtunnel`; log
`devtunnel.log` on macOS), authorized by the CLI login of the user (`devtunnel user login -g -d`) or, with
`--devtunnel-token FILE`, by a connect token read from FILE at each start. Both expire; renewal is in
docs/transport.md, "`devtunnel connect` as a service". Other tunnels, such as `ants bridge connect`, need a service of their own.

## Roster

Each daemon lists its persona in the KV bucket `ants_roster`, keyed by persona id: machine, harness, workdir,
capabilities, `whoami`, start and last heartbeat time, activity (`idle`, or `working` while a message is being
handled), the number of messages being handled and of live harnesses (`RosterEntry` in `ants-proto`). It refreshes the
entry every 10 s, and at once when the persona turns idle or working, and deletes it on a clean stop. Entries from older
daemons lack the activity fields. The bucket keeps an entry 30 s after its last refresh, so a persona whose daemon died
drops out on its own. Capabilities are free-form tags for routing, from the config file's `capabilities` or
`--capability rust --capability billing`. A persona's credentials write only its own entry; every persona's and the
human's read the roster, so agents can pick whom to ask (see Messenger). Readers take the persona from the key and
skip, with a warning, an entry that names another. Persona credentials issued before personas could read it need
`ants auth user` again.

`ants roster` lists the entries: persona, state, activity, live harnesses, harness, machine, time since the last
heartbeat, uptime, capabilities and workdir (`-` for a field an older daemon leaves out). An entry is `live` while its heartbeats keep coming, and `stale` once it has gone 20 s
without one: its daemon is likely gone and the entry expires shortly. Ages come from the daemons' clocks, so skew
between machines shifts them. `--json` prints one entry per line with its `state` added.

### Persona identity and role

Set `whoami` on each `[[persona]]` to describe the role, responsibilities and boundaries:

```toml
[[persona]]
name = "reviewer"
workdir = "~/src/app"
whoami = """
Review Rust changes for correctness and regressions.
Provide findings and recommendations; leave implementation to a development persona.
"""
capabilities = ["rust", "review"]
```

The daemon prepends the persona name and `whoami` to every harness prompt, including resumed sessions. The same
`whoami` is published in the roster and returned by `list_personas`, so other personas can understand the role and
choose a recipient with `ask_agent` or `send_message`. `route` still matches capability tags, not the `whoami` text.
Other personas' `whoami` fields are role descriptions, not instructions for the receiving session.

`--whoami TEXT` sets or overrides the field for a selected persona; `--whoami ""` clears it. Missing or blank
`whoami` adds no role instructions, and old roster entries without the field still work. `ants roster --json` includes
the field when configured. The text is visible to every persona: keep secrets and private instructions out.
Memory and task instructions remain separate and are not published in the roster.

## Dead letters

A message gets five attempts. When the last one fails, the daemon stores the message as a dead letter on
`ants.dead.<persona>` (with the error and delivery count), sends a notice to the thread's owner (see Escalations) on the
same thread, and only then terminates it. If the last attempt never finishes (the daemon stopped, or the ack wait ran
out), the consumer's one extra delivery dead-letters it without another prompt (or, when that attempt recorded its
reply, sends the reply). A payload that is not an envelope becomes a dead letter at once.

```sh
ants dead list          # seq, persona, thread, from, deliveries, time, error, text
ants dead replay 42     # send the message to its inbox again, as it was, and remove the dead letter
ants dead drop 42       # remove it without replaying
ants dead watch         # until Ctrl-C, dead-letter what JetStream stops delivering, and print it
```

Reading and removing dead letters by sequence needs a human's credentials. A stream created before dead letters
existed lacks `ants.dead.>`; `ants init` adds it.

If storing the dead letter fails on that extra delivery too (NATS trouble), JetStream delivers the message no more and
only publishes `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ANTS.<consumer>`. `ants dead watch`, and `ants inbox
--follow` while it runs, turn each such advisory of a persona's inbox consumer into a dead letter in the same format,
with the same notice, unless that message is a dead letter already. Fetching a message by sequence
(`STREAM.MSG.GET`) would let a persona read every inbox, so only human credentials hear the advisories; those
issued before must be issued again with `ants auth user human`. Advisories are core NATS and not stored: one published
while no watcher runs is lost, and its message stays in the stream unnoticed until it expires.

## Harnesses

| `--harness` / `harness =` | Runs | Sign-in |
|---|---|---|
| `copilot` (default) | `copilot --acp` | `copilot login` |
| `claude` | `claude-agent-acp`, from `npm install -g @agentclientprotocol/claude-agent-acp` | `claude login`, or `ANTHROPIC_API_KEY` |
| `antigravity` | `agy acp --disable-telemetry`, from `curl -fsSL https://antigravity.google/cli/install.sh \| bash` | `agy` once (or `agy acp --login` without a browser), or a Gemini API key |

The command must be on the daemon's `PATH`. `ants harness check --harness claude` tests the handshake.

Claude Code sessions are switched to its `default` permission mode when they open, since its `auto` mode approves
tools without asking the client; a Claude Code that does not offer `default` fails the session. Claude Code still
decides some requests on its own before ants sees them: read-only tools and commands (such as `Read`, `Grep`, `ls`,
`echo`), and anything its own settings allow (`permissions.allow` in `~/.claude/settings.json` and the workdir's
`.claude/settings*.json`). It also reads `CLAUDE.md` and runs its own hooks from those settings.

Antigravity CLI sessions are likewise switched to its `default` mode, since its saved `agentMode` (Auto unless set)
runs edits and commands it deems safe without asking the client. Its own rules still decide some requests first: the
allow and deny rules in `~/.gemini/antigravity-cli/settings.json`, and reads and edits inside the workspace. It reads
`GEMINI.md` and `AGENTS.md`. Sign-in is kept in the OS keychain, so a daemon running as a launchd or systemd service
must run as the user who signed in. Its logs are in `~/.gemini/antigravity-cli/log/`.

## Tool permissions

When a harness asks to run a tool, the daemon answers from the persona's allowlist (`--allow RULE`, repeatable, or
`allow = [...]` in the config file). A rule is `*`, an ACP tool kind (`read`, `edit`, `delete`, `move`, `search`,
`execute`, `think`, `fetch`, `switch_mode`, `other`), or `<kind>:<pattern>`, where `*` in the pattern stands for any
run of characters:

| Rule | Allows |
|---|---|
| `read` | every read |
| `execute:cargo test*` | commands starting with `cargo test`; a command with `;`, `&`, `\|`, `<`, `>`, a backtick or `$(` never matches |
| `edit:src/*` | edits under `<workdir>/src`; relative patterns never match outside the workdir |
| `edit:/home/me/notes/*` | edits matching the absolute path |
| `fetch:https://docs.rs/*` | fetches of matching URLs |
| `other:bd-*` | other tools, such as MCP tools, by title |
| `other:send_message` | the daemon's own messaging tool (see MCP tools) |

Allowed requests are answered "allow once". Every other request goes to the humans: the daemon publishes an
`ApprovalRequest` to `ants.approvals.<persona>` (kept in the stream), sets the thread's status to `waiting_approval`,
and waits `--approval-timeout-secs` (default 300) for an `ApprovalDecision` on `ants.approvals.<persona>.<request id>`.
Only human credentials may publish decisions, and the first one wins. The verdict is `allow_once`, `allow_always` (the
harness remembers it for the session) or `reject`; no decision in time rejects the request.
`--allow-tools` is `--allow '*'`, for trusted, sandboxed workdirs only. `ants harness prompt` rejects every tool request.

```sh
ants approve                              # pending requests: persona/id, thread, tool, files, options, time left
ants approve --follow                     # then keep printing new ones until Ctrl-C (--json: one request per line)
ants approve dev-a/019a7d2e               # one request in full, with the tool input; a unique start of the id will do
ants approve dev-a/019a7d2e allow-once    # answer it: allow-once, allow-always or reject
```

A request is named `<persona>/<id>`. Ids are unique only per persona, as any persona may publish a request reusing
another's id, so the persona picks among requests sharing an id. A bare id (or start of one) still works when it
matches a single request; when it matches several, the command lists their `<persona>/<id>` names and answers none.

A request is pending until it has a decision or expires; both requests and decisions are read back from the stream,
without consuming anything, so every human sees every request, including those made while no CLI was running.
Answering stores the decision in the stream before returning, and refuses a request already answered, an expired one,
or a verdict the harness did not offer. The decision is stored only while the request's decision subject holds none
(`Nats-Expected-Last-Subject-Sequence`), so when two humans answer at once, the first decision wins and the second
human is told the request was already answered. The persona acts on the decision stored, which it reads back from the
stream with a direct get once it hears one, not on the first it hears: core NATS still delivers a publish the stream
refuses. On a stream from before direct gets, or with credentials from before them, the persona takes the first
decision it hears and logs a warning; `ants init` and new credentials from `ants auth user` fix that. A request
belongs to the persona its subject names; one whose body names another persona is skipped with a warning, so it is
neither listed nor answerable.

## MCP tools

Every harness session gets the daemon's MCP server, named `ants`, in ACP `session/new` and `session/load`. Its tools
speak as the persona, on the thread the session serves:

| Tool | Does |
|---|---|
| `send_message(to, text, thread?)` | Sends to another persona or `human` and returns once JetStream has stored it. `thread` omitted: the current thread; `new`: a fresh one; else that thread id. |
| `ask_agent(to, text, timeout_secs?)` | Sends on the current thread and waits for the reply (default 300 s, at most 1800), which it returns. |
| `check_inbox(all_threads?)` | Returns, once each, replies to `ask_agent` calls that arrived after the call gave up; this thread's only unless `all_threads`. |
| `list_personas(capability?)` | Lists the other personas in the roster with `whoami`, capabilities, harness, machine and activity (idle or working); only those advertising `capability` when given. |
| `route(capability, text, wait?, timeout_secs?)` | Sends the request to the first persona by name, other than the caller, whose capabilities include `capability` (whole tag, any case). Waits for the reply and returns it like `ask_agent`, or with `wait: false` returns once sent like `send_message`. |
| `post_plan(text)` | Posts to the plan room of the current thread, spending one turn (see Plan rooms). |
| `read_plan(thread?)` | Returns the posts of the current thread's plan room, or of `thread`, oldest first (the last 50), and the turns it has left. |

Messages follow on from the one the thread is handling, so they spend its turn budget like replies: free after a message
from a human or to one, one turn after a message from an agent, refused when none is left (a new thread inherits the
same budget and owner). `ask_agent` also needs a turn left for the reply, and a session may not message its own persona.
The tools work only while a message is in progress on the thread.

A reply to `ask_agent` reaches the waiting call through a live subscription to the persona's inbox, while the inbox
consumer still holds it back behind the prompt that asked. When the consumer delivers it later, the daemon acks it
without prompting, or keeps it for `check_inbox` when the call gave up (also across restarts: asks waiting for a reply
and late replies are kept in `<home>/mcp.json`). Until `check_inbox` takes them, each later prompt on the thread starts
with a line counting its late replies. Only a reply from the persona asked counts; any other message is a prompt as
usual.

The server listens on `127.0.0.1` on a free port (streamable HTTP, stateless) and on the Unix socket
`<home>/mcp.sock` (mode 0600). The sessions of each thread get their own bearer token, which tells the daemon the
calling thread; requests without a known token get 401. Harnesses whose ACP `mcpCapabilities` lack `http` get a stdio
server instead, `ants mcp --socket <home>/mcp.sock` with the token in `ANTS_MCP_TOKEN`, which forwards stdio to the
socket: one small process per session, only for those harnesses. Tool requests go through the persona's allowlist like
any other (`other:send_message`, `other:ask_agent`, `other:check_inbox`, `other:list_personas`, `other:route`,
`other:post_plan`, `other:read_plan`, or `other`). Persona credentials issued
before the MCP tools existed lack the subscription to the persona's own inbox; issue them again with `ants auth user`.

## Messenger

`ants ask` without `@persona` goes to the persona `messenger`. The messenger is a persona like any other, served by
`ants daemon`, whose job is to route: it finds the persona for a request by the capabilities in the roster, with
`list_personas` and `route`, and answers with that persona's reply.

A daemon whose config file sets `provider` serves a built-in messenger on that provider, with the provider's default
model, reasoning effort and context and the settings of a persona that sets nothing else: no allowlist, capabilities
or `whoami`, the file's `nats` and `js_domain`. Its home and workdir are `~/.ants/messenger`, created on start, and its
credentials `~/.config/ants/auth/messenger.creds` (`ants auth user messenger`) when that file exists; with no other
persona served, `--creds` (`ANTS_CREDS`) fills in otherwise. One messenger keeps one session store, so a daemon does not
start the built-in messenger while the roster lists a messenger on another machine: it checks the roster every 10 s
and starts once that entry is gone, deleted on a clean stop or expired 30 s after its daemon's last heartbeat. Of two
daemons starting at once, one claims the roster entry and the other waits. A daemon on a machine whose entry is
already there (its own earlier run) starts at once. A persona named `messenger` in the file replaces the built-in one
and starts without the check.

The messenger's MCP server instructions say what to do; a persona `messenger` with a workdir holding an `AGENTS.md` (or
the harness's own instructions file) along these lines makes it firmer:

```toml
[[persona]]
name = "messenger"
workdir = "~/.ants/messenger-work"          # holds AGENTS.md below
creds = "~/.config/ants/auth/messenger.creds"
allow = ["other:list_personas", "other:route", "other:ask_agent", "other:send_message", "other:check_inbox"]
```

```markdown
This agent is the messenger of an ants team. It routes each request instead of doing the work itself.
1. Decide which capability the request needs; `list_personas` shows who advertises what.
2. Call `route` with that capability and the request, and answer with the reply.
3. A request needing several capabilities gets one `route` per capability; combine the replies.
4. When no persona has the capability, say so and name the capabilities there are.
```

In channels the messenger triages humans' posts that @mention nobody (see Channel delivery).

`route` picks deterministically: the first persona by name with the tag, so the same roster always routes the same
way; `list_personas` and `ask_agent` reach any other. Routed requests follow the thread's turn budget like `ask_agent`
and `send_message`. Every persona gets these tools, not only the messenger.

## Plan rooms

Each thread has a plan room, where the personas on it plan together before the work starts: one posts a plan with
`post_plan`, asks the others (`ask_agent`, `send_message`) to read it with `read_plan` and post their part, and so on.
Posts go on `ants.plan.<thread>.<from>` and are kept in the stream like messages. Nobody is prompted by a post; a
persona reads the room when asked to.

Posts spend the thread's turn budget like messages between agents. Each post carries the turns the room has left: one
less than the lower of the budget of the message its persona is handling and what the room's last post left. The post
that spends the last turn hands the thread to its owner (see Escalations): its persona sends the owner a notice on the
thread with the last post, and the room refuses further agent posts until a human posts there. Two personas spending the
same turn cannot both succeed: a post states the room's last message it saw (`Nats-Expected-Last-Subject-Sequence`,
nats-server 2.11 or later), and the loser reads the room again.

```sh
ants plan <thread>                      # the room's posts: time, author, turns left, text
ants plan <thread> "ship friday"        # post as the CLI's human, which gives the room a fresh budget (8 turns)
ants watch 'ants.plan.>'                # every room, live
```

Personas and humans read a room back through their own consumer, `plan-<persona>`, created for the read and
deleted after it; their credentials allow it to filter plan subjects only. Persona and human credentials issued before
plan rooms need `ants auth user` again.

## Channels

A channel is a named group of personas, like a Slack channel: humans post to it, each as the CLI's human (see
[CLI](#cli)), and the members a post concerns wake up and answer in the channel, on the post's thread.

```sh
ants channel create ops --member dev-a,dev-b --messenger triage   # fails if #ops exists
ants channel add ops dev-c                     # more members
ants channel remove ops dev-a
ants channel set ops --member dev-b,dev-c      # replace the members; --messenger P or --no-messenger the messenger
ants channel list                              # channels, members and messenger
ants channel delete ops                        # the posts stay in the stream

ants post '#ops' "@dev-b the deploy failed"    # wakes dev-b only; prints the new thread
ants post ops "who owns billing?"              # no @mention: the messenger triages it
ants post ops --thread <thread> "any news?"    # on an existing thread
ants post --wait ops "status, anyone?"         # also print the posts on the thread as they arrive
ants channel read ops                          # posts by thread (--thread T, --limit N for the last N threads)
ants watch 'ants.chan.>'                       # every channel, live
```

`#` before the channel name is optional; quote it, as shells take an unquoted `#` for the start of a comment.

**Mentions.** A post that @mentions personas wakes only those of them that are members of the channel; `ants post`
warns about a mention of a persona that is not a member, which wakes nobody. An agent's post that @mentions nobody wakes
nobody.

**Messenger triage.** A post of any human that @mentions nobody goes to the channel's messenger: the persona
`--messenger` names, else the default persona `messenger` (`ants channel list` shows `messenger (default)`). The
messenger decides which members the post concerns and wakes them by @mentioning them in a post on the thread. The
messenger need not be a member.

**Threads.** A post that starts no thread (no `--thread`) starts a new one, named after the post's own id, which `ants
post` prints. The personas a post wakes reply in the channel on that thread, and `--thread` lets any human answer there
too. A thread's owner is the human whose post started it: each post carries it as `owner`, every later post on the
thread keeps it, whoever writes it, and `ants post --thread` reads it back from the thread's first posts kept. Only when
no post kept names one, the posting human takes the thread. `ants post --wait` prints each post on the thread as it
arrives, until `--timeout` (seconds, default 600) or Ctrl-C. `ants channel read` prints each thread under its id: the
post that started it, then the replies, indented.

**Turn budget.** Channel posts spend a thread's turn budget like messages between agents: every agent's post spends a
turn of the thread's budget, whichever post it answers, and the thread goes back to its owner, else the daemon's default
human (see Escalations), when none are left. So the agents one post wakes spend from the same turns, and agents
mentioning each other cannot multiply them. Any human's post gives the thread a fresh budget (8 turns). `ants watch`
shows the turns the thread has left after each post.

**Channels and plan rooms.** Plan rooms ([Plan rooms](#plan-rooms)) are passive boards attached to one thread: a
post there wakes nobody, and a persona reads the room only when asked to. A channel is the other way round: a post
wakes the members it @mentions, or the messenger, and replies come back to the channel.

**Where channels live.** Posts go on `ants.chan.<channel>.<from>` and are kept in the stream; `ants channel read` reads
them back through the CLI's human's consumer `chanread-<human>`, created for the read and deleted after it, and drops a
post whose sender or channel disagrees with its subject (`ants watch` marks such a post forged). A channel's members and
messenger are an entry in the KV bucket `ants_channels` under the channel id, which only humans' credentials write:
`create` fails when the channel exists, and `set`, `add` and `remove` change the entry only if nobody else changed it
since it was read, retrying otherwise. An entry whose channel disagrees with its key is skipped with a warning (`set`,
`add` or `remove` rewrites it). Members and messengers that the roster does not list get a warning rather than a
refusal, as their daemon may start later; a human cannot be a member or the messenger.

### Channel delivery

A daemon takes every channel post through its persona's durable consumer `chan-<persona>` (filter `ants.chan.>`),
with the same guarantees as its inbox: the posts of one thread are answered in order and different threads
concurrently, an attempt is marked in `ants_attempts`, a post is acked only once its reply is stored, and a post whose
last attempt fails becomes a dead letter on `ants.dead.<persona>`, with a notice to the thread's owner, else the
default human, on the post's thread. The
consumer starts at the posts stored from its creation on, so a persona's first start does not answer the channels'
history. The daemon watches `ants_channels` for the members of each channel, skipping an entry whose channel disagrees
with its key.

A post prompts a persona only when it @mentions it and the persona is a member of the channel, or when the persona is
to triage it (below). The rest are acked without a prompt: posts in other channels, the persona's own, posts that
mention only others or nobody. A malformed post, or one whose `from` or channel disagrees with its subject, is dropped (terminated, no dead
letter). A prompt starts with the posts on its thread that the persona has not been shown yet, oldest first, without its own: those
since the last post it answered on the thread, or since the thread started. At most 50 are shown, with a count of the
older ones left out. The stream sequence of the last post answered is kept per thread under
`<persona>.<thread>.seen` in `ants_sessions`, so a restarted daemon goes on from there.

The final answer is posted to the same channel and thread on `ants.chan.<channel>.<persona>`, replying to the post. An
@mention in it wakes only members of the channel. Every agent's post spends one turn of the thread's budget: each post
carries the turns the thread has left after it, and the daemon reads the posts stored on the thread after the one it
answers, before prompting and again before posting, taking the lowest of their turns, or the latest human post's, which
gives a fresh budget. A post that reaches a persona with no turns left is not prompted, and an answer whose thread ran
out of turns during the prompt is not posted: either way the thread's owner, else the default human, gets a notice in
the inbox (`ants.inbox.<human>.<persona>`) on the thread with the post, and the unposted answer if any, as in the inbox.
Replies racing each other on one thread may overshoot the budget slightly, but fan-out does not multiply it. Spending
caps apply as to inbox messages.

A post of any human (`human` or one `ants_humans` lists) that @mentions nobody prompts the channel's messenger instead:
the persona its entry assigns, or else `messenger` (built in or configured), a member or not. Its prompt adds the triage
rules of ants and lists the channel's members with their capabilities and `whoami` from the roster, members missing from
the roster as offline; the messenger's own instructions add to the rules. It answers on the post's thread with a short
post that @mentions the members the post is relevant to, saying briefly why, or says none are and mentions nobody; it
does not answer the request itself. The answer goes on `ants.chan.<channel>.<messenger>` and spends a turn like any
agent's post. Its mentions wake members only: a mention of a non-member stays in the post's `mentions` but wakes nobody.

## Persona memory

Memory and task tracking are optional and tool-agnostic. ants does not initialize a tool workspace, run setup or
context-loading commands, or require bd. Install and configure any CLI or MCP tools through the harness as usual.

Set `memory` and `tasks` independently on each `[[persona]]` to describe which tools to use and how. Both default to
unset. Their text is prepended to every prompt, including prompts in resumed sessions; it is guidance to the agent,
not commands executed by ants. An empty string clears that role's instructions. Configured tools still follow the
normal harness permissions and ants approval rules.

The persona's `env` map overrides the harness's inherited environment, including for hooks and tool subprocesses.
Values are literal: `~`, `$HOME` and relative paths are not expanded or resolved by ants. Use absolute paths for
persona-local data; sharing a path shares the tool's state across personas. The persona's `home` remains for ants'
own files, and does not configure external tools.

The equivalent flags are `--memory TEXT`, `--tasks TEXT` and repeatable `--env NAME=VALUE`. With a config file, select
a persona using `--persona`; each flag overrides only its corresponding instructions or environment key.

### Example: bd for memory and task tracking

bd is one possible tool, configured entirely outside ants. Create a workspace manually:

```sh
mkdir -p ~/.ants/dev-a
bd init -C ~/.ants/dev-a --prefix dev-a
```

Then configure the persona (replace `/absolute/path` with the home directory):

```toml
[[persona]]
name = "dev-a"
workdir = "~/src/some-repo"
memory = "Run bd prime at the start; use bd remember for durable memories."
tasks = "Use bd ready, bd claim, bd create and bd close for task tracking."
env = { BD_DB = "/absolute/path/.ants/dev-a/.bd/bd.db", BD_REMOTE = "" }
```

Keep `bd` on the harness's `PATH`. `BD_DB` selects the workspace instead of the workdir's; empty `BD_REMOTE` avoids an
inherited remote overriding it. Optional harness hooks can load context (`bd setup copilot` or `bd setup claude` in
the workdir, or user-level hooks; `bd setup gemini` writes instructions to run `bd prime` into `GEMINI.md`).
Memory and tasks can instead use different tools by changing their instructions and environment settings.

**Migration:** existing personas no longer get a bd workspace or `BD_DB` automatically. Existing databases are left
untouched; add the instructions and environment settings above to retain bd-based memory and task tracking.

## Credentials

On a shared hub, each persona and the human connect with their own NKey/JWT credentials. Each persona may publish
only to the subjects its role allows (see `Permissions::for_persona` in `ants-proto`).

A message from `<from>` to `<to>` goes on `ants.inbox.<to>.<from>`, and a persona's credentials publish only on
`ants.inbox.*.<persona>`, so the subject proves the sender. Daemons, `ants ask --wait` and `ants inbox` check the
envelope's `from` and `to` against the subject: a daemon dead-letters a mismatch without its envelope (so it cannot be
replayed), and the CLI drops it with a warning; `ants watch` marks it forged. The human's credentials may send under
any sender, to replay dead letters as they were; `ants dead replay` replays only a message to the persona that buried
it. Messages left on the older `ants.inbox.<persona>` subjects are no longer delivered; daemons and `ants inbox` move
their consumers to the new subjects on start. Credentials issued before must be issued again with `ants auth user`.

Plan room posts work the same way: a post by `<from>` to the room of thread `<thread>` goes on
`ants.plan.<thread>.<from>`, every credential (the human's too) posts only on `ants.plan.*.<persona>`, and readers
drop a post whose `from` or thread disagrees with its subject (`ants watch` marks it forged).

Channels work the same way too. A channel is a named group of personas; its id follows persona id syntax. A post by
`<from>` to channel `<channel>` goes on `ants.chan.<channel>.<from>` (kept in the stream), every credential posts only
on `ants.chan.*.<persona>` (and `ants.out.chan.*.<persona>` in a leaf's outbox), and readers drop a post whose `from` or
channel disagrees with its subject. A post carries its thread id and the personas it @mentions; a top-level post starts
a new thread, named after the post's own id, and a post carries the thread's `owner`, the human whose post started it. A
channel's members and optional messenger are a `ChannelEntry` in the KV bucket `ants_channels`, under the channel id:
only humans write it, personas read it, and readers skip an entry whose channel disagrees with its key. A persona takes
channel posts through its own consumer `chan-<persona>` and reads channels back through `chanread-<persona>`;
credentials pin the filter of both under `ants.chan.>`, so neither reaches an inbox. Credentials and streams set up
before channels need `ants auth user` and `ants init` again.

Approval decisions go on `ants.approvals.<persona>.<request id>`, which only humans publish to. A persona hears the
decisions on its own requests and reads back the one the stream stored with a direct get
(`$JS.API.DIRECT.GET.ANTS.ants.approvals.<persona>.*`, which returns only the last message on that subject), so it
reads no one else's; the stream allows direct gets for this. Credentials and streams set up before then need
`ants auth user`, `ants auth leaf` (leaf uplinks carry their personas' rights) and `ants init` again.

Humans: a team may have several, each with its own name, credentials and inbox, and all with the same rights. A
human is the persona `human`, or a persona listed in the KV bucket `ants_humans` under its id (a `HumanEntry` naming
it); readers skip an entry whose persona disagrees with its key. `ants auth user NAME --human` issues a human's
credentials, whatever the name (`Permissions::for_persona` with `Role::Human`); without `--human` it issues a
persona's, except for the name `human`, which always gets a human's. A human's credentials allow what this section
grants the human: creating and upgrading the stream and the buckets, reading and removing dead letters, sending under
any sender to replay them, deciding approvals, setting up leaf outboxes and the intake, watching `ants.>`, plus
writing `ants_humans`. Each human reads its replies only through its own inbox consumer, `inbox-<name>`. Persona
credentials only read `ants_humans`. The holder of the team directory (`ants auth init`) is the first human and
issues its own credentials with `ants auth user NAME --human`. A human joins `ants_humans` on its first CLI connection, not when its
credentials are issued: until then daemons take its messages for an agent's. The CLI acts as the human its credentials
name (see CLI); daemons read the bucket (see Escalations). An envelope may carry `owner`, the human who started its
thread: replies, messages an agent sends on or from the thread, escalations and notices on it keep it; envelopes
without one stay valid, and daemons ignore an `owner` that is no human. Credentials issued before `ants_humans` need `ants auth user` again.

Escalation chains (wire only so far; daemons publish no chain and route nothing by these fields yet): a roster entry may
carry `escalate_to`, the persona's chain in order, whose first persona leads. An envelope may carry `escalation`
(`Escalation` in `ants-proto`): the stuck persona and its thread, the reason (`turn_budget`, `cap`, `plan_room`,
`dead_letter`, `approval` with the request id, or `asked`), the chain level (1 for the stuck persona's own chain), the
level's leader and the personas visited. One level up goes to every persona of the leader's chain not visited yet; an
empty chain, a leader already visited, or a level past 3 hands it to the thread's owner instead. An agent's decision
on another persona's approval request travels as an envelope's `approval` on `ants.inbox.<requester>.<decider>`, so the
subject pins the decider and persona credentials need no rights on `ants.approvals.*`; the requester takes it only from
a persona of its own chain. Envelopes and entries without these fields stay valid.

```sh
ants auth init                         # operator + account keys and server.conf in ~/.config/ants/auth
ants auth user human                   # ~/.config/ants/auth/human.creds, for the CLI
ants auth user alice --human           # alice.creds: another human, with the same rights
ants auth user dev-a                   # copy to the machine that runs dev-a
```

Add `include "server.conf"` to the nats-server configuration (with JetStream enabled), run `ants init` once
with a human's credentials to create the stream and the roster, sessions, attempts, channels and humans buckets, then pass `--creds` (or `ANTS_CREDS`) to every `ants` command.
Credentials carry their permissions in the CONNECT line, past the 4 KB nats-server allows by default, so `server.conf`
sets `max_control_line: 16384`; add that line to a `server.conf` written before channels.
`ANTS_AUTH_DIR` (or `--dir`) moves the team directory. Keep it private: the seeds in it sign every credential.
nats-server resolves `include` relative to the including file, even for a path that starts with `/`, so copy
`server.conf` next to the server configuration (or include it by a relative path).

### Leaf nodes

A machine can run its own nats-server as a leaf node of the hub, and point every `ants` process at it. A client on a
leaf reaches the hub's JetStream only through a JetStream domain: give the hub `jetstream { domain: hub }`, set
`--js-domain hub` (or `ANTS_JS_DOMAIN=hub`, or `js_domain` in the config file) for every `ants` process, on the hub
as well, and issue every credential with it:

```sh
ants auth user dev-a --js-domain hub
ants auth leaf macbook --persona dev-a --persona dev-b --persona human --human alice --js-domain hub   # leaf-macbook.creds
```

Credentials issued with `--js-domain` allow the JetStream API both as `$JS.<domain>.API` and as the plain `$JS.API`
the hub maps it to (the hub checks both); without it they allow only the plain form, which a leaf does not forward.
`ants auth leaf` issues the credentials of the leaf's uplink (`leafnodes.remotes[].credentials`): they allow what the
listed personas and humans may do together, and only for a leaf node connection, never a client. List each human
that runs the CLI on the machine: `human` with `--persona` or `--human`, any other with `--human NAME`, so that it
gets a human's rights. The leaf still checks each local client against its own credentials; the uplink credentials
bound what crosses to and from the hub. Hub and leaf configuration: [docs/transport.md](docs/transport.md).

`ants leaf config` writes the leaf's nats-server configuration, `~/.config/ants/leaf.conf` by default: it listens on
`127.0.0.1:4222` only (the default server of every `ants` process), runs JetStream in the machine's own domain for
its outbox only (stored next to the configuration, in `jetstream/`), includes the team's `server.conf` and has one
uplink to the hub:

```sh
# on macbook, with server.conf and leaf-macbook.creds copied into ~/.config/ants/auth
ants leaf config macbook --uplink wss://hub.example.com/nats             # the hub's websocket, behind a proxy
ants leaf config macbook --uplink nats-leaf://127.0.0.1:7422             # its leafnode port, via devtunnel connect
                                                                         # or ants bridge connect (below)
nats-server -c ~/.config/ants/leaf.conf                                  # or as a service, see Services
```

| Uplink | Reaches |
|---|---|
| `nats-leaf://host[:port]` (or `nats://`) | the hub's leafnode port, default 7422, in plain text (a tunnel provides the encryption) |
| `tls://host[:port]` | the same port with TLS |
| `ws://host[:port]/path`, `wss://host[:port]/path` | the hub's websocket listener, directly or through a reverse proxy; the leaf asks for `<path>/leafnode` |

`--ca-file ca.pem` trusts a private CA (`tls://` and `wss://`), `--proxy http://proxy:3128` goes through an HTTP
`CONNECT` forward proxy. `--uplink-creds` and `--server-conf` default to `leaf-<machine>.creds` and `server.conf` in
the team directory (`--dir`, `ANTS_AUTH_DIR`); the configuration names the credentials by absolute path and includes
`server.conf` by a relative one, as nats-server requires. Switching the uplink changes nothing else on the machine:
run `ants leaf config` again with the new `--uplink` (it rewrites the whole file) and send the running leaf `SIGHUP`
(`systemctl --user reload ants-nats`, `launchctl kill SIGHUP gui/$(id -u)/ants.nats`); local clients stay connected.

While the uplink is down, inbox messages, replies and plan posts wait in the machine's outbox, a stream on the leaf,
and the hub takes them once the uplink is back. The hub needs `feature_flags { js_api_reply_v2: true }` for that, and
the human sets it up once per machine, with the leaf connected:

```sh
ants leaf outbox macbook --persona dev-a --persona dev-b --persona human --human alice   # again when they change
ants leaf outbox macbook --remove                                                       # stop taking from the machine
```

The hub's stream takes from a machine's outbox only what the listed personas and humans may send themselves, so issue
the leaf's uplink credentials for the same personas and humans. Whatever needs an answer from the hub at once still
fails while the uplink is down: inbox consumers and acks, sessions and the roster, `post_plan`, dead letters,
approvals and replays. A message sent while the uplink is down lands after what reached the hub meanwhile; a reply
whose request could not be acked may come twice. Details: [docs/transport.md](docs/transport.md), "Store and forward on a leaf node".

## Transport options

Every `ants` process speaks NATS (`nats://`, `tls://`, `ws://`, `wss://`), with TLS on rustls; a private CA comes in
through `SSL_CERT_FILE`. With a leaf node per machine, `ants` always uses `nats://127.0.0.1:4222` and only the leaf's
uplink varies:

| Uplink | When |
|---|---|
| `wss://host/path` to the hub's websocket behind a reverse proxy (such as Caddy) | The usual choice; works through an HTTP `CONNECT` forward proxy (`ants leaf config --proxy`). |
| `wss://` to an anonymous Dev Tunnels HTTPS port | No proxy of its own; NATS credentials are the access control (researched, not verified). A private tunnel cannot be used this way. |
| `nats-leaf://127.0.0.1:7422` through `devtunnel connect` to the hub's leafnode port | A private tunnel; the tunnel provides the encryption. |
| `tls://host:7422` | The hub's leafnode port exposed directly with TLS. |
| `nats-leaf://127.0.0.1:7422` through `ants bridge connect` to `ants bridge serve` | A network that blocks WebSocket; plain HTTPS requests (see below). |

`ants` itself cannot use an HTTP forward proxy (async-nats has no proxy support); a leaf node can, and so can
`ants bridge connect`. Hub and leaf configuration, reconnect behaviour and the tested paths:
[docs/transport.md](docs/transport.md).

### HTTPS fallback bridge

Where a proxy blocks WebSocket, `ants bridge` carries the leaf node's uplink in plain HTTPS requests: `POST` for what
the leaf sends, long-polled `GET` for what it receives, each tagged with its byte offset so that retried requests
neither lose nor repeat bytes. On the hub, `ants bridge serve` listens on `127.0.0.1:8423` behind the TLS-terminating
reverse proxy (Caddy: `handle /bridge* { reverse_proxy 127.0.0.1:8423 }`) and connects each session to the hub's
leafnode port (`--upstream`, default `127.0.0.1:7422`). On the worker, `ants bridge connect` listens on
`127.0.0.1:7422`, so the leaf's uplink is the same as through `devtunnel connect`:

```sh
# hub
export ANTS_BRIDGE_TOKEN=$(openssl rand -hex 32)                     # optional; the same on every worker
ants bridge serve

# macbook
ants bridge connect https://hub.example.com/bridge                  # with the same ANTS_BRIDGE_TOKEN
ants bridge connect https://hub.example.com/bridge --proxy http://user:pw@proxy.corp:3128   # through a forward proxy
ants leaf config macbook --uplink nats-leaf://127.0.0.1:7422
```

NATS credentials still authenticate the leaf inside the bridged stream; the token (`--token` or `ANTS_BRIDGE_TOKEN`)
only keeps strangers from opening sessions. `--header 'Name: value'` adds a request header for a gateway in front of
the hub. `SSL_CERT_FILE` trusts a private CA. Each message costs a round trip, so prefer `wss://` where it works.
Details: [docs/transport.md](docs/transport.md), "HTTPS fallback bridge".

## Footprint

An idle daemon serving one persona stays under 20 MB resident memory. `scripts/footprint.sh` checks it: it starts a
throwaway `nats-server -js` and `ants daemon`, samples the daemon's RSS with `ps`, and fails at or above the limit
(`--max-mb`, default 20). CI runs it on Linux and macOS. `--ask "reply with ok"` also measures one live harness
session, which runs the real harness.

```sh
scripts/footprint.sh --port 14722                     # idle
scripts/footprint.sh --port 14722 --ask "reply with ok"
```

Measured with ants 0.1.0 (release build), nats-server 2.15.3:

| | Linux x86_64 | macOS arm64 |
|---|---|---|
| Idle daemon | 5.9 MB | 8.5 MB |
| Idle daemon with the MCP server | not measured | 10.5 MB (+0.8 MB over 9.7 MB in the same run) |
| Daemon with one Copilot session | not measured | 10.5 MB (+2.0 MB) |
| Copilot CLI harness processes | not measured | 255 MB |

## Development

```sh
cargo build --release   # target/release/ants
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo deny check        # licenses, advisories, bans, sources; see deny.toml
```

Tests that need `nats-server` on PATH skip without it; `ANTS_REQUIRE_TOOLS=1` (as in CI) makes them fail
instead. `crates/ants/tests/e2e.rs` runs `ants ask --wait` against `ants daemon` on a throwaway nats-server, with the
test binary itself as a stub ACP harness.

CI (`.github/workflows/ci.yml`) runs formatting, clippy, the tests with nats-server installed, a release build and
the footprint check on Linux and macOS, and `cargo deny check` on Linux. A dependency with a license outside the
allow-list in `deny.toml`, a security advisory, a yanked crate or a source other than crates.io fails it.

## Layout

| Path | Role |
|---|---|
| `crates/ants-proto` | Wire contract: ids, subjects, envelope, turn budget, permissions; no I/O |
| `crates/ants-daemon` | NATS/JetStream bus, ACP harness pool, inbox loop, tool permissions, MCP server |
| `crates/ants` | The `ants` binary: CLI, `ants daemon`, `ants auth`, `ants leaf` |
| `crates/ants-test-support` | Test helpers (dev-only): tools on PATH, a throwaway nats-server |
| `contrib/` | launchd and systemd unit templates, the agent skill for the CLI |
| `scripts/` | `install-units.sh` (services), `footprint.sh` (memory check) |
| `docs/transport.md` | Transport spikes: hub, reverse proxy, leaf nodes, Dev Tunnels |

Work and specs are tracked in bd (`.bd/`): `bd ready`, `bd spec list`, `bd spec show <area>`.
