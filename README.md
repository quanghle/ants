# ants

Headless agent teams on several machines, talking to each other and to a human at the terminal over NATS.

- **Backbone:** NATS + JetStream for durable inboxes with acks and redelivery, and NATS KV for the roster and sessions.
- **Transport:** any uplink that carries NATS. Options are a Dev Tunnel (TCP), `ws://`/`wss://`, or an HTTPS fallback
  bridge. Tested setups and their limits are in [docs/transport.md](docs/transport.md).
- **Harnesses:** agents run behind ACP (Agent Client Protocol) over stdio. Copilot CLI (`copilot --acp`) comes first.
- **Footprint:** one Rust binary, `ants`, runs as the CLI, the persona daemon and its MCP server.

## Build

```sh
cargo build --release   # target/release/ants
cargo test
```

## Try it

```sh
nats-server -js &                                   # local hub
ants harness check                                  # ACP handshake with Copilot CLI
ants daemon --persona dev-a --workdir ~/src/some-repo &
ants ask --wait @dev-a "summarise the open TODOs"
ants inbox                                          # replies not read yet
ants watch                                          # live traffic
```

`ANTS_NATS_URL` (or `--nats`) selects the server, for example `wss://hub.example/nats`.

Replies to the human wait in the durable JetStream consumer `inbox-human` until read, so those that arrive while no
CLI runs are kept. `ants inbox` prints the unread ones and marks them read; `--peek` leaves them unread, `--thread`
and `--limit` pick some and leave the rest unread, `--follow` keeps waiting for new ones and `--json` prints one
envelope per line. `ants ask --wait` marks only the reply it prints read.

The daemon keeps one harness process per active thread, at most `--max-harnesses` (default 4). Different threads are
prompted concurrently; messages on one thread are handled one at a time, in order, and a failed one is retried before
the rest of its thread. A thread's harness stops after `--idle-timeout-secs` (default 600) without a message, and its
session resumes with ACP `session/load` on the next one. A harness with a prompt in flight is never stopped: when all
are busy at the cap, a new thread waits for one to finish. Ctrl-C lets prompts in flight finish (a second Ctrl-C stops
at once, and their messages are redelivered later).

Every message in hand, waiting for its thread or a harness or being prompted (a tool request waiting for the human
included), gets a progress ack every 20 s. A message held by a daemon that crashed or lost its link is redelivered
after the 60 s ack wait; a copy redelivered to a daemon still holding the message is ignored. The daemon updates its
inbox consumer to these settings when it starts.

Which session each thread runs in is kept in the KV bucket `ants_sessions` under `<persona>.<thread>`, so a restarted
daemon resumes it too. `ants init` (or a daemon without credentials) creates the bucket. An entry is rewritten whenever the thread's harness starts and expires
30 days later, like the stream's messages.

## Config file

One daemon process can serve several personas, each over its own NATS connection with its own credentials. List them
in `~/.ants/config.toml` (or the file named by `--config` / `ANTS_CONFIG`) and run `ants daemon`:

```toml
nats = "wss://hub.example/nats"            # server for every persona that sets none

[[persona]]
name = "dev-a"
workdir = "~/src/repo-a"                   # must exist
creds = "~/.config/ants/auth/dev-a.creds"
harness = "copilot"                        # default
home = "~/.ants/dev-a"                     # default: ~/.ants/<name>
allow = ["read", "search"]                 # tool allowlist, see Tool permissions
approval_timeout_secs = 300                # default
capabilities = ["rust", "review"]          # advertised in the roster
max_harnesses = 4                          # default
idle_timeout_secs = 600                    # default
allow_tools = false                        # default

[[persona]]
name = "dev-b"
nats = "nats://127.0.0.1:4222"             # this persona's own server
workdir = "~/src/repo-b"
creds = "~/.config/ants/auth/dev-b.creds"
```

`~` expands to the home directory; other relative paths are relative to the file. Names must be unique and unknown
keys are errors. A persona that fails to start (bad credentials, unreachable server) is logged and the others keep
running; the daemon exits with an error only when none could start. Ctrl-C stops them all.

How flags and the file combine:

- `ants daemon` serves every persona in `~/.ants/config.toml`.
- `ants daemon --persona dev-a` ignores that default file and takes the persona from the flags (`--workdir`, `--home`,
  `--harness`, `--max-harnesses`, ...), as before.
- `ants daemon --config FILE --persona dev-a` serves only `dev-a` from `FILE`; persona flags given override its entries.
- In the file, `nats` and `creds` win over `--nats` / `ANTS_NATS_URL` and `--creds` / `ANTS_CREDS`, which fill in what
  the file leaves out. `--creds` applies only when one persona is served.

## Roster

Each daemon lists its persona in the KV bucket `ants_roster`, keyed by persona id: machine, harness, workdir,
capabilities, start and last heartbeat time (`RosterEntry` in `ants-proto`). It refreshes the entry every 10 s and
deletes it on a clean stop. The bucket keeps an entry 30 s after its last refresh, so a persona whose daemon died
drops out on its own. Capabilities are free-form tags for routing, from the config file's `capabilities` or
`--capability rust --capability billing`. A persona's credentials write only its own entry; the human's read the
roster.
## Dead letters

A message gets five attempts. When the last one fails, the daemon stores the message as a dead letter on
`ants.dead.<persona>` (with the error and delivery count), sends a notice to the human on the same thread, and only
then terminates it. If the last attempt never finishes (the daemon stopped, or the ack wait ran out), the consumer's
one extra delivery dead-letters it without another prompt. A payload that is not an envelope becomes a dead letter at
once.

```sh
ants dead list          # seq, persona, thread, from, deliveries, time, error, text
ants dead replay 42     # send the message to its inbox again, as it was, and remove the dead letter
ants dead drop 42       # remove it without replaying
```

Reading and removing dead letters by sequence needs the human's credentials. A stream created before dead letters
existed lacks `ants.dead.>`; `ants init` adds it.

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

Allowed requests are answered "allow once". Every other request goes to the human: the daemon publishes an
`ApprovalRequest` to `ants.approvals.<persona>` (kept in the stream), sets the thread's status to `waiting-approval`, and
waits `--approval-timeout-secs` (default 300) for an `ApprovalDecision` on `ants.approvals.<persona>.<request id>`.
Only the human's credentials may publish decisions. The verdict is `allow_once`, `allow_always` (the harness remembers
it for the session) or `reject`; no decision in time rejects the request. `--allow-tools` is `--allow '*'`, for
trusted, sandboxed workdirs only. `ants harness prompt` rejects every tool request.

## MCP tools

Every harness session gets the daemon's MCP server, named `ants`, in ACP `session/new` and `session/load`. Its tools
speak as the persona, on the thread the session serves:

| Tool | Does |
|---|---|
| `send_message(to, text, thread?)` | Sends to another persona or `human` and returns once JetStream has stored it. `thread` omitted: the current thread; `new`: a fresh one; else that thread id. |
| `ask_agent(to, text, timeout_secs?)` | Sends on the current thread and waits for the reply (default 300 s, at most 1800), which it returns. |
| `check_inbox(all_threads?)` | Returns, once each, replies to `ask_agent` calls that arrived after the call gave up; this thread's only unless `all_threads`. |

Messages follow on from the one the thread is handling, so they spend its turn budget like replies: free after a
message from the human, one turn after a message from an agent, refused when none is left (a new thread inherits the
same budget). `ask_agent` also needs a turn left for the reply, and a session may not message its own persona. The
tools work only while a message is in progress on the thread.

A reply to `ask_agent` reaches the waiting call through a live subscription to the persona's inbox, while the inbox
consumer still holds it back behind the prompt that asked. When the consumer delivers it later, the daemon acks it
without prompting, or keeps it for `check_inbox` when the call gave up (also across restarts: asks waiting for a reply
and late replies are kept in `<home>/mcp.json`). Only a reply from the persona asked counts; any other message is a
prompt as usual.

The server listens on `127.0.0.1` on a free port (streamable HTTP, stateless) and on the Unix socket
`<home>/mcp.sock` (mode 0600). The sessions of each thread get their own bearer token, which tells the daemon the
calling thread; requests without a known token get 401. Harnesses whose ACP `mcpCapabilities` lack `http` get a stdio
server instead, `ants mcp --socket <home>/mcp.sock` with the token in `ANTS_MCP_TOKEN`, which forwards stdio to the
socket: one small process per session, only for those harnesses. Tool requests go through the persona's allowlist like
any other (`other:send_message`, `other:ask_agent`, `other:check_inbox`, or `other`). Persona credentials issued
before the MCP tools existed lack the subscription to the persona's own inbox; issue them again with `ants auth user`.

## Persona memory

Each persona keeps its to-dos and memories in its own bd workspace, `~/.ants/<persona>/.bd/bd.db` on its machine
(`--home` moves it). The daemon creates it on first start and runs every harness session with `BD_DB` pointing at it,
so `bd remember`, `bd ready` and the session-start `bd prime` hook all see the persona's workspace, not the workdir's.
`bd` must be on the daemon's `PATH`, and the harness needs the bd session-start hooks (`bd setup copilot` in the workdir, or user-level hooks).

## Credentials

On a shared hub, each persona and the human connect with their own NKey/JWT credentials. Each persona may publish
only to the subjects its role allows (see `Permissions::for_persona` in `ants-proto`).

```sh
ants auth init                         # operator + account keys and server.conf in ~/.config/ants/auth
ants auth user human                   # ~/.config/ants/auth/human.creds, for the CLI
ants auth user dev-a                   # copy to the machine that runs dev-a
```

Add `include "server.conf"` to the nats-server configuration (with JetStream enabled), run `ants init` once
with the human's credentials to create the stream and the roster and sessions buckets, then pass `--creds` (or `ANTS_CREDS`) to every `ants` command.
`ANTS_AUTH_DIR` (or `--dir`) moves the team directory. Keep it private: the seeds in it sign every credential.
nats-server resolves `include` relative to the including file, even for a path that starts with `/`, so copy
`server.conf` next to the server configuration (or include it by a relative path).

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

## Layout

| Crate | Role |
|---|---|
| `crates/ants-proto` | Wire contract: ids, subjects, envelope, turn budget |
| `crates/ants-daemon` | NATS/JetStream bus, ACP harness client, inbox loop |
| `crates/ants` | The `ants` binary |

Work and specs are tracked in bd (`.bd/`): `bd ready`, `bd spec list`, `bd spec show <area>`.
