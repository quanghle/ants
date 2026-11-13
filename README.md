# ants

Headless agent teams on several machines, talking to each other and to a human at the terminal over NATS.

- **Backbone:** NATS + JetStream for durable inboxes with acks and redelivery, and NATS KV for the roster and sessions.
- **Transport:** any uplink that carries NATS. Options are a Dev Tunnel (TCP), `ws://`/`wss://`, or an HTTPS fallback bridge.
- **Harnesses:** agents run behind ACP (Agent Client Protocol) over stdio. Copilot CLI (`copilot --acp`) comes first.
- **Footprint:** one Rust binary, `ants`, runs as the CLI, the persona daemon and (later) the MCP server.

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
ants watch                                          # live traffic
```

`ANTS_NATS_URL` (or `--nats`) selects the server, for example `wss://hub.example/nats`.

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

Add `include "/path/to/server.conf"` to the nats-server configuration (with JetStream enabled), run `ants init` once
with the human's credentials to create the stream, then pass `--creds` (or `ANTS_CREDS`) to every `ants` command.
`ANTS_AUTH_DIR` (or `--dir`) moves the team directory. Keep it private: the seeds in it sign every credential.

## Layout

| Crate | Role |
|---|---|
| `crates/ants-proto` | Wire contract: ids, subjects, envelope, turn budget |
| `crates/ants-daemon` | NATS/JetStream bus, ACP harness client, inbox loop |
| `crates/ants` | The `ants` binary |

Work and specs are tracked in bd (`.bd/`): `bd ready`, `bd spec list`, `bd spec show <area>`.
