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

## Layout

| Crate | Role |
|---|---|
| `crates/ants-proto` | Wire contract: ids, subjects, envelope, turn budget |
| `crates/ants-daemon` | NATS/JetStream bus, ACP harness client, inbox loop |
| `crates/ants` | The `ants` binary |

Work and specs are tracked in bd (`.bd/`): `bd ready`, `bd spec list`, `bd spec show <area>`.
