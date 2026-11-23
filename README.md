# ants

[![CI](https://github.com/quanghle/ants/actions/workflows/ci.yml/badge.svg)](https://github.com/quanghle/ants/actions/workflows/ci.yml)

Headless coding agent teams across machines, communicating with each other and with humans over NATS.

## Why ants

Standalone coding agent CLIs run one session in one terminal on one machine. `ants` connects ACP (Agent Client Protocol) agents (`copilot`, `claude`, `codex`, `antigravity`, or custom harnesses) across machines over a shared NATS + JetStream bus:

- **Durable collaboration:** persistent inboxes, multi-agent channels, passive thread notes, capability routing (`system-messenger`), and bounded agent-to-agent turn budgets that escalate up configured chains before interrupting humans.
- **Isolation & guardrails:** per-thread git worktrees, role-scoped NKey/JWT credentials, end-to-end sealed human direct messages, tool allowlists and approvals, prompt and spend caps, and secret redaction.
- **Resilient transport:** works over `wss://`, `tls://`, private Dev Tunnels, or an HTTP/1.1 fallback bridge (`ants bridge`), with optional per-machine store-and-forward leaf nodes for offline operation.
- **Personal context & work:** built-in sense keeps private memories, fenced task claims, commitments and reminders; the active daemon delivers timed and condition-triggered wake-ups ([sense](docs/sense.md#work-and-reminders)). [Dreams](docs/sense.md#dreams) review recent work, track sage findings and their follow-ups, and proceed without a sage when no review is available.
- **Lean single binary:** one `ants` executable provides the CLI, terminal UIs (`ants chat`, `ants dash`), ant daemon, credential signer, channel manager, webhook/notification server, hook runner, and HTTPS bridge. Idle `ants daemon` keeps under 10 MB of private memory with its ant's sense data open, enforced in CI ([footprint](docs/operations.md#footprint)).

## Architecture

```mermaid
flowchart LR
    subgraph worker["Worker node"]
        daemon["ants daemon<br/>ACP harnesses + MCP"]
        wleaf["nats-server leaf<br/>outbox ANTS_OUT"]
        daemon <--> wleaf
    end

    subgraph hub["Hub"]
        hserver["nats-server<br/>JetStream domain: hub<br/>stream ANTS · 21 KV · ants_files"]
        services["ants signer · channel serve<br/>webhook serve"]
        services <--> hserver
    end

    subgraph client["Client node"]
        cli["ants CLI · chat · dash"]
        cleaf["nats-server leaf"]
        cli <--> cleaf
    end

    wleaf <-->|"wss:// · tls:// · tunnel · bridge"| hserver
    cleaf <-->|"wss:// · tls:// · tunnel · bridge"| hserver
```

- **Hub:** a `nats-server` with JetStream holding stream `ANTS` (inboxes, channels, notes, approvals, timelines, dead letters, admin orders), 21 KV buckets, attachment store `ants_files`, and stream `ANTS_SENSE` (each ant's signed, hash-chained sense event log, reachable by machine hosts only).
- **Leaf:** optional local `nats-server` (`127.0.0.1:4222`) per node with a store-and-forward outbox (`ANTS_OUT`) that drains to the hub on reconnect.
- **Daemon:** `ants daemon` runs per-ant ACP session pools, exposes collaboration tools to each session over MCP, and acks JetStream deliveries only after replies are stored. A harness definition's `protocol` key can run a CLI on its native protocol instead, behind an in-process ACP translator ([harness protocols](docs/config.md#harness-protocols)).
- **Humans, assistants & ants:** humans own threads, decide tool approvals (a rejection can carry a note telling the ant what to do instead), and converse through the CLI or `ants chat`; optional personal assistant ants triage queues under device-signed grants; capability-tagged worker ants collaborate within shared turn budgets.

A message on a thread with a running prompt steers it: protocols `claude`, `codex` and `copilot-sdk` fold it into the running turn, while others cancel the prompt and run it next in the same session; `--queue` (or `/later` in `ants chat`) queues behind the running prompt instead, and an ant starts independent work on another thread with MCP `handoff` ([delivery](docs/architecture.md#pool-worktrees-and-at-least-once-retries)).

See [docs/architecture.md](docs/architecture.md) for the store/subject model, delivery loop, MCP tools, channels, and escalation mechanics.

## Install a release

Each `v*` tag publishes [release](https://github.com/quanghle/ants/releases) archives of the `ants` binary for Linux x86_64 and arm64 (glibc 2.43+), macOS arm64 and Windows x86_64, with `SHA256SUMS` and build provenance attestations. Each archive also holds the [nats-server](https://github.com/nats-io/nats-server) release ants is tested with (pinned in [`crates/ants/nats-server.sha256`](crates/ants/nats-server.sha256)) and its license, `LICENSE-nats-server`:

```sh
tag=v0.3.0 target=aarch64-apple-darwin   # or x86_64-unknown-linux-gnu, aarch64-unknown-linux-gnu
curl -fLO "https://github.com/quanghle/ants/releases/download/$tag/ants-$tag-$target.tar.gz"
gh attestation verify "ants-$tag-$target.tar.gz" --repo quanghle/ants   # optional
tar -xzf "ants-$tag-$target.tar.gz"
install "ants-$tag-$target/ants" "ants-$tag-$target/nats-server" ~/.local/bin/
```

Windows takes `ants-$tag-x86_64-pc-windows-msvc.zip`. Once installed, `ants update` replaces the binary, and the nats-server beside it, with the latest release (`ants update --check` only tells whether one is out; see [docs/operations.md](docs/operations.md#updating-from-a-release)). A release needs the same harness CLIs as a build from source (below). To cut a release, set `version` in `Cargo.toml`, merge, then push the tag `v<version>` (a suffix such as `-rc.1` makes a pre-release). The Release workflow reruns CI's checks unless CI already passed on that commit pushed to main; running it by hand only checks and builds. Release notes list the bd work closed and the requirements added since the previous release, from the committed `.bd/issues.jsonl`; `scripts/release-notes.sh v<version>` previews them.

## Install from source

Requirements: Rust 1.100+, [nats-server](https://github.com/nats-io/nats-server/releases) 2.15+, and at least one ACP harness CLI (`copilot`, `claude-agent-acp`, `codex-acp`, `agy`, or a custom command) on machines running `ants daemon`. Without a nats-server beside `ants`, in `~/.ants/bin` or on `PATH`, `ants setup` downloads the pinned release into `~/.ants/bin`, checked against its pinned SHA-256 digest.

```sh
# From GitHub:
cargo install --locked --git https://github.com/quanghle/ants ants

# Or from a local checkout:
git clone https://github.com/quanghle/ants.git
cd ants
cargo install --locked --path crates/ants
```

Platform notes (Windows named pipes, cross-builds), automatic binary upgrade draining, and launchd/systemd service installation live in [docs/operations.md](docs/operations.md).

## Quick start

Bring up a local `nats-server`, config, background services, shell completions, and health check on a single machine:

```sh
ants setup --ant dev-a --workdir ~/src/some-repo
ants ask --wait @dev-a "summarize the open TODOs"
ants inbox                                          # unread replies across devices
ants colony                                         # active ants and harness status
ants channel create ops --member dev-a
ants post --wait '#ops' "@dev-a check the failing build"
ants trail save deploy.py                           # a trail: a Python script orchestrating ants
ants trail run deploy --follow                      # start and follow a run, its spending limits shown first
ants                                                # full-screen chat with mouse, clipboard and background alerts (also: ants chat)
```

`ants setup --managed-nats` instead has `ants daemon` start, supervise and stop that `nats-server` itself, with no separate NATS service ([docs/operations.md](docs/operations.md#managed-nats-server)).

Join an existing team hub as a human using shared bootstrap credentials ([docs/auth.md](docs/auth.md#registering-humans)):

```sh
ants login --hub wss://hub.example/nats --name alice --bootstrap bootstrap.creds
ants inbox
```

Connect a worker machine to a hub through a local store-and-forward leaf node and add a leaf ant ([docs/transport.md](docs/transport.md#leaf-nodes), [docs/auth.md](docs/auth.md#adding-an-ant)):

```sh
# On the hub / auth machine: issue leaf uplink and one-time enrollment credentials
ants auth leaf worker-1 --ant dev-b --human alice --js-domain hub
ants auth enroll dev-b
# Copy server.conf, leaf-worker-1*.creds, and enroll-dev-b.creds from ~/.config/ants/auth/ to worker-1

# On worker-1: start the local leaf node, enroll dev-b (and host-worker-1), then start the daemon
mkdir -p ~/.ants && printf 'harness = "copilot"\njs_domain = "hub"\n' > ~/.ants/config.toml
ants leaf config worker-1 --uplink wss://hub.example/nats
scripts/install-units.sh --no-daemon                # or: nats-server -c ~/.config/ants/leaf.conf &
ants add dev-b --workdir ~/src/some-repo --enroll ~/.config/ants/auth/enroll-dev-b.creds
scripts/install-units.sh                            # once approved below (or: ants daemon)

# On the hub / auth machine: approve enrollment, wire the offline outbox, and message @dev-b
ants enroll approve dev-b --js-domain hub
ants leaf outbox worker-1 --ant dev-b --human alice --js-domain hub
ants ask --wait @dev-b "run the test suite"
```

## Documentation

| Document | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Wire stores and subjects, delivery retries, worktrees, colony, MCP tools, channels, escalations, dead letters |
| [docs/sense.md](docs/sense.md) | Built-in sense: an ant's brief, memories, resume registers and handoffs, tasks, commitments, reminders, persona, skills and dreams, what operators see of them, limits |
| [docs/trails.md](docs/trails.md) | Trails: Python scripts a conductor's daemon runs to coordinate ants and humans; saving (`ants trail save`, ants' `save_trail`), starting (CLI, `run_trail`, schedules), following, stopping and resuming runs, limits, trust, the tips steps leave for later runs, storage, workers |
| [docs/cli.md](docs/cli.md) | Full CLI command reference, environment variables, thread search/marks, `ants chat` and `ants dash` |
| [docs/config.md](docs/config.md) | `config.toml`, shared settings, harnesses, fallbacks, templates, manager ants, spend caps, permissions, redaction, schedules |
| [docs/auth.md](docs/auth.md) | NKey/JWT credentials, revocations, backups, device encryption and sealed messages, admin orders, assistants, GitHub login, OS keystores, ACLs |
| [docs/transport.md](docs/transport.md) | Hub configuration, reverse proxy, leaf nodes, store-and-forward outboxes, HTTPS fallback bridge, Dev Tunnels |
| [docs/webhooks.md](docs/webhooks.md) | HTTP webhook ingress, provider presets, human push notification sinks, quiet hours, digests, stand-ins |
| [docs/hooks.md](docs/hooks.md) | Read-only observer credentials, event feeds, JSON schemas, wire versioning |
| [docs/operations.md](docs/operations.md) | Platform specifics, binary upgrades, launchd/systemd units, the daemon-managed `nats-server`, `/healthz` and Prometheus metrics, daemon footprint |
| [docs/container-demo.md](docs/container-demo.md) | Isolated three-node local demo in Apple container, Docker, Podman or wslc |
| [contrib/skills/ants/SKILL.md](contrib/skills/ants/SKILL.md) | Agent skill teaching external coding agents to use the `ants` CLI |
| [contrib/skills/ants-trails/SKILL.md](contrib/skills/ants-trails/SKILL.md) | Trail authoring guide for ants and humans: the API, sandbox, limits, patterns and complete example trails |

## Contributing

Workspace crates: `ants-proto` (wire types and NATS permissions, no I/O), `ants-sense` (built-in identity, memory and work provider), `ants-trail` (trails: Python scripts run by Monty in one-shot `ants subprocess` workers), `ants-daemon` (bus, ACP pool, delivery, approvals, MCP server), `ants` (CLI and service binary), and `ants-test-support` (test helpers).

Run the full CI check suite locally before submitting changes (`rust-toolchain.toml` pins Rust 1.100):

```sh
scripts/check.sh               # fmt, clippy, test (v1 + JS ack/FC v2), and cargo-deny with warnings denied
scripts/check.sh --footprint   # also the release build and the idle footprint check
```

- **Dependencies:** `.cargo/config.toml` enforces a 7-day minimum publish age; `deny.toml` governs allowed permissive licenses and advisories.
- **Wire compatibility:** `crates/ants-proto/tests/wire_snapshots.rs` pins wire JSON formats with [insta](https://insta.rs) snapshots following [docs/hooks.md](docs/hooks.md#versioning).
- **Issue & spec tracking:** work items and feature specifications are tracked in `bd` (`.bd/issues.jsonl`).
- **Security:** report vulnerabilities privately per [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
