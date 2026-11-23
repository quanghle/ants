# Container demo

A local three-node team in Linux containers, run by [Apple container](https://github.com/apple/container),
[Docker](https://docs.docker.com/engine/), [Podman](https://podman.io/) or
[wslc](https://learn.microsoft.com/en-us/windows/wsl/wsl-container):

```mermaid
flowchart TD
    hub["hub<br/>JetStream domain: hub<br/>ant: hub-ant"]
    leafa["leaf-a<br/>NATS leaf<br/>ant: leaf-a-ant"]
    leafb["leaf-b<br/>NATS leaf<br/>ant: leaf-b-ant"]
    leafa -->|"leaf uplink"| hub
    leafb -->|"leaf uplink"| hub
```

Each node is a separate Linux container (a VM of its own under Apple container) with its own NATS server, persistent volume, workdir, Copilot sign-in and
one `ants daemon` serving exactly one ant. Every ant runs Copilot through its
[native protocol](config.md#harness-protocols) (`protocol = "copilot-sdk"`) rather than ACP. The built-in messenger is disabled. The leaves use scoped NKey/JWT uplinks
and local JetStream domains `leaf-a` and `leaf-b`. Only the hub holds the team signing keys. The demo human `human` is
an [admin](auth.md#admins), and each node's daemon runs with
[host credentials](auth.md#machine-host-credentials) for its machine (`hub`, `leaf-a`, `leaf-b`), so admin orders such as
`ants ant pause`, `ants ant set` and `ants ant restart` reach every ant. Every ant keeps its memories, tasks and
reminders in [sense](sense.md), ants' built-in provider, its data written with its node's host credentials. The hub also runs `human`'s [personal assistant](auth.md#dedicated-assistant),
`human-assistant`, enabled on every hub start (`ants assistant enable`, which keeps what earlier runs left), working in
`/demo/assistant`. A native harness runs its tools unasked, so the daemon's allow rules and `human`'s approvals do
not apply; instead the assistant's harness, `copilot-assistant`, limits the model to Copilot's `view`, `grep` and
`glob` and the daemon's MCP tools (`--available-tools`), and excludes web search and the built-in GitHub MCP server's
tools by name (`--excluded-tools`): no shell, edits or fetches. This online demo requires
the hub to remain reachable; it does not provision [store-and-forward outboxes](transport.md#store-and-forward).

## Requirements

- `bash` and `jq` on `PATH`, and one container CLI:
  - Apple `container` with custom-network support, on an Apple silicon Mac with macOS 26 or later, its service running
    (`container system start`);
  - Docker with BuildKit (the default builder since Docker 23), its daemon running;
  - Podman;
  - wslc, from WSL as `wslc.exe` (the build context is passed by its Windows path).

  The script takes the first of `container`, `docker`, `podman`, `wslc` and `wslc.exe` on `PATH`;
  `ANTS_DEMO_RUNTIME` names another (`ANTS_DEMO_RUNTIME=podman`). The image builds for the host's architecture, arm64
  or amd64.
- Network access to the image registries, Debian packages, crates.io, npm and GitHub Copilot.
- A GitHub account with Copilot CLI access. Real prompts use the account's model allowance.

No host Rust, Node.js, NATS or Copilot installation is required. The image builds the workspace with the pinned Rust
toolchain and `Cargo.lock`, and installs NATS 2.15.3 and Copilot CLI 1.0.112. The Rust image tag must track
`rust-toolchain.toml` when that toolchain changes. Client telemetry, OpenTelemetry and automatic Copilot updates are
disabled; GitHub's server-side service accounting is unaffected.

## Start

From the repository root:

```sh
scripts/demo-containers.sh build
scripts/demo-containers.sh up
scripts/demo-containers.sh smoke
```

Apple container's build VM gets four CPUs and 8 GiB for the release build;
under Docker, Podman or wslc, give the engine's VM or WSL comparable resources. The first build downloads dependencies and compiles a Linux release
binary. `up` also builds when the image is absent; rerun `build` after source changes. The smoke check verifies all four
live Copilot-backed ants, the assistant's grant and their MCP sockets, pauses and resumes each ant by admin order from the hub, checks that each node's ant keeps its sense data (`ants ant memories`),
then sends notes
from every node, reading them from every other node through the hub. It makes no model calls and needs no Copilot
sign-in.

Each container gets two CPUs, 2 GiB of memory and a named volume (sparse, 4 GiB, under Apple container). Linux-native volumes support the Unix
socket permissions used by the daemon's MCP bridge. A dedicated network connects the nodes; no ports are published on the
host. NATS client ports and daemon metrics listen only on each container's loopback interface. The hub's authenticated
leaf port listens on the demo network. Production NATS configuration, services and credentials stay untouched.

## Sign in and send work

Sign in separately in each container, following the displayed GitHub device-code flow:

```sh
scripts/demo-containers.sh login hub
scripts/demo-containers.sh login leaf-a
scripts/demo-containers.sh login leaf-b
scripts/demo-containers.sh ants hub ask --wait @hub-ant "Identify the node."
scripts/demo-containers.sh ants hub ask --wait @leaf-a-ant "Identify the node."
scripts/demo-containers.sh ants leaf-a ask --wait @leaf-b-ant "Identify the node."
scripts/demo-containers.sh ants hub ask --wait @human-assistant "Summarize what waits on human."
scripts/demo-containers.sh ants hub ask --wait @hub-ant \
  "Ask leaf-a-ant and leaf-b-ant to identify their nodes, then summarize both replies."
```

The default Copilot model is left unchanged. The node ants' Copilot runs every tool unasked, the assistant only its restricted set; no
host directories are mounted into running nodes. Do not copy production secrets or repositories into the demo workdirs.
Authentication is intentionally interactive: host GitHub tokens and Copilot configuration are
not copied or forwarded. Sign-in data stays under `/demo/home/` in each node's volume, potentially in plaintext where no
credential store is available.

## Inspect and stop

```sh
scripts/demo-containers.sh status
scripts/demo-containers.sh logs leaf-a
scripts/demo-containers.sh ants hub thread list
scripts/demo-containers.sh down
```

`down` removes only the named demo containers and network after checking ownership labels. It preserves the image and
named volumes `ants-demo-hub`, `ants-demo-leaf-a` and `ants-demo-leaf-b`, including generated NATS credentials,
JetStream data, Copilot sign-ins and ant homes. `up` refuses existing containers; run `down` before recreating the
topology. After a failed `up`, inspect the node logs and run `down` before retrying. Each container CLI has its own containers,
networks and volumes: a demo started with one is stopped with the same one.

`ANTS_DEMO_NAME` changes the resource prefix (default `ants-demo`); `ANTS_DEMO_STATE` changes the host directory holding
the ownership marker, temporary build context and smoke logs (default `~/.local/share/ants/container-demo/`).
Set both for a second independent demo. Nonempty directories without the demo marker and resources belonging
to another state directory are refused. `ANTS_DEMO_COPILOT_VERSION` changes the npm version on the next `build`.
