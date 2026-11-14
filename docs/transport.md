# Transport

One `nats-server` runs as the hub with JetStream in domain `hub`. Each worker runs a local `nats-server` leaf node on `127.0.0.1:4222` with local JetStream in domain `<machine>` for offline store-and-forward (`ANTS_OUT`). Every `ants` process on a leaf machine uses `nats://127.0.0.1:4222` and `--js-domain hub`; switching uplinks changes only `leaf.conf`. Single-machine setups can connect directly to the hub.

| Path | Endpoint | Proxy and auth | Notes |
|---|---|---|---|
| Leaf over `wss://` | `wss://hub.example.com:443/nats` (`/nats/leafnode`) | Optional `--proxy`; `leaf-<machine>.creds` | Primary uplink behind a TLS-terminating reverse proxy. |
| Leaf over private Dev Tunnel | `nats-leaf://127.0.0.1:7422` via `devtunnel connect` | CLI login or 24 h token; `leaf-<machine>.creds` | Encrypted by tunnel; no public DNS or TLS certificate needed. |
| Leaf over anonymous Dev Tunnel HTTPS | `wss://<id>-8080.<region>.devtunnels.ms:443` | Anonymous tunnel port; `leaf-<machine>.creds` | Private HTTPS ports require `X-Tunnel-Authorization`, unsupported by leaf remotes. |
| Leaf over `tls://` | `tls://hub.example.com:7422` | Optional `--proxy`; `leaf-<machine>.creds` | Direct TLS uplink to an exposed hub leafnode port. |
| Leaf over HTTPS fallback bridge | `nats-leaf://127.0.0.1:7422` via `ants bridge connect` -> `ants bridge serve` | Optional `--proxy` (Basic auth), `--header`, `--token`; `leaf-<machine>.creds` | HTTP/1.1 POST/GET pipe when proxies block WebSocket or gateways need custom headers. |
| Direct client (no leaf) | `nats://`, `tls://`, `ws://`, `wss://` to hub `:4222` or `:8080` | User `.creds`; no HTTP proxy support | Single-machine or leaf-free workers (`devtunnel connect` `:4222`); no offline outbox. |

## Hub

The hub runs JetStream, a loopback client port, a plain WebSocket listener behind a reverse proxy or tunnel, and a loopback leafnode port:

```hcl
server_name: hub
listen: 127.0.0.1:4222
ping_interval: "30s"
jetstream {
  store_dir: "/var/lib/ants/js"
  domain: hub
}
websocket {
  listen: 127.0.0.1:8080
  no_tls: true
}
leafnodes {
  listen: 127.0.0.1:7422
  no_advertise: true
}
feature_flags {
  js_api_reply_v2: true
}
include "server.conf"
```

- `include "server.conf"` resolves relative to the config directory even if prefixed with `/`. Copy `server.conf` (`ants auth init`) next to the hub config; it configures operator/account trust and `max_control_line: 16384` so permission JWTs fit in `CONNECT`.
- `websocket` routes paths ending in `/leafnode` to leaf connections, `/mqtt` to MQTT, and any other path (such as `/nats`) to client connections. WebSocket and TCP clients authenticate identically with `.creds`.
- `leafnodes { no_advertise: true }` stops the hub from advertising `127.0.0.1:7422` to leaves, keeping reconnects on the proxy or tunnel address (likewise set `no_advertise: true` on clustered hubs or client WebSocket blocks).
- `feature_flags { js_api_reply_v2: true }` scopes JetStream source control replies to `$JSC.R.<machine>.*.ANTS_OUT.>`, required for hub outbox sourcing under scoped leaf credentials.

## Reverse proxy

Caddy passes WebSocket upgrades with no extra config and sets no idle timeout on upgraded connections:

```caddyfile
hub.example.com {
  # tls /etc/ants/hub.pem /etc/ants/hub.key   # private CA; omit for ACME
  handle /nats* {
    reverse_proxy 127.0.0.1:8080
  }
  handle /bridge* {                            # optional: ants bridge serve
    reverse_proxy 127.0.0.1:8423
  }
  handle /hooks* {                             # optional: ants webhook serve
    reverse_proxy 127.0.0.1:8424
  }
  respond 404
}
```

Clients connect to `wss://hub.example.com/nats` (`/nats`); leaves configured with `--uplink wss://hub.example.com/nats` dial `/nats/leafnode`. Proxies with 60 s idle timeouts (such as default `nginx` `proxy_read_timeout`) stay open via 20 s client/leaf pings and 30 s hub pings.

## Client connections

- Every `ants` command accepts `--nats` (`ANTS_NATS_URL`, default `nats://127.0.0.1:4222`), `--creds` (`ANTS_CREDS`), and `--js-domain` (`ANTS_JS_DOMAIN`).
- `tls://` and `wss://` use `rustls` with platform root certificates. `SSL_CERT_FILE=/path/to/ca.pem` replaces the root store on direct client connections and `ants bridge connect`. Behind a local leaf, clients use `nats://127.0.0.1:4222` and private CA trust goes in `ants leaf config --ca-file`.
- Direct clients preserve URL paths and query strings on WebSocket upgrades, send no custom HTTP headers, and ignore `HTTPS_PROXY` and `HTTP_PROXY`. HTTP `CONNECT` proxies require a local leaf node or `ants bridge connect`.
- `ants add --enroll` and `ants enroll` use core NATS subjects (`ants.enroll.*`), which leaf uplinks exclude. Pass `--hub <hub-url>` to `ants add` on a leaf machine and `--nats <hub-url>` to `ants enroll` on the signing machine.

## Leaf nodes

`ants leaf config <machine> --uplink <url>` writes `~/.config/ants/leaf.conf` (override with `--out`):

```sh
ants leaf config macbook --uplink wss://hub.example.com/nats
ants leaf config macbook --uplink wss://hub.example.com/nats --ca-file ca.pem --proxy http://proxy.corp:3128
ants leaf config macbook --uplink nats-leaf://127.0.0.1:7422
```

Generated configuration:

```hcl
server_name: "macbook"
listen: "127.0.0.1:4222"
ping_interval: "20s"
include "auth/server.conf"
jetstream {
  domain: "macbook"
  store_dir: "/Users/alice/.config/ants/jetstream"
  max_memory_store: 64MB
  max_file_store: 1GB
}
leafnodes {
  remotes: [
    {
      urls: ["wss://hub.example.com:443/nats"]
      account: "A..."
      credentials: "/Users/alice/.config/ants/auth/leaf-macbook.creds"
      tls {}
    }
  ]
}
```

- `--listen` accepts loopback addresses only (default `127.0.0.1:4222`).
- Explicit ports (`443`, `80`, `7422`) are always written because `nats-server` defaults portless remote URLs to `7422`.
- Multiple `--uplink` URLs must share one scheme. `tls://` and `wss://` always emit `tls {}` (or `tls { ca_file: "..." }`), preventing plaintext fallback and satisfying `nats-server` when `proxy { url: "..." }` is set. `--proxy` takes `http://host:port` or `https://host:port` without credentials.
- Switch uplinks by rerunning `ants leaf config` and sending `SIGHUP` (`launchctl kill SIGHUP gui/$(id -u)/ants.nats` or `systemctl --user reload ants-nats`); local clients stay connected.

### JetStream domain

With `jetstream { domain: hub }` on the hub, every `ants` process sets `--js-domain hub` (`ANTS_JS_DOMAIN=hub` or `js_domain = "hub"` in `~/.ants/config.toml`), sending JetStream API and KV requests to `$JS.hub.API.>`.

- Each leaf runs local JetStream in domain `<machine>` (never `hub`) solely for `ANTS_OUT`. Plain `$JS.API.>` resolves to the leaf, so local clients reach the hub stream and KV buckets only via `$JS.hub.API.>`. The leaf blocks local `$JS.<machine>.API.>` traffic from crossing up while allowing the hub to query `$JS.<machine>.API.>` for outbox sourcing.
- Issue all user, leaf, and channel-manager credentials with `--js-domain hub`. Because the hub checks publish permissions before and after mapping `$JS.hub.API.>` to `$JS.API.>` (and `$JS.hub.API.$KV.>` to `$KV.>`), domain credentials allow both forms and work on the hub and on leaves.

### Leaf uplink credentials

```sh
ants auth leaf macbook --ant dev-a --ant dev-b --ant messenger --human human --human alice --js-domain hub
ants auth leaf macbook --ant dev-a --webhook github --js-domain hub
```

`ants auth leaf` writes `leaf-<machine>.creds` (`leafnodes.remotes[].credentials`):

- Sets `allowed_connection_types: ["LEAFNODE", "LEAFNODE_WS"]`, rejecting client connections.
- Grants the union of listed `--ant`, `--human` (`--ant human` also counts as human), and `--webhook` permissions (read from `<webhook>.creds`), plus outbox sourcing subjects (`$JS.<machine>.API.*.ANTS_OUT`, `$JS.S.<machine>.>`, `$JSC.R.<machine>.*.ANTS_OUT.>`, `_INBOX.<human>.leaf.<machine>.>`) and literal subject anchors (such as `ants.inbox.dev-a.dev-a`) so hub wildcard stream interest crosses the uplink.
- Excludes `ants.out.>`. Local clients still authenticate individually against `server.conf`; the leaf filters traffic to uplink bounds so unauthorized local clients cannot drop the uplink. Reissue `leaf-<machine>.creds` whenever machine users or webhooks change.

## Store and forward

Run `ants leaf outbox` as a human while the leaf is connected to the hub (and rerun when ants or humans on the machine change):

```sh
ants leaf outbox macbook --ant dev-a --ant dev-b --ant messenger --human human --human alice
ants leaf outbox macbook --remove
```

JetStream sources move buffered messages to the hub automatically across outages and restarts:

1. **Leaf outbox (`ANTS_OUT`)**: in domain `<machine>` (`ants.out.>`, file storage, 30 d retention). When publishing to `ANTS` fails with no stream / no responders or times out, clients write the same payload and `Nats-Msg-Id` to `ants.out.<rest>`.
2. **Hub intake (`ANTS_IN`)**: sources each machine's `ANTS_OUT` via `$JS.<machine>.API` (delivery `$JS.S.<machine>`) under `ants.in.<machine>.>`, isolating machines from each other.
3. **Hub admission (`ANTS` and `ants_links`)**: `ANTS` sources from `ANTS_IN` per admitted user only subjects allowed for that role (`ants.out.inbox.*.<ant>`, `ants.out.notes.*.<user>`, `ants.out.chan.*.<user>`, `ants.out.dead.<ant>`, or `ants.out.inbox.>` for all machine inboxes when a human is listed), rewriting them to `ants.*`. `ants_links` sources `ants.in.<machine>.ants.out.links.<ant>.*.*` into `$KV.ants_links.<ant>.<thread>.<recipient>`. Unadmitted subjects stay in `ANTS_IN`. New admissions start after current `ANTS_IN` entries, and `Nats-Msg-Id` deduplicates within `ANTS`'s 2 min window.

`ants leaf outbox` uses reply prefix `_INBOX.<human>.leaf.<machine>`, updates sources, and waits up to 10 s for `ANTS_IN` to hear from `ANTS_OUT`. If inactive, it fails naming `feature_flags { js_api_reply_v2: true }` on the hub while leaving the setup in place.

| Operation | While leaf uplink is down |
|---|---|
| Inbox messages and replies (`ants ask`, daemon replies, agent messages) | Buffered in `ANTS_OUT`; daemons record unacked requests in `<home>/answered` and ack redeliveries on reconnect (ignoring `"no responders"`) without re-prompting. |
| Unsequenced notes (`ants notes <thread> TEXT`), channel posts (`ants post`, daemon replies), passed links (`ants_links`) | Buffered in `ANTS_OUT` and sourced into `ANTS` or `ants_links` on reconnect. |
| Daemon dead letters (`ants.dead.<ant>`) | Buffered in `ANTS_OUT` and noted in `<home>/buried`; notice and `+TERM` run on redelivery (or within 30 s of reconnect on the 6th extra delivery) after checking `deadread-<ant>` against duplicates past 2 min. |
| Pull consumers, acks, stream reads (`ants inbox`, `ants channel read`, `ants notes`, `ants dead list`/`replay`) | Fail immediately; require the hub stream. |
| Sequenced `post_notes`, tool approvals (`ants.approvals.*`), channel admin (`ants.chanadm.*`), enrollment (`ants.enroll.*`) | Fail immediately; require live hub coordination or optimistic concurrency. |
| KV writes (`ants_roster`, `ants_sessions`, `ants_attempts`, `ants_channels`, `ants_humans`, `ants_chains`) | Fail immediately so late puts cannot overwrite newer keys; daemons refresh `ants_roster` every 10 s on reconnect. |
| Owner records (`ants_owners`, from `ants ask` and webhooks delivering to ants) | Fail with a warning; the message still goes out, and ants' messages on its thread carry no owner. |

### Republishing and live waiters

Sourced messages enter `ANTS` without a core NATS publish on `ants.inbox.*`. `ants init` configures `ANTS` to republish stored `ants.inbox.*.*` messages to `_INBOX.{{wildcard(1)}}.stored.{{wildcard(2)}}` with `Nats-Subject` intact, readable only by the recipient and its leaf uplink (daemons warn on older streams until upgraded). Live inbox waiters subscribe to both `ants.inbox.<user>.*` and `_INBOX.<user>.stored.*`, deduplicating the last 1,024 messages by `Nats-Msg-Id` (or payload).

Core NATS republishes do not cross a down leaf uplink. Waiter behavior across an uplink or client outage depends on the caller:

- **Human `ants ask --wait` and `--follow`**: pair live subscriptions with a 5 s `STREAM.MSG.GET` poll on `ANTS` (`next_by_subj: ants.inbox.<human>.*`, up to 100 per round) from the starting stream sequence, deduplicated by `Nats-Msg-Id`, and mark printed replies read in the durable consumer `inbox-<human>`. Replies stored on the hub during an uplink outage arrive within 5 s of reconnect (`ants inbox` also reads unread replies from `inbox-<human>`).
- **Daemon `ask_agent` and escalation waiters**: listen on live subscriptions while connected, and receive replies stored during an outage via the durable consumer `inbox-<ant>` once reconnected.
- **Ant-credential `ants ask --wait` / `--follow` and `ants post --wait` / `--follow`**: `ants ask` as an ant uses only core NATS live subscriptions (ants lack `STREAM.MSG.GET`, and `inbox-<ant>` belongs to the daemon), while `ants post --wait` / `--follow` subscribes via core NATS to `ants.chan.<channel>.*` (`ANTS` does not republish `ants.chan.*`). Both miss messages stored on the hub while the uplink was down (and `ants post` waiters also miss outbox-sourced posts); read stored history after reconnect with `ants channel read` or `ants notes`.

## HTTPS fallback bridge

Where TLS-inspecting proxies block WebSocket upgrades, `ants bridge` carries a leaf's TCP uplink over HTTP/1.1 requests:

```text
leaf node --nats-leaf://127.0.0.1:7422--> ants bridge connect ==HTTPS==> reverse proxy --> ants bridge serve --> hub :7422
```

```sh
# hub (behind reverse proxy /bridge* -> 127.0.0.1:8423)
ANTS_BRIDGE_TOKEN=... ants bridge serve                                     # --listen 127.0.0.1:8423 --upstream 127.0.0.1:7422

# worker
ANTS_BRIDGE_TOKEN=... ants bridge connect https://hub.example.com/bridge    # --listen 127.0.0.1:7422
ants leaf config macbook --uplink nats-leaf://127.0.0.1:7422
```

- **Protocol**: under `<path>`, `POST /open` dials `--upstream` (10 s timeout, max 256 sessions) and returns a 128-bit hex id; `POST /s/<id>?off=N` sends upstream bytes at offset `N` (`204`); `GET /s/<id>?off=N` acks bytes before `N` and long-polls up to 20 s for bytes from `N` (empty `200` on timeout, `410` on EOF); `DELETE /s/<id>` closes the session. One request per direction is in flight, max 128 KiB per body, 1 MiB downstream buffer per session.
- **Retries and lifetime**: byte offsets make retries idempotent. `ants bridge connect` retries connection and `5xx` errors with backoff up to 4 s for 60 s; `ants bridge serve` expires sessions idle for 60 s, after which the worker drops the local socket and the leaf opens a new session.
- **Auth, TLS, proxies, headers**: `LEAFNODE` auth (`leaf-<machine>.creds`) runs inside the TCP stream; `--token` (`ANTS_BRIDGE_TOKEN`) adds constant-time `Bearer` checks on HTTP requests. `ants bridge connect` uses `rustls` (`SSL_CERT_FILE`), supports `--proxy http://[user:password@]host:port` (Basic auth), and sends repeatable `--header 'Name: value'`. Prefer `wss://` where WebSocket works.

## Dev Tunnels

Host a persistent private tunnel on the hub after signing in (`devtunnel user login -g -d`):

```sh
devtunnel create ants-hub
devtunnel port create ants-hub -p 7422                   # TCP leafnode port for worker leaves
devtunnel port create ants-hub -p 4222                   # optional: TCP client port for leaf-free workers
devtunnel port create ants-hub -p 8080 --protocol http   # optional: WebSocket listener (https/wss URL)
devtunnel host ants-hub
```

Connect from a worker:

```sh
devtunnel connect ants-hub.<region>                      # or -t "$(cat connect.token)"
```

`devtunnel connect` binds forwarded ports on loopback (`127.0.0.1` and IPv6 loopback) with identical port numbers, requiring NATS clients or the leaf in the same network namespace. Point a worker leaf at `nats-leaf://127.0.0.1:7422`; when running a local leaf on `127.0.0.1:4222`, omit port `4222` from the tunnel or move `--listen` to avoid port collision.

Direct `wss://` to `https://<tunnel-id>-8080.<region>.devtunnels.ms` requires anonymous port access (`devtunnel access create ants-hub -p 8080 --anonymous`) with NATS `.creds` enforcing auth; never expose the NATS monitoring port. Private HTTPS ports require a cookie or `X-Tunnel-Authorization: tunnel <token>`, which only `ants bridge connect --header` can send.

### `devtunnel connect` as a service

`scripts/install-units.sh --devtunnel ants-hub.<region>` (`--devtunnel-bin PATH`) installs `contrib/launchd/ants.devtunnel.plist` or `contrib/systemd/ants-devtunnel.service`, restarting `devtunnel connect` at most every 30 s (`~/Library/Logs/ants/devtunnel.log` or `journalctl --user -u ants-devtunnel`). systemd starts `ants-nats` after `ants-devtunnel`; launchd relies on leaf remote retries.

- **CLI login (default)**: run `devtunnel user login -g -d` as the service user with the tunnel owner or an authorized org/tenant account (`devtunnel access create ants-hub --org ORG` or `--tenant`). Keychain credentials last several days; re-running `devtunnel user login -g -d` after expiry recovers on the next 30 s retry without restart. Headless workers can use Entra service principals (`--sp-tenant-id`, `--sp-client-id`, `--sp-certificate-file`) or managed identities (`--mi-client-id`).
- **Connect token (`--devtunnel-token FILE`)**: issue a 24 h token (`devtunnel token ants-hub --scopes connect`), write `FILE` (`0600`), and install with `--devtunnel-token FILE` (`--access-token -` on stdin). Replace `FILE` before expiry and restart `ants.devtunnel` / `ants-devtunnel`.

Stopping `devtunnel connect` leaves the hub-side socket from `devtunnel host` to `nats-server` open until hub `ping_interval: "30s"` reaps it.

## Keepalive and limits

| Mechanism | Value | Behavior |
|---|---|---|
| Client and leaf `PING` | `20s` (2 missed drops) | Detects silent links (sleep, dead tunnel) in ~60 s and keeps 60 s idle-timeout proxies open. |
| Hub `ping_interval` | `30s` (default `2m`) | Reaps dead client, leaf, and `devtunnel host` sockets promptly. |
| Reconnect backoff | Unlimited; client `4s`, leaf `1–2s` | Reconnects across proxy restarts, tunnel drops, and `SIGHUP` switches; core subscribers miss offline traffic, durable consumers resume. |
| Daemon startup retry | `1s` doubling to `30s` | `ants daemon` and `ants channel serve` retry unreachable NATS TCP/DNS and `ANTS` stream info until the leaf uplink joins; bad URLs, TLS errors, bad `.creds`, and permission violations fail immediately (as do other CLI commands). |
| Progress ack and `ack_wait` | `20s` progress / `60s` `ack_wait` | Daemons renew in-hand messages every 20 s; silent links or crashes hold at most one message per pull slot for up to 60 s before redelivery (in-hand duplicates are ignored). Human `inbox-<name>` also uses `60s` `ack_wait`. |
| Roster heartbeat / TTL | `10s` refresh / `30s` TTL (`20s` stale) | Disconnected daemons expire within 30 s and reappear within 10 s of reconnect. |
| Retention and deduplication | `30d` streams/KV; `2m` dup window | `ANTS`, `ANTS_OUT`, `ANTS_IN`, `ants_sessions`, `ants_attempts`, `ants_links` and `ants_owners` retain 30 d (`ants_webhooks` 7 d). Outbox messages land after live traffic (up to ~2 min source backoff); timed-out publishes that reached the hub and replay after 2 min store twice. |
| `max_control_line` | `16384` (16 KiB) | Set in `server.conf` on hub and leaves so user and leaf JWT permissions fit in `CONNECT`. |
