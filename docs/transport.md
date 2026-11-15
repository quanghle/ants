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
resolver { type: full, dir: "/var/lib/ants/jwt" }
```

Without `max_file_store` and `max_memory_store` in its `jetstream` block, the hub takes up to 75% of the disk and of the memory. The accounts `ants auth` issues do not tell these limits, so the daemons warn of a store filling up only when `[storage]` of their config file repeats them (see [JetStream storage](../README.md#jetstream-storage)).

- `include "server.conf"` resolves relative to the config directory even if prefixed with `/`. Copy `server.conf` (`ants auth init`) next to the hub config; it configures operator/account trust and `max_control_line: 16384` so permission JWTs fit in `CONNECT`.
- `websocket` routes paths ending in `/leafnode` to leaf connections, `/mqtt` to MQTT, and any other path (such as `/nats`) to client connections. WebSocket and TCP clients authenticate identically with `.creds`.
- `leafnodes { no_advertise: true }` stops the hub from advertising `127.0.0.1:7422` to leaves, keeping reconnects on the proxy or tunnel address (likewise set `no_advertise: true` on clustered hubs or client WebSocket blocks).
- `resolver { type: full, dir: "..." }`, after the include, replaces `server.conf`'s `MEMORY` resolver (kept as `MEMORY` inside `server.conf` so leaf nodes can set their own directory and because `SIGHUP` cannot switch resolver types): the hub persists pushed accounts across restarts and syncs them to reconnecting leaf nodes (see [Revocation](#revocation)). Preloaded JWTs from `server.conf` apply only when newer than the stored JWT. Give an absolute `dir` (relative paths resolve against `nats-server`'s working directory), and restart the hub when first adding it. `ants auth push` and `ants doctor` check whether the hub runs a `full` resolver (`$SYS.REQ.CLAIMS.LIST`).
- `feature_flags { js_api_reply_v2: true }` scopes JetStream source control replies to `$JSC.R.<machine>.*.ANTS_OUT.>`, required for hub outbox sourcing under scoped leaf credentials.
- `nats-server` 2.16 enables `js_ack_fc_v2` by default, routing acks and flow control to `$JS.ACK.<domain>.<account_hash>.<stream>.<consumer>.…` and `$JS.FC.<domain>.<account_hash>.<stream>.…` (domain `_` without one). Credentials allow both v1 and v2 forms (including `$JS.FC.<machine>.*.ANTS_OUT.>` on leaf uplinks).

### Revocation

`ants auth revoke NAME` (or `--key U...`) adds user keys to the account's `revocations` map, rewrites `server.conf` (preserving the trusted operator JWT), and pushes the updated account JWT to `--nats`:

- **Hub push:** a short-lived system-account user (`system.nk`, 10 m TTL) publishes `$SYS.REQ.ACCOUNT.<account>.CLAIMS.UPDATE`, closing revoked connections immediately, then probes `$SYS.REQ.CLAIMS.LIST` to report whether a `full` resolver persisted it. Without a `full` resolver, copy `server.conf` to the hub and reload (`SIGHUP`) after every revocation (`ants auth push` retries unreachable servers).
- **Leaf propagation & catch-up:** leaves join the hub's system account via `leaf-<machine>.system.creds` (subscribe `$SYS.REQ.ACCOUNT.<account>.CLAIMS.UPDATE` and `$SYS._INBOX.<machine_hash>.*`; publish `_INBOX.ants-push.>` and `$SYS.REQ.CLAIMS.PACK`; leaf connections only). Live leaves apply pushes immediately and persist them in their own `full` resolver (`interval: "10s"`). Offline leaves catch up within 10 s of reconnecting to a `full`-resolver hub via `$SYS.REQ.CLAIMS.PACK` (against a `MEMORY`-resolver hub, run `ants auth push` again after reconnect).
- **Uplinks:** revoking `leaf-<machine>` drops that leaf's data uplink at the hub while leaving the system-account uplink intact.

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

- Every `ants` command accepts `--nats` (`ANTS_NATS_URL`, default `nats://127.0.0.1:4222`), `--creds` (`ANTS_CREDS`), and `--js-domain` (`ANTS_JS_DOMAIN`). Unset, each comes from the config file's `[cli]` section, else its top-level `nats` and `js_domain` (except for `ants daemon` and `ants add`), else the default. Client URLs (`--nats`, `--hub`, `nats` in the daemon and webhook configs) carrying a user and password or a token are refused before connecting, as async-nats would connect without them; authentication uses `.creds` files, whose NKey seed may sit in the OS credential store (`ants keys store`; see [auth.md](auth.md#private-keys-in-the-os-credential-store)). Leaf uplink URLs go to nats-server and are not checked, and leaf uplink credentials keep their seed in the file nats-server reads.
- `tls://` and `wss://` use `rustls` with platform root certificates. `SSL_CERT_FILE=/path/to/ca.pem` replaces the root store on direct client connections and `ants bridge connect`. Behind a local leaf, clients use `nats://127.0.0.1:4222` and private CA trust goes in `ants leaf config --ca-file`.
- Direct clients preserve URL paths and query strings on WebSocket upgrades, send no custom HTTP headers, and ignore `HTTPS_PROXY` and `HTTP_PROXY`. HTTP `CONNECT` proxies require a local leaf node or `ants bridge connect`.
- `ants add --enroll` and `ants enroll` use core NATS subjects (`ants.enroll.*`). Leaf uplinks carry a machine's requests up and their answers down (see [Enrollment behind a leaf node](#enrollment-behind-a-leaf-node)); `ants enroll` on a machine behind a leaf needs that leaf's uplink issued with `--enroll-approver`, or `--nats <hub-url>`. `ants add --hub <url>` sends the request to another server than `--nats`. A manager ant's `request_ant` asks `ants signer` for a new ant's credentials the same way, on `ants.antreq.<manager>`, and its `retire_ant` asks it there to revoke those of an ant it added (see [config.md](config.md#manager-ants)).
- An ant's `list_reactions` asks on `ants.reactions.<ant>` (core NATS, repeated every 2 s for up to 10 s) and hears the answer on `_INBOX.<ant>.reactions`, among its reply inboxes. `ants webhook serve` answers with each `[[human]]` entry's credentials, in queue group `reactions` (see [auth.md](auth.md#wire-acls-and-verification-invariants)).

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
resolver { type: full, dir: "/Users/alice/.config/ants/jwt", interval: "10s" }
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
    {
      urls: ["wss://hub.example.com:443/nats"]
      account: "A..."   # the system account
      credentials: "/Users/alice/.config/ants/auth/leaf-macbook.system.creds"
      tls {}
    }
  ]
}
```

- `--listen` accepts loopback addresses only (default `127.0.0.1:4222`).
- Explicit ports (`443`, `80`, `7422`) are always written because `nats-server` defaults portless remote URLs to `7422`.
- Multiple `--uplink` URLs must share one scheme. `tls://` and `wss://` always emit `tls {}` (or `tls { ca_file: "..." }`), preventing plaintext fallback and satisfying `nats-server` when `proxy { url: "..." }` is set. `--proxy` takes `http://host:port` or `https://host:port` without credentials.
- The second remote joins the hub's system account with `--system-creds` (default: `leaf-<machine>.system.creds` beside the uplink credentials, when present), so account pushes reach the leaf (see [Revocation](#revocation)). The `full` resolver after the include overrides `server.conf`'s `MEMORY` one and keeps the last pushed account in `jwt/` beside `leaf.conf`; every 10 s it asks the hub's resolver for newer accounts, to catch up on pushes made while the uplink was down.
- Switch uplinks by rerunning `ants leaf config`: it sends `SIGHUP` through the leaf node service `scripts/install-units.sh` installed for that `leaf.conf` (`launchctl kill SIGHUP gui/<uid>/ants.nats`, `systemctl --user reload ants-nats`) when it runs, and otherwise prints the commands to run by hand; local clients stay connected. `nats-server` reloads remotes added and dropped (a remote being its URLs, account and credentials) and their TLS settings, and refuses a reload changing anything else, so other changes (listen address, machine name, `server.conf` path, the proxy of an unchanged uplink) restart the leaf node instead (`launchctl kickstart -k`, `systemctl --user restart`), cutting local connections until they reconnect.

### JetStream domain

With `jetstream { domain: hub }` on the hub, every `ants` process sets `--js-domain hub` (`ANTS_JS_DOMAIN=hub` or `js_domain = "hub"` in `~/.ants/config.toml`), sending JetStream API and KV requests to `$JS.hub.API.>`.

- Each leaf runs local JetStream in domain `<machine>` (never `hub`) solely for `ANTS_OUT`. Plain `$JS.API.>` resolves to the leaf, so local clients reach the hub stream and KV buckets only via `$JS.hub.API.>`. The leaf blocks local `$JS.<machine>.API.>` traffic from crossing up while allowing the hub to query `$JS.<machine>.API.>` for outbox sourcing.
- Issue all user, leaf, and channel-manager credentials with `--js-domain hub`. Because the hub checks publish permissions before and after mapping `$JS.hub.API.>` to `$JS.API.>` (and `$JS.hub.API.$KV.>` to `$KV.>`), domain credentials allow both forms and work on the hub and on leaves.

### Leaf uplink credentials

```sh
ants auth leaf macbook --ant dev-a --ant dev-b --ant messenger --human human --human alice --js-domain hub
ants auth leaf macbook --ant dev-a --webhook github --js-domain hub
ants auth leaf macbook --add-ant dev-c --remove-webhook github --js-domain hub
ants auth leaf macbook --js-domain hub                   # the same members again
```

`ants auth leaf` writes `leaf-<machine>.creds` (`leafnodes.remotes[].credentials`):

- Sets `allowed_connection_types: ["LEAFNODE", "LEAFNODE_WS"]`, rejecting client connections.
- Grants the union of listed `--ant`, `--human` (`--ant human` also counts as human), and `--webhook` permissions (read from `<webhook>.creds`), plus outbox sourcing subjects (`$JS.<machine>.API.*.ANTS_OUT`, `$JS.S.<machine>.>`, `$JSC.R.<machine>.*.ANTS_OUT.>`, `_INBOX.<human>.leaf.<machine>.>`) and literal subject anchors (such as `ants.inbox.dev-a.dev-a`) so hub wildcard stream interest crosses the uplink.
- Carries enrollment requests up (publish `ants.enroll.*`) and their answers down (subscribe `ants.enroll.*.reply`), and likewise each of its ants' requests for new ants (`ants.antreq.<ant>`, answered on `ants.antreq.<ant>.reply`) and for reactions (`ants.reactions.<ant>`, answered in its reply inboxes, `_INBOX.<ant>.reactions`), never other machines' requests or answers from the machine.
- Carries renewal requests up (publish `ants.renew.*`) and their answers down (subscribe `ants.renew.*.reply`), so `ants renew` works behind the leaf; each request proves the credentials it renews by signature (see [auth.md](auth.md#renewing-over-nats)). A human's credentials carry the request of the uplink itself, which connects as no client.
- With `--enroll-approver`, for the machine holding the account key, also carries what `ants enroll` and `ants signer` need: every request down, answers up, tool requests to humans up and their decisions down, and the admission of a new ant into `ANTS` and `ants_links` (stream info and update, replies under `_INBOX._enroll`).
- Excludes `ants.out.>`. Local clients authenticate individually against `server.conf`; the leaf filters traffic to uplink bounds so unauthorized local clients cannot drop the uplink. Each run records members in `issued.jsonl` so a bare `ants auth leaf <machine>` reissues the previous set (`--add-*`/`--remove-*` adjust it; see [auth.md](auth.md#credentials-across-upgrades)).
- **Size ceiling:** each ant adds ~4 KB (~8 KB with `--js-domain`). `nats-server` caps a leaf's `CONNECT` at `16 × max_control_line` (256 KiB), so `ants auth leaf`, `ants enroll approve` and `ants signer` refuse uplink JWTs over `252 KiB` (~30 ants with a domain, ~60 without); run extra ants behind a second leaf under another machine name.

### Enrollment behind a leaf node

```sh
ants auth enroll dev-c                                            # account-key machine
ants add dev-c --workdir ~/src/app --enroll enroll-dev-c.creds    # leaf machine
ants enroll approve dev-c --js-domain hub                         # account-key machine
```

- **Request & verification:** `ants add --enroll` reads `server_name` and uplink credentials from `leaf.conf` (`--leaf-config`), signing the request with both the enrollment key and the leaf uplink key. `ants enroll` rejects expired, revoked or mismatched enrollment/uplink JWTs or invalid signatures.
- **Approval & uplink reconnect:** `ants enroll approve` re-signs the leaf uplink JWT to add the ant (`leaf_jwt`), records the ant in `issued.jsonl`, and admits it to `ANTS` and `ants_links` outbox sourcing if `ANTS_IN` sources the machine. `ants add` writes `<ant>.creds` and the renewed uplink credentials (unless modified concurrently), reloads `ants.daemon`, and then has `ants.nats` connect the uplink again with the new file: it toggles `./` on the remote's `credentials` path in `leaf.conf` and reloads `ants.nats` (`SIGHUP`), reconnecting only the uplink while local clients stay connected. Without the service installed, it prints the commands. `ants renew` does the same when the renewed file is the leaf's uplink credentials.
- **Manager `request_ant`:** signs with the manager's credentials instead of enrollment credentials (`ants signer` renews the uplink JWT and outbox admission). To avoid dropping local client connections on restart, the daemon toggles `./` on the remote's `credentials` path in `leaf.conf` and reloads `ants.nats` (`SIGHUP`), reconnecting only the uplink.

## Store and forward

Run `ants leaf outbox` as a human while the leaf is connected to the hub (and rerun when ants or humans on the machine change). Without `--ant` or `--human`, it admits those the uplink credentials of the machine's `leaf.conf` carry (`--leaf-config FILE`), changed by `--add-ant`, `--add-human`, `--remove-ant` and `--remove-human`:

```sh
ants leaf outbox macbook --ant dev-a --ant dev-b --ant messenger --human human --human alice
ants leaf outbox macbook
ants leaf outbox macbook --remove
```

JetStream sources move buffered messages to the hub automatically across outages and restarts:

1. **Leaf outbox (`ANTS_OUT`)**: in domain `<machine>` (`ants.out.>`, file storage, 30 d retention). When publishing to `ANTS` fails with no stream / no responders or times out, clients write the same payload and `Nats-Msg-Id` to `ants.out.<rest>`.
2. **Hub intake (`ANTS_IN`)**: sources each machine's `ANTS_OUT` via `$JS.<machine>.API` (delivery `$JS.S.<machine>`) under `ants.in.<machine>.>`, isolating machines from each other.
3. **Hub admission (`ANTS`, `ants_links` and `ants_owners`)**: `ANTS` sources from `ANTS_IN` per admitted user only subjects allowed for that role (`ants.out.inbox.*.<ant>`, `ants.out.notes.*.<user>`, `ants.out.chan.*.<user>`, `ants.out.dead.<ant>`, `ants.out.timeline.*.<ant>`, or `ants.out.inbox.>` for all machine inboxes when a human is listed), rewriting them to `ants.*`. `ants_links` sources `ants.in.<machine>.ants.out.links.<ant>.*.*` into `$KV.ants_links.<ant>.<thread>.<recipient>`. When a human is listed, `ants_owners` sources `ants.in.<machine>.ants.out.owners.*.*` into `$KV.ants_owners.<thread>.<recipient>`; its stream refuses a second record of a key, but drops one repeating the first's `Nats-Msg-Id` (`owner.<key>`) for as long as it keeps records, so no record from an outbox is refused, which would stall the source. Unadmitted subjects stay in `ANTS_IN`. New admissions start after current `ANTS_IN` entries, and `Nats-Msg-Id` deduplicates within `ANTS`'s 24 h window. Only `ANTS` needs that window: a message stored directly never passes through `ANTS_OUT` or `ANTS_IN`, so it and its buffered twin meet in `ANTS`, which drops whichever lands second, sourced or published.

`ants leaf outbox` uses reply prefix `_INBOX.<human>.leaf.<machine>`, updates sources, and waits up to 10 s for `ANTS_IN` to hear from `ANTS_OUT`. If inactive, it fails naming `feature_flags { js_api_reply_v2: true }` on the hub while leaving the setup in place.

| Operation | While leaf uplink is down |
|---|---|
| Inbox messages and replies (`ants ask`, daemon replies, ant messages) | Buffered in `ANTS_OUT`; daemons record unacked requests in `<home>/answered` and ack redeliveries on reconnect (ignoring `"no responders"`) without re-prompting. |
| Unsequenced notes (`ants notes <thread> TEXT`), channel posts (`ants post`, daemon replies), passed links (`ants_links`) | Buffered in `ANTS_OUT` and sourced into `ANTS` or `ants_links` on reconnect. |
| Daemon dead letters (`ants.dead.<ant>`) | Buffered in `ANTS_OUT` and noted in `<home>/buried`; notice and `+TERM` run on redelivery (or within 30 s of reconnect on the 6th extra delivery) after checking `deadread-<ant>` against duplicates landing past the 24 h window. |
| Pull consumers, acks, stream reads (`ants inbox`, `ants channel read`, `ants notes`, `ants dead list`/`replay`) | Fail immediately; require the hub stream. |
| Sequenced `post_notes`, tool approvals (`ants.approvals.*`), channel admin (`ants.chanadm.*`), `ants cancel` of a prompt on another machine (`ants.cancel.*`), `ants queue` of an ant on another machine (`ants.queue.*`) | Fail immediately; require live hub coordination or optimistic concurrency. |
| Enrollment (`ants.enroll.*`) | `ants add --enroll` keeps repeating its request until the uplink is back and an answer comes or the enrollment credentials expire or are refused. |
| Requests for new ants (`ants.antreq.*`) | `request_ant` keeps repeating its request until the uplink is back and an answer comes or its timeout passes. |
| Requests for reactions (`ants.reactions.*`) | `list_reactions` repeats its request for up to 10 s, then reports that no answer came. |
| Renewal (`ants.renew.*`) | `ants renew` keeps repeating its request until the uplink is back and an answer comes or the credentials expire. |
| KV writes (`ants_roster`, `ants_sessions`, `ants_attempts` (`ants withdraw` included), `ants_channels`, `ants_humans`, `ants_chains`, `ants_read`, `ants_presence`, `ants_notify`, `ants_reactions`, `ants_threads`, `ants_webhooks`) | Fail immediately so late puts cannot overwrite newer keys; daemons refresh `ants_roster` every 10 s on reconnect. |
| Owner records (`ants_owners`, from `ants ask`) | Buffered in `ANTS_OUT` ahead of the message and sourced into `ants_owners` on reconnect; a second record of a thread and ant is dropped as a repeat. `ANTS` and `ants_owners` source apart, so a daemon holds a human's message sourced from an outbox (`Nats-Stream-Source`) up to 10 s until its thread's owner is recorded for the ant. Webhooks reach the hub directly and record no owner in an outbox. |

### Republishing and live waiters

Sourced messages enter `ANTS` without a core NATS publish on `ants.inbox.*`. `ants init` configures `ANTS` to republish stored `ants.inbox.*.*` messages to `_INBOX.{{wildcard(1)}}.stored.{{wildcard(2)}}`. Live inbox waiters subscribe to both `ants.inbox.<user>.*` and `_INBOX.<user>.stored.*` (deduplicating the last 1,024 messages by `Nats-Msg-Id`), accepting `_INBOX.<user>.stored.<from>` messages only when they carry no reply subject, a single `Nats-Stream: ANTS` header and a single `Nats-Subject: ants.inbox.<user>.<from>` header (blocking spoofed direct-get or push-consumer deliveries).

Because core NATS republishes do not cross a down uplink, waiters also poll `ANTS` across outages:

- **`ants ask --wait` / `--follow`**: polls `ANTS` every 5 s (up to 100 messages/round from the starting sequence): humans via batched direct gets (`$JS.API.DIRECT.GET.ANTS`, `next_by_subj`) updating `ants_read`; ants via short-lived no-ack spare consumers `inbox-<ant>_1..3` (`inactive_threshold` 30 s).
- **Daemon `ask_ant` and escalation waiters**: listen on live subscriptions while connected and receive outage replies via durable consumer `inbox-<ant>` on reconnect.
- **`ants post --wait` / `--follow`**: subscribes to `ants.chan.<channel>.*` and polls stored channel posts every 5 s (humans via batched direct gets; ants via `chanread-<ant>_1..3`), starting from the publish ack sequence or 5 m before send time if buffered in `ANTS_OUT`, deduplicating the last 1,024 post IDs.
- **Reactions while following** (`ants inbox -f`, `ants ask -f`, `ants post -f`): credentials holding `$JS.API.CONSUMER.CREATE.KV_ants_reactions` (humans) open one ordered push consumer (`DeliverPolicy::New`, headers only) on `$KV.ants_reactions.>` before the message is sent or the inbox read, and print changes to shown messages; ant credentials skip it silently, and a consumer that cannot be created logs one warning while following continues.

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

`--token-file FILE` reads the token from `FILE` at start instead of `--token` or `ANTS_BRIDGE_TOKEN`, keeping it out of the command line and environment.

- **Protocol**: under `<path>`, `POST /open` dials `--upstream` (10 s timeout, max 256 sessions) and returns a 128-bit hex id; `POST /s/<id>?off=N` sends upstream bytes at offset `N` (`204`); `GET /s/<id>?off=N` acks bytes before `N` and long-polls up to 20 s for bytes from `N` (empty `200` on timeout, `410` on EOF); `DELETE /s/<id>` closes the session. One request per direction is in flight, max 128 KiB per body, 1 MiB downstream buffer per session.
- **Retries and lifetime**: byte offsets make retries idempotent. `ants bridge connect` retries connection and `5xx` errors with backoff up to 4 s for 60 s; `ants bridge serve` expires sessions idle for 60 s, after which the worker drops the local socket and the leaf opens a new session.
- **Auth, TLS, proxies, headers**: `LEAFNODE` auth (`leaf-<machine>.creds`) runs inside the TCP stream; `--token` (`ANTS_BRIDGE_TOKEN`) adds constant-time `Bearer` checks on HTTP requests. `ants bridge connect` uses `rustls` (`SSL_CERT_FILE`), supports `--proxy http://[user:password@]host:port` (Basic auth), and sends repeatable `--header 'Name: value'`. Prefer `wss://` where WebSocket works.

### `ants bridge` as a service

Write the shared bearer token to a file (`0600`) on both machines, e.g. `openssl rand -hex 32 > ~/.config/ants/bridge.token`, then:

```sh
# hub
scripts/install-units.sh --nats-conf ~/.config/ants/hub.conf --no-daemon --bridge-serve --bridge-token ~/.config/ants/bridge.token
# worker
scripts/install-units.sh --bridge https://hub.example.com/bridge --bridge-token ~/.config/ants/bridge.token \
    [--bridge-proxy http://[user:password@]proxy:3128] [--bridge-ca corp-ca.pem]
```

- **Units**: `--bridge-serve` installs `contrib/launchd/ants.bridge-serve.plist` or `contrib/systemd/ants-bridge-serve.service` (`ants bridge serve --token-file FILE`, default `--listen` and `--upstream`); `--bridge URL` installs `contrib/launchd/ants.bridge.plist` or `contrib/systemd/ants-bridge.service` (`ants bridge connect --token-file FILE [--proxy URL] URL`, `SSL_CERT_FILE` from `--bridge-ca`). Both restart at most every 10 s after a failure (`~/Library/Logs/ants/bridge.log`, `bridge-serve.log`, or `journalctl --user -u ants-bridge -u ants-bridge-serve`). `--bridge` excludes `--devtunnel`: both listen on `127.0.0.1:7422`.
- **Start order**: systemd starts `ants-nats` after `ants-bridge` on the worker, and `ants-bridge-serve` after `ants-nats` (pulling it in) on the hub. launchd has no start order: the worker's leaf node retries its uplink until `ants.bridge` listens, and on the hub, sessions opened before `ants.nats` listens fail and the leaves retry them.
- **Token rotation**: the token file is read at each start; replace it on both ends and restart `ants.bridge-serve` / `ants-bridge-serve` and `ants.bridge` / `ants-bridge` (`launchctl kickstart -k gui/$(id -u)/<label>` or `systemctl --user restart <unit>`).
- **Checks**: `ants doctor` finds the installed units and checks that each runs, that its token file exists with mode `0600`, and that its bridge answers within 5 s and takes the token (`bridge url` through the unit's proxy and `SSL_CERT_FILE`, `bridge serve url` on the listen address), naming the fix of each problem.
- **Proxy credentials**: `--bridge-proxy` credentials appear in the installed unit and the process arguments. `--header` values (e.g. `X-Tunnel-Authorization`) and other `--listen` or `--upstream` addresses need an edit of the installed unit.

## Dev Tunnels

Host a persistent private tunnel on the hub after signing in (`devtunnel user login -g -d`):

```sh
devtunnel create ants-hub
devtunnel port create ants-hub -p 7422                   # TCP leafnode port for worker leaves
devtunnel port create ants-hub -p 4222                   # optional: TCP client port for leaf-free workers
devtunnel port create ants-hub -p 8080 --protocol http   # optional: WebSocket listener (https/wss URL)
devtunnel host ants-hub                                  # or as a service: install-units.sh --devtunnel-host ants-hub
```

Connect from a worker:

```sh
devtunnel connect ants-hub.<region>                      # or -t "$(cat connect.token)"
```

`devtunnel connect` binds forwarded ports on loopback (`127.0.0.1` and IPv6 loopback) with identical port numbers, requiring NATS clients or the leaf in the same network namespace. Point a worker leaf at `nats-leaf://127.0.0.1:7422`; when running a local leaf on `127.0.0.1:4222`, omit port `4222` from the tunnel or move `--listen` to avoid port collision.

Direct `wss://` to `https://<tunnel-id>-8080.<region>.devtunnels.ms` requires anonymous port access (`devtunnel access create ants-hub -p 8080 --anonymous`) with NATS `.creds` enforcing auth; never expose the NATS monitoring port. Private HTTPS ports require a cookie or `X-Tunnel-Authorization: tunnel <token>`, which only `ants bridge connect --header` can send.

### `devtunnel host` as a service

On the hub, `scripts/install-units.sh --devtunnel-host ants-hub` (`--devtunnel-bin PATH`) installs `contrib/launchd/ants.devtunnel-host.plist` or `contrib/systemd/ants-devtunnel-host.service`, restarting `devtunnel host` at most every 30 s whenever it exits (`~/Library/Logs/ants/devtunnel-host.log` or `journalctl --user -u ants-devtunnel-host`). systemd starts `ants-devtunnel-host` after `ants-nats` (and pulls it in); under launchd, connections arriving before `ants.nats` listens fail and the leaves retry them. Combine with the hub's own units, e.g. `scripts/install-units.sh --nats-conf ~/.config/ants/hub.conf --no-daemon --channel-manager CREDS --devtunnel-host ants-hub`.

`devtunnel host` requires the CLI login of the tunnel owner (or an account with host access):

- **Sign in** as the service user before installing: `devtunnel user login -g -d` (GitHub, device code; `-e` for Microsoft Entra). Check the login with `devtunnel user show`.
- **Renew the login** before it expires (keychain credentials last several days): re-run `devtunnel user login -g -d` as the same user. An expired login makes `devtunnel host` exit or fail to start; after a new login the next 30 s restart recovers, without reinstalling. Restart immediately with `launchctl kickstart -k gui/$(id -u)/ants.devtunnel-host` or `systemctl --user restart ants-devtunnel-host`.
- **Checks**: `ants doctor` runs `devtunnel user show` for an installed `ants.devtunnel-host` / `ants-devtunnel-host` and fails without a login, warning within a day of an expiry the CLI reports.
- **Headless hubs**: sign in with an Entra service principal (`devtunnel user login --sp-tenant-id ID --sp-client-id ID --sp-certificate-file FILE`) or managed identity (`--mi-client-id ID`) that owns or may host the tunnel, avoiding interactive renewal.
- **While the host is down**: worker `devtunnel connect` and leaf uplinks fail and retry; leaves queue outbound traffic in `ANTS_OUT` (see "Store and forward").

### `devtunnel connect` as a service

`scripts/install-units.sh --devtunnel ants-hub.<region>` (`--devtunnel-bin PATH`) installs `contrib/launchd/ants.devtunnel.plist` or `contrib/systemd/ants-devtunnel.service`, restarting `devtunnel connect` at most every 30 s (`~/Library/Logs/ants/devtunnel.log` or `journalctl --user -u ants-devtunnel`). systemd starts `ants-nats` after `ants-devtunnel`; launchd relies on leaf remote retries.

- **CLI login (default)**: run `devtunnel user login -g -d` as the service user with the tunnel owner or an authorized org/tenant account (`devtunnel access create ants-hub --org ORG` or `--tenant`). Keychain credentials last several days; re-running `devtunnel user login -g -d` after expiry recovers on the next 30 s retry without restart. Headless workers can use Entra service principals (`--sp-tenant-id`, `--sp-client-id`, `--sp-certificate-file`) or managed identities (`--mi-client-id`).
- **Connect token (`--devtunnel-token FILE`)**: issue a 24 h token (`devtunnel token ants-hub --scopes connect`), write `FILE` (`0600`), and install with `--devtunnel-token FILE` (`--access-token -` on stdin). Replace `FILE` before expiry and restart `ants.devtunnel` / `ants-devtunnel`.
- **Checks**: `ants doctor` checks the installed unit: running, and either the CLI login (as for `devtunnel host`) or the token file, present with mode `0600` and failing once its token expired, warning 6 h before (the JWT's `exp`; a day after the file was written for a token that is no JWT).

Stopping `devtunnel connect` leaves the hub-side socket from `devtunnel host` to `nats-server` open until hub `ping_interval: "30s"` reaps it.

## Keepalive and limits

| Mechanism | Value | Behavior |
|---|---|---|
| Client and leaf `PING` | `20s` (2 missed drops) | Detects silent links (sleep, dead tunnel) in ~60 s and keeps 60 s idle-timeout proxies open. |
| Hub `ping_interval` | `30s` (default `2m`) | Reaps dead client, leaf, and `devtunnel host` sockets promptly. |
| Reconnect backoff | Unlimited; client `4s`, leaf `1–2s` | Reconnects across proxy restarts, tunnel drops, and `SIGHUP` switches; core subscribers miss offline traffic, durable consumers resume. |
| Daemon startup retry | `1s` doubling to `30s` | `ants daemon` and `ants channel serve` retry unreachable NATS TCP/DNS and unanswered JetStream requests (the info of the user's own inbox consumer) until the leaf uplink joins; bad URLs, TLS errors, bad `.creds`, and permission violations fail immediately (as do other CLI commands). |
| Progress ack and `ack_wait` | `20s` progress / `60s` `ack_wait` | Daemons renew in-hand messages every 20 s; silent links or crashes hold at most one message per pull slot for up to 60 s before redelivery (in-hand duplicates are ignored). |
| Roster heartbeat / TTL | `10s` refresh / `30s` TTL (`20s` stale) | Disconnected daemons expire within 30 s and reappear within 10 s of reconnect. |
| Retention and deduplication | `30d` streams/KV; `24h` dup window on `ANTS` (`ants_owners` 30 d, others `2m`) | `ANTS`, `ANTS_OUT`, `ANTS_IN`, `ants_sessions`, `ants_attempts`, `ants_links`, `ants_owners` and `ants_reactions` retain 30 d (`ants_webhooks` 7 d). Outbox messages land after what reached the hub meanwhile: usually within a second of reconnect, at worst ~2.5 min later (30 s source request timeout plus up to 2 min retry backoff). Readers keep stream order rather than sorting by `sent_at`, as machine clocks differ. A timed-out publish that reached the hub after all stores once if its outbox copy lands within 24 h. |
| `max_control_line` | `16384` (16 KiB) | Set in `server.conf` on hub and leaves so user JWT permissions fit in a client's `CONNECT`. A leaf's `CONNECT` may take 16 times as much, so `ants auth leaf` and enrollment approval refuse uplink JWTs over 252 KiB. |
