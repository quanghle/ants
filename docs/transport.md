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

- `include "server.conf"` resolves relative to the config directory even if prefixed with `/`. Copy `server.conf` (`ants auth init`) next to the hub config; it configures operator/account trust and `max_control_line: 16384` so permission JWTs fit in `CONNECT`.
- `websocket` routes paths ending in `/leafnode` to leaf connections, `/mqtt` to MQTT, and any other path (such as `/nats`) to client connections. WebSocket and TCP clients authenticate identically with `.creds`.
- `leafnodes { no_advertise: true }` stops the hub from advertising `127.0.0.1:7422` to leaves, keeping reconnects on the proxy or tunnel address (likewise set `no_advertise: true` on clustered hubs or client WebSocket blocks).
- `resolver { type: full, dir: "..." }`, after the include, replaces `server.conf`'s `MEMORY` resolver: the hub keeps the accounts pushed to it in that directory and hands them to leaf nodes that missed a push (see [Revocation](#revocation)). A hub running without it needs a restart to take it (a change of resolver is not reloadable).
- `feature_flags { js_api_reply_v2: true }` scopes JetStream source control replies to `$JSC.R.<machine>.*.ANTS_OUT.>`, required for hub outbox sourcing under scoped leaf credentials.

### Revocation

`server.conf` preloads the team's account JWT into a `MEMORY` resolver, or into the `full` resolver configured after it. `ants auth revoke NAME` (or `--key U...`) adds the user keys to the account's `revocations` (key to revocation time; JWTs of the key issued at or before it are refused), rewrites `server.conf` keeping its operator JWT (nats-server reloads no change of the trusted operator), and pushes the account JWT to the `--nats` server:

- **Push:** a user of the system account, signed with `system.nk` for ten minutes, requests `$SYS.REQ.ACCOUNT.<account>.CLAIMS.UPDATE`; the server applies the account at once and closes the revoked users' connections. Pointed at a leaf node, the push updates that leaf only. An unreachable server leaves the revocation recorded in `revoked.json` and `server.conf`; `ants auth push` retries. Hubs on a `server.conf` from before revocations (whose system account allows no connection) need the new one copied and reloaded first.
- **Leaf nodes:** a leaf node configured with the system account credentials `ants auth leaf` writes beside its uplink's (`leaf-<machine>.system.creds`) joins the hub's system account through a second uplink, hears the pushed account and applies it at once, closing the revoked users' local connections. The push lists the leaf nodes that applied it (by server name, the machine). Their `full` resolver (`resolver { type: full, dir: ".../jwt", interval: "10s" }`) stores the pushed account, so a restart or reload with an older `server.conf` keeps the revocations; a newer `server.conf` replaces it. The hub needs a `server.conf` whose system account allows leaf connections (one written by `ants auth init`, `revoke` or `push` since this uplink exists) copied and reloaded first. Leaf nodes configured before this second uplink existed need `ants auth leaf` and `ants leaf config` run again and a restart (a change of resolver is not reloadable).
- **Catching up:** every 10 s (the resolver's `interval`; every minute in a `leaf.conf` written without it) a leaf node's resolver sends `$SYS.REQ.CLAIMS.PACK` with a hash of its accounts; a hub with a `full` resolver (see [Hub](#hub)) answers with its own when they differ, and the leaf keeps those newer than its own. A leaf node not connected during a push thus applies it within 10 s of reconnecting, closing the revoked users' local connections, with no other push. Behind a hub without a `full` resolver, a leaf node not connected during the push misses it: run `ants auth push` again once it is, or copy it the new `server.conf`.
- **System account uplink permissions:** it may subscribe only to `$SYS.REQ.ACCOUNT.<account>.CLAIMS.UPDATE` and to the leaf's own system inbox (`$SYS._INBOX.<hash of the machine name>.*`, where the answers to its resolver arrive), and publish only answers on `_INBOX.ants-push.>` and `$SYS.REQ.CLAIMS.PACK`, and only as a leaf node connection. System account credentials issued before catching up existed lack the inbox and `$SYS.REQ.CLAIMS.PACK`: renew them with `ants auth leaf` and restart the leaf node.
- **Reload:** copying the new `server.conf` over the hub's or a leaf's and sending `SIGHUP` applies the revocations too, also closing connections. Copy it to a hub without a `full` resolver in any case: such a hub keeps its account in memory only, and a restart otherwise loads the old one. A leaf node without a system account uplink authenticates its local clients against its own copy, so a revoked ant behind it keeps working through its uplink until it reloads a new `server.conf` (or its uplink credentials are reissued without the ant).
- **Uplinks:** revoking `leaf-<machine>` drops that leaf's uplink at the hub. Its system account uplink stays, as revocations apply to the team's account only; it carries account updates and nothing else.

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

- Every `ants` command accepts `--nats` (`ANTS_NATS_URL`, default `nats://127.0.0.1:4222`), `--creds` (`ANTS_CREDS`), and `--js-domain` (`ANTS_JS_DOMAIN`). Unset, each comes from the config file's `[cli]` section, else its top-level `nats` and `js_domain` (except for `ants daemon` and `ants add`), else the default. Client URLs (`--nats`, `--hub`, `nats` in the daemon and webhook configs) carrying a user and password or a token are refused before connecting, as async-nats would connect without them; authentication uses `.creds` files. Leaf uplink URLs go to nats-server and are not checked.
- `tls://` and `wss://` use `rustls` with platform root certificates. `SSL_CERT_FILE=/path/to/ca.pem` replaces the root store on direct client connections and `ants bridge connect`. Behind a local leaf, clients use `nats://127.0.0.1:4222` and private CA trust goes in `ants leaf config --ca-file`.
- Direct clients preserve URL paths and query strings on WebSocket upgrades, send no custom HTTP headers, and ignore `HTTPS_PROXY` and `HTTP_PROXY`. HTTP `CONNECT` proxies require a local leaf node or `ants bridge connect`.
- `ants add --enroll` and `ants enroll` use core NATS subjects (`ants.enroll.*`). Leaf uplinks carry a machine's requests up and their answers down (see [Enrollment behind a leaf node](#enrollment-behind-a-leaf-node)); `ants enroll` on a machine behind a leaf needs that leaf's uplink issued with `--enroll-approver`, or `--nats <hub-url>`. `ants add --hub <url>` sends the request to another server than `--nats`.

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
```

`ants auth leaf` writes `leaf-<machine>.creds` (`leafnodes.remotes[].credentials`):

- Sets `allowed_connection_types: ["LEAFNODE", "LEAFNODE_WS"]`, rejecting client connections.
- Grants the union of listed `--ant`, `--human` (`--ant human` also counts as human), and `--webhook` permissions (read from `<webhook>.creds`), plus outbox sourcing subjects (`$JS.<machine>.API.*.ANTS_OUT`, `$JS.S.<machine>.>`, `$JSC.R.<machine>.*.ANTS_OUT.>`, `_INBOX.<human>.leaf.<machine>.>`) and literal subject anchors (such as `ants.inbox.dev-a.dev-a`) so hub wildcard stream interest crosses the uplink.
- Carries enrollment requests up (publish `ants.enroll.*`) and their answers down (subscribe `ants.enroll.*.reply`), never other machines' requests or answers from the machine.
- With `--enroll-approver`, for the machine holding the account key, also carries what `ants enroll` needs: every request down, answers up, and the admission of a new ant into `ANTS` and `ants_links` (stream info and update, replies under `_INBOX._enroll`).
- Excludes `ants.out.>`. Local clients still authenticate individually against `server.conf`; the leaf filters traffic to uplink bounds so unauthorized local clients cannot drop the uplink. Reissue `leaf-<machine>.creds` whenever machine users or webhooks change; an enrollment through the leaf renews them itself (below). Uplink credentials issued before enrollment crossed leaves lack `ants.enroll.*`: issue them once more. `ants leaf config` warns about uplink credentials outdated or expiring soon and names the `ants auth leaf` and `ants leaf outbox` commands renewing them (README, Credentials across upgrades).
- Grows with every user: each ant adds about 4 KB to the JWT (7.5 KB with `--js-domain`, which doubles the JetStream API subjects), each human more. The hub (nats-server 2.14 and later) closes a leaf connection whose `CONNECT` exceeds 16 × `max_control_line` (256 KiB with `server.conf`), so `ants auth leaf` refuses, before writing anything, an uplink JWT over 252 KiB (about 34 ants with a domain, 66 without), naming its size and the limit. Run the rest of the machine's users behind another leaf node under another machine name. Permissions are not widened to patterns to save space: the uplink allows only what the machine's users may do.

### Enrollment behind a leaf node

```sh
ants auth enroll dev-c                                   # account-key machine
ants add dev-c --workdir ~/src/app --enroll enroll-dev-c.creds   # leaf machine, through its leaf node
ants enroll approve dev-c --js-domain hub                # account-key machine
```

- `ants add --enroll` reads the machine's `leaf.conf` (`--leaf-config FILE`, default `~/.config/ants/leaf.conf` when present) for its `server_name` and uplink credentials, and names the leaf in the request, signed with the uplink key. Each repeat of the request reads the uplink credentials again.
- Every request carries the user JWT of its enrollment credentials and a signature by their key over the request (ant, public key, host, time, enrollment JWT, leaf machine and uplink JWT). `ants enroll` ignores requests whose enrollment credentials are not the team account's for that ant, expired, revoked (`ants auth revoke`) or unproven, and requests naming a leaf whose uplink JWT is not the team account's `leaf-<machine>` leaf-only user, is revoked or whose signature fails; a revoked uplink key is never renewed, as a JWT signed after its revocation would pass. A leaf thus cannot ask for an ant without its enrollment credentials, nor name another machine's leaf.
- Approval of a request naming a leaf:
  - re-signs the leaf's uplink JWT for the same key, adding the ant's permissions (`Permissions::for_leaf` for the ant alone) to what it already allows, and sends it with the ant's JWT (`leaf_jwt`); approval fails, before admitting or answering anything, when the renewed JWT would exceed the uplink limit (above): reject the request and enroll the ant behind another leaf node;
  - when `ANTS_IN` sources the machine's outbox, adds the ant's admission to `ANTS` (without its inbox subjects when a human's admission takes every inbox of the machine) and to `ants_links`, starting after the current `ANTS_IN` entries, before answering.
- The asking machine checks the renewed JWT (team account, leaf key, `leaf-<machine>`) and writes it into the uplink credentials file, unless that file changed meanwhile to allow something the renewal lacks. `nats-server` reads the file at each connect but does not reconnect on `SIGHUP` for new contents, so the leaf node restarts: `ants add` restarts the leaf node service `scripts/install-units.sh` installed for that `leaf.conf` (`launchctl kickstart -k gui/<uid>/ants.nats`, `systemctl --user restart ants-nats`) when it runs, last, after reloading the daemon; otherwise it prints those commands to run by hand. Local clients, the daemon's ants among them, reconnect.
- A rogue leaf may hear answers to its machine's requests: they hold only public JWTs, useless without the seeds that stay on the asking machine. Later `ants auth leaf` and `ants leaf outbox` runs replace the lists, so name the enrolled ant in them too.

## Store and forward

Run `ants leaf outbox` as a human while the leaf is connected to the hub (and rerun when ants or humans on the machine change):

```sh
ants leaf outbox macbook --ant dev-a --ant dev-b --ant messenger --human human --human alice
ants leaf outbox macbook --remove
```

JetStream sources move buffered messages to the hub automatically across outages and restarts:

1. **Leaf outbox (`ANTS_OUT`)**: in domain `<machine>` (`ants.out.>`, file storage, 30 d retention). When publishing to `ANTS` fails with no stream / no responders or times out, clients write the same payload and `Nats-Msg-Id` to `ants.out.<rest>`.
2. **Hub intake (`ANTS_IN`)**: sources each machine's `ANTS_OUT` via `$JS.<machine>.API` (delivery `$JS.S.<machine>`) under `ants.in.<machine>.>`, isolating machines from each other.
3. **Hub admission (`ANTS`, `ants_links` and `ants_owners`)**: `ANTS` sources from `ANTS_IN` per admitted user only subjects allowed for that role (`ants.out.inbox.*.<ant>`, `ants.out.notes.*.<user>`, `ants.out.chan.*.<user>`, `ants.out.dead.<ant>`, or `ants.out.inbox.>` for all machine inboxes when a human is listed), rewriting them to `ants.*`. `ants_links` sources `ants.in.<machine>.ants.out.links.<ant>.*.*` into `$KV.ants_links.<ant>.<thread>.<recipient>`. When a human is listed, `ants_owners` sources `ants.in.<machine>.ants.out.owners.*.*` into `$KV.ants_owners.<thread>.<recipient>`; its stream refuses a second record of a key, but drops one repeating the first's `Nats-Msg-Id` (`owner.<key>`) for as long as it keeps records, so no record from an outbox is refused, which would stall the source. Unadmitted subjects stay in `ANTS_IN`. New admissions start after current `ANTS_IN` entries, and `Nats-Msg-Id` deduplicates within `ANTS`'s 24 h window. Only `ANTS` needs that window: a message stored directly never passes through `ANTS_OUT` or `ANTS_IN`, so it and its buffered twin meet in `ANTS`, which drops whichever lands second, sourced or published.

`ants leaf outbox` uses reply prefix `_INBOX.<human>.leaf.<machine>`, updates sources, and waits up to 10 s for `ANTS_IN` to hear from `ANTS_OUT`. If inactive, it fails naming `feature_flags { js_api_reply_v2: true }` on the hub while leaving the setup in place.

| Operation | While leaf uplink is down |
|---|---|
| Inbox messages and replies (`ants ask`, daemon replies, agent messages) | Buffered in `ANTS_OUT`; daemons record unacked requests in `<home>/answered` and ack redeliveries on reconnect (ignoring `"no responders"`) without re-prompting. |
| Unsequenced notes (`ants notes <thread> TEXT`), channel posts (`ants post`, daemon replies), passed links (`ants_links`) | Buffered in `ANTS_OUT` and sourced into `ANTS` or `ants_links` on reconnect. |
| Daemon dead letters (`ants.dead.<ant>`) | Buffered in `ANTS_OUT` and noted in `<home>/buried`; notice and `+TERM` run on redelivery (or within 30 s of reconnect on the 6th extra delivery) after checking `deadread-<ant>` against duplicates landing past the 24 h window. |
| Pull consumers, acks, stream reads (`ants inbox`, `ants channel read`, `ants notes`, `ants dead list`/`replay`) | Fail immediately; require the hub stream. |
| Sequenced `post_notes`, tool approvals (`ants.approvals.*`), channel admin (`ants.chanadm.*`), `ants cancel` of a prompt on another machine (`ants.cancel.*`) | Fail immediately; require live hub coordination or optimistic concurrency. |
| Enrollment (`ants.enroll.*`) | `ants add --enroll` keeps repeating its request until the uplink is back and an answer comes or the enrollment credentials expire. |
| KV writes (`ants_roster`, `ants_sessions`, `ants_attempts`, `ants_channels`, `ants_humans`, `ants_chains`) | Fail immediately so late puts cannot overwrite newer keys; daemons refresh `ants_roster` every 10 s on reconnect. |
| Owner records (`ants_owners`, from `ants ask`) | Buffered in `ANTS_OUT` ahead of the message and sourced into `ants_owners` on reconnect; a second record of a thread and ant is dropped as a repeat. `ANTS` and `ants_owners` source apart, so a daemon holds a human's message sourced from an outbox (`Nats-Stream-Source`) up to 10 s until its thread's owner is recorded for the ant. Webhooks reach the hub directly and record no owner in an outbox. |

### Republishing and live waiters

Sourced messages enter `ANTS` without a core NATS publish on `ants.inbox.*`. `ants init` configures `ANTS` to republish stored `ants.inbox.*.*` messages to `_INBOX.{{wildcard(1)}}.stored.{{wildcard(2)}}` with `Nats-Subject` intact, readable only by the recipient and its leaf uplink (daemons warn on older streams until upgraded). Live inbox waiters subscribe to both `ants.inbox.<user>.*` and `_INBOX.<user>.stored.*`, deduplicating the last 1,024 messages by `Nats-Msg-Id` (or payload).

Core NATS republishes do not cross a down leaf uplink. Waiter behavior across an uplink or client outage depends on the caller:

- **`ants ask --wait` and `--follow`**: pair live subscriptions with a 5 s poll of `ANTS` for `ants.inbox.<user>.*` (up to 100 per round) from the starting stream sequence, deduplicated by `Nats-Msg-Id`. Replies stored on the hub during an uplink outage arrive within 5 s of reconnect.
  - Humans poll with `STREAM.MSG.GET` (`next_by_subj`) and mark printed replies read in their read marker in `ants_read`, shared by all their devices. `ants inbox` reads the inbox the same way from the marker on, and `--follow` wakes on the republish to `_INBOX.<human>.stored.*` (and every 30 s), so every device of the human shows every new reply. While following without `--thread`, the device keeps its presence in `ants_presence` (memory, 30 s TTL, renewed every 10 s), so `ants webhook serve` pushes escalations only to the human's other devices.
  - Ants lack `STREAM.MSG.GET` (it takes any subject, exposing every inbox), and `inbox-<ant>` belongs to the daemon, so each round takes a free spare `inbox-<ant>_1..3` with a strict create (ack policy none, `inactive_threshold` 30 s), reads from the next sequence and deletes it. Ant credentials pin each spare's filter to `ants.inbox.<ant>.*` and allow only create, `MSG.NEXT` and delete, never acks, so the daemon still takes every message from `inbox-<ant>`. Ant credentials and leaf uplinks issued before the inbox spares need renewing (connecting with them warns and names the command); until then the poll fails quietly and only live replies arrive.
- **Daemon `ask_agent` and escalation waiters**: listen on live subscriptions while connected, and receive replies stored during an outage via the durable consumer `inbox-<ant>` once reconnected.
- **`ants post --wait` / `--follow`**: subscribes via core NATS to `ants.chan.<channel>.*` (`ANTS` does not republish `ants.chan.*`, so outbox-sourced posts never come live) and every 5 s also reads back the posts `ANTS` stored on that subject after the post, as `ants channel read` does: a human by `STREAM.MSG.GET` (`next_by_subj`), an ant through a free spare `chanread-<ant>_<n>` taken by a strict create and deleted after each round. The read-back starts after the post's stream sequence from its publish ack; for a post kept in `ANTS_OUT`, it starts 5 min before the post was sent (clock slack) and takes only what follows the post once the post shows in `ANTS`. Live and read-back posts are deduplicated by post id (last 1,024), so a reply stored during an uplink or client outage, or sourced from any machine's outbox, arrives once, within 5 s of reconnect or of being stored.

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
- **Headless hubs**: sign in with an Entra service principal (`devtunnel user login --sp-tenant-id ID --sp-client-id ID --sp-certificate-file FILE`) or managed identity (`--mi-client-id ID`) that owns or may host the tunnel, avoiding interactive renewal.
- **While the host is down**: worker `devtunnel connect` and leaf uplinks fail and retry; leaves queue outbound traffic in `ANTS_OUT` (see "Store and forward").

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
| Daemon startup retry | `1s` doubling to `30s` | `ants daemon` and `ants channel serve` retry unreachable NATS TCP/DNS and unanswered JetStream requests (the info of the user's own inbox consumer) until the leaf uplink joins; bad URLs, TLS errors, bad `.creds`, and permission violations fail immediately (as do other CLI commands). |
| Progress ack and `ack_wait` | `20s` progress / `60s` `ack_wait` | Daemons renew in-hand messages every 20 s; silent links or crashes hold at most one message per pull slot for up to 60 s before redelivery (in-hand duplicates are ignored). |
| Roster heartbeat / TTL | `10s` refresh / `30s` TTL (`20s` stale) | Disconnected daemons expire within 30 s and reappear within 10 s of reconnect. |
| Retention and deduplication | `30d` streams/KV; `24h` dup window on `ANTS` (`ants_owners` 30 d, others `2m`) | `ANTS`, `ANTS_OUT`, `ANTS_IN`, `ants_sessions`, `ants_attempts`, `ants_links` and `ants_owners` retain 30 d (`ants_webhooks` 7 d). Outbox messages land after what reached the hub meanwhile: usually within a second of reconnect, at worst ~2.5 min later (30 s source request timeout plus up to 2 min retry backoff). Readers keep stream order rather than sorting by `sent_at`, as machine clocks differ. A timed-out publish that reached the hub after all stores once if its outbox copy lands within 24 h. |
| `max_control_line` | `16384` (16 KiB) | Set in `server.conf` on hub and leaves so user JWT permissions fit in a client's `CONNECT`. A leaf's `CONNECT` may take 16 times as much (nats-server 2.14 and later; no limit before), so `ants auth leaf` and enrollment approval refuse uplink JWTs over 252 KiB. |
