# Transport: reaching the hub

Findings of the spikes ants-qjf.9 (hub over `wss://` through a reverse proxy and a Dev Tunnels HTTPS port) and
ants-qjf.8 (hub over a Dev Tunnel TCP port), and what they mean for ants-32g.2 (leaf node per machine) and ants-32g.3
(HTTPS fallback bridge, built: see "HTTPS fallback bridge").

Each finding is marked **verified** (run with nats-server 2.15.3, async-nats 0.52, Caddy 2.11.9 on macOS arm64, every
hop on loopback; the Dev Tunnels runs used two Linux arm64 Apple `container` VMs, hub and worker, through the real
service) or **researched** (documentation or source reading only).

## Summary

| Path | Status |
|---|---|
| `ants` → `wss://` reverse proxy (Caddy, path `/nats`) → nats-server websocket, with and without JWT credentials | verified |
| Self-signed or private CA for `wss://` / `tls://` via `SSL_CERT_FILE` | verified |
| Leaf node → `wss://` reverse proxy → hub websocket (path `/nats/leafnode`) | verified |
| Leaf node → HTTP `CONNECT` forward proxy → `wss://` reverse proxy → hub | verified |
| Leaf node → TCP port forward (stand-in for `devtunnel connect`) → hub leafnode port | verified |
| `ants` itself behind an HTTP forward proxy | not possible: async-nats has no proxy support; a leaf node has |
| JetStream API (`$JS.API.>`) from a client on a leaf node | needs a JetStream domain: `--js-domain` (ants-v6w), verified |
| Leaf uplink credentials bounding what a machine's personas may do | `ants auth leaf` (ants-hmf), verified |
| Leaf configuration from `ants leaf config`, uplink switched by rewriting it and `SIGHUP` | verified (ants-32g.2) |
| Store and forward: a leaf's outbox, sourced by the hub once the uplink is back | `ants leaf outbox` (ants-0ul), verified |
| Reconnect after proxy restart, forwarder drop and silent link loss; durable consumer resumes | verified |
| Dev Tunnels: `devtunnel connect` to the hub's client port (4222) for `ants` and a daemon, and to its leafnode port (7422) for a leaf | verified |
| Dev Tunnels: drop of either end, and a 3-minute freeze of the worker (stand-in for laptop sleep) | verified: nothing lost; see "Dev Tunnels" for the delays |
| Dev Tunnels HTTPS port as `wss://` with anonymous access | researched |
| Private Dev Tunnels HTTPS port from a `wss://` NATS client or leaf node | not possible: no way to send `X-Tunnel-Authorization` |
| Leaf node → `ants bridge connect` → HTTP `CONNECT` proxy → TLS terminator → `ants bridge serve` → hub leafnode port | verified (ants-32g.3) |
| Bridge session through a 8 s outage of the proxy in front of `ants bridge serve` | verified: the leaf connection stays up |

Recommendation: every machine runs a local leaf node and `ants` connects only to it (ants-32g.2). The uplink is either
a `wss://` endpoint (a reverse proxy, or an anonymous Dev Tunnels HTTPS port with NATS JWTs as the real access
control) or `devtunnel connect` to the hub's leafnode port for a private tunnel. Where a proxy blocks WebSocket, the
HTTPS fallback bridge (`ants bridge`, ants-32g.3) carries the leafnode connection in plain HTTPS requests instead.

## Hub

One nats-server with JetStream, a client port for processes on the hub machine, a websocket listener without TLS for
the TLS-terminating proxy or tunnel in front of it, and a leafnode port for tunnelled TCP leaf connections:

```hcl
server_name: hub
listen: 127.0.0.1:4222
ping_interval: "30s"            # notice dead clients and leaf nodes sooner than the 2m default
jetstream {
  store_dir: "/var/lib/ants/js"
  domain: hub                   # lets clients on leaf nodes reach JetStream, see "JetStream across a leaf node"
}
websocket {
  listen: 127.0.0.1:8080
  no_tls: true                  # TLS ends at the proxy or tunnel
}
leafnodes {
  listen: 127.0.0.1:7422
  no_advertise: true            # leaf nodes keep using the tunnel or proxy address
}
feature_flags {
  js_api_reply_v2: true         # needed to source the leaves' outboxes, see "Store and forward on a leaf node"
}
include "server.conf"           # from `ants auth init`, copied next to this file
```

- nats-server resolves `include` relative to the including file, even when the path starts with `/` (verified: an
  absolute path fails with "no such file or directory").
- The websocket listener accepts any URL path. A path ending in `/leafnode` makes the connection a leaf node, one
  ending in `/mqtt` an MQTT client, anything else a client (source, `wsUpgrade`). A proxy can therefore mount it under
  `/nats`, and leaf nodes then request `/nats/leafnode` (verified).
- In operator mode websocket clients authenticate with their `.creds` exactly as TCP clients do (verified).
- Without `no_advertise` the hub tells leaf nodes its own leafnode address. After the forwarder dropped, the leaf
  reconnected straight to `127.0.0.1:7422` of the hub, bypassing the forwarder (verified; across machines that
  address is wrong or, worse, the worker's own forward). The same applies to clustered hubs and the websocket
  `advertise` / `no_advertise` options for clients (researched; the spike ran one server).

## Reverse proxy (Caddy)

```caddyfile
hub.example.com {
  # tls /etc/ants/hub.pem /etc/ants/hub.key   # a private CA; omit for an ACME certificate
  handle /nats* {
    reverse_proxy 127.0.0.1:8080
  }
  handle /bridge* {                            # only with the HTTPS fallback bridge
    reverse_proxy 127.0.0.1:8423
  }
  respond 404
}
```

Caddy passes the WebSocket upgrade through without extra configuration and sets no idle timeout on upgraded
connections. Proxies with a read or idle timeout (nginx `proxy_read_timeout` defaults to 60 s, as do many load
balancers) see traffic every 20 s from `ants` (see "Keepalive and reconnect"); for leaf nodes keep the hub
`ping_interval` below the proxy's timeout.

## Clients

- URL: `wss://hub.example.com/nats`. async-nats sends the path and query string unchanged in the upgrade request
  (verified: `GET /nats?tunnel-token=abc`) but cannot add request headers.
- TLS trust: `wss://` and `tls://` use the platform store through rustls-native-certs. For a self-signed or private
  CA, `SSL_CERT_FILE=/path/ca.pem` replaces the platform store for that process (verified: `UnknownIssuer` without
  it, connected with it). No `ants` flag is needed. With a leaf node, `ants` uses `nats://127.0.0.1:4222` and the CA
  goes in the leaf remote's `tls { ca_file }`.
- No HTTP proxy support in async-nats, so `ants` ignores `HTTPS_PROXY`. A leaf node supports one (below).

Verified through Caddy: `ants init`, `ants watch` and `ants ask` without credentials on an open hub, and with
`--creds human.creds` on a hub in operator mode (without credentials: `authorization violation`).

## Leaf node per machine

`ants leaf config <machine> --uplink <url>` writes this file (ants-32g.2; options in the README, "Leaf nodes"). By
hand it looks like:

```hcl
server_name: macbook
listen: 127.0.0.1:4222                       # every ants process on the machine connects here
include "server.conf"                        # the hub's operator and account JWTs (no seeds in it)
jetstream {                                  # only for the outbox, see "Store and forward on a leaf node"
  domain: macbook                            # the machine's name; never the hub's domain
  store_dir: "/Users/me/.config/ants/jetstream"
  max_memory_store: 64MB
  max_file_store: 1GB
}
leafnodes {
  remotes: [{
    urls: ["wss://hub.example.com/nats"]     # or ["nats-leaf://127.0.0.1:7422"] through `devtunnel connect`
    account: <ants account public key>       # the account in server.conf
    credentials: "leaf-macbook.creds"        # from `ants auth leaf macbook --persona ... --js-domain hub`
    # tls { ca_file: "ca.pem" }              # private CA only
    # proxy { url: "http://proxy.corp:3128" }  # HTTP CONNECT forward proxy; optional username and password
  }]
}
```

Verified: the leaf connects through Caddy over `wss://`, through an HTTP `CONNECT` forward proxy (a minimal Python
proxy) with `proxy { url }`, and through a plain TCP forward to the hub's leafnode port. `ants ask` on a leaf reached
`ants watch` on the hub and the reverse, and a durable consumer on a leaf received messages published on the hub. The
leaf retries its remote every one to two seconds while the uplink is down, and reconnected each time it came back.

Constraints found:

- **Leaf credentials.** The remote needs a user in the team account. Its permissions bound everything the machine can
  do on the hub; the per-persona permissions are enforced by the leaf, which authenticates local clients against the
  same `server.conf`. `ants auth leaf` issues it (ants-hmf, see "Leaf uplink credentials" below).
- **JetStream across a leaf node.** A leaf without JetStream denies the non-domain JetStream API `$JS.API.>` on its
  leaf connection (source: `leafnode.go`; verified: `ants init` on the leaf fails with "jetstream unavailable").
  Publishing into the stream still works, since its acks are plain replies. Two ways out, both verified:
  - Give the hub `jetstream { domain: hub }` and make clients use it, so they publish to `$JS.hub.API.>`: a consumer
    on the leaf using `jetstream::with_domain(client, "hub")` created its consumer and received messages. Persona
    permissions must then allow `$JS.hub.API.…` as well as `$JS.API.…`; ack subjects stay `$JS.ACK.ANTS.…`. Mapping
    the domain on the leaf with `default_js_domain: { <account>: hub }` does not work for a JetStream-enabled account.
    This is what ants does (ants-v6w, see "JetStream domain" below).
  - Leave the hub without a domain and set `default_js_domain: { <account>: "" }` on the leaf. `ants init` then works
    unchanged, but nats-server warns that the option is a temporary compatibility measure and will be removed.
- **No store and forward in nats-server.** While the uplink is down, publishing into the hub's stream from the leaf
  fails at once ("no stream found for given subject"); nats-server queues nothing. ants keeps such messages in an
  outbox on the leaf instead, see "Store and forward on a leaf node".
- **No custom headers or query on leaf upgrades.** A leaf builds its upgrade URL from host and path only and sends no
  extra headers (source: `leafnode.go`), so a private Dev Tunnels HTTPS port is out of reach for leaf nodes too.
- **Remote URLs without a port get 7422, websocket ones too** (source: `opts.go`, `setBaselineOptions`), so
  `wss://hub.example.com/nats` would dial port 7422. `ants leaf config` spells out 443 / 80 / 7422.
- **Websocket and TCP URLs do not mix** in one remote (config error). A `wss://` remote behind `proxy { url }` needs a
  `tls` block, even an empty one (config error otherwise); `ants leaf config` always writes one for `tls://` and
  `wss://`, so the uplink never falls back to plain text.
- **Switching the uplink on a running leaf.** nats-server 2.15 reloads added and removed remotes on `SIGHUP`. Verified
  (`a_generated_leaf_reaches_the_hub_and_switches_its_uplink_on_reload` in `crates/ants/src/leaf.rs`): a leaf generated
  with a `nats-leaf://` uplink, switched to a dead port and then to `ws://`, each by rewriting the file and `SIGHUP`;
  a client connected to the leaf throughout lost the hub's JetStream after the first switch and had it back after
  the second.

## JetStream domain

The hub's `jetstream { domain: hub }` is the one setting. Every ants process takes it as `--js-domain hub`,
`ANTS_JS_DOMAIN=hub` or `js_domain = "hub"` in the daemon config file (top level or per persona), and then sends all
JetStream API requests, KV puts and deletes included, to `$JS.hub.API.…` (`bus::jetstream`). Without it nothing
changes: `$JS.API.…` as before, which works only on the hub itself.

Credentials are issued for the domain: `ants auth user dev-a --js-domain hub` (the global flag, or `ANTS_JS_DOMAIN`).
Verified with nats-server 2.15.3:

- The hub maps `$JS.hub.API.>` to `$JS.API.>` (and `$JS.hub.API.$KV.>` to `$KV.>`), and checks a client's publish
  permissions on the subject it sent **and** on the mapped one: a human with only `$JS.hub.API.STREAM.INFO.ANTS` got
  `Permissions Violation for Publish to "$JS.API.STREAM.INFO.ANTS"`. So credentials for a domain allow both forms
  (`Permissions::with_js_domain`); those without one allow only the plain form. Both forms reach the same stream, so
  this grants nothing more; a client on the hub may use either, a client on a leaf only the domain form.
- A leaf node's own JetStream, which only holds the outbox, has the machine's name as its domain. It answers the
  plain `$JS.API` on the leaf, so clients there must use the hub's domain. A leaf denies its own domain's API on its
  uplink, and the hub reaches it as `$JS.<machine>.API`.

## Leaf uplink credentials

```sh
ants auth leaf macbook --persona dev-a --persona dev-b --persona human --human alice --js-domain hub
# -> ~/.config/ants/auth/leaf-macbook.creds, for leafnodes.remotes[].credentials on macbook
```

The leaf user may do what the listed personas and humans may do together (`Permissions::for_leaf`, the union of
`Permissions::for_persona` in each one's role), with the domain forms of `--js-domain`; list each human that runs the
CLI on the machine, `human` with `--persona` and any other with `--human NAME`. Its
JWT sets `allowed_connection_types: [LEAFNODE, LEAFNODE_WS]`, so it works only as a leaf uplink (over TCP or
`wss://`), never as a client that could act as all those personas at once. Issue it again when the machine's
personas change. Beyond the personas' subjects it carries only the hub's sourcing of the machine's outbox (see "Store
and forward on a leaf node"): the outbox's own `ants.out.` subjects never cross the uplink.

How nats-server applies a leaf user's permissions (source: `leafnode.go`, `processLeafNodeConnect`, and verified):
they read from the leaf's side, as for a client. `pub` bounds what the leaf may send to the hub, `sub` what the hub
sends to the leaf; the hub swaps them internally because, from its side, messages flow the other way. The hub also
tells the leaf its permissions, and the leaf filters what it forwards itself. That matters: the hub closes a leaf
connection that sends a subscription or message outside them, but a local client beyond the bound never makes the
leaf send one.

Verified (`clients_on_a_leaf_node_reach_the_hub_stream_through_its_domain` in `crates/ants/src/auth.rs`): a hub with
`jetstream { domain: hub }` and `leafnodes { no_advertise: true }`, a leaf without JetStream including the same
`server.conf`, uplink credentials for dev-a only.

- dev-a on the leaf, with the domain, creates its inbox consumer, gets the human's message, keeps its session in KV
  and replies; without the domain, `ensure_stream` on the leaf fails.
- dev-b, whose credentials the leaf accepts, gets no JetStream across, its status does not reach the hub, and the
  hub's message to `ants.inbox.dev-b` does not reach it; the uplink stays up and dev-a's traffic keeps flowing.
- The leaf credentials are refused as a client connection to the hub.

Configuration for ants-32g.2 (per-machine leaf templates), adding to "Hub" and "Leaf node per machine" above:

```hcl
# hub: as in "Hub"; the parts this relies on
jetstream { store_dir: "/var/lib/ants/js", domain: hub }
leafnodes { listen: 127.0.0.1:7422, no_advertise: true }
include "server.conf"

# leaf on macbook: no jetstream block (since ants-0ul, one for the outbox only, see "Leaf node per machine")
listen: 127.0.0.1:4222
include "server.conf"
leafnodes {
  remotes: [{
    urls: ["wss://hub.example.com/nats"]
    account: <ants account public key>
    credentials: "leaf-macbook.creds"
  }]
}
```

```sh
export ANTS_NATS_URL=nats://127.0.0.1:4222 ANTS_JS_DOMAIN=hub   # on every machine, hub included
```

## Store and forward on a leaf node

ants-0ul. nats-server has no store and forward for a leaf node: while the uplink is down, a publish into the hub's
stream fails at once. Each machine's leaf node therefore runs JetStream of its own, in the machine's domain, with one
stream, the machine's outbox `ANTS_OUT` (subjects `ants.out.>`, file storage, 30 days). The hub takes from it in two
hops, both JetStream sources, which the hub pulls and resumes on its own once the uplink is back (also after either
server restarts):

1. The hub's intake stream `ANTS_IN` sources every outbox through the machine's domain
   (`external.api: $JS.<machine>.API`, `deliver: $JS.S.<machine>`) and stores each message under
   `ants.in.<machine>.`, so nothing a machine sends can pass for another machine or land in `ANTS` directly.
2. `ANTS` sources from `ANTS_IN`, for each machine and persona, only what the persona may publish itself
   (`ants.in.<machine>.ants.out.inbox.*.<persona>`, `...plan.*.<persona>`, `...chan.*.<persona>` and, for a persona
   that is no human, `...dead.<persona>`; the human's `inbox.>`), and transforms
   it back to the usual subject. The rest stays in the intake, where only the human can read it.

Source filters of one stream must not overlap, so when the human is on a machine its `inbox.>` covers the other
personas' inbox subjects there, and they get only their plan and channel subjects. Every message keeps its `Nats-Msg-Id` (the
envelope or post id): a sourced copy of a message `ANTS` already took within its duplicate window (2 minutes) is
dropped.

Clients (`bus::store`): an inbox message, reply, plan post, channel post or a daemon's dead letter goes to the hub's
stream as before; when that fails with "no stream" (no responders across a down uplink) or times out, the same message,
with the same id, goes to `ants.out.<rest of the subject>` on the leaf. The CLI says so (`ants ask`: "kept in this
machine's outbox"); a daemon logs a warning. Everything else needs the hub at once and is not buffered (ants-xyb):

- Consumers and their acks, and stream reads (`ants watch --replay`, `ants dead list`/`replay`): they read the hub's
  stream.
- Plan posts that expect a sequence (a persona's `post_plan`): the expected last subject sequence is optimistic
  concurrency, and only the hub's stream, at the time of the post, can tell the poster whether it won the turn. A
  buffered copy would land minutes later against a newer room, where the hub could only drop a post the poster was
  told was kept. Plan posts without an expected sequence are buffered.
- Approvals: a request waits for a human's decision, read from the hub, until it expires. A buffered request would
  reach the human after the asker had given up on it.
- KV (sessions, roster, attempt markers, escalation chains): a bucket keeps the last value per key, so a buffered put
  landing late could overwrite a newer one. A daemon puts its roster heartbeat every 10 seconds, so the roster is
  current again soon after the uplink is back, and a daemon gets no messages to work on meanwhile.
- Dead letters a human writes (`ants dead watch`, `ants inbox --follow`): they come from the hub's max-deliveries
  advisories and stream, so the hub is up whenever they are written.

A dead letter in the outbox leaves its message unfinished: the notice on the thread needs the escalation state in
`ants_sessions` and the termination needs the hub. The daemon notes the dead letter in `<home>/buried` of the persona
(`<envelope id>.json`, by envelope id like `answered`); the message's next delivery after the uplink is back, even to a
restarted daemon, buries the noted letter again without another prompt. The hub's stream drops the copy by its
`Nats-Msg-Id` (`dead-<stream sequence>`) and the daemon sends the notice and terminates the message, once. A daemon
buries a message on its last attempt, and the consumer allows one more delivery after that one, which then carries the
noted letter to the hub (see Limits for a letter kept on that extra delivery).

Set up, as a human, with the leaf up and connected:

```sh
ants leaf outbox macbook --persona dev-a --persona dev-b --persona human --human alice   # again when they change
ants leaf outbox macbook --remove                                                       # stop taking from it
```

It creates or updates the outbox through the hub (`$JS.macbook.API`), creates `ANTS_IN` with a source for the machine
if missing, and replaces the machine's sources of `ANTS`, keeping those that did not change. New ones start after
what `ANTS_IN` holds at the time, so earlier messages of a newly admitted persona stay out.

Found while building it (nats-server 2.15.3, source reading and verified):

- **Replies to the hub must be publishable by the leaf.** The hub tells a leaf about a subscription only if the leaf's
  uplink credentials allow publishing on it (`canSubscribeInternal`, swapped permissions). The hub's requests to a
  leaf get their replies only on subjects the leaf may publish:
  - Setting up the outbox, a human connects with the inbox prefix `_INBOX.<human>.leaf.<machine>`, which that leaf
    may publish for `human` and each human its uplink credentials list. A leaf cannot answer a human's other requests,
    which use `_INBOX.<human>`.
  - The hub's own requests for a source use `$JSC.R.<random>` by default, which the leaf cannot be allowed without
    allowing every other source's replies. `feature_flags { js_api_reply_v2: true }` on the hub makes them
    `$JSC.R.<domain>.<account hash>.<stream>.…`, so the leaf may publish `$JSC.R.<machine>.*.ANTS_OUT.>` only. Without
    the flag the hub never gets an answer from the leaf and the source stalls.
  - The source's deliveries and flow control use `$JS.S.<machine>.>` (leaf to hub) and `$JS.FC.ANTS_OUT.>` (hub to
    leaf); the uplink credentials allow those and the outbox's stream and consumer API under `$JS.<machine>.API`.
- **Stream interest crosses only allowances `ReverseMatch` finds.** The hub's stream listens on `ants.inbox.>`,
  `ants.plan.>` and `ants.chan.>`. nats-server tells a leaf about such a wildcard subscription only if `ReverseMatch` finds a publish
  allowance of the uplink under it (`canSubscribeInternal`), and `ReverseMatch` (`sublist.go`, `getAllNodes`) skips an
  allowance with `*` before its last token, such as dev-a's `ants.inbox.*.dev-a`. Only the human's `ants.inbox.>`
  would match, and no persona's plan or channel subjects. So the uplink credentials also carry one literal subject of each
  persona's inbox, plan and channel pattern (`ants.inbox.dev-a.dev-a`, `ants.plan.dev-a.dev-a`,
  `ants.chan.dev-a.dev-a`), which the pattern allows
  anyway (uplink credentials issued without them need `ants auth leaf` again). With that, a persona's messages,
  replies, plan and channel posts reach the hub's stream directly while the uplink is up, whoever else is on the machine; a
  persona not on the uplink still gets "no stream" at once, since the leaf forwards only what the uplink may publish
  (`a_generated_leaf_reaches_the_hub_and_switches_its_uplink_on_reload` in `crates/ants/src/leaf.rs`).
- **The leaf's domain must differ from the hub's**, or the leaf would answer the hub's API itself.

Verified (`the_hub_takes_what_a_leaf_kept_while_its_uplink_was_down` in `crates/ants/src/outbox.rs`): a hub and a
generated leaf with dev-a and the human on it. A message stored directly plus a copy of it in the outbox is in `ANTS`
once. With the uplink switched to a dead port, dev-a's message, plan post, channel post and dead letter and dev-b's
plan post and dead letter go to the outbox; after switching back, the intake has all of them, `ANTS` has dev-a's four
and none of dev-b's, which the machine was not admitted for. A daemon's dead letter of a message failing while the
uplink is down lands once, with its notice, after the uplink is back
(`a_dead_letter_kept_in_the_outbox_is_buried_once_the_uplink_is_back` in `crates/ants-daemon/src/daemon.rs`).

Limits:

- Order across an outage: buffered messages land after whatever reached the hub meanwhile, up to about two minutes
  after the uplink is back (the source's retry backoff).
- A publish that timed out but reached the hub after all, and whose buffered copy lands after the duplicate window,
  is stored twice.
- Live subscribers (`ants ask --wait`, a persona's `ask_agent`) see only messages published directly, not sourced
  ones; the inbox consumers see both.
- A daemon that replies while the uplink is down cannot ack the request. The reply goes to the outbox, under a
  message id made of the persona and the request's envelope id, and the daemon notes the request in `<home>/answered`
  of the persona; the redelivery after the uplink is back, even to a restarted daemon, is acked without being answered
  again (ants-fv1). async-nats's `double_ack` takes the "no responders" status the client gets then for a
  confirmation, so the daemon acks with a request of its own (`bus::double_ack`). Verified by
  `a_reply_kept_in_the_outbox_answers_its_message_once` in `crates/ants-daemon/src/daemon.rs`.
- Messages a persona put in the outbox before `ants leaf outbox` admitted it stay in the intake. A machine set up
  before dead letters were buffered needs `ants leaf outbox` again for `ANTS` to take its dead letters, and persona
  credentials issued before need `ants auth user` again to publish on `ants.out.dead.<persona>`.
- A dead letter stored in the outbox on the message's extra delivery (the last attempt never finished) has no
  delivery left to send its notice and terminate the message. JetStream gives up on the message and publishes a
  max-deliveries advisory: a running `ants dead watch` dead-letters it from the hub, with the same `Nats-Msg-Id`, and
  tells the human, unless the outbox copy has landed first; then no notice goes out. An outbox copy landing more than
  two minutes after the watcher's letter is stored twice.
- The outbox copy of a daemon's dead letter and the one the redelivery stores usually land within two minutes of each
  other (the source's retry backoff, the consumer's 60 second ack wait); further apart, the hub's stream keeps both.

## HTTPS fallback bridge

ants-32g.3. For networks whose proxies drop the WebSocket upgrade (a TLS-inspecting proxy, say), `ants bridge` carries
the leaf node's TCP connection to the hub's leafnode port in ordinary HTTP/1.1 requests. Neither async-nats nor a
nats-server leaf remote speaks anything but NATS over TCP or WebSocket, so the bridge is a byte pipe with a process at
each end and NATS itself unchanged inside it:

```
leaf node --nats-leaf://127.0.0.1:7422--> ants bridge connect ==HTTPS==> reverse proxy --> ants bridge serve --> hub :7422
```

```sh
# hub, next to nats-server; the reverse proxy routes /bridge* to it (see "Reverse proxy")
ANTS_BRIDGE_TOKEN=... ants bridge serve                # listens on 127.0.0.1:8423, connects to 127.0.0.1:7422

# worker
ANTS_BRIDGE_TOKEN=... ants bridge connect https://hub.example.com/bridge   # listens on 127.0.0.1:7422
ants leaf config macbook --uplink nats-leaf://127.0.0.1:7422
```

Protocol, under the URL's path: `POST /open` opens a session (a TCP connection to the upstream) and answers its random
128-bit id; `POST /s/<id>?off=N` carries upstream bytes starting at byte offset N; `GET /s/<id>?off=N` acknowledges
everything before N and answers the downstream bytes from N, waiting up to 20 s for some (an empty answer otherwise, so
proxies with 60 s idle timeouts never see a quiet request), or 410 once the upstream closed and all was taken;
`DELETE /s/<id>` closes it. One request in flight per direction, each body at most 128 KiB; the hub holds at most
1 MiB of downstream bytes per session and stops reading the upstream beyond that.

- **Reliability.** Every request names its offset, so a request or answer lost on the way is sent again: the hub skips
  upstream bytes it already wrote and sends downstream bytes again until they are acknowledged. The worker retries
  connection failures and 5xx answers with back-off up to 4 s for 60 s; the hub closes a session after 60 s without
  requests. After that the worker closes the local connection, and the leaf node reconnects through a new session.
- **Authentication.** NATS authentication runs inside the stream as on any uplink: the leaf's `ants auth leaf`
  credentials, with `LEAFNODE` as the connection type. `--token` (or `ANTS_BRIDGE_TOKEN`) on both ends adds a bearer
  token every request must carry, so strangers cannot open sessions (the hub compares it in constant time); session ids
  are unguessable. `ants bridge serve` connects only to its one `--upstream`, never where a request asks.
- **TLS** ends at the reverse proxy; `ants bridge serve` speaks plain HTTP on loopback. The worker uses rustls with the
  platform's CAs (`SSL_CERT_FILE` replaces them, as for `wss://`), TLS 1.2 or 1.3.
- **Forward proxy.** `--proxy http://[user:password@]host:port` tunnels through an HTTP `CONNECT` proxy, with basic
  proxy authentication.
- **Gateways.** `--header 'Name: value'` (repeatable) goes with every request, for a gateway in front of the hub, such
  as `X-Tunnel-Authorization: tunnel <token>` for a private Dev Tunnels HTTPS port.
- **Cost.** One round trip per chunk in each direction, so throughput is at most 128 KiB per round trip and each
  message pays a round trip's latency; ample for agent traffic, not for bulk transfers. Use it only where WebSocket
  fails.

Verified: `a_leaf_node_reaches_its_hub_through_the_bridge` (`crates/ants/src/bridge.rs`) runs a hub, a leaf node and
both ends of the bridge over plain HTTP on loopback and sends a message each way, one of them three requests long;
`repeated_requests_neither_lose_nor_double_bytes` repeats requests as retries would. By hand, with nats-server 2.15.3
on loopback: a leaf through `ants bridge connect`, a Python HTTP `CONNECT` proxy with basic authentication, a Python
TLS terminator (self-signed certificate, `SSL_CERT_FILE`) and `ants bridge serve` reached the hub and a message
crossed; stopping the proxy and terminator for 8 s left the leaf connection up; restarting `ants bridge serve` dropped
the session, and the leaf reconnected through a new one once the worker's pending poll had timed out (50 s; a
reverse proxy that answers 502 when its backend drops ends it sooner). A real WebSocket-blocking proxy was not
available.

## Keepalive and reconnect

async-nats reconnects forever by default, with a back-off capped at 4 s, and `ants` relies on that. Measured:

| Event | Result |
|---|---|
| Caddy stopped for 15 s, then restarted | `ants watch` reconnected on its next attempt; core subscribers miss what was sent meanwhile |
| TCP forwarder (stand-in for `devtunnel connect`) killed, restarted 15 s later | disconnect noticed at once; reconnected within 4 s of the restart |
| Durable consumer while the proxy was down | resumed after reconnect with every message published meanwhile; one `missed idle heartbeat` stream error |
| Proxy frozen (`SIGSTOP`: a silent link, as after laptop sleep or a dead tunnel), default 60 s ping | disconnect noticed after 177 s |
| Same with a 20 s ping (now set in `bus::connect`) | disconnect noticed after 56 s; reconnected once the proxy resumed |
| Forwarder closing connections idle for 45 s, 20 s ping | no drops in 2 minutes |

One message published during the silent period went to the consumer's pull request still registered for the dead
connection, so the server counted it as delivered and redelivered it only after `ack_wait` (60 s in the test, and
the daemon's since ants-vh4; it was 5 minutes). Nothing is lost, but a silent link can hold one message per thread for
up to the daemon's ack wait. The daemon sends progress acks every 20 s for each message in hand, so only a message
whose daemon crashed or never received it waits out the ack wait; one whose progress acks were lost on the silent
link comes back while still in hand, and the daemon ignores that copy.

`ants daemon` waits for a persona's server that is unreachable at start, as the local leaf node when launchd starts
both services at once (ants-lz9): a refused, timed-out or unresolved connection is retried after 1 s, then twice as
long each time up to 30 s, with a warning naming the URL (user and password masked), until it connects or the daemon
is stopped. Rejected credentials, an unreadable credentials file, a malformed URL and TLS errors fail the persona at
once. The loop is explicit rather than async-nats `retry_on_initial_connect`, which returns a client before it has
connected, so the JetStream calls that follow would wait out their timeouts, and which keeps retrying rejected
credentials as well. The other `ants` commands still fail at once.

## Dev Tunnels

`devtunnel` 1.0.2115 (official build, from `https://tunnelsassetsprod.blob.core.windows.net/cli/<os>-<arch>-devtunnel`
as in `https://aka.ms/DevTunnelCliInstall`) refuses to host without a login ("Anonymous does not have 'create' access
scope"). Neither the CLI help nor the documentation lists a telemetry opt-out. In the verified runs the CLI looked up
only `global.rel.tunnels.api.visualstudio.com`, `<region>.rel.tunnels.api.visualstudio.com`,
`<region>-data.rel.tunnels.api.visualstudio.com`, `tunnelsassetsprod.blob.core.windows.net` (update check) and, for
the login, `github.com` / `api.github.com`; the known Microsoft telemetry hosts were pinned to `0.0.0.0` in
`/etc/hosts` beforehand, so attempts to reach those would not show up as lookups.

Hub machine, once logged in (`devtunnel user login -g -d`: GitHub account, device code):

```sh
devtunnel create ants-hub                                # persistent tunnel, private to the creating account
devtunnel port create ants-hub -p 4222                   # client port, raw TCP, for workers without a leaf node
devtunnel port create ants-hub -p 7422                   # leafnode port, raw TCP (protocol auto)
devtunnel port create ants-hub -p 8080 --protocol http   # websocket listener; the public URL is https/wss
devtunnel host ants-hub                                  # keep running under launchd or systemd
```

Worker machine, private tunnel:

```sh
devtunnel connect ants-hub.<region> -t "$(cat connect.token)"   # from `devtunnel token ants-hub --scopes connect`
```

The worker needs no login of its own: a connect token issued on the hub sufficed (verified). `devtunnel connect`
forwards each tunnel port to the same port number on `127.0.0.1` and `::1` only, so a container or VM running it has
to run its NATS clients too. A worker without a leaf node points `ants` at `nats://127.0.0.1:4222`; one with a leaf
node gives it the remote `nats-leaf://127.0.0.1:7422`, and then must not have the hub's 4222 forwarded where its own
leaf listens (leave port 4222 off the tunnel, or run the leaf's client port elsewhere).

### `devtunnel connect` as a service

A leaf whose uplink is `nats-leaf://127.0.0.1:7422` needs `devtunnel connect` running next to it.
`scripts/install-units.sh --devtunnel ants-hub.<region>` installs it as a service beside nats-server and the daemon
(`contrib/launchd/ants.devtunnel.plist`, `contrib/systemd/ants-devtunnel.service`), with `--devtunnel-bin PATH` if
`devtunnel` is not on `PATH`. The service restarts `devtunnel connect` whenever it exits, every 30 s at most; the CLI
exits with status 3 on a missing login or a rejected token. The systemd nats-server unit starts after it; launchd has
no start order, and the leaf retries its uplink until the tunnel is up either way. Logs:
`~/Library/Logs/ants/devtunnel.log`, `journalctl --user -u ants-devtunnel`.

The connection is authorized in one of two ways, each needing renewal:

- CLI login (the default). Log in once on the worker, as the user the service runs as, with the account that created
  the tunnel or one it grants access (`devtunnel access create ants-hub --org ORG` for a GitHub organization,
  `--tenant` for the Entra tenant): `devtunnel user login -g -d`. The CLI caches the login in the system key chain for
  several days. `devtunnel user show` reports the login, or `Not logged in.` once it has expired; the service log then
  shows `Login required.` on every retry. Logging in again the same way is all it takes: the next retry, at most
  30 s later, connects without a restart. A GitHub login needs the device code entered in a browser each time, so a
  headless worker needs someone to do that every few days. An Entra service principal
  (`devtunnel user login --sp-tenant-id T --sp-client-id C --sp-certificate-file F`) or managed identity
  (`--mi-client-id`) logs in without a person and can run from a daily timer; not tried here.
- Access token (`--devtunnel-token FILE`). The service runs `devtunnel connect TUNNEL --access-token -` with FILE on
  standard input, read afresh at each start. On a machine logged in to the tunnel's account, such as the hub, issue
  one with `devtunnel token ants-hub --scopes connect` and write the token alone into FILE (mode 600) on the worker.
  It expires after 24 hours and only a logged-in user can issue the next one, so something has to carry a fresh token
  to the worker at least daily: replace FILE before the old token expires, then restart the service
  (`launchctl kickstart -k gui/$(id -u)/ants.devtunnel`, `systemctl --user restart ants-devtunnel`), since a running
  `devtunnel connect` does not reread it. Whether a running connection outlives its token is untested; the restart
  costs a few seconds of uplink, which the outbox covers. Suited to a trial; an always-on worker is better served by
  a login.

Measured, hub default `ping_interval` (2 minutes), daemon on the worker through tunnelled 4222, leaf node through
tunnelled 7422:

| Event | Result |
|---|---|
| `ants ask` on the hub → daemon on the worker → reply in `ants inbox` on the hub | works; the daemon got the prompt about 0.3 s after the ask |
| `ants watch` on the worker's leaf node | sees the hub's traffic |
| `devtunnel connect` killed for 30 s, a message sent meanwhile, then restarted | the daemon reconnected at once, but the message came only 5 minutes later (`ack_wait`, since cut to 60 s) |
| `devtunnel host` killed for 20 s, a message sent meanwhile, then restarted | `devtunnel connect` stayed up and rejoined by itself; the daemon had the message 0.2 s after reconnecting; the leaf reconnected within 1 s |
| Every worker process frozen (`SIGSTOP`) for 3 minutes, a message sent meanwhile | the relay logged the client SSH session as disconnected, yet on resume the session carried on: the daemon handled the message within 1 s, and the leaf never dropped |

Killing the worker's `devtunnel connect` does not close the hub-side socket that `devtunnel host` opened to
nats-server; it stayed open for more than 5 minutes, until `devtunnel host` itself was stopped. The server meanwhile
delivered the new message to the dead connection's pull request, the stale-pull case from "Keepalive and reconnect",
so the hub's own `ping_interval` (30 s or less) is what bounds it, together with the daemon's ack wait (ants-vh4). A
real laptop sleep, with the Wi-Fi going down and the host clock jumping, was not reproduced; the freeze covers only
processes that stop running.

The HTTPS port URL `https://<tunnel-id>-8080.<region>.devtunnels.ms` serves as
`wss://<tunnel-id>-8080.<region>.devtunnels.ms` for `ants` or a leaf node only if the port allows anonymous access:

- Private tunnels accept a login cookie or the header `X-Tunnel-Authorization: tunnel <token>`. Neither async-nats nor
  nats-server leaf remotes can send it, and the service takes no token in the query string (microsoft/dev-tunnels#478).
- Anonymous access (`devtunnel access create ants-hub -p 8080 --anonymous`) opens the port to anyone who learns the
  URL; the NATS JWT credentials remain the access control. The monitoring port must never be tunnelled.
- `devtunnel token ants-hub --scopes connect` tokens expire after 24 hours and only a logged-in user can renew them;
  a CLI login lasts several days. A headless worker on a private tunnel therefore needs a periodic re-login (see
  "`devtunnel connect` as a service"), or the anonymous HTTPS port with NATS JWTs.
- The anti-phishing page is skipped for requests without `Accept: text/html`, which covers WebSocket upgrades.
- `devtunnel port create --request-timeout` exists ("timeout seconds for web requests, 0 = disabled"); whether it
  cuts long-lived WebSockets is untested.

## Recommendations

ants-32g.2, leaf node per machine with pluggable uplink (done: `ants leaf config`, and the services of ants-tlp.1 in
`contrib/`, see the README, "Services"):

- `ants` processes always use `nats://127.0.0.1:4222`; the uplink is only the leaf's `remotes` entry: a `wss://` URL
  or `nats-leaf://127.0.0.1:7422` through `devtunnel connect`, plus optional `proxy { url }` and `tls { ca_file }`.
- Needs a leaf user from `ants auth leaf` and a JetStream domain on the hub, with every process and credential using
  it (`--js-domain`); see "JetStream domain" and "Leaf uplink credentials".
- The hub sets `leafnodes { no_advertise: true }` and a `ping_interval` of 30 s or less.

ants-32g.3, HTTPS fallback bridge (done: `ants bridge`, see "HTTPS fallback bridge"):

- WebSocket passed through Caddy and through an HTTP `CONNECT` proxy, so the bridge is needed only behind proxies that
  inspect TLS and drop the `Upgrade`, which the spike could not reproduce. Since neither async-nats nor a leaf node
  speaks long-polling, it carries the leaf's TCP connection itself, between a local listener on the worker and the
  hub's leafnode port, rather than talking NATS.
