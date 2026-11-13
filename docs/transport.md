# Transport: reaching the hub

Findings of the spikes ants-qjf.9 (hub over `wss://` through a reverse proxy and a Dev Tunnels HTTPS port) and
ants-qjf.8 (hub over a Dev Tunnel TCP port), and what they mean for ants-32g.2 (leaf node per machine) and ants-32g.3
(HTTPS fallback bridge).

Each finding is marked **verified** (run with nats-server 2.15.3, async-nats 0.52, Caddy 2.11.9 on macOS arm64, every
hop on loopback) or **researched** (documentation or source reading only).

## Summary

| Path | Status |
|---|---|
| `ants` → `wss://` reverse proxy (Caddy, path `/nats`) → nats-server websocket, with and without JWT credentials | verified |
| Self-signed or private CA for `wss://` / `tls://` via `SSL_CERT_FILE` | verified |
| Leaf node → `wss://` reverse proxy → hub websocket (path `/nats/leafnode`) | verified |
| Leaf node → HTTP `CONNECT` forward proxy → `wss://` reverse proxy → hub | verified |
| Leaf node → TCP port forward (stand-in for `devtunnel connect`) → hub leafnode port | verified |
| `ants` itself behind an HTTP forward proxy | not possible: async-nats has no proxy support; a leaf node has |
| JetStream API (`$JS.API.>`) from a client on a leaf node | needs a JetStream domain (or a deprecated option) |
| Reconnect after proxy restart, forwarder drop and silent link loss; durable consumer resumes | verified |
| Dev Tunnels: `devtunnel host` / `connect` for TCP, HTTPS port as `wss://` | researched: blocked on `devtunnel user login` |
| Private Dev Tunnels HTTPS port from a `wss://` NATS client or leaf node | not possible: no way to send `X-Tunnel-Authorization` |

Recommendation: every machine runs a local leaf node and `ants` connects only to it (ants-32g.2). The uplink is either
a `wss://` endpoint (a reverse proxy, or an anonymous Dev Tunnels HTTPS port with NATS JWTs as the real access
control) or `devtunnel connect` to the hub's leafnode port for a private tunnel. The HTTPS fallback bridge (ants-32g.3)
stays deferred: nothing tested blocked WebSocket.

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

```hcl
server_name: macbook
listen: 127.0.0.1:4222                       # every ants process on the machine connects here
include "server.conf"                        # the hub's operator and account JWTs (no seeds in it)
leafnodes {
  remotes: [{
    urls: ["wss://hub.example.com/nats"]     # or ["nats-leaf://127.0.0.1:7422"] through `devtunnel connect`
    account: <ants account public key>       # the account in server.conf
    credentials: "leaf-macbook.creds"
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
  same `server.conf`. `ants auth` cannot issue a leaf user yet (the spike signed one by hand).
- **JetStream across a leaf node.** A leaf without JetStream denies the non-domain JetStream API `$JS.API.>` on its
  leaf connection (source: `leafnode.go`; verified: `ants init` on the leaf fails with "jetstream unavailable").
  Publishing into the stream still works, since its acks are plain replies. Two ways out, both verified:
  - Give the hub `jetstream { domain: hub }` and make clients use it, so they publish to `$JS.hub.API.>`: a consumer
    on the leaf using `jetstream::with_domain(client, "hub")` created its consumer and received messages. Persona
    permissions must then allow `$JS.hub.API.…` instead of `$JS.API.…`; ack subjects stay `$JS.ACK.ANTS.…`. Mapping
    the domain on the leaf with `default_js_domain: { <account>: hub }` does not work for a JetStream-enabled account.
  - Leave the hub without a domain and set `default_js_domain: { <account>: "" }` on the leaf. `ants init` then works
    unchanged, but nats-server warns that the option is a temporary compatibility measure and will be removed.
- **No store and forward.** While the uplink is down, publishing on the leaf fails at once ("no stream found for given
  subject"); nothing is queued locally.
- **No custom headers or query on leaf upgrades.** A leaf builds its upgrade URL from host and path only and sends no
  extra headers (source: `leafnode.go`), so a private Dev Tunnels HTTPS port is out of reach for leaf nodes too.

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
connection, so the server counted it as delivered and redelivered it only after `ack_wait` (60 s in the test; the
daemon uses 5 minutes). Nothing is lost, but a silent link can hold one message per thread for up to the daemon's ack
wait.

`ants daemon` fails a persona whose server is unreachable at start (async-nats `retry_on_initial_connect` is off).
With a local leaf node that server is the local one, which is up.

## Dev Tunnels

Researched only. `devtunnel` 1.0.2115 (official build, signed by Microsoft, from
`https://tunnelsassetsprod.blob.core.windows.net/cli/osx-arm64-devtunnel` as in `https://aka.ms/DevTunnelCliInstall`)
refuses to host without a login ("Anonymous does not have 'create' access scope"). Neither the CLI help nor the
documentation lists a telemetry opt-out.

Hub machine, once logged in (`devtunnel user login -g -d`: GitHub account, device code):

```sh
devtunnel create ants-hub                                # persistent tunnel, private to the creating account
devtunnel port create ants-hub -p 7422                   # leafnode port, raw TCP (protocol auto)
devtunnel port create ants-hub -p 8080 --protocol http   # websocket listener; the public URL is https/wss
devtunnel host ants-hub                                  # keep running under launchd or systemd
```

Worker machine, private tunnel:

```sh
devtunnel user login -g -d               # or pass --access-token to connect
devtunnel connect ants-hub               # forwards 127.0.0.1:7422 and :8080 to the hub
```

The worker's leaf remote is then `nats-leaf://127.0.0.1:7422`. `devtunnel connect` forwards each tunnel port to the
same local port number, which is why the hub tunnels its leafnode port (7422) and not 4222, where the worker's own leaf
node listens.

The HTTPS port URL `https://<tunnel-id>-8080.<region>.devtunnels.ms` serves as
`wss://<tunnel-id>-8080.<region>.devtunnels.ms` for `ants` or a leaf node only if the port allows anonymous access:

- Private tunnels accept a login cookie or the header `X-Tunnel-Authorization: tunnel <token>`. Neither async-nats nor
  nats-server leaf remotes can send it, and the service takes no token in the query string (microsoft/dev-tunnels#478).
- Anonymous access (`devtunnel access create ants-hub -p 8080 --anonymous`) opens the port to anyone who learns the
  URL; the NATS JWT credentials remain the access control. The monitoring port must never be tunnelled.
- `devtunnel token ants-hub --scopes connect` tokens expire after 24 hours and only a logged-in user can renew them;
  a CLI login lasts several days. A headless worker on a private tunnel therefore needs a periodic re-login, or the
  anonymous HTTPS port with NATS JWTs.
- The anti-phishing page is skipped for requests without `Accept: text/html`, which covers WebSocket upgrades.
- `devtunnel port create --request-timeout` exists ("timeout seconds for web requests, 0 = disabled"); whether it
  cuts long-lived WebSockets is untested.

Still to run by hand for ants-qjf.8: host on one machine, `devtunnel connect` from another, then laptop sleep and a
tunnel drop with `ants watch` and a daemon running.

## Recommendations

ants-32g.2, leaf node per machine with pluggable uplink:

- `ants` processes always use `nats://127.0.0.1:4222`; the uplink is only the leaf's `remotes` entry: a `wss://` URL
  or `nats-leaf://127.0.0.1:7422` through `devtunnel connect`, plus optional `proxy { url }` and `tls { ca_file }`.
- Needs a leaf user from `ants auth`, and a JetStream domain on the hub with persona permissions for
  `$JS.<domain>.API.…` (or the deprecated `default_js_domain` workaround).
- The hub sets `leafnodes { no_advertise: true }` and a `ping_interval` of 30 s or less.

ants-32g.3, HTTPS fallback bridge:

- WebSocket passed through Caddy and through an HTTP `CONNECT` proxy, so a bridge is needed only behind proxies that
  inspect TLS and drop the `Upgrade`, which the spike could not reproduce. Keep it deferred until such a network shows
  up. If built, it needs its own NATS client on the hub side and a local stand-in for a NATS server on the worker side,
  since neither async-nats nor a leaf node speaks long-polling.
