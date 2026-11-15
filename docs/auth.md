# Credentials and ACLs

`ants auth` manages decentralized NATS NKey/JWT credentials in `~/.config/ants/auth` (`--dir` / `ANTS_AUTH_DIR`; keep `*.nk` seeds private):

```sh
ants auth init                                          # writes operator.nk, account.nk, system.nk, server.conf
ants auth user human --js-domain hub                    # human.creds ("human" always gets Role::Human)
ants auth user alice --human --js-domain hub            # alice.creds (human role, new device ID)
ants auth user dev-a --js-domain hub                    # dev-a.creds (ant role)
ants auth user github --webhook --to dev-a --js-domain hub
ants auth user nightly --webhook --to dev-a --js-domain hub  # schedules use webhook credentials
ants auth channel-manager --js-domain hub               # channel-manager.creds
ants auth user audit --observer --js-domain hub         # audit.creds (read-only, for hooks)
ants auth user alice --human --store-key --js-domain hub  # seed stored in OS credential store
```

## Issuing and revoking credentials

Every key issued or signed (`user`, `enroll`, `channel-manager`, `leaf`, `enroll approve`, `enroll renew`, `signer`) is recorded in `issued.jsonl` (public key, name, kind, issue time, expiry, permissions version, plus device ID/label or leaf members). Credentials stay valid until expiry unless revoked:

```sh
ants auth revoke alice --nats wss://hub.example/nats        # every key issued to alice, all devices
ants auth revoke --key UABC... --nats wss://hub.example/nats # one public key
ants auth revoke leaf-macbook --no-push                     # record in server.conf without pushing yet
ants auth revoked                                           # key, name, revoked at, expiry
ants auth issued [--json]                                   # all issued keys + renewal commands
ants auth push --nats wss://hub.example/nats                # push account JWT to hub and leaves
```

`ants auth revoke NAME` revokes all keys for `NAME` in `issued.jsonl` (plus matching `.creds` files in `--dir`), updates `revoked.json` and `server.conf` (revoking JWTs for those keys issued at or before the revocation timestamp while leaving newer keys valid), and pushes the updated account via `$SYS.REQ.ACCOUNT.<account>.CLAIMS.UPDATE` to close revoked connections immediately. Revocations of keys whose credentials have all expired are pruned on the next `revoke` or `push`.

Include `server.conf` on the hub followed by `resolver { type: full, dir: "/ABSOLUTE/DIR" }` so pushed revocations persist across hub restarts and offline leaf nodes catch up within 10 s of reconnecting via their `leaf-<machine>.system.creds` uplink. Without a `full` resolver, copy `server.conf` to the hub and reload (`SIGHUP`) after each revocation. Full hub/leaf resolver mechanics live in [transport.md](transport.md#revocation).

Run `ants init` once with human credentials to create stream `ANTS` and all 14 KV buckets (the first connection of any CLI command with human credentials also creates missing objects and upgrades outdated stream/bucket settings in one concurrent pass, preserving leaf outbox sources). On `nats-server` 2.15+, `ants init` raises `max_consumers` from the default `1000` to `10000` on `ANTS`, `KV_ants_roster`, `KV_ants_channels` and `KV_ants_humans`, and enables `ants_roster` expiry markers (`nats-server` 2.11+).

## Backing up the team keys

Keep an off-machine backup of the team directory (`operator.nk`, `account.nk`, `system.nk`, `server.conf`, `issued.jsonl`, `revoked.json`; `.creds` files are omitted):

```sh
ants auth backup /media/usb/ants-team.age               # prompts for passphrase twice
ANTS_BACKUP_PASSPHRASE=... ants auth backup team.age    # or --passphrase-file FILE
ants auth restore team.age --dir ~/.config/ants/auth    # --force replaces existing team files
```

Backups use ASCII-armored [age](https://age-encryption.org) passphrase encryption (scrypt work factor 18 + ChaCha20-Poly1305, mode `0600`), compatible with `age -d` and `age -p`. Team keys always stay on disk (never in the OS credential store). `backup.json` records the last backup's issued-key count and revocation digest so `ants doctor` warns when new keys or revocations are unbacked-up. Restoring an older backup loses revocations made since: re-revoke those keys and run `ants auth push`.

## Devices

Each `ants auth user NAME --human` issues credentials with a unique UUIDv7 `ants-device:<id>` JWT tag and optional `--label` (credentials from before device IDs show as `legacy`):

```sh
ants auth user alice --human --label "Alice's laptop"
ants auth devices alice [--json]
ants auth rename alice "Alice's laptop" "Work laptop"   # by unique label, device ID, or public key
ants auth revoke alice --device 01968f7e-...            # revoke one device only
```

All devices of a human share one read marker in `ants_read` (CAS-updated; migrating and deleting any legacy `inbox-<name>` consumer on first `ants inbox` run) and per-device presence in `ants_presence` for notification suppression (see [webhooks.md](webhooks.md#notifications)).

## Adding an ant

`ants add` generates a user NKey on the target machine (`<ant>.nk` at mode `0600`, or seed straight into the OS store with `--store-key`), appends `[[ant]]` to `config.toml`, writes `<ant>.creds` once signed, and reloads `ants.daemon` / `ants-daemon`:

```sh
# Out of band
ants add dev-c --workdir ~/src/app                                # prints public key U...
ants auth user dev-c --public-key U... --js-domain hub            # auth machine: prints JWT
ants add dev-c --jwt eyJ...                                       # accepts JWT, file, or '-'

# Over NATS (direct or through leaf nodes)
ants auth enroll dev-c                                            # auth machine: enroll-dev-c.creds (default --ttl 1h; 0 = none)
ants add dev-c --workdir ~/src/app --enroll enroll-dev-c.creds    # target machine: waits for approval (--hub URL overrides --nats)
ants enroll list --js-domain hub                                  # auth machine
ants enroll approve dev-c --js-domain hub                         # or: ants enroll reject dev-c [--reason TEXT]
```

`ants add --enroll` repeats its request every 5 s and stops with an error at a rejection, when the enrollment credentials expire, or when the server refuses them (revoked), even while the server is unreachable.

Behind a leaf node (`~/.config/ants/leaf.conf` or `--leaf-config`), `ants add --enroll` signs the request with the leaf uplink key; `ants enroll approve` renews the leaf uplink JWT (up to the 252 KiB limit, ~30 ants with a domain or 60 without; beyond it the approval fails before anything is signed or recorded), records the ant in `issued.jsonl`, admits it to `ANTS_IN` outbox sourcing, and `ants add` restarts the leaf node service (`ants enroll` behind a leaf requires `ants auth leaf --enroll-approver`). After out-of-band `--jwt` behind a leaf, run `ants auth leaf MACHINE --add-ant NAME` and `ants leaf outbox MACHINE`. See [transport.md](transport.md#enrollment-behind-a-leaf-node).

## Renewing over NATS

Valid, unrevoked credentials at permissions version 2+ (ants, humans, webhooks, observers, `channel-manager`, and `leaf-<machine>` uplinks) can renew over NATS without copying files from the auth machine:

```sh
ants renew ~/.ants/dev-a.creds                                                            # target machine
ants renew ~/.config/ants/auth/leaf-macbook.creds --creds ~/.config/ants/auth/alice.creds # leaf uplink via human creds
ants enroll renewals --js-domain hub                                                      # auth machine: list pending
ants enroll renew dev-a --js-domain hub                                                   # sign one (or --auto to sign continuously)
```

`ants renew` generates a local NKey, publishes signed `RenewRequest`s on `ants.renew.<name>` every 5 s, verifies the returned JWT, atomically replaces the `.creds` file (preserving OS credential store placement), and reloads `ants.daemon` or restarts `ants.nats`. `ants enroll renew` preserves role, device, domain, webhook destinations, leaf members and TTL while stamping the current permissions version, records `renewed_from` in `issued.jsonl`, and prints `ants auth revoke --key` for the old key. `ants enroll renew --auto` continuously approves valid unrevoked requests (reconnecting every 5 m to refresh `revoked.json`). Enrollment, leaf system, expired, revoked, or pre-v2 credentials must be reissued via `ants auth`.

## Credentials across upgrades

Each user JWT carries `ants-perms:<n>` (current: `4`), and live daemons, webhooks, schedules and `ants channel serve` publish their running `ants` and `perms` versions. Connecting with outdated or soon-to-expire (< 7 d) credentials logs a warning with the exact renewal command; `ants doctor` (**versions**) verifies that all live services run the current permissions version before old rights (such as non-human stream info on `ANTS`, `ants_sessions`, `ants_attempts`, `ants_owners`, `ants_webhooks`) are withdrawn by renewing.

| Version | Key additions (cumulative; leaf uplinks on affected machines need renewal + `ants leaf outbox` too) |
|---|---|
| `1` | Base inbox, notes, channel, roster, approvals, dead letters; pre-tag credentials count as `< 1`. |
| `2` | Over-NATS renewal (`ants.renew.*`), `channel-manager.serve` version marker, `ants_read`, `ants_presence`, `ants_owners`, `ants cancel`, `ants spend`, `notify-<human>` consumer, ant inbox/notes/channel spare readers (`_1..3`). |
| `3` | Human batched direct gets (`$JS.API.DIRECT.GET.ANTS`), human `STREAM.UPDATE` on `KV_ants_roster` (expiry markers), ant `sentread-<ant>` consumer. |
| `4` | **Required before `nats-server` 2.16 / `js_ack_fc_v2`** (v2 `$JS.ACK.<domain>.<hash>.*` and `$JS.FC.<domain>.<hash>.*` subjects; older creds fail acks and stall KV watches). Also adds: `KV_ants_channels`/`KV_ants_humans` `STREAM.UPDATE` (`max_consumers`), template pool daily spend reads, `ants.antreq.*` + signer approvals, `ants_reactions`, `ants_threads`, `ants_notify`, `ants queue`/`ants withdraw` (`ants.queue.*`, `$KV.ants_attempts.*.msg.*`), JetStream storage checks (`$JS.API.INFO`, `$JS.API.STREAM.LIST`, `_storage.*.*.warned`), and unserved-message markers (`<self>.unserved.*`). |

## Private keys in the OS credential store

`ants keys store` moves a `.creds` file's NKey seed into the OS credential store — macOS login Keychain (service `ants`), Linux Secret Service via `secret-tool` (`libsecret-tools` / `libsecret`), or Windows Credential Manager (`ants:<public_key>`) — after reading the seed back and authenticating against `--nats`, leaving only the JWT and public key reference in the file:

```sh
ants keys store ~/.config/ants/auth/alice.creds ~/.ants/dev-a.creds   # default: --creds
ants keys unstore ~/.ants/dev-a.creds                                  # write seed back into file
ants keys remove ~/.ants/dev-a.creds                                   # delete file and stored seed (does not revoke)
```

`ants add --store-key`, `ants auth user --store-key`, and `ants auth channel-manager --store-key` write new seeds straight into the store without touching disk (printing the approved JWT if `ants add --enroll --store-key` fails so `--jwt` can finish later). `ants keys store` refuses leaf uplink credentials (read by `nats-server`), team keys (`operator.nk`, `account.nk`, `system.nk`), and ephemeral in-memory keys.

Platform notes (where no unlocked store is available, keep `0600` file-backed `.creds`):

- **Services & macOS:** `ants daemon` (and its schedules), `ants webhook serve` (webhooks and human notifications), and `ants channel serve` retry locked/unavailable stores at startup (`1s` doubling to `30s`), failing immediately only if the entry is missing or mismatched; one-shot CLI commands fail immediately. On macOS, replacing `~/.cargo/bin/ants` (`cargo install`) prompts Keychain access again: grant **Always Allow** in a GUI session and restart the service.
- **Linux Secret Service:** desktop login unlocks the keyring (`secret-tool` times out after 60 s if locked; `ants` queries D-Bus via `busctl`, `gdbus` or `dbus-send` so locked collections never masquerade as missing entries).
  - *KDE Wallet:* requires running `kwalletd6`/`kwalletd5` plus `libqca-qt6-plugins`; closed wallets prompt on read and fail deletes as locked until opened.
  - *KeePassXC:* enable **Secret Service Integration**, expose a group in database settings, and disable **Confirm when passwords are retrieved by clients** (and deletion confirmation if recycle bin is off) for unattended use.
  - *Headless systemd / WSL 2:* requires `systemd --user` and `$XDG_RUNTIME_DIR/bus` (`dbus-user-session`; `[boot] systemd=true` in `/etc/wsl.conf` on WSL 2, which cannot access the host's Windows Credential Manager). System units, cron and `sudo` have no session bus. Unlock over SSH after boot via `printf '%s' "$PASSWORD" | gnome-keyring-daemon --replace --unlock --components=secrets`, or keep file-backed `.creds` rather than storing a keyring password on disk.
- **Windows:** Credential Manager entries require an interactive/password logon; key-authenticated SSH sessions, other accounts, or passwordless scheduled tasks fail with error `1312` — use file-backed `.creds` there.

## Wire ACLs and verification invariants

- **Subject-pinned senders:** ants and webhooks publish only under their own final subject token (`ants.inbox.*.<self>`, `ants.notes.*.<self>`, `ants.chan.*.<self>`, `ants.dead.<self>`, `ants.chanadm.*.<self>`, plus `ants.out.*` on leaves). Readers verify payload `from`/target fields against the NATS subject or KV key (`ants watch` and `ants thread` mark mismatches `forged`). Only humans publish under arbitrary inbox senders (`ants dead replay`), write `ants.approvals.<ant>.<id>`, send `ants.cancel.*` / `ants.queue.*`, write `ants_humans` / `ants_channels` (with `channel-manager`), or call `STREAM.MSG.GET` / `$JS.API.DIRECT.GET.ANTS`.
- **Scoped reads & key privacy:** stream info on `ANTS`, `ants_sessions`, `ants_attempts`, `ants_owners` and `ants_webhooks` exposes subjects/keys, so only humans hold `STREAM.INFO` there. Ants discover JetStream via `CONSUMER.INFO` on `inbox-<ant>`, pin consumer filters in the create subject (`inbox-<ant>[_1..3]`, `chan-<ant>`, `chanread-<ant>[_1..3]`, `notes-<ant>[_1..3]`, `deadread-<ant>`, `sentread-<ant>`), and read/write single keys via direct gets and plain publishes. Humans direct-get only spend keys (`*.*.spend`, `*.day.*`) in `ants_sessions`, message marks (`*.msg.*`) and `<self>.unserved.*` in `ants_attempts`, and only their own keys in `ants_read`, `ants_presence`, `ants_notify`, and `marks.<self>` in `ants_threads` (`title.*` is shared across humans; ants access neither `ants_threads` nor others' `ants_reactions`).
- **First-writer `ants_owners`:** written only by humans and ant-targeting webhooks (`<thread>.<ant>`, `Nats-Msg-Id: owner.<key>`). The stream enforces `discard_new_per_subject` with a 30 d duplicate window and denies deletes/purges, so first owner wins while outbox replays drop cleanly without stalling sourcing.
- **Read-only observers:** `Permissions::for_observer` allows only durable consumers `observe-{requests,decisions,dead}-<observer>` on `ANTS`, `ants.status.*.*`, `ants_roster` reads/watches, and self-renewal (see [hooks.md](hooks.md)).
- **Metadata verification:** daemons honour `owner` only for `human` or `ants_humans` members backed by channel posts or `ants_owners` (plus verified `origin_thread` chains in `ants_links`, max 32 hops), and honour `webhook: true` only with `<sender>.webhook` in `ants_webhooks` (or from a human).
