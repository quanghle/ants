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

Every key issued or signed (`user`, `enroll`, `bootstrap`, `channel-manager`, `leaf`, `enroll approve`, `enroll renew`, `signer`) is recorded in `issued.jsonl` (public key, name, kind, issue time, expiry, permissions version, plus device ID/label, GitHub binding and grant, or leaf members). Credentials stay valid until expiry unless revoked:

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

Run `ants init` once with human credentials to create stream `ANTS` and all 15 KV buckets (the first connection of any CLI command with human credentials also creates missing objects, after one concurrent round of stream infos). `ants init` only creates, and adds the subjects a stream `ANTS` created by an older version lacks (such as `ants.timeline.>`): an object already on the hub otherwise keeps its settings, such as the sources `ants leaf outbox` adds. It creates `ANTS`, `KV_ants_roster`, `KV_ants_channels` and `KV_ants_humans` with `max_consumers` `10000` rather than the `1000` `nats-server` gives by default (the buckets by a stream update right after creating them), and `ants_roster` with expiry markers.

## Backing up the team keys

Keep an off-machine backup of the team directory (`operator.nk`, `account.nk`, `system.nk`, `server.conf`, `issued.jsonl`, `revoked.json`; `.creds` files are omitted):

```sh
ants auth backup /media/usb/ants-team.age               # prompts for passphrase twice
ANTS_BACKUP_PASSPHRASE=... ants auth backup team.age    # or --passphrase-file FILE
ants auth restore team.age --dir ~/.config/ants/auth    # --force replaces existing team files
```

Backups use ASCII-armored [age](https://age-encryption.org) passphrase encryption (scrypt work factor 18 + ChaCha20-Poly1305, mode `0600`), compatible with `age -d` and `age -p`. Team keys always stay on disk (never in the OS credential store). `backup.json` records the last backup's issued-key count and revocation digest so `ants doctor` warns when new keys or revocations are unbacked-up. Restoring an older backup loses revocations made since: re-revoke those keys and run `ants auth push`.

## Devices

Each `ants auth user NAME --human` issues credentials with a unique UUIDv7 `ants-device:<id>` JWT tag and optional `--label`; the tag is required on human credentials:

```sh
ants auth user alice --human --label "Alice's laptop"
ants auth devices alice [--json]
ants auth rename alice "Alice's laptop" "Work laptop"   # by unique label, device ID, or public key
ants auth revoke alice --device 01968f7e-...            # revoke one device only
```

`ants auth devices` prints, per device, its ID, label, public key, encryption key and issue time, plus `github LOGIN (grant)` for a device of a name bound to a GitHub user, and `revoked`; `--json` prints the same as JSON lines (`github`, `grant`).

All devices of a human share its inbox read marker (`<human>`) and its channel read markers (`<human>.chan`, from permissions version 2) in `ants_read` (CAS-updated) and per-device presence in `ants_presence` for notification suppression (see [webhooks.md](webhooks.md#notifications)) and for showing in `ants chat` which other humans are online.

### Device encryption keys

Each human device also gets an X25519 encryption key (an nkeys xkey, public key `X...`), generated beside its NKey and bound into its user JWT as the tag `ants-xkey:<X...>`, so the account's signature ties it to that human and device. Messages sealed for the device are opened with its seed, which never leaves the device. The seed follows the NKey seed: in the `.creds` file as an `ANTS XKEY SEED` block after the NKey seed (nats tools read only the first two blocks and ignore it), or with `--store-key` in the OS credential store under its public key, the file keeping an `ANTS STORED XKEY` block. Ant, webhook, observer, channel manager and leaf credentials carry none.

```sh
ants auth user alice --human --public-key U... --xkey X...   # sign keys generated on the device; without --xkey none is bound
ants renew ~/.config/ants/auth/alice.creds                  # credentials binding none get a new key
ants login --hub wss://hub.example/nats --name alice       # a registered device gets one too
```

`ants auth devices` lists each device's encryption key (`xkey` in `--json`), as `issued.jsonl` records it.

#### Directory of device keys

Senders find the encryption keys of humans' devices in KV `ants_devices`. On connecting (and on `ants init`), a human's CLI whose credentials bind a key puts their user JWT under `<human>.<device>` when the entry differs (one direct get otherwise), warning without failing when it cannot. A human's credentials may create the bucket, read all of it and put only their own `<human>.>` keys, so no human swaps or drops another's device key; ants, webhooks, observers and the channel manager get no right on it. The user name `revoked` is reserved.

A reader takes a device's key only from a JWT it verifies itself: signed by the account that signed the reader's own credentials, a human's user JWT named for that human, tagged with that device and binding an `ants-xkey:`, unexpired, and with no `revoked.<U...>` record for its user key. It skips and reports (a warning) every other entry of the humans it reads, and never uses its key.

`ants auth revoke`, and `ants auth push` for those not recorded yet, put `revoked.<U...>` (`{key, revoked_at}`) for each revocation in `revoked.json` of a human's credentials, or of a key `issued.jsonl` does not know, once the hub applied the pushed account. They use short-lived credentials issued from the account key that may put only those records. When the hub cannot take them, the command fails after the push, and the next `ants auth push` records them. `ants doctor` checks it as **credentials xkey**: no key bound warns with `ants renew` as the fix; a seed missing, unreadable or not that of the JWT's tag fails.

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

Behind a leaf node (`~/.config/ants/leaf.conf` or `--leaf-config`), `ants add --enroll` signs the request with the leaf uplink key; `ants enroll approve` renews the leaf uplink JWT (up to the 252 KiB limit, ~29 ants with a domain or 54 without; beyond it the approval fails before anything is signed or recorded), records the ant in `issued.jsonl`, admits it to `ANTS_IN` outbox sourcing, and `ants add` has the leaf node reconnect its uplink with the renewed credentials, by a reload keeping local connections (`ants enroll` behind a leaf requires `ants auth leaf --enroll-approver`). After out-of-band `--jwt` behind a leaf, run `ants auth leaf MACHINE --add-ant NAME` and `ants leaf outbox MACHINE`. See [transport.md](transport.md#enrollment-behind-a-leaf-node).

## Registering humans

Humans register their own devices with `ants login`, which `ants signer` answers after a GitHub sign-in. Requests travel with the team's public bootstrap credentials:

```sh
ants auth bootstrap [--ttl 0] [--out FILE]   # bootstrap.creds (mode 0644; default: no expiry)
ants auth revoke bootstrap --nats wss://hub.example/nats   # withdraw them; reissue with ants auth bootstrap
```

Bootstrap credentials are meant to be shared: they only publish registration requests on `ants.login.*` and hear the answers on `ants.login.*.reply`, with messages of at most 4 KiB and no JetStream. They are recorded in `issued.jsonl` (kind `bootstrap`) and revoked like any key. Registration happens on the hub only: leaf uplinks never carry `ants.login` subjects, so both `ants login` and `ants signer` connect to the hub.

A registration request on `ants.login.<name>` carries the name asked for, the device's new NKey public key, a one-off X25519 reply key, optionally the device's [encryption key](#device-encryption-keys) and label, and its making and expiry times (at most 15 min apart), signed by the NKey. Requests are core NATS messages, stored nowhere: the machine repeats its request every 5 s until an answer settles it, and `ants signer` (with a `[registration]` table in its `--config`; see [config.md](config.md#human-registration)) answers each repeat on `ants.login.<name>.reply` with what it has for that key, sealed to the latest reply key and naming the signer's sealing key:

- `verify`: sign in to GitHub at the URL with the user code, before the time given. The signer runs one GitHub device flow per key, at most 10 at once, and reads the signed-in user's ID, login, verified email addresses and memberships with the user's token, which it then drops.
- `waiting`: the GitHub user who signed in (login) is outside the policy, and a human of the team decides ([config.md](config.md#human-registration)); `signed` or `refused` follows.
- `signed`: the human's credentials (a JWT for the request's NKey, with the device's encryption key when given), valid for the policy's `ttl`, recorded in `issued.jsonl` with the label, GitHub binding and grant.
- `refused`: why, such as registration not enabled, a key already issued, a human's rejection or no decision in time, or a name bound to someone else.

Requests that are malformed, unsigned by their key, expired or for another subject get no answer. A key whose registration ended gets the same answer until its request expires; another attempt uses a new key.

On the machine being registered:

```sh
ants login --hub wss://hub.example/nats --name alice [--label laptop] [--store-key] [--bootstrap FILE]
```

`--bootstrap` defaults to `~/.config/ants/auth/bootstrap.creds`. `ants login` makes the device's NKey and [encryption key](#device-encryption-keys) in memory, repeats its request until an answer comes, the request expires (15 min) or Ctrl-C, and shows the sign-in URL and user code only when the URL is GitHub's device login page (`https://github.com/login/device`); any other URL stops the login without showing it. Signed credentials are accepted only when issued to its key, binding its encryption key, for the name asked, by the account of the bootstrap credentials; they go to `~/.config/ants/auth/NAME.creds` (mode `0600`, with the encryption key seed as an `ANTS XKEY SEED` block, replacing an earlier file, as a new login does to renew an `email-domain` grant), or with `--store-key` both seeds go to the [OS credential store](#private-keys-in-the-os-credential-store) after the credentials authenticate at the hub. When `[cli]` in `ANTS_CONFIG` (`~/.ants/config.toml`) names no `creds`, it gets `creds` set to that file and, when it has no `nats`, `nats` set to the hub. A refusal, expiry or Ctrl-C leaves no key and no file behind.

### GitHub bindings

A name registered with a GitHub sign-in belongs to that GitHub user's numeric ID for good: renamed GitHub accounts keep their names, and no other GitHub user can register it. The binding is recorded on each device's record in `issued.jsonl`, with the grant that admitted the device (`org`, `team`, `email-domain` or `approval`), so it is kept across renewals and in backups. `ants auth revoke NAME` keeps the binding but withdraws an `approval` grant (the key's line appended again, marked `withdrawn`), so the user registers again only by the policy or a new approval; revoking one device or key withdraws nothing. An existing human name (issued with `ants auth user --human` before registration was enabled) is bound by the operator before its owner registers more devices with `ants login`:

```sh
ants auth user alice --human --github octocat [--config FILE]   # bind alice to GitHub user octocat, new device
```

`--github` looks the login up on GitHub (with the signer's token and GitHub URLs of the `[registration]` table in `--config`, `ANTS_CONFIG` or the default config file when present; see [config.md](config.md#human-registration)) and records its ID. It refuses `human`, the reserved names `bootstrap`, `channel-manager` and `signer`, a name holding credentials of anything but a human, a name bound to another GitHub user and a GitHub user bound to another name; on a name already bound to the same user it adds a device. `ants signer` applies the same rules, and also refuses to register a name that holds human credentials but no binding.

## Renewing over NATS

Valid, unrevoked credentials (ants, humans, webhooks, observers, `channel-manager`, and `leaf-<machine>` uplinks) can renew over NATS without copying files from the auth machine:

```sh
ants renew ~/.ants/dev-a.creds                                                            # target machine
ants renew ~/.config/ants/auth/leaf-macbook.creds --creds ~/.config/ants/auth/alice.creds # leaf uplink via human creds
ants enroll renewals --js-domain hub                                                      # auth machine: list pending
ants enroll renew dev-a --js-domain hub                                                   # sign one (or --auto to sign continuously)
```

`ants renew` generates a local NKey, publishes signed `RenewRequest`s on `ants.renew.<name>` every 5 s, verifies the returned JWT, atomically replaces the `.creds` file (preserving OS credential store placement), and reloads `ants.daemon`, or for leaf uplink credentials reconnects the uplink by reloading `ants.nats` with the `credentials` path respelled in `leaf.conf` (see [transport.md](transport.md#enrollment-behind-a-leaf-node)). For a human's credentials without an encryption key, `ants renew` also generates one and asks for it bound (the request's signed `xkey`), keeping its seed beside the new NKey seed; credentials that hold one keep it. `ants enroll renew` refuses a requested key for credentials of others than humans or already binding one, and preserves role, device, encryption key, domain, webhook destinations, leaf members and TTL while stamping the current permissions version, records `renewed_from` in `issued.jsonl`, and prints `ants auth revoke --key` for the old key. `ants enroll renew --auto` continuously approves valid unrevoked requests (reconnecting every 5 m to refresh `revoked.json`). Enrollment, leaf system, expired, or revoked credentials must be reissued via `ants auth`.

Credentials a registration granted (a device registered with `ants login`, whose latest line in `issued.jsonl` has a grant) are renewed by `ants signer` instead, which `ants enroll renew` and `ants enroll renewals` leave them to. The signer answers each repeat of such a request with the same `RenewReply`, holding either the JWT or `refused` with the reason, which `ants renew` fails with:

- `org` or `team` grant: the signer looks the GitHub user up by ID with its own token (see [config.md](config.md#human-registration)) and checks the memberships of the policy, reread. A user still a member under the policy is renewed with that grant and the current login; a user who is no member any more, or no GitHub user any more, is refused, every key of the name is revoked and the account pushed. Without a `[registration]` table or token, or when GitHub cannot tell, the renewal is refused alone: try again later. At most 10 checks run at once; more requests wait for their next repeat.
- `approval` grant: renewed while the approval stands; once `ants auth revoke NAME` withdrew it, refused, naming `ants login`.
- `email-domain` grant: refused, naming `ants login`, as only a new sign-in shows the verified email addresses.

A renewal keeps the credentials' earlier lifetime, not the policy's current `ttl`.

## Credentials across upgrades

Each user JWT carries `ants-perms:<n>` (current: `2`), required on every user JWT `ants auth` issues, and live daemons, webhooks, schedules and `ants channel serve` publish their running `ants` and `perms` versions. The version goes up whenever the permissions of a kind of credentials change. Connecting with outdated or soon-to-expire (< 7 d) credentials logs a warning with the exact renewal command (credentials newer than the running ants ask for an upgrade of ants); `ants doctor` (**versions**) verifies that all live services run the current permissions version before rights an older version had are withdrawn by renewing. Leaf uplinks whose members' rights changed need renewal and `ants leaf outbox` too.

## Private keys in the OS credential store

`ants keys store` moves a `.creds` file's NKey seed, and the seed of its [encryption key](#device-encryption-keys) when it holds one, into the OS credential store — macOS login Keychain (service `ants`), Linux Secret Service via `secret-tool` (`libsecret-tools` / `libsecret`), or Windows Credential Manager (`ants:<public_key>`) — after reading the seed back and authenticating against `--nats`, leaving only the JWT and public key reference in the file:

```sh
ants keys store ~/.config/ants/auth/alice.creds ~/.ants/dev-a.creds   # default: --creds
ants keys unstore ~/.ants/dev-a.creds                                  # write seeds back into file
ants keys remove ~/.ants/dev-a.creds                                   # delete file and stored seeds (does not revoke)
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

- **Subject-pinned senders:** ants and webhooks publish only under their own final subject token (`ants.inbox.*.<self>`, `ants.notes.*.<self>`, `ants.chan.*.<self>`, `ants.dead.<self>`, `ants.chanadm.*.<self>`, `ants.timeline.*.<self>`, plus `ants.out.*` on leaves). Readers verify payload `from`/target fields against the NATS subject or KV key (`ants watch` and `ants thread` mark mismatches `forged`). Only humans publish under arbitrary inbox senders (`ants dead replay`), write `ants.approvals.<ant>.<id>` (any human may decide any ant's request, so a [stand-in](webhooks.md#stand-in) needs no extra rights; the decision's `by` is informational, not pinned by the subject), send `ants.cancel.*` / `ants.queue.*`, write `ants_humans` / `ants_channels` (with `channel-manager`), or call `STREAM.MSG.GET` / `$JS.API.DIRECT.GET.ANTS`.
- **Scoped reads & key privacy:** stream info on `ANTS`, `ants_sessions`, `ants_attempts`, `ants_owners` and `ants_webhooks` exposes subjects/keys, so only humans hold `STREAM.INFO` there. Ants discover JetStream via `CONSUMER.INFO` on `inbox-<ant>`, pin consumer filters in the create subject (`inbox-<ant>[_1..3]`, `chan-<ant>`, `chanread-<ant>[_1..3]`, `notes-<ant>[_1..3]`, `deadread-<ant>`, `sentread-<ant>`), and read/write single keys via direct gets and plain publishes. Humans direct-get only spend keys (`*.*.spend`, `*.day.*`) in `ants_sessions`, message marks (`*.msg.*`) and `<self>.unserved.*` in `ants_attempts`, and only their own keys in `ants_read` (`<self>`, `<self>.chan`), `ants_presence` (puts; every human's is readable by direct gets and an ordered consumer, while ants read none), `ants_notify`, `ants_devices` (puts; all of it is readable), and `marks.<self>` in `ants_threads` (`title.*`, `summary.*` and `done.*` are shared across humans; ants access neither `ants_threads` nor others' `ants_reactions`). An ant's title suggestion travels as `title` on its own reply (inbox message, channel post or notes post), whose subject pins the sender; only a human's CLI stores it, and only on a `title.<thread>` key with no entry, not even a cleared title's delete marker.
- **Reactions for ants:** subjects cannot tie a reaction key (`<reactor>.<message>.<reaction>`) to the messages an ant reads, and the harness can read the ant's `.creds`, so ant credentials neither read nor write `ants_reactions` (human credentials write only their own keys). An ant asks only as itself: on `ants.reactions.<self>` for the reactions to a message, on `ants.reactions.<self>.react` to add or remove its own; it hears the answer on `_INBOX.<self>.reactions`, among its own reply inboxes, which only humans and `ants signer` publish to (adding no subject per ant to a leaf uplink). `ants signer` answers both in queue group `reactions`, with short-lived credentials it issues itself from the account key (adding to those of `ants enroll` stream info and direct gets on `ANTS`, every read and write of `ants_reactions`, and the reply subjects); `ants webhook serve` also answers listings with a `[[human]]` entry's credentials, which cannot write an ant's reaction. Either acts only when the first message stored in `ANTS` with that id (from 5 min before the time in its UUIDv7 to 24 h after, the duplicate window) is on a subject the ant reads: `ants.inbox.<self>.*`, `ants.inbox.*.<self>`, `ants.notes.>` or `ants.chan.>`. Senders choose ids, but no one learns an id before its message is stored, so a later copy stored under a readable subject grants nothing; any other message gets the same `unknown` answer as a missing one, and no change. A daemon without credentials, on an open server where every connection reads and writes everything, answers its ants in-process with the same check.
- **Attachments (`ants_files`):** a message carries only references (`attachments: [{id, name, mime, size, sha256}]`); the bytes sit in stream `OBJ_ants_files` as chunks on `$O.ants_files.C.<from>.<kind>.<target>.<id>.<n>`, where `<kind>.<target>` (`inbox.<to>`, `notes.<thread>` or `chan.<channel>`) follows the subject of the carrying message, so subject permissions confine them as they confine the messages. Every user writes chunks only under its own name (`$O.ants_files.C.<self>.>`, plus `ants.out.files.<self>.>` for a leaf outbox), each with `Nats-Expected-Last-Subject-Sequence: 0` when written directly, so stored bytes are never replaced, and with `Nats-Msg-Id` `<handle>.<n>`, so a copy sourced from a leaf outbox within a day is dropped as a duplicate ([transport.md](transport.md#store-and-forward)); the stream denies deletes and purges. Ants read by direct gets only `…C.*.inbox.<self>.>`, `…C.<self>.inbox.>`, `…C.*.notes.>` and `…C.*.chan.>`, with no stream info or consumer; humans read all, and alone create the store and update its sources (`ants leaf outbox`; an enrollment approver's uplink and `ants signer` too, on admitting a new ant). No Object Store metadata subjects are used, as their base64 names cannot be scoped. Readers check the bytes against the reference's size and SHA-256. Webhooks, observers, the channel manager and enrollment credentials get no access.
- **First-writer `ants_owners`:** written only by humans and ant-targeting webhooks (`<thread>.<ant>`, `Nats-Msg-Id: owner.<key>`). The stream enforces `discard_new_per_subject` with a 30 d duplicate window and denies deletes/purges, so first owner wins while outbox replays drop cleanly without stalling sourcing.
- **Read-only observers:** `Permissions::for_observer` allows only durable consumers `observe-{requests,decisions,dead}-<observer>` on `ANTS`, `ants.status.*.*`, `ants_roster` reads/watches, and self-renewal (see [hooks.md](hooks.md)).
- **Metadata verification:** daemons honour `owner` only for `human` or `ants_humans` members backed by channel posts or `ants_owners` (plus verified `origin_thread` chains in `ants_links`, max 32 hops), and honour `webhook: true` only with `<sender>.webhook` in `ants_webhooks` (or from a human).
