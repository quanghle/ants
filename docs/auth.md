# Credentials and ACLs

`ants auth` manages decentralized NATS NKey/JWT credentials in `~/.config/ants/auth` (`--dir` / `ANTS_AUTH_DIR`; keep `*.nk` seeds private):

```sh
ants auth init                                          # writes operator.nk, account.nk, system.nk, server.conf
ants auth user human --js-domain hub                    # human.creds ("human" always gets Role::Human)
ants auth user alice --human --js-domain hub            # alice.creds (human role, new device ID)
ants auth user alice --human --admin --js-domain hub    # alice.creds (human role and admin role)
ants auth user pa --assistant-of alice --js-domain hub  # pa.creds (ant role, assistant of alice)
ants auth user dev-a --js-domain hub                    # dev-a.creds (ant role)
ants auth user github --webhook --to dev-a --js-domain hub
ants auth user nightly --webhook --to dev-a --js-domain hub  # schedules use webhook credentials
ants auth channel-manager --js-domain hub               # channel-manager.creds
ants auth host macbook --js-domain hub                  # host-macbook.creds (the machine's daemon)
ants auth user audit --observer --js-domain hub         # audit.creds (read-only, for hooks)
ants auth user alice --human --store-key --js-domain hub  # seed stored in OS credential store
```

## Issuing and revoking credentials

Every key issued or signed (`user`, `enroll`, `bootstrap`, `channel-manager`, `host`, `leaf`, `enroll approve`, `enroll renew`, `signer`) is recorded in `issued.jsonl` (public key, name, kind, issue time, expiry, permissions version, plus device ID/label, GitHub binding and grant, `admin`, `assistant_of`, or leaf members). Credentials stay valid until expiry unless revoked:

```sh
ants auth revoke alice --nats wss://hub.example/nats        # every key issued to alice, all devices
ants auth revoke --key UABC... --nats wss://hub.example/nats # one public key
ants auth revoke leaf-macbook --no-push                     # record in server.conf without pushing yet
ants auth revoked                                           # key, name, revoked at, expiry
ants auth issued [--json]                                   # all issued keys + renewal commands
ants auth admin list [--json]                               # live keys carrying the admin role
ants auth admin drop alice --nats wss://hub.example/nats    # revoke every admin key of alice
ants auth push --nats wss://hub.example/nats                # push account JWT to hub and leaves
```

`ants auth revoke NAME` revokes all keys for `NAME` in `issued.jsonl` (plus matching `.creds` files in `--dir`), updates `revoked.json` and `server.conf` (revoking JWTs for those keys issued at or before the revocation timestamp while leaving newer keys valid), and pushes the updated account via `$SYS.REQ.ACCOUNT.<account>.CLAIMS.UPDATE` to close revoked connections immediately. Revoking an ant or assistant by name also deletes its identity record from `ants_identities` once the account is pushed (with `--no-push` the record stays; `--key` and `--device` revocations keep it), so an ant added again later under the name is born anew, with a new `id` and avatar. Revocations of keys whose credentials have all expired are pruned on the next `revoke` or `push`.

Include `server.conf` on the hub followed by `resolver { type: full, dir: "/ABSOLUTE/DIR" }` so pushed revocations persist across hub restarts and offline leaf nodes catch up within 10 s of reconnecting via their `leaf-<machine>.system.creds` uplink. Without a `full` resolver, copy `server.conf` to the hub and reload (`SIGHUP`) after each revocation. Full hub/leaf resolver mechanics live in [transport.md](transport.md#revocation).

Run `ants init` once with human credentials to create stream `ANTS` and all 17 KV buckets (the first connection of any CLI command with human credentials also creates missing objects, after one concurrent round of stream infos). `ants init` only creates, adds the subjects a stream `ANTS` created by an older version lacks (such as `ants.timeline.>` or `ants.admin.>`), and widens to 24 h a shorter duplicate window of `ANTS` or `OBJ_ants_files` (such as the 2 min default of a store created before the window was set), in place, keeping their messages: an object already on the hub otherwise keeps its settings, such as the sources `ants leaf outbox` adds. `ants doctor` warns about such a window. It creates `ANTS`, `KV_ants_roster`, `KV_ants_channels` and `KV_ants_humans` with `max_consumers` `10000` rather than the `1000` `nats-server` gives by default (the buckets by a stream update right after creating them), and `ants_roster` with expiry markers.

## Machine host credentials

`ants auth host MACHINE` issues a machine's host credentials (user `host-<machine>`, never expiring, written to `host-<machine>.creds` in the team directory unless `--out` names another file). The daemon serving a whole config file on that machine connects with them once, apart from its ants, over its own inbox prefix `_INBOX_HOST.<machine>`, and keeps the machine's record in `ants_machines` (`<machine>`: the ants it serves) current: on start, and on each reload that changes them. The record is never deleted, so an ant whose daemon stopped and whose roster entry expired still has a machine (`ants machines`).

The daemon finds them as the `[host]` table's `creds` ([config.md](config.md)), else `~/.config/ants/auth/host.creds`, else `host-<machine>.creds` there for the table's `machine` or the first label of the host name. The machine is the table's `machine`, else the one the credentials name, else the first label of the host name; a table naming another machine than its credentials is an error. On the machine holding the team keys, `ants setup` issues them where the daemon finds them once the config file's ants have credentials; elsewhere copy the file to `~/.config/ants/auth/host.creds`. Behind a leaf node, the host connects through the leaf's uplink, which allows what the host of the leaf's own machine may do, so the credentials must name that machine.

A host may only put its own record (`$KV.ants_machines.<machine>`) and, for its machine's [admin orders](#admin-orders), look up and create the consumer `admin-<machine>` of `ANTS` filtering `ants.admin.<machine>.>`, take its messages and ack them, put and direct-get their outcomes (`$KV.ants_machines.<machine>.>`) and direct-get the revocation records of `ants_devices` (`revoked.<U...>`); it reads no inbox, channel or other bucket. No ant, webhook, observer or human writes `ants_machines`; humans look it up, create it and read all of it, by batched direct gets alone (`$JS.API.DIRECT.GET.KV_ants_machines`, no consumer). A daemon without host credentials, whose ants have credentials, logs once that the record and the admin orders stay off and serves its ants as before; with no credentials anywhere (a hub without `ants auth`), the host connects as no one. `ants doctor` warns of the missing credentials and names the `ants auth host` command. `ants auth renew` renews host credentials as other kinds (new key, current permissions); they do not renew over NATS.

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

`ants auth devices` prints, per device, its ID, label, public key, encryption key and issue time, plus `github LOGIN (grant)` for a device of a name bound to a GitHub user, `admin` for one carrying the [admin role](#admins), and `revoked`; `--json` prints the same as JSON lines (`github`, `grant`, `admin`). Without `--json`, a line `assistant NAME KEY issued TIME [revoked]` follows for each key of the human's [assistant](#assistants).

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

Senders find the encryption keys of humans' devices in KV `ants_devices`. On connecting (and on `ants init`), a human's CLI whose credentials bind a key puts their user JWT under `<human>.<device>` when the entry differs (one direct get otherwise), warning without failing when it cannot. A human's credentials may create the bucket, read all of it and put only their own `<human>.>` keys, so no human swaps or drops another's device key; webhooks, observers and the channel manager get no right on it, and ants only the reads below. The user name `revoked` is reserved.

An assistant's daemon also puts its own user JWT there, under `<assistant>.assistant` ([acting for its human](#acting-for-its-human)); ants may direct-get those records and the `revoked.*` ones, nothing else of the bucket.

A reader takes a device's key only from a JWT it verifies itself: signed by the account that signed the reader's own credentials, a human's user JWT named for that human, tagged with that device and binding an `ants-xkey:`, unexpired, and with no `revoked.<U...>` record for its user key. It skips and reports (a warning) every other entry of the humans it reads, and never uses its key.

`ants auth revoke`, and `ants auth push` for those not recorded yet, put `revoked.<U...>` (`{key, revoked_at}`) for each revocation in `revoked.json` of a human's credentials, or of a key `issued.jsonl` does not know, once the hub applied the pushed account. They use short-lived credentials issued from the account key that may put only those records. When the hub cannot take them, the command fails after the push, and the next `ants auth push` records them. `ants doctor` checks it as **credentials xkey**: no key bound warns with `ants renew` as the fix; a seed missing, unreadable or not that of the JWT's tag fails.

#### Sealed messages between humans

`ants ask @HUMAN` with a human's credentials seals the message end to end, as does `ants chat` for what it sends to another human (where nothing is sent in the clear: `ants ask --plaintext` does that), so the hub, ants and other humans' devices see none of its text. The message is sent with an empty `text` and a `sealed` object:

```json
"sealed": {"v": 1, "device": "<sending device>", "keys": [{"device": "<id>", "key": "<base64>"}], "body": "<base64>"}
```

- **`body`:** a random 24-byte nonce followed by the XSalsa20-Poly1305 encryption, under a random 32-byte content key, of a JSON `SealedBody`: `from`, `to`, `thread_id`, `id` (the message's) and `text`, plus `title` and `attachments` when present.
- **`keys`:** per recipient device, its id and the content key sealed with crypto_box (nkeys `XKey::seal`) from the sending device's encryption key to that device's. They cover every device of both humans with a verified key in `ants_devices` at sending time, the sending one included, and the assistant either enrolled as a device ([Assistant as a device](#assistant-as-a-device)), and no other: a device added later opens no earlier message.
- **In the clear:** the subject, id, thread, `from`, `to`, time, `in_reply_to`, owner, sizes, recipient device ids and reactions.
- **Attachments:** each file of `--attach` is stored in `ants_files` as the XSalsa20-Poly1305 encryption of its bytes under the message's content key and a random 24-byte nonce of its own, 16 bytes longer than the file (so a sealed file is at most 8 MiB less 16 bytes). Its reference, with `sealed: {"nonce": "<base64>"}` and the `size` and `sha256` of the bytes as stored, travels only inside the `SealedBody`; the reference in chunk 0's `Ants-Attachment` header names it `sealed`, of type `application/octet-stream`, so neither the envelope nor the store shows the file's name, type or hash.

Sending refuses, and sends nothing, when the credentials bind no encryption key, when the sending device's key is not the verified one in `ants_devices`, or when the recipient has no device of its own with a verified key (an enrolled assistant does not count); each error names its fix, and `--plaintext` sends the message in the clear instead. Only a human seals, and only to another human; messages to ants and channel posts stay in the clear. So do answers to questions: only ants ask them (`ask_human`), so an answer goes to an ant, and `ants answer` and the question cards and `/answer` of `ants chat` refuse a question from a human, which no CLI sends, pointing at `ants ask @HUMAN`.

A reader opens a sealed message only through its own device's entry in `keys`, which must open with the verified key of the sending device the message names, a device of its `from` human (or, from an assistant, the device a grant standing enrolls it as: never one of its human's messages), and a body naming the envelope's `from`, `to`, thread and id. `ants inbox` (`-f` too), `ants thread show`, `ants thread list`, `ants thread search` and `ants ask --wait`/`-f` print the opened text, looking up in `ants_devices` a sending device not known when the command started, so a device enrolled meanwhile does not make its messages `[forged]`; a message without an entry for the device prints as `[sealed]` and one that does not open as `[forged]` (`sealed`: `opened`, `unreadable` or `forged` in `--json`). `--grep` matches only opened text. The attachments of an opened message list under it by their names (`📎 name (size) handle`, the size the file's); `ants attachment get HANDLE` of an attachment stored `sealed` finds the sealed message carrying it on the subject of its handle, from the time in its id on, opens it, checks the stored bytes against the reference and writes the opened file under its name, and writes nothing where the device does not open that message. In `ants chat`, the DM and thread views and `/search` open sealed messages with the device's key, looking up a sending device added since it started, and mark them and the titles of their threads with 🔒; a message without an entry for the device reads `[sealed] sent before this device had a key` and one that does not open `[forged]`. `ants watch` and dead letters keep the ciphertext and show `[sealed, N bytes]`. When `ants_devices` cannot be read, a sender with no known key makes a message `[sealed]` rather than `[forged]`.

On a thread holding a sealed message between two humans, `ants thread title` (and `/title` in `ants chat`) seals the title the same way for every device of both with a verified key, refusing when either has none, and refuses a third human. The `title.<thread>` entry of `ants_threads` then carries an empty `title` and `sealed`, whose body is a JSON `SealedTitle` (`thread_id`, `by`, `title`) checked against the entry and opened with the verified key of a sending device of `by`. `ants thread list`, `ants thread show` and THREAD completion show the title where the device opens it and `[sealed]` elsewhere, and no ant's suggested title is stored for such a thread. A notice of `ants webhook serve` about a sealed message says only `a sealed message arrived`, naming its sender and thread ([webhooks.md](webhooks.md#notifications)).

## Roles

A role comes only with a user JWT signed by the team account: no config file, message or KV entry grants one. Tags are matched case-insensitively. `ants_proto::verify_user_jwt` checks a user JWT against the team account's public keys (issuer, user type, a `U...` subject, a valid name, expiry) and reads its roles; `verify_admin` also checks a signature by the JWT's key over a payload, and `verify_assistant_of` the human the JWT names. A reader also checks that the key is not revoked: `ants auth revoke`, `ants auth admin drop` and `ants auth push` put `revoked.<U...>` in `ants_devices` for every revoked key of a human's or an assistant's credentials ([directory of device keys](#directory-of-device-keys)).

### Admins

`ants auth user NAME --human --admin` (or `ants auth user human --admin`) issues a device of the human whose user JWT carries the tag `ants-admin`; only a human's credentials take it. An admin may order any ant's daemon to change the ant. The role belongs to the device's credentials: other devices of the human have it only if issued with `--admin`, and `ants auth devices` and `ants auth issued` mark the ones that do (`admin`, `human (admin)`). Renewal (`ants renew`, `ants enroll renew`, the signer's) keeps it, so a renewed key carries it too.

- **Granting:** issue a new device with `--admin`; the human's other devices stay as they are.
- **Listing:** `ants auth admin list [--json]` prints the keys carrying the role, neither revoked nor expired: human, device, label, key, issue time and expiry.
- **Dropping:** `ants auth admin drop NAME` revokes every such key of the human, earlier keys of renewed credentials among them, and pushes the account (`--no-push` records the revocation only); the human's other devices keep working. `ants auth revoke NAME` withdraws the role with every other key of the name.

### Admin orders

An admin's order to an ant (`AdminOrder`) travels on `ants.admin.<machine>.<ant>`, kept by stream `ANTS`, to the daemon serving the whole config file on that machine. The daemon takes it over its host connection ([Machine host credentials](#machine-host-credentials)) from consumer `admin-<machine>`, one order at a time in stream order, records its outcome once and acks it only after storing the outcome. The order (JSON):

```json
{"id": "<UUIDv7>", "admin": "<human>", "machine": "<machine>", "ant": "<ant>", "op": {"kind": "<op>", ...}, "sent_at": "<RFC 3339>", "credentials": "<admin device's user JWT>", "signature": "<base64url Ed25519>"}
```

`op` is one of `set` (`changes`: each a `field`, the path of keys from the ant's `[[ant]]` table, or, first `harnesses`, from the config file's `[harnesses]` table, the `expected` value the field holds, left out when it must be absent, or in its place the `expected_digest` a `read_settings` outcome gave of a secret's value, and the new `value`, left out to remove it; all apply or none), `pause`, `resume`, `restart` (`now`: working harnesses too, without waiting for their prompts), `add` (`settings`: the `[[ant]]` table of a new ant on the machine, named by the order's ant), `retire`, `read_memories` and `read_settings` (no fields). `signature` is the NKey signature of the credentials over `ants-admin-order`, the id, the admin, the machine, the ant, `sent_at` (RFC 3339, nanoseconds, `Z`) and the op as canonical JSON (no whitespace, the keys of every object sorted), each on a line of its own. The daemon refuses an order, recording why, unless:

- it came on the subject of the machine and ant it names;
- `credentials` verifies with the account that signed the daemon's host credentials, is a human's user JWT named for the admin the order names and tagged `ants-admin` ([Admins](#admins)), had not expired when the stream stored the order, and its key made `signature`;
- no `revoked.<U...>` record in `ants_devices` names its key, when the daemon takes the order;
- `sent_at` is at most 5 min ahead of the time the stream stored it;
- the daemon's config file leaves `[admin] remote` on ([config.md](config.md)).

An order waits in `ANTS` (30 d) while its machine's daemon is away, so none but a `read_memories` is refused for its age. Against replay, an outcome is created under the order's id and never replaced: the daemon acks an order whose outcome exists without acting on it again, and `ANTS` drops an id sent again within its 24 h duplicate window. A field the op does not know, or an op it does not know, fails the signature or the reading of the order, which is then refused.

A `set` order that passes the checks edits the ant's `[[ant]]` table in the daemon's config file in place, or, for a field under `harnesses`, the file's `[harnesses]` table, which every ant of the machine shares, keeping the rest of the text, comments and formatting included. Each change applies only if its field holds the `expected` value (numbers compare by value, so `3` and `3.0` match), or a value of digest `expected_digest`, or already holds the new `value`; otherwise the order is refused naming the field (`model changed since the order was sent: it holds "o3", the order expected "gpt-5"`), and nothing changes. A JSON object becomes an inline table, a missing parent of a nested field (`["env", "RUST_LOG"]`) an inline table too; `name` never changes, and `null` is no value. The file as edited must load as on a reload ([config.md](config.md)), templated ants staying within their template's bounds, or the order is refused with the loader's error and nothing changes. The daemon then writes it beside the old file (`config.toml.new`, same permissions, flushed), renames it over the old one, has the daemon reload it at once rather than at its next check of the file, records the outcome `applied` and only then acks the order. An order taken again after its edit was written finds its fields holding their new values: it is applied without writing again.

A `read_settings` order reads the ant's `[[ant]]` table and the file's `[harnesses]` table as they stand, and its outcome's `result` carries them: `fields`, the keys of the ant's table and, under `harnesses`, the harnesses table, and `hidden`, the fields holding a secret, and those holding them, each with the `digest` of its value. Every human reads an outcome, so a string holding a secret, as the daemon's redaction finds them (secret-named or token-like values of the ant's and the harnesses' `env`, known token formats), reads `[redacted]` where the secret is; its digest is an HMAC-SHA256 under a key random to the daemon's process, so a digest read before the daemon restarted matches nothing and a change expecting it is refused. A `set` order's new values, secrets included, travel and stay in the order and its outcome as sent: set secrets on the machine itself. `ants ant show`, `ants ant set` and the ant view of `ants chat` send one with an admin's credentials, unless one to the ant is still pending, and otherwise take the last applied read.

A `pause` order sets `paused = true` in the ant's `[[ant]]` table the same way, and a `resume` order removes the key; the daemon reloads at once, which pauses or resumes the running ant in place, restarting no harness ([config.md](config.md#pausing-an-ant)), and the ant's roster entry names the admin whose order paused it. An ant already paused, or not paused, is left as it is, the order applied without writing. `ants ant pause ANT` and `ants ant resume ANT` send them.

A `read_memories` order is answered only while the machine's daemon is online: one stored more than 2 min before the daemon takes it is refused as stale, since no one waits for its answer any more and the memories would be out of date. Otherwise the daemon reads the ant's core memories (`whoami`, `memory`, `tasks`) from its config file (refusing when it serves none or the file lists no such ant) and its harness's memory files and bd memories ([config.md](config.md#memory-files)), and records the outcome `applied` with the memories as its `result`: `core`, `harness`, `workdir`, `files` (each `path`, `size`, `text`, `truncated`), `bd` (`db`, `memories` each `key`, `content`, `truncated`, or `error`) and `skipped` (each `what` and `why`). The texts are capped (32 KiB each, 192 KiB of JSON together) so the outcome fits the bucket. `ants ant memories ANT [--json]` sends one and shows the answer.

A `restart` order that passes the checks has the daemon serving the ant stop its harnesses: the idle ones at once, the working ones once their current prompt ends; with `now` it first cancels those prompts as `ants cancel` does, each message answered with what its prompt wrote so far and the notice of the cancel, and acked, not retried. Each thread's next prompt starts a new harness, which resumes the thread's session; messages arriving meanwhile wait in the stream as usual. The outcome is recorded once the restart is under way, not once every harness stopped: `applied`, its `result` counting the idle harnesses stopped (`stopped`), the working ones left to stop after their prompt (`after_prompt`) and the prompts cancelled (`cancelled`). An ant the daemon serves while it is not serving the ant's inbox yet (connecting, or waiting for another daemon to hand it over) runs no harness: the order is applied with nothing stopped. An order to an ant the machine's daemon does not serve is refused. `ants ant restart ANT [--now]` sends one, as does `r` in the ant view of `ants chat`.

Every other op is refused as `not supported yet` in this version.

The outcome (`AdminOutcome`) is stored in `ants_machines` under `<machine>.<ant>.<id>`, of the machine and ant of the subject the order came on, so a machine's host credentials write only outcomes of orders to that machine: `id`, `machine`, `ant`, what the order names (`admin`, `op`, `sent_at`, left out when it does not read), `state` (`applied` or `refused`), `reason`, `result` (such as the memories read) and `decided_at`. An order in the stream with no outcome is pending. `ants ant orders ANT [--json]` lists the orders to an ant on every machine, oldest first, with where each stands.

### Assistants

`ants auth user NAME --assistant-of HUMAN` issues the credentials of an ant serving as the assistant of a human: its user JWT carries the tag `ants-assistant-of:<human>`. The tag in the JWT is the only thing that makes an ant an assistant; an ant's config cannot. The command refuses:

- a human (`human`, or a name with a human's credentials), or an ant added from a template by a manager;
- a HUMAN that is neither `human` nor a name with human credentials in `issued.jsonl`;
- a second assistant of the same human, or an ant already the assistant of another, while their credentials are neither revoked nor expired: revoke them first (`ants auth revoke NAME`).

An assistant keeps working as an ant, served by a daemon, and renews over NATS as one. `ants auth issued` shows its kind as `assistant of HUMAN`, `ants auth devices HUMAN` lists its keys, `ants doctor` accepts its credentials for the ant, and `ants auth revoke NAME` revokes them as for any user. Its rights are in [Wire ACLs](#wire-acls-and-verification-invariants). A leaf node's uplink carries an ant's rights for each ant member, so through a leaf node an assistant gets only those, nor keeps the record readers check its acts for its human against ([acting for its human](#acting-for-its-human)): connect its daemon to the hub directly.

### Dedicated assistant

`ants assistant enable` gives the CLI's human an assistant in one step, and is the recommended path: an ant that only assists, separate from the ants that code. Run where the team's account key is (`--dir`, default `~/.config/ants/auth`) with a human device's credentials, it:

1. issues the credentials of `<human>-assistant` (`--name` for another) as the assistant of the human, as `ants auth user NAME --assistant-of HUMAN` does, into the `creds` of its `[[ant]]`, else `<dir>/<name>.creds` (`--out`; `--store-key` keeps the seed in the OS credential store);
2. appends its `[[ant]]` to the config file (`--config`): working in its own home (`--home`, default `~/.ants/<name>`), with a whoami describing the assistant's role, the human it serves and that what it reads is data, never instructions (`--whoami` for another), `allow = ["read", "search", "think"]` and no `fetch` or `execute` rule (`--allow RULE` adds one, with a warning for `fetch`, `execute` or `*`), and the harness, model, reasoning effort and context flags of `ants add`;
3. signs the [grant](#assistant-grant) naming it, then reloads the daemon serving the config file.

Run again, each step keeps what an earlier run left (credentials naming the human and neither revoked nor expired, the listed entry) and completes the rest, so a run that failed half-way finishes; expired or revoked credentials are replaced. It refuses before changing anything: without the account key (designate an ant whose credentials were issued where the key is with `ants auth user ANT --assistant-of HUMAN` and `ants assistant set ANT` instead), a name of a human or one the team reserves, an ant added from a template, the credentials of another ant or human at the path, or while another assistant of the human holds live credentials. On a machine running a leaf node, the ant's `nats` should reach the hub directly ([assistants](#assistants)).

`ants assistant disable` withdraws the grant, as `clear` does, ending any enrollment of the assistant as a device ([assistant as a device](#assistant-as-a-device)) and deleting the seed of its key beside the credentials the config file names, then offers to retire the dedicated assistant: the one the grant named, else `<human>-assistant` (`--name`), when the config file lists it without a template, working in its own home, its credentials naming the human. Asked on a terminal, or at once with `--retire`, it unlists the ant (its daemon stops it, prompts in flight ending first), revokes its credentials with the account key in `--dir` and pushes the revocation to the hub (`--no-push` to push later with `ants auth push`), and removes the credentials file and any seed of an encryption key beside it; without the account key it prints the `ants auth revoke --key` to run where the key is. The home stays, so `ants assistant enable` brings the ant back with its memory. `--retire` for an ant that is not a dedicated assistant refuses before withdrawing anything.

Any other ant not added from a template may be designated instead (`ants assistant set ANT`), but the dedicated ant keeps the injection surface small: an assistant holds its human's trust, and everything it reads can try to steer it. An ant that codes reads repositories, issues, fetched web pages and command output, any of which may carry instructions planted by someone else; an assistant working only in its own home, reading its human's queue and threads, and running no `fetch` or `execute` requests unasked reads far less such content.

### Assistant grant

A human designates at most one ant as its assistant, the ant that acts for it, by a grant its device signs. `ants assistant set ANT` stores it in KV `ants_humans` under `<human>.assistant`, in place of any earlier one; `ants assistant clear` stores a signed withdrawal there (an `assistant` of `null`) rather than deleting the key; `ants assistant show [HUMAN]` prints the assistant a grant names, with when and by which device, once it verifies. `set` and `clear` need a human device's credentials (`ants auth user NAME --human`, or `ants login`). The record (`AssistantGrant`) is:

```json
{"human": "<human>", "assistant": "<ant>" | null, "issued_at": "<RFC 3339>", "jwt": "<signing device's user JWT>", "signature": "<base64url Ed25519>"}
```

`signature` is the device credentials' NKey signature over `ants-assistant-grant`, the human, the assistant (empty for a withdrawal) and `issued_at` (RFC 3339, nanoseconds), each on a line of its own. A reader honours a grant only when:

- the key is `<human>.assistant` of the human the record names;
- `jwt` verifies with the account that signed the reader's own credentials, is a human's user JWT named for that human and tagged with a device (`ants-device:`), was not expired at `issued_at`, and its user key made `signature`;
- `issued_at` is at most 5 min ahead of the reader's clock, and not before the newest record of that human the reader saw;
- the ant acting for the human carries the human in the `ants-assistant-of:` tag of its own verified credentials (issued by `ants auth user ANT --assistant-of HUMAN`), so the grant alone makes no ant an assistant.

A record failing any check counts as no assistant: a refusal never keeps an earlier assistant. Against replay, only the human's credentials write `<human>.assistant` (from permissions version 3 a human puts only `ants_humans` entries, `<name>`, and its own `<human>.>` keys; ants, webhooks and the other kinds of credentials write none), a withdrawal is a newer signed record rather than a gap an older record could fill, and a reader drops a record issued before one it saw. `set` stores with a compare-and-set on the key's revision and issues its record after the one stored, so the human's devices with clocks apart still replace each other's.

A human's text naming no one, `ants ask` without `@ant` and the `ants chat` command bar's text with no addressee (no thread open, no DM view, or nobody else on the open thread), goes to its assistant while the grant naming it verifies, else to the messenger; the CLI reads and verifies the grant at each such send, so a withdrawal holds from the next one, and names the recipient: `to pa (assistant of alice)` after `ants ask`'s message line, `sent to pa (assistant of alice) on thread …` under the bar.

While a human is away, `ants webhook serve` relays what waits for it to its assistant when the human set no stand-in of its own ([webhooks.md](webhooks.md#stand-in)).

Daemons keep the grants of every human from the watch they keep of `ants_humans` for its entries, without a consumer of their own; a withdrawal, or a replacement, takes effect for a daemon as soon as its watch delivers the update. A grant outlives the revocation or expiry of the device that signed it: `ants assistant clear` from another device ends it. `set` (for an existing ant; [`enable`](#dedicated-assistant) creates a dedicated one) refuses an ant the config file (`template` of its `[[ant]]`) or the team's registry on the machine records as added from a template (an assistant is a dedicated ant, not one of a pool), and warns when the roster or the config file gives the ant a workdir other than its own home, or the config file lets it run tools without asking (`allow_tools`, or `allow` rules `*`, `fetch` or `execute`): what such an ant reads can steer it while it holds its human's trust. Where neither the config file nor the registry knows the ant, `set` says that the template check was not made.

#### Assistant as a device

An assistant opens and seals its human's sealed messages only when the human enrolled it as one of its devices; it is off otherwise. `ants assistant seal on` re-signs the grant with a `sealing` object, `{"device": "<id>", "xkey": "<X...>"}`, and the signature covers two more lines, the device and the key (a withdrawal carries none). Without `--key`, it takes the encryption key of the assistant on this machine (the credentials of its `[[ant]]` in the config file), made if it has none: the seed waits beside those credentials, `pa.creds` keeping it in `pa.xk` (mode 0600), apart from them, as the ant's user JWT binds no key. For an assistant running on another machine, `ants assistant key ANT` there prints its key (making it), and `ants assistant seal on --key KEY` enrolls that. Enrolling the same key again keeps the device id; `ants assistant show` prints the device and key.

While a grant standing enrolls it, readers verifying the grant as above take the device as one of the human's, without an `ants_devices` entry:

- `ants ask` and `ants chat` seal a message between the human and another human for the assistant too, either way, and `ants thread title` the title of their thread.
- The assistant's daemon opens and seals with the key for its [tools](#tools-acting-for-its-human) (`ants_daemon::sealing::AssistantSealing`), loading the seed beside its credentials on start (warning, with sealing off, when it cannot): a sealed message from or to its human, or from the assistant, opens, and one the assistant sends for its human is sealed from the assistant (`from` the ant, never its human) for every device of the human and of the recipient, refusing attachments and a recipient without a device of its own. It reads `ants_devices` to check the sending devices.
- A reader checks a message sealed with the device as sent by the assistant: one naming its human as `from` reads `[forged]`, so the assistant cannot pass a message off as its human's.

`ants assistant seal off`, `ants assistant clear`, `ants assistant disable`, and `ants assistant set` or `enable` of another ant end the enrollment at once: senders read the grant standing for each message they seal, so nothing more is sealed for the device, and the assistant's daemon opens and seals nothing once its watch delivers the record. The key is no credential the account signed, so ending it takes no account key and no `ants auth push`. The command deletes the seed where the assistant runs on the same machine, and otherwise says where it stays: deleting it there leaves the messages sealed for the device before unopened by anyone holding it. Revoking the assistant's own credentials, `ants auth revoke ANT` and `ants auth push` with the account key, stays apart from this.

#### Acting for its human

What an assistant does for its human it does under its own name: the subject or key pins the assistant as the sender, and the payload names the human in `on_behalf_of` (absent otherwise). It is carried by an inbox message (`Envelope`, an approval decision sent on one included), an `ApprovalDecision`, a reaction (`ReactionEntry`, and `ReactionChange` as followers hear it), a thread title (`ThreadTitle`) and a thread marked done (`ThreadDone`). The daemon sets it for the [tools an assistant acts for its human with](#tools-acting-for-its-human) only while its credentials name a human and the human's grant names the assistant.

So that a reader can check the assistant's tag without a JWT in every message, the assistant's daemon puts, on start, its user JWT in `ants_devices` under `<assistant>.assistant` (`{"jwt": "<user JWT>"}`, `AssistantRecord`), when the entry differs, warning without failing when it cannot. Only an assistant's credentials put that key, and only their own; no device id is `assistant`, so the key is no device's entry. A reader honours `on_behalf_of: <human>` from publisher `<assistant>` only when:

- the human's grant standing, verified as in [assistant grant](#assistant-grant), names the publisher;
- the record under `<assistant>.assistant` verifies with the account that signed the reader's own credentials, is an unexpired user JWT named for the publisher, carries `ants-assistant-of:<human>`, and its user key has no `revoked.<U...>` record.

A claim failing either is forged: the act counts as the assistant's own. Daemons log a warning and drop the claim before acting on the message; `ants inbox`, `ants thread show`, `ants watch` and the thread view of `ants chat` show the sender as `pa (forged: for alice)`, and an honoured claim as `alice via pa` (`--json` adds `acting`: `{"for": "alice"}` or `{"forged": "alice"}`). Daemons read the grant from their watch of `ants_humans`, so a withdrawal or a replacement stops the honouring as soon as the watch delivers it; the CLI reads it once per process, and a reader without a checker (`ants thread search`) shows every claim as forged.

On a message from an assistant for its human, the recipient's daemon tells the human: a message in its inbox, from the recipient, on a thread of its own, as for a [stand-in](webhooks.md#stand-in)'s decision, naming the assistant, the recipient, the thread and the first line of the message. A decision for a human never takes effect as the human's unconfirmed: the requester's daemon holds an approval decision whose `on_behalf_of` it honours (neither applied nor taken as the chain's) and tells the human it is held and how to answer it, as in [confirming an assistant's acts](#confirming-an-assistants-acts); until then the request waits for the human, a stand-in, the chain or the confirmation, or expires.

#### Confirming an assistant's acts

An assistant's decision on a tool request, sent for its human to the requesting ant's inbox (an `Envelope` with `approval` and `on_behalf_of`), takes effect only with the human's consent, which the human's device signs and the requester's daemon checks, not the assistant's:

- `ants assistant confirm` lists the decisions waiting for the CLI's human (`<ant>/<id>`, the assistant, its verdict, the tool and the time left); `ants assistant confirm ACT` confirms the assistant's decision on request `ACT` (as `ants tools` writes it, a unique prefix accepted), so it takes effect as the human's, and `--deny` drops it, the request waiting for another decision. Only a decision of the assistant the grant names now is answered.
- `ants assistant allow RULE...` adds pre-allow rules, under which a decision of the assistant takes effect unconfirmed, unless it is `allow_always`; `--remove` removes the rules given, `--clear` all of them, and without a rule it lists them. A rule is `<ant>/<rule>` (requests of that ant) or `*/<rule>` (every ant's), `<rule>` an allow rule of the ant's config file ([config.md](config.md)): `*`, a kind, or `<kind>:<pattern>`. A rule covers a request only as far as the request shows its call: a command rule needs the tool's raw input to parse, a path rule absolute paths. Rules stand for the assistant designated when they were signed only.

Both need a human device's credentials, and keep records under the human's own name in KV `ants_humans`, which only the human's credentials write, each replacing the stored one by a compare-and-set as the grant does:

```json
{"human": "<human>", "acts": [{"assistant": "<ant>", "act": {"decision": {"ant": "<ant>", "request_id": "<uuid>", "verdict": "allow_once"}}, "answer": "confirm" | "deny", "until": "<RFC 3339>"}], "issued_at": "<RFC 3339>", "jwt": "<signing device's user JWT>", "signature": "<base64url Ed25519>"}
{"human": "<human>", "assistant": "<ant>", "rules": ["dev-a/execute:cargo test*"], "issued_at": "<RFC 3339>", "jwt": "<signing device's user JWT>", "signature": "<base64url Ed25519>"}
```

The first (`Confirmations`, under `<human>.confirmed`) keeps the human's latest answers, at most 64, each dropped once past `until` (the request's expiry); the second (`PreAllow`, under `<human>.preallow`) at most 64 rules. Their signatures cover `ants-assistant-confirmations`, the human, `issued_at` (RFC 3339, nanoseconds) and the answers as canonical JSON, or `ants-assistant-pre-allow`, the human, the assistant, `issued_at` and the rules as canonical JSON, each on a line of its own. A reader honours one as it honours a grant: the key names the human, `jwt` is a device of the human's under the reader's account, it made `signature`, `issued_at` is at most 5 min ahead and not before the human's newest record the reader saw.

The requester's daemon keeps the latest decision of each assistant held on each request it waits on, from its watch of `ants_humans`. It applies one as soon as the grant still names the assistant and the human's answer to that act (the request, the assistant and the verdict) is `confirm`, or a pre-allow rule covers it; a `deny` drops it. A human's own stored decision coming first wins. Withdrawing or replacing the assistant drops the decisions the daemon holds of it at once. The request's status reports the decision as the human's (`decided`, `by: human`, `decider: <human>`) with `via: <assistant>`, which `ants watch` prints as `by alice via pa`. The CLI reads the same records, unverified, to tell such a request decided: `ants tools list` and `show` and the approvals view show it decided by `pa for alice`, `ants inbox -f` prints a line when a decision is held, and `ants chat` home shows it on the request: `pa chose allow-once for alice: ants assistant confirm dev-a/<id>`.

An assistant still has no right to publish admin orders or decisions on `ants.approvals.*.*` (see [wire ACLs](#wire-acls-and-verification-invariants)), and a decision stored there with `on_behalf_of` decides nothing. The consent records also carry an `order` act (`{"order": {"machine", "ant", "id"}}`) for an [admin order](#admin-orders) an assistant would issue for its human; no daemon applies such an order yet.

#### Tools acting for its human

An assistant's harness sessions get, beside the other [MCP tools](../README.md#mcp-tools), tools that act for its human, while the human's grant, read from the daemon's watch of `ants_humans` and verified as in [assistant grant](#assistant-grant), names the ant. Listing the tools leaves them out otherwise, and each call checks the grant again: a withdrawal or a replacement removes them from the next listing and refuses every call at once (`the grant of alice no longer names pa: nothing is done for alice`), on connections already open too, and a later grant brings them back.

| Tool | What it does for the human |
|---|---|
| `human_queue(thread?)` | Read what the human's home in `ants chat` shows from the last 7 days: questions ants asked it not yet answered, tool requests waiting for its decision or its stand-in's (`<ant>/<id>`, the tool, its options and expiry, and whether the assistant's decision on it is held), and messages to it, not from the assistant, after its read marker, leaving out threads marked done since their last message, at most 30 of each, with their message ids and threads. With `thread`, the messages to and from the human on that thread. |
| `answer_for_human(message_id, answer)` | Answer a question to the human with an option's number or typed text, on the question's thread. |
| `send_for_human(to, text, thread?, plaintext?)` | Send a message to an ant or another human, on `thread` or a new one; no attachments. |
| `react_for_human(message_id, reaction, remove?)` | Add or remove a reaction to a message to or from the human. |
| `title_for_human(thread, title)` | Set the title of a thread of the human's (as `ants thread title`). |
| `done_for_human(thread)` | Mark a thread of the human's done (as `ants thread done`). |
| `decide_for_human(request, verdict)` | Decide a tool request waiting for the human (`allow_once`, `allow_always`, `reject`). |

Each act goes under the assistant's own name with `on_behalf_of` the human: a message (an answer, or a decision, carried by an `Envelope`), a reaction, a title, a done mark. An answer counts as the human's for the question it answers, which takes it out of the queue; the asking ant's daemon tells the human of it, and of a message sent to an ant, as in [acting for its human](#acting-for-its-human). For a message to another human, a reaction, a title and a done mark, which no daemon receives, the tool tells the human itself, with a message in its inbox from the assistant (`[ants] pa, as the assistant of alice, ...`, with no `on_behalf_of`). A decision goes to the requesting ant's inbox, never `ants.approvals.*.*`, and is held there until the human [confirms it](#confirming-an-assistants-acts) or a pre-allow rule covers it.

Sealed messages are read and sent only while the grant [enrolls the assistant as a device](#assistant-as-a-device):

- `human_queue` opens the human's sealed messages for the assistant, and shows `[sealed]` for one it cannot open (no enrollment, or a message sealed before it) and `[forged]` for one failing its sender check.
- `send_for_human` seals a message to a human for the recipient's and the human's devices, refusing without the enrollment; `plaintext: true` sends it unsealed. Its notice to the human never quotes a sealed text.
- `title_for_human` refuses a thread carrying sealed messages: titles are stored in the clear.

The tools need no right beyond the [assistant's](#wire-acls-and-verification-invariants): reads of the `ANTS` stream and `ants_read` marker of its human, puts of `ants_threads` titles and done marks and of its own reactions, and reads of `ants_devices`. An assistant cannot yet issue [admin orders](#admin-orders) for its human: no tool does.

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

On the machine holding the account key, `ants chat` lists the pending requests too, on home and on the credentials page (`c`), where `a` then `y` approves one and `r` then `y` rejects it.

`ants add --enroll` repeats its request every 5 s and stops with an error at a rejection, when the enrollment credentials expire, or when the server refuses them (revoked), even while the server is unreachable.

Behind a leaf node (`~/.config/ants/leaf.conf` or `--leaf-config`), `ants add --enroll` signs the request with the leaf uplink key; `ants enroll approve` renews the leaf uplink JWT (up to the 252 KiB limit, ~50 ants with a domain or 54 without; beyond it the approval fails before anything is signed or recorded), records the ant in `issued.jsonl`, admits it to `ANTS_IN` outbox sourcing, and `ants add` has the leaf node reconnect its uplink with the renewed credentials, by a reload keeping local connections (`ants enroll` behind a leaf requires `ants auth leaf --enroll-approver`). After out-of-band `--jwt` behind a leaf, run `ants auth leaf MACHINE --add-ant NAME` and `ants leaf outbox MACHINE`. See [transport.md](transport.md#enrollment-behind-a-leaf-node).

## Registering humans

A person joins the team as a human without the operator of the account-key machine issuing credentials: `ants login` on the person's machine asks `ants signer` on the hub for a new device's credentials, the signer has the person sign in to GitHub, checks the GitHub user against the team's registration policy, binds the human name to that GitHub user and signs.

1. **Operator, once:** on the account-key machine, issue the public [bootstrap credentials](#bootstrap-credentials) (`ants auth bootstrap`) and share `bootstrap.creds`; give the config file of `ants signer`, which connects to the hub, a [`[registration]`](config.md#human-registration) table naming the team's GitHub OAuth App, the policy and the signer's GitHub token.
2. **Person:** `ants login --hub URL --name NAME --bootstrap bootstrap.creds` generates the device's NKey and [encryption key](#device-encryption-keys) and asks for credentials.
3. **Signer:** starts a GitHub device flow ([RFC 8628](https://www.rfc-editor.org/rfc/rfc8628)) and answers with GitHub's sign-in page and a user code, which `ants login` shows.
4. **Person:** signs in to GitHub in a browser and enters the code, letting the team's OAuth App read memberships and email addresses (scopes `read:org user:email`).
5. **Signer:** reads the GitHub user's numeric ID, login and verified email addresses with the user's token, and the memberships of the policy's organizations and teams with its own token (the user's without one), then drops the user's token: it is never sent or stored. A user the [policy](#registration-policy) admits is signed at once; anyone else waits for a human of the team to approve.
6. **Signer:** checks that the name may be [bound](#github-bindings) to that GitHub user, then signs a human's credentials for the device's NKey, binding its encryption key, for the signer's JetStream domain and valid for the policy's `ttl` (default 30 d). `issued.jsonl` records them with the label, the GitHub user and the grant.
7. **Person:** `ants login` writes `~/.config/ants/auth/NAME.creds` and, when `[cli]` names no credentials yet, points it at them, so `ants` commands act as `NAME`.

`ants auth user NAME --human` still issues a human's credentials by hand, on the account-key machine.

### Bootstrap credentials

```sh
ants auth bootstrap [--ttl 0] [--out FILE]                 # bootstrap.creds (mode 0644; default: no expiry)
ants auth revoke bootstrap --nats wss://hub.example/nats   # withdraw them; reissue with ants auth bootstrap
```

Bootstrap credentials (user `bootstrap`) are public, meant to be shared with whoever may join: they only publish registration requests on `ants.login.*` and hear the answers on `ants.login.*.reply`, with messages of at most 4 KiB, and have no JetStream access, so they read and write nothing stored ([transport.md](transport.md#client-connections)). Holding them admits no one: every registration still needs a GitHub sign-in that the policy or a human admits. They are recorded in `issued.jsonl` (kind `bootstrap`) and revoked like any key. Registration happens on the hub only: leaf uplinks never carry `ants.login` subjects, so both `ants login` and `ants signer` connect to the hub.

### Signing in with `ants login`

```sh
ants login --hub wss://hub.example/nats --name alice [--label laptop] [--store-key] [--bootstrap FILE]
```

`--bootstrap` defaults to `~/.config/ants/auth/bootstrap.creds`. `ants login` makes the device's NKey and encryption key in memory and sends a registration request on `ants.login.<name>`: the name asked for, the NKey's public key, a one-off X25519 reply key, the encryption key, the label, and its making and expiry times (at most 15 min apart), signed by the NKey. Requests are core NATS messages, stored nowhere: `ants login` repeats its request every 5 s until an answer settles it, the request expires (15 min) or Ctrl-C. `ants signer` answers each repeat on `ants.login.<name>.reply` with what it has for that key: the request's public key in the clear, and the answer sealed to the latest reply key, naming the signer's sealing key, so other holders of the bootstrap credentials hear it but cannot open it:

- `verify`: sign in to GitHub at the URL with the user code, before the time given. `ants login` shows them only when the URL is GitHub's device login page (`https://github.com/login/device`); any other URL stops the login without showing it.
- `waiting`: the GitHub user who signed in (login) is outside the policy, and a human of the team decides; `signed` or `refused` follows.
- `signed`: the human's credentials (a user JWT).
- `refused`: why, such as registration not enabled, the signer busy, a key already issued, a name not that GitHub user's to hold, a human's rejection or no decision in time.

The signer runs one GitHub device flow per key, and at most 10 registrations at once (those waiting for a human included), refusing more as busy; each repeat of a key gets the same user code. Requests that are malformed, unsigned by their key, expired, for another subject or unlike the earlier request of their key get no answer. A key whose registration ended gets the same answer until its request expires; another attempt uses a new key.

`ants login` takes signed credentials only when issued to its key, for a human of the name asked, binding its encryption key, by the account of the bootstrap credentials. They go to `~/.config/ants/auth/NAME.creds` (mode `0600`, with the encryption key seed as an `ANTS XKEY SEED` block), replacing an earlier file, whose credentials stay valid until they expire or are revoked. With `--store-key` both seeds go to the [OS credential store](#private-keys-in-the-os-credential-store) instead, and the file naming them is written once they read back and authenticate at the hub (with a warning when the hub does not answer). When `[cli]` in `ANTS_CONFIG` (`~/.ants/config.toml`) names no `creds`, it gets `creds` set to that file and, when it names no `nats`, `nats` set to the hub. A refusal, expiry or Ctrl-C leaves no key and no file behind.

### Registration policy

The [`[registration]`](config.md#human-registration) table of the signer's config file, reread for each request, decides who is signed at once, with the first grant that applies recorded with the device in `issued.jsonl`:

- `org`: an active member of a listed GitHub organization;
- `team`: an active member of a listed team (`org/team`);
- `email-domain`: a user with a verified email address in a listed domain (that exact domain, not its subdomains);
- `approval`: anyone else, once a human of the team allows it.

For a GitHub user outside the policy, `ants login` shows that it waits while the signer asks the humans with an approval request of the reserved user `signer`, on a thread of its own (`register human NAME: GitHub user LOGIN (id N), outside the registration policy`, its input naming the name, GitHub user, label and key), offering `allow_once` or `reject`. It shows in `ants tools`, `ants inbox -f`, `ants dash` and all `ants webhook serve` notification sinks, and any human decides it (`ants tools approve signer/<id>` or `reject`). The signer waits up to its `--approval-timeout` (default `15m`), at most until the request expires: once a human allows it, the device is signed with the grant `approval`, naming that human; a rejection or no decision in time refuses it. At most 3 registrations wait for a human at once (more are refused). Waiting registrations drop on a signer restart, after which the next repeat of the request starts a new GitHub sign-in.

An approval stands for that GitHub user and name: the user's later devices of the name are signed at once, until `ants auth revoke NAME` withdraws it ([Offboarding](#offboarding)).

### GitHub bindings

A name registered with a GitHub sign-in belongs to that GitHub user's numeric ID for good, so a renamed GitHub account keeps its name:

- The first registration signed for a free name binds it. The same GitHub user registering the name again adds a device; another GitHub user asking for it is refused, and so is a GitHub user already bound to another name.
- `human`, the reserved names `bootstrap`, `channel-manager`, `revoked` and `signer`, and names any key other than a human's was issued to (an ant, webhook, leaf node, enrollment, ...) are refused.
- A name holding human credentials but no binding (issued with `ants auth user --human`) is refused until the operator binds it:

```sh
ants auth user alice --human --github octocat [--config FILE]   # bind alice to GitHub user octocat, new device
```

`--github` looks the login up on GitHub (with the GitHub URLs and the signer's token of the `[registration]` table in `--config`, `ANTS_CONFIG` or the default config file when it exists; github.com without a token otherwise) and records its ID with the new device, under the same rules but binding a name that holds human credentials; on a name already bound to the same user it adds a device. A device issued this way has no grant, so `ants enroll renew` renews it like any other; the person's devices registered with `ants login` go through the policy.

The binding is recorded on each device's line in `issued.jsonl` with the grant and the GitHub login (as of the signing or latest renewal), so it is kept across renewals and revocations and in [backups](#backing-up-the-team-keys), and `ants auth devices` shows it ([Devices](#devices)).

### Lifetime and renewal

Registered credentials expire after the policy's `ttl` (default 30 d). Before then, `ants renew` (the `--creds` or `[cli]` credentials by default) renews them [over NATS](#renewing-over-nats), also behind a leaf node. `ants signer`, not `ants enroll renew`, answers the renewal of a device whose latest line in `issued.jsonl` has a grant, with the same `RenewReply` for each repeat, holding the JWT or `refused` with the reason, at which `ants renew` fails:

- `org` or `team` grant: the signer looks the GitHub user up by ID with its own token and checks the memberships of the policy, reread. A user still a member of a listed organization or team is renewed, with the grant the policy now gives and the current login. A user who is no longer one (who left, or whose organization or team the policy no longer lists), or no GitHub user any more, is refused, and every key of the name is revoked and the account pushed. Without a `[registration]` table or token, or when GitHub cannot tell, the renewal is refused alone: try again later. At most 10 checks run at once; more requests wait for their next repeat.
- `approval` grant: renewed while the approval stands; once withdrawn, refused, naming `ants login`.
- `email-domain` grant: refused, naming `ants login`, as only a new sign-in shows the verified email addresses: such a device registers again with `ants login` before its credentials expire.

A renewal keeps the credentials' lifetime, not the policy's current `ttl`. Expired credentials are not renewed: run `ants login` again, which adds a device for the same GitHub user while the policy or a standing approval admits it.

Credentials the signer signs or renews for a registration carry the user JWT tag `ants-registered`, which `ants enroll renew` leaves out, so the holder's renewal hints name the right path: `ants renew`, `ants doctor` and the warnings logged at connect name `ants signer` for them, `ants login` once they expired, and `ants enroll renew NAME` only for credentials without the tag. The tag only guides the holder; the signer goes by `issued.jsonl`.

### Membership sweep

Between renewals, `ants signer` checks the memberships again every `sweep` of the `[registration]` table (default `6h`, the first sweep at start; `"0"` turns it off), for every name with live credentials (neither revoked nor expired) an `org` or `team` grant admitted: it looks the bound GitHub user up by ID with its own token, as a renewal does, one user per second. A user GitHub definitely finds no longer a member of a listed organization or team, or no GitHub user any more, loses every key of the name, as at a refused renewal. A name is kept whenever GitHub cannot tell (an error, a timeout or a rate limit), until the next sweep. Credentials of `approval` and `email-domain` grants, and devices issued with `ants auth user`, are not swept.

- **Off:** without the `[registration]` table, with `sweep = "0"`, without the signer's token (`token_file` or `token_store`), or with a policy listing no organization or team; `ants signer` prints why once and looks again every 10 min.
- **Seeing the policy:** each sweep first checks that the token sees every listed organization and team (`GET /orgs/ORG`, `GET /orgs/ORG/teams/SLUG`); otherwise the sweep is skipped with a message, as GitHub would then report every user a non-member.
- **Rate limits:** the sweep follows GitHub's `x-ratelimit-remaining` and `x-ratelimit-reset` headers, keeping the last 100 requests of each window for registrations and renewals, and waits out a `403` or `429` refusal (`retry-after`, or the window's reset), up to an hour at a time.

Each name revoked by a sweep or a refused renewal is printed, logged, and appended to `offboarded.jsonl` in the team directory (`at`, `name`, `github`, `by`: `sweep` or `renewal`, `reason`, `keys`), the account is pushed, and the revoked devices are recorded in the [directory of device keys](#directory-of-device-keys), as `ants auth revoke` does. A failed push or record names `ants auth push`.

### Offboarding

- **Revoking:** `ants auth revoke NAME` revokes every key of the name, closing its connections once the account is pushed, and withdraws an `approval` grant (the key's line appended again, marked `withdrawn`); revoking one device (`--device`) or key (`--key`) withdraws nothing. The binding stays, so the name remains that GitHub user's, who registers again only through the policy or a new approval. A user still in a listed organization or team, or with a verified email address in a listed domain, is signed again at once: remove the user there, or the entry from the policy, too.
- **Leaving an organization or team:** the next [membership sweep](#membership-sweep) (every `6h` by default), or the first renewal after the user left, whichever comes first, revokes every key of the name an `org` or `team` grant admitted. Without the signer's token, or with `sweep = "0"`, they stay valid until they expire. `ants auth revoke NAME` cuts them off at once.
- **Email domains:** an `email-domain` grant ends with its credentials, as only a new sign-in showing a verified address in a listed domain registers again.
- **Closing registration:** without the `[registration]` table the signer refuses every registration and every `org` or `team` renewal, and sweeps no memberships; revoking the bootstrap credentials keeps requests from reaching it. Registered devices stay valid until they expire or are revoked.

## Renewing over NATS

Valid, unrevoked credentials (ants, humans, webhooks, observers, `channel-manager`, and `leaf-<machine>` uplinks) can renew over NATS without copying files from the auth machine:

```sh
ants renew ~/.ants/dev-a.creds                                                            # target machine
ants renew ~/.config/ants/auth/leaf-macbook.creds --creds ~/.config/ants/auth/alice.creds # leaf uplink via human creds
ants enroll renewals --js-domain hub                                                      # auth machine: list pending
ants enroll renew dev-a --js-domain hub                                                   # sign one (or --auto to sign continuously)
```

`ants renew` generates a local NKey, publishes signed `RenewRequest`s on `ants.renew.<name>` every 5 s, verifies the returned JWT, atomically replaces the `.creds` file (preserving OS credential store placement), and reloads `ants.daemon`, or for leaf uplink credentials reconnects the uplink by reloading `ants.nats` with the `credentials` path respelled in `leaf.conf` (see [transport.md](transport.md#enrollment-behind-a-leaf-node)). For a human's credentials without an encryption key, `ants renew` also generates one and asks for it bound (the request's signed `xkey`), keeping its seed beside the new NKey seed; credentials that hold one keep it. `ants enroll renew` refuses a requested key for credentials of others than humans or already binding one, and preserves role, device, encryption key, domain, webhook destinations, leaf members and TTL while stamping the current permissions version, records `renewed_from` in `issued.jsonl`, and prints `ants auth revoke --key` for the old key. `ants enroll renew --auto` continuously approves valid unrevoked requests (reconnecting every 5 m to refresh `revoked.json`); on the account-key machine, the credentials page of `ants chat` (`c`) signs one with `a` then `y`. Enrollment, leaf system, expired, or revoked credentials must be reissued via `ants auth`.

Credentials a registration granted (a device registered with `ants login`, whose latest line in `issued.jsonl` has a grant) are renewed by `ants signer` instead, while the grant holds, and `ants enroll renew` and `ants enroll renewals` leave them to it (see [Lifetime and renewal](#lifetime-and-renewal)).

## Credentials across upgrades

Each user JWT carries `ants-perms:<n>` (current: `3`), required on every user JWT `ants auth` issues, and live daemons, webhooks, schedules and `ants channel serve` publish their running `ants` and `perms` versions. The version goes up whenever the permissions of a kind of credentials change. Version 3 narrows a human's `ants_humans` writes from every key to the entries and its own `<human>.>` keys ([assistant grant](#assistant-grant)), and adds machine host credentials (with their machine's admin orders, the outcomes of those and the revocation records), humans' rights on `ants_machines`, humans' publishing of admin orders (`ants.admin.*.*`) and, in leaf uplinks, the host of the leaf's machine, and lets an assistant put its own record in `ants_devices` and ants direct-get the assistants' records and the `revoked.*` ones there ([acting for its human](#acting-for-its-human)), and adds the identity records in `ants_identities` ([identities](#wire-acls-and-verification-invariants)); it also drops a human's direct get on `ants_reactions` its no-subject direct get already covers, and, with `--js-domain`, the plain form of a right whose domain form covers it; credentials of version 2 or before keep writing every `ants_humans` key until renewed, and until renewed an assistant's acts for its human read as forged. Connecting with outdated or soon-to-expire (< 7 d) credentials logs a warning with the exact renewal command (credentials newer than the running ants ask for an upgrade of ants); `ants doctor` (**versions**) verifies that all live services run the current permissions version before rights an older version had are withdrawn by renewing. Leaf uplinks whose members' rights changed need renewal and `ants leaf outbox` too.

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

- **Subject-pinned senders:** ants and webhooks publish only under their own final subject token (`ants.inbox.*.<self>`, `ants.notes.*.<self>`, `ants.chan.*.<self>`, `ants.dead.<self>`, `ants.chanadm.*.<self>`, `ants.timeline.*.<self>`, plus `ants.out.*` on leaves). Readers verify payload `from`/target fields against the NATS subject or KV key (`ants watch` and `ants thread` mark mismatches `forged`). Only humans publish under arbitrary inbox senders (`ants dead replay`), write `ants.approvals.<ant>.<id>` (any human may decide any ant's request, so a [stand-in](webhooks.md#stand-in) needs no extra rights; the decision's `by` is informational, not pinned by the subject), send `ants.cancel.*` / `ants.queue.*`, write `ants_humans` (entries, and only their own `<human>.>` keys such as the [assistant grant](#assistant-grant)) / `ants_channels` (with `channel-manager`), or call `STREAM.MSG.GET` / `$JS.API.DIRECT.GET.ANTS`.
- **Scoped reads & key privacy:** stream info on `ANTS`, `ants_sessions`, `ants_attempts`, `ants_owners` and `ants_webhooks` exposes subjects/keys, so only humans hold `STREAM.INFO` there. Ants discover JetStream via `CONSUMER.INFO` on `inbox-<ant>`, pin consumer filters in the create subject (`inbox-<ant>[_1..3]`, `chan-<ant>`, `chanread-<ant>[_1..3]`, `notes-<ant>[_1..3]`, `deadread-<ant>`, `sentread-<ant>`), and read/write single keys via direct gets and plain publishes. Humans direct-get only spend keys (`*.*.spend`, `*.day.*`) in `ants_sessions`, message marks (`*.msg.*`) and `<self>.unserved.*` in `ants_attempts`, and only their own keys in `ants_read` (`<self>`, `<self>.chan`), `ants_presence` (puts; every human's is readable by direct gets and an ordered consumer, while ants read none), `ants_notify`, `ants_devices` (puts; all of it is readable), and `marks.<self>` and `snoozed.<self>` in `ants_threads` (`title.*`, `summary.*` and `done.*` are shared across humans; ants access neither `ants_threads` nor others' `ants_reactions`). An ant's title suggestion travels as `title` on its own reply (inbox message, channel post or notes post), whose subject pins the sender; only a human's CLI stores it, and only on a `title.<thread>` key with no entry, not even a cleared title's delete marker.
- **Reactions for ants:** subjects cannot tie a reaction key (`<reactor>.<message>.<reaction>`) to the messages an ant reads, and the harness can read the ant's `.creds`, so ant credentials neither read nor write `ants_reactions` (human credentials write only their own keys). An ant asks only as itself: on `ants.reactions.<self>` for the reactions to a message, on `ants.reactions.<self>.react` to add or remove its own; it hears the answer on `_INBOX.<self>.reactions`, among its own reply inboxes, which only humans and `ants signer` publish to (adding no subject per ant to a leaf uplink). `ants signer` answers both in queue group `reactions`, with short-lived credentials it issues itself from the account key (adding to those of `ants enroll` stream info and direct gets on `ANTS`, every read and write of `ants_reactions`, and the reply subjects); `ants webhook serve` also answers listings with a `[[human]]` entry's credentials, which cannot write an ant's reaction. Either acts only when the first message stored in `ANTS` with that id (from 5 min before the time in its UUIDv7 to 24 h after, the duplicate window) is on a subject the ant reads: `ants.inbox.<self>.*`, `ants.inbox.*.<self>`, `ants.notes.>` or `ants.chan.>`. Senders choose ids, but no one learns an id before its message is stored, so a later copy stored under a readable subject grants nothing; any other message gets the same `unknown` answer as a missing one, and no change. A daemon without credentials, on an open server where every connection reads and writes everything, answers its ants in-process with the same check.
- **Attachments (`ants_files`):** a message carries only references (`attachments: [{id, name, mime, size, sha256}]`); the bytes sit in stream `OBJ_ants_files` as chunks on `$O.ants_files.C.<from>.<kind>.<target>.<id>.<n>`, where `<kind>.<target>` (`inbox.<to>`, `notes.<thread>` or `chan.<channel>`) follows the subject of the carrying message, so subject permissions confine them as they confine the messages. Every user writes chunks only under its own name (`$O.ants_files.C.<self>.>`, plus `ants.out.files.<self>.>` for a leaf outbox), each with `Nats-Expected-Last-Subject-Sequence: 0` when written directly, so stored bytes are never replaced, and with `Nats-Msg-Id` `<handle>.<n>`, so a copy sourced from a leaf outbox within a day is dropped as a duplicate ([transport.md](transport.md#store-and-forward)); the stream denies deletes and purges. Ants read by direct gets only `…C.*.inbox.<self>.>`, `…C.<self>.inbox.>`, `…C.*.notes.>` and `…C.*.chan.>`, with no stream info or consumer; humans read all, and alone create the store and update its sources (`ants leaf outbox`; an enrollment approver's uplink and `ants signer` too, on admitting a new ant). No Object Store metadata subjects are used, as their base64 names cannot be scoped. Readers check the bytes against the reference's size and SHA-256. Webhooks, observers, the channel manager and enrollment credentials get no access.
- **First-writer `ants_owners`:** written only by humans and ant-targeting webhooks (`<thread>.<ant>`, `Nats-Msg-Id: owner.<key>`). The stream enforces `discard_new_per_subject` with a 30 d duplicate window and denies deletes/purges, so first owner wins while outbox replays drop cleanly without stalling sourcing.
- **Assistants:** `Permissions::for_assistant` gives an assistant an ant's rights plus what a human reads and keeps of its own, under its own name: it watches `ants.>`, reads `ANTS` (stream info, `STREAM.MSG.GET`, direct gets) and the buckets a human reads, titles threads and marks them done, keeps its own reactions, marks, snoozes and read markers, and reads every attachment and the directory of device keys. Of its human's state it only reads, by direct gets: the read markers (`<human>`, `<human>.chan` in `ants_read`), `marks.<human>` and `snoozed.<human>` in `ants_threads`, and `<human>.unserved.*` in `ants_attempts`. It sends only as itself, so its acts are never indistinguishable from its human's: what it does for its human names the human in `on_behalf_of`, which readers honour only as in [acting for its human](#acting-for-its-human), and it puts only its own record in `ants_devices` (`<assistant>.assistant`). It gets none of a human's rights whose subject does not name who acts, until readers can tell an assistant's act and its human's confirmation apart: deciding approvals (`ants.approvals.*.*`), `ants.cancel.*` / `ants.queue.*`, withdrawing queued messages (`*.msg.*` in `ants_attempts`), recording owners in `ants_owners`, publishing dead letters, answering reactions or renewal requests. Nor does it change the team: no writes to `ants_humans` (where grants stand) or `ants_channels`, no stream or bucket created or updated, no message deleted, no device's rights (`ants_devices` puts but its own record, `ants_presence` puts, `ants_notify`), no human's inbox, nothing of the account key.
- **Admin orders:** only humans publish `ants.admin.<machine>.<ant>`, for any machine and ant; ants, assistants, webhooks and observers send none. A machine's host credentials take only consumer `admin-<machine>` and write only outcomes of orders to their machine (`$KV.ants_machines.<machine>.>`). Humans read outcomes by batched direct gets and write none. The daemon trusts no sender for the role: it verifies the admin's signature, credentials, tag and revocation itself ([Admin orders](#admin-orders)). Any human may publish an order, so a human without the role can only make a daemon record refusals.
- **Identities (`ants_identities`):** an ant's daemon, under the ant's credentials (an assistant's too), puts and updates only its own record (`$KV.ants_identities.<ant>`): it creates the record when absent on start and counts each prompt it completes by compare-and-set, so a standby daemon taking over keeps the count. Ants, assistants and humans read every record by direct get (an ant reading the others' avatars costs one right where a subject per ant would not fit a leaf uplink); records carry no secrets. Humans put any record (`$KV.ants_identities.*`, for `ants ant avatar`) and create the bucket; `ants init` does, as ants may neither create nor look it up. `ants auth revoke ANT` deletes the record with the revoker's credentials. Before permissions version 3, credentials have none of these rights: such an ant keeps no record and shows the avatar of its name.
- **Read-only observers:** `Permissions::for_observer` allows only durable consumers `observe-{requests,decisions,dead}-<observer>` on `ANTS`, `ants.status.*.*`, `ants_roster` reads/watches, and self-renewal (see [hooks.md](hooks.md)).
- **Metadata verification:** daemons honour `owner` only for `human` or `ants_humans` members backed by channel posts or `ants_owners` (plus verified `origin_thread` chains in `ants_links`, max 32 hops), honour `webhook: true` only with `<sender>.webhook` in `ants_webhooks` (or from a human), and honour `on_behalf_of` only from the assistant the human's standing grant names whose record in `ants_devices` carries the human's `ants-assistant-of:` tag ([acting for its human](#acting-for-its-human)), never applying a decision made for a human without the human's signed confirmation or pre-allow rule ([confirming an assistant's acts](#confirming-an-assistants-acts)).
