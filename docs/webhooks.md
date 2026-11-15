# Webhooks and notifications

`ants webhook serve` listens on plain HTTP (`127.0.0.1:8424` by default, behind a TLS reverse proxy), turning `POST <path>/<webhook>` requests into new inbox or channel threads and pushing human notifications (`~/.ants/webhooks.toml` or `--config FILE`).

## Webhook ingress

```toml
listen = "127.0.0.1:8424"                # overridden by --listen
metrics = "127.0.0.1:9465"               # serve /healthz and /metrics (default: off); overridden by --metrics
path = "/hooks"                          # POST /hooks/<name>, GET/POST /hooks/_approval
nats = "nats://127.0.0.1:4222"           # overridden by --nats / ANTS_NATS_URL
js_domain = "hub"                        # overridden by --js-domain / ANTS_JS_DOMAIN

[[webhook]]
name = "github"
creds = "~/.config/ants/auth/github.creds"    # ants auth user github --webhook --to dev-a
auth = "hmac-sha256"                          # signature_header defaults to X-Hub-Signature-256 ([sha256=]<hex>)
secret_file = "~/.ants/github.secret"         # mode 0600; or secret_env = "VAR"
id = "header:X-GitHub-Delivery"               # or "json:/pointer"; default "body-sha256"
ant = "dev-a"
owner = "alice"                               # optional human owner for replies and escalations
text = "GitHub {header:X-GitHub-Event} on {json:/repository/full_name}\n\n{body}"
allow = ["read:*", "execute:gh pr view*"]     # optional thread tool rules; default read-only, ["*"] opts out

[[webhook]]
name = "alerts"                               # ants auth user alerts --webhook --channel ops
creds = "alerts.creds"
auth = "bearer"                               # Authorization: Bearer <secret>; or auth = "token" + token_header
secret_env = "ANTS_ALERTS_TOKEN"
id = "json:/alert/id"
channel = "ops"
mention = ["dev-a"]                           # optional; or broadcast = true for all current members

[[webhook]]
name = "gitlab"                               # ants webhook add gitlab --to dev-a --preset gitlab --signing-token
creds = "gitlab.creds"
auth = "standard-webhooks"                    # webhook-id, webhook-timestamp, webhook-signature (v1,<base64> ...)
secret_file = "gitlab.secret"                 # the signing secret: whsec_<base64>, such as GitLab's signing token
tolerance = "5m"                              # how far webhook-timestamp may be from now (default 5m, 1s to 1d)
id = "header:webhook-id"                      # the default with standard-webhooks
ant = "dev-a"
```

- **Auth & HTTP responses:** `hmac-sha256` (`signature_header`), `bearer` (`Authorization: Bearer <secret>`), `token` (`token_header`, e.g. `X-Gitlab-Token`) and `standard-webhooks` compare secrets and signatures in constant time (`401` on mismatch). Limits: `256 KiB` body (`413`), `15s` request timeout, `64` concurrent requests (`503` with `Retry-After: 5` when overloaded or NATS cannot store within 5 s; nothing buffers locally). Returns `202` (`{"thread": "...", "duplicate": false}`) when stored, `200` (`"duplicate": true`) on repeat, `400` bad JSON/ID, `404` unknown hook, `405` non-`POST`, `415` non-UTF-8 `{body}`.
- **Standard Webhooks:** `auth = "standard-webhooks"` checks [Standard Webhooks](https://www.standardwebhooks.com) signatures, such as those of a GitLab (19.0+) signing token: `webhook-signature` lists space-separated `v1,<base64>` HMAC-SHA256 signatures of `<webhook-id>.<webhook-timestamp>.<body>`, keyed with the base64 of the secret after `whsec_` (the prefix is optional), and any one matching passes; other versions (`v1a`) are ignored. A missing `webhook-id` or `webhook-signature`, or a `webhook-timestamp` (Unix seconds) further than `tolerance` before or after now, answers `401` before the body is read. `id` defaults to `header:webhook-id`, so a request replayed within the tolerance is a duplicate.
- **Deduplication & marker:** deduplicates for 7 d via `<webhook>.<hash>` in `ants_webhooks` plus 24 h `Nats-Msg-Id: webhook.<webhook>.<hash>`. Publishes `<webhook>.webhook` (`{machine, version, perms}`) on connect and hourly.
- **Templates & untrusted input:** `text` substitutes `{body}`, `{id}`, `{webhook}`, `{header:Name}` and `{json:/pointer}` (`{{`/`}}` escape braces). Mentions come strictly from `mention` or `broadcast`, never from payload text. Threads are narrowed to `allow` (default read-only; see [config.md](config.md#tool-permissions)) until a human messages the ant on the thread; the [built-in harnesses](config.md#harnesses) run their own tools unasked, so narrowing does not reach those tools (`ants doctor` warns). Replies go to `owner` if set (else logged); webhook posts never reset turn budgets.
- **Reload:** reloads on `SIGHUP` or file change (polled every 3 s, applied once stable across two checks), restarting only changed/added/removed `[[webhook]]` and `[[human]]` workers while keeping in-flight HTTP requests alive. `listen` and `metrics` require a restart.

## Adding a webhook

`ants webhook add` issues credentials (locally via `--dir`, or over NATS via `--enroll CREDS [--hub URL]` + `ants enroll approve NAME --webhook ...` on the hub, never through a leaf), writes a 64-hex secret to `<name>.secret` (`0600`), appends `[[webhook]]` to `webhooks.toml`, reloads `ants.webhook` / `ants-webhook`, and prints the URL and secret:

```sh
ants webhook add github --to dev-a --preset github --js-domain hub
ants webhook add alerts --channel ops --preset grafana
ants webhook add gitlab --to dev-a --preset gitlab --signing-token
```

`--secret-file FILE` (its first line; `-` for standard input) or `--secret-env VAR` takes the secret from there instead of making a new one, copied to `<name>.secret`. `--preset gitlab --signing-token` checks GitLab's signing token (`auth = "standard-webhooks"`, `id = "header:webhook-id"`) rather than a secret token. GitLab generates that token (Settings > Webhooks > Generate signing token, shown once): without `--secret-file` or `--secret-env`, the command prints the URL to set in GitLab and reads the pasted token from the terminal without echo. A token that is no `whsec_<base64>` is refused before anything is written.

| `--preset` | `auth` | Header | `id` | `text` |
|---|---|---|---|---|
| (none) | `bearer` | `Authorization: Bearer <secret>` | `body-sha256` | `[webhook {webhook}] {id}\n\n{body}` |
| `github` | `hmac-sha256` | `X-Hub-Signature-256` | `header:X-GitHub-Delivery` | `GitHub {header:X-GitHub-Event} on {json:/repository/full_name}\n\n{body}` (use `application/json`) |
| `gitlab` | `token` | `X-Gitlab-Token` | `header:Idempotency-Key` (17.4+) | `GitLab {header:X-Gitlab-Event} on {json:/project/path_with_namespace}\n\n{body}` (secret token) |
| `gitlab --signing-token` | `standard-webhooks` | `webhook-signature` | `header:webhook-id` (19.0+) | as `gitlab` |
| `grafana` | `hmac-sha256` | `X-Grafana-Alerting-Signature` | `body-sha256` | `Grafana: {json:/title}\n\n{json:/message}` |

## Notifications

`[[human]]` entries in `webhooks.toml` push pending tool requests, inbox escalations (including reminders, expired approvals, and unserved-ant notices), and optional reply completions:

```toml
public_url = "https://hub.example/hooks"      # enables signed approve/reject links at <public_url>/_approval
action_secret_file = "~/.ants/action.secret"  # HMAC key, 16+ chars, mode 0600; or action_secret_env
timezone = "Europe/London"                    # default IANA zone for quiet hours (default UTC)

[[human]]
name = "alice"
creds = "~/.config/ants/auth/alice.creds"     # ants auth user alice --human
ants = ["dev-a", "dev-b"]                     # optional filter for tool requests (default: all ants)
approvals = true                              # default true
escalations = true                            # default true (also enables unserved-ant notices)
completions = true                            # ants' replies to alice, but for TL;DRs and digests (default false)
quiet = "22:00-07:00"                         # optional local quiet window (start inclusive, end exclusive)
timezone = "America/New_York"                 # optional per-human zone (requires `quiet`)
bypass = ["approvals"]                        # optional kinds bypassing quiet hours and snooze: approvals, escalations
digest = true                                 # one digest of the held notices when quiet hours or snooze end (default false)
stand_in = "bob"                              # optional human relayed what waits for alice while away
```

- **Delivery & presence:** each human reads shared durable consumer `notify-<human>` on `ANTS` (deliver new, explicit ack), so multiple `ants webhook serve` instances send each notice once (skipping decided/expired/forged requests and escalations > 24 h old). Sinks with `device = "<id>"` (`ants auth devices alice`) skip notices while that device records active presence in `ants_presence` (`ants tools list -f`, `ants dash`, `ants chat`, or `ants inbox -f` without `--thread`/`--json` for approvals; `ants chat` or `ants inbox -f` without `--thread` for escalations and completions). Unreadable presence defaults to away.
- **Reactions for ants:** each `[[human]]` connection also answers ants' `list_reactions` requests (`ants.reactions.<ant>`, queue group `reactions` shared with `ants signer`, so one process answers each), since ant credentials do not read `ants_reactions`. It lists the reactions only for a message the asking ant reads; otherwise it answers `unknown` (see [auth.md](auth.md#wire-acls-and-verification-invariants)). Requests to add or remove an ant's reaction (`react`) go to `ants signer` alone: human credentials write only the human's own reactions.
- **Sealed messages:** a notice about a [sealed message](auth.md#sealed-messages-between-humans) names its sender and thread and says only `a sealed message arrived`, with none of its text, title or attachments.
- **Test:** `ants webhook notify-test HUMAN [--config FILE]` sends a test notice to all sinks of `HUMAN` (ignoring presence and holds, without NATS), prints `ok`/`fail` per sink without leaking URLs or secrets, reports quiet-hours status, and exits `1` on any failure.

### Sinks

All sinks run concurrently within `timeout` (`1s`–`30s`, default `10s`); failures log only the sink label (`ntfy topic T`, `slack`, `telegram chat C`, `exec PROGRAM`, `http HOST`).

| `kind` | Config keys | Payload & actions |
|---|---|---|
| `ntfy` | `topic`, optional `url` (default `https://ntfy.sh`), optional `secret_file`/`secret_env` (Bearer) | JSON publish (`priority: 4` for approvals); one-tap HTTP `POST` action buttons |
| `slack` | `secret_file` or `secret_env` (incoming webhook URL; `url` rejected) | `mrkdwn` section + URL buttons opening `/_approval` confirm page |
| `telegram` | `chat`, `secret_file`/`secret_env` (bot token), optional `url` (default `https://api.telegram.org`) | `sendMessage` (previews off) + inline URL buttons opening confirm page |
| `exec` | `command = ["prog", ...]` (no shell; secrets rejected) | 1-line [JSON notice](#json-notice) on stdin; stdout dropped, stderr logged; `SIGKILL` on timeout |
| `http` | `url` (no user/pass/query), `secret_file`/`secret_env` (16+ chars) | `POST` [JSON notice](#json-notice) with `X-Ants-Timestamp: <unix_s>` and `X-Ants-Signature: sha256=<hex>` over `<timestamp>.<body>` |

### Quiet hours, snooze and muted threads

Held notices for approvals, escalations and completions are dropped (logged as `held back a notice`, leaving requests pending and inbox messages unread); unserved-ant notices (`unserved`) wait until quiet hours or snooze end. `bypass` never overrides a muted thread.

- **Digest:** with `digest = true`, each approval, escalation or completion held back by quiet hours or a snooze (not by a mute) is counted per kind in KV `ants_notify` under `held.<human>` (`{since, counts}`), with compare-and-set by whichever instance took it. Once neither quiet hours nor a snooze hold (checked every minute and before the next notice sent, so `ants notify resume` ends one early), one instance claims the count by deleting it at the revision it read and sends a single `digest` notice to every sink: the counts per kind since the first held notice, and the tool requests still pending (up to 10 in the text, all in the [JSON notice](#json-notice)). The count survives restarts and is shared by every instance listing the human, which should all set `digest` alike; a digest is sent at most once, a lost one is not retried, and nothing is marked read or decided.

- **Snooze:** `ants notify snooze DUR` (`1s`–`365d`), `ants notify resume`, `ants notify status [--json]`.
- **Thread mute:** `ants thread mute THREAD [--for DUR]` (default `30d`) and `ants thread unmute THREAD`.

Runtime holds live in KV `ants_notify` under `<human>` (`{snoozed_until, muted}`), updated via CAS across devices and read by direct get before each notice; the digest's count lives beside them under `held.<human>`.

### Stand-in

A human counts as away when quiet hours or a snooze hold back a notice and no `device` of its sinks follows what the notice is about (presence as above). While away, `ants webhook serve` relays to the human's stand-in:

- pending tool requests waiting for the human (`ApprovalRequest.human`: the thread's human owner, else the default human), and
- escalations an ant sent to the human's inbox, reminders included (not expired-request notices, not muted threads).

The stand-in is the one the ant's daemon stamps on the request or escalation (`stand_in` of its `[[ant]]`, see [config.md](config.md)), else the human's own `stand_in`; it must be a registered human other than the human away, otherwise nothing is relayed. Dead-letter, harness and storage escalations carry no ant stand-in. The relay is an inbox message from the human away to the stand-in, on the escalation's or request's thread, carrying the escalation (an `approval` escalation for a request) with `away = {human, ant}`; it is stored once per notice (`Nats-Msg-Id: standin-<human>-<seq>`), never relayed again, and pushed to the stand-in's own sinks as a notice titled `for <human> (away): …` (subject to the stand-in's own holds; a relayed tool request is skipped when the stand-in is notified of that ant's requests anyway). Relayed notices carry no approve/reject links: the text gives `ants tools approve|reject <ant>/<id>` or `ants ask --thread T @<ant> TEXT`.

The stand-in answers with the same rights any human has: the first stored decision on a tool request wins, and a human's message on a thread ends its escalation. The human away is then told in its inbox: the ant's daemon sends `<stand-in> decided tool request <ant>/<id> … in place of <human>` when the decision names (`ApprovalDecision.by`) another human than the one the request waited for, and `<stand-in> answered the escalation of <ant> … in place of <human>` when another human answers a pending escalation or reminder. Only `ants webhook serve` decides who is away, so relays need the human away listed as a `[[human]]` of a running notifier. A relayed notice still counts in the human's own [digest](#quiet-hours-snooze-and-muted-threads) (with `digest = true`), which lists the tool requests still pending when the hold ends: those the stand-in decided no longer appear.

### Approve and reject links

With `public_url` and `action_secret_*`, approval notices include `<public_url>/_approval?ant=&id=&seq=&verdict=&by=&exp=&sig=` per offered verdict (`Approve`, `Always allow`, `Reject`). `GET` renders a confirmation page (safe against link previews); `POST` verifies HMAC-SHA256 and expiry, then writes the `ApprovalDecision` using the human's credentials (`200` ok, `400` malformed, `403` bad signature, `404` unknown human/request, `405` method, `409` already decided/expired, `410` expired link, `503` hub unreachable). Keep sink channels private (ntfy topics are public unless token-gated).

### JSON notice

`exec` and `http` sinks receive schema `version: 1` (additive fields/kinds keep `version: 1`; breaking changes increment `version`; ignore unknown `kind`s):

| Field | Kinds | Meaning |
|---|---|---|
| `version` | all | `1` |
| `kind` | all | `approval`, `escalation`, `completion`, `unserved`, `digest`, or `test` |
| `id` | all | Request ID, message ID, first waiting message ID, or fresh digest or test UUID (deduplicate on `(kind, id)`) |
| `human`, `title`, `text`, `urgent`, `sent_at`, `links` | all | Target human, formatted notice, urgency (`true` for `approval`), RFC 3339 UTC timestamp, `{verdict, label, url}[]` |
| `ant`, `thread` | `approval`, `escalation`, `completion` | Source ant and thread ID to answer on |
| `request`, `seq`, `tool`, `options`, `expires_at` | `approval` | `<ant>/<id>`, `ANTS` sequence, `{kind, title, locations?, input?}`, verdicts offered, RFC 3339 expiry |
| `reason`, `stuck`, `level` | `escalation` | Escalation reason, stuck ant, chain level (`0` = direct) |
| `stand_in_for` | `escalation` | Present on a [stand-in](#stand-in) relay: the human away; `ant` is then the ant to answer |
| `ant`, `messages` | `unserved` | Unserved ant and waiting message UUIDs |
| `since`, `counts`, `pending` | `digest` | RFC 3339 time of the first held notice, `{kind: n}` held back per kind, `{request, ant, thread, title, expires_at}[]` of the tool requests still pending (`urgent` when any) |
