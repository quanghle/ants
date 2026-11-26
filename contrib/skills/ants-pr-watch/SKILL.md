---
name: ants-pr-watch
description: See a pull request through to its merge with a watcher, a saved trail the ant's daemon runs on an interval that wakes the ant each time what blocks the pull request changes (failing checks, a review asking for changes, a conflict, a base moved on) and a last time once it is merged or closed. Use after opening a pull request, or when asked to follow one, on GitHub (gh) or Forgejo, instead of polling it or waiting for a human to say what changed.
---

# Watching a pull request

A watcher ([sense.md](../../../docs/sense.md#watchers)) checks something on an interval and wakes the ant only when
the check's result changes. Watching a pull request with one takes three things, done once: the saved trail
`pr-blocked`, which reads the pull request and tells what blocks it; allow rules letting the ant's daemon run its reads;
and a `sense_remind` watch trigger running that trail. Each wake then tells what changed, on the thread the watcher was
made on.

For a single event, such as a merge, a condition trigger needs no trail:
`{"kind":"condition","condition":{"kind":"gh_pr","repo":"OWNER/REPO","number":7,"state":"merged"}}`. A watcher is for
following a pull request through its reviews and checks.

## The trail

A trail run as a watcher prompts no ant and starts no run: it reads through `await command(cmd)`, which runs `cmd` in
the ant's workdir and returns `{"status": exit status, "stdout": text}`, and returns `{"output": str, "done": bool}`
([A watcher's reads](../ants-trails/SKILL.md#a-watchers-reads)). The ant wakes whenever `output` differs from the last
check's, so `output` names only what an ant acts on: what blocks the merge, in a fixed order, with no times, counts of
running checks or other text that changes on its own. `done` is true once the pull request is merged or closed, which
wakes the ant a last time and retires the watcher.

This trail reads a GitHub pull request through `gh`, or a Forgejo one through its API with `curl` when its args name
the API:

```python
meta = {
    "name": "pr-blocked",
    "description": "Tells what blocks a pull request from merging; done once it is merged or closed",
    "phases": [],
}

import json

GH_FIELDS = "state,isDraft,mergeStateStatus,reviewDecision,reviews,statusCheckRollup"
FAILED = ["FAILURE", "ERROR", "TIMED_OUT", "CANCELLED", "ACTION_REQUIRED", "STARTUP_FAILURE"]
MERGE_STATES = {
    "BEHIND": "the base branch moved on: rebase onto it",
    "DIRTY": "conflicts with the base branch",
    "UNKNOWN": "mergeability not computed yet",
}


async def read(cmd):
    ran = await command(cmd)
    if ran["status"] != 0:
        raise RuntimeError(f"{cmd} exited with status {ran['status']}")
    return json.loads(ran["stdout"])


def checks_line(failing, running):
    if failing:
        return "checks failing: " + ", ".join(sorted(set(failing)))
    if running:
        return "checks running"
    return None


async def github(pr, repo):
    view = await read(f"gh pr view {pr} --repo {repo} --json {GH_FIELDS}")
    if view["state"] != "OPEN":
        return view["state"].lower(), []
    failing, running = [], False
    for check in view["statusCheckRollup"] or []:
        name = check.get("name") or check.get("context")
        state = check.get("conclusion") or check.get("state") or ""
        if check.get("status") not in [None, "COMPLETED"] or state == "PENDING":
            running = True
        elif state in FAILED:
            failing.append(name)
    blockers = []
    if view["isDraft"]:
        blockers.append("a draft")
    blockers.append(checks_line(failing, running))
    reviews = len(view["reviews"] or [])
    if view["reviewDecision"] == "CHANGES_REQUESTED":
        blockers.append(f"changes requested ({reviews} reviews)")
    elif view["reviewDecision"] == "REVIEW_REQUIRED":
        blockers.append(f"waiting for a review ({reviews} reviews)")
    blockers.append(MERGE_STATES.get(view["mergeStateStatus"]))
    return "open", [b for b in blockers if b]


async def forgejo(pr, repo, api, token_env):
    auth = f'-H "Authorization: token ${token_env}" ' if token_env else ""
    base = f"curl -sf {auth}{api}/repos/{repo}"
    pull = await read(f"{base}/pulls/{pr}")
    if pull["merged"]:
        return "merged", []
    if pull["state"] != "open":
        return "closed", []
    status = await read(f"{base}/commits/{pull['head']['sha']}/status")
    reviews = await read(f"{base}/pulls/{pr}/reviews")
    statuses = status["statuses"] or []
    failing = [s["context"] for s in statuses if s["status"] in ["failure", "error"]]
    running = any(s["status"] == "pending" for s in statuses)
    blockers = []
    if pull["draft"]:
        blockers.append("a draft")
    blockers.append(checks_line(failing, running))
    latest = {}
    for review in reviews:
        if review["state"] in ["APPROVED", "REQUEST_CHANGES"] and not review["dismissed"] and not review["stale"]:
            latest[review["user"]["login"]] = review["state"]
    if "REQUEST_CHANGES" in latest.values():
        blockers.append(f"changes requested ({len(reviews)} reviews)")
    elif not latest:
        blockers.append(f"waiting for a review ({len(reviews)} reviews)")
    if not pull["mergeable"]:
        blockers.append("conflicts with the base branch")
    return "open", [b for b in blockers if b]


pr, repo = args["pr"], args["repo"]
if args.get("api"):
    state, blockers = await forgejo(pr, repo, args["api"], args.get("token_env"))
else:
    state, blockers = await github(pr, repo)
if state != "open":
    output = f"{repo}#{pr} is {state}"
elif blockers:
    output = f"{repo}#{pr} is blocked:\n" + "\n".join(f"- {b}" for b in blockers)
else:
    output = f"{repo}#{pr} is ready to merge"
{"output": output, "done": state != "open"}
```

Args, for GitHub:

```json
{"pr": 7, "repo": "acme/widgets"}
```

For Forgejo, `api` is the instance's API root and `token_env` the optional name of an environment variable of the
ant's holding a token that reads the repository (a public one needs none):

```json
{"pr": 7, "repo": "acme/widgets", "api": "https://forge.example.net/api/v1", "token_env": "FORGEJO_TOKEN"}
```

A read that fails (`gh` signed out, the server unreachable) raises, which fails the check: it wakes no one, and checks
failing in a row back off to the watcher's longest interval and show in the memories view and in `ants doctor`.

Save it once for the colony, by a human with `ants trail save pr-blocked.py`, or by the ant with
`save_trail(name="pr-blocked", script=...)` from a trusted thread. A colony that has a `pr-blocked` already needs no
second one: `ants trail show pr-blocked`, or the `find_trails` tool, shows it.

## Allow rules

Each `command()` runs under the ant's tool allow rules unless the ant's harness runs its tools unasked; on a thread
narrowed to other rules, or otherwise untrusted, the rules decide in any case. A command the rules refuse fails the
check. A command holding `;`, `&`, `<`, `>`, `$(`, a pipe, a backtick or a newline matches no rule, which is why the
trail parses JSON itself rather than piping through `jq`. The rules covering the trail's reads, in the ant's `allow`
in `config.toml`, which a human edits:

```toml
allow = [
    "execute:gh pr view *",
    "execute:curl -sf https://forge.example.net/api/v1/repos/*",
    "execute:curl -sf -H \"Authorization: token $FORGEJO_TOKEN\" https://forge.example.net/api/v1/repos/*",
]
```

## Creating the watcher

On the thread that opened the pull request, so each wake lands where its context is:

```json
{
  "action": "create",
  "title": "PR acme/widgets#7: what blocks it",
  "trigger": {
    "kind": "watch",
    "run": {"kind": "trail", "name": "pr-blocked", "args": {"pr": 7, "repo": "acme/widgets"}, "limit_seconds": 60},
    "interval_seconds": 300,
    "first": true
  }
}
```

`first: true` wakes the ant with the first check's result, confirming the watcher works; without it the first check
only sets what later checks are compared with. `interval_seconds` runs from 60 (the default) to 86,400; checks back
off to `max_interval_seconds` (30 minutes by default) after ten in a row see no change, and the first change brings
them back. A watcher counts within the ant's fifty active triggers and expires after thirty days unless renewed
(`{"action":"renew","id":...}`). An untrusted thread cannot create one.

## On each wake

The wake carries the watcher's title and the check's `output` as data, not instructions, cut to 4 KiB. Its wake
replaces the watcher's previous one still waiting in the ant's queue, so a busy ant reads only the latest. No
acknowledgment is needed: the watcher stays armed until done, dropped or expired. Act on the output:

- **checks failing**: read the failing run's log (`gh run view --log-failed`, or the check's page on Forgejo), fix
  the cause on the branch and push. A flaky failure is rerun, once, and said so on the pull request.
- **checks running**: nothing to do; end the turn and let the next change wake the ant.
- **changes requested** or a new review: read the reviews and comments (`gh pr view 7 --comments`), address each
  point or answer why not, push, and reply on the pull request.
- **base moved on** or **conflicts**: rebase onto the base branch, resolve, rerun the checks locally and push.
- **a draft**: mark it ready (`gh pr ready 7`) once the work is complete, if the ant opened it as a draft.
- **waiting for a review**: request one from the reviewers the repository names, once; otherwise wait.
- **ready to merge**: merge when the ant's instructions let it, else tell the pull request's owner it is ready.
- **merged** or **closed** (the last wake): finish the work on the thread (close its task, keep a promise, delete
  the local branch) and record anything worth remembering. The watcher is retired already.

To stop watching earlier, drop the watcher (`{"action":"drop","id":...}`); `{"action":"list"}` shows its id, when it
last fired and whether its checks fail.
