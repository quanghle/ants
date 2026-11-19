# Trails

A trail is a Python script that coordinates several ants and humans over time. A conductor, the ant a run is started
on, has its daemon run the script; each `agent()`, `human()` or `trail()` call of the script is a step (a message the
conductor sends on a fresh thread, or a child run of a saved trail), and the run waits for its answer without a
process, surviving restarts. The script's code, not a model, decides which ant takes each step, how many steps run at
once and how their results merge. A run may go on in another trail at one of its waypoints (`goto()`), so small
saved trails compose into routes at run time.

This document says what a trail is, how runs start, route, report, stop and resume, what they may spend and trust, and
where they are kept. How to write one is in the authoring guide,
[contrib/skills/ants-trails/SKILL.md](../contrib/skills/ants-trails/SKILL.md); how each part works is in
[architecture.md](architecture.md#trail-workers) (workers) and [architecture.md](architecture.md#trails) (the
daemon's part), and every command and setting in [cli.md](cli.md#commands) and [config.md](config.md#trails).

## Trails, skills and sense

- **A skill** tells one ant how to do something: instructions the ant follows turn by turn, its intermediate results in
  one context window.
- **Sense** is an ant's memory ([sense.md](sense.md)): its identity, memories and work, learned by the ant itself. No
  trail enters it.
- **A trail** is a script a conductor's daemon runs, coordinating several ants and humans over time: its plan lives in
  code, its intermediate results in script variables, each step in a clean context. Trails are colony-wide, written
  by humans or ants, and handed to ants, never learned.

Write a trail when work splits into many steps that each deserve a fresh context, when findings need a verifier other
than their author, when discovery must repeat until it runs dry, or when a human must approve a step mid-run. For a
task one ant finishes on one thread, send a message instead.

## The script

A trail is a `.py` file in [Monty](https://github.com/pydantic/monty)'s subset of Python whose first statement is a
literal `meta`, read without running the script:

```python
meta = {"name": "review", "description": "Reviews each changed file", "phases": ["review", "report"]}

phase("review")
reviews = await parallel(*[agent(f"Review {path} in {args['repo']}.", capability="rust") for path in args["files"]])
phase("report")
{"reviews": reviews}
```

The script runs top to bottom with `await` allowed at the top level; the value of its last expression, which must be
JSON, is the run's result (a trail with [waypoints](#waypoints) returns it from one). It reaches the host only through
these names:

| Name | What it does |
|---|---|
| `await agent(prompt, ant=None, capability=None, schema=None, label=None)` | One step: `prompt` to the ant `ant` names, to an ant other than the conductor advertising `capability` (picked as `route` picks one), or to the conductor itself. Returns the reply's text, or with `schema` (a JSON Schema) its JSON, asked for again at most twice on a mismatch. |
| `await human(questions)` | One step: asks the run's owner, else the default human, 1 to 4 questions as `ask_human` does; returns the answer's text. |
| `await trail(name, args=None)` | One step, answered by a child run of the saved trail `name` with `args` as its input; returns its result ([Child runs](#child-runs-trail)). |
| `await goto(trail=None, at=None, state=None)` | Ends the leg where called and goes on in `trail` (this one without it) at waypoint `at` (its start without it) with `state` as `args`; returns only by raising ([Routes](#routes-goto)). |
| `phase(title)`, `log(text)`, `print()` | Notes on the run's thread; never wait. |
| `args` | The leg's JSON input (`None` without): the run's input on its first leg, the goto's `state` on later ones. |
| `await parallel(*coroutines)` | All results in argument order once every coroutine finished (`asyncio.gather`). |
| `await pipeline(items, *stages)` | Each item through the async stages on its own pace; the last stage's results in item order. |

A step that fails raises `RuntimeError` where it is awaited: an `ant=` naming a human or no valid ant name, a
`capability` no ant but the conductor advertises, malformed `human()` questions, a reply still off its schema after
two re-asks, a step's message withdrawn from its recipient's queue (`<ant> never answered`), an unknown saved trail,
`args` over 64 KiB, an ancestor-trail cycle or a failed child run in `trail()`, or the run's step limit
reached. A step to an ant no daemon serves waits in that ant's queue, as any message does, until the run ends or a
human stops it. An exception the script does not catch fails the run with its traceback, lines numbered as in the
file (`trail.py`).

The script runs sandboxed: no files, environment, network, subprocesses or sleep; the clock reads the leg's start time
on every call and `random` is seeded once per leg, so the same answers make the same calls. The authoring guide
covers the API, the sandbox, the bundled modules, patterns and complete example trails, each run by the repository's
tests (`crates/ants/tests/trail_guide.rs`); the `run_trail` tool's description carries a short version of it.

## Waypoints

A trail's meta may list up to 32 `waypoints`, the places a run enters it: each names a top-level
`async def <name>(args)` of the script, which takes the leg's input and returns the leg's result. A run of the trail
starts at its first waypoint, a [`goto()`](#routes-goto) enters it at any. The top level of such a trail holds only its
meta, imports, definitions and constants, its steps in its waypoints: a top-level `await`, `if`, loop or expression
fails the check.

```python
meta = {"name": "triage", "description": "Triages an issue", "waypoints": ["classify", "report"]}

async def classify(args):
    kind = await agent(f"Is issue {args['issue']} a bug or a question? Answer bug or question.")
    if kind.strip() == "bug":
        await goto("fix", state={"issue": args["issue"]})
    return await report({"issue": args["issue"], "outcome": "answered"})

async def report(args):
    return await agent(f"Comment on issue {args['issue']}: {args['outcome']}.")
```

## Saved trails

`ants trail save FILE` checks a file, without running it, and saves it for the whole colony in `ants_trails` under its
meta's `name` or `--name`: 1–64 of `a-z`, `0-9` and `-`, not a UUID (meta's own `name` may also hold capitals and `_`,
needing `--name` then). A saved trail's script holds at most 256 KiB, its description 1000 characters, at most 32
phases of one line of 1–120 characters each, and at most 32 waypoints. Only humans save and remove trails; ants and
humans read them.

A conductor's daemon also finds trails in `~/.ants/trails/<name>.py` on its machine, and the CLI lists and runs the
`.py` files of the CLI machine's `~/.ants/trails` by the same name, the file's, whatever its meta's `name`. On a name
both have, the colony's trail runs, so every conductor runs the same trail by that name.

```sh
ants trail save review.py               # check and save for the colony; replaces a trail of that name
ants trail list                         # the colony's trails, then this machine's
ants trail show review                  # meta, source and script
ants trail rm review                    # the colony's only: a local file stays until deleted
```

In `ants chat`, `/trail list` and `/trail show NAME` show what `ants trail list` and `ants trail show` print, in the
pager.

## Starting a run

A run is started three ways; each names a conductor, an owner and a thread to report on:

- **`ants trail run`** (human credentials): a saved trail by name, or a `.py` file checked and sent inline (a local
  trail is sent inline too, as the conductor may run on another machine). `--args` is the script's JSON `args` (at most
  64 KiB). The conductor defaults to where `ants ask` without `@ant` goes, the human's assistant, else the messenger;
  `--ant` names another. The run reports on `--thread` (an id, a unique start of one, or `last`) or a new thread,
  and is owned by the human. The command prints the trail, conductor and thread and what the run may spend, then asks
  `Start the run? [y/N]`; without a terminal `--yes` is required. `--follow` then follows the run until it ends,
  exiting 0 once it is done and non-zero once it failed or stopped; `--json` prints `{"run","conductor","thread"}`.

  ```sh
  ants trail run review --args '{"repo": "/src/app", "files": ["src/main.rs"]}' --steps 100 --follow
  ants trail run ./draft.py --ant dev-a --thread last --yes
  ```

  In `ants chat`, `/trail run NAME [@ANT] [--steps N] [--at-once N] [--turns N] [--wall DUR] [--thread THREAD]
  [ARGS]` does the same for a saved trail, `--thread` also taking `here` for the thread open or selected: it shows the
  same summary over the view, and `y` starts the run ([cli.md](cli.md#ants-chat-command-bar)).

- **`run_trail`**, an MCP tool of ants ([architecture.md](architecture.md#mcp-tools)): the calling ant conducts a run of
  a saved trail (`name`) or of a script of its own (`script`), with `args`, on the current thread, owned by the thread's
  owner, under `limits` (`{"steps", "at_once", "turns", "wall"}`, as [Limits](#limits) names them; each left out takes
  its default). It returns the run's id at once, or with `wait: true` the run's result or error, waiting up to `timeout`
  (default `5m`, at most `30m`), else the run's id while the run goes on. `trail_status` tells the ant how a run it
  conducts stands, or lists those not ended, and `stop_trail` stops one ([Restarts, stops and
  resumes](#restarts-stops-and-resumes)). The tools are offered only while the bus keeps trail runs.
- **A schedule** with `trail = "NAME"` in place of `text` ([config.md](config.md#schedules)): each firing sends `ant`,
  the conductor, a message `Scheduled run of trail NAME.` carrying the trail and `args` (a TOML table); its daemon
  starts the saved trail on the firing's new thread instead of prompting the ant, owned by the schedule's `owner`, under
  the message's id as run id, so a redelivered firing starts no second run. A run that cannot start, such as for an
  unknown trail, is told to the thread's owner there. The entry's `limits` table (`steps`, `at_once`, `turns`, `wall`)
  sets what each run may spend, the firing carrying them; a limit left out takes its default, and one of 0 or over its
  maximum fails config loading. Such a run is untrusted ([Trust](#trust)).

  ```toml
  [[schedule]]
  name = "weekly-review"
  creds = "~/.config/ants/auth/weekly-review.creds"
  cron = "@weekly"
  ant = "lead"
  owner = "alice"
  trail = "review"
  args = { repo = "/src/app", files = ["src/main.rs"] }
  limits = { steps = 100, wall = "2d" }
  ```

The conductor refuses limits over the maxima, a script over 256 KiB and args over 64 KiB; a run's id is a UUID.

## Routes: goto()

`await goto(trail=None, at=None, state=None)` ends the run's leg where it is called and starts the next: in the trail
`trail` names, else the leg's own, at its waypoint `at`, else at its start (its first waypoint), with `state` (JSON, at
most 64 KiB) as `args`. The leg left is given up as a stop gives up a run's steps: its session is dropped, the steps it
waits on and queued (those of a `parallel()` beside the goto too) are dropped, and its child runs stopped. The run keeps
its id, conductor, owner, origin, thread, limits and spending, and its new leg starts with no phase. A goto is no step:
it sends no message and spends nothing. The legs a run went through are its route; the result of the leg that ends is
the run's.

A named trail is read as saved when the goto is taken, the colony's else the conductor's `~/.ants/trails`; without
`trail`, the leg's own script goes on as the leg started it. The run's record pins each leg's script, so saving a trail
anew mid-run changes no leg started.

The conductor refuses a goto it cannot follow: `goto()` raises `RuntimeError` where it is awaited, naming why, and the
script may catch it and take another exit. It refuses a trail not saved or a waypoint the trail does not list, a
`state` over 64 KiB, a leg the run's record (1 MiB) has no room for, an inline script for an untrusted run
([Trust](#trust)), and a trail and waypoint the run entered before:

```text
RuntimeError: the run entered trail fix at waypoint patch before; a run enters a trail at each waypoint only once
```

A run enters each waypoint of each trail at most once, so every route ends: a loop, such as fix and review rounds,
belongs in one waypoint, bounded by its code. Of several gotos waiting at once, the first called that the conductor
takes ends the leg; those refused before it raise.

The route shows on the run thread as `triage@classify -> fix@reproduce -> fix@patch -> triage@report`: each leg's trail
(`inline` for an inline script), with `@<waypoint>` when entered at a waypoint ([The run thread](#the-run-thread)).

## Child runs: trail()

`await trail(name, args=None)` is a step answered by a child run of the saved trail `name` (the colony's, else the
conductor's `~/.ants/trails`; never an inline script), started at its start with `args` as its input. The conductor
runs it as the parent's, with the parent's owner and origin, on a spin-off of the parent's thread, the step's thread,
labelled `trail <name>`. Its steps and turns count against the limits of the root run, the run no other run started,
and it ends within the wall time its parent has left. The result of its route answers the call; a child that fails
raises `RuntimeError: trail <name> failed: <error>` there, which the script may catch. A trail on the route of a calling
run (the caller, its parent, and so on up to the root), whichever of its legs entered it, is refused: no trail calls
itself, not even through a goto(). A call that starts no child run, refused so or naming a trail not saved or
one that does not compile, raises `RuntimeError` naming why and counts as no step.

Stopping or ending a run stops its children, and resuming it resumes them: those stopped, and those that failed on a
limit, whose errors then no longer answer their calls, so a run failed on its child's limit resumes under raised limits
with that child. A child tells its start and end in the notes of its own thread, messaging no one; its end note points
to resuming its parent.

## The run thread

A run reports on the thread it started with, as its conductor:

- **Notes:** the start posts `Run <run> of trail <name> started: <description>` to the thread's notes, suggesting the
  title `trail <name>: <description>`; a resume posts `Run <run> resumed by <human>.`; each goto taken posts
  `route: <route>`, the legs so far, such as `route: triage@classify -> fix@reproduce`. The `phase()`, `log()` and
  `print()` output of each resume of the script becomes one note of `phase: …` and `log: …` lines (at most 4000
  characters). Notices and escalations arriving on a step's thread, and replies the run no longer waits on, are
  quoted in notes too. Notes wake nobody and spend no turn.
- **Step threads:** each step's thread is a spin-off of the run thread, keeps the run's owner and carries a
  `trail_step`; its recipient answers it as any message, and the first reply answers the call.
- **Status events:** a `step` event (`run`, `call`, `label`, `to`, `step_thread`, `stage`: `started`, `answered`,
  `failed` or `dropped`) on the conductor's status of the run thread tells each step, and a `leg` event (`run`, `leg`
  counted from 0, `trail` unless inline, `waypoint` when entered at one) each leg's start, the first at the run's start
  ([hooks.md](hooks.md#statusevent-on-antsstatusantthread)).
- **In `ants chat`:** the thread view of the run thread lists each step where it started, with its label, the ant it
  went to, its stage and its thread, which `Enter` on the step opens ([cli.md](cli.md#ants-chat)). Status events are
  never stored, so the view also reads the steps from the records of the runs that the ants on the thread conduct
  when the thread shows: the markers each conductor keeps under the thread (`<ant>.on.<thread>.*`, read by a filtered
  direct get, so a thread's steps are read alone; each marker's time is its step's start), then for each run found its
  journal (`answered`, or `failed` for an error) and, while it goes on, the calls its session waits on (`started`); a
  step otherwise is `dropped`, as is one sent anew on a new thread after a resume. A run's thread opened after its
  steps ran lists them all the same, for as long as the run's records are kept.
- **End message:** once the run is done, failed or stopped, the conductor messages the owner, else the default human,
  on the run thread: `Run <run> of trail <name> is done (N steps, M turns):`, naming the trail the run started, with
  the result (cut at 8000 characters, `ants trail show <run>` showing the rest), the error (naming
  `ants trail resume <run>` when a limit was met), or who stopped it and how to resume it; a run of more than one leg
  ends it with a line `route: <route>`.

`ants trail runs` lists the runs of every conductor, or `--ant CONDUCTOR`'s; `ants trail show RUN` prints a run (its
id or a unique start of one): state, phase, spending, args, its route (each leg's trail, waypoint, start and steps),
its child runs with their states, the calls waiting and queued, and its journal; `--follow` follows it as
`ants trail run --follow` does. These need human credentials. In `ants chat`, `/trail runs` shows the same list in the
pager, and `/trail show RUN` the run as `ants trail show RUN` prints it.

## Limits

A run's limits are set when it starts, by `ants trail run`'s flags (`/trail run`'s in `ants chat`), `run_trail`'s
`limits` or a schedule's `limits` table (the key of each, a duration such as `2d` for `wall`), and kept in its record;
they may be raised on resume:

| Limit | Default | Maximum | Flag, key | Past it |
|---|---|---|---|---|
| Steps, `agent()`, `human()` and `trail()` calls (child runs included; re-asks not counted) | 50 | 500 | `--steps`, `steps` | the call raises `RuntimeError: the run reached its limit of N steps` |
| Steps outstanding at once | 4 | 16 | `--at-once`, `at_once` | later steps wait in the run; nothing raises |
| Turns the steps spend, as ant messages spend them | 200 | 2000 | `--turns`, `turns` | the run fails |
| Wall time from start, stopped time included | 24h | 7d | `--wall`, `wall` | the run fails, waiting or not |
| Compute of one resume of the script | 10 s | 10 s | none | `TimeoutError`, the run fails; the worker is killed 5 s later |
| Dumped session, the script's suspended state | 768 KiB | 768 KiB | none | the run fails, naming its size |
| Memory of the script | 64 MiB | 64 MiB | none | `MemoryError`, the run fails |
| Python call depth | 200 | 200 | none | `RecursionError` in the script |

A run failed on a limit keeps which in its record's `limit`: `steps` (a call past them raised and the script then
failed), `turns`, `wall`, `compute`, `session` or `memory`; a run failing as its child run failed on one keeps the
child's. `ants trail show RUN` names it (`state: failed on its turns limit: …`), and a failed run resumes only with one
([Restarts, stops and resumes](#restarts-stops-and-resumes)).

Each `agent()` step reserves up to 8 of the run's turns as its message's turn budget; the turns its reply spent, and
one per re-ask, count against the run's turns, and a run that cannot reserve one fails on its turn limit. Each step
also counts against the spending caps of the ant answering it. A goto spends nothing and changes no limit: the run's
legs share its limits, spending and wall time. A child run's steps and turns count against its root run's limits too,
and its wall time ends with what its parent has left. Keep the script's own work small: only the compute between steps
counts against the compute limit, and every variable counts against the session's size at each suspension.

## Trust

A run takes the trust of the thread it started on (its origin):

- **Trusted:** runs started by a human with `ants trail run`, and by an ant's `run_trail` on a thread a human or ant
  started. These run saved trails and inline scripts alike.
- **Untrusted:** runs on a thread a webhook or a schedule started, or one asking a review, which relays text from
  outside the bus. These run only saved trails: an inline script is refused (`a run started from a webhook's thread
  runs only saved trails`), a goto to one too (`… goes only to saved trails`), and each step's thread is narrowed to
  the read-only tool rules webhook threads get by default ([config.md](config.md#tool-permissions)). A schedule
  starting a trail sets no `allow` of its own.

A run's legs and child runs keep its origin, so a goto or `trail()` never makes a run more trusted.

Only humans start, stop and resume runs by request (`ants.trail.<ant>`, answered by the conductor's daemon while up),
admins stop and resume them by [admin order](auth.md#admin-orders) as well, save and remove trails, and read every run's
records; an ant reads and writes only the runs it conducts, starts runs only with `run_trail`, reads and stops those it
conducts with `trail_status` and `stop_trail`, and resumes none ([auth.md](auth.md)). A `human()` step goes to the run's
owner, and a step's thread keeps that owner, so replies and escalations of the step's recipient reach the human who owns
the run.

## Storage

| Store | Kept | Holds |
|---|---|---|
| KV `ants_trails` | never expires | Saved trails, keyed by name: name, description, phases, waypoints, script, who saved it and when. |
| KV `ants_trail_runs` | 37 days after the last write | Each conductor's runs, keyed under the conductor: `<ant>.<run>` the run's header (its route: each leg's trail or inline script, waypoint, args, first call id, start and steps; owner, origin, limits, state (a failed one's broken `limit`), spending, thread, parent run, times), `<ant>.<run>.session` the current leg's dumped session with the calls waiting and queued, `<ant>.<run>.call.<n>` one journal entry per answered call (`{call, function, args, answer}`, call ids numbered on across legs), `<ant>.runs` the conductor's unfinished runs, `<ant>.steps.<thread>` a marker per step's thread (`{run, call, to, run_thread}`), and `<ant>.on.<thread>.<step thread>` the same marker under the thread of the step's run, put with it before the step goes out. |

`ants init` creates both buckets. A run's states are `running`, `waiting` (on human steps only), `stopped`, `done` and
`failed`. Its records stay 37 days after their last write: 30 days of keeping plus the longest wall time, so a
finished run stays at least 30 days. The full layout is in [architecture.md](architecture.md#stores-and-subjects).

## Restarts, stops and resumes

- **Restart:** an answer's journal entry is stored before its reply is acked, and a run's session and header before
  its steps go out. A restarted daemon continues every run its `<ant>.runs` lists: steps sent before wait on, steps
  never sent go out, answers stored past the session are given to the script. A goto stores the run's header with
  the new leg, gives up the steps of the leg left, then stores the new leg's session: a daemon restarted between the
  two gives up those steps, still in the stored session, before the new leg goes on.
- **Replay:** a session the worker cannot restore (dumped by another Monty version, or lost) is rebuilt from the
  journal: the current leg's pinned script starts again at its waypoint with the leg's input, clock and seed, its
  steps answered from the journal in order. A script whose steps no longer match the journal fails the run, naming
  the first call that differs.
- **Stop:** `ants trail stop RUN` stops a run and its running child runs: no further step goes out and the replies of
  the steps waited on answer
  nothing. The conductor withdraws the steps sent to itself; the command lists those still in other ants' queues, each
  with the `ants withdraw ANT MESSAGE` withdrawing it. The conductor stops a run it conducts the same way with
  `stop_trail`, the run then stopped by the conductor, and the tool lists the steps in other ants' queues for a human to
  withdraw. An admin's `ants ant stop-trail ANT RUN` has the conductor's daemon stop it by [admin
  order](auth.md#admin-orders): unlike the request, which only a daemon up answers, the order waits for the machine's
  daemon while it is away and is recorded with the admin and time (`ants ant orders ANT`), its result listing the steps
  in other ants' queues. Ending a parent run stops its still-running children. Wall time keeps running while stopped.
- **Resume:** `ants trail resume RUN` resumes a stopped run, or one whose record names the limit it failed on, from
  its last stored session, resuming its child runs stopped or failed on a limit too; the steps waited on go out anew, on new threads. Limit flags replace the run's limits they name, within the
  maxima. A run done, failed otherwise, running or waiting is refused. An admin's `ants ant resume-trail ANT RUN`
  resumes it by admin order the same way, limit flags included.

```sh
ants trail runs
ants trail stop 0198c4
ants trail resume 0198c4 --turns 400 --wall 2d --follow
```

`/trail stop RUN` and `/trail resume RUN` in `ants chat` stop and resume a run the same way, a resume under the run's
own limits.

## Workers

Each start or resume of a script runs in a fresh worker, the `ants` binary started as `ants subprocess` with an empty
environment, which computes until the script waits on its steps, ends or raises, and exits: an idle daemon runs no
worker. `[trails] workers` in the config file caps the workers computing at once across a daemon process's runs
(default 4, 1 to 16), bounding the memory and CPU trails take beside the harnesses ([config.md](config.md#trails)):

```toml
[trails]
workers = 4
```

A worker failing with the run unchanged is retried, after 1 s and up to 1 min apart.

`ants doctor` starts the worker of the daemon service's `ants`, else its own, as a daemon does, with a request it
refuses before running anything. With a human's credentials it lists the runs stuck: those stopped or failed on a limit
(steps, turns or wall time, as their records say) that `ants trail resume` may still continue, naming the limits to
raise; those whose steps went to ants not in the colony, naming `ants trail stop`; and those whose conductor is not in
the colony ([cli.md](cli.md#identity-setup-completions-and-doctor)).
