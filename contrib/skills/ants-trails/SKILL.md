---
name: ants-trails
description: Write trails, Python scripts that orchestrate ants step by step (fan out over many items, verify findings with a second ant, loop until nothing new turns up, ask a human mid-run). Use when asked to write, review or fix a trail, or when work needs many fresh-context steps whose order, concurrency and merging a script should decide rather than one ant's judgment.
---

# Writing trails

A trail is a script in Monty's subset of Python whose code, not a model, decides which ant takes each step, how many
steps run at once and how their results merge. The conductor, the ant whose daemon runs the trail, sends each step as
a message on a fresh thread and stores the run after every step, so a run survives restarts and waits hours for its
steps without a process.

A trail is handed to ants, never learned. A skill is instructions one ant follows turn by turn, its intermediate
results in one context window; a trail moves the plan into code and keeps intermediate results in script variables, so
no item is skipped out of laziness, no ant verifies its own work and no goal drifts through lossy summaries. Sense is
an ant's own identity (its persona, skills and memories, learned by itself): no trail ever enters it. Trails are
colony-wide, written by humans or ants.

Write a trail when work splits into many steps that each deserve a clean context, when findings need a verifier other
than their author, or when discovery must repeat until it runs dry. For a task one ant finishes on one thread, send a
message instead.

## The file

A trail is a `.py` file whose first statement is a literal `meta`: a `name` of letters, digits, `-` and `_`, a
`description`, and the `phases` the script announces. The meta is read without running the script; a file without it
is refused. The script runs top to bottom with `await` allowed at the top level, and the value of its last expression,
which must be JSON (None, bools, numbers, strings, lists, dicts with string keys), is the run's result.

```python
meta = {
    "name": "summarize",
    "description": "Summarizes one file in three bullet points",
    "phases": [],
}

summary = await agent(
    f"Summarize {args['path']} in the repository at {args['repo']} in three bullet points. "
    "Reply with the bullet points only.",
    label=f"summarize {args['path']}",
)
{"path": args["path"], "summary": summary}
```

Args:

```json
{"repo": "/src/ants", "path": "crates/ants-trail/src/run.rs"}
```

## The API

Nothing else of the host is reachable: these names are defined before the script runs.

- `await agent(prompt, ant=None, capability=None, schema=None, label=None)`: one step. The conductor sends `prompt` on
  a fresh thread: to the ant `ant=` names, to an ant advertising `capability=` (picked as the `route` MCP tool picks
  one), or with neither to the conductor itself. The step's thread keeps the run's owner and links to the run's
  thread. Returns the reply's text; with `schema=`, a JSON Schema as a dict, returns the reply parsed as JSON. A reply
  that does not match the schema is asked for again on the step's thread, naming the mismatch, at most twice; then the
  call raises. `label=` names the step in the run's thread and status events.
- `await human(questions)`: asks the run's owner, as the `ask_human` MCP tool does. `questions` is a list of 1-4
  dicts, each with a `question` (1-500 characters) and 2-9 `options` (labels, or dicts with a `label` and a
  `description`), and optionally a `header`, `multi_select` (default False) and `free_text` (default True). Returns the
  answer as text: for one question, the label picked (labels joined by `, ` with `multi_select`) or the text typed; for
  several, one line per question, `title: answer`. Set `"free_text": False` to compare the answer with a label.
- `phase(title)`: starts a phase, named as in meta's `phases`, shown as a note on the run's thread.
- `log(text)`: a note on the run's thread; `print()` writes notes too. Neither waits.
- `args`: the JSON value the run was started with (None without).
- `await parallel(*coroutines)`: runs the coroutines at once and returns their results in argument order once every
  one has finished: a barrier.
- `await pipeline(items, *stages)`: passes each item through `stages`, async functions of one argument, each item
  moving on to its next stage as soon as its own step is answered, with no barrier between stages; returns the last
  stage's results in item order.

Only `agent()` and `human()` suspend a run. Both are coroutine functions: await them, or hand their coroutines to
`parallel`. Their arguments must be JSON: anything else raises TypeError.

### Errors

A step that fails (no such ant, a reply still off its schema after two retries, the run's step limit reached) raises
`RuntimeError` with the reason where the step is awaited. An exception the script does not catch fails the run with
its traceback, lines numbered as in the file. Inside `parallel` or `pipeline`, an uncaught exception of one coroutine
fails the whole gather, and the run abandons the other steps. Catch a step's error inside the per-item coroutine when
the trail can go on without that item, and log what was lost.

## The sandbox

- No files, environment, network or subprocesses: `open()` raises PermissionError, `os.getenv()` RuntimeError, and
  modules Monty does not bundle (`socket`, `subprocess`, third-party packages) are not found. Monty bundles `json`,
  `re`, `math`, `datetime`, `random`, `collections`, `itertools`, `functools`, `dataclasses` and a few more.
- The clock and `random` are fixed per run: `datetime.datetime.now()` returns the run's start time on every call,
  and `random` is seeded once per run, so a replay of the run makes the same calls. `time.sleep()` raises: to wait,
  ask a step to.
- No type checking: annotations are not checked, and a mistake surfaces only when its line runs. Try a trail on a
  small input first.

## Limits

| Limit | Default (maximum) | Past it |
|---|---|---|
| Steps, `agent()` and `human()` calls | 50 (500) | the call raises `RuntimeError: the run reached its limit of N steps` |
| Steps at once | 4 (16) | later steps wait in the run until a step is answered; nothing raises |
| Turns, spent by the steps as ant messages spend turns | 200 (2000) | the run fails, naming the limit |
| Wall time | 24 h (7 d) | the run fails, naming the limit |
| Compute of one resume | 10 s | the run fails, naming the limit |
| Stored session, the script's suspended state | 768 KiB | the run fails, naming its size and the limit |
| Memory | 64 MiB | `MemoryError` in the script |
| Nested calls | 200 | `RecursionError` in the script |

A run's limits are set when it starts, and each step also counts against the spending caps of the ant that answers
it. A run failed on a limit continues from its stored record with `ants trail resume`. Keep the script's own work
small: only the compute between steps (parsing, sorting, building prompts) counts against the compute limit, and
large variables (whole files, long lists of replies) count against the session limit at every suspension.

## Patterns

- **Pipeline by default.** `pipeline(items, stage1, stage2)` lets a fast item reach its second stage while a slow one
  is still in its first. Use `parallel` as a barrier only when the next stage needs every result, as a synthesis or a
  deduplication does.
- **Adversarial verify.** Give each finding to a skeptic step that tries to refute it, on a different ant (or
  capability) than the one that made the finding: no ant grades its own work. Keep only what survives, and report what
  could not be verified apart from what was refuted.
- **Loop until dry.** Repeat discovery, telling each round what is already known, until a round finds nothing new;
  cap the rounds, and log when the cap stops a search that still found something.
- **No silent caps.** When the script trims its input (to fit the step limit, say), `log()` what it skipped and why;
  return unverified or failed items rather than dropping them.
- **Schemas for structured results.** Pass `schema=` whenever the script reads fields of a reply. Keep schemas small,
  with `required` fields; a reply that does not match is asked for again.
- **Self-contained prompts.** Each step starts a fresh thread with no context: name the repository, branch, file,
  question and the shape of the answer in the prompt itself, and pass earlier results into it as text.
- **Handle a step's error.** Wrap a step in `try`/`except RuntimeError` when the run can go on without it; let it
  raise when nothing after it makes sense.
- **Budget the steps.** Count the steps the input implies (files × steps per file) against the step limit before
  starting a large run; a run needing more takes a higher limit.

## Examples

Each example below is a complete trail, run with the args after it by the repository's tests.

### Review every changed file, with adversarial verify

A reviewer ant reviews each file; a skeptic ant tries to refute each finding as soon as its file's review arrives. The
pipeline gives each file its own pace; within a file, `parallel` verifies the file's findings at once.

```python
meta = {
    "name": "review-changes",
    "description": "Reviews each changed file, then has a second ant try to refute every finding",
    "phases": ["review", "report"],
}

FINDINGS = {
    "type": "object",
    "properties": {
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"line": {"type": "integer"}, "issue": {"type": "string"}},
                "required": ["line", "issue"],
            },
        },
    },
    "required": ["findings"],
}

VERDICT = {
    "type": "object",
    "properties": {"real": {"type": "boolean"}, "reason": {"type": "string"}},
    "required": ["real", "reason"],
}

repo, branch = args["repo"], args["branch"]
files = args["files"]
limit = args.get("max_files", 20)
if len(files) > limit:
    log(f"reviewing the first {limit} of {len(files)} files; skipped: {', '.join(files[limit:])}")
    files = files[:limit]


async def review(path):
    try:
        found = await agent(
            f"Review the change to {path} on branch {branch} of the repository at {repo} "
            f"(git diff main...{branch} -- {path}). Report only defects that change behaviour, "
            "each with its line in the new file and one sentence on what goes wrong.",
            ant=args["reviewer"],
            schema=FINDINGS,
            label=f"review {path}",
        )
    except RuntimeError as e:
        log(f"no review of {path}: {e}")
        return []
    return [{"path": path, "line": f["line"], "issue": f["issue"]} for f in found["findings"]]


async def verify(finding):
    where = f"{finding['path']}:{finding['line']}"
    try:
        verdict = await agent(
            f"A reviewer claims a defect at {where} on branch {branch} of the repository at {repo}: "
            f"{finding['issue']}\nRead the code and try to refute the claim. "
            "Set real to true only if the defect holds, and give the reason.",
            ant=args["skeptic"],
            schema=VERDICT,
            label=f"verify {where}",
        )
    except RuntimeError as e:
        log(f"could not verify {where}: {e}")
        return {**finding, "real": None, "reason": str(e)}
    return {**finding, "real": verdict["real"], "reason": verdict["reason"]}


async def verify_all(findings):
    return await parallel(*[verify(f) for f in findings])


phase("review")
checked = [f for per_file in await pipeline(files, review, verify_all) for f in per_file]
phase("report")
confirmed = [f for f in checked if f["real"] is True]
unverified = [f for f in checked if f["real"] is None]
refuted = len(checked) - len(confirmed) - len(unverified)
log(f"{len(confirmed)} confirmed, {refuted} refuted, {len(unverified)} unverified")
{"confirmed": confirmed, "unverified": unverified}
```

Args:

```json
{
  "repo": "/src/ants",
  "branch": "trails",
  "files": ["crates/ants-trail/src/run.rs", "crates/ants-trail/src/worker.rs", "crates/ants/src/main.rs"],
  "reviewer": "dev-a",
  "skeptic": "dev-b"
}
```

### Research a question across sources, then synthesize

Each source is searched on its own; the synthesis needs every source's notes, so `parallel` is the barrier here.

```python
meta = {
    "name": "research",
    "description": "Researches a question in several sources at once, then writes one answer citing them",
    "phases": ["gather", "synthesize"],
}

NOTES = {
    "type": "object",
    "properties": {
        "relevant": {"type": "boolean"},
        "notes": {"type": "string"},
        "citations": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["relevant", "notes", "citations"],
}

question = args["question"]


async def gather(source):
    try:
        found = await agent(
            f"Research this question using only {source}: {question}\n"
            "Reply with notes on what this source says and a citation (path, URL or issue id) for each claim. "
            "Set relevant to false when the source says nothing about the question.",
            capability=args.get("capability"),
            schema=NOTES,
            label=f"research {source}",
        )
    except RuntimeError as e:
        log(f"no notes from {source}: {e}")
        return None
    return {"source": source, **found}


phase("gather")
results = await parallel(*[gather(source) for source in args["sources"]])
notes = [r for r in results if r is not None and r["relevant"]]
silent = [r["source"] for r in results if r is not None and not r["relevant"]]
if silent:
    log(f"nothing on the question in: {', '.join(silent)}")

phase("synthesize")
if notes:
    gathered = "\n\n".join(
        f"From {n['source']}:\n{n['notes']}\nCitations: {', '.join(n['citations'])}" for n in notes
    )
    answer = await agent(
        "Answer this question from the research notes below, citing them; say where the sources disagree "
        f"and what stays open.\n\nQuestion: {question}\n\n{gathered}",
        label="synthesize",
    )
else:
    answer = None
    log("no source had anything on the question")
{"question": question, "answer": answer, "sources": [n["source"] for n in notes]}
```

Args:

```json
{
  "question": "How does a trail run survive a daemon restart?",
  "sources": [
    "the docs/ folder of the repository at /src/ants",
    "the bd issue tracker of /src/ants (bd search)",
    "the code of crates/ants-trail in /src/ants"
  ],
  "capability": "rust"
}
```

### Loop until dry

Discovery repeats, each round told what earlier rounds found, until a round brings nothing new or the round cap
stops it.

```python
meta = {
    "name": "find-until-dry",
    "description": "Lists the problems of an area round after round until a round finds nothing new",
    "phases": ["search", "report"],
}

ITEMS = {
    "type": "object",
    "properties": {"items": {"type": "array", "items": {"type": "string"}}},
    "required": ["items"],
}

area, rounds = args["area"], args.get("max_rounds", 5)
known = []
dry = False

phase("search")
for n in range(1, rounds + 1):
    already = "\n".join(f"- {item}" for item in known) or "(none yet)"
    found = await agent(
        f"List problems in {area} of the repository at {args['repo']}: dead code, missing tests, "
        f"unclear error messages. Leave out these, found already:\n{already}\n"
        "Reply with each new problem as one line naming its file; an empty list when nothing new is left.",
        capability=args.get("capability"),
        schema=ITEMS,
        label=f"search round {n}",
    )
    new = [item for item in found["items"] if item not in known]
    log(f"round {n}: {len(new)} new")
    if not new:
        dry = True
        break
    known.extend(new)

phase("report")
if not dry:
    log(f"stopped after {rounds} rounds while rounds still found problems; more may remain")
{"problems": known, "dry": dry}
```

Args:

```json
{"repo": "/src/ants", "area": "crates/ants-daemon/src/sense.rs", "max_rounds": 4}
```

### Ask the owner before acting

A plan goes to the run's thread as a note, the owner approves it or not, and only then a step applies it. The apply
step starts with no context, so its prompt carries the plan.

```python
meta = {
    "name": "plan-and-apply",
    "description": "Drafts a plan for a change, asks the run's owner to approve it, then applies it",
    "phases": ["plan", "approve", "apply"],
}

repo, change = args["repo"], args["change"]

phase("plan")
plan = await agent(
    f"Draft a step-by-step plan for this change in the repository at {repo}: {change}\n"
    "Change nothing yet; reply with the plan only.",
    ant=args["ant"],
    label="plan",
)
log(plan)

phase("approve")
choice = await human([
    {
        "question": f"Apply the plan noted on this run's thread for: {change}",
        "header": "Plan",
        "options": [
            {"label": "apply", "description": "a step applies the plan on a new branch"},
            {"label": "stop", "description": "the run ends with nothing changed"},
        ],
        "free_text": False,
    }
])

if choice == "apply":
    phase("apply")
    report = await agent(
        f"Apply this plan in the repository at {repo} on a new branch and commit; "
        f"reply with the branch and what changed.\n\nChange: {change}\n\nPlan:\n{plan}",
        ant=args["ant"],
        label="apply",
    )
else:
    report = None
    log("the owner stopped the run; nothing applied")
{"applied": report is not None, "plan": plan, "report": report}
```

Args:

```json
{"repo": "/src/ants", "change": "rename the retired term persona to ant in docs/", "ant": "dev-a"}
```

## Running a trail

A human starts a run with `ants trail run`, which shows what the run may spend before it starts; an ant starts one
with the `run_trail` MCP tool, from a saved trail's name or a script of its own; a `[[schedule]]` entry with `trail =`
starts one on its schedule. Saved trails are colony-wide, put and removed only by humans; a daemon also reads
`~/.ants/trails/*.py`. A run started on a thread a webhook started may run only a saved trail. `ants trail runs`,
`show`, `stop` and `resume` manage runs.
