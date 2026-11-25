//! The turns an ACP harness runs of its own between prompts, as claude-agent-acp does as a background command it
//! started ends: a `session/update` of the agent's output ([`OUTPUT`]) coming while no prompt and no opening of a
//! session is pending starts one, reported ([`OwnTurnReport`]) and kept with the session's updates that follow, at most
//! [`STASHED`] turns. The prompt taking it ([`own_turn::of`]) is not written to the harness: the turn's updates go to
//! the daemon as the prompt's, those still to come too, and its end answers the prompt. A turn ends at a
//! `usage_update` carrying `_claude/origin`, as claude-agent-acp ends each, else once the harness said nothing of it
//! for [`QUIET`]. A prompt the harness is sent meanwhile takes what of the turn is still to come. A turn the report
//! refuses is kept for no prompt: its updates go on to the daemon as any between prompts do, where only their cost
//! counts.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::Mutex;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, Lines};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::harness::own_turn::{self, OwnTurn, OwnTurnReport};
use crate::sync::MutexExt as _;

/// The most turns kept for a prompt to take: the oldest is dropped.
const STASHED: usize = 8;

/// How long a turn whose end the harness does not mark lasts after its last update.
const QUIET: Duration = Duration::from_secs(30);

/// The `sessionUpdate` kinds of the agent's output, which start a turn of its own between prompts.
const OUTPUT: [&str; 5] = ["agent_message_chunk", "agent_thought_chunk", "tool_call", "tool_call_update", "plan"];

pub(super) struct OwnTurns {
    name: String,
    report: OwnTurnReport,
    state: Mutex<State>,
    /// Wakes the reader of the harness's lines when lines for the daemon are queued, or a turn taken may end.
    wake: Notify,
}

#[derive(Default)]
struct State {
    /// The prompts sent to the harness and not answered, by JSON-RPC id: their sessions.
    prompts: HashMap<String, String>,
    /// The requests opening a session sent and not answered, by JSON-RPC id: a loaded session's history comes as
    /// updates meanwhile.
    opening: HashSet<String>,
    /// The turns of the harness's own, oldest first.
    turns: VecDeque<Turn>,
    /// Lines for the daemon to read before the harness's next.
    queued: VecDeque<String>,
}

struct Turn {
    id: String,
    session: String,
    /// Its updates no prompt took yet.
    lines: Vec<String>,
    ended: bool,
    /// When its last update came.
    last: Instant,
    taken: Taken,
}

impl Turn {
    /// Whether it may still go on: not ended, and taken or not quiet for [`QUIET`].
    fn open(&self) -> bool {
        !self.ended && (self.taken.is_by() || self.last.elapsed() < QUIET)
    }
}

enum Taken {
    No,
    /// By the prompt of this JSON-RPC id, its updates going to it as they come.
    By(Value),
    /// By none, its prompt cancelled or the report refusing it: its updates still to come go on to the daemon, as
    /// between prompts, where only its cost counts.
    Dropped,
}

/// The answer to prompt `id` ending its turn with `stop`.
fn answer(id: &Value, stop: &str) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "result": { "stopReason": stop } }).to_string()
}

impl OwnTurns {
    /// The turns of its own harness `name` runs, reported to `report`.
    pub(super) fn new(name: &str, report: OwnTurnReport) -> Self {
        Self { name: name.to_owned(), report, state: Mutex::default(), wake: Notify::new() }
    }

    /// Whether `line`, from the daemon, goes on to the harness: all but a prompt taking a turn kept here.
    pub(super) fn sent(&self, line: &str) -> bool {
        if !line.contains("\"session/") {
            return true;
        }
        let Ok(message) = serde_json::from_str::<Value>(line) else { return true };
        let params = &message["params"];
        let session = params["sessionId"].as_str().unwrap_or_default();
        let mut state = self.state.locked();
        let State { prompts, opening, turns, queued } = &mut *state;
        match (message["method"].as_str(), message.get("id")) {
            (Some("session/new" | "session/load" | "session/resume" | "session/fork"), Some(id)) => {
                opening.insert(id.to_string());
            }
            (Some("session/prompt"), Some(id)) => {
                let taking = own_turn::of(params["_meta"].as_object()).and_then(|own| {
                    turns.iter().position(|turn| turn.id == own && turn.session == session && turn.taken.is_none())
                });
                let Some(index) = taking else {
                    // What the harness still runs of a turn of its own goes to the prompt it takes.
                    for turn in turns.iter_mut().filter(|turn| turn.session == session && !turn.taken.is_by()) {
                        turn.ended = true;
                    }
                    prompts.insert(id.to_string(), session.to_owned());
                    return true;
                };
                let turn = &mut turns[index];
                queued.extend(std::mem::take(&mut turn.lines));
                if turn.open() {
                    turn.taken = Taken::By(id.clone());
                } else {
                    turns.remove(index);
                    queued.push_back(answer(id, "end_turn"));
                }
                drop(state);
                self.wake.notify_one();
                return false;
            }
            (Some("session/cancel"), None) => {
                let taken = turns.iter_mut().find(|turn| turn.session == session && turn.taken.is_by());
                if let Some(turn) = taken
                    && let Taken::By(id) = std::mem::replace(&mut turn.taken, Taken::Dropped)
                {
                    queued.push_back(answer(&id, "cancelled"));
                    drop(state);
                    self.wake.notify_one();
                }
            }
            _ => {}
        }
        true
    }

    /// `line`, from the harness, if it goes on to the daemon now: not when kept for a turn of the harness's own, nor
    /// when lines are queued before it, which it joins.
    fn received(&self, line: String) -> Option<String> {
        let mut state = self.state.locked();
        let State { prompts, opening, turns, queued } = &mut *state;
        let pass = |queued: &mut VecDeque<String>, line: String| {
            if queued.is_empty() {
                return Some(line);
            }
            queued.push_back(line);
            None
        };
        let notification = line.contains("\"method\"");
        if notification && !line.contains("\"session/update\"") {
            return pass(queued, line);
        }
        if !notification && prompts.is_empty() && opening.is_empty() {
            return pass(queued, line);
        }
        let Ok(message) = serde_json::from_str::<Value>(&line) else { return pass(queued, line) };
        if message.get("method").is_none() {
            if let Some(id) = message.get("id") {
                let id = id.to_string();
                prompts.remove(&id);
                opening.remove(&id);
            }
            return pass(queued, line);
        }
        let session = message["params"]["sessionId"].as_str().unwrap_or_default();
        let update = &message["params"]["update"];
        let kind = update["sessionUpdate"].as_str().unwrap_or_default();
        let ends = kind == "usage_update" && update["_meta"].get("_claude/origin").is_some();
        if let Some(index) = turns.iter().rposition(|turn| turn.session == session && turn.open()) {
            let turn = &mut turns[index];
            (turn.last, turn.ended) = (Instant::now(), ends);
            return match &turn.taken {
                Taken::No => {
                    turn.lines.push(line);
                    None
                }
                Taken::Dropped => {
                    if ends {
                        turns.remove(index);
                    }
                    pass(queued, line)
                }
                Taken::By(id) => {
                    let ending = ends.then(|| answer(id, "end_turn"));
                    let passed = pass(queued, line);
                    if let Some(ending) = ending {
                        turns.remove(index);
                        queued.push_back(ending);
                    }
                    passed
                }
            };
        }
        let prompted = prompts.values().any(|prompted| prompted == session);
        if prompted || !opening.is_empty() || !OUTPUT.contains(&kind) {
            return pass(queued, line);
        }
        turns.retain(|turn| turn.taken.is_none() || turn.open());
        let id = uuid::Uuid::new_v4().to_string();
        let turn = Turn {
            id: id.clone(),
            session: session.to_owned(),
            lines: vec![line],
            ended: false,
            last: Instant::now(),
            taken: Taken::No,
        };
        turns.push_back(turn);
        if turns.iter().filter(|turn| turn.taken.is_none()).count() > STASHED
            && let Some(oldest) = turns.iter().position(|turn| turn.taken.is_none())
        {
            turns.remove(oldest);
        }
        drop(state);
        let text = format!("[ants] {} started a turn of its own.", self.name);
        if !self.report.send(OwnTurn { id: id.clone(), text }) {
            let mut state = self.state.locked();
            if let Some(turn) = state.turns.iter_mut().find(|turn| turn.id == id) {
                (turn.taken, turn.lines) = (Taken::Dropped, Vec::new());
            }
        }
        None
    }

    /// The next line queued for the daemon, if any.
    fn queued(&self) -> Option<String> {
        self.state.locked().queued.pop_front()
    }

    /// When the first turn taken whose end the harness may not mark lasts until.
    fn deadline(&self) -> Option<Instant> {
        let state = self.state.locked();
        state.turns.iter().filter(|turn| turn.taken.is_by()).map(|turn| turn.last + QUIET).min()
    }

    /// End the turns taken that have been quiet for [`QUIET`], answering their prompts.
    fn expire(&self) {
        let mut state = self.state.locked();
        let State { turns, queued, .. } = &mut *state;
        turns.retain(|turn| match &turn.taken {
            Taken::By(id) if turn.last.elapsed() >= QUIET => {
                queued.push_back(answer(id, "end_turn"));
                false
            }
            _ => true,
        });
    }
}

impl Taken {
    fn is_none(&self) -> bool {
        matches!(self, Self::No)
    }

    fn is_by(&self) -> bool {
        matches!(self, Self::By(_))
    }
}

/// The next line for the daemon from the harness's `lines`, the turns of its `own` taken care of, if it reports them;
/// None once its output ended.
pub(super) async fn next(
    own: Option<&OwnTurns>,
    lines: &mut Lines<impl AsyncBufRead + Unpin>,
) -> io::Result<Option<String>> {
    let Some(own) = own else { return lines.next_line().await };
    loop {
        if let Some(line) = own.queued() {
            return Ok(Some(line));
        }
        let deadline = own.deadline();
        let line = tokio::select! {
            biased;
            () = own.wake.notified() => continue,
            () = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {
                own.expire();
                continue;
            }
            line = lines.next_line() => line?,
        };
        let Some(line) = line else { return Ok(None) };
        if let Some(line) = own.received(line) {
            return Ok(Some(line));
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use ants_proto::ThreadId;
    use ants_test_support::scratch_dir;
    use tokio::sync::mpsc;

    use crate::harness::native::Protocol;
    use crate::harness::native::tests::{define, launch, run};
    use crate::harness::{Launch, ModelSettings, OwnTurn, Prompt, Session, Spend};

    /// An ACP harness answering its first prompt `Started`, then, once `done` exists in its directory, running a turn
    /// of its own as claude-agent-acp 0.106.0 was seen to as a background command ends: context use, the answer
    /// `Finished`, and, once `ended` exists, the `usage_update` with `_claude/origin` ending it. Its later prompts
    /// answer `Prompt<n>`; each line it reads is appended to `received`.
    const ADAPTER: &str = r#"#!/bin/sh
d=$(dirname "$0")
answer() {
    id=$(printf '%s\n' "$1" | grep -o '"id":[^,}]*' | head -n 1)
    printf '{"jsonrpc":"2.0",%s,"result":%s}\n' "$id" "$2"
}
update() {
    printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":%s}}\n' "$1"
}
chunk() {
    update "{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"type\":\"text\",\"text\":\"$1\"},\"messageId\":\"m$1\"}"
}
waiting() {
    while [ ! -f "$d/$1" ]; do sleep 0.05; done
}
n=0
while read -r line; do
    printf '%s\n' "$line" >> "$d/received"
    case "$line" in
    *'"initialize"'*) answer "$line" '{"protocolVersion":1,"agentInfo":{"name":"adapter","version":"1.0"}}' ;;
    *'"session/new"'*) answer "$line" '{"sessionId":"s1"}' ;;
    *'"session/prompt"'*)
        n=$((n + 1))
        if [ $n = 1 ]; then
            chunk Started
            update '{"sessionUpdate":"usage_update","used":10,"size":100,"cost":{"amount":0.1,"currency":"USD"},"_meta":{"_claude/origin":{"kind":"human"}}}'
            answer "$line" '{"stopReason":"end_turn"}'
            (
                waiting done
                update '{"sessionUpdate":"usage_update","used":12,"size":100}'
                chunk Finished
                waiting ended
                update '{"sessionUpdate":"usage_update","used":12,"size":100,"cost":{"amount":0.3,"currency":"USD"},"_meta":{"_claude/origin":{"kind":"task-notification","producer":"session-task"}}}'
            ) &
        else
            chunk "Prompt$n"
            answer "$line" '{"stopReason":"end_turn"}'
        fi
        ;;
    *'"method"'*'"id"'* | *'"id"'*'"method"'*) answer "$line" '{}' ;;
    esac
done
"#;

    /// A launch of [`ADAPTER`] in a scratch directory for thread t1, the turns it runs of its own going to the
    /// receiver, taken by prompts if `taking`, else refused.
    fn adapter(name: &str, taking: bool) -> (PathBuf, Launch, mpsc::UnboundedReceiver<OwnTurn>) {
        let dir = scratch_dir(name).canonicalize().unwrap();
        let command = dir.join("adapter");
        ants_test_support::executable(&command, ADAPTER);
        let harness = define(name, Protocol::Acp, &format!("command = {:?}", command.display().to_string())).unwrap();
        let (tx, reported) = mpsc::unbounded_channel();
        let launch = launch(&harness, &dir, &[], ModelSettings::default())
            .with_own_turns(Arc::new(move |thread: &ThreadId, turn: OwnTurn| {
                assert_eq!(thread.as_str(), "t1");
                tx.send(turn).is_ok() && taking
            }))
            .for_thread(&"t1".parse().unwrap());
        (dir, launch, reported)
    }

    /// The prompts the adapter in `dir` read.
    fn prompts(dir: &Path) -> Vec<String> {
        let received = std::fs::read_to_string(dir.join("received")).unwrap_or_default();
        received.lines().filter(|line| line.contains("\"session/prompt\"")).map(str::to_owned).collect()
    }

    async fn reported(own: &mut mpsc::UnboundedReceiver<OwnTurn>) -> OwnTurn {
        tokio::time::timeout(std::time::Duration::from_secs(10), own.recv()).await.unwrap().unwrap()
    }

    #[tokio::test]
    async fn a_turn_an_acp_harness_runs_of_its_own_is_reported_and_taken_by_its_prompt_as_it_runs() {
        let (dir, launch, mut own) = adapter("acp-own-turn", true);
        let session = Session::start(&launch, None).await.unwrap();
        assert_eq!(run(&session, Prompt::new("sleep in the background")).await.0.unwrap(), "Started");
        assert!(own.try_recv().is_err(), "the prompt's turn is no turn of the harness's own");

        std::fs::write(dir.join("done"), "").unwrap();
        let turn = reported(&mut own).await;
        assert_eq!(turn.text, "[ants] acp-own-turn started a turn of its own.");
        let taking = Prompt { own_turn: Some(turn.id.clone()), ..Prompt::new(turn.text.clone()) };
        {
            let taking = run(&session, taking);
            tokio::pin!(taking);
            let running = tokio::time::timeout(std::time::Duration::from_millis(300), &mut taking).await;
            assert!(running.is_err(), "the turn runs on");
            std::fs::write(dir.join("ended"), "").unwrap();
            let (reply, _, spends) = taking.await;
            assert_eq!(reply.unwrap(), "Finished", "the turn's answer");
            assert!(spends.contains(&Spend::Cost { session: session.id(), total: 0.3 }), "{spends:?}");
        }
        assert_eq!(prompts(&dir).len(), 1, "taking the turn writes nothing to the harness");

        let again = Prompt { own_turn: Some(turn.id), ..Prompt::new("told again") };
        assert_eq!(run(&session, again).await.0.unwrap(), "Prompt2", "a turn taken already prompts with the message");
        assert!(prompts(&dir)[1].contains("told again"));
        session.close().await;
        assert!(own.try_recv().is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_prompt_coming_while_an_acp_harness_runs_a_turn_of_its_own_is_sent_and_leaves_what_came_to_be_taken() {
        let (dir, launch, mut own) = adapter("acp-own-turn-first", true);
        let session = Session::start(&launch, None).await.unwrap();
        assert_eq!(run(&session, Prompt::new("sleep in the background")).await.0.unwrap(), "Started");
        std::fs::write(dir.join("done"), "").unwrap();
        let turn = reported(&mut own).await;

        assert_eq!(run(&session, Prompt::new("next")).await.0.unwrap(), "Prompt2", "sent to the harness");
        std::fs::write(dir.join("ended"), "").unwrap();
        let taking = Prompt { own_turn: Some(turn.id), ..Prompt::new(turn.text) };
        assert_eq!(run(&session, taking).await.0.unwrap(), "Finished", "what came of the turn before the prompt");
        assert_eq!(prompts(&dir).len(), 2);
        session.close().await;
        assert!(own.try_recv().is_err(), "the turn's end went to the prompt that came");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_turn_of_an_acp_harness_s_own_the_report_refuses_is_dropped() {
        let (dir, launch, mut own) = adapter("acp-own-turn-refused", false);
        let session = Session::start(&launch, None).await.unwrap();
        assert_eq!(run(&session, Prompt::new("sleep in the background")).await.0.unwrap(), "Started");
        std::fs::write(dir.join("ended"), "").unwrap();
        std::fs::write(dir.join("done"), "").unwrap();
        reported(&mut own).await;
        // The end of the turn, after its answer, reaches the adapter's output before the next prompt.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let (reply, _, spends) = run(&session, Prompt::new("next")).await;
        assert_eq!(reply.unwrap(), "Prompt2", "nothing of the turn refused");
        assert!(spends.contains(&Spend::Cost { session: session.id(), total: 0.3 }), "its cost counts: {spends:?}");
        session.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }
}
