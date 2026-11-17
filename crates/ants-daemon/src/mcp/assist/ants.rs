//! Acting on an ant for the human: `queue_for_human`, `cancel_for_human` and `withdraw_for_human` list the ant's
//! queue, cancel its prompt on a thread and withdraw a message waiting for it, asking the ant's daemons, and
//! `order_for_human` sends an admin order to the ant's machine, held there until the human confirms it.

use std::fmt::Write as _;

use ants_proto::{Act, AdminOp, AdminOrder, CancelRequest, Machine, MessageMark, QueueRequest, ThreadId, UserId, Uuid};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{ErrorData, schemars, tool, tool_router};
use serde::Deserialize;

use super::{ANSWER_WAIT, HumanStore};
use crate::delivery::attempts::Recorded;
use crate::delivery::queue;
use crate::mcp::Hub;
use crate::mcp::tools::{Tools, text};

/// The most messages `queue_for_human` lists of those the ant's daemons have not taken from the stream yet.
const QUEUE_SHOWN: usize = 50;

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct QueueForHuman {
    /// The ant whose queue to list.
    ant: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct CancelForHuman {
    /// The ant running the prompt.
    ant: String,
    /// The thread it runs on.
    thread: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct WithdrawForHuman {
    /// The ant the message waits for.
    ant: String,
    /// The id of the message, as queue_for_human lists it, or of a message of the human's to the ant.
    message_id: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct OrderForHuman {
    /// The ant to order.
    ant: String,
    /// The machine whose daemon serves the ant, as `ants machines` lists it.
    machine: String,
    /// pause, resume, restart (once the ant is idle), restart_now, read_settings, read_memories or set.
    op: String,
    /// For set, the changes of the ant's settings, all carried out or none.
    #[serde(default)]
    changes: Option<Vec<SetChange>>,
}

/// A change of one field of an ant's settings in an order to set them.
#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct SetChange {
    /// The field, dotted from the ant's `[[ant]]` table: `model`, `caps.daily`; or from the config file's top level
    /// under `harnesses`: `harnesses.copilot.command`.
    field: String,
    /// The new value; absent or null to remove the field.
    #[serde(default)]
    value: Option<serde_json::Value>,
    /// The value the field holds now, as read_settings shows it, for the change to apply; absent when the field must
    /// be absent.
    #[serde(default)]
    expected: Option<serde_json::Value>,
    /// In place of `expected`, for a value read_settings hid as a secret: the digest it gave of it.
    #[serde(default)]
    expected_digest: Option<String>,
}

impl SetChange {
    fn into_field_change(self) -> Result<ants_proto::FieldChange, String> {
        let field: Vec<String> = self.field.trim().split('.').map(str::to_owned).collect();
        if field.iter().any(String::is_empty) {
            return Err(format!("field {}: dotted keys, such as caps.daily", self.field));
        }
        let value = self.value.filter(|v| !v.is_null());
        Ok(ants_proto::FieldChange { field, expected: self.expected, expected_digest: self.expected_digest, value })
    }
}

impl Hub {
    /// The ant `ant` names, whose queue the assistant reads or acts on for its human.
    fn queued_ant(&self, ant: &str) -> Result<UserId, String> {
        let ant: UserId = ant.trim().trim_start_matches('@').parse().map_err(|e| format!("{e}"))?;
        if self.humans.is_human(&ant) {
            return Err(format!("{ant} is a human; only an ant has a queue"));
        }
        Ok(ant)
    }

    /// Why no daemon of `ant` answered a request for `human`.
    fn unanswered(&self, ant: &UserId, human: &UserId) -> String {
        format!(
            "no daemon serving {ant} answered within {}s (one answers only while the grant of {human} names {})",
            ANSWER_WAIT.as_secs(),
            self.ant
        )
    }

    /// The messages waiting for `ant`, as `ants queue` lists them, and how many more wait in the stream unlisted.
    async fn waiting(
        &self,
        human: &UserId,
        store: &dyn HumanStore,
        ant: &UserId,
        shown: usize,
    ) -> Result<queue::Listed, String> {
        let request = QueueRequest::new(ant.clone(), self.ant.clone()).for_human(human.clone());
        let listed = store.queue(request, self.humans.humans(), shown).await.map_err(|e| format!("{e:#}"))?;
        listed.ok_or_else(|| self.unanswered(ant, human))
    }

    async fn queue_for_human(&self, args: QueueForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let ant = self.queued_ant(&args.ant)?;
        let (waiting, more) = self.waiting(&human, store, &ant, QUEUE_SHOWN).await?;
        let mut out = if waiting.is_empty() {
            format!("No message waits for {ant}.")
        } else {
            format!(
                "Messages waiting for {ant}, those its daemon holds first, then those awaiting redelivery and those \
                 still in the stream; each with how many of its thread go before it:"
            )
        };
        for m in &waiting {
            let from = match &m.channel {
                Some(channel) => format!("{} in #{channel}", m.from),
                None => m.from.to_string(),
            };
            let at = m.sent_at.format("%Y-%m-%d %H:%M UTC");
            let (id, thread, state) = (m.id, &m.thread_id, queue::state(m));
            let _ = write!(out, "\n- message {id} on thread {thread} from {from}, sent {at}: {state}");
        }
        if more > 0 {
            let _ = write!(out, "\n{more} more wait in the stream, not listed.");
        }
        Ok(out)
    }

    async fn cancel_for_human(&self, args: CancelForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let ant = self.queued_ant(&args.ant)?;
        let thread: ThreadId = args.thread.trim().parse().map_err(|e| format!("{e}"))?;
        let request = CancelRequest::new(ant.clone(), thread.clone(), self.ant.clone()).for_human(human.clone());
        let answers = store.cancel(request).await.map_err(|e| format!("{e:#}"))?;
        if answers.is_empty() {
            return Err(self.unanswered(&ant, &human));
        }
        if !answers.iter().any(|a| a.cancelled) {
            return Err(format!("{ant} has no prompt running on thread {thread}"));
        }
        Ok(format!(
            "Cancelled the prompt of {ant} on thread {thread} for {human}; the partial reply follows on the thread, and \
             the daemon of {ant} tells {human}."
        ))
    }

    async fn withdraw_for_human(&self, args: WithdrawForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let ant = self.queued_ant(&args.ant)?;
        let id: Uuid = args.message_id.trim().parse().map_err(|e| format!("message_id: {e}"))?;
        let waiting = match self.waiting(&human, store, &ant, usize::MAX).await {
            Ok((waiting, _)) => waiting,
            Err(_) => return self.record_withdrawal(&human, store, &ant, id).await,
        };
        let thread = match waiting.iter().find(|m| m.id == id) {
            Some(m) if m.running => {
                let thread = &m.thread_id;
                return Err(format!(
                    "{ant} started on message {id} already; cancel_for_human stops its prompt on thread {thread}"
                ));
            }
            Some(m) if m.started => {
                return Err(format!(
                    "{ant} started on message {id} already, in an attempt that did not end (its daemon stopped, or its \
                     reply is stored); the message awaits redelivery to finish it"
                ));
            }
            Some(m) => m.thread_id.clone(),
            None => {
                let messages = self.human_messages(&human, store).await?;
                let sent = messages.find(id).filter(|m| m.envelope.to == ant && messages.by_human(&m.envelope));
                sent.map(|m| m.envelope.thread_id.clone()).ok_or_else(|| {
                    format!(
                        "message {id} is neither waiting for {ant} nor one of {human} to it; queue_for_human \
                         lists those waiting"
                    )
                })?
            }
        };
        let request =
            CancelRequest::new(ant.clone(), thread, self.ant.clone()).for_human(human.clone()).withdrawing(id);
        let answers = store.cancel(request).await.map_err(|e| format!("{e:#}"))?;
        if answers.iter().any(|a| a.cancelled) {
            return Ok(format!(
                "Withdrew message {id} from the queue of {ant} for {human}; its thread is told once its turn comes, \
                 and the daemon of {ant} tells {human}."
            ));
        }
        match answers.into_iter().find_map(|a| a.mark) {
            Some(MessageMark::Withdrawn { by, on_behalf_of, .. }) => {
                let by = on_behalf_of.map_or_else(|| by.to_string(), |h| format!("{by} for {h}"));
                Ok(format!("Message {id} was withdrawn already, by {by}."))
            }
            Some(MessageMark::Started { thread_id, .. } | MessageMark::Retrying { thread_id, .. }) => Err(format!(
                "{ant} started on message {id} already; cancel_for_human stops its prompt on thread {thread_id}"
            )),
            Some(MessageMark::Finished { .. }) => Err(format!("{ant} finished message {id} already")),
            None => self.record_withdrawal(&human, store, &ant, id).await,
        }
    }

    /// Withdraw message `id`, one of `human`'s to `ant`, while no daemon of the ant answers: by a record under a key
    /// naming the assistant, which the ant's daemon honours before taking the message up.
    async fn record_withdrawal(
        &self,
        human: &UserId,
        store: &dyn HumanStore,
        ant: &UserId,
        id: Uuid,
    ) -> Result<String, String> {
        let messages = self.human_messages(human, store).await?;
        let Some(sent) = messages.find(id).filter(|m| m.envelope.to == *ant && messages.by_human(&m.envelope)) else {
            return Err(format!(
                "{}; with none serving, only a message of {human} to {ant} is withdrawn",
                self.unanswered(ant, human)
            ));
        };
        let thread = sent.envelope.thread_id.clone();
        let recorded = store.record_withdrawal(ant.clone(), id, human.clone(), self.ant.clone()).await;
        match recorded.map_err(|e| format!("{e:#}"))? {
            Recorded::Holds => Ok(format!(
                "No daemon serving {ant} answered: recorded the withdrawal of message {id} on thread {thread} for \
                 {human}. A daemon of {ant} honours it before {ant} is prompted with the message, while the grant of \
                 {human} names {}, and then tells the thread and {human}.",
                self.ant
            )),
            Recorded::Marked(MessageMark::Withdrawn { by, on_behalf_of, .. }) => {
                let by = on_behalf_of.map_or_else(|| by.to_string(), |h| format!("{by} for {h}"));
                Ok(format!("Message {id} was withdrawn already, by {by}."))
            }
            Recorded::Marked(MessageMark::Started { .. } | MessageMark::Retrying { .. }) => Err(format!(
                "{ant} started on message {id} already; cancel_for_human stops its prompt on thread {thread} once a \
                 daemon of {ant} serves"
            )),
            Recorded::Marked(MessageMark::Finished { .. }) => Err(format!("{ant} finished message {id} already")),
        }
    }

    async fn order_for_human(&self, args: OrderForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let op = match args.op.trim().replace('-', "_").as_str() {
            "pause" => AdminOp::Pause,
            "resume" => AdminOp::Resume,
            "restart" => AdminOp::Restart { now: false },
            "restart_now" => AdminOp::Restart { now: true },
            "read_settings" => AdminOp::ReadSettings,
            "read_memories" => AdminOp::ReadMemories,
            "set" => match args.changes {
                Some(changes) if !changes.is_empty() => AdminOp::Set {
                    changes: changes.into_iter().map(SetChange::into_field_change).collect::<Result<_, _>>()?,
                },
                _ => return Err("op set needs changes: the fields to change and the values they hold now".to_owned()),
            },
            other => {
                return Err(format!(
                    "unknown op {other}: pause, resume, restart, restart_now, read_settings, read_memories or set"
                ));
            }
        };
        let ant: UserId = args.ant.trim().parse().map_err(|e| format!("ant: {e}"))?;
        let machine: Machine = args.machine.trim().parse().map_err(|e| format!("machine: {e}"))?;
        let (kind, what) = (op.kind(), op.describe());
        let order =
            AdminOrder::new(self.ant.clone(), machine.clone(), ant.clone(), op, "").on_behalf(Some(human.clone()));
        let order = store.send_order(order).await.map_err(|e| format!("{e:#}"))?;
        let act = Act::Order { machine: machine.clone(), ant: ant.clone(), id: order.id }.qualified();
        let told = self
            .tell(
                &human,
                format!(
                    "ordered {what} of {ant} on {machine} for {human}: the daemon of {machine} holds it until {human} \
                     confirms it (`o` on the home of ants chat lists it, or `ants assistant confirm {act}`, or `--deny`)."
                ),
            )
            .await;
        Ok(format!(
            "Sent order {act} ({what}) for {human}. The daemon of {machine} carries it out only once {human} confirms \
             it or a pre-allow rule of {human} (`{ant}/order:{kind}`) covers it; until then it is held. `ants ant \
             orders {ant}` shows where it stands.{told}"
        ))
    }
}

#[tool_router(router = assist_ants_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "As the assistant of a human, list the messages waiting for an ant, as `ants queue` does: \
        those its daemon holds, those awaiting redelivery, and the inbox messages and channel posts waking it still in \
        the stream, each with its message id, thread, sender and place, and how many more wait unlisted. The daemon \
        answers only while the human's grant names this ant.")]
    async fn queue_for_human(&self, Parameters(args): Parameters<QueueForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.queue_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, cancel the prompt an ant runs on a thread for it; the \
        partial reply follows on the thread, and the ant's daemon tells the human.")]
    async fn cancel_for_human(
        &self,
        Parameters(args): Parameters<CancelForHuman>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.cancel_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, withdraw a message waiting for an ant (queue_for_human \
        lists them) for it, so the ant is never prompted with it; its thread is told once its turn comes, and the \
        ant's daemon tells the human. While no daemon of the ant serves, only a message of the human's to the ant is \
        withdrawn, by a record the ant's daemon honours before taking the message up.")]
    async fn withdraw_for_human(
        &self,
        Parameters(args): Parameters<WithdrawForHuman>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.withdraw_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, send an admin order for it to an ant's machine: pause, \
        resume, restart, restart_now, read_settings, read_memories, or set with `changes` (each a dotted `field`, \
        the new `value`, or none to remove it, and the `expected` value read_settings shows). The machine's daemon holds it until the human \
        confirms it or a pre-allow rule of the human covers it, then carries it out; the human is told.")]
    async fn order_for_human(&self, Parameters(args): Parameters<OrderForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.order_for_human(args).await))
    }
}
