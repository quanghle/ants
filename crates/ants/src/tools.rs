//! `ants tools`: the tool requests ants escalate to the humans, and their answers.
//!
//! Requests and decisions both live in the ANTS stream, so the pending ones are read back by subject, without consuming
//! anything: every human sees every request on `ants.approvals.<ant>` that has not expired and has no decision on
//! `ants.approvals.<ant>.<request id>`. An expired request needs no answer: its ant already rejected it. The
//! ant of a request is the one its subject names, as credentials pin it there; a request whose body names another
//! is skipped with a warning.
//!
//! Any human decides, and the first decision wins: a decision is stored only while the request's subject holds none
//! ([`answer_request`]), so of two humans answering at once, the second is told the request was already answered.
//! The ant acts on the stored decision, not on the first it hears, since a refused publish still reaches subscribers.
//!
//! An ant of the requester's escalate_to may decide too: its decision travels as an envelope to the requester's inbox,
//! `ants.inbox.<requester>.<decider>`, where humans read it back. A request such a decision came to first, as the
//! stream ordered them and before the request expired, is decided too ([`State::DecidedByAnt`]), and a human's later
//! answer is refused. The chain is the ant's colony entry's, else the one kept in `ants_chains`, as the ant takes a
//! decision only from its own chain.
//!
//! A human approving the same call of an ant again and again is shown the allow rule that would spare it
//! ([`suggestion`]): once [`answer::SUGGEST_AFTER`] requests of the ant for the same rule
//! ([`ants_daemon::permission::rule_for`]) are approved, counting the one at hand, as the stream holds them. `ants tools
//! approve --rule` approves and adds the rule to the ant's allow list in the config file, then reloads the daemon.
//!
//! Request ids are unique only per ant: any ant can publish a request reusing another's id. So a request is
//! written `<ant>/<id>`, and a bare id (or start of one) matching requests of several ants is refused with
//! the qualified ids to choose from.
//!
//! A human's assistant decides for its human with such an envelope too, claiming to act for the human
//! (`on_behalf_of`): the requester holds the decision until the human consents to it, by confirming it (`ants assistant
//! confirm`) or by a pre-allow rule covering the call (`ants assistant allow`), as the daemon's `approvals` module
//! tells. A request with such a decision the human consented to before it expired is decided too, by the assistant for
//! the human ([`State::DecidedByAnt`] with the decision's `on_behalf_of` set); one still waiting for consent stays
//! pending, with its held decisions ([`held`]). The human's records are read as stored: only the human's credentials
//! write them, and the requester's daemon verifies them before it applies anything.
//!
//! `ants tools reject --note` sends the ant a note on the request's thread once the rejection is stored
//! (`send_note` of `cmd/tools.rs`), as ACP's answer to a permission request carries no message.
//!
//! `ants tools list --follow` and `ants inbox --follow` follow the requests alike ([`follow`]); the inbox also shows
//! how each ends.

use ants_proto::{ApprovalDecision, UserId};

mod answer;
mod assisted;
mod following;
mod requests;
mod text;

pub use answer::{Answer, Suggestion, answer_request, suggestion};
pub use assisted::HeldAct;
pub(crate) use assisted::{deciders, record};
pub use following::{Update, follow, render};
pub use requests::{Query, find, held, pending, qualified, select, state};
#[cfg(test)]
pub use requests::{decision, requests};
pub(crate) use text::{details, row, suggestion_text};

/// Where a request stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Pending,
    /// A human's decision.
    Decided(ApprovalDecision),
    /// The decision of an ant of the requester's escalate_to, the one named; or of a human's assistant, named, for
    /// the human its `on_behalf_of` names, who consented to it.
    DecidedByAnt(UserId, ApprovalDecision),
    /// No decision in time; the ant rejected it.
    Expired,
}

#[cfg(test)]
mod tests;
