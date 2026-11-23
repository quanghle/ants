//! The `as_str` of each wire enum is the name serde writes for the variant, for every variant: a hand-written table
//! cannot drift from the wire.

use ants_proto::{
    Activity, ChannelNotify, DecidedBy, EscalationReason, Following, NoticeKind, OrderState, PromptEnd, RatingOutcome,
    State, StepStage, TipVerdict, Verdict,
};

/// Asserts that `as_str` of each variant listed is what serde writes for it. A variant missing from the list fails to
/// compile.
macro_rules! named_as_on_the_wire {
    ($enum:ident: $($variant:ident),+ $(,)?) => {
        for value in [$($enum::$variant),+] {
            match value {
                $($enum::$variant)|+ => {}
            }
            assert_eq!(serde_json::to_value(value).unwrap(), value.as_str(), "{value:?}");
        }
    };
}

#[test]
fn every_wire_enum_s_as_str_is_the_name_serde_writes() {
    named_as_on_the_wire!(Activity: Idle, Working, Unknown);
    named_as_on_the_wire!(ChannelNotify: All, Humans, Mentions);
    named_as_on_the_wire!(DecidedBy: Human, Chain, Expiry);
    named_as_on_the_wire!(EscalationReason:
        TurnBudget, Cap, Notes, DeadLetter, Approval, Asked, ContextWindow, Harness, Storage, Question, Unknown
    );
    named_as_on_the_wire!(Following: Inbox, Approvals);
    named_as_on_the_wire!(NoticeKind:
        Spend, SignedOut, Quota, Failing, NoFallback, GaveUp, Withdrawn, Expired, Unserved, Assistant, InPlace,
        RequestExpired, Steered, McpSignIn, RunSpent
    );
    named_as_on_the_wire!(OrderState: Pending, Held, Applied, Refused);
    named_as_on_the_wire!(PromptEnd: Replied, NoReply, Cancelled, Capped, Failed, Unknown);
    named_as_on_the_wire!(RatingOutcome: Worked, Partly, Failed);
    named_as_on_the_wire!(State: Working, WaitingApproval, Idle, Failed, SignedOut);
    named_as_on_the_wire!(StepStage: Started, Answered, Failed, Dropped);
    named_as_on_the_wire!(TipVerdict: Helpful, Stale, Wrong);
    named_as_on_the_wire!(Verdict: AllowOnce, AllowAlways, Reject);
}
