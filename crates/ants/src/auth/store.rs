//! What the store account exports and the bus's account imports, under the same names, so permissions name the
//! subjects they always did.
//!
//! - As services: the JetStream API, plain and in any domain's form (`$JS.<domain>.API`, which the store account
//!   maps to its own when the domain is its server's), acks and flow control replies, and every subject a stream
//!   stores: the KV buckets', the object stores', those of the ants stream, of sense's event logs and of a leaf's
//!   outbox. Their answers come back through the service import, so a reply subject always names one of the bus's
//!   account, never a stored one.
//! - As streams: the reply inboxes push consumers deliver to and the ants stream republishes stored inbox messages
//!   on, and the advisories dead letters come from.

use ants_proto::{OUTBOX_SUBJECTS, STREAM_SUBJECTS, subject};
use serde_json::{Value, json};

/// How long a JetStream API request may take its answers: above the longest wait of a pull request (async-nats waits
/// 30 seconds by default), as a pull, like a batched direct get, gets many answers to one request.
const API_RESPONSE_THRESHOLD: std::time::Duration = std::time::Duration::from_mins(5);

/// The services the store account exports, each with whether its answers come as a stream of messages.
fn services() -> Vec<(&'static str, bool)> {
    let mut services = vec![
        ("$JS.API.>", true),
        // Overlaps no other: `$JS.API.API.…` names no JetStream API request.
        ("$JS.*.API.>", true),
        ("$JS.ACK.>", false),
        ("$JS.FC.>", false),
        (subject::ALL_KV, false),
        ("$O.>", false),
        (subject::ALL_SENSE, false),
        (OUTBOX_SUBJECTS, false),
    ];
    services.extend(STREAM_SUBJECTS.iter().map(|s| (*s, false)));
    services
}

/// What the store account exports as streams: the reply inboxes of users and of hosts
/// ([`ants_proto::host_inbox_prefix`]), and the advisories of messages JetStream stops delivering
/// ([`subject::max_deliveries_advisories`]).
const STREAMS: [&str; 3] = ["_INBOX.>", "_INBOX_HOST.>", "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"];

/// The `exports` of the store account's JWT.
pub(super) fn exports() -> Value {
    let threshold = u64::try_from(API_RESPONSE_THRESHOLD.as_nanos()).unwrap_or(u64::MAX);
    let services = services().into_iter().map(|(subject, streamed)| {
        let mut export = json!({ "name": subject, "subject": subject, "type": "service" });
        if streamed {
            export["response_type"] = "Stream".into();
            export["response_threshold"] = threshold.into();
        }
        export
    });
    let streams = STREAMS.iter().map(|subject| json!({ "name": subject, "subject": subject, "type": "stream" }));
    services.chain(streams).collect()
}

/// The `imports` of the bus's account's JWT, from the store account `store`.
pub(super) fn imports(store: &str) -> Value {
    let services = services().into_iter().map(|(subject, _)| (subject, "service"));
    let streams = STREAMS.iter().map(|subject| (*subject, "stream"));
    services
        .chain(streams)
        .map(|(subject, kind)| json!({ "name": subject, "subject": subject, "account": store, "type": kind }))
        .collect()
}
