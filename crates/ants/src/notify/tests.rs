//! Tests of notifications: sinks, quiet hours, mutes, snoozes, stand-ins, digests, folded replies, questions, sealed
//! messages, signed links, buttons, and exec and http sinks.

mod config;
mod holding;
mod notices;
mod sinks;

use std::assert_matches;
use std::path::Path;

use ants_proto::{Away, DeviceId, Envelope, Escalation, EscalationReason, ThreadId, ToolCall, Verdict};
use ants_test_support::user as p;
use data_encoding::HEXLOWER;
#[cfg(unix)]
use http_body_util::BodyExt as _;
#[cfg(unix)]
use http_body_util::Full;
#[cfg(unix)]
use hyper::body::Bytes;
#[cfg(unix)]
use hyper::http::Request;
#[cfg(unix)]
use hyper::http::{Response, StatusCode};
#[cfg(unix)]
use hyper_util::rt::TokioIo;
use jiff::tz::TimeZone;
use ring::hmac;
use serde_json::{Value, json};

use super::config::{NTFY, QuietHours, Sink, TELEGRAM};
use super::links::{Action, LinkError, details, html};
use super::notices::{Button, counted, notice_json};
use super::sinks::{Delivery, cut};
use super::*;

fn human(text: &str) -> Result<Human> {
    let entry: HumanEntry = toml::from_str(text)?;
    let env = |name: &str| match name {
        "SLACK_URL" => Some("https://hooks.slack.com/services/T0/B0/xyz\n".to_owned()),
        "BOT_TOKEN" => Some("123:abc".to_owned()),
        "HOOK_SECRET" => Some("0123456789abcdef-hook".to_owned()),
        _ => None,
    };
    Human::from_entry(entry, None, &|p: &Path| Path::new("/etc/ants").join(p), &env)
}

fn error(text: &str) -> String {
    format!("{:#}", human(text).err().expect("an error"))
}

const NTFY_SINK: &str = "[[notify]]\nkind = \"ntfy\"\ntopic = \"alice-ants\"\n";

fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text).unwrap().with_timezone(&Utc)
}

/// Where a delivery `POST`s, its bearer token and its JSON body.
fn posted(delivery: Delivery) -> (String, Option<String>, Value) {
    let Delivery::Post { url, headers, body } = delivery else { panic!("not a POST") };
    let bearer = headers.iter().find(|(name, _)| *name == "authorization");
    let bearer = bearer.map(|(_, value)| value.strip_prefix("Bearer ").unwrap().to_owned());
    (url, bearer, serde_json::from_slice(&body).unwrap())
}

fn links() -> Links {
    Links::new("https://hub.example/hooks/", "0123456789abcdef-secret").unwrap()
}

fn action() -> Action {
    Action {
        ant: p("dev-a"),
        id: Uuid::now_v7(),
        seq: 7,
        verdict: Verdict::AllowOnce,
        by: p("alice"),
        expires: Utc::now().timestamp() + 60,
    }
}

fn request() -> ApprovalRequest {
    let tool = ToolCall {
        kind: "execute".to_owned(),
        title: "rm -rf <build>".to_owned(),
        locations: vec!["src/a.rs".to_owned()],
        input: Some("{\"command\":\"rm -rf build\"}".to_owned()),
    };
    let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
    ApprovalRequest::new(p("dev-a"), ThreadId::generate(), tool, options, Duration::from_secs(600))
}
