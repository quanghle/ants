//! One webhook's requests: whether they are authorized, and the [`Event`] each one makes, with its id and text.

use std::time::{Duration, SystemTime};

use ants_daemon::secrets::same;
use anyhow::Result;
use data_encoding::{BASE64, HEXLOWER, HEXLOWER_PERMISSIVE};
use hyper::http::{HeaderMap, HeaderName, StatusCode, header};
use ring::{digest, hmac};
use serde_json::Value;

use super::config::{Auth, IdSource, Piece, Webhook};
use super::{ID_HASH_LEN, WEBHOOK_ID, WEBHOOK_SIGNATURE, WEBHOOK_TIMESTAMP};

/// A request refused, with its status and why.
pub(super) struct Refusal(pub(super) StatusCode, pub(super) String);

pub(super) fn refuse<T>(status: StatusCode, why: impl Into<String>) -> Result<T, Refusal> {
    Err(Refusal(status, why.into()))
}

/// An authenticated event, ready to deliver.
pub(super) struct Event {
    /// Hash of its id, in its key and `Nats-Msg-Id`.
    pub(super) hash: String,
    pub(super) text: String,
}

#[cfg(test)]
impl Webhook {
    /// Whether a request with `headers` and `body` gets through authentication.
    pub(crate) fn accepts(&self, headers: &HeaderMap, body: &[u8]) -> bool {
        self.authorized_before_body(headers) && self.authorized(headers, body)
    }

    /// The hash of the id and the text of the event in an authenticated request.
    pub(crate) fn event_of(&self, headers: &HeaderMap, body: &[u8]) -> (String, String) {
        let event = self.event(headers, body).ok().expect("an event");
        (event.hash, event.text)
    }
}

impl Webhook {
    /// Whether the threads it starts are narrowed to fewer tools than all.
    pub fn narrows(&self) -> bool {
        self.allow.as_deref().is_none_or(|allow| !ants_daemon::harness::allows_all(allow))
    }

    pub(super) fn authorized_before_body(&self, headers: &HeaderMap) -> bool {
        match &self.auth {
            Auth::Hmac { .. } => true,
            Auth::Bearer(token) => headers
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .is_some_and(|given| same(given.trim().as_bytes(), token.as_bytes())),
            Auth::Token { secret, header } => headers
                .get(header)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|given| same(given.trim().as_bytes(), secret.as_bytes())),
            Auth::Standard { tolerance, .. } => {
                headers.get(WEBHOOK_ID).is_some_and(|id| !id.is_empty())
                    && headers.contains_key(WEBHOOK_SIGNATURE)
                    && recent(headers, *tolerance, SystemTime::now())
            }
        }
    }

    pub(super) fn authorized(&self, headers: &HeaderMap, body: &[u8]) -> bool {
        match &self.auth {
            Auth::Hmac { secret, header } => {
                let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
                let signature = headers
                    .get(header)
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .map(|v| v.strip_prefix("sha256=").unwrap_or(v))
                    .and_then(|hex| HEXLOWER_PERMISSIVE.decode(hex.as_bytes()).ok());
                signature.is_some_and(|signature| hmac::verify(&key, body, &signature).is_ok())
            }
            Auth::Standard { key, .. } => {
                let (Some(id), Some(timestamp), Some(signatures)) =
                    (headers.get(WEBHOOK_ID), headers.get(WEBHOOK_TIMESTAMP), headers.get(WEBHOOK_SIGNATURE))
                else {
                    return false;
                };
                let mut signed = hmac::Context::with_key(&hmac::Key::new(hmac::HMAC_SHA256, key));
                for part in [id.as_bytes(), b".", timestamp.as_bytes(), b".", body] {
                    signed.update(part);
                }
                let expected = signed.sign();
                String::from_utf8_lossy(signatures.as_bytes())
                    .split_ascii_whitespace()
                    .filter_map(|signature| signature.strip_prefix("v1,"))
                    .filter_map(|signature| BASE64.decode(signature.as_bytes()).ok())
                    .any(|signature| same(&signature, expected.as_ref()))
            }
            Auth::Bearer(_) | Auth::Token { .. } => true,
        }
    }

    /// The event in an authenticated request: its id and its text from the template.
    pub(super) fn event(&self, headers: &HeaderMap, body: &[u8]) -> Result<Event, Refusal> {
        let wants_json = matches!(self.id, IdSource::Json(_)) || self.text.iter().any(|p| matches!(p, Piece::Json(_)));
        let parsed = if wants_json {
            match serde_json::from_slice::<Value>(body) {
                Ok(value) => Some(value),
                Err(e) => return refuse(StatusCode::BAD_REQUEST, format!("the body is no JSON: {e}")),
            }
        } else {
            None
        };
        let id = match &self.id {
            IdSource::BodySha256 => HEXLOWER.encode(digest::digest(&digest::SHA256, body).as_ref()),
            IdSource::Header(name) => header_text(headers, name),
            IdSource::Json(pointer) => parsed.as_ref().and_then(|v| v.pointer(pointer)).map_or_default(json_text),
        };
        if id.is_empty() {
            return refuse(StatusCode::BAD_REQUEST, "the event has no id");
        }
        let mut text = String::new();
        for piece in &self.text {
            match piece {
                Piece::Text(s) => text.push_str(s),
                Piece::Body => match std::str::from_utf8(body) {
                    Ok(body) => text.push_str(body),
                    Err(_) => return refuse(StatusCode::UNSUPPORTED_MEDIA_TYPE, "the body is no UTF-8 text"),
                },
                Piece::Id => text.push_str(&id),
                Piece::Webhook => text.push_str(self.name.as_str()),
                Piece::Header(name) => text.push_str(&header_text(headers, name)),
                Piece::Json(pointer) => {
                    text.push_str(&parsed.as_ref().and_then(|v| v.pointer(pointer)).map_or_default(json_text));
                }
            }
        }
        let hash = HEXLOWER.encode(digest::digest(&digest::SHA256, id.as_bytes()).as_ref())[..ID_HASH_LEN].to_owned();
        Ok(Event { hash, text })
    }
}

/// Whether `webhook-timestamp`, in Unix seconds, is within `tolerance` of `now`, before or after it.
pub(super) fn recent(headers: &HeaderMap, tolerance: Duration, now: SystemTime) -> bool {
    let Some(timestamp) = headers.get(WEBHOOK_TIMESTAMP).and_then(|v| v.to_str().ok()?.parse::<u64>().ok()) else {
        return false;
    };
    let now = now.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs();
    timestamp.abs_diff(now) <= tolerance.as_secs()
}

fn header_text(headers: &HeaderMap, name: &HeaderName) -> String {
    headers.get(name).map_or_default(|v| String::from_utf8_lossy(v.as_bytes()).trim().to_owned())
}

/// A JSON value as text: a string as itself, null as nothing, anything else as JSON.
fn json_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}
