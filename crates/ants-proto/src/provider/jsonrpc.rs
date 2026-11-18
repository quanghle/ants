//! JSON-RPC 2.0 messages, as the provider protocol frames them: one [`Message`] per line ([`Message::from_line`],
//! [`Message::to_line`]).

use std::fmt;

use serde::de::{DeserializeOwned, Deserializer};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

use super::Capability;

/// The `jsonrpc` member of every message: `"2.0"`, and nothing else.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Version;

impl Serialize for Version {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("2.0")
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let version = String::deserialize(deserializer)?;
        if version == "2.0" {
            return Ok(Self);
        }
        Err(serde::de::Error::custom(format!("jsonrpc {version:?}, not \"2.0\"")))
    }
}

/// A request's id, which its response repeats.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Number(n) => write!(f, "{n}"),
            Self::String(s) => f.write_str(s),
        }
    }
}

/// A call that expects a [`Response`] with its id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub jsonrpc: Version,
    pub id: RequestId,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl Request {
    pub fn new(id: RequestId, method: &str, params: &impl Serialize) -> Result<Self, serde_json::Error> {
        Ok(Self { jsonrpc: Version, id, method: method.to_owned(), params: to_params(params)? })
    }

    /// The params as `T`, an [`RpcError::INVALID_PARAMS`] error when they are not one.
    pub fn params<T: DeserializeOwned>(&self) -> Result<T, RpcError> {
        params(self.params.as_ref())
    }
}

/// A call that expects no response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Notification {
    pub jsonrpc: Version,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl Notification {
    pub fn new(method: &str, params: &impl Serialize) -> Result<Self, serde_json::Error> {
        Ok(Self { jsonrpc: Version, method: method.to_owned(), params: to_params(params)? })
    }

    /// The params as `T`, an [`RpcError::INVALID_PARAMS`] error when they are not one.
    pub fn params<T: DeserializeOwned>(&self) -> Result<T, RpcError> {
        params(self.params.as_ref())
    }
}

/// `params` as JSON, none when null: JSON-RPC params, when present, are an object or an array.
fn to_params(params: &impl Serialize) -> Result<Option<Value>, serde_json::Error> {
    Ok(Some(serde_json::to_value(params)?).filter(|params| !params.is_null()))
}

fn params<T: DeserializeOwned>(params: Option<&Value>) -> Result<T, RpcError> {
    T::deserialize(params.unwrap_or(&Value::Null)).map_err(|e| RpcError::invalid_params(&e.to_string()))
}

/// The answer to a [`Request`]: its result or an error. The id is null only when the request's could not be read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub jsonrpc: Version,
    pub id: Option<RequestId>,
    #[serde(flatten)]
    pub outcome: Outcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Result(Value),
    Error(RpcError),
}

impl Response {
    /// The response to request `id` with `result`, or with an [`RpcError::INTERNAL_ERROR`] when it does not serialize.
    pub fn new(id: RequestId, result: Result<impl Serialize, RpcError>) -> Self {
        let outcome = match result.map(serde_json::to_value) {
            Ok(Ok(value)) => Outcome::Result(value),
            Ok(Err(e)) => Outcome::Error(RpcError::internal(&format!("writing the result: {e}"))),
            Err(error) => Outcome::Error(error),
        };
        Self { jsonrpc: Version, id: Some(id), outcome }
    }

    /// The response to a request that could not be read, whose id is unknown.
    pub fn unread(error: RpcError) -> Self {
        Self { jsonrpc: Version, id: None, outcome: Outcome::Error(error) }
    }

    /// The result as `T`, or the error.
    pub fn into_result<T: DeserializeOwned>(self) -> Result<T, RpcError> {
        match self.outcome {
            Outcome::Result(value) => {
                serde_json::from_value(value).map_err(|e| RpcError::internal(&format!("reading the result: {e}")))
            }
            Outcome::Error(error) => Err(error),
        }
    }
}

/// One line of the protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum Message {
    Request(Request),
    Notification(Notification),
    Response(Response),
}

impl Message {
    /// Read one line: a request when it has a method and an id, a notification when it has a method alone, else a
    /// response. A line that is no JSON is an [`RpcError::PARSE_ERROR`], one that is no message an
    /// [`RpcError::INVALID_REQUEST`].
    pub fn from_line(line: &str) -> Result<Self, RpcError> {
        let value: Value = serde_json::from_str(line).map_err(|e| RpcError::parse_error(&e.to_string()))?;
        let Some(object) = value.as_object() else { return Err(RpcError::invalid_request("not an object")) };
        let invalid = |e: serde_json::Error| RpcError::invalid_request(&e.to_string());
        match (object.contains_key("method"), object.contains_key("id")) {
            (true, true) => serde_json::from_value(value).map(Self::Request).map_err(invalid),
            (true, false) => serde_json::from_value(value).map(Self::Notification).map_err(invalid),
            (false, _) => serde_json::from_value(value).map(Self::Response).map_err(invalid),
        }
    }

    /// The message as one line, ending in a newline.
    pub fn to_line(&self) -> String {
        let mut line = serde_json::to_string(self).unwrap_or_else(|e| {
            let error = Self::Response(Response::unread(RpcError::internal(&format!("writing a message: {e}"))));
            serde_json::to_string(&error).unwrap_or_default()
        });
        line.push('\n');
        line
    }
}

/// The JSON-RPC error object, which every provider method fails with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message} (code {code})")]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub const PARSE_ERROR: i64 = -32_700;
    pub const INVALID_REQUEST: i64 = -32_600;
    pub const METHOD_NOT_FOUND: i64 = -32_601;
    pub const INVALID_PARAMS: i64 = -32_602;
    pub const INTERNAL_ERROR: i64 = -32_603;
    /// The method's capability was not agreed at initialize.
    pub const NOT_AGREED: i64 = -32_001;
    /// No protocol version offered is one the provider speaks; `data` lists those it does.
    pub const UNSUPPORTED_VERSION: i64 = -32_002;
    /// The provider is not up: starting, restarting or stopped.
    pub const UNAVAILABLE: i64 = -32_003;
    /// The thread a wake request names is not one the ant has a session for.
    pub const UNKNOWN_THREAD: i64 = -32_004;

    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), data: None }
    }

    #[must_use]
    pub fn with_data(self, data: Value) -> Self {
        Self { data: Some(data), ..self }
    }

    pub fn parse_error(detail: &str) -> Self {
        Self::new(Self::PARSE_ERROR, format!("parse error: {detail}"))
    }

    pub fn invalid_request(detail: &str) -> Self {
        Self::new(Self::INVALID_REQUEST, format!("invalid request: {detail}"))
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(Self::METHOD_NOT_FOUND, format!("method not found: {method}"))
    }

    pub fn invalid_params(detail: &str) -> Self {
        Self::new(Self::INVALID_PARAMS, format!("invalid params: {detail}"))
    }

    pub fn internal(detail: &str) -> Self {
        Self::new(Self::INTERNAL_ERROR, format!("internal error: {detail}"))
    }

    pub fn not_agreed(capability: Capability) -> Self {
        Self::new(Self::NOT_AGREED, format!("capability {capability} was not agreed at initialize"))
    }

    pub fn unsupported_version(offered: &[u32], supported: &[u32]) -> Self {
        Self::new(Self::UNSUPPORTED_VERSION, format!("no common protocol version: offered {offered:?}"))
            .with_data(serde_json::json!({ "supported": supported }))
    }

    pub fn unavailable(why: &str) -> Self {
        Self::new(Self::UNAVAILABLE, format!("provider unavailable: {why}"))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn line(value: &Value) -> String {
        value.to_string()
    }

    #[test]
    fn lines_read_as_requests_notifications_and_responses() {
        let request = Message::from_line(&line(&json!({"jsonrpc": "2.0", "id": 1, "method": "brief", "params": {}})));
        let Ok(Message::Request(request)) = request else { panic!("not a request: {request:?}") };
        assert_eq!((request.id, request.method.as_str()), (RequestId::Number(1), "brief"));

        let notification = Message::from_line(&line(&json!({"jsonrpc": "2.0", "method": "log"})));
        assert!(matches!(notification, Ok(Message::Notification(n)) if n.method == "log" && n.params.is_none()));

        let null_result = Message::from_line(&line(&json!({"jsonrpc": "2.0", "id": "a", "result": null})));
        let Ok(Message::Response(response)) = null_result else { panic!("not a response: {null_result:?}") };
        assert_eq!(response.outcome, Outcome::Result(Value::Null));
        assert_eq!(response.id, Some(RequestId::String("a".to_owned())));

        let error = json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": "parse error: x"}});
        let Ok(Message::Response(response)) = Message::from_line(&line(&error)) else { panic!("not a response") };
        assert_eq!(response.into_result::<Value>().unwrap_err().code, RpcError::PARSE_ERROR);
    }

    #[test]
    fn bad_lines_are_refused() {
        assert_eq!(Message::from_line("{").unwrap_err().code, RpcError::PARSE_ERROR);
        assert_eq!(Message::from_line("[]").unwrap_err().code, RpcError::INVALID_REQUEST);
        let wrong_version = line(&json!({"jsonrpc": "1.0", "id": 1, "method": "brief"}));
        assert_eq!(Message::from_line(&wrong_version).unwrap_err().code, RpcError::INVALID_REQUEST);
        let neither = line(&json!({"jsonrpc": "2.0", "id": 1}));
        assert_eq!(Message::from_line(&neither).unwrap_err().code, RpcError::INVALID_REQUEST);
    }

    #[test]
    fn messages_make_one_line_each_and_read_back() {
        let request = Request::new(RequestId::Number(7), "tools/call", &json!({"text": "two\nlines"})).unwrap();
        let ok = Response::new(RequestId::Number(7), Ok::<_, RpcError>(json!({"thread": "t"})));
        let failed = Response::new(RequestId::Number(8), Err::<Value, _>(RpcError::method_not_found("nope")));
        for message in [Message::Request(request), Message::Response(ok), Message::Response(failed)] {
            let line = message.to_line();
            assert_eq!(line.matches('\n').count(), 1, "{line}");
            assert!(line.ends_with('\n'));
            assert_eq!(Message::from_line(&line).unwrap(), message);
        }
    }

    #[test]
    fn params_and_results_read_as_their_types() {
        #[derive(Debug, Deserialize)]
        struct Params {
            #[expect(dead_code, reason = "only parsed")]
            n: u32,
        }
        let request = Request::new(RequestId::Number(1), "x", &json!({"n": "not a number"})).unwrap();
        assert_eq!(request.params::<Params>().unwrap_err().code, RpcError::INVALID_PARAMS);
        let response = Response::new(RequestId::Number(1), Ok::<_, RpcError>(json!("text")));
        assert_eq!(response.into_result::<u32>().unwrap_err().code, RpcError::INTERNAL_ERROR);
    }
}
