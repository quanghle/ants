//! Why a read or write of a client's failed ([`Error`]): what was being done, and what went wrong with it
//! ([`ErrorKind`]).

use serde::{Deserialize, Serialize};

use crate::transport::TransportError;

/// Why a read or write failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    /// What was being done.
    pub what: String,
    pub kind: ErrorKind,
}

/// What went wrong with a read or write.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    #[error("{0}")]
    Transport(TransportError),
    /// JetStream answered with an error.
    #[error("JetStream refused it: {description} (code {code}, error {err_code})")]
    Refused { code: u16, err_code: u32, description: String },
    /// The server answered with a status in place of a message.
    #[error("answered {status} {description}")]
    Status { status: u16, description: String },
    /// An answer that does not parse, and why not.
    #[error("an answer that does not parse: {0}")]
    Malformed(String),
    /// A bucket's entry is a message a stream relayed ([`crate::relayed`]), not one its writer put.
    #[error("its entry is a message a stream relayed, not one its writer put")]
    Relayed,
    /// Other devices changed an entry each time this one tried to.
    #[error("it kept moving")]
    Conflict,
    /// What was asked for cannot be sent, and why not.
    #[error("{0}")]
    Invalid(String),
}

impl Error {
    pub(crate) fn new(what: &str, kind: ErrorKind) -> Self {
        Self { what: what.to_owned(), kind }
    }

    /// `error`, of the transport, while `what` was done.
    pub(crate) fn transport(what: &str, error: TransportError) -> Self {
        Self::new(what, ErrorKind::Transport(error))
    }

    pub(crate) fn status(what: &str, status: u16, description: Option<String>) -> Self {
        Self::new(what, ErrorKind::Status { status, description: description.unwrap_or_default() })
    }

    pub(crate) fn malformed(what: &str, why: impl ToString) -> Self {
        Self::new(what, ErrorKind::Malformed(why.to_string()))
    }

    pub(crate) fn invalid(what: &str, why: impl ToString) -> Self {
        Self::new(what, ErrorKind::Invalid(why.to_string()))
    }
}

/// What was being done, then what went wrong; a request that cannot be sent is told by its reason alone.
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            ErrorKind::Invalid(why) => f.write_str(why),
            kind => write!(f, "{}: {kind}", self.what),
        }
    }
}

impl std::error::Error for Error {}
