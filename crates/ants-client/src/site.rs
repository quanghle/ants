//! Where a page connects, as whatever serves it says in [`FILE`] beside it: `ants web` on a single machine, the
//! directory `ants web export` writes for a hub's reverse proxy.

use serde::{Deserialize, Serialize};

/// The file beside the page that holds its [`Site`], as JSON.
pub const FILE: &str = "site.json";

/// What the server of a page tells it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Site {
    /// The WebSocket address of the NATS server the page connects to: ws:// or wss://.
    pub server: String,
    /// The text of the hub's bootstrap credentials file (`ants auth bootstrap`), public, with which the page asks
    /// `ants signer` to register its device; none on a server the page connects to without credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<String>,
}

impl Site {
    /// The site as [`FILE`] holds it.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("a site serializes") + "\n"
    }

    /// The site [`FILE`] holds.
    pub fn from_json(text: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_site_reads_back_as_written_and_names_no_bootstrap_credentials_it_has_none_of() {
        let local = Site { server: "ws://127.0.0.1:4281".to_owned(), bootstrap: None };
        assert_eq!(local.to_json(), "{\n  \"server\": \"ws://127.0.0.1:4281\"\n}\n");
        assert_eq!(Site::from_json(&local.to_json()).unwrap(), local);
        let hub = Site { server: "wss://hub.example/nats".to_owned(), bootstrap: Some("-----BEGIN".to_owned()) };
        assert_eq!(Site::from_json(&hub.to_json()).unwrap(), hub);
        assert!(Site::from_json("{}").is_err());
    }
}
