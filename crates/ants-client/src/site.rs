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
    /// The GitHub the page trusts to sign in at while it registers, `https://HOST`: it shows no sign-in page but this
    /// one's device login page ([`crate::device::registration::device_page`]). github.com when none is named.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github_url: Option<String>,
    /// The human a page connecting without credentials acts as: the one the CLI serving it acts as without them.
    /// None for a hub's page, which connects as its device.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub human: Option<String>,
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

    /// The GitHub the page trusts to sign in at.
    pub fn github_url(&self) -> &str {
        self.github_url.as_deref().unwrap_or(crate::device::registration::GITHUB_URL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_site_reads_back_as_written_and_names_no_bootstrap_credentials_or_github_it_has_none_of() {
        let local = Site { server: "ws://127.0.0.1:4281".to_owned(), bootstrap: None, github_url: None, human: None };
        assert_eq!(local.to_json(), "{\n  \"server\": \"ws://127.0.0.1:4281\"\n}\n");
        let named = Site { human: Some("alice".to_owned()), ..local.clone() };
        assert_eq!(Site::from_json(&named.to_json()).unwrap(), named);
        assert_eq!(Site::from_json(&local.to_json()).unwrap(), local);
        assert_eq!(local.github_url(), "https://github.com");
        let hub = Site {
            server: "wss://hub.example/nats".to_owned(),
            bootstrap: Some("-----BEGIN".to_owned()),
            github_url: Some("https://ghe.example".to_owned()),
            human: None,
        };
        assert_eq!(Site::from_json(&hub.to_json()).unwrap(), hub);
        assert_eq!(hub.github_url(), "https://ghe.example");
        assert!(Site::from_json("{}").is_err());
    }
}
