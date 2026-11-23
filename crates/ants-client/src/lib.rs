//! What a human's client does with ants' wire, whatever it runs on: the CLI, `ants chat`, a page in a browser.
//!
//! Three parts, the first two with no I/O:
//! - what a client shows, derived from what it reads: the entries of stored messages ([`entry`], [`parser`]) with
//!   their attachments ([`attachments`]) and what a sealed one shows as ([`sealed`]), the messages a stream relayed,
//!   which a reader refuses ([`relayed`]), and the human's inbox with what its read marker tells unread ([`inbox`]);
//! - what a human sends ([`sending`]), and the requests of its reads and writes with the reading of their answers
//!   ([`requests`]);
//! - those reads and writes themselves ([`client`]), over a transport of four calls that names no runtime
//!   ([`transport`]), and why one failed ([`error`]).
//!
//! Beside them, what the server of a page tells it of where to connect ([`site`]), and a device's credentials, their
//! registration and their renewal ([`device`]).
//!
//! The crate builds natively and for `wasm32-unknown-unknown`. A binding, for a browser or another platform, wraps
//! [`client::Client`] and implements [`transport::Transport`]; nothing here names the binding's types.
//!
//! Each item has one path, through its module: the crate's root re-exports none.

pub mod attachments;
pub mod client;
pub mod device;
pub mod entry;
pub mod error;
pub mod inbox;
pub mod parser;
pub mod relayed;
pub mod requests;
pub mod sealed;
pub mod sending;
pub mod site;
pub mod text;
pub mod transport;

#[cfg(test)]
mod tests {
    //! Tests of what the crate may depend on: no NATS client, runtime, terminal or browser binding.

    #[test]
    fn the_manifest_names_no_nats_client_runtime_terminal_or_browser_binding() {
        let manifest = include_str!("../Cargo.toml");
        for name in ["async-nats", "tokio", "ratatui", "wasm-bindgen", "js-sys"] {
            assert!(!manifest.contains(name), "{name}");
        }
    }
}
