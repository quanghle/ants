//! Each ant's provider, what holds its identity, memories and work ([`ants_proto::provider`]): built-in sugar for
//! every ant. One provider serves all the ants of a process, connected on first use; each call names its ant.

use std::sync::Arc;

use ants_proto::UserId;
use ants_proto::provider::{
    BoxFuture, Capabilities, Connected, Host, Info, InitializeParams, Provider, RpcError, WakeRequest, Woken,
};
use ants_sugar::Sugar;
use tokio::sync::OnceCell;

pub mod brief;
pub mod context;

/// The capabilities ants declares at initialize: those it calls. Wake, which ants serves, waits for its server.
pub const USES: Capabilities = Capabilities { wake: false, ..Capabilities::ALL };

/// How ants names itself at initialize.
pub fn client() -> Info {
    Info { name: "ants".to_owned(), version: env!("CARGO_PKG_VERSION").to_owned() }
}

static SUGAR: OnceCell<Option<Connected>> = OnceCell::const_new();

/// The provider of `ant`, connected: none when it cannot be, the ant then going without one.
pub async fn of(_ant: &UserId) -> Option<Connected> {
    SUGAR.get_or_init(|| connect(Arc::new(Sugar::new()))).await.clone()
}

/// Initialize `provider` and agree with it on what both sides use.
pub async fn connect(provider: Arc<dyn Provider>) -> Option<Connected> {
    let params = InitializeParams::new(client(), USES);
    match Connected::connect(provider, params, Arc::new(Ants)).await {
        Ok(connected) => {
            tracing::debug!(provider = connected.name(), capabilities = ?connected.capabilities(), "provider connected");
            Some(connected)
        }
        Err(e) => {
            tracing::warn!(error = %e, "no provider: connecting it failed");
            None
        }
    }
}

/// What ants serves its providers.
struct Ants;

impl Host for Ants {
    fn wake(&self, _request: WakeRequest) -> BoxFuture<'_, Result<Woken, RpcError>> {
        Box::pin(std::future::ready(Err(RpcError::unavailable("ants does not wake ants for providers yet"))))
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::provider::Capability;

    use super::*;

    #[tokio::test]
    async fn every_ant_gets_sugar_with_its_tools_and_brief() {
        let provider = of(&"dev-a".parse().unwrap()).await.expect("sugar connects");
        assert_eq!(provider.name(), ants_sugar::NAME);
        assert_eq!(provider.capabilities(), ants_sugar::CAPABILITIES);
        assert!(provider.has(Capability::Tools) && provider.has(Capability::Context));
        let other = of(&"dev-b".parse().unwrap()).await.unwrap();
        assert_eq!(other.agreed(), provider.agreed());
    }
}
