//! [`AdminOp::ExportProvider`] and [`AdminOp::ImportProvider`]: move what the ant's provider holds of it
//! ([`crate::provider::Providers::of`]) with `ants ant export` and `ants ant import`, which reach the provider only
//! through the daemon serving the ant.
//!
//! An export goes to the attachments' store ([`payload`]), as it may be far larger than an outcome holds; the outcome
//! says where ([`ProviderExport`]). Like a read of memories, an export is refused as stale once it waited longer
//! than [`ants_proto::MEMORIES_FRESH`] for the machine's daemon. An import takes an export the admin stored, of the
//! provider of the same name only; whatever stops it refuses the order with why.

use std::time::Duration;

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::provider::{AntRef, Capability, Connected, Export, ImportRequest};
use ants_proto::{AdminOrder, AdminOutcome, AdminPayload, ProviderExport};
use async_nats::jetstream;
use chrono::{DateTime, Utc};

use super::memories::{cut_error, stale};
use super::{NO_CONFIG, Orders, applied, payload, refused};
use crate::config;

/// How long the ant's provider may take to export or import it.
const PROVIDER_TIMEOUT: Duration = Duration::from_secs(60);

pub(super) async fn export(
    js: &jetstream::Context,
    orders: &Orders,
    order: &AdminOrder,
    identity: &str,
    stored_at: DateTime<Utc>,
) -> AdminOutcome {
    if let Some(stale) = stale(stored_at, "exports from providers") {
        return refused(order, &stale);
    }
    if let Err(e) = listed(orders, order) {
        return refused(order, &e);
    }
    export_from(js, orders, order, identity, orders.providers.of(&order.ant).await.as_ref()).await
}

/// The export of the ant of `order` from `provider`, stored.
pub(super) async fn export_from(
    js: &jetstream::Context,
    orders: &Orders,
    order: &AdminOrder,
    identity: &str,
    provider: Option<&Connected>,
) -> AdminOutcome {
    let mut result = ProviderExport::default();
    if let Some(connected) = provider {
        result.provider = Some(connected.name().to_owned());
        if connected.has(Capability::Export) {
            let ant = AntRef { ant: order.ant.clone(), identity: identity.to_owned() };
            let export = match call(connected, connected.export(ant)).await {
                Ok(export) if export.provider == connected.name() => export,
                Ok(export) => {
                    return refused(
                        order,
                        &format!("provider {} exported as provider {}", connected.name(), export.provider),
                    );
                }
                Err(e) => return refused(order, &e),
            };
            let bytes = match serde_json::to_vec(&export) {
                Ok(bytes) => bytes,
                Err(e) => return refused(order, &format!("writing the export: {e}")),
            };
            match payload::put(js, &orders.machine, &bytes).await {
                Ok(stored) => result.payload = Some(stored),
                Err(e) => return refused(order, &format!("{e:#}")),
            }
        }
    }
    match serde_json::to_value(&result) {
        Ok(result) => applied(order, Some(result)),
        Err(e) => refused(order, &format!("writing the outcome: {e}")),
    }
}

pub(super) async fn import(
    js: &jetstream::Context,
    orders: &Orders,
    order: &AdminOrder,
    identity: &str,
    payload: &AdminPayload,
) -> AdminOutcome {
    if let Err(e) = listed(orders, order) {
        return refused(order, &e);
    }
    import_into(js, orders, order, identity, payload, orders.providers.of(&order.ant).await.as_ref()).await
}

/// Hand `provider` the export stored at `payload` for the ant of `order`.
pub(super) async fn import_into(
    js: &jetstream::Context,
    orders: &Orders,
    order: &AdminOrder,
    identity: &str,
    payload: &AdminPayload,
    provider: Option<&Connected>,
) -> AdminOutcome {
    let ant = &order.ant;
    let Some(connected) = provider else { return refused(order, &format!("{ant} has no provider to import into")) };
    let name = connected.name();
    if !connected.has(Capability::Export) {
        return refused(order, &format!("provider {name} of {ant} takes no import"));
    }
    if payload.machine != orders.machine {
        return refused(order, &format!("the export was stored for machine {}, not this one", payload.machine));
    }
    let bytes = match payload::get(js, payload).await {
        Ok(bytes) => bytes,
        Err(e) => return refused(order, &format!("{e:#}")),
    };
    let export: Export = match serde_json::from_slice(&bytes) {
        Ok(export) => export,
        Err(e) => return refused(order, &format!("reading the export: {e}")),
    };
    if export.provider != name {
        return refused(
            order,
            &format!("the export is of provider {}, and the provider of {ant} is {name}", export.provider),
        );
    }
    let request = ImportRequest { ant: ant.clone(), identity: identity.to_owned(), export };
    match call(connected, connected.import(request)).await {
        Ok(_) => applied(order, None),
        Err(e) => refused(order, &e),
    }
}

/// What the provider `connected` answered within [`PROVIDER_TIMEOUT`], else why not.
async fn call<T>(
    connected: &Connected,
    answer: impl Future<Output = Result<T, ants_proto::provider::RpcError>>,
) -> Result<T, String> {
    match tokio::time::timeout(PROVIDER_TIMEOUT, answer).await {
        Err(_) => Err(format!("provider {} took over {}s", connected.name(), PROVIDER_TIMEOUT.as_secs())),
        Ok(Err(e)) => Err(format!("provider {}: {}", connected.name(), cut_error(e))),
        Ok(Ok(answer)) => Ok(answer),
    }
}

/// Whether the config file the daemon serves lists the ant of `order`, else why not.
fn listed(orders: &Orders, order: &AdminOrder) -> Result<(), String> {
    let file = orders.config.as_ref().ok_or(NO_CONFIG)?;
    let loaded = config::load(&file.path).map_err(|e| format!("{e:#}"))?;
    if loaded.ants.iter().any(|ant| ant.name == order.ant) {
        Ok(())
    } else {
        Err(format!("{} lists no ant {}", file.path.display(), order.ant))
    }
}

#[cfg(test)]
mod tests;
