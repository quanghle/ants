//! Tests of provider export and import orders: an export stores the provider's export for the admin, an import goes to
//! the provider of the same name only, and orders name an ant of the config.

use std::sync::Arc;

use ants_proto::provider::Capabilities;
use ants_proto::{AdminOp, Machine, OrderState};
use ants_test_support::NatsServer;
use serde_json::json;
use tokio::sync::Notify;

use super::*;
use crate::admin::ConfigFile;
use crate::admin::memories::tests::{Fake, MEMORIES_AND_EXPORT};
use crate::admin::restart::Restarts;
use crate::bus;

fn orders(machine: &str) -> Orders {
    let dir = ants_test_support::scratch_dir("export-provider");
    let path = dir.join("config.toml");
    std::fs::write(&path, "harness = \"claude\"\nmessenger = false\n\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n")
        .unwrap();
    Orders {
        machine: machine.parse().unwrap(),
        nats: String::new(),
        remote: true,
        accounts: vec![],
        config: Some(ConfigFile::new(path, Arc::new(Notify::new()))),
        restarts: Restarts::default(),
        providers: Default::default(),
        creds: None,
        js_domain: None,
        leaf: None,
        hold: Default::default(),
    }
}

fn order(op: AdminOp) -> AdminOrder {
    AdminOrder::new("alice".parse().unwrap(), "m1".parse().unwrap(), "dev-a".parse().unwrap(), op, "")
}

async fn store() -> Option<(NatsServer, jetstream::Context)> {
    let server = NatsServer::start()?;
    let (_, js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "test").await.unwrap();
    js.create_stream(crate::hub::files_config()).await.unwrap();
    Some((server, js))
}

/// An export of the ant's provider is stored whole, chunk by chunk, for the outcome to name; a provider without
/// export, or no provider, says so with no payload.
#[tokio::test]
async fn an_export_stores_the_provider_s_export_for_the_admin() {
    let Some((_server, js)) = store().await else { return };
    let orders = orders("m1");
    let mut fake = Fake::new("brain", MEMORIES_AND_EXPORT);
    fake.data = json!({ "log": "z".repeat(300 * 1024) });
    let (fake, connected) = fake.connect().await;
    let order = order(AdminOp::ExportProvider { identity: "1f".to_owned() });

    let outcome = export_from(&js, &orders, &order, "1f", Some(&connected)).await;
    assert_eq!(outcome.state, OrderState::Applied, "{outcome:?}");
    let result: ProviderExport = serde_json::from_value(outcome.result.unwrap()).unwrap();
    assert_eq!(result.provider.as_deref(), Some("brain"));
    let stored = result.payload.expect("the export is stored");
    assert_eq!((stored.machine.as_str(), stored.chunks()), ("m1", 3));
    let export: Export = serde_json::from_slice(&payload::get(&js, &stored).await.unwrap()).unwrap();
    assert_eq!(export, Export { provider: "brain".to_owned(), data: fake.data.clone() });
    assert_eq!(fake.asked.lock().unwrap()[0].identity, "1f");

    let (_, tools) = Fake::new("tools", Capabilities { tools: true, ..Capabilities::NONE }).connect().await;
    let outcome = export_from(&js, &orders, &order, "1f", Some(&tools)).await;
    let result: ProviderExport = serde_json::from_value(outcome.result.unwrap()).unwrap();
    assert_eq!(result, ProviderExport { provider: Some("tools".to_owned()), payload: None });
    let outcome = export_from(&js, &orders, &order, "1f", None).await;
    assert_eq!(outcome.result, Some(json!({})));

    let mut failing = Fake::new("brain", MEMORIES_AND_EXPORT);
    failing.fail = Some(ants_proto::provider::RpcError::internal("disk full"));
    let (_, failing) = failing.connect().await;
    let outcome = export_from(&js, &orders, &order, "1f", Some(&failing)).await;
    assert_eq!(outcome.state, OrderState::Refused);
    assert!(outcome.reason.unwrap().contains("disk full"));
}

/// An import hands the ant's provider an export of the same provider, stored for the order's machine; any other is
/// refused with why.
#[tokio::test]
async fn an_import_goes_to_the_provider_of_the_same_name_only() {
    let Some((_server, js)) = store().await else { return };
    let orders = orders("m1");
    let export = Export { provider: "brain".to_owned(), data: json!({ "memories": ["a"] }) };
    let bytes = serde_json::to_vec(&export).unwrap();
    let m1: Machine = "m1".parse().unwrap();
    let stored = payload::put(&js, &m1, &bytes).await.unwrap();
    let op = |payload: &AdminPayload| AdminOp::ImportProvider { identity: "1f".to_owned(), payload: payload.clone() };

    let (fake, brain) = Fake::new("brain", MEMORIES_AND_EXPORT).connect().await;
    let outcome = import_into(&js, &orders, &order(op(&stored)), "1f", &stored, Some(&brain)).await;
    assert_eq!(outcome.state, OrderState::Applied, "{outcome:?}");
    let imported = fake.imported.lock().unwrap().clone();
    assert_eq!(imported, [ImportRequest { ant: "dev-a".parse().unwrap(), identity: "1f".to_owned(), export }]);

    let refused = async |stored: &AdminPayload, provider: Option<&Connected>| {
        let outcome = import_into(&js, &orders, &order(op(stored)), "1f", stored, provider).await;
        assert_eq!(outcome.state, OrderState::Refused);
        outcome.reason.unwrap()
    };
    let (other, mind) = Fake::new("mind", MEMORIES_AND_EXPORT).connect().await;
    let why = refused(&stored, Some(&mind)).await;
    assert_eq!(why, "the export is of provider brain, and the provider of dev-a is mind");
    assert!(other.imported.lock().unwrap().is_empty());
    let (_, tools) = Fake::new("brain", Capabilities { tools: true, ..Capabilities::NONE }).connect().await;
    assert_eq!(refused(&stored, Some(&tools)).await, "provider brain of dev-a takes no import");
    assert_eq!(refused(&stored, None).await, "dev-a has no provider to import into");
    let elsewhere = payload::put(&js, &"m2".parse().unwrap(), &bytes).await.unwrap();
    assert!(refused(&elsewhere, Some(&brain)).await.contains("stored for machine m2"));
    let changed = AdminPayload { sha256: "00".to_owned(), ..stored.clone() };
    assert!(refused(&changed, Some(&brain)).await.contains("does not match its reference"));
    assert_eq!(fake.imported.lock().unwrap().len(), 1);
}

/// Neither is carried out for an ant the config does not list, and an export waits no longer than a memories read.
#[tokio::test]
async fn orders_name_an_ant_of_the_config() {
    let orders = orders("m1");
    let mut other = order(AdminOp::ExportProvider { identity: "1f".to_owned() });
    other.ant = "dev-z".parse().unwrap();
    assert!(listed(&orders, &other).unwrap_err().contains("lists no ant dev-z"));
    assert_eq!(listed(&orders, &order(AdminOp::ExportProvider { identity: "1f".to_owned() })), Ok(()));
    let old = Utc::now() - ants_proto::MEMORIES_FRESH - chrono::TimeDelta::seconds(1);
    assert!(stale(old, "exports from providers").unwrap().starts_with("stale"));
}
