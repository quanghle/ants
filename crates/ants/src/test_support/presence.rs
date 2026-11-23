//! Fixtures of presence: the devices of humans.

use std::time::Duration;

use ants_daemon::bus;
use ants_proto::{DeviceId, Role, UserId};
use async_nats::jetstream;

use crate::test_support::Server;

/// A connection of a new device of `human`, with credentials of its own, and the device's id.
pub(crate) async fn device(server: &Server, human: &UserId, label: &str) -> (jetstream::Context, DeviceId) {
    let creds = server.dir().join(format!("{human}-{label}.creds"));
    let permissions = ants_proto::Permissions::for_user(human, Role::Human);
    let team = server.dir().join("auth");
    let device =
        crate::auth::Issue::new(&team, human, Role::Human.into(), permissions).labelled(Some(label)).into_file(&creds);
    let device = device.unwrap().expect("a human's device");
    let (_, mut js) = bus::connect(&server.url, Some(&creds), None, human, "test").await.unwrap();
    js.set_timeout(Duration::from_secs(1));
    (js, device)
}
