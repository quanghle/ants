//! A machine's leaf node: its nats-server configuration and members, the outbox storing and forwarding to the hub,
//! the HTTP bridge carrying its uplink where WebSocket is blocked, and the nats-server `ants daemon` runs itself.

pub(crate) mod bridge;
pub(crate) mod leaf;
pub(crate) mod members;
pub(crate) mod natsd;
pub(crate) mod outbox;
