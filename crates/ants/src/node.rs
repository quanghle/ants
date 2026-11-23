//! A machine's leaf node: its nats-server configuration and members, the outbox storing and forwarding to the hub,
//! the HTTP bridge carrying its uplink where WebSocket is blocked, the nats-server `ants daemon` runs itself, and the
//! WebSocket listener pages connect to.

pub(crate) mod bridge;
pub(crate) mod leaf;
pub(crate) mod members;
pub(crate) mod natsd;
pub(crate) mod outbox;
pub(crate) mod web;
