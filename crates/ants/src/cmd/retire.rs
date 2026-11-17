//! `ants retire`: retire an ant added from a template, as a human.

use anyhow::Result;

use crate::connection::Connection;
use crate::retire::{self, RetireArgs};

pub(crate) async fn run(args: &RetireArgs, conn: &Connection<'_>) -> Result<()> {
    let colony = colony(conn, &args.name).await;
    retire::retire(args, colony.as_ref(), retire::STOP_WAIT, conn.nats, conn.js_domain).await
}

/// The colony `ants retire` waits on for `name` to stop, None (said) when it cannot be read.
async fn colony(conn: &Connection<'_>, name: &impl std::fmt::Display) -> Option<async_nats::jetstream::Context> {
    let connected = match conn.nats() {
        Ok(nats) => nats.connect("ants-retire").await,
        Err(e) => Err(e),
    };
    match connected {
        Ok((_, js)) => Some(js),
        Err(e) => {
            println!("cannot read the colony ({e:#}): not waiting for {name} to stop");
            None
        }
    }
}
