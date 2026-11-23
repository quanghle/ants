//! `ants cancel`: stop the prompt an ant runs on a thread.

use ants_proto::UserId;
use anyhow::{Result, bail};

use crate::connection::Nats;
use crate::messages::cancel::cancelled;
use crate::threads::thread;

pub(crate) async fn run(nats: &Nats<'_>, ant: UserId, thread: thread::Pick) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's; only a human cancels a prompt");
    }
    let (client, js) = nats.connect("ants-cancel").await?;
    let thread = thread.resolve(nats, &js).await?;
    println!("{}", cancelled(&client, me, ant, thread).await?);
    Ok(())
}
