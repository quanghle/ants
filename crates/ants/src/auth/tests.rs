//! Tests of team credentials against nats-server: the fixtures connecting as each user and watching for permission
//! violations, and the rights of each, one submodule per area.

use std::assert_matches;
use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::channel::{Channels, Earlier, Reading, Threads};
use ants_daemon::colony::Registration;
use ants_daemon::config::registration::{GithubAccount, Grant};
use ants_daemon::daemon::Claims;
use ants_daemon::delivery::dead::{self, Attempt};
use ants_daemon::people::humans::Registry;
use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::records::owners;
use ants_daemon::secrets::creds::{Finding, Kind as CredsKind};
use ants_daemon::stream::notes;
use ants_proto::{
    ChannelEntry, ChannelId, ChannelPost, ColonyEntry, DeadLetter, Destination, Envelope, JsDomain, Machine, NotesPost,
    PERMISSIONS_VERSION, STREAM, ThreadId, attempt_key, chan_consumer, chan_reader, inbox_prefix, link_key,
    notes_consumer, session_key, subject,
};
use async_nats::jetstream::AckKind;
use async_nats::{Client, jetstream};
use data_encoding::BASE64URL_NOPAD;
use futures::{StreamExt as _, TryStreamExt as _};
use nkeys::XKey;
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::uplink::allowed;
use super::*;
use crate::node::members::EnrollApprover;
use crate::test_support::Server;

mod channels;
mod credentials;
mod dead_letters;
mod machines;
mod reading;
mod records;
mod relayed;
mod roles;
mod services;
mod store;
mod threads;
mod uplink;

const WAIT: Duration = Duration::from_secs(5);

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

/// The server handles operations in order, so the next violation names the first denied one.
/// Returns once the server has registered the subscriptions of `js`'s client: async-nats' flush only writes them
/// out, so another client's publish could otherwise get there first, and be missed.
async fn subscribed(js: &jetstream::Context, user: &UserId) {
    bus::open_stream(js, user).await.unwrap();
}

async fn next_violation(client: &Client, errors: &mut mpsc::UnboundedReceiver<String>) -> String {
    client.flush().await.unwrap();
    timeout(WAIT, errors.recv()).await.expect("no violation").unwrap()
}

async fn connect_to(url: &str, creds: &Path, domain: Option<&JsDomain>, user: &UserId) -> jetstream::Context {
    let (_, mut js) = bus::connect(url, Some(creds), domain, user, "test").await.unwrap();
    js.set_timeout(Duration::from_secs(1));
    js
}
