//! Tests of `ants webhook serve`: config, signatures, templates, delivery once, channel mentions, reloads, refusals and
//! timeouts, stops, and notifying humans with their links, pushes and digests.

mod config;
mod notifying;
mod serving;

use std::assert_matches;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::Path;
use std::time::SystemTime;

use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::records::owners;
use ants_daemon::schedule::Mentions;
use ants_proto::{ChannelId, ChannelPost, Destination, Envelope, STREAM, WEBHOOKS_BUCKET, subject, webhook_marker_key};
use ants_test_support::user as p;
use async_nats::jetstream;
use data_encoding::HEXLOWER;
use hyper::http::HeaderMap;
use ring::{digest, hmac};
use serde_json::Value;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use super::config::IdSource;
use super::requests::recent;
use super::*;
use crate::test_support::webhook::{creds, hub, stored};

fn parse(text: &str) -> Result<Config> {
    let env = |name: &str| match name {
        "HOOK_SECRET" => Some("s3cret".to_owned()),
        // The test vector of the Standard Webhooks libraries.
        "SIGNING_SECRET" => Some("whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw".to_owned()),
        _ => None,
    };
    Config::parse(text, Path::new("/etc/ants/webhooks.toml"), Path::new("/home/q"), env)
}

const HOOK: &str = "[[webhook]]\nname = \"github\"\nauth = \"bearer\"\nsecret_env = \"HOOK_SECRET\"\n";

struct Answer {
    status: u16,
    headers: String,
    body: String,
}

impl Answer {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("{e}: {}", self.body))
    }
}

async fn request(addr: SocketAddr, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Answer {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: hooks\r\nConnection: close\r\n");
    for (name, value) in headers {
        let _ = write!(head, "{name}: {value}\r\n");
    }
    let _ = write!(head, "Content-Length: {}\r\n\r\n", body.len());
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    answer(stream).await
}

/// The answer read from `stream` until the server closes it.
async fn answer(mut stream: TcpStream) -> Answer {
    let mut answer = String::new();
    stream.read_to_string(&mut answer).await.unwrap();
    let (headers, body) = answer.split_once("\r\n\r\n").unwrap_or_else(|| panic!("no answer: {answer:?}"));
    let status = headers.split(' ').nth(1).unwrap().parse().unwrap();
    Answer { status, headers: headers.to_lowercase(), body: body.to_owned() }
}

fn signed(body: &[u8]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, b"s3cret");
    format!("sha256={}", HEXLOWER.encode(hmac::sign(&key, body).as_ref()))
}

/// `ants webhook serve` with the config at `path`, checked for changes every 50 ms, and where it listens.
async fn start(path: &Path, nats: &str) -> (JoinHandle<Result<Ended>>, SocketAddr) {
    start_with(path, nats, REQUEST_TIMEOUT, std::future::pending()).await
}

/// [`start`], with requests limited to `timeout`, until `stop`.
async fn start_with(
    path: &Path,
    nats: &str,
    timeout: Duration,
    stop: impl Future<Output = Ended> + Send + 'static,
) -> (JoinHandle<Result<Ended>>, SocketAddr) {
    let config = Config::load(path).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (nats, path) = (nats.to_owned(), path.to_owned());
    let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_millis(50)).unwrap();
    let reload = Reload { triggers, load: move || Config::load(&path) };
    (tokio::spawn(async move { serve_timing(config, &nats, None, listener, reload, stop, timeout).await }), addr)
}
