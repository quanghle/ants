//! Tests of the bridge: requests repeated or out of place neither lose nor double bytes, proxy URLs, offsets and the
//! token file are read, and a leaf node reaches its hub through both ends.

use std::sync::Arc;
use std::time::Duration;

use ants_test_support::NatsServer;
use futures::StreamExt as _;
use hyper::body::Bytes;
use hyper::http::{Method, StatusCode};
use tokio::net::TcpListener;

use super::connect::Http;
use super::serve::offset;
use super::*;
use crate::http::parse_proxy;

async fn local() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    (listener, port)
}

/// A bridge in front of an echo server, and its traffic.
async fn echo_bridge(token: Option<&str>) -> (u16, Arc<Traffic>) {
    let (echo, echo_port) = local().await;
    tokio::spawn(async move {
        while let Ok((stream, _)) = echo.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = stream.into_split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    let (listener, port) = local().await;
    let server = Server::new(format!("127.0.0.1:{echo_port}"), token.map(str::to_owned));
    let traffic = server.traffic();
    tokio::spawn(server.run(listener));
    (port, traffic)
}

#[tokio::test]
async fn repeated_requests_neither_lose_nor_double_bytes() {
    let (port, traffic) = echo_bridge(Some("s3cret")).await;
    let url = format!("http://127.0.0.1:{port}/bridge");
    let wrong = Remote::new(&url, Some("guess"), &[], None).unwrap();
    let (status, _) = Http::new(&wrong).once(&Method::POST, "/open", &Bytes::new(), REQUEST).await.unwrap();
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let remote = Remote::new(&url, Some("s3cret"), &[], None).unwrap();
    let mut http = Http::new(&remote);
    let mut call = async |method: Method, path: String, body: &'static [u8]| {
        http.once(&method, &path, &Bytes::from_static(body), REQUEST).await.unwrap()
    };
    let (status, id) = call(Method::POST, "/open".into(), b"").await;
    assert_eq!(status, StatusCode::OK);
    let s = format!("/s/{}", String::from_utf8(id.to_vec()).unwrap());

    // The second POST repeats "llo" of the first, as a retry after a lost answer would.
    assert_eq!(call(Method::POST, format!("{s}?off=0"), b"hello").await.0, StatusCode::NO_CONTENT);
    assert_eq!(call(Method::POST, format!("{s}?off=2"), b"llo world").await.0, StatusCode::NO_CONTENT);
    assert_eq!(call(Method::POST, format!("{s}?off=99"), b"gap").await.0, StatusCode::CONFLICT);

    let mut echoed = Vec::new();
    while echoed.len() < 11 {
        let (status, body) = call(Method::GET, format!("{s}?off=0"), b"").await;
        assert_eq!(status, StatusCode::OK);
        echoed = body.to_vec();
    }
    assert_eq!(echoed, b"hello world");
    // Asked again from 6, as after a lost answer: the bytes from there come again; then there is nothing new.
    assert_eq!(call(Method::GET, format!("{s}?off=6"), b"").await.1, Bytes::from_static(b"world"));
    assert_eq!(call(Method::GET, format!("{s}?off=0"), b"").await.0, StatusCode::CONFLICT);

    assert_eq!(call(Method::DELETE, s.clone(), b"").await.0, StatusCode::NO_CONTENT);
    assert_eq!(call(Method::GET, format!("{s}?off=11"), b"").await.0, StatusCode::NOT_FOUND);

    let mut out = Exposition::default();
    metrics::Source::render(&*traffic, &mut out);
    let out = out.into_string();
    for line in [
        "ants_bridge_sessions 0",
        "ants_bridge_sessions_total 1",
        "ants_bridge_failures_total 0",
        "ants_bridge_bytes_total{direction=\"up\"} 11",
        "ants_bridge_bytes_total{direction=\"down\"} 11",
    ] {
        assert!(out.lines().any(|l| l == line), "{line} in:\n{out}");
    }
}

#[test]
fn proxy_urls_and_offsets_parse() {
    assert_eq!(parse_proxy("http://proxy:3128").unwrap(), ("proxy:3128".to_owned(), None));
    let (dial, auth) = parse_proxy("http://me:pw@proxy/").unwrap();
    assert_eq!((dial.as_str(), auth.as_deref()), ("proxy:80", Some("Basic bWU6cHc=")));
    assert!(parse_proxy("socks5://proxy:1080").is_err());
    assert_eq!(offset("x=1&off=42"), Some(42));
    assert!(Remote::new("wss://hub/bridge", None, &[], None).is_err());
    assert!(Remote::new("https://hub/bridge?x=1", None, &[], None).is_err());
    assert!(Remote::new("https://hub/bridge", None, &["no colon".into()], None).is_err());
}

#[test]
fn token_file_is_read_trimmed_and_must_hold_a_token() {
    let dir = ants_test_support::scratch_dir("bridge-token");
    let file = dir.join("token");
    std::fs::write(&file, "s3cret\n").unwrap();
    assert_eq!(token(None, Some(&file)).unwrap().as_deref(), Some("s3cret"));
    assert_eq!(token(Some("given".into()), None).unwrap().as_deref(), Some("given"));
    std::fs::write(&file, " \n").unwrap();
    assert!(token(None, Some(&file)).is_err());
    assert!(token(None, Some(&dir.join("missing"))).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

/// A leaf node reaches its hub through `ants bridge connect` and `ants bridge serve` over plain HTTP, both ways,
/// with a message larger than one request.
#[tokio::test]
async fn a_leaf_node_reaches_its_hub_through_the_bridge() {
    let leaf_port = ants_test_support::free_port();
    let Some(hub) =
        NatsServer::spawn_with(|_| format!("leafnodes {{ listen: \"127.0.0.1:{leaf_port}\", no_advertise: true }}\n"))
    else {
        return;
    };
    let (serve_listener, serve_port) = local().await;
    tokio::spawn(Server::new(format!("127.0.0.1:{leaf_port}"), Some("t".into())).run(serve_listener));
    let (connect_listener, connect_port) = local().await;
    let remote = Remote::new(&format!("http://127.0.0.1:{serve_port}/bridge/"), Some("t"), &[], None).unwrap();
    tokio::spawn(remote.run(connect_listener));
    let leaf = NatsServer::spawn_with(|_| {
        format!("leafnodes {{ remotes: [{{ urls: [\"nats-leaf://127.0.0.1:{connect_port}\"] }}] }}\n")
    })
    .unwrap();

    let on_hub = async_nats::connect(&hub.url).await.unwrap();
    let on_leaf = async_nats::connect(&leaf.url).await.unwrap();
    let big = Bytes::from(vec![7u8; 3 * CHUNK]);
    for (from, to, payload) in [(&on_leaf, &on_hub, Bytes::from_static(b"up")), (&on_hub, &on_leaf, big)] {
        let mut sub = to.subscribe("bridged").await.unwrap();
        to.flush().await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                from.publish("bridged", payload.clone()).await.unwrap();
                from.flush().await.unwrap();
                if let Ok(Some(msg)) = tokio::time::timeout(Duration::from_millis(250), sub.next()).await {
                    return msg.payload;
                }
            }
        })
        .await
        .expect("no message across the bridge");
        assert_eq!(got, payload);
    }
}
