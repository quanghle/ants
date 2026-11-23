//! Tests of the page of the web client: the pin and the bundle's digest as ants-web's build computes it, what
//! `ants web` answers and with which headers, what `ants web export` writes and prints, the WebSocket address the page
//! is given, and a binary built without the page.

use ants_test_support::scratch_dir;

use super::bundle::{self, Pin};
use super::*;

/// A bundle of a page built for ants 9.9.9, whose digest `find . -type f | sed 's|^\./||' | LC_ALL=C sort | xargs
/// shasum -a 256 | shasum -a 256` gives as [`DIGEST`].
fn bundle_dir(name: &str) -> PathBuf {
    let dir = scratch_dir(name);
    std::fs::create_dir_all(dir.join("assets")).unwrap();
    std::fs::write(dir.join("index.html"), "<p>page</p>\n").unwrap();
    std::fs::write(dir.join("ants-version"), "9.9.9\n").unwrap();
    std::fs::write(dir.join("assets/a.js"), "x\n").unwrap();
    std::fs::write(dir.join("B"), "B").unwrap();
    dir
}

const DIGEST: &str = "2fdc7fc0944187943b79ca6485bc479b2cba8693c2d6aab53d2026e88f4be5ac";

fn pin(digest: &str, version: &str) -> Pin {
    Pin { digest: digest.to_owned(), commit: "ac9d69b".to_owned(), version: version.to_owned() }
}

#[test]
fn a_pin_names_one_page_by_its_digest_commit_and_ants_version() {
    let line = format!("{DIGEST}  ants-web ac9d69b for ants 9.9.9");
    assert_eq!(Pin::parse(&format!("# a comment\n\n{line}\n")).unwrap(), pin(DIGEST, "9.9.9"));
    assert_eq!(pin(DIGEST, "9.9.9").build(), "ants-web ac9d69b for ants 9.9.9");
    assert_eq!(Pin::parse("# none\n").unwrap_err(), "it pins no page");
    assert_eq!(Pin::parse(&format!("{line}\n{line}\n")).unwrap_err(), "it pins more than one page");
    assert!(Pin::parse(&format!("{}  ants-web ac9d69b for ants 9.9.9", DIGEST.to_uppercase())).is_err());
    assert!(Pin::parse(&format!("{DIGEST}  ants-web ac9d69b")).unwrap_err().contains("is not `SHA-256"));
}

#[test]
fn the_pinned_page_is_built_for_this_ants() {
    let pinned = Pin::parse(include_str!("../../../web.sha256")).unwrap();
    assert_eq!(pinned.version, env!("CARGO_PKG_VERSION"), "pin a page built for this version: scripts/web.sh --pin");
}

#[test]
fn the_digest_is_that_of_the_listing_sorted_by_path_bytes_as_shasum_prints_it() {
    let dir = bundle_dir("web-digest");
    let files = bundle::files(&dir).unwrap();
    assert_eq!(files, ["B", "ants-version", "assets/a.js", "index.html"]);
    assert_eq!(bundle::digest(&dir, &files).unwrap(), DIGEST);
    assert_eq!(bundle::check(&dir, &pin(DIGEST, "9.9.9"), "9.9.9").unwrap(), files);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_bundle_of_another_digest_or_for_another_ants_version_is_refused() {
    let dir = bundle_dir("web-refused");
    let other = "0".repeat(64);
    let refused = bundle::check(&dir, &pin(&other, "9.9.9"), "9.9.9").unwrap_err();
    assert!(
        refused.contains(&format!("has the digest {DIGEST}, not that of the pinned ants-web ac9d69b")),
        "{refused}"
    );
    let refused = bundle::check(&dir, &pin(DIGEST, "9.9.9"), "1.0.0").unwrap_err();
    assert_eq!(refused, "the pinned page is ants-web ac9d69b for ants 9.9.9, not for ants 1.0.0");

    std::fs::write(dir.join("ants-version"), "1.0.0\n").unwrap();
    let files = bundle::files(&dir).unwrap();
    let digest = bundle::digest(&dir, &files).unwrap();
    let refused = bundle::check(&dir, &pin(&digest, "9.9.9"), "9.9.9").unwrap_err();
    assert!(refused.ends_with("is built for ants 1.0.0, not 9.9.9"), "{refused}");

    std::fs::remove_file(dir.join("index.html")).unwrap();
    assert!(bundle::check(&dir, &pin(&digest, "9.9.9"), "9.9.9").unwrap_err().ends_with("holds no index.html"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[cfg(not(feature = "web"))]
#[test]
fn a_binary_built_without_the_page_says_how_to_get_one_with_it() {
    let refused = Page::embedded().unwrap_err().to_string();
    assert!(refused.starts_with("this ants is built without the page"), "{refused}");
    assert!(refused.contains("cargo build --release --features web"), "{refused}");
}

static PAGE: Page = Page {
    build: "ants-web ac9d69b for ants 9.9.9",
    files: &[
        ("ants-version", b"9.9.9\n"),
        ("assets/index-1.js", b"js"),
        ("assets/web_bg-1.wasm", b"\0asm"),
        ("index.html", b"<p>page</p>"),
    ],
};

fn served() -> Served {
    let site = Site { server: "ws://127.0.0.1:4281".to_owned(), bootstrap: None };
    Served::new(PAGE, node_web::page_listen(), site)
}

fn get(served: &Served, method: Method, host: &str, path: &str) -> Response<Full<Bytes>> {
    served.answer(&Request::builder().method(method).uri(path).header(header::HOST, host).body(()).unwrap())
}

fn header_of<B>(response: &Response<B>, name: impl header::AsHeaderName) -> &str {
    response.headers().get(name).map_or("", |value| value.to_str().unwrap())
}

async fn body(response: Response<Full<Bytes>>) -> Bytes {
    http_body_util::BodyExt::collect(response.into_body()).await.unwrap().to_bytes()
}

#[tokio::test]
async fn ants_web_answers_with_the_page_and_its_site_under_its_content_security_policy() {
    let served = served();
    let host = "127.0.0.1:4280";
    let index = get(&served, Method::GET, host, "/");
    assert_eq!(index.status(), StatusCode::OK);
    assert_eq!(header_of(&index, header::CONTENT_TYPE), "text/html; charset=utf-8");
    assert_eq!(header_of(&index, header::CACHE_CONTROL), "no-cache");
    assert_eq!(
        header_of(&index, header::CONTENT_SECURITY_POLICY),
        "default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; img-src 'self'; \
         connect-src 'self' ws://127.0.0.1:4281; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
    );
    assert_eq!(header_of(&index, header::X_CONTENT_TYPE_OPTIONS), "nosniff");
    assert_eq!(header_of(&index, header::REFERRER_POLICY), "no-referrer");
    assert_eq!(body(index).await, "<p>page</p>");

    let wasm = get(&served, Method::GET, host, "/assets/web_bg-1.wasm");
    assert_eq!(header_of(&wasm, header::CONTENT_TYPE), "application/wasm");
    assert_eq!(header_of(&wasm, header::CACHE_CONTROL), "public, max-age=31536000, immutable");
    let site = get(&served, Method::GET, host, "/site.json");
    assert_eq!(header_of(&site, header::CONTENT_TYPE), "application/json");
    assert_eq!(Site::from_json(std::str::from_utf8(&body(site).await).unwrap()).unwrap(), served.site);

    let head = get(&served, Method::HEAD, host, "/ants-version");
    assert_eq!((head.status(), header_of(&head, header::CONTENT_LENGTH)), (StatusCode::OK, "6"));
    assert!(body(head).await.is_empty());
    let missing = get(&served, Method::GET, host, "/assets/../index.html");
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert!(!header_of(&missing, header::CONTENT_SECURITY_POLICY).is_empty(), "every answer has the headers");
    let post = get(&served, Method::POST, host, "/");
    assert_eq!((post.status(), header_of(&post, header::ALLOW)), (StatusCode::METHOD_NOT_ALLOWED, "GET, HEAD"));

    let elsewhere = get(&served, Method::GET, "localhost:4280", "/threads");
    assert_eq!(elsewhere.status(), StatusCode::PERMANENT_REDIRECT, "another origin, which the listener refuses");
    assert_eq!(header_of(&elsewhere, header::LOCATION), "http://127.0.0.1:4280/threads");
}

#[tokio::test]
async fn ants_web_serves_over_http_until_stopped() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen = listener.local_addr().unwrap();
    let site = Site { server: "ws://127.0.0.1:4281".to_owned(), bootstrap: None };
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(serve(listener, Arc::new(Served::new(PAGE, listen, site)), async {
        let _ = stopped.await;
    }));
    let (status, page) =
        crate::http::fetch(Method::GET, &format!("http://{listen}/index.html"), &[], None, 1024).await.unwrap();
    assert_eq!((status, &page[..]), (StatusCode::OK, &b"<p>page</p>"[..]));
    stop.send(()).unwrap();
    serving.await.unwrap();
}

#[test]
fn ants_web_export_writes_the_page_and_its_site_and_prints_the_headers_to_serve_it_with() {
    let dir = scratch_dir("web-export");
    std::fs::create_dir_all(dir.join("assets")).unwrap();
    std::fs::write(dir.join("assets/index-0.js"), "an older page's").unwrap();
    std::fs::write(dir.join("index.html"), "an older page").unwrap();
    let site = Site { server: "wss://hub.example/nats".to_owned(), bootstrap: Some("creds".to_owned()) };
    let told = export(PAGE, &dir, site.clone()).unwrap();

    assert_eq!(std::fs::read(dir.join("index.html")).unwrap(), b"<p>page</p>");
    assert_eq!(std::fs::read(dir.join("assets/web_bg-1.wasm")).unwrap(), b"\0asm");
    assert_eq!(std::fs::read(dir.join("assets/index-0.js")).unwrap(), b"an older page's", "kept for pages open");
    assert_eq!(Site::from_json(&std::fs::read_to_string(dir.join("site.json")).unwrap()).unwrap(), site);
    let shown = dir.display();
    assert!(
        told.starts_with(&format!(
            "wrote the page of ants 9.9.9 (ants-web ac9d69b for ants 9.9.9) to {shown}, with site.json naming \
             wss://hub.example/nats and the bootstrap credentials\n"
        )),
        "{told}"
    );
    assert!(told.contains("  Content-Security-Policy: default-src 'none'; script-src 'self' 'wasm-unsafe-eval';"));
    assert!(told.contains("connect-src 'self' wss://hub.example/nats;"), "{told}");
    assert!(told.contains("  X-Content-Type-Options: nosniff\n"), "{told}");
    assert!(told.ends_with("public, max-age=31536000, immutable for assets/.\n"), "{told}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn the_page_connects_to_a_websocket_address_only() {
    assert_eq!(websocket(" wss://hub.example/nats ").unwrap(), "wss://hub.example/nats");
    assert_eq!(websocket("ws://127.0.0.1:4281").unwrap(), "ws://127.0.0.1:4281");
    for refused in ["nats://127.0.0.1:4222", "https://hub.example", "wss://hub.example/nats?x=1", "wss://", "hub"] {
        assert!(websocket(refused).is_err(), "{refused}");
    }
    assert!(websocket("wss://hub.example/a;b").is_err(), "a character ending a source of the policy");
}

#[test]
fn ants_web_connects_the_page_to_the_machines_listener_and_says_when_it_refuses_the_page() {
    let dir = scratch_dir("web-local-server");
    let conf = dir.join("nats.conf");
    let origin = node_web::origin();
    let missing = local_server(&conf, &origin).unwrap_err().to_string();
    assert!(missing.contains("has no websocket listener") && missing.contains("ants setup --web"), "{missing}");
    std::fs::write(&conf, "listen: 127.0.0.1:4222\n").unwrap();
    assert!(local_server(&conf, &origin).unwrap_err().to_string().contains("has no websocket listener"));

    let at = node_web::listen("127.0.0.1:4222").unwrap();
    std::fs::write(&conf, node_web::block(at, &origin)).unwrap();
    assert_eq!(local_server(&conf, &origin).unwrap(), ("ws://127.0.0.1:4281".to_owned(), None));
    let (server, warning) = local_server(&conf, "http://127.0.0.1:8000").unwrap();
    assert_eq!(server, "ws://127.0.0.1:4281");
    let warning = warning.unwrap();
    assert!(warning.contains("takes pages of http://127.0.0.1:4280 only: add \"http://127.0.0.1:8000\""), "{warning}");

    std::fs::write(&conf, "websocket { port: 8080, no_tls: true }\n").unwrap();
    assert!(local_server(&conf, &origin).unwrap_err().to_string().contains("names no listen address"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn ants_web_and_its_export_parse_as_documented() {
    use clap::Parser as _;
    let parse = |args: &[&str]| crate::cli::Cli::try_parse_from(["ants", "web"].iter().chain(args));
    let Some(crate::cli::Command::Web(web)) = parse(&[]).unwrap().command else { panic!("not ants web") };
    assert_eq!((web.serve.listen, web.serve.no_open, web.command.is_none()), (node_web::page_listen(), false, true));
    let Some(crate::cli::Command::Web(web)) = parse(&["export", "out", "--server", "wss://hub/nats"]).unwrap().command
    else {
        panic!("not ants web")
    };
    assert!(matches!(web.command, Some(WebCommand::Export { bootstrap: None, .. })));
    assert!(parse(&["--no-open", "export", "out", "--server", "wss://hub/nats"]).is_err());
}
