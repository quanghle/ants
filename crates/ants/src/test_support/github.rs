//! A fake GitHub on a local port.

use std::sync::Arc;

use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::http::{Request, Response, header};
use hyper_util::rt::TokioIo;

/// What a fake GitHub got: `METHOD /path?query`, the authorization header and the body.
pub(crate) type Route = dyn Fn(&str, Option<&str>, &str) -> (u16, String) + Send + Sync;

/// A fake GitHub on a local port, answering each request with `route`: its URL, for both `github_url` and
/// `api_url`.
pub(crate) async fn fake(route: Arc<Route>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let route = route.clone();
            let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                let route = route.clone();
                async move {
                    let what = format!("{} {}", request.method(), request.uri());
                    let auth = request.headers().get(header::AUTHORIZATION).map(|v| v.to_str().unwrap().to_owned());
                    let body = request.into_body().collect().await?.to_bytes();
                    let (status, answer) = route(&what, auth.as_deref(), &String::from_utf8_lossy(&body));
                    let response = Response::builder().status(status).body(Full::new(Bytes::from(answer)));
                    Ok::<_, hyper::Error>(response.unwrap())
                }
            });
            tokio::spawn(hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(tcp), service));
        }
    });
    url
}
