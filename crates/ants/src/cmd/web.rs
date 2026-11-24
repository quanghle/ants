//! `ants web` and `ants web export`: the page of the web client, embedded in a binary built with the `web` feature
//! (`build.rs`), served on loopback while `ants web` runs, or written into a directory for the reverse proxy fronting
//! a hub. Either way it is served with the response headers of [`Served::headers`], whose content security policy
//! lets the page run scripts and WebAssembly of its own origin and connect to its own origin and its server's
//! WebSocket address only, and with `site.json` beside it naming that address ([`ants_client::site`]).

#[cfg(test)]
mod bundle;
#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ants_client::device::registration::{GITHUB_URL, device_page};
use ants_client::site::{self, Site};
use anyhow::{Context as _, Result, bail};
use clap::Subcommand;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::http::{Method, Request, Response, StatusCode, Uri, header};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;

use crate::auth;
use crate::node::web as node_web;

/// The page embedded in the binary, as `build.rs` writes it.
#[cfg(feature = "web")]
mod embedded {
    include!(concat!(env!("OUT_DIR"), "/page.rs"));
}

/// `ants web`: serve the page, or a subcommand.
#[derive(clap::Args, Debug)]
#[command(args_conflicts_with_subcommands = true)]
pub(crate) struct WebArgs {
    #[command(subcommand)]
    command: Option<WebCommand>,
    #[command(flatten)]
    serve: ServeArgs,
}

/// How `ants web` serves the page.
#[derive(clap::Args, Debug)]
struct ServeArgs {
    /// Loopback address to serve the page on. The listener `--web` of `ants setup` and `ants leaf config` adds takes
    /// pages of the default's origin only, http://127.0.0.1:4280; another address is another origin, to add to the
    /// listener's allowed_origins.
    #[arg(long, value_name = "ADDR", default_value_t = node_web::page_listen())]
    listen: SocketAddr,
    /// WebSocket address of the NATS server the page connects to, ws:// or wss:// (default: the listener of
    /// ~/.config/ants/leaf.conf, else of ~/.config/ants/nats.conf, that `--web` of `ants leaf config` and
    /// `ants setup` adds).
    #[arg(long, value_name = "URL")]
    server: Option<String>,
    /// Print the page's address without opening the browser.
    #[arg(long)]
    no_open: bool,
}

#[derive(Subcommand, Debug)]
enum WebCommand {
    /// Write the page into DIR for the reverse proxy fronting a hub, on the origin the hub's websocket block names
    /// in allowed_origins, with site.json naming the hub's WebSocket address and the bus's bootstrap credentials,
    /// public, with which the page registers its device. Files of the page already in DIR are replaced, others kept,
    /// so that a page still open loads what it names. Prints the response headers to serve DIR with.
    Export {
        /// Directory to write the page into, created if missing.
        dir: PathBuf,
        /// The hub's WebSocket address as browsers reach it, such as wss://hub.example/nats.
        #[arg(long, value_name = "URL")]
        server: String,
        /// The bus's bootstrap credentials, from `ants auth bootstrap`, for the page to register its device with
        /// `ants signer`. Without them, the page has a device's JWT pasted in.
        #[arg(long, value_name = "FILE")]
        bootstrap: Option<PathBuf>,
        /// The GitHub the page trusts to sign in at while it registers (https://HOST): it shows a sign-in page only
        /// when it is this GitHub's device login page, <URL>/login/device. Give a GitHub Enterprise Server's URL when
        /// the hub's `[registration]` names one.
        #[arg(long, value_name = "URL", default_value = GITHUB_URL, requires = "bootstrap")]
        github_url: String,
    },
}

/// What a binary built without the page says.
#[cfg(not(feature = "web"))]
const WITHOUT: &str = "this ants is built without the page of the web client: release binaries have it (`ants update` \
                       installs the latest), or build one with `scripts/web.sh && cargo build --release --features web` \
                       in a checkout of ants";

/// How long a connection may take to send its request's headers.
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// A page: the ants-web build it is, and its files, each path `/`-separated with its contents.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Page {
    build: &'static str,
    files: &'static [(&'static str, &'static [u8])],
}

impl Page {
    /// The page embedded in the binary.
    #[cfg(feature = "web")]
    fn embedded() -> Result<Self> {
        Ok(Self { build: embedded::BUILD, files: embedded::FILES })
    }

    /// The error saying how to get a binary with the page, this one having none.
    #[cfg(not(feature = "web"))]
    fn embedded() -> Result<Self> {
        bail!(WITHOUT)
    }

    fn file(&self, path: &str) -> Option<&'static [u8]> {
        self.files.iter().find(|(name, _)| *name == path).map(|(_, contents)| *contents)
    }

    /// The ants version the page is built for.
    fn version(&self) -> String {
        self.file("ants-version").map_or_default(|v| String::from_utf8_lossy(v).trim().to_owned())
    }
}

/// Run `ants web`.
pub(crate) async fn run(args: WebArgs) -> Result<()> {
    let page = Page::embedded()?;
    match args.command {
        Some(WebCommand::Export { dir, server, bootstrap, github_url }) => {
            device_page(&github_url).map_err(|e| anyhow::anyhow!("--github-url {e}"))?;
            let bootstrap = bootstrap.as_deref().map(read_bootstrap).transpose()?;
            let github_url = bootstrap.is_some().then_some(github_url);
            print!("{}", export(page, &dir, Site { server: websocket(&server)?, bootstrap, github_url })?);
            Ok(())
        }
        None => serve_args(page, args.serve).await,
    }
}

/// Serve `page` as `args` say until Ctrl-C or SIGTERM, opening the browser on it unless told not to.
async fn serve_args(page: Page, args: ServeArgs) -> Result<()> {
    let ServeArgs { listen, server, no_open } = args;
    if !listen.ip().is_loopback() {
        bail!("{listen}: `ants web` serves the page on a loopback address only; `ants web export` writes it for a hub");
    }
    let origin = node_web::origin_of(listen);
    let server = match server {
        Some(server) => websocket(&server)?,
        None => {
            let dir = auth::default_dir()?;
            let (leaf, hub) = (dir.with_file_name("leaf.conf"), dir.with_file_name("nats.conf"));
            let conf = if leaf.exists() { leaf } else { hub };
            let (server, warning) = local_server(&conf, &origin)?;
            if let Some(warning) = warning {
                eprintln!("warning: {warning}");
            }
            server
        }
    };
    let listener = TcpListener::bind(listen).await.with_context(|| format!("listening on {listen}"))?;
    let served = Served::new(page, listen, Site { server, bootstrap: None, github_url: None });
    println!(
        "serving the page of ants {} ({}) on {origin}, connecting to {}, until Ctrl-C",
        page.version(),
        page.build,
        served.site.server
    );
    if !no_open {
        match crate::tui::attach::view(PathBuf::from(&origin)).await {
            Ok(viewer) => println!("opened {origin} with {viewer}"),
            Err(e) => eprintln!("warning: {e:#}; open {origin} in a browser"),
        }
    }
    serve(listener, Arc::new(served), crate::serve::stop_signal()).await;
    Ok(())
}

/// The WebSocket address of the listener in the nats-server configuration `conf` for pages of `origin`, with a
/// warning when the listener refuses them; an error naming how to add one when it has none.
fn local_server(conf: &Path, origin: &str) -> Result<(String, Option<String>)> {
    let shown = conf.display();
    let no_listener = || {
        format!(
            "{shown} has no websocket listener for the page to connect to: `ants setup --web` adds one, or \
             `ants leaf config MACHINE --uplink URL --web` on a leaf node; or give --server"
        )
    };
    let text = std::fs::read_to_string(conf).with_context(no_listener)?;
    let Some(listener) = node_web::Listener::read(&text) else { bail!(no_listener()) };
    if listener.listen.is_empty() {
        bail!("the websocket listener of {shown} names no listen address: give --server");
    }
    let warning = (!listener.origins.is_empty() && !listener.takes(origin)).then(|| {
        format!(
            "the websocket listener of {shown} takes pages of {} only: add \"{origin}\" to its allowed_origins and \
             restart nats-server, or serve on the default --listen",
            listener.origins.join(", ")
        )
    });
    Ok((format!("ws://{}", listener.listen), warning))
}

/// `url` checked to be a WebSocket address a content security policy names: ws:// or wss://, a host, and neither a
/// query nor a character ending a source.
fn websocket(url: &str) -> Result<String> {
    let url = url.trim();
    let wrong = || format!("{url}: expected a WebSocket address such as wss://hub.example/nats");
    let uri: Uri = url.parse().with_context(wrong)?;
    if !matches!(uri.scheme_str(), Some("ws" | "wss"))
        || uri.host().is_none_or(str::is_empty)
        || uri.query().is_some()
        || url.contains([';', ',', '\'', '"'])
    {
        bail!(wrong());
    }
    Ok(url.to_owned())
}

/// The text of the bootstrap credentials file at `path`, checked to be the team's.
fn read_bootstrap(path: &Path) -> Result<String> {
    crate::cmd::login::bootstrap_account(path)?;
    std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))
}

/// Write `page` into `dir` with `site`: what to tell of it, with the response headers to serve it with.
fn export(page: Page, dir: &Path, site: Site) -> Result<String> {
    for (path, contents) in page.files {
        let file = dir.join(path);
        if let Some(parent) = file.parent() {
            std::fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        std::fs::write(&file, contents).with_context(|| format!("writing {}", file.display()))?;
    }
    let file = dir.join(site::FILE);
    std::fs::write(&file, site.to_json()).with_context(|| format!("writing {}", file.display()))?;
    let served = Served::new(page, node_web::page_listen(), site);
    let mut out = format!(
        "wrote the page of ants {} ({}) to {}, with {} naming {}{}\n\n",
        page.version(),
        page.build,
        dir.display(),
        site::FILE,
        served.site.server,
        if served.site.bootstrap.is_some() { " and the bootstrap credentials" } else { "" }
    );
    let _ = writeln!(
        out,
        "Serve {} on the origin the hub's websocket block names in allowed_origins, with on every answer:",
        dir.display()
    );
    for (name, value) in served.headers() {
        let _ = writeln!(out, "  {name}: {value}");
    }
    let _ = writeln!(
        out,
        "with Content-Type: application/wasm for *.wasm, and Cache-Control: no-cache for index.html, ants-version \
         and {}, {IMMUTABLE} for assets/.",
        site::FILE
    );
    Ok(out)
}

/// The caching of a file whose name changes with its contents.
const IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// What `ants web` serves: the page on `listen`, with `site`.
struct Served {
    page: Page,
    listen: SocketAddr,
    site: Site,
    site_json: Bytes,
}

impl Served {
    fn new(page: Page, listen: SocketAddr, site: Site) -> Self {
        let site_json = Bytes::from(site.to_json());
        Self { page, listen, site, site_json }
    }

    /// The headers of every answer: the content security policy letting the page load scripts, styles, images and
    /// WebAssembly of its own origin alone and connect to it and its server's WebSocket address alone, with nothing
    /// framing it, sniffing its types, or told where it was.
    fn headers(&self) -> [(&'static str, String); 4] {
        let csp = format!(
            "default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; style-src 'self'; img-src 'self'; \
             connect-src 'self' {}; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
            self.site.server
        );
        [
            ("Content-Security-Policy", csp),
            ("X-Content-Type-Options", "nosniff".to_owned()),
            ("Referrer-Policy", "no-referrer".to_owned()),
            ("Cross-Origin-Opener-Policy", "same-origin".to_owned()),
        ]
    }

    /// The answer to `request`: a file of the page or `site.json` for a GET or a HEAD on the address served, a
    /// redirect to it for another host name of the machine, which is another origin.
    fn answer<B>(&self, request: &Request<B>) -> Response<Full<Bytes>> {
        let mut response = Response::builder();
        for (name, value) in self.headers() {
            response = response.header(name, value);
        }
        let head = request.method() == Method::HEAD;
        if !head && request.method() != Method::GET {
            let response = response.status(StatusCode::METHOD_NOT_ALLOWED).header(header::ALLOW, "GET, HEAD");
            return response.body(Full::default()).expect("a valid response");
        }
        let host = request.headers().get(header::HOST).and_then(|host| host.to_str().ok());
        let path = request.uri().path();
        if host != Some(&self.listen.to_string()) {
            let to = format!("{}{path}", node_web::origin_of(self.listen));
            let response = response.status(StatusCode::PERMANENT_REDIRECT).header(header::LOCATION, to);
            return response.body(Full::default()).expect("a valid response");
        }
        let name = match path.trim_start_matches('/') {
            "" => "index.html",
            name => name,
        };
        let contents = if name == site::FILE {
            Some(self.site_json.clone())
        } else {
            self.page.file(name).map(Bytes::from_static)
        };
        let Some(contents) = contents else {
            let response = response.status(StatusCode::NOT_FOUND).header(header::CONTENT_TYPE, "text/plain");
            return response.body(Full::new(Bytes::from_static(b"not found\n"))).expect("a valid response");
        };
        let cache = if name.starts_with("assets/") { IMMUTABLE } else { "no-cache" };
        let response = response
            .header(header::CONTENT_TYPE, content_type(name))
            .header(header::CACHE_CONTROL, cache)
            .header(header::CONTENT_LENGTH, contents.len());
        response.body(if head { Full::default() } else { Full::new(contents) }).expect("a valid response")
    }
}

/// The content type of the file at `path`, by its extension.
fn content_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("wasm") => "application/wasm",
        Some("css") => "text/css; charset=utf-8",
        Some("json" | "webmanifest") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        _ => "text/plain; charset=utf-8",
    }
}

/// Answer the connections `listener` accepts with `served` until `stop`.
async fn serve(listener: TcpListener, served: Arc<Served>, stop: impl Future<Output = ()>) {
    tokio::pin!(stop);
    loop {
        let stream = tokio::select! {
            () = &mut stop => return,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(e) => {
                    tracing::warn!(error = %e, "accepting a connection");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let served = served.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |request| {
                let answer = served.answer(&request);
                async move { Ok::<_, Infallible>(answer) }
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(HEADER_TIMEOUT)
                .serve_connection(TokioIo::new(stream), service);
            if let Err(e) = connection.await {
                tracing::debug!(error = %e, "page connection");
            }
        });
    }
}
