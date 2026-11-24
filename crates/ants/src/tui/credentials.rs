//! Credentials asked for: the enrollment requests of machines running `ants add --enroll` and the renewal requests of
//! `ants renew`. Only credentials signed with the team's account key hear them, so [`Io`] listens only where that key
//! is on this machine, as `ants enroll list` and `ants enroll renewals` do, and approves, renews or rejects one once
//! `y` confirms, as `ants enroll approve`, `renew` and `reject` do. Elsewhere the page tells where they are answered:
//! on the machine `ants signer` recorded as holding the key ([`AccountHolder`]), when one did.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use ants_proto::{AccountHolder, EnrollRequest, JsDomain, RenewRequest, UserId};
use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span, Text};
use tokio::time::Instant;

use super::listed::{Listed, Shown};
use super::model::{Action, Data, Key, Model, To};
use super::style;
use crate::credentials::approver::{self, Approver};
use crate::live::maybe;
use crate::text::ago;
use crate::{ants::machines, auth};

/// How often the requests are gathered; each gathering listens [`approver::LISTEN`], as machines repeat theirs.
const GATHER_EVERY: Duration = Duration::from_secs(15);

/// Where the team's account key is, which alone hears and answers the requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Holder {
    /// In this team directory, on this machine.
    Here(PathBuf),
    /// Not on this machine: not in this team directory, or no team directory, for this reason.
    Elsewhere(String),
}

/// A request heard.
#[derive(Debug, Clone, PartialEq)]
pub enum Asked {
    /// An ant's machine asks for its first credentials (`ants add --enroll`).
    Enroll(EnrollRequest),
    /// A holder of credentials asks for them renewed, for a new key (`ants renew`).
    Renew(RenewRequest),
}

/// What a request is, as the selection keeps it: whether it is a renewal, the name and the asking key.
pub type Id = (bool, String, String);

impl Asked {
    pub fn id(&self) -> Id {
        match self {
            Self::Enroll(r) => (false, r.ant.to_string(), r.public_key.clone()),
            Self::Renew(r) => (true, r.name.clone(), r.public_key.clone()),
        }
    }

    /// The ant or other name the credentials are for.
    pub fn name(&self) -> String {
        self.id().1
    }

    fn sent_at(&self) -> DateTime<Utc> {
        match self {
            Self::Enroll(r) => r.sent_at,
            Self::Renew(r) => r.sent_at,
        }
    }

    /// What is asked, from where, in a line.
    pub fn describe(&self) -> String {
        match self {
            Self::Enroll(r) => {
                let leaf = r.leaf.as_ref().map_or_default(|l| format!(" through the leaf node of {}", l.machine));
                let host_creds =
                    r.host_creds.as_ref().map_or_default(|h| format!(", with the host credentials of {}", h.machine));
                format!("{} asks to enroll from host {}{leaf}, key {}{host_creds}", r.ant, r.host, short(&r.public_key))
            }
            Self::Renew(r) => format!(
                "{} asks for its credentials renewed from host {}, new key {}",
                r.name,
                r.host,
                short(&r.public_key)
            ),
        }
    }

    /// The order the key `a` (approve or renew) or `r` (reject, enrollments only) gives on the request.
    fn order(&self, reject: bool) -> Option<Order> {
        match (self, reject) {
            (Self::Enroll(r), false) => Some(Order::Approve(r.ant.clone(), r.public_key.clone())),
            (Self::Enroll(r), true) => Some(Order::Reject(r.ant.clone(), r.public_key.clone())),
            (Self::Renew(r), false) => Some(Order::Renew(r.name.clone(), r.public_key.clone())),
            (Self::Renew(_), true) => None,
        }
    }
}

/// The start of a public key, enough to tell machines apart.
fn short(key: &str) -> String {
    key.chars().take(12).chain("…".chars()).collect()
}

/// An answer to a request, as `ants enroll approve`, `reject` and `renew` give it: the name and the asking key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Order {
    Approve(UserId, String),
    Reject(UserId, String),
    Renew(String, String),
}

impl Order {
    /// The order, as the key `y` confirms it.
    pub fn describe(&self) -> String {
        match self {
            Self::Approve(ant, key) => format!("approve {ant} for key {}", short(key)),
            Self::Reject(ant, key) => format!("reject {ant} for key {}", short(key)),
            Self::Renew(name, key) => format!("renew the credentials of {name} for key {}", short(key)),
        }
    }
}

/// The requests heard, and how the latest gathering and order went.
#[derive(Debug, Default)]
pub struct Heard {
    /// Where the account key is; unknown until the UI looked.
    pub holder: Option<Holder>,
    pub asked: Vec<Asked>,
    /// When the requests were last gathered, or why that failed.
    pub read: Option<Result<DateTime<Utc>, String>>,
    /// The order being carried out.
    pub busy: Option<Order>,
    /// Where `ants signer` recorded the account key to be, read when it is not on this machine.
    pub recorded: Option<AccountHolder>,
}

impl Heard {
    /// The requests for `name`.
    pub fn of<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Asked> {
        self.asked.iter().filter(move |a| a.name() == name)
    }

    /// The requests heard, as home's line counts them; else, with the account key on another machine, which one;
    /// None when neither is known.
    pub fn line(&self) -> Option<Line<'static>> {
        if self.asked.is_empty() {
            let elsewhere = matches!(self.holder, Some(Holder::Elsewhere(_)));
            let recorded = self.recorded.as_ref().filter(|_| elsewhere)?;
            let text = format!(
                "requests for credentials are answered on machine {}, where the team's account key is",
                recorded.machine
            );
            return Some(Line::styled(text, style::dim()));
        }
        let names: Vec<String> = self
            .asked
            .iter()
            .map(|a| match a {
                Asked::Enroll(r) => format!("{} (enroll)", r.ant),
                Asked::Renew(r) => format!("{} (renewal)", r.name),
            })
            .collect();
        let text = format!("{} asking for credentials: {} · c lists them", self.asked.len(), names.join(", "));
        Some(Line::styled(text, Style::new().fg(style::PENDING)))
    }

    pub fn take(&mut self, event: Event, now: DateTime<Utc>) -> Option<Result<String, String>> {
        match event {
            Event::Heard(Ok(asked)) => {
                self.asked = asked;
                self.read = Some(Ok(now));
                None
            }
            Event::Heard(Err(e)) => {
                self.read = Some(Err(e));
                None
            }
            Event::Registered(recorded) => {
                self.recorded = recorded;
                None
            }
            Event::Done(order, done) => {
                if self.busy.as_ref() == Some(&order) {
                    self.busy = None;
                }
                if done.is_ok() {
                    let answered = match &order {
                        Order::Approve(ant, key) | Order::Reject(ant, key) => (false, ant.to_string(), key.clone()),
                        Order::Renew(name, key) => (true, name.clone(), key.clone()),
                    };
                    self.asked.retain(|a| a.id() != answered);
                }
                Some(done)
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct Page {
    /// The requests, and the order the next `y` gives.
    list: Listed<Id, Order>,
}

impl Page {
    pub const ARMED_KEYS: &str = "y confirms the order above · any other key leaves the request waiting";

    /// Drop the confirmation asked for, if any: whether there was one.
    pub fn disarm(&mut self) -> bool {
        self.list.disarm()
    }

    /// The keys of the order `a` or `r` asked to confirm, while it waits.
    pub fn prompt(&self) -> Option<&'static str> {
        self.list.armed().map(|_| Self::ARMED_KEYS)
    }

    fn ids(asked: &[Asked]) -> Vec<Id> {
        asked.iter().map(Asked::id).collect()
    }

    pub fn key(&mut self, key: Key, data: &Data, _: DateTime<Utc>) -> Option<To> {
        let heard = &data.credentials;
        let asked = |at: usize, reject| heard.asked.get(at).and_then(|a| a.order(reject));
        let order = self.list.key(key, &Self::ids(&heard.asked), heard.busy.is_some(), asked)?;
        Some(To::Do(Action::Credentials(order)))
    }

    pub fn draw(&self, frame: &mut Frame, area: Rect, model: &Model, now: DateTime<Utc>, focused: bool) {
        let data = &model.data;
        let heard = &data.credentials;
        let mut about = Vec::new();
        match &heard.holder {
            None => about.push(Line::styled("looking for the team's account key…", style::dim())),
            Some(Holder::Elsewhere(why)) => {
                let there = match &heard.recorded {
                    Some(recorded) => machines::held_on(recorded, now),
                    None => "the machine holding it, where `ants auth init` ran (`ants signer` running there records \
                             its name)"
                        .to_owned(),
                };
                about.push(Line::raw(format!(
                    "The team's account key is not on this machine ({why}), and only it hears and answers the \
                     requests: approve them on {there}, with `ants enroll list` and `ants enroll approve ANT`, or \
                     `ants enroll renewals` and `ants enroll renew NAME`, or `ants enroll renew --auto` running there."
                )));
            }
            Some(Holder::Here(dir)) => {
                let read = match &heard.read {
                    None => format!("listening for requests with the account key in {}…", dir.display()),
                    Some(Ok(at)) => format!(
                        "heard with the account key in {}, listening {}s, {}",
                        dir.display(),
                        approver::LISTEN.as_secs(),
                        ago(*at, now)
                    ),
                    Some(Err(e)) => format!("listening for requests: {e}"),
                };
                about.push(Line::styled(read, style::dim()));
            }
        }
        let rows: Vec<Text> = heard
            .asked
            .iter()
            .map(|a| {
                let (label, color) = match a {
                    Asked::Enroll(_) => ("enroll", Color::Green),
                    Asked::Renew(_) => ("renew", Color::Blue),
                };
                let keys = match a {
                    Asked::Enroll(_) => "a approve · r reject",
                    Asked::Renew(_) => "a renew",
                };
                Text::from(vec![
                    Line::from(vec![
                        Span::styled(format!("{label:<8}"), Style::new().fg(color)),
                        Span::raw(a.describe()),
                        Span::styled(format!("  {}", ago(a.sent_at(), now)), style::dim()),
                    ]),
                    Line::styled(format!("        {keys}"), style::dim()),
                ])
            })
            .collect();
        let shown = Shown {
            title: format!(" Credentials asked for ({}) ", heard.asked.len()),
            about,
            busy: heard.busy.as_ref(),
            describe: Order::describe,
            empty: matches!(heard.holder, Some(Holder::Here(_))).then_some("no requests heard"),
            rows,
            keys: &Self::ids(&heard.asked),
            viewport: super::mouse::Viewport::Credentials,
        };
        self.list.draw(frame, area, &model.ui.mouse, shown, focused);
    }
}

/// What a gathering or an order brought.
pub enum Event {
    Heard(Result<Vec<Asked>, String>),
    /// Where `ants signer` recorded the account key to be, read where it is not on this machine.
    Registered(Option<AccountHolder>),
    /// What carrying the order out did, or why it failed.
    Done(Order, Result<String, String>),
}

/// The connection with credentials the account key signs, short-lived, and when it was made.
type Connected = Rc<RefCell<Option<(Rc<Approver>, Instant)>>>;

/// The gatherings of the requests and the orders, where the account key is on this machine.
pub struct Io {
    /// Where the record of the machine holding the account key is read.
    js: jetstream::Context,
    url: String,
    domain: Option<JsDomain>,
    /// The team directory holding the account key; None when it is not on this machine.
    dir: Option<PathBuf>,
    /// How long each gathering listens.
    listen: Duration,
    every: tokio::time::Interval,
    connected: Connected,
    gather: Option<LocalBoxFuture<'static, Event>>,
    order: Option<LocalBoxFuture<'static, Event>>,
}

impl Io {
    /// The requests over the server at `url`, for the hub's JetStream `domain`, with the account key in the team
    /// directory `team` if it is there, each gathering listening `listen`; and where the account key is. Where it is
    /// not, each gathering reads instead which machine holds it, through `js`.
    pub fn new(
        js: jetstream::Context,
        url: &str,
        domain: Option<JsDomain>,
        team: Result<PathBuf>,
        listen: Duration,
    ) -> (Self, Holder) {
        let holder = match team {
            Ok(dir) => match auth::account_key(&dir) {
                Ok(_) => Holder::Here(dir),
                Err(_) => Holder::Elsewhere(format!("not in {}", dir.display())),
            },
            Err(e) => Holder::Elsewhere(format!("{e:#}")),
        };
        let dir = match &holder {
            Holder::Here(dir) => Some(dir.clone()),
            Holder::Elsewhere(_) => None,
        };
        let every = tokio::time::interval(GATHER_EVERY);
        let io = Self {
            js,
            url: url.to_owned(),
            domain,
            dir,
            listen,
            every,
            connected: Connected::default(),
            gather: None,
            order: None,
        };
        (io, holder)
    }

    /// The next gathering or order done. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Event {
        loop {
            tokio::select! {
                _ = self.every.tick() => {
                    if self.gather.is_none() {
                        self.start_gather();
                    }
                }
                event = maybe(&mut self.gather) => {
                    self.gather = None;
                    return event;
                }
                event = maybe(&mut self.order) => {
                    self.order = None;
                    self.start_gather();
                    return event;
                }
            }
        }
    }

    /// Gather again now.
    pub fn refresh(&mut self) {
        self.start_gather();
    }

    fn start_gather(&mut self) {
        let Some(dir) = self.dir.clone() else {
            let js = self.js.clone();
            self.gather = Some(
                async move {
                    let holder = machines::holder(&js)
                        .await
                        .inspect_err(|e| tracing::debug!("reading the account key's holder: {e:#}"));
                    Event::Registered(holder.ok().flatten())
                }
                .boxed_local(),
            );
            return;
        };
        let approver = self.approver(dir);
        self.gather = Some(
            async move {
                let gathered = async {
                    let approver = approver.await?;
                    let (enrolls, renewals) = futures::try_join!(approver.pending(None), approver.renewals(None))?;
                    let mut asked: Vec<Asked> = enrolls.into_iter().map(Asked::Enroll).collect();
                    asked.extend(renewals.into_iter().map(Asked::Renew));
                    asked.sort_by_key(Asked::sent_at);
                    anyhow::Ok(asked)
                };
                Event::Heard(gathered.await.map_err(|e| format!("{e:#}")))
            }
            .boxed_local(),
        );
    }

    /// Carry `order` out, unless another one runs; whether it starts.
    pub fn order(&mut self, order: Order) -> bool {
        let Some(dir) = self.dir.clone() else { return false };
        if self.order.is_some() {
            return false;
        }
        let approver = self.approver(dir.clone());
        self.order = Some(
            async move {
                let done = async { Self::carry_out(&*approver.await?, &dir, &order).await };
                let done = done.await.map_err(|e| format!("{}: {e:#}", order.describe()));
                Event::Done(order, done)
            }
            .boxed_local(),
        );
        true
    }

    /// Carry `order` out with `approver`, the credentials in `dir`; what was done.
    async fn carry_out(approver: &Approver, dir: &Path, order: &Order) -> Result<String> {
        Ok(match order {
            Order::Approve(ant, key) => {
                let approved = approver.approve(dir, ant, Some(key)).await?;
                let mut done = match approved.leaf {
                    Some(leaf) => format!("{ant} approved, through the leaf node of {}", leaf.machine),
                    None => format!("{ant} approved"),
                };
                if let Some(host) = approved.host {
                    done = format!("{done}; {}", host.describe());
                }
                done
            }
            Order::Reject(ant, key) => {
                approver.reject(ant, Some(key), None).await?;
                format!("{ant} rejected")
            }
            Order::Renew(name, key) => {
                let (_, renewed) = approver.renew(dir, name, Some(key)).await?;
                format!(
                    "{} renewed; once the new credentials are in use, retire the earlier key: `ants auth revoke --key \
                     {} --earlier`, then `ants auth push`",
                    renewed.finding.holder(),
                    renewed.previous
                )
            }
        })
    }

    /// The connection to the server, made afresh once older than [`approver::AUTO_RECONNECT`], as its credentials last
    /// only a while.
    fn approver(&self, dir: PathBuf) -> LocalBoxFuture<'static, Result<Rc<Approver>>> {
        let (connected, url, domain, listen) =
            (self.connected.clone(), self.url.clone(), self.domain.clone(), self.listen);
        async move {
            if let Some((approver, at)) = connected.borrow().as_ref()
                && at.elapsed() < approver::AUTO_RECONNECT
            {
                return Ok(approver.clone());
            }
            let approver = Rc::new(Approver::connect(&url, &dir, domain.as_ref(), listen).await?);
            *connected.borrow_mut() = Some((approver.clone(), Instant::now()));
            Ok(approver)
        }
        .boxed_local()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::live::{at, user};
    use crate::tui::model::Focus;
    use crate::tui::nav;
    use crate::tui::test_support::{model, screen};

    fn enrolling(ant: &str, key: &str) -> Asked {
        Asked::Enroll(EnrollRequest {
            ant: user(ant),
            public_key: key.to_owned(),
            host: "m2".to_owned(),
            sent_at: at(1),
            enrollment: String::new(),
            signature: String::new(),
            leaf: None,
            order: None,
            host_creds: None,
        })
    }

    fn renewing(name: &str, key: &str) -> Asked {
        Asked::Renew(RenewRequest { sent_at: at(2), ..RenewRequest::new(name, key, "laptop") })
    }

    #[test]
    fn requests_heard_show_on_home_and_the_ant_and_y_confirms_the_answer() {
        let mut m = model();
        m.data.live.colony(vec![crate::test_support::colony::entry("dev-a", 60, 2, at(2))], at(2));
        m.data.credentials.holder = Some(Holder::Here(PathBuf::from("/team")));
        let heard = vec![enrolling("dev-c", "UCKEYOFDEVC00"), renewing("dev-a", "UAKEYOFDEVA00")];
        assert_eq!(m.data.credentials.take(Event::Heard(Ok(heard)), at(3)), None);
        let shown = screen(&m, 140, 30, at(3));
        assert!(shown.contains("2 asking for credentials: dev-c (enroll), dev-a (renewal) · c lists them"), "{shown}");

        m.select(nav::Item::Ant(user("dev-a")));
        let shown = screen(&m, 140, 30, at(3));
        assert!(shown.contains("dev-a asks for its credentials renewed from host laptop"), "{shown}");

        m.select(nav::Item::Home);
        m.focus = Focus::View;
        m.key(Key::Char('c'), at(3));
        assert_eq!(m.page().name(), "credentials");
        let shown = screen(&m, 140, 30, at(3));
        assert!(shown.contains("enroll  dev-c asks to enroll from host m2, key UCKEYOFDEVC0…"), "{shown}");
        assert!(shown.contains("a approve · r reject") && shown.contains("a renew"), "{shown}");
        assert!(shown.contains("a approve · r reject · Esc back · ↑↓ select"), "{shown}");

        assert_eq!(m.key(Key::Char('y'), at(3)), None, "y alone confirms nothing");
        m.key(Key::Char('a'), at(3));
        let shown = screen(&m, 140, 30, at(3));
        assert!(
            shown.contains("y to approve dev-c for key UCKEYOFDEVC0…") && shown.contains(Page::ARMED_KEYS),
            "{shown}"
        );
        m.key(Key::Char('n'), at(3));
        assert_eq!(m.key(Key::Char('y'), at(3)), None, "any other key disarms");
        m.key(Key::Char('r'), at(3));
        let reject = Order::Reject(user("dev-c"), "UCKEYOFDEVC00".to_owned());
        assert_eq!(m.key(Key::Char('y'), at(3)), Some(Action::Credentials(reject.clone())));
        assert!(screen(&m, 140, 30, at(3)).contains("reject dev-c for key UCKEYOFDEVC0……"), "the order runs");
        m.key(Key::Char('a'), at(3));
        assert_eq!(m.key(Key::Char('y'), at(3)), None, "one order at a time");

        m.credentials_event(Event::Done(reject, Ok("dev-c rejected".to_owned())), at(4));
        assert!(screen(&m, 140, 30, at(4)).contains("dev-c rejected"), "the outcome is a notice");
        assert_eq!(m.data.credentials.asked, [renewing("dev-a", "UAKEYOFDEVA00")], "the request answered goes");
        assert!(m.data.credentials.busy.is_none());
        m.key(Key::Char('r'), at(4));
        assert_eq!(m.key(Key::Char('y'), at(4)), None, "a renewal is not rejected");
        m.key(Key::Char('a'), at(4));
        let renew = Order::Renew("dev-a".to_owned(), "UAKEYOFDEVA00".to_owned());
        assert_eq!(m.key(Key::Char('y'), at(4)), Some(Action::Credentials(renew.clone())));
        m.credentials_event(Event::Done(renew, Err("renew: refused".to_owned())), at(5));
        assert_eq!(m.data.credentials.asked.len(), 1, "a failed order leaves the request");
    }

    #[test]
    fn without_the_account_key_the_page_tells_where_requests_are_answered() {
        let mut m = model();
        m.data.credentials.holder = Some(Holder::Elsewhere("not in /home/a/.config/ants/auth".to_owned()));
        m.focus = Focus::View;
        m.key(Key::Char('c'), at(1));
        let shown = screen(&m, 140, 30, at(1));
        assert!(
            shown.contains("The team's account key is not on this machine (not in /home/a/.config/ants/auth)"),
            "{shown}"
        );
        assert!(shown.contains("`ants enroll approve ANT`") && shown.contains("where `ants auth init` ran"), "{shown}");
        assert_eq!(m.key(Key::Char('a'), at(1)), None);
        assert!(!screen(&m, 140, 30, at(1)).contains("y to"), "nothing to answer");
        assert_eq!(m.data.credentials.line(), None, "home says nothing of an unknown machine");

        let recorded = AccountHolder {
            machine: "keybox".parse().unwrap(),
            hostname: "keybox.lan".to_owned(),
            version: String::new(),
            updated_at: at(1),
        };
        m.credentials_event(Event::Registered(Some(recorded)), at(1));
        let shown = screen(&m, 140, 30, at(1) + chrono::Duration::minutes(3));
        assert!(
            shown.contains("approve them on machine keybox (host keybox.lan), as `ants signer` there recorded 3m ago"),
            "{shown}"
        );
        assert!(!shown.contains("auth init"), "{shown}");
        m.key(Key::Esc, at(1));
        let shown = screen(&m, 140, 30, at(1));
        assert!(
            shown.contains("requests for credentials are answered on machine keybox, where the team's account key is"),
            "{shown}"
        );
    }

    #[tokio::test]
    async fn where_the_account_key_is_the_io_hears_a_request_and_approves_it() {
        let Some(server) = crate::test_support::Server::start() else { return };
        let listen = Duration::from_millis(400);
        let (_, js) = server.connect_as(&user("alice"), ants_proto::Role::Human).await;
        let (mut elsewhere, holder) = Io::new(js.clone(), &server.url, None, Ok(server.dir().to_owned()), listen);
        assert!(matches!(holder, Holder::Elsewhere(why) if why.starts_with("not in")));
        assert!(matches!(elsewhere.next().await, Event::Registered(None)), "no signer recorded the account key");
        let team = server.dir().join("auth");
        let (mut io, holder) = Io::new(js, &server.url, None, Ok(team.clone()), listen);
        assert_eq!(holder, Holder::Here(team.clone()));
        let dev_c = user("dev-c");
        let creds = server.dir().join("enroll-dev-c.creds");
        auth::issue_enrollment(&team, &dev_c, Duration::from_secs(60), &creds).unwrap();
        let key = nkeys::KeyPair::new_user();
        let repeat = Duration::from_millis(100);
        let asking = crate::credentials::enroll::request(
            &server.url,
            crate::credentials::enroll::Enrollment::new(&creds, &dev_c, &key),
            repeat,
            std::future::pending(),
        );
        let approving = async {
            loop {
                match io.next().await {
                    Event::Heard(Ok(asked)) if !asked.is_empty() => {
                        assert_eq!(asked[0].id(), (false, "dev-c".to_owned(), key.public_key()));
                        break;
                    }
                    Event::Heard(Ok(_)) => io.refresh(),
                    Event::Heard(Err(e)) => panic!("{e}"),
                    Event::Done(..) | Event::Registered(_) => unreachable!(),
                }
            }
            assert!(io.order(Order::Approve(dev_c.clone(), key.public_key())));
            assert!(!io.order(Order::Reject(dev_c.clone(), key.public_key())), "one order at a time");
            loop {
                if let Event::Done(_, done) = io.next().await {
                    return done;
                }
            }
        };
        let both = async { tokio::join!(asking, approving) };
        let (approval, done) = Box::pin(tokio::time::timeout(Duration::from_secs(10), both)).await.unwrap();
        assert!(approval.is_ok(), "{approval:?}");
        assert_eq!(done, Ok("dev-c approved".to_owned()));
    }
}
