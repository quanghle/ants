//! `ants spend`: the tokens and cost each ant spent, as its daemon keeps them in [`SESSIONS_BUCKET`]
//! ([`ants_daemon::limits::caps`]): per UTC day under `<ant>.day.<YYYY-MM-DD>`, and per thread since a human's last message on
//! it under `<ant>.<thread>.spend`. Caps come from the local config file, for the ants it lists: the CLI has no other
//! way to know the caps of ants served elsewhere.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::connection::Nats;
use ants_daemon::limits::caps::{self, Caps, Spent};
use ants_daemon::records::direct_kv::DirectKv;
use ants_proto::{SESSIONS_BUCKET, ThreadId, UserId};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use chrono::{Days, NaiveDate, Utc};
use futures::{StreamExt as _, TryStreamExt as _};
use serde::Serialize;

/// How many spend entries are read at once.
const CONCURRENT: usize = 16;
/// UTC days reported by default, today included.
pub const DEFAULT_DAYS: u32 = 7;

/// What to report: one ant or all, one thread or the days up to today.
pub struct Select {
    pub ant: Option<UserId>,
    pub thread: Option<crate::threads::thread::Pick>,
    pub days: u32,
}

/// What an ant spent in one UTC day.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Day {
    pub day: String,
    #[serde(flatten)]
    pub spent: Spent,
}

/// What a thread spent since a human's last message on it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ThreadSpent {
    pub thread: ThreadId,
    #[serde(flatten)]
    pub spent: Spent,
}

/// What an ant spent in each day reported, newest first, and in all of them.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AntReport {
    pub ant: UserId,
    pub days: Vec<Day>,
    pub total: Spent,
    /// Its threads that spent anything since a human's last message on them, most costly first; only when one ant is
    /// reported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads: Option<Vec<ThreadSpent>>,
    /// Its caps, when the local config file lists it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caps: Option<Caps>,
}

/// What an ant spent on one thread since a human's last message on it, and in all.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ThreadReport {
    pub ant: UserId,
    pub thread: ThreadId,
    #[serde(flatten)]
    pub spent: Spent,
    /// What it spent on the thread in all, whatever humans' messages there gave it fresh allowances.
    pub total: Spent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub caps: Option<Caps>,
}

/// The `days` UTC days up to `today`, newest first, as `YYYY-MM-DD`.
pub fn window(today: NaiveDate, days: u32) -> Vec<String> {
    (0..days)
        .map_while(|n| today.checked_sub_days(Days::new(n.into())))
        .map(|day| day.format("%Y-%m-%d").to_string())
        .collect()
}

/// The ant and day of a daily spend key, `<ant>.day.<YYYY-MM-DD>`.
fn parse_day_key(key: &str) -> Option<(UserId, String)> {
    let (ant, day) = key.split_once(".day.")?;
    NaiveDate::parse_from_str(day, "%Y-%m-%d").ok()?;
    Some((ant.parse().ok()?, day.to_owned()))
}

/// The ant and thread of a thread spend key, `<ant>.<thread>.spend`.
fn parse_thread_key(key: &str) -> Option<(UserId, ThreadId)> {
    let (ant, rest) = key.split_once('.')?;
    let thread = rest.strip_suffix(".spend")?;
    Some((ant.parse().ok()?, thread.parse().ok()?))
}

/// `ant`'s report over `days` (newest first) from what it spent on the days found, the others counting as nothing.
fn report(
    ant: UserId,
    days: &[String],
    found: &BTreeMap<String, Spent>,
    threads: Option<Vec<ThreadSpent>>,
    caps: Option<Caps>,
) -> AntReport {
    let days: Vec<Day> =
        days.iter().map(|day| Day { day: day.clone(), spent: found.get(day).copied().unwrap_or_default() }).collect();
    let mut total = Spent::default();
    for day in &days {
        total += day.spent;
    }
    AntReport { ant, days, total, threads, caps }
}

/// The keys of [`SESSIONS_BUCKET`] matching the key pattern `filter`, by stream info, which only human credentials
/// allow; none when the bucket does not exist.
async fn keys(js: &jetstream::Context, filter: &str) -> Result<Vec<String>> {
    let prefix = format!("$KV.{SESSIONS_BUCKET}.");
    let stream = match js.get_stream(format!("KV_{SESSIONS_BUCKET}")).await {
        Ok(stream) => stream,
        Err(e) if crate::node::outbox::not_found(&e) => return Ok(Vec::new()),
        Err(e) => return Err(e).context(format!("looking up {SESSIONS_BUCKET}, which needs human credentials")),
    };
    let subjects: Vec<(String, usize)> = stream
        .info_with_subjects(format!("{prefix}{filter}"))
        .await
        .with_context(|| format!("listing {filter} in {SESSIONS_BUCKET}"))?
        .try_collect()
        .await
        .with_context(|| format!("listing {filter} in {SESSIONS_BUCKET}"))?;
    let mut keys: Vec<String> =
        subjects.into_iter().filter_map(|(subject, _)| subject.strip_prefix(&prefix).map(str::to_owned)).collect();
    keys.sort();
    Ok(keys)
}

/// What `ant`, or every ant that spent anything in them, spent in `days`; with `ant`, also its threads.
pub async fn ants(
    js: &jetstream::Context,
    kv: &DirectKv,
    ant: Option<&UserId>,
    days: &[String],
    caps: &BTreeMap<UserId, Caps>,
) -> Result<Vec<AntReport>> {
    let wanted: BTreeSet<&str> = days.iter().map(String::as_str).collect();
    let mut found: BTreeMap<UserId, BTreeMap<String, Spent>> = BTreeMap::new();
    if let Some(ant) = ant {
        found.insert(ant.clone(), BTreeMap::new());
    }
    for key in keys(js, &format!("{}.day.*", ant.map_or("*", UserId::as_str))).await? {
        let Some((ant, day)) = parse_day_key(&key).filter(|(_, day)| wanted.contains(day.as_str())) else { continue };
        if let Some(spent) = caps::day_spent(kv, &ant, &day).await? {
            found.entry(ant).or_default().insert(day, spent);
        }
    }
    let mut reports = Vec::with_capacity(found.len());
    for (name, spent) in found {
        let threads = match ant {
            Some(_) => Some(threads_of(js, kv, &name).await?),
            None => None,
        };
        let caps = caps.get(&name).copied();
        reports.push(report(name, days, &spent, threads, caps));
    }
    Ok(reports)
}

/// The threads of `ant` that spent anything since a human's last message on them, most costly first.
async fn threads_of(js: &jetstream::Context, kv: &DirectKv, ant: &UserId) -> Result<Vec<ThreadSpent>> {
    let mut threads = Vec::new();
    for key in keys(js, &format!("{ant}.*.spend")).await? {
        let Some((ant, thread)) = parse_thread_key(&key) else { continue };
        if let Some(spent) = caps::thread_spent(kv, &ant, &thread).await?
            && (spent.tokens > 0 || spent.cost > 0.0)
        {
            threads.push(ThreadSpent { thread, spent });
        }
    }
    threads.sort_by(|a, b| b.spent.cost.total_cmp(&a.spent.cost).then(b.spent.tokens.cmp(&a.spent.tokens)));
    Ok(threads)
}

/// What `ant`, or each ant on it, spent on `thread` since a human's last message there, and in all.
pub async fn thread(
    js: &jetstream::Context,
    kv: &DirectKv,
    ant: Option<&UserId>,
    thread: &ThreadId,
    caps: &BTreeMap<UserId, Caps>,
) -> Result<Vec<ThreadReport>> {
    let mut reports = Vec::new();
    for key in keys(js, &format!("{}.{thread}.spend", ant.map_or("*", UserId::as_str))).await? {
        let Some((ant, thread)) = parse_thread_key(&key) else { continue };
        if let Some((spent, total)) = caps::thread_spent_and_total(kv, &ant, &thread).await? {
            let caps = caps.get(&ant).copied();
            reports.push(ThreadReport { ant, thread, spent, total, caps });
        }
    }
    Ok(reports)
}

fn tokens(n: u64, cap: Option<u64>) -> String {
    match cap {
        Some(cap) => format!("{n}/{cap}"),
        None => n.to_string(),
    }
}

fn cost(cost: f64, cap: Option<f64>) -> String {
    match cap {
        Some(cap) => format!("{cost:.2}/{cap:.2}"),
        None => format!("{cost:.2}"),
    }
}

/// What each ant spent on each of `threads` since a human's last message there, for the threads it spent anything on.
pub async fn on_threads(
    js: &jetstream::Context,
    kv: &DirectKv,
    threads: &HashSet<ThreadId>,
) -> Result<HashMap<ThreadId, Vec<(UserId, Spent)>>> {
    let found = keys(js, "*.*.spend").await?.into_iter().filter_map(|key| parse_thread_key(&key));
    let reads = futures::stream::iter(found.filter(|(_, thread)| threads.contains(thread)))
        .map(async |(ant, thread)| Ok::<_, anyhow::Error>((caps::thread_spent(kv, &ant, &thread).await?, ant, thread)))
        .buffer_unordered(CONCURRENT);
    let read: Vec<Result<_>> = reads.collect().await;
    let mut spent: HashMap<ThreadId, Vec<(UserId, Spent)>> = HashMap::new();
    for read in read {
        if let (Some(s), ant, thread) = read? {
            spent.entry(thread).or_default().push((ant, s));
        }
    }
    spent.values_mut().for_each(|by| by.sort_by(|a, b| a.0.cmp(&b.0)));
    Ok(spent)
}

/// What each ant that spent anything that UTC day spent in it.
pub async fn today(js: &jetstream::Context, kv: &DirectKv) -> Result<Vec<(UserId, Spent)>> {
    let days = window(Utc::now().date_naive(), 1);
    let reports = ants(js, kv, None, &days, &BTreeMap::new()).await?;
    Ok(reports.into_iter().map(|r| (r.ant, r.total)).collect())
}

/// `rows` under `header`, columns padded to line up.
fn layout(header: &[&str], rows: Vec<Vec<String>>) -> String {
    let rows: Vec<Vec<String>> =
        std::iter::once(header.iter().map(|h| (*h).to_owned()).collect()).chain(rows).collect();
    let mut widths = vec![0; header.len()];
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let lines: Vec<String> = rows
        .iter()
        .map(|row| {
            let cells: Vec<String> = row.iter().zip(&widths).map(|(cell, width)| format!("{cell:width$}")).collect();
            cells.join("  ").trim_end().to_owned()
        })
        .collect();
    lines.join("\n")
}

/// One row per ant: what it spent today, against its daily caps, and in all `days` days.
pub fn summary_table(reports: &[AntReport], days: u32) -> String {
    let (tokens_n, cost_n) = (format!("TOKENS_{days}D"), format!("COST_{days}D"));
    let rows = reports
        .iter()
        .map(|r| {
            let caps = r.caps.unwrap_or_default();
            let today = r.days.first().map_or_default(|d| d.spent);
            vec![
                r.ant.to_string(),
                tokens(today.tokens, caps.daily_tokens),
                cost(today.cost, caps.daily_cost),
                r.total.tokens.to_string(),
                cost(r.total.cost, None),
            ]
        })
        .collect();
    layout(&["ANT", "TODAY_TOKENS", "TODAY_COST", &tokens_n, &cost_n], rows)
}

/// One ant's days, each against its daily caps, and their total; then its threads, each against its thread caps.
pub fn ant_tables(report: &AntReport) -> String {
    let caps = report.caps.unwrap_or_default();
    let mut rows: Vec<Vec<String>> = report
        .days
        .iter()
        .map(|d| vec![d.day.clone(), tokens(d.spent.tokens, caps.daily_tokens), cost(d.spent.cost, caps.daily_cost)])
        .collect();
    rows.push(vec!["TOTAL".to_owned(), report.total.tokens.to_string(), cost(report.total.cost, None)]);
    let mut out = layout(&["DAY", "TOKENS", "COST"], rows);
    match report.threads.as_deref() {
        Some([]) => out.push_str("\n\nno thread has spent anything since a human's last message on it"),
        Some(threads) => {
            let rows = threads
                .iter()
                .map(|t| {
                    let spent = t.spent;
                    vec![
                        t.thread.to_string(),
                        tokens(spent.tokens, caps.thread_tokens),
                        cost(spent.cost, caps.thread_cost),
                    ]
                })
                .collect();
            out.push_str("\n\nspent since a human's last message on the thread:\n");
            out.push_str(&layout(&["THREAD", "TOKENS", "COST"], rows));
        }
        None => {}
    }
    out
}

/// One row per ant on the thread: what it spent since a human's last message there, against its thread caps, then in
/// all.
pub fn thread_table(reports: &[ThreadReport]) -> String {
    let rows = reports
        .iter()
        .map(|r| {
            let caps = r.caps.unwrap_or_default();
            vec![
                r.ant.to_string(),
                r.thread.to_string(),
                tokens(r.spent.tokens, caps.thread_tokens),
                cost(r.spent.cost, caps.thread_cost),
                r.total.tokens.to_string(),
                format!("{:.2}", r.total.cost),
            ]
        })
        .collect();
    layout(&["ANT", "THREAD", "TOKENS", "COST", "TOTAL_TOKENS", "TOTAL_COST"], rows)
}

fn in_days(days: u32) -> String {
    if days == 1 { "today (UTC)".to_owned() } else { format!("in the last {days} UTC days") }
}

/// Print what `select` asks for: tables, or with `json` one JSON object per ant (per ant on the thread, with
/// `--thread`) per line.
pub async fn show(nats: &Nats<'_>, select: &Select, caps: &BTreeMap<UserId, Caps>, json: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-spend").await?;
    let kv = DirectKv::new(&js, None, SESSIONS_BUCKET).await?;
    let ant = select.ant.as_ref();
    if let Some(id) = &crate::threads::thread::resolve(select.thread.clone(), nats, &js).await? {
        let reports = thread(&js, &kv, ant, id, caps).await?;
        if json {
            for report in &reports {
                println!("{}", serde_json::to_string(report)?);
            }
        } else if reports.is_empty() {
            let by = ant.map_or_default(|ant| format!(" by {ant}"));
            println!("no spend recorded on thread {id}{by}");
        } else {
            println!("{}", thread_table(&reports));
        }
        return Ok(());
    }
    let days = window(Utc::now().date_naive(), select.days);
    let reports = ants(&js, &kv, ant, &days, caps).await?;
    if json {
        for report in &reports {
            println!("{}", serde_json::to_string(report)?);
        }
        return Ok(());
    }
    match (ant, reports.first()) {
        (Some(ant), Some(report))
            if report.total == Spent::default() && report.threads.as_ref().is_none_or(Vec::is_empty) =>
        {
            println!("no spend recorded by {ant} {}", in_days(select.days));
        }
        (Some(_), Some(report)) => println!("{}", ant_tables(report)),
        _ if reports.is_empty() => println!("no spend recorded {}", in_days(select.days)),
        _ => println!("{}", summary_table(&reports, select.days)),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use ants_daemon::bus;
    use ants_daemon::harness::Spend;
    use ants_daemon::limits::caps::Ledger;
    use ants_proto::{Role, daily_spend_key, session_key};

    use super::*;
    use crate::test_support::Server;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn date(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    fn spent(tokens: u64, cost: f64) -> Spent {
        Spent { tokens, cost }
    }

    #[test]
    fn the_window_runs_back_from_today_across_months() {
        assert_eq!(window(date("2026-03-02"), 3), ["2026-03-02", "2026-03-01", "2026-02-28"]);
        assert_eq!(window(date("2026-03-02"), 1), ["2026-03-02"]);
    }

    #[test]
    fn keys_parse_into_ant_and_day_or_thread() {
        assert_eq!(parse_day_key("dev-a.day.2026-11-14"), Some((p("dev-a"), "2026-11-14".to_owned())));
        assert_eq!(parse_day_key("dev-a.day.spend"), None, "the spend of a thread named day");
        assert_eq!(parse_day_key("dev-a.t1.spend"), None);
        let t1: ThreadId = "t1".parse().unwrap();
        assert_eq!(parse_thread_key("dev-a.t1.spend"), Some((p("dev-a"), t1)));
        assert_eq!(parse_thread_key("dev-a.day.spend").map(|(_, t)| t.to_string()), Some("day".to_owned()));
        assert_eq!(parse_thread_key("dev-a.t1"), None, "a session");
        assert_eq!(parse_thread_key("dev-a.t1.seen"), None);
        assert_eq!(parse_thread_key("dev-a.mention.7.alice"), None);
    }

    #[test]
    fn a_report_fills_the_days_without_spend_and_adds_them_up() {
        let days = window(date("2026-11-14"), 3);
        let found =
            BTreeMap::from([("2026-11-14".to_owned(), spent(100, 0.25)), ("2026-11-12".to_owned(), spent(50, 1.0))]);
        let r = report(p("dev-a"), &days, &found, None, None);
        let got: Vec<(&str, Spent)> = r.days.iter().map(|d| (d.day.as_str(), d.spent)).collect();
        assert_eq!(
            got,
            [("2026-11-14", spent(100, 0.25)), ("2026-11-13", Spent::default()), ("2026-11-12", spent(50, 1.0))]
        );
        assert_eq!(r.total, spent(150, 1.25));
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["days"][0], serde_json::json!({"day": "2026-11-14", "tokens": 100, "cost": 0.25}));
        assert!(json.get("caps").is_none() && json.get("threads").is_none(), "{json}");
    }

    #[test]
    fn tables_show_spend_against_the_caps_known() {
        let days = window(date("2026-11-14"), 2);
        let found = BTreeMap::from([("2026-11-14".to_owned(), spent(1200, 0.4))]);
        let caps = Caps { daily_tokens: Some(5000), thread_cost: Some(2.0), ..Caps::default() };
        let capped = report(p("dev-a"), &days, &found, None, Some(caps));
        let uncapped = report(p("messenger"), &days, &BTreeMap::new(), None, None);
        assert_eq!(
            summary_table(&[capped.clone(), uncapped], 2),
            "ANT        TODAY_TOKENS  TODAY_COST  TOKENS_2D  COST_2D\n\
             dev-a      1200/5000     0.40        1200       0.40\n\
             messenger  0             0.00        0          0.00"
        );

        let thread = ThreadSpent { thread: "t1".parse().unwrap(), spent: spent(30, 0.5) };
        let one = AntReport { threads: Some(vec![thread]), ..capped.clone() };
        assert_eq!(
            ant_tables(&one),
            "DAY         TOKENS     COST\n\
             2026-11-14  1200/5000  0.40\n\
             2026-11-13  0/5000     0.00\n\
             TOTAL       1200       0.40\n\
             \n\
             spent since a human's last message on the thread:\n\
             THREAD  TOKENS  COST\n\
             t1      30      0.50/2.00"
        );
        let idle = AntReport { threads: Some(Vec::new()), ..capped };
        assert!(ant_tables(&idle).ends_with("no thread has spent anything since a human's last message on it"));

        let on_thread = ThreadReport {
            ant: p("dev-a"),
            thread: "t1".parse().unwrap(),
            spent: spent(30, 0.5),
            total: spent(80, 1.25),
            caps: Some(caps),
        };
        assert_eq!(
            thread_table(&[on_thread]),
            "ANT    THREAD  TOKENS  COST       TOTAL_TOKENS  TOTAL_COST\n\
             dev-a  t1      30      0.50/2.00  80            1.25"
        );
    }

    /// Spend as the daemons store it, read with the human's credentials.
    #[tokio::test]
    async fn the_human_reads_what_each_ant_spent() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
        let (_, h_js) = server.connect_as(&human, Role::Human).await;
        let kv = DirectKv::new(&h_js, None, SESSIONS_BUCKET).await.unwrap();
        let today = date("2026-11-14");
        let days = window(today, 7);
        let none = BTreeMap::new();
        assert!(ants(&h_js, &kv, None, &days, &none).await.unwrap().is_empty(), "no bucket yet");
        bus::init_own_buckets(&h_js).await.unwrap();
        assert!(ants(&h_js, &kv, None, &days, &none).await.unwrap().is_empty(), "nothing spent yet");

        let t1: ThreadId = "t1".parse().unwrap();
        let (_, a_js) = server.connect(&dev_a).await;
        let a_kv = bus::ensure_sessions(&a_js, None, &dev_a).await.unwrap();
        // An ant without caps still keeps what it spends.
        let ledger = Ledger::kv(a_kv.clone(), dev_a.clone(), Caps::default());
        ledger.record_on(&t1, &Spend::Tokens(100), &days[0]).await.unwrap();
        a_kv.put(daily_spend_key(&dev_a, &days[3]), serde_json::to_vec(&spent(50, 0.5)).unwrap()).await.unwrap();
        a_kv.put(session_key(&dev_a, &t1), "session-1").await.unwrap();
        let (_, b_js) = server.connect(&dev_b).await;
        let b_kv = bus::ensure_sessions(&b_js, None, &dev_b).await.unwrap();
        let b_ledger = Ledger::kv(b_kv.clone(), dev_b.clone(), Caps::default());
        b_ledger.record_on(&t1, &Spend::Tokens(7), &days[0]).await.unwrap();
        let long_ago = window(today, 20).pop().unwrap();
        b_kv.put(daily_spend_key(&dev_b, &long_ago), serde_json::to_vec(&spent(1, 9.0)).unwrap()).await.unwrap();

        let caps = BTreeMap::from([(dev_a.clone(), Caps { daily_tokens: Some(1000), ..Caps::default() })]);
        let all = ants(&h_js, &kv, None, &days, &caps).await.unwrap();
        let totals: Vec<(&str, Spent)> = all.iter().map(|r| (r.ant.as_str(), r.total)).collect();
        assert_eq!(totals, [("dev-a", spent(150, 0.5)), ("dev-b", spent(7, 0.0))], "dev-b's older day is left out");
        assert_eq!(all[0].caps, caps.get(&dev_a).copied());
        assert_eq!((all[0].threads.as_ref(), all[1].caps), (None, None));

        let one = ants(&h_js, &kv, Some(&dev_a), &days, &caps).await.unwrap();
        assert_eq!(one.len(), 1);
        let threads = one[0].threads.clone().unwrap();
        assert_eq!(threads, [ThreadSpent { thread: t1.clone(), spent: spent(100, 0.0) }]);
        let month = window(today, 30);
        let b = ants(&h_js, &kv, Some(&dev_b), &month, &caps).await.unwrap();
        assert_eq!(b[0].total, spent(8, 9.0));
        let nobody = ants(&h_js, &kv, Some(&p("dev-c")), &days, &caps).await.unwrap();
        assert_eq!((nobody[0].total, nobody[0].threads.as_deref()), (Spent::default(), Some(&[][..])));

        let on_t1 = thread(&h_js, &kv, None, &t1, &caps).await.unwrap();
        let got: Vec<(&str, Spent, Spent)> = on_t1.iter().map(|r| (r.ant.as_str(), r.spent, r.total)).collect();
        assert_eq!(got, [("dev-a", spent(100, 0.0), spent(100, 0.0)), ("dev-b", spent(7, 0.0), spent(7, 0.0))]);
        ledger.reset(&t1).await.unwrap();
        ledger.record_on(&t1, &Spend::Tokens(20), &days[0]).await.unwrap();
        let on_t1 = thread(&h_js, &kv, Some(&dev_a), &t1, &caps).await.unwrap();
        assert_eq!((on_t1[0].spent, on_t1[0].total), (spent(20, 0.0), spent(120, 0.0)), "a human's message");
        let line = serde_json::to_value(&on_t1[0]).unwrap();
        assert_eq!((&line["tokens"], &line["total"]), (&20.into(), &serde_json::json!({"tokens": 120, "cost": 0.0})));
        assert_eq!(thread(&h_js, &kv, Some(&dev_b), &t1, &caps).await.unwrap().len(), 1);

        // The human reads spend, not the ants' sessions.
        assert!(kv.get(session_key(&dev_a, &t1)).await.is_err());
    }
}
