//! Tests of the listing of schedules: each one's next firing, the last one due and whether it went out as the keys of
//! the webhooks bucket say, and a schedule that fires no more; and another machine's schedules, listed by admin order.

use std::time::Duration;

use ants_daemon::admin::ConfigFile;
use ants_daemon::schedule::{Mentions, When};
use ants_proto::{Permissions, Role};
use ants_test_support::user as p;

use super::*;
use crate::auth::{Issue, Registered};
use crate::credentials::issued::Kind;
use crate::test_support::Server;
use crate::test_support::orders::host_of_m1;
use crate::test_support::webhook::{creds, hub};

fn schedule(name: &str, when: When) -> Schedule {
    Schedule {
        name: p(name),
        creds: None,
        nats: None,
        js_domain: None,
        when,
        destination: Destination::Ant(p("dev-a")),
        mentions: Mentions::Nobody,
        owner: None,
        text: "check the nightly build".to_owned(),
        trail: None,
        catch_up: false,
        allow: None,
    }
}

/// `schedules` as listed at `now`, `sent` saying which firings went out, when read.
fn listed(schedules: &[Schedule], sent: Option<&Sent>, now: DateTime<Utc>) -> Vec<Listed> {
    with_sent(schedules.iter().map(|schedule| schedule.listed(now)).collect(), sent)
}

fn at(text: &str) -> DateTime<Utc> {
    text.parse().unwrap()
}

#[test]
fn a_schedule_lists_its_next_firing_and_whether_its_last_went_out() {
    let hourly = schedule("pulse", When::Every(Duration::from_secs(3_600)));
    // No timetable the config file takes fires no more; one beyond the clock stands in for it.
    let never = schedule("never", When::Every(Duration::MAX));
    let now = at("2026-11-25T10:20:00Z");
    let sent = Sent::from([(p("pulse"), BTreeSet::from([at("2026-11-25T09:00:00Z").timestamp()]))]);
    let listed = listed(&[hourly.clone(), never], Some(&sent), now);
    assert_eq!(listed[0].next, Some(at("2026-11-25T11:00:00Z")));
    assert_eq!(
        listed[0].last,
        Some(Last { due: at("2026-11-25T10:00:00Z"), sent: Some(false) }),
        "10:00 did not go out"
    );
    assert_eq!((listed[1].next, &listed[1].last), (None, &None));
    let table = table(&listed);
    assert!(table.contains("2026-11-25 10:00 UTC not sent") && table.contains("fires no more"), "{table}");
    assert_eq!(json_line(&listed[0])["ant"], "dev-a");

    let sent = Sent::from([(p("pulse"), BTreeSet::from([at("2026-11-25T10:00:00Z").timestamp()]))]);
    let listed = self::listed(std::slice::from_ref(&hourly), Some(&sent), now);
    assert_eq!(listed[0].last.as_ref().and_then(|last| last.sent), Some(true));
    assert!(super::table(&listed).contains("10:00 UTC sent"));
    let unread = self::listed(&[hourly], None, now);
    assert!(super::table(&unread).contains("(unread)"));
    assert_eq!(super::table(&[]), "no schedules\n");
}

#[tokio::test]
async fn the_keys_of_the_webhooks_bucket_say_which_firings_went_out() {
    let Some((server, js, _)) = hub().await else { return };
    let nightly = schedule("nightly", When::Every(Duration::from_secs(3_600)));
    let creds = creds(&server, &nightly.name, nightly.destination.clone());
    let (_, own) = ants_daemon::bus::connect(&server.url, Some(&creds), None, &nightly.name, "test").await.unwrap();
    for key in ["nightly.1800000000", "nightly.1800003600", "nightly.webhook"] {
        own.publish(subject::kv(WEBHOOKS_BUCKET, key), "{}".into()).await.unwrap().await.unwrap();
    }
    let other = schedule("other", When::Every(Duration::from_secs(3_600)));
    let sent = sent(&js, &[&nightly.name, &other.name]).await.unwrap();
    assert_eq!(sent[&p("nightly")], BTreeSet::from([1_800_000_000, 1_800_003_600]), "the marker is no firing");
    assert!(sent[&p("other")].is_empty());
}

const CONFIG: &str = r#"harness = "copilot"
human = "human"

[[ant]]
name = "dev-a"
workdir = "."

[[schedule]]
name = "nightly"
cron = "0 3 * * *"
ant = "dev-a"
text = "Audit the dependencies."

[[schedule]]
name = "pulse"
every = "15m"
channel = "ops"
text = "Anything stuck?"
catch_up = true
"#;

/// An admin lists the schedules of another machine's config file, which an admin order has the daemon there list,
/// through an ant it serves; with an ant, those sending to it. The order needs a human device's admin credentials,
/// and a machine recording no ant is named.
#[tokio::test]
async fn an_admin_lists_another_machine_s_schedules_by_admin_order() {
    let Some(server) = Server::start() else { return };
    let (alice, bob) = (p("alice"), p("bob"));
    let (_, h_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let device = Registered { label: Some("laptop"), admin: true, ..Registered::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    Issue::new(&server.dir().join("auth"), &alice, Kind::Human, rights).with(&device).into_file(&admin_creds).unwrap();
    let nats = Nats::new(&server.url, Some(&admin_creds), None, None).unwrap();
    let (_, js) = nats.connect("test").await.unwrap();
    let path = server.dir().join("config.toml");
    std::fs::write(&path, CONFIG).unwrap();
    let triggers = crate::reload::Triggers::new(path.clone(), None, Duration::from_secs(3600)).unwrap();
    let (_ants, _task) = host_of_m1(&server, &js, true, Some(ConfigFile::new(path.clone(), triggers.edits()))).await;
    let m1: Machine = "m1".parse().unwrap();

    let listing = read_from(&nats, &js, Source::On(&m1), None).await.unwrap();
    let names: Vec<&str> = listing.listed.iter().map(|schedule| schedule.name.as_str()).collect();
    assert_eq!(names, ["nightly", "pulse"]);
    let local = read(&js, &path, None).await.unwrap();
    assert_eq!(listing.listed.len(), local.listed.len());
    for (remote, local) in listing.listed.iter().zip(&local.listed) {
        assert_eq!((&remote.when, &remote.to, remote.catch_up), (&local.when, &local.to, local.catch_up));
    }
    assert_eq!((listing.left_out, &listing.unread), (0, &None));
    assert!(page(&listing).contains("channel #ops"), "{}", page(&listing));

    let to_dev_a = read_from(&nats, &js, Source::On(&m1), Some(&p("dev-a"))).await.unwrap();
    let names: Vec<&str> = to_dev_a.listed.iter().map(|schedule| schedule.name.as_str()).collect();
    assert_eq!(names, ["nightly"]);

    let nobody = read_from(&nats, &js, Source::On(&"m9".parse().unwrap()), None).await.unwrap_err();
    assert!(nobody.to_string().contains("m9 records serving no ant"), "{nobody:#}");
    let bob_creds = server.creds_as(&bob, Role::Human);
    let not_admin = Nats::new(&server.url, Some(&bob_creds), None, None).unwrap();
    let refused = read_from(&not_admin, &js, Source::On(&m1), None).await.unwrap_err();
    assert!(refused.to_string().contains("admin"), "{refused:#}");
}
