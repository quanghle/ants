//! Tests of the listing of schedules: each one's next firing, the last one due and whether it went out as the keys of
//! the webhooks bucket say, and a schedule that fires no more.

use std::time::Duration;

use ants_daemon::schedule::{Mentions, When};
use ants_test_support::user as p;

use super::*;
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
    let listed = super::listed(std::slice::from_ref(&hourly), Some(&sent), now);
    assert_eq!(listed[0].last.as_ref().and_then(|last| last.sent), Some(true));
    assert!(super::table(&listed).contains("10:00 UTC sent"));
    let unread = super::listed(&[hourly], None, now);
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
    let sent = sent(&js, &[nightly, other]).await.unwrap();
    assert_eq!(sent[&p("nightly")], BTreeSet::from([1_800_000_000, 1_800_003_600]), "the marker is no firing");
    assert!(sent[&p("other")].is_empty());
}
