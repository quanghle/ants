//! The schedules of the config file: each one's next firing, warning of one that fires no more and of one sending to an
//! ant no daemon serves, its firings waiting in the ant's queue.

use ants_daemon::schedule::Schedule;
use ants_proto::{ColonyEntry, Destination};
use chrono::{DateTime, Utc};

use super::Check;

/// One check per schedule of `schedules`, at `now`, against the `colony`'s entries when read.
pub(super) fn check_schedules(
    schedules: &[Schedule],
    colony: Option<&[ColonyEntry]>,
    now: DateTime<Utc>,
) -> Vec<Check> {
    schedules.iter().map(|schedule| check_schedule(schedule, colony, now)).collect()
}

fn check_schedule(schedule: &Schedule, colony: Option<&[ColonyEntry]>, now: DateTime<Utc>) -> Check {
    let check = format!("schedule {}", schedule.name);
    let Some(next) = schedule.when.next_after(now) else {
        let fix = vec![format!("give schedule {} a timetable to come in the config file, or remove it", schedule.name)];
        return Check::warn(check, format!("{} fires no more", schedule.when), fix);
    };
    let next = next.format("%Y-%m-%d %H:%M UTC");
    match &schedule.destination {
        Destination::Ant(ant) => {
            let served = colony.map(|entries| {
                entries.iter().find(|entry| entry.ant == *ant && ants_daemon::colony::is_live(entry, now))
            });
            match served {
                Some(None) => Check::warn(
                    check,
                    format!(
                        "{}, next {next}, sends to {ant}, which no daemon serves: its firings wait in its queue",
                        schedule.when
                    ),
                    vec![format!(
                        "serve {ant} with `ants daemon` on its machine, or point the schedule at a served ant"
                    )],
                ),
                Some(Some(entry)) => {
                    let paused =
                        if entry.paused.is_some() { ", paused: firings skipped or, with catch_up, held" } else { "" };
                    Check::ok(check, format!("{}, next {next}, to {ant} on {}{paused}", schedule.when, entry.machine))
                }
                None => Check::ok(check, format!("{}, next {next}, to {ant} (colony not read)", schedule.when)),
            }
        }
        Destination::Channel(channel) => Check::ok(check, format!("{}, next {next}, to #{channel}", schedule.when)),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ants_daemon::schedule::{Mentions, When};
    use ants_test_support::user as p;

    use super::*;
    use crate::doctor::Status;

    fn schedule(when: When, to: &str) -> Schedule {
        Schedule {
            name: p("nightly"),
            creds: None,
            nats: None,
            js_domain: None,
            when,
            destination: Destination::Ant(p(to)),
            mentions: Mentions::Nobody,
            owner: None,
            text: "check".to_owned(),
            trail: None,
            catch_up: false,
            allow: None,
        }
    }

    #[test]
    fn a_schedule_firing_no_more_or_to_an_ant_no_daemon_serves_warns() {
        let now = Utc::now();
        let hourly = When::Every(Duration::from_secs(3_600));
        let colony = [ants_test_support::colony_entry("dev-a", "m1")];
        let checks = check_schedules(
            &[
                schedule(hourly.clone(), "dev-a"),
                schedule(hourly, "dev-b"),
                schedule(When::Every(Duration::MAX), "dev-a"),
            ],
            Some(&colony),
            now,
        );
        let statuses: Vec<_> = checks.iter().map(|c| c.status).collect();
        assert_eq!(statuses, [Status::Ok, Status::Warn, Status::Warn], "{checks:?}");
        assert!(checks[1].detail.contains("dev-b, which no daemon serves"), "{}", checks[1].detail);
        assert!(checks[2].detail.contains("fires no more"), "{}", checks[2].detail);
    }
}
