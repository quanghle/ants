use std::collections::HashSet;

use super::*;

#[test]
fn motion_follows_the_mood_the_same_everywhere() {
    let frames = |mood| {
        let mut motion = Motion::new("dev-a", mood, 100.0);
        (0..400).map(|i| motion.frame(100.0 + f64::from(i) * 0.05)).collect::<Vec<_>>()
    };
    assert_eq!(frames(Mood::Idle), frames(Mood::Idle), "seeded, not random");
    let idle = frames(Mood::Idle);
    assert!(idle.iter().any(|f| f.closed), "it blinks");
    assert!(idle.iter().any(|f| f.twitch.is_some()), "it twitches");
    assert!(idle.iter().any(|f| f.turn != 0), "it looks around");
    assert!(idle.iter().all(|f| !f.chew && !f.grey && !f.alarm && !f.bob && f.glint.is_none()));
    assert!(frames(Mood::Thinking).iter().all(|f| f.turn == 0), "only an idle ant looks around");
    let glints: HashSet<_> = frames(Mood::Thinking).iter().filter_map(|f| f.glint).collect();
    assert!(glints.len() > 5, "a shine sweeps the brow: {glints:?}");
    assert!(frames(Mood::Tool).iter().any(|f| f.chew));
    let waving = frames(Mood::Waiting);
    assert!(
        waving.iter().any(|f| f.twitch == Some(Side::Left)) && waving.iter().any(|f| f.twitch == Some(Side::Right))
    );
    assert!(frames(Mood::Stuck).iter().any(|f| f.alarm));
    assert!(frames(Mood::Message).iter().any(|f| f.bob));
    let paused = frames(Mood::Paused);
    assert!(paused.iter().all(|f| f.closed && f.twitch.is_none() && !f.grey && f.turn == 0), "eyes shut, still");
    assert!(frames(Mood::Offline).iter().all(|f| f.closed && f.grey), "greyed");
}
