//! How an avatar moves: the [`Frame`] a head is drawn in at each moment, as its [`Motion`] in a [`Mood`] paces it.

use super::fnv1a64;

/// Which antenna twitches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Left,
    Right,
}

/// How a head is drawn at a moment of its animation; the default is a still head, eyes open, facing front.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub struct Frame {
    pub closed: bool,
    /// The antenna whose tip bends in.
    pub twitch: Option<Side>,
    /// The mandibles close.
    pub chew: bool,
    /// The colour drains.
    pub grey: bool,
    /// The brow's shine at this column, -1 to 12, as it sweeps across.
    pub glint: Option<i8>,
    /// The antennae turn red.
    pub alarm: bool,
    /// The head dips a pixel.
    pub bob: bool,
    /// -1 looking left, 1 right, 0 front.
    pub turn: i8,
}

/// What an ant is doing, as its avatar shows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum Mood {
    /// Live with nothing to do: it looks around now and then.
    #[default]
    Idle,
    /// A prompt runs: a shine sweeps its brow.
    Thinking,
    /// A tool call runs: it chews.
    Tool,
    /// It waits on a human: its antennae wave.
    Waiting,
    /// It fails: its antennae flash red.
    Stuck,
    /// A message from it arrived: it nods.
    Message,
    /// Its eyes shut.
    Paused,
    /// Stale or signed out: eyes shut, colour drained.
    Offline,
}

impl Mood {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Thinking => "thinking",
            Self::Tool => "tool",
            Self::Waiting => "waiting",
            Self::Stuck => "stuck",
            Self::Message => "message",
            Self::Paused => "paused",
            Self::Offline => "offline",
        }
    }

    fn still(self) -> bool {
        matches!(self, Self::Paused | Self::Offline)
    }
}

/// One avatar's motion in a mood: blinks, antenna twitches and glances on a schedule its seed and mood make the same
/// on every screen, and the rest driven by the mood. Times are seconds on any clock that only goes forward.
#[derive(Debug, Clone)]
pub struct Motion {
    mood: Mood,
    rng: SplitMix,
    offset: f64,
    blink_at: f64,
    blink_until: f64,
    twitch_at: f64,
    twitch_until: f64,
    side: Side,
    look_at: f64,
    look_from: f64,
    look_dir: i8,
}

impl Motion {
    /// The motion of the avatar of `seed` in `mood`, starting at `t`.
    pub fn new(seed: &str, mood: Mood, t: f64) -> Self {
        let mut rng = SplitMix(fnv1a64(format!("{seed}{}", mood.as_str()).as_bytes()));
        let blink_at = t + rng.uniform(0.5, 4.0);
        let twitch_at = t + rng.uniform(2.0, 8.0);
        let offset = rng.uniform(0.0, 3.0);
        let look_at = t + rng.uniform(2.0, 6.0);
        Self {
            mood,
            rng,
            offset,
            blink_at,
            blink_until: 0.0,
            twitch_at,
            twitch_until: 0.0,
            side: Side::Left,
            look_at,
            look_from: f64::NEG_INFINITY,
            look_dir: 1,
        }
    }

    pub fn mood(&self) -> Mood {
        self.mood
    }

    /// The frame at `t`, no earlier than the last asked for.
    pub fn frame(&mut self, t: f64) -> Frame {
        let mood = self.mood;
        if !mood.still() {
            if t >= self.blink_at {
                self.blink_until = t + 0.15;
                // Now and then a second blink follows.
                self.blink_at = t + if self.rng.next() < 0.2 { 0.35 } else { self.rng.uniform(2.5, 6.5) };
            }
            if t >= self.twitch_at {
                self.twitch_until = t + 0.25;
                self.side = if self.rng.next() < 0.5 { Side::Left } else { Side::Right };
                self.twitch_at = t + self.rng.uniform(5.0, 12.0);
            }
            if t >= self.look_at && mood == Mood::Idle {
                self.look_from = t;
                self.look_dir = if self.rng.next() < 0.5 { -1 } else { 1 };
                self.look_at = t + self.rng.uniform(6.0, 14.0);
            }
        }
        // A glance: one side for 0.7s, the front for 0.3s, the other side for 0.7s.
        let since = t - self.look_from;
        let turn = if since < 0.7 {
            self.look_dir
        } else if (1.0..1.7).contains(&since) {
            -self.look_dir
        } else {
            0
        };
        let mut twitch = (t < self.twitch_until).then_some(self.side);
        let shifted = t + self.offset;
        if mood == Mood::Waiting {
            // Antennae wave left, right, left, right for 1.2s every 3s.
            let phase = shifted % 3.0;
            twitch =
                (phase < 1.2).then(|| if ((phase / 0.3) as u32).is_multiple_of(2) { Side::Left } else { Side::Right });
        }
        let sweep = shifted % 2.5;
        let flash = shifted % 2.0;
        let nod = shifted % 4.0;
        Frame {
            closed: mood.still() || t < self.blink_until,
            twitch,
            chew: mood == Mood::Tool && (t / 0.3) as u64 % 2 == 1,
            grey: mood == Mood::Offline,
            // A shine crosses the brow in 0.6s every 2.5s.
            glint: (mood == Mood::Thinking && sweep < 0.6).then(|| (sweep / 0.6 * 13.0) as i8 - 1),
            // Two red flashes every 2s.
            alarm: mood == Mood::Stuck && flash < 0.8 && ((flash / 0.2) as u32).is_multiple_of(2),
            // Two quick nods every 4s.
            bob: mood == Mood::Message && nod < 0.6 && (nod / 0.15) as u32 % 2 == 1,
            turn,
        }
    }
}

/// SplitMix64: a small generator, enough to pace an animation the same everywhere.
#[derive(Debug, Clone)]
struct SplitMix(u64);

impl SplitMix {
    /// The next number in [0, 1).
    fn next(&mut self) -> f64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        (z >> 11) as f64 / (1u64 << 53) as f64
    }

    fn uniform(&mut self, low: f64, high: f64) -> f64 {
        low + (high - low) * self.next()
    }
}

#[cfg(test)]
mod tests;
