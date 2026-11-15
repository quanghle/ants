use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::IDENTITIES_BUCKET;
use crate::{Error, UserId};

/// An ant's record in [`IDENTITIES_BUCKET`], under the key of its name: what makes it the same ant on every machine.
/// Its daemon creates it on start only when absent, so across machines the first creator wins and later ones keep it;
/// the daemon serving the ant then adds each completed prompt by compare-and-set, keeping the other fields. Humans set
/// and clear `avatar` and `color` (`ants ant avatar`), by compare-and-set too. `ants auth revoke NAME` of the ant
/// deletes it, so a later ant of that name is born anew. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AntIdentity {
    pub ant: UserId,
    /// 128 random bits in lowercase hex, set at birth; the avatar's seed unless `avatar` sets one.
    pub id: String,
    /// When the record was created.
    pub born: DateTime<Utc>,
    /// The avatar's seed a human set, in place of `id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    /// The avatar's colour a human set, one of [`HUE_NAMES`], in place of the seed's hue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    /// The prompts the ant completed since its birth.
    #[serde(default)]
    pub prompts: u64,
    /// Set when the ant was moved to another bus, by whom and when; no daemon serves it while set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub moved: Option<MovedMark>,
    /// The ant this one was cloned from, when it was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloned_from: Option<ClonedFrom>,
}

/// When and by whom an ant was moved off its bus ([`AntIdentity::moved`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovedMark {
    pub at: DateTime<Utc>,
    pub by: UserId,
}

/// The ant a clone was made from ([`AntIdentity::cloned_from`]): its name and id then, and when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClonedFrom {
    pub ant: UserId,
    pub id: String,
    pub at: DateTime<Utc>,
}

impl AntIdentity {
    /// A record for `ant` born at `born` with `id`, before any prompt.
    pub fn new(ant: UserId, id: String, born: DateTime<Utc>) -> Self {
        Self { ant, id, born, avatar: None, color: None, prompts: 0, moved: None, cloned_from: None }
    }

    pub fn key(&self) -> &str {
        self.ant.as_str()
    }

    /// Check that the record was read from its own key: credentials pin each ant to its own key, while the body is
    /// whatever the writer put there.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "identity record", user: self.ant.clone(), at: format!("key {key}") })
    }

    /// The avatar's seed: the one a human set, else the record's id.
    pub fn seed(&self) -> &str {
        self.avatar.as_deref().unwrap_or(&self.id)
    }

    pub fn stage(&self) -> Stage {
        Stage::of(self.prompts)
    }
}

/// FNV-1a 64 of `bytes`: stable across builds and platforms, as std's hashers are not.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3))
}

// The part tables are a contract: changing one redraws every ant, so they only grow, behind a version. Each is the left
// half of its rows (6 of 12 columns), mirrored: '#' antenna, 'B' head, 'M' mandible.
const ANTENNAE: [[&str; 4]; 8] = [
    ["#.....", ".#....", "..#...", "...#.."],
    ["#.....", "#.....", ".##...", "...#.."],
    ["##....", "##....", "..#...", "...#.."],
    [".##...", "#..#..", "...#..", "...#.."],
    ["......", "##....", "..##..", "....#."],
    ["#.#...", ".#....", "..#...", "...#.."],
    [".#....", "#.....", ".#....", "..##.."],
    ["......", "......", "..#...", "...#.."],
];
const HEADS: [[&str; 6]; 6] = [
    ["...BBB", "..BBBB", ".BBBBB", ".BBBBB", "..BBBB", "...BBB"],
    ["..BBB.", ".BBBBB", "BBBBBB", ".BBBBB", "..BBBB", "....BB"],
    ["..BBBB", "BBBBBB", "BBBBBB", "BBBBBB", ".BBBBB", "..BBBB"],
    ["...BBB", "...BBB", "..BBBB", "..BBBB", "..BBBB", "...BBB"],
    ["..BBBB", ".BBBBB", ".BBBBB", ".BBBBB", ".BBBBB", "..BBBB"],
    [".BBBBB", ".BBBBB", "..BBBB", "..BBBB", "...BBB", "....BB"],
];
/// Eye pixels as (row, column) on the left half; the first is the glint.
const EYES: [&[(usize, usize)]; 8] = [
    &[(6, 2)],
    &[(6, 1), (6, 2), (7, 1), (7, 2)],
    &[(5, 3), (6, 3)],
    &[(6, 1), (7, 1), (8, 1)],
    &[(6, 2), (6, 3)],
    &[(5, 2), (5, 3), (6, 2), (6, 3)],
    &[(7, 1), (7, 2), (7, 3)],
    &[(8, 2)],
];
const MANDIBLES: [[&str; 2]; 6] = [
    ["...M..", "....M."],
    ["..M...", "...MM."],
    [".MM...", "...M.."],
    ["..MM..", "....M."],
    ["...M..", "...M.."],
    [".M....", "..MM.."],
];
/// Markings, as darker pixels on the left half.
const MARKINGS: [&[(usize, usize)]; 4] =
    [&[], &[(6, 5), (7, 5)], &[(5, 5), (6, 5), (7, 5), (8, 5)], &[(8, 2), (8, 3), (8, 4), (8, 5)]];
const OUTLINES: [(char, char); 4] = [('(', ')'), ('[', ']'), ('{', '}'), ('<', '>')];
const FACE_EYES: [char; 8] = ['o', 'O', '^', '-', '*', '@', '.', '0'];
const MOUTHS: [char; 6] = ['v', 'w', '=', 'x', 'u', '~'];

pub const ANTENNAE_NAMES: [&str; 8] = ["straight", "elbowed", "clubbed", "curled", "swept", "forked", "wavy", "stubby"];
pub const HEAD_NAMES: [&str; 6] = ["round", "heart", "wide", "narrow", "square", "shield"];
pub const EYES_NAMES: [&str; 8] = ["dot", "big", "high", "tall", "wide", "big high", "slit", "low dot"];
pub const MANDIBLES_NAMES: [&str; 6] = ["short", "crossing", "wide", "hooked", "straight", "sweeping"];
pub const MARKINGS_NAMES: [&str; 4] = ["plain", "spot", "stripe", "band"];
/// The avatar colours, in hue order from red, each a twelfth of the colour wheel on from the last.
pub const HUE_NAMES: [&str; 12] =
    ["red", "orange", "yellow", "lime", "green", "mint", "cyan", "azure", "blue", "violet", "magenta", "rose"];

/// Characters of every one-line face: outline, eye, mouth, eye, outline.
pub const FACE_WIDTH: usize = 5;
/// Rows of a head's grid: 12 pixels and a blank row above and below, room for the head to bob.
pub const HEAD_ROWS: usize = 14;
/// Columns of a head's grid: 12 pixels and a blank column each side, room for the head to turn.
pub const HEAD_COLUMNS: usize = 14;

/// An ant's avatar as its seed picks it: the parts of its pixel-art head, its hue and its one-line ASCII face.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Avatar {
    pub antennae: usize,
    pub head: usize,
    pub eyes: usize,
    pub mandibles: usize,
    pub markings: usize,
    /// Index into [`HUE_NAMES`].
    pub hue: usize,
    face: (usize, usize, usize),
}

impl Avatar {
    /// The avatar `seed` picks, with FNV-1a 64.
    pub fn derive(seed: &str) -> Self {
        let mut h = fnv1a64(seed.as_bytes());
        let mut pick = |n: usize| {
            let n = n as u64;
            let picked = (h % n) as usize;
            h /= n;
            picked
        };
        Self {
            antennae: pick(ANTENNAE.len()),
            head: pick(HEADS.len()),
            eyes: pick(EYES.len()),
            mandibles: pick(MANDIBLES.len()),
            markings: pick(MARKINGS.len()),
            hue: pick(HUE_NAMES.len()),
            face: (pick(OUTLINES.len()), pick(FACE_EYES.len()), pick(MOUTHS.len())),
        }
    }

    /// The avatar of `ant` with `record` its identity record, when one is readable: from the record's seed and in its
    /// colour, else from the ant's name.
    pub fn of(ant: &UserId, record: Option<&AntIdentity>) -> Self {
        let Some(record) = record else {
            return Self::derive(ant.as_str());
        };
        let mut avatar = Self::derive(record.seed());
        if let Some(hue) = record.color.as_deref().and_then(hue_named) {
            avatar.hue = hue;
        }
        avatar
    }

    /// The one-line ASCII face, such as `(ovo)`.
    pub fn face(&self) -> String {
        let (outline, eye, mouth) = self.face;
        let (open, close) = OUTLINES[outline];
        let eye = FACE_EYES[eye];
        format!("{open}{eye}{}{eye}{close}", MOUTHS[mouth])
    }

    pub fn hue_name(&self) -> &'static str {
        HUE_NAMES[self.hue]
    }

    /// The colour of the face: the body's, as the worker stage on `theme` draws it.
    pub fn color(&self, theme: Theme) -> Rgb {
        Palette::new(self.hue, theme, Stage::Worker, false, false).body
    }

    /// The parts by name: antennae, head, eyes, mandibles, markings.
    pub fn parts(&self) -> [&'static str; 5] {
        [
            ANTENNAE_NAMES[self.antennae],
            HEAD_NAMES[self.head],
            EYES_NAMES[self.eyes],
            MANDIBLES_NAMES[self.mandibles],
            MARKINGS_NAMES[self.markings],
        ]
    }

    /// The head's pixels in `frame` at growth `stage`, [`HEAD_ROWS`] by [`HEAD_COLUMNS`].
    pub fn head(&self, stage: Stage, frame: &Frame) -> Grid {
        let mut grid: Vec<Vec<Pixel>> = ANTENNAE[self.antennae]
            .iter()
            .chain(HEADS[self.head].iter())
            .chain(MANDIBLES[self.mandibles].iter())
            .map(|half| {
                let left: Vec<Pixel> = half.chars().map(Pixel::from_char).collect();
                left.iter().chain(left.iter().rev()).copied().collect()
            })
            .collect();
        if let Some(side) = frame.twitch {
            let half = match side {
                Side::Left => 0..6,
                Side::Right => 6..12,
            };
            // The top two rows bend in together, so the antenna stays joined.
            for row in &mut grid[..2] {
                let tip = toward_centre(row);
                row[half.clone()].copy_from_slice(&tip[half.clone()]);
            }
        }
        if frame.chew {
            grid[11] = toward_centre(&grid[11]);
        }
        for &(r, c) in MARKINGS[self.markings] {
            if grid[r][c] == Pixel::Body {
                grid[r][c] = Pixel::Marking;
                grid[r][11 - c] = Pixel::Marking;
            }
        }
        match frame.glint {
            None => {
                if let Some(&(r, c)) = [(5, 4), (5, 3)].iter().find(|&&(r, c)| grid[r][c] == Pixel::Body) {
                    grid[r][c] = Pixel::Highlight;
                    grid[r][11 - c] = Pixel::Highlight;
                }
            }
            Some(glint) => {
                for (r, c) in [(4, i32::from(glint) + 1), (5, i32::from(glint))] {
                    if let Ok(c) = usize::try_from(c)
                        && c < 12
                        && matches!(grid[r][c], Pixel::Body | Pixel::Marking)
                    {
                        grid[r][c] = Pixel::Highlight;
                    }
                }
            }
        }
        let eye = EYES[self.eyes];
        let lid = eye.iter().map(|&(r, _)| r).max().unwrap_or(0);
        for (i, &(r, c)) in eye.iter().enumerate() {
            let mut c = c;
            while c < 5 && !grid[r][c].on_head() {
                c += 1;
            }
            let px = if frame.closed {
                if r == lid { Pixel::Outline } else { Pixel::Body }
            } else if i == 0 && eye.len() > 1 {
                Pixel::Glint
            } else {
                Pixel::Pupil
            };
            grid[r][c] = px;
            grid[r][11 - c] = px;
        }
        if stage >= Stage::Veteran {
            for (r, c) in [(6, 9), (7, 8), (8, 7)] {
                if grid[r][c].on_head() {
                    grid[r][c] = Pixel::Scar;
                }
            }
        }
        if stage >= Stage::Elder {
            for (r, c) in [(2, 4), (2, 7), (3, 4), (3, 5), (3, 6), (3, 7)] {
                grid[r][c] = Pixel::Crown;
            }
        }
        let mut padded = [[Pixel::Blank; HEAD_COLUMNS]; 12];
        for (to, from) in padded.iter_mut().zip(&grid) {
            to[1..13].copy_from_slice(from);
        }
        if frame.turn != 0 {
            padded = turned(&padded, frame.turn.signum());
        }
        let top = if frame.bob { 2 } else { 1 };
        let mut rows = [[Pixel::Blank; HEAD_COLUMNS]; HEAD_ROWS];
        rows[top..top + 12].copy_from_slice(&padded);
        Grid(rows)
    }
}

/// The index into [`HUE_NAMES`] of the colour `name`.
pub fn hue_named(name: &str) -> Option<usize> {
    HUE_NAMES.iter().position(|n| n.eq_ignore_ascii_case(name.trim()))
}

/// Each half's pixels moved a column toward the centre, staying in their half.
fn toward_centre(row: &[Pixel]) -> Vec<Pixel> {
    let mut out = vec![Pixel::Blank; 12];
    for (c, &px) in row.iter().enumerate() {
        if px == Pixel::Blank {
            continue;
        }
        let n = if c < 5 {
            c + 1
        } else if c > 6 {
            c - 1
        } else {
            c
        };
        out[n] = px;
    }
    out
}

/// The head looking aside, `d` -1 left or 1 right: the face's features (eyes, markings, shine, scar) and the jaw slide
/// a column toward `d` while the head's outline stays, so the near eye reaches the edge and the far one comes in, as a
/// three-quarter view; the antennae lean the same way. A crown sits on the head, so it stays.
fn turned(grid: &[[Pixel; HEAD_COLUMNS]; 12], d: i8) -> [[Pixel; HEAD_COLUMNS]; 12] {
    let mut head: Vec<(usize, usize)> = (4..10)
        .flat_map(|r| (0..HEAD_COLUMNS).map(move |c| (r, c)))
        .filter(|&(r, c)| grid[r][c] != Pixel::Blank)
        .collect();
    let on_head = |r: usize, c: usize| grid[r][c] != Pixel::Blank && (4..10).contains(&r);
    let mut out = *grid;
    for &(r, c) in &head {
        out[r][c] = Pixel::Body;
    }
    // Where features pile up at the edge, eyes win.
    let rank = |px: Pixel| match px {
        Pixel::Highlight => 0,
        Pixel::Marking => 1,
        Pixel::Scar => 2,
        Pixel::Outline => 3,
        Pixel::Pupil => 4,
        Pixel::Glint => 5,
        _ => -1,
    };
    head.sort_by_key(|&(r, c)| rank(grid[r][c]));
    for (r, c) in head {
        let px = grid[r][c];
        if px == Pixel::Body {
            continue;
        }
        let step = |n: usize| if d > 0 { n.wrapping_sub(1) } else { n + 1 };
        let mut n = if d > 0 { c + 1 } else { c.wrapping_sub(1) };
        while n != c && !(n < HEAD_COLUMNS && on_head(r, n)) {
            n = step(n);
        }
        out[r][n] = px;
    }
    for r in 0..4 {
        let row = &grid[r];
        let mut moved = row.map(|px| if px == Pixel::Crown { px } else { Pixel::Blank });
        for (c, &px) in row.iter().enumerate() {
            if px == Pixel::Outline {
                let to = c as isize + isize::from(d);
                let n = if r < 3 && (0..HEAD_COLUMNS as isize).contains(&to) && row[to as usize] != Pixel::Crown {
                    to as usize
                } else {
                    c
                };
                moved[n] = px;
            }
        }
        out[r] = moved;
    }
    for r in [10, 11] {
        let mut row = [Pixel::Blank; HEAD_COLUMNS];
        if d > 0 {
            row[1..].copy_from_slice(&grid[r][..HEAD_COLUMNS - 1]);
        } else {
            row[..HEAD_COLUMNS - 1].copy_from_slice(&grid[r][1..]);
        }
        out[r] = row;
    }
    out
}

/// A pixel of a head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Pixel {
    Blank,
    /// Antennae and shut eyelids; they flash red when the ant is stuck.
    Outline,
    Body,
    /// The brow's shine.
    Highlight,
    Marking,
    Mandible,
    Pupil,
    /// The white of an eye's first pixel.
    Glint,
    /// The elder's crown.
    Crown,
    /// The veteran's scar.
    Scar,
}

impl Pixel {
    fn from_char(c: char) -> Self {
        match c {
            '#' => Self::Outline,
            'B' => Self::Body,
            'M' => Self::Mandible,
            _ => Self::Blank,
        }
    }

    /// The pixel as the prototype tables write it: `.` blank, `#` outline, `B` body, `H` highlight, `P` marking, `M`
    /// mandible, `K` pupil, `W` glint, `G` crown, `S` scar.
    pub fn as_char(self) -> char {
        match self {
            Self::Blank => '.',
            Self::Outline => '#',
            Self::Body => 'B',
            Self::Highlight => 'H',
            Self::Marking => 'P',
            Self::Mandible => 'M',
            Self::Pupil => 'K',
            Self::Glint => 'W',
            Self::Crown => 'G',
            Self::Scar => 'S',
        }
    }

    fn on_head(self) -> bool {
        matches!(self, Self::Body | Self::Highlight | Self::Marking)
    }
}

/// A head's pixels, [`HEAD_ROWS`] rows of [`HEAD_COLUMNS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Grid(pub [[Pixel; HEAD_COLUMNS]; HEAD_ROWS]);

impl Grid {
    /// The head in half-blocks, a line of [`HEAD_COLUMNS`] cells for every two pixel rows, coloured by `palette`.
    pub fn halfblocks(&self, palette: &Palette) -> Vec<Vec<Cell>> {
        self.0
            .chunks(2)
            .map(|pair| {
                (0..HEAD_COLUMNS)
                    .map(|x| {
                        let (top, bottom) = (palette.of(pair[0][x]), palette.of(pair[1][x]));
                        match (top, bottom) {
                            (None, None) => Cell { ch: ' ', fg: None, bg: None },
                            (Some(top), None) => Cell { ch: '▀', fg: Some(top), bg: None },
                            (None, Some(bottom)) => Cell { ch: '▄', fg: Some(bottom), bg: None },
                            (Some(top), Some(bottom)) => Cell { ch: '▀', fg: Some(top), bg: Some(bottom) },
                        }
                    })
                    .collect()
            })
            .collect()
    }
}

impl fmt::Display for Grid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for row in &self.0 {
            let line: String = row.iter().map(|px| px.as_char()).collect();
            writeln!(f, "{line}")?;
        }
        Ok(())
    }
}

/// A half-block of a drawn head: its character with the top pixel's colour as `fg` and the bottom one's as `bg`, or
/// either alone, or neither for a blank.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cell {
    pub ch: char,
    pub fg: Option<Rgb>,
    pub bg: Option<Rgb>,
}

/// A 24-bit colour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Rgb(pub u8, pub u8, pub u8);

impl Rgb {
    /// The nearest colour of the 256-colour palette's 6x6x6 cube or grey ramp.
    pub fn ansi256(self) -> u8 {
        let level = |v: u8| -> (u8, u8) {
            const STEPS: [u8; 6] = [0, 95, 135, 175, 215, 255];
            let i = STEPS
                .iter()
                .enumerate()
                .min_by_key(|(_, s)| (i32::from(**s) - i32::from(v)).abs())
                .map_or(0, |(i, _)| i);
            (i as u8, STEPS[i])
        };
        let ((r, rv), (g, gv), (b, bv)) = (level(self.0), level(self.1), level(self.2));
        let distance = |x: (u8, u8, u8)| {
            let d = |a: u8, b: u8| (i32::from(a) - i32::from(b)).pow(2);
            d(x.0, self.0) + d(x.1, self.1) + d(x.2, self.2)
        };
        let cube = 16 + 36 * r + 6 * g + b;
        let mean = (u16::from(self.0) + u16::from(self.1) + u16::from(self.2)) / 3;
        let grey = if mean < 8 { 0 } else { ((mean - 8) / 10).min(23) as u8 };
        let grey_value = 8 + 10 * grey;
        if distance((grey_value, grey_value, grey_value)) < distance((rv, gv, bv)) { 232 + grey } else { cube }
    }
}

/// The terminal's background, which the palette suits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Theme {
    #[default]
    Dark,
    Light,
}

/// An ant's growth, by the prompts it completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Stage {
    /// Below 10 prompts: pale, as a newly emerged ant is.
    Callow,
    /// From 10: full colour.
    Worker,
    /// From 100: a scar across the right cheek.
    Veteran,
    /// From 1000: a crown between the antennae.
    Elder,
}

impl Stage {
    pub fn of(prompts: u64) -> Self {
        match prompts {
            0..10 => Self::Callow,
            10..100 => Self::Worker,
            100..1000 => Self::Veteran,
            _ => Self::Elder,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Callow => "callow",
            Self::Worker => "worker",
            Self::Veteran => "veteran",
            Self::Elder => "elder",
        }
    }

    /// Prompts to reach the next stage; None for the last.
    pub fn next_at(self) -> Option<u64> {
        match self {
            Self::Callow => Some(10),
            Self::Worker => Some(100),
            Self::Veteran => Some(1000),
            Self::Elder => None,
        }
    }
}

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

/// The colours of a head's pixels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Palette {
    pub body: Rgb,
    pub highlight: Rgb,
    pub marking: Rgb,
    pub outline: Rgb,
    pub mandible: Rgb,
    pub pupil: Rgb,
    pub glint: Rgb,
    pub crown: Rgb,
    pub scar: Rgb,
}

struct ThemeValues {
    body: f64,
    highlight: f64,
    marking: f64,
    dark: f64,
    saturation: f64,
    callow: (f64, f64),
    grey: (f64, f64, f64, f64),
    alarm: Rgb,
    gold: Rgb,
    scar: Rgb,
}

const DARK: ThemeValues = ThemeValues {
    body: 0.50,
    highlight: 0.68,
    marking: 0.38,
    dark: 0.30,
    saturation: 0.65,
    callow: (0.72, 0.30),
    grey: (0.35, 0.45, 0.28, 0.25),
    alarm: Rgb(225, 45, 45),
    gold: Rgb(235, 185, 40),
    scar: Rgb(240, 225, 215),
};
const LIGHT: ThemeValues = ThemeValues {
    body: 0.42,
    highlight: 0.58,
    marking: 0.30,
    dark: 0.20,
    saturation: 0.70,
    callow: (0.62, 0.35),
    grey: (0.62, 0.72, 0.52, 0.45),
    alarm: Rgb(200, 20, 20),
    gold: Rgb(190, 140, 0),
    scar: Rgb(250, 240, 232),
};

impl Palette {
    /// The colours of hue `hue` (an index into [`HUE_NAMES`]) on `theme` at `stage`, drained when `grey`, the antennae
    /// red when `alarm`.
    pub fn new(hue: usize, theme: Theme, stage: Stage, grey: bool, alarm: bool) -> Self {
        let t = match theme {
            Theme::Dark => &DARK,
            Theme::Light => &LIGHT,
        };
        let h = hue as f64 / HUE_NAMES.len() as f64;
        let (body, highlight, marking, outline) = if grey {
            let (b, hl, p, d) = t.grey;
            (hls(0.0, b, 0.0), hls(0.0, hl, 0.0), hls(0.0, p, 0.0), hls(0.0, d, 0.0))
        } else if stage == Stage::Callow {
            let (l, s) = t.callow;
            (hls(h, l, s), hls(h, (l + 0.12).min(0.9), s), hls(h, l - 0.10, s), hls(h, l - 0.2, s))
        } else {
            let s = t.saturation;
            (hls(h, t.body, s), hls(h, t.highlight, s + 0.05), hls(h, t.marking, s), hls(h, t.dark, s - 0.2))
        };
        Self {
            body,
            highlight,
            marking,
            outline: if alarm { t.alarm } else { outline },
            mandible: outline,
            pupil: Rgb(20, 20, 24),
            glint: Rgb(240, 240, 240),
            crown: t.gold,
            scar: t.scar,
        }
    }

    /// The colour of `px`; None for a blank.
    pub fn of(&self, px: Pixel) -> Option<Rgb> {
        Some(match px {
            Pixel::Blank => return None,
            Pixel::Outline => self.outline,
            Pixel::Body => self.body,
            Pixel::Highlight => self.highlight,
            Pixel::Marking => self.marking,
            Pixel::Mandible => self.mandible,
            Pixel::Pupil => self.pupil,
            Pixel::Glint => self.glint,
            Pixel::Crown => self.crown,
            Pixel::Scar => self.scar,
        })
    }
}

/// Hue, lightness and saturation, each 0 to 1, as RGB, as Python's colorsys converts them.
fn hls(h: f64, l: f64, s: f64) -> Rgb {
    let (r, g, b) = if s == 0.0 {
        (l, l, l)
    } else {
        let m2 = if l <= 0.5 { l * (1.0 + s) } else { l + s - l * s };
        let m1 = 2.0 * l - m2;
        let v = |hue: f64| {
            let hue = hue.rem_euclid(1.0);
            if hue < 1.0 / 6.0 {
                m1 + (m2 - m1) * hue * 6.0
            } else if hue < 0.5 {
                m2
            } else if hue < 2.0 / 3.0 {
                m1 + (m2 - m1) * (2.0 / 3.0 - hue) * 6.0
            } else {
                m1
            }
        };
        (v(h + 1.0 / 3.0), v(h), v(h - 1.0 / 3.0))
    };
    let byte = |c: f64| (c * 255.0).round_ties_even() as u8;
    Rgb(byte(r), byte(g), byte(b))
}

#[cfg(test)]
mod tests;
