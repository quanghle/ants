//! The [`Avatar`] an ant's seed derives: the parts of its pixel-art head, drawn as a [`Grid`] of [`Pixel`]s, and its
//! one-line ASCII face.

use std::fmt;

use super::{AntIdentity, Frame, Palette, Rgb, Side, Stage, Theme, fnv1a64};
use crate::UserId;

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

#[cfg(test)]
mod tests;
