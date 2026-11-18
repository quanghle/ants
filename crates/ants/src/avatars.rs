//! The ants' avatars as the CLI shows them ([`ants_proto::Avatar`]): each ant's identity record read from
//! [`IDENTITIES_BUCKET`], its one-line face and pixel-art head coloured for the terminal, and the ants whose faces
//! cannot be told apart.

use std::collections::BTreeMap;
use std::io::IsTerminal as _;
use std::time::Duration;

use ants_daemon::config::Theme as ThemeSetting;
use ants_daemon::records::direct_kv::{DirectKv, Lookup};
use ants_proto::{
    AntIdentity, Avatar, Frame, IDENTITIES_BUCKET, INCARNATION_KEY, JsDomain, Palette, Rgb, Stage, Theme, UserId,
    hue_named,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;

/// Every ant's identity record, by name.
pub type Identities = BTreeMap<UserId, AntIdentity>;

/// How long [`read_or_none`] reads before giving up, so that an unreachable bucket does not hold up a command.
const READ_TIMEOUT: Duration = Duration::from_secs(3);

/// Every ant's identity record, read with a human's credentials by direct gets of the bucket's stream.
pub async fn read(js: &jetstream::Context) -> Result<Identities> {
    let filter = format!("$KV.{IDENTITIES_BUCKET}.*");
    let mut identities = Identities::new();
    for (key, payload) in crate::machines::latest_in(js, IDENTITIES_BUCKET, &filter).await? {
        if key == INCARNATION_KEY {
            continue;
        }
        match serde_json::from_slice::<AntIdentity>(&payload) {
            Ok(record) if record.check_key(&key).is_ok() => {
                identities.insert(record.ant.clone(), record);
            }
            Ok(_) => tracing::warn!(key, "skipping the identity of another ant"),
            Err(e) => tracing::warn!(key, error = %e, "skipping a malformed identity record"),
        }
    }
    Ok(identities)
}

/// [`read`], or no records when the bucket cannot be read within a few seconds: each ant's avatar then comes from its
/// name, as for an ant with no record.
pub async fn read_or_none(js: &jetstream::Context) -> Identities {
    match tokio::time::timeout(READ_TIMEOUT, read(js)).await {
        Ok(Ok(identities)) => identities,
        Ok(Err(e)) => {
            tracing::debug!("avatars from the ants' names: {e:#}");
            Identities::new()
        }
        Err(_) => {
            tracing::debug!("avatars from the ants' names: no answer within {READ_TIMEOUT:?}");
            Identities::new()
        }
    }
}

/// How many colours a terminal shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Depth {
    /// None: `NO_COLOR` is set, or the output is no terminal.
    Plain,
    /// The 256-colour palette.
    Palette,
    /// 24-bit colour, as `COLORTERM` tells.
    True,
}

impl Depth {
    /// The colours standard output shows.
    pub fn stdout() -> Self {
        let no_color = std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty());
        if no_color || !std::io::stdout().is_terminal() {
            return Self::Plain;
        }
        Self::of_colorterm(std::env::var("COLORTERM").ok().as_deref())
    }

    fn of_colorterm(colorterm: Option<&str>) -> Self {
        match colorterm {
            Some("truecolor" | "24bit") => Self::True,
            _ => Self::Palette,
        }
    }
}

/// The ants' faces, from their identity records (else their names), coloured as standard output shows them.
pub struct Faces {
    identities: Identities,
    depth: Depth,
    theme: Theme,
}

impl Faces {
    pub fn new(identities: Identities, depth: Depth, theme: Theme) -> Self {
        Self { identities, depth, theme }
    }

    /// The faces of `identities` for standard output, in the theme of the config file's `[cli]`.
    pub fn stdout(identities: Identities) -> Self {
        let depth = Depth::stdout();
        let theme = if depth == Depth::Plain { Theme::Dark } else { theme() };
        Self::new(identities, depth, theme)
    }

    /// Faces without colour, every ant's from its name.
    #[cfg(test)]
    pub fn plain() -> Self {
        Self::new(Identities::new(), Depth::Plain, Theme::Dark)
    }

    pub fn identity(&self, ant: &UserId) -> Option<&AntIdentity> {
        self.identities.get(ant)
    }

    pub fn avatar(&self, ant: &UserId) -> Avatar {
        Avatar::of(ant, self.identity(ant))
    }

    /// The ant's face, such as `(ovo)`, in its colour.
    pub fn face(&self, ant: &UserId) -> String {
        let avatar = self.avatar(ant);
        paint(self.depth, &avatar.face(), avatar.color(self.theme))
    }

    /// The ant's face and name, the face in its colour.
    pub fn named(&self, ant: &UserId) -> String {
        format!("{} {ant}", self.face(ant))
    }

    /// The face and name of `user` when it has an identity record, as only ants do.
    pub fn known(&self, user: &UserId) -> Option<String> {
        self.identities.contains_key(user).then(|| self.named(user))
    }

    /// The ant's head at `stage` in `frame`: half-blocks in its colours, or the ASCII grid of its pixels without colour.
    pub fn head(&self, ant: &UserId, stage: Stage, frame: &Frame) -> Vec<String> {
        let avatar = self.avatar(ant);
        let grid = avatar.head(stage, frame);
        if self.depth == Depth::Plain {
            return grid.to_string().lines().map(str::to_owned).collect();
        }
        let palette = Palette::new(avatar.hue, self.theme, stage, frame.grey, frame.alarm);
        grid.halfblocks(&palette)
            .iter()
            .map(|row| {
                let line: String = row.iter().map(|cell| cell_text(self.depth, cell)).collect();
                line.trim_end().to_owned()
            })
            .collect()
    }

    /// A warning for each pair of `ants` whose faces share outline, eyes, mouth and colour, so that only their names
    /// tell them apart.
    pub fn collisions<'a>(&self, ants: impl IntoIterator<Item = &'a UserId>) -> Vec<String> {
        let mut seen: BTreeMap<(String, usize), &UserId> = BTreeMap::new();
        let mut warnings = Vec::new();
        for ant in ants {
            let avatar = self.avatar(ant);
            let face = avatar.face();
            match seen.get(&(face.clone(), avatar.hue)) {
                Some(first) if *first != ant => warnings.push(format!(
                    "{first} and {ant} share the face {face} in {}: `ants ant avatar {ant} --seed WORD` gives {ant} \
                     another",
                    avatar.hue_name()
                )),
                Some(_) => {}
                None => {
                    seen.insert((face, avatar.hue), ant);
                }
            }
        }
        warnings
    }
}

/// `text` in `color`, as `depth` shows it.
pub fn paint(depth: Depth, text: &str, color: Rgb) -> String {
    match depth {
        Depth::Plain => text.to_owned(),
        Depth::Palette => format!("\x1b[38;5;{}m{text}\x1b[0m", color.ansi256()),
        Depth::True => format!("\x1b[38;2;{};{};{}m{text}\x1b[0m", color.0, color.1, color.2),
    }
}

fn cell_text(depth: Depth, cell: &ants_proto::Cell) -> String {
    let code = |ground: u8, c: Rgb| match depth {
        Depth::True => format!("{ground}8;2;{};{};{}", c.0, c.1, c.2),
        _ => format!("{ground}8;5;{}", c.ansi256()),
    };
    match (cell.fg, cell.bg) {
        (None, None) => cell.ch.to_string(),
        (Some(fg), None) => format!("\x1b[{}m{}\x1b[0m", code(3, fg), cell.ch),
        (None, Some(bg)) => format!("\x1b[{}m{}\x1b[0m", code(4, bg), cell.ch),
        (Some(fg), Some(bg)) => format!("\x1b[{};{}m{}\x1b[0m", code(3, fg), code(4, bg), cell.ch),
    }
}

/// The theme of the config file's `[cli]` `theme`, the terminal's when `auto` ([`detect`]).
pub fn theme() -> Theme {
    let setting = crate::defaults::cli().map_or_else(|_| Default::default(), |d| d.theme);
    match setting {
        ThemeSetting::Dark => Theme::Dark,
        ThemeSetting::Light => Theme::Light,
        ThemeSetting::Auto => detect(),
    }
}

/// The theme suiting the terminal's background: the colour the terminal answers an OSC 11 query with within 200ms,
/// light when its luminance is above a half; else light when `COLORFGBG` ends in 7 or 15; else dark.
pub fn detect() -> Theme {
    let background = crate::tty::ask(b"\x1b]11;?\x07").iter().find_map(|answer| parse_background(answer));
    if let Some(theme) = background.map(|rgb| luminance_theme(rgb.0, rgb.1, rgb.2)) {
        return theme;
    }
    colorfgbg_theme(std::env::var("COLORFGBG").ok().as_deref())
}

fn luminance_theme(r: f64, g: f64, b: f64) -> Theme {
    if 0.2126 * r + 0.7152 * g + 0.0722 * b > 0.5 { Theme::Light } else { Theme::Dark }
}

fn colorfgbg_theme(colorfgbg: Option<&str>) -> Theme {
    match colorfgbg.and_then(|v| v.rsplit(';').next()) {
        Some("7" | "15") => Theme::Light,
        _ => Theme::Dark,
    }
}

/// The colour of an OSC 11 answer such as `ESC ] 11 ; rgb:ffff/ffff/ffff BEL`, each channel 0 to 1.
fn parse_background(reply: &[u8]) -> Option<(f64, f64, f64)> {
    let text = std::str::from_utf8(reply).ok()?;
    let rgb = text.split_once("\x1b]11;rgb:")?.1;
    let rgb = rgb.split(['\x07', '\x1b']).next()?;
    let mut channels = rgb.split('/').map(|hex| {
        let hex = hex.get(..hex.len().min(4))?;
        let max = 16f64.powi(i32::try_from(hex.len()).ok()?) - 1.0;
        Some(f64::from(u16::from_str_radix(hex, 16).ok()?) / max)
    });
    Some((channels.next()??, channels.next()??, channels.next()??))
}

/// Set or clear the avatar seed and colour of `ant`'s identity record by compare-and-set, with a human's credentials:
/// `seed` and `color` each Some(None) to clear, None to keep. The record as stored.
pub async fn update(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    ant: &UserId,
    seed: Option<Option<String>>,
    color: Option<Option<String>>,
) -> Result<AntIdentity> {
    if let Some(Some(color)) = &color
        && hue_named(color).is_none()
    {
        bail!("no colour {color}: one of {}", ants_proto::HUE_NAMES.join(", "));
    }
    let kv = DirectKv::whole(js, js_domain, IDENTITIES_BUCKET).await?;
    for _ in 0..5 {
        let (found, last) = kv.lookup_last(ant.as_str()).await?;
        let mut record = match found {
            Lookup::NoBucket => bail!("no {IDENTITIES_BUCKET} bucket: `ants init` creates it"),
            Lookup::NoValue => bail!("{ant} has no identity record: its daemon creates one when it starts"),
            Lookup::Value(value) => {
                let record: AntIdentity =
                    serde_json::from_slice(&value).with_context(|| format!("reading the identity of {ant}"))?;
                record.check_key(ant.as_str())?;
                record
            }
        };
        if let Some(seed) = &seed {
            record.avatar.clone_from(seed);
        }
        if let Some(color) = &color {
            record.color = color.as_deref().and_then(hue_named).map(|i| ants_proto::HUE_NAMES[i].to_owned());
        }
        if kv.update(ant.as_str(), serde_json::to_vec(&record)?, last).await? {
            return Ok(record);
        }
    }
    bail!("the identity of {ant} kept changing while being updated")
}

/// What `ants ant avatar` changes in the ant's record: a new seed or colour, or back to the record's own.
#[derive(Debug, Default)]
pub struct Change {
    /// Some(None) clears the seed, back to the sense id the record names, else its id.
    pub seed: Option<Option<String>>,
    /// Some(None) clears the colour, back to the seed's hue.
    pub color: Option<Option<String>>,
}

/// `ants ant avatar`: print the avatar of `ant`, its birth and growth, after making `change` to its identity record.
pub async fn show(nats: &crate::connection::Nats<'_>, ant: &UserId, change: Change) -> Result<()> {
    let (_, js) = nats.connect("ants-ant-avatar").await?;
    let changed = if change.seed.is_some() || change.color.is_some() {
        Some(update(&js, nats.domain, ant, change.seed, change.color).await?)
    } else {
        None
    };
    let mut identities = match changed {
        Some(_) => Identities::new(),
        None => read(&js).await?,
    };
    if let Some(record) = changed {
        identities.insert(ant.clone(), record);
    }
    let faces = Faces::stdout(identities);
    print!("{}", describe(&faces, ant, chrono::Utc::now()));
    Ok(())
}

/// The head of `ant` and lines on its face, parts, colour, birth and growth.
pub fn describe(faces: &Faces, ant: &UserId, now: chrono::DateTime<chrono::Utc>) -> String {
    use std::fmt::Write as _;
    let record = faces.identity(ant);
    let stage = record.map_or(Stage::Callow, AntIdentity::stage);
    let avatar = faces.avatar(ant);
    let mut out = String::new();
    for line in faces.head(ant, stage, &Frame::default()) {
        out.push_str(&line);
        out.push('\n');
    }
    let [antennae, head, eyes, mandibles, markings] = avatar.parts();
    let set = |set: bool| if set { " (set)" } else { "" };
    let _ = write!(
        out,
        "{}  {}{}\n{antennae} antennae, {head} head, {eyes} eyes, {mandibles} mandibles, {markings} markings{}\n",
        faces.named(ant),
        avatar.hue_name(),
        set(record.is_some_and(|r| r.color.is_some())),
        set(record.is_some_and(|r| r.avatar.is_some())),
    );
    let Some(record) = record else {
        out.push_str("no identity record yet: the avatar comes from the name until the ant's daemon starts\n");
        return out;
    };
    let age = crate::colony::short(crate::colony::since(record.born, now));
    let _ = writeln!(out, "born {} ({age} ago)", record.born.format("%Y-%m-%d %H:%M:%SZ"));
    let next = match stage.next_at() {
        Some(at) => format!("; {} at {at}", Stage::of(at).as_str()),
        None => String::new(),
    };
    let prompts = if record.prompts == 1 { "prompt" } else { "prompts" };
    let _ = writeln!(out, "{}, {} {prompts}{next}", stage.as_str(), record.prompts);
    out
}

#[cfg(test)]
mod tests;
