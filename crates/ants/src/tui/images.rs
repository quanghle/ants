//! Image attachments drawn in the thread view with `[cli]` `inline_images` on: each fetched, decoded and encoded once
//! for the terminal's graphics protocol (Kitty, iTerm2 or Sixel, else halfblocks), fit in [`MAX_COLS`] by
//! [`MAX_ROWS`] cells, and drawn under its line where it fits whole ([`Images`], [`place`], [`draw`]); fetching and
//! encoding run beside the drawing ([`Io`]). An image that fails keeps its line alone.

use std::collections::{HashMap, HashSet};
use std::io::Cursor;

use ants_proto::AttachmentHandle;
use anyhow::Result;
use async_nats::jetstream;
use futures::future::{FutureExt as _, LocalBoxFuture};
use futures::stream::{FuturesUnordered, StreamExt as _};
use ratatui::Frame;
use ratatui::layout::{Rect, Size};
use ratatui::text::Line;
use ratatui_image::picker::Picker;
use ratatui_image::protocol::Protocol;
use ratatui_image::{FilterType, Image, Resize};

use super::model::{Model, Page};
use crate::attachments::{self, Listed};

/// Widest an image is drawn, in cells.
pub const MAX_COLS: u16 = 48;
/// Tallest an image is drawn, in cells.
pub const MAX_ROWS: u16 = 12;
/// Most images of a thread drawn: its latest.
const MAX_IMAGES: usize = 12;
/// Most memory decoding an image takes.
const MAX_DECODED: u64 = 128 * 1024 * 1024;

/// The images encoded for the terminal, by handle: None for one that failed.
#[derive(Default)]
pub struct Images {
    /// Whether images are drawn at all.
    pub on: bool,
    encoded: HashMap<AttachmentHandle, Option<Protocol>>,
}

impl Images {
    pub fn new(on: bool) -> Self {
        Self { on, encoded: HashMap::new() }
    }

    /// The image under `handle`, once encoded.
    pub fn get(&self, handle: &AttachmentHandle) -> Option<&Protocol> {
        self.encoded.get(handle)?.as_ref()
    }

    pub fn insert(&mut self, handle: AttachmentHandle, image: Option<Protocol>) {
        self.encoded.insert(handle, image);
    }

    /// Forget the images but those `wanted`.
    pub fn retain(&mut self, wanted: &[Listed]) {
        self.encoded.retain(|handle, _| wanted.iter().any(|l| &l.handle == handle));
    }

    pub fn clear(&mut self) {
        self.encoded.clear();
    }
}

/// The images to draw for what `model` shows: the latest image attachments of the open thread.
pub fn wanted(model: &Model) -> Vec<Listed> {
    let Page::Thread(page) = model.page() else { return Vec::new() };
    if !model.data.images.on {
        return Vec::new();
    }
    let all = page.history.entries.iter().rev().flat_map(|e| e.attachments.iter().rev());
    let mut wanted: Vec<Listed> =
        all.filter(|l| l.unavailable.is_none() && l.attachment.is_image()).take(MAX_IMAGES).cloned().collect();
    wanted.reverse();
    wanted
}

/// Where an image goes among a view's lines: on the rows from line `line`, `x` cells in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placed {
    pub handle: AttachmentHandle,
    pub line: usize,
    pub x: u16,
    pub size: Size,
}

/// Make room under the line of attachment `listed`, the last of `lines`, for its image when encoded and it fits in
/// `width` cells past `indent`, noting where it goes in `placed`.
pub fn place(
    lines: &mut Vec<Line<'static>>,
    placed: &mut Vec<Placed>,
    images: &Images,
    listed: &Listed,
    indent: usize,
    width: usize,
) {
    let Some(image) = images.get(&listed.handle) else { return };
    let size = image.size();
    let Ok(x) = u16::try_from(indent) else { return };
    if indent + usize::from(size.width) > width {
        return;
    }
    placed.push(Placed { handle: listed.handle.clone(), line: lines.len(), x, size });
    lines.extend(std::iter::repeat_n(Line::raw(""), usize::from(size.height)));
}

/// Draw the images `placed` among lines shown from line `top` in `area`, those whole in it.
pub fn draw(frame: &mut Frame, images: &Images, placed: &[Placed], top: usize, area: Rect) {
    for p in placed {
        let Some(image) = images.get(&p.handle) else { continue };
        let Some(row) = p.line.checked_sub(top) else { continue };
        let fits = row + usize::from(p.size.height) <= usize::from(area.height)
            && usize::from(p.x) + usize::from(p.size.width) <= usize::from(area.width);
        let Ok(row) = u16::try_from(row) else { continue };
        if fits {
            let at = Rect::new(area.x + p.x, area.y + row, p.size.width, p.size.height);
            frame.render_widget(Image::new(image), at);
        }
    }
}

/// How the terminal draws images: asked of it, which `ants chat` does after entering its screen and before reading
/// keys; halfblocks when it does not tell.
pub fn picker() -> Picker {
    let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
    // The query puts the terminal back as it found it; the screen needs it raw.
    let _ = ratatui::crossterm::terminal::enable_raw_mode();
    picker
}

type Encoded = (AttachmentHandle, Option<Protocol>);

/// Fetches and encodes the images wanted, each once while wanted.
pub struct Io {
    picker: Picker,
    js: jetstream::Context,
    asked: HashSet<AttachmentHandle>,
    jobs: FuturesUnordered<LocalBoxFuture<'static, Encoded>>,
}

impl Io {
    pub fn new(picker: Picker, js: jetstream::Context) -> Self {
        Self { picker, js, asked: HashSet::new(), jobs: FuturesUnordered::new() }
    }

    /// Fetch and encode those of `wanted` not asked for yet, forgetting the rest; whether any are new.
    pub fn want(&mut self, wanted: &[Listed]) -> bool {
        self.asked.retain(|handle| wanted.iter().any(|l| &l.handle == handle));
        let mut new = false;
        for listed in wanted {
            if self.asked.insert(listed.handle.clone()) {
                new = true;
                let (js, picker, listed) = (self.js.clone(), self.picker.clone(), listed.clone());
                self.jobs.push(
                    async move {
                        let image = encoded(&js, picker, &listed).await.ok();
                        (listed.handle, image)
                    }
                    .boxed_local(),
                );
            }
        }
        new
    }

    /// Ask for every image again, as after the terminal was handed over and may have dropped those it had.
    pub fn reset(&mut self) {
        self.asked.clear();
    }

    /// The next image encoded that is still wanted; pending while none is being encoded.
    pub async fn next(&mut self) -> Encoded {
        loop {
            match self.jobs.next().await {
                Some((handle, image)) if self.asked.contains(&handle) => return (handle, image),
                Some(_) => {}
                None => std::future::pending().await,
            }
        }
    }
}

/// The image attachment `listed`, fetched, checked and encoded with `picker`.
async fn encoded(js: &jetstream::Context, picker: Picker, listed: &Listed) -> Result<Protocol> {
    let (_, bytes) = attachments::fetch(js, &listed.handle, Some(&listed.attachment)).await?;
    tokio::task::spawn_blocking(move || encode(&picker, &bytes)).await?
}

/// The image in `bytes`, decoded within [`MAX_DECODED`] and fit in [`MAX_COLS`] by [`MAX_ROWS`] cells.
fn encode(picker: &Picker, bytes: &[u8]) -> Result<Protocol> {
    let mut reader = image::ImageReader::new(Cursor::new(bytes)).with_guessed_format()?;
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(MAX_DECODED);
    reader.limits(limits);
    let image = reader.decode()?;
    let fit = Resize::Fit(Some(FilterType::Triangle));
    Ok(picker.new_protocol(image, Size::new(MAX_COLS, MAX_ROWS), fit)?)
}

#[cfg(test)]
pub mod tests {
    use ants_daemon::files;
    use ants_proto::{Envelope, Role, UserId};
    use image::{DynamicImage, ImageFormat, RgbImage};

    use super::*;
    use crate::auth::tests::Server;

    /// A PNG `width` by `height` pixels.
    pub fn png(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = Cursor::new(Vec::new());
        let image = DynamicImage::from(RgbImage::from_pixel(width, height, image::Rgb([200, 40, 40])));
        image.write_to(&mut bytes, ImageFormat::Png).unwrap();
        bytes.into_inner()
    }

    /// The image in `bytes` encoded as halfblocks, at 10 by 20 pixels a cell.
    pub fn halfblocks(bytes: &[u8]) -> Protocol {
        encode(&Picker::halfblocks(), bytes).unwrap()
    }

    #[test]
    fn an_image_fits_in_the_cells_allowed_and_keeps_its_shape() {
        assert_eq!(halfblocks(&png(2000, 500)).size(), Size::new(MAX_COLS, 6), "wide");
        assert_eq!(halfblocks(&png(500, 2000)).size(), Size::new(6, MAX_ROWS), "tall");
        assert_eq!(halfblocks(&png(40, 40)).size(), Size::new(4, 2), "small ones are not enlarged");
        assert!(encode(&Picker::halfblocks(), b"not an image").is_err());
        assert!(encode(&Picker::halfblocks(), &png(40, 40)[..60]).is_err(), "cut short");
    }

    #[test]
    fn an_image_takes_rows_under_its_line_where_it_fits_and_draws_only_whole() {
        let message = Envelope::new(UserId::human(), "dev-a".parse().unwrap(), "look");
        let bytes = png(40, 40);
        let attachment = files::reference("shot.png", "image/png", &bytes).unwrap();
        let [listed] = attachments::listed(&[attachment], |id| message.attachment_handle(id)).try_into().unwrap();
        let mut images = Images::new(true);
        let (mut lines, mut placed) = (vec![Line::raw("📎 shot.png")], Vec::new());
        place(&mut lines, &mut placed, &images, &listed, 2, 40);
        assert_eq!((lines.len(), placed.len()), (1, 0), "not encoded yet");

        images.insert(listed.handle.clone(), Some(halfblocks(&bytes)));
        place(&mut lines, &mut placed, &images, &listed, 2, 5);
        assert_eq!((lines.len(), placed.len()), (1, 0), "too narrow");
        place(&mut lines, &mut placed, &images, &listed, 2, 40);
        let at = Placed { handle: listed.handle.clone(), line: 1, x: 2, size: Size::new(4, 2) };
        assert_eq!((lines.len(), &placed[..]), (3, &[at][..]));

        let drawn = |top, height| {
            let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(10, height)).unwrap();
            let frame = terminal.draw(|f| draw(f, &images, &placed, top, f.area())).unwrap();
            frame.buffer.content.iter().filter(|c| c.bg == ratatui::style::Color::Rgb(200, 40, 40)).count()
        };
        assert_eq!(drawn(0, 3), 8, "two rows of four cells");
        assert_eq!(drawn(0, 2), 0, "cut off at the bottom");
        assert_eq!(drawn(2, 3), 0, "scrolled past");

        images.retain(&[]);
        assert!(images.get(&listed.handle).is_none());
    }

    #[tokio::test]
    async fn the_images_wanted_are_fetched_and_encoded_once() {
        let Some(server) = Server::start() else { return };
        let alice: UserId = "alice".parse().unwrap();
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        js.create_stream(ants_daemon::hub::files_config()).await.unwrap();
        let (shot, notes) = (server.dir().join("shot.png"), server.dir().join("broken.png"));
        std::fs::write(&shot, png(40, 40)).unwrap();
        std::fs::write(&notes, b"not an image").unwrap();
        let files = attachments::read_all(&[shot, notes]).unwrap();
        let mut message = Envelope::new(alice, "dev-a".parse().unwrap(), "see attached");
        message.attachments = attachments::references(&files);
        attachments::store(&js, &files, |id| message.attachment_handle(id)).await.unwrap();
        let listed = attachments::listed_on(&message.subject(), &message.attachments);

        let mut io = Io::new(Picker::halfblocks(), js);
        assert!(io.want(&listed));
        assert!(!io.want(&listed), "asked once");
        let mut got = HashMap::new();
        for _ in 0..2 {
            let (handle, image) = io.next().await;
            got.insert(handle, image.map(|i| i.size()));
        }
        assert_eq!(got[&listed[0].handle], Some(Size::new(4, 2)));
        assert_eq!(got[&listed[1].handle], None, "the line alone");
        io.reset();
        assert!(io.want(&listed[..1]), "asked again");
    }
}
