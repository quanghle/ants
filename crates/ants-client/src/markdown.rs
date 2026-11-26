//! Markdown in messages as a tree every client draws alike ([`parse`]): paragraphs, headings, block quotes, fenced and
//! indented code blocks, nested and task lists, tables and rules ([`Block`]), holding text, inline code, emphasis,
//! strong and struck text, links and images ([`Inline`]).
//!
//! A line break in the text stays one, as in a chat message, rather than joining the lines as CommonMark does. The
//! tree is safe to show as it is: it holds no HTML, raw HTML in the text coming as its text; control characters and
//! those reordering text are left out, as text or as character references ([`ants_proto::shown`]), tabs becoming
//! spaces but in code blocks; and a link or image is clickable, with an [`Inline::Link::href`] or
//! [`Inline::Image::src`], only to `http:`, `https:` or (a link) `mailto:`, its URL otherwise only shown.

use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, LinkType, Options, Parser, Tag, TagEnd};
use serde::{Deserialize, Serialize};

/// A message's markdown, as blocks.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Document {
    pub blocks: Vec<Block>,
}

/// A block of a message's markdown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Block {
    Paragraph {
        content: Vec<Inline>,
    },
    /// The text of an item of a tight list, in no paragraph: no blank line around it.
    Plain {
        content: Vec<Inline>,
    },
    Heading {
        /// 1 to 6.
        level: u8,
        content: Vec<Inline>,
    },
    Quote {
        blocks: Vec<Block>,
    },
    /// Code, its lines as written: tabs kept, line breaks `\n`.
    Code {
        /// The first word of a fenced block's info string.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        language: Option<String>,
        text: String,
    },
    List {
        /// The number of an ordered list's first item; none for a bulleted list.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start: Option<u64>,
        items: Vec<Item>,
    },
    Table {
        /// How each column is aligned.
        alignments: Vec<Alignment>,
        /// The header's cells.
        head: Vec<Cell>,
        rows: Vec<Vec<Cell>>,
    },
    Rule,
}

/// An item of a list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    /// Whether a task list's item is done; none for an item of no task list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<bool>,
    pub blocks: Vec<Block>,
}

/// A cell of a table: its text.
pub type Cell = Vec<Inline>;

/// How a column of a table is aligned.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Alignment {
    #[default]
    None,
    Left,
    Center,
    Right,
}

/// Text in a block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Inline {
    Text {
        text: String,
    },
    Code {
        text: String,
    },
    /// A line break, as the text breaks its line.
    Break,
    Emphasis {
        content: Vec<Inline>,
    },
    Strong {
        content: Vec<Inline>,
    },
    Strikethrough {
        content: Vec<Inline>,
    },
    Link {
        /// Where it points, as written, to show.
        url: String,
        /// Where a click takes, when [`url`](Inline::Link::url) is safe to follow ([`href`]).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        href: Option<String>,
        content: Vec<Inline>,
    },
    Image {
        /// Where it is, as written, to show.
        url: String,
        /// Where to load it from, when [`url`](Inline::Image::url) is `http:` or `https:`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        src: Option<String>,
        /// Its alternative text.
        alt: Vec<Inline>,
    },
}

/// `text`, markdown, as a tree safe to show (see the [module](self)).
pub fn parse(text: &str) -> Document {
    let options = Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS;
    let mut builder = Builder { open: vec![Frame::new(Open::Root)] };
    for event in Parser::new_ext(text, options) {
        builder.event(event);
    }
    while builder.open.len() > 1 {
        builder.close();
    }
    let mut root = builder.open.pop().expect("the root");
    root.plain();
    Document { blocks: root.blocks }
}

/// `url` when a click may follow it: to `http:`, `https:` or `mailto:`, with no whitespace in it.
pub fn href(url: &str) -> Option<String> {
    safe(url, &["http", "https", "mailto"])
}

/// `url` when an image may be loaded from it: `http:` or `https:`, with no whitespace in it.
fn src(url: &str) -> Option<String> {
    safe(url, &["http", "https"])
}

fn safe(url: &str, schemes: &[&str]) -> Option<String> {
    let (scheme, rest) = url.split_once(':')?;
    let known = schemes.iter().any(|s| s.eq_ignore_ascii_case(scheme));
    (known && !rest.is_empty() && !url.chars().any(|c| c.is_whitespace() || !ants_proto::shown(c)))
        .then(|| url.to_owned())
}

/// `inlines` as plain text: their text and code, that of the links and images in them too, without line breaks.
pub fn plain(inlines: &[Inline]) -> String {
    let mut out = String::new();
    for inline in inlines {
        match inline {
            Inline::Text { text } | Inline::Code { text } => out.push_str(text),
            Inline::Break => {}
            Inline::Emphasis { content }
            | Inline::Strong { content }
            | Inline::Strikethrough { content }
            | Inline::Link { content, .. }
            | Inline::Image { alt: content, .. } => out.push_str(&plain(content)),
        }
    }
    out
}

impl Document {
    /// The links a click may follow ([`href`]), each with its text, in the order they come; none with no text.
    pub fn links(&self) -> Vec<(String, String)> {
        fn inlines(content: &[Inline], out: &mut Vec<(String, String)>) {
            for inline in content {
                match inline {
                    Inline::Link { href, content, .. } => {
                        let text = plain(content);
                        if let Some(href) = href.clone().filter(|_| !text.is_empty()) {
                            out.push((text, href));
                        }
                        inlines(content, out);
                    }
                    Inline::Emphasis { content }
                    | Inline::Strong { content }
                    | Inline::Strikethrough { content }
                    | Inline::Image { alt: content, .. } => inlines(content, out),
                    Inline::Text { .. } | Inline::Code { .. } | Inline::Break => {}
                }
            }
        }
        let mut out = Vec::new();
        each_block(&self.blocks, &mut |block| match block {
            Block::Paragraph { content } | Block::Plain { content } | Block::Heading { content, .. } => {
                inlines(content, &mut out);
            }
            Block::Table { head, rows, .. } => {
                for cell in head.iter().chain(rows.iter().flatten()) {
                    inlines(cell, &mut out);
                }
            }
            Block::Quote { .. } | Block::Code { .. } | Block::List { .. } | Block::Rule => {}
        });
        out
    }

    /// The code blocks, each with its language, in the order they come.
    pub fn code_blocks(&self) -> Vec<(Option<&str>, &str)> {
        let mut out = Vec::new();
        each_block(&self.blocks, &mut |block| {
            if let Block::Code { language, text } = block {
                out.push((language.as_deref(), text.as_str()));
            }
        });
        out
    }
}

/// Visit each of `blocks` and the blocks in them, each before those it holds.
fn each_block<'a>(blocks: &'a [Block], visit: &mut impl FnMut(&'a Block)) {
    for block in blocks {
        visit(block);
        match block {
            Block::Quote { blocks } => each_block(blocks, visit),
            Block::List { items, .. } => {
                for item in items {
                    each_block(&item.blocks, visit);
                }
            }
            _ => {}
        }
    }
}

/// `text` without control characters nor those reordering text, a tab as a space.
fn clean(text: &str) -> String {
    text.chars().filter_map(|c| if c == '\t' { Some(' ') } else { ants_proto::shown(c).then_some(c) }).collect()
}

/// Code without control characters nor those reordering text, but tabs and line breaks.
fn clean_code(text: &str) -> String {
    text.chars().filter(|&c| c == '\t' || c == '\n' || ants_proto::shown(c)).collect()
}

/// What a frame being built is.
enum Open {
    Root,
    Paragraph,
    Heading(u8),
    Quote,
    Code(Option<String>, String),
    List(Option<u64>, Vec<Item>),
    Item(Option<bool>),
    Table(Vec<Alignment>, Vec<Cell>, Vec<Vec<Cell>>),
    /// A row of a table, whether its header, and its cells.
    Row(bool, Vec<Cell>),
    Cell,
    Emphasis,
    Strong,
    Strikethrough,
    /// A link: its URL, and where a click takes.
    Link(String, Option<String>),
    Image(String),
    /// What the parser's options leave out, its text kept in what holds it.
    Other,
}

/// A block or inline being built, with the blocks and the text it holds so far.
struct Frame {
    open: Open,
    blocks: Vec<Block>,
    inlines: Vec<Inline>,
}

impl Frame {
    fn new(open: Open) -> Self {
        Self { open, blocks: Vec::new(), inlines: Vec::new() }
    }

    /// Make the text held outside any block, as in a tight list's item, a block of its own.
    fn plain(&mut self) {
        if !self.inlines.is_empty() {
            let content = std::mem::take(&mut self.inlines);
            self.blocks.push(Block::Plain { content });
        }
    }
}

struct Builder {
    open: Vec<Frame>,
}

impl Builder {
    fn top(&mut self) -> &mut Frame {
        self.open.last_mut().expect("the root")
    }

    fn event(&mut self, event: Event<'_>) {
        match event {
            Event::Start(tag) => self.start(tag),
            Event::End(tag) => self.end(&tag),
            Event::Text(text) => match &mut self.top().open {
                Open::Code(_, code) => code.push_str(&clean_code(&text)),
                _ => self.text(&text),
            },
            Event::Code(code) => {
                let text = clean(&code);
                if !text.is_empty() {
                    self.top().inlines.push(Inline::Code { text });
                }
            }
            Event::InlineMath(text)
            | Event::DisplayMath(text)
            | Event::FootnoteReference(text)
            | Event::InlineHtml(text) => self.text(&text),
            Event::Html(html) => {
                for line in html.lines() {
                    self.text(line);
                    self.top().inlines.push(Inline::Break);
                }
            }
            Event::SoftBreak | Event::HardBreak => self.top().inlines.push(Inline::Break),
            Event::Rule => self.block(Block::Rule),
            Event::TaskListMarker(done) => {
                if let Some(Frame { open: Open::Item(task), .. }) =
                    self.open.iter_mut().rev().find(|f| matches!(f.open, Open::Item(_)))
                {
                    *task = Some(done);
                }
            }
        }
    }

    /// Add `text`, cleaned, to the text being built, joined to text just before it.
    fn text(&mut self, text: &str) {
        let text = clean(text);
        if text.is_empty() {
            return;
        }
        match self.top().inlines.last_mut() {
            Some(Inline::Text { text: before }) => before.push_str(&text),
            _ => self.top().inlines.push(Inline::Text { text }),
        }
    }

    /// Add `block` to the frame being built, after the text it holds so far.
    fn block(&mut self, block: Block) {
        let top = self.top();
        top.plain();
        top.blocks.push(block);
    }

    fn start(&mut self, tag: Tag<'_>) {
        let open = match tag {
            Tag::Paragraph | Tag::HtmlBlock => Open::Paragraph,
            Tag::Heading { level, .. } => Open::Heading(match level {
                HeadingLevel::H1 => 1,
                HeadingLevel::H2 => 2,
                HeadingLevel::H3 => 3,
                HeadingLevel::H4 => 4,
                HeadingLevel::H5 => 5,
                HeadingLevel::H6 => 6,
            }),
            Tag::BlockQuote(_) => Open::Quote,
            Tag::CodeBlock(kind) => {
                let language = match kind {
                    CodeBlockKind::Fenced(info) => info.split_whitespace().next().map(clean),
                    CodeBlockKind::Indented => None,
                };
                Open::Code(language.filter(|l| !l.is_empty()), String::new())
            }
            Tag::List(start) => Open::List(start, Vec::new()),
            Tag::Item => Open::Item(None),
            Tag::Table(alignments) => {
                let alignments = alignments
                    .into_iter()
                    .map(|a| match a {
                        pulldown_cmark::Alignment::None => Alignment::None,
                        pulldown_cmark::Alignment::Left => Alignment::Left,
                        pulldown_cmark::Alignment::Center => Alignment::Center,
                        pulldown_cmark::Alignment::Right => Alignment::Right,
                    })
                    .collect();
                Open::Table(alignments, Vec::new(), Vec::new())
            }
            Tag::TableHead => Open::Row(true, Vec::new()),
            Tag::TableRow => Open::Row(false, Vec::new()),
            Tag::TableCell => Open::Cell,
            Tag::Emphasis => Open::Emphasis,
            Tag::Strong => Open::Strong,
            Tag::Strikethrough => Open::Strikethrough,
            Tag::Link { link_type, dest_url, .. } => {
                let url = clean(&dest_url);
                // An email autolink's destination is the address alone.
                let to = if link_type == LinkType::Email { format!("mailto:{url}") } else { url.clone() };
                Open::Link(url, href(&to))
            }
            Tag::Image { dest_url, .. } => Open::Image(clean(&dest_url)),
            Tag::Superscript
            | Tag::Subscript
            | Tag::FootnoteDefinition(_)
            | Tag::DefinitionList
            | Tag::DefinitionListTitle
            | Tag::DefinitionListDefinition
            | Tag::MetadataBlock(_) => Open::Other,
        };
        let block = !matches!(
            open,
            Open::Emphasis
                | Open::Strong
                | Open::Strikethrough
                | Open::Link(..)
                | Open::Image(_)
                | Open::Cell
                | Open::Row(..)
                | Open::Other
        );
        if block {
            self.top().plain();
        }
        self.open.push(Frame::new(open));
    }

    fn end(&mut self, _: &TagEnd) {
        if self.open.len() > 1 {
            self.close();
        }
    }

    /// End the frame being built, adding what it makes to the one holding it.
    fn close(&mut self) {
        let mut frame = self.open.pop().expect("a frame");
        let content = std::mem::take(&mut frame.inlines);
        let inline = |inline: Inline, builder: &mut Self| builder.top().inlines.push(inline);
        match frame.open {
            Open::Root => unreachable!("the root stays open"),
            Open::Paragraph => self.block(Block::Paragraph { content }),
            Open::Heading(level) => self.block(Block::Heading { level, content }),
            Open::Quote => {
                frame.inlines = content;
                frame.plain();
                self.block(Block::Quote { blocks: frame.blocks });
            }
            Open::Code(language, text) => self.block(Block::Code { language, text }),
            Open::List(start, items) => self.block(Block::List { start, items }),
            Open::Item(task) => {
                frame.inlines = content;
                frame.plain();
                if let Open::List(_, items) = &mut self.top().open {
                    items.push(Item { task, blocks: frame.blocks });
                }
            }
            Open::Table(alignments, head, rows) => {
                self.block(Block::Table { alignments, head, rows });
            }
            Open::Row(true, cells) => {
                if let Open::Table(_, head, _) = &mut self.top().open {
                    *head = cells;
                }
            }
            Open::Row(false, cells) => {
                if let Open::Table(_, _, rows) = &mut self.top().open {
                    rows.push(cells);
                }
            }
            Open::Cell => {
                if let Open::Row(_, cells) = &mut self.top().open {
                    cells.push(content);
                }
            }
            Open::Emphasis => inline(Inline::Emphasis { content }, self),
            Open::Strong => inline(Inline::Strong { content }, self),
            Open::Strikethrough => inline(Inline::Strikethrough { content }, self),
            Open::Link(url, href) => inline(Inline::Link { url, href, content }, self),
            Open::Image(url) => inline(Inline::Image { src: src(&url), url, alt: content }, self),
            Open::Other => {
                let top = self.top();
                if !frame.blocks.is_empty() {
                    top.plain();
                    top.blocks.append(&mut frame.blocks);
                }
                top.inlines.extend(content);
            }
        }
    }
}

#[cfg(test)]
mod tests;
