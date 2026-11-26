//! Markdown in messages, as the views draw it: headings, bold, italic, strikethrough, inline code, fenced and indented
//! code blocks (behind a dim bar, neither highlighted nor reflowed, a line too wide going on below after a mark),
//! nested lists, block quotes, links with their URL, rules and simple tables, as styled lines no wider than asked.
//!
//! The text is parsed as every client parses it ([`ants_client::markdown`]): a line break in it stays one, and no
//! control character, as text or as character reference, reaches the terminal (tabs become spaces).

use ants_client::markdown::{self, Alignment, Block, Inline, Item};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use unicode_width::{UnicodeWidthChar as _, UnicodeWidthStr as _};

/// How many columns a tab in a code block takes.
const TAB: usize = 4;
/// Before each line of a code block.
const CODE_BAR: &str = "▎ ";
/// Before the rest of a line of a code block too wide for the view, on the lines below it.
const CODE_GOES_ON: &str = "▎↳";

/// `text`, markdown, as lines at most `width` columns wide: a code block's line too wide for it is cut at the width, the
/// rest going on below, and a table too wide for it has each row wrapped.
pub fn render(text: &str, width: usize) -> Vec<Line<'static>> {
    let mut renderer = Renderer::new(width.max(1));
    renderer.blocks(&markdown::parse(text).blocks);
    renderer.flush();
    renderer.lines
}

fn dim() -> Style {
    Style::new().add_modifier(Modifier::DIM)
}

/// A line of a code block, its tabs expanded.
fn expand_tabs(line: &str) -> String {
    let mut out = String::new();
    for c in line.chars() {
        if c == '\t' {
            let to = (out.width() / TAB + 1) * TAB;
            out.extend(std::iter::repeat_n(' ', to - out.width()));
        } else {
            out.push(c);
        }
    }
    out
}

/// What encloses the text being rendered.
enum Frame {
    Quote,
    List(Option<u64>),
    /// A list item: its marker shows on its first line, spaces as wide on the others.
    Item {
        marker: String,
        shown: bool,
    },
}

/// A link or image being rendered: where it points, and its text so far.
struct Link {
    url: String,
    text: String,
    image: bool,
}

#[derive(Default)]
struct Table {
    alignments: Vec<Alignment>,
    /// Each row, whether a header, and its cells.
    rows: Vec<(bool, Vec<Vec<Span<'static>>>)>,
}

struct Renderer {
    width: usize,
    lines: Vec<Line<'static>>,
    frames: Vec<Frame>,
    /// The lines of the paragraph, heading or item text being rendered, split where the text breaks them.
    inline: Vec<Vec<Span<'static>>>,
    styles: Vec<Style>,
    links: Vec<Link>,
    table: Option<Table>,
    /// The task marker of the item being rendered, due before its text.
    task: Option<&'static str>,
    /// Whether a blank line is due before the next block.
    gap: bool,
}

impl Renderer {
    fn new(width: usize) -> Self {
        Self {
            width,
            lines: Vec::new(),
            frames: Vec::new(),
            inline: Vec::new(),
            styles: Vec::new(),
            links: Vec::new(),
            table: None,
            task: None,
            gap: false,
        }
    }

    fn blocks(&mut self, blocks: &[Block]) {
        for block in blocks {
            self.block(block);
        }
    }

    fn block(&mut self, block: &Block) {
        match block {
            Block::Paragraph { content } => {
                self.open();
                self.text_of_block(content);
                self.close();
            }
            Block::Plain { content } => self.text_of_block(content),
            Block::Heading { level, content } => {
                self.open();
                let style = Style::new().add_modifier(Modifier::BOLD);
                self.styles.push(if *level == 1 { style.add_modifier(Modifier::UNDERLINED) } else { style });
                self.text_of_block(content);
                self.close();
                self.styles.pop();
            }
            Block::Quote { blocks } => {
                self.open();
                self.frames.push(Frame::Quote);
                self.blocks(blocks);
                self.close();
                self.frames.pop();
            }
            Block::Code { text, .. } => {
                self.open();
                let width = self.width.saturating_sub(self.indent() + CODE_BAR.width()).max(1);
                for line in text.lines() {
                    for (n, row) in cut(&expand_tabs(line), width).into_iter().enumerate() {
                        let bar = if n == 0 { CODE_BAR } else { CODE_GOES_ON };
                        self.emit(vec![Span::styled(bar, dim()), Span::raw(row)]);
                    }
                }
                self.gap = true;
            }
            Block::List { start, items } => {
                self.open();
                self.frames.push(Frame::List(*start));
                for item in items {
                    self.item(item);
                }
                self.flush();
                self.frames.pop();
                // A list nested in an item is part of the item, which takes no gap after it.
                self.gap = !matches!(self.frames.last(), Some(Frame::Item { .. }));
            }
            Block::Table { alignments, head, rows } => {
                self.open();
                self.table = Some(Table { alignments: alignments.clone(), rows: Vec::new() });
                for (head, cells) in std::iter::once((true, head)).chain(rows.iter().map(|row| (false, row))) {
                    if let Some(table) = &mut self.table {
                        table.rows.push((head, Vec::new()));
                    }
                    for cell in cells {
                        if let Some((_, cells)) = self.table.as_mut().and_then(|t| t.rows.last_mut()) {
                            cells.push(Vec::new());
                        }
                        self.inlines(cell);
                    }
                }
                let table = self.table.take().unwrap_or_default();
                self.table(table);
                self.gap = true;
            }
            Block::Rule => {
                self.open();
                let width = self.width.saturating_sub(self.indent()).max(1);
                self.emit(vec![Span::styled("─".repeat(width), dim())]);
                self.gap = true;
            }
        }
    }

    fn item(&mut self, item: &Item) {
        self.open();
        let depth = self.frames.iter().filter(|f| matches!(f, Frame::List(_))).count();
        let marker = match self.frames.iter_mut().rev().find(|f| matches!(f, Frame::List(_))) {
            Some(Frame::List(Some(n))) => {
                *n += 1;
                format!("{}. ", *n - 1)
            }
            _ => format!("{} ", ["•", "◦", "▪"][depth.saturating_sub(1) % 3]),
        };
        self.frames.push(Frame::Item { marker, shown: false });
        self.task = item.task.map(|done| if done { "[x] " } else { "[ ] " });
        self.blocks(&item.blocks);
        self.task = None;
        self.flush();
        self.frames.pop();
    }

    /// Add the text of a block, after the task marker of the item it begins, if any.
    fn text_of_block(&mut self, content: &[Inline]) {
        if let Some(task) = self.task.take() {
            self.text(task, self.style());
        }
        self.inlines(content);
    }

    fn inlines(&mut self, content: &[Inline]) {
        for inline in content {
            self.inline(inline);
        }
    }

    fn inline(&mut self, inline: &Inline) {
        match inline {
            Inline::Text { text } => self.text(text, self.style()),
            Inline::Code { text } => self.text(text, self.style().fg(Color::Yellow)),
            Inline::Break => self.inline.push(Vec::new()),
            Inline::Emphasis { content } => self.styled(Style::new().add_modifier(Modifier::ITALIC), content),
            Inline::Strong { content } => self.styled(Style::new().add_modifier(Modifier::BOLD), content),
            Inline::Strikethrough { content } => {
                self.styled(Style::new().add_modifier(Modifier::CROSSED_OUT), content);
            }
            Inline::Link { url, content, .. } => {
                self.styles.push(Style::new().fg(Color::Blue).add_modifier(Modifier::UNDERLINED));
                self.links.push(Link { url: url.clone(), text: String::new(), image: false });
                self.inlines(content);
                self.end_link();
            }
            Inline::Image { url, alt, .. } => {
                self.text("[image: ", self.style());
                self.styles.push(Style::new());
                self.links.push(Link { url: url.clone(), text: String::new(), image: true });
                self.inlines(alt);
                self.end_link();
            }
        }
    }

    /// Add `content` in `style`, over the style it is in.
    fn styled(&mut self, style: Style, content: &[Inline]) {
        self.styles.push(style);
        self.inlines(content);
        self.styles.pop();
    }

    /// End the link or image being rendered: its URL after its text, unless the text is the URL.
    fn end_link(&mut self) {
        self.styles.pop();
        let Some(link) = self.links.pop() else { return };
        let named = link.text != link.url && Some(link.text.as_str()) != link.url.strip_prefix("mailto:");
        let url = if named && !link.url.is_empty() { format!(" ({})", link.url) } else { String::new() };
        if link.image {
            self.text(&format!("]{url}"), self.style());
        } else if !url.is_empty() {
            self.text(&url, self.style().patch(dim()));
        }
    }

    /// The style of inline text here.
    fn style(&self) -> Style {
        self.styles.iter().fold(Style::new(), |style, s| style.patch(*s))
    }

    /// Add inline text: to the table cell being rendered, else to the current line.
    fn text(&mut self, text: &str, style: Style) {
        if text.is_empty() {
            return;
        }
        if let Some(link) = self.links.last_mut() {
            link.text.push_str(text);
        }
        if let Some(cell) = self.table.as_mut().and_then(|t| t.rows.last_mut()).and_then(|(_, cells)| cells.last_mut())
        {
            cell.push(Span::styled(text.to_owned(), style));
            return;
        }
        if self.inline.is_empty() {
            self.inline.push(Vec::new());
        }
        self.inline.last_mut().expect("a line").push(Span::styled(text.to_owned(), style));
    }

    /// Start a block: render the text before it, and a blank line if due.
    fn open(&mut self) {
        self.flush();
        if std::mem::take(&mut self.gap) && !self.lines.is_empty() {
            let mut prefix = String::new();
            for frame in &self.frames {
                match frame {
                    Frame::Quote => prefix.push_str("│ "),
                    Frame::Item { marker, .. } => prefix.push_str(&" ".repeat(marker.width())),
                    Frame::List(_) => {}
                }
            }
            self.lines.push(Line::styled(prefix.trim_end().to_owned(), dim()));
        }
    }

    /// End a block: render its text, and have a blank line before the next.
    fn close(&mut self) {
        self.flush();
        self.gap = true;
    }

    /// Render the inline text so far, wrapped.
    fn flush(&mut self) {
        let width = self.width.saturating_sub(self.indent()).max(1);
        while self.inline.last().is_some_and(Vec::is_empty) {
            self.inline.pop();
        }
        for line in std::mem::take(&mut self.inline) {
            for row in wrap(&line, width) {
                self.emit(row);
            }
        }
    }

    /// How wide the prefix of a line is here.
    fn indent(&self) -> usize {
        self.frames
            .iter()
            .map(|f| match f {
                Frame::Quote => 2,
                Frame::Item { marker, .. } => marker.width(),
                Frame::List(_) => 0,
            })
            .sum()
    }

    /// Add a line, after the quote bars and list markers of the blocks it is in.
    fn emit(&mut self, spans: Vec<Span<'static>>) {
        let mut line = Vec::new();
        for frame in &mut self.frames {
            match frame {
                Frame::Quote => line.push(Span::styled("│ ", dim())),
                Frame::Item { marker, shown } => {
                    if *shown {
                        line.push(Span::raw(" ".repeat(marker.width())));
                    } else {
                        line.push(Span::styled(marker.clone(), Style::new().fg(Color::Cyan)));
                        *shown = true;
                    }
                }
                Frame::List(_) => {}
            }
        }
        line.extend(spans);
        self.lines.push(Line::from(line));
    }

    /// Render a table: its columns aligned and the header bold, unless too wide, when each row is wrapped instead.
    fn table(&mut self, table: Table) {
        let columns = table.rows.iter().map(|(_, cells)| cells.len()).max().unwrap_or(0);
        let mut widths = vec![0; columns];
        for (_, cells) in &table.rows {
            for (width, cell) in widths.iter_mut().zip(cells) {
                *width = (*width).max(cell.iter().map(Span::width).sum());
            }
        }
        let available = self.width.saturating_sub(self.indent()).max(1);
        let fits = widths.iter().sum::<usize>() + 3 * columns.saturating_sub(1) <= available;
        let bar = || Span::styled(" │ ", dim());
        for (head, cells) in table.rows {
            let mut row = Vec::new();
            for (i, width) in widths.iter().enumerate() {
                if i > 0 {
                    row.push(bar());
                }
                let mut cell = cells.get(i).cloned().unwrap_or_default();
                if head {
                    cell = cell.into_iter().map(|s| s.patch_style(Modifier::BOLD)).collect();
                }
                if fits {
                    let pad = width - cell.iter().map(Span::width).sum::<usize>();
                    let left = match table.alignments.get(i) {
                        Some(Alignment::Right) => pad,
                        Some(Alignment::Center) => pad / 2,
                        _ => 0,
                    };
                    row.push(Span::raw(" ".repeat(left)));
                    row.extend(cell);
                    row.push(Span::raw(" ".repeat(pad - left)));
                } else {
                    row.extend(cell);
                }
            }
            if fits {
                self.emit(merge(row));
            } else {
                for wrapped in wrap(&row, available) {
                    self.emit(wrapped);
                }
            }
            if head && fits {
                let rule: Vec<String> = widths.iter().map(|w| "─".repeat(*w)).collect();
                self.emit(vec![Span::styled(rule.join("─┼─"), dim())]);
            }
        }
    }
}

/// `text` cut into pieces at most `width` columns wide (a character wider than that alone in its piece).
fn cut(text: &str, width: usize) -> Vec<String> {
    let mut pieces = vec![String::new()];
    let mut used = 0;
    for c in text.chars() {
        let w = c.width().unwrap_or(0);
        if used + w > width && used > 0 {
            pieces.push(String::new());
            used = 0;
        }
        pieces.last_mut().expect("a piece").push(c);
        used += w;
    }
    pieces
}

/// `text` split into runs of whitespace and of anything else.
fn words(text: &str) -> Vec<&str> {
    let mut words = Vec::new();
    let mut start = 0;
    let mut blank = None;
    for (i, c) in text.char_indices() {
        let is_blank = c.is_whitespace();
        if blank.is_some_and(|b| b != is_blank) {
            words.push(&text[start..i]);
            start = i;
        }
        blank = Some(is_blank);
    }
    if start < text.len() {
        words.push(&text[start..]);
    }
    words
}

/// `spans`, one line, wrapped at word boundaries into rows at most `width` columns wide, a word wider than that cut.
pub fn wrap(spans: &[Span<'static>], width: usize) -> Vec<Vec<Span<'static>>> {
    let mut rows: Vec<Vec<Span<'static>>> = vec![Vec::new()];
    let mut used = 0;
    for span in spans {
        for word in words(&span.content) {
            let w = word.width();
            if used + w <= width {
                rows.last_mut().expect("a row").push(Span::styled(word.to_owned(), span.style));
                used += w;
                continue;
            }
            if used > 0 {
                rows.push(Vec::new());
                used = 0;
            }
            if word.trim().is_empty() {
                continue;
            }
            for piece in cut(word, width) {
                if used > 0 {
                    rows.push(Vec::new());
                }
                used = piece.width();
                rows.last_mut().expect("a row").push(Span::styled(piece, span.style));
            }
        }
    }
    rows.into_iter().map(merge).collect()
}

/// `spans` without trailing whitespace, those of one style next to each other joined.
fn merge(spans: Vec<Span<'static>>) -> Vec<Span<'static>> {
    let mut merged: Vec<Span<'static>> = Vec::new();
    for span in spans {
        match merged.last_mut() {
            Some(last) if last.style == span.style => last.content.to_mut().push_str(&span.content),
            _ if span.content.is_empty() => {}
            _ => merged.push(span),
        }
    }
    while let Some(last) = merged.last_mut() {
        let trimmed = last.content.trim_end().len();
        if trimmed > 0 {
            last.content.to_mut().truncate(trimmed);
            break;
        }
        merged.pop();
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(lines: &[Line<'_>]) -> Vec<String> {
        lines.iter().map(ToString::to_string).collect()
    }

    /// The style of the first span of `lines` containing `text`.
    fn style_of(lines: &[Line<'_>], text: &str) -> Style {
        let span = lines.iter().flat_map(|l| &l.spans).find(|s| s.content.contains(text));
        span.unwrap_or_else(|| panic!("no span with {text:?}")).style
    }

    #[test]
    fn headings_and_emphasis_are_styled_and_lines_stay_broken() {
        let lines = render("# Deploy\n\nIt **failed** with *one* ~~two~~ `three`\nsee below", 80);
        assert_eq!(text(&lines), ["Deploy", "", "It failed with one two three", "see below"]);
        let bold = Style::new().add_modifier(Modifier::BOLD);
        assert_eq!(style_of(&lines, "Deploy"), bold.add_modifier(Modifier::UNDERLINED));
        assert_eq!(style_of(&lines, "failed"), bold);
        assert_eq!(style_of(&lines, "one"), Style::new().add_modifier(Modifier::ITALIC));
        assert_eq!(style_of(&lines, "two"), Style::new().add_modifier(Modifier::CROSSED_OUT));
        assert_eq!(style_of(&lines, "three"), Style::new().fg(Color::Yellow));
        let nested = render("## *a* **b**", 80);
        assert_eq!(style_of(&nested, "a"), bold.add_modifier(Modifier::ITALIC));
    }

    #[test]
    fn code_blocks_keep_their_lines_and_spaces_behind_a_dim_bar() {
        let lines = render("Run:\n\n```sh\nfor x in a b; do\n\techo  \"$x\"   # twice\ndone\n```\nafter", 24);
        assert_eq!(
            text(&lines),
            ["Run:", "", "▎ for x in a b; do", "▎     echo  \"$x\"   # twi", "▎↳ce", "▎ done", "", "after"]
        );
        assert_eq!(style_of(&lines, "done"), Style::new(), "normal brightness, no highlighting");
        assert_eq!(style_of(&lines, "▎"), dim());
        let indented = render("text\n\n    let x = 1;", 80);
        assert_eq!(text(&indented), ["text", "", "▎ let x = 1;"]);
    }

    #[test]
    fn a_code_line_wider_than_the_view_shows_whole_going_on_below() {
        let line: String = (0..200).map(|n| char::from(b'a' + u8::try_from(n % 26).unwrap())).collect();
        let lines = text(&render(&format!("```\n{line}\n```"), 80));
        assert!(lines.iter().all(|l| l.width() <= 80), "{lines:#?}");
        assert!(lines[0].starts_with("▎ ") && lines[1..].iter().all(|l| l.starts_with("▎↳")), "{lines:#?}");
        let shown: String = lines.iter().map(|l| l.chars().skip(2).collect::<String>()).collect();
        assert_eq!(shown, line);
    }

    #[test]
    fn nested_lists_indent_and_number_their_items() {
        let md = "- one\n- two\n  - inner wraps around here\n    1. first\n    2. second\n- three\n\n3. c\n4. d";
        let lines = render(md, 22);
        assert_eq!(
            text(&lines),
            [
                "• one",
                "• two",
                "  ◦ inner wraps around",
                "    here",
                "    1. first",
                "    2. second",
                "• three",
                "",
                "3. c",
                "4. d",
            ]
        );
        let tasks = render("- [x] done\n- [ ] not yet", 80);
        assert_eq!(text(&tasks), ["• [x] done", "• [ ] not yet"]);
    }

    #[test]
    fn quotes_links_and_rules_show_their_bars_urls_and_width() {
        let md = "> quoted *text*\n>\n> - item\n\nsee [the docs](https://x.example/d) or <https://y.example>\n\n---";
        let lines = render(md, 20);
        assert_eq!(
            text(&lines),
            [
                "│ quoted text",
                "│",
                "│ • item",
                "",
                "see the docs",
                "(https://x.example/d",
                ") or",
                "https://y.example",
                "",
                "────────────────────",
            ]
        );
        assert_eq!(style_of(&lines, "the docs"), Style::new().fg(Color::Blue).add_modifier(Modifier::UNDERLINED));
        assert_eq!(text(&render("![chart](c.png) <a@b.example>", 80)), ["[image: chart] (c.png) a@b.example"]);
    }

    #[test]
    fn tables_align_their_columns_or_wrap_when_too_wide() {
        let md = "| ant | turns | state |\n|---|---:|:---:|\n| dev-a | 5 | ok |\n| scout | 12 | stuck |";
        let lines = render(md, 80);
        assert_eq!(
            text(&lines),
            ["ant   │ turns │ state", "──────┼───────┼──────", "dev-a │     5 │  ok", "scout │    12 │ stuck"]
        );
        assert_eq!(style_of(&lines, "turns"), Style::new().add_modifier(Modifier::BOLD));
        assert_eq!(text(&render(md, 12))[..2], ["ant │ turns", "│ state"]);
    }

    #[test]
    fn long_words_are_cut_and_wide_characters_counted() {
        assert_eq!(text(&render("abcdefghij klm", 4)), ["abcd", "efgh", "ij", "klm"]);
        assert_eq!(text(&render("日本語のテキスト", 6)), ["日本語", "のテキ", "スト"]);
    }

    #[test]
    fn no_control_character_or_escape_sequence_gets_through() {
        let md = "a \u{1b}[31mred\u{7} &#27;[2J &#x9b;\n\n```\n\u{1b}]52;c;aGk=\u{7}\n```\n\n[x](https://e.example/&#27;[0m)";
        let lines = render(md, 80);
        for span in lines.iter().flat_map(|l| &l.spans) {
            assert!(!span.content.chars().any(char::is_control), "{:?}", span.content);
        }
        assert_eq!(text(&lines), ["a [31mred [2J", "", "▎ ]52;c;aGk=", "", "x (https://e.example/[0m)"]);
    }
}
