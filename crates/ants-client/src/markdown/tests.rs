//! Tests of the markdown tree: what each block and inline parses to, and that no HTML, control character or unsafe
//! link gets through.

use super::*;

fn text(text: &str) -> Inline {
    Inline::Text { text: text.to_owned() }
}

fn paragraph(content: Vec<Inline>) -> Block {
    Block::Paragraph { content }
}

/// Every string of `document` it shows or follows: its text, code, URLs, languages.
fn strings(document: &Document) -> Vec<String> {
    let json = serde_json::to_value(document).unwrap();
    let mut out = Vec::new();
    let mut stack = vec![&json];
    while let Some(value) = stack.pop() {
        match value {
            serde_json::Value::String(s) => out.push(s.clone()),
            serde_json::Value::Array(values) => stack.extend(values),
            serde_json::Value::Object(map) => stack.extend(map.values()),
            _ => {}
        }
    }
    out
}

#[test]
fn headings_emphasis_and_code_parse_and_line_breaks_stay() {
    let document = parse("# Deploy\n\nIt **failed** with *one* ~~two~~ `three`\nsee below");
    let content = vec![
        text("It "),
        Inline::Strong { content: vec![text("failed")] },
        text(" with "),
        Inline::Emphasis { content: vec![text("one")] },
        text(" "),
        Inline::Strikethrough { content: vec![text("two")] },
        text(" "),
        Inline::Code { text: "three".into() },
        Inline::Break,
        text("see below"),
    ];
    let heading = Block::Heading { level: 1, content: vec![text("Deploy")] };
    assert_eq!(document.blocks, [heading, paragraph(content)]);
}

#[test]
fn code_blocks_keep_their_lines_and_tabs_with_their_language() {
    let document = parse("```sh extra\nfor x in a b; do\n\techo \"$x\"\ndone\n```\n\n    let x = 1;");
    let fenced = Block::Code { language: Some("sh".into()), text: "for x in a b; do\n\techo \"$x\"\ndone\n".into() };
    let indented = Block::Code { language: None, text: "let x = 1;".into() };
    assert_eq!(document.blocks, [fenced, indented]);
    assert_eq!(document.code_blocks(), [(Some("sh"), "for x in a b; do\n\techo \"$x\"\ndone\n"), (None, "let x = 1;")]);
}

#[test]
fn lists_nest_tight_items_are_plain_and_tasks_are_marked() {
    let document = parse("- one\n  - inner\n- [x] done\n\n3. c\n4. d");
    let plain = |s: &str| Block::Plain { content: vec![text(s)] };
    let inner = Block::List { start: None, items: vec![Item { task: None, blocks: vec![plain("inner")] }] };
    let bullets = Block::List {
        start: None,
        items: vec![
            Item { task: None, blocks: vec![plain("one"), inner] },
            Item { task: Some(true), blocks: vec![plain("done")] },
        ],
    };
    let numbered = Block::List {
        start: Some(3),
        items: vec![Item { task: None, blocks: vec![plain("c")] }, Item { task: None, blocks: vec![plain("d")] }],
    };
    assert_eq!(document.blocks, [bullets, numbered]);
    let loose = parse("- a\n\n- b");
    let Block::List { items, .. } = &loose.blocks[0] else { panic!("{loose:?}") };
    assert_eq!(items[0].blocks, [paragraph(vec![text("a")])], "a loose list's items hold paragraphs");
}

#[test]
fn quotes_rules_and_tables_parse() {
    let document = parse("> quoted\n\n---\n\n| ant | turns |\n|---|---:|\n| dev-a | *5* |");
    let quote = Block::Quote { blocks: vec![paragraph(vec![text("quoted")])] };
    let table = Block::Table {
        alignments: vec![Alignment::None, Alignment::Right],
        head: vec![vec![text("ant")], vec![text("turns")]],
        rows: vec![vec![vec![text("dev-a")], vec![Inline::Emphasis { content: vec![text("5")] }]]],
    };
    assert_eq!(document.blocks, [quote, Block::Rule, table]);
}

#[test]
fn links_and_images_are_clickable_only_to_safe_schemes() {
    let document = parse(
        "[docs](https://x.example/d) <https://y.example> <a@b.example> [rel](c/d) [js](javascript:alert(1)) \
         [data](data:text/html,x) [up](HTTP://z.example) ![chart](https://c.example/c.png) ![local](c.png) \
         ![mail](mailto:a@b.example)",
    );
    let Block::Paragraph { content } = &document.blocks[0] else { panic!("{document:?}") };
    let links: Vec<(&str, Option<&str>)> = content
        .iter()
        .filter_map(|inline| match inline {
            Inline::Link { url, href, .. } => Some((url.as_str(), href.as_deref())),
            Inline::Image { url, src, .. } => Some((url.as_str(), src.as_deref())),
            _ => None,
        })
        .collect();
    assert_eq!(
        links,
        [
            ("https://x.example/d", Some("https://x.example/d")),
            ("https://y.example", Some("https://y.example")),
            ("a@b.example", Some("mailto:a@b.example")),
            ("c/d", None),
            ("javascript:alert(1)", None),
            ("data:text/html,x", None),
            ("HTTP://z.example", Some("HTTP://z.example")),
            ("https://c.example/c.png", Some("https://c.example/c.png")),
            ("c.png", None),
            ("mailto:a@b.example", None),
        ]
    );
    assert_eq!(
        document.links(),
        [
            ("docs".to_owned(), "https://x.example/d".to_owned()),
            ("https://y.example".to_owned(), "https://y.example".to_owned()),
            ("a@b.example".to_owned(), "mailto:a@b.example".to_owned()),
            ("up".to_owned(), "HTTP://z.example".to_owned()),
        ]
    );
    assert_eq!(href("https://x.example/a b"), None, "no whitespace");
    assert_eq!(href("https:"), None);
}

#[test]
fn html_comes_as_its_text() {
    let document = parse("<div onclick=\"x()\">\n<script>alert(1)</script>\n</div>\n\nsay <b>hi</b>");
    assert_eq!(
        document.blocks,
        [
            paragraph(vec![
                text("<div onclick=\"x()\">"),
                Inline::Break,
                text("<script>alert(1)</script>"),
                Inline::Break,
                text("</div>"),
                Inline::Break,
            ]),
            paragraph(vec![text("say <b>hi</b>")]),
        ]
    );
}

#[test]
fn no_control_character_escape_sequence_or_reordering_character_gets_through() {
    let md = "a \u{1b}[31mred\u{7} &#27;[2J &#x9b;\u{202e}x\n\n```\n\u{1b}]52;c;aGk=\u{7}\r\n```\n\n\
              [x](https://e.example/&#27;[0m) `\u{1b}c`\n\n| a\u{1b} |\n|---|\n| b\u{7} |";
    let document = parse(md);
    for s in strings(&document) {
        assert!(s.chars().all(|c| c == '\n' || ants_proto::shown(c)), "{s:?}");
    }
    let Block::Code { text: code, .. } = &document.blocks[1] else { panic!("{document:?}") };
    assert_eq!(code, "]52;c;aGk=\n");
    let Block::Paragraph { content } = &document.blocks[2] else { panic!("{document:?}") };
    assert!(matches!(&content[0], Inline::Link { url, href: Some(_), .. } if url == "https://e.example/[0m"));
}

#[test]
fn the_tree_serializes_and_reads_back() {
    let document = parse("# T\n\n- [ ] a [l](https://l.example)\n\n```rs\nx\n```\n\n|h|\n|:-:|\n|c|");
    let json = serde_json::to_value(&document).unwrap();
    assert_eq!(
        json["blocks"][0],
        serde_json::json!({"kind": "heading", "level": 1, "content": [{"kind": "text", "text": "T"}]})
    );
    assert_eq!(json["blocks"][1]["items"][0]["task"], false);
    assert_eq!(serde_json::from_value::<Document>(json).unwrap(), document);
}
