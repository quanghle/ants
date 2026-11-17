//! Screens drawn as text, for the tests of `ants chat` and `ants dash`.

/// What `draw` shows on a `width` by `height` screen, as text: a line a row, each without its trailing blanks.
pub(crate) fn rendered(width: u16, height: u16, draw: impl FnOnce(&mut ratatui::Frame)) -> String {
    let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(width, height)).unwrap();
    terminal.draw(draw).unwrap();
    let cells: Vec<&str> = terminal.backend().buffer().content.iter().map(ratatui::buffer::Cell::symbol).collect();
    cells.chunks(usize::from(width)).map(|row| row.concat().trim_end().to_owned()).collect::<Vec<_>>().join("\n")
}
