//! What a turn left running in the background as told by the job objects of a harness's CLIs, on Windows. A CLI's job
//! holds everything it started, its MCP servers and an ACP adapter's own CLI among them, which no process group sets
//! apart there: a process in it counts once it was not running as the turn began, or the turn before left it running,
//! unless it is the console host of a process there, which runs as long as that does.

use crate::harness::background::Background;

/// A CLI's job object as listed at a turn's end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Listed {
    /// The CLI's process ID.
    pub cli: i32,
    /// The processes it held as the turn began, by pid and start; none for a job made since.
    pub began: Vec<(i32, u64)>,
    /// The processes it holds now.
    pub now: Vec<Background>,
}

/// The turns of a harness's CLIs, for what each left running in their job objects.
#[derive(Debug, Default)]
pub(super) struct Turns {
    /// What the turn before left running in the background.
    before: Vec<Background>,
}

impl Turns {
    /// As a turn ended, its CLIs' jobs listed: what the turn left running in the background, the processes of a job
    /// other than its CLI and console hosts, not running as the turn began or left running by the turn before.
    pub(super) fn ended(&mut self, jobs: Vec<Listed>) -> Vec<Background> {
        let before = &self.before;
        let left: Vec<Background> = jobs
            .into_iter()
            .flat_map(|Listed { cli, began, now }| {
                now.into_iter().filter(move |process| {
                    process.pid != cli
                        && !console_host(&process.command)
                        && (!began.contains(&(process.pid, process.start))
                            || before.iter().any(|left| left.pid == process.pid && left.start == process.start))
                })
            })
            .collect();
        self.before.clone_from(&left);
        left
    }
}

/// Whether `command`, the path of a process's executable, is Windows' console host, `conhost.exe`.
fn console_host(command: &str) -> bool {
    let name = command.rsplit(['\\', '/']).next().unwrap_or(command);
    name.eq_ignore_ascii_case("conhost.exe")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn process(pid: i32, start: u64) -> Background {
        Background { pid, start, command: format!("process {pid}") }
    }

    fn listed(cli: i32, began: &[&Background], now: &[&Background]) -> Listed {
        let began = began.iter().map(|process| (process.pid, process.start)).collect();
        Listed { cli, began, now: now.iter().copied().cloned().collect() }
    }

    #[test]
    fn a_turn_leaves_running_what_it_started_in_the_job_but_the_cli() {
        let mut turns = Turns::default();
        let (cli, mcp) = (process(10, 100), process(11, 101));
        assert_eq!(
            turns.ended(vec![listed(10, &[], &[&cli, &mcp])]),
            vec![mcp.clone()],
            "in a job made during the turn, all but the CLI count"
        );

        let check = process(12, 250);
        assert_eq!(
            turns.ended(vec![listed(10, &[&cli, &mcp], &[&cli, &mcp, &check])]),
            vec![mcp.clone(), check.clone()],
            "the turn before left the first one running"
        );

        let mut turns = Turns::default();
        assert_eq!(
            turns.ended(vec![listed(10, &[&cli, &mcp], &[&cli, &mcp, &check])]),
            vec![check.clone()],
            "what ran as the turn began is the CLI's own"
        );

        let (watcher, helper) = (process(13, 320), process(14, 280));
        assert_eq!(
            turns.ended(vec![listed(10, &[&cli, &mcp, &check, &helper], &[&cli, &mcp, &check, &helper, &watcher])]),
            vec![check, watcher],
            "what the turn before left running still counts, what started between turns does not"
        );

        let reused = process(12, 120);
        let jobs = vec![
            listed(10, &[&cli, &mcp, &reused], &[&cli, &mcp, &reused]),
            listed(20, &[], &[&process(20, 410)]),
            listed(30, &[], &[&process(31, 450)]),
        ];
        assert_eq!(
            turns.ended(jobs),
            vec![process(31, 450)],
            "a process ID reused is another process, and each job's own CLI is left out"
        );
        assert_eq!(turns.ended(Vec::new()), Vec::new());

        let host = Background { pid: 40, start: 500, command: r"C:\Windows\System32\CONHOST.EXE".into() };
        let job = Listed { cli: 41, began: Vec::new(), now: vec![host, process(42, 510)] };
        assert_eq!(turns.ended(vec![job]), vec![process(42, 510)], "a console host is not left running by the turn");
    }
}
