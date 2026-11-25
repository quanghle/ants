//! Ending what native CLIs left running: the processes tagged with a run that is gone, those of a run or a daemon still
//! running spared, and the descendants walked of a CLI, tagged or not.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use super::TAG;
use super::unix::{DAEMON, Daemon, GRACE, Proc, Run, Tag, descendants, end, os};

/// The environment variable making [`a_loop_left_running`] loop.
const LOOP: &str = "ANTS_TEST_LOOP";

/// Not a test: the loop [`looping`] starts, a run of this test executable, whose environment macOS lets other
/// processes read, as it does not that of its own programs.
#[test]
#[ignore = "run by the tests of what native CLIs left running"]
fn a_loop_left_running() {
    while std::env::var_os(LOOP).is_some() {
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// The command of a loop running until killed, which macOS lets the daemon read the environment of, with no input
/// nor output.
pub(crate) fn looping() -> Command {
    let test = format!("{}::a_loop_left_running", module_path!().split_once("::").unwrap().1);
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", &test, "--ignored", "--quiet", "--test-threads=1"]).env(LOOP, "1");
    command.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
    command
}

/// A child of this test, killed when dropped.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Running {
    /// A loop ([`looping`]) tagged with `tag`, else untagged, once listed.
    fn tagged(tag: Option<&str>) -> Self {
        let mut command = looping();
        match tag {
            Some(tag) => command.env(TAG, tag),
            None => command.env_remove(TAG),
        };
        let running = Self(command.spawn().unwrap());
        running.listed();
        running
    }

    fn pid(&self) -> i32 {
        i32::try_from(self.0.id()).unwrap()
    }

    /// It as listed, once it is.
    fn listed(&self) -> Proc {
        let pid = self.pid();
        ants_test_support::wait_until(Duration::from_secs(10), Duration::from_millis(20), || {
            os::process(pid).ok_or_else(|| format!("{pid} not listed"))
        })
    }

    /// Whether it was ended within the grace period of an end and a second.
    fn ended(&mut self) -> bool {
        let deadline = std::time::Instant::now() + GRACE + Duration::from_secs(1);
        while std::time::Instant::now() < deadline {
            if self.0.try_wait().unwrap().is_some() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }

    fn runs(&mut self) -> bool {
        self.0.try_wait().unwrap().is_none()
    }
}

#[test]
fn a_tag_names_the_daemon_and_the_run() {
    let tag = Tag { daemon: Daemon { pid: 812, start: 1_764_000_000_123_456 }, serial: 3 };
    assert_eq!(tag.value(), "812.1764000000123456.3");
    assert_eq!(Tag::parse(&tag.value()), Some(tag));
    assert_eq!(Tag::parse("812.17"), None);
    assert_eq!(Tag::parse("a.b.c"), None);
    assert_eq!(DAEMON.pid, i32::try_from(std::process::id()).unwrap());
    assert_ne!(DAEMON.start, 0, "the daemon's start time is read");
}

#[test]
fn what_is_tagged_with_a_run_gone_is_ended_and_what_runs_spared() {
    let run = Run::new();
    let mut live = Running::tagged(Some(&run.tag()));
    let released = Run::new();
    let mut gone = Running::tagged(Some(&released.tag()));
    drop(released);
    let dead_daemon = Tag { daemon: Daemon { pid: i32::MAX, start: 1 }, serial: 1 };
    let mut orphan = Running::tagged(Some(&dead_daemon.value()));
    let daemon = Running::tagged(None);
    let running = Tag { daemon: Daemon { pid: daemon.pid(), start: daemon.listed().start }, serial: 1 };
    let mut other = Running::tagged(Some(&running.value()));
    let mut untagged = Running::tagged(None);

    end("test", &[]);
    assert!(gone.ended(), "a run released");
    assert!(orphan.ended(), "a run of a daemon gone");
    assert!(live.runs(), "a run live is spared");
    assert!(other.runs(), "a run of another daemon is spared");
    assert!(untagged.runs(), "what is untagged is spared");

    drop(run);
    end("test", &[]);
    assert!(live.ended(), "the run released");
}

#[test]
fn the_descendants_walked_of_a_cli_are_ended_though_untagged() {
    let mut cli = Command::new("sh");
    cli.args(["-c", "sh -c 'while :; do sleep 1; done' & echo $!; wait"]).env_remove(TAG);
    let mut cli = Running(cli.stdin(Stdio::null()).stdout(Stdio::piped()).spawn().unwrap());
    let mut line = String::new();
    std::io::BufRead::read_line(&mut std::io::BufReader::new(cli.0.stdout.take().unwrap()), &mut line).unwrap();
    let child: i32 = line.trim().parse().unwrap();
    let walked = ants_test_support::wait_until(Duration::from_secs(10), Duration::from_millis(20), || {
        let walked = descendants(cli.pid());
        if walked.iter().any(|process| process.pid == child) { Ok(walked) } else { Err(format!("{walked:?}")) }
    });
    end("test", &walked);
    assert!(cli.ended(), "the loop ended, and with it the CLI's wait");
    assert!(os::process(child).is_none(), "the loop is gone");
}
