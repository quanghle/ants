"""Drives `ants chat` in a pseudo-terminal and reads its screen back, for the U scenarios of docs/e2e-scenarios.md.

Usage: python3 chat.py STEP...

  wait:TEXT[:SECONDS]   wait until the screen shows TEXT (default 30 s)
  gone:TEXT[:SECONDS]   wait until the screen no longer shows TEXT
  type:TEXT             type TEXT
  key:NAME              press enter, esc, tab, up, down, left, right, backspace or ctrl-c
  sleep:SECONDS         let the chat run
  snap                  print the screen

On a step that fails, prints the screen and exits 1. Ends the chat with /quit.
"""

import fcntl
import os
import pty
import select
import signal
import struct
import sys
import termios
import time

import pyte

COLS, ROWS = 160, 45
KEYS = {
    "enter": b"\r",
    "esc": b"\x1b",
    "tab": b"\t",
    "up": b"\x1b[A",
    "down": b"\x1b[B",
    "right": b"\x1b[C",
    "left": b"\x1b[D",
    "backspace": b"\x7f",
    "ctrl-c": b"\x03",
}


class Exited(Exception):
    """ants chat closed its terminal."""


class Chat:
    def __init__(self):
        self.screen = pyte.Screen(COLS, ROWS)
        self.stream = pyte.ByteStream(self.screen)
        pid, fd = pty.fork()
        if pid == 0:
            env = dict(os.environ, TERM="xterm-256color", COLORFGBG="15;0")
            env.pop("NO_COLOR", None)
            os.execvpe("ants", ["ants", "chat"], env)
        self.pid, self.fd = pid, fd
        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", ROWS, COLS, 0, 0))

    def pump(self, seconds):
        end = time.monotonic() + seconds
        while True:
            left = end - time.monotonic()
            if left <= 0:
                return
            ready, _, _ = select.select([self.fd], [], [], left)
            if not ready:
                return
            try:
                data = os.read(self.fd, 65536)
            except OSError:
                raise Exited()
            # The chat asks the terminal for its background colour and, on some platforms, waits for the answer.
            if b"\x1b]11;?" in data:
                os.write(self.fd, b"\x1b]11;rgb:1e1e/1e1e/1e1e\x07")
            # pyte keeps no alternate screen: start afresh when the chat enters it.
            if b"\x1b[?1049h" in data:
                self.screen.reset()
            self.stream.feed(data)

    def text(self):
        return "\n".join(self.screen.display)

    def fail(self, why):
        print(self.text())
        print(f"FAILED: {why}")
        return 1

    def send(self, data):
        os.write(self.fd, data)
        self.pump(0.5)

    def wait(self, text, seconds, present=True):
        end = time.monotonic() + seconds
        while (text in self.text()) != present:
            if time.monotonic() > end:
                raise SystemExit(self.fail(f"{'no' if present else 'still'} {text!r} within {seconds} s"))
            self.pump(0.5)

    def close(self):
        try:
            self.send(b"/quit\r")
            self.pump(2)
        except Exited:
            pass
        try:
            os.kill(self.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def main(steps):
    chat = Chat()
    try:
        run(chat, steps)
    except Exited:
        raise SystemExit(chat.fail("ants chat exited"))
    print(chat.text())
    chat.close()


def run(chat, steps):
    chat.pump(3)
    for step in steps:
        verb, _, arg = step.partition(":")
        print(f"-- {step}", flush=True)
        if verb in ("wait", "gone"):
            text, _, seconds = arg.rpartition(":")
            if not text or not seconds.isdigit():
                text, seconds = arg, "30"
            chat.wait(text, int(seconds), present=verb == "wait")
        elif verb == "type":
            chat.send(arg.encode())
        elif verb == "key":
            chat.send(KEYS[arg])
        elif verb == "sleep":
            chat.pump(float(arg))
        elif verb == "snap":
            print(chat.text())
        else:
            raise SystemExit(chat.fail(f"unknown step {step!r}"))


if __name__ == "__main__":
    main(sys.argv[1:])
