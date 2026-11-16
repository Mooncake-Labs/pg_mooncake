"""A persistent psql session driven over pipes.

Statements are fed to one long-lived psql process so session state (temp tables, SET, psql
variables, and the session's read-your-writes LSN that mirror scans wait for) carries over between
statements, as when pg_regress runs a file. After each unit a marker carrying psql's ERROR,
SQLSTATE and LAST_ERROR_MESSAGE variables is echoed, so outcomes are known without parsing stderr.
"""

import csv
import io
import os
import select
import shutil
import subprocess
import threading
import time

NULL_TOKEN = "§N§"  # how NULL is printed; does not occur in the regression data
_END = "@@MD_END@@"
_FIN = "@@MD_FIN@@"


class Result:
    __slots__ = ("rows", "error", "sqlstate", "message", "timed_out", "died", "elapsed", "raw")

    def __init__(self):
        self.rows = []
        self.error = False
        self.sqlstate = ""
        self.message = ""
        self.timed_out = False
        self.died = False
        self.elapsed = 0.0
        self.raw = ""

    @property
    def ok(self):
        return not (self.error or self.timed_out or self.died)

    def outcome(self):
        if self.died:
            return "died"
        if self.timed_out:
            return "timeout"
        return "error" if self.error else "ok"

    def __repr__(self):
        return f"Result({self.outcome()}, {len(self.rows)} rows, {self.sqlstate} {self.message[:80]!r})"


class PsqlSession:
    def __init__(self, psql, conninfo, timeout=120.0, env=None, trace=None, name=""):
        self.psql = psql
        self.conninfo = conninfo
        self.timeout = timeout
        self.env = env
        self.trace = trace  # optional file object: every unit sent, with outcome and duration
        self.name = name
        self.proc = None
        self.pid = None
        self._buf = b""
        self._stderr = []

    def start(self):
        cmd = [self.psql, "-X", "-q", "-v", "ON_ERROR_STOP=0", "-d", self.conninfo]
        stdbuf = shutil.which("stdbuf")
        if stdbuf:  # psql block-buffers stdout on a pipe; markers must arrive promptly
            cmd = [stdbuf, "-oL", "-eL"] + cmd
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, bufsize=0, env=self.env)
        self._buf = b""
        self._stderr = []
        threading.Thread(target=self._drain_stderr, args=(self.proc,), daemon=True).start()
        r = self.run("\\pset format csv\n\\pset tuples_only on\n"
                     f"\\pset null '{NULL_TOKEN}'\n\\pset pager off\n"
                     "SET client_min_messages = warning;\nSELECT pg_backend_pid();\n")
        if not r.ok or not r.rows:
            raise RuntimeError(f"psql session failed to start: {r!r} {self.stderr_tail()}")
        self.pid = int(r.rows[-1][0])
        return self

    def _drain_stderr(self, proc):
        for line in iter(proc.stderr.readline, b""):
            self._stderr.append(line.decode("utf-8", "replace"))
            if len(self._stderr) > 200:
                del self._stderr[:100]

    def stderr_tail(self, n=5):
        return "".join(self._stderr[-n:])

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def close(self):
        if self.proc is None:
            return
        try:
            self.proc.stdin.close()
        except OSError:
            pass
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.proc = None

    def kill(self):
        if self.proc is not None:
            self.proc.kill()
            self.proc.wait()
            self.proc = None

    def run(self, text, timeout=None, on_timeout=None):
        """Run `text` (complete lines: statements and/or meta-commands); return a Result.

        On timeout, on_timeout(backend_pid) is called (it should cancel the statement); if the
        session still does not answer it is killed and the Result is marked died."""
        res = Result()
        if not self.alive():
            res.died = True
            return res
        if not text.endswith("\n"):
            text += "\n"
        trailer = f"\\echo {_END} :ERROR :SQLSTATE\n\\echo :LAST_ERROR_MESSAGE\n\\echo {_FIN}\n"
        t0 = time.monotonic()
        try:
            self.proc.stdin.write((text + trailer).encode())
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError):
            res.died = True
            return res
        out = self._read_until_fin(t0 + (timeout or self.timeout))
        cancelled = False
        if out is None and on_timeout is not None and self.alive():
            on_timeout(self.pid)
            cancelled = True
            out = self._read_until_fin(time.monotonic() + 15)
        res.elapsed = time.monotonic() - t0
        if out is None or out is False:
            res.timed_out = out is None
            res.died = True  # the session and its state are gone; the caller restarts it
            self._trace(text, res)
            self.kill()
            return res
        res.timed_out = cancelled
        body, _, tail = out.rpartition(_END)
        status = tail.split("\n", 1)
        flags = status[0].split()
        res.error = len(flags) > 0 and flags[0] == "true"
        res.sqlstate = flags[1] if len(flags) > 1 else ""
        msg = status[1] if len(status) > 1 else ""
        res.message = msg.strip() if res.error else ""
        res.raw = body
        try:
            res.rows = list(csv.reader(io.StringIO(body)))
        except csv.Error:
            res.rows = [[line] for line in body.splitlines()]
        self._trace(text, res)
        return res

    def _trace(self, text, res):
        if self.trace:
            self.trace.write(f"-- [{self.name} pid={self.pid}] {res.outcome()} {res.elapsed:.2f}s "
                             f"rows={len(res.rows)} {res.sqlstate} {res.message[:200]!r}\n{text.rstrip()}\n")
            self.trace.flush()

    def _read_until_fin(self, deadline):
        """Decoded output up to the FIN marker; None on timeout, False on EOF."""
        fd = self.proc.stdout.fileno()
        marker = (_FIN + "\n").encode()
        while True:
            idx = self._buf.find(marker)
            if idx >= 0:
                out = self._buf[:idx]
                self._buf = self._buf[idx + len(marker):]
                return out.decode("utf-8", "replace")
            left = deadline - time.monotonic()
            if left <= 0:
                return None
            r, _, _ = select.select([fd], [], [], min(left, 1.0))
            if r:
                chunk = os.read(fd, 1 << 16)
                if not chunk:
                    return False
                self._buf += chunk
