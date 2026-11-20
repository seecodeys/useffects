"""Persistent per-trade state (one JSON file per trade date) so a restart never double-enters and always
knows whether it owns an open position. Writes are atomic and fsync'd, with a .bak copy of the previous
version; a single-process lock prevents two daemons from trading the same state directory."""
from __future__ import annotations

import fcntl
import json
import os
import tempfile
from pathlib import Path

PHASES = ("PLANNED", "SKIPPED", "ENTRY_SUBMITTED", "ENTERED", "EXIT_SUBMITTED", "FLAT", "ERROR")
ACTIVE = ("ENTRY_SUBMITTED", "ENTERED", "EXIT_SUBMITTED", "ERROR")     # the bot may own a position


class StateCorrupt(RuntimeError):
    pass


class TradeState:
    def __init__(self, state_dir: str, trade_date: str):
        self.path = Path(state_dir) / f"trade_{trade_date}.json"
        self.bak = self.path.with_suffix(".json.bak")
        self.data = self._load() or {"trade_date": trade_date, "phase": None, "events": []}

    def _load(self):
        for p in (self.path, self.bak):
            if p.exists():
                try:
                    return json.loads(p.read_text())
                except Exception:
                    continue
        if self.path.exists():
            raise StateCorrupt(f"{self.path} and its .bak are unreadable")
        return None

    @property
    def phase(self):
        return self.data.get("phase")

    @property
    def trade_date(self) -> str:
        return self.data["trade_date"]

    def get(self, k, default=None):
        return self.data.get(k, default)

    def update(self, ts: str, phase: str | None = None, note: str = "", **fields):
        if phase:
            assert phase in PHASES, phase
            self.data["phase"] = phase
        self.data.update(fields)
        self.data["events"].append({"ts": ts, "phase": self.data["phase"], "note": note})
        if len(self.data["events"]) > 300:                       # keep the file small; the log has everything
            self.data["events"] = self.data["events"][:50] + self.data["events"][-200:]
        self._write()

    def _write(self):
        d = self.path.parent
        d.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            try:
                json.loads(self.path.read_text())
                self.bak.write_bytes(self.path.read_bytes())     # keep the last good version
            except Exception:
                pass
        fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp_", suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump(self.data, f, indent=2, default=str)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, self.path)
        dfd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)

    @property
    def owned(self) -> int:
        """Signed quantity the bot owns: its entry fills minus its exit fills (0 once FLAT)."""
        if self.phase in ("ENTERED", "EXIT_SUBMITTED"):
            d = int(self.data.get("direction", 0))
            return d * max(int(self.data.get("entry_filled", 0)) - int(self.data.get("exit_filled", 0) or 0), 0)
        return 0

    @property
    def open_quantity(self) -> int:
        """Signed quantity the bot believes it holds (informational; exits are sized from the broker)."""
        if self.phase in ("ENTERED", "EXIT_SUBMITTED"):
            d = int(self.data.get("direction", 0))
            return d * (int(self.data.get("entry_filled", 0)) - int(self.data.get("exit_filled", 0)))
        return 0


def all_trades(state_dir: str) -> list[dict]:
    out = []
    for p in sorted(Path(state_dir).glob("trade_*.json")):
        try:
            out.append(json.loads(p.read_text()))
        except Exception:
            pass
    return out


def unfinished(state_dir: str) -> list[str]:
    """Trade dates whose state may still involve a position: ACTIVE phases, or FLAT not yet reconciled.
    Corrupt files are reported too (as dates) so the caller can raise the alarm."""
    out = []
    for p in sorted(Path(state_dir).glob("trade_*.json")):
        date = p.stem.replace("trade_", "")
        try:
            t = TradeState(state_dir, date)
        except StateCorrupt:
            out.append(date); continue
        if (t.phase in ACTIVE or t.phase == "FLAT") and not t.get("reconciled"):
            out.append(date)
    return out


def kill_switch_active(state_dir: str) -> bool:
    return (Path(state_dir) / "KILL").exists() or os.environ.get("MET_KILL", "") == "1"


class DaemonLock:
    """Exclusive lock on <state_dir>/.lock for the daemon's lifetime."""

    def __init__(self, state_dir: str):
        Path(state_dir).mkdir(parents=True, exist_ok=True)
        self.path = Path(state_dir) / ".lock"
        self.fh = None

    def acquire(self):
        self.fh = open(self.path, "a+")
        try:
            fcntl.flock(self.fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.fh.close(); self.fh = None
            raise RuntimeError(f"another met daemon holds {self.path}")
        self.fh.seek(0); self.fh.truncate(); self.fh.write(str(os.getpid())); self.fh.flush()
        return self

    def release(self):
        if self.fh:
            fcntl.flock(self.fh, fcntl.LOCK_UN); self.fh.close(); self.fh = None
