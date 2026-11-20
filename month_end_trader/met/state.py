"""Persistent per-trade state (one JSON file per trade date) so a restart never double-enters and always
knows whether it owns an open position. Writes are atomic."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

PHASES = ("PLANNED", "SKIPPED", "ENTRY_SUBMITTED", "ENTERED", "EXIT_SUBMITTED", "FLAT", "ERROR")


class TradeState:
    def __init__(self, state_dir: str, trade_date: str):
        self.path = Path(state_dir) / f"trade_{trade_date}.json"
        self.data = json.loads(self.path.read_text()) if self.path.exists() else {"trade_date": trade_date,
                                                                                  "phase": None, "events": []}

    @property
    def phase(self):
        return self.data.get("phase")

    def get(self, k, default=None):
        return self.data.get(k, default)

    def update(self, ts: str, phase: str | None = None, note: str = "", **fields):
        if phase:
            assert phase in PHASES, phase
            self.data["phase"] = phase
        self.data.update(fields)
        self.data["events"].append({"ts": ts, "phase": self.data["phase"], "note": note})
        self._write()

    def _write(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".tmp_", suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump(self.data, f, indent=2, default=str)
        os.replace(tmp, self.path)

    @property
    def open_quantity(self) -> int:
        """Signed quantity the bot believes it holds."""
        if self.phase in ("ENTERED", "EXIT_SUBMITTED"):
            return int(self.data.get("entry_filled", 0)) * int(self.data.get("direction", 0)) - \
                int(self.data.get("exit_filled", 0)) * int(self.data.get("direction", 0))
        return 0


def all_trades(state_dir: str) -> list[dict]:
    out = []
    for p in sorted(Path(state_dir).glob("trade_*.json")):
        try:
            out.append(json.loads(p.read_text()))
        except Exception:
            pass
    return out


def kill_switch_active(state_dir: str) -> bool:
    return (Path(state_dir) / "KILL").exists() or os.environ.get("MET_KILL", "") == "1"
