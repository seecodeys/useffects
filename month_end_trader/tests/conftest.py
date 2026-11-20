import datetime as dt
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from met import calendar as cal  # noqa: E402
from met.dryrun import load_fixture, make_env  # noqa: E402


@pytest.fixture(scope="session")
def data():
    return load_fixture()


@pytest.fixture
def env(tmp_path, data):
    """Factory: env(start_datetime_et, **fake_broker_kwargs) -> (cfg, clock, broker, executor, alerts)."""
    spy, tlt = data

    def make(start=dt.datetime(2025, 9, 29, 8, 0), **kw):
        return make_env(start.replace(tzinfo=cal.ET), str(tmp_path), spy, tlt, **kw)
    return make


def run_until(ex, clock, until):
    """Drive the daemon tick loop until the fake clock reaches `until` (ET naive or aware)."""
    if until.tzinfo is None:
        until = until.replace(tzinfo=cal.ET)
    ex.run_forever(stop=lambda: clock.now() >= until)
