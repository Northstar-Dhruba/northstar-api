"""Tests for the single-writer operations lock and its CLI integration.

The lock is an OS file lock keyed to the database's resolved path. These tests
run the real platform implementation (msvcrt on Windows, flock on Linux) in
this process and across a real child process, including a child that is killed
while holding the lock. The other platform's backend is covered by a thin
contract test of its call shape and error translation only; it is not a
substitute for running on that platform.

The CLI tests prove that every mutating command refuses without changing the
database while another writer holds the lock, that ``operations daily`` treats
that as a safe skip with exit 0, and that normal operation resumes once the
lock is released.
"""

from __future__ import annotations

import ast
import errno
import io
import os
import signal
import sqlite3
import subprocess
import sys
from contextlib import closing
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from northstar_application.ports import (
    FuturesForwardResearchRecordQuery,
    FuturesHistoricalMarketDataQuery,
    FuturesPaperFillQuery,
    FuturesPaperOrderQuery,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesOHLCVBar, FuturesProductReference
from northstar_core.paper_trading import PaperPortfolioIdentity
from northstar_infrastructure.market_data import (
    NSEFuturesTradingSessionResolver,
    SQLiteFuturesHistoricalMarketDataStore,
)

import northstar_api.operations_lock as lock_module
from northstar_api.cli import ExitCode, main
from northstar_api.operations_lock import (
    DatabaseOperationsLock,
    OperationsAlreadyActiveError,
    operations_lock_path,
)
from northstar_api.runtime import build_database_runtime, build_upstox_market_sync_runtime

# ---------------------------------------------------------------------------
# Lock identity and lifecycle
# ---------------------------------------------------------------------------


def test_the_lock_path_is_the_resolved_database_path_plus_a_suffix(tmp_path: Path) -> None:
    database = tmp_path / "northstar.sqlite3"

    assert operations_lock_path(database) == database.resolve().with_name(
        "northstar.sqlite3.operations.lock"
    )
    assert DatabaseOperationsLock(database).path == operations_lock_path(database)


def test_spellings_of_one_database_share_one_lock(tmp_path: Path) -> None:
    (tmp_path / "sub").mkdir()
    database = tmp_path / "northstar.sqlite3"
    respelled = tmp_path / "sub" / ".." / "northstar.sqlite3"

    assert operations_lock_path(respelled) == operations_lock_path(database)
    with DatabaseOperationsLock(database):
        with pytest.raises(OperationsAlreadyActiveError):
            DatabaseOperationsLock(respelled).__enter__()


def test_the_first_acquisition_succeeds_and_creates_an_empty_lock_file(tmp_path: Path) -> None:
    lock = DatabaseOperationsLock(tmp_path / "a.sqlite3")

    with lock as held:
        assert held is lock
        assert lock.path.is_file()

    assert lock.path.read_bytes() == b""  # no pid, no credential, nothing


def test_a_second_acquisition_for_the_same_database_is_refused(tmp_path: Path) -> None:
    database = tmp_path / "a.sqlite3"

    with DatabaseOperationsLock(database):
        with pytest.raises(OperationsAlreadyActiveError, match="nothing was changed") as refused:
            with DatabaseOperationsLock(database):
                pytest.fail("a second writer must never enter")

    assert refused.value.database == database


def test_the_lock_can_be_acquired_again_after_release(tmp_path: Path) -> None:
    database = tmp_path / "a.sqlite3"

    with DatabaseOperationsLock(database):
        pass
    with DatabaseOperationsLock(database):
        pass


def test_two_databases_lock_independently(tmp_path: Path) -> None:
    with DatabaseOperationsLock(tmp_path / "a.sqlite3"):
        with DatabaseOperationsLock(tmp_path / "b.sqlite3"):
            pass


def test_an_exception_inside_the_block_releases_the_lock(tmp_path: Path) -> None:
    database = tmp_path / "a.sqlite3"

    with pytest.raises(ValueError, match="boom"):
        with DatabaseOperationsLock(database):
            raise ValueError("boom")

    with DatabaseOperationsLock(database):
        pass


def test_a_stale_lock_file_does_not_block_acquisition(tmp_path: Path) -> None:
    database = tmp_path / "a.sqlite3"
    operations_lock_path(database).write_bytes(b"left behind by a crashed process")

    with DatabaseOperationsLock(database):
        pass


def test_the_lock_is_not_reentrant(tmp_path: Path) -> None:
    database = tmp_path / "a.sqlite3"
    lock = DatabaseOperationsLock(database)

    with lock:
        with pytest.raises(RuntimeError, match="not re-entrant"):
            lock.__enter__()
        with pytest.raises(OperationsAlreadyActiveError):
            DatabaseOperationsLock(database).__enter__()


def test_a_missing_directory_is_a_configuration_error_and_creates_nothing(
    tmp_path: Path,
) -> None:
    database = tmp_path / "missing" / "northstar.sqlite3"

    with pytest.raises(Exception, match="Database directory does not exist"):
        DatabaseOperationsLock(database).__enter__()

    assert not (tmp_path / "missing").exists()


# The child reports the pid of the interpreter that actually holds the lock: on
# Windows a virtualenv's python.exe is a launcher that runs the real interpreter
# as its own child, so killing the launcher would not kill the lock holder.
_HOLDER = """
import os, sys
from pathlib import Path
from northstar_api.operations_lock import DatabaseOperationsLock

with DatabaseOperationsLock(Path(sys.argv[1])):
    print("held", os.getpid(), flush=True)
    sys.stdin.read()
"""


def _holder(database: Path) -> tuple[subprocess.Popen, int]:
    process = subprocess.Popen(
        [sys.executable, "-c", _HOLDER, str(database)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    word, pid = process.stdout.readline().split()
    assert word == "held"
    return process, int(pid)


def _finish(process: subprocess.Popen) -> None:
    process.wait()
    process.stdin.close()
    process.stdout.close()


def test_another_process_holding_the_lock_blocks_this_one(tmp_path: Path) -> None:
    database = tmp_path / "a.sqlite3"
    holder, _ = _holder(database)
    try:
        with pytest.raises(OperationsAlreadyActiveError):
            DatabaseOperationsLock(database).__enter__()
    finally:
        holder.stdin.close()  # the holder leaves its with-block and exits cleanly
        _finish(holder)

    with DatabaseOperationsLock(database):
        pass


def test_a_killed_holder_releases_the_lock(tmp_path: Path) -> None:
    database = tmp_path / "a.sqlite3"
    holder, pid = _holder(database)

    # An abrupt kill of the real holder: no __exit__, no unlock, no cleanup runs.
    os.kill(pid, signal.SIGTERM if os.name == "nt" else signal.SIGKILL)
    _finish(holder)

    assert operations_lock_path(database).exists()  # the file stays; ownership is gone
    with DatabaseOperationsLock(database):
        pass


# ---------------------------------------------------------------------------
# Platform backends
# ---------------------------------------------------------------------------


@pytest.mark.skipif(os.name != "nt", reason="the msvcrt backend is native to Windows")
def test_windows_uses_the_msvcrt_backend() -> None:
    assert isinstance(lock_module._platform_backend(), lock_module._WindowsBackend)


@pytest.mark.skipif(os.name == "nt", reason="the flock backend is native to POSIX")
def test_posix_uses_the_flock_backend() -> None:
    assert isinstance(lock_module._platform_backend(), lock_module._PosixBackend)


class _FakeFcntl:
    LOCK_EX, LOCK_NB, LOCK_UN = 2, 4, 8

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple[int, int]] = []

    def flock(self, descriptor: int, operation: int) -> None:
        self.calls.append((descriptor, operation))
        if self.error is not None and operation != self.LOCK_UN:
            raise self.error


def test_the_posix_backend_requests_an_exclusive_non_blocking_flock() -> None:
    fake = _FakeFcntl()
    backend = lock_module._PosixBackend(fake)

    assert backend.try_lock(7) is True
    backend.unlock(7)

    assert fake.calls == [(7, fake.LOCK_EX | fake.LOCK_NB), (7, fake.LOCK_UN)]


def test_the_posix_backend_reports_a_held_lock_and_propagates_other_errors() -> None:
    assert lock_module._PosixBackend(_FakeFcntl(BlockingIOError())).try_lock(7) is False
    with pytest.raises(PermissionError):
        lock_module._PosixBackend(_FakeFcntl(PermissionError("denied"))).try_lock(7)


def test_the_windows_backend_maps_only_contention_to_held(tmp_path: Path) -> None:
    descriptor = os.open(tmp_path / "f", os.O_RDWR | os.O_CREAT)
    try:
        for code, held in ((errno.EACCES, True), (errno.EDEADLK, True)):
            fake = SimpleNamespace(LK_NBLCK=2, LK_UNLCK=0, locking=None)

            def contended(fd, mode, size, code=code):
                raise OSError(code, "locked")

            fake.locking = contended
            assert lock_module._WindowsBackend(fake).try_lock(descriptor) is (not held)

        def broken(fd, mode, size):
            raise OSError(errno.EBADF, "bad descriptor")

        broken_fake = SimpleNamespace(LK_NBLCK=2, LK_UNLCK=0, locking=broken)
        with pytest.raises(OSError):
            lock_module._WindowsBackend(broken_fake).try_lock(descriptor)
    finally:
        os.close(descriptor)


def test_the_lock_module_reads_no_clock_and_shells_out_to_nothing() -> None:
    tree = ast.parse(Path(lock_module.__file__).read_text(encoding="utf-8"))
    imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)} | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }

    assert not {"time", "datetime", "subprocess", "threading", "socket"} & {
        str(name).split(".")[0] for name in imported
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            assert node.attr not in {"now", "utcnow", "sleep", "getpid", "environ"}


# ---------------------------------------------------------------------------
# CLI integration: refusal without mutation, then recovery
# ---------------------------------------------------------------------------

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_CONTRACT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_SESSIONS = NSEFuturesTradingSessionResolver().sessions_in_range(
    _NIFTY, date(2026, 7, 29), date(2026, 9, 30)
)
_PORTFOLIO = "nifty-lock-ops"
_STRATEGY = "directional-mvp-v1-nifty-lock-ops"
_ARGS = ["--product", "NIFTY", "--exchange", "NSE", "--expiration", "2026-10-27"]


def _cli(argv: list[str], **kwargs):
    out, err = io.StringIO(), io.StringIO()
    env = kwargs.pop("env", {})
    code = main(argv, env=env, stdout=out, stderr=err, **kwargs)
    return code, out.getvalue(), err.getvalue()


def _seed(database: Path, bars: int = 22) -> None:
    """Store bars directly: data entry is not what these tests exercise."""
    build_database_runtime(database)
    closes = [Decimal(25000)] * 21 + [Decimal(25100)]
    SQLiteFuturesHistoricalMarketDataStore(database).store(
        tuple(
            FuturesOHLCVBar(
                contract=_CONTRACT,
                point_in_time=_SESSIONS[index].closes_at,
                timeframe=Timeframe("1d"),
                open=QuoteValue(closes[index] - 1),
                high=QuoteValue(closes[index] + 5),
                low=QuoteValue(closes[index] - 5),
                close=QuoteValue(closes[index]),
                volume=Quantity(Decimal(1000)),
            )
            for index in range(bars)
        )
    )


def _paper(database: Path, bar: int):
    return _cli(
        ["paper", "run", "--database", str(database), *_ARGS, "--strategy", _STRATEGY,
         "--portfolio", _PORTFOLIO, "--target", "1", "--as-of", _SESSIONS[bar - 1].closes_at.value]
    )  # fmt: skip


def _economics(database: Path):
    return _cli(
        ["economics", "set", "--database", str(database), *_ARGS,
         "--point-value", "65", "--currency", "INR"]
    )  # fmt: skip


def _never_fetch(url, headers, timeout):
    raise AssertionError("no provider request may be made while the lock is held")


def _sync(database: Path, bar: int):
    day = _SESSIONS[bar - 1].trading_date.isoformat()
    return _cli(
        ["market-data", "sync", "--database", str(database), *_ARGS,
         "--start", day, "--end", day, "--provider", "upstox"],
        env={"UPSTOX_ANALYTICS_TOKEN": "placeholder-not-a-credential"},
        upstox_market_sync_runtime=lambda path, token: build_upstox_market_sync_runtime(
            path, token, fetch=_never_fetch
        ),
    )  # fmt: skip


def _facts(database: Path) -> tuple:
    rt = build_database_runtime(database)
    portfolio = PaperPortfolioIdentity(_PORTFOLIO)
    return (
        rt.market_repository.get_bars(FuturesHistoricalMarketDataQuery(_CONTRACT, Timeframe("1d"))),
        rt.forward_repository.get_records(
            FuturesForwardResearchRecordQuery(_CONTRACT, Timeframe("1d"))
        ),
        rt.order_repository.get_orders(FuturesPaperOrderQuery(portfolio)),
        rt.fill_repository.get_fills(FuturesPaperFillQuery(portfolio)),
        rt.economics_repository.get_economics(_CONTRACT),
    )


_ACTIVE = "Another Northstar operations writer is active for database"


def test_every_mutating_command_refuses_without_change_while_the_lock_is_held(
    tmp_path: Path,
) -> None:
    database = tmp_path / "ops.sqlite3"
    _seed(database)
    before = _facts(database)

    with DatabaseOperationsLock(database):
        for code, out, err in (_paper(database, 21), _economics(database), _sync(database, 22)):
            assert code == ExitCode.STATE
            assert f"STATE ERROR: {_ACTIVE}" in err
            assert "nothing was changed" in err
            assert out == ""
        assert _facts(database) == before

    # Released: the same commands now operate normally.
    assert _economics(database)[0] == ExitCode.SUCCESS
    code, out, _ = _paper(database, 21)
    assert code == ExitCode.SUCCESS
    assert "PAPER SESSION: COMPLETED" in out
    after = _facts(database)
    assert len(after[1]) == 1 and after[4] is not None


def test_the_concurrency_boundary_for_one_database(tmp_path: Path) -> None:
    """A writer holding DB A blocks a second writer of DB A, never one of DB B."""
    database_a, database_b = tmp_path / "a.sqlite3", tmp_path / "b.sqlite3"
    for database in (database_a, database_b):
        _seed(database)
        assert _economics(database)[0] == ExitCode.SUCCESS
        assert _paper(database, 21)[0] == ExitCode.SUCCESS
    before_a = _facts(database_a)

    with DatabaseOperationsLock(database_a):
        code, _, err = _paper(database_a, 22)
        assert code == ExitCode.STATE and _ACTIVE in err
        assert _facts(database_a) == before_a  # no decision, order, fill or data written

        code, out, _ = _paper(database_b, 22)
        assert code == ExitCode.SUCCESS  # another database is unaffected
        assert "Action: BUY" in out

    code, out, _ = _paper(database_a, 22)
    assert code == ExitCode.SUCCESS
    assert "Action: BUY" in out
    assert len(_facts(database_a)[2]) == 1  # the BUY order now exists


def test_paper_status_reads_while_a_writer_holds_the_lock(tmp_path: Path) -> None:
    database = tmp_path / "ops.sqlite3"
    _seed(database)
    _economics(database)

    with DatabaseOperationsLock(database):
        code, out, _ = _cli(
            ["paper", "status", "--database", str(database), "--strategy", _STRATEGY,
             "--portfolio", _PORTFOLIO, "--as-of", _SESSIONS[20].closes_at.value]
        )  # fmt: skip

    assert code == ExitCode.SUCCESS
    assert "PAPER STATUS" in out


def test_a_manual_write_into_a_missing_directory_is_still_configuration(tmp_path: Path) -> None:
    code, _, err = _paper(tmp_path / "missing" / "ops.sqlite3", 21)

    assert code == ExitCode.CONFIGURATION
    assert "Database directory does not exist" in err
    assert not (tmp_path / "missing").exists()


# ---------------------------------------------------------------------------
# operations daily: a held lock is a safe skip
# ---------------------------------------------------------------------------

_SECRET = "db-LOCK-SECRET-NEVER-PRINTED-0123456789"


def _fixed_clock() -> datetime:
    return datetime(2026, 9, 20, tzinfo=UTC)


def _daily_env(database: Path) -> dict[str, str]:
    return {
        "NORTHSTAR_DATABASE": str(database),
        "NORTHSTAR_FUTURES_PRODUCT": "ES",
        "NORTHSTAR_FUTURES_EXCHANGE": "CME",
        "NORTHSTAR_FUTURES_EXPIRATION": "2026-12-18",
        "NORTHSTAR_STRATEGY": "alpha",
        "NORTHSTAR_PORTFOLIO": "futures-paper-alpha",
        "NORTHSTAR_TARGET": "1",
        "DATABENTO_API_KEY": _SECRET,
    }


def test_operations_daily_skips_safely_while_another_runner_is_active(tmp_path: Path) -> None:
    database = tmp_path / "daily.sqlite3"
    built: list[object] = []
    reads: list[int] = []

    def clock():
        reads.append(1)
        raise AssertionError("the clock must not be read by a skipped run")

    with DatabaseOperationsLock(database):
        code, out, err = _cli(
            ["operations", "daily"],
            env=_daily_env(database),
            daily_sync_runtime=lambda *args: built.append(args),
            clock=clock,
        )

    assert code == ExitCode.SUCCESS
    assert out.splitlines()[0] == "DAILY OPERATION: SKIPPED"
    assert _ACTIVE in out
    assert "daily operation skipped: another operations runner is active" in err
    assert err.splitlines()[-1].endswith("INFO exit SUCCESS (0)")
    assert built == [] and reads == []
    assert not database.exists()  # nothing was opened or created
    assert _SECRET not in out + err


def test_operations_daily_runs_normally_when_the_lock_is_free(tmp_path: Path) -> None:
    """Unchanged behaviour: an empty history is still the documented DATA refusal."""
    database = tmp_path / "daily.sqlite3"

    code, out, err = _cli(
        ["operations", "daily"],
        env=_daily_env(database),
        clock=_fixed_clock,
    )

    assert code == ExitCode.DATA
    assert "No persisted Futures history" in err
    assert "SKIPPED" not in out
    # The lock was released: a writer can take it now.
    with DatabaseOperationsLock(database):
        pass


def test_the_lock_file_never_contains_a_secret(tmp_path: Path) -> None:
    database = tmp_path / "daily.sqlite3"
    _cli(["operations", "daily"], env=_daily_env(database), clock=_fixed_clock)

    lock_file = operations_lock_path(database)
    assert lock_file.exists()
    assert lock_file.read_bytes() == b""
    with closing(sqlite3.connect(database)) as connection:
        tables = {row[0] for row in connection.execute("select name from sqlite_master")}
    assert "operations_lock" not in tables  # no database checkpoint or lock table
