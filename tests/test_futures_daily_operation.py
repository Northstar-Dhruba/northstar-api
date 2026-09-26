"""Acceptance: ``northstar operations daily`` as the scheduled futures entry point.

Every run goes through ``northstar_api.cli.main`` with the production database
runtime, the real exchange calendar and the real Databento adapter -- including
its completed-session guard -- against real temporary SQLite files. Only the
Databento *client* is a double, and the one clock reading is injected. The
provider double, sessions and daily OHLCV are the 9.11 acceptance timeline:
S24 (2026-07-03, an early close) decides BUY, and S25 opens at 7650.
"""

from __future__ import annotations

import io
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
from northstar_application.application_services import (
    AcquireFuturesDailyHistoryUseCase,
    AggregateFuturesDailySessionBarUseCase,
)
from northstar_application.ports import FuturesDailyHistoricalAcquisitionQuery
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, Symbol
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_core.paper_trading import FuturesContractCount, PaperPortfolioIdentity
from northstar_core.strategy import StrategyIdentity
from northstar_infrastructure.market_data import (
    DatabentoFuturesHistoricalMarketDataSource,
    DatabentoFuturesHistoricalMarketDataSourceError,
    ExchangeCalendarFuturesTradingSessionResolver,
    FuturesTradingSessionInProgressError,
    SQLiteFuturesHistoricalMarketDataStore,
)
from test_futures_dashboard import _get
from test_futures_paper_cli_acceptance import (
    _SECRET,
    _SESSIONS,
    FakeTimeseries,
    Operator,
    Outcome,
    Provider,
    _close,
    _utc,
)

from northstar_api.app import create_app
from northstar_api.cli import ExitCode, main
from northstar_api.runtime import build_database_runtime, initialize_database
from northstar_api.settings import DashboardSettings

_ES_DEC = FuturesContract(
    FuturesProductReference(Symbol("ES"), ExchangeCode("CME")), ExpirationDate("2026-12-18")
)
_SETTINGS = {
    "NORTHSTAR_FUTURES_PRODUCT": "ES",
    "NORTHSTAR_FUTURES_EXCHANGE": "CME",
    "NORTHSTAR_FUTURES_EXPIRATION": "2026-12-18",
    "NORTHSTAR_STRATEGY": "alpha",
    "NORTHSTAR_PORTFOLIO": "futures-paper-alpha",
    "NORTHSTAR_TARGET": "1",
}
_EARLY_CLOSE = next(i for i, s in enumerate(_SESSIONS) if s.trading_date == date(2026, 7, 3))


def _opens(index: int) -> str:
    return _utc(_SESSIONS[index].opens_at.value).isoformat()


def _after(index: int, **delta: float) -> datetime:
    return _utc(_close(index)) + timedelta(**(delta or {"hours": 1}))


@dataclass
class FailingTimeseries(FakeTimeseries):
    """Fails bar requests for one session, optionally echoing the secret."""

    fail_opens: str = ""
    echo_secret: bool = False

    def get_range(self, **parameters):
        if parameters["schema"] != "definition" and parameters["start"].isoformat() == (
            self.fail_opens
        ):
            if self.echo_secret:
                raise DatabentoFuturesHistoricalMarketDataSourceError(
                    f"upstream rejected key {_SECRET}"
                )
            raise RuntimeError(f"upstream said {_SECRET}")
        return super().get_range(**parameters)


@dataclass
class Scheduler:
    """The scheduler: each run is one ``northstar operations daily`` invocation."""

    database: Path
    provider: Provider = field(default_factory=Provider)
    builds: list[str] = field(default_factory=list)
    clock_reads: int = 0

    @property
    def op(self) -> Operator:
        return Operator(self.database, self.provider)

    @property
    def env(self) -> dict[str, str]:
        return {
            **_SETTINGS,
            "NORTHSTAR_DATABASE": str(self.database),
            "DATABENTO_API_KEY": _SECRET,
        }

    def build(self, path: Path, api_key: str, clock) -> AcquireFuturesDailyHistoryUseCase:
        """Production acquisition wiring with only the Databento client replaced."""
        self.builds.append(api_key)
        initialize_database(path)
        return AcquireFuturesDailyHistoryUseCase(
            ExchangeCalendarFuturesTradingSessionResolver(),
            DatabentoFuturesHistoricalMarketDataSource(
                api_key, client=self.provider.client, clock=clock
            ),
            AggregateFuturesDailySessionBarUseCase(),
            SQLiteFuturesHistoricalMarketDataStore(path),
        )

    def daily(self, now: datetime, env: dict | None = None) -> Outcome:
        def clock() -> datetime:
            self.clock_reads += 1
            return now

        out, err = io.StringIO(), io.StringIO()
        code = main(
            ["operations", "daily"],
            env=self.env if env is None else env,
            stdout=out,
            stderr=err,
            daily_sync_runtime=self.build,
            clock=clock,
        )
        return Outcome(code, out.getvalue(), err.getvalue())

    def bootstrap(self, last: int) -> None:
        assert self.op.sync(0, last).code == 0

    @property
    def requests(self) -> list[str]:
        return list(self.provider.client.timeseries.bar_requests)

    def bars(self) -> list[str]:
        return [row[0] for row in self.op.query("SELECT point_in_time FROM futures_ohlcv")]


def _assert_clean(outcome: Outcome) -> None:
    lowered = outcome.text.lower()
    assert _SECRET not in outcome.text
    assert "traceback" not in lowered
    assert "executed at" not in lowered


def _section(outcome: Outcome, title: str) -> list[str]:
    lines = outcome.lines
    start = lines.index(title) + 1
    end = next((i for i in range(start, len(lines)) if lines[i] == ""), len(lines))
    return lines[start:end]


def _log(outcome: Outcome) -> list[str]:
    prefix = "northstar.operations "
    return [line.removeprefix(prefix) for line in outcome.err.splitlines() if prefix in line]


# ---------------------------------------------------------------------------
# Main schedule, played once
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def schedule(tmp_path_factory) -> dict:
    scheduler = Scheduler(tmp_path_factory.mktemp("daily") / "northstar.sqlite3")
    scheduler.bootstrap(23)
    assert scheduler.op.economics().code == 0
    s: dict = {"scheduler": scheduler, "bootstrap_requests": len(scheduler.requests)}

    scheduler.provider.publish(24)
    s["first"] = scheduler.daily(_after(24))
    s["first_requests"] = scheduler.requests[s["bootstrap_requests"] :]
    s["first_counts"], s["first_dump"] = scheduler.op.counts(), scheduler.op.dump()
    s["clock_reads"] = scheduler.clock_reads

    s["again"] = scheduler.daily(_after(24))
    s["later"] = scheduler.daily(_after(24, hours=3))
    s["retry_requests"] = scheduler.requests[s["bootstrap_requests"] :]
    s["retry_dump"] = scheduler.op.dump()

    # Restart: a new process, provider and scheduler over the same file.
    restarted = Scheduler(scheduler.database)
    restarted.provider.publish(25)
    s["settle"] = restarted.daily(_after(25))
    s["settle_requests"] = restarted.requests
    s["settle_counts"] = restarted.op.counts()
    s["settle_bars"] = restarted.bars()
    s["settle_again"] = restarted.daily(_after(25))
    s["settle_again_counts"] = restarted.op.counts()
    return s


def test_every_run_is_clean(schedule) -> None:
    for key in ("first", "again", "later", "settle", "settle_again"):
        _assert_clean(schedule[key])


def test_a_new_completed_session_is_synced_and_paper_traded_at_its_bar(schedule) -> None:
    first = schedule["first"]
    assert first.code == ExitCode.SUCCESS
    assert first.lines[:6] == [
        "DAILY OPERATION",
        "Contract: ES@CME 2026-12-18",
        f"Captured UTC: {_after(24).isoformat().replace('+00:00', 'Z')}",
        f"Persisted history before sync: through {_close(23)}",
        "Completed sessions acquired: 1 (2026-07-03 .. 2026-07-03)",
        f"Market cutoff: {_close(24)} (latest persisted daily bar)",
    ]
    assert schedule["first_requests"] == [_opens(24)]
    assert _section(first, "DECISION") == ["Action: BUY", f"Decision instant: {_close(24)}"]
    assert _section(first, "ORDER")[:3] == ["State: PENDING", "Side: BUY", "Contracts: 1"]
    assert f"Cutoff: {_close(24)}" in first.lines
    assert schedule["first_counts"] == (1, 1, 0)


def test_the_clock_is_read_exactly_once_per_run(schedule) -> None:
    assert schedule["clock_reads"] == 1


def test_the_operation_logs_its_facts(schedule) -> None:
    log = _log(schedule["first"])
    captured = _after(24).isoformat().replace("+00:00", "Z")
    assert log[:5] == [
        "INFO daily operation started",
        "INFO contract ES@CME 2026-12-18, strategy alpha, portfolio futures-paper-alpha, target 1",
        f"INFO captured UTC {captured}",
        f"INFO persisted daily history through {_close(23)}",
        "INFO completed sessions to acquire: 1 (2026-07-03 .. 2026-07-03)",
    ]
    assert "INFO acquisition completed: 1 sessions, 1 with a persisted daily bar" in log
    assert f"INFO market cutoff {_close(24)} (latest persisted daily bar)" in log
    assert f"INFO decision: Action: BUY; Decision instant: {_close(24)}" in log
    assert any(line.startswith("INFO paper order: State: PENDING; Side: BUY") for line in log)
    assert log[-2:] == ["INFO valuation available", "INFO exit SUCCESS (0)"]


def test_repeated_runs_without_a_new_session_are_idempotent(schedule) -> None:
    for key in ("again", "later"):
        run = schedule[key]
        assert run.code == ExitCode.SUCCESS
        assert "INFO no new completed session" in _log(run)
        assert "Completed sessions acquired: 0" in run.lines
        assert f"Market cutoff: {_close(24)} (latest persisted daily bar)" in run.lines
        assert _section(run, "ORDER") == _section(schedule["first"], "ORDER")
    assert schedule["retry_requests"] == schedule["first_requests"]
    assert schedule["retry_dump"] == schedule["first_dump"]


def test_the_next_session_settles_the_pending_order_at_its_open(schedule) -> None:
    settle = schedule["settle"]
    history = _section(settle, "ORDERS FOR THIS CONTRACT AND STRATEGY")
    assert settle.code == ExitCode.SUCCESS
    assert "  Simulated fill price: 7650" in history
    assert "  Price basis: OPEN of next synced session" in history
    assert f"  Fill observable from: {_close(25)}" in history
    assert _section(settle, "DECISION")[1] == f"Decision instant: {_close(25)}"
    assert _section(settle, "ORDER") == ["State: NO ACTION", "Reason: TARGET ALREADY MET"]
    assert "ES@CME 2026-12-18 (command contract): LONG 1 @ 7650" in settle.lines
    assert schedule["settle_counts"] == (2, 1, 1)


def test_a_restarted_process_derives_missing_sessions_from_the_database(schedule) -> None:
    assert schedule["settle_requests"] == [_opens(25)]
    assert schedule["settle_bars"][-2:] == [_close(24), _close(25)]
    assert f"Persisted history before sync: through {_close(24)}" in schedule["settle"].lines
    assert schedule["settle_again"].code == ExitCode.SUCCESS
    assert schedule["settle_again_counts"] == schedule["settle_counts"]


# ---------------------------------------------------------------------------
# Session selection
# ---------------------------------------------------------------------------


def test_several_missing_sessions_are_acquired_in_one_range(tmp_path: Path) -> None:
    scheduler = Scheduler(tmp_path / "gap.sqlite3")
    scheduler.bootstrap(21)
    before = len(scheduler.requests)
    for index in (22, 23, 24):
        scheduler.provider.publish(index)

    run = scheduler.daily(_after(24))

    assert run.code == ExitCode.SUCCESS
    assert scheduler.requests[before:] == [_opens(22), _opens(23), _opens(24)]
    assert scheduler.bars()[-3:] == [_close(22), _close(23), _close(24)]
    assert f"Market cutoff: {_close(24)} (latest persisted daily bar)" in run.lines


def test_the_session_in_progress_is_never_requested(tmp_path: Path) -> None:
    scheduler = Scheduler(tmp_path / "in-progress.sqlite3")
    scheduler.bootstrap(22)
    before = len(scheduler.requests)
    scheduler.provider.publish(23)
    scheduler.provider.publish(24)
    mid_session = _utc(_SESSIONS[24].opens_at.value) + timedelta(hours=6)

    run = scheduler.daily(mid_session)

    assert run.code == ExitCode.SUCCESS
    assert scheduler.requests[before:] == [_opens(23)]
    assert f"Market cutoff: {_close(23)} (latest persisted daily bar)" in run.lines
    _assert_clean(run)

    # The adapter guard independently refuses the session if it were ever passed.
    acquisition = scheduler.build(scheduler.database, _SECRET, lambda: mid_session)
    query = FuturesDailyHistoricalAcquisitionQuery(
        _ES_DEC, _SESSIONS[24].trading_date, _SESSIONS[24].trading_date
    )
    with pytest.raises(FuturesTradingSessionInProgressError):
        acquisition.execute(query)
    assert scheduler.requests[before:] == [_opens(23)]


def test_an_early_close_completes_at_its_resolved_close(tmp_path: Path) -> None:
    assert _close(_EARLY_CLOSE) == "2026-07-03T17:00:00Z"
    scheduler = Scheduler(tmp_path / "early.sqlite3")
    scheduler.bootstrap(_EARLY_CLOSE - 1)
    assert scheduler.op.economics().code == 0
    before = len(scheduler.requests)
    scheduler.provider.publish(_EARLY_CLOSE)

    at_close = scheduler.daily(_utc(_close(_EARLY_CLOSE)))
    after_close = scheduler.daily(_after(_EARLY_CLOSE, minutes=1))

    assert "INFO no new completed session" in _log(at_close)
    assert f"Market cutoff: {_close(_EARLY_CLOSE - 1)} (latest persisted daily bar)" in (
        at_close.lines
    )
    assert after_close.code == ExitCode.SUCCESS
    assert scheduler.requests[before:] == [_opens(_EARLY_CLOSE)]
    assert f"Market cutoff: {_close(_EARLY_CLOSE)} (latest persisted daily bar)" in (
        after_close.lines
    )


# ---------------------------------------------------------------------------
# Failures
# ---------------------------------------------------------------------------


def _failing(scheduler: Scheduler, index: int, *, echo_secret: bool = False) -> None:
    scheduler.provider.client.timeseries = FailingTimeseries(
        minutes=scheduler.provider.client.timeseries.minutes,
        fail_opens=_opens(index),
        echo_secret=echo_secret,
    )


def test_a_sync_failure_stops_before_paper_trading_and_keeps_earlier_sessions(
    tmp_path: Path,
) -> None:
    scheduler = Scheduler(tmp_path / "provider.sqlite3")
    scheduler.bootstrap(23)
    scheduler.provider.publish(24)
    scheduler.provider.publish(25)
    _failing(scheduler, 25)

    failed = scheduler.daily(_after(25))

    assert failed.code == ExitCode.PROVIDER
    assert "PROVIDER ERROR: DatabentoFuturesHistoricalMarketDataSourceError" in failed.err
    assert "may already have been stored" in failed.err
    assert "PAPER SESSION" not in failed.out
    assert scheduler.bars()[-1] == _close(24)
    assert scheduler.op.counts() == (0, 0, 0)
    assert _log(failed)[-1] == "INFO exit PROVIDER (6)"
    _assert_clean(failed)

    scheduler.provider.client.timeseries = FakeTimeseries(
        minutes=scheduler.provider.client.timeseries.minutes
    )
    resumed = scheduler.daily(_after(25))

    assert resumed.code == ExitCode.SUCCESS
    assert scheduler.requests == [_opens(25)]
    assert f"Market cutoff: {_close(25)} (latest persisted daily bar)" in resumed.lines


def test_a_provider_error_echoing_the_secret_is_redacted(tmp_path: Path) -> None:
    scheduler = Scheduler(tmp_path / "redact.sqlite3")
    scheduler.bootstrap(23)
    scheduler.provider.publish(24)
    _failing(scheduler, 24, echo_secret=True)

    failed = scheduler.daily(_after(24))

    assert failed.code == ExitCode.PROVIDER
    assert "upstream rejected key [REDACTED]" in failed.err
    assert "daily operation stopped: DatabentoFuturesHistoricalMarketDataSourceError" in (
        failed.err
    )
    _assert_clean(failed)


def test_missing_economics_is_partial_success_with_persisted_execution(tmp_path: Path) -> None:
    scheduler = Scheduler(tmp_path / "no-economics.sqlite3")
    scheduler.bootstrap(24)
    assert scheduler.daily(_after(24)).code == ExitCode.SUCCESS
    scheduler.provider.publish(25)

    run = scheduler.daily(_after(25))

    assert run.code == ExitCode.DATA
    assert run.lines[run.lines.index("PAPER SESSION: COMPLETED") - 7] == "DAILY OPERATION"
    assert _section(run, "P&L (gross simulated)")[0] == "P&L: unavailable"
    assert "completed and its facts were persisted" in run.err
    log = _log(run)
    assert (
        "WARNING execution complete; P&L unavailable because product economics are not configured"
    ) in log
    assert log[-1] == "INFO exit DATA (4)"
    assert scheduler.op.counts() == (2, 1, 1)
    _assert_clean(run)


def test_an_empty_database_requires_a_manual_bootstrap(tmp_path: Path) -> None:
    scheduler = Scheduler(tmp_path / "empty.sqlite3")

    run = scheduler.daily(_after(24))

    assert run.code == ExitCode.DATA
    assert "No persisted Futures history for ES@CME 2026-12-18." in run.err
    assert "Bootstrap market data with 'northstar market-data sync'" in run.err
    assert scheduler.builds == []
    assert scheduler.requests == []
    assert _log(run)[-1] == "INFO exit DATA (4)"


def test_a_missing_api_key_is_configuration_before_any_provider(tmp_path: Path) -> None:
    scheduler = Scheduler(tmp_path / "absent.sqlite3")
    env = {k: v for k, v in scheduler.env.items() if k != "DATABENTO_API_KEY"}

    run = scheduler.daily(_after(24), env=env)

    assert run.code == ExitCode.CONFIGURATION
    assert "DATABENTO_API_KEY is not set; operations daily needs Databento credentials." in run.err
    assert scheduler.builds == []
    assert scheduler.clock_reads == 0
    assert not scheduler.database.exists()


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"NORTHSTAR_TARGET": ""}, "missing NORTHSTAR_TARGET"),
        ({"NORTHSTAR_FUTURES_EXPIRATION": "2026-13-01"}, "NORTHSTAR_FUTURES_EXPIRATION"),
    ],
)
def test_incomplete_settings_are_configuration(tmp_path: Path, change, message) -> None:
    scheduler = Scheduler(tmp_path / "settings.sqlite3")

    run = scheduler.daily(_after(24), env={**scheduler.env, **change})

    assert run.code == ExitCode.CONFIGURATION
    assert message in run.err
    assert not scheduler.database.exists()


def test_no_settings_at_all_is_configuration(tmp_path: Path) -> None:
    run = Scheduler(tmp_path / "none.sqlite3").daily(_after(24), env={})

    assert run.code == ExitCode.CONFIGURATION
    assert "Futures operation is not configured" in run.err


def test_the_web_origin_is_not_required(tmp_path: Path) -> None:
    scheduler = Scheduler(tmp_path / "origin.sqlite3")
    scheduler.bootstrap(23)

    run = scheduler.daily(_after(23))

    assert "NORTHSTAR_WEB_ORIGIN" not in scheduler.env
    assert run.code == ExitCode.SUCCESS


# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------


def test_daily_help_documents_environment_configuration() -> None:
    out, err = io.StringIO(), io.StringIO()
    code = main(["operations", "daily", "--help"], env={}, stdout=out, stderr=err)
    text = out.getvalue()

    assert code == 0
    assert "every setting comes from the" in text
    for name in (
        "NORTHSTAR_DATABASE",
        "NORTHSTAR_FUTURES_PRODUCT",
        "NORTHSTAR_FUTURES_EXCHANGE",
        "NORTHSTAR_FUTURES_EXPIRATION",
        "NORTHSTAR_STRATEGY",
        "NORTHSTAR_PORTFOLIO",
        "NORTHSTAR_TARGET",
        "DATABENTO_API_KEY",
    ):
        assert name in text
    assert "NORTHSTAR_WEB_ORIGIN" not in text
    assert "--database" not in text and "--api-key" not in text
    assert "northstar market-data sync" in text


def test_top_level_help_lists_operations() -> None:
    out = io.StringIO()
    assert main(["--help"], env={}, stdout=out, stderr=io.StringIO()) == 0
    assert "operations" in out.getvalue()


# ---------------------------------------------------------------------------
# SQLite: a dashboard reader alongside the daily writer
# ---------------------------------------------------------------------------


def test_dashboard_reads_during_the_daily_write_stay_consistent(tmp_path: Path) -> None:
    scheduler = Scheduler(tmp_path / "shared.sqlite3")
    scheduler.bootstrap(23)
    assert scheduler.op.economics().code == 0
    scheduler.provider.publish(24)
    assert scheduler.daily(_after(24)).code == ExitCode.SUCCESS
    scheduler.provider.publish(25)
    settings = DashboardSettings(
        database=scheduler.database,
        web_origin="https://northstar.example",
        contract=_ES_DEC,
        strategy=StrategyIdentity("alpha"),
        portfolio=PaperPortfolioIdentity("futures-paper-alpha"),
        target=FuturesContractCount(1),
    )
    app = create_app(settings, runtime=build_database_runtime(scheduler.database))
    done, statuses = threading.Event(), []

    def read() -> None:
        while not done.is_set():
            statuses.append(_get(app, "/futures/dashboard").status)

    reader = threading.Thread(target=read)
    reader.start()
    try:
        run = scheduler.daily(_after(25))
    finally:
        done.set()
        reader.join()

    assert run.code == ExitCode.SUCCESS
    assert statuses and set(statuses) == {200}
    final = _get(app, "/futures/dashboard").json
    assert final["paper"]["order_state"] == "no_order"
    assert final["portfolio"]["positions"][0]["average_entry"] == "7650"
    with sqlite3.connect(scheduler.database) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)
