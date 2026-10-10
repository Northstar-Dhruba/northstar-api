"""INDIA-8D: read-only operational status on the futures dashboard.

GET /futures/dashboard gains an ``operations`` section for the chronologically
operated NSE contract: the configured finality and the policy's assessment of
the next required session, the session backlog, and the pre-expiry window.
It is derived on each read from persisted facts, the NSE calendar, the
configured finality policy and the NSE expiry guard -- never from a provider,
a clock or any new persisted state.

Facts are produced exactly as an operator would, through INDIA-7's manual
workflow (production CLI, real NSE sessions, the range-honouring Upstox
transport double), on temporary SQLite. The network is refused throughout.
"""

from __future__ import annotations

import socket
import sqlite3
from contextlib import closing
from datetime import date, datetime
from pathlib import Path

import pytest
from northstar_core.derivatives import ExpirationDate
from northstar_core.futures import FuturesContract
from northstar_core.paper_trading import FuturesContractCount, PaperPortfolioIdentity
from northstar_core.strategy import StrategyIdentity
from northstar_infrastructure.market_data import (
    UpstoxFuturesNativeDailyMarketDataSource,
    upstox_http,
)
from test_futures_dashboard import _ES_DEC, _get
from test_futures_dashboard import Book as CmeBook
from test_india7_nifty_incremental_operations_acceptance import (
    _CONTRACT,
    _E8_BAR,
    _NIFTY,
    _PORTFOLIO,
    _SESSIONS,
    _STRATEGY,
    _close,
    _day,
    _expiry_operator,
    _operator,
)

from northstar_api.app import create_app
from northstar_api.operations_lock import DatabaseOperationsLock
from northstar_api.runtime import build_database_runtime
from northstar_api.settings import (
    DashboardSettings,
    DashboardSettingsError,
    FuturesDailyBarFinalityMode,
    FuturesSessionOperationSettings,
    load_dashboard_settings,
)

_ORIGIN = "http://localhost:5173"
_E7, _E6, _E5 = _E8_BAR + 1, _E8_BAR + 2, _E8_BAR + 3
_EXPIRY_BAR = len(_SESSIONS)


@pytest.fixture(scope="module", autouse=True)
def no_network():
    import databento

    def refuse(*args, **kwargs):
        raise AssertionError("INDIA-8D must not touch the network")

    # socket.socket.connect stays usable: asyncio's Windows event loop needs a
    # local socket pair to drive the in-process ASGI requests.
    patcher = pytest.MonkeyPatch()
    patcher.setattr(socket, "create_connection", refuse)
    patcher.setattr(upstox_http, "urlopen", refuse)
    patcher.setattr(databento, "Historical", refuse)
    yield
    patcher.undo()


# ---------------------------------------------------------------------------
# The dashboard over an operator's database
# ---------------------------------------------------------------------------


def _operations(
    finality: str | None, final_through: int | None, go_live: int | str | None
) -> FuturesSessionOperationSettings:
    mode = FuturesDailyBarFinalityMode(finality or "disabled")
    approved = date.fromisoformat(_day(final_through)) if final_through is not None else None
    if isinstance(go_live, int):
        go_live = _day(go_live)
    return FuturesSessionOperationSettings(
        mode, approved, date.fromisoformat(go_live) if go_live is not None else None
    )


def _settings(
    database: Path,
    *,
    contract: FuturesContract = _CONTRACT,
    finality: str | None = None,
    final_through: int | None = None,
    go_live: int | str | None = None,
) -> DashboardSettings:
    return DashboardSettings(
        database=database,
        web_origin=_ORIGIN,
        contract=contract,
        strategy=_STRATEGY,
        portfolio=_PORTFOLIO,
        target=FuturesContractCount(1),
        operations=_operations(finality, final_through, go_live),
    )


def _app(database: Path, **settings):
    return create_app(_settings(database, **settings), runtime=build_database_runtime(database))


def _dashboard(database: Path, query: str = "", **settings) -> dict:
    response = _get(_app(database, **settings), "/futures/dashboard", query)
    assert response.status == 200, response.body
    return response.json


def _dump(database: Path) -> list:
    with closing(sqlite3.connect(database)) as connection:
        return list(connection.iterdump())


def _cycled(tmp_path: Path, name: str, through: int, synced_through: int | None = None):
    """Bootstrap bars 1..20, operate 21..``through`` day by day, then sync ahead."""
    op = _operator(tmp_path, name)
    op.bootstrap(20)
    for bar in range(21, through + 1):
        op.cycle(bar)
    if synced_through is not None:
        assert op.sync(through + 1, synced_through).code == 0
    return op


def _expiry_cycled(tmp_path: Path, name: str, through: int):
    op = _expiry_operator(tmp_path, name)
    for bar in range(_E8_BAR, through + 1):
        op.cycle(bar)
    return op


# ---------------------------------------------------------------------------
# Backlog and next required session
# ---------------------------------------------------------------------------


def test_before_go_live_with_no_decision(tmp_path: Path) -> None:
    op = _operator(tmp_path, "pre-go-live")
    op.bootstrap(20)

    operations = _dashboard(op.database, go_live=23)["operations"]

    assert operations["status"] == "available"
    assert operations["backlog"] == {
        "status": "available",
        "reason": None,
        "stage": "not_started",
        "go_live": _day(23),
        "latest_decision_session": None,
        "latest_market_session": _day(20),
        "next_required_session": _day(23),
        "next_required_cutoff": _close(23),
        "final_sessions_pending": "0",
        "stored_sessions_pending": "0",
        "market_data_ahead": False,
        "caught_up": True,
    }
    assert operations["finality"] == {
        "mode": "disabled",
        "final_through": None,
        "next_session_outcome": "UNKNOWN",
        "next_session_reason": operations["finality"]["next_session_reason"],
    }
    expiry = operations["expiry"]
    assert (expiry["window"], expiry["flatten_required"], expiry["position_flat"]) == (
        None,
        None,
        True,
    )
    assert expiry["reopening_blocked"] is False


def test_no_decision_and_no_go_live_asks_for_go_live(tmp_path: Path) -> None:
    op = _operator(tmp_path, "no-go-live")
    op.bootstrap(20)

    operations = _dashboard(op.database)["operations"]

    backlog = operations["backlog"]
    assert (backlog["status"], backlog["stage"]) == ("available", "go_live_required")
    assert backlog["latest_market_session"] == _day(20)
    for unknown in (
        "next_required_session", "final_sessions_pending", "stored_sessions_pending",
        "market_data_ahead", "caught_up",
    ):  # fmt: skip
        assert backlog[unknown] is None
    assert operations["finality"]["next_session_outcome"] is None


def test_an_empty_database_is_readable(tmp_path: Path) -> None:
    database = tmp_path / "empty.sqlite3"

    dashboard = _dashboard(database, go_live=21)

    backlog = dashboard["operations"]["backlog"]
    assert (backlog["stage"], backlog["next_required_session"]) == ("not_started", _day(21))
    assert backlog["latest_market_session"] is None
    assert dashboard["operations"]["expiry"]["position_flat"] is None
    assert dashboard["portfolio"]["status"] == "unavailable"


@pytest.mark.parametrize(
    ("go_live", "message"),
    [("2026-10-20", "is not a trading session"), ("2026-10-28", "is after the expiry")],
    ids=["holiday", "after-expiry"],
)
def test_an_unusable_go_live_leaves_the_dashboard_readable(
    tmp_path: Path, go_live: str, message: str
) -> None:
    op = _operator(tmp_path, "bad-go-live")
    op.bootstrap(20)

    dashboard = _dashboard(op.database, go_live=go_live)

    backlog = dashboard["operations"]["backlog"]
    assert backlog["status"] == "unavailable" and message in backlog["reason"]
    assert backlog["next_required_session"] is None
    assert dashboard["market"]["status"] == "available"


def test_caught_up_after_operating_day_by_day(tmp_path: Path) -> None:
    op = _cycled(tmp_path, "caught-up", through=23)

    operations = _dashboard(
        op.database, finality="operator-approved", final_through=23, go_live=21
    )["operations"]

    backlog = operations["backlog"]
    assert backlog == {
        "status": "available",
        "reason": None,
        "stage": "operating",
        "go_live": None,
        "latest_decision_session": _day(23),
        "latest_market_session": _day(23),
        "next_required_session": _day(24),
        "next_required_cutoff": _close(24),
        "final_sessions_pending": "0",
        "stored_sessions_pending": "0",
        "market_data_ahead": False,
        "caught_up": True,
    }
    assert operations["finality"]["next_session_outcome"] == "NOT_YET_FINAL"


def test_market_data_ahead_of_paper_decisions(tmp_path: Path) -> None:
    op = _cycled(tmp_path, "ahead", through=22, synced_through=25)

    backlog = _dashboard(op.database)["operations"]["backlog"]

    assert backlog["latest_decision_session"] == _day(22)
    assert backlog["latest_market_session"] == _day(25)
    assert backlog["next_required_session"] == _day(23)
    assert backlog["stored_sessions_pending"] == "3"
    assert backlog["market_data_ahead"] is True
    assert backlog["caught_up"] is False


# ---------------------------------------------------------------------------
# Finality
# ---------------------------------------------------------------------------


def test_operator_approved_finality_with_a_final_next_session(tmp_path: Path) -> None:
    op = _cycled(tmp_path, "final", through=22, synced_through=25)

    operations = _dashboard(op.database, finality="operator-approved", final_through=24)[
        "operations"
    ]

    finality = operations["finality"]
    assert (finality["mode"], finality["final_through"]) == ("operator-approved", _day(24))
    assert finality["next_session_outcome"] == "FINAL"
    assert operations["backlog"]["final_sessions_pending"] == "2"
    assert operations["backlog"]["caught_up"] is False


def test_operator_approved_finality_with_a_not_yet_final_next_session(tmp_path: Path) -> None:
    op = _cycled(tmp_path, "not-final", through=22, synced_through=25)

    operations = _dashboard(op.database, finality="operator-approved", final_through=22)[
        "operations"
    ]

    assert operations["finality"]["next_session_outcome"] == "NOT_YET_FINAL"
    assert operations["finality"]["next_session_reason"]
    assert operations["backlog"]["final_sessions_pending"] == "0"
    assert operations["backlog"]["market_data_ahead"] is True


def test_disabled_finality_is_unknown(tmp_path: Path) -> None:
    op = _cycled(tmp_path, "disabled", through=22, synced_through=25)

    operations = _dashboard(op.database, finality="disabled")["operations"]

    assert operations["finality"]["mode"] == "disabled"
    assert operations["finality"]["final_through"] is None
    assert operations["finality"]["next_session_outcome"] == "UNKNOWN"
    assert operations["backlog"]["final_sessions_pending"] == "0"


# ---------------------------------------------------------------------------
# Expiry safety (E = 10-27, E-6 = 10-16, E-5 = 10-19; 10-20 is a holiday)
# ---------------------------------------------------------------------------


def _expiry(tmp_path: Path, name: str, through: int) -> dict:
    op = _expiry_cycled(tmp_path, name, through)
    return _dashboard(op.database)["operations"]


def test_before_e6_the_window_is_outside(tmp_path: Path) -> None:
    e8 = _expiry(tmp_path, "e8", _E8_BAR)["expiry"]
    e7 = _expiry(tmp_path, "e7", _E7)["expiry"]

    assert e8["expiry_session"] == "2026-10-27"
    assert e8["flatten_sessions_before_expiry"] == "5"
    assert (e8["sessions_after_latest_decision"], e8["window"]) == ("8", "outside")
    assert (e8["flatten_required"], e8["reopening_blocked"]) == (False, False)
    assert (e7["sessions_after_latest_decision"], e7["window"]) == ("7", "outside")
    assert e7["flatten_required"] is False and e7["position_flat"] is False
    # The next decision (E-6) is governed by the guard, so nothing can reopen.
    assert e7["reopening_blocked"] is True


def test_e6_requires_the_flatten(tmp_path: Path) -> None:
    op = _expiry_cycled(tmp_path, "e6", _E6)
    dashboard = _dashboard(op.database)

    expiry = dashboard["operations"]["expiry"]
    assert (expiry["sessions_after_latest_decision"], expiry["window"]) == ("6", "flatten")
    assert (expiry["flatten_required"], expiry["position_flat"]) == (True, False)
    assert expiry["reopening_blocked"] is True
    assert dashboard["paper"]["order"]["side"] == "SELL"
    assert dashboard["paper"]["order"]["state"] == "pending"


def test_e5_is_the_protected_no_reopen_window_and_the_portfolio_is_flat(tmp_path: Path) -> None:
    expiry = _expiry(tmp_path, "e5", _E5)["expiry"]

    assert expiry == {
        "status": "available",
        "reason": None,
        "expiry_session": "2026-10-27",
        "flatten_sessions_before_expiry": "5",
        "sessions_after_latest_decision": "5",
        "window": "protected",
        "flatten_required": True,
        "position_flat": True,
        "pending_orders": "0",
        "assessed_as_of": "2026-10-19T10:10:00Z",
        "reopening_blocked": True,
    }


def test_a_fully_processed_expiry_requires_rollover(tmp_path: Path) -> None:
    operations = _expiry(tmp_path, "expired", _EXPIRY_BAR)

    backlog = operations["backlog"]
    assert backlog["stage"] == "rollover_required"
    assert backlog["latest_decision_session"] == "2026-10-27"
    assert backlog["next_required_session"] is None
    assert backlog["caught_up"] is True
    assert operations["finality"]["next_session_outcome"] is None
    expiry = operations["expiry"]
    assert (expiry["sessions_after_latest_decision"], expiry["window"]) == ("0", "protected")
    assert (expiry["position_flat"], expiry["reopening_blocked"]) == (True, True)


def test_the_november_calendar_fails_closed_without_breaking_the_dashboard(
    tmp_path: Path,
) -> None:
    november = FuturesContract(_NIFTY, ExpirationDate("2026-11-23"))

    dashboard = _dashboard(tmp_path / "nov.sqlite3", contract=november, go_live="2026-10-28")

    backlog = dashboard["operations"]["backlog"]
    assert backlog["status"] == "unavailable" and backlog["reason"]
    assert dashboard["operations"]["finality"]["next_session_outcome"] is None
    assert dashboard["contract"]["expiration"] == "2026-11-23"


def test_an_earlier_as_of_reports_the_state_at_that_cutoff(tmp_path: Path) -> None:
    op = _expiry_cycled(tmp_path, "as-of", _E5)

    operations = _dashboard(op.database, f"as_of={_close(_E7)}")["operations"]

    assert operations["backlog"]["latest_decision_session"] == _day(_E7)
    assert operations["expiry"]["window"] == "outside"


# ---------------------------------------------------------------------------
# Read-only, provider-free, clock-free
# ---------------------------------------------------------------------------


def test_reading_makes_no_provider_request(tmp_path: Path, monkeypatch) -> None:
    op = _cycled(tmp_path, "no-provider", through=22, synced_through=24)
    requests = list(op.upstox.candle_requests)

    def forbidden(*args, **kwargs):
        raise AssertionError("the dashboard must never build a provider adapter")

    monkeypatch.setattr(UpstoxFuturesNativeDailyMarketDataSource, "__init__", forbidden)
    _dashboard(op.database, finality="operator-approved", final_through=24)

    assert op.upstox.candle_requests == requests


def test_reading_mutates_nothing_and_takes_no_lock(tmp_path: Path) -> None:
    op = _cycled(tmp_path, "read-only", through=22, synced_through=24)
    app = _app(op.database, finality="operator-approved", final_through=24)
    lock = Path(f"{op.database.resolve()}.operations.lock")  # left by the manual writers
    before, lock_before = _dump(op.database), lock.stat().st_mtime_ns

    first = _get(app, "/futures/dashboard")
    second = _get(app, "/futures/dashboard")

    assert first.status == 200 and first.body == second.body
    assert _dump(op.database) == before
    assert lock.stat().st_mtime_ns == lock_before
    # A running operation holds the lock; reading is unaffected by it.
    with DatabaseOperationsLock(op.database):
        assert _get(app, "/futures/dashboard").body == first.body


class _NoNow(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: D102
        raise AssertionError("datetime.now() read by the dashboard")

    @classmethod
    def utcnow(cls):  # noqa: D102
        raise AssertionError("datetime.utcnow() read by the dashboard")


def test_finality_is_never_derived_from_the_clock(tmp_path: Path, monkeypatch) -> None:
    import northstar_application.application_services.futures_daily_bar_finality_policies as fp
    import northstar_application.application_services.futures_expiry_flatten_guard as guard
    import northstar_infrastructure.market_data.nse_futures_session as nse

    from northstar_api import operational_status, operations, runtime

    op = _cycled(tmp_path, "clock-free", through=22, synced_through=25)
    for module in (operational_status, operations, runtime, fp, guard, nse):
        if hasattr(module, "datetime"):
            monkeypatch.setattr(module, "datetime", _NoNow)

    outcomes = [
        _dashboard(op.database, finality="operator-approved", final_through=through)["operations"][
            "finality"
        ]["next_session_outcome"]
        for through in (22, 23)
    ]

    # Only the approved date decides; the same facts read twice give FINAL only once approved.
    assert outcomes == ["NOT_YET_FINAL", "FINAL"]


# ---------------------------------------------------------------------------
# Settings and CME
# ---------------------------------------------------------------------------

_NIFTY_ENV = {
    "NORTHSTAR_DATABASE": "C:/data/nifty.sqlite3",
    "NORTHSTAR_WEB_ORIGIN": "https://northstar.example",
    "NORTHSTAR_FUTURES_PRODUCT": "NIFTY",
    "NORTHSTAR_FUTURES_EXCHANGE": "NSE",
    "NORTHSTAR_FUTURES_EXPIRATION": "2026-10-27",
    "NORTHSTAR_STRATEGY": _STRATEGY.identity,
    "NORTHSTAR_PORTFOLIO": _PORTFOLIO.identity,
    "NORTHSTAR_TARGET": "1",
}


class _TrackingEnv(dict):
    def __init__(self, values: dict) -> None:
        super().__init__(values)
        self.read: list[str] = []

    def get(self, key, default=None):
        self.read.append(key)
        return super().get(key, default)


def test_dashboard_settings_read_finality_but_never_the_token() -> None:
    env = _TrackingEnv(
        {
            **_NIFTY_ENV,
            "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY": "operator-approved",
            "NORTHSTAR_FUTURES_FINAL_THROUGH": "2026-10-05",
            "NORTHSTAR_FUTURES_GO_LIVE": "2026-10-01",
            "UPSTOX_ANALYTICS_TOKEN": "placeholder-not-a-credential",
        }
    )

    settings = load_dashboard_settings(env)

    assert settings.operations == FuturesSessionOperationSettings(
        FuturesDailyBarFinalityMode.OPERATOR_APPROVED, date(2026, 10, 5), date(2026, 10, 1)
    )
    assert "UPSTOX_ANALYTICS_TOKEN" not in env.read
    assert load_dashboard_settings(_NIFTY_ENV).operations.finality_mode.value == "disabled"
    with pytest.raises(DashboardSettingsError, match="NORTHSTAR_FUTURES_FINAL_THROUGH"):
        load_dashboard_settings(
            {**_NIFTY_ENV, "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY": "operator-approved"}
        )


def test_cme_operations_are_not_applicable(tmp_path: Path) -> None:
    book = CmeBook(tmp_path / "cme.sqlite3").economics().history().daily(26)
    settings = DashboardSettings(
        database=book.path,
        web_origin=_ORIGIN,
        contract=_ES_DEC,
        strategy=StrategyIdentity("alpha"),
        portfolio=PaperPortfolioIdentity("futures-paper-alpha"),
        target=FuturesContractCount(1),
        operations=_operations("operator-approved", 23, 21),
    )
    app = create_app(settings, runtime=build_database_runtime(book.path))

    dashboard = _get(app, "/futures/dashboard").json

    assert dashboard["operations"] == {
        "status": "not_applicable",
        "reason": "no chronological operation for CME contracts",
        "finality": None,
        "backlog": None,
        "expiry": None,
    }
    assert dashboard["research"]["action"] == "BUY"
