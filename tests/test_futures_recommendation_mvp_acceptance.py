"""Final acceptance: the Futures Recommendation MVP from persisted data to the dashboard.

One timeline runs through production composition over one temporary SQLite
file:

    market-data sync -> economics set -> operations daily (D1) -> GET dashboard
    -> retries -> operations daily (D2) -> GET dashboard -> fresh process

Market data enters through ``northstar market-data sync`` and ``northstar
operations daily`` with the real exchange-calendar resolver, the real Databento
adapter and its completed-session guard, the real daily fold and the real
SQLite stores. Economics go through ``northstar economics set``; every
dashboard read goes through the ASGI application built by ``create_app`` with
its production startup, or the module-level ``northstar_api.app`` in a new
Python process. The only controlled boundaries are the Databento *client*
(DBN-shaped records, no network, a placeholder key) and the one wall-clock
reading.

The timeline mirrors the monitored deployment -- ES Dec 2026, strategy alpha,
portfolio futures-paper-alpha, target 1 -- on the real CME sessions from
2026-08-17. Twenty flat closes and ten rising ones warm the directional policy
up; the tenth, 2026-09-25 (D1), decides BUY. D2 is the next actual session,
Monday 2026-09-28. Its OPEN, 6620.123456789, uses the provider's full 1e-9
price resolution and differs from the decision quote (D1 close 6612.75) and
from the D2 close (6608.5), so the fill price is unambiguous. D2 decides HOLD.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
from contextlib import closing
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from northstar_application.application_services import (
    FreezeFuturesForwardResearchDecisionUseCase,
    RunFuturesPaperTradingSessionUseCase,
)
from northstar_application.ports import FuturesForwardResearchRecordQuery
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    Money,
    PointInTime,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_core.paper_trading import OrderSide, PaperPortfolioIdentity
from northstar_core.strategy import FuturesAssetAnalysisGenerator, Strategy, StrategyIdentity
from northstar_infrastructure.market_data import (
    DatabentoFuturesHistoricalMarketDataSource,
    ExchangeCalendarFuturesTradingSessionResolver,
)
from test_futures_daily_operation import Scheduler
from test_futures_dashboard import _get, _lifespan
from test_futures_paper_cli_acceptance import _SECRET, FakeMinute, Outcome, _ns, _raw, _utc

from northstar_api.app import create_app
from northstar_api.cli import ExitCode, main
from northstar_api.runtime import build_database_runtime
from northstar_api.settings import load_dashboard_settings

_API = Path(__file__).resolve().parents[1]
_DEPLOY = _API / "deploy"
_DEPLOYED_DATABASE = "/data/northstar.sqlite3"

_ES = FuturesProductReference(Symbol("ES"), ExchangeCode("CME"))
_ES_DEC = FuturesContract(_ES, ExpirationDate("2026-12-18"))
_ALPHA = StrategyIdentity("alpha")
_PORTFOLIO = PaperPortfolioIdentity("futures-paper-alpha")
_USD = Currency("USD")
_SESSIONS = ExchangeCalendarFuturesTradingSessionResolver().sessions_in_range(
    _ES, date(2026, 8, 17), date(2026, 10, 2)
)
_D1, _D2 = 29, 30
_SETTINGS = {
    "NORTHSTAR_FUTURES_PRODUCT": "ES",
    "NORTHSTAR_FUTURES_EXCHANGE": "CME",
    "NORTHSTAR_FUTURES_EXPIRATION": "2026-12-18",
    "NORTHSTAR_STRATEGY": "alpha",
    "NORTHSTAR_PORTFOLIO": "futures-paper-alpha",
    "NORTHSTAR_TARGET": "1",
}

# (open, high, low, close, volume) per session index.
_CLOSES = ["6600.25"] * 20 + [str(Decimal("6601.50") + Decimal("1.25") * k) for k in range(9)]
_DAILY: dict[int, tuple[str, str, str, str, int]] = {
    index: (close, str(Decimal(close) + Decimal("2.5")), str(Decimal(close) - 2), close, 1000)
    for index, close in enumerate(_CLOSES)
}
_DAILY[_D1] = ("6611.50", "6614.00", "6610.25", "6612.75", 1250)
_DAILY[_D2] = ("6620.123456789", "6631.75", "6604.00", "6608.50", 1000)
_D2_OPEN = "6620.123456789"
_D2_CLOSE = "6608.5"
_DECISION_QUOTE = "6612.75"
_UNREALIZED = "-581.17283945"  # (6608.5 - 6620.123456789) x 1 contract x 50 USD


def _close(index: int) -> str:
    return _SESSIONS[index].closes_at.value


def _after(index: int, **delta: float) -> datetime:
    return _utc(_close(index)) + timedelta(**(delta or {"hours": 1}))


def _opens(index: int) -> str:
    return _utc(_SESSIONS[index].opens_at.value).isoformat()


def _publish(scheduler: Scheduler, index: int) -> None:
    """Hold one session at the provider as four minutes folding to its daily OHLCV."""
    opening, high, low, closing_, volume = _DAILY[index]
    opens_ns = _ns(_SESSIONS[index].opens_at.value)
    volumes = (volume // 4, volume // 4, volume // 4, volume - 3 * (volume // 4))
    scheduler.provider.client.timeseries.minutes[("ESZ6", _opens(index))] = [
        FakeMinute(opens_ns + minute * 60 * 10**9, _raw(q), _raw(q), _raw(q), _raw(q), v)
        for minute, (q, v) in enumerate(zip((opening, high, low, closing_), volumes, strict=True))
    ]


def _cli(*argv: str, **kwargs) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    code = main(list(argv), stdout=out, stderr=err, **kwargs)
    return Outcome(code, out.getvalue(), err.getvalue())


def _dump(path: Path) -> dict[str, list[tuple]]:
    with closing(sqlite3.connect(path)) as connection:
        tables = [
            r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        ]
        return {
            "schema": sorted(connection.execute("SELECT type, name, sql FROM sqlite_master")),
            **{t: sorted(connection.execute(f"SELECT * FROM {t}")) for t in tables},  # noqa: S608
        }


def _counts(dump: dict[str, list[tuple]]) -> tuple[int, int, int, int]:
    return tuple(
        len(dump[table])
        for table in (
            "futures_ohlcv",
            "futures_forward_research_records",
            "futures_paper_orders",
            "futures_paper_fills",
        )
    )


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _section(outcome: Outcome, title: str) -> list[str]:
    lines = outcome.lines
    start = lines.index(title) + 1
    end = next((i for i in range(start, len(lines)) if lines[i] == ""), len(lines))
    return lines[start:end]


def _api_env(database: Path) -> dict[str, str]:
    return {
        **_SETTINGS,
        "NORTHSTAR_DATABASE": str(database),
        "NORTHSTAR_WEB_ORIGIN": "https://northstar.example",
    }


def _app(database: Path):
    """A fresh application whose runtime is built by its own production startup."""
    app = create_app(load_dashboard_settings(_api_env(database)))
    assert {"type": "lifespan.startup.complete"} in _lifespan(app)
    return app


_FRESH_PROCESS = """
import sys
sys.path.insert(0, sys.argv[1])
from test_futures_dashboard import _get, _lifespan
from northstar_api.app import app
assert {"type": "lifespan.startup.complete"} in _lifespan(app)
for path in ("/futures/dashboard", "/health"):
    response = _get(app, path)
    sys.stdout.write(f"{response.status} {response.body.decode()}\\n")
"""


def _fresh_process(database: Path) -> list[tuple[int, bytes]]:
    """GET the dashboard and health from the module-level app in a new interpreter."""
    inherited = {
        k: v for k, v in os.environ.items() if not k.startswith(("NORTHSTAR_", "DATABENTO_"))
    }
    completed = subprocess.run(  # noqa: S603 - fixed interpreter and script
        [sys.executable, "-c", _FRESH_PROCESS, str(_API / "tests")],
        env={**inherited, **_api_env(database)},
        capture_output=True,
        check=True,
        timeout=120,
    )
    responses = []
    for line in completed.stdout.decode().splitlines():
        status, body = line.split(" ", 1)
        responses.append((int(status), body.encode()))
    return responses


# ---------------------------------------------------------------------------
# The timeline, played once
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mvp(tmp_path_factory) -> SimpleNamespace:
    base = tmp_path_factory.getbasetemp().resolve()
    database = tmp_path_factory.mktemp("mvp") / "northstar.sqlite3"
    # Never the deployed database, whatever the developer's environment holds.
    assert database.resolve().is_relative_to(base)
    assert database.as_posix() != _DEPLOYED_DATABASE
    assert os.environ.get("NORTHSTAR_DATABASE") != str(database)

    scheduler = Scheduler(database)
    s = SimpleNamespace(database=database, outcomes=[])

    # Bootstrap S0..S28 manually, then configure economics.
    for index in range(_D1):
        _publish(scheduler, index)
    s.bootstrap = _cli(
        "market-data", "sync", "--database", str(database), "--product", "ES",
        "--exchange", "CME", "--expiration", "2026-12-18",
        "--start", _SESSIONS[0].trading_date.isoformat(),
        "--end", _SESSIONS[_D1 - 1].trading_date.isoformat(),
        env={"DATABENTO_API_KEY": _SECRET},
        market_sync_runtime=lambda path, key: scheduler.build(path, key, lambda: _after(_D1 - 1)),
    )  # fmt: skip
    s.economics = _cli(
        "economics", "set", "--database", str(database), "--product", "ES",
        "--exchange", "CME", "--point-value", "50", "--currency", "USD", env={},
    )  # fmt: skip
    s.requests_bootstrap = len(scheduler.requests)

    # The provider already holds D2 before D1 is processed: it must not leak.
    _publish(scheduler, _D1)
    _publish(scheduler, _D2)
    s.d1 = scheduler.daily(_after(_D1))
    s.requests_d1 = scheduler.requests[s.requests_bootstrap :]
    s.dump_d1, s.digest_d1 = _dump(database), _digest(database)
    runtime = build_database_runtime(database)
    s.records_d1 = runtime.forward_repository.get_records(
        FuturesForwardResearchRecordQuery(_ES_DEC, Timeframe("1d"))
    )
    s.snapshot_d1 = runtime.snapshot.execute(_ES_DEC, _ALPHA, _PORTFOLIO, PointInTime(_close(_D1)))

    # Dashboard reads: nothing may analyse, freeze, execute or reach the provider.
    def forbidden(*args, **kwargs):
        raise AssertionError("a dashboard read must never analyse, freeze, execute or acquire")

    app = _app(database)
    with pytest.MonkeyPatch.context() as patch:
        for owner, name in (
            (FuturesAssetAnalysisGenerator, "generate"),
            (Strategy, "evaluate_futures"),
            (FreezeFuturesForwardResearchDecisionUseCase, "execute"),
            (RunFuturesPaperTradingSessionUseCase, "execute"),
            (DatabentoFuturesHistoricalMarketDataSource, "__init__"),
        ):
            patch.setattr(owner, name, forbidden)
        s.dashboard_d1 = [_get(app, "/futures/dashboard") for _ in range(3)]
        s.dashboard_d1_as_of = _get(app, "/futures/dashboard", f"as_of={_close(_D1)}")
        s.health_d1 = _get(app, "/health")
    s.dump_after_get, s.digest_after_get = _dump(database), _digest(database)

    # Retries before D2 completes: the same clock, then mid-session D2.
    s.retry = scheduler.daily(_after(_D1))
    s.dump_retry = _dump(database)
    s.mid_d2 = scheduler.daily(_utc(_SESSIONS[_D2].opens_at.value) + timedelta(hours=12))
    s.dump_mid_d2 = _dump(database)
    s.requests_before_d2 = scheduler.requests[s.requests_bootstrap :]

    # D2 completes.
    s.d2 = scheduler.daily(_after(_D2))
    s.requests_d2 = scheduler.requests[s.requests_bootstrap :]
    s.dump_d2 = _dump(database)
    s.d2_retry = scheduler.daily(_after(_D2, hours=2))
    s.dump_d2_retry = _dump(database)
    s.dashboard_d2 = _get(app, "/futures/dashboard")
    runtime = build_database_runtime(database)
    s.records_d2 = runtime.forward_repository.get_records(
        FuturesForwardResearchRecordQuery(_ES_DEC, Timeframe("1d"))
    )
    s.snapshot_d2 = runtime.snapshot.execute(_ES_DEC, _ALPHA, _PORTFOLIO, PointInTime(_close(_D2)))
    s.outcomes = [s.bootstrap, s.economics, s.d1, s.retry, s.mid_d2, s.d2, s.d2_retry]

    # Restart: drop every in-memory object and read the same file again.
    del app, runtime
    s.restarted = _app(database)
    s.restarted_dashboard = _get(s.restarted, "/futures/dashboard")
    s.restarted_health = _get(s.restarted, "/health")
    restarted_runtime = build_database_runtime(database)
    s.restarted_snapshot = restarted_runtime.snapshot.execute(
        _ES_DEC, _ALPHA, _PORTFOLIO, PointInTime(_close(_D2))
    )
    s.fresh_process = _fresh_process(database)
    s.dump_final = _dump(database)
    with closing(sqlite3.connect(database)) as connection:
        s.integrity = connection.execute("PRAGMA integrity_check").fetchone()
    return s


# ---------------------------------------------------------------------------
# Timeline and warm-up
# ---------------------------------------------------------------------------


def test_every_command_is_clean_and_never_reveals_the_secret(mvp) -> None:
    for outcome in mvp.outcomes:
        lowered = outcome.text.lower()
        assert _SECRET not in outcome.text
        assert "traceback" not in lowered
        assert "executed at" not in lowered
    assert mvp.bootstrap.code == ExitCode.SUCCESS
    assert mvp.economics.code == ExitCode.SUCCESS


def test_the_timeline_uses_real_exchange_sessions(mvp) -> None:
    assert len(_SESSIONS[: _D1 + 1]) == 30
    assert _SESSIONS[0].trading_date == date(2026, 8, 17)
    assert _SESSIONS[_D1].trading_date == date(2026, 9, 25)
    assert _close(_D1) == "2026-09-25T22:00:00Z"
    # D2 is the next actual session: across the weekend, not the next calendar day.
    assert _SESSIONS[_D2].trading_date == date(2026, 9, 28)
    assert _close(_D2) == "2026-09-28T22:00:00Z"
    # Labor Day is an early-close session inside the warm-up history.
    assert _close(15) == "2026-09-07T17:00:00Z"
    assert "Sessions in range: 29" in mvp.bootstrap.lines
    assert "Sessions without trades: 0" in mvp.bootstrap.lines


def test_d1_is_decided_after_the_warm_up(mvp) -> None:
    prior = [row for row in mvp.dump_d1["futures_ohlcv"] if row[4] != _close(_D1)]
    assert len(prior) == _D1 >= 20
    (record,) = mvp.records_d1
    assert record.decision_instant == PointInTime(_close(_D1))
    assert record.strategy_identity == _ALPHA
    assert record.result.recommendation.action.value == "BUY"
    assert record.result.recommendation.asset_analysis.summarized_signals == ("strong bullish",)
    assert record.result.market_observation_context.latest_quote == QuoteValue(
        Decimal(_DECISION_QUOTE)
    )


# ---------------------------------------------------------------------------
# D1: frozen BUY, pending order, no same-session fill
# ---------------------------------------------------------------------------


def test_d1_acquires_the_completed_session_and_cuts_off_at_its_bar(mvp) -> None:
    d1 = mvp.d1
    assert d1.code == ExitCode.SUCCESS
    assert d1.lines[:6] == [
        "DAILY OPERATION",
        "Contract: ES@CME 2026-12-18",
        "Captured UTC: 2026-09-25T23:00:00Z",
        f"Persisted history before sync: through {_close(_D1 - 1)}",
        "Completed sessions acquired: 1 (2026-09-25 .. 2026-09-25)",
        f"Market cutoff: {_close(_D1)} (latest persisted daily bar)",
    ]
    # The paper cutoff is the bar, never the 23:00 clock reading.
    assert f"Cutoff: {_close(_D1)}" in d1.lines
    # D2 was already available at the provider and was never requested.
    assert mvp.requests_d1 == [_opens(_D1)]
    assert _section(d1, "DECISION") == ["Action: BUY", f"Decision instant: {_close(_D1)}"]


def test_d1_leaves_a_pending_order_and_no_same_session_fill(mvp) -> None:
    order = _section(mvp.d1, "ORDER")
    assert order[:3] == ["State: PENDING", "Side: BUY", "Contracts: 1"]
    assert re.fullmatch(r"ID: [0-9a-f]{64}", order[3])
    assert _section(mvp.d1, "PORTFOLIO (all contracts)") == ["Flat"]
    assert _counts(mvp.dump_d1) == (30, 1, 1, 0)

    snapshot = mvp.snapshot_d1
    (persisted,) = snapshot.orders
    assert persisted.identity.identity == order[3].removeprefix("ID: ")
    assert persisted.intent.side is OrderSide.BUY
    assert persisted.intent.contracts.value == 1
    assert persisted.intent.decided_at == PointInTime(_close(_D1))
    assert snapshot.fills == ()
    assert snapshot.pending_orders == (persisted,)
    assert snapshot.portfolio.positions == ()


def test_the_d1_dashboard_shows_the_frozen_buy_and_the_pending_order(mvp) -> None:
    response = mvp.dashboard_d1[0]
    assert response.status == 200
    body = response.json
    (order,) = mvp.snapshot_d1.orders
    (record,) = mvp.records_d1
    context = record.result.market_observation_context

    assert body["contract"] == {
        "product": "ES",
        "exchange": "CME",
        "expiration": "2026-12-18",
        "timeframe": "1d",
    }
    research = body["research"]
    assert (research["status"], research["action"]) == ("available", "BUY")
    assert research["decision_instant"] == record.decision_instant.value == _close(_D1)
    assert research["signals"] == ["strong bullish"]
    assert research["evidence"]["latest_quote"] == str(context.latest_quote.value)
    assert research["evidence"]["previous_close"] == "6611.5"
    assert body["market"] == {
        "status": "available",
        "reason": None,
        "session_instant": _close(_D1),
        "open": "6611.5",
        "high": "6614",
        "low": "6610.25",
        "close": _DECISION_QUOTE,
        "volume": "1250",
    }
    assert body["paper"]["order_state"] == "pending"
    assert body["paper"]["order"] == {
        "state": "pending",
        "side": "BUY",
        "contracts": "1",
        "order_identity": order.identity.identity,
        "decided_at": _close(_D1),
        "simulated_fill_price": None,
        "price_basis": None,
        "fill_observable_from": None,
    }
    assert body["portfolio"]["positions"] == []
    assert [o["order_identity"] for o in body["portfolio"]["pending_orders"]] == [
        order.identity.identity
    ]
    assert body["pnl"]["rows"] == []
    assert body["freshness"] == {
        "cutoff": _close(_D1),
        "cutoff_source": "latest_persisted_session",
        "latest_market_session": _close(_D1),
        "latest_decision_instant": _close(_D1),
    }


# ---------------------------------------------------------------------------
# GET is read-only; retries are idempotent; nothing leaks before a close
# ---------------------------------------------------------------------------


def test_dashboard_gets_write_nothing_and_never_recompute(mvp) -> None:
    responses = [*mvp.dashboard_d1, mvp.dashboard_d1_as_of]
    assert {r.status for r in responses} == {200}
    assert len({r.body for r in mvp.dashboard_d1}) == 1
    assert mvp.dashboard_d1_as_of.json["freshness"]["cutoff_source"] == "requested"
    assert mvp.health_d1.status == 200
    assert mvp.dump_after_get == mvp.dump_d1
    assert mvp.digest_after_get == mvp.digest_d1


def test_the_same_session_retry_changes_nothing(mvp) -> None:
    retry = mvp.retry
    assert retry.code == ExitCode.SUCCESS
    assert "Completed sessions acquired: 0" in retry.lines
    assert _section(retry, "ORDER") == _section(mvp.d1, "ORDER")
    assert mvp.dump_retry == mvp.dump_d1


def test_a_session_in_progress_never_leaks_into_facts(mvp) -> None:
    mid = mvp.mid_d2
    assert mid.code == ExitCode.SUCCESS
    assert "Captured UTC: 2026-09-28T10:00:00Z" in mid.lines
    assert "Completed sessions acquired: 0" in mid.lines
    assert f"Market cutoff: {_close(_D1)} (latest persisted daily bar)" in mid.lines
    assert f"Cutoff: {_close(_D1)}" in mid.lines
    assert _section(mid, "ORDER")[0] == "State: PENDING"
    assert mvp.requests_before_d2 == [_opens(_D1)]
    assert mvp.dump_mid_d2 == mvp.dump_d1


# ---------------------------------------------------------------------------
# D2: the pending order fills at the next session's OPEN
# ---------------------------------------------------------------------------


def test_d2_is_acquired_and_fills_the_d1_order_at_its_open(mvp) -> None:
    d2 = mvp.d2
    assert d2.code == ExitCode.SUCCESS
    assert "Completed sessions acquired: 1 (2026-09-28 .. 2026-09-28)" in d2.lines
    assert f"Market cutoff: {_close(_D2)} (latest persisted daily bar)" in d2.lines
    assert mvp.requests_d2 == [_opens(_D1), _opens(_D2)]
    assert _counts(mvp.dump_d2) == (31, 2, 1, 1)

    (fill,) = mvp.snapshot_d2.fills
    (order,) = mvp.snapshot_d2.orders
    (d1_order,) = mvp.snapshot_d1.orders
    assert order == d1_order
    assert fill.order_identity == order.identity
    assert fill.contracts == order.intent.contracts
    # The fill price is the D2 OPEN -- not the D1 close, the decision quote or the D2 close.
    assert len({_D2_OPEN, _DECISION_QUOTE, _DAILY[_D1][3], _DAILY[_D2][3]}) == 3
    assert fill.fill_quote == QuoteValue(Decimal(_D2_OPEN))
    assert fill.fill_quote != QuoteValue(Decimal(_DECISION_QUOTE))
    assert fill.fill_quote != QuoteValue(Decimal(_D2_CLOSE))
    # Observable from the D2 bar's completion, strictly after the decision.
    assert fill.filled_at == PointInTime(_close(_D2))
    assert fill.filled_at.compare(order.intent.decided_at) > 0

    history = _section(d2, "ORDERS FOR THIS CONTRACT AND STRATEGY")
    assert "  State: FILLED" in history
    assert f"  Simulated fill price: {_D2_OPEN}" in history
    assert "  Price basis: OPEN of next synced session" in history
    assert f"  Fill observable from: {_close(_D2)}" in history


def test_the_portfolio_is_long_one_at_the_d2_open(mvp) -> None:
    (position,) = mvp.snapshot_d2.portfolio.positions
    assert position.contract == _ES_DEC
    assert position.net_contracts == 1
    assert position.average_entry == QuoteValue(Decimal(_D2_OPEN))
    assert "ES@CME 2026-12-18 (command contract): LONG 1 @ 6620.123456789" in mvp.d2.lines


def test_d2_freezes_a_distinct_decision_and_keeps_d1_immutable(mvp) -> None:
    d1_record, d2_record = mvp.records_d2
    assert d1_record == mvp.records_d1[0]
    assert d2_record.decision_instant == PointInTime(_close(_D2))
    assert d2_record.result.recommendation.action.value == "HOLD"
    d1_rows = mvp.dump_d1["futures_forward_research_records"]
    assert set(d1_rows) < set(mvp.dump_d2["futures_forward_research_records"])
    # The paper policy reported its own reason; no order was created for D2.
    assert _section(mvp.d2, "DECISION") == ["Action: HOLD", f"Decision instant: {_close(_D2)}"]
    assert _section(mvp.d2, "ORDER") == ["State: NO ACTION", "Reason: HOLD"]
    assert mvp.dump_d2["futures_paper_orders"] == mvp.dump_d1["futures_paper_orders"]


def test_the_d2_retry_changes_nothing(mvp) -> None:
    assert mvp.d2_retry.code == ExitCode.SUCCESS
    assert "Completed sessions acquired: 0" in mvp.d2_retry.lines
    assert mvp.dump_d2_retry == mvp.dump_d2


def test_valuation_is_exact_money_in_the_settlement_currency(mvp) -> None:
    valuation = mvp.snapshot_d2.valuation
    assert valuation is not None
    (row,) = valuation.contracts
    assert row.contract == _ES_DEC
    assert row.settlement_currency == _USD
    assert row.realized_pnl == Money(Decimal(0), _USD)
    assert row.mark_quote == QuoteValue(Decimal(_D2_CLOSE))
    assert row.mark_instant == PointInTime(_close(_D2))
    assert row.unrealized_pnl == Money(Decimal(_UNREALIZED), _USD)
    assert isinstance(row.unrealized_pnl.amount, Decimal)
    pnl = _section(mvp.d2, "P&L (gross simulated)")
    assert pnl == [
        "ES@CME 2026-12-18",
        "  Realized P&L: 0 USD",
        f"  Mark: {_D2_CLOSE} (daily close at {_close(_D2)})",
        f"  Unrealized P&L: {_UNREALIZED} USD",
    ]


# ---------------------------------------------------------------------------
# The D2 dashboard, read by the same long-lived application
# ---------------------------------------------------------------------------


def test_the_d2_dashboard_shows_the_fill_position_and_pnl(mvp) -> None:
    response = mvp.dashboard_d2
    assert response.status == 200
    body = response.json
    (order,) = mvp.snapshot_d2.orders

    assert (body["research"]["action"], body["research"]["decision_instant"]) == (
        "HOLD",
        _close(_D2),
    )
    assert (body["paper"]["decision_instant"], body["paper"]["order_state"]) == (
        _close(_D2),
        "no_order",
    )
    assert body["paper"]["order"] is None
    latest, earlier = body["recent_decisions"]
    assert (latest["action"], latest["order_state"], latest["order"]) == ("HOLD", "no_order", None)
    assert (earlier["action"], earlier["decision_instant"]) == ("BUY", _close(_D1))
    assert earlier["order"] == {
        "state": "filled",
        "side": "BUY",
        "contracts": "1",
        "order_identity": order.identity.identity,
        "decided_at": _close(_D1),
        "simulated_fill_price": _D2_OPEN,
        "price_basis": "OPEN of next synced session",
        "fill_observable_from": _close(_D2),
    }
    assert body["market"]["open"] == _D2_OPEN
    assert body["market"]["close"] == _D2_CLOSE
    assert body["portfolio"]["positions"] == [
        {
            "contract": {"product": "ES", "exchange": "CME", "expiration": "2026-12-18"},
            "selected_contract": True,
            "direction": "LONG",
            "net_contracts": "1",
            "average_entry": _D2_OPEN,
        }
    ]
    assert body["portfolio"]["pending_orders"] == []
    assert body["pnl"] == {
        "status": "available",
        "reason": None,
        "missing_product": None,
        "rows": [
            {
                "contract": {"product": "ES", "exchange": "CME", "expiration": "2026-12-18"},
                "settlement_currency": "USD",
                "realized_pnl": "0",
                "position": "open",
                "mark_quote": _D2_CLOSE,
                "mark_instant": _close(_D2),
                "unrealized_pnl": _UNREALIZED,
                "unrealized_reason": None,
            }
        ],
    }


def test_the_dashboard_serializes_exact_decimal_strings(mvp) -> None:
    raw = mvp.dashboard_d2.body
    for fragment in (
        b'"open":"6620.123456789"',
        b'"simulated_fill_price":"6620.123456789"',
        b'"average_entry":"6620.123456789"',
        b'"mark_quote":"6608.5"',
        b'"unrealized_pnl":"-581.17283945"',
        b'"realized_pnl":"0"',
        b'"net_contracts":"1"',
        b'"volume":"1000"',
    ):
        assert fragment in raw
    # No total across contracts or currencies is invented.
    assert b"total" not in raw.lower()
    assert _SECRET.encode() not in raw
    assert b"executed" not in raw.lower()


# ---------------------------------------------------------------------------
# Restart and health
# ---------------------------------------------------------------------------


def test_a_fresh_application_reproduces_the_dashboard(mvp) -> None:
    assert mvp.restarted_dashboard.status == 200
    assert mvp.restarted_dashboard.body == mvp.dashboard_d2.body
    assert mvp.restarted_snapshot == mvp.snapshot_d2
    assert mvp.dump_final == mvp.dump_d2


def test_a_new_process_reproduces_the_dashboard_and_is_healthy(mvp) -> None:
    (dashboard_status, dashboard_body), (health_status, health_body) = mvp.fresh_process
    assert dashboard_status == 200
    assert dashboard_body == mvp.dashboard_d2.body
    assert (health_status, json.loads(health_body)) == (200, {"status": "ok"})


def test_the_restarted_health_is_ok_and_sqlite_is_intact(mvp) -> None:
    assert (mvp.restarted_health.status, mvp.restarted_health.json) == (200, {"status": "ok"})
    assert mvp.integrity == ("ok",)


# ---------------------------------------------------------------------------
# Deployment artifacts: inexpensive facts; Docker behaviour is manual acceptance
# ---------------------------------------------------------------------------


def _services(compose: str) -> dict[str, str]:
    """Split the compose file's services into their text blocks by indentation."""
    body = compose.split("\nservices:\n", 1)[1].split("\nvolumes:\n", 1)[0]
    blocks: dict[str, str] = {}
    name = None
    for line in body.splitlines():
        header = re.fullmatch(r"  ([a-z]+):", line)
        if header:
            name = header.group(1)
            blocks[name] = ""
        elif name is not None:
            blocks[name] += line + "\n"
    return blocks


def test_deployment_shares_data_and_confines_the_secret() -> None:
    compose = (_DEPLOY / "compose.yaml").read_text(encoding="utf-8")
    services = _services(compose)
    assert set(services) == {"api", "operations", "web"}

    database = re.search(r"NORTHSTAR_DATABASE: \$\{NORTHSTAR_DATABASE:-([^}]+)\}", compose)
    assert database is not None and database.group(1).startswith("/data/")
    for name in ("api", "operations"):
        assert "- northstar-data:/data" in services[name]
        assert "<<: *futures-settings" in services[name]
    assert "northstar-data" not in services["web"]
    # The same image runs the API and the scheduled operation.
    assert "image: northstar-api:local" in services["operations"]
    assert "image: northstar-api:local" in services["api"]

    assert "DATABENTO_API_KEY" in services["operations"]
    outside = compose.replace(services["operations"], "")
    assert "DATABENTO_API_KEY" not in outside

    assert "ports:" not in services["api"]
    assert "8000" not in services["web"]
    assert '"443:443"' in services["web"]


def test_deployment_fronts_the_api_through_caddy_on_pinned_runtimes() -> None:
    caddy = (_DEPLOY / "Caddyfile").read_text(encoding="utf-8")
    assert re.search(r"handle_path /api/\* \{\s*reverse_proxy api:8000\s*\}", caddy)
    assert "basic_auth" in caddy

    web = (_DEPLOY / "Dockerfile.web").read_text(encoding="utf-8")
    assert re.search(r"^FROM node:22\.\S+ AS build$", web, re.MULTILINE)
    api = (_DEPLOY / "Dockerfile.api").read_text(encoding="utf-8")
    python_images = re.findall(r"^FROM python:(\S+)", api, re.MULTILINE)
    assert python_images and {image.split("-")[0] for image in python_images} == {"3.13"}
    pyproject = (_API / "pyproject.toml").read_text(encoding="utf-8")
    assert 'requires-python = ">=3.13,<3.14"' in pyproject
