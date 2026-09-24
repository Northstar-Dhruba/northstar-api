"""Tests for the read-only futures dashboard HTTP API.

Every request goes through the real ASGI application over real temporary
SQLite files. Paper facts are produced by the production ``northstar`` CLI or,
for shapes the signal fixture cannot reach, stored through the production
SQLite stores. There is no network and no provider key.
"""

from __future__ import annotations

import asyncio
import io
import json
import shutil
import sqlite3
from contextlib import closing
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from northstar_application.application_services import (
    FreezeFuturesForwardResearchDecisionUseCase,
    RunFuturesPaperTradingSessionUseCase,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesOHLCVBar, FuturesProductReference
from northstar_core.paper_trading import (
    FuturesContractCount,
    FuturesExecutionIntent,
    FuturesPaperFill,
    FuturesPaperOrder,
    OrderSide,
    PaperFillIdentity,
    PaperOrderIdentity,
    PaperPortfolioIdentity,
)
from northstar_core.strategy import FuturesAssetAnalysisGenerator, Strategy, StrategyIdentity
from northstar_infrastructure.market_data import SQLiteFuturesHistoricalMarketDataStore
from northstar_infrastructure.persistence import (
    SQLiteFuturesPaperFillStore,
    SQLiteFuturesPaperOrderStore,
)

from northstar_api.app import create_app
from northstar_api.cli import main
from northstar_api.runtime import build_database_runtime
from northstar_api.settings import (
    DashboardSettings,
    DashboardSettingsError,
    load_dashboard_settings,
)

_ES = FuturesProductReference(Symbol("ES"), ExchangeCode("CME"))
_ES_DEC = FuturesContract(_ES, ExpirationDate("2026-12-18"))
_ES_MAR = FuturesContract(_ES, ExpirationDate("2027-03-19"))
_FESX_DEC = FuturesContract(
    FuturesProductReference(Symbol("FESX"), ExchangeCode("EUREX")), ExpirationDate("2026-12-18")
)
_ALPHA = StrategyIdentity("alpha")
_PORTFOLIO = PaperPortfolioIdentity("futures-paper-alpha")
_ORIGIN = "http://localhost:5173"
_GENERIC_500 = "Persisted futures state is unavailable or inconsistent."


def _session_dates(count: int) -> list[date]:
    dates: list[date] = []
    day = date(2026, 6, 1)
    while len(dates) < count:
        if day.weekday() < 5 and day != date(2026, 6, 19):
            dates.append(day)
        day += timedelta(days=1)
    return dates


_DATES = _session_dates(30)


def _at(session: int) -> str:
    return f"{_DATES[session - 1].isoformat()}T21:00:00Z"


def _bar(
    session: int,
    close: str,
    *,
    open_: str | None = None,
    high: str | None = None,
    low: str | None = None,
    volume: str = "1000",
    contract: FuturesContract = _ES_DEC,
) -> FuturesOHLCVBar:
    closing = Decimal(close)
    opening = Decimal(open_) if open_ is not None else closing
    return FuturesOHLCVBar(
        contract=contract,
        point_in_time=PointInTime(_at(session)),
        timeframe=Timeframe("1d"),
        open=QuoteValue(opening),
        high=QuoteValue(Decimal(high) if high is not None else max(opening, closing) + 2),
        low=QuoteValue(Decimal(low) if low is not None else min(opening, closing) - 2),
        close=QuoteValue(closing),
        volume=Quantity(Decimal(volume)),
    )


_RISE = ["7600"] * 15 + [str(7601 + index) for index in range(10)]
_LATER = {
    26: _bar(26, "7611", open_="7650", high="7700", low="7600"),
    27: _bar(27, "3000", open_="7600", high="7610", low="2990", volume="250000"),
    28: _bar(28, "2900", open_="2950", high="3050", low="2800", volume="250000"),
    29: _bar(29, "2900", open_="2900", high="2950", low="2850"),
}


# ---------------------------------------------------------------------------
# Facts through production adapters and the production CLI
# ---------------------------------------------------------------------------


class Book:
    """One SQLite file the operator fills through production commands."""

    def __init__(self, path: Path) -> None:
        self.path = path
        build_database_runtime(path)

    def bars(self, *bars: FuturesOHLCVBar) -> Book:
        SQLiteFuturesHistoricalMarketDataStore(self.path).store(bars)
        return self

    def history(self, sessions: int = 25) -> Book:
        return self.bars(*(_bar(n, c) for n, c in enumerate(_RISE[:sessions], start=1)))

    def cli(self, *argv: str) -> int:
        return main(list(argv), env={}, stdout=io.StringIO(), stderr=io.StringIO())

    def economics(self, product="ES", exchange="CME", point_value="50", currency="USD") -> Book:
        code = self.cli(
            "economics", "set", "--database", str(self.path), "--product", product,
            "--exchange", exchange, "--point-value", point_value, "--currency", currency,
        )  # fmt: skip
        assert code == 0
        return self

    def run(self, session: int) -> Book:
        code = self.cli(
            "paper", "run", "--database", str(self.path), "--product", "ES",
            "--exchange", "CME", "--expiration", "2026-12-18", "--strategy", "alpha",
            "--portfolio", "futures-paper-alpha", "--target", "1", "--as-of", _at(session),
        )  # fmt: skip
        assert code in (0, 4)
        return self

    def daily(self, last: int) -> Book:
        for session in range(25, last + 1):
            if session in _LATER:
                self.bars(_LATER[session])
            self.run(session)
        return self

    def trade(
        self,
        order_id: str,
        contract: FuturesContract,
        side: OrderSide,
        contracts: int,
        quote: str | None,
        *,
        decided: int = 1,
        filled: int = 2,
        strategy: StrategyIdentity = _ALPHA,
    ) -> Book:
        order = FuturesPaperOrder(
            PaperOrderIdentity(order_id),
            FuturesExecutionIntent(
                _PORTFOLIO,
                contract,
                side,
                FuturesContractCount(contracts),
                strategy,
                PointInTime(_at(decided)),
            ),
        )
        SQLiteFuturesPaperOrderStore(self.path).store((order,))
        if quote is not None:
            fill = FuturesPaperFill(
                PaperFillIdentity(f"fill-{order_id}"),
                order.identity,
                order.intent,
                order.intent.contracts,
                QuoteValue(Decimal(quote)),
                PointInTime(_at(filled)),
            )
            SQLiteFuturesPaperFillStore(self.path).store((fill,))
        return self

    def dump(self) -> dict:
        with closing(sqlite3.connect(self.path)) as connection:
            tables = [
                r[0]
                for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            ]
            return {
                "schema": sorted(connection.execute("SELECT type, name, sql FROM sqlite_master")),
                **{
                    t: sorted(connection.execute(f"SELECT * FROM {t}"))  # noqa: S608
                    for t in tables
                },
            }


@pytest.fixture
def book(tmp_path: Path) -> Book:
    return Book(tmp_path / "northstar.sqlite3")


def _settings(path: Path, strategy: str = "alpha") -> DashboardSettings:
    return DashboardSettings(
        database=path,
        web_origin=_ORIGIN,
        contract=_ES_DEC,
        strategy=StrategyIdentity(strategy),
        portfolio=_PORTFOLIO,
        target=FuturesContractCount(1),
    )


# ---------------------------------------------------------------------------
# Raw ASGI client
# ---------------------------------------------------------------------------


class Response:
    def __init__(self, status: int, headers: dict[str, str], body: bytes) -> None:
        self.status, self.headers, self.body = status, headers, body

    @property
    def json(self) -> Any:
        return json.loads(self.body)


def _get(app, path: str, query: str = "", headers: dict[str, str] | None = None) -> Response:
    return asyncio.run(_request(app, "GET", path, query, headers or {}))


async def _request(app, method: str, path: str, query: str, headers: dict) -> Response:
    status, response_headers, body = 0, {}, bytearray()
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        nonlocal status
        if message["type"] == "http.response.start":
            status = message["status"]
            response_headers.update(
                {k.decode().lower(): v.decode() for k, v in message.get("headers", [])}
            )
        elif message["type"] == "http.response.body":
            body.extend(message.get("body", b""))

    await app(
        {
            "type": "http",
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": query.encode(),
            "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
        },
        receive,
        send,
    )
    return Response(status, response_headers, bytes(body))


def _lifespan(app) -> list[dict]:
    async def drive() -> list[dict]:
        incoming = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
        sent: list[dict] = []

        async def receive():
            return incoming.pop(0)

        async def send(message):
            sent.append(message)

        try:
            await app({"type": "lifespan", "asgi": {"version": "3.0"}}, receive, send)
        except Exception:  # noqa: BLE001 - the failure message is asserted instead
            pass
        return sent

    return asyncio.run(drive())


def _app(book: Book, strategy: str = "alpha"):
    return create_app(_settings(book.path, strategy), runtime=build_database_runtime(book.path))


def _dashboard(book: Book, query: str = "", **kwargs) -> dict:
    response = _get(_app(book, **kwargs), "/futures/dashboard", query)
    assert response.status == 200, response.body
    return response.json


def _row(dashboard: dict, expiration: str = "2026-12-18", product: str = "ES") -> dict:
    (row,) = [
        r
        for r in dashboard["pnl"]["rows"]
        if r["contract"]["expiration"] == expiration and r["contract"]["product"] == product
    ]
    return row


# ---------------------------------------------------------------------------
# Health and configuration
# ---------------------------------------------------------------------------


def test_health_reads_the_configured_database(book: Book) -> None:
    response = _get(_app(book), "/health")

    assert (response.status, response.json) == (200, {"status": "ok"})


def test_health_is_unavailable_when_the_database_cannot_be_read(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    directory.mkdir()
    app = _app(Book(directory / "northstar.sqlite3"))
    shutil.rmtree(directory)

    response = _get(app, "/health")

    assert (response.status, response.json) == (503, {"status": "unavailable"})
    assert b"Traceback" not in response.body and b"sqlite" not in response.body.lower()


def test_an_unconfigured_app_serves_equity_and_refuses_futures() -> None:
    app = create_app()

    assert _get(app, "/health").status == 503
    dashboard = _get(app, "/futures/dashboard")
    assert (dashboard.status, dashboard.json) == (
        503,
        {"detail": "Futures dashboard is not configured."},
    )
    paths = set(app.openapi()["paths"])
    assert {"/analyze", "/watchlist/refresh", "/health", "/futures/dashboard"} <= paths


def test_the_runtime_is_built_at_startup_not_on_creation(book: Book) -> None:
    built: list[Path] = []

    def factory(path: Path):
        built.append(path)
        return build_database_runtime(path)

    app = create_app(_settings(book.path), runtime_factory=factory)
    assert built == []
    assert _get(app, "/futures/dashboard").status == 503

    messages = _lifespan(app)

    assert messages[0]["type"] == "lifespan.startup.complete"
    assert built == [book.path]
    assert _get(app, "/futures/dashboard").status == 200


def test_an_unusable_database_fails_startup_clearly(tmp_path: Path) -> None:
    app = create_app(_settings(tmp_path / "missing" / "db.sqlite3"))

    messages = _lifespan(app)

    assert messages[0]["type"] == "lifespan.startup.failed"
    assert "Database directory does not exist" in messages[0]["message"]


_ENV = {
    "NORTHSTAR_DATABASE": "C:/data/northstar.sqlite3",
    "NORTHSTAR_WEB_ORIGIN": "https://northstar.example",
    "NORTHSTAR_FUTURES_PRODUCT": "ES",
    "NORTHSTAR_FUTURES_EXCHANGE": "CME",
    "NORTHSTAR_FUTURES_EXPIRATION": "2026-12-18",
    "NORTHSTAR_STRATEGY": "alpha",
    "NORTHSTAR_PORTFOLIO": "futures-paper-alpha",
    "NORTHSTAR_TARGET": "1",
}


class TrackingEnv(dict):
    def __init__(self, values: dict) -> None:
        super().__init__(values)
        self.read: list[str] = []

    def get(self, key, default=None):
        self.read.append(key)
        return super().get(key, default)


def test_settings_are_parsed_into_core_values_without_the_provider_key() -> None:
    env = TrackingEnv({**_ENV, "DATABENTO_API_KEY": "db-SECRET"})

    settings = load_dashboard_settings(env)

    assert settings == DashboardSettings(
        database=Path("C:/data/northstar.sqlite3"),
        web_origin="https://northstar.example",
        contract=_ES_DEC,
        strategy=_ALPHA,
        portfolio=_PORTFOLIO,
        target=FuturesContractCount(1),
    )
    assert "DATABENTO_API_KEY" not in env.read
    assert load_dashboard_settings({}) is None


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"NORTHSTAR_TARGET": ""}, "missing NORTHSTAR_TARGET"),
        ({"NORTHSTAR_TARGET": "0"}, "NORTHSTAR_TARGET is invalid"),
        ({"NORTHSTAR_TARGET": "1.5"}, "NORTHSTAR_TARGET is invalid"),
        ({"NORTHSTAR_FUTURES_EXPIRATION": "2026-13-01"}, "NORTHSTAR_FUTURES_EXPIRATION"),
        ({"NORTHSTAR_FUTURES_PRODUCT": "E S"}, "NORTHSTAR_FUTURES_PRODUCT"),
        ({"NORTHSTAR_WEB_ORIGIN": "*"}, "NORTHSTAR_WEB_ORIGIN is invalid"),
        ({"NORTHSTAR_WEB_ORIGIN": "https://a.example/app"}, "NORTHSTAR_WEB_ORIGIN is invalid"),
    ],
)
def test_incomplete_or_invalid_settings_fail_clearly(changes: dict, message: str) -> None:
    with pytest.raises(DashboardSettingsError, match=message):
        load_dashboard_settings({**_ENV, **changes})


# ---------------------------------------------------------------------------
# CORS and route surface
# ---------------------------------------------------------------------------


def test_cors_allows_exactly_the_configured_origin(book: Book) -> None:
    app = _app(book)

    allowed = _get(app, "/futures/dashboard", headers={"Origin": _ORIGIN})
    other = _get(app, "/futures/dashboard", headers={"Origin": "https://evil.example"})

    assert allowed.headers.get("access-control-allow-origin") == _ORIGIN
    assert "access-control-allow-origin" not in other.headers


def test_the_futures_surface_is_get_only(book: Book) -> None:
    paths = _app(book).openapi()["paths"]

    assert set(paths["/health"]) == set(paths["/futures/dashboard"]) == {"get"}
    assert [path for path in paths if path.startswith("/futures")] == ["/futures/dashboard"]


# ---------------------------------------------------------------------------
# Dashboard: market and research
# ---------------------------------------------------------------------------


def test_no_market_data_is_a_successful_unavailable_dashboard(book: Book) -> None:
    dashboard = _dashboard(book)

    assert dashboard["contract"] == {
        "product": "ES",
        "exchange": "CME",
        "expiration": "2026-12-18",
        "timeframe": "1d",
    }
    assert dashboard["research"]["status"] == "unavailable"
    assert dashboard["market"]["status"] == "unavailable"
    assert dashboard["portfolio"]["status"] == "unavailable"
    assert dashboard["pnl"]["status"] == "unavailable"
    assert dashboard["recent_decisions"] == []
    assert dashboard["freshness"] == {
        "cutoff": None,
        "cutoff_source": "none",
        "latest_market_session": None,
        "latest_decision_instant": None,
    }
    assert dashboard["research"]["policy"] == "built-in directional MVP"


def test_market_history_without_a_frozen_record_claims_no_recommendation(book: Book) -> None:
    dashboard = _dashboard(book.history(19))

    assert dashboard["research"]["status"] == "unavailable"
    assert dashboard["research"]["reason"] == "no frozen recommendation available"
    assert dashboard["research"]["action"] is None
    assert "warm" not in json.dumps(dashboard)
    assert dashboard["market"]["status"] == "available"
    assert dashboard["freshness"]["cutoff"] == _at(19)
    assert dashboard["freshness"]["cutoff_source"] == "latest_persisted_session"
    assert dashboard["portfolio"] == {
        "status": "available",
        "reason": None,
        "positions": [],
        "pending_orders": [],
    }


def test_a_frozen_buy_is_shown_with_its_evidence_and_pending_order(book: Book) -> None:
    dashboard = _dashboard(book.history().run(25))

    research = dashboard["research"]
    assert (research["status"], research["action"], research["decision_instant"]) == (
        "available",
        "BUY",
        _at(25),
    )
    assert research["signals"] == ["strong bullish"]
    assert (research["strategy"], research["policy"]) == ("alpha", "built-in directional MVP")
    evidence = research["evidence"]
    assert evidence["observed_at"] == _at(25)
    assert (evidence["latest_quote"], evidence["previous_close"]) == ("7610", "7609")
    assert len(evidence["recent_closes"]) == len(evidence["recent_volumes"]) == 20
    assert dashboard["market"] == {
        "status": "available",
        "reason": None,
        "session_instant": _at(25),
        "open": "7610",
        "high": "7612",
        "low": "7608",
        "close": "7610",
        "volume": "1000",
    }
    paper = dashboard["paper"]
    assert (paper["target"], paper["order_state"], paper["decision_instant"]) == (
        "1",
        "pending",
        _at(25),
    )
    order = paper["order"]
    assert (order["state"], order["side"], order["contracts"]) == ("pending", "BUY", "1")
    assert order["decided_at"] == _at(25) and len(order["order_identity"]) == 64
    assert order["simulated_fill_price"] is None and order["fill_observable_from"] is None
    assert dashboard["portfolio"]["pending_orders"][0]["order_identity"] == order["order_identity"]
    assert dashboard["pnl"] == {
        "status": "available",
        "reason": None,
        "missing_product": None,
        "rows": [],
    }


def test_the_next_session_shows_the_open_fill_and_close_mark(book: Book) -> None:
    dashboard = _dashboard(book.economics().history().daily(26))

    assert dashboard["paper"]["order_state"] == "no_order"
    latest, earlier = dashboard["recent_decisions"]
    assert (latest["decision_instant"], latest["order_state"], latest["order"]) == (
        _at(26),
        "no_order",
        None,
    )
    assert earlier["order"] == {
        "state": "filled",
        "side": "BUY",
        "contracts": "1",
        "order_identity": earlier["order"]["order_identity"],
        "decided_at": _at(25),
        "simulated_fill_price": "7650",
        "price_basis": "OPEN of next synced session",
        "fill_observable_from": _at(26),
    }
    assert dashboard["portfolio"]["positions"] == [
        {
            "contract": {"product": "ES", "exchange": "CME", "expiration": "2026-12-18"},
            "selected_contract": True,
            "direction": "LONG",
            "net_contracts": "1",
            "average_entry": "7650",
        }
    ]
    assert _row(dashboard) == {
        "contract": {"product": "ES", "exchange": "CME", "expiration": "2026-12-18"},
        "settlement_currency": "USD",
        "realized_pnl": "0",
        "position": "open",
        "mark_quote": "7611",
        "mark_instant": _at(26),
        "unrealized_pnl": "-1950",
        "unrealized_reason": None,
    }
    assert "executed_at" not in json.dumps(dashboard)


@pytest.mark.parametrize(("session", "action"), [(27, "SELL"), (29, "HOLD")])
def test_sell_and_hold_come_from_the_frozen_record(book: Book, session: int, action: str) -> None:
    book.economics().history().daily(29)

    dashboard = _dashboard(book, f"as_of={_at(session)}")

    assert dashboard["research"]["action"] == action
    assert dashboard["freshness"]["cutoff_source"] == "requested"


def test_a_short_after_the_reversal(book: Book) -> None:
    book.economics().history().daily(29)

    at_d2 = _dashboard(book, f"as_of={_at(27)}")
    short = _dashboard(book, f"as_of={_at(28)}")

    assert (at_d2["paper"]["order"]["side"], at_d2["paper"]["order"]["contracts"]) == ("SELL", "2")
    assert at_d2["paper"]["order_state"] == "pending"
    (position,) = short["portfolio"]["positions"]
    assert (position["direction"], position["net_contracts"], position["average_entry"]) == (
        "SHORT",
        "1",
        "2950",
    )
    row = _row(short)
    assert (row["realized_pnl"], row["mark_quote"], row["unrealized_pnl"]) == (
        "-235000",
        "2900",
        "2500",
    )


def test_the_default_cutoff_is_the_latest_persisted_session(book: Book) -> None:
    book.economics().history().daily(29)

    dashboard = _dashboard(book)

    assert dashboard["freshness"] == {
        "cutoff": _at(29),
        "cutoff_source": "latest_persisted_session",
        "latest_market_session": _at(29),
        "latest_decision_instant": _at(29),
    }
    assert dashboard["research"]["action"] == "HOLD"
    assert len(dashboard["recent_decisions"]) == 5


def test_an_explicit_earlier_cutoff_excludes_later_facts(book: Book) -> None:
    book.economics().history().daily(26)

    dashboard = _dashboard(book, f"as_of={_at(25)}")

    assert dashboard["research"]["decision_instant"] == _at(25)
    assert [d["decision_instant"] for d in dashboard["recent_decisions"]] == [_at(25)]
    assert dashboard["paper"]["order_state"] == "pending"
    assert dashboard["portfolio"]["positions"] == []
    assert dashboard["market"]["session_instant"] == _at(25)


def test_offset_cutoffs_are_returned_canonically(book: Book) -> None:
    book.history().run(25)
    next_day = (_DATES[24] + timedelta(days=1)).isoformat()

    dashboard = _dashboard(book, f"as_of={next_day}T02:30:00%2B05:30")

    assert dashboard["freshness"]["cutoff"] == _at(25)
    assert dashboard["research"]["decision_instant"] == _at(25)


# ---------------------------------------------------------------------------
# Research versus execution, portfolio scope, P&L
# ---------------------------------------------------------------------------


def test_research_buy_is_never_shown_as_the_executed_sell(book: Book) -> None:
    book.history().trade("seed-long-3", _ES_DEC, OrderSide.BUY, 3, "7600").run(25)

    dashboard = _dashboard(book.economics())

    assert dashboard["research"]["action"] == "BUY"
    assert (dashboard["paper"]["order"]["side"], dashboard["paper"]["order"]["contracts"]) == (
        "SELL",
        "2",
    )
    assert dashboard["recent_decisions"][0]["action"] == "BUY"
    assert dashboard["recent_decisions"][0]["order"]["side"] == "SELL"


def test_the_whole_portfolio_is_shown_across_contracts_and_currencies(book: Book) -> None:
    book.economics().economics("FESX", "EUREX", "10", "EUR").history().run(25)
    book.trade("fesx-long", _FESX_DEC, OrderSide.BUY, 2, "5000")
    book.trade("mar-pending", _ES_MAR, OrderSide.SELL, 1, None, decided=3)
    book.bars(_LATER[26]).run(26)

    dashboard = _dashboard(book)

    positions = {
        (p["contract"]["product"], p["contract"]["expiration"]): p
        for p in dashboard["portfolio"]["positions"]
    }
    assert positions[("ES", "2026-12-18")]["selected_contract"] is True
    assert positions[("FESX", "2026-12-18")]["selected_contract"] is False
    assert [o["contract"]["expiration"] for o in dashboard["portfolio"]["pending_orders"]] == [
        "2027-03-19"
    ]
    fesx = _row(dashboard, product="FESX")
    assert (fesx["settlement_currency"], fesx["realized_pnl"]) == ("EUR", "0")
    assert (fesx["mark_quote"], fesx["unrealized_pnl"]) == (None, None)
    assert fesx["unrealized_reason"] == "no synced daily close observable by cutoff"
    assert _row(dashboard)["settlement_currency"] == "USD"
    assert "total" not in json.dumps(dashboard).lower()


def test_missing_economics_keeps_every_other_section(book: Book) -> None:
    dashboard = _dashboard(book.history().daily(26))

    assert dashboard["pnl"] == {
        "status": "unavailable",
        "reason": "product economics not configured",
        "missing_product": "ES@CME",
        "rows": [],
    }
    assert dashboard["research"]["action"] == "BUY"
    assert dashboard["portfolio"]["positions"][0]["average_entry"] == "7650"
    assert dashboard["recent_decisions"][1]["order"]["state"] == "filled"


def test_high_precision_values_stay_exact_strings(book: Book) -> None:
    book.history(1).trade("a", _ES_DEC, OrderSide.BUY, 2, "100").trade(
        "b", _ES_DEC, OrderSide.BUY, 1, "102", filled=3
    )

    response = _get(_app(book), "/futures/dashboard", f"as_of={_at(3)}")

    assert b'"average_entry":"100.6666666666666666666666667"' in response.body
    assert b'"net_contracts":"3"' in response.body


# ---------------------------------------------------------------------------
# Read-only, restart, no recomputation, errors
# ---------------------------------------------------------------------------


def test_reading_writes_nothing_and_restart_reproduces_the_response(book: Book) -> None:
    book.economics().history().daily(28)
    app = _app(book)
    before = book.dump()

    first = _get(app, "/futures/dashboard")
    second = _get(app, "/futures/dashboard")
    _get(app, "/health")

    assert book.dump() == before
    assert first.body == second.body
    assert _get(_app(book), "/futures/dashboard").body == first.body


def test_a_refresh_never_recomputes_or_executes(book: Book, monkeypatch) -> None:
    book.economics().history().daily(26)
    app = _app(book)

    def forbidden(*args, **kwargs):
        raise AssertionError("a dashboard read must never analyse, freeze or execute")

    monkeypatch.setattr(FuturesAssetAnalysisGenerator, "generate", forbidden)
    monkeypatch.setattr(Strategy, "evaluate_futures", forbidden)
    monkeypatch.setattr(FreezeFuturesForwardResearchDecisionUseCase, "execute", forbidden)
    monkeypatch.setattr(RunFuturesPaperTradingSessionUseCase, "execute", forbidden)

    response = _get(app, "/futures/dashboard")

    assert response.status == 200
    assert response.json["research"]["action"] == "BUY"


@pytest.mark.parametrize(
    "as_of", ["2026-07-06T21:00:00", "2026-07-06", "yesterday", "2026-13-01T00:00:00Z"]
)
def test_an_invalid_as_of_is_422(book: Book, as_of: str) -> None:
    response = _get(_app(book), "/futures/dashboard", f"as_of={as_of}")

    assert response.status == 422
    assert response.json == {
        "detail": "as_of must be an ISO-8601 timestamp with an explicit offset."
    }


def test_a_mixed_strategy_portfolio_is_a_generic_500(book: Book) -> None:
    book.history().run(25)

    response = _get(_app(book, strategy="beta"), "/futures/dashboard")

    assert (response.status, response.json) == (500, {"detail": _GENERIC_500})
    assert b"Error" not in response.body and b"Traceback" not in response.body


def test_corrupt_storage_is_a_generic_500(book: Book) -> None:
    book.economics().history().daily(26)
    with closing(sqlite3.connect(book.path)) as connection:
        connection.execute("UPDATE futures_product_economics SET point_value_amount = '0'")
        connection.commit()

    response = _get(_app(book), "/futures/dashboard")

    assert (response.status, response.json) == (500, {"detail": _GENERIC_500})
    assert _get(_app(book), "/health").status == 503
