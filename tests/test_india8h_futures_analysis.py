"""INDIA-8H-B: the read-only GET /futures/analysis read model.

Facts come from INDIA-7's operator workflow on temporary SQLite (production CLI,
real NSE sessions, the range-honouring Upstox transport double); the CME case
uses the dashboard tests' production book. Expected amounts are hand-derived
from the INDIA-7 synthetic markets (65 INR per point; each bar opens three
points above the previous close). The network is refused throughout.
"""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import FuturesExpiryFlattenGuard
from northstar_application.ports import FuturesSessionResolutionError
from northstar_core.derivatives import QuoteValue
from northstar_core.foundation.value_objects import PointInTime, Quantity, Timeframe
from northstar_core.futures import FuturesOHLCVBar
from northstar_core.paper_trading import FuturesContractCount, PaperPortfolioIdentity
from northstar_core.strategy import StrategyIdentity
from northstar_infrastructure.market_data import (
    SQLiteFuturesHistoricalMarketDataStore,
    UpstoxFuturesNativeDailyMarketDataSource,
)
from test_futures_dashboard import _ES_DEC, _get
from test_futures_dashboard import Book as CmeBook
from test_india7_nifty_incremental_operations_acceptance import (
    _CONTRACT,
    _E8_BAR,
    _close,
    _operator,
)
from test_india8d_operational_status import (
    _ORIGIN,
    _app,
    _cycled,
    _dump,
    _expiry_cycled,
    no_network,  # noqa: F401 - module-scoped autouse fixture
)

from northstar_api import futures_analysis
from northstar_api.app import create_app
from northstar_api.operations_lock import DatabaseOperationsLock
from northstar_api.runtime import build_database_runtime
from northstar_api.settings import DashboardSettings

_E7, _E6, _E5 = _E8_BAR + 1, _E8_BAR + 2, _E8_BAR + 3


def _analysis(database: Path, query: str = "", **settings) -> dict:
    response = _get(_app(database, **settings), "/futures/analysis", query)
    assert response.status == 200, response.body
    return response.json


def _money(amount: str) -> dict:
    return {"amount": amount, "currency": "INR"}


def _seconds(start: str, end: str) -> str:
    def parse(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    return str(int((parse(end) - parse(start)).total_seconds()))


# ---------------------------------------------------------------------------
# Populated NIFTY analysis (normal INDIA-7 path through bar 26)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def operated(tmp_path_factory) -> Path:
    return _cycled(tmp_path_factory.mktemp("operated"), "operated", through=26).database


def test_context_and_decisions(operated: Path) -> None:
    analysis = _analysis(operated)

    assert analysis["context"] == {
        "contract": {"product": "NIFTY", "exchange": "NSE", "expiration": "2026-10-27"},
        "strategy": "directional-mvp-v1-nifty-paper-ops",
        "portfolio": "nifty-paper-ops-oct26",
        "cutoff": _close(26),
        "cutoff_source": "latest_persisted_session",
        "horizons": ["1", "5"],
    }
    paper = analysis["paper"]
    assert paper["status"] == "available"
    # 21 HOLD, 22 BUY, 23 BUY, 24 SELL, 25 HOLD, 26 HOLD
    assert paper["decisions"] == {"total": "6", "buy": "2", "sell": "1", "hold": "3"}
    assert paper["execution"] == {
        "order_count": "2",
        "fill_count": "2",
        "contracts_bought": "1",
        "contracts_sold": "2",
        "turnover_contracts": "3",
        "net_contracts": "-1",
    }


def test_completed_trade_and_canonical_portfolio(operated: Path) -> None:
    paper = _analysis(operated)["paper"]

    (trade,) = paper["trades"]["completed"]
    assert trade == {
        "direction": "LONG",
        "opened_at": _close(23),
        "closed_at": _close(25),
        "contracts": "1",
        "closing_average_entry": "25103",
        "average_exit": "24003",
        "realized_pnl": _money("-71500"),  # (24003 - 25103) * 65
        "holding_seconds": _seconds(_close(23), _close(25)),
    }
    stats = paper["trades"]["statistics"]
    assert (stats["completed_count"], stats["winning_count"]) == ("1", "0")
    assert (stats["losing_count"], stats["breakeven_count"]) == ("1", "0")
    assert stats["win_rate"] == "0"
    assert stats["completed_realized_pnl"] == _money("-71500")
    assert stats["average_trade_pnl"] == _money("-71500")
    assert (stats["gross_profit"], stats["gross_loss"]) == (_money("0"), _money("71500"))
    assert (stats["profit_factor"], stats["profit_factor_unavailable_reason"]) == ("0", None)
    assert stats["median_holding_seconds"] == trade["holding_seconds"]

    portfolio = paper["portfolio"]
    assert portfolio["canonical_realized_pnl"] == _money("-71500")
    assert portfolio["unrealized_pnl"] == _money("195")  # (24000 - 24003) * -1 * 65
    assert portfolio["open_exposure"] == {
        "direction": "SHORT",
        "net_contracts": "-1",
        "average_entry": "24003",
        "opened_at": _close(25),
        "realized_pnl": _money("0"),
        "mark_quote": "24000",
        "mark_instant": _close(26),
        "unrealized_pnl": _money("195"),
    }


def test_equity_curve_and_drawdown(operated: Path) -> None:
    analysis = _analysis(operated)

    curve = analysis["equity_curve"]
    assert len(curve) == 26 and all(p["total_pnl"] == _money("0") for p in curve[:22])
    assert [(p["instant"], p["net_contracts"], p["total_pnl"]["amount"]) for p in curve[22:]] == [
        (_close(23), "1", "6305"),  # (25200 - 25103) * 65
        (_close(24), "1", "-71695"),  # (24000 - 25103) * 65
        (_close(25), "-1", "-71305"),  # -71500 realized + 195
        (_close(26), "-1", "-71305"),
    ]
    assert curve[24]["realized_pnl"] == _money("-71500")
    assert curve[24]["unrealized_pnl"] == _money("195")
    assert analysis["paper"]["drawdown"] == {
        "amount": _money("78000"),
        "peak_pnl": _money("6305"),
        "peak_instant": _close(23),
        "trough_pnl": _money("-71695"),
        "trough_instant": _close(24),
    }


def test_research_horizons_and_actions(operated: Path) -> None:
    research = _analysis(operated)["research"]

    assert research["status"] == "available"
    assert research["decision_count"] == "7"  # bars 20..26 have the warm-up
    h1, h5 = research["horizons"]
    assert (h1["horizon"], h5["horizon"]) == ("1", "5")
    assert h1["total_count"] == h5["total_count"] == "7"
    assert h1["insufficient_future_observations_count"] == "1"
    assert h5["insufficient_future_observations_count"] == "5"
    assert h5["measured_count"] == "2"
    assert h1["undefined_return_basis_count"] == "0"

    by_action = {group["action"]: group for group in research["by_action"]}
    assert list(by_action) == ["BUY", "SELL", "HOLD"]
    assert sum(int(group["decision_count"]) for group in by_action.values()) == 7
    assert (by_action["BUY"]["decision_count"], by_action["SELL"]["decision_count"]) == ("2", "1")
    for group in by_action.values():
        assert [h["horizon"] for h in group["horizons"]] == ["1", "5"]
        assert all(h["total_count"] == group["decision_count"] for h in group["horizons"])


def test_research_matches_the_application_analysis(operated: Path) -> None:
    from northstar_api.runtime import build_futures_analysis

    runtime = build_database_runtime(operated)
    expected = build_futures_analysis(runtime).execute(
        _CONTRACT,
        StrategyIdentity("directional-mvp-v1-nifty-paper-ops"),
        PaperPortfolioIdentity("nifty-paper-ops-oct26"),
        futures_analysis.ANALYSIS_HORIZONS,
        PointInTime(_close(26)),
    )

    research = _analysis(operated)["research"]
    for metrics, response in zip(expected.research_metrics, research["horizons"], strict=True):
        average = metrics.average_forward_return
        assert response["average_forward_return"] == (str(average.value) if average else None)
        assert response["measured_count"] == str(metrics.measured_count)
    sell = expected.action_metrics[1].horizons[0]
    assert research["by_action"][1]["horizons"][0]["minimum_forward_return"] == (
        str(sell.minimum_forward_return.value) if sell.minimum_forward_return else None
    )


def test_numbers_are_exact_strings(operated: Path) -> None:
    body = json.dumps(_analysis(operated))

    assert "-71500" in body
    for value in json.loads(body)["equity_curve"]:
        assert isinstance(value["total_pnl"]["amount"], str)
    assert not any(word in body.lower() for word in ("profitable", "good", "bad", "optimi"))


# ---------------------------------------------------------------------------
# Empty and partial states
# ---------------------------------------------------------------------------


def test_no_decisions_yet(tmp_path: Path) -> None:
    op = _operator(tmp_path, "no-decisions")
    op.bootstrap(20)

    analysis = _analysis(op.database)

    paper = analysis["paper"]
    assert paper["decisions"]["total"] == "0" and paper["execution"]["order_count"] == "0"
    stats = paper["trades"]["statistics"]
    assert stats["completed_count"] == "0" and paper["trades"]["completed"] == []
    assert stats["win_rate"] is None and stats["average_trade_pnl"] is None
    assert stats["profit_factor"] is None
    assert stats["profit_factor_unavailable_reason"] == "NO_COMPLETED_TRADES"
    assert stats["minimum_holding_seconds"] is None
    assert paper["portfolio"]["open_exposure"] is None
    assert paper["portfolio"]["unrealized_pnl"] == _money("0")
    assert paper["drawdown"]["amount"] == _money("0")
    assert analysis["research"]["decision_count"] == "1"
    assert analysis["expiry"]["decision_count"] == "0"


def test_open_position_only(tmp_path: Path) -> None:
    database = _cycled(tmp_path, "open-only", through=23).database

    paper = _analysis(database)["paper"]

    assert paper["trades"]["completed"] == []
    assert paper["trades"]["statistics"]["win_rate"] is None
    exposure = paper["portfolio"]["open_exposure"]
    assert (exposure["direction"], exposure["net_contracts"]) == ("LONG", "1")
    assert exposure["unrealized_pnl"] == _money("6305")


def test_winning_trade_without_losses_has_no_profit_factor(tmp_path: Path) -> None:
    database = _expiry_cycled(tmp_path, "winner", _E5).database

    stats = _analysis(database)["paper"]["trades"]["statistics"]

    assert (stats["winning_count"], stats["losing_count"]) == ("1", "0")
    assert stats["win_rate"] == "1"
    assert stats["completed_realized_pnl"] == _money("6500")  # (25203 - 25103) * 65
    assert stats["profit_factor"] is None
    assert stats["profit_factor_unavailable_reason"] == "NO_LOSING_TRADES"


def test_no_bars_is_unavailable_but_readable(tmp_path: Path) -> None:
    analysis = _analysis(tmp_path / "empty.sqlite3")

    assert analysis["context"]["cutoff"] is None
    assert analysis["context"]["cutoff_source"] == "none"
    assert analysis["paper"]["status"] == "unavailable"
    assert analysis["research"]["status"] == "unavailable"
    assert analysis["expiry"]["status"] == "unavailable"
    assert analysis["equity_curve"] == []


def test_an_invalid_as_of_is_422(operated: Path) -> None:
    response = _get(_app(operated), "/futures/analysis", "as_of=yesterday")

    assert response.status == 422


# ---------------------------------------------------------------------------
# Expiry governance
# ---------------------------------------------------------------------------


def test_expiry_classification_of_frozen_decisions(tmp_path: Path) -> None:
    database = _expiry_cycled(tmp_path, "expiry", _E5).database

    expiry = _analysis(database)["expiry"]

    # E-8 and E-7 outside, E-6 the flatten decision, E-5 protected.
    assert expiry == {
        "status": "available",
        "reason": None,
        "decision_count": "4",
        "outside": "2",
        "flatten": "1",
        "protected": "1",
        "unresolved": "0",
        "unresolved_reason": None,
    }


def test_an_unresolvable_calendar_is_never_outside(tmp_path: Path, monkeypatch) -> None:
    database = _expiry_cycled(tmp_path, "unresolved", _E7).database

    class FailClosed(FuturesExpiryFlattenGuard):
        def __init__(self) -> None:
            pass

        def assess(self, contract, decision_instant):
            raise FuturesSessionResolutionError("NSE calendar not loaded for that date")

    monkeypatch.setattr(futures_analysis, "expiry_guard_for", lambda contract: FailClosed())

    expiry = _analysis(database)["expiry"]

    assert (expiry["decision_count"], expiry["unresolved"]) == ("2", "2")
    assert (expiry["outside"], expiry["flatten"], expiry["protected"]) == ("0", "0", "0")
    assert expiry["unresolved_reason"] == "NSE calendar not loaded for that date"


# ---------------------------------------------------------------------------
# Cutoff, read-only, provider-free
# ---------------------------------------------------------------------------


def test_an_earlier_cutoff_ignores_later_and_poison_facts(tmp_path: Path) -> None:
    op = _cycled(tmp_path, "cutoff", through=24)
    query = f"as_of={_close(24)}"
    before = _get(_app(op.database), "/futures/analysis", query).body

    for bar in (25, 26):
        op.cycle(bar)
    poison = FuturesOHLCVBar(
        contract=_CONTRACT,
        point_in_time=PointInTime("2026-09-30T10:00:00Z"),
        timeframe=Timeframe("1d"),
        open=QuoteValue(Decimal(1)),
        high=QuoteValue(Decimal(99999)),
        low=QuoteValue(Decimal(1)),
        close=QuoteValue(Decimal(99999)),
        volume=Quantity(Decimal(1)),
    )
    SQLiteFuturesHistoricalMarketDataStore(op.database).store((poison,))

    after = _get(_app(op.database), "/futures/analysis", query)
    assert after.status == 200 and after.body == before
    assert json.loads(after.body)["context"]["cutoff_source"] == "requested"


def test_reading_makes_no_provider_request_and_mutates_nothing(operated: Path, monkeypatch) -> None:
    def forbidden(*args, **kwargs):
        raise AssertionError("the analysis must never build a provider adapter")

    monkeypatch.setattr(UpstoxFuturesNativeDailyMarketDataSource, "__init__", forbidden)
    app = _app(operated)
    before = _dump(operated)

    first = _get(app, "/futures/analysis")
    second = _get(app, "/futures/analysis")

    assert first.status == 200 and first.body == second.body
    assert _dump(operated) == before


def test_reading_while_the_operations_lock_is_held(operated: Path) -> None:
    app = _app(operated)
    expected = _get(app, "/futures/analysis").body

    with DatabaseOperationsLock(operated):
        assert _get(app, "/futures/analysis").body == expected


# ---------------------------------------------------------------------------
# CME
# ---------------------------------------------------------------------------


def test_cme_analysis_works_without_expiry_classification(tmp_path: Path) -> None:
    book = CmeBook(tmp_path / "cme.sqlite3").economics().history().daily(28)
    settings = DashboardSettings(
        database=book.path,
        web_origin=_ORIGIN,
        contract=_ES_DEC,
        strategy=StrategyIdentity("alpha"),
        portfolio=PaperPortfolioIdentity("futures-paper-alpha"),
        target=FuturesContractCount(1),
    )
    app = create_app(settings, runtime=build_database_runtime(book.path))

    analysis = _get(app, "/futures/analysis").json

    assert analysis["paper"]["status"] == "available"
    assert analysis["paper"]["portfolio"]["canonical_realized_pnl"]["currency"] == "USD"
    assert int(analysis["paper"]["decisions"]["total"]) > 0
    assert analysis["research"]["status"] == "available"
    assert analysis["expiry"]["status"] == "not_applicable"
    assert _get(app, "/futures/dashboard").status == 200


def test_missing_economics_keeps_research(tmp_path: Path) -> None:
    book = CmeBook(tmp_path / "no-economics.sqlite3").history(25)
    settings = DashboardSettings(
        database=book.path,
        web_origin=_ORIGIN,
        contract=_ES_DEC,
        strategy=StrategyIdentity("alpha"),
        portfolio=PaperPortfolioIdentity("futures-paper-alpha"),
        target=FuturesContractCount(1),
    )
    app = create_app(settings, runtime=build_database_runtime(book.path))

    analysis = _get(app, "/futures/analysis").json

    assert analysis["paper"]["status"] == "unavailable"
    assert analysis["paper"]["reason"] == "contract economics not configured"
    assert analysis["paper"]["missing_contract"]["expiration"] == "2026-12-18"
    assert analysis["research"]["status"] == "available"
    assert analysis["equity_curve"] == []
