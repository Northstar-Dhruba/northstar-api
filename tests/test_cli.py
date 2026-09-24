"""Tests for the operational futures paper-trading CLI.

Paper commands run against real temporary SQLite files through the production
runtime; the provider is never contacted. Rendering and error paths that real
data cannot reach cheaply use lightweight runtime doubles carrying real Core
and Application values.
"""

from __future__ import annotations

import dataclasses
import io
import tomllib
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    FuturesContractPnl,
    FuturesDailyAcquisitionResult,
    FuturesPaperTradingSnapshot,
    FuturesPaperTradingValuation,
)
from northstar_application.ports import (
    FuturesDailyHistoricalAcquisitionQuery,
    FuturesTradingSession,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    Money,
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
    FuturesPaperPortfolio,
    FuturesPosition,
    OrderSide,
    PaperFillIdentity,
    PaperOrderIdentity,
    PaperPortfolioIdentity,
)
from northstar_core.strategy import StrategyIdentity
from northstar_infrastructure.market_data import (
    DatabentoFuturesHistoricalMarketDataSourceError,
    FuturesTradingSessionInProgressError,
    SQLiteFuturesHistoricalMarketDataStore,
)

from northstar_api import cli
from northstar_api.cli import ExitCode, main
from northstar_api.runtime import DatabaseRuntime, build_database_runtime

_SECRET = "db-SECRET-NEVER-PRINTED-0123456789"
_ES_DEC = FuturesContract(
    FuturesProductReference(Symbol("ES"), ExchangeCode("CME")), ExpirationDate("2026-12-18")
)
_ES_MAR = FuturesContract(_ES_DEC.product, ExpirationDate("2027-03-19"))
_FESX_DEC = FuturesContract(
    FuturesProductReference(Symbol("FESX"), ExchangeCode("EUREX")), ExpirationDate("2026-12-18")
)
_DAILY = Timeframe("1d")
_ALPHA = StrategyIdentity("alpha")
_PORTFOLIO = PaperPortfolioIdentity("futures-paper-alpha")


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


def _bar(session: int, close: str, *, open_: str | None = None) -> FuturesOHLCVBar:
    closing = Decimal(close)
    opening = Decimal(open_) if open_ is not None else closing
    return FuturesOHLCVBar(
        contract=_ES_DEC,
        point_in_time=PointInTime(_at(session)),
        timeframe=_DAILY,
        open=QuoteValue(opening),
        high=QuoteValue(max(opening, closing) + 50),
        low=QuoteValue(min(opening, closing) - 50),
        close=QuoteValue(closing),
        volume=Quantity(Decimal("1000")),
    )


# Fifteen flat closes then a steady rise: session 25 decides BUY.
_RISE = ["7600"] * 15 + [str(7601 + index) for index in range(10)]
_S26 = _bar(26, "7611", open_="7650")


@dataclasses.dataclass
class Outcome:
    code: int
    out: str
    err: str


def _cli(argv: list[str], *, env: dict | None = None, **kwargs) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    code = main(argv, env=env if env is not None else {}, stdout=out, stderr=err, **kwargs)
    return Outcome(code, out.getvalue(), err.getvalue())


def _assert_clean(outcome: Outcome) -> None:
    assert "Traceback" not in outcome.out + outcome.err
    assert _SECRET not in outcome.out + outcome.err
    assert "Executed at" not in outcome.out


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


def _store_bars(database: Path, *bars: FuturesOHLCVBar) -> None:
    build_database_runtime(database)
    SQLiteFuturesHistoricalMarketDataStore(database).store(bars)


def _phase_a(database: Path, sessions: int = 25) -> None:
    _store_bars(database, *(_bar(n, close) for n, close in enumerate(_RISE[:sessions], start=1)))


def _set_economics(database: Path, point_value: str = "50", currency: str = "USD") -> Outcome:
    return _cli(
        [
            "economics", "set", "--database", str(database), "--product", "ES",
            "--exchange", "CME", "--point-value", point_value, "--currency", currency,
        ]
    )  # fmt: skip


def _paper_run(database: Path, as_of: str, *, target: str = "1", **kwargs) -> Outcome:
    return _cli(
        [
            "paper", "run", "--database", str(database), "--product", "ES",
            "--exchange", "CME", "--expiration", "2026-12-18", "--strategy", "alpha",
            "--portfolio", "futures-paper-alpha", "--target", target, "--as-of", as_of,
        ],
        **kwargs,
    )  # fmt: skip


def _paper_status(database: Path, as_of: str, *, strategy: str = "alpha", **kwargs) -> Outcome:
    return _cli(
        [
            "paper", "status", "--database", str(database), "--strategy", strategy,
            "--portfolio", "futures-paper-alpha", "--as-of", as_of,
        ],
        **kwargs,
    )  # fmt: skip


# ---------------------------------------------------------------------------
# Command tree and packaging
# ---------------------------------------------------------------------------


def test_top_level_help_lists_the_three_groups() -> None:
    outcome = _cli(["--help"])

    assert outcome.code == 0
    for group in ("economics", "market-data", "paper"):
        assert group in outcome.out


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["economics", "set", "--help"], ["--database", "--point-value", "--currency"]),
        (["economics", "show", "--help"], ["--database", "--product", "--exchange"]),
        (["market-data", "sync", "--help"], ["--expiration", "--start", "--end"]),
        (["paper", "run", "--help"], ["--target", "--as-of", "--strategy", "--portfolio"]),
        (["paper", "status", "--help"], ["--as-of", "--strategy", "--portfolio"]),
    ],
)
def test_nested_help_documents_required_arguments(argv, expected) -> None:
    outcome = _cli(argv)

    assert outcome.code == 0
    for option in expected:
        assert option in outcome.out


def test_status_needs_no_contract_or_target() -> None:
    help_text = _cli(["paper", "status", "--help"]).out

    for option in ("--product", "--expiration", "--target"):
        assert option not in help_text


@pytest.mark.parametrize("argv", [[], ["paper"], ["paper", "run"], ["unknown"]])
def test_incomplete_commands_are_input_errors(argv) -> None:
    outcome = _cli(argv)

    assert outcome.code == ExitCode.INPUT
    assert outcome.out == ""
    assert "usage:" in outcome.err


def test_both_console_scripts_are_registered() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    scripts = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]["scripts"]

    assert scripts == {"northstar-api": "northstar_api:main", "northstar": "northstar_api.cli:main"}


def test_the_fastapi_app_does_not_load_the_cli() -> None:
    package = Path(cli.__file__).parent
    for module in ("app.py", "routers/futures.py", "settings.py", "__init__.py"):
        source = (package / module).read_text(encoding="utf-8")
        assert "northstar_api.cli" not in source and "import cli" not in source


# ---------------------------------------------------------------------------
# economics
# ---------------------------------------------------------------------------


def test_economics_set_then_show(database: Path) -> None:
    stored = _set_economics(database)
    shown = _cli(
        ["economics", "show", "--database", str(database), "--product", "ES", "--exchange", "CME"]
    )

    assert stored.code == shown.code == 0
    assert stored.out.splitlines()[:3] == [
        "ECONOMICS: READY",
        "Product: ES@CME",
        "Point value: 50 USD / quote-point / contract",
    ]
    assert shown.out.splitlines() == [
        "Product: ES@CME",
        "Point value: 50 USD / quote-point / contract",
    ]
    assert stored.err == shown.err == ""


def test_an_equal_economics_retry_succeeds_and_a_change_conflicts(database: Path) -> None:
    _set_economics(database)

    retry = _set_economics(database, point_value="50.000")
    conflict = _set_economics(database, point_value="25")

    assert retry.code == 0
    assert conflict.code == ExitCode.STATE
    assert "STATE ERROR: FuturesProductEconomicsConflictError" in conflict.err
    assert conflict.out == ""
    assert (
        "50 USD"
        in _cli(
            [
                "economics",
                "show",
                "--database",
                str(database),
                "--product",
                "ES",
                "--exchange",
                "CME",
            ]
        ).out
    )


def test_high_precision_point_values_are_kept_exactly(database: Path) -> None:
    precise = "12.345678901234567890123456789012345"

    outcome = _set_economics(database, point_value=precise, currency="eur")

    assert f"Point value: {precise} EUR / quote-point / contract" in outcome.out


def test_missing_economics_is_a_data_error(database: Path) -> None:
    outcome = _cli(
        ["economics", "show", "--database", str(database), "--product", "NQ", "--exchange", "CME"]
    )

    assert outcome.code == ExitCode.DATA
    assert "Product economics not configured for NQ@CME" in outcome.err
    assert outcome.out == ""


# ---------------------------------------------------------------------------
# market-data sync
# ---------------------------------------------------------------------------

_SYNC = [
    "market-data", "sync", "--product", "ES", "--exchange", "CME",
    "--expiration", "2026-12-18", "--start", "2026-09-14", "--end", "2026-09-16",
]  # fmt: skip


class StubAcquisition:
    def __init__(self, outcome) -> None:
        self.outcome = outcome
        self.queries: list = []

    def execute(self, query):
        self.queries.append(query)
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome(query)


def _sync(database: Path, outcome, env: dict | None = None) -> tuple[Outcome, list]:
    calls: list = []
    acquisition = StubAcquisition(outcome)

    def runtime(path: Path, api_key: str) -> StubAcquisition:
        calls.append((path, api_key))
        return acquisition

    result = _cli(
        [*_SYNC, "--database", str(database)],
        env={"DATABENTO_API_KEY": _SECRET} if env is None else env,
        market_sync_runtime=runtime,
    )
    return result, calls


def test_sync_without_an_api_key_is_a_configuration_error(database: Path) -> None:
    outcome, calls = _sync(database, lambda q: None, env={})

    assert outcome.code == ExitCode.CONFIGURATION
    assert "DATABENTO_API_KEY is not set" in outcome.err
    assert calls == []


def test_a_completed_sync_reports_the_actual_counts(database: Path) -> None:
    outcome, calls = _sync(database, lambda q: FuturesDailyAcquisitionResult(q, 3, 2))

    assert outcome.code == 0
    assert calls == [(database, _SECRET)]
    assert "Contract: ES@CME 2026-12-18" in outcome.out
    assert "Date range: 2026-09-14 .. 2026-09-16" in outcome.out
    assert "Sessions in range: 3" in outcome.out
    assert "Sessions with a persisted daily bar: 2" in outcome.out
    assert "Sessions without trades: 1" in outcome.out
    _assert_clean(outcome)


def test_an_in_progress_session_is_a_data_error_that_keeps_earlier_sessions(
    database: Path,
) -> None:
    session = FuturesTradingSession(
        date(2026, 9, 16), PointInTime("2026-09-15T22:00:00Z"), PointInTime("2026-09-16T21:00:00Z")
    )
    now = datetime(2026, 9, 16, 15, 0, tzinfo=UTC)

    outcome, _ = _sync(database, FuturesTradingSessionInProgressError(_ES_DEC, session, now))

    assert outcome.code == ExitCode.DATA
    assert "DATA ERROR: Session 2026-09-16 has not completed." in outcome.err
    assert "Session close: 2026-09-16T21:00:00Z" in outcome.err
    assert "Current UTC: 2026-09-16T15:00:00Z" in outcome.err
    assert "Sync stopped at 2026-09-16." in outcome.err
    assert "may already have been stored" in outcome.err
    assert "rolled back" not in outcome.err
    _assert_clean(outcome)


def test_a_provider_failure_is_a_provider_error(database: Path) -> None:
    outcome, _ = _sync(
        database, DatabentoFuturesHistoricalMarketDataSourceError("Databento request failed.")
    )

    assert outcome.code == ExitCode.PROVIDER
    assert "PROVIDER ERROR: DatabentoFuturesHistoricalMarketDataSourceError" in outcome.err
    assert "may already have been stored" in outcome.err
    _assert_clean(outcome)


@pytest.mark.parametrize(
    ("start", "end"),
    [("2026/09/14", "2026-09-16"), ("20260914", "2026-09-16"), ("2026-09-16", "2026-09-14")],
)
def test_malformed_sync_dates_are_input_errors(database: Path, start: str, end: str) -> None:
    argv = [*_SYNC, "--database", str(database)]
    argv[argv.index("--start") + 1], argv[argv.index("--end") + 1] = start, end

    outcome = _cli(argv, env={"DATABENTO_API_KEY": _SECRET})

    assert outcome.code == ExitCode.INPUT


# ---------------------------------------------------------------------------
# paper run against real SQLite
# ---------------------------------------------------------------------------


def test_warm_up_is_reported_not_failed(database: Path) -> None:
    _phase_a(database, sessions=19)

    outcome = _paper_run(database, _at(19))

    assert outcome.code == 0
    assert "Decision: unavailable" in outcome.out
    assert "insufficient persisted daily history / warm-up" in outcome.out
    assert "State: no current decision" in outcome.out
    _assert_clean(outcome)


def test_a_buy_decision_is_pending_with_policy_and_target_disclosures(database: Path) -> None:
    _phase_a(database)

    outcome = _paper_run(database, _at(25))

    assert outcome.code == 0
    lines = outcome.out.splitlines()
    assert lines[0] == "PAPER SESSION: COMPLETED"
    for expected in (
        "Contract: ES@CME 2026-12-18",
        "Strategy: alpha",
        "Policy: built-in directional MVP",
        "Portfolio: futures-paper-alpha",
        "Target: 1",
        f"Cutoff: {_at(25)}",
        "Action: BUY",
        f"Decision instant: {_at(25)}",
        "State: PENDING",
        "Side: BUY",
        "Contracts: 1",
    ):
        assert expected in lines
    assert any(line.startswith("ID: ") for line in lines)
    assert "Flat" in lines
    assert "Target contracts should remain fixed for this paper portfolio." in outcome.err
    assert "Target contracts" not in outcome.out
    _assert_clean(outcome)


def test_the_next_session_shows_the_earlier_order_filled_at_the_open(database: Path) -> None:
    _phase_a(database)
    _set_economics(database)
    _paper_run(database, _at(25))
    _store_bars(database, _S26)

    outcome = _paper_run(database, _at(26))

    assert outcome.code == 0
    assert f"Decision instant: {_at(26)}" in outcome.out
    assert "State: NO ACTION" in outcome.out
    assert "Reason: TARGET ALREADY MET" in outcome.out
    assert f"Decision {_at(25)}:" in outcome.out
    assert "  State: FILLED" in outcome.out
    assert "  Simulated fill price: 7650" in outcome.out
    assert "  Price basis: OPEN of next synced session" in outcome.out
    assert f"  Fill observable from: {_at(26)}" in outcome.out
    assert "ES@CME 2026-12-18 (command contract): LONG 1 @ 7650" in outcome.out
    assert "  Realized P&L: 0 USD" in outcome.out
    assert f"  Mark: 7611 (daily close at {_at(26)})" in outcome.out
    assert "  Unrealized P&L: -1950 USD" in outcome.out
    _assert_clean(outcome)


def test_missing_economics_keeps_the_completed_session_visible(database: Path) -> None:
    _phase_a(database)
    _paper_run(database, _at(25))
    _store_bars(database, _S26)

    outcome = _paper_run(database, _at(26))

    assert outcome.code == ExitCode.DATA
    assert outcome.out.startswith("PAPER SESSION: COMPLETED")
    assert "  State: FILLED" in outcome.out
    assert "LONG 1 @ 7650" in outcome.out
    assert "P&L: unavailable" in outcome.out
    assert "product economics not configured" in outcome.out
    assert "ES@CME" in outcome.out.split("P&L: unavailable")[1]
    assert "completed and its facts were persisted" in outcome.err
    _assert_clean(outcome)


def test_a_repeated_run_is_idempotent(database: Path) -> None:
    _phase_a(database)

    first = _paper_run(database, _at(25))
    second = _paper_run(database, _at(25))

    assert first.code == second.code == 0
    assert first.out == second.out


def test_a_changed_target_is_a_state_conflict(database: Path) -> None:
    _phase_a(database)
    _paper_run(database, _at(25))

    outcome = _paper_run(database, _at(25), target="2")

    assert outcome.code == ExitCode.STATE
    assert "STATE ERROR: FuturesPaperOrderConflictError" in outcome.err
    assert outcome.out == ""


# ---------------------------------------------------------------------------
# Backward-cutoff guard
# ---------------------------------------------------------------------------


class SpySession:
    def __init__(self, real) -> None:
        self.real = real
        self.calls = 0

    def execute(self, *args):
        self.calls += 1
        return self.real.execute(*args)


def _spied(spy_box: list):
    def build(path: Path) -> DatabaseRuntime:
        runtime = build_database_runtime(path)
        spy = SpySession(runtime.paper_session)
        spy_box.append(spy)
        return dataclasses.replace(runtime, paper_session=spy)

    return build


def test_a_run_behind_the_latest_frozen_decision_is_refused(database: Path) -> None:
    _phase_a(database)
    _paper_run(database, _at(25))
    _store_bars(database, _S26)
    _paper_run(database, _at(26))
    spies: list[SpySession] = []

    refused = _paper_run(database, "2026-07-07T20:59:59+00:00", database_runtime=_spied(spies))

    assert _at(26) == "2026-07-07T21:00:00Z"
    assert refused.code == ExitCode.STATE
    assert "Paper run refused: cutoff precedes the latest frozen decision" in refused.err
    assert f"Latest decision: {_at(26)}" in refused.err
    assert "must move forward in time" in refused.err
    assert spies[0].calls == 0
    assert refused.out == ""


@pytest.mark.parametrize(
    "as_of",
    ["2026-07-08T02:30:00+05:30", "2026-07-07T21:00:00Z", "2026-07-09T00:00:00-04:00"],
    ids=["offset-equal", "equal", "later"],
)
def test_equal_and_later_cutoffs_are_allowed(database: Path, as_of: str) -> None:
    _phase_a(database)
    _store_bars(database, _S26)
    _paper_run(database, _at(26))
    spies: list[SpySession] = []

    outcome = _paper_run(database, as_of, database_runtime=_spied(spies))

    assert outcome.code == 0
    assert spies[0].calls == 1


def test_the_guard_ignores_other_strategies(database: Path) -> None:
    _phase_a(database)
    _store_bars(database, _S26)
    _cli(
        [
            "paper", "run", "--database", str(database), "--product", "ES", "--exchange", "CME",
            "--expiration", "2026-12-18", "--strategy", "beta", "--portfolio", "beta-book",
            "--target", "1", "--as-of", _at(26),
        ]
    )  # fmt: skip

    assert _paper_run(database, _at(25)).code == 0


# ---------------------------------------------------------------------------
# paper status
# ---------------------------------------------------------------------------


def test_status_of_an_empty_portfolio(database: Path) -> None:
    outcome = _paper_status(database, _at(25))

    assert outcome.code == 0
    for expected in ("PAPER STATUS", "Orders: 0", "Fills: 0", "Pending: 0", "Flat"):
        assert expected in outcome.out.splitlines()
    assert "No filled contracts" in outcome.out


def test_status_lists_pending_orders_read_only(database: Path) -> None:
    _phase_a(database)
    _paper_run(database, _at(25))
    before = database.read_bytes()

    outcome = _paper_status(database, "2026-08-01T00:00:00+00:00")

    assert outcome.code == 0
    assert "Orders: 1" in outcome.out and "Pending: 1" in outcome.out
    assert f"PENDING ES@CME 2026-12-18: BUY 1 decided {_at(25)} ID " in outcome.out
    assert database.read_bytes() == before


def test_status_can_look_at_an_earlier_cutoff(database: Path) -> None:
    _phase_a(database)
    _set_economics(database)
    _paper_run(database, _at(25))
    _store_bars(database, _S26)
    _paper_run(database, _at(26))

    earlier = _paper_status(database, _at(25))
    later = _paper_status(database, _at(26))

    assert earlier.code == later.code == 0
    assert "Pending: 1" in earlier.out and "Flat" in earlier.out
    assert "Pending: 0" in later.out and "LONG 1 @ 7650" in later.out


def test_status_for_another_strategy_is_a_state_error(database: Path) -> None:
    _phase_a(database)
    _paper_run(database, _at(25))

    outcome = _paper_status(database, _at(25), strategy="beta")

    assert outcome.code == ExitCode.STATE
    assert "STATE ERROR: FuturesPaperPortfolioStrategyConflictError" in outcome.err
    assert "one paper portfolio belongs to one strategy" in outcome.err
    assert outcome.out == ""


class Returning:
    def __init__(self, value) -> None:
        self.value = value

    def execute(self, *args):
        return self.value


def test_status_without_economics_still_shows_the_portfolio(database: Path) -> None:
    _phase_a(database)
    _paper_run(database, _at(25))
    _store_bars(database, _S26)
    _paper_run(database, _at(26))

    outcome = _paper_status(database, _at(26))

    assert outcome.code == ExitCode.DATA
    assert "LONG 1 @ 7650" in outcome.out
    assert "P&L: unavailable" in outcome.out
    assert "Reason: product economics not configured for ES@CME" in outcome.out
    assert outcome.err == (
        "DATA ERROR: P&L unavailable because product economics are not configured.\n"
    )


def _order(order_id: str, contract: FuturesContract, side: OrderSide, contracts: int):
    return FuturesPaperOrder(
        PaperOrderIdentity(order_id),
        FuturesExecutionIntent(
            _PORTFOLIO,
            contract,
            side,
            FuturesContractCount(contracts),
            _ALPHA,
            PointInTime("2026-09-01T21:00:00Z"),
        ),
    )


def _fill(order: FuturesPaperOrder, quote: str) -> FuturesPaperFill:
    return FuturesPaperFill(
        PaperFillIdentity(f"fill-{order.identity.identity}"),
        order.identity,
        order.intent,
        order.intent.contracts,
        QuoteValue(Decimal(quote)),
        PointInTime("2026-09-02T21:00:00Z"),
    )


def test_status_renders_every_row_state_without_totals() -> None:
    as_of = PointInTime("2026-09-10T21:00:00Z")
    dec_long = FuturesPosition(_ES_DEC, 2, QuoteValue(Decimal("100")))
    mar_short = FuturesPosition(_ES_MAR, -1, QuoteValue(Decimal("200")))
    portfolio = FuturesPaperPortfolio(_PORTFOLIO, _ALPHA, (dec_long, mar_short), as_of)
    usd = Currency("USD")
    rows = (
        FuturesContractPnl(
            _ES_DEC,
            Money(Decimal("0"), usd),
            dec_long,
            QuoteValue(Decimal("110")),
            PointInTime("2026-09-09T21:00:00Z"),
            Money(Decimal("1000"), usd),
        ),
        FuturesContractPnl(_ES_MAR, Money(Decimal("0"), usd), mar_short, None, None, None),
        FuturesContractPnl(
            _FESX_DEC,
            Money(Decimal("250"), Currency("EUR")),
            None,
            None,
            None,
            Money(Decimal("0"), Currency("EUR")),
        ),
    )
    valuation = FuturesPaperTradingValuation(_PORTFOLIO, _ALPHA, as_of, portfolio, rows)
    orders = (
        _order("o-1", _ES_DEC, OrderSide.BUY, 2),
        _order("o-2", _ES_MAR, OrderSide.SELL, 1),
        _order("o-3", _FESX_DEC, OrderSide.BUY, 1),
    )
    fills = (_fill(orders[0], "100"),)
    snapshot = FuturesPaperTradingSnapshot(
        contract=None,
        portfolio=portfolio,
        latest_market_bar=None,
        recent_decisions=(),
        orders=orders,
        fills=fills,
        valuation=valuation,
        missing_economics=None,
    )
    runtime = DatabaseRuntime(
        economics_store=None,
        economics_repository=None,
        market_repository=None,
        forward_repository=None,
        order_repository=None,
        fill_repository=None,
        paper_session=None,
        valuation=None,
        snapshot=Returning(snapshot),
    )

    outcome = _paper_status(Path("unused.sqlite3"), as_of.value, database_runtime=lambda p: runtime)

    assert outcome.code == 0
    text = outcome.out
    assert "Orders: 3" in text and "Fills: 1" in text and "Pending: 2" in text
    assert "ES@CME 2026-12-18: LONG 2 @ 100" in text
    assert "ES@CME 2027-03-19: SHORT 1 @ 200" in text
    assert "  Mark: 110 (daily close at 2026-09-09T21:00:00Z)" in text
    assert "  Unrealized P&L: 1000 USD" in text
    assert "  Unrealized P&L: unavailable" in text
    assert "  Reason: no synced daily close observable by cutoff" in text
    assert "  Realized P&L: 250 EUR" in text
    assert "  Unrealized P&L: 0 EUR (no open position)" in text
    assert "total" not in text.lower()


# ---------------------------------------------------------------------------
# Input, configuration, internal errors and secrets
# ---------------------------------------------------------------------------

_BASE_RUN = {
    "--product": "ES",
    "--exchange": "CME",
    "--expiration": "2026-12-18",
    "--strategy": "alpha",
    "--portfolio": "p",
    "--target": "1",
    "--as-of": "2026-09-24T21:00:00+00:00",
}


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--product", ""),
        ("--exchange", ""),
        ("--expiration", "2026-13-01"),
        ("--expiration", "18-12-2026"),
        ("--target", "0"),
        ("--target", "1.5"),
        ("--target", "+1"),
        ("--target", "abc"),
        ("--as-of", "2026-09-24T21:00:00"),
        ("--as-of", "2026-09-24"),
        ("--as-of", "yesterday"),
        ("--strategy", ""),
    ],
)
def test_invalid_run_arguments_are_input_errors(database: Path, option: str, value: str) -> None:
    arguments = {**_BASE_RUN, option: value}
    argv = ["paper", "run", "--database", str(database)]
    for key, text in arguments.items():
        argv += [key, text]

    outcome = _cli(argv)

    assert outcome.code == ExitCode.INPUT
    assert outcome.err.startswith("INPUT ERROR: Invalid ")
    assert outcome.out == ""
    assert not database.exists()
    _assert_clean(outcome)


@pytest.mark.parametrize(
    ("point_value", "currency"),
    [("abc", "USD"), ("0", "USD"), ("-5", "USD"), ("NaN", "USD"), ("50", "US"), ("50", "")],
)
def test_invalid_economics_are_input_errors(
    database: Path, point_value: str, currency: str
) -> None:
    outcome = _set_economics(database, point_value=point_value, currency=currency)

    assert outcome.code == ExitCode.INPUT
    assert outcome.err.startswith("INPUT ERROR: Invalid ")
    assert not database.exists()


def test_a_database_in_a_missing_directory_is_a_configuration_error(tmp_path: Path) -> None:
    outcome = _paper_status(tmp_path / "missing" / "northstar.sqlite3", _at(25))

    assert outcome.code == ExitCode.CONFIGURATION
    assert "Database directory does not exist" in outcome.err
    assert not (tmp_path / "missing").exists()


def test_an_unexpected_failure_is_a_concise_internal_error(database: Path) -> None:
    def broken(path: Path) -> DatabaseRuntime:
        raise RuntimeError("boom with internals")

    outcome = _paper_status(database, _at(25), database_runtime=broken)

    assert outcome.code == ExitCode.INTERNAL
    assert outcome.err == "INTERNAL ERROR: unexpected RuntimeError.\n"
    _assert_clean(outcome)


class TrackingEnv(dict):
    def __init__(self, *args) -> None:
        super().__init__(*args)
        self.read: list[str] = []

    def get(self, key, default=None):
        self.read.append(key)
        return super().get(key, default)

    def __getitem__(self, key):
        self.read.append(key)
        return super().__getitem__(key)


def test_only_market_data_sync_reads_the_secret(database: Path) -> None:
    _phase_a(database)
    env = TrackingEnv({"DATABENTO_API_KEY": _SECRET})

    outcomes = [
        _set_economics(database),
        _paper_run(database, _at(25), env=env),
        _paper_status(database, _at(25), env=env),
    ]
    _cli(
        ["economics", "show", "--database", str(database), "--product", "ES", "--exchange", "CME"],
        env=env,
    )
    assert env.read == []

    synced, _ = _sync(database, lambda q: FuturesDailyAcquisitionResult(q, 1, 1), env=env)
    assert env.read == ["DATABENTO_API_KEY"]
    for outcome in (*outcomes, synced):
        _assert_clean(outcome)


def test_the_acquisition_query_carries_the_parsed_range(database: Path) -> None:
    acquisition = StubAcquisition(lambda q: FuturesDailyAcquisitionResult(q, 3, 3))

    _cli(
        [*_SYNC, "--database", str(database)],
        env={"DATABENTO_API_KEY": _SECRET},
        market_sync_runtime=lambda path, key: acquisition,
    )

    assert acquisition.queries == [
        FuturesDailyHistoricalAcquisitionQuery(_ES_DEC, date(2026, 9, 14), date(2026, 9, 16))
    ]
