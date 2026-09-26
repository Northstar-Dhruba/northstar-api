"""Acceptance: the ``northstar`` CLI as a daily manual paper-trading workflow.

Every command runs through ``northstar_api.cli.main`` with the production
database runtime against real temporary SQLite files. Market data enters
through ``market-data sync`` with the production acquisition stack -- the real
exchange-calendar resolver, the real Databento adapter (including its
completed-session guard), the real daily fold and the real SQLite store. Only
the Databento *client* is a double, returning minute records shaped like DBN
data, and the adapter's clock is injected. There is no network and no API key
beyond a placeholder.

Main timeline, one ES Dec 2026 contract, strategy alpha, target 1, on real CME
sessions S0.. from 2026-06-01:

    S0..S24   fifteen flat closes, then a steady rise; S24 (an early close)
              decides BUY -> pending BUY 1
    S25 (F1)  OPEN 7650, far from its close, high and low; the BUY fills there
    S26 (D2)  collapse; decides SELL -> pending SELL 2 against long 1
    S27 (F2)  the SELL 2 fills at its OPEN; SELL again -> target already met
    S28       unchanged; HOLD

Economics are explicit test fixtures (50 USD and 10 EUR per point per
contract), not exchange metadata. All P&L is gross simulated P&L.
"""

from __future__ import annotations

import io
import shutil
import sqlite3
import tomllib
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from databento_dbn import InstrumentClass
from northstar_application.application_services import (
    AcquireFuturesDailyHistoryUseCase,
    AggregateFuturesDailySessionBarUseCase,
)
from northstar_core.derivatives import ExpirationDate, QuoteValue
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.futures import FuturesContract, FuturesOHLCVBar, FuturesProductReference
from northstar_infrastructure.market_data import (
    DatabentoFuturesHistoricalMarketDataSource,
    ExchangeCalendarFuturesTradingSessionResolver,
    SQLiteFuturesHistoricalMarketDataStore,
)

from northstar_api.cli import ExitCode, main
from northstar_api.runtime import initialize_database

_SECRET = "db-ACCEPTANCE-SECRET-0123456789"
_ES = FuturesProductReference(Symbol("ES"), ExchangeCode("CME"))
_FESX = FuturesProductReference(Symbol("FESX"), ExchangeCode("EUREX"))
_FESX_DEC = FuturesContract(_FESX, ExpirationDate("2026-12-18"))
_SESSIONS = ExchangeCalendarFuturesTradingSessionResolver().sessions_in_range(
    _ES, date(2026, 6, 1), date(2026, 7, 31)
)
_TABLES = {
    "futures_ohlcv",
    "futures_forward_research_records",
    "futures_paper_orders",
    "futures_paper_fills",
    "futures_product_economics",
}
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _close(index: int) -> str:
    return _SESSIONS[index].closes_at.value


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


# (open, high, low, close, volume) per session index.
_RISE = ["7600"] * 15 + [str(7601 + index) for index in range(10)]
_DAILY: dict[int, tuple[str, str, str, str, int]] = {
    index: (close, str(int(close) + 2), str(int(close) - 2), close, 1000)
    for index, close in enumerate(_RISE)
}
_DAILY[25] = ("7650", "7700", "7600", "7611", 1000)
_DAILY[26] = ("7600", "7610", "2990", "3000", 250000)
_DAILY[27] = ("2950", "3050", "2800", "2900", 250000)
_DAILY[28] = ("2900", "2950", "2850", "2900", 1000)


# ---------------------------------------------------------------------------
# The Databento client double: DBN-shaped records, no network
# ---------------------------------------------------------------------------


@dataclass
class FakeDefinition:
    raw_symbol: str
    expiration: int
    asset: str = "ES"
    exchange: str = "XCME"
    instrument_class: object = InstrumentClass.FUTURE
    security_type: str = "FUT"


@dataclass
class FakeMinute:
    ts_event: int
    open: int
    high: int
    low: int
    close: int
    volume: int


def _raw(quote: str) -> int:
    return int(Decimal(quote) * 10**9)


def _ns(instant: str) -> int:
    return int((_utc(instant) - _EPOCH).total_seconds()) * 10**9


def _expiry_ns(expiration: str) -> int:
    return _ns(f"{expiration}T14:30:00Z")


_DEFINITIONS = [
    FakeDefinition("ESZ6", _expiry_ns("2026-12-18")),
    FakeDefinition("ESH7", _expiry_ns("2027-03-19")),
]


@dataclass
class FakeTimeseries:
    minutes: dict[tuple[str, str], list[FakeMinute]] = field(default_factory=dict)
    failure: Exception | None = None
    bar_requests: list[str] = field(default_factory=list)

    def get_range(self, **parameters):
        if self.failure is not None:
            raise self.failure
        if parameters["schema"] == "definition":
            return list(_DEFINITIONS)
        start = parameters["start"].isoformat()
        self.bar_requests.append(start)
        return list(self.minutes.get((parameters["symbols"][0], start), []))


@dataclass
class FakeMetadata:
    def get_dataset_range(self, dataset):
        edge = {"start": "2010-06-06T00:00:00Z", "end": "2027-06-30T00:00:00Z"}
        return {**edge, "schema": {"definition": edge}}


@dataclass
class FakeClient:
    timeseries: FakeTimeseries = field(default_factory=FakeTimeseries)
    metadata: FakeMetadata = field(default_factory=FakeMetadata)


class Provider:
    """What the provider holds, plus the clock the adapter reads."""

    def __init__(self) -> None:
        self.client = FakeClient()
        self.now: datetime | None = None
        self.keys: list[str] = []

    def publish(self, index: int, ohlcv: tuple | None = None, symbol: str = "ESZ6") -> None:
        """Hold one session as four one-minute records folding to the daily OHLCV."""
        opening, high, low, closing, volume = ohlcv or _DAILY[index]
        opens_ns = _ns(_SESSIONS[index].opens_at.value)
        quotes = (opening, high, low, closing)
        volumes = (volume // 4, volume // 4, volume // 4, volume - 3 * (volume // 4))
        self.client.timeseries.minutes[
            (symbol, _utc(_SESSIONS[index].opens_at.value).isoformat())
        ] = [
            FakeMinute(opens_ns + minute * 60 * 10**9, _raw(q), _raw(q), _raw(q), _raw(q), v)
            for minute, (q, v) in enumerate(zip(quotes, volumes, strict=True))
        ]

    def runtime(self, path: Path, api_key: str) -> AcquireFuturesDailyHistoryUseCase:
        """The production sync wiring with only the Databento client and clock replaced."""
        self.keys.append(api_key)
        initialize_database(path)
        return AcquireFuturesDailyHistoryUseCase(
            ExchangeCalendarFuturesTradingSessionResolver(),
            DatabentoFuturesHistoricalMarketDataSource(
                api_key, client=self.client, clock=lambda: self.now
            ),
            AggregateFuturesDailySessionBarUseCase(),
            SQLiteFuturesHistoricalMarketDataStore(path),
        )


# ---------------------------------------------------------------------------
# The operator
# ---------------------------------------------------------------------------


@dataclass
class Outcome:
    code: int
    out: str
    err: str

    @property
    def lines(self) -> list[str]:
        return self.out.splitlines()

    @property
    def text(self) -> str:
        return self.out + self.err


class Operator:
    """A human at a terminal: every action is one ``northstar`` command."""

    def __init__(self, database: Path, provider: Provider | None = None) -> None:
        self.database = database
        self.provider = provider or Provider()
        self.outcomes: list[Outcome] = []

    def cli(self, *argv: str, env: dict | None = None) -> Outcome:
        out, err = io.StringIO(), io.StringIO()
        code = main(
            list(argv),
            env={} if env is None else env,
            stdout=out,
            stderr=err,
            market_sync_runtime=self.provider.runtime,
        )
        outcome = Outcome(code, out.getvalue(), err.getvalue())
        self.outcomes.append(outcome)
        return outcome

    def economics(
        self, point_value: str = "50", currency: str = "USD", product: str = "ES", exchange="CME"
    ) -> Outcome:
        return self.cli(
            "economics", "set", "--database", str(self.database), "--product", product,
            "--exchange", exchange, "--point-value", point_value, "--currency", currency,
        )  # fmt: skip

    def show(self, product: str = "ES", exchange: str = "CME") -> Outcome:
        return self.cli(
            "economics", "show", "--database", str(self.database),
            "--product", product, "--exchange", exchange,
        )  # fmt: skip

    def sync(self, first: int, last: int, *, now: datetime | None = None, env=None) -> Outcome:
        for index in range(first, last + 1):
            key = ("ESZ6", _utc(_SESSIONS[index].opens_at.value).isoformat())
            if key not in self.provider.client.timeseries.minutes:
                self.provider.publish(index)
        self.provider.now = now or _utc(_close(last)) + timedelta(hours=1)
        return self.cli(
            "market-data", "sync", "--database", str(self.database), "--product", "ES",
            "--exchange", "CME", "--expiration", "2026-12-18",
            "--start", _SESSIONS[first].trading_date.isoformat(),
            "--end", _SESSIONS[last].trading_date.isoformat(),
            env={"DATABENTO_API_KEY": _SECRET} if env is None else env,
        )  # fmt: skip

    def run(
        self,
        as_of: str,
        *,
        target: str = "1",
        strategy: str = "alpha",
        product: str = "ES",
        exchange: str = "CME",
        expiration: str = "2026-12-18",
    ) -> Outcome:
        return self.cli(
            "paper", "run", "--database", str(self.database), "--product", product,
            "--exchange", exchange, "--expiration", expiration, "--strategy", strategy,
            "--portfolio", "futures-paper-alpha", "--target", target, "--as-of", as_of,
        )  # fmt: skip

    def status(self, as_of: str, strategy: str = "alpha") -> Outcome:
        return self.cli(
            "paper", "status", "--database", str(self.database), "--strategy", strategy,
            "--portfolio", "futures-paper-alpha", "--as-of", as_of,
        )  # fmt: skip

    def query(self, sql: str) -> list[tuple]:
        with sqlite3.connect(self.database) as connection:
            return connection.execute(sql).fetchall()

    def counts(self) -> tuple[int, int, int]:
        return tuple(
            self.query(f"SELECT COUNT(*) FROM {table}")[0][0]  # noqa: S608
            for table in (
                "futures_forward_research_records",
                "futures_paper_orders",
                "futures_paper_fills",
            )
        )

    def dump(self) -> dict[str, list[tuple]]:
        tables = [row[0] for row in self.query("SELECT name FROM sqlite_master WHERE type='table'")]
        return {
            "schema": sorted(self.query("SELECT type, name, sql FROM sqlite_master")),
            **{t: sorted(self.query(f"SELECT * FROM {t}")) for t in tables},  # noqa: S608
        }


def _assert_clean(outcome: Outcome) -> None:
    lowered = outcome.text.lower()
    assert "traceback" not in lowered
    assert _SECRET not in outcome.text
    assert "executed at" not in lowered
    assert "broker" not in lowered
    assert "total" not in lowered


def _section(outcome: Outcome, title: str) -> list[str]:
    lines = outcome.lines
    start = lines.index(title) + 1
    end = next((i for i in range(start, len(lines)) if lines[i] == ""), len(lines))
    return lines[start:end]


# ---------------------------------------------------------------------------
# Main timeline, played once
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def day(tmp_path_factory) -> SimpleNamespace:
    op = Operator(tmp_path_factory.mktemp("main") / "northstar.sqlite3")
    s = SimpleNamespace(op=op)

    s.economics_set = op.economics()
    s.economics_show = op.show()
    s.economics_retry = op.economics("50.0")
    s.economics_rows = op.query("SELECT * FROM futures_product_economics")
    s.economics_conflict = op.economics("25")
    s.economics_rows_after = op.query("SELECT * FROM futures_product_economics")

    s.sync_history = op.sync(0, 24)
    s.run_d1 = op.run(_close(24))
    s.counts_d1, s.dump_d1 = op.counts(), op.dump()
    s.retry_d1 = op.run(_close(24))
    s.counts_retry, s.dump_retry = op.counts(), op.dump()
    s.status_d1 = op.status(_close(24))
    s.dump_after_first_status = op.dump()

    s.sync_f1 = op.sync(25, 25)
    s.run_f1 = op.run(_close(25))
    s.counts_f1 = op.counts()
    s.dump_before_status = op.dump()
    s.status_f1 = op.status(_close(25))
    s.dump_after_status = op.dump()
    s.status_f1_again = Operator(op.database).status(_close(25))
    copy = op.database.with_name("restarted.sqlite3")
    shutil.copyfile(op.database, copy)
    s.status_f1_copy = Operator(copy).status(_close(25))

    s.sync_d2 = op.sync(26, 26)
    s.run_d2 = op.run(_close(26))
    s.counts_d2 = op.counts()
    s.sync_f2 = op.sync(27, 27)
    s.run_f2 = op.run(_close(27))
    s.counts_f2 = op.counts()
    s.status_f2 = op.status(_close(27))
    s.sync_s28 = op.sync(28, 28)
    s.run_hold = op.run(_close(28))
    s.counts_hold = op.counts()

    s.dump_before_guard = op.dump()
    s.backward = op.run(_close(27))
    s.dump_after_backward = op.dump()
    s.equal_retry = op.run(_close(28))
    s.offset_retry = op.run(
        (_utc(_close(28)) + timedelta(hours=5, minutes=30)).replace(tzinfo=None).isoformat()
        + "+05:30"
    )
    s.dump_after_retries = op.dump()
    s.changed_target = op.run(_close(28), target="2")
    s.dump_after_target = op.dump()
    s.historical_status = op.status(_close(25))

    s.counts_before_mixed = op.counts()
    s.mixed = op.run(_close(28), strategy="beta")
    s.counts_after_mixed = op.counts()
    s.beta_records = op.query(
        "SELECT COUNT(*) FROM futures_forward_research_records WHERE strategy_identity = 'beta'"
    )[0][0]
    s.tables = {row[0] for row in op.query("SELECT name FROM sqlite_master WHERE type='table'")}
    return s


def test_every_operational_output_is_clean(day) -> None:
    for outcome in day.op.outcomes:
        _assert_clean(outcome)


def test_economics_are_set_shown_retried_and_immutable(day) -> None:
    assert day.economics_set.code == 0
    assert day.economics_set.lines[:3] == [
        "ECONOMICS: READY",
        "Product: ES@CME",
        "Point value: 50 USD / quote-point / contract",
    ]
    assert day.economics_show.code == 0
    assert day.economics_show.lines == [
        "Product: ES@CME",
        "Point value: 50 USD / quote-point / contract",
    ]
    assert day.economics_retry.code == 0
    assert day.economics_conflict.code == ExitCode.STATE
    assert "FuturesProductEconomicsConflictError" in day.economics_conflict.err
    assert day.economics_rows == day.economics_rows_after == [("ES", "CME", "50", "USD")]


def test_history_syncs_through_the_production_acquisition_stack(day) -> None:
    sync = day.sync_history
    assert sync.code == 0
    assert "Sessions in range: 25" in sync.lines
    assert day.op.query("SELECT COUNT(*) FROM futures_ohlcv")[0][0] >= 25
    assert day.op.provider.keys and set(day.op.provider.keys) == {_SECRET}


def test_d1_freezes_buy_and_leaves_the_order_pending(day) -> None:
    run = day.run_d1
    assert run.code == 0
    assert run.lines[0] == "PAPER SESSION: COMPLETED"
    assert _section(run, "CONTEXT") == [
        "Contract: ES@CME 2026-12-18",
        "Strategy: alpha",
        "Policy: built-in directional MVP",
        "Portfolio: futures-paper-alpha",
        "Target: 1",
        f"Cutoff: {_close(24)}",
    ]
    assert _section(run, "DECISION") == ["Action: BUY", f"Decision instant: {_close(24)}"]
    order = _section(run, "ORDER")
    assert order[:3] == ["State: PENDING", "Side: BUY", "Contracts: 1"]
    assert order[3].startswith("ID: ") and len(order[3]) == len("ID: ") + 64
    assert _section(run, "PORTFOLIO (all contracts)") == ["Flat"]
    assert _section(run, "P&L (gross simulated)") == ["No filled contracts"]
    assert "Target contracts should remain fixed for this paper portfolio." in run.err
    assert day.counts_d1 == (1, 1, 0)


def test_the_same_day_retry_is_idempotent(day) -> None:
    assert day.retry_d1.code == 0
    assert day.retry_d1.out == day.run_d1.out
    assert day.counts_retry == day.counts_d1
    assert day.dump_retry == day.dump_d1


def test_status_at_d1_shows_the_pending_order_and_no_exposure(day) -> None:
    status = day.status_d1
    order_id = _section(day.run_d1, "ORDER")[3].removeprefix("ID: ")
    assert status.code == 0
    assert _section(status, "SUMMARY") == [
        "Orders: 1",
        "Fills: 0",
        "Pending: 1",
        f"PENDING ES@CME 2026-12-18: BUY 1 decided {_close(24)} ID {order_id}",
    ]
    assert _section(status, "PORTFOLIO (all contracts)") == ["Flat"]
    assert _section(status, "P&L (gross simulated)") == ["No filled contracts"]


def test_the_next_session_fills_the_earlier_order_at_its_open(day) -> None:
    run = day.run_f1
    history = _section(run, "ORDERS FOR THIS CONTRACT AND STRATEGY")
    assert run.code == 0
    assert _DAILY[25][0] not in _DAILY[25][1:4]
    assert history[0] == "Decisions: 2, orders: 1, filled: 1, pending: 0"
    assert history[1] == f"Decision {_close(24)}:"
    assert "  State: FILLED" in history
    assert "  Simulated fill price: 7650" in history
    assert "  Price basis: OPEN of next synced session" in history
    assert f"  Fill observable from: {_close(25)}" in history
    assert day.counts_f1 == (2, 1, 1)


def test_the_current_decision_is_distinct_from_earlier_orders(day) -> None:
    run = day.run_f1
    assert _section(run, "DECISION")[1] == f"Decision instant: {_close(25)}"
    assert _section(run, "ORDER") == ["State: NO ACTION", "Reason: TARGET ALREADY MET"]
    assert "FILLED" not in " ".join(_section(run, "ORDER"))


def test_status_marks_at_the_close_after_filling_at_the_open(day) -> None:
    status = day.status_f1
    assert status.code == 0
    assert _section(status, "SUMMARY") == ["Orders: 1", "Fills: 1", "Pending: 0"]
    assert _section(status, "PORTFOLIO (all contracts)") == ["ES@CME 2026-12-18: LONG 1 @ 7650"]
    assert _section(status, "P&L (gross simulated)") == [
        "ES@CME 2026-12-18",
        "  Realized P&L: 0 USD",
        f"  Mark: 7611 (daily close at {_close(25)})",
        "  Unrealized P&L: -1950 USD",
    ]


def test_status_is_read_only_and_survives_restart(day) -> None:
    assert day.dump_after_first_status == day.dump_retry
    assert day.dump_after_status == day.dump_before_status
    assert day.status_f1_again.out == day.status_f1.out
    assert day.status_f1_copy.out == day.status_f1.out


def test_a_sell_reverses_the_long_with_sell_2(day) -> None:
    run = day.run_d2
    assert run.code == 0
    assert _section(run, "DECISION")[0] == "Action: SELL"
    assert _section(run, "ORDER")[:3] == ["State: PENDING", "Side: SELL", "Contracts: 2"]
    assert day.counts_d2 == (3, 2, 1)


def test_the_reversal_settles_at_the_next_open_and_shows_both_pnl_sides(day) -> None:
    run, status = day.run_f2, day.status_f2
    assert run.code == status.code == 0
    assert _section(run, "ORDER") == ["State: NO ACTION", "Reason: TARGET ALREADY MET"]
    assert "  Simulated fill price: 2950" in _section(run, "ORDERS FOR THIS CONTRACT AND STRATEGY")
    assert day.counts_f2 == (4, 2, 2)
    assert _section(status, "PORTFOLIO (all contracts)") == ["ES@CME 2026-12-18: SHORT 1 @ 2950"]
    assert _section(status, "P&L (gross simulated)") == [
        "ES@CME 2026-12-18",
        "  Realized P&L: -235000 USD",
        f"  Mark: 2900 (daily close at {_close(27)})",
        "  Unrealized P&L: 2500 USD",
    ]


def test_hold_keeps_the_short_without_an_order(day) -> None:
    run = day.run_hold
    assert run.code == 0
    assert _section(run, "DECISION")[0] == "Action: HOLD"
    assert _section(run, "ORDER") == ["State: NO ACTION", "Reason: HOLD"]
    assert "ES@CME 2026-12-18 (command contract): SHORT 1 @ 2950" in run.lines
    assert day.counts_hold == (5, 2, 2)


def test_a_backward_run_is_refused_without_touching_the_database(day) -> None:
    refused = day.backward
    assert refused.code == ExitCode.STATE
    assert refused.out == ""
    assert f"Latest decision: {_close(28)}" in refused.err
    assert "Operational paper runs must move forward in time." in refused.err
    assert "Use research/replay tooling for historical reconstruction." in refused.err
    assert day.dump_after_backward == day.dump_before_guard


def test_equal_and_offset_equivalent_cutoffs_are_idempotent_retries(day) -> None:
    assert day.equal_retry.code == day.offset_retry.code == 0
    assert _section(day.equal_retry, "ORDER") == _section(day.offset_retry, "ORDER")
    assert day.dump_after_retries == day.dump_before_guard


def test_a_changed_target_conflicts_and_changes_nothing(day) -> None:
    outcome = day.changed_target
    assert outcome.code == ExitCode.STATE
    assert "FuturesPaperOrderConflictError" in outcome.err
    assert "Target contracts should remain fixed" in outcome.err
    assert day.dump_after_target == day.dump_after_retries


def test_status_may_look_back_in_time(day) -> None:
    assert day.historical_status.code == 0
    assert day.historical_status.out == day.status_f1.out


def test_another_strategy_cannot_trade_the_portfolio(day) -> None:
    outcome = day.mixed
    assert outcome.code == ExitCode.STATE
    assert "STATE ERROR: FuturesPaperPortfolioStrategyConflictError" in outcome.err
    before, after = day.counts_before_mixed, day.counts_after_mixed
    assert after[1:] == before[1:]
    assert day.beta_records == after[0] - before[0] == 1


def test_only_the_five_futures_tables_exist(day) -> None:
    assert day.tables == _TABLES


# ---------------------------------------------------------------------------
# Secondary databases
# ---------------------------------------------------------------------------


def _operator(tmp_path: Path, name: str) -> Operator:
    return Operator(tmp_path / f"{name}.sqlite3")


def test_warm_up_is_reported_without_facts(tmp_path: Path) -> None:
    op = _operator(tmp_path, "warm")
    op.sync(0, 18)

    run = op.run(_close(18))

    assert run.code == 0
    assert _section(run, "DECISION") == [
        "Decision: unavailable",
        "Reason: insufficient persisted daily history / warm-up",
    ]
    assert op.counts() == (0, 0, 0)
    _assert_clean(run)


def test_missing_economics_never_hides_persisted_execution(tmp_path: Path) -> None:
    op = _operator(tmp_path, "no-economics")
    op.sync(0, 25)

    pending = op.run(_close(24))
    filled = op.run(_close(25))
    facts = (
        op.query("SELECT * FROM futures_paper_orders"),
        op.query("SELECT * FROM futures_paper_fills"),
    )

    assert pending.code == 0
    assert filled.code == ExitCode.DATA
    assert filled.lines[0] == "PAPER SESSION: COMPLETED"
    assert _section(filled, "P&L (gross simulated)")[0] == "P&L: unavailable"
    assert "product economics not configured" in filled.out
    assert "completed and its facts were persisted" in filled.err
    assert op.counts() == (2, 1, 1)

    configured = op.economics()
    status = op.status(_close(25))

    assert configured.code == status.code == 0
    assert "  Unrealized P&L: -1950 USD" in status.lines
    assert (
        op.query("SELECT * FROM futures_paper_orders"),
        op.query("SELECT * FROM futures_paper_fills"),
    ) == facts


def test_a_multi_contract_multi_currency_portfolio_hides_nothing(tmp_path: Path) -> None:
    op = _operator(tmp_path, "portfolio")
    op.economics()
    op.economics("10", "EUR", product="FESX", exchange="EUREX")
    op.sync(0, 24)
    # EUREX is not a supported sync venue, so its bars go through the production SQLite store.
    fesx = [
        FuturesOHLCVBar(
            contract=_FESX_DEC,
            point_in_time=_SESSIONS[i].closes_at,
            timeframe=Timeframe("1d"),
            open=QuoteValue(Decimal(o)),
            high=QuoteValue(Decimal(h)),
            low=QuoteValue(Decimal(lo)),
            close=QuoteValue(Decimal(c)),
            volume=Quantity(Decimal(v)),
        )
        for i, (o, h, lo, c, v) in ((i, _DAILY[i]) for i in range(26))
    ]
    SQLiteFuturesHistoricalMarketDataStore(op.database).store(tuple(fesx[:25]))
    op.run(_close(24))
    op.run(_close(24), product="FESX", exchange="EUREX")
    SQLiteFuturesHistoricalMarketDataStore(op.database).store((fesx[25],))
    op.sync(25, 25)

    run = op.run(_close(25))
    status = op.status(_close(25))

    assert run.code == status.code == 0
    assert _section(run, "PORTFOLIO (all contracts)") == [
        "ES@CME 2026-12-18 (command contract): LONG 1 @ 7650",
        "FESX@EUREX 2026-12-18: LONG 1 @ 7650",
    ]
    pnl = _section(status, "P&L (gross simulated)")
    assert "  Unrealized P&L: -1950 USD" in pnl
    assert "  Unrealized P&L: -390 EUR" in pnl
    assert "  Realized P&L: 0 EUR" in pnl
    assert _section(status, "SUMMARY") == ["Orders: 2", "Fills: 2", "Pending: 0"]
    _assert_clean(status)


# ---------------------------------------------------------------------------
# Sync safety
# ---------------------------------------------------------------------------


def test_a_sync_stops_at_the_session_in_progress_and_resumes(tmp_path: Path) -> None:
    op = _operator(tmp_path, "resume")
    third = _SESSIONS[2]
    mid_session = _utc(third.opens_at.value) + timedelta(hours=6)

    stopped = op.sync(0, 2, now=mid_session)

    assert stopped.code == ExitCode.DATA
    assert f"Session {third.trading_date.isoformat()} has not completed." in stopped.err
    assert f"Session close: {third.closes_at}" in stopped.err
    assert f"Current UTC: {mid_session.isoformat().replace('+00:00', 'Z')}" in stopped.err
    assert f"Sync stopped at {third.trading_date.isoformat()}." in stopped.err
    assert "Earlier completed sessions in the range may already have been stored." in stopped.err
    assert op.query("SELECT point_in_time FROM futures_ohlcv ORDER BY 1") == [
        (_close(0),),
        (_close(1),),
    ]
    requested = len(op.provider.client.timeseries.bar_requests)

    resumed = op.sync(0, 2)
    again = op.sync(0, 2)

    assert requested == 2
    assert resumed.code == again.code == 0
    for outcome in (resumed, again):
        assert any(
            line.startswith("Sessions with a persisted daily bar: 3 ") for line in outcome.lines
        )
    assert op.query("SELECT COUNT(*) FROM futures_ohlcv")[0][0] == 3
    _assert_clean(stopped)


def test_provider_failures_and_missing_keys_are_translated(tmp_path: Path) -> None:
    op = _operator(tmp_path, "provider")
    missing = op.sync(0, 0, env={})
    op.provider.client.timeseries.failure = RuntimeError(f"upstream said {_SECRET}")
    failed = op.sync(0, 0)

    assert missing.code == ExitCode.CONFIGURATION
    assert "DATABENTO_API_KEY is not set" in missing.err
    assert op.provider.keys == [_SECRET]
    assert failed.code == ExitCode.PROVIDER
    assert "PROVIDER ERROR: DatabentoFuturesHistoricalMarketDataSourceError" in failed.err
    assert "may already have been stored" in failed.err
    _assert_clean(missing)
    _assert_clean(failed)


# ---------------------------------------------------------------------------
# Conflicts, corruption and failures
# ---------------------------------------------------------------------------


def test_changed_provider_evidence_is_a_market_data_conflict(tmp_path: Path) -> None:
    op = _operator(tmp_path, "market-conflict")
    op.sync(0, 0)
    rows = op.query("SELECT * FROM futures_ohlcv")
    op.provider.publish(0, ("1", "2", "0.5", "1.5", 8))

    outcome = op.sync(0, 0)

    assert outcome.code == ExitCode.STATE
    assert "FuturesHistoricalMarketDataConflictError" in outcome.err
    assert op.query("SELECT * FROM futures_ohlcv") == rows


def test_a_back_filled_session_conflicts_with_the_frozen_decision(tmp_path: Path) -> None:
    op = _operator(tmp_path, "forward-conflict")
    op.sync(0, 24)
    op.sync(26, 26)
    assert op.run(_close(26)).code == 0
    records = op.query("SELECT * FROM futures_forward_research_records")

    op.sync(25, 25)
    outcome = op.run(_close(26))

    assert outcome.code == ExitCode.STATE
    assert "FuturesForwardResearchRecordConflictError" in outcome.err
    assert op.query("SELECT * FROM futures_forward_research_records") == records


def test_corrupt_economics_are_a_state_error(tmp_path: Path) -> None:
    op = _operator(tmp_path, "corrupt")
    op.economics()
    with sqlite3.connect(op.database) as connection:
        connection.execute("UPDATE futures_product_economics SET point_value_amount = '0'")

    outcome = op.show()

    assert outcome.code == ExitCode.STATE
    assert outcome.err.startswith("STATE ERROR: FuturesProductEconomicsStorageError")
    assert op.query("SELECT point_value_amount FROM futures_product_economics") == [("0",)]
    _assert_clean(outcome)


def test_economics_show_for_an_unconfigured_product_is_data(tmp_path: Path) -> None:
    outcome = _operator(tmp_path, "unconfigured").show("NQ")

    assert outcome.code == ExitCode.DATA
    assert "Product economics not configured for NQ@CME." in outcome.err
    _assert_clean(outcome)


def test_an_unexpected_failure_is_a_concise_internal_error(tmp_path: Path) -> None:
    def broken(path: Path):
        raise KeyError("internal detail")

    out, err = io.StringIO(), io.StringIO()
    code = main(
        ["paper", "status", "--database", str(tmp_path / "x.sqlite3"), "--strategy", "alpha",
         "--portfolio", "p", "--as-of", "2026-07-06T22:00:00Z"],
        env={}, stdout=out, stderr=err, database_runtime=broken,
    )  # fmt: skip

    assert code == ExitCode.INTERNAL
    assert err.getvalue() == "INTERNAL ERROR: unexpected KeyError.\n"
    assert out.getvalue() == ""


# ---------------------------------------------------------------------------
# Command boundary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "argv",
    [
        ["paper", "run", "--product", "ES", "--exchange", "CME", "--expiration", "2026-12-18",
         "--strategy", "alpha", "--portfolio", "p", "--target", "1",
         "--as-of", "2026-07-06T22:00:00"],
        ["paper", "run", "--product", "ES", "--exchange", "CME", "--expiration", "2026-02-30",
         "--strategy", "alpha", "--portfolio", "p", "--target", "1",
         "--as-of", "2026-07-06T22:00:00Z"],
        ["paper", "run", "--product", "ES", "--exchange", "CME", "--expiration", "2026-12-18",
         "--strategy", "alpha", "--portfolio", "p", "--target", "0",
         "--as-of", "2026-07-06T22:00:00Z"],
        ["economics", "set", "--product", "ES", "--exchange", "CME", "--point-value", "50",
         "--currency", "U$D"],
        ["economics", "set", "--product", "ES", "--exchange", "CME", "--point-value", "0",
         "--currency", "USD"],
        ["market-data", "sync", "--product", "ES", "--exchange", "CME",
         "--expiration", "2026-12-18", "--start", "2026-6-1", "--end", "2026-06-30"],
        ["paper", "status", "--strategy", "alpha", "--portfolio", "p"],
    ],
    ids=["naive-as-of", "bad-expiration", "zero-target", "bad-currency", "zero-point-value",
         "malformed-date", "missing-as-of"],
)  # fmt: skip
def test_invalid_input_is_rejected_before_the_database(tmp_path: Path, argv) -> None:
    database = tmp_path / "never.sqlite3"

    outcome = Operator(database).cli(
        *argv, "--database", str(database), env={"DATABENTO_API_KEY": _SECRET}
    )

    assert outcome.code == ExitCode.INPUT
    assert not database.exists()
    _assert_clean(outcome)


@pytest.mark.parametrize("kind", ["missing-directory", "directory"])
def test_an_unusable_database_path_is_configuration(tmp_path: Path, kind: str) -> None:
    database = tmp_path / "absent" / "db.sqlite3" if kind == "missing-directory" else tmp_path

    outcome = Operator(database).status("2026-07-06T22:00:00Z")

    assert outcome.code == ExitCode.CONFIGURATION
    assert not (tmp_path / "absent").exists()
    _assert_clean(outcome)


def test_paper_and_economics_commands_never_read_the_secret(tmp_path: Path) -> None:
    op = _operator(tmp_path, "no-secret")
    op.sync(0, 24)

    class Watching(dict):
        read: list = []

        def get(self, key, default=None):
            self.read.append(key)
            return super().get(key, default)

        def __getitem__(self, key):
            self.read.append(key)
            return super().__getitem__(key)

    env = Watching({"DATABENTO_API_KEY": _SECRET})
    for argv in (
        ["economics", "set", "--database", str(op.database), "--product", "ES",
         "--exchange", "CME", "--point-value", "50", "--currency", "USD"],
        ["economics", "show", "--database", str(op.database), "--product", "ES",
         "--exchange", "CME"],
        ["paper", "run", "--database", str(op.database), "--product", "ES", "--exchange", "CME",
         "--expiration", "2026-12-18", "--strategy", "alpha", "--portfolio",
         "futures-paper-alpha", "--target", "1", "--as-of", _close(24)],
        ["paper", "status", "--database", str(op.database), "--strategy", "alpha",
         "--portfolio", "futures-paper-alpha", "--as-of", _close(24)],
    ):  # fmt: skip
        assert op.cli(*argv, env=env).code == 0

    assert Watching.read == []


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["--help"], ["economics", "market-data", "paper"]),
        (["economics", "--help"], ["set", "show"]),
        (["economics", "set", "--help"], ["--database", "--product", "--exchange",
                                          "--point-value", "--currency"]),
        (["market-data", "sync", "--help"], ["--database", "--product", "--exchange",
                                             "--expiration", "--start", "--end"]),
        (["paper", "run", "--help"], ["--database", "--product", "--exchange", "--expiration",
                                      "--strategy", "--portfolio", "--target", "--as-of"]),
        (["paper", "status", "--help"], ["--database", "--strategy", "--portfolio", "--as-of"]),
    ],
)  # fmt: skip
def test_help_documents_every_required_argument(argv, expected) -> None:
    outcome = Operator(Path("unused")).cli(*argv)

    assert outcome.code == 0
    for option in expected:
        assert option in outcome.out
    for absent in ("--timeframe", "front", "rollover", "daily-run", "schedule"):
        assert absent not in outcome.out


def test_the_northstar_script_is_registered() -> None:
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    scripts = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]["scripts"]

    assert scripts["northstar"] == "northstar_api.cli:main"
    assert scripts["northstar-api"] == "northstar_api:main"
