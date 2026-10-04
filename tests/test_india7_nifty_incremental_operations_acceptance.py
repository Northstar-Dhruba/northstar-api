"""Acceptance: INDIA-7 incremental operational paper trading on NIFTY Futures.

This proves the operated sequence, not a historical back-fill. For each
completed NSE session D, chosen explicitly by the operator:

    northstar market-data sync --provider upstox --start D --end D
    northstar paper run --as-of <D session close>

Everything runs through the production CLI and runtime on real temporary
SQLite files, on real NSE sessions from the real NSEFuturesTradingSessionResolver.
Only the Upstox HTTP transport is a double; it honours the requested
``--start``/``--end`` exactly, as the real provider does. No clock decides
anything: every cutoff and every range is explicit. The token is a placeholder
and the network is refused for the whole module.

Operational identity: NIFTY / NSE / 2026-10-27, strategy
``directional-mvp-v1-nifty-paper-ops``, portfolio ``nifty-paper-ops-oct26``,
target 1, 65 INR per quote point per contract. P&L is gross.

Normal operating path (bars are session numbers from 2026-07-29), designed
around the frozen built-in rule:

    bars  1..20   flat 25000, volume 1000         bootstrap, no paper
    bar   21      unchanged                        HOLD -> no order
    bar   22      25100                            BUY -> BUY 1 pending
    bar   23      25200                            fills BUY at bar 23 OPEN;
                                                   BUY again -> target already met
    bar   24      24000 on volume 3000             SELL -> SELL 2 pending
    bars 25, 26   unchanged                        SELL fills at bar 25 OPEN; HOLD

Each bar opens three points above the previous close, so a fill's price names
its bar. The expiry scenario is kept separate and uses the October countdown:
E = Tue 10-27, E-6 = Fri 10-16, E-5 = Mon 10-19 (Tue 10-20 is an NSE holiday).
"""

from __future__ import annotations

import gzip
import io
import json
import socket
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError

import pytest
from northstar_application.application_services import FuturesPaperExecutionIdentityService
from northstar_application.ports import (
    FuturesForwardResearchRecordQuery,
    FuturesHistoricalMarketDataQuery,
    FuturesPaperFillQuery,
    FuturesPaperOrderQuery,
    FuturesSessionResolutionError,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol, Timeframe
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_core.paper_trading import (
    FuturesContractCount,
    OrderSide,
    PaperPortfolioIdentity,
)
from northstar_core.strategy import StrategyIdentity
from northstar_infrastructure.market_data import (
    NSEFuturesTradingSessionResolver,
    upstox_http,
)
from northstar_infrastructure.market_data.upstox_instrument_master import (
    NSE_INSTRUMENT_MASTER_URL,
)

from northstar_api import cli as cli_module
from northstar_api.cli import ExitCode, main
from northstar_api.runtime import build_database_runtime, build_upstox_market_sync_runtime

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_CONTRACT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_STRATEGY = StrategyIdentity("directional-mvp-v1-nifty-paper-ops")
_PORTFOLIO = PaperPortfolioIdentity("nifty-paper-ops-oct26")
_DAILY = Timeframe("1d")
_POINT_VALUE = Decimal(65)
_LOT = 65
_TOKEN = "placeholder-not-a-credential"
_CONTRACT_ARGS = ["--product", "NIFTY", "--exchange", "NSE", "--expiration", "2026-10-27"]

# Every real NSE session of the contract's life, from its first listed day to expiry.
_SESSIONS = NSEFuturesTradingSessionResolver().sessions_in_range(
    _NIFTY, date(2026, 7, 29), date(2026, 10, 27)
)
_E = date(2026, 10, 27)
_E6 = date(2026, 10, 16)
_E5 = date(2026, 10, 19)


def _bar_of(day: date) -> int:
    return next(i for i, s in enumerate(_SESSIONS, start=1) if s.trading_date == day)


def _close(bar: int) -> str:
    return _SESSIONS[bar - 1].closes_at.value


def _day(bar: int) -> str:
    return _SESSIONS[bar - 1].trading_date.isoformat()


# ---------------------------------------------------------------------------
# Synthetic markets
# ---------------------------------------------------------------------------


@dataclass
class Market:
    closes: list[Decimal]
    volumes: list[int]

    def open_(self, bar: int) -> Decimal:
        return (self.closes[bar - 2] if bar > 1 else self.closes[0]) + 3

    def close(self, bar: int) -> Decimal:
        return self.closes[bar - 1]


def _normal_market() -> Market:
    closes = [Decimal(25000)] * 21 + [Decimal(v) for v in (25100, 25200, 24000, 24000, 24000)]
    volumes = [1000] * 23 + [3000, 1000, 1000]
    return Market(closes, volumes)


_E8_BAR = _bar_of(date(2026, 10, 14))


def _expiry_market() -> Market:
    """Flat to E-9, then BUY at E-8; HOLD at E-6; rising (BUY signals) from E-5 to E."""
    closes = [Decimal(25000)] * (_E8_BAR - 1) + [
        Decimal(v)
        for v in (25100, 25200, 25200, 25300, 25400, 25500, 25600, 25700, 25800)  # E-8 .. E
    ]
    assert len(closes) == len(_SESSIONS)
    return Market(closes, [1000] * len(closes))


def _rule(market: Market, bar: int) -> str:
    """Independent restatement of the frozen rule, for the oracle only."""
    closes = market.closes[bar - 20 : bar]
    volumes = [Decimal(v) for v in market.volumes[bar - 20 : bar]]
    short, long_ = sum(closes[-5:]) / 5, sum(closes) / 20
    elevated = volumes[-1] >= sum(volumes) / 20
    latest, previous = market.close(bar), market.close(bar - 1)
    if latest > previous and short > long_ and elevated:
        return "BUY"
    if latest < previous and short < long_ and elevated:
        return "SELL"
    return "HOLD"


# ---------------------------------------------------------------------------
# Upstox HTTP double honouring the requested range
# ---------------------------------------------------------------------------

_IST = timezone(timedelta(hours=5, minutes=30))
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _master(include_contract: bool = True) -> bytes:
    expiry = datetime(2026, 10, 27, 23, 59, 59, tzinfo=_IST)
    records = []
    if include_contract:
        records.append(
            {
                "segment": "NSE_FO",
                "exchange": "NSE",
                "instrument_type": "FUT",
                "underlying_symbol": "NIFTY",
                "expiry": int((expiry - _EPOCH).total_seconds()) * 1000,
                "instrument_key": "NSE_FO|48704",
                "lot_size": _LOT,
                "trading_symbol": "NIFTY FUT 27 OCT 26",
            }
        )
    return gzip.compress(json.dumps(records).encode("utf-8"))


@dataclass
class FakeUpstox:
    market: Market
    omit: set[int] = field(default_factory=set)
    revised: dict[int, Decimal] = field(default_factory=dict)
    candle_error: Exception | None = None
    master_has_contract: bool = True
    candle_requests: list[tuple[str, str]] = field(default_factory=list)

    def __call__(self, url: str, headers, timeout: float) -> bytes:
        if url == NSE_INSTRUMENT_MASTER_URL:
            return _master(self.master_has_contract)
        if self.candle_error is not None:
            raise self.candle_error
        *_, to, start = url.rstrip("/").split("/")
        self.candle_requests.append((start, to))
        first, last = date.fromisoformat(start), date.fromisoformat(to)
        rows = []
        for bar, session in enumerate(_SESSIONS, start=1):
            if not first <= session.trading_date <= last or bar in self.omit:
                continue
            open_ = self.market.open_(bar)
            close = self.revised.get(bar, self.market.close(bar))
            rows.append(
                [
                    f"{session.trading_date.isoformat()}T00:00:00+05:30",
                    int(open_),
                    int(max(open_, close) + 5),
                    int(min(open_, close) - 5),
                    int(close),
                    self.market.volumes[bar - 1] * _LOT,
                    0,
                ]
            )
        rows.reverse()  # newest first, as Upstox returns them
        return json.dumps({"status": "success", "data": {"candles": rows}}).encode("utf-8")


# ---------------------------------------------------------------------------
# No network for the whole module
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def no_network():
    import databento

    def refuse(*args, **kwargs):
        raise AssertionError("INDIA-7 acceptance must not touch the network")

    patcher = pytest.MonkeyPatch()
    patcher.setattr(socket.socket, "connect", refuse)
    patcher.setattr(socket, "create_connection", refuse)
    patcher.setattr(upstox_http, "urlopen", refuse)
    patcher.setattr(databento, "Historical", refuse)
    yield
    patcher.undo()


# ---------------------------------------------------------------------------
# The operator: explicit commands only
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Outcome:
    code: int
    out: str
    err: str

    def section(self, title: str) -> list[str]:
        lines = self.out.splitlines()
        if title not in lines:
            return []
        start = lines.index(title) + 1
        end = next((i for i in range(start, len(lines)) if lines[i] == ""), len(lines))
        return lines[start:end]


def _never_read_the_clock():
    raise AssertionError("the INDIA-7 manual path must not read the clock")


class Operator:
    def __init__(self, database: Path, market: Market) -> None:
        self.database = database
        self.upstox = FakeUpstox(market)

    def _cli(self, argv: list[str], **kwargs) -> Outcome:
        out, err = io.StringIO(), io.StringIO()
        code = main(
            argv,
            env=kwargs.pop("env", {}),
            stdout=out,
            stderr=err,
            clock=_never_read_the_clock,
            **kwargs,
        )
        return Outcome(code, out.getvalue(), err.getvalue())

    def economics(self) -> Outcome:
        return self._cli(
            ["economics", "set", "--database", str(self.database), *_CONTRACT_ARGS,
             "--point-value", "65", "--currency", "INR"]
        )  # fmt: skip

    def sync(self, first: int, last: int | None = None) -> Outcome:
        last = first if last is None else last
        return self._cli(
            ["market-data", "sync", "--database", str(self.database), *_CONTRACT_ARGS,
             "--start", _day(first), "--end", _day(last), "--provider", "upstox"],
            env={"UPSTOX_ANALYTICS_TOKEN": _TOKEN},
            upstox_market_sync_runtime=lambda path, token: build_upstox_market_sync_runtime(
                path, token, fetch=self.upstox
            ),
        )  # fmt: skip

    def paper(self, bar: int) -> Outcome:
        return self._cli(
            ["paper", "run", "--database", str(self.database), *_CONTRACT_ARGS,
             "--strategy", _STRATEGY.identity, "--portfolio", _PORTFOLIO.identity,
             "--target", "1", "--as-of", _close(bar)]
        )  # fmt: skip

    def status(self, bar: int) -> Outcome:
        return self._cli(
            ["paper", "status", "--database", str(self.database),
             "--strategy", _STRATEGY.identity, "--portfolio", _PORTFOLIO.identity,
             "--as-of", _close(bar)]
        )  # fmt: skip

    def bootstrap(self, last: int) -> None:
        assert self.economics().code == ExitCode.SUCCESS
        synced = self.sync(1, last)
        assert synced.code == ExitCode.SUCCESS, synced.err

    def cycle(self, bar: int) -> tuple[Outcome, Outcome]:
        synced = self.sync(bar)
        assert synced.code == ExitCode.SUCCESS, synced.err
        ran = self.paper(bar)
        assert ran.code == ExitCode.SUCCESS, ran.err
        return synced, ran

    # -- persisted facts ---------------------------------------------------

    def facts(self) -> dict:
        rt = build_database_runtime(self.database)
        return {
            "bars": rt.market_repository.get_bars(
                FuturesHistoricalMarketDataQuery(_CONTRACT, _DAILY)
            ),
            "records": rt.forward_repository.get_records(
                FuturesForwardResearchRecordQuery(_CONTRACT, _DAILY)
            ),
            "orders": rt.order_repository.get_orders(FuturesPaperOrderQuery(_PORTFOLIO)),
            "fills": rt.fill_repository.get_fills(FuturesPaperFillQuery(_PORTFOLIO)),
        }

    def valuation(self, bar: int):
        return build_database_runtime(self.database).valuation.execute(
            _PORTFOLIO, _STRATEGY, PointInTime(_close(bar))
        )

    def rows(self, sql: str) -> list[tuple]:
        with closing(sqlite3.connect(self.database)) as connection:
            return connection.execute(sql).fetchall()


def _operator(tmp_path: Path, name: str, market: Market | None = None) -> Operator:
    return Operator(tmp_path / f"{name}.sqlite3", market or _normal_market())


def _independent_pnl(fills, mark: Decimal) -> tuple[Decimal, Decimal, int, Decimal]:
    """Average-entry fold over persisted fills, then mark-to-close, at 65 INR."""
    net, average, realized = 0, Decimal(0), Decimal(0)
    for fill in fills:
        signed = fill.contracts.value if fill.side is OrderSide.BUY else -fill.contracts.value
        quote = fill.fill_quote.value
        if net == 0 or (net > 0) == (signed > 0):
            average = (average * abs(net) + quote * abs(signed)) / (abs(net) + abs(signed))
            net += signed
            continue
        realized += (quote - average) * (1 if net > 0 else -1) * min(abs(net), abs(signed))
        remainder = net + signed
        if remainder != 0 and (remainder > 0) != (net > 0):
            average = quote
        net = remainder
    unrealized = (mark - average) * net if net else Decimal(0)
    return realized * _POINT_VALUE, unrealized * _POINT_VALUE, net, average


def test_the_synthetic_paths_produce_the_documented_decisions() -> None:
    normal = _normal_market()
    assert [_rule(normal, bar) for bar in range(21, 27)] == [
        "HOLD", "BUY", "BUY", "SELL", "HOLD", "HOLD"
    ]  # fmt: skip
    expiry = _expiry_market()
    assert [_rule(expiry, bar) for bar in range(_E8_BAR, len(_SESSIONS) + 1)] == [
        "BUY", "BUY", "HOLD", "BUY", "BUY", "BUY", "BUY", "BUY", "BUY"
    ]  # fmt: skip
    assert _SESSIONS[_E8_BAR + 1].trading_date == _E6
    assert _SESSIONS[_E8_BAR + 2].trading_date == _E5


# ---------------------------------------------------------------------------
# A-F. One operated contract, session by session
# ---------------------------------------------------------------------------


def test_the_incremental_operational_cycle(tmp_path: Path) -> None:
    op = _operator(tmp_path, "nifty-paper-ops")
    market = op.upstox.market

    # A. Bootstrap: 20 bars through the production Upstox path, no paper.
    op.bootstrap(20)
    facts = op.facts()
    assert len(facts["bars"]) == 20
    assert facts["records"] == () and facts["orders"] == () and facts["fills"] == ()
    status = op.status(20)
    assert status.section("PORTFOLIO (all contracts)") == ["Flat"]
    assert status.section("SUMMARY")[:3] == ["Orders: 0", "Fills: 0", "Pending: 0"]

    # B. A HOLD day: frozen once, no order, still flat; rerun is idempotent.
    synced, hold = op.cycle(21)
    assert "Date range: " + _day(21) + " .. " + _day(21) + " (trading dates)" in synced.out
    assert "Sessions in range: 1" in synced.out
    assert hold.section("DECISION") == ["Action: HOLD", f"Decision instant: {_close(21)}"]
    assert hold.section("ORDER") == ["State: NO ACTION", "Reason: HOLD"]
    assert op.paper(21).out == hold.out
    facts = op.facts()
    assert [r.decision_instant.value for r in facts["records"]] == [_close(21)]
    assert facts["orders"] == () and facts["fills"] == ()

    # C. A directional decision creates exactly one pending order.
    _, buy = op.cycle(22)
    assert buy.section("DECISION")[0] == "Action: BUY"
    order_lines = buy.section("ORDER")
    assert order_lines[:3] == ["State: PENDING", "Side: BUY", "Contracts: 1"]
    facts = op.facts()
    [order] = facts["orders"]
    assert facts["fills"] == ()
    record_22 = next(r for r in facts["records"] if r.decision_instant.value == _close(22))
    expected_id = FuturesPaperExecutionIdentityService().order_identity(record_22, _PORTFOLIO)
    assert order.identity == expected_id
    assert f"ID: {expected_id.identity}" in order_lines
    assert op.paper(22).out == buy.out
    assert op.facts()["orders"] == (order,) and op.facts()["fills"] == ()

    # D. Bar 23 is synced, but the cutoff stays at bar 22: still pending, no lookahead.
    assert op.sync(23).code == ExitCode.SUCCESS
    assert len(op.facts()["bars"]) == 23
    still = op.paper(22)
    assert still.out == buy.out
    assert still.section("ORDER")[0] == "State: PENDING"
    facts = op.facts()
    assert facts["fills"] == ()
    assert _close(23) not in [r.decision_instant.value for r in facts["records"]]

    # E. The bar-23 cycle: settle at the bar-23 OPEN, freeze bar 23, judge the post-fill state.
    settle = op.paper(23)
    assert settle.code == ExitCode.SUCCESS, settle.err
    assert settle.section("DECISION")[0] == "Action: BUY"
    assert settle.section("ORDER") == ["State: NO ACTION", "Reason: TARGET ALREADY MET"]
    history = settle.out.splitlines()
    filled = history[history.index(f"Decision {_close(22)}:") + 1 :][:8]
    assert "  State: FILLED" in filled
    assert f"  Simulated fill price: {market.open_(23)}" in filled
    assert f"  Fill observable from: {_close(23)}" in filled
    [fill] = op.facts()["fills"]
    assert (fill.side, fill.contracts) == (OrderSide.BUY, FuturesContractCount(1))
    assert fill.fill_quote.value == market.open_(23) == Decimal(25103)
    assert fill.filled_at.value == _close(23)
    assert settle.section("PORTFOLIO (all contracts)") == [
        f"NIFTY@NSE 2026-10-27 (command contract): LONG 1 @ {market.open_(23)}"
    ]
    assert "  Unrealized P&L: 6305 INR" in settle.out  # (25200 - 25103) * 65
    assert op.paper(23).out == settle.out
    assert len(op.facts()["fills"]) == 1

    # Continue: SELL reverses through zero, then fills at the bar-25 OPEN.
    _, sell = op.cycle(24)
    assert sell.section("ORDER")[:3] == ["State: PENDING", "Side: SELL", "Contracts: 2"]
    op.cycle(25)
    op.cycle(26)

    # F. P&L, recomputed independently from persisted fills and the bar-26 close.
    facts = op.facts()
    assert len(facts["orders"]) == 2 and len(facts["fills"]) == 2
    realized, unrealized, net, average = _independent_pnl(facts["fills"], market.close(26))
    assert (net, average) == (-1, Decimal(24003))
    assert realized == (Decimal(24003) - Decimal(25103)) * 65 == Decimal(-71500)
    assert unrealized == (Decimal(24000) - Decimal(24003)) * -1 * 65 == Decimal(195)
    [pnl] = op.valuation(26).contracts
    assert pnl.realized_pnl.amount == realized
    assert pnl.unrealized_pnl.amount == unrealized
    assert pnl.position.net_contracts == net
    assert pnl.position.average_entry.value == average
    final = op.status(26)
    assert "  Realized P&L: -71500 INR" in final.out
    assert "  Unrealized P&L: 195 INR" in final.out
    assert [r.result.recommendation.action.value for r in facts["records"]] == [
        _rule(market, bar) for bar in range(21, 27)
    ]


# ---------------------------------------------------------------------------
# G. Catch-up: data as a range, decisions one cutoff at a time
# ---------------------------------------------------------------------------


def _final_facts(op: Operator, bar: int) -> tuple:
    facts = op.facts()
    [pnl] = op.valuation(bar).contracts
    return facts["records"], facts["orders"], facts["fills"], pnl


def test_catching_up_a_range_equals_operating_day_by_day(tmp_path: Path) -> None:
    daily = _operator(tmp_path, "daily")
    daily.bootstrap(20)
    daily_outputs = [daily.cycle(bar)[1].out for bar in range(21, 27)]

    caught_up = _operator(tmp_path, "caught-up")
    caught_up.bootstrap(20)
    ranged = caught_up.sync(21, 26)
    assert ranged.code == ExitCode.SUCCESS and "Sessions in range: 6" in ranged.out
    caught_up_outputs = [caught_up.paper(bar).out for bar in range(21, 27)]

    assert _final_facts(caught_up, 26) == _final_facts(daily, 26)
    assert caught_up_outputs == daily_outputs


# ---------------------------------------------------------------------------
# H. The latest-only pitfall, pinned as the operational rule
# ---------------------------------------------------------------------------


def test_running_only_the_latest_cutoff_skips_intermediate_decisions(tmp_path: Path) -> None:
    op = _operator(tmp_path, "latest-only")
    op.bootstrap(20)
    assert op.sync(21, 24).code == ExitCode.SUCCESS

    latest = op.paper(24)

    assert latest.code == ExitCode.SUCCESS
    facts = op.facts()
    assert [r.decision_instant.value for r in facts["records"]] == [_close(24)]
    # Bar 24's SELL against a flat portfolio: the bar-22 BUY was never taken.
    [order] = facts["orders"]
    assert (order.intent.side, order.intent.contracts) == (OrderSide.SELL, FuturesContractCount(1))

    backward = op.paper(22)
    assert backward.code == ExitCode.STATE
    assert "Paper run refused: cutoff precedes the latest frozen decision" in backward.err
    assert op.facts()["records"] == facts["records"]


# ---------------------------------------------------------------------------
# I. Market-data idempotency and conflict
# ---------------------------------------------------------------------------


def test_resyncing_an_identical_day_is_idempotent_and_a_revision_conflicts(
    tmp_path: Path,
) -> None:
    op = _operator(tmp_path, "sync-idempotency")
    op.bootstrap(20)
    assert op.sync(21).code == ExitCode.SUCCESS
    stored = op.facts()["bars"]

    again = op.sync(21)
    assert again.code == ExitCode.SUCCESS
    assert "Sessions with a persisted daily bar: 1" in again.out
    assert op.facts()["bars"] == stored
    assert op.rows("SELECT COUNT(*) FROM futures_ohlcv") == [(21,)]

    op.upstox.revised[21] = Decimal(25050)
    revised = op.sync(21)
    assert revised.code == ExitCode.STATE
    assert "FuturesHistoricalMarketDataConflictError" in revised.err
    assert op.facts()["bars"] == stored
    assert op.facts()["bars"][-1].close.value == Decimal(25000)


# ---------------------------------------------------------------------------
# J. Provider and data failures, then recovery by rerunning the same command
# ---------------------------------------------------------------------------


def test_a_missing_completed_session_stores_nothing_and_recovers(tmp_path: Path) -> None:
    op = _operator(tmp_path, "missing")
    op.bootstrap(20)
    op.upstox.omit.add(22)

    failed = op.sync(21, 22)
    assert failed.code == ExitCode.DATA
    assert "FuturesDailySessionCoverageError" in failed.err
    assert _day(22) in failed.err
    assert "Nothing from this range was stored." in failed.err
    assert len(op.facts()["bars"]) == 20

    op.upstox.omit.clear()
    assert op.sync(21, 22).code == ExitCode.SUCCESS
    assert len(op.facts()["bars"]) == 22
    assert op.paper(21).code == op.paper(22).code == ExitCode.SUCCESS
    assert len(op.facts()["records"]) == 2


def _http_error(status: int, body: bytes) -> HTTPError:
    return HTTPError("u", status, "provider error", {}, io.BytesIO(body))


def _invalid_instrument(fake: FakeUpstox) -> None:
    body = b'{"status":"error","errors":[{"errorCode":"UDAPI100011"}]}'
    fake.candle_error = _http_error(400, body)


def _expired_instrument(fake: FakeUpstox) -> None:
    fake.master_has_contract = False


def _edge_blocked(fake: FakeUpstox) -> None:
    fake.candle_error = _http_error(403, b"error code: 1010")


def _provider_down(fake: FakeUpstox) -> None:
    fake.candle_error = _http_error(503, b"")


@pytest.mark.parametrize(
    ("configure", "error"),
    [
        (_invalid_instrument, "UpstoxInvalidInstrumentKeyError"),
        (_expired_instrument, "UpstoxInstrumentResolutionError"),
        (_edge_blocked, "UpstoxAccessBlockedError"),
        (_provider_down, "UpstoxProviderUnavailableError"),
    ],
    ids=["invalid-instrument", "expired-instrument", "edge-blocked", "provider-5xx"],
)
def test_provider_failures_exit_provider_without_mutation(
    tmp_path: Path, configure, error: str
) -> None:
    op = _operator(tmp_path, "provider-failure")
    op.bootstrap(20)
    before = op.facts()
    configure(op.upstox)

    failed = op.sync(21)

    assert failed.code == ExitCode.PROVIDER
    assert error in failed.err
    assert _TOKEN not in failed.out + failed.err
    assert op.facts() == before

    op.upstox = FakeUpstox(op.upstox.market)  # the provider recovers; same database
    assert op.sync(21).code == ExitCode.SUCCESS
    assert len(op.facts()["bars"]) == 21
    assert op.paper(21).code == ExitCode.SUCCESS


def test_a_calendar_failure_during_sync_stores_nothing(tmp_path: Path) -> None:
    op = _operator(tmp_path, "calendar-sync")
    op.bootstrap(20)
    before = op.facts()

    # The real NSE calendar fails closed across the un-notified 2026-11-08 Muhurat session.
    failed = op._cli(
        ["market-data", "sync", "--database", str(op.database), *_CONTRACT_ARGS,
         "--start", "2026-11-06", "--end", "2026-11-09", "--provider", "upstox"],
        env={"UPSTOX_ANALYTICS_TOKEN": _TOKEN},
        upstox_market_sync_runtime=lambda path, token: build_upstox_market_sync_runtime(
            path, token, fetch=op.upstox
        ),
    )  # fmt: skip

    assert failed.code == ExitCode.PROVIDER
    assert "FuturesSessionResolutionError" in failed.err
    assert op.upstox.candle_requests == [(_day(1), _day(20))]  # no candle request for the range
    assert op.facts() == before


def test_a_calendar_failure_during_paper_writes_no_execution_and_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    op = _operator(tmp_path, "calendar-paper")
    op.bootstrap(20)
    op.cycle(21)
    assert op.sync(22).code == ExitCode.SUCCESS

    def fail_closed(self, product, start_date, end_date):
        raise FuturesSessionResolutionError("calendar unavailable (fail closed)")

    with monkeypatch.context() as patch:
        patch.setattr(NSEFuturesTradingSessionResolver, "sessions_in_range", fail_closed)
        failed = op.paper(22)

    assert failed.code == ExitCode.PROVIDER
    assert "FuturesSessionResolutionError" in failed.err
    facts = op.facts()
    assert facts["orders"] == () and facts["fills"] == ()
    # The research decision is frozen before execution is assessed; it is evidence, not a trade.
    assert [r.decision_instant.value for r in facts["records"]] == [_close(21), _close(22)]

    recovered = op.paper(22)
    assert recovered.code == ExitCode.SUCCESS
    assert recovered.section("ORDER")[:3] == ["State: PENDING", "Side: BUY", "Contracts: 1"]
    facts = op.facts()
    assert len(facts["records"]) == 2 and len(facts["orders"]) == 1

    clean = _operator(tmp_path, "calendar-clean")
    clean.bootstrap(20)
    clean.cycle(21)
    clean.cycle(22)
    assert clean.facts()["orders"] == facts["orders"]
    assert clean.facts()["records"] == facts["records"]


# ---------------------------------------------------------------------------
# K. Expiry safety, operated incrementally (kept apart from the normal path)
# ---------------------------------------------------------------------------


def _expiry_operator(tmp_path: Path, name: str) -> Operator:
    op = _operator(tmp_path, name, _expiry_market())
    op.bootstrap(_E8_BAR - 1)
    return op


def test_the_expiry_window_operated_day_by_day(tmp_path: Path) -> None:
    op = _expiry_operator(tmp_path, "expiry-daily")
    market = op.upstox.market
    e7, e6, e5 = _E8_BAR + 1, _E8_BAR + 2, _E8_BAR + 3

    _, entry = op.cycle(_E8_BAR)
    assert entry.section("ORDER")[:3] == ["State: PENDING", "Side: BUY", "Contracts: 1"]
    assert "Expiry flatten" not in entry.out

    _, before = op.cycle(e7)
    assert before.section("ORDER") == ["State: NO ACTION", "Reason: TARGET ALREADY MET"]

    _, flatten = op.cycle(e6)
    assert flatten.section("DECISION")[0] == "Action: HOLD"
    assert flatten.section("ORDER")[:4] == [
        "State: PENDING", "Side: SELL", "Contracts: 1", "Expiry flatten: yes"
    ]  # fmt: skip

    _, settled = op.cycle(e5)
    assert settled.section("DECISION")[0] == "Action: BUY"
    assert settled.section("ORDER") == ["State: NO ACTION", "Reason: EXPIRY FLATTEN WINDOW"]
    assert settled.section("PORTFOLIO (all contracts)") == ["Flat"]

    for bar in range(e5 + 1, len(_SESSIONS) + 1):
        _, protected = op.cycle(bar)
        assert protected.section("DECISION")[0] == "Action: BUY"
        assert protected.section("ORDER") == ["State: NO ACTION", "Reason: EXPIRY FLATTEN WINDOW"]

    facts = op.facts()
    assert len(facts["orders"]) == 2 and len(facts["fills"]) == 2
    flatten_fill = facts["fills"][1]
    assert flatten_fill.filled_at.value == _close(e5)
    assert flatten_fill.fill_quote.value == market.open_(e5)
    assert (flatten_fill.side, flatten_fill.contracts) == (OrderSide.SELL, FuturesContractCount(1))

    rt = build_database_runtime(op.database)
    session = rt.paper_session_for(_CONTRACT).execute(
        _CONTRACT, _STRATEGY, _PORTFOLIO, FuturesContractCount(1), facts["bars"][-1].point_in_time
    )
    governed = {
        r.record.decision_instant.value: r.decision.expiry_flatten for r in session.run.results
    }
    assert governed[_close(e6)] is True
    assert governed[_close(e7)] is False
    [pnl] = op.valuation(len(_SESSIONS)).contracts
    assert pnl.position is None
    assert pnl.realized_pnl.amount == (market.open_(e5) - market.open_(e7)) * 65


def test_a_missed_e6_caught_up_in_order_matches_day_by_day(tmp_path: Path) -> None:
    e7, e6, e5 = _E8_BAR + 1, _E8_BAR + 2, _E8_BAR + 3
    daily = _expiry_operator(tmp_path, "expiry-daily-to-e5")
    for bar in range(_E8_BAR, e5 + 1):
        daily.cycle(bar)

    caught_up = _expiry_operator(tmp_path, "expiry-caught-up")
    caught_up.cycle(_E8_BAR)
    caught_up.cycle(e7)
    assert caught_up.sync(e6, e5).code == ExitCode.SUCCESS
    assert caught_up.paper(e6).code == caught_up.paper(e5).code == ExitCode.SUCCESS

    assert _final_facts(caught_up, e5) == _final_facts(daily, e5)


def test_skipping_the_e6_cutoff_fills_the_flatten_one_session_late(tmp_path: Path) -> None:
    """The documented unsafe operator case: pinned, not fixed."""
    e7, e6, e5, e4 = _E8_BAR + 1, _E8_BAR + 2, _E8_BAR + 3, _E8_BAR + 4
    op = _expiry_operator(tmp_path, "expiry-skipped-e6")
    market = op.upstox.market
    op.cycle(_E8_BAR)
    op.cycle(e7)
    assert op.sync(e6, e4).code == ExitCode.SUCCESS

    late = op.paper(e5)  # E-6 cutoff never run
    assert late.section("ORDER")[:4] == [
        "State: PENDING", "Side: SELL", "Contracts: 1", "Expiry flatten: yes"
    ]  # fmt: skip
    assert op.paper(e4).code == ExitCode.SUCCESS

    flatten_fill = op.facts()["fills"][1]
    assert flatten_fill.filled_at.value == _close(e4)  # not E-5: one session late
    assert flatten_fill.fill_quote.value == market.open_(e4)
    assert _close(e6) not in [r.decision_instant.value for r in op.facts()["records"]]
    assert op.paper(e6).code == ExitCode.STATE


# ---------------------------------------------------------------------------
# L. Clock-free
# ---------------------------------------------------------------------------


class _NoNow(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: D102
        raise AssertionError("datetime.now() read on the INDIA-7 manual path")

    @classmethod
    def utcnow(cls):  # noqa: D102
        raise AssertionError("datetime.utcnow() read on the INDIA-7 manual path")


def test_the_manual_path_never_reads_the_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import northstar_application.application_services.futures_expiry_flatten_guard as guard
    import northstar_infrastructure.market_data.nse_futures_session as nse
    import northstar_infrastructure.market_data.upstox_futures_native_daily_market_data as adapter
    import northstar_infrastructure.market_data.upstox_instrument_master as master

    from northstar_api import operations, runtime

    for module in (cli_module, operations, runtime, adapter, master, nse, guard):
        monkeypatch.setattr(module, "datetime", _NoNow)
    monkeypatch.setattr(cli_module, "_utc_now", _never_read_the_clock)

    op = _operator(tmp_path, "clock-free")
    op.bootstrap(20)
    for bar in (21, 22, 23):
        op.cycle(bar)
    assert op.status(23).code == ExitCode.SUCCESS
    assert len(op.facts()["fills"]) == 1


# ---------------------------------------------------------------------------
# M. Isolation
# ---------------------------------------------------------------------------


def test_the_operational_database_holds_only_its_own_identities(tmp_path: Path) -> None:
    op = _operator(tmp_path, "isolation")
    op.bootstrap(20)
    for bar in range(21, 27):
        op.cycle(bar)

    for table in ("futures_ohlcv", "futures_forward_research_records", "futures_paper_orders"):
        assert op.rows(f"SELECT DISTINCT product_code, exchange_code FROM {table}") == [
            ("NIFTY", "NSE")
        ]
    assert op.rows("SELECT DISTINCT strategy_identity FROM futures_forward_research_records") == [
        (_STRATEGY.identity,)
    ]
    assert op.rows(
        "SELECT DISTINCT strategy_identity, portfolio_identity FROM futures_paper_orders"
    ) == [(_STRATEGY.identity, _PORTFOLIO.identity)]
    with closing(sqlite3.connect(op.database)) as connection:
        dump = "\n".join(connection.iterdump())
    for forbidden in (
        "'ES'", "'CME'", "USD", "directional-mvp-v1-nifty-baseline", "nifty-baseline-oct26"
    ):  # fmt: skip
        assert forbidden not in dump


def test_the_network_is_refused_and_only_the_double_is_used(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match="must not touch the network"):
        socket.create_connection(("api.upstox.com", 443))
    with pytest.raises(AssertionError, match="must not touch the network"):
        upstox_http.default_fetch("https://api.upstox.com/v3/x", {}, 1.0)

    op = _operator(tmp_path, "transport")
    op.bootstrap(20)
    op.sync(21)
    assert op.upstox.candle_requests == [(_day(1), _day(20)), (_day(21), _day(21))]
