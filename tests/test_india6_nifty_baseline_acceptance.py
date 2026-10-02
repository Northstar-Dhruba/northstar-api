"""Acceptance: INDIA-6 NIFTY Futures baseline research evidence, on synthetic data.

The existing built-in futures directional strategy is run, unmodified, against
the exact dated contract NIFTY / NSE / 2026-10-27 under its own baseline
identity, through the merged production components:

    market data   ``market-data sync --provider upstox`` -> Upstox adapter (HTTP
                  faked) -> native-daily acquisition -> NSE calendar -> SQLite
    research      RunFuturesHistoricalResearchUseCase over the SQLite repository
                  (there is no research CLI)
    paper         ``economics set`` and ``paper run`` / ``paper status`` through
                  the production runtime, plus the runtime's own valuation

The prices are synthetic and committed; no real Upstox data is. The sessions
are the real NSE sessions from 2026-07-29, the contract's first listed day.

Synthetic path, designed around the frozen rule (latest 20 bars; BUY when the
close rose, the 5-bar average is above the 20-bar average and volume is at
least its 20-bar average; SELL on the mirror image; otherwise HOLD):

    bars  1..15   flat at 25000, volume 1000            warm-up, no decision
    bars 16..20   rising 25010 .. 25050                 bar 20 decides BUY
    bar  21       unchanged 25050                       HOLD
    bar  22       falls to 22000 on volume 3000         SELL
    bars 23..25   22000, 22100, 22100                   HOLD
    bars 26..30   extreme values, stored but always after the evaluation
                  cutoff (bar 25): they must never influence anything

Each bar opens three points above the previous close, so a fill's price shows
which bar it came from. The last decision (bar 25, early September) is weeks
before the E-6 expiry trigger on 2026-10-16, so the pre-expiry guard never acts
here; INDIA-5 owns that acceptance.

P&L is gross simulated P&L at 65 INR per quote point per contract: no fees,
slippage or other costs.
"""

from __future__ import annotations

import gzip
import io
import json
import socket
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import (
    CalculateFuturesPaperTradingMetricsUseCase,
    FuturesPaperExecutionIdentityService,
    RunFuturesHistoricalResearchUseCase,
)
from northstar_application.ports import (
    FuturesForwardResearchRecordQuery,
    FuturesHistoricalMarketDataQuery,
    FuturesPaperFillQuery,
    FuturesPaperOrderQuery,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    PointInTime,
    Symbol,
    Timeframe,
)
from northstar_core.futures import (
    FuturesContract,
    FuturesContractEconomics,
    FuturesPointValue,
    FuturesProductReference,
)
from northstar_core.paper_trading import (
    FuturesContractCount,
    OrderSide,
    PaperPortfolioIdentity,
)
from northstar_core.strategy import (
    FuturesAssetAnalysisGenerator,
    ResearchHorizon,
    Strategy,
    StrategyIdentity,
)
from northstar_infrastructure.market_data import (
    NSEFuturesTradingSessionResolver,
    SQLiteFuturesHistoricalMarketDataRepository,
    upstox_http,
)
from northstar_infrastructure.market_data.upstox_instrument_master import (
    NSE_INSTRUMENT_MASTER_URL,
)

from northstar_api.cli import ExitCode, main
from northstar_api.runtime import build_database_runtime, build_upstox_market_sync_runtime

_NIFTY = FuturesProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_CONTRACT = FuturesContract(_NIFTY, ExpirationDate("2026-10-27"))
_STRATEGY = StrategyIdentity("directional-mvp-v1-nifty-baseline")
_PORTFOLIO = PaperPortfolioIdentity("nifty-baseline-oct26")
_DAILY = Timeframe("1d")
_HORIZONS = (ResearchHorizon(1), ResearchHorizon(5))
_POINT_VALUE = Decimal("65")
_INR = Currency("INR")
_LOT = 65
_TARGET = 1
_PLACEHOLDER_TOKEN = "placeholder-not-a-credential"

# The first 30 real NSE sessions of the contract's life.
_SESSIONS = NSEFuturesTradingSessionResolver().sessions_in_range(
    _NIFTY, datetime(2026, 7, 29).date(), datetime(2026, 9, 30).date()
)[:30]

_CLOSES = (
    [Decimal(25000)] * 15
    + [Decimal(v) for v in (25010, 25020, 25030, 25040, 25050)]
    + [Decimal(v) for v in (25050, 22000, 22000, 22100, 22100)]
    + [Decimal(v) for v in (40000, 9000, 41000, 8000, 42000)]  # future poison, bars 26..30
)
_VOLUMES = [1000] * 21 + [3000] + [1000] * 3 + [900000] * 5
_OPENS = [Decimal(25003)] + [close + 3 for close in _CLOSES[:-1]]
_CUTOFF_BAR = 25
_EXPECTED_ACTIONS = ("BUY", "HOLD", "SELL", "HOLD", "HOLD", "HOLD")  # bars 20..25


def _bar_number(instant: PointInTime) -> int:
    return next(i for i, s in enumerate(_SESSIONS, start=1) if s.closes_at == instant)


def _close_instant(bar: int) -> PointInTime:
    return _SESSIONS[bar - 1].closes_at


# ---------------------------------------------------------------------------
# No network: everything below must run without contacting any provider
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def no_network():
    """Module-scoped, so it is already in force while the shared baseline is built."""
    import databento

    def refuse(*args, **kwargs):
        raise AssertionError("INDIA-6 acceptance must not touch the network")

    patcher = pytest.MonkeyPatch()
    patcher.setattr(socket.socket, "connect", refuse)
    patcher.setattr(socket, "create_connection", refuse)
    patcher.setattr(upstox_http, "urlopen", refuse)
    patcher.setattr(databento, "Historical", refuse)
    yield
    patcher.undo()


# ---------------------------------------------------------------------------
# Upstox HTTP double (documented v3 shape) feeding the production sync
# ---------------------------------------------------------------------------

_IST = timezone(timedelta(hours=5, minutes=30))


def _master() -> bytes:
    expiry = datetime(2026, 10, 27, 23, 59, 59, tzinfo=_IST)
    record = {
        "segment": "NSE_FO",
        "exchange": "NSE",
        "instrument_type": "FUT",
        "underlying_symbol": "NIFTY",
        "expiry": int((expiry - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()) * 1000,
        "instrument_key": "NSE_FO|48704",
        "lot_size": _LOT,
        "trading_symbol": "NIFTY FUT 27 OCT 26",
    }
    return gzip.compress(json.dumps([record]).encode("utf-8"))


def _candles(bars: int) -> bytes:
    rows = []
    for index in range(bars):
        open_, close = _OPENS[index], _CLOSES[index]
        rows.append(
            [
                f"{_SESSIONS[index].trading_date.isoformat()}T00:00:00+05:30",
                int(open_),
                int(max(open_, close) + 5),
                int(min(open_, close) - 5),
                int(close),
                _VOLUMES[index] * _LOT,  # Upstox reports underlying units
                0,
            ]
        )
    rows.reverse()  # newest first, as Upstox returns them
    return json.dumps({"status": "success", "data": {"candles": rows}}).encode("utf-8")


class FakeUpstox:
    def __init__(self, bars: int) -> None:
        self.bars = bars
        self.urls: list[str] = []

    def __call__(self, url: str, headers, timeout: float) -> bytes:
        self.urls.append(url)
        if url == NSE_INSTRUMENT_MASTER_URL:
            return _master()
        return _candles(self.bars)


# ---------------------------------------------------------------------------
# Command helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Outcome:
    code: int
    out: str
    err: str


def _cli(argv: list[str], **kwargs) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    env = kwargs.pop("env", {})
    code = main(argv, env=env, stdout=out, stderr=err, **kwargs)
    return Outcome(code, out.getvalue(), err.getvalue())


_CONTRACT_ARGS = ["--product", "NIFTY", "--exchange", "NSE", "--expiration", "2026-10-27"]


def _prepare(database: Path, bars: int = 30) -> FakeUpstox:
    """Sync ``bars`` sessions through the production Upstox path and set economics."""
    fake = FakeUpstox(bars)
    synced = _cli(
        [
            "market-data", "sync", "--database", str(database), *_CONTRACT_ARGS,
            "--start", _SESSIONS[0].trading_date.isoformat(),
            "--end", _SESSIONS[bars - 1].trading_date.isoformat(),
            "--provider", "upstox",
        ],
        env={"UPSTOX_ANALYTICS_TOKEN": _PLACEHOLDER_TOKEN},
        upstox_market_sync_runtime=lambda path, token: build_upstox_market_sync_runtime(
            path, token, fetch=fake
        ),
    )  # fmt: skip
    assert synced.code == ExitCode.SUCCESS, synced.err
    economics = _cli(
        [
            "economics", "set", "--database", str(database), *_CONTRACT_ARGS,
            "--point-value", "65", "--currency", "INR",
        ]
    )  # fmt: skip
    assert economics.code == ExitCode.SUCCESS, economics.err
    return fake


def _paper_run(database: Path, bar: int) -> Outcome:
    return _cli(
        [
            "paper", "run", "--database", str(database), *_CONTRACT_ARGS,
            "--strategy", _STRATEGY.identity, "--portfolio", _PORTFOLIO.identity,
            "--target", str(_TARGET), "--as-of", _close_instant(bar).value,
        ]
    )  # fmt: skip


def _backfill(database: Path, first: int = 19, last: int = _CUTOFF_BAR) -> dict[int, Outcome]:
    """One paper run per session close, in order, as the real-data runbook does."""
    outcomes = {}
    for bar in range(first, last + 1):
        outcomes[bar] = _paper_run(database, bar)
        assert outcomes[bar].code in (ExitCode.SUCCESS, ExitCode.DATA), outcomes[bar].err
    return outcomes


def _research(database: Path, cutoff_bar: int = _CUTOFF_BAR):
    return RunFuturesHistoricalResearchUseCase(
        SQLiteFuturesHistoricalMarketDataRepository(database), FuturesAssetAnalysisGenerator()
    ).execute(_CONTRACT, _DAILY, Strategy(_STRATEGY), _HORIZONS, _close_instant(cutoff_bar))


def _rows(database: Path, sql: str) -> list[tuple]:
    # closing(): a sqlite3 connection's own context manager commits but never closes.
    with closing(sqlite3.connect(database)) as connection:
        return connection.execute(sql).fetchall()


def _expected_action(bar: int) -> str:
    """Independent restatement of the frozen rule, for the oracle only."""
    closes = _CLOSES[bar - 20 : bar]
    volumes = [Decimal(v) for v in _VOLUMES[bar - 20 : bar]]
    short, long_ = sum(closes[-5:]) / 5, sum(closes) / 20
    elevated = volumes[-1] >= sum(volumes) / 20
    latest, previous = _CLOSES[bar - 1], _CLOSES[bar - 2]
    if latest > previous and short > long_ and elevated:
        return "BUY"
    if latest < previous and short < long_ and elevated:
        return "SELL"
    return "HOLD"


@pytest.fixture(scope="module")
def baseline(tmp_path_factory, no_network) -> dict:
    """One dedicated baseline database: 30 bars synced, paper backfilled to bar 25."""
    database = tmp_path_factory.mktemp("india6") / "nifty-baseline.sqlite3"
    fake = _prepare(database)
    outcomes = _backfill(database)
    return {"database": database, "fake": fake, "paper": outcomes}


# ---------------------------------------------------------------------------
# Data entered through the production path, for the exact contract
# ---------------------------------------------------------------------------


def test_the_synthetic_history_is_the_exact_contract_on_real_nse_sessions(baseline) -> None:
    database = baseline["database"]
    bars = SQLiteFuturesHistoricalMarketDataRepository(database).get_bars(
        FuturesHistoricalMarketDataQuery(_CONTRACT, _DAILY)
    )

    assert len(bars) == 30
    assert [bar.point_in_time for bar in bars] == [s.closes_at for s in _SESSIONS]
    assert [bar.close.value for bar in bars] == _CLOSES
    assert [bar.open.value for bar in bars] == _OPENS
    assert [int(bar.volume.value) for bar in bars] == _VOLUMES  # normalised to contracts
    assert _SESSIONS[0].trading_date.isoformat() == "2026-07-29"


def test_the_baseline_stays_clear_of_the_expiry_trigger() -> None:
    last_decision = _SESSIONS[_CUTOFF_BAR - 1].trading_date
    e6 = datetime(2026, 10, 16).date()

    assert last_decision < e6
    assert (e6 - last_decision).days > 30


# ---------------------------------------------------------------------------
# 1. Warm-up
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cutoff", [1, 10, 19])
def test_no_research_decision_before_bar_20(baseline, cutoff: int) -> None:
    run = _research(baseline["database"], cutoff)

    assert run.analysis_results == ()
    assert run.outcomes == ()


def test_bar_20_is_the_first_research_decision(baseline) -> None:
    run = _research(baseline["database"], 20)

    assert len(run.analysis_results) == 1
    assert run.analysis_results[0].recommendation.point_in_time == _close_instant(20)


def test_the_paper_run_before_bar_20_freezes_nothing(baseline) -> None:
    warm_up = baseline["paper"][19]

    assert "Decision: unavailable" in warm_up.out
    assert "insufficient persisted daily history / warm-up" in warm_up.out
    instants = [
        record.decision_instant
        for record in build_database_runtime(baseline["database"]).forward_repository.get_records(
            FuturesForwardResearchRecordQuery(_CONTRACT, _DAILY)
        )
    ]
    assert _close_instant(19) not in instants


# ---------------------------------------------------------------------------
# 5. Signal activity, against the frozen rule
# ---------------------------------------------------------------------------


def test_the_baseline_produces_buy_sell_and_hold(baseline) -> None:
    run = _research(baseline["database"])
    actions = tuple(r.recommendation.action.value for r in run.analysis_results)

    assert actions == _EXPECTED_ACTIONS
    assert actions == tuple(_expected_action(bar) for bar in range(20, _CUTOFF_BAR + 1))
    assert {"BUY", "SELL", "HOLD"} <= set(actions)


# ---------------------------------------------------------------------------
# 3 & 11. Point-in-time correctness, pinned explicitly
# ---------------------------------------------------------------------------


def test_each_decision_sees_exactly_its_own_trailing_twenty_bars(baseline) -> None:
    for result in _research(baseline["database"]).analysis_results:
        context = result.market_observation_context
        bar = _bar_number(context.observed_at)

        assert result.recommendation.point_in_time == _close_instant(bar)
        assert context.latest_quote.value == _CLOSES[bar - 1]
        assert context.previous_close.value == _CLOSES[bar - 2]
        assert [q.value for q in context.recent_closes] == _CLOSES[bar - 20 : bar]
        assert [int(v.value) for v in context.recent_volumes] == _VOLUMES[bar - 20 : bar]
        assert int(context.latest_volume.value) == _VOLUMES[bar - 1]


def test_stored_future_bars_never_change_a_run_at_an_earlier_cutoff(tmp_path: Path) -> None:
    """The same cutoff over a store holding only bars 1..25 and over one holding 1..30."""
    short, long_ = tmp_path / "short.sqlite3", tmp_path / "long.sqlite3"
    _prepare(short, bars=25)
    _prepare(long_, bars=30)

    assert _research(short) == _research(long_)


def test_a_later_cutoff_never_alters_an_earlier_decision(baseline) -> None:
    at_cutoff = _research(baseline["database"], _CUTOFF_BAR).analysis_results
    later = _research(baseline["database"], 30).analysis_results

    assert later[: len(at_cutoff)] == at_cutoff
    assert len(later) == len(at_cutoff) + 5


def test_outcomes_use_only_strictly_later_bars_within_the_cutoff(baseline) -> None:
    run = _research(baseline["database"])
    cutoff = run.available_through

    for outcome in run.outcomes:
        decision_bar = _bar_number(outcome.decision_instant)
        target_bar = decision_bar + outcome.horizon.observations
        if target_bar <= _CUTOFF_BAR:
            assert outcome.evaluation_instant == _close_instant(target_bar)
            assert outcome.evaluation_quote.value == _CLOSES[target_bar - 1]
            assert outcome.evaluation_instant.compare(outcome.decision_instant) > 0
            assert outcome.evaluation_instant.compare(cutoff) <= 0
        else:
            assert outcome.evaluation_instant is None
            assert outcome.unavailable_reason.value == "INSUFFICIENT_FUTURE_OBSERVATIONS"
        assert outcome.decision_quote.value == _CLOSES[decision_bar - 1]


def test_paper_valuation_ignores_stored_future_bars(baseline) -> None:
    """Bars 26..30 are stored; the mark at the bar-25 cutoff is still bar 25's close."""
    valuation = build_database_runtime(baseline["database"]).valuation.execute(
        _PORTFOLIO, _STRATEGY, _close_instant(_CUTOFF_BAR)
    )
    [pnl] = valuation.contracts

    assert pnl.mark_instant == _close_instant(_CUTOFF_BAR)
    assert pnl.mark_quote.value == _CLOSES[_CUTOFF_BAR - 1]


# ---------------------------------------------------------------------------
# 4. Research and paper agree decision by decision
# ---------------------------------------------------------------------------


def test_frozen_paper_decisions_match_historical_research(baseline) -> None:
    database = baseline["database"]
    research = {
        r.recommendation.point_in_time: r.recommendation.action.value
        for r in _research(database).analysis_results
    }
    frozen = {
        record.decision_instant: record.result.recommendation.action.value
        for record in build_database_runtime(database).forward_repository.get_records(
            FuturesForwardResearchRecordQuery(_CONTRACT, _DAILY)
        )
        if record.strategy_identity == _STRATEGY
    }

    assert frozen == research
    assert len(frozen) == 6


# ---------------------------------------------------------------------------
# 6. Paper execution semantics
# ---------------------------------------------------------------------------


def _orders_and_fills(database: Path):
    rt = build_database_runtime(database)
    orders = rt.order_repository.get_orders(FuturesPaperOrderQuery(_PORTFOLIO))
    fills = rt.fill_repository.get_fills(FuturesPaperFillQuery(_PORTFOLIO))
    return rt, orders, fills


def test_intents_follow_the_target_position_rule(baseline) -> None:
    _, orders, _ = _orders_and_fills(baseline["database"])

    assert [(o.intent.decided_at, o.intent.side, o.intent.contracts) for o in orders] == [
        (_close_instant(20), OrderSide.BUY, FuturesContractCount(1)),  # BUY -> +1
        (_close_instant(22), OrderSide.SELL, FuturesContractCount(2)),  # SELL -> -1 from +1
    ]


def test_each_fill_is_the_open_of_the_next_stored_bar(baseline) -> None:
    _, orders, fills = _orders_and_fills(baseline["database"])

    assert len(fills) == 2
    for order, fill in zip(orders, fills, strict=True):
        next_bar = _bar_number(order.intent.decided_at) + 1
        assert fill.order_identity == order.identity
        assert fill.filled_at == _close_instant(next_bar)
        assert fill.fill_quote.value == _OPENS[next_bar - 1]
        assert fill.fill_quote.value != _CLOSES[next_bar - 1]


def test_order_identities_are_the_ordinary_deterministic_ones(baseline) -> None:
    rt, orders, _ = _orders_and_fills(baseline["database"])
    records = {
        r.decision_instant: r
        for r in rt.forward_repository.get_records(
            FuturesForwardResearchRecordQuery(_CONTRACT, _DAILY)
        )
    }
    service = FuturesPaperExecutionIdentityService()

    for order in orders:
        assert order.identity == service.order_identity(
            records[order.intent.decided_at], _PORTFOLIO
        )


def test_no_decision_in_the_baseline_is_expiry_governed(baseline) -> None:
    rt = build_database_runtime(baseline["database"])
    session = rt.paper_session_for(_CONTRACT).execute(
        _CONTRACT, _STRATEGY, _PORTFOLIO, FuturesContractCount(_TARGET), _close_instant(25)
    )

    for result in session.run.results:
        assert not result.decision.expiry_flatten
        assert result.expiry_window is not None
        assert not result.expiry_window.flatten_required


# ---------------------------------------------------------------------------
# 7. Economics and P&L, recomputed independently
# ---------------------------------------------------------------------------


def test_economics_are_65_inr_for_the_exact_contract(baseline) -> None:
    economics = build_database_runtime(baseline["database"]).economics_repository.get_economics(
        _CONTRACT
    )

    assert economics == FuturesContractEconomics(_CONTRACT, FuturesPointValue(_POINT_VALUE, _INR))


def _independent_pnl(fills, mark: Decimal) -> tuple[Decimal, Decimal, int]:
    """Average-entry fold over the fills, then a mark-to-close; plain Decimal maths."""
    net, average, realized_points = 0, Decimal(0), Decimal(0)
    for fill in fills:
        signed = fill.contracts.value if fill.side is OrderSide.BUY else -fill.contracts.value
        quote = fill.fill_quote.value
        if net == 0 or (net > 0) == (signed > 0):
            average = (average * abs(net) + quote * abs(signed)) / (abs(net) + abs(signed))
            net += signed
            continue
        closing = min(abs(net), abs(signed))
        realized_points += (quote - average) * (1 if net > 0 else -1) * closing
        remainder = net + signed
        if remainder == 0 or (remainder > 0) == (net > 0):
            net = remainder
        else:
            net, average = remainder, quote
    unrealized_points = (mark - average) * net
    return realized_points * _POINT_VALUE, unrealized_points * _POINT_VALUE, net


def test_valuation_matches_an_independent_recomputation(baseline) -> None:
    rt, _, fills = _orders_and_fills(baseline["database"])
    mark = _CLOSES[_CUTOFF_BAR - 1]

    realized, unrealized, net = _independent_pnl(fills, mark)
    [pnl] = rt.valuation.execute(_PORTFOLIO, _STRATEGY, _close_instant(_CUTOFF_BAR)).contracts

    # Long 1 at 25053 closed at 22003, then short 1 from 22003 marked at 22100.
    assert realized == (Decimal(22003) - Decimal(25053)) * 65 == Decimal(-198250)
    assert unrealized == (Decimal(22100) - Decimal(22003)) * -1 * 65 == Decimal(-6305)
    assert net == -1
    assert pnl.contract == _CONTRACT
    assert pnl.realized_pnl.amount == realized
    assert pnl.realized_pnl.currency == _INR
    assert pnl.unrealized_pnl.amount == unrealized
    assert pnl.unrealized_pnl.currency == _INR
    assert pnl.position.net_contracts == net


def test_paper_status_reports_the_same_pnl(baseline) -> None:
    status = _cli(
        [
            "paper", "status", "--database", str(baseline["database"]),
            "--strategy", _STRATEGY.identity, "--portfolio", _PORTFOLIO.identity,
            "--as-of", _close_instant(_CUTOFF_BAR).value,
        ]
    )  # fmt: skip

    assert status.code == ExitCode.SUCCESS, status.err
    assert "Realized P&L: -198250 INR" in status.out
    assert "Unrealized P&L: -6305 INR" in status.out


# ---------------------------------------------------------------------------
# 10. Baseline metrics and evidence
# ---------------------------------------------------------------------------


def test_the_baseline_evidence_summary(baseline) -> None:
    database = baseline["database"]
    research = _research(database)
    actions = [r.recommendation.action.value for r in research.analysis_results]
    rt = build_database_runtime(database)
    session = rt.paper_session_for(_CONTRACT).execute(
        _CONTRACT, _STRATEGY, _PORTFOLIO, FuturesContractCount(_TARGET), _close_instant(25)
    )
    [metrics] = CalculateFuturesPaperTradingMetricsUseCase().execute(session.run)
    [pnl] = rt.valuation.execute(_PORTFOLIO, _STRATEGY, _close_instant(25)).contracts

    evidence = {
        "contract": str(research.contract),
        "strategy": research.strategy_identity.identity,
        "sample": (
            research.analysis_results[0].recommendation.point_in_time.value,
            research.available_through.value,
        ),
        "decisions": len(actions),
        "buy": actions.count("BUY"),
        "sell": actions.count("SELL"),
        "hold": actions.count("HOLD"),
        "orders": metrics.order_count,
        "filled_orders": metrics.filled_order_count,
        "net_contracts": metrics.current_net_contracts,
        "realized_inr": pnl.realized_pnl.amount,
        "unrealized_inr": pnl.unrealized_pnl.amount,
    }

    assert evidence == {
        "contract": "NIFTY@NSE 2026-10-27",
        "strategy": "directional-mvp-v1-nifty-baseline",
        "sample": (_close_instant(20).value, _close_instant(25).value),
        "decisions": 6,
        "buy": 1,
        "sell": 1,
        "hold": 4,
        "orders": 2,
        "filled_orders": 2,
        "net_contracts": -1,
        "realized_inr": Decimal(-198250),
        "unrealized_inr": Decimal(-6305),
    }
    assert metrics.decision_count == 6
    assert metrics.hold_count == 4
    assert metrics.target_already_met_count == 0
    assert metrics.expiry_window_count == 0


def test_research_metrics_cover_both_horizons(baseline) -> None:
    from northstar_application.application_services import (
        CalculateFuturesHistoricalResearchMetricsUseCase,
    )

    metrics = CalculateFuturesHistoricalResearchMetricsUseCase().execute(
        _research(baseline["database"])
    )

    by_horizon = {m.horizon.observations: m for m in metrics}
    assert set(by_horizon) == {1, 5}
    assert by_horizon[1].total_count == by_horizon[5].total_count == 6
    assert by_horizon[1].measured_count == 5  # bar 25 has no later bar by the cutoff
    assert by_horizon[5].measured_count == 1  # only bar 20 reaches bar 25
    assert all(m.strategy_identity == _STRATEGY for m in metrics)


# ---------------------------------------------------------------------------
# 2. Determinism and idempotency
# ---------------------------------------------------------------------------


def test_two_research_runs_over_the_same_bars_are_equal(baseline) -> None:
    assert _research(baseline["database"]) == _research(baseline["database"])


def test_rerunning_the_final_paper_cutoff_adds_nothing(baseline) -> None:
    database = baseline["database"]
    before = _orders_and_fills(database)[1:]

    again = _paper_run(database, _CUTOFF_BAR)

    assert again.code == ExitCode.SUCCESS
    assert again.out == baseline["paper"][_CUTOFF_BAR].out
    assert _orders_and_fills(database)[1:] == before


def test_an_independent_rebuild_produces_identical_facts(baseline, tmp_path: Path) -> None:
    rebuilt = tmp_path / "rebuilt.sqlite3"
    _prepare(rebuilt)
    outcomes = _backfill(rebuilt)

    assert _orders_and_fills(rebuilt)[1:] == _orders_and_fills(baseline["database"])[1:]
    assert {bar: o.out for bar, o in outcomes.items()} == {
        bar: o.out for bar, o in baseline["paper"].items()
    }
    assert _research(rebuilt) == _research(baseline["database"])


# ---------------------------------------------------------------------------
# 8. Isolation
# ---------------------------------------------------------------------------


def test_only_the_baseline_identities_and_contract_are_persisted(baseline) -> None:
    database = baseline["database"]

    for table in (
        "futures_ohlcv",
        "futures_forward_research_records",
        "futures_paper_orders",
    ):
        assert _rows(database, f"SELECT DISTINCT product_code, exchange_code FROM {table}") == [
            ("NIFTY", "NSE")
        ]
    assert _rows(
        database, "SELECT DISTINCT strategy_identity FROM futures_forward_research_records"
    ) == [("directional-mvp-v1-nifty-baseline",)]
    assert _rows(database, "SELECT DISTINCT strategy_identity FROM futures_paper_orders") == [
        ("directional-mvp-v1-nifty-baseline",)
    ]
    assert _rows(database, "SELECT DISTINCT portfolio_identity FROM futures_paper_orders") == [
        ("nifty-baseline-oct26",)
    ]


def test_no_es_or_cme_value_reaches_the_baseline_store(baseline) -> None:
    with closing(sqlite3.connect(baseline["database"])) as connection:
        dump = "\n".join(connection.iterdump())

    for forbidden in ("'ES'", "'CME'", "USD", "futures-paper-alpha"):
        assert forbidden not in dump


# ---------------------------------------------------------------------------
# 12. No network
# ---------------------------------------------------------------------------


def test_only_the_faked_upstox_transport_was_used(baseline) -> None:
    assert baseline["fake"].urls == [
        NSE_INSTRUMENT_MASTER_URL,
        "https://api.upstox.com/v3/historical-candle/NSE_FO%7C48704/days/1/"
        f"{_SESSIONS[29].trading_date.isoformat()}/{_SESSIONS[0].trading_date.isoformat()}",
    ]


def test_the_network_is_refused_inside_this_module() -> None:
    with pytest.raises(AssertionError, match="must not touch the network"):
        socket.create_connection(("api.upstox.com", 443))
    with pytest.raises(AssertionError, match="must not touch the network"):
        upstox_http.default_fetch("https://api.upstox.com/v3/x", {}, 1.0)
