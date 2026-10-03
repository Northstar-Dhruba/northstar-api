"""Read-only futures analysis read model for the configured contract.

Everything is derived on each request, at one explicit cutoff, from persisted
facts: CalculateFuturesAnalysisUseCase supplies paper performance and
historical research, and the venue's pre-expiry guard classifies each frozen
paper decision. Nothing is persisted, no provider is contacted and no clock is
read.

Expiry classification is composed here rather than in the Application
analysis because only the composition knows a contract's venue calendar. Each
frozen decision of the configured strategy through the cutoff is assessed by
the guard and counted as outside the window, the flatten decision, or inside
the protected window -- the dashboard's own classification. A decision the
calendar cannot place is counted as unresolved, never as outside.

Values are reported, not judged: there is no score, threshold or verdict.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from northstar_application.application_services import (
    FuturesAnalysis,
    FuturesExpiryWindowError,
    FuturesPaperCompletedTrade,
    FuturesPaperOpenExposure,
    FuturesPaperPerformance,
)
from northstar_application.ports import (
    FuturesForwardResearchRecordQuery,
    FuturesSessionResolutionError,
)
from northstar_core.foundation.value_objects import Money, PointInTime, Timeframe
from northstar_core.futures import FuturesContract
from northstar_core.strategy import ResearchHorizon

from northstar_api.operational_status import _window
from northstar_api.runtime import DatabaseRuntime, expiry_guard_for
from northstar_api.schemas.futures import FuturesContractResponse
from northstar_api.schemas.futures_analysis import (
    AnalysisResearchResponse,
    CompletedTradeResponse,
    DecisionActivityResponse,
    DrawdownResponse,
    EquityPointResponse,
    ExecutionActivityResponse,
    ExpiryAnalysisResponse,
    MoneyResponse,
    OpenExposureResponse,
    PaperAnalysisResponse,
    PortfolioAnalysisResponse,
    ResearchActionResponse,
    ResearchHorizonResponse,
    TradesResponse,
    TradeStatisticsResponse,
)
from northstar_api.settings import DashboardSettings

# The frozen baseline horizons; the Application analysis takes them as input.
ANALYSIS_HORIZONS = (ResearchHorizon(1), ResearchHorizon(5))
_DAILY = Timeframe("1d")
_MICROSECOND = timedelta(microseconds=1)


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------


def _contract(contract: FuturesContract) -> FuturesContractResponse:
    return FuturesContractResponse(
        product=contract.product.product_code.value,
        exchange=contract.product.exchange_code.value,
        expiration=contract.expiration_date.value,
    )


def _money(money: Money | None) -> MoneyResponse | None:
    if money is None:
        return None
    return MoneyResponse(amount=str(money.amount), currency=money.currency.value)


def _text(value) -> str | None:
    return None if value is None else str(value)


def _seconds(duration: timedelta | None) -> str | None:
    """Exact elapsed seconds, e.g. ``"172800"`` or ``"0.25"``."""
    if duration is None:
        return None
    seconds, micro = divmod(duration // _MICROSECOND, 1_000_000)
    return str(seconds) if not micro else f"{seconds}.{micro:06d}".rstrip("0")


# ---------------------------------------------------------------------------
# Paper
# ---------------------------------------------------------------------------


def _trade(trade: FuturesPaperCompletedTrade) -> CompletedTradeResponse:
    return CompletedTradeResponse(
        direction=trade.direction.value,
        opened_at=trade.opened_at.value,
        closed_at=trade.closed_at.value,
        contracts=str(trade.contracts),
        closing_average_entry=str(trade.closing_average_entry.value),
        average_exit=str(trade.average_exit.value),
        realized_pnl=_money(trade.realized_pnl),
        holding_seconds=_seconds(trade.holding_duration),
    )


def _open(exposure: FuturesPaperOpenExposure | None) -> OpenExposureResponse | None:
    if exposure is None:
        return None
    return OpenExposureResponse(
        direction=exposure.direction.value,
        net_contracts=str(exposure.position.net_contracts),
        average_entry=str(exposure.position.average_entry.value),
        opened_at=exposure.opened_at.value,
        realized_pnl=_money(exposure.realized_pnl),
        mark_quote=_text(exposure.mark_quote.value if exposure.mark_quote else None),
        mark_instant=exposure.mark_instant.value if exposure.mark_instant else None,
        unrealized_pnl=_money(exposure.unrealized_pnl),
    )


def _paper(performance: FuturesPaperPerformance) -> PaperAnalysisResponse:
    decisions, execution, stats = (
        performance.decisions,
        performance.execution,
        performance.statistics,
    )
    exposure = performance.open_exposure
    if exposure is None:
        unrealized = Money(Decimal(0), performance.settlement_currency)  # flat: zero
    else:
        unrealized = exposure.unrealized_pnl  # None when unmarked, never zero
    drawdown = performance.max_drawdown
    reason = stats.profit_factor_unavailable_reason
    return PaperAnalysisResponse(
        status="available",
        reason=None,
        missing_contract=None,
        decisions=DecisionActivityResponse(
            total=str(decisions.decision_count),
            buy=str(decisions.buy_count),
            sell=str(decisions.sell_count),
            hold=str(decisions.hold_count),
        ),
        execution=ExecutionActivityResponse(
            order_count=str(execution.order_count),
            fill_count=str(execution.fill_count),
            contracts_bought=str(execution.contracts_bought),
            contracts_sold=str(execution.contracts_sold),
            turnover_contracts=str(execution.turnover_contracts),
            net_contracts=str(execution.net_contracts),
        ),
        trades=TradesResponse(
            statistics=TradeStatisticsResponse(
                completed_count=str(stats.completed_count),
                winning_count=str(stats.winning_count),
                losing_count=str(stats.losing_count),
                breakeven_count=str(stats.breakeven_count),
                win_rate=_text(stats.win_rate),
                completed_realized_pnl=_money(stats.total_realized_pnl),
                average_trade_pnl=_money(stats.average_trade_pnl),
                gross_profit=_money(stats.gross_profit),
                gross_loss=_money(stats.gross_loss),
                profit_factor=_text(stats.profit_factor),
                profit_factor_unavailable_reason=reason.value if reason else None,
                minimum_holding_seconds=_seconds(stats.minimum_holding_duration),
                maximum_holding_seconds=_seconds(stats.maximum_holding_duration),
                average_holding_seconds=_seconds(stats.average_holding_duration),
                median_holding_seconds=_seconds(stats.median_holding_duration),
            ),
            completed=tuple(_trade(trade) for trade in performance.completed_trades),
        ),
        portfolio=PortfolioAnalysisResponse(
            canonical_realized_pnl=_money(performance.realized_pnl),
            unrealized_pnl=_money(unrealized),
            open_exposure=_open(exposure),
        ),
        drawdown=(
            DrawdownResponse(
                amount=_money(drawdown.amount),
                peak_pnl=_money(drawdown.peak_pnl),
                peak_instant=drawdown.peak_instant.value,
                trough_pnl=_money(drawdown.trough_pnl),
                trough_instant=drawdown.trough_instant.value,
            )
            if drawdown is not None
            else None
        ),
    )


def _paper_unavailable(
    reason: str, missing: FuturesContract | None = None
) -> PaperAnalysisResponse:
    return PaperAnalysisResponse(
        status="unavailable",
        reason=reason,
        missing_contract=_contract(missing) if missing is not None else None,
        decisions=None,
        execution=None,
        trades=None,
        portfolio=None,
        drawdown=None,
    )


def _equity_curve(performance: FuturesPaperPerformance | None) -> tuple[EquityPointResponse, ...]:
    if performance is None:
        return ()
    return tuple(
        EquityPointResponse(
            instant=point.instant.value,
            mark_quote=str(point.mark_quote.value),
            net_contracts=str(point.net_contracts),
            realized_pnl=_money(point.realized_pnl),
            unrealized_pnl=_money(point.unrealized_pnl),
            total_pnl=_money(point.total_pnl),
        )
        for point in performance.equity_curve
    )


# ---------------------------------------------------------------------------
# Research
# ---------------------------------------------------------------------------


def _horizon(metrics) -> ResearchHorizonResponse:
    def percent(value):
        return None if value is None else str(value.value)

    return ResearchHorizonResponse(
        horizon=str(metrics.horizon.observations),
        total_count=str(metrics.total_count),
        measured_count=str(metrics.measured_count),
        insufficient_future_observations_count=str(metrics.insufficient_future_observations_count),
        undefined_return_basis_count=str(metrics.undefined_return_basis_count),
        average_forward_return=percent(metrics.average_forward_return),
        median_forward_return=percent(metrics.median_forward_return),
        minimum_forward_return=percent(metrics.minimum_forward_return),
        maximum_forward_return=percent(metrics.maximum_forward_return),
    )


def _research(analysis: FuturesAnalysis) -> AnalysisResearchResponse:
    return AnalysisResearchResponse(
        status="available",
        reason=None,
        decision_count=str(analysis.research_decision_count),
        horizons=tuple(_horizon(metrics) for metrics in analysis.research_metrics),
        by_action=tuple(
            ResearchActionResponse(
                action=group.action,
                decision_count=str(group.decision_count),
                horizons=tuple(_horizon(metrics) for metrics in group.horizons),
            )
            for group in analysis.action_metrics
        ),
    )


# ---------------------------------------------------------------------------
# Expiry
# ---------------------------------------------------------------------------


def _expiry(
    runtime: DatabaseRuntime, settings: DashboardSettings, cutoff: PointInTime
) -> ExpiryAnalysisResponse:
    contract = settings.contract
    guard = expiry_guard_for(contract)
    if guard is None:
        venue = contract.product.exchange_code.value
        return ExpiryAnalysisResponse(
            status="not_applicable",
            reason=f"no pre-expiry guard for {venue} contracts",
            decision_count=None,
            outside=None,
            flatten=None,
            protected=None,
            unresolved=None,
            unresolved_reason=None,
        )
    instants = [
        record.decision_instant
        for record in runtime.forward_repository.get_records(
            FuturesForwardResearchRecordQuery(contract, _DAILY)
        )
        if record.strategy_identity == settings.strategy
        and record.decision_instant.compare(cutoff) <= 0
    ]
    counts = {"outside": 0, "flatten": 0, "protected": 0}
    unresolved, first_reason = 0, None
    for instant in instants:
        try:
            counts[_window(guard.assess(contract, instant))] += 1
        except (FuturesExpiryWindowError, FuturesSessionResolutionError) as error:
            unresolved += 1
            first_reason = first_reason or str(error)
    return ExpiryAnalysisResponse(
        status="available",
        reason=None,
        decision_count=str(len(instants)),
        outside=str(counts["outside"]),
        flatten=str(counts["flatten"]),
        protected=str(counts["protected"]),
        unresolved=str(unresolved),
        unresolved_reason=first_reason,
    )


def _expiry_unavailable(reason: str) -> ExpiryAnalysisResponse:
    return ExpiryAnalysisResponse(
        status="unavailable",
        reason=reason,
        decision_count=None,
        outside=None,
        flatten=None,
        protected=None,
        unresolved=None,
        unresolved_reason=None,
    )


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------


def analysis_sections(
    runtime: DatabaseRuntime,
    settings: DashboardSettings,
    analysis: FuturesAnalysis | None,
    cutoff: PointInTime | None,
    no_session_reason: str,
) -> tuple[
    PaperAnalysisResponse,
    tuple[EquityPointResponse, ...],
    AnalysisResearchResponse,
    ExpiryAnalysisResponse,
]:
    """Return the paper, equity, research and expiry sections of the response."""
    if analysis is None or cutoff is None:
        research = AnalysisResearchResponse(
            status="unavailable",
            reason=no_session_reason,
            decision_count=None,
            horizons=(),
            by_action=(),
        )
        return (
            _paper_unavailable(no_session_reason),
            (),
            research,
            _expiry_unavailable(no_session_reason),
        )
    performance = analysis.performance
    paper = (
        _paper(performance)
        if performance is not None
        else _paper_unavailable("contract economics not configured", analysis.missing_economics)
    )
    return (
        paper,
        _equity_curve(performance),
        _research(analysis),
        _expiry(runtime, settings, cutoff),
    )
