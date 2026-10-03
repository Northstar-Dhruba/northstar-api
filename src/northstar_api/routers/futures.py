"""Read-only futures dashboard routes.

Both routes read persisted state only, through the shared database runtime and
GetFuturesPaperTradingSnapshotUseCase; the operational status is derived from
the same facts (``northstar_api.operational_status``). Nothing here acquires
market data, freezes or recomputes a recommendation, runs paper trading or
writes SQLite.
The recommendation is exactly the latest frozen research record; a refresh can
never create one. No wall clock is read: without ``as_of`` the cutoff is the
latest persisted daily bar of the configured contract.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse
from northstar_application.application_services import (
    ForwardResearchContractViolationError,
    FuturesContractEconomicsContractViolationError,
    FuturesHistoricalDataContractViolationError,
    FuturesPaperDecisionSnapshot,
    FuturesPaperPortfolioStrategyConflictError,
    FuturesPaperTradingContractViolationError,
    FuturesPaperTradingSnapshot,
    InvalidFuturesPaperFillHistoryError,
)
from northstar_application.ports import FuturesHistoricalMarketDataQuery
from northstar_core.foundation.value_objects import PointInTime, Timeframe
from northstar_core.futures import FuturesContract, FuturesOHLCVBar
from northstar_infrastructure.market_data import FuturesHistoricalStorageError
from northstar_infrastructure.persistence import (
    FuturesContractEconomicsStorageError,
    FuturesForwardResearchStorageError,
    FuturesPaperTradingStorageError,
)

from northstar_api.futures_analysis import ANALYSIS_HORIZONS, analysis_sections
from northstar_api.operational_status import operational_status
from northstar_api.runtime import DatabaseRuntime, build_futures_analysis
from northstar_api.schemas.futures import (
    FreshnessResponse,
    FuturesContractResponse,
    FuturesDashboardResponse,
    HealthResponse,
    MarketResponse,
    ObservationEvidenceResponse,
    PaperOrderResponse,
    PaperResponse,
    PendingOrderResponse,
    PnlResponse,
    PnlRowResponse,
    PortfolioResponse,
    PositionResponse,
    RecentDecisionResponse,
    ResearchResponse,
    SelectedContractResponse,
)
from northstar_api.schemas.futures_analysis import (
    AnalysisContextResponse,
    FuturesAnalysisResponse,
)
from northstar_api.settings import DashboardSettings

POLICY = "built-in directional MVP"
PRICE_BASIS = "OPEN of next synced session"
_DAILY = Timeframe("1d")
_NO_SESSION = "no persisted daily session for the configured contract"

_STORAGE_ERRORS: tuple[type[Exception], ...] = (
    FuturesHistoricalStorageError,
    FuturesForwardResearchStorageError,
    FuturesPaperTradingStorageError,
    FuturesContractEconomicsStorageError,
)
_PERSISTED_STATE_ERRORS: tuple[type[Exception], ...] = (
    *_STORAGE_ERRORS,
    FuturesPaperPortfolioStrategyConflictError,
    FuturesPaperTradingContractViolationError,
    ForwardResearchContractViolationError,
    FuturesContractEconomicsContractViolationError,
    FuturesHistoricalDataContractViolationError,
    InvalidFuturesPaperFillHistoryError,
)

router = APIRouter()


@dataclass(frozen=True, slots=True)
class FuturesDashboard:
    """The configured monitored contract and the runtime that reads it."""

    settings: DashboardSettings
    runtime: DatabaseRuntime


def _dashboard(request: Request) -> FuturesDashboard | None:
    return getattr(request.app.state, "futures", None)


# ---------------------------------------------------------------------------
# Translation of Application and Core values to exact strings
# ---------------------------------------------------------------------------


def _contract(contract: FuturesContract) -> FuturesContractResponse:
    return FuturesContractResponse(
        product=contract.product.product_code.value,
        exchange=contract.product.exchange_code.value,
        expiration=contract.expiration_date.value,
    )


def _order(decision: FuturesPaperDecisionSnapshot) -> PaperOrderResponse | None:
    order, fill = decision.order, decision.fill
    if order is None:
        return None
    return PaperOrderResponse(
        state="filled" if fill is not None else "pending",
        side=order.intent.side.value,
        contracts=str(order.intent.contracts.value),
        order_identity=order.identity.identity,
        decided_at=order.intent.decided_at.value,
        simulated_fill_price=str(fill.fill_quote.value) if fill is not None else None,
        price_basis=PRICE_BASIS if fill is not None else None,
        fill_observable_from=fill.filled_at.value if fill is not None else None,
    )


def _order_state(decision: FuturesPaperDecisionSnapshot) -> str:
    if decision.order is None:
        return "no_order"
    return "filled" if decision.fill is not None else "pending"


def _signals(decision: FuturesPaperDecisionSnapshot) -> tuple[str, ...]:
    return decision.record.result.recommendation.asset_analysis.summarized_signals


def _research(settings: DashboardSettings, snapshot: FuturesPaperTradingSnapshot | None):
    latest = snapshot.recent_decisions[0] if snapshot and snapshot.recent_decisions else None
    common = {"strategy": settings.strategy.identity, "policy": POLICY}
    if latest is None:
        return ResearchResponse(
            status="unavailable",
            reason="no frozen recommendation available" if snapshot else _NO_SESSION,
            action=None,
            decision_instant=None,
            signals=(),
            evidence=None,
            **common,
        )
    record = latest.record
    context = record.result.market_observation_context
    return ResearchResponse(
        status="available",
        reason=None,
        action=record.result.recommendation.action.value,
        decision_instant=record.decision_instant.value,
        signals=_signals(latest),
        evidence=ObservationEvidenceResponse(
            observed_at=context.observed_at.value,
            latest_quote=str(context.latest_quote.value),
            previous_close=str(context.previous_close.value),
            latest_volume=str(context.latest_volume.value),
            session_high=str(context.session_high.value),
            session_low=str(context.session_low.value),
            recent_closes=tuple(str(quote.value) for quote in context.recent_closes),
            recent_volumes=tuple(str(volume.value) for volume in context.recent_volumes),
        ),
        **common,
    )


def _market(bar: FuturesOHLCVBar | None) -> MarketResponse:
    if bar is None:
        return MarketResponse(
            status="unavailable",
            reason=_NO_SESSION,
            session_instant=None,
            open=None,
            high=None,
            low=None,
            close=None,
            volume=None,
        )
    return MarketResponse(
        status="available",
        reason=None,
        session_instant=bar.point_in_time.value,
        open=str(bar.open.value),
        high=str(bar.high.value),
        low=str(bar.low.value),
        close=str(bar.close.value),
        volume=str(bar.volume.value),
    )


def _paper(settings: DashboardSettings, snapshot: FuturesPaperTradingSnapshot | None):
    latest = snapshot.recent_decisions[0] if snapshot and snapshot.recent_decisions else None
    return PaperResponse(
        portfolio=settings.portfolio.identity,
        strategy=settings.strategy.identity,
        target=str(settings.target.value),
        decision_instant=latest.record.decision_instant.value if latest else None,
        order_state=_order_state(latest) if latest else None,
        order=_order(latest) if latest else None,
    )


def _portfolio(settings: DashboardSettings, snapshot: FuturesPaperTradingSnapshot | None):
    if snapshot is None:
        return PortfolioResponse(
            status="unavailable", reason=_NO_SESSION, positions=(), pending_orders=()
        )
    return PortfolioResponse(
        status="available",
        reason=None,
        positions=tuple(
            PositionResponse(
                contract=_contract(position.contract),
                selected_contract=position.contract == settings.contract,
                direction="LONG" if position.net_contracts > 0 else "SHORT",
                net_contracts=str(abs(position.net_contracts)),
                average_entry=str(position.average_entry.value),
            )
            for position in snapshot.portfolio.positions
        ),
        pending_orders=tuple(
            PendingOrderResponse(
                contract=_contract(order.intent.contract),
                side=order.intent.side.value,
                contracts=str(order.intent.contracts.value),
                order_identity=order.identity.identity,
                decided_at=order.intent.decided_at.value,
            )
            for order in snapshot.pending_orders
        ),
    )


def _pnl(snapshot: FuturesPaperTradingSnapshot | None) -> PnlResponse:
    if snapshot is None:
        return PnlResponse(status="unavailable", reason=_NO_SESSION, missing_contract=None, rows=())
    if snapshot.valuation is None:
        return PnlResponse(
            status="unavailable",
            reason="contract economics not configured",
            missing_contract=_contract(snapshot.missing_economics),
            rows=(),
        )
    rows = []
    for row in snapshot.valuation.contracts:
        marked = row.unrealized_pnl is not None
        rows.append(
            PnlRowResponse(
                contract=_contract(row.contract),
                settlement_currency=row.settlement_currency.value,
                realized_pnl=str(row.realized_pnl.amount),
                position="open" if row.is_open else "flat",
                mark_quote=str(row.mark_quote.value) if row.mark_quote is not None else None,
                mark_instant=row.mark_instant.value if row.mark_instant is not None else None,
                unrealized_pnl=str(row.unrealized_pnl.amount) if marked else None,
                unrealized_reason=(
                    None if marked else "no synced daily close observable by cutoff"
                ),
            )
        )
    return PnlResponse(status="available", reason=None, missing_contract=None, rows=tuple(rows))


def _recent(snapshot: FuturesPaperTradingSnapshot | None) -> tuple[RecentDecisionResponse, ...]:
    if snapshot is None:
        return ()
    return tuple(
        RecentDecisionResponse(
            action=decision.record.result.recommendation.action.value,
            decision_instant=decision.record.decision_instant.value,
            signals=_signals(decision),
            order_state=_order_state(decision),
            order=_order(decision),
        )
        for decision in snapshot.recent_decisions
    )


def _latest_bar(bars: Sequence[FuturesOHLCVBar]) -> FuturesOHLCVBar | None:
    latest: FuturesOHLCVBar | None = None
    for bar in bars:
        if latest is None or bar.point_in_time.compare(latest.point_in_time) > 0:
            latest = bar
    return latest


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


def _requested_cutoff(as_of: str | None) -> PointInTime | None:
    """Parse an explicit ``as_of``; None means the latest persisted session."""
    if as_of is None:
        return None
    try:
        return PointInTime(as_of)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="as_of must be an ISO-8601 timestamp with an explicit offset.",
        ) from exc


def _unavailable_state() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Persisted futures state is unavailable or inconsistent.",
    )


@router.get("/health", response_model=HealthResponse)
def health(request: Request):
    """Report whether the configured database can be read."""
    dashboard = _dashboard(request)
    unavailable = JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"status": "unavailable"}
    )
    if dashboard is None:
        return unavailable
    try:
        dashboard.runtime.economics_repository.get_economics(dashboard.settings.contract)
    except _STORAGE_ERRORS:
        return unavailable
    return HealthResponse(status="ok")


@router.get("/futures/dashboard", response_model=FuturesDashboardResponse)
def futures_dashboard(request: Request, as_of: str | None = None) -> FuturesDashboardResponse:
    """Return the persisted state of the configured contract and paper portfolio."""
    dashboard = _dashboard(request)
    if dashboard is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Futures dashboard is not configured.",
        )
    settings, runtime = dashboard.settings, dashboard.runtime

    cutoff = _requested_cutoff(as_of)
    source = "requested"
    try:
        bars = runtime.market_repository.get_bars(
            FuturesHistoricalMarketDataQuery(settings.contract, _DAILY)
        )
        if as_of is None:
            latest = _latest_bar(bars)
            cutoff = latest.point_in_time if latest is not None else None
            source = "latest_persisted_session" if cutoff is not None else "none"
        snapshot = (
            runtime.snapshot.execute(
                settings.contract, settings.strategy, settings.portfolio, cutoff
            )
            if cutoff is not None
            else None
        )
        visible = tuple(
            bar for bar in bars if cutoff is not None and bar.point_in_time.compare(cutoff) <= 0
        )
        operations = operational_status(settings, snapshot, visible)
    except _PERSISTED_STATE_ERRORS as exc:
        raise _unavailable_state() from exc

    latest_record = snapshot.latest_forward_record if snapshot else None
    bar = snapshot.latest_market_bar if snapshot else None
    contract = settings.contract
    return FuturesDashboardResponse(
        contract=SelectedContractResponse(**_contract(contract).model_dump(), timeframe="1d"),
        research=_research(settings, snapshot),
        market=_market(bar),
        paper=_paper(settings, snapshot),
        portfolio=_portfolio(settings, snapshot),
        pnl=_pnl(snapshot),
        recent_decisions=_recent(snapshot),
        freshness=FreshnessResponse(
            cutoff=cutoff.value if cutoff is not None else None,
            cutoff_source=source,
            latest_market_session=bar.point_in_time.value if bar is not None else None,
            latest_decision_instant=(
                latest_record.decision_instant.value if latest_record is not None else None
            ),
        ),
        operations=operations,
    )


@router.get("/futures/analysis", response_model=FuturesAnalysisResponse)
def futures_analysis(request: Request, as_of: str | None = None) -> FuturesAnalysisResponse:
    """Return the read-only paper and research analysis of the configured contract.

    The cutoff follows the dashboard: an explicit ``as_of``, or else the latest
    persisted daily bar of the contract. Research horizons are the frozen
    baseline, 1 and 5 sessions.
    """
    dashboard = _dashboard(request)
    if dashboard is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Futures dashboard is not configured.",
        )
    settings, runtime = dashboard.settings, dashboard.runtime
    cutoff = _requested_cutoff(as_of)
    source = "requested"
    try:
        if as_of is None:
            latest = _latest_bar(
                runtime.market_repository.get_bars(
                    FuturesHistoricalMarketDataQuery(settings.contract, _DAILY)
                )
            )
            cutoff = latest.point_in_time if latest is not None else None
            source = "latest_persisted_session" if cutoff is not None else "none"
        analysis = (
            build_futures_analysis(runtime).execute(
                settings.contract,
                settings.strategy,
                settings.portfolio,
                ANALYSIS_HORIZONS,
                cutoff,
            )
            if cutoff is not None
            else None
        )
        paper, equity_curve, research, expiry = analysis_sections(
            runtime, settings, analysis, cutoff, _NO_SESSION
        )
    except _PERSISTED_STATE_ERRORS as exc:
        raise _unavailable_state() from exc

    return FuturesAnalysisResponse(
        context=AnalysisContextResponse(
            contract=_contract(settings.contract),
            strategy=settings.strategy.identity,
            portfolio=settings.portfolio.identity,
            cutoff=cutoff.value if cutoff is not None else None,
            cutoff_source=source,
            horizons=tuple(str(horizon.observations) for horizon in ANALYSIS_HORIZONS),
        ),
        paper=paper,
        equity_curve=equity_curve,
        research=research,
        expiry=expiry,
    )
