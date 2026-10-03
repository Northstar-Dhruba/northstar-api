"""Response schemas for the read-only futures dashboard.

Every quote, amount, volume and contract count is a string holding its exact
canonical Decimal or integer text, so no value passes through a binary float
here or in a JavaScript client. Every instant is canonical UTC PointInTime
text. Money amounts carry their currency beside them and are never totalled.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

Availability = Literal["available", "unavailable"]
OrderState = Literal["no_order", "pending", "filled"]


class FuturesContractResponse(BaseModel):
    product: str
    exchange: str
    expiration: str


class SelectedContractResponse(FuturesContractResponse):
    timeframe: str


class ObservationEvidenceResponse(BaseModel):
    """The market observation context frozen inside the research record."""

    observed_at: str
    latest_quote: str
    previous_close: str
    latest_volume: str
    session_high: str
    session_low: str
    recent_closes: tuple[str, ...]
    recent_volumes: tuple[str, ...]


class ResearchResponse(BaseModel):
    status: Availability
    reason: str | None
    action: str | None
    decision_instant: str | None
    signals: tuple[str, ...]
    strategy: str
    policy: str
    evidence: ObservationEvidenceResponse | None


class MarketResponse(BaseModel):
    status: Availability
    reason: str | None
    session_instant: str | None
    open: str | None
    high: str | None
    low: str | None
    close: str | None
    volume: str | None


class PaperOrderResponse(BaseModel):
    """A persisted paper order and, when visible at the cutoff, its simulated fill."""

    state: OrderState
    side: str
    contracts: str
    order_identity: str
    decided_at: str
    simulated_fill_price: str | None
    price_basis: str | None
    fill_observable_from: str | None


class PaperResponse(BaseModel):
    """Paper execution of the selected contract's latest frozen decision."""

    portfolio: str
    strategy: str
    target: str
    decision_instant: str | None
    order_state: OrderState | None
    order: PaperOrderResponse | None


class PositionResponse(BaseModel):
    contract: FuturesContractResponse
    selected_contract: bool
    direction: Literal["LONG", "SHORT"]
    net_contracts: str
    average_entry: str


class PendingOrderResponse(BaseModel):
    contract: FuturesContractResponse
    side: str
    contracts: str
    order_identity: str
    decided_at: str


class PortfolioResponse(BaseModel):
    """The whole paper portfolio, every contract included."""

    status: Availability
    reason: str | None
    positions: tuple[PositionResponse, ...]
    pending_orders: tuple[PendingOrderResponse, ...]


class PnlRowResponse(BaseModel):
    contract: FuturesContractResponse
    settlement_currency: str
    realized_pnl: str
    position: Literal["open", "flat"]
    mark_quote: str | None
    mark_instant: str | None
    unrealized_pnl: str | None
    unrealized_reason: str | None


class PnlResponse(BaseModel):
    """Gross simulated P&L per contract; each amount in its own currency.

    ``missing_contract`` names the one dated contract -- product, exchange and
    expiration -- whose economics are not configured; other expiries of its
    product may well be configured.
    """

    status: Availability
    reason: str | None
    missing_contract: FuturesContractResponse | None
    rows: tuple[PnlRowResponse, ...]


class RecentDecisionResponse(BaseModel):
    action: str
    decision_instant: str
    signals: tuple[str, ...]
    order_state: OrderState
    order: PaperOrderResponse | None


class FreshnessResponse(BaseModel):
    """Persisted timestamps only; nothing here claims the data is live."""

    cutoff: str | None
    cutoff_source: Literal["requested", "latest_persisted_session", "none"]
    latest_market_session: str | None
    latest_decision_instant: str | None


class FinalityResponse(BaseModel):
    """The configured daily-bar finality; never inferred from a clock.

    ``next_session_outcome`` is the configured policy's assessment of the next
    required session, or None when there is no such session.
    """

    mode: Literal["disabled", "operator-approved"]
    final_through: str | None
    next_session_outcome: Literal["FINAL", "NOT_YET_FINAL", "UNKNOWN"] | None
    next_session_reason: str | None


class OperationalBacklogResponse(BaseModel):
    """Where chronological paper operation stands, from persisted facts only.

    Sessions are NSE trading dates. ``next_required_session`` is the session
    the next decision must be taken at, and ``next_required_cutoff`` its close.
    ``final_sessions_pending`` counts sessions the configured finality already
    treats as final but no decision covers yet; ``stored_sessions_pending``
    counts persisted daily bars from the next required session on. Caught up
    means neither count is positive. This is the latest processed state, not
    the outcome of any particular run, which is not persisted.
    """

    status: Availability
    reason: str | None
    stage: Literal["go_live_required", "not_started", "operating", "rollover_required"] | None
    go_live: str | None
    latest_decision_session: str | None
    latest_market_session: str | None
    next_required_session: str | None
    next_required_cutoff: str | None
    final_sessions_pending: str | None
    stored_sessions_pending: str | None
    market_data_ahead: bool | None
    caught_up: bool | None


class ExpirySafetyResponse(BaseModel):
    """The selected contract's pre-expiry flatten window, from the venue's guard.

    ``sessions_after_latest_decision`` counts trading sessions after the latest
    decision's session through expiry. ``window`` is ``flatten`` for the one
    decision that must take the position flat (E-(K+1)) and ``protected`` from
    E-K through expiry. ``reopening_blocked`` is whether the next decision is
    governed by the guard, so no signal can open or increase the contract.
    """

    status: Availability
    reason: str | None
    expiry_session: str
    flatten_sessions_before_expiry: str
    sessions_after_latest_decision: str | None
    window: Literal["outside", "flatten", "protected"] | None
    flatten_required: bool | None
    position_flat: bool | None
    reopening_blocked: bool | None


class OperationsResponse(BaseModel):
    """Read-only chronological operational state; ``not_applicable`` off NSE."""

    status: Literal["available", "not_applicable"]
    reason: str | None
    finality: FinalityResponse | None
    backlog: OperationalBacklogResponse | None
    expiry: ExpirySafetyResponse | None


class FuturesDashboardResponse(BaseModel):
    contract: SelectedContractResponse
    research: ResearchResponse
    market: MarketResponse
    paper: PaperResponse
    portfolio: PortfolioResponse
    pnl: PnlResponse
    recent_decisions: tuple[RecentDecisionResponse, ...]
    freshness: FreshnessResponse
    operations: OperationsResponse


class HealthResponse(BaseModel):
    status: Literal["ok", "unavailable"]
