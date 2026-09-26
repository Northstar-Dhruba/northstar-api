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
    """Gross simulated P&L per contract; each amount in its own currency."""

    status: Availability
    reason: str | None
    missing_product: str | None
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


class FuturesDashboardResponse(BaseModel):
    contract: SelectedContractResponse
    research: ResearchResponse
    market: MarketResponse
    paper: PaperResponse
    portfolio: PortfolioResponse
    pnl: PnlResponse
    recent_decisions: tuple[RecentDecisionResponse, ...]
    freshness: FreshnessResponse


class HealthResponse(BaseModel):
    status: Literal["ok", "unavailable"]
