"""Response schemas for the read-only futures analysis.

Conventions follow the dashboard: every quote, amount, count, ratio and return
is a string holding its exact canonical Decimal or integer text -- never a
binary float -- and every instant is canonical UTC PointInTime text.

- Money is ``{"amount", "currency"}``; amounts are never summed across
  currencies.
- Forward returns are percentages (``"1.5"`` means 1.5%) of market movement
  after a decision; they are not strategy returns.
- Win rate and profit factor are plain Decimal ratios (``"0.5"``, ``"2"``).
- Durations are elapsed seconds as an exact decimal string
  (``"172800"``, ``"0.25"``); there is no trading-day conversion.
- An undefined value is ``null``, with a reason where the analysis gives one;
  it is never a zero.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from northstar_api.schemas.futures import Availability, FuturesContractResponse

Direction = Literal["LONG", "SHORT"]
Action = Literal["BUY", "SELL", "HOLD"]


class MoneyResponse(BaseModel):
    amount: str
    currency: str


class AnalysisContextResponse(BaseModel):
    contract: FuturesContractResponse
    strategy: str
    portfolio: str
    cutoff: str | None
    cutoff_source: Literal["requested", "latest_persisted_session", "none"]
    horizons: tuple[str, ...]


class DecisionActivityResponse(BaseModel):
    """Frozen paper decisions on the contract, by recorded action."""

    total: str
    buy: str
    sell: str
    hold: str


class ExecutionActivityResponse(BaseModel):
    """Orders and fills in whole contracts; turnover is bought plus sold."""

    order_count: str
    fill_count: str
    contracts_bought: str
    contracts_sold: str
    turnover_contracts: str
    net_contracts: str


class CompletedTradeResponse(BaseModel):
    direction: Direction
    opened_at: str
    closed_at: str
    contracts: str
    closing_average_entry: str
    average_exit: str
    realized_pnl: MoneyResponse
    holding_seconds: str


class TradeStatisticsResponse(BaseModel):
    """Statistics over completed trades only.

    ``completed_realized_pnl`` is the sum of completed-trade P&L, an analytical
    decomposition that may differ in trailing Decimal digits from the
    canonical realized P&L, and that excludes what an open position has
    already realized.
    """

    completed_count: str
    winning_count: str
    losing_count: str
    breakeven_count: str
    win_rate: str | None
    completed_realized_pnl: MoneyResponse
    average_trade_pnl: MoneyResponse | None
    gross_profit: MoneyResponse
    gross_loss: MoneyResponse
    profit_factor: str | None
    profit_factor_unavailable_reason: Literal["NO_COMPLETED_TRADES", "NO_LOSING_TRADES"] | None
    minimum_holding_seconds: str | None
    maximum_holding_seconds: str | None
    average_holding_seconds: str | None
    median_holding_seconds: str | None


class TradesResponse(BaseModel):
    statistics: TradeStatisticsResponse
    completed: tuple[CompletedTradeResponse, ...]


class OpenExposureResponse(BaseModel):
    """The position still open at the cutoff; never counted as a trade.

    ``realized_pnl`` is what partial closes of it have already realized. The
    mark fields and ``unrealized_pnl`` are null together when no stored daily
    close was observable by the cutoff.
    """

    direction: Direction
    net_contracts: str
    average_entry: str
    opened_at: str
    realized_pnl: MoneyResponse
    mark_quote: str | None
    mark_instant: str | None
    unrealized_pnl: MoneyResponse | None


class PortfolioAnalysisResponse(BaseModel):
    """Canonical gross simulated P&L of the contract at the cutoff.

    ``canonical_realized_pnl`` is the portfolio realized P&L used for
    valuation. ``unrealized_pnl`` is zero when flat and null when an open
    position has no mark.
    """

    canonical_realized_pnl: MoneyResponse
    unrealized_pnl: MoneyResponse | None
    open_exposure: OpenExposureResponse | None


class DrawdownResponse(BaseModel):
    """Largest absolute fall of cumulative gross simulated P&L from its peak.

    There is no account capital, so this is an amount, never a percentage.
    """

    amount: MoneyResponse
    peak_pnl: MoneyResponse
    peak_instant: str
    trough_pnl: MoneyResponse
    trough_instant: str


class PaperAnalysisResponse(BaseModel):
    status: Availability
    reason: str | None
    missing_contract: FuturesContractResponse | None
    decisions: DecisionActivityResponse | None
    execution: ExecutionActivityResponse | None
    trades: TradesResponse | None
    portfolio: PortfolioAnalysisResponse | None
    drawdown: DrawdownResponse | None


class EquityPointResponse(BaseModel):
    """Gross simulated P&L at one stored daily close; total = realized + unrealized."""

    instant: str
    mark_quote: str
    net_contracts: str
    realized_pnl: MoneyResponse
    unrealized_pnl: MoneyResponse
    total_pnl: MoneyResponse


class ResearchHorizonResponse(BaseModel):
    """Market forward returns after decisions at one horizon, in percent."""

    horizon: str
    total_count: str
    measured_count: str
    insufficient_future_observations_count: str
    undefined_return_basis_count: str
    average_forward_return: str | None
    median_forward_return: str | None
    minimum_forward_return: str | None
    maximum_forward_return: str | None


class ResearchActionResponse(BaseModel):
    action: Action
    decision_count: str
    horizons: tuple[ResearchHorizonResponse, ...]


class AnalysisResearchResponse(BaseModel):
    """Historical research of the configured strategy over stored daily bars.

    Recomputed from stored bars through the cutoff; nothing is persisted.
    """

    status: Availability
    reason: str | None
    decision_count: str | None
    horizons: tuple[ResearchHorizonResponse, ...]
    by_action: tuple[ResearchActionResponse, ...]


class ExpiryAnalysisResponse(BaseModel):
    """Frozen paper decisions classified by the venue's pre-expiry guard.

    ``unresolved`` counts decisions the calendar could not place; they are
    never counted as outside the window.
    """

    status: Literal["available", "not_applicable", "unavailable"]
    reason: str | None
    decision_count: str | None
    outside: str | None
    flatten: str | None
    protected: str | None
    unresolved: str | None
    unresolved_reason: str | None


class FuturesAnalysisResponse(BaseModel):
    context: AnalysisContextResponse
    paper: PaperAnalysisResponse
    equity_curve: tuple[EquityPointResponse, ...]
    research: AnalysisResearchResponse
    expiry: ExpiryAnalysisResponse
