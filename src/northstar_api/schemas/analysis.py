"""Schemas for the Story 1 analysis endpoint."""

from __future__ import annotations

from northstar_core.foundation.value_objects import Symbol
from pydantic import BaseModel, ConfigDict, field_validator


class AnalyzeAssetRequest(BaseModel):
    """Request payload for the single-asset analysis endpoint."""

    model_config = ConfigDict(str_strip_whitespace=True)

    symbol: str

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("symbol is required.")
        try:
            Symbol(normalized)
        except (TypeError, ValueError) as exc:
            raise ValueError("symbol must be a valid asset symbol.") from exc
        return normalized.upper()


class AnalyzeWatchlistRequest(BaseModel):
    """Ordered symbols requested for one watchlist refresh."""

    symbols: tuple[str, ...]

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized_symbols: list[str] = []
        for value in values:
            normalized_symbols.append(Symbol(value).value)
        return tuple(normalized_symbols)


class ExplanationReasonResponse(BaseModel):
    """One structured reason supporting a recommendation."""

    rationale: str
    supporting_signals: tuple[str, ...]


class RecommendationExplanationResponse(BaseModel):
    """Structured explanation supporting a recommendation."""

    reasons: tuple[ExplanationReasonResponse, ...]


class MarketObservationContextResponse(BaseModel):
    """Factual observations preserved by the Analyze Asset application result."""

    observed_at: str
    latest_price: str
    previous_close: str
    latest_volume: str
    daily_high: str
    daily_low: str
    recent_closes: tuple[str, ...]
    recent_volumes: tuple[str, ...]


class AnalyzeAssetResponse(BaseModel):
    """Public response for a single-asset recommendation and explanation."""

    symbol: str
    recommendation: str
    explanation: RecommendationExplanationResponse
    market_observation_context: MarketObservationContextResponse


class AnalyzeWatchlistItemResponse(BaseModel):
    """One ordered watchlist refresh outcome."""

    symbol: str
    result: AnalyzeAssetResponse | None = None
    error: str | None = None


class AnalyzeWatchlistResponse(BaseModel):
    """Ordered outcomes returned by a watchlist refresh."""

    items: tuple[AnalyzeWatchlistItemResponse, ...]
