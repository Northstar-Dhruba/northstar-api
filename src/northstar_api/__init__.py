"""Northstar API package for the Intelligence MVP."""

from __future__ import annotations

from typing import Final

from fastapi import FastAPI, HTTPException, status
from northstar_application.application_services import AnalyzeAssetUseCase, AssetAnalysisInput
from northstar_core.domain.exchange import Exchange
from northstar_core.domain.instrument import Instrument
from northstar_core.domain.listing import Listing
from northstar_core.domain.value_objects import ListingStatus, Tradability
from northstar_core.foundation.value_objects import Currency, ExchangeCode, PointInTime, Symbol
from northstar_core.strategy import Strategy, StrategyIdentity
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


class AnalyzeAssetResponse(BaseModel):
    """Public response for a single-asset recommendation."""

    symbol: str
    recommendation: str


class InMemoryMarketObservationProvider:
    """Minimal market context adapter for Story 1 API delivery."""

    _SUPPORTED_SYMBOLS: Final[dict[str, Listing]] = {
        "AAPL": Listing(
            instrument=Instrument(Symbol("AAPL"), "Apple Inc.", "Equity"),
            exchange=Exchange(ExchangeCode("NASDAQ"), "NASDAQ"),
            currency=Currency("USD"),
            listing_status=ListingStatus("Active"),
            tradability=Tradability("Permitted"),
            description="Apple Inc.",
        ),
        "MSFT": Listing(
            instrument=Instrument(Symbol("MSFT"), "Microsoft Corporation", "Equity"),
            exchange=Exchange(ExchangeCode("NASDAQ"), "NASDAQ"),
            currency=Currency("USD"),
            listing_status=ListingStatus("Active"),
            tradability=Tradability("Permitted"),
            description="Microsoft Corporation",
        ),
    }

    def get_analysis_input(self, symbol: Symbol) -> AssetAnalysisInput:
        listing = self._SUPPORTED_SYMBOLS.get(symbol.value)
        if listing is None:
            raise LookupError(f"Unknown symbol: {symbol.value}")

        return AssetAnalysisInput(
            listing=listing,
            point_in_time=PointInTime("2026-09-06T09:30:00Z"),
            summarized_signals=("strong bullish",),
        )


_ANALYZE_USE_CASE = AnalyzeAssetUseCase(
    Strategy(StrategyIdentity("mvp")),
    InMemoryMarketObservationProvider(),
)

app = FastAPI(title="Northstar Intelligence MVP", version="0.1.0")


@app.post("/analyze", response_model=AnalyzeAssetResponse)
def analyze_asset(request: AnalyzeAssetRequest) -> AnalyzeAssetResponse:
    """Analyze one asset symbol and return the current domain recommendation."""
    try:
        symbol = Symbol(request.symbol)
        recommendation = _ANALYZE_USE_CASE.execute(symbol)
    except LookupError as exc:
        raise HTTPException(status_code=status.HTTP_404, detail="Unknown symbol.") from exc
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_422, detail="Invalid request.") from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500, detail="Application failure.") from exc

    return AnalyzeAssetResponse(
        symbol=recommendation.asset_analysis.listing.instrument.symbol.value,
        recommendation=recommendation.action.value,
    )


def main() -> None:
    """Run the API via uvicorn."""
    import uvicorn

    uvicorn.run("northstar_api:app", host="0.0.0.0", port=8000, reload=False)


__all__ = ["AnalyzeAssetRequest", "AnalyzeAssetResponse", "app", "main"]
