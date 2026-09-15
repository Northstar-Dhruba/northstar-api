"""Analysis router for the Story 1 endpoint."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from northstar_application.application_services import (
    AnalyzeAssetResult,
    AnalyzeAssetUseCase,
    AnalyzeWatchlistUseCase,
)
from northstar_core.foundation.value_objects import Symbol
from northstar_core.strategy import AssetAnalysisGenerator, Strategy, StrategyIdentity
from northstar_infrastructure.market_data import YahooFinanceMarketObservationSource

from northstar_api.schemas.analysis import (
    AnalyzeAssetRequest,
    AnalyzeAssetResponse,
    AnalyzeWatchlistItemResponse,
    AnalyzeWatchlistRequest,
    AnalyzeWatchlistResponse,
    ExplanationReasonResponse,
    MarketObservationContextResponse,
    RecommendationExplanationResponse,
)

router = APIRouter()


_ANALYZE_USE_CASE = AnalyzeAssetUseCase(
    Strategy(StrategyIdentity("mvp")),
    YahooFinanceMarketObservationSource(),
    AssetAnalysisGenerator(),
)
_ANALYZE_WATCHLIST_USE_CASE = AnalyzeWatchlistUseCase(_ANALYZE_USE_CASE)


def _serialize_analyze_asset_result(result: AnalyzeAssetResult) -> AnalyzeAssetResponse:
    return AnalyzeAssetResponse(
        symbol=result.recommendation.asset_analysis.listing.instrument.symbol.value,
        recommendation=result.recommendation.action.value,
        explanation=RecommendationExplanationResponse(
            reasons=tuple(
                ExplanationReasonResponse(
                    rationale=reason.rationale,
                    supporting_signals=reason.supporting_signals,
                )
                for reason in result.explanation.reasons
            )
        ),
        market_observation_context=MarketObservationContextResponse(
            observed_at=str(result.market_observation_context.observed_at),
            latest_price=str(result.market_observation_context.latest_price),
            previous_close=str(result.market_observation_context.previous_close),
            latest_volume=str(result.market_observation_context.latest_volume),
            daily_high=str(result.market_observation_context.daily_high),
            daily_low=str(result.market_observation_context.daily_low),
            recent_closes=tuple(
                str(price) for price in result.market_observation_context.recent_closes
            ),
            recent_volumes=tuple(
                str(volume) for volume in result.market_observation_context.recent_volumes
            ),
        ),
    )


@router.post("/analyze", response_model=AnalyzeAssetResponse)
def analyze_asset(request: AnalyzeAssetRequest) -> AnalyzeAssetResponse:
    """Analyze one asset symbol and return its recommendation and explanation."""
    try:
        symbol = Symbol(request.symbol)
        result = _ANALYZE_USE_CASE.execute(symbol)
    except LookupError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Unknown symbol.",
        ) from exc
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid request.",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Application failure.",
        ) from exc

    return _serialize_analyze_asset_result(result)


@router.post("/watchlist/refresh", response_model=AnalyzeWatchlistResponse)
def refresh_watchlist(request: AnalyzeWatchlistRequest) -> AnalyzeWatchlistResponse:
    """Refresh intelligence for the requested symbols in input order."""
    result = _ANALYZE_WATCHLIST_USE_CASE.execute(tuple(Symbol(value) for value in request.symbols))
    return AnalyzeWatchlistResponse(
        items=tuple(
            AnalyzeWatchlistItemResponse(
                symbol=item.symbol.value,
                result=_serialize_analyze_asset_result(item.result) if item.result else None,
                error=item.failure.code.value if item.failure else None,
            )
            for item in result.items
        )
    )
