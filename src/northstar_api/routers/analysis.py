"""Analysis router for the Story 1 endpoint."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from northstar_application.application_services import AnalyzeAssetUseCase
from northstar_core.foundation.value_objects import Symbol
from northstar_core.strategy import AssetAnalysisGenerator, Strategy, StrategyIdentity
from northstar_infrastructure.market_data import YahooFinanceMarketObservationSource

from northstar_api.schemas.analysis import (
    AnalyzeAssetRequest,
    AnalyzeAssetResponse,
    ExplanationReasonResponse,
    RecommendationExplanationResponse,
)

router = APIRouter()


_ANALYZE_USE_CASE = AnalyzeAssetUseCase(
    Strategy(StrategyIdentity("mvp")),
    YahooFinanceMarketObservationSource(),
    AssetAnalysisGenerator(),
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
    )
