"""Endpoint contract tests for the Story 2 analysis response."""

from northstar_api.routers.analysis import analyze_asset
from northstar_api.schemas.analysis import AnalyzeAssetRequest


def test_analyze_returns_recommendation_with_structured_explanation() -> None:
    response = analyze_asset(AnalyzeAssetRequest(symbol="aapl"))

    assert response.model_dump(mode="json") == {
        "symbol": "AAPL",
        "recommendation": "BUY",
        "explanation": {
            "reasons": [
                {
                    "rationale": "Recommendation is supported by the analyzed market signals.",
                    "supporting_signals": ["strong bullish"],
                }
            ]
        },
    }
