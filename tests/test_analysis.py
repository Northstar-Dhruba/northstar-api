"""Endpoint contract tests for the Story 3 analysis composition."""

import json

import pytest
from fastapi import HTTPException

from northstar_api.routers import analysis
from northstar_api.schemas.analysis import AnalyzeAssetRequest


def _yahoo_payload() -> bytes:
    closes = [100 + index for index in range(20)]
    volumes = [1000] * 20
    return json.dumps(
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "currency": "USD",
                            "symbol": "AAPL",
                            "exchangeName": "NASDAQ",
                            "quoteType": "EQUITY",
                            "longName": "Apple Inc.",
                            "previousClose": 118,
                        },
                        "timestamp": list(range(1_700_000_000, 1_700_000_020)),
                        "indicators": {
                            "quote": [
                                {
                                    "close": closes,
                                    "volume": volumes,
                                    "high": [value + 2 for value in closes],
                                    "low": [value - 2 for value in closes],
                                }
                            ]
                        },
                    }
                ]
            }
        }
    ).encode()


def test_analyze_returns_recommendation_with_structured_explanation() -> None:
    analysis._ANALYZE_USE_CASE._observation_source._fetch = lambda url, timeout: _yahoo_payload()

    response = analysis.analyze_asset(AnalyzeAssetRequest(symbol="aapl"))

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


def test_analyze_translates_application_failure_to_generic_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(symbol: object) -> object:
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(analysis._ANALYZE_USE_CASE, "execute", fail)

    with pytest.raises(HTTPException) as error:
        analysis.analyze_asset(AnalyzeAssetRequest(symbol="AAPL"))

    assert error.value.status_code == 500
    assert error.value.detail == "Application failure."
