"""ASGI endpoint contract tests for the Alpha API."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from northstar_application.application_services import (
    AnalyzeWatchlistFailure,
    AnalyzeWatchlistFailureCode,
    AnalyzeWatchlistItemResult,
    AnalyzeWatchlistResult,
)
from northstar_core.foundation.value_objects import Symbol

from northstar_api.app import app
from northstar_api.routers import analysis


def _yahoo_payload() -> bytes:
    closes = [100 + index for index in range(20)]
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
                                    "volume": [1000] * 20,
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


def _request(path: str, payload: Any) -> tuple[int, dict[str, Any]]:
    return asyncio.run(_request_async(path, json.dumps(payload).encode()))


async def _request_async(path: str, body: bytes) -> tuple[int, dict[str, Any]]:
    sent = False
    response_status = 0
    response_body = bytearray()

    async def receive() -> dict[str, Any]:
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        nonlocal response_status
        if message["type"] == "http.response.start":
            response_status = message["status"]
        elif message["type"] == "http.response.body":
            response_body.extend(message.get("body", b""))

    await app(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [(b"content-type", b"application/json")],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
        },
        receive,
        send,
    )
    return response_status, json.loads(response_body)


@pytest.fixture(autouse=True)
def deterministic_provider():
    observation_source = analysis._ANALYZE_USE_CASE._observation_source
    original = observation_source._fetch
    observation_source._fetch = lambda url, timeout: _yahoo_payload()
    yield
    observation_source._fetch = original


def test_analyze_asgi_returns_recommendation_explanation_and_evidence() -> None:
    status_code, response = _request("/analyze", {"symbol": "aapl"})

    assert status_code == 200
    assert response["symbol"] == "AAPL"
    assert response["recommendation"] == "BUY"
    assert response["explanation"]["reasons"]
    assert response["market_observation_context"]["latest_price"] == "119 USD"


@pytest.mark.parametrize("payload", [{}, {"symbol": "AAPL!"}, {"symbol": ""}])
def test_analyze_asgi_rejects_invalid_request(payload: dict[str, str]) -> None:
    status_code, _ = _request("/analyze", payload)

    assert status_code == 422


def test_analyze_asgi_returns_not_found_for_unknown_symbol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        analysis._ANALYZE_USE_CASE._observation_source,
        "_fetch",
        lambda url, timeout: json.dumps({"chart": {"result": []}}).encode(),
    )

    status_code, response = _request("/analyze", {"symbol": "AAPL"})

    assert status_code == 404
    assert response["detail"] == "Unknown symbol."


@pytest.mark.parametrize("fetch_result", [b"not-json", b'{"chart":{"result":[{}]}}'])
def test_analyze_asgi_maps_provider_failures_to_500(
    monkeypatch: pytest.MonkeyPatch,
    fetch_result: bytes,
) -> None:
    monkeypatch.setattr(
        analysis._ANALYZE_USE_CASE._observation_source,
        "_fetch",
        lambda url, timeout: fetch_result,
    )

    status_code, response = _request("/analyze", {"symbol": "AAPL"})

    assert status_code == 500
    assert response["detail"] == "Application failure."


def test_watchlist_refresh_asgi_preserves_order_and_results() -> None:
    status_code, response = _request(
        "/watchlist/refresh",
        {"symbols": ["AAPL", "MSFT"]},
    )

    assert status_code == 200
    assert [item["symbol"] for item in response["items"]] == ["AAPL", "MSFT"]
    assert all(item["result"] is not None for item in response["items"])


def test_watchlist_refresh_asgi_supports_empty_watchlist() -> None:
    status_code, response = _request("/watchlist/refresh", {"symbols": []})

    assert status_code == 200
    assert response == {"items": []}


def test_watchlist_refresh_asgi_preserves_partial_failure_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    success_result = analysis._ANALYZE_USE_CASE.execute(Symbol("AAPL"))
    partial_result = AnalyzeWatchlistResult(
        items=(
            AnalyzeWatchlistItemResult(Symbol("AAPL"), result=success_result),
            AnalyzeWatchlistItemResult(
                Symbol("MSFT"),
                failure=AnalyzeWatchlistFailure(AnalyzeWatchlistFailureCode.PROVIDER_UNAVAILABLE),
            ),
        )
    )
    monkeypatch.setattr(
        analysis._ANALYZE_WATCHLIST_USE_CASE,
        "execute",
        lambda symbols: partial_result,
    )

    status_code, response = _request(
        "/watchlist/refresh",
        {"symbols": ["AAPL", "MSFT"]},
    )

    assert status_code == 200
    assert response["items"][0]["result"] is not None
    assert response["items"][1]["error"] == "PROVIDER_UNAVAILABLE"
