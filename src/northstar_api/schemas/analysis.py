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


class AnalyzeAssetResponse(BaseModel):
    """Public response for a single-asset recommendation."""

    symbol: str
    recommendation: str
