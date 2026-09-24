"""Deployment settings for the private futures dashboard.

Read only from the environment of the API process, and only here. They name
one monitored contract, one strategy label, one paper portfolio and its fixed
target. No setting is persisted, and none is needed by the read itself except
the contract, strategy and portfolio it reads; the target is displayed only.

DATABENTO_API_KEY is deliberately not among them: the dashboard never acquires
market data.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, Symbol
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_core.paper_trading import FuturesContractCount, PaperPortfolioIdentity
from northstar_core.strategy import StrategyIdentity

VARIABLES = (
    "NORTHSTAR_DATABASE",
    "NORTHSTAR_WEB_ORIGIN",
    "NORTHSTAR_FUTURES_PRODUCT",
    "NORTHSTAR_FUTURES_EXCHANGE",
    "NORTHSTAR_FUTURES_EXPIRATION",
    "NORTHSTAR_STRATEGY",
    "NORTHSTAR_PORTFOLIO",
    "NORTHSTAR_TARGET",
)
_ORIGIN = re.compile(r"https?://[^/\s]+")
_COUNT = re.compile(r"[1-9][0-9]*")


class DashboardSettingsError(ValueError):
    """Raised when the dashboard environment is incomplete or invalid."""


@dataclass(frozen=True, slots=True)
class DashboardSettings:
    """Validated configuration of the one monitored futures contract and portfolio."""

    database: Path
    web_origin: str
    contract: FuturesContract
    strategy: StrategyIdentity
    portfolio: PaperPortfolioIdentity
    target: FuturesContractCount


def _value(name: str, text: str, build):
    try:
        return build(text)
    except (TypeError, ValueError) as exc:
        raise DashboardSettingsError(f"{name} is invalid: {exc}") from exc


def _origin(text: str) -> str:
    if _ORIGIN.fullmatch(text) is None:
        raise ValueError("must be one origin such as https://northstar.example, with no path")
    return text


def _target(text: str) -> FuturesContractCount:
    if _COUNT.fullmatch(text) is None:
        raise ValueError("must be a positive whole number")
    return FuturesContractCount(int(text))


def load_dashboard_settings(env: Mapping[str, str]) -> DashboardSettings | None:
    """Return validated settings, None when none are set, or raise when incomplete."""
    values = {name: env.get(name, "").strip() for name in VARIABLES}
    if not any(values.values()):
        return None
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise DashboardSettingsError(
            f"Futures dashboard configuration is incomplete; missing {', '.join(missing)}."
        )
    product = FuturesProductReference(
        _value("NORTHSTAR_FUTURES_PRODUCT", values["NORTHSTAR_FUTURES_PRODUCT"], Symbol),
        _value("NORTHSTAR_FUTURES_EXCHANGE", values["NORTHSTAR_FUTURES_EXCHANGE"], ExchangeCode),
    )
    expiration = _value(
        "NORTHSTAR_FUTURES_EXPIRATION", values["NORTHSTAR_FUTURES_EXPIRATION"], ExpirationDate
    )
    return DashboardSettings(
        database=Path(values["NORTHSTAR_DATABASE"]),
        web_origin=_value("NORTHSTAR_WEB_ORIGIN", values["NORTHSTAR_WEB_ORIGIN"], _origin),
        contract=FuturesContract(product, expiration),
        strategy=_value("NORTHSTAR_STRATEGY", values["NORTHSTAR_STRATEGY"], StrategyIdentity),
        portfolio=_value(
            "NORTHSTAR_PORTFOLIO", values["NORTHSTAR_PORTFOLIO"], PaperPortfolioIdentity
        ),
        target=_value("NORTHSTAR_TARGET", values["NORTHSTAR_TARGET"], _target),
    )
