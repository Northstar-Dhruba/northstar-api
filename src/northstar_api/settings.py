"""Deployment settings for the private futures dashboard and daily operation.

Read only from the process environment, and only here. They name one monitored
contract, one strategy label, one paper portfolio and its fixed target. No
setting is persisted. The dashboard additionally needs the one web origin it
serves; the daily operation does not.

DATABENTO_API_KEY is deliberately not among them: the dashboard never acquires
market data, and the daily operation reads the secret separately at its command
boundary.
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

OPERATION_VARIABLES = (
    "NORTHSTAR_DATABASE",
    "NORTHSTAR_FUTURES_PRODUCT",
    "NORTHSTAR_FUTURES_EXCHANGE",
    "NORTHSTAR_FUTURES_EXPIRATION",
    "NORTHSTAR_STRATEGY",
    "NORTHSTAR_PORTFOLIO",
    "NORTHSTAR_TARGET",
)
VARIABLES = (
    "NORTHSTAR_DATABASE",
    "NORTHSTAR_WEB_ORIGIN",
    *OPERATION_VARIABLES[1:],
)
_ORIGIN = re.compile(r"https?://[^/\s]+")
_COUNT = re.compile(r"[1-9][0-9]*")


class DashboardSettingsError(ValueError):
    """Raised when the futures environment is incomplete or invalid."""


@dataclass(frozen=True, slots=True)
class FuturesOperationSettings:
    """Validated configuration of the one operated futures contract and portfolio."""

    database: Path
    contract: FuturesContract
    strategy: StrategyIdentity
    portfolio: PaperPortfolioIdentity
    target: FuturesContractCount


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


def _operation(values: Mapping[str, str]) -> FuturesOperationSettings:
    product = FuturesProductReference(
        _value("NORTHSTAR_FUTURES_PRODUCT", values["NORTHSTAR_FUTURES_PRODUCT"], Symbol),
        _value("NORTHSTAR_FUTURES_EXCHANGE", values["NORTHSTAR_FUTURES_EXCHANGE"], ExchangeCode),
    )
    expiration = _value(
        "NORTHSTAR_FUTURES_EXPIRATION", values["NORTHSTAR_FUTURES_EXPIRATION"], ExpirationDate
    )
    return FuturesOperationSettings(
        database=Path(values["NORTHSTAR_DATABASE"]),
        contract=FuturesContract(product, expiration),
        strategy=_value("NORTHSTAR_STRATEGY", values["NORTHSTAR_STRATEGY"], StrategyIdentity),
        portfolio=_value(
            "NORTHSTAR_PORTFOLIO", values["NORTHSTAR_PORTFOLIO"], PaperPortfolioIdentity
        ),
        target=_value("NORTHSTAR_TARGET", values["NORTHSTAR_TARGET"], _target),
    )


def _read(env: Mapping[str, str], names: tuple[str, ...], purpose: str) -> dict[str, str] | None:
    values = {name: env.get(name, "").strip() for name in names}
    if not any(values.values()):
        return None
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise DashboardSettingsError(
            f"{purpose} configuration is incomplete; missing {', '.join(missing)}."
        )
    return values


def load_operation_settings(env: Mapping[str, str]) -> FuturesOperationSettings:
    """Return validated daily-operation settings, or raise when absent or incomplete."""
    values = _read(env, OPERATION_VARIABLES, "Futures operation")
    if values is None:
        raise DashboardSettingsError(
            "Futures operation is not configured; set " + ", ".join(OPERATION_VARIABLES) + "."
        )
    return _operation(values)


def load_dashboard_settings(env: Mapping[str, str]) -> DashboardSettings | None:
    """Return validated settings, None when none are set, or raise when incomplete."""
    values = _read(env, VARIABLES, "Futures dashboard")
    if values is None:
        return None
    operation = _operation(values)
    return DashboardSettings(
        database=operation.database,
        web_origin=_value("NORTHSTAR_WEB_ORIGIN", values["NORTHSTAR_WEB_ORIGIN"], _origin),
        contract=operation.contract,
        strategy=operation.strategy,
        portfolio=operation.portfolio,
        target=operation.target,
    )
