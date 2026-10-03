"""Deployment settings for the private futures dashboard and daily operation.

Read only from the process environment, and only here. They name one monitored
contract, one strategy label, one paper portfolio and its fixed target. No
setting is persisted. The dashboard additionally needs the one web origin it
serves; the daily operation does not.

DATABENTO_API_KEY is deliberately not among them: the dashboard never acquires
market data, and the daily operation reads the secret separately at its command
boundary.

NORTHSTAR_FUTURES_MARKET_DATA_PROVIDER names the futures market-data provider
the daily operation acquires from. It is read on its own, only by the daily
operation, and is never inferred from which credential happens to be present.

Three more settings drive the chronological (Upstox / NSE) daily operation only:

    NORTHSTAR_FUTURES_DAILY_BAR_FINALITY   disabled (default) | operator-approved
    NORTHSTAR_FUTURES_FINAL_THROUGH        last trading date the operator approved
                                           as final; required when operator-approved
    NORTHSTAR_FUTURES_GO_LIVE              the first trading session the operated
                                           portfolio decides on, used only while
                                           no decision has been frozen yet

None of them carries a time of day: finality is never inferred from the clock.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
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
MARKET_DATA_PROVIDER_VARIABLE = "NORTHSTAR_FUTURES_MARKET_DATA_PROVIDER"
DAILY_BAR_FINALITY_VARIABLE = "NORTHSTAR_FUTURES_DAILY_BAR_FINALITY"
FINAL_THROUGH_VARIABLE = "NORTHSTAR_FUTURES_FINAL_THROUGH"
GO_LIVE_VARIABLE = "NORTHSTAR_FUTURES_GO_LIVE"
SESSION_OPERATION_VARIABLES = (
    DAILY_BAR_FINALITY_VARIABLE,
    FINAL_THROUGH_VARIABLE,
    GO_LIVE_VARIABLE,
)
_TRADING_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_ORIGIN = re.compile(r"https?://[^/\s]+")
_COUNT = re.compile(r"[1-9][0-9]*")


class DashboardSettingsError(ValueError):
    """Raised when the futures environment is incomplete or invalid."""


class FuturesMarketDataProvider(StrEnum):
    """The futures market-data providers runtime composition can select between.

    Each takes a different Application acquisition path: Databento supplies
    minute bars folded into daily session bars, Upstox supplies native daily
    candles. Which path a provider uses is decided in the runtime, never here.
    """

    DATABENTO = "databento"
    UPSTOX = "upstox"


def parse_market_data_provider(text: str) -> FuturesMarketDataProvider:
    """Return the named provider, or raise ValueError naming the supported ones."""
    try:
        return FuturesMarketDataProvider(text.strip().lower())
    except ValueError:
        supported = ", ".join(provider.value for provider in FuturesMarketDataProvider)
        raise ValueError(f"must be one of {supported}; got {text!r}") from None


def load_market_data_provider(env: Mapping[str, str]) -> FuturesMarketDataProvider:
    """Return the configured provider; unset or blank keeps the established Databento path."""
    text = env.get(MARKET_DATA_PROVIDER_VARIABLE, "").strip()
    if not text:
        return FuturesMarketDataProvider.DATABENTO
    return _value(MARKET_DATA_PROVIDER_VARIABLE, text, parse_market_data_provider)


class FuturesDailyBarFinalityMode(StrEnum):
    """How the chronological daily operation establishes daily-bar finality.

    DISABLED never establishes it, so nothing is acquired or decided.
    OPERATOR_APPROVED treats sessions through an explicit final-through trading
    date as final. There is deliberately no automatic, time-based mode.
    """

    DISABLED = "disabled"
    OPERATOR_APPROVED = "operator-approved"


@dataclass(frozen=True, slots=True)
class FuturesSessionOperationSettings:
    """Validated settings of the chronological daily operation.

    ``final_through`` is present exactly when the mode is OPERATOR_APPROVED.
    ``go_live`` is optional here; whether it is required depends on persisted
    state, which only the operation itself can read.
    """

    finality_mode: FuturesDailyBarFinalityMode
    final_through: date | None
    go_live: date | None


def _trading_date(text: str) -> date:
    if _TRADING_DATE.fullmatch(text) is None:
        raise ValueError("must be a trading date YYYY-MM-DD")
    return date.fromisoformat(text)


def _finality_mode(text: str) -> FuturesDailyBarFinalityMode:
    try:
        return FuturesDailyBarFinalityMode(text.strip().lower())
    except ValueError:
        supported = ", ".join(mode.value for mode in FuturesDailyBarFinalityMode)
        raise ValueError(f"must be one of {supported}; got {text!r}") from None


def load_session_operation_settings(env: Mapping[str, str]) -> FuturesSessionOperationSettings:
    """Return the chronological operation's finality and go-live settings.

    An unset finality mode is DISABLED. OPERATOR_APPROVED without a valid
    final-through date is an error, never a silent fall-back to DISABLED.
    """
    mode_text = env.get(DAILY_BAR_FINALITY_VARIABLE, "").strip()
    mode = (
        _value(DAILY_BAR_FINALITY_VARIABLE, mode_text, _finality_mode)
        if mode_text
        else FuturesDailyBarFinalityMode.DISABLED
    )
    final_through: date | None = None
    if mode is FuturesDailyBarFinalityMode.OPERATOR_APPROVED:
        text = env.get(FINAL_THROUGH_VARIABLE, "").strip()
        if not text:
            raise DashboardSettingsError(
                f"{FINAL_THROUGH_VARIABLE} is required when {DAILY_BAR_FINALITY_VARIABLE}="
                f"{mode.value}."
            )
        final_through = _value(FINAL_THROUGH_VARIABLE, text, _trading_date)
    go_live_text = env.get(GO_LIVE_VARIABLE, "").strip()
    go_live = _value(GO_LIVE_VARIABLE, go_live_text, _trading_date) if go_live_text else None
    return FuturesSessionOperationSettings(mode, final_through, go_live)


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
