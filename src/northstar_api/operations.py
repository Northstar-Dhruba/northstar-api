"""Daily futures operation: the facts one scheduled invocation establishes.

The daily operation is an operational-layer composition, not a use case. It
decides nothing a use case already decides; it only answers two questions the
existing commands leave to the operator:

* which completed sessions are missing from persisted history, as of one
  captured instant, according to the resolved exchange calendar; and
* which cutoff the paper session runs at -- always the latest persisted daily
  bar's own point in time, never the clock.

The clock is read once, by the command, and passed in here.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TextIO

from northstar_application.application_services import FuturesPaperTradingSessionResult
from northstar_application.ports import (
    FuturesHistoricalMarketDataQuery,
    FuturesHistoricalMarketDataRepository,
    FuturesTradingSession,
    FuturesTradingSessionResolver,
)
from northstar_core.foundation.value_objects import PointInTime, Timeframe
from northstar_core.futures import FuturesContract, FuturesOHLCVBar

LOGGER_NAME = "northstar.operations"
NO_HISTORY = (
    "No persisted Futures history for {contract}. Bootstrap market data with "
    "'northstar market-data sync' before running scheduled daily operations."
)
_DAILY = Timeframe("1d")
# Widens only the calendar enumeration; membership is decided by resolved closes.
_ENUMERATION_MARGIN = timedelta(days=7)


@dataclass(frozen=True, slots=True)
class FuturesDailyOperationResult:
    """What one daily operation observed, acquired and processed."""

    captured_at: PointInTime
    contract: FuturesContract
    history_through: PointInTime
    sessions_considered: tuple[FuturesTradingSession, ...]
    daily_bars_acquired: int
    cutoff: PointInTime
    paper_session: FuturesPaperTradingSessionResult
    valuation_available: bool


def captured_instant(now: datetime) -> PointInTime:
    """Validate the one clock reading and return it as a canonical UTC instant."""
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise TypeError("The daily operation clock must return an aware datetime.")
    return PointInTime(now.astimezone(UTC).isoformat())


def latest_daily_bar(
    repository: FuturesHistoricalMarketDataRepository, contract: FuturesContract
) -> FuturesOHLCVBar | None:
    """Return the newest persisted daily bar of the contract, if any."""
    bars = repository.get_bars(FuturesHistoricalMarketDataQuery(contract, _DAILY))
    return bars[-1] if bars else None


def _utc(instant: PointInTime) -> datetime:
    return datetime.fromisoformat(instant.value.replace("Z", "+00:00"))


def completed_sessions_after(
    resolver: FuturesTradingSessionResolver,
    contract: FuturesContract,
    history_through: PointInTime,
    captured_at: PointInTime,
) -> tuple[FuturesTradingSession, ...]:
    """Return sessions closing after persisted history and strictly before the capture.

    A session is complete only once the captured instant is strictly after its
    resolved close, matching the acquisition guard. Early closes, holidays and
    weekends come from the calendar; no date or weekday is inferred here.
    """
    start = _utc(history_through).date() - _ENUMERATION_MARGIN
    end = _utc(captured_at).date() + timedelta(days=1)
    return tuple(
        session
        for session in resolver.sessions_in_range(contract.product, start, end)
        if session.closes_at.compare(history_through) > 0
        and captured_at.compare(session.closes_at) > 0
    )


class _Redacting(logging.Filter):
    def __init__(self, secrets: Sequence[str]) -> None:
        super().__init__()
        self._secrets = secrets

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for secret in self._secrets:
            message = message.replace(secret, "[REDACTED]")
        record.msg, record.args = message, None
        return True


@contextmanager
def operation_logger(stream: TextIO, secrets: Sequence[str]) -> Iterator[logging.Logger]:
    """Log plain lines to ``stream`` for one invocation, redacting every known secret."""
    logger = logging.getLogger(LOGGER_NAME)
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(name)s %(levelname)s %(message)s"))
    handler.addFilter(_Redacting(secrets))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        yield logger
    finally:
        logger.removeHandler(handler)
