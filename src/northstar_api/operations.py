"""Daily futures operation: the facts one scheduled invocation establishes.

The daily operation is an operational-layer composition, not a use case. It
decides nothing a use case already decides; it only answers two questions the
existing commands leave to the operator:

* which completed sessions are missing from persisted history, as of one
  captured instant, according to the resolved exchange calendar; and
* which cutoff the paper session runs at -- always the latest persisted daily
  bar's own point in time, never the clock.

The clock is read once, by the command, and passed in here.

The chronological operation (Upstox / NSE) is planned here too, from persisted
facts only and without any clock: the latest frozen decision of the operated
contract and strategy, or an explicit go-live session while there is none; the
resolved sessions after it through the contract's expiry; and the finality
policy's assessment of each, stopping at the first session that is not final.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import TextIO

from northstar_application.application_services import FuturesPaperTradingSessionResult
from northstar_application.ports import (
    FuturesDailyBarFinality,
    FuturesDailyBarFinalityPolicy,
    FuturesForwardResearchRecordQuery,
    FuturesForwardResearchRecordRepository,
    FuturesHistoricalMarketDataQuery,
    FuturesHistoricalMarketDataRepository,
    FuturesTradingSession,
    FuturesTradingSessionResolver,
)
from northstar_core.foundation.value_objects import PointInTime, Timeframe
from northstar_core.futures import FuturesContract, FuturesOHLCVBar
from northstar_core.strategy import StrategyIdentity

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


class FuturesOperationConfigurationError(ValueError):
    """Raised when the chronological operation's go-live configuration cannot be used."""


@dataclass(frozen=True, slots=True)
class FuturesSessionBacklog:
    """The sessions the chronological operation must process next, in order.

    ``assessments`` holds every session assessed, in order: the final ones,
    then the first one that is not final, if any, where assessment stopped.
    ``eligible`` is the final prefix. ``exhausted`` means no trading session
    remains after the latest decision through the contract's expiry.
    """

    latest_decision: PointInTime | None
    go_live: date | None
    assessments: tuple[FuturesDailyBarFinality, ...]
    eligible: tuple[FuturesTradingSession, ...]
    exhausted: bool

    @property
    def waiting_on(self) -> FuturesDailyBarFinality | None:
        """Return the first non-final assessment, where processing must stop."""
        last = self.assessments[-1] if self.assessments else None
        return None if last is None or last.is_final else last


def latest_frozen_decision(
    repository: FuturesForwardResearchRecordRepository,
    contract: FuturesContract,
    strategy: StrategyIdentity,
) -> PointInTime | None:
    """Return the latest frozen decision instant of one contract and strategy, if any."""
    latest: PointInTime | None = None
    for record in repository.get_records(FuturesForwardResearchRecordQuery(contract, _DAILY)):
        if record.strategy_identity != strategy:
            continue
        if latest is None or record.decision_instant.compare(latest) > 0:
            latest = record.decision_instant
    return latest


def plan_session_backlog(
    resolver: FuturesTradingSessionResolver,
    policy: FuturesDailyBarFinalityPolicy,
    contract: FuturesContract,
    latest_decision: PointInTime | None,
    go_live: date | None,
) -> FuturesSessionBacklog:
    """Return the next sessions to process, assessed in order, never past the first non-final.

    Without a frozen decision, processing starts at ``go_live``, which must be
    a trading session of the contract's venue no later than its expiry. With
    one, it resumes at the first session closing after that decision; go-live
    no longer applies. Sessions are only ever resolved, never computed from
    calendar days, and resolver failures propagate unchanged.
    """
    product = contract.product
    expiry = date.fromisoformat(contract.expiration_date.value)
    if latest_decision is None:
        if go_live is None:
            raise FuturesOperationConfigurationError(
                f"No decision has been frozen for {contract} yet; set "
                "NORTHSTAR_FUTURES_GO_LIVE to the first trading session to operate."
            )
        if go_live > expiry:
            raise FuturesOperationConfigurationError(
                f"NORTHSTAR_FUTURES_GO_LIVE {go_live.isoformat()} is after the expiry of "
                f"{contract}."
            )
        if resolver.resolve(product, go_live) is None:
            raise FuturesOperationConfigurationError(
                f"NORTHSTAR_FUTURES_GO_LIVE {go_live.isoformat()} is not a trading session "
                f"of {product.exchange_code}."
            )
        candidates = resolver.sessions_in_range(product, go_live, expiry)
    else:
        # The decision instant is its session's close, so later sessions are
        # exactly those resolved from that civil date that close after it.
        start = _utc(latest_decision).date()
        candidates = (
            ()
            if start > expiry
            else tuple(
                session
                for session in resolver.sessions_in_range(product, start, expiry)
                if session.closes_at.compare(latest_decision) > 0
            )
        )

    assessments: list[FuturesDailyBarFinality] = []
    eligible: list[FuturesTradingSession] = []
    for session in candidates:
        assessment = policy.assess(contract, session)
        assessments.append(assessment)
        if not assessment.is_final:
            break
        eligible.append(session)
    return FuturesSessionBacklog(
        latest_decision=latest_decision,
        go_live=go_live if latest_decision is None else None,
        assessments=tuple(assessments),
        eligible=tuple(eligible),
        exhausted=not candidates,
    )


class FuturesChronologicalOperationStatus(StrEnum):
    """How one chronological daily operation ended without an error."""

    COMPLETED = "COMPLETED"
    WAITING = "WAITING"


@dataclass(frozen=True, slots=True)
class FuturesChronologicalOperationResult:
    """What one chronological daily operation assessed, acquired and processed."""

    contract: FuturesContract
    backlog: FuturesSessionBacklog
    acquired_sessions: int
    processed: tuple[FuturesTradingSession, ...]
    valuation_available: bool

    @property
    def status(self) -> FuturesChronologicalOperationStatus:
        if self.processed:
            return FuturesChronologicalOperationStatus.COMPLETED
        return FuturesChronologicalOperationStatus.WAITING


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
