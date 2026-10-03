"""Factual report over recorded Upstox daily-candle evidence. Draws no conclusion.

The report reads an evidence file written by ``finality-evidence observe``
and derives, per exact contract and trading date, what was observed and when
it was observed to change. Its only other input is Northstar's NSE calendar,
for each session's close. It contacts no provider, reads no token, opens no
Northstar database, reads no clock and writes nothing: the same file and
filters always give an equal report.

Observations and changes
------------------------
Observations of one contract and trading date form a session, ordered by
``requested_at``, then ``received_at``, then file line. The Upstox instrument
key is provider metadata and never splits a session; distinct keys are listed.

An observed change is an observation that differs from the immediately
previous observation of the same session in at least one tracked field:

    candle_presence, provider_timestamp, open, high, low, close, volume,
    open_interest, lot_size, volume_contracts

Each field is compared on its own, numbers by Decimal value (``25010.5``
equals ``25010.50``; the recorded text stays what is shown). When the candle
appears or disappears only ``candle_presence`` is reported for it and for the
contract count derived from it. A
lot-size change is reported as ``lot_size`` and, when the derived contract
count differs, ``volume_contracts`` -- never as ``volume``, which is the raw
provider volume. An identical repeat is an unchanged observation.

An observed change is timed by the observation that saw it. When the provider
actually revised the candle is unknown: only that it differed by then.

Timing from the close
---------------------
Each observation's elapsed time is ``requested_at`` minus the session's
resolved NSE close, an exact signed timedelta; an observation before the
close is negative and kept. When the calendar cannot establish the session
(an unloaded year, an un-notified special session such as the 2026-11-08
Muhurat, a non-session date, another venue) the close is unavailable with the
calendar's reason, and elapsed times are None -- never zero.

Right-censoring
---------------
Every session is right-censored at its last observation. No observed change
after a time does not show that the provider made no later revision: a
revision after the last observation, or after the contract stopped being
collectible (an expired contract leaves the Upstox instrument master), is
not in the evidence at all.

Aggregates are descriptive statistics of these observations under their
collection schedule. There is no threshold, probability or verdict.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from northstar_application.ports import FuturesSessionResolutionError
from northstar_core.foundation.value_objects import ExchangeCode, PointInTime, Symbol
from northstar_core.futures import FuturesProductReference
from northstar_infrastructure.market_data import (
    MalformedUpstoxCandleEvidenceError,
    NSEFuturesTradingSessionResolver,
)

CHANGE_FIELDS = (
    "candle_presence",
    "provider_timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
    "lot_size",
    "volume_contracts",
)
_CANDLE_FIELDS = ("provider_timestamp", "open", "high", "low", "close", "volume", "open_interest")


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class EvidenceSessionKey:
    """The logical session: an exact contract and trading date."""

    product: str
    exchange: str
    expiration: str
    trading_date: date

    @property
    def contract_label(self) -> str:
        return f"{self.product}@{self.exchange} {self.expiration}"


@dataclass(frozen=True, slots=True)
class EvidenceCandle:
    """A recorded candle; ``text`` holds each field's recorded text for display."""

    provider_timestamp: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    open_interest: Decimal | None
    text: dict[str, str | None]


@dataclass(frozen=True, slots=True)
class EvidenceObservation:
    line: int
    key: EvidenceSessionKey
    requested_at: datetime
    received_at: datetime
    instrument_key: str
    lot_size: Decimal
    lot_size_text: str
    candle: EvidenceCandle | None
    volume_contracts: Decimal | None
    volume_contracts_text: str | None
    volume_contracts_note: str | None


@dataclass(frozen=True, slots=True)
class ObservedChange:
    """An observation that differed from the previous one in ``changed_fields``."""

    observation: EvidenceObservation
    elapsed_from_close: timedelta | None
    changed_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SessionEvidence:
    """Everything observed for one contract and trading date, in observation order."""

    key: EvidenceSessionKey
    session_close: datetime | None
    close_unavailable_reason: str | None
    observations: tuple[EvidenceObservation, ...]
    elapsed_from_close: tuple[timedelta | None, ...]
    changes: tuple[ObservedChange, ...]
    unchanged_since_last_change: int
    instrument_keys: tuple[str, ...]

    @property
    def first_observed(self) -> datetime:
        return self.observations[0].requested_at

    @property
    def last_observed(self) -> datetime:
        """Also the instant at which the session's evidence is right-censored."""
        return self.observations[-1].requested_at

    @property
    def observation_span(self) -> timedelta:
        return self.last_observed - self.first_observed

    @property
    def first_candle_observation(self) -> EvidenceObservation | None:
        return next((o for o in self.observations if o.candle is not None), None)

    @property
    def candle_appeared_after_absence(self) -> bool:
        """Whether an observation without a candle preceded the first one with it."""
        first = self.first_candle_observation
        return first is not None and first is not self.observations[0]

    @property
    def last_observed_change(self) -> ObservedChange | None:
        return self.changes[-1] if self.changes else None

    @property
    def latest(self) -> EvidenceObservation:
        return self.observations[-1]


@dataclass(frozen=True, slots=True)
class DurationSummary:
    count: int
    minimum: timedelta
    median: timedelta
    maximum: timedelta


@dataclass(frozen=True, slots=True)
class EvidenceAggregate:
    """Descriptive statistics over the reported sessions; no threshold or verdict.

    ``last_observed_change_delays`` covers sessions with a resolved close and
    at least one observed change, measured to their last observed change.
    """

    session_count: int
    observation_count: int
    sessions_with_observed_change: int
    sessions_without_observed_change: int
    sessions_with_candle_after_absence: int
    sessions_with_close_unavailable: int
    observations_per_session_minimum: int | None
    observations_per_session_median: Decimal | None
    observations_per_session_maximum: int | None
    last_observed_change_delays: DurationSummary | None


@dataclass(frozen=True, slots=True)
class FinalityEvidenceReport:
    sessions: tuple[SessionEvidence, ...]
    aggregate: EvidenceAggregate


@dataclass(frozen=True, slots=True)
class EvidenceFilters:
    product: str | None = None
    exchange: str | None = None
    expiration: str | None = None
    trading_date: date | None = None

    def matches(self, key: EvidenceSessionKey) -> bool:
        return (
            (self.product is None or key.product == self.product)
            and (self.exchange is None or key.exchange == self.exchange)
            and (self.expiration is None or key.expiration == self.expiration)
            and (self.trading_date is None or key.trading_date == self.trading_date)
        )


# ---------------------------------------------------------------------------
# Parsing recorded evidence
# ---------------------------------------------------------------------------


def _malformed(line: int, what: str) -> MalformedUpstoxCandleEvidenceError:
    return MalformedUpstoxCandleEvidenceError(f"Evidence line {line} has {what}.", line)


def _text(record: dict[str, Any], name: str, line: int) -> str:
    value = record.get(name)
    if not isinstance(value, str) or not value:
        raise _malformed(line, f"no valid {name}")
    return value


def _decimal(value: Any, name: str, line: int) -> Decimal:
    if not isinstance(value, str):
        raise _malformed(line, f"no exact numeric text for {name}")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise _malformed(line, f"an unreadable {name}") from exc
    if not number.is_finite():
        raise _malformed(line, f"a non-finite {name}")
    return number


def _instant(value: Any, name: str, line: int) -> datetime:
    try:
        canonical = PointInTime(value).value
    except (TypeError, ValueError) as exc:
        raise _malformed(line, f"an unreadable {name}") from exc
    return datetime.fromisoformat(canonical.replace("Z", "+00:00"))


def _observation(record: dict[str, Any], line: int) -> EvidenceObservation:
    contract = record.get("contract")
    if not isinstance(contract, dict):
        raise _malformed(line, "no contract")
    try:
        trading_date = date.fromisoformat(_text(record, "trading_date", line))
    except ValueError as exc:
        raise _malformed(line, "an unreadable trading_date") from exc
    key = EvidenceSessionKey(
        product=_text(contract, "product", line),
        exchange=_text(contract, "exchange", line),
        expiration=_text(contract, "expiration", line),
        trading_date=trading_date,
    )
    requested_at = _instant(record.get("requested_at"), "requested_at", line)
    received_at = _instant(record.get("received_at"), "received_at", line)
    lot_text = _text(record, "lot_size", line)

    raw = record.get("candle")
    candle: EvidenceCandle | None = None
    if raw is not None:
        if not isinstance(raw, dict):
            raise _malformed(line, "a candle that is not an object")
        interest = raw.get("open_interest")
        candle = EvidenceCandle(
            provider_timestamp=_text(raw, "provider_timestamp", line),
            open=_decimal(raw.get("open"), "open", line),
            high=_decimal(raw.get("high"), "high", line),
            low=_decimal(raw.get("low"), "low", line),
            close=_decimal(raw.get("close"), "close", line),
            volume=_decimal(raw.get("volume"), "volume", line),
            open_interest=None if interest is None else _decimal(interest, "open_interest", line),
            text={name: raw.get(name) for name in _CANDLE_FIELDS},
        )
    contracts_text = record.get("volume_contracts")
    return EvidenceObservation(
        line=line,
        key=key,
        requested_at=requested_at,
        received_at=received_at,
        instrument_key=_text(record, "instrument_key", line),
        lot_size=_decimal(lot_text, "lot_size", line),
        lot_size_text=lot_text,
        candle=candle,
        volume_contracts=(
            None if contracts_text is None else _decimal(contracts_text, "volume_contracts", line)
        ),
        volume_contracts_text=contracts_text,
        volume_contracts_note=record.get("volume_contracts_note"),
    )


# ---------------------------------------------------------------------------
# Derivation
# ---------------------------------------------------------------------------


def changed_fields(previous: EvidenceObservation, current: EvidenceObservation) -> tuple[str, ...]:
    """Return the tracked fields that differ, in CHANGE_FIELDS order."""
    changed: list[str] = []
    before, after = previous.candle, current.candle
    if (before is None) != (after is None):
        changed.append("candle_presence")
    elif before is not None and after is not None:
        changed += [
            name for name in _CANDLE_FIELDS if getattr(before, name) != getattr(after, name)
        ]
    if previous.lot_size != current.lot_size:
        changed.append("lot_size")
    # Derived from the candle, so compared only when both observations have one.
    if (
        before is not None
        and after is not None
        and previous.volume_contracts != current.volume_contracts
    ):
        changed.append("volume_contracts")
    return tuple(name for name in CHANGE_FIELDS if name in changed)


def _close(
    resolver: NSEFuturesTradingSessionResolver, key: EvidenceSessionKey
) -> tuple[datetime | None, str | None]:
    try:
        product = FuturesProductReference(Symbol(key.product), ExchangeCode(key.exchange))
        session = resolver.resolve(product, key.trading_date)
    except (FuturesSessionResolutionError, TypeError, ValueError) as error:
        return None, str(error)
    if session is None:
        return None, f"{key.trading_date.isoformat()} is not an NSE futures trading session"
    return datetime.fromisoformat(session.closes_at.value.replace("Z", "+00:00")), None


def _session(
    resolver: NSEFuturesTradingSessionResolver,
    key: EvidenceSessionKey,
    observations: Sequence[EvidenceObservation],
) -> SessionEvidence:
    ordered = tuple(sorted(observations, key=lambda o: (o.requested_at, o.received_at, o.line)))
    close, reason = _close(resolver, key)

    def elapsed(observation: EvidenceObservation) -> timedelta | None:
        return None if close is None else observation.requested_at - close

    changes: list[ObservedChange] = []
    since = 0
    for previous, current in zip(ordered, ordered[1:], strict=False):
        fields = changed_fields(previous, current)
        if fields:
            changes.append(ObservedChange(current, elapsed(current), fields))
            since = 0
        else:
            since += 1
    keys: list[str] = []
    for observation in ordered:
        if observation.instrument_key not in keys:
            keys.append(observation.instrument_key)
    return SessionEvidence(
        key=key,
        session_close=close,
        close_unavailable_reason=reason,
        observations=ordered,
        elapsed_from_close=tuple(elapsed(o) for o in ordered),
        changes=tuple(changes),
        unchanged_since_last_change=since,
        instrument_keys=tuple(keys),
    )


def _median_count(counts: list[int]) -> Decimal:
    ordered = sorted(counts)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return Decimal(ordered[middle])
    return (Decimal(ordered[middle - 1]) + Decimal(ordered[middle])) / 2


def _durations(values: list[timedelta]) -> DurationSummary | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    median = ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2
    return DurationSummary(len(ordered), ordered[0], median, ordered[-1])


def _aggregate(sessions: tuple[SessionEvidence, ...]) -> EvidenceAggregate:
    counts = [len(session.observations) for session in sessions]
    delays = [
        session.last_observed_change.elapsed_from_close
        for session in sessions
        if session.last_observed_change is not None
        and session.last_observed_change.elapsed_from_close is not None
    ]
    revised = sum(1 for session in sessions if session.changes)
    return EvidenceAggregate(
        session_count=len(sessions),
        observation_count=sum(counts),
        sessions_with_observed_change=revised,
        sessions_without_observed_change=len(sessions) - revised,
        sessions_with_candle_after_absence=sum(
            1 for session in sessions if session.candle_appeared_after_absence
        ),
        sessions_with_close_unavailable=sum(
            1 for session in sessions if session.session_close is None
        ),
        observations_per_session_minimum=min(counts) if counts else None,
        observations_per_session_median=_median_count(counts) if counts else None,
        observations_per_session_maximum=max(counts) if counts else None,
        last_observed_change_delays=_durations(delays),
    )


def build_report(
    records: Sequence[dict[str, Any]],
    filters: EvidenceFilters = EvidenceFilters(),  # noqa: B008 - immutable
) -> FinalityEvidenceReport:
    """Derive the report from records in file order; line numbers start at 1."""
    observations = [_observation(record, line) for line, record in enumerate(records, start=1)]
    grouped: dict[EvidenceSessionKey, list[EvidenceObservation]] = {}
    for observation in observations:
        if filters.matches(observation.key):
            grouped.setdefault(observation.key, []).append(observation)
    resolver = NSEFuturesTradingSessionResolver()
    sessions = tuple(
        _session(resolver, key, grouped[key])
        for key in sorted(
            grouped, key=lambda k: (k.product, k.exchange, k.expiration, k.trading_date)
        )
    )
    return FinalityEvidenceReport(sessions=sessions, aggregate=_aggregate(sessions))


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _utc(value: datetime) -> str:
    return PointInTime(value.isoformat()).value


def format_duration(value: timedelta) -> str:
    """Exact signed elapsed time, e.g. ``+5h 03m 12s`` or ``-0h 15m 00.5s``."""
    micro = value // timedelta(microseconds=1)
    sign = "-" if micro < 0 else "+"
    micro = abs(micro)
    seconds, fraction = divmod(micro, 1_000_000)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    tail = f".{fraction:06d}".rstrip("0") if fraction else ""
    return f"{sign}{hours}h {minutes:02d}m {seconds:02d}{tail}s"


def _elapsed(value: timedelta | None) -> str:
    return "close unavailable" if value is None else f"{format_duration(value)} from close"


def _session_lines(session: SessionEvidence) -> list[str]:
    key = session.key
    lines = [
        "",
        f"SESSION {key.contract_label} | trading date {key.trading_date.isoformat()}",
        (
            f"Session close: {_utc(session.session_close)}"
            if session.session_close is not None
            else f"Session close: unavailable ({session.close_unavailable_reason})"
        ),
        f"Observations: {len(session.observations)}",
        f"First observed: {_utc(session.first_observed)} "
        f"({_elapsed(session.elapsed_from_close[0])})",
        f"Last observed: {_utc(session.last_observed)} "
        f"({_elapsed(session.elapsed_from_close[-1])})",
        f"Observation span: {format_duration(session.observation_span)}",
    ]
    first = session.first_candle_observation
    if first is None:
        lines.append("First candle observed: none (every observation returned no candle)")
    else:
        index = session.observations.index(first)
        lines.append(
            f"First candle observed: {_utc(first.requested_at)} "
            f"({_elapsed(session.elapsed_from_close[index])})"
            + (
                "; after an observation without a candle"
                if session.candle_appeared_after_absence
                else ""
            )
        )
    if len(session.instrument_keys) > 1:
        lines.append(
            f"Provider instrument keys observed: {len(session.instrument_keys)} (provider metadata)"
        )
    lines.append(f"Observed changes: {len(session.changes)}")
    lines += [
        f"  {_utc(change.observation.requested_at)} ({_elapsed(change.elapsed_from_close)}): "
        + ", ".join(change.changed_fields)
        for change in session.changes
    ]
    last = session.last_observed_change
    lines.append(
        "Last observed change: none (no observed change)"
        if last is None
        else f"Last observed change: {_utc(last.observation.requested_at)} "
        f"({_elapsed(last.elapsed_from_close)})"
    )
    lines.append(f"Unchanged observations since: {session.unchanged_since_last_change}")
    lines.append(
        f"Right-censored at: {_utc(session.last_observed)}; a revision after the last "
        "observation would not appear in this evidence."
    )
    latest = session.latest
    lines.append(f"Latest observation: {_utc(latest.requested_at)}")
    if latest.candle is None:
        lines.append("  Candle: none returned")
    else:
        text = latest.candle.text
        contracts = latest.volume_contracts_text or f"unavailable ({latest.volume_contracts_note})"
        lines += [
            f"  Provider timestamp: {text['provider_timestamp']}",
            f"  Open: {text['open']}",
            f"  High: {text['high']}",
            f"  Low: {text['low']}",
            f"  Close: {text['close']}",
            f"  Volume (provider units): {text['volume']}",
            f"  Volume (contracts): {contracts}",
            f"  Open interest: {text['open_interest'] or 'not provided'}",
        ]
    lines.append(f"  Lot size: {latest.lot_size_text}")
    return lines


def _aggregate_lines(aggregate: EvidenceAggregate) -> list[str]:
    lines = [
        "",
        "AGGREGATE (observed under this collection schedule)",
        f"Sessions observed: {aggregate.session_count}",
        f"Observations: {aggregate.observation_count}",
    ]
    if aggregate.session_count:
        lines.append(
            "Observations per session: "
            f"min {aggregate.observations_per_session_minimum} / "
            f"median {aggregate.observations_per_session_median} / "
            f"max {aggregate.observations_per_session_maximum}"
        )
    lines += [
        f"Sessions with an observed change: {aggregate.sessions_with_observed_change}",
        f"Sessions with no observed change: {aggregate.sessions_without_observed_change}",
        "Sessions whose candle was first observed after an observation without one: "
        f"{aggregate.sessions_with_candle_after_absence}",
        f"Sessions with session close unavailable: {aggregate.sessions_with_close_unavailable}",
    ]
    delays = aggregate.last_observed_change_delays
    if delays is None:
        lines.append(
            "Last observed change from close: none (no session with a resolved close "
            "and an observed change)"
        )
    else:
        lines.append(
            "Last observed change from close (sessions with a resolved close and an "
            f"observed change: {delays.count}): min {format_duration(delays.minimum)} / "
            f"median {format_duration(delays.median)} / max {format_duration(delays.maximum)}"
        )
    lines.append(
        "Each session is right-censored at its last observation; these figures describe "
        "the recorded observations only and carry no threshold."
    )
    return lines


def report_lines(
    report: FinalityEvidenceReport, evidence: str, filters: EvidenceFilters
) -> list[str]:
    applied = [
        f"{name}={value}"
        for name, value in (
            ("product", filters.product),
            ("exchange", filters.exchange),
            ("expiration", filters.expiration),
            ("trading-date", filters.trading_date.isoformat() if filters.trading_date else None),
        )
        if value is not None
    ]
    lines = [
        "CANDLE EVIDENCE REPORT",
        f"Evidence file: {evidence}",
        f"Filters: {', '.join(applied) if applied else 'none'}",
        "Observation times are requested_at instants; an observed change is timed by the "
        "observation that saw it, not by when the provider made it.",
        "Collection may stop once an expired contract leaves the Upstox instrument master; "
        "later revisions would then be unobserved.",
    ]
    if not report.sessions:
        lines.append("No observations match.")
    for session in report.sessions:
        lines += _session_lines(session)
    return lines + _aggregate_lines(report.aggregate)
