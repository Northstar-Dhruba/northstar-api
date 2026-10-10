"""Read-only operational status of the chronologically operated futures contract.

Derived on every dashboard read from persisted facts and safe configuration
only, through the very planning the chronological daily operation uses --
``plan_session_backlog`` over the venue's calendar and the configured finality
policy -- and the venue's pre-expiry flatten guard. Nothing is acquired,
frozen, executed or stored, no provider is contacted and no clock is read:
finality is the configured policy's assessment, never a time of day.

Only a venue the chronological operation supports has this status. Any other
venue is ``not_applicable`` rather than given NSE semantics.

No persisted fact records what an operation run did, so nothing here claims a
"last run" outcome; that stays in the operation's log. What is reported is the
latest processed state: the latest decision, the next required session and
whether persisted data or approved finality is ahead of it.

A calendar that fails closed, or a go-live the operation would refuse, makes
only the affected part unavailable, with its reason; the dashboard stays
readable.

A contract with no session left through expiry is ``expiry_exception``, not
``rollover_required``, while it still holds a position or a pending order:
only a fill makes it flat. The expiry window is assessed as of the latest
decision, never today's date; the daily operation alone compares the real
India date with the expiration date.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta

from northstar_application.application_services import (
    FuturesExpiryFlattenGuard,
    FuturesExpiryWindowAssessment,
    FuturesExpiryWindowError,
    FuturesPaperTradingSnapshot,
)
from northstar_application.ports import (
    FuturesSessionResolutionError,
    FuturesTradingSessionResolver,
)
from northstar_core.foundation.value_objects import PointInTime
from northstar_core.futures import FuturesContract, FuturesOHLCVBar

from northstar_api.operations import (
    FuturesContractExposure,
    FuturesOperationConfigurationError,
    FuturesSessionBacklog,
    contract_exposure,
    plan_session_backlog,
)
from northstar_api.runtime import (
    build_daily_bar_finality_policy,
    chronological_session_resolver,
    expiry_guard_for,
)
from northstar_api.schemas.futures import (
    ExpirySafetyResponse,
    FinalityResponse,
    OperationalBacklogResponse,
    OperationsResponse,
)
from northstar_api.settings import DashboardSettings

# Widens only the search for the session containing an instant; membership is
# decided by resolved session boundaries alone.
_MEMBERSHIP_SEARCH_MARGIN = timedelta(days=2)
_CALENDAR_ERRORS = (FuturesSessionResolutionError, FuturesOperationConfigurationError)


def operational_status(
    settings: DashboardSettings,
    snapshot: FuturesPaperTradingSnapshot | None,
    bars: Sequence[FuturesOHLCVBar],
) -> OperationsResponse:
    """Return the operational status as of the snapshot's cutoff.

    ``bars`` are the selected contract's persisted daily bars visible at that
    cutoff; the latest decision and latest bar are the snapshot's own.
    """
    contract = settings.contract
    resolver = chronological_session_resolver(contract)
    if resolver is None:
        venue = contract.product.exchange_code.value
        return OperationsResponse(
            status="not_applicable",
            reason=f"no chronological operation for {venue} contracts",
            finality=None,
            backlog=None,
            expiry=None,
        )

    latest = (
        snapshot.recent_decisions[0].record.decision_instant
        if snapshot is not None and snapshot.recent_decisions
        else None
    )
    latest_bar = snapshot.latest_market_bar if snapshot is not None else None
    exposure = contract_exposure(snapshot, contract) if snapshot is not None else None
    backlog, plan = _backlog(settings, resolver, latest, latest_bar, bars, exposure)
    guard = expiry_guard_for(contract)
    return OperationsResponse(
        status="available",
        reason=None,
        finality=_finality(settings, plan),
        backlog=backlog,
        expiry=_expiry(guard, contract, exposure, latest, plan) if guard is not None else None,
    )


# ---------------------------------------------------------------------------
# Finality
# ---------------------------------------------------------------------------


def _finality(settings: DashboardSettings, plan: FuturesSessionBacklog | None) -> FinalityResponse:
    operations = settings.operations
    following = plan.assessments[0] if plan is not None and plan.assessments else None
    return FinalityResponse(
        mode=operations.finality_mode.value,
        final_through=(
            operations.final_through.isoformat() if operations.final_through is not None else None
        ),
        next_session_outcome=following.outcome.value if following is not None else None,
        next_session_reason=following.reason if following is not None else None,
    )


# ---------------------------------------------------------------------------
# Backlog
# ---------------------------------------------------------------------------


def _backlog(
    settings: DashboardSettings,
    resolver: FuturesTradingSessionResolver,
    latest: PointInTime | None,
    latest_bar: FuturesOHLCVBar | None,
    bars: Sequence[FuturesOHLCVBar],
    exposure: FuturesContractExposure | None,
) -> tuple[OperationalBacklogResponse, FuturesSessionBacklog | None]:
    """Return the backlog section and, when it could be planned, the operation's plan."""
    contract, go_live = settings.contract, settings.operations.go_live
    try:
        latest_session = _session_of(resolver, contract, latest) if latest else None
        market_session = (
            _session_of(resolver, contract, latest_bar.point_in_time) if latest_bar else None
        )
        if latest is None and go_live is None:
            response = _backlog_response(
                stage="go_live_required", latest_market_session=market_session
            )
            return response, None
        plan = plan_session_backlog(
            resolver,
            build_daily_bar_finality_policy(settings.operations),
            contract,
            latest,
            go_live,
        )
    except _CALENDAR_ERRORS as error:
        return _backlog_response(status="unavailable", reason=str(error)), None

    following = plan.assessments[0].session if plan.assessments else None
    stored = (
        sum(1 for bar in bars if bar.point_in_time.compare(following.closes_at) >= 0)
        if following is not None
        else 0
    )
    final = len(plan.eligible)
    if plan.exhausted:
        unresolved = exposure is not None and not exposure.resolved
        stage = "expiry_exception" if unresolved else "rollover_required"
    else:
        stage = "not_started" if latest is None else "operating"
    response = _backlog_response(
        stage=stage,
        go_live=plan.go_live,
        latest_decision_session=latest_session,
        latest_market_session=market_session,
        next_required_session=following.trading_date if following is not None else None,
        next_required_cutoff=following.closes_at.value if following is not None else None,
        final_sessions_pending=final,
        stored_sessions_pending=stored,
        market_data_ahead=stored > 0,
        caught_up=final == 0 and stored == 0,
    )
    return response, plan


def _backlog_response(
    *,
    status: str = "available",
    reason: str | None = None,
    stage: str | None = None,
    go_live: date | None = None,
    latest_decision_session: date | None = None,
    latest_market_session: date | None = None,
    next_required_session: date | None = None,
    next_required_cutoff: str | None = None,
    final_sessions_pending: int | None = None,
    stored_sessions_pending: int | None = None,
    market_data_ahead: bool | None = None,
    caught_up: bool | None = None,
) -> OperationalBacklogResponse:
    return OperationalBacklogResponse(
        status=status,
        reason=reason,
        stage=stage,
        go_live=_day(go_live),
        latest_decision_session=_day(latest_decision_session),
        latest_market_session=_day(latest_market_session),
        next_required_session=_day(next_required_session),
        next_required_cutoff=next_required_cutoff,
        final_sessions_pending=_count(final_sessions_pending),
        stored_sessions_pending=_count(stored_sessions_pending),
        market_data_ahead=market_data_ahead,
        caught_up=caught_up,
    )


# ---------------------------------------------------------------------------
# Expiry safety
# ---------------------------------------------------------------------------


def _expiry(
    guard: FuturesExpiryFlattenGuard,
    contract: FuturesContract,
    exposure: FuturesContractExposure | None,
    latest: PointInTime | None,
    plan: FuturesSessionBacklog | None,
) -> ExpirySafetyResponse:
    window_size = guard.policy.sessions_before_expiry
    following = plan.assessments[0].session if plan is not None and plan.assessments else None
    common = {
        "expiry_session": contract.expiration_date.value,
        "flatten_sessions_before_expiry": str(window_size),
        "position_flat": exposure.position is None if exposure is not None else None,
        "pending_orders": _count(len(exposure.pending_orders)) if exposure is not None else None,
        "assessed_as_of": latest.value if latest is not None else None,
    }
    try:
        assessed = guard.assess(contract, latest) if latest is not None else None
        upcoming = guard.assess(contract, following.closes_at) if following is not None else None
    except (FuturesExpiryWindowError, FuturesSessionResolutionError) as error:
        return ExpirySafetyResponse(
            status="unavailable",
            reason=str(error),
            sessions_after_latest_decision=None,
            window=None,
            flatten_required=None,
            reopening_blocked=None,
            **common,
        )
    # The guard only tightens towards expiry: once a decision is governed, so
    # is every later one.
    if assessed is not None and assessed.flatten_required:
        blocked: bool | None = True
    else:
        blocked = upcoming.flatten_required if upcoming is not None else None
    return ExpirySafetyResponse(
        status="available",
        reason=None,
        sessions_after_latest_decision=(
            _count(assessed.sessions_after_decision_through_expiry) if assessed else None
        ),
        window=_window(assessed) if assessed is not None else None,
        flatten_required=assessed.flatten_required if assessed is not None else None,
        reopening_blocked=blocked,
        **common,
    )


def _window(assessment: FuturesExpiryWindowAssessment) -> str:
    remaining = assessment.sessions_after_decision_through_expiry
    if remaining > assessment.sessions_before_expiry + 1:
        return "outside"
    if remaining == assessment.sessions_before_expiry + 1:
        return "flatten"
    return "protected"


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------


def _session_of(
    resolver: FuturesTradingSessionResolver, contract: FuturesContract, instant: PointInTime
) -> date | None:
    """Return the trading date whose window ``opens_at < instant <= closes_at`` holds."""
    around = datetime.fromisoformat(instant.value.replace("Z", "+00:00")).astimezone(UTC).date()
    containing = [
        session.trading_date
        for session in resolver.sessions_in_range(
            contract.product,
            around - _MEMBERSHIP_SEARCH_MARGIN,
            around + _MEMBERSHIP_SEARCH_MARGIN,
        )
        if session.opens_at.compare(instant) < 0 and instant.compare(session.closes_at) <= 0
    ]
    return containing[0] if len(containing) == 1 else None


def _day(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _count(value: int | None) -> str | None:
    return str(value) if value is not None else None
