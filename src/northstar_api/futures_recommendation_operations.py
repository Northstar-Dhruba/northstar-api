"""Private futures recommendation operations: monitor, then generate (F-5D-2).

Northstar is a private, single-user decision-support tool. A recommendation
here is an internal record in the operated SQLite file. Nothing is published,
delivered, notified or executed, and nothing here can do any of that.

This module composes existing Application use cases; it decides no price,
lifecycle transition or status itself. The daily operation calls
``run_futures_recommendation_cycle`` under its DatabaseOperationsLock, only
when ``NORTHSTAR_FUTURES_RECOMMENDATIONS=enabled``:

1. monitor every open recommendation of the product, on every expiry, through
   final, operator-reviewed stored candles (MonitorFuturesTradeRecommendationsUseCase);
2. re-read the durable lifecycles;
3. refuse to generate when more than one is open (an anomaly), or when one is
   open on another expiry than the operated contract: daily acquisition
   follows only the operated contract, so that recommendation would never
   receive the candles that can close it (unsafe rollover);
4. otherwise generate for the operated contract and strategy
   (GenerateFuturesTradeRecommendationsUseCase), which records any new
   recommendation together with its RECORDED audit, atomically.

Every read verifies each recommendation's RECORDED audit and lifecycle
evidence (F-5D-1); corruption raises and is never reported as a rejection.

Approved configurations
-----------------------
A generation policy and profile run only if named in an approved registry.
The production registry, APPROVED_FUTURES_RECOMMENDATION_CONFIGURATIONS, is
empty: ADR-018 approves no production lookback, multipliers, price increment,
minimum reward-to-risk or validity horizon. So enabling the feature with any
profile name is a configuration error until one is approved and added here in
code. Tests supply a synthetic registry explicitly; nothing falls back to it.

Contract cutoff
---------------
OperatorConfiguredFuturesContractCutoffSource answers only for the operated
contract, and only with the last session the operator configured, after
checking that the NSE calendar resolves it and that it is not after the
expiration date. Its authority reads ``operator-configured: <reference>``:
it is the operator's statement of where the date came from, not an exchange
verification by Northstar. Without one, generation is BLOCKED with
CONTRACT_CUTOFF_UNAVAILABLE. No cutoff is ever derived from the expiration
date.

Evidence provenance
-------------------
Candles are evaluated only when the finality policy says FINAL and the
operator manifest approves their exact fingerprint. Lifecycle steps persist the
fingerprint of the candle evaluated, not who approved it or when: that record
stays in the operator's evidence register.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType

from northstar_application.application_services import (
    FuturesIncompleteCoverageDeclaration,
    FuturesRecommendationMonitoringOutcome,
    FuturesRecommendationMonitoringResult,
    FuturesRecommendationMonitoringStopReason,
    FuturesReviewedCandleVerification,
    FuturesTradeRecommendationGenerationProfile,
    FuturesTradeRecommendationGenerationResult,
    GenerateFuturesTradeRecommendationsUseCase,
    MonitorFuturesTradeRecommendationsUseCase,
)
from northstar_application.ports import (
    FuturesContractCutoff,
    FuturesContractCutoffSource,
    FuturesDailyBarFinalityPolicy,
    FuturesTradeRecommendationQuery,
    FuturesTradingSessionResolver,
)
from northstar_core.foundation.value_objects import PointInTime
from northstar_core.futures import FuturesContract
from northstar_core.recommendations.futures import (
    FuturesRecommendationGenerationPolicy,
    FuturesTradeRecommendationLifecycle,
)
from northstar_core.strategy import StrategyIdentity

from northstar_api.runtime import FuturesRecommendationRuntime
from northstar_api.settings import (
    CONTRACT_LAST_SESSION_VARIABLE,
    RECOMMENDATION_PROFILE_VARIABLE,
    FuturesRecommendationSettings,
)

_Stop = FuturesRecommendationMonitoringStopReason


class FuturesRecommendationConfigurationError(ValueError):
    """Raised when the recommendation feature's configuration cannot be used."""


@dataclass(frozen=True, slots=True)
class FuturesRecommendationConfiguration:
    """One approved generation policy and the profile that names it."""

    policy: FuturesRecommendationGenerationPolicy
    profile: FuturesTradeRecommendationGenerationProfile

    def __post_init__(self) -> None:
        if self.profile.policy_version != self.policy.version:
            raise FuturesRecommendationConfigurationError(
                f"Profile {self.profile.version} names policy {self.profile.policy_version}, "
                f"not {self.policy.version}."
            )


# ADR-018 approves no production parameters, so no configuration is approved.
APPROVED_FUTURES_RECOMMENDATION_CONFIGURATIONS: Mapping[str, FuturesRecommendationConfiguration] = (
    MappingProxyType({})
)


def select_configuration(
    approved: Mapping[str, FuturesRecommendationConfiguration],
    settings: FuturesRecommendationSettings,
    strategy: StrategyIdentity,
) -> FuturesRecommendationConfiguration:
    """Return the approved configuration the settings name for ``strategy``, or refuse."""
    configuration = approved.get(settings.profile_version or "")
    if configuration is None:
        known = ", ".join(sorted(approved)) or "none is approved"
        raise FuturesRecommendationConfigurationError(
            f"{RECOMMENDATION_PROFILE_VARIABLE}={settings.profile_version} is not an approved "
            f"recommendation configuration ({known}). Nothing was generated."
        )
    if configuration.profile.version != settings.profile_version:
        raise FuturesRecommendationConfigurationError(
            f"Approved configuration {settings.profile_version} holds profile "
            f"{configuration.profile.version}."
        )
    if strategy not in configuration.policy.approved_strategies:
        raise FuturesRecommendationConfigurationError(
            f"Recommendation configuration {settings.profile_version} does not approve the "
            f"operated strategy {strategy}."
        )
    return configuration


class OperatorConfiguredFuturesContractCutoffSource(FuturesContractCutoffSource):
    """The operated contract's last session as the operator configured it, or nothing."""

    def __init__(self, contract: FuturesContract, cutoff: FuturesContractCutoff | None) -> None:
        self._contract = contract
        self._cutoff = cutoff

    def resolve(self, contract: FuturesContract) -> FuturesContractCutoff | None:
        return self._cutoff if contract == self._contract else None


def operator_cutoff_source(
    sessions: FuturesTradingSessionResolver,
    contract: FuturesContract,
    settings: FuturesRecommendationSettings,
) -> OperatorConfiguredFuturesContractCutoffSource:
    """Return the configured cutoff source after checking it against the calendar."""
    if settings.last_session is None:
        return OperatorConfiguredFuturesContractCutoffSource(contract, None)
    expiration = date.fromisoformat(contract.expiration_date.value)
    if settings.last_session > expiration:
        raise FuturesRecommendationConfigurationError(
            f"{CONTRACT_LAST_SESSION_VARIABLE} {settings.last_session.isoformat()} is after "
            f"{contract}'s expiration date."
        )
    session = sessions.resolve(contract.product, settings.last_session)
    if session is None:
        raise FuturesRecommendationConfigurationError(
            f"{CONTRACT_LAST_SESSION_VARIABLE} {settings.last_session.isoformat()} is not a "
            f"trading session of {contract.product.exchange_code}."
        )
    return OperatorConfiguredFuturesContractCutoffSource(
        contract,
        FuturesContractCutoff(
            contract, session, f"operator-configured: {settings.cutoff_authority}"
        ),
    )


# ---------------------------------------------------------------------------
# The cycle
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FuturesRecommendationCycleResult:
    """What one recommendation cycle monitored, found and generated.

    ``lifecycles`` is the durable state re-read after monitoring. When
    ``open_anomaly`` or ``rollover_contracts`` is set, generation did not run
    (``generation`` is None) and an operator must act.
    """

    monitoring: FuturesRecommendationMonitoringResult
    lifecycles: tuple[FuturesTradeRecommendationLifecycle, ...]
    open_anomaly: bool
    rollover_contracts: tuple[FuturesContract, ...]
    generation: FuturesTradeRecommendationGenerationResult | None

    @property
    def generation_refused(self) -> bool:
        return self.open_anomaly or bool(self.rollover_contracts)


def run_futures_recommendation_cycle(
    runtime: FuturesRecommendationRuntime,
    *,
    contract: FuturesContract,
    strategy: StrategyIdentity,
    configuration: FuturesRecommendationConfiguration,
    cutoffs: FuturesContractCutoffSource,
    sessions: FuturesTradingSessionResolver,
    finality: FuturesDailyBarFinalityPolicy,
    reviewed: FuturesReviewedCandleVerification,
    as_of: PointInTime,
) -> FuturesRecommendationCycleResult:
    """Monitor every open recommendation of the product, then generate if it is safe."""
    product = contract.product
    monitoring = MonitorFuturesTradeRecommendationsUseCase(
        runtime.repository, runtime.lifecycle_store, runtime.market_repository, sessions, finality
    ).execute(product=product, as_of=as_of, reviewed_candles=reviewed)

    lifecycles = runtime.repository.get_lifecycles(FuturesTradeRecommendationQuery(product))
    open_lifecycles = [lifecycle for lifecycle in lifecycles if not lifecycle.is_terminal]
    open_anomaly = len(open_lifecycles) > 1
    rollover = tuple(
        sorted(
            {
                lifecycle.recommendation.contract
                for lifecycle in open_lifecycles
                if lifecycle.recommendation.contract != contract
            },
            key=str,
        )
    )
    generation = None
    if not open_anomaly and not rollover:
        generation = GenerateFuturesTradeRecommendationsUseCase(
            runtime.research_repository,
            runtime.market_repository,
            sessions,
            finality,
            cutoffs,
            runtime.repository,
            runtime.store,
            runtime.audit_store,
        ).execute(
            contract=contract,
            strategy=strategy,
            policy=configuration.policy,
            profile=configuration.profile,
            issued_at=as_of,
            reviewed_candles=reviewed,
        )
    return FuturesRecommendationCycleResult(
        monitoring=monitoring,
        lifecycles=lifecycles,
        open_anomaly=open_anomaly,
        rollover_contracts=rollover,
        generation=generation,
    )


def run_incomplete_coverage_declaration(
    runtime: FuturesRecommendationRuntime,
    *,
    contract: FuturesContract,
    sessions: FuturesTradingSessionResolver,
    finality: FuturesDailyBarFinalityPolicy,
    reviewed: FuturesReviewedCandleVerification,
    as_of: PointInTime,
    declaration: FuturesIncompleteCoverageDeclaration,
) -> FuturesRecommendationMonitoringOutcome:
    """Monitor the product with one explicit declaration; return the declared one's outcome."""
    result = MonitorFuturesTradeRecommendationsUseCase(
        runtime.repository, runtime.lifecycle_store, runtime.market_repository, sessions, finality
    ).execute(
        product=contract.product,
        as_of=as_of,
        reviewed_candles=reviewed,
        coverage_declarations=(declaration,),
    )
    return next(
        outcome
        for outcome in result.outcomes
        if outcome.recommendation == declaration.recommendation_identity
    )


# ---------------------------------------------------------------------------
# Private rendering
# ---------------------------------------------------------------------------

_HEADER = "RECOMMENDATIONS (private; nothing published, delivered or executed)"


def outcome_line(outcome: FuturesRecommendationMonitoringOutcome) -> str:
    gap = f" [{outcome.evidence_gap.value}]" if outcome.evidence_gap is not None else ""
    session = (
        f" at {outcome.blocking_session.trading_date.isoformat()}"
        if outcome.blocking_session is not None
        else ""
    )
    return (
        f"  {outcome.recommendation.identity} {outcome.contract}: "
        f"{outcome.status_before.value} -> {outcome.status_after.value}, "
        f"{outcome.steps_recorded} step(s) recorded; {outcome.stop_reason.value}{gap}{session}"
    )


def cycle_lines(result: FuturesRecommendationCycleResult) -> list[str]:
    outcomes = result.monitoring.outcomes
    lines = [
        "",
        _HEADER,
        f"Monitored: {len(outcomes)} open recommendation(s), "
        f"{sum(o.steps_recorded for o in outcomes)} step(s) recorded",
        *(outcome_line(outcome) for outcome in outcomes),
        "Open after monitoring: "
        + (
            ", ".join(
                f"{lc.recommendation.identity.identity} {lc.status.value}"
                for lc in result.lifecycles
                if not lc.is_terminal
            )
            or "none"
        ),
    ]
    actions: list[str] = []
    for outcome in outcomes:
        if outcome.stop_reason is _Stop.AWAITING_EVIDENCE_PAST_VALIDITY:
            actions.append(
                f"{outcome.recommendation.identity} is past validity awaiting evidence "
                f"({outcome.evidence_gap.value}); supply the evidence, or declare incomplete "
                "coverage with 'northstar recommendations expire-incomplete'."
            )
        if outcome.stop_reason is _Stop.CONCURRENT_PROGRESS:
            actions.append(
                f"{outcome.recommendation.identity} was progressed by another writer; the next "
                "run resumes from the stored state."
            )
    if result.open_anomaly:
        lines.append("OPEN ANOMALY: more than one open recommendation; generation refused")
        actions.append("Investigate the open recommendations; nothing is netted or cancelled.")
    if result.rollover_contracts:
        contracts = ", ".join(str(c) for c in result.rollover_contracts)
        lines.append(
            f"UNSAFE ROLLOVER: open recommendation(s) on {contracts}, which this operation does "
            "not acquire; generation refused"
        )
        actions.append(
            f"Resolve the open recommendation(s) on {contracts} before operating another "
            "contract: operate that contract until they close, or declare incomplete coverage."
        )
    generation = result.generation
    if generation is None:
        lines.append("Generation: not run")
    else:
        status = generation.status.value
        if generation.block_reason is not None:
            status += f" ({generation.block_reason.value})"
        lines.append(f"Generation: {status}")
        lines += [
            f"  recorded {r.identity.identity} {r.direction.value}" for r in generation.recorded
        ]
        lines += [f"  existing {r.identity.identity}" for r in generation.existing]
        if generation.selection is not None:
            lines += [
                f"  research rejected: {rejection.reason.value}"
                for rejection in generation.selection.rejections
            ]
        if generation.generation is not None:
            lines += [
                f"  generation rejected: {rejection.reason.value}"
                for rejection in generation.generation.rejections
            ]
    lines += [f"Operator action: {action}" for action in actions]
    return lines


def declaration_lines(outcome: FuturesRecommendationMonitoringOutcome) -> list[str]:
    return [
        "INCOMPLETE-COVERAGE DECLARATION (private)",
        outcome_line(outcome),
        f"Status: {outcome.status_after.value}",
    ]
