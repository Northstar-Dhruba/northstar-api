"""Operational command line for daily futures paper trading.

    northstar economics set | show
    northstar market-data sync
    northstar paper run | status
    northstar operations daily
    northstar finality-evidence observe

The manual commands take every value on the command line; the only thing they
read from the environment is the selected provider's secret -- DATABENTO_API_KEY
for ``--provider databento`` (the default) or UPSTOX_ANALYTICS_TOKEN for
``--provider upstox`` -- and only ``market-data sync`` reads it. Nothing there
reads a clock: cutoffs and trading-date ranges are supplied by the operator.

``operations daily`` is the unattended entry point. It reads its configuration
and the secret from the environment, reads the wall clock exactly once, syncs
the completed sessions missing from persisted history, and runs the paper
session at the latest persisted daily bar. It logs plain lines to stderr.

``finality-evidence observe`` collects provider evidence only. It appends one
observation of an exact Upstox daily candle -- at the injected clock's instants
-- to an explicit append-only JSON Lines file, and reads UPSTOX_ANALYTICS_TOKEN
and nothing else from the environment. It never opens a Northstar database,
never writes market data and never decides that a candle is final.

Summaries go to stdout; warnings and errors go to stderr.

Exit codes:

    0  SUCCESS
    1  INTERNAL       unexpected failure
    2  INPUT          an argument could not be parsed or validated
    3  CONFIGURATION  missing API key or settings, or a database that cannot be opened
    4  DATA           a session not yet complete, no persisted history to extend,
                      a contract whose expiry window the session calendar cannot
                      place, or contract economics not configured; for ``paper run`` and
                      ``operations daily`` this means the paper session completed
                      and was persisted but P&L was unavailable
    5  STATE          an immutable conflict, a mixed-strategy portfolio, a
                      backward operational run, malformed persisted state, or a
                      manual write refused because another writer holds the
                      database's operations lock (nothing was changed)

Every command that mutates the database -- ``economics set``, ``market-data
sync``, ``paper run`` and ``operations daily`` -- holds that database's single
operations lock for its whole run. ``operations daily`` treats a held lock as a
safe skip and exits 0; a manual command refuses with 5.
    6  PROVIDER       the market-data provider or session calendar failed
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import re
import sqlite3
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import IntEnum
from pathlib import Path
from typing import TextIO

from northstar_application.application_services import (
    AcquireFuturesDailyHistoryUseCase,
    ForwardResearchContractViolationError,
    FuturesContractEconomicsContractViolationError,
    FuturesContractEconomicsNotFoundError,
    FuturesDailyAcquisitionResult,
    FuturesDailySessionCoverageError,
    FuturesExpiryWindowError,
    FuturesHistoricalDataContractViolationError,
    FuturesPaperPortfolioStrategyConflictError,
    FuturesPaperTradingContractViolationError,
    FuturesPaperTradingSessionResult,
    InvalidFuturesPaperFillHistoryError,
)
from northstar_application.ports import (
    FuturesContractEconomicsConflictError,
    FuturesDailyHistoricalAcquisitionQuery,
    FuturesForwardResearchRecordConflictError,
    FuturesForwardResearchRecordQuery,
    FuturesHistoricalMarketDataConflictError,
    FuturesPaperFillConflictError,
    FuturesPaperOrderConflictError,
    FuturesSessionResolutionError,
    FuturesTradingSession,
)
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import (
    Currency,
    ExchangeCode,
    PointInTime,
    Symbol,
    Timeframe,
)
from northstar_core.futures import (
    FuturesContract,
    FuturesContractEconomics,
    FuturesPointValue,
    FuturesProductReference,
)
from northstar_core.paper_trading import FuturesContractCount, PaperPortfolioIdentity
from northstar_core.strategy import StrategyIdentity
from northstar_infrastructure.market_data import (
    DatabentoFuturesHistoricalMarketDataSourceError,
    ExchangeCalendarFuturesTradingSessionResolver,
    FuturesHistoricalStorageError,
    FuturesTradingSessionInProgressError,
    NSEFuturesTradingSessionResolver,
    UpstoxCandleEvidenceLog,
    UpstoxCandleEvidenceLogError,
    UpstoxDailyCandleEvidenceCollector,
    UpstoxDailyCandleEvidenceObservation,
    UpstoxMarketDataSourceError,
)
from northstar_infrastructure.persistence import (
    FuturesContractEconomicsStorageError,
    FuturesForwardResearchStorageError,
    FuturesPaperTradingStorageError,
)

from northstar_api import _cli_rendering as render
from northstar_api.operations import (
    NO_HISTORY,
    FuturesChronologicalOperationResult,
    FuturesDailyOperationResult,
    FuturesOperationConfigurationError,
    FuturesSessionBacklog,
    captured_instant,
    completed_sessions_after,
    latest_daily_bar,
    latest_frozen_decision,
    operation_logger,
    plan_session_backlog,
)
from northstar_api.operations_lock import DatabaseOperationsLock, OperationsAlreadyActiveError
from northstar_api.runtime import (
    DatabaseConfigurationError,
    DatabaseRuntime,
    FuturesDailyAcquisition,
    build_daily_bar_finality_policy,
    build_database_runtime,
    build_market_sync_runtime,
    build_upstox_market_sync_runtime,
)
from northstar_api.settings import (
    DAILY_BAR_FINALITY_VARIABLE,
    FINAL_THROUGH_VARIABLE,
    GO_LIVE_VARIABLE,
    MARKET_DATA_PROVIDER_VARIABLE,
    OPERATION_VARIABLES,
    DashboardSettingsError,
    FuturesDailyBarFinalityMode,
    FuturesMarketDataProvider,
    FuturesOperationSettings,
    FuturesSessionOperationSettings,
    load_market_data_provider,
    load_operation_settings,
    load_session_operation_settings,
)

API_KEY_VARIABLE = "DATABENTO_API_KEY"
UPSTOX_TOKEN_VARIABLE = "UPSTOX_ANALYTICS_TOKEN"
_DAILY = Timeframe("1d")
_DATE_TEXT = re.compile(r"\d{4}-\d{2}-\d{2}")
_COUNT_TEXT = re.compile(r"[1-9][0-9]*")
_TARGET_WARNING = (
    "WARNING: Target contracts should remain fixed for this paper portfolio. "
    "Changing it after execution facts exist may conflict with frozen orders."
)
_SYNC_PARTIAL = "Earlier completed sessions in the range may already have been stored."
# Native daily acquisition validates the whole range before one atomic write.
_SYNC_NOTHING_STORED = "Nothing from this range was stored."


class ExitCode(IntEnum):
    SUCCESS = 0
    INTERNAL = 1
    INPUT = 2
    CONFIGURATION = 3
    DATA = 4
    STATE = 5
    PROVIDER = 6


class CommandError(Exception):
    """An expected operational failure with its exit code and operator message."""

    def __init__(self, code: ExitCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


_STATE_ERRORS: tuple[type[Exception], ...] = (
    FuturesForwardResearchRecordConflictError,
    FuturesPaperOrderConflictError,
    FuturesPaperFillConflictError,
    FuturesHistoricalMarketDataConflictError,
    FuturesContractEconomicsConflictError,
    FuturesPaperPortfolioStrategyConflictError,
    FuturesPaperTradingContractViolationError,
    ForwardResearchContractViolationError,
    FuturesContractEconomicsContractViolationError,
    FuturesHistoricalDataContractViolationError,
    InvalidFuturesPaperFillHistoryError,
    # One type covers unavailable and corrupt storage; the opened path was already checked.
    FuturesHistoricalStorageError,
    FuturesForwardResearchStorageError,
    FuturesPaperTradingStorageError,
    FuturesContractEconomicsStorageError,
)
_PROVIDER_ERRORS: tuple[type[Exception], ...] = (
    DatabentoFuturesHistoricalMarketDataSourceError,
    UpstoxMarketDataSourceError,
    FuturesSessionResolutionError,
)

Clock = Callable[[], datetime]
DailySyncRuntime = Callable[[Path, str, Clock], AcquireFuturesDailyHistoryUseCase]


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _daily_sync_runtime(
    path: Path, api_key: str, clock: Clock
) -> AcquireFuturesDailyHistoryUseCase:
    return build_market_sync_runtime(path, api_key, clock=clock)


def _upstox_candle_evidence(token: str, clock: Clock) -> UpstoxDailyCandleEvidenceCollector:
    return UpstoxDailyCandleEvidenceCollector(token, clock=clock)


@dataclass(frozen=True, slots=True)
class _Context:
    env: Mapping[str, str]
    out: TextIO
    err: TextIO
    database_runtime: Callable[[Path], DatabaseRuntime]
    market_sync_runtime: Callable[[Path, str], AcquireFuturesDailyHistoryUseCase]
    daily_sync_runtime: DailySyncRuntime = _daily_sync_runtime
    upstox_market_sync_runtime: Callable[[Path, str], FuturesDailyAcquisition] = (
        build_upstox_market_sync_runtime
    )
    clock: Clock = _utc_now
    upstox_candle_evidence: Callable[[str, Clock], UpstoxDailyCandleEvidenceCollector] = (
        _upstox_candle_evidence
    )
    # Secrets read by this invocation; every output line is scrubbed of them.
    secrets: list[str] = field(default_factory=list)

    def redact(self, text: str) -> str:
        for secret in self.secrets:
            text = text.replace(secret, "[REDACTED]")
        return text

    def write(self, lines: Sequence[str]) -> None:
        self.out.write(self.redact("\n".join(lines) + "\n"))

    def warn(self, message: str) -> None:
        self.err.write(self.redact(message + "\n"))


# ---------------------------------------------------------------------------
# Input parsing: every operator value becomes a Core value here or fails as INPUT
# ---------------------------------------------------------------------------


def _parse(label: str, text: str, build: Callable[[str], object]):
    try:
        return build(text)
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise CommandError(ExitCode.INPUT, f"Invalid {label} {text!r}: {exc}") from exc


def _decimal(text: str) -> Decimal:
    try:
        return Decimal(text.strip())
    except InvalidOperation as exc:
        raise ValueError("must be decimal text such as 50 or 12.5") from exc


def _trading_date(text: str) -> date:
    if _DATE_TEXT.fullmatch(text) is None:
        raise ValueError("must be YYYY-MM-DD")
    return date.fromisoformat(text)


def _count(text: str) -> FuturesContractCount:
    if _COUNT_TEXT.fullmatch(text) is None:
        raise ValueError("must be a positive whole number")
    return FuturesContractCount(int(text))


def _product(args: argparse.Namespace) -> FuturesProductReference:
    product = _parse("product", args.product, Symbol)
    exchange = _parse("exchange", args.exchange, ExchangeCode)
    return FuturesProductReference(product, exchange)


def _contract(args: argparse.Namespace) -> FuturesContract:
    product = _product(args)
    return FuturesContract(product, _parse("expiration", args.expiration, ExpirationDate))


def _as_of(args: argparse.Namespace) -> PointInTime:
    return _parse("as-of (ISO-8601 with an explicit offset)", args.as_of, PointInTime)


def _database(args: argparse.Namespace) -> Path:
    return Path(args.database)


def _is_before(left: PointInTime, right: PointInTime) -> bool:
    return left.compare(right) < 0


# ---------------------------------------------------------------------------
# economics
# ---------------------------------------------------------------------------


def _economics_set(args: argparse.Namespace, context: _Context) -> ExitCode:
    contract = _contract(args)
    amount = _parse("point value", args.point_value, _decimal)
    currency = _parse("currency", args.currency, Currency)
    point_value = _parse(
        "point value", args.point_value, lambda _: FuturesPointValue(amount, currency)
    )
    economics = FuturesContractEconomics(contract, point_value)

    with DatabaseOperationsLock(_database(args)):
        accepted = context.database_runtime(_database(args)).economics_store.store((economics,))
    if isinstance(accepted, bool) or accepted != 1:
        raise CommandError(
            ExitCode.STATE, f"Economics store accepted {accepted!r} values, expected 1."
        )
    context.write(
        [
            "ECONOMICS: READY",
            *render.economics_lines(economics),
            "Stored, or already stored with identical values.",
        ]
    )
    return ExitCode.SUCCESS


def _economics_show(args: argparse.Namespace, context: _Context) -> ExitCode:
    contract = _contract(args)
    economics = context.database_runtime(_database(args)).economics_repository.get_economics(
        contract
    )
    if economics is None:
        raise CommandError(
            ExitCode.DATA,
            f"Contract economics not configured for {contract}. "
            "Use 'northstar economics set' to configure them.",
        )
    context.write(render.economics_lines(economics))
    return ExitCode.SUCCESS


# ---------------------------------------------------------------------------
# market-data
# ---------------------------------------------------------------------------


def _secret(context: _Context, variable: str, provider: str, command: str) -> str:
    """Read one provider secret, refusing a missing one; it is redacted from all output."""
    secret = context.env.get(variable, "").strip()
    if not secret:
        raise CommandError(
            ExitCode.CONFIGURATION,
            f"{variable} is not set; {command} needs {provider} credentials.",
        )
    context.secrets.append(secret)
    return secret


def _api_key(context: _Context, command: str) -> str:
    return _secret(context, API_KEY_VARIABLE, "Databento", command)


def _market_acquisition(
    context: _Context, provider: FuturesMarketDataProvider, database: Path, command: str
) -> tuple[FuturesDailyAcquisition, str]:
    """Compose the selected provider's acquisition path and its failure note.

    Only the selected provider's secret is read. The two paths are composed
    separately because they are different Application use cases; the caller
    sees only their shared execute(query) -> result shape.
    """
    if provider is FuturesMarketDataProvider.UPSTOX:
        token = _secret(context, UPSTOX_TOKEN_VARIABLE, "Upstox", command)
        return context.upstox_market_sync_runtime(database, token), _SYNC_NOTHING_STORED
    api_key = _api_key(context, command)
    return context.market_sync_runtime(database, api_key), _SYNC_PARTIAL


def _acquire(
    acquisition: FuturesDailyAcquisition,
    query: FuturesDailyHistoricalAcquisitionQuery,
    stopped: str = _SYNC_PARTIAL,
) -> FuturesDailyAcquisitionResult:
    try:
        return acquisition.execute(query)
    except FuturesTradingSessionInProgressError as error:
        now = error.current_utc.isoformat().replace("+00:00", "Z")
        raise CommandError(
            ExitCode.DATA,
            f"Session {error.trading_date.isoformat()} has not completed.\n"
            f"Session close: {error.session_close}\n"
            f"Current UTC: {now}\n"
            f"Sync stopped at {error.trading_date.isoformat()}. {stopped}",
        ) from error
    except FuturesDailySessionCoverageError as error:
        # The provider and the calendar disagree about which sessions exist in
        # the requested range: a candle not (yet) published, a range reaching
        # before the contract listed, or a session the calendar does not know.
        raise CommandError(
            ExitCode.DATA, f"{type(error).__name__}: {error}\nSync stopped. {stopped}"
        ) from error
    except (*_PROVIDER_ERRORS, FuturesHistoricalDataContractViolationError) as error:
        raise CommandError(
            ExitCode.PROVIDER, f"{type(error).__name__}: {error}\nSync stopped. {stopped}"
        ) from error
    except FuturesHistoricalMarketDataConflictError as error:
        raise CommandError(
            ExitCode.STATE, f"{type(error).__name__}: {error}\nSync stopped. {stopped}"
        ) from error


def _market_data_sync(args: argparse.Namespace, context: _Context) -> ExitCode:
    contract = _contract(args)
    start = _parse("start date", args.start, _trading_date)
    end = _parse("end date", args.end, _trading_date)
    query = _parse(
        "date range",
        f"{args.start}..{args.end}",
        lambda _: FuturesDailyHistoricalAcquisitionQuery(contract, start, end),
    )
    provider = FuturesMarketDataProvider(args.provider)

    with DatabaseOperationsLock(_database(args)):
        acquisition, stopped = _market_acquisition(
            context, provider, _database(args), "market-data sync"
        )
        context.write(
            [
                "MARKET DATA SYNC",
                f"Provider: {provider.value}",
                f"Contract: {contract}",
                f"Date range: {start} .. {end} (trading dates)",
            ]
        )
        result = _acquire(acquisition, query, stopped)

    context.write(
        [
            "SYNC: COMPLETED",
            f"Sessions in range: {result.session_count}",
            f"Sessions with a persisted daily bar: {result.daily_bar_count} "
            "(identical bars already stored count as persisted)",
            f"Sessions without trades: {result.session_count - result.daily_bar_count}",
        ]
    )
    return ExitCode.SUCCESS


# ---------------------------------------------------------------------------
# finality-evidence
# ---------------------------------------------------------------------------

_EVIDENCE_VENUE = "NSE"


def _evidence_lines(observation: UpstoxDailyCandleEvidenceObservation, evidence: Path) -> list[str]:
    record = observation.to_record()
    candle = record["candle"]
    lines = [
        "CANDLE EVIDENCE: OBSERVATION RECORDED",
        f"Provider: {record['provider']}",
        f"Contract: {observation.contract}",
        f"Trading date: {record['trading_date']}",
        f"Requested at: {record['requested_at']}",
        f"Received at: {record['received_at']}",
        f"Candle present: {'yes' if candle else 'no'}",
    ]
    if candle:
        contracts = record["volume_contracts"]
        if contracts is None:
            contracts = f"unavailable ({record['volume_contracts_note']})"
        lines += [
            f"Open: {candle['open']}",
            f"High: {candle['high']}",
            f"Low: {candle['low']}",
            f"Close: {candle['close']}",
            f"Volume (provider units): {candle['volume']}",
            f"Volume (contracts): {contracts}",
            f"Open interest: {candle['open_interest'] or 'not provided'}",
        ]
    lines += [
        f"Evidence file: {evidence}",
        "Recorded as provider evidence only; one observation supports no conclusion.",
    ]
    return lines


def _finality_evidence_observe(args: argparse.Namespace, context: _Context) -> ExitCode:
    """Append one observation of an Upstox daily candle to an evidence file.

    Evidence only: no Northstar database, market data, finality approval or
    paper state is read or written. Provider failures append nothing.
    """
    contract = _contract(args)
    trading_date = _parse("trading date", args.trading_date, _trading_date)
    venue = contract.product.exchange_code.value
    if venue != _EVIDENCE_VENUE:
        raise CommandError(
            ExitCode.INPUT,
            f"finality-evidence observe supports {_EVIDENCE_VENUE} contracts only; "
            f"{contract} trades on {venue}.",
        )
    # Resolver failures (a calendar that fails closed) propagate as PROVIDER.
    if NSEFuturesTradingSessionResolver().resolve(contract.product, trading_date) is None:
        raise CommandError(
            ExitCode.INPUT,
            f"{trading_date.isoformat()} is not an {_EVIDENCE_VENUE} futures trading session.",
        )

    evidence = Path(args.evidence)
    log = UpstoxCandleEvidenceLog(evidence)
    try:
        log.require_appendable()  # before any provider request
        token = _secret(context, UPSTOX_TOKEN_VARIABLE, "Upstox", "finality-evidence observe")
        observation = context.upstox_candle_evidence(token, context.clock).observe(
            contract, trading_date
        )
        log.append(observation)
    except UpstoxCandleEvidenceLogError as error:
        raise CommandError(ExitCode.STATE, f"{type(error).__name__}: {error}") from error
    except OSError as error:
        raise CommandError(
            ExitCode.CONFIGURATION, f"Evidence file {evidence} cannot be used: {error}"
        ) from error

    context.write(_evidence_lines(observation, evidence))
    return ExitCode.SUCCESS


# ---------------------------------------------------------------------------
# paper
# ---------------------------------------------------------------------------


def _refuse_backward_run(
    runtime: DatabaseRuntime,
    contract: FuturesContract,
    strategy: StrategyIdentity,
    as_of: PointInTime,
) -> None:
    """Operational policy: a paper run never moves behind its latest frozen decision."""
    latest: PointInTime | None = None
    for record in runtime.forward_repository.get_records(
        FuturesForwardResearchRecordQuery(contract, _DAILY)
    ):
        if record.strategy_identity != strategy:
            continue
        if latest is None or _is_before(latest, record.decision_instant):
            latest = record.decision_instant
    if latest is not None and _is_before(as_of, latest):
        raise CommandError(
            ExitCode.STATE,
            "Paper run refused: cutoff precedes the latest frozen decision for this "
            f"contract/strategy.\nLatest decision: {latest}\nRequested cutoff: {as_of}\n"
            "Operational paper runs must move forward in time. "
            "Use research/replay tooling for historical reconstruction.",
        )


def _run_paper_session(
    context: _Context,
    runtime: DatabaseRuntime,
    contract: FuturesContract,
    strategy: StrategyIdentity,
    portfolio: PaperPortfolioIdentity,
    target: FuturesContractCount,
    as_of: PointInTime,
) -> tuple[FuturesPaperTradingSessionResult, bool]:
    """Run and render one paper session; return it and whether P&L was available."""
    _refuse_backward_run(runtime, contract, strategy, as_of)
    context.warn(_TARGET_WARNING)

    # The contract's own venue selects the session, and with it any pre-expiry guard.
    session = runtime.paper_session_for(contract).execute(
        contract, strategy, portfolio, target, as_of
    )
    context.write(
        [
            "PAPER SESSION: COMPLETED",
            *render.context_lines(
                contract=contract,
                strategy=strategy.identity,
                portfolio=portfolio.identity,
                target=target.value,
                cutoff=as_of.value,
            ),
            *render.decision_lines(session),
            *render.order_lines(session),
            *render.history_lines(session),
            *render.portfolio_lines(session.report.portfolio, contract),
        ]
    )
    try:
        valuation = runtime.valuation.execute(portfolio, strategy, as_of)
    except FuturesContractEconomicsNotFoundError as error:
        context.write(
            render.pnl_unavailable_lines(f"contract economics not configured for {error.contract}")
        )
        context.warn(
            "DATA ERROR: P&L unavailable because contract economics are not configured. "
            "The paper session above completed and its facts were persisted. "
            "Use 'northstar economics set', then 'northstar paper status'."
        )
        return session, False
    context.write(render.pnl_lines(valuation))
    return session, True


def _paper_run(args: argparse.Namespace, context: _Context) -> ExitCode:
    contract = _contract(args)
    strategy = _parse("strategy", args.strategy, StrategyIdentity)
    portfolio = _parse("portfolio", args.portfolio, PaperPortfolioIdentity)
    target = _parse("target", args.target, _count)
    as_of = _as_of(args)

    with DatabaseOperationsLock(_database(args)):
        runtime = context.database_runtime(_database(args))
        _, valued = _run_paper_session(
            context, runtime, contract, strategy, portfolio, target, as_of
        )
    return ExitCode.SUCCESS if valued else ExitCode.DATA


def _paper_status(args: argparse.Namespace, context: _Context) -> ExitCode:
    strategy = _parse("strategy", args.strategy, StrategyIdentity)
    portfolio = _parse("portfolio", args.portfolio, PaperPortfolioIdentity)
    as_of = _as_of(args)

    runtime = context.database_runtime(_database(args))
    # Whole-portfolio scope only: status selects no contract.
    snapshot = runtime.snapshot.execute(None, strategy, portfolio, as_of)
    valuation = snapshot.valuation

    context.write(
        [
            "PAPER STATUS",
            *render.context_lines(
                strategy=strategy.identity, portfolio=portfolio.identity, cutoff=as_of.value
            ),
            *render.execution_summary_lines(
                len(snapshot.orders), len(snapshot.fills), snapshot.pending_orders
            ),
            *render.portfolio_lines(snapshot.portfolio),
            *(
                render.pnl_lines(valuation)
                if valuation is not None
                else render.pnl_unavailable_lines(
                    f"contract economics not configured for {snapshot.missing_economics}"
                )
            ),
        ]
    )
    if valuation is None:
        context.warn("DATA ERROR: P&L unavailable because contract economics are not configured.")
        return ExitCode.DATA
    return ExitCode.SUCCESS


# ---------------------------------------------------------------------------
# operations
# ---------------------------------------------------------------------------


def _summary(lines: Sequence[str]) -> str:
    return "; ".join(line for line in lines if line)


_CHRONOLOGICAL_VENUE = "NSE"


def _configured(load):
    try:
        return load()
    except DashboardSettingsError as error:
        raise CommandError(ExitCode.CONFIGURATION, str(error)) from error


def _daily_operation(
    context: _Context, log: logging.Logger
) -> FuturesDailyOperationResult | FuturesChronologicalOperationResult:
    """Route to the provider's daily operation; each runs under one operations lock.

    Databento keeps the established clock-decided, latest-only operation.
    Upstox runs the chronological operation, which is clock-free and supports
    only NSE contracts; the contract's own exchange decides that, not the
    provider. Configuration and the provider secret are checked before the lock.

    With finality disabled nothing can be acquired, so the Upstox secret is
    neither required nor read; only operator-approved finality needs it.
    """
    settings = _configured(lambda: load_operation_settings(context.env))
    provider = _configured(lambda: load_market_data_provider(context.env))
    if provider is FuturesMarketDataProvider.UPSTOX:
        session_settings = _configured(lambda: load_session_operation_settings(context.env))
        venue = settings.contract.product.exchange_code.value
        if venue != _CHRONOLOGICAL_VENUE:
            raise CommandError(
                ExitCode.CONFIGURATION,
                f"{MARKET_DATA_PROVIDER_VARIABLE}=upstox operates {_CHRONOLOGICAL_VENUE} "
                f"contracts only; {settings.contract} trades on {venue}.",
            )
        token = (
            _secret(context, UPSTOX_TOKEN_VARIABLE, "Upstox", "operations daily")
            if session_settings.finality_mode is FuturesDailyBarFinalityMode.OPERATOR_APPROVED
            else None
        )
        # One lock for the whole cycle -- backlog, finality, sync and every session.
        with DatabaseOperationsLock(settings.database):
            return _chronological_daily_operation(context, log, settings, session_settings, token)
    api_key = _api_key(context, "operations daily")
    # One lock for the whole cycle -- clock, sync and paper session -- never per step.
    with DatabaseOperationsLock(settings.database):
        return _daily_operation_cycle(context, log, settings, api_key)


def _finality_line(settings: FuturesSessionOperationSettings) -> str:
    if settings.finality_mode is FuturesDailyBarFinalityMode.OPERATOR_APPROVED:
        return (
            f"Finality mode: {settings.finality_mode.value} "
            f"(final through {settings.final_through.isoformat()})"
        )
    return f"Finality mode: {settings.finality_mode.value}"


def _backlog_lines(backlog: FuturesSessionBacklog) -> list[str]:
    latest = backlog.latest_decision.value if backlog.latest_decision else "none"
    lines = [f"Latest frozen decision: {latest}"]
    if backlog.go_live is not None:
        lines.append(f"Go-live session: {backlog.go_live.isoformat()} (no decision frozen yet)")
    lines.append("Sessions assessed:")
    lines += [
        f"  {a.session.trading_date.isoformat()} {a.outcome.value}: {a.reason}"
        for a in backlog.assessments
    ] or ["  none"]
    return lines


def _replay_latest_cutoff(
    context: _Context,
    runtime: DatabaseRuntime,
    settings: FuturesOperationSettings,
    latest: PointInTime,
) -> None:
    """Replay the latest frozen cutoff quietly so interrupted execution completes.

    A decision can be frozen and its execution then fail. Replaying its own
    cutoff is the existing idempotent paper run: it freezes nothing new,
    creates a missing order under its deterministic identity, and writes
    nothing when the cutoff already completed.
    """
    quiet = replace(context, out=io.StringIO(), err=io.StringIO())
    _run_paper_session(
        quiet,
        runtime,
        settings.contract,
        settings.strategy,
        settings.portfolio,
        settings.target,
        latest,
    )


def _chronological_daily_operation(
    context: _Context,
    log: logging.Logger,
    settings: FuturesOperationSettings,
    session_settings: FuturesSessionOperationSettings,
    token: str | None,
) -> FuturesChronologicalOperationResult:
    """Process every eligible session in order; the caller holds the operations lock.

    Nothing here reads a clock. Market data for the final sessions is acquired
    as one range, then each session is paper-run at its own close, oldest
    first, stopping at the first failure so no later decision is ever taken
    past a session that did not complete.
    """
    contract = settings.contract
    log.info("chronological daily operation started")
    log.info(
        "contract %s, strategy %s, portfolio %s, target %s",
        contract,
        settings.strategy.identity,
        settings.portfolio.identity,
        settings.target.value,
    )
    log.info("%s", _finality_line(session_settings).lower())

    runtime = context.database_runtime(settings.database)
    latest = latest_frozen_decision(runtime.forward_repository, contract, settings.strategy)
    try:
        backlog = plan_session_backlog(
            NSEFuturesTradingSessionResolver(),
            build_daily_bar_finality_policy(session_settings),
            contract,
            latest,
            session_settings.go_live,
        )
    except FuturesOperationConfigurationError as error:
        raise CommandError(ExitCode.CONFIGURATION, str(error)) from error
    for assessment in backlog.assessments:
        log.info(
            "session %s finality %s: %s",
            assessment.session.trading_date,
            assessment.outcome.value,
            assessment.reason,
        )

    header = [
        "DAILY OPERATION: CHRONOLOGICAL",
        "Provider: upstox",
        f"Contract: {contract}",
        f"Strategy: {settings.strategy.identity}",
        f"Portfolio: {settings.portfolio.identity}",
        _finality_line(session_settings),
        *_backlog_lines(backlog),
    ]

    if backlog.exhausted:
        if latest is not None:
            _replay_latest_cutoff(context, runtime, settings, latest)
        context.write([*header, "", "STATUS: ROLLOVER REQUIRED"])
        log.warning("no trading session remains through expiry; rollover required")
        raise CommandError(
            ExitCode.CONFIGURATION,
            f"Rollover required: {contract} has no trading session left to operate through "
            "its expiry. Configure the next contract explicitly; nothing rolls automatically.",
        )

    waiting = backlog.waiting_on
    if not backlog.eligible:
        reason = waiting.reason if waiting is not None else "no session to process"
        context.write([*header, "", f"STATUS: WAITING -- {reason}"])
        log.info("waiting: %s", reason)
        return FuturesChronologicalOperationResult(contract, backlog, 0, (), True)

    first, last = backlog.eligible[0].trading_date, backlog.eligible[-1].trading_date
    if token is None:
        # Unreachable by design: only operator-approved finality yields final
        # sessions, and that mode always reads the token. Fail closed regardless.
        raise CommandError(
            ExitCode.CONFIGURATION,
            f"{UPSTOX_TOKEN_VARIABLE} is not set; operations daily needs Upstox credentials.",
        )
    acquisition = context.upstox_market_sync_runtime(settings.database, token)
    acquired = _acquire(
        acquisition,
        FuturesDailyHistoricalAcquisitionQuery(contract, first, last),
        _SYNC_NOTHING_STORED,
    )
    log.info(
        "acquired %d sessions (%s .. %s), %d with a persisted daily bar",
        acquired.session_count,
        first,
        last,
        acquired.daily_bar_count,
    )
    context.write([*header, f"Acquired: {first} .. {last} ({acquired.session_count} sessions)"])

    if latest is not None:
        _replay_latest_cutoff(context, runtime, settings, latest)
        log.info("latest frozen cutoff %s replayed idempotently", latest)

    processed: list[FuturesTradingSession] = []
    valued_all = True
    for session in backlog.eligible:
        context.write(["", f"SESSION {session.trading_date.isoformat()}"])
        try:
            result, valued = _run_paper_session(
                context,
                runtime,
                contract,
                settings.strategy,
                settings.portfolio,
                settings.target,
                session.closes_at,
            )
        except Exception:
            log.error("stopped at session %s; no later session was processed", session.trading_date)
            raise
        processed.append(session)
        valued_all = valued_all and valued
        log.info(
            "session %s decision: %s; order: %s",
            session.trading_date,
            _summary(render.decision_lines(result)[2:]),
            _summary(render.order_lines(result)[2:]),
        )

    status = [f"STATUS: COMPLETED -- {len(processed)} session(s) processed"]
    if waiting is not None:
        status.append(f"Waiting at {waiting.session.trading_date.isoformat()}: {waiting.reason}")
    context.write(["", *status])
    return FuturesChronologicalOperationResult(
        contract, backlog, acquired.session_count, tuple(processed), valued_all
    )


def _daily_operation_cycle(
    context: _Context, log: logging.Logger, settings: FuturesOperationSettings, api_key: str
) -> FuturesDailyOperationResult:
    """Run the daily cycle; the caller holds the database's operations lock."""
    now = context.clock()
    captured = captured_instant(now)
    contract = settings.contract
    log.info("daily operation started")
    log.info(
        "contract %s, strategy %s, portfolio %s, target %s",
        contract,
        settings.strategy.identity,
        settings.portfolio.identity,
        settings.target.value,
    )
    log.info("captured UTC %s", captured)

    runtime = context.database_runtime(settings.database)
    latest = latest_daily_bar(runtime.market_repository, contract)
    if latest is None:
        raise CommandError(ExitCode.DATA, NO_HISTORY.format(contract=contract))
    history_through = latest.point_in_time
    log.info("persisted daily history through %s", history_through)

    sessions = completed_sessions_after(
        ExchangeCalendarFuturesTradingSessionResolver(), contract, history_through, captured
    )
    acquired = 0
    if sessions:
        first, last = sessions[0].trading_date, sessions[-1].trading_date
        log.info("completed sessions to acquire: %d (%s .. %s)", len(sessions), first, last)
        acquisition = context.daily_sync_runtime(settings.database, api_key, lambda: now)
        result = _acquire(
            acquisition, FuturesDailyHistoricalAcquisitionQuery(contract, first, last)
        )
        acquired = result.daily_bar_count
        log.info(
            "acquisition completed: %d sessions, %d with a persisted daily bar",
            result.session_count,
            result.daily_bar_count,
        )
    else:
        log.info("no new completed session")

    # The cutoff is persisted evidence, never the clock.
    cutoff = latest_daily_bar(runtime.market_repository, contract).point_in_time
    log.info("market cutoff %s (latest persisted daily bar)", cutoff)
    context.write(
        [
            "DAILY OPERATION",
            f"Contract: {contract}",
            f"Captured UTC: {captured}",
            f"Persisted history before sync: through {history_through}",
            f"Completed sessions acquired: {len(sessions)}"
            + (f" ({sessions[0].trading_date} .. {sessions[-1].trading_date})" if sessions else ""),
            f"Market cutoff: {cutoff} (latest persisted daily bar)",
            "",
        ]
    )

    session, valued = _run_paper_session(
        context,
        runtime,
        contract,
        settings.strategy,
        settings.portfolio,
        settings.target,
        cutoff,
    )
    log.info("decision: %s", _summary(render.decision_lines(session)[2:]))
    log.info("paper order: %s", _summary(render.order_lines(session)[2:]))
    if valued:
        log.info("valuation available")
    else:
        log.warning(
            "execution complete; P&L unavailable because contract economics are not configured"
        )
    return FuturesDailyOperationResult(
        captured_at=captured,
        contract=contract,
        history_through=history_through,
        sessions_considered=sessions,
        daily_bars_acquired=acquired,
        cutoff=cutoff,
        paper_session=session,
        valuation_available=valued,
    )


def _operations_daily(args: argparse.Namespace, context: _Context) -> ExitCode:
    with operation_logger(context.err, context.secrets) as log:
        try:
            result = _daily_operation(context, log)
        except OperationsAlreadyActiveError as active:
            # Another runner owns this database; it does the work. A safe skip.
            log.warning("daily operation skipped: another operations runner is active")
            context.write(["DAILY OPERATION: SKIPPED", str(active)])
            log.info("exit %s (%d)", ExitCode.SUCCESS.name, ExitCode.SUCCESS.value)
            return ExitCode.SUCCESS
        except Exception as error:
            code, message = _classify(error)
            log.error("daily operation stopped: %s", message.splitlines()[0])
            log.info("exit %s (%d)", code.name, code.value)
            raise
        code = ExitCode.SUCCESS if result.valuation_available else ExitCode.DATA
        log.info("exit %s (%d)", code.name, code.value)
        return code


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def _add_database(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database", required=True, help="SQLite database file")


def _add_product(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--product", required=True, help="product code, e.g. ES or NIFTY")
    parser.add_argument("--exchange", required=True, help="exchange code, e.g. CME or NSE")


def _add_contract(parser: argparse.ArgumentParser) -> None:
    _add_product(parser)
    parser.add_argument("--expiration", required=True, help="contract expiration, YYYY-MM-DD")


def _add_paper_identity(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--strategy",
        required=True,
        help="strategy label; every label runs the built-in directional MVP policy",
    )
    parser.add_argument("--portfolio", required=True, help="paper portfolio identity")
    parser.add_argument(
        "--as-of",
        required=True,
        help="cutoff, ISO-8601 with an explicit offset, e.g. 2026-09-24T21:00:00+00:00",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="northstar", description="Northstar daily futures paper trading."
    )
    groups = parser.add_subparsers(
        dest="group", required=True, metavar="{economics,market-data,paper,operations}"
    )

    economics = groups.add_parser("economics", help="configure the economics of one dated contract")
    economics_commands = economics.add_subparsers(dest="command", required=True)
    set_parser = economics_commands.add_parser(
        "set", help="store one dated contract's economics once"
    )
    _add_database(set_parser)
    _add_contract(set_parser)
    set_parser.add_argument(
        "--point-value",
        required=True,
        help=(
            "currency per 1.0 quote point per contract, for this expiration only, "
            "e.g. 50 for ES or 65 for a NIFTY lot of 65"
        ),
    )
    set_parser.add_argument(
        "--currency", required=True, help="settlement currency, e.g. USD or INR"
    )
    set_parser.set_defaults(handler=_economics_set)
    show_parser = economics_commands.add_parser(
        "show", help="show one dated contract's stored economics"
    )
    _add_database(show_parser)
    _add_contract(show_parser)
    show_parser.set_defaults(handler=_economics_show)

    market = groups.add_parser("market-data", help="sync completed daily sessions")
    market_commands = market.add_subparsers(dest="command", required=True)
    sync_parser = market_commands.add_parser(
        "sync",
        help=(
            f"acquire daily sessions from the selected provider (Databento needs "
            f"{API_KEY_VARIABLE}; Upstox needs {UPSTOX_TOKEN_VARIABLE})"
        ),
    )
    _add_database(sync_parser)
    _add_contract(sync_parser)
    sync_parser.add_argument("--start", required=True, help="first trading date, YYYY-MM-DD")
    sync_parser.add_argument("--end", required=True, help="last trading date, YYYY-MM-DD")
    sync_parser.add_argument(
        "--provider",
        choices=[provider.value for provider in FuturesMarketDataProvider],
        default=FuturesMarketDataProvider.DATABENTO.value,
        help=(
            "futures market-data provider (default: databento). databento acquires "
            "completed CME sessions from minute data; upstox acquires native daily NSE "
            "candles for exactly the dates given, with no completeness check of its own"
        ),
    )
    sync_parser.set_defaults(handler=_market_data_sync)

    paper = groups.add_parser("paper", help="run or inspect paper trading")
    paper_commands = paper.add_subparsers(dest="command", required=True)
    run_parser = paper_commands.add_parser(
        "run", help="freeze the latest decision and paper trade through the cutoff"
    )
    _add_database(run_parser)
    _add_contract(run_parser)
    _add_paper_identity(run_parser)
    run_parser.add_argument(
        "--target", required=True, help="target contracts; keep fixed for the portfolio"
    )
    run_parser.set_defaults(handler=_paper_run)
    status_parser = paper_commands.add_parser(
        "status", help="read-only portfolio, pending orders and P&L as of a cutoff"
    )
    _add_database(status_parser)
    _add_paper_identity(status_parser)
    status_parser.set_defaults(handler=_paper_status)

    operations = groups.add_parser(
        "operations", help="unattended daily operation configured from the environment"
    )
    operations_commands = operations.add_subparsers(dest="command", required=True)
    daily_parser = operations_commands.add_parser(
        "daily",
        help="sync completed sessions, then paper trade at the latest persisted daily bar",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Scheduler entry point. Takes no arguments: every setting comes from the\n"
            "environment, and the wall clock is read once to decide which sessions have\n"
            "completed.\n\n"
            "Environment:\n"
            + "".join(f"  {name}\n" for name in OPERATION_VARIABLES)
            + f"  {API_KEY_VARIABLE}  (secret; never echoed)\n"
            + f"  {MARKET_DATA_PROVIDER_VARIABLE}  (optional; databento by default)\n\n"
            "Persisted history must first be bootstrapped with 'northstar market-data sync'.\n"
            "The paper cutoff is the latest persisted daily bar, never the clock.\n\n"
            f"With {MARKET_DATA_PROVIDER_VARIABLE}=upstox (NSE contracts only) the operation\n"
            "reads no clock. It processes every session after the latest frozen decision\n"
            "in order, each at its own close, but only sessions its finality policy treats\n"
            "as final:\n"
            f"  {UPSTOX_TOKEN_VARIABLE}  (secret; never echoed)\n"
            f"  {DAILY_BAR_FINALITY_VARIABLE}  disabled (default) or operator-approved\n"
            f"  {FINAL_THROUGH_VARIABLE}  last approved trading date (operator-approved)\n"
            f"  {GO_LIVE_VARIABLE}  first session to operate while no decision is frozen"
        ),
    )
    daily_parser.set_defaults(handler=_operations_daily)

    evidence = groups.add_parser(
        "finality-evidence",
        help="collect provider evidence about daily-candle revisions; decides nothing",
    )
    evidence_commands = evidence.add_subparsers(dest="command", required=True)
    observe_parser = evidence_commands.add_parser(
        "observe",
        help=(
            f"append one observation of an exact Upstox daily candle to an evidence file "
            f"(needs {UPSTOX_TOKEN_VARIABLE}); writes no market data"
        ),
    )
    observe_parser.add_argument(
        "--evidence",
        required=True,
        help="append-only JSON Lines evidence file, kept outside every repository",
    )
    _add_contract(observe_parser)
    observe_parser.add_argument(
        "--trading-date", required=True, help="the NSE trading session observed, YYYY-MM-DD"
    )
    observe_parser.set_defaults(handler=_finality_evidence_observe)
    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _fail(context: _Context, code: ExitCode, message: str) -> ExitCode:
    context.warn(f"{code.name} ERROR: {message}")
    return code


def _database_busy(error: BaseException) -> bool:
    """Return whether a storage error was caused by SQLite being locked or busy."""
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, sqlite3.OperationalError) and any(
            word in str(current).lower() for word in ("locked", "busy")
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


def _classify(error: Exception) -> tuple[ExitCode, str]:
    """Map a failure to its exit code and operator message."""
    if isinstance(error, CommandError):
        return error.code, error.message
    if isinstance(error, DatabaseConfigurationError):
        return ExitCode.CONFIGURATION, str(error)
    if isinstance(error, OperationsAlreadyActiveError):
        return ExitCode.STATE, str(error)
    if isinstance(
        error, FuturesTradingSessionInProgressError | FuturesContractEconomicsNotFoundError
    ):
        return ExitCode.DATA, str(error)
    if isinstance(error, FuturesExpiryWindowError):
        # The contract and the session calendar disagree; a data fact, not a crash.
        return ExitCode.DATA, f"{type(error).__name__}: {error}"
    if isinstance(error, _STATE_ERRORS):
        message = f"{type(error).__name__}: {error}"
        if _database_busy(error):
            message += " (database busy: another process holds the SQLite write lock; retry later)"
        return ExitCode.STATE, message
    if isinstance(error, _PROVIDER_ERRORS):
        return ExitCode.PROVIDER, f"{type(error).__name__}: {error}"
    # The operator gets a concise message, never a traceback.
    return ExitCode.INTERNAL, f"unexpected {type(error).__name__}."


def main(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
    database_runtime: Callable[[Path], DatabaseRuntime] = build_database_runtime,
    market_sync_runtime: Callable[
        [Path, str], AcquireFuturesDailyHistoryUseCase
    ] = build_market_sync_runtime,
    daily_sync_runtime: DailySyncRuntime = _daily_sync_runtime,
    upstox_market_sync_runtime: Callable[
        [Path, str], FuturesDailyAcquisition
    ] = build_upstox_market_sync_runtime,
    clock: Clock = _utc_now,
    upstox_candle_evidence: Callable[
        [str, Clock], UpstoxDailyCandleEvidenceCollector
    ] = _upstox_candle_evidence,
) -> int:
    """Run one command and return its exit code."""
    out = stdout if stdout is not None else sys.stdout
    err = stderr if stderr is not None else sys.stderr
    try:
        with redirect_stdout(out), redirect_stderr(err):
            args = build_parser().parse_args(argv)
    except SystemExit as exit_:
        return exit_.code if isinstance(exit_.code, int) else ExitCode.INPUT

    context = _Context(
        env=os.environ if env is None else env,
        out=out,
        err=err,
        database_runtime=database_runtime,
        market_sync_runtime=market_sync_runtime,
        daily_sync_runtime=daily_sync_runtime,
        upstox_market_sync_runtime=upstox_market_sync_runtime,
        clock=clock,
        upstox_candle_evidence=upstox_candle_evidence,
    )
    try:
        return args.handler(args, context)
    except Exception as error:
        return _fail(context, *_classify(error))


if __name__ == "__main__":
    sys.exit(main())
