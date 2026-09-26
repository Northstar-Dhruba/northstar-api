"""Operational command line for daily futures paper trading.

    northstar economics set | show
    northstar market-data sync
    northstar paper run | status
    northstar operations daily

The manual commands take every value on the command line; the only thing they
read from the environment is the provider secret DATABENTO_API_KEY, and only
``market-data sync`` reads it. Nothing there reads a clock: cutoffs are supplied
by the operator.

``operations daily`` is the unattended entry point. It reads its configuration
and the secret from the environment, reads the wall clock exactly once, syncs
the completed sessions missing from persisted history, and runs the paper
session at the latest persisted daily bar. It logs plain lines to stderr.

Summaries go to stdout; warnings and errors go to stderr.

Exit codes:

    0  SUCCESS
    1  INTERNAL       unexpected failure
    2  INPUT          an argument could not be parsed or validated
    3  CONFIGURATION  missing API key or settings, or a database that cannot be opened
    4  DATA           a session not yet complete, no persisted history to extend, or
                      product economics not configured; for ``paper run`` and
                      ``operations daily`` this means the paper session completed
                      and was persisted but P&L was unavailable
    5  STATE          an immutable conflict, a mixed-strategy portfolio, a
                      backward operational run, or malformed persisted state
    6  PROVIDER       the market-data provider or session calendar failed
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from enum import IntEnum
from pathlib import Path
from typing import TextIO

from northstar_application.application_services import (
    AcquireFuturesDailyHistoryUseCase,
    ForwardResearchContractViolationError,
    FuturesDailyAcquisitionResult,
    FuturesHistoricalDataContractViolationError,
    FuturesPaperPortfolioStrategyConflictError,
    FuturesPaperTradingContractViolationError,
    FuturesPaperTradingSessionResult,
    FuturesProductEconomicsContractViolationError,
    FuturesProductEconomicsNotFoundError,
    InvalidFuturesPaperFillHistoryError,
)
from northstar_application.ports import (
    FuturesDailyHistoricalAcquisitionQuery,
    FuturesForwardResearchRecordConflictError,
    FuturesForwardResearchRecordQuery,
    FuturesHistoricalMarketDataConflictError,
    FuturesPaperFillConflictError,
    FuturesPaperOrderConflictError,
    FuturesProductEconomicsConflictError,
    FuturesSessionResolutionError,
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
    FuturesPointValue,
    FuturesProductEconomics,
    FuturesProductReference,
)
from northstar_core.paper_trading import FuturesContractCount, PaperPortfolioIdentity
from northstar_core.strategy import StrategyIdentity
from northstar_infrastructure.market_data import (
    DatabentoFuturesHistoricalMarketDataSourceError,
    ExchangeCalendarFuturesTradingSessionResolver,
    FuturesHistoricalStorageError,
    FuturesTradingSessionInProgressError,
)
from northstar_infrastructure.persistence import (
    FuturesForwardResearchStorageError,
    FuturesPaperTradingStorageError,
    FuturesProductEconomicsStorageError,
)

from northstar_api import _cli_rendering as render
from northstar_api.operations import (
    NO_HISTORY,
    FuturesDailyOperationResult,
    captured_instant,
    completed_sessions_after,
    latest_daily_bar,
    operation_logger,
)
from northstar_api.runtime import (
    DatabaseConfigurationError,
    DatabaseRuntime,
    build_database_runtime,
    build_market_sync_runtime,
)
from northstar_api.settings import (
    OPERATION_VARIABLES,
    DashboardSettingsError,
    load_operation_settings,
)

API_KEY_VARIABLE = "DATABENTO_API_KEY"
_DAILY = Timeframe("1d")
_DATE_TEXT = re.compile(r"\d{4}-\d{2}-\d{2}")
_COUNT_TEXT = re.compile(r"[1-9][0-9]*")
_TARGET_WARNING = (
    "WARNING: Target contracts should remain fixed for this paper portfolio. "
    "Changing it after execution facts exist may conflict with frozen orders."
)
_SYNC_PARTIAL = "Earlier completed sessions in the range may already have been stored."


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
    FuturesProductEconomicsConflictError,
    FuturesPaperPortfolioStrategyConflictError,
    FuturesPaperTradingContractViolationError,
    ForwardResearchContractViolationError,
    FuturesProductEconomicsContractViolationError,
    FuturesHistoricalDataContractViolationError,
    InvalidFuturesPaperFillHistoryError,
    # One type covers unavailable and corrupt storage; the opened path was already checked.
    FuturesHistoricalStorageError,
    FuturesForwardResearchStorageError,
    FuturesPaperTradingStorageError,
    FuturesProductEconomicsStorageError,
)
_PROVIDER_ERRORS: tuple[type[Exception], ...] = (
    DatabentoFuturesHistoricalMarketDataSourceError,
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


@dataclass(frozen=True, slots=True)
class _Context:
    env: Mapping[str, str]
    out: TextIO
    err: TextIO
    database_runtime: Callable[[Path], DatabaseRuntime]
    market_sync_runtime: Callable[[Path, str], AcquireFuturesDailyHistoryUseCase]
    daily_sync_runtime: DailySyncRuntime = _daily_sync_runtime
    clock: Clock = _utc_now
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
    reference = _product(args)
    amount = _parse("point value", args.point_value, _decimal)
    currency = _parse("currency", args.currency, Currency)
    point_value = _parse(
        "point value", args.point_value, lambda _: FuturesPointValue(amount, currency)
    )
    economics = FuturesProductEconomics(reference, point_value)

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
    reference = _product(args)
    economics = context.database_runtime(_database(args)).economics_repository.get_economics(
        reference
    )
    if economics is None:
        raise CommandError(
            ExitCode.DATA,
            f"Product economics not configured for {reference}. "
            "Use 'northstar economics set' to configure them.",
        )
    context.write(render.economics_lines(economics))
    return ExitCode.SUCCESS


# ---------------------------------------------------------------------------
# market-data
# ---------------------------------------------------------------------------


def _api_key(context: _Context, command: str) -> str:
    api_key = context.env.get(API_KEY_VARIABLE, "").strip()
    if not api_key:
        raise CommandError(
            ExitCode.CONFIGURATION,
            f"{API_KEY_VARIABLE} is not set; {command} needs Databento credentials.",
        )
    context.secrets.append(api_key)
    return api_key


def _acquire(
    acquisition: AcquireFuturesDailyHistoryUseCase, query: FuturesDailyHistoricalAcquisitionQuery
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
            f"Sync stopped at {error.trading_date.isoformat()}. {_SYNC_PARTIAL}",
        ) from error
    except (*_PROVIDER_ERRORS, FuturesHistoricalDataContractViolationError) as error:
        raise CommandError(
            ExitCode.PROVIDER, f"{type(error).__name__}: {error}\nSync stopped. {_SYNC_PARTIAL}"
        ) from error
    except FuturesHistoricalMarketDataConflictError as error:
        raise CommandError(
            ExitCode.STATE, f"{type(error).__name__}: {error}\nSync stopped. {_SYNC_PARTIAL}"
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
    api_key = _api_key(context, "market-data sync")

    acquisition = context.market_sync_runtime(_database(args), api_key)
    context.write(
        [
            "MARKET DATA SYNC",
            f"Contract: {contract}",
            f"Date range: {start} .. {end} (trading dates)",
        ]
    )
    result = _acquire(acquisition, query)

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

    session = runtime.paper_session.execute(contract, strategy, portfolio, target, as_of)
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
    except FuturesProductEconomicsNotFoundError as error:
        context.write(render.pnl_unavailable_lines(f"product economics not configured: {error}"))
        context.warn(
            "DATA ERROR: P&L unavailable because product economics are not configured. "
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

    runtime = context.database_runtime(_database(args))
    _, valued = _run_paper_session(context, runtime, contract, strategy, portfolio, target, as_of)
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
                    f"product economics not configured for {snapshot.missing_economics}"
                )
            ),
        ]
    )
    if valuation is None:
        context.warn("DATA ERROR: P&L unavailable because product economics are not configured.")
        return ExitCode.DATA
    return ExitCode.SUCCESS


# ---------------------------------------------------------------------------
# operations
# ---------------------------------------------------------------------------


def _summary(lines: Sequence[str]) -> str:
    return "; ".join(line for line in lines if line)


def _daily_operation(context: _Context, log: logging.Logger) -> FuturesDailyOperationResult:
    try:
        settings = load_operation_settings(context.env)
    except DashboardSettingsError as error:
        raise CommandError(ExitCode.CONFIGURATION, str(error)) from error
    api_key = _api_key(context, "operations daily")
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
            "execution complete; P&L unavailable because product economics are not configured"
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
    parser.add_argument("--product", required=True, help="product code, e.g. ES")
    parser.add_argument("--exchange", required=True, help="exchange code, e.g. CME")


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

    economics = groups.add_parser("economics", help="configure product economics")
    economics_commands = economics.add_subparsers(dest="command", required=True)
    set_parser = economics_commands.add_parser("set", help="store product economics once")
    _add_database(set_parser)
    _add_product(set_parser)
    set_parser.add_argument(
        "--point-value", required=True, help="currency per 1.0 quote point per contract"
    )
    set_parser.add_argument("--currency", required=True, help="settlement currency, e.g. USD")
    set_parser.set_defaults(handler=_economics_set)
    show_parser = economics_commands.add_parser("show", help="show stored product economics")
    _add_database(show_parser)
    _add_product(show_parser)
    show_parser.set_defaults(handler=_economics_show)

    market = groups.add_parser("market-data", help="sync completed daily sessions")
    market_commands = market.add_subparsers(dest="command", required=True)
    sync_parser = market_commands.add_parser(
        "sync", help=f"acquire completed sessions from Databento (needs {API_KEY_VARIABLE})"
    )
    _add_database(sync_parser)
    _add_contract(sync_parser)
    sync_parser.add_argument("--start", required=True, help="first trading date, YYYY-MM-DD")
    sync_parser.add_argument("--end", required=True, help="last trading date, YYYY-MM-DD")
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
            + f"  {API_KEY_VARIABLE}  (secret; never echoed)\n\n"
            "Persisted history must first be bootstrapped with 'northstar market-data sync'.\n"
            "The paper cutoff is the latest persisted daily bar, never the clock."
        ),
    )
    daily_parser.set_defaults(handler=_operations_daily)
    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _fail(context: _Context, code: ExitCode, message: str) -> ExitCode:
    context.warn(f"{code.name} ERROR: {message}")
    return code


def _classify(error: Exception) -> tuple[ExitCode, str]:
    """Map a failure to its exit code and operator message."""
    if isinstance(error, CommandError):
        return error.code, error.message
    if isinstance(error, DatabaseConfigurationError):
        return ExitCode.CONFIGURATION, str(error)
    if isinstance(
        error, FuturesTradingSessionInProgressError | FuturesProductEconomicsNotFoundError
    ):
        return ExitCode.DATA, str(error)
    if isinstance(error, _STATE_ERRORS):
        return ExitCode.STATE, f"{type(error).__name__}: {error}"
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
    clock: Clock = _utc_now,
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
        clock=clock,
    )
    try:
        return args.handler(args, context)
    except Exception as error:
        return _fail(context, *_classify(error))


if __name__ == "__main__":
    sys.exit(main())
