"""Operational command line for daily manual futures paper trading.

    northstar economics set | show
    northstar market-data sync
    northstar paper run | status

Every value is explicit on the command line; the only thing read from the
environment is the provider secret DATABENTO_API_KEY, and only by
``market-data sync``. Nothing reads a clock here: cutoffs are supplied by the
operator. Summaries go to stdout; warnings and errors go to stderr.

Exit codes:

    0  SUCCESS
    1  INTERNAL       unexpected failure
    2  INPUT          an argument could not be parsed or validated
    3  CONFIGURATION  missing API key, or a database that cannot be opened
    4  DATA           a session not yet complete, or product economics not
                      configured; for ``paper run`` this means the paper session
                      completed and was persisted but P&L was unavailable
    5  STATE          an immutable conflict, a mixed-strategy portfolio, a
                      backward operational run, or malformed persisted state
    6  PROVIDER       the market-data provider or session calendar failed
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import IntEnum
from pathlib import Path
from typing import TextIO

from northstar_application.application_services import (
    AcquireFuturesDailyHistoryUseCase,
    BuildFuturesPaperPortfolioUseCase,
    ForwardResearchContractViolationError,
    FuturesHistoricalDataContractViolationError,
    FuturesPaperPortfolioStrategyConflictError,
    FuturesPaperTradingContractViolationError,
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
    FuturesPaperFillQuery,
    FuturesPaperOrderConflictError,
    FuturesPaperOrderQuery,
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
    FuturesHistoricalStorageError,
    FuturesTradingSessionInProgressError,
)
from northstar_infrastructure.persistence import (
    FuturesForwardResearchStorageError,
    FuturesPaperTradingStorageError,
    FuturesProductEconomicsStorageError,
)

from northstar_api import _cli_rendering as render
from northstar_api.runtime import (
    DatabaseConfigurationError,
    DatabaseRuntime,
    build_database_runtime,
    build_market_sync_runtime,
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


@dataclass(frozen=True, slots=True)
class _Context:
    env: Mapping[str, str]
    out: TextIO
    err: TextIO
    database_runtime: Callable[[Path], DatabaseRuntime]
    market_sync_runtime: Callable[[Path, str], AcquireFuturesDailyHistoryUseCase]

    def write(self, lines: Sequence[str]) -> None:
        self.out.write("\n".join(lines) + "\n")

    def warn(self, message: str) -> None:
        self.err.write(message + "\n")


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


def _market_data_sync(args: argparse.Namespace, context: _Context) -> ExitCode:
    contract = _contract(args)
    start = _parse("start date", args.start, _trading_date)
    end = _parse("end date", args.end, _trading_date)
    query = _parse(
        "date range",
        f"{args.start}..{args.end}",
        lambda _: FuturesDailyHistoricalAcquisitionQuery(contract, start, end),
    )
    api_key = context.env.get(API_KEY_VARIABLE, "").strip()
    if not api_key:
        raise CommandError(
            ExitCode.CONFIGURATION,
            f"{API_KEY_VARIABLE} is not set; market-data sync needs Databento credentials.",
        )

    acquisition = context.market_sync_runtime(_database(args), api_key)
    context.write(
        [
            "MARKET DATA SYNC",
            f"Contract: {contract}",
            f"Date range: {start} .. {end} (trading dates)",
        ]
    )
    try:
        result = acquisition.execute(query)
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


def _paper_run(args: argparse.Namespace, context: _Context) -> ExitCode:
    contract = _contract(args)
    strategy = _parse("strategy", args.strategy, StrategyIdentity)
    portfolio = _parse("portfolio", args.portfolio, PaperPortfolioIdentity)
    target = _parse("target", args.target, _count)
    as_of = _as_of(args)

    runtime = context.database_runtime(_database(args))
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
        return ExitCode.DATA
    context.write(render.pnl_lines(valuation))
    return ExitCode.SUCCESS


def _paper_status(args: argparse.Namespace, context: _Context) -> ExitCode:
    strategy = _parse("strategy", args.strategy, StrategyIdentity)
    portfolio = _parse("portfolio", args.portfolio, PaperPortfolioIdentity)
    as_of = _as_of(args)

    runtime = context.database_runtime(_database(args))
    missing_economics: str | None = None
    try:
        valuation = runtime.valuation.execute(portfolio, strategy, as_of)
    except FuturesProductEconomicsNotFoundError as error:
        valuation, missing_economics = None, str(error)

    orders = runtime.order_repository.get_orders(FuturesPaperOrderQuery(portfolio))
    fills = runtime.fill_repository.get_fills(FuturesPaperFillQuery(portfolio))
    decided = [o for o in orders if not _is_before(as_of, o.intent.decided_at)]
    visible = [f for f in fills if not _is_before(as_of, f.filled_at)]
    filled = {fill.order_identity for fill in visible}
    pending = tuple(order for order in decided if order.identity not in filled)
    held = (
        valuation.portfolio
        if valuation is not None
        else BuildFuturesPaperPortfolioUseCase().execute(portfolio, strategy, fills, as_of)
    )

    context.write(
        [
            "PAPER STATUS",
            *render.context_lines(
                strategy=strategy.identity, portfolio=portfolio.identity, cutoff=as_of.value
            ),
            *render.execution_summary_lines(len(decided), len(visible), pending),
            *render.portfolio_lines(held),
            *(
                render.pnl_lines(valuation)
                if valuation is not None
                else render.pnl_unavailable_lines(
                    f"product economics not configured: {missing_economics}"
                )
            ),
        ]
    )
    if valuation is None:
        context.warn("DATA ERROR: P&L unavailable because product economics are not configured.")
        return ExitCode.DATA
    return ExitCode.SUCCESS


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
        prog="northstar", description="Northstar daily manual futures paper trading."
    )
    groups = parser.add_subparsers(
        dest="group", required=True, metavar="{economics,market-data,paper}"
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
    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _fail(context: _Context, code: ExitCode, message: str) -> ExitCode:
    context.warn(f"{code.name} ERROR: {message}")
    return code


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
    )
    try:
        return args.handler(args, context)
    except CommandError as error:
        return _fail(context, error.code, error.message)
    except DatabaseConfigurationError as error:
        return _fail(context, ExitCode.CONFIGURATION, str(error))
    except FuturesTradingSessionInProgressError as error:
        return _fail(context, ExitCode.DATA, str(error))
    except FuturesProductEconomicsNotFoundError as error:
        return _fail(context, ExitCode.DATA, str(error))
    except _STATE_ERRORS as error:
        return _fail(context, ExitCode.STATE, f"{type(error).__name__}: {error}")
    except _PROVIDER_ERRORS as error:
        return _fail(context, ExitCode.PROVIDER, f"{type(error).__name__}: {error}")
    except Exception as error:  # the operator gets a concise message, never a traceback
        return _fail(context, ExitCode.INTERNAL, f"unexpected {type(error).__name__}.")


if __name__ == "__main__":
    sys.exit(main())
