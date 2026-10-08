"""Production wiring for the operational CLI.

This is a composition root: it is the only place that chooses concrete
Infrastructure adapters for the futures paper-trading use cases. Every adapter
works against one explicitly supplied SQLite file; nothing is cached between
invocations, so a fresh process reconstructs everything from that file.

Futures market-data providers
-----------------------------
Each provider is composed onto its own Application acquisition path, and the
two are deliberately not merged behind one source port:

    Databento  minute bars -> CME exchange calendar -> fold into daily bars
    Upstox     native daily candles -> NSE calendar -> stamp at session close

Both use cases accept the same explicit FuturesDailyHistoricalAcquisitionQuery
and return the same FuturesDailyAcquisitionResult, which is all a caller needs.
FuturesDailyAcquisition names that shared shape structurally, here in the
composition root only; it is not an Application abstraction.

Pre-expiry flatten guard
------------------------
One DatabaseRuntime serves every contract in its database, and a contract's
venue is known only when a command supplies it. The paper session is therefore
chosen per contract, by ``contract.product.exchange_code``, through
``DatabaseRuntime.paper_session_for``:

    NSE          paper session guarded by the NSE calendar, K = 5
    every other  the unguarded paper session, exactly as before

The venue decides, never the market-data provider or whichever credential is
present: where data comes from says nothing about where the contract trades.
K is a fixed composition constant rather than configuration, because a run
replays every frozen decision under its deterministic order identity, and a K
that differed between runs could change a past decision's intent.

Option economics
----------------
Option contract economics live in the same SQLite file but have their own
wiring, and DatabaseRuntime and initialize_database are not involved. The
writable OptionEconomicsRuntime creates only ``option_contract_economics``, and
only ``options economics set`` builds it, under the operations lock. Reads use
build_option_economics_lookup, which creates nothing: not the file, not the
table. A futures-only database therefore acquires the option table only on the
first option economics write.

Option instrument reference
---------------------------
The NIFTY option listings read from the public Upstox master live in the same
file in two tables of their own. OptionInstrumentRuntime serves only
``options instruments sync``, under the operations lock, and its store creates
the listing tables just before its first write. ``options instruments show``
uses build_option_listing_lookup, which opens the file read-only and creates
nothing.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Protocol

from northstar_application.application_services import (
    AcquireFuturesDailyHistoryUseCase,
    AcquireFuturesNativeDailyHistoryUseCase,
    AggregateFuturesDailySessionBarUseCase,
    BuildFuturesPaperTradingValuationUseCase,
    CalculateFuturesAnalysisUseCase,
    DisabledFuturesDailyBarFinalityPolicy,
    FuturesDailyAcquisitionResult,
    FuturesExpiryFlattenGuard,
    FuturesExpiryFlattenPolicy,
    GetFuturesPaperTradingSnapshotUseCase,
    GetOptionContractEconomicsUseCase,
    OperatorApprovedFuturesDailyBarFinalityPolicy,
    RunFuturesPaperTradingSessionUseCase,
)
from northstar_application.ports import (
    FuturesContractEconomicsRepository,
    FuturesContractEconomicsStore,
    FuturesDailyBarFinalityPolicy,
    FuturesDailyHistoricalAcquisitionQuery,
    FuturesForwardResearchRecordRepository,
    FuturesHistoricalMarketDataRepository,
    FuturesPaperFillRepository,
    FuturesPaperOrderRepository,
    FuturesTradingSessionResolver,
    OptionContractEconomicsRepository,
    OptionContractEconomicsStore,
)
from northstar_core.futures import FuturesContract
from northstar_core.strategy import FuturesAssetAnalysisGenerator
from northstar_infrastructure.market_data import (
    DatabentoFuturesHistoricalMarketDataSource,
    ExchangeCalendarFuturesTradingSessionResolver,
    NSEFuturesTradingSessionResolver,
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
    SQLiteOptionListingRepository,
    SQLiteOptionListingStore,
    UpstoxFuturesNativeDailyMarketDataSource,
    UpstoxOptionInstrumentMaster,
    initialize_futures_market_data_schema,
)
from northstar_infrastructure.market_data.upstox_http import UpstoxFetch
from northstar_infrastructure.persistence import (
    SQLiteFuturesContractEconomicsRepository,
    SQLiteFuturesContractEconomicsStore,
    SQLiteFuturesForwardResearchRecordRepository,
    SQLiteFuturesForwardResearchRecordStore,
    SQLiteFuturesPaperFillRepository,
    SQLiteFuturesPaperFillStore,
    SQLiteFuturesPaperOrderRepository,
    SQLiteFuturesPaperOrderStore,
    SQLiteOptionContractEconomicsRepository,
    SQLiteOptionContractEconomicsStore,
    initialize_futures_contract_economics_schema,
    initialize_futures_forward_research_record_schema,
    initialize_futures_paper_trading_schema,
    initialize_option_contract_economics_schema,
)

from northstar_api.settings import FuturesDailyBarFinalityMode, FuturesSessionOperationSettings

# The accepted Indian MVP policy: flat by the OPEN of the session five trading
# sessions before expiry, so the flatten is decided at the E-6 close.
NSE_EXPIRY_FLATTEN_SESSIONS = 5


def _nse_expiry_guard() -> FuturesExpiryFlattenGuard:
    return FuturesExpiryFlattenGuard(
        NSEFuturesTradingSessionResolver(),
        FuturesExpiryFlattenPolicy(NSE_EXPIRY_FLATTEN_SESSIONS),
    )


# Venue -> the guard its paper session is composed with. A venue absent here
# keeps the unguarded paper session; no other venue borrows the NSE calendar,
# which in any case refuses every venue but NSE.
_EXPIRY_GUARDS: dict[str, Callable[[], FuturesExpiryFlattenGuard]] = {
    "NSE": _nse_expiry_guard,
}

# Venue -> the calendar its chronological daily operation plans sessions with.
_CHRONOLOGICAL_RESOLVERS: dict[str, Callable[[], FuturesTradingSessionResolver]] = {
    "NSE": NSEFuturesTradingSessionResolver,
}


def expiry_guard_for(contract: FuturesContract) -> FuturesExpiryFlattenGuard | None:
    """Return the pre-expiry guard composed for this contract's venue, if it has one."""
    factory = _EXPIRY_GUARDS.get(contract.product.exchange_code.value)
    return factory() if factory is not None else None


def chronological_session_resolver(
    contract: FuturesContract,
) -> FuturesTradingSessionResolver | None:
    """Return the calendar the chronological operation plans this contract with, if any."""
    factory = _CHRONOLOGICAL_RESOLVERS.get(contract.product.exchange_code.value)
    return factory() if factory is not None else None


class DatabaseConfigurationError(RuntimeError):
    """Raised when the configured SQLite database cannot be opened or initialized."""


class FuturesDailyAcquisition(Protocol):
    """What a market-data command needs from either provider's acquisition use case."""

    def execute(
        self, query: FuturesDailyHistoricalAcquisitionQuery
    ) -> FuturesDailyAcquisitionResult: ...


@dataclass(frozen=True, slots=True)
class DatabaseRuntime:
    """Every database-backed port and use case the operational commands need.

    ``paper_session`` is the unguarded session. ``expiry_guarded_paper_sessions``
    maps a venue to the session composed with that venue's pre-expiry guard;
    commands obtain the right one for a contract from ``paper_session_for``.
    """

    economics_store: FuturesContractEconomicsStore
    economics_repository: FuturesContractEconomicsRepository
    market_repository: FuturesHistoricalMarketDataRepository
    forward_repository: FuturesForwardResearchRecordRepository
    order_repository: FuturesPaperOrderRepository
    fill_repository: FuturesPaperFillRepository
    paper_session: RunFuturesPaperTradingSessionUseCase
    valuation: BuildFuturesPaperTradingValuationUseCase
    snapshot: GetFuturesPaperTradingSnapshotUseCase
    expiry_guarded_paper_sessions: Mapping[str, RunFuturesPaperTradingSessionUseCase] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def paper_session_for(self, contract: FuturesContract) -> RunFuturesPaperTradingSessionUseCase:
        """Return the paper session for this contract's venue.

        Only the contract's own exchange is consulted. A venue without a guard
        gets ``paper_session`` itself, unchanged.
        """
        if not isinstance(contract, FuturesContract):
            raise TypeError("DatabaseRuntime contract must be a FuturesContract.")
        venue = contract.product.exchange_code.value
        return self.expiry_guarded_paper_sessions.get(venue, self.paper_session)


def initialize_database(path: Path) -> None:
    """Create every futures table in one SQLite file through production initializers.

    Economics are the contract-level ``futures_contract_economics`` table. The
    historical product-level ``futures_product_economics`` table is no longer
    created; in an existing file it is left exactly as it was and never read.
    """
    if not path.parent.is_dir():
        raise DatabaseConfigurationError(f"Database directory does not exist: {path.parent}")
    if path.is_dir():
        raise DatabaseConfigurationError(f"Database path is a directory: {path}")
    try:
        with closing(sqlite3.connect(path)) as connection:
            initialize_futures_market_data_schema(connection)
            initialize_futures_forward_research_record_schema(connection)
            initialize_futures_paper_trading_schema(connection)
            initialize_futures_contract_economics_schema(connection)
    except sqlite3.Error as exc:
        raise DatabaseConfigurationError(f"Database cannot be opened: {path}") from exc


def build_database_runtime(path: Path) -> DatabaseRuntime:
    """Initialize the database and wire the SQLite adapters and use cases."""
    initialize_database(path)
    market = SQLiteFuturesHistoricalMarketDataRepository(path)
    forward = SQLiteFuturesForwardResearchRecordRepository(path)
    orders = SQLiteFuturesPaperOrderRepository(path)
    fills = SQLiteFuturesPaperFillRepository(path)
    economics = SQLiteFuturesContractEconomicsRepository(path)

    def paper_session(
        expiry_guard: FuturesExpiryFlattenGuard | None = None,
    ) -> RunFuturesPaperTradingSessionUseCase:
        # Guarded or not, every session works over the same SQLite ports.
        return RunFuturesPaperTradingSessionUseCase(
            market_repository=market,
            forward_store=SQLiteFuturesForwardResearchRecordStore(path),
            forward_repository=forward,
            order_store=SQLiteFuturesPaperOrderStore(path),
            order_repository=orders,
            fill_store=SQLiteFuturesPaperFillStore(path),
            fill_repository=fills,
            expiry_guard=expiry_guard,
        )

    return DatabaseRuntime(
        economics_store=SQLiteFuturesContractEconomicsStore(path),
        economics_repository=economics,
        market_repository=market,
        forward_repository=forward,
        order_repository=orders,
        fill_repository=fills,
        paper_session=paper_session(),
        valuation=BuildFuturesPaperTradingValuationUseCase(
            order_repository=orders,
            fill_repository=fills,
            market_repository=market,
            economics_repository=economics,
        ),
        snapshot=GetFuturesPaperTradingSnapshotUseCase(
            market_repository=market,
            forward_repository=forward,
            order_repository=orders,
            fill_repository=fills,
            economics_repository=economics,
        ),
        expiry_guarded_paper_sessions=MappingProxyType(
            {venue: paper_session(guard()) for venue, guard in _EXPIRY_GUARDS.items()}
        ),
    )


@dataclass(frozen=True, slots=True)
class OptionEconomicsRuntime:
    """The writable option economics ports and the fail-closed query, over one SQLite file.

    Building it creates the option table, so it serves configuration writes
    only; read-only commands use build_option_economics_lookup instead.
    """

    store: OptionContractEconomicsStore
    repository: OptionContractEconomicsRepository
    lookup: GetOptionContractEconomicsUseCase


def _require_option_database_path(path: Path) -> None:
    """Refuse a database path that can never be a SQLite file, creating nothing."""
    if not path.parent.is_dir():
        raise DatabaseConfigurationError(f"Database directory does not exist: {path.parent}")
    if path.is_dir():
        raise DatabaseConfigurationError(f"Database path is a directory: {path}")


def initialize_option_economics_database(path: Path) -> None:
    """Create only the option contract economics table in one SQLite file.

    This is a write: it is for configuration commands, never for reads. The file
    may already hold futures tables; they are neither created nor touched here.
    The futures initializer, in turn, never creates this table.
    """
    _require_option_database_path(path)
    try:
        with closing(sqlite3.connect(path)) as connection:
            initialize_option_contract_economics_schema(connection)
    except sqlite3.Error as exc:
        raise DatabaseConfigurationError(f"Database cannot be opened: {path}") from exc


def build_option_economics_lookup(path: Path) -> GetOptionContractEconomicsUseCase:
    """Wire the fail-closed option economics query read-only, creating nothing.

    Only the path is checked. A database file that does not exist yet, or one
    without the option table, holds no option economics, so the query fails
    closed with not-found rather than creating either.
    """
    _require_option_database_path(path)
    return GetOptionContractEconomicsUseCase(SQLiteOptionContractEconomicsRepository(path))


def build_option_economics_runtime(path: Path) -> OptionEconomicsRuntime:
    """Initialize the option economics table and wire its SQLite adapters for writes."""
    initialize_option_economics_database(path)
    repository = SQLiteOptionContractEconomicsRepository(path)
    return OptionEconomicsRuntime(
        store=SQLiteOptionContractEconomicsStore(path),
        repository=repository,
        lookup=GetOptionContractEconomicsUseCase(repository),
    )


@dataclass(frozen=True, slots=True)
class OptionInstrumentRuntime:
    """The Upstox option master and the listing store, for the instrument sync only.

    Building it checks the database path and creates nothing. The store creates
    the two listing tables itself, immediately before its first write, so a
    sync whose provider fetch fails leaves the database untouched.
    """

    master: UpstoxOptionInstrumentMaster
    store: SQLiteOptionListingStore


def build_option_instruments_runtime(
    path: Path, *, fetch: UpstoxFetch | None = None
) -> OptionInstrumentRuntime:
    """Wire the public Upstox option master and the listing store for one database.

    ``fetch`` replaces the master's HTTP transport, for tests only.
    """
    _require_option_database_path(path)
    master = (
        UpstoxOptionInstrumentMaster()
        if fetch is None
        else UpstoxOptionInstrumentMaster(fetch=fetch)
    )
    return OptionInstrumentRuntime(master=master, store=SQLiteOptionListingStore(path))


def build_option_listing_lookup(path: Path) -> SQLiteOptionListingRepository:
    """Wire the read-only option listing repository, creating nothing.

    Only the path is checked. A database file that does not exist yet, or one
    without the listing tables, holds no listings.
    """
    _require_option_database_path(path)
    return SQLiteOptionListingRepository(path)


def build_market_sync_runtime(
    path: Path, api_key: str, *, clock: Callable[[], datetime] | None = None
) -> AcquireFuturesDailyHistoryUseCase:
    """Initialize the database and wire Databento daily acquisition into it.

    Without a clock the adapter's completed-session guard reads the wall clock.
    """
    initialize_database(path)
    source = (
        DatabentoFuturesHistoricalMarketDataSource(api_key)
        if clock is None
        else DatabentoFuturesHistoricalMarketDataSource(api_key, clock=clock)
    )
    return AcquireFuturesDailyHistoryUseCase(
        ExchangeCalendarFuturesTradingSessionResolver(),
        source,
        AggregateFuturesDailySessionBarUseCase(),
        SQLiteFuturesHistoricalMarketDataStore(path),
    )


def build_upstox_market_sync_runtime(
    path: Path,
    access_token: str,
    *,
    fetch: UpstoxFetch | None = None,
    current_instant: datetime | None = None,
) -> AcquireFuturesNativeDailyHistoryUseCase:
    """Initialize the database and wire Upstox native daily acquisition into it.

    This builder reads no clock. Unlike the Databento adapter, the Upstox
    adapter has no completed-session guard, and none is added here: whether a
    session's daily candle is final is not established, so the caller's
    explicit trading-date range is the only eligibility rule.

    ``current_instant`` is an aware instant the caller captured once. With it,
    the adapter serves that instant's venue date from Upstox's current-day
    endpoint; without it -- manual ``market-data sync`` -- every date is
    historical. It routes endpoints only and decides no eligibility.

    ``fetch`` replaces the adapter's HTTP transport, for tests only.
    """
    initialize_database(path)
    source = (
        UpstoxFuturesNativeDailyMarketDataSource(access_token, current_instant=current_instant)
        if fetch is None
        else UpstoxFuturesNativeDailyMarketDataSource(
            access_token, fetch=fetch, current_instant=current_instant
        )
    )
    return AcquireFuturesNativeDailyHistoryUseCase(
        NSEFuturesTradingSessionResolver(),
        source,
        SQLiteFuturesHistoricalMarketDataStore(path),
    )


def build_futures_analysis(runtime: DatabaseRuntime) -> CalculateFuturesAnalysisUseCase:
    """Compose the read-only futures analysis over one database runtime's ports.

    It reads through the runtime's repositories only, with the built-in
    analysis generator the paper session uses; it holds no store.
    """
    return CalculateFuturesAnalysisUseCase(
        forward_repository=runtime.forward_repository,
        order_repository=runtime.order_repository,
        fill_repository=runtime.fill_repository,
        market_repository=runtime.market_repository,
        economics_repository=runtime.economics_repository,
        analysis_generator=FuturesAssetAnalysisGenerator(),
    )


def build_daily_bar_finality_policy(
    settings: FuturesSessionOperationSettings,
) -> FuturesDailyBarFinalityPolicy:
    """Compose the configured daily-bar finality policy; disabled unless approved.

    Only the two deterministic Application policies exist. Neither reads a
    clock, and there is deliberately no automatic, time-based composition.
    """
    if settings.finality_mode is FuturesDailyBarFinalityMode.OPERATOR_APPROVED:
        return OperatorApprovedFuturesDailyBarFinalityPolicy(settings.final_through)
    return DisabledFuturesDailyBarFinalityPolicy()
