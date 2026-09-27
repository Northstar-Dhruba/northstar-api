"""Production wiring for the operational CLI.

This is a composition root: it is the only place that chooses concrete
Infrastructure adapters for the futures paper-trading use cases. Every adapter
works against one explicitly supplied SQLite file; nothing is cached between
invocations, so a fresh process reconstructs everything from that file.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from northstar_application.application_services import (
    AcquireFuturesDailyHistoryUseCase,
    AggregateFuturesDailySessionBarUseCase,
    BuildFuturesPaperTradingValuationUseCase,
    GetFuturesPaperTradingSnapshotUseCase,
    RunFuturesPaperTradingSessionUseCase,
)
from northstar_application.ports import (
    FuturesContractEconomicsRepository,
    FuturesContractEconomicsStore,
    FuturesForwardResearchRecordRepository,
    FuturesHistoricalMarketDataRepository,
    FuturesPaperFillRepository,
    FuturesPaperOrderRepository,
)
from northstar_infrastructure.market_data import (
    DatabentoFuturesHistoricalMarketDataSource,
    ExchangeCalendarFuturesTradingSessionResolver,
    SQLiteFuturesHistoricalMarketDataRepository,
    SQLiteFuturesHistoricalMarketDataStore,
    initialize_futures_market_data_schema,
)
from northstar_infrastructure.persistence import (
    SQLiteFuturesContractEconomicsRepository,
    SQLiteFuturesContractEconomicsStore,
    SQLiteFuturesForwardResearchRecordRepository,
    SQLiteFuturesForwardResearchRecordStore,
    SQLiteFuturesPaperFillRepository,
    SQLiteFuturesPaperFillStore,
    SQLiteFuturesPaperOrderRepository,
    SQLiteFuturesPaperOrderStore,
    initialize_futures_contract_economics_schema,
    initialize_futures_forward_research_record_schema,
    initialize_futures_paper_trading_schema,
)


class DatabaseConfigurationError(RuntimeError):
    """Raised when the configured SQLite database cannot be opened or initialized."""


@dataclass(frozen=True, slots=True)
class DatabaseRuntime:
    """Every database-backed port and use case the operational commands need."""

    economics_store: FuturesContractEconomicsStore
    economics_repository: FuturesContractEconomicsRepository
    market_repository: FuturesHistoricalMarketDataRepository
    forward_repository: FuturesForwardResearchRecordRepository
    order_repository: FuturesPaperOrderRepository
    fill_repository: FuturesPaperFillRepository
    paper_session: RunFuturesPaperTradingSessionUseCase
    valuation: BuildFuturesPaperTradingValuationUseCase
    snapshot: GetFuturesPaperTradingSnapshotUseCase


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
    return DatabaseRuntime(
        economics_store=SQLiteFuturesContractEconomicsStore(path),
        economics_repository=economics,
        market_repository=market,
        forward_repository=forward,
        order_repository=orders,
        fill_repository=fills,
        paper_session=RunFuturesPaperTradingSessionUseCase(
            market_repository=market,
            forward_store=SQLiteFuturesForwardResearchRecordStore(path),
            forward_repository=forward,
            order_store=SQLiteFuturesPaperOrderStore(path),
            order_repository=orders,
            fill_store=SQLiteFuturesPaperFillStore(path),
            fill_repository=fills,
        ),
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
    )


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
