"""Tests for the operator option daily market-data command.

    northstar options market-data sync

Commands run against real temporary SQLite files holding a synthetic persisted
Upstox listing. Candles are served by an injected fetch; no real provider
instrument key is used, no network is touched and no clock is read.
"""

from __future__ import annotations

import io
import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError

import pytest
from northstar_application.application_services import AcquireOptionNativeDailyHistoryUseCase
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import (
    ExchangeCode,
    PointInTime,
    Quantity,
    Symbol,
    Timeframe,
)
from northstar_core.options import (
    OptionContract,
    OptionOHLCVBar,
    OptionPremium,
    OptionProductReference,
    OptionRight,
    OptionStrike,
)
from northstar_infrastructure.market_data import (
    NSEOptionTradingSessionResolver,
    SQLiteOptionDailyAcquisitionStore,
    SQLiteOptionHistoricalMarketDataStore,
    SQLiteOptionListingRepository,
    SQLiteOptionListingStore,
    UpstoxOptionListing,
    UpstoxOptionMasterSnapshot,
    UpstoxOptionNativeDailyMarketDataSource,
)

from northstar_api.cli import ExitCode, main
from northstar_api.operations_lock import DatabaseOperationsLock
from northstar_api.runtime import build_database_runtime, build_option_market_data_runtime

_TOKEN = "options-test-token"
_ENV = {"UPSTOX_ANALYTICS_TOKEN": _TOKEN}
_NIFTY = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_PUT = OptionContract(
    _NIFTY, ExpirationDate("2026-10-27"), OptionStrike(Decimal("22600")), OptionRight.PUT
)
_NOVEMBER_CALL = OptionContract(
    _NIFTY, ExpirationDate("2026-11-23"), OptionStrike(Decimal("22600")), OptionRight.CALL
)
_HISTORICAL = "https://api.upstox.com/v3/historical-candle/"
_MARKET_TABLES = {"option_ohlcv", "option_daily_provider_open_interest"}
_FUTURES_TABLES = {
    "futures_ohlcv",
    "futures_forward_research_records",
    "futures_paper_orders",
    "futures_paper_fills",
    "futures_contract_economics",
}


def _row(day: str, close: str = "132.6", volume: str = "2600", oi: str = "6686095") -> str:
    return f'["{day}T00:00:00+05:30", 191.8, 226.05, 122.45, {close}, {volume}, {oi}]'


def _candles(*rows: str) -> bytes:
    return f'{{"status": "success", "data": {{"candles": [{", ".join(rows)}]}}}}'.encode()


# Newest first, as Upstox answers.
_WEEK = _candles(_row("2026-10-07"), _row("2026-10-06", close="140.1"), _row("2026-10-05"))


class Fetch:
    """Serves the historical candle endpoint only and records every request."""

    def __init__(self, body: bytes = _WEEK, error: Exception | None = None) -> None:
        self.body, self.error = body, error
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __call__(self, url: str, headers, timeout) -> bytes:
        self.calls.append((url, dict(headers)))
        if not url.startswith(_HISTORICAL) or "/intraday/" in url:
            raise AssertionError(f"unexpected URL requested: {url}")
        if self.error is not None:
            raise self.error
        return self.body


def _refusing_clock() -> datetime:
    raise AssertionError("options market-data sync must not read a clock")


@dataclass(frozen=True)
class Outcome:
    code: int
    out: str
    err: str


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "northstar.sqlite3"
    listings = (
        UpstoxOptionListing(_PUT, "NSE_FO|1001", 65),
        UpstoxOptionListing(_NOVEMBER_CALL, "NSE_FO|1002", 65),
    )
    SQLiteOptionListingStore(path).store(
        UpstoxOptionMasterSnapshot(
            snapshot_sha256="a" * 64,
            source_url="https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
            fetched_at=PointInTime("2026-10-04T04:30:00Z"),
            record_count=100,
            option_record_count=2,
            listings=listings,
        )
    )
    return path


def _sync(
    database: Path,
    fetch: Fetch | None = None,
    *,
    env: dict[str, str] | None = None,
    runtime=None,
    expiration: str = "2026-10-27",
    strike: str = "22600",
    right: str = "PUT",
    start: str = "2026-10-05",
    end: str = "2026-10-07",
    product: str = "NIFTY",
    exchange: str = "NSE",
) -> Outcome:
    fetch = fetch if fetch is not None else Fetch()
    if runtime is None:

        def runtime(path, token):
            return build_option_market_data_runtime(path, token, fetch=fetch)

    out, err = io.StringIO(), io.StringIO()
    code = main(
        ["options", "market-data", "sync", "--database", str(database),
         "--product", product, "--exchange", exchange, "--expiration", expiration,
         "--strike", strike, "--right", right, "--start", start, "--end", end],
        env=_ENV if env is None else env,
        stdout=out,
        stderr=err,
        clock=_refusing_clock,
        option_market_data_runtime=runtime,
    )  # fmt: skip
    return Outcome(code, out.getvalue(), err.getvalue())


def _tables(database: Path) -> set[str]:
    with closing(sqlite3.connect(database)) as connection:
        return {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }


def _rows(database: Path, table: str) -> list[tuple]:
    if table not in _tables(database):
        return []
    with closing(sqlite3.connect(database)) as connection:
        return sorted(connection.execute(f"SELECT * FROM {table}").fetchall())  # noqa: S608


def _bar_dates(database: Path) -> list[str]:
    return [row[6] for row in _rows(database, "option_ohlcv")]


def _oi_dates(database: Path) -> list[str]:
    return [row[6] for row in _rows(database, "option_daily_provider_open_interest")]


_HEADER = (
    "OPTIONS MARKET DATA SYNC\n"
    "Provider: upstox\n"
    "Contract: NIFTY@NSE 2026-10-27 22600 PUT\n"
    "Date range: 2026-10-05 .. 2026-10-07 (trading dates, historical endpoint only)\n"
)
_STOPPED = "Sync stopped. Nothing from this range was stored."
_PERSISTED = "(identical bars already stored count as persisted)"


# ---------------------------------------------------------------------------
# Success
# ---------------------------------------------------------------------------


def test_a_liquid_contract_syncs_every_session(database: Path) -> None:
    fetch = Fetch()

    outcome = _sync(database, fetch)

    assert outcome == Outcome(
        0,
        _HEADER + "SYNC: COMPLETED\n"
        "Sessions in range: 3\n"
        "Sessions with a persisted daily bar: 3 " + _PERSISTED + "\n"
        "Sessions without a provider candle: 0\n"
        "Provider open-interest records persisted: 3 (raw provider values, not normalized)\n"
        "Finality: not assessed\n",
        "",
    )
    assert _bar_dates(database) == [
        "2026-10-05T10:10:00Z",
        "2026-10-06T10:10:00Z",
        "2026-10-07T10:10:00Z",
    ]
    assert _oi_dates(database) == ["2026-10-05", "2026-10-06", "2026-10-07"]
    assert [row[-1] for row in _rows(database, "option_ohlcv")] == ["40", "40", "40"]
    assert {row[-1] for row in _rows(database, "option_daily_provider_open_interest")} == {
        "6686095"
    }
    ((url, headers),) = fetch.calls
    assert url == f"{_HISTORICAL}NSE_FO%7C1001/days/1/2026-10-07/2026-10-05"
    assert headers["Authorization"] == f"Bearer {_TOKEN}"


def test_sessions_without_a_provider_candle_are_reported_not_fabricated(database: Path) -> None:
    outcome = _sync(database, Fetch(_candles(_row("2026-10-07"), _row("2026-10-05"))))

    assert outcome == Outcome(
        0,
        _HEADER + "SYNC: COMPLETED\n"
        "Sessions in range: 3\n"
        "Sessions with a persisted daily bar: 2 " + _PERSISTED + "\n"
        "Sessions without a provider candle: 1\n"
        "Missing trading dates: 2026-10-06\n"
        "Provider open-interest records persisted: 2 (raw provider values, not normalized)\n"
        "Finality: not assessed\n",
        "",
    )
    assert _bar_dates(database) == ["2026-10-05T10:10:00Z", "2026-10-07T10:10:00Z"]
    assert _oi_dates(database) == ["2026-10-05", "2026-10-07"]
    assert "no-trade" not in outcome.out.lower()


def test_a_range_without_any_candle_succeeds_and_stores_nothing(database: Path) -> None:
    outcome = _sync(database, Fetch(_candles()))

    assert outcome == Outcome(
        0,
        _HEADER + "SYNC: COMPLETED\n"
        "Sessions in range: 3\n"
        "Sessions with a persisted daily bar: 0 " + _PERSISTED + "\n"
        "Sessions without a provider candle: 3\n"
        "Missing trading dates: 2026-10-05, 2026-10-06, 2026-10-07\n"
        "Provider open-interest records persisted: 0 (raw provider values, not normalized)\n"
        "Finality: not assessed\n",
        "",
    )
    assert not _tables(database) & _MARKET_TABLES


def test_a_repeated_sync_is_idempotent(database: Path) -> None:
    first = _sync(database)
    before = (
        _rows(database, "option_ohlcv"),
        _rows(database, "option_daily_provider_open_interest"),
    )

    second = _sync(database)

    assert second == first
    assert (
        _rows(database, "option_ohlcv"),
        _rows(database, "option_daily_provider_open_interest"),
    ) == before


def test_a_later_sync_fills_a_session_without_a_candle(database: Path) -> None:
    _sync(database, Fetch(_candles(_row("2026-10-07"), _row("2026-10-05"))))

    outcome = _sync(database)

    assert outcome.code == 0
    assert "Sessions without a provider candle: 0" in outcome.out
    assert len(_bar_dates(database)) == 3
    assert len(_oi_dates(database)) == 3


def test_the_sync_reads_no_clock_and_never_routes_to_the_current_day(database: Path) -> None:
    fetch = Fetch(_candles())

    # An end date that may be venue-today is not rejected and still uses the historical endpoint.
    outcome = _sync(database, fetch, start="2026-10-08", end="2026-10-08")

    assert outcome.code == 0
    assert "Missing trading dates: 2026-10-08" in outcome.out
    assert all("/intraday/" not in url for url, _ in fetch.calls)


def test_the_sync_leaves_futures_tables_untouched(database: Path) -> None:
    build_database_runtime(database)
    before = _tables(database)

    assert _sync(database).code == 0

    assert _tables(database) == before | _MARKET_TABLES
    assert _FUTURES_TABLES <= before
    for table in _FUTURES_TABLES:
        assert _rows(database, table) == []


def test_building_the_runtime_creates_nothing(tmp_path: Path) -> None:
    path = tmp_path / "fresh.sqlite3"

    build_option_market_data_runtime(path, _TOKEN, fetch=Fetch())

    assert not path.exists()


# ---------------------------------------------------------------------------
# INPUT 2
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"right": "PE"}, "Invalid right"),
        ({"strike": "abc"}, "Invalid strike"),
        ({"strike": "-5"}, "Invalid strike"),
        ({"expiration": "27-10-2026"}, "Invalid expiration"),
        ({"start": "2026-10-5"}, "Invalid start date"),
        ({"end": "2026-02-30"}, "Invalid end date"),
        ({"start": "2026-10-07", "end": "2026-10-05"}, "Invalid date range"),
        ({"product": "BANKNIFTY"}, "Unsupported option product"),
        ({"exchange": "BSE"}, "Unsupported option product"),
        ({"product": "NIFTY 50"}, "Invalid product"),
        ({"end": "2026-10-28"}, "after the contract's expiration 2026-10-27"),
    ],
    ids=[
        "right-alias", "strike-text", "strike-negative", "expiration", "start", "end",
        "reversed", "product", "exchange", "product-case", "after-expiration",
    ],
)  # fmt: skip
def test_invalid_input_exits_2_before_anything(tmp_path: Path, overrides, message) -> None:
    path = tmp_path / "untouched.sqlite3"
    called: list[object] = []

    outcome = _sync(path, runtime=lambda *a: called.append(a), **overrides)

    assert outcome.code == ExitCode.INPUT
    assert message in outcome.err
    assert outcome.out == ""
    assert called == []
    assert list(tmp_path.iterdir()) == []


def test_the_expiration_date_itself_is_a_valid_end(database: Path) -> None:
    outcome = _sync(database, Fetch(_candles()), start="2026-10-26", end="2026-10-27")

    assert outcome.code == 0


# ---------------------------------------------------------------------------
# CONFIGURATION 3
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("env", [{}, {"UPSTOX_ANALYTICS_TOKEN": "   "}])
def test_a_missing_token_exits_3_and_builds_nothing(database: Path, env) -> None:
    called: list[object] = []

    outcome = _sync(database, env=env, runtime=lambda *a: called.append(a))

    assert outcome.code == ExitCode.CONFIGURATION
    assert "UPSTOX_ANALYTICS_TOKEN is not set" in outcome.err
    assert called == []
    assert not _tables(database) & _MARKET_TABLES


def test_a_multiline_token_exits_3_without_echoing_it(database: Path) -> None:
    outcome = _sync(database, env={"UPSTOX_ANALYTICS_TOKEN": "abc\ndef"})

    assert outcome.code == ExitCode.CONFIGURATION
    assert "must be a single line" in outcome.err
    assert "abc" not in outcome.err


def test_a_missing_database_directory_exits_3(tmp_path: Path) -> None:
    outcome = _sync(tmp_path / "missing" / "northstar.sqlite3")

    assert outcome.code == ExitCode.CONFIGURATION


# ---------------------------------------------------------------------------
# DATA 4
# ---------------------------------------------------------------------------


def test_a_contract_without_a_persisted_listing_exits_4(database: Path) -> None:
    fetch = Fetch()

    outcome = _sync(database, fetch, strike="22650")

    assert outcome.code == ExitCode.DATA
    assert "DATA ERROR: OptionProviderListingNotStoredError" in outcome.err
    assert "northstar options instruments sync" in outcome.err
    assert _STOPPED in outcome.err
    assert fetch.calls == []
    assert not _tables(database) & _MARKET_TABLES


def test_a_candle_on_a_non_session_exits_4_and_stores_nothing(database: Path) -> None:
    # 2026-10-04 is a Sunday.
    body = _candles(_row("2026-10-05"), _row("2026-10-04"))

    outcome = _sync(database, Fetch(body), start="2026-10-03", end="2026-10-05")

    assert outcome.code == ExitCode.DATA
    assert "DATA ERROR: OptionDailySessionCoverageError" in outcome.err
    assert "candles for non-sessions 2026-10-04" in outcome.err
    assert _STOPPED in outcome.err
    assert not _tables(database) & _MARKET_TABLES


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"start": "2026-07-31", "end": "2026-08-04"}, "effective from 2026-08-03"),
        ({"start": "2026-12-31", "end": "2027-01-04", "expiration": "2027-03-30"}, "not loaded"),
        (
            {"start": "2026-11-06", "end": "2026-11-09", "expiration": "2026-11-23",
             "right": "CALL"},
            "special session",
        ),
    ],
    ids=["before-session-regime", "unloaded-year", "special-session"],
)  # fmt: skip
def test_a_range_the_session_reference_cannot_resolve_exits_4(
    database: Path, overrides, message
) -> None:
    fetch = Fetch()

    outcome = _sync(database, fetch, **overrides)

    assert outcome.code == ExitCode.DATA
    assert "DATA ERROR: OptionTradingSessionResolutionError" in outcome.err
    assert message in outcome.err
    assert fetch.calls == []


# ---------------------------------------------------------------------------
# STATE 5
# ---------------------------------------------------------------------------


def test_a_differing_stored_bar_exits_5_and_stores_no_open_interest(database: Path) -> None:
    SQLiteOptionHistoricalMarketDataStore(database).store(
        (
            OptionOHLCVBar(
                _PUT, PointInTime("2026-10-06T10:10:00Z"), Timeframe("1d"),
                OptionPremium(Decimal("191.8")), OptionPremium(Decimal("226.05")),
                OptionPremium(Decimal("122.45")), OptionPremium(Decimal("199")),
                Quantity(Decimal("40")),
            ),
        )
    )  # fmt: skip

    outcome = _sync(database)

    assert outcome.code == ExitCode.STATE
    assert "STATE ERROR: OptionHistoricalMarketDataConflictError" in outcome.err
    assert _STOPPED in outcome.err
    assert _bar_dates(database) == ["2026-10-06T10:10:00Z"]
    assert _oi_dates(database) == []


def test_a_differing_stored_open_interest_exits_5_and_stores_no_bar(database: Path) -> None:
    _sync(database, Fetch(_candles(_row("2026-10-05"))), start="2026-10-05", end="2026-10-05")

    outcome = _sync(database, Fetch(_candles(_row("2026-10-06"), _row("2026-10-05", oi="65"))))

    assert outcome.code == ExitCode.STATE
    assert "STATE ERROR: OptionOpenInterestConflictError" in outcome.err
    assert _bar_dates(database) == ["2026-10-05T10:10:00Z"]
    assert _oi_dates(database) == ["2026-10-05"]


def test_a_corrupt_persisted_listing_exits_5(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("UPDATE option_provider_listings SET exchange_lot_size = 'x'")

    outcome = _sync(database)

    assert outcome.code == ExitCode.STATE
    assert "OptionListingStorageError" in outcome.err


def test_a_sync_refuses_while_another_writer_holds_the_lock(database: Path) -> None:
    fetch = Fetch()

    with DatabaseOperationsLock(database):
        outcome = _sync(database, fetch)

    assert outcome.code == ExitCode.STATE
    assert fetch.calls == []
    assert not _tables(database) & _MARKET_TABLES


# ---------------------------------------------------------------------------
# PROVIDER 6
# ---------------------------------------------------------------------------


def _http_error(status: int, body: bytes = b"") -> HTTPError:
    return HTTPError("u", status, "provider error", {}, io.BytesIO(body))


@pytest.mark.parametrize(
    ("fetch", "name"),
    [
        (Fetch(error=_http_error(503)), "UpstoxProviderUnavailableError"),
        (Fetch(error=_http_error(401)), "UpstoxAuthenticationError"),
        (
            Fetch(error=_http_error(400, json.dumps(
                {"status": "error", "errors": [{"errorCode": "UDAPI100011"}]}).encode())),
            "UpstoxInvalidInstrumentKeyError",
        ),
        (Fetch(_candles(_row("2026-10-05", volume="100"))), "UpstoxMarketDataSourceError"),
        (Fetch(_candles(_row("2026-10-05", oi="null"))), "UpstoxMarketDataSourceError"),
        (Fetch(_candles(_row("2026-10-05", close="-1"))), "UpstoxMarketDataSourceError"),
        (Fetch(b"not json"), "UpstoxProviderUnavailableError"),
    ],
    ids=["unavailable", "auth", "expired-key", "volume-remainder", "null-oi",
         "negative-premium", "undecodable"],
)  # fmt: skip
def test_a_provider_failure_exits_6_and_stores_nothing(database: Path, fetch, name) -> None:
    outcome = _sync(database, fetch)

    assert outcome.code == ExitCode.PROVIDER
    assert f"PROVIDER ERROR: {name}" in outcome.err
    assert _STOPPED in outcome.err
    assert _TOKEN not in outcome.err + outcome.out
    assert not _tables(database) & _MARKET_TABLES


# ---------------------------------------------------------------------------
# INTERNAL 1
# ---------------------------------------------------------------------------


class _MismatchedSource(UpstoxOptionNativeDailyMarketDataSource):
    """Hands the store open interest for a date it built no bar for."""

    def take_open_interest(self):
        captured = super().take_open_interest()
        return captured[:-1]


def test_open_interest_not_matching_the_bars_exits_1(database: Path) -> None:
    def runtime(path, token):
        source = _MismatchedSource(
            token, listings=SQLiteOptionListingRepository(path), fetch=Fetch()
        )
        store = SQLiteOptionDailyAcquisitionStore(path, open_interest=source, provider="upstox")
        return AcquireOptionNativeDailyHistoryUseCase(
            NSEOptionTradingSessionResolver(), source, store
        )

    outcome = _sync(database, runtime=runtime)

    assert outcome.code == ExitCode.INTERNAL
    assert "INTERNAL ERROR: OptionOpenInterestCaptureError" in outcome.err
    assert _STOPPED in outcome.err
    assert not _tables(database) & _MARKET_TABLES


# ---------------------------------------------------------------------------
# Neighbouring commands
# ---------------------------------------------------------------------------


def test_the_options_instruments_and_economics_commands_are_unchanged() -> None:
    from northstar_api.cli import build_parser

    parser = build_parser()
    for argv in (
        ["options", "instruments", "sync", "--database", "d", "--product", "NIFTY",
         "--exchange", "NSE"],
        ["options", "economics", "show", "--database", "d", "--product", "NIFTY",
         "--exchange", "NSE", "--expiration", "2026-10-27", "--strike", "1", "--right", "PUT"],
        ["market-data", "sync", "--database", "d", "--product", "NIFTY", "--exchange", "NSE",
         "--expiration", "2026-10-27", "--start", "2026-10-05", "--end", "2026-10-07"],
    ):  # fmt: skip
        assert parser.parse_args(argv).handler.__name__ in {
            "_options_instruments_sync",
            "_options_economics_show",
            "_market_data_sync",
        }
