"""Tests for the read-only point-in-time option chain command.

    northstar options chain show

Commands run against real temporary SQLite files holding synthetic persisted
Upstox listings and canonical option bars. No real provider instrument key is
used; no network, token, clock or lock is touched.
"""

from __future__ import annotations

import io
import socket
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_application.application_services import BuildOptionChainSnapshotUseCase
from northstar_application.ports import OptionListedContractRepository
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
    ProviderOptionOpenInterest,
    SQLiteOptionChainDailyBarRepository,
    SQLiteOptionHistoricalMarketDataStore,
    SQLiteOptionListingStore,
    SQLiteOptionProviderOpenInterestStore,
    UpstoxOptionListing,
    UpstoxOptionMasterSnapshot,
)

from northstar_api.cli import ExitCode, build_parser, main
from northstar_api.runtime import build_database_runtime

_NIFTY = OptionProductReference(Symbol("NIFTY"), ExchangeCode("NSE"))
_CALL, _PUT = OptionRight.CALL, OptionRight.PUT
_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
_FIRST_SYNC = "2026-10-08T06:59:12.429903Z"
_LATER_SYNC = "2026-10-08T14:26:54.079725Z"
_OCT7, _OCT8, _OCT9 = "2026-10-07T10:10:00Z", "2026-10-08T10:10:00Z", "2026-10-09T10:10:00Z"


def _contract(strike: str, right: OptionRight = _CALL, expiration: str = "2026-10-27"):
    return OptionContract(_NIFTY, ExpirationDate(expiration), OptionStrike(Decimal(strike)), right)


_C22550, _P22550 = _contract("22550"), _contract("22550", _PUT)
_C22600, _P22600 = _contract("22600"), _contract("22600", _PUT)
_C22650 = _contract("22650")
_C22700 = _contract("22700")


def _bar(contract, instant: str = _OCT8, close: str = "132.6", volume: str = "40"):
    return OptionOHLCVBar(
        contract=contract,
        point_in_time=PointInTime(instant),
        timeframe=Timeframe("1d"),
        open=OptionPremium(Decimal(close)),
        high=OptionPremium(Decimal(close)),
        low=OptionPremium(Decimal(close)),
        close=OptionPremium(Decimal(close)),
        volume=Quantity(Decimal(volume)),
    )


def _sync(path: Path, at: str, sha: str, *contracts: OptionContract) -> None:
    listings = tuple(
        UpstoxOptionListing(c, f"NSE_FO|{c.expiration_date}-{c.strike}-{c.right.value}", 65)
        for c in contracts
    )
    SQLiteOptionListingStore(path).store(
        UpstoxOptionMasterSnapshot(sha * 64, _URL, PointInTime(at), 100, len(listings), listings)
    )


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "northstar.sqlite3"
    first = (
        _C22550,
        _P22550,
        _C22600,
        _P22600,
        _C22650,
        _contract("22600", expiration="2026-11-24"),
    )
    _sync(path, _FIRST_SYNC, "a", *first)
    # A later sync adds the 22700 CALL; its exactly stamped Oct-8 bar is hindsight.
    _sync(path, _LATER_SYNC, "b", *first, _C22700)
    SQLiteOptionHistoricalMarketDataStore(path).store(
        (
            _bar(_P22550, close="87.75", volume="4064"),
            _bar(_C22600),
            _bar(_P22600, close="140.1", volume="12"),
            _bar(_C22700, close="99", volume="7"),
            _bar(_C22600, _OCT7, close="150"),
            _bar(_C22550, _OCT9, close="300"),
        )
    )
    SQLiteOptionProviderOpenInterestStore(path).store(
        (
            ProviderOptionOpenInterest(
                "upstox", _C22600, datetime(2026, 10, 8).date(), Decimal("7777777")
            ),
        )  # fmt: skip
    )
    return path


@dataclass(frozen=True)
class Outcome:
    code: int
    out: str
    err: str


def _refusing_clock() -> datetime:
    raise AssertionError("options chain show must not read a clock")


def _show(
    database: Path,
    *,
    trading_date: str = "2026-10-08",
    expiration: str = "2026-10-27",
    product: str = "NIFTY",
    exchange: str = "NSE",
    env: dict[str, str] | None = None,
    **kwargs,
) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    code = main(
        ["options", "chain", "show", "--database", str(database), "--product", product,
         "--exchange", exchange, "--expiration", expiration, "--trading-date", trading_date],
        env={} if env is None else env,
        stdout=out,
        stderr=err,
        clock=_refusing_clock,
        **kwargs,
    )  # fmt: skip
    return Outcome(code, out.getvalue(), err.getvalue())


def _schema(path: Path) -> list[tuple]:
    with closing(sqlite3.connect(path)) as connection:
        return sorted(connection.execute("SELECT type, name, sql FROM sqlite_master").fetchall())


_EXPECTED = (
    "OPTION CHAIN: NIFTY@NSE 2026-10-27\n"
    "Trading date: 2026-10-08\n"
    "As of: 2026-10-08T10:10:00Z (session close)\n"
    "Listed contracts known by as-of: 5\n"
    "Strikes: 3\n"
    "Contracts with a daily bar: 3\n"
    "Contracts with no daily bar: 2\n"
    "\n"
    "STRIKE    CALL            PUT\n"
    "22550     no daily bar    C=87.75 V=4064\n"
    "22600     C=132.6 V=40    C=140.1 V=12\n"
    "22650     no daily bar    no known listing\n"
)


# ---------------------------------------------------------------------------
# Success
# ---------------------------------------------------------------------------


def test_the_chain_shows_every_known_listing_with_its_exact_bar(database: Path) -> None:
    assert _show(database) == Outcome(0, _EXPECTED, "")


def test_the_output_is_deterministic(database: Path) -> None:
    assert _show(database) == _show(database)


def test_a_hindsight_listing_and_its_bar_never_appear(database: Path) -> None:
    outcome = _show(database)

    assert "22700" not in outcome.out
    assert "C=99" not in outcome.out


def test_bars_from_other_sessions_never_leak_into_the_chain(database: Path) -> None:
    outcome = _show(database)

    assert "C=150" not in outcome.out  # Oct-7
    assert "C=300" not in outcome.out  # Oct-9


def test_a_later_date_includes_the_later_listing(database: Path) -> None:
    outcome = _show(database, trading_date="2026-10-09")

    assert outcome.code == 0
    assert "22700     no daily bar    no known listing" in outcome.out
    assert "22550     C=300 V=40      no daily bar" in outcome.out
    assert "Contracts with a daily bar: 1" in outcome.out


def test_no_provider_key_or_open_interest_is_rendered(database: Path) -> None:
    outcome = _show(database)

    assert "NSE_FO" not in outcome.out
    assert "7777777" not in outcome.out
    assert "no-trade" not in outcome.out.lower()
    assert "provider candle" not in outcome.out.lower()


def test_the_command_reads_no_token_network_lock_or_clock_and_writes_nothing(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(*args, **kwargs):
        raise AssertionError("options chain show must not touch the network")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    before = (_schema(database), database.read_bytes())
    files = sorted(database.parent.iterdir())

    outcome = _show(database, env={"UPSTOX_ANALYTICS_TOKEN": "unused-token"})

    assert outcome == Outcome(0, _EXPECTED, "")
    assert (_schema(database), database.read_bytes()) == before
    assert sorted(database.parent.iterdir()) == files


def test_a_missing_database_has_no_known_listing_and_is_not_created(tmp_path: Path) -> None:
    path = tmp_path / "absent.sqlite3"

    outcome = _show(path)

    assert outcome.code == ExitCode.DATA
    assert "OptionChainListingNotKnownError" in outcome.err
    assert list(tmp_path.iterdir()) == []


def test_a_futures_only_database_is_left_untouched(tmp_path: Path) -> None:
    path = tmp_path / "futures.sqlite3"
    build_database_runtime(path)
    before = _schema(path)

    assert _show(path).code == ExitCode.DATA
    assert _schema(path) == before


# ---------------------------------------------------------------------------
# DATA 4
# ---------------------------------------------------------------------------


def test_a_date_before_the_first_listing_observation_exits_4_even_with_bars(
    database: Path,
) -> None:
    outcome = _show(database, trading_date="2026-10-07")

    assert outcome.code == ExitCode.DATA
    assert outcome.out == ""
    assert "DATA ERROR: OptionChainListingNotKnownError" in outcome.err
    assert "known by 2026-10-07T10:10:00Z" in outcome.err


def test_an_expiration_without_known_listings_exits_4(database: Path) -> None:
    outcome = _show(database, expiration="2026-11-03")

    assert outcome.code == ExitCode.DATA
    assert "OptionChainListingNotKnownError" in outcome.err


@pytest.mark.parametrize(
    ("trading_date", "expiration", "message"),
    [
        ("2026-07-31", "2026-10-27", "effective from 2026-08-03"),
        ("2027-01-04", "2027-03-30", "not loaded"),
        ("2026-11-08", "2026-11-24", "special session"),
    ],
    ids=["before-session-regime", "unloaded-year", "special-session"],
)
def test_a_date_the_session_reference_cannot_resolve_exits_4(
    database: Path, trading_date, expiration, message
) -> None:
    outcome = _show(database, trading_date=trading_date, expiration=expiration)

    assert outcome.code == ExitCode.DATA
    assert "DATA ERROR: OptionTradingSessionResolutionError" in outcome.err
    assert message in outcome.err


# ---------------------------------------------------------------------------
# INPUT 2 and CONFIGURATION 3
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("trading_date", ["2026-10-10", "2026-10-11", "2026-10-02"])
def test_a_weekend_or_holiday_exits_2(database: Path, trading_date: str) -> None:
    outcome = _show(database, trading_date=trading_date)

    assert outcome.code == ExitCode.INPUT
    assert "OptionChainSessionNotFoundError" in outcome.err
    assert "not a NIFTY@NSE option trading session" in outcome.err


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"expiration": "2026-10-07"}, "have expired"),
        ({"expiration": "27-10-2026"}, "Invalid expiration"),
        ({"trading_date": "2026-10-8"}, "Invalid trading date"),
        ({"trading_date": "2026-02-30"}, "Invalid trading date"),
        ({"product": "BANKNIFTY"}, "Unsupported option product"),
        ({"exchange": "BSE"}, "Unsupported option product"),
        ({"product": "NIFTY 50"}, "Invalid product"),
    ],
    ids=["expired", "expiration", "date-format", "date-calendar", "product", "exchange",
         "product-text"],
)  # fmt: skip
def test_invalid_input_exits_2(database: Path, overrides, message) -> None:
    outcome = _show(database, **overrides)

    assert outcome.code == ExitCode.INPUT
    assert message in outcome.err
    assert outcome.out == ""


def test_the_expiration_day_has_a_chain(database: Path) -> None:
    _sync(database, _FIRST_SYNC, "c", _contract("22600", expiration="2026-10-08"))

    outcome = _show(database, expiration="2026-10-08")

    assert outcome.code == 0
    assert "Listed contracts known by as-of: 1" in outcome.out


def test_a_missing_database_directory_exits_3(tmp_path: Path) -> None:
    outcome = _show(tmp_path / "missing" / "northstar.sqlite3")

    assert outcome.code == ExitCode.CONFIGURATION


# ---------------------------------------------------------------------------
# STATE 5 and INTERNAL 1
# ---------------------------------------------------------------------------


def test_a_corrupt_listing_exits_5(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("UPDATE option_provider_listings SET strike = '22600.0' "
                           "WHERE strike = '22600' AND option_right = 'PUT'")  # fmt: skip

    outcome = _show(database)

    assert outcome.code == ExitCode.STATE
    assert "STATE ERROR: OptionListingStorageError" in outcome.err


def test_a_corrupt_option_bar_exits_5(database: Path) -> None:
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("UPDATE option_ohlcv SET volume = '40.5' WHERE strike = '22600' "
                           "AND option_right = 'CALL' AND point_in_time = ?", (_OCT8,))  # fmt: skip

    outcome = _show(database)

    assert outcome.code == ExitCode.STATE
    assert "STATE ERROR: OptionHistoricalStorageError" in outcome.err


class _DuplicatingListings(OptionListedContractRepository):
    def listed_contracts(self, query):
        return (_C22600, _C22600)


def test_a_repository_contract_violation_exits_1(database: Path) -> None:
    def runtime(path):
        return BuildOptionChainSnapshotUseCase(
            NSEOptionTradingSessionResolver(),
            _DuplicatingListings(),
            SQLiteOptionChainDailyBarRepository(path),
        )

    outcome = _show(database, option_chain_runtime=runtime)

    assert outcome.code == ExitCode.INTERNAL
    assert "INTERNAL ERROR: OptionChainContractViolationError" in outcome.err


# ---------------------------------------------------------------------------
# Neighbouring commands
# ---------------------------------------------------------------------------


def test_existing_option_and_futures_commands_are_unchanged() -> None:
    parser = build_parser()
    expected = {
        ("options", "instruments", "show"): "_options_instruments_show",
        ("options", "economics", "show"): "_options_economics_show",
        ("options", "market-data", "sync"): "_options_market_data_sync",
        ("options", "chain", "show"): "_options_chain_show",
        ("market-data", "sync"): "_market_data_sync",
    }
    contract = ["--product", "NIFTY", "--exchange", "NSE", "--expiration", "2026-10-27"]
    extra = {
        ("options", "instruments", "show"): ["--strike", "1", "--right", "PUT"],
        ("options", "economics", "show"): ["--strike", "1", "--right", "PUT"],
        ("options", "market-data", "sync"): ["--strike", "1", "--right", "PUT", "--start",
                                             "2026-10-05", "--end", "2026-10-07"],
        ("options", "chain", "show"): ["--trading-date", "2026-10-08"],
        ("market-data", "sync"): ["--start", "2026-10-05", "--end", "2026-10-07"],
    }  # fmt: skip
    for command, handler in expected.items():
        args = parser.parse_args([*command, "--database", "d", *contract, *extra[command]])
        assert args.handler.__name__ == handler
