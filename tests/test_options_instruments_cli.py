"""Tests for the operator option instrument reference commands.

    northstar options instruments sync | show

Commands run against real temporary SQLite files. The Upstox master is a
synthetic, deterministic fixture served by an injected fetch; no real provider
instrument key is used and no network is touched.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import socket
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError

import pytest

from northstar_api.cli import ExitCode, main
from northstar_api.operations_lock import DatabaseOperationsLock
from northstar_api.runtime import build_database_runtime, build_option_instruments_runtime

_IST = timezone(timedelta(hours=5, minutes=30))
_FIRST = datetime(2026, 10, 8, 10, 0, 0, tzinfo=_IST)
_LATER = datetime(2026, 10, 9, 10, 0, 0, tzinfo=_IST)
_FUTURES_TABLES = {
    "futures_ohlcv",
    "futures_forward_research_records",
    "futures_paper_orders",
    "futures_paper_fills",
    "futures_contract_economics",
}
_LISTING_TABLES = {"option_listing_snapshots", "option_provider_listings"}


def _expiry_ms(day: date) -> int:
    instant = datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=_IST)
    return int((instant - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()) * 1000


def _option(key: str, expiry: date, strike: float, kind: str = "CE", **extra) -> dict[str, Any]:
    return {
        "segment": "NSE_FO",
        "exchange": "NSE",
        "underlying_symbol": "NIFTY",
        "instrument_type": kind,
        "expiry": _expiry_ms(expiry),
        "strike_price": strike,
        "instrument_key": key,
        "lot_size": 65,
        "trading_symbol": f"NIFTY {int(strike)} {kind}",
        "weekly": True,
        **extra,
    }


def _records() -> list[dict[str, Any]]:
    return [
        {"segment": "NSE_INDEX", "instrument_type": "INDEX",
         "instrument_key": "NSE_INDEX|Nifty 50"},
        {**_option("NSE_FO|80001", date(2026, 10, 27), 0.0, "FUT")},
        _option("NSE_FO|80101", date(2026, 10, 27), 25000.0, "CE"),
        _option("NSE_FO|80102", date(2026, 10, 27), 25000.0, "PE"),
        _option("NSE_FO|80103", date(2026, 10, 27), 25050.0, "CE"),
        _option("NSE_FO|80104", date(2026, 10, 13), 22600.0, "CE"),
        _option("NSE_FO|80201", date(2026, 10, 27), 56000.0, "CE", underlying_symbol="BANKNIFTY"),
    ]  # fmt: skip


def _body(records: list[dict[str, Any]]) -> bytes:
    return gzip.compress(json.dumps(records).encode("utf-8"))


class Fetch:
    def __init__(self, body: bytes | None = None, error: Exception | None = None) -> None:
        self.body, self.error, self.calls = body, error, 0

    def __call__(self, url, headers, timeout):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.body


class Clock:
    def __init__(self, value: datetime = _FIRST) -> None:
        self.value, self.reads = value, 0

    def __call__(self) -> datetime:
        self.reads += 1
        return self.value


def _refusing_clock() -> datetime:
    raise AssertionError("this command must not read a clock")


@dataclass(frozen=True)
class Outcome:
    code: int
    out: str
    err: str


def _cli(argv: list[str], **kwargs) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    code = main(argv, env={}, stdout=out, stderr=err, **kwargs)
    return Outcome(code, out.getvalue(), err.getvalue())


def _sync(
    database: Path,
    fetch: Fetch,
    clock: Clock | None = None,
    *,
    product: str = "NIFTY",
    exchange: str = "NSE",
) -> Outcome:
    return _cli(
        ["options", "instruments", "sync", "--database", str(database),
         "--product", product, "--exchange", exchange],
        option_instruments_runtime=lambda path: build_option_instruments_runtime(path, fetch=fetch),
        clock=clock or Clock(),
    )  # fmt: skip


def _show(
    database: Path,
    *,
    expiration: str = "2026-10-27",
    strike: str = "25000",
    right: str = "CALL",
    product: str = "NIFTY",
    exchange: str = "NSE",
    **kwargs,
) -> Outcome:
    return _cli(
        ["options", "instruments", "show", "--database", str(database), "--product", product,
         "--exchange", exchange, "--expiration", expiration, "--strike", strike, "--right", right],
        clock=_refusing_clock,
        **kwargs,
    )  # fmt: skip


def _query(database: Path, sql: str) -> list[tuple]:
    with closing(sqlite3.connect(database)) as connection:
        return connection.execute(sql).fetchall()


def _tables(database: Path) -> set[str]:
    return {row[0] for row in _query(database, "SELECT name FROM sqlite_master WHERE type='table'")}


def _schema(database: Path) -> list[tuple]:
    return sorted(_query(database, "SELECT type, name, sql FROM sqlite_master"))


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------


def test_a_sync_stores_the_snapshot_and_reports_it_exactly(database: Path) -> None:
    body = _body(_records())
    clock = Clock()

    outcome = _sync(database, Fetch(body), clock)

    assert outcome.code == ExitCode.SUCCESS
    assert outcome.out.splitlines() == [
        "OPTION INSTRUMENTS: SYNCED",
        "Provider: upstox",
        "Product: NIFTY@NSE",
        f"Snapshot: {hashlib.sha256(body).hexdigest()}",
        "Master records: 7",
        "NIFTY option records: 4",
        "Reference persistence: completed",
    ]
    assert outcome.err == ""
    assert clock.reads == 1
    assert _tables(database) == _LISTING_TABLES
    assert _query(database, "SELECT first_fetched_at FROM option_listing_snapshots") == [
        ("2026-10-08T04:30:00Z",)
    ]
    assert len(_query(database, "SELECT * FROM option_provider_listings")) == 4


def test_a_repeated_identical_sync_is_idempotent(database: Path) -> None:
    body = _body(_records())
    _sync(database, Fetch(body), Clock(_FIRST))
    before = (
        _query(database, "SELECT * FROM option_listing_snapshots"),
        _query(database, "SELECT * FROM option_provider_listings ORDER BY 4, 5, 6"),
    )

    again = _sync(database, Fetch(body), Clock(_LATER))

    assert again.code == ExitCode.SUCCESS
    assert (
        _query(database, "SELECT * FROM option_listing_snapshots"),
        _query(database, "SELECT * FROM option_provider_listings ORDER BY 4, 5, 6"),
    ) == before


def test_the_clock_is_read_exactly_once_and_only_after_the_master_arrives(
    database: Path,
) -> None:
    fetch = Fetch(_body(_records()))
    seen: list[int] = []

    class Ordered(Clock):
        def __call__(self) -> datetime:
            seen.append(fetch.calls)
            return super().__call__()

    clock = Ordered()
    _sync(database, fetch, clock)

    assert clock.reads == 1
    assert seen == [1]


def test_a_naive_clock_instant_is_refused_and_stores_nothing(database: Path) -> None:
    outcome = _sync(database, Fetch(_body(_records())), Clock(datetime(2026, 10, 8, 10, 0)))

    assert outcome.code == ExitCode.INTERNAL
    assert outcome.out == ""
    assert not database.exists()


@pytest.mark.parametrize(
    ("product", "exchange"),
    [("BANKNIFTY", "NSE"), ("NIFTY", "BSE"), ("FINNIFTY", "NSE"), ("nifty 50", "NSE"),
     ("NIFTY", "")],
)  # fmt: skip
def test_an_unsupported_or_invalid_product_is_an_input_error_before_anything(
    database: Path, product: str, exchange: str
) -> None:
    fetch, clock = Fetch(_body(_records())), Clock()

    outcome = _sync(database, fetch, clock, product=product, exchange=exchange)

    assert outcome.code == ExitCode.INPUT
    assert outcome.err.startswith("INPUT ERROR: ")
    assert (fetch.calls, clock.reads) == (0, 0)
    assert list(database.parent.iterdir()) == []


@pytest.mark.parametrize(
    "error", [URLError("down"), TimeoutError(), OSError("reset")], ids=["url", "timeout", "os"]
)
def test_a_failed_master_fetch_is_a_provider_error_and_creates_no_database(
    database: Path, error: Exception
) -> None:
    clock = Clock()

    outcome = _sync(database, Fetch(error=error), clock)

    assert outcome.code == ExitCode.PROVIDER
    assert outcome.err.startswith("PROVIDER ERROR: UpstoxProviderUnavailableError")
    assert clock.reads == 0
    assert not database.exists()


@pytest.mark.parametrize(
    "records",
    [
        [_option("NSE_FO|1", date(2026, 10, 27), 25000.0),
         _option("NSE_FO|2", date(2026, 10, 27), 25000.0)],
        [_option("NSE_FO|1", date(2026, 10, 27), 25000.0, lot_size=0)],
        [_option("NSE_FO|1", date(2026, 10, 27), -1.0)],
        {"not": "an array"},
    ],
    ids=["ambiguous", "bad-lot", "bad-strike", "not-array"],
)  # fmt: skip
def test_a_malformed_or_ambiguous_master_is_a_provider_error(database: Path, records) -> None:
    outcome = _sync(database, Fetch(_body(records)))

    assert outcome.code == ExitCode.PROVIDER
    assert outcome.err.startswith("PROVIDER ERROR: UpstoxOptionMasterError")
    assert not database.exists()


def test_an_undecodable_master_is_a_provider_error(database: Path) -> None:
    outcome = _sync(database, Fetch(b"\x1f\x8bnot gzip"))

    assert outcome.code == ExitCode.PROVIDER
    assert not database.exists()


def test_a_contradicting_master_is_a_state_conflict_and_changes_nothing(database: Path) -> None:
    _sync(database, Fetch(_body(_records())))
    before = _schema(database), _query(database, "SELECT * FROM option_provider_listings")
    rekeyed = [
        _option("NSE_FO|99999", date(2026, 10, 27), 25000.0, "CE"),
        _option("NSE_FO|80999", date(2026, 11, 24), 25000.0, "CE"),
    ]

    outcome = _sync(database, Fetch(_body(rekeyed)), Clock(_LATER))

    assert outcome.code == ExitCode.STATE
    assert outcome.err.startswith("STATE ERROR: OptionListingConflictError")
    assert "NIFTY@NSE 2026-10-27 25000 CALL" in outcome.err
    assert (_schema(database), _query(database, "SELECT * FROM option_provider_listings")) == before
    assert len(_query(database, "SELECT * FROM option_listing_snapshots")) == 1


def test_a_sync_refuses_while_another_writer_holds_the_lock(database: Path) -> None:
    fetch, clock = Fetch(_body(_records())), Clock()

    with DatabaseOperationsLock(database):
        outcome = _sync(database, fetch, clock)

    assert outcome.code == ExitCode.STATE
    assert "Another Northstar operations writer is active" in outcome.err
    assert (fetch.calls, clock.reads) == (0, 0)
    assert not database.exists()


def test_a_sync_takes_the_operations_lock(database: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import northstar_api.cli as cli_module

    entered: list[Path] = []
    real = cli_module.DatabaseOperationsLock

    class Recording(real):
        def __enter__(self):
            entered.append(self.path)
            return super().__enter__()

    monkeypatch.setattr(cli_module, "DatabaseOperationsLock", Recording)

    assert _sync(database, Fetch(_body(_records()))).code == ExitCode.SUCCESS
    assert entered == [Path(f"{database.resolve()}.operations.lock")]


def test_a_database_in_a_missing_directory_is_a_configuration_error(tmp_path: Path) -> None:
    fetch = Fetch(_body(_records()))

    outcome = _sync(tmp_path / "missing" / "northstar.sqlite3", fetch)

    assert outcome.code == ExitCode.CONFIGURATION
    assert "Database directory does not exist" in outcome.err
    assert fetch.calls == 0


def test_a_directory_as_the_database_is_a_configuration_error(tmp_path: Path) -> None:
    (tmp_path / "db").mkdir()
    fetch = Fetch(_body(_records()))

    outcome = _sync(tmp_path / "db", fetch)

    assert outcome.code == ExitCode.CONFIGURATION
    assert "Database path is a directory" in outcome.err
    assert fetch.calls == 0


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------


def test_show_prints_the_exact_stored_mapping(database: Path) -> None:
    body = _body(_records())
    _sync(database, Fetch(body))

    outcome = _show(database, right="PUT")

    assert outcome.code == ExitCode.SUCCESS
    assert outcome.out.splitlines() == [
        "OPTION INSTRUMENT: READY",
        "Contract: NIFTY@NSE 2026-10-27 25000 PUT",
        "Provider: upstox",
        "Instrument key: NSE_FO|80102",
        "Exchange lot size: 65",
        f"Established snapshot: {hashlib.sha256(body).hexdigest()}",
        "Established at: 2026-10-08T04:30:00Z",
    ]
    assert outcome.err == ""


def test_show_of_a_missing_mapping_is_a_data_error(database: Path) -> None:
    _sync(database, Fetch(_body(_records())))

    outcome = _show(database, strike="25100")

    assert outcome.code == ExitCode.DATA
    assert outcome.err == (
        "DATA ERROR: Option instrument mapping not stored for NIFTY@NSE 2026-10-27 25100 CALL "
        "(provider upstox). Use 'northstar options instruments sync' while the contract is "
        "listed.\n"
    )
    assert outcome.out == ""


def test_show_on_a_missing_database_creates_nothing(database: Path) -> None:
    outcome = _show(database)

    assert outcome.code == ExitCode.DATA
    assert list(database.parent.iterdir()) == []


def test_show_on_a_futures_only_database_is_data_and_changes_no_schema(database: Path) -> None:
    build_database_runtime(database)
    before = _schema(database)

    outcome = _show(database)

    assert outcome.code == ExitCode.DATA
    assert _schema(database) == before
    assert _tables(database) == _FUTURES_TABLES


def test_show_needs_no_lock_and_creates_no_lock_file(database: Path) -> None:
    _sync(database, Fetch(_body(_records())))
    lock_file = Path(f"{database.resolve()}.operations.lock")

    with DatabaseOperationsLock(database):
        held = _show(database)

    assert held.code == ExitCode.SUCCESS
    lock_file.unlink()
    assert _show(database).code == ExitCode.SUCCESS
    assert not lock_file.exists()


def test_show_reads_no_clock_and_touches_no_network(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _sync(database, Fetch(_body(_records())))

    def refuse(*args, **kwargs):
        raise AssertionError("network access attempted")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)

    assert _show(database).code == ExitCode.SUCCESS  # _show injects a clock that refuses


def test_show_runs_no_ddl_or_initializer(database: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import northstar_infrastructure.market_data.sqlite_option_provider_listings as adapter

    _sync(database, Fetch(_body(_records())))
    before = _schema(database)

    def forbidden(*args, **kwargs):
        raise AssertionError("show must never initialize schema")

    monkeypatch.setattr(adapter, "initialize_option_listing_schema", forbidden)

    assert _show(database).code == ExitCode.SUCCESS
    assert _show(database, strike="1").code == ExitCode.DATA
    assert _schema(database) == before


def test_an_expired_listing_stays_showable_after_it_leaves_the_master(database: Path) -> None:
    _sync(database, Fetch(_body(_records())))
    later = [r for r in _records() if r.get("instrument_key") != "NSE_FO|80104"]
    assert _sync(database, Fetch(_body(later)), Clock(_LATER)).code == ExitCode.SUCCESS

    outcome = _show(database, expiration="2026-10-13", strike="22600")

    assert outcome.code == ExitCode.SUCCESS
    assert "Instrument key: NSE_FO|80104" in outcome.out
    assert "Established at: 2026-10-08T04:30:00Z" in outcome.out


@pytest.mark.parametrize(
    ("override", "label"),
    [
        ({"product": "BANKNIFTY"}, "Unsupported option product"),
        ({"right": "CE"}, "Invalid right"),
        ({"right": "call"}, "Invalid right"),
        ({"strike": "0"}, "Invalid strike"),
        ({"expiration": "2026-02-30"}, "Invalid expiration"),
    ],
)
def test_invalid_show_arguments_are_input_errors_and_create_nothing(
    database: Path, override: dict, label: str
) -> None:
    outcome = _show(database, **override)

    assert outcome.code == ExitCode.INPUT
    assert label in outcome.err
    assert list(database.parent.iterdir()) == []


def test_show_in_a_missing_directory_is_a_configuration_error(tmp_path: Path) -> None:
    outcome = _show(tmp_path / "missing" / "northstar.sqlite3")

    assert outcome.code == ExitCode.CONFIGURATION


# ---------------------------------------------------------------------------
# Help and regression
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["options", "--help"], ["economics", "instruments"]),
        (["options", "instruments", "--help"], ["sync", "show"]),
        (["options", "instruments", "sync", "--help"], ["--database", "--product", "--exchange"]),
        (
            ["options", "instruments", "show", "--help"],
            ["--database", "--product", "--exchange", "--expiration", "--strike", "--right"],
        ),
    ],
)
def test_help_documents_the_commands(argv: list[str], expected: list[str]) -> None:
    outcome = _cli(argv)

    assert outcome.code == ExitCode.SUCCESS
    for text in expected:
        assert text in outcome.out


def test_sync_takes_no_contract_or_token_arguments() -> None:
    help_text = _cli(["options", "instruments", "sync", "--help"]).out

    for option in ("--expiration", "--strike", "--right", "--token", "--archive-dir"):
        assert option not in help_text


def test_a_futures_only_database_stays_futures_only(database: Path) -> None:
    build_database_runtime(database)
    _cli(["economics", "set", "--database", str(database), "--product", "NIFTY",
          "--exchange", "NSE", "--expiration", "2026-10-27", "--point-value", "65",
          "--currency", "INR"])  # fmt: skip
    _show(database)

    assert _tables(database) == _FUTURES_TABLES


def test_economics_commands_are_unaffected_by_a_listing_sync(database: Path) -> None:
    _sync(database, Fetch(_body(_records())))
    contract = ["--database", str(database), "--product", "NIFTY", "--exchange", "NSE",
                "--expiration", "2026-10-27"]  # fmt: skip

    futures = _cli(["economics", "set", *contract, "--point-value", "65", "--currency", "INR"])
    options = _cli(["options", "economics", "set", *contract, "--strike", "25000", "--right",
                    "CALL", "--point-value", "65", "--currency", "INR"])  # fmt: skip

    assert (futures.code, options.code) == (ExitCode.SUCCESS, ExitCode.SUCCESS)
    assert options.out.splitlines()[0] == "OPTION ECONOMICS: READY"
    assert _tables(database) == _FUTURES_TABLES | _LISTING_TABLES | {"option_contract_economics"}
