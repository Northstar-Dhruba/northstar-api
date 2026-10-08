"""Tests for the operator option economics commands.

    northstar options economics set | show

Commands run against real temporary SQLite files through the production
Options runtime. Every value is parsed into its Core value before the database
is touched, the right is exactly CALL or PUT, and the futures database
initializer is never involved: a futures-only database never acquires the
option table, and an options economics command creates nothing but it.
"""

from __future__ import annotations

import io
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import pytest
from northstar_application.application_services import GetOptionContractEconomicsUseCase
from northstar_application.ports import OptionContractEconomicsRepository

from northstar_api.cli import ExitCode, main
from northstar_api.operations_lock import DatabaseOperationsLock
from northstar_api.runtime import (
    OptionEconomicsRuntime,
    build_database_runtime,
    build_option_economics_runtime,
    initialize_option_economics_database,
)

_FUTURES_TABLES = {
    "futures_ohlcv",
    "futures_forward_research_records",
    "futures_paper_orders",
    "futures_paper_fills",
    "futures_contract_economics",
}


@dataclass(frozen=True)
class Outcome:
    code: int
    out: str
    err: str


def _cli(argv: list[str], **kwargs) -> Outcome:
    out, err = io.StringIO(), io.StringIO()
    code = main(argv, env={}, stdout=out, stderr=err, **kwargs)
    return Outcome(code, out.getvalue(), err.getvalue())


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "northstar.sqlite3"


def _contract_args(
    database: Path,
    *,
    product: str = "NIFTY",
    exchange: str = "NSE",
    expiration: str = "2026-10-27",
    strike: str = "25000",
    right: str = "CALL",
) -> list[str]:
    return [
        "--database", str(database), "--product", product, "--exchange", exchange,
        "--expiration", expiration, "--strike", strike, "--right", right,
    ]  # fmt: skip


def _set(database: Path, point_value: str = "65", currency: str = "INR", **contract) -> Outcome:
    return _cli(
        ["options", "economics", "set", *_contract_args(database, **contract),
         "--point-value", point_value, "--currency", currency]
    )  # fmt: skip


def _show(database: Path, **contract) -> Outcome:
    return _cli(["options", "economics", "show", *_contract_args(database, **contract)])


def _tables(database: Path) -> set[str]:
    with closing(sqlite3.connect(database)) as connection:
        return {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }


def _assert_clean(outcome: Outcome) -> None:
    assert "Traceback" not in outcome.out + outcome.err


# ---------------------------------------------------------------------------
# set and show
# ---------------------------------------------------------------------------


def test_set_then_show(database: Path) -> None:
    stored = _set(database)
    shown = _show(database)

    assert stored.code == shown.code == ExitCode.SUCCESS
    assert stored.out.splitlines() == [
        "OPTION ECONOMICS: READY",
        "Contract: NIFTY@NSE 2026-10-27 25000 CALL",
        "Point value: 65 INR / premium-point / contract",
        "Stored, or already stored with identical values.",
    ]
    assert shown.out.splitlines() == [
        "Contract: NIFTY@NSE 2026-10-27 25000 CALL",
        "Point value: 65 INR / premium-point / contract",
    ]
    assert stored.err == shown.err == ""


def test_values_are_shown_in_canonical_form(database: Path) -> None:
    stored = _set(database, "65.000", "inr", strike="24950.50", right="PUT")

    assert stored.code == ExitCode.SUCCESS
    assert stored.out.splitlines()[1:3] == [
        "Contract: NIFTY@NSE 2026-10-27 24950.5 PUT",
        "Point value: 65 INR / premium-point / contract",
    ]
    assert _show(database, strike="24950.5", right="PUT").code == ExitCode.SUCCESS


def test_high_precision_point_values_are_kept_exactly(database: Path) -> None:
    precise = "65.123456789012345678901234567890123"

    outcome = _set(database, precise)

    assert f"Point value: {precise} INR / premium-point / contract" in outcome.out


def test_every_contract_keeps_its_own_point_value(database: Path) -> None:
    assert _set(database, "75", expiration="2025-12-30").code == ExitCode.SUCCESS
    assert _set(database, "65", expiration="2026-01-27").code == ExitCode.SUCCESS
    assert _set(database, "65", expiration="2026-01-27", right="PUT").code == ExitCode.SUCCESS

    assert "75 INR" in _show(database, expiration="2025-12-30").out
    assert "65 INR" in _show(database, expiration="2026-01-27").out
    assert "65 INR" in _show(database, expiration="2026-01-27", right="PUT").out


def test_an_equal_retry_succeeds_and_a_change_conflicts(database: Path) -> None:
    _set(database)

    retry = _set(database, "65.0")
    changed = _set(database, "75")
    recurrency = _set(database, "65", "USD")

    assert retry.code == ExitCode.SUCCESS
    for conflict in (changed, recurrency):
        assert conflict.code == ExitCode.STATE
        assert conflict.err.startswith("STATE ERROR: OptionContractEconomicsConflictError")
        assert "NIFTY@NSE 2026-10-27 25000 CALL" in conflict.err
        assert conflict.out == ""
    assert "65 INR" in _show(database).out


# ---------------------------------------------------------------------------
# show: fail closed, never a neighbour
# ---------------------------------------------------------------------------


def test_missing_economics_are_a_data_error(database: Path) -> None:
    _set(database, strike="25050")

    outcome = _show(database)

    assert outcome.code == ExitCode.DATA
    assert outcome.err == (
        "DATA ERROR: Option contract economics not configured for "
        "NIFTY@NSE 2026-10-27 25000 CALL. "
        "Use 'northstar options economics set' to configure them.\n"
    )
    assert outcome.out == ""


# ---------------------------------------------------------------------------
# show: genuinely read-only
# ---------------------------------------------------------------------------


def _schema(database: Path) -> list[tuple]:
    with closing(sqlite3.connect(database)) as connection:
        return sorted(connection.execute("SELECT type, name, sql FROM sqlite_master"))


def test_show_on_a_missing_database_file_is_data_and_creates_no_file(database: Path) -> None:
    outcome = _show(database)

    assert outcome.code == ExitCode.DATA
    assert outcome.err == (
        "DATA ERROR: Option contract economics not configured for "
        "NIFTY@NSE 2026-10-27 25000 CALL. "
        "Use 'northstar options economics set' to configure them.\n"
    )
    assert outcome.out == ""
    assert not database.exists()
    assert list(database.parent.iterdir()) == []


def test_show_on_a_futures_only_database_is_data_and_changes_no_schema(database: Path) -> None:
    build_database_runtime(database)
    _cli(
        ["economics", "set", "--database", str(database), "--product", "NIFTY",
         "--exchange", "NSE", "--expiration", "2026-10-27",
         "--point-value", "65", "--currency", "INR"]
    )  # fmt: skip
    schema_before = _schema(database)

    outcome = _show(database)

    assert outcome.code == ExitCode.DATA
    assert "Option contract economics not configured for" in outcome.err
    assert _tables(database) == _FUTURES_TABLES
    assert "option_contract_economics" not in _tables(database)
    assert _schema(database) == schema_before


def test_show_never_initializes_the_option_schema(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import northstar_infrastructure.persistence.sqlite_option_contract_economics as adapter

    import northstar_api.runtime as runtime

    _set(database)

    def forbidden(*args, **kwargs):
        raise AssertionError("show must never initialize schema")

    monkeypatch.setattr(runtime, "initialize_option_economics_database", forbidden)
    monkeypatch.setattr(runtime, "initialize_option_contract_economics_schema", forbidden)
    monkeypatch.setattr(adapter, "initialize_option_contract_economics_schema", forbidden)

    found = _show(database)
    missing = _show(database, right="PUT")

    assert found.code == ExitCode.SUCCESS
    assert "65 INR / premium-point / contract" in found.out
    assert missing.code == ExitCode.DATA


def test_show_needs_no_operations_lock(database: Path) -> None:
    _set(database)

    with DatabaseOperationsLock(database):
        outcome = _show(database)

    assert outcome.code == ExitCode.SUCCESS
    assert outcome.out.splitlines() == [
        "Contract: NIFTY@NSE 2026-10-27 25000 CALL",
        "Point value: 65 INR / premium-point / contract",
    ]


def test_show_creates_no_lock_file(database: Path) -> None:
    _show(database)

    assert list(database.parent.iterdir()) == []


def test_set_takes_the_lock_and_creates_the_option_table_lazily(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import northstar_api.cli as cli_module

    entered: list[Path] = []
    real_lock = cli_module.DatabaseOperationsLock

    class RecordingLock(real_lock):
        def __enter__(self):
            entered.append(self.path)
            assert not database.exists() or "option_contract_economics" not in _tables(database)
            return super().__enter__()

    monkeypatch.setattr(cli_module, "DatabaseOperationsLock", RecordingLock)

    outcome = _set(database)

    assert outcome.code == ExitCode.SUCCESS
    assert entered == [Path(f"{database.resolve()}.operations.lock")]
    assert _tables(database) == {"option_contract_economics"}
    assert _show(database).code == ExitCode.SUCCESS


@pytest.mark.parametrize(
    "neighbour",
    [
        {"strike": "25050"},
        {"right": "PUT"},
        {"expiration": "2026-10-20"},
        {"product": "BANKNIFTY"},
        {"exchange": "BSE"},
    ],
    ids=["strike", "right", "expiry", "product", "exchange"],
)
def test_a_neighbouring_contract_never_answers(database: Path, neighbour: dict) -> None:
    _set(database)

    outcome = _show(database, **neighbour)

    assert outcome.code == ExitCode.DATA
    assert "Option contract economics not configured" in outcome.err


def test_futures_economics_never_answer_for_an_option(database: Path) -> None:
    futures = _cli(
        ["economics", "set", "--database", str(database), "--product", "NIFTY",
         "--exchange", "NSE", "--expiration", "2026-10-27",
         "--point-value", "65", "--currency", "INR"]
    )  # fmt: skip

    assert futures.code == ExitCode.SUCCESS
    assert _show(database).code == ExitCode.DATA


# ---------------------------------------------------------------------------
# Input: parsed before the database is touched
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("strike", ["0", "-25000", "abc", "NaN", "Infinity", "", "25,000"])
def test_an_invalid_strike_is_an_input_error(database: Path, strike: str) -> None:
    for outcome in (_set(database, strike=strike), _show(database, strike=strike)):
        assert outcome.code == ExitCode.INPUT
        assert outcome.err.startswith("INPUT ERROR: Invalid strike ")
        _assert_clean(outcome)
    assert not database.exists()


@pytest.mark.parametrize("right", ["CE", "PE", "call", "put", "Call", "C", "", " CALL", "BOTH"])
def test_an_invalid_right_is_an_input_error(database: Path, right: str) -> None:
    for outcome in (_set(database, right=right), _show(database, right=right)):
        assert outcome.code == ExitCode.INPUT
        assert outcome.err.startswith("INPUT ERROR: Invalid right (exactly CALL or PUT) ")
        _assert_clean(outcome)
    assert not database.exists()


@pytest.mark.parametrize("point_value", ["0", "-65", "abc", "NaN", "Infinity", "", "0.000"])
def test_an_invalid_point_value_is_an_input_error(database: Path, point_value: str) -> None:
    outcome = _set(database, point_value)

    assert outcome.code == ExitCode.INPUT
    assert outcome.err.startswith("INPUT ERROR: Invalid point value ")
    assert not database.exists()


@pytest.mark.parametrize("currency", ["IN", "", "INR1", "₹", "RUPEES"])
def test_an_invalid_currency_is_an_input_error(database: Path, currency: str) -> None:
    outcome = _set(database, currency=currency)

    assert outcome.code == ExitCode.INPUT
    assert outcome.err.startswith("INPUT ERROR: Invalid currency ")
    assert not database.exists()


@pytest.mark.parametrize("expiration", ["2026-02-30", "20261027", "27-10-2026", "", "2026-10"])
def test_an_invalid_expiration_is_an_input_error(database: Path, expiration: str) -> None:
    for outcome in (_set(database, expiration=expiration), _show(database, expiration=expiration)):
        assert outcome.code == ExitCode.INPUT
        assert outcome.err.startswith("INPUT ERROR: Invalid expiration ")
    assert not database.exists()


@pytest.mark.parametrize(
    "missing", ["--database", "--product", "--exchange", "--expiration", "--strike", "--right"]
)
def test_every_contract_argument_is_required(database: Path, missing: str) -> None:
    argv = ["options", "economics", "set", *_contract_args(database)]
    argv += ["--point-value", "65", "--currency", "INR"]
    index = argv.index(missing)
    del argv[index : index + 2]

    outcome = _cli(argv)

    assert outcome.code == ExitCode.INPUT
    assert missing in outcome.err
    assert not database.exists()


# ---------------------------------------------------------------------------
# Configuration and the operations lock
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("command", ["set", "show"])
def test_a_database_in_a_missing_directory_is_a_configuration_error(
    tmp_path: Path, command: str
) -> None:
    database = tmp_path / "missing" / "northstar.sqlite3"

    outcome = _set(database) if command == "set" else _show(database)

    assert outcome.code == ExitCode.CONFIGURATION
    assert "Database directory does not exist" in outcome.err
    assert not (tmp_path / "missing").exists()


def test_a_directory_as_the_database_is_a_configuration_error(tmp_path: Path) -> None:
    outcome = _show(tmp_path)

    assert outcome.code == ExitCode.CONFIGURATION
    assert "Database path is a directory" in outcome.err


def test_set_refuses_while_another_writer_holds_the_lock(database: Path) -> None:
    with DatabaseOperationsLock(database):
        outcome = _set(database)

    assert outcome.code == ExitCode.STATE
    assert "Another Northstar operations writer is active" in outcome.err
    assert not database.exists()
    assert _set(database).code == ExitCode.SUCCESS


def test_a_foreign_repository_answer_is_a_state_error(database: Path) -> None:
    class Foreign(OptionContractEconomicsRepository):
        def get_economics(self, contract):
            return "65 INR"

    outcome = _cli(
        ["options", "economics", "show", *_contract_args(database)],
        option_economics_lookup=lambda path: GetOptionContractEconomicsUseCase(Foreign()),
    )

    assert outcome.code == ExitCode.STATE
    assert outcome.err.startswith("STATE ERROR: OptionContractEconomicsContractViolationError")


def test_corrupt_storage_is_a_state_error(database: Path) -> None:
    _set(database)
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("UPDATE option_contract_economics SET point_value_amount = '0'")

    outcome = _show(database)

    assert outcome.code == ExitCode.STATE
    assert outcome.err.startswith("STATE ERROR: OptionContractEconomicsStorageError")
    _assert_clean(outcome)


def test_a_store_that_misreports_its_count_is_a_state_error(database: Path) -> None:
    def runtime(path: Path) -> OptionEconomicsRuntime:
        real = build_option_economics_runtime(path)

        class Miscounting(type(real.store)):
            def store(self, economics):
                super().store(economics)
                return True

        return OptionEconomicsRuntime(Miscounting(path), real.repository, real.lookup)

    outcome = _cli(
        ["options", "economics", "set", *_contract_args(database),
         "--point-value", "65", "--currency", "INR"],
        option_economics_runtime=runtime,
    )  # fmt: skip

    assert outcome.code == ExitCode.STATE
    assert "Option economics store accepted True values, expected 1." in outcome.err


# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------


def test_top_level_help_lists_the_options_group() -> None:
    outcome = _cli(["--help"])

    assert outcome.code == ExitCode.SUCCESS
    assert "options" in outcome.out


@pytest.mark.parametrize(
    ("argv", "expected"),
    [
        (["options", "--help"], ["economics"]),
        (["options", "economics", "--help"], ["set", "show"]),
        (
            ["options", "economics", "set", "--help"],
            ["--database", "--product", "--exchange", "--expiration", "--strike", "--right",
             "--point-value", "--currency", "CALL", "PUT"],
        ),
        (
            ["options", "economics", "show", "--help"],
            ["--database", "--product", "--exchange", "--expiration", "--strike", "--right"],
        ),
    ],
)  # fmt: skip
def test_help_documents_required_arguments(argv: list[str], expected: list[str]) -> None:
    outcome = _cli(argv)

    assert outcome.code == ExitCode.SUCCESS
    for text in expected:
        assert text in outcome.out


def test_show_takes_no_point_value_or_currency() -> None:
    help_text = _cli(["options", "economics", "show", "--help"]).out

    assert "--point-value" not in help_text
    assert "--currency" not in help_text


def test_there_is_no_flat_option_economics_command() -> None:
    outcome = _cli(["option-economics", "set", "--help"])

    assert outcome.code == ExitCode.INPUT


# ---------------------------------------------------------------------------
# Database separation and futures regression
# ---------------------------------------------------------------------------


def test_only_set_creates_and_it_creates_only_the_option_table(database: Path) -> None:
    _show(database)
    assert not database.exists()

    _set(database)
    assert _tables(database) == {"option_contract_economics"}


def test_a_futures_only_database_never_contains_the_option_table(database: Path) -> None:
    build_database_runtime(database)
    futures = _cli(
        ["economics", "set", "--database", str(database), "--product", "ES",
         "--exchange", "CME", "--expiration", "2026-12-18",
         "--point-value", "50", "--currency", "USD"]
    )  # fmt: skip
    _cli(
        ["economics", "show", "--database", str(database), "--product", "ES",
         "--exchange", "CME", "--expiration", "2026-12-18"]
    )  # fmt: skip

    assert futures.code == ExitCode.SUCCESS
    assert _tables(database) == _FUTURES_TABLES


def test_options_economics_join_an_existing_futures_database_without_touching_it(
    database: Path,
) -> None:
    _cli(
        ["economics", "set", "--database", str(database), "--product", "NIFTY",
         "--exchange", "NSE", "--expiration", "2026-10-27",
         "--point-value", "65", "--currency", "INR"]
    )  # fmt: skip
    with closing(sqlite3.connect(database)) as connection:
        futures_rows = connection.execute("SELECT * FROM futures_contract_economics").fetchall()

    assert _set(database).code == ExitCode.SUCCESS

    assert _tables(database) == _FUTURES_TABLES | {"option_contract_economics"}
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT * FROM futures_contract_economics").fetchall() == (
            futures_rows
        )
    futures_show = _cli(
        ["economics", "show", "--database", str(database), "--product", "NIFTY",
         "--exchange", "NSE", "--expiration", "2026-10-27"]
    )  # fmt: skip
    assert futures_show.out.splitlines() == [
        "Contract: NIFTY@NSE 2026-10-27",
        "Point value: 65 INR / quote-point / contract",
    ]


def test_the_option_initializer_creates_only_its_own_table(database: Path) -> None:
    initialize_option_economics_database(database)
    initialize_option_economics_database(database)

    assert _tables(database) == {"option_contract_economics"}
