"""INDIA-8G-A2: ``northstar finality-evidence report``.

Synthetic evidence only, written through A1's own observation model so every
line is exactly what ``observe`` records. The report must read nothing but the
file and the NSE calendar: no token, clock, database or network.
"""

from __future__ import annotations

import io
import socket
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, Symbol
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_infrastructure.market_data import (
    UpstoxDailyCandleEvidence,
    UpstoxDailyCandleEvidenceObservation,
    upstox_http,
)

from northstar_api import finality_evidence
from northstar_api.cli import ExitCode, main
from northstar_api.finality_evidence import EvidenceFilters, build_report

_OCT, _NOV = "2026-10-27", "2026-11-23"
_MON = date(2026, 10, 5)  # 15:40 regime: close 10:10Z
_TUE = date(2026, 10, 6)
_JUL = date(2026, 7, 31)  # 15:30 regime: close 10:00Z
_MUHURAT = date(2026, 11, 8)
_FORBIDDEN = (
    "final", "stable", "safe", "settled", "enough evidence", "recommend",
    "threshold should", "token valid",
)  # fmt: skip


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("the evidence report must not touch the network")

    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(upstox_http, "urlopen", refuse)


def _contract(expiration: str = _OCT, product: str = "NIFTY") -> FuturesContract:
    return FuturesContract(
        FuturesProductReference(Symbol(product), ExchangeCode("NSE")), ExpirationDate(expiration)
    )


def _at(day: date, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=UTC)


def _line(
    requested: datetime,
    *,
    day: date = _MON,
    expiration: str = _OCT,
    product: str = "NIFTY",
    candle: bool = True,
    close: str = "25180.5",
    open_: str = "25100",
    volume: int = 4200 * 65,
    interest: str | None = "13500000",
    lot: int = 65,
    stamp: str | None = None,
    key: str = "NSE_FO|48704",
) -> str:
    evidence = (
        UpstoxDailyCandleEvidence(
            provider_timestamp=stamp or f"{day.isoformat()}T00:00:00+05:30",
            open=Decimal(open_),
            high=Decimal("25250"),
            low=Decimal("25020"),
            close=Decimal(close),
            volume=Decimal(volume),
            open_interest=None if interest is None else Decimal(interest),
        )
        if candle
        else None
    )
    contracts, remainder = divmod(volume, lot)
    return UpstoxDailyCandleEvidenceObservation(
        requested_at=requested,
        received_at=requested + timedelta(milliseconds=400),
        contract=_contract(expiration, product),
        trading_date=day,
        instrument_key=key,
        lot_size=lot,
        candle=evidence,
        volume_contracts=None if evidence is None or remainder else contracts,
        volume_contracts_note=(
            None if evidence is None or not remainder else f"raw volume {volume} is not whole"
        ),
    ).to_json_line()


def _write(path: Path, *lines: str) -> Path:
    path.write_text("".join(lines), encoding="utf-8")
    return path


def _report(path: Path, *filters: str):
    out, err = io.StringIO(), io.StringIO()

    def forbidden(*args, **kwargs):
        raise AssertionError("the report must not use a clock, database or provider")

    code = main(
        ["finality-evidence", "report", "--evidence", str(path), *filters],
        env=TrackingEnv({}),
        stdout=out,
        stderr=err,
        clock=forbidden,
        database_runtime=forbidden,
        upstox_candle_evidence=forbidden,
        upstox_market_sync_runtime=forbidden,
    )
    return code, out.getvalue(), err.getvalue()


class TrackingEnv(dict):
    def __init__(self, values: dict) -> None:
        super().__init__(values)
        self.read: list[str] = []

    def get(self, key, default=None):
        self.read.append(key)
        return super().get(key, default)


def _session(*lines: str):
    import json

    (session,) = build_report([json.loads(line) for line in lines]).sessions
    return session


def _fields(*lines: str) -> list[tuple[str, ...]]:
    return [change.changed_fields for change in _session(*lines).changes]


def _rendered(text: str) -> str:
    return "\n".join(line for line in text.splitlines() if not line.startswith("Evidence file:"))


# ---------------------------------------------------------------------------
# A-D. Presence and repeats
# ---------------------------------------------------------------------------


def test_one_observation_with_a_candle() -> None:
    session = _session(_line(_at(_MON, 11)))

    assert len(session.observations) == 1 and session.changes == ()
    assert session.first_candle_observation is session.observations[0]
    assert not session.candle_appeared_after_absence
    assert session.unchanged_since_last_change == 0
    assert session.last_observed == _at(_MON, 11)


def test_one_observation_without_a_candle() -> None:
    session = _session(_line(_at(_MON, 10, 30), candle=False))

    assert session.first_candle_observation is None
    assert session.latest.candle is None and session.changes == ()


def test_a_candle_appearing_is_an_observed_change() -> None:
    session = _session(_line(_at(_MON, 10, 15), candle=False), _line(_at(_MON, 10, 45)))

    assert [c.changed_fields for c in session.changes] == [("candle_presence",)]
    assert session.candle_appeared_after_absence
    assert session.first_candle_observation.requested_at == _at(_MON, 10, 45)


def test_identical_repeats_are_unchanged_observations() -> None:
    session = _session(*(_line(_at(_MON, hour)) for hour in (11, 13, 16)))

    assert session.changes == () and session.unchanged_since_last_change == 2
    assert session.observation_span == timedelta(hours=5)


# ---------------------------------------------------------------------------
# E-K. Changed fields
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("change", "fields"),
    [
        ({"close": "25190"}, ("close",)),
        ({"volume": 4300 * 65}, ("volume", "volume_contracts")),
        ({"interest": "13600000"}, ("open_interest",)),
        ({"stamp": "2026-10-05T00:00:00+05:30 "}, ("provider_timestamp",)),
        ({"interest": None}, ("open_interest",)),
    ],
    ids=["close-only", "volume-only", "open-interest-only", "provider-timestamp", "oi-missing"],
)
def test_one_field_changes(change: dict, fields: tuple[str, ...]) -> None:
    assert _fields(_line(_at(_MON, 11)), _line(_at(_MON, 13), **change)) == [fields]


def test_a_lot_size_change_is_not_a_raw_volume_change() -> None:
    raw = 65 * 75  # whole contracts at both lot sizes
    fields = _fields(
        _line(_at(_MON, 11), volume=raw, lot=65), _line(_at(_MON, 13), volume=raw, lot=75)
    )

    assert fields == [("lot_size", "volume_contracts")]


def test_several_fields_change_at_once() -> None:
    fields = _fields(
        _line(_at(_MON, 11)),
        _line(_at(_MON, 13), close="25200", volume=4400 * 65, interest="13700000"),
    )

    assert fields == [("close", "volume", "open_interest", "volume_contracts")]


def test_multiple_observed_changes_with_their_elapsed_times() -> None:
    session = _session(
        _line(_at(_MON, 10, 30), candle=False),
        _line(_at(_MON, 11)),
        _line(_at(_MON, 12)),
        _line(_at(_MON, 14), close="25190"),
        _line(_at(_MON, 18), close="25190", interest="13600000"),
        _line(_at(_MON, 22), close="25190", interest="13600000"),
    )

    assert [(c.observation.requested_at, c.changed_fields) for c in session.changes] == [
        (_at(_MON, 11), ("candle_presence",)),
        (_at(_MON, 14), ("close",)),
        (_at(_MON, 18), ("open_interest",)),
    ]
    assert [c.elapsed_from_close for c in session.changes] == [
        timedelta(minutes=50),
        timedelta(hours=3, minutes=50),
        timedelta(hours=7, minutes=50),
    ]
    assert session.unchanged_since_last_change == 1


def test_numbers_compare_by_exact_decimal_value() -> None:
    same = _session(_line(_at(_MON, 11), close="25010.50"), _line(_at(_MON, 12), close="25010.5"))
    moved = _session(_line(_at(_MON, 11), close="25010.50"), _line(_at(_MON, 12), close="25010.51"))

    assert same.changes == ()
    assert same.latest.candle.text["close"] == "25010.5"  # recorded text is what is shown
    assert [c.changed_fields for c in moved.changes] == [("close",)]


def test_an_instrument_key_change_does_not_split_the_session() -> None:
    session = _session(_line(_at(_MON, 11)), _line(_at(_MON, 12), key="NSE_FO|99999"))

    assert len(session.observations) == 2 and session.changes == ()
    assert session.instrument_keys == ("NSE_FO|48704", "NSE_FO|99999")


# ---------------------------------------------------------------------------
# L-N. Sessions, contracts and order
# ---------------------------------------------------------------------------


def test_sessions_and_contracts_are_grouped_and_aggregated(tmp_path: Path) -> None:
    import json

    lines = [
        _line(_at(_MON, 11)),
        _line(_at(_MON, 15), close="25190"),
        _line(_at(_TUE, 11), day=_TUE),
        _line(_at(_TUE, 13), day=_TUE),
        _line(_at(_MON, 11), expiration=_NOV, key="NSE_FO|61471"),
        _line(_at(_MON, 10, 20), expiration=_NOV, key="NSE_FO|61471", candle=False),
        _line(_at(_MON, 12), expiration=_NOV, key="NSE_FO|61471", interest="1"),
    ]
    report = build_report([json.loads(line) for line in lines])

    keys = [(s.key.expiration, s.key.trading_date) for s in report.sessions]
    assert keys == [(_OCT, _MON), (_OCT, _TUE), (_NOV, _MON)]
    aggregate = report.aggregate
    assert (aggregate.session_count, aggregate.observation_count) == (3, 7)
    assert aggregate.sessions_with_observed_change == 2
    assert aggregate.sessions_without_observed_change == 1
    assert aggregate.sessions_with_candle_after_absence == 1
    assert aggregate.observations_per_session_minimum == 2
    assert aggregate.observations_per_session_median == Decimal(2)
    assert aggregate.observations_per_session_maximum == 3
    delays = aggregate.last_observed_change_delays
    assert delays.count == 2
    assert (delays.minimum, delays.maximum) == (
        timedelta(hours=1, minutes=50),
        timedelta(hours=4, minutes=50),
    )
    assert delays.median == timedelta(hours=3, minutes=20)


def test_observations_are_ordered_by_time_not_file_order() -> None:
    session = _session(
        _line(_at(_MON, 16), close="25190"), _line(_at(_MON, 11)), _line(_at(_MON, 13))
    )

    assert [o.requested_at for o in session.observations] == [
        _at(_MON, 11),
        _at(_MON, 13),
        _at(_MON, 16),
    ]
    assert [c.observation.requested_at for c in session.changes] == [_at(_MON, 16)]


# ---------------------------------------------------------------------------
# P-T. Timing from the session close
# ---------------------------------------------------------------------------


def test_elapsed_time_from_the_current_close_regime() -> None:
    session = _session(_line(_at(_MON, 12, 10)))

    assert session.session_close == _at(_MON, 10, 10)  # 15:40 IST
    assert session.elapsed_from_close == (timedelta(hours=2),)


def test_the_historical_close_regime_is_preserved() -> None:
    session = _session(_line(_at(_JUL, 11), day=_JUL, expiration="2026-08-25"))

    assert session.session_close == _at(_JUL, 10, 0)  # 15:30 IST before 2026-08-03
    assert session.elapsed_from_close == (timedelta(hours=1),)


def test_an_observation_before_the_close_is_kept_as_negative(tmp_path: Path) -> None:
    path = _write(tmp_path / "e.jsonl", _line(_at(_MON, 9, 55), candle=False), _line(_at(_MON, 11)))

    code, out, _ = _report(path)

    assert code == ExitCode.SUCCESS
    assert "First observed: 2026-10-05T09:55:00Z (-0h 15m 00s from close)" in out
    assert _session(_line(_at(_MON, 9, 55))).elapsed_from_close == (-timedelta(minutes=15),)


def test_an_unresolved_close_stays_unavailable(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "e.jsonl",
        _line(_at(_MUHURAT, 14), day=_MUHURAT, expiration=_NOV),
        _line(_at(_MUHURAT, 16), day=_MUHURAT, expiration=_NOV, close="25300"),
    )

    code, out, _ = _report(path)

    assert code == ExitCode.SUCCESS
    session = _session(
        _line(_at(_MUHURAT, 14), day=_MUHURAT, expiration=_NOV),
        _line(_at(_MUHURAT, 16), day=_MUHURAT, expiration=_NOV, close="25300"),
    )
    assert session.session_close is None and session.close_unavailable_reason
    assert session.elapsed_from_close == (None, None)
    assert session.changes[0].elapsed_from_close is None
    assert "Session close: unavailable (" in out
    assert "(close unavailable)" in out
    assert "Sessions with session close unavailable: 1" in out
    assert "Last observed change from close: none" in out


# ---------------------------------------------------------------------------
# U. Right-censoring and rendering
# ---------------------------------------------------------------------------


def test_every_session_is_right_censored_at_its_last_observation(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "e.jsonl",
        _line(_at(_MON, 11)),
        _line(_at(_MON, 14), close="25190"),
        _line(_at(_MON, 20), close="25190"),
        _line(_at(_TUE, 11), day=_TUE),
    )

    code, out, _ = _report(path)

    assert code == ExitCode.SUCCESS
    assert out.count("Right-censored at:") == 2
    assert "Right-censored at: 2026-10-05T20:00:00Z;" in out
    assert "Last observed change: 2026-10-05T14:00:00Z (+3h 50m 00s from close)" in out
    assert "Unchanged observations since: 1" in out
    assert "Last observed change: none (no observed change)" in out
    assert "  2026-10-05T14:00:00Z (+3h 50m 00s from close): close" in out
    assert "  Close: 25190" in out


def test_the_output_carries_no_verdict(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "e.jsonl",
        _line(_at(_MON, 10, 15), candle=False),
        _line(_at(_MON, 11)),
        *(_line(_at(_MON, hour), close="25190") for hour in (14, 16, 18, 20, 22)),
    )

    _, out, _ = _report(path)

    rendered = _rendered(out).lower()
    for phrase in _FORBIDDEN:
        assert phrase not in rendered, phrase


def test_the_same_file_gives_the_same_report(tmp_path: Path) -> None:
    path = _write(tmp_path / "e.jsonl", _line(_at(_MON, 11)), _line(_at(_MON, 14), close="1"))
    before = path.read_bytes()

    first, second = _report(path), _report(path)

    assert first == second and first[0] == ExitCode.SUCCESS
    assert path.read_bytes() == before


# ---------------------------------------------------------------------------
# V-X. Evidence errors and filters
# ---------------------------------------------------------------------------


def test_a_malformed_middle_line_is_state(tmp_path: Path) -> None:
    path = _write(tmp_path / "e.jsonl", _line(_at(_MON, 11)), "{oops\n", _line(_at(_MON, 12)))

    code, out, err = _report(path)

    assert code == ExitCode.STATE and out == ""
    assert "MalformedUpstoxCandleEvidenceError" in err and "line 2" in err


def test_a_record_with_invalid_fields_is_state(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "e.jsonl",
        _line(_at(_MON, 11)),
        _line(_at(_MON, 12)).replace('"close": "25180.5"', '"close": 25180.5'),
    )

    code, _, err = _report(path)

    assert code == ExitCode.STATE
    assert "line 2 has no exact numeric text for close" in err


def test_a_truncated_final_line_is_state(tmp_path: Path) -> None:
    path = _write(tmp_path / "e.jsonl", _line(_at(_MON, 11)), '{"schema": "northstar.up')

    code, out, err = _report(path)

    assert code == ExitCode.STATE and out == ""
    assert "TruncatedUpstoxCandleEvidenceError" in err


def test_a_missing_evidence_file_is_configuration(tmp_path: Path) -> None:
    code, _, err = _report(tmp_path / "absent.jsonl")

    assert code == ExitCode.CONFIGURATION
    assert "Evidence file not found" in err


def test_filters_select_exact_sessions(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "e.jsonl",
        _line(_at(_MON, 11)),
        _line(_at(_TUE, 11), day=_TUE),
        _line(_at(_MON, 11), expiration=_NOV, key="NSE_FO|61471"),
    )

    _, by_expiration, _ = _report(path, "--expiration", _NOV)
    _, by_date, _ = _report(path, "--trading-date", "2026-10-06", "--product", "NIFTY")
    code, none, _ = _report(path, "--product", "BANKNIFTY")

    assert by_expiration.count("\nSESSION ") == 1 and "NIFTY@NSE 2026-11-23" in by_expiration
    assert "Filters: expiration=2026-11-23" in by_expiration
    assert by_date.count("\nSESSION ") == 1 and "trading date 2026-10-06" in by_date
    assert code == ExitCode.SUCCESS and "No observations match." in none
    assert "Sessions observed: 0" in none


@pytest.mark.parametrize(
    "arguments",
    [("--trading-date", "2026-13-01"), ("--expiration", "soon"), ("--product", "NIF TY")],
    ids=["date", "expiration", "product"],
)
def test_invalid_filters_are_input(tmp_path: Path, arguments: tuple[str, str]) -> None:
    path = _write(tmp_path / "e.jsonl", _line(_at(_MON, 11)))

    code, _, _ = _report(path, *arguments)

    assert code == ExitCode.INPUT


# ---------------------------------------------------------------------------
# Z. Offline and read-only
# ---------------------------------------------------------------------------


def test_the_report_reads_no_environment_clock_database_or_provider(tmp_path: Path) -> None:
    path = _write(tmp_path / "e.jsonl", _line(_at(_MON, 11)))
    env = TrackingEnv({"UPSTOX_ANALYTICS_TOKEN": "x", "NORTHSTAR_DATABASE": "y"})

    def forbidden(*args, **kwargs):
        raise AssertionError("not allowed")

    code = main(
        ["finality-evidence", "report", "--evidence", str(path)],
        env=env,
        stdout=io.StringIO(),
        stderr=io.StringIO(),
        clock=forbidden,
        database_runtime=forbidden,
        upstox_candle_evidence=forbidden,
    )

    assert code == ExitCode.SUCCESS
    assert env.read == []
    assert sorted(p.name for p in tmp_path.iterdir()) == ["e.jsonl"]


def test_the_report_module_reads_no_clock_and_no_canonical_storage() -> None:
    import ast

    source = Path(finality_evidence.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    attributes = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    assert not {"now", "utcnow", "today"} & attributes
    assert "_utc_now" not in names
    imported = {
        node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module
    }
    assert not any("persistence" in name or "runtime" in name for name in imported)
    assert "northstar_api.operations_lock" not in imported
    assert "sqlite3" not in source


def test_the_structured_report_needs_no_rendering() -> None:
    import json

    report = build_report(
        [json.loads(_line(_at(_MON, 11))), json.loads(_line(_at(_TUE, 11), day=_TUE))],
        EvidenceFilters(trading_date=_TUE),
    )

    (session,) = report.sessions
    assert session.key.trading_date == _TUE
    assert report.aggregate.session_count == 1
