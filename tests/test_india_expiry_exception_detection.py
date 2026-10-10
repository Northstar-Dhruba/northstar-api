"""M1.4.3.2: fail-closed detection of unresolved expiry exceptions (EXP-1, EXP-2).

``northstar operations daily`` (Upstox / NSE) reports ``STATUS: EXPIRY
EXCEPTION`` with exit 7 -- never ``ROLLOVER REQUIRED`` or ``WAITING`` -- when
the operated contract still holds a position or a pending order and either

* S-1: no trading session remains through expiry (fact-based, no clock); or
* S-2: the Asia/Kolkata date is strictly later than its expiration date.

Detection reads persisted facts through the existing paper snapshot and writes
nothing: no settlement, cancellation, fill, expiry or rollover. The dashboard
reports the S-1 stage ``expiry_exception``.

Everything runs through the production CLI and runtime on temporary SQLite,
with INDIA-7's synthetic markets and range-honouring Upstox transport double.
The clock is always injected, never the test machine's, and the network is
refused for the whole module.
"""

from __future__ import annotations

import socket
from dataclasses import replace
from datetime import UTC, date, datetime

import pytest
from northstar_infrastructure.market_data import upstox_http
from test_india7_nifty_incremental_operations_acceptance import (
    _CONTRACT,
    _E8_BAR,
    _SESSIONS,
    Operator,
    _close,
    _day,
    _expiry_operator,
)
from test_india8b_nse_chronological_operations import (
    _AFTER_THE_CONTRACT,
    _Clock,
    _daily,
    _never_read_the_clock,
    _operator,
    _requests_after,
)
from test_india8d_operational_status import _dashboard

from northstar_api import cli as cli_module
from northstar_api.cli import ExitCode
from northstar_api.operations import india_calendar_date, is_past_expiry

_E7, _E6 = _E8_BAR + 1, _E8_BAR + 2
_EXPIRY_BAR = len(_SESSIONS)
_THROUGH_EXPIRY = "2026-10-30"  # approves every session through the 2026-10-27 expiry

_BEFORE_EXPIRY = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
# 23:59 IST on the expiration date: still not past expiry.
_LAST_MINUTE_OF_EXPIRY = datetime(2026, 10, 27, 18, 29, tzinfo=UTC)
# 00:00 IST on 2026-10-28 -- while the UTC date is still 2026-10-27.
_FIRST_MINUTE_AFTER_EXPIRY = datetime(2026, 10, 27, 18, 30, tzinfo=UTC)


@pytest.fixture(scope="module", autouse=True)
def no_network():
    import databento

    def refuse(*args, **kwargs):
        raise AssertionError("expiry exception detection must not touch the network")

    # socket.socket.connect stays usable for the in-process ASGI dashboard reads.
    patcher = pytest.MonkeyPatch()
    patcher.setattr(socket, "create_connection", refuse)
    patcher.setattr(upstox_http, "urlopen", refuse)
    patcher.setattr(databento, "Historical", refuse)
    yield
    patcher.undo()


# ---------------------------------------------------------------------------
# Scenarios, built only through the production CLI
# ---------------------------------------------------------------------------


def _open_long(tmp_path, name: str) -> Operator:
    """LONG 1 filled at the E-7 open, nothing pending; E-6 is not yet approved."""
    op = _expiry_operator(tmp_path, name)
    op.cycle(_E8_BAR)
    op.cycle(_E7)
    return op


def _stranded_flatten(tmp_path, name: str) -> Operator:
    """EXP-2 via manual cutoffs: E-6 .. E-1 skipped, the flatten decided at E's close."""
    op = _open_long(tmp_path, name)
    assert op.sync(_E6, _EXPIRY_BAR).code == ExitCode.SUCCESS
    flatten = op.paper(_EXPIRY_BAR)
    assert flatten.code == ExitCode.SUCCESS, flatten.err
    assert flatten.section("ORDER")[:4] == [
        "State: PENDING", "Side: SELL", "Contracts: 1", "Expiry flatten: yes"
    ]  # fmt: skip
    return op


def _completed_flatten(tmp_path, name: str) -> Operator:
    op = _expiry_operator(tmp_path, name)
    for bar in range(_E8_BAR, _EXPIRY_BAR + 1):
        op.cycle(bar)
    return op


def _status_lines(out: str) -> list[str]:
    return [line for line in out.splitlines() if line.startswith("STATUS: ")]


def _assert_expiry_exception(run, *, position: str, pending: int) -> None:
    assert run.code == ExitCode.EXPIRY_EXCEPTION, run.err
    assert _status_lines(run.out) == ["STATUS: EXPIRY EXCEPTION"]
    assert "ROLLOVER REQUIRED" not in run.out and "WAITING" not in run.out
    assert f"Affected contract: {_CONTRACT}" in run.out
    assert "Expiration date: 2026-10-27" in run.out
    assert f"Position: {position}" in run.out
    assert f"Pending orders: {pending}" in run.out
    assert run.out.count(f"PENDING {_CONTRACT}:") == pending
    assert "Do not configure the next contract" in run.out
    assert run.err.splitlines()[-1].startswith("EXPIRY_EXCEPTION ERROR: Expiry exception:")
    assert "Configure the next contract explicitly" not in run.err


# ---------------------------------------------------------------------------
# V-1 .. V-5
# ---------------------------------------------------------------------------


def test_v1_post_expiry_waiting_with_an_open_position_is_an_expiry_exception(tmp_path) -> None:
    op = _open_long(tmp_path, "v1")
    before, requests = op.facts(), len(op.upstox.candle_requests)
    clock = _Clock(_AFTER_THE_CONTRACT)

    run = _daily(op, clock=clock, final_through=_E7)

    _assert_expiry_exception(run, position="OPEN LONG 1", pending=0)
    assert "is past its expiration date (India date 2026-11-02)" in run.out
    assert "Finality mode: operator-approved (final through" in run.out
    assert clock.reads == 1
    assert op.facts() == before
    assert _requests_after(op, requests) == [] and op.upstox.current_day_requests == 0


def test_v2_rollover_reached_with_a_stranded_flatten_is_an_expiry_exception(tmp_path) -> None:
    op = _stranded_flatten(tmp_path, "v2")
    before, requests = op.facts(), len(op.upstox.candle_requests)

    # S-1 is fact-based: the clock is never read.
    run = _daily(op, clock=_never_read_the_clock, final_through=_THROUGH_EXPIRY)

    _assert_expiry_exception(run, position="OPEN LONG 1", pending=1)
    assert "has no trading session left through expiry" in run.out
    assert op.facts() == before
    assert _requests_after(op, requests) == []
    # The read-only paper status agrees with the reported exception.
    status = op.status(_EXPIRY_BAR)
    assert "Pending: 1" in status.out and f"PENDING {_CONTRACT}: SELL 1" in status.out


def test_v3_a_completed_flatten_keeps_rollover_required(tmp_path) -> None:
    op = _completed_flatten(tmp_path, "v3")
    before = op.facts()

    run = _daily(op, clock=_never_read_the_clock, final_through=_THROUGH_EXPIRY)

    assert run.code == ExitCode.CONFIGURATION
    assert _status_lines(run.out) == ["STATUS: ROLLOVER REQUIRED"]
    assert "Configure the next contract explicitly" in run.err
    assert "EXPIRY EXCEPTION" not in run.out
    assert op.facts() == before


def test_v4_waiting_before_expiry_is_unchanged(tmp_path) -> None:
    op = _open_long(tmp_path, "v4")
    before = op.facts()
    clock = _Clock(_BEFORE_EXPIRY)

    run = _daily(op, clock=clock, final_through=_E7)

    assert run.code == ExitCode.SUCCESS, run.err
    assert _status_lines(run.out) == [
        f"STATUS: WAITING -- Session {_day(_E6)} is after the operator-approved final-through "
        f"date {_day(_E7)}; it has not been approved as final."
    ]
    assert clock.reads == 1  # read only because the contract is exposed
    assert op.facts() == before


def test_v5_a_daily_operated_flat_rollover_is_unchanged(tmp_path) -> None:
    op = _expiry_operator(tmp_path, "v5")
    first = _daily(
        op, clock=_Clock(_AFTER_THE_CONTRACT), final_through=_THROUGH_EXPIRY, go_live=_E8_BAR
    )
    assert first.code == ExitCode.SUCCESS, first.err
    before = op.facts()

    run = _daily(op, clock=_never_read_the_clock, final_through=_THROUGH_EXPIRY)

    assert run.code == ExitCode.CONFIGURATION
    assert _status_lines(run.out) == ["STATUS: ROLLOVER REQUIRED"]
    assert op.facts() == before


# ---------------------------------------------------------------------------
# S-1: every combination of position and pending order at rollover
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("keep_position", "keep_pending", "expected"),
    [
        (True, True, ExitCode.EXPIRY_EXCEPTION),
        (True, False, ExitCode.EXPIRY_EXCEPTION),
        (False, True, ExitCode.EXPIRY_EXCEPTION),
        (False, False, ExitCode.CONFIGURATION),
    ],
    ids=["open-and-pending", "open-no-pending", "flat-with-pending", "flat-nothing-pending"],
)
def test_rollover_requires_a_flat_contract_with_nothing_pending(
    tmp_path, monkeypatch: pytest.MonkeyPatch, keep_position, keep_pending, expected
) -> None:
    """The real stranded exposure, narrowed to each combination the rule distinguishes."""
    op = _stranded_flatten(tmp_path, f"s1-{keep_position}-{keep_pending}")
    before = op.facts()
    read = cli_module._contract_exposure

    def narrowed(runtime, settings, latest):
        exposure = read(runtime, settings, latest)
        assert exposure.position is not None and len(exposure.pending_orders) == 1
        return replace(
            exposure,
            position=exposure.position if keep_position else None,
            pending_orders=exposure.pending_orders if keep_pending else (),
        )

    monkeypatch.setattr(cli_module, "_contract_exposure", narrowed)
    run = _daily(op, clock=_never_read_the_clock, final_through=_THROUGH_EXPIRY)

    assert run.code == expected, run.err
    if expected is ExitCode.EXPIRY_EXCEPTION:
        _assert_expiry_exception(
            run, position="OPEN LONG 1" if keep_position else "Flat", pending=int(keep_pending)
        )
    else:
        assert _status_lines(run.out) == ["STATUS: ROLLOVER REQUIRED"]
    assert op.facts() == before


# ---------------------------------------------------------------------------
# S-2: the India calendar date against the expiration date
# ---------------------------------------------------------------------------


def test_a_flat_contract_with_a_pending_order_after_expiry_is_an_expiry_exception(
    tmp_path,
) -> None:
    op = _operator(tmp_path, "flat-pending")
    entry = _daily(op, final_through=22, go_live=21)  # HOLD at 21, BUY pending at 22
    assert entry.code == ExitCode.SUCCESS, entry.err
    before = op.facts()

    run = _daily(op, clock=_Clock(_AFTER_THE_CONTRACT), final_through=22)

    _assert_expiry_exception(run, position="Flat", pending=1)
    assert op.facts() == before


def test_no_position_and_no_pending_order_after_expiry_keeps_waiting_without_a_clock(
    tmp_path,
) -> None:
    op = _operator(tmp_path, "nothing-open")
    assert _daily(op, final_through=21, go_live=21).code == ExitCode.SUCCESS  # HOLD, flat
    before = op.facts()
    assert before["orders"] == ()

    run = _daily(op, clock=_never_read_the_clock, final_through=21)

    assert run.code == ExitCode.SUCCESS, run.err
    assert _status_lines(run.out)[0].startswith("STATUS: WAITING")
    assert op.facts() == before


@pytest.mark.parametrize(
    ("instant", "expected"),
    [
        (_LAST_MINUTE_OF_EXPIRY, ExitCode.SUCCESS),
        (_FIRST_MINUTE_AFTER_EXPIRY, ExitCode.EXPIRY_EXCEPTION),
    ],
    ids=["on-the-expiration-date", "one-india-day-after"],
)
def test_past_expiry_means_strictly_after_the_expiration_date_in_india(
    tmp_path, instant: datetime, expected: ExitCode
) -> None:
    op = _open_long(tmp_path, f"boundary-{expected.value}")
    before = op.facts()

    run = _daily(op, clock=_Clock(instant), final_through=_E7)

    assert run.code == expected, run.err
    if expected is ExitCode.SUCCESS:
        assert _status_lines(run.out)[0].startswith("STATUS: WAITING")
    else:
        assert "(India date 2026-10-28)" in run.out
    assert op.facts() == before


def test_the_india_calendar_rule() -> None:
    assert india_calendar_date(_LAST_MINUTE_OF_EXPIRY) == date(2026, 10, 27)
    assert india_calendar_date(_FIRST_MINUTE_AFTER_EXPIRY) == date(2026, 10, 28)
    assert not is_past_expiry(_CONTRACT, date(2026, 10, 27))
    assert is_past_expiry(_CONTRACT, date(2026, 10, 28))
    with pytest.raises(TypeError):
        india_calendar_date(datetime(2026, 10, 28, 0, 0))  # naive


def test_an_expired_instrument_cannot_hide_the_expiry_exception(tmp_path) -> None:
    op = _open_long(tmp_path, "expired-instrument")
    op.upstox.master_has_contract = False  # an expired contract is no longer listed
    before, requests = op.facts(), len(op.upstox.candle_requests)

    run = _daily(op, clock=_Clock(_AFTER_THE_CONTRACT), final_through=_THROUGH_EXPIRY)

    _assert_expiry_exception(run, position="OPEN LONG 1", pending=0)
    assert "PROVIDER ERROR" not in run.err
    assert op.facts() == before
    assert _requests_after(op, requests) == [] and op.upstox.current_day_requests == 0


def test_before_expiry_an_instrument_failure_is_still_a_provider_error(tmp_path) -> None:
    op = _open_long(tmp_path, "unlisted-before-expiry")
    op.upstox.master_has_contract = False
    before = op.facts()

    run = _daily(op, clock=_Clock(_BEFORE_EXPIRY), final_through=_THROUGH_EXPIRY)

    assert run.code == ExitCode.PROVIDER
    assert "UpstoxInstrumentResolutionError" in run.err
    assert "EXPIRY EXCEPTION" not in run.out
    assert op.facts() == before


# ---------------------------------------------------------------------------
# Exit codes and the dashboard
# ---------------------------------------------------------------------------


def test_the_expiry_exception_exit_code_is_distinct() -> None:
    codes = [code.value for code in ExitCode]
    assert len(codes) == len(set(codes))
    assert ExitCode.EXPIRY_EXCEPTION == 7
    assert ExitCode.EXPIRY_EXCEPTION not in (
        ExitCode.SUCCESS, ExitCode.CONFIGURATION, ExitCode.PROVIDER
    )  # fmt: skip


def test_the_dashboard_reports_an_expiry_exception_not_a_rollover(tmp_path) -> None:
    stranded = _dashboard(_stranded_flatten(tmp_path, "dash-stranded").database)["operations"]
    assert stranded["backlog"]["stage"] == "expiry_exception"
    expiry = stranded["expiry"]
    assert (expiry["position_flat"], expiry["pending_orders"]) == (False, "1")
    assert expiry["assessed_as_of"] == _close(_EXPIRY_BAR)

    flat = _dashboard(_completed_flatten(tmp_path, "dash-flat").database)["operations"]
    assert flat["backlog"]["stage"] == "rollover_required"
    assert (flat["expiry"]["position_flat"], flat["expiry"]["pending_orders"]) == (True, "0")
