"""M1.4.4.2: independent reconciliation of futures paper P&L on synthetic data.

The expected values come from ``independent_pnl_verifier``, which recomputes
positions and gross P&L from the persisted facts with the standard library
only. Northstar's reported values come from ``pnl_reporting_adapter``, through
its CLI, dashboard and analysis surfaces. The two are compared field by field
and every comparison is classified:

- EXACT: equal as exact rationals;
- PROVISIONAL_PRECISION: within the provisional 1e-15 threshold, and only for
  a contract whose exact average entry has no finite decimal expansion, where
  Northstar's 28-digit context must round (U-3, not an approved policy);
- MISMATCH: anything else -- never tolerated;
- UNAVAILABLE: both sides report the value as unavailable;
- NOT_APPLICABLE: both sides agree the value does not exist (a flat contract has
  no average entry and no mark);
- NOT_EXPOSED: the surface does not report the field (never a pass).

Every database is a disposable synthetic SQLite file: orders, fills, bars and
economics are written through Northstar's own insert-only stores, or produced
by the production CLI with INDIA-7's fake Upstox. The network is refused and
no clock is read. Each verification is bracketed by byte, logical-dump and
sidecar snapshots of the database, which must not change.

Set NORTHSTAR_RECONCILIATION_EVIDENCE to a directory to write the comparison
evidence as JSON; by default nothing is written.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import independent_pnl_verifier as independent
import pnl_reporting_adapter as northstar
import pytest
from northstar_core.derivatives import QuoteValue
from northstar_core.foundation.value_objects import (
    Currency,
    PointInTime,
    Quantity,
    Timeframe,
)
from northstar_core.futures import (
    FuturesContractEconomics,
    FuturesOHLCVBar,
    FuturesPointValue,
)
from northstar_core.paper_trading import (
    FuturesContractCount,
    FuturesExecutionIntent,
    FuturesPaperFill,
    FuturesPaperOrder,
    OrderSide,
    PaperFillIdentity,
    PaperOrderIdentity,
)
from northstar_infrastructure.market_data import (
    SQLiteFuturesHistoricalMarketDataStore,
    upstox_http,
)
from northstar_infrastructure.persistence import (
    SQLiteFuturesContractEconomicsStore,
    SQLiteFuturesPaperFillStore,
    SQLiteFuturesPaperOrderStore,
)
from test_india7_nifty_incremental_operations_acceptance import (
    _E8_BAR,
    _PORTFOLIO,
    _STRATEGY,
    _close,
)
from test_india8b_nse_chronological_operations import _daily, _operator
from test_india_expiry_exception_detection import _open_long

from northstar_api.runtime import build_database_runtime

THRESHOLD = Fraction(1, 10**15)  # provisional (U-3); never an approved accounting policy
OCT = "NIFTY@NSE 2026-10-27"
NOV = "NIFTY@NSE 2026-11-24"
POINT_VALUE = {OCT: "65", NOV: "75"}  # NOV at 75 proves economics are per contract
INR = Currency("INR")
VERIFIER = Path(independent.__file__)
RECORDS: list[dict] = []


@pytest.fixture(scope="module", autouse=True)
def no_network():
    import databento

    def refuse(*args, **kwargs):
        raise AssertionError("M1.4.4.2 must not touch the network")

    patcher = pytest.MonkeyPatch()
    patcher.setattr(socket, "create_connection", refuse)
    patcher.setattr(upstox_http, "urlopen", refuse)
    patcher.setattr(databento, "Historical", refuse)
    yield
    patcher.undo()


@pytest.fixture(scope="module", autouse=True)
def evidence():
    yield
    target = os.environ.get("NORTHSTAR_RECONCILIATION_EVIDENCE")
    if target:
        Path(target).mkdir(parents=True, exist_ok=True)
        payload = {
            "verifier": VERIFIER.name,
            "verifier_sha256": hashlib.sha256(VERIFIER.read_bytes()).hexdigest(),
            "threshold_inr": str(THRESHOLD),
            "records": RECORDS,
        }
        (Path(target) / "reconciliation-results.json").write_text(
            json.dumps(payload, indent=2, default=str), encoding="utf-8", newline="\n"
        )


def _day(n: int) -> str:
    return f"2026-10-{n:02d}T10:00:00Z"


# ---------------------------------------------------------------------------
# Synthetic databases through Northstar's own insert-only stores
# ---------------------------------------------------------------------------


class Book:
    """A disposable synthetic database written only through the production stores."""

    def __init__(self, path: Path) -> None:
        self.path = path
        build_database_runtime(path)  # creates the schema exactly as production does
        self._bars: set[tuple[str, int]] = set()

    def economics(self, contract: str, point_value: str | None = None) -> None:
        value = FuturesPointValue(Decimal(point_value or POINT_VALUE[contract]), INR)
        SQLiteFuturesContractEconomicsStore(self.path).store(
            (FuturesContractEconomics(northstar.contract_of(contract), value),)
        )

    def bar(self, contract: str, day: int, open_: str, close: str) -> None:
        if (contract, day) in self._bars:
            return
        self._bars.add((contract, day))
        o, c = Decimal(open_), Decimal(close)
        SQLiteFuturesHistoricalMarketDataStore(self.path).store(
            (
                FuturesOHLCVBar(
                    northstar.contract_of(contract),
                    PointInTime(_day(day)),
                    Timeframe("1d"),
                    QuoteValue(o),
                    QuoteValue(max(o, c) + 5),
                    QuoteValue(min(o, c) - 5),
                    QuoteValue(c),
                    Quantity(1000),
                ),
            )
        )

    def trade(
        self,
        contract: str,
        side: str,
        count: int,
        decided: int,
        filled: int | None = None,
        quote: str | None = None,
        identity: str | None = None,
    ) -> None:
        intent = FuturesExecutionIntent(
            portfolio_identity=_PORTFOLIO,
            contract=northstar.contract_of(contract),
            side=OrderSide(side),
            contracts=FuturesContractCount(count),
            strategy_identity=_STRATEGY,
            decided_at=PointInTime(_day(decided)),
        )
        key = identity or hashlib.sha256(f"{contract}{side}{count}{decided}".encode()).hexdigest()
        order = FuturesPaperOrder(PaperOrderIdentity(key), intent)
        SQLiteFuturesPaperOrderStore(self.path).store((order,))
        if filled is None:
            return
        self.bar(contract, filled, quote, quote)
        SQLiteFuturesPaperFillStore(self.path).store(
            (
                FuturesPaperFill(
                    PaperFillIdentity("fill-" + key),
                    order.identity,
                    intent,
                    FuturesContractCount(count),
                    QuoteValue(Decimal(quote)),
                    PointInTime(_day(filled)),
                ),
            )
        )


# ---------------------------------------------------------------------------
# Database preservation
# ---------------------------------------------------------------------------


def _sidecars(database: Path) -> list[tuple[str, int]]:
    return sorted(
        (p.name, p.stat().st_size)
        for suffix in ("-wal", "-shm", "-journal")
        if (p := database.with_name(database.name + suffix)).exists()
    )


def logical(database: Path) -> str:
    uri = f"{database.resolve().as_uri()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        return hashlib.sha256("\n".join(connection.iterdump()).encode()).hexdigest()


def preserved(database: Path) -> dict:
    return {
        "file_sha256": hashlib.sha256(database.read_bytes()).hexdigest(),
        "logical_sha256": logical(database),
        "sidecars": _sidecars(database),
    }


def verify(database: Path, cutoff: str) -> tuple[independent.Verification, dict]:
    """Run the verifier between two preservation snapshots, which must be identical."""
    before = preserved(database)
    result = independent.verify(database, _PORTFOLIO.identity, _STRATEGY.identity, cutoff)
    after = preserved(database)
    assert after == before, "the independent verifier changed the database"
    return result, before


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


@dataclass
class Comparison:
    scenario: str
    surface: str
    contract: str
    field: str
    expected: str
    reported: str
    difference: str
    outcome: str


def _exact(value) -> str:
    if value is None:
        return "unavailable"
    if isinstance(value, Fraction):
        decimal = f"{Decimal(value.numerator) / Decimal(value.denominator):.30f}".rstrip("0")
        return f"{value.numerator}/{value.denominator} (~{decimal.rstrip('.')})"
    return str(value)


def classify(
    expected, reported, *, precision_eligible: bool, kind: str = "amount", applicable: bool = True
):
    """Return (outcome, difference); tolerance only ever applies to rounding of a basis."""
    if reported == northstar.NOT_EXPOSED:
        return "NOT_EXPOSED", "-"
    if expected is None and reported is None:
        return ("UNAVAILABLE" if applicable else "NOT_APPLICABLE"), "-"
    if expected is None or reported is None:
        return "MISMATCH", "one side unavailable"
    if kind == "instant":
        same = independent.parse_instant(reported, "reported") == expected
        return ("EXACT", "0") if same else ("MISMATCH", "different instant")
    if kind == "int":
        return ("EXACT", "0") if int(reported) == expected else ("MISMATCH", "different count")
    difference = abs(Fraction(str(reported)) - expected)
    if difference == 0:
        return "EXACT", "0"
    if precision_eligible and difference <= THRESHOLD:
        return "PROVISIONAL_PRECISION", f"{float(difference):.3g}"
    return "MISMATCH", f"{float(difference):.3g}"


def compare(
    scenario: str, result: independent.Verification, surfaces, before: dict, record: bool = True
) -> list:
    rows: list[Comparison] = []

    def add(surface, contract, name, expected, reported, eligible=False, kind="amount", flat=False):
        outcome, difference = classify(
            expected, reported, precision_eligible=eligible, kind=kind, applicable=not flat
        )
        rows.append(
            Comparison(
                scenario,
                surface,
                contract,
                name,
                _exact(expected),
                str(reported),
                difference,
                outcome,
            )
        )

    for surface in surfaces:
        if surface.cutoff not in (northstar.NOT_EXPOSED, None):
            add(surface.name, "-", "cutoff", result.cutoff_instant, surface.cutoff, kind="instant")
        add(
            surface.name,
            "-",
            "pending_orders",
            len(result.pending_orders),
            surface.pending_orders,
            kind="int",
        )
        add(surface.name, "-", "fills", result.fills_visible, surface.fills, kind="int")
        reported_contracts = set(surface.contracts)
        for row in result.contracts:
            key = str(row.contract)
            rep = surface.contracts.get(key)
            if rep is None:
                if surface.name == "analysis" or surface.status != "available":
                    continue  # the analysis covers one selected contract only
                rows.append(
                    Comparison(
                        scenario, surface.name, key, "row", "present", "absent", "-", "MISMATCH"
                    )
                )
                continue
            reported_contracts.discard(key)
            eligible = row.non_terminating_basis
            mark, flat = row.mark, row.net_contracts == 0
            add(
                surface.name, key, "net_contracts", row.net_contracts, rep.net_contracts, kind="int"
            )
            add(
                surface.name,
                key,
                "average_entry",
                row.average_entry,
                rep.average_entry,
                eligible,
                flat=flat,
            )
            add(surface.name, key, "realized", row.realized, rep.realized, eligible)
            add(surface.name, key, "unrealized", row.unrealized, rep.unrealized, eligible)
            add(surface.name, key, "total", row.total, rep.total, eligible)
            add(
                surface.name,
                key,
                "mark_quote",
                mark.close if mark else None,
                rep.mark_quote,
                flat=flat,
            )
            add(
                surface.name,
                key,
                "mark_instant",
                mark.instant if mark else None,
                rep.mark_instant,
                kind="instant",
                flat=flat,
            )
        for extra in sorted(reported_contracts):
            rep = surface.contracts[extra]
            if surface.name == "analysis":
                # The analysis always reports its selected contract. With no visible fill the
                # verifier has no row: the only correct report is flat with zero P&L.
                add(
                    surface.name,
                    extra,
                    "net_contracts (no visible fill)",
                    0,
                    rep.net_contracts,
                    kind="int",
                )
                for name in ("realized", "unrealized", "total"):
                    add(
                        surface.name,
                        extra,
                        f"{name} (no visible fill)",
                        Fraction(0),
                        getattr(rep, name),
                    )
                continue
            rows.append(
                Comparison(
                    scenario, surface.name, extra, "row", "absent", "present", "-", "MISMATCH"
                )
            )
    if not record:
        return rows
    RECORDS.append(
        {
            "scenario": scenario,
            "cutoff": result.cutoff,
            "database_before_verification": before,
            "verifier": _serialise(result),
            "surfaces": {s.name: s.status for s in surfaces},
            "comparisons": [asdict(row) for row in rows],
        }
    )
    return rows


def _serialise(result: independent.Verification) -> dict:
    return {
        "cutoff": result.cutoff,
        "fills_visible": result.fills_visible,
        "pending_orders": [
            asdict(p) | {"contract": str(p.contract)} for p in result.pending_orders
        ],
        "findings": list(result.findings),
        "net_pnl": result.net_pnl,
        "contracts": [
            {
                "contract": str(row.contract),
                "currency": row.currency,
                "point_value": _exact(row.point_value),
                "net_contracts": row.net_contracts,
                "average_entry": _exact(row.average_entry),
                "realized": _exact(row.realized),
                "unrealized": _exact(row.unrealized),
                "unrealized_status": row.unrealized_status,
                "total": _exact(row.total),
                "mark": None
                if row.mark is None
                else {
                    "close": row.mark.close_text,
                    "point_in_time": row.mark.point_in_time,
                    "source": row.mark.source,
                    "approval": row.mark.approval,
                },
                "partial_closes": row.partial_closes,
                "reversals": row.reversals,
                "non_terminating_basis": row.non_terminating_basis,
            }
            for row in result.contracts
        ],
    }


def reconcile(scenario: str, database: Path, cutoff: str, selected: str = OCT):
    """Verify, read every Northstar surface at the same cutoff, compare; nothing may change."""
    result, before = verify(database, cutoff)
    contracts = [str(row.contract) for row in result.contracts] or [selected]
    surfaces = [
        northstar.paper_status(database, cutoff),
        northstar.dashboard(database, selected, cutoff),
    ]
    surfaces += [northstar.analysis(database, contract, cutoff) for contract in contracts]
    point_values = northstar.Surface("cli economics show", "available")
    for row in result.contracts:
        point_values.contracts[str(row.contract)] = northstar.Reported(
            point_value=northstar.economics_show(database, str(row.contract))
        )
    rows = compare(scenario, result, surfaces, before)
    for row in result.contracts:
        outcome, difference = classify(
            row.point_value,
            point_values.contracts[str(row.contract)].point_value,
            precision_eligible=False,
        )
        rows.append(
            Comparison(
                scenario,
                point_values.name,
                str(row.contract),
                "point_value",
                _exact(row.point_value),
                str(point_values.contracts[str(row.contract)].point_value),
                difference,
                outcome,
            )
        )
    RECORDS[-1]["comparisons"] += [asdict(r) for r in rows if r.surface == point_values.name]
    assert logical(database) == before["logical_sha256"], "a Northstar read changed the database"
    mismatches = [r for r in rows if r.outcome == "MISMATCH"]
    assert not mismatches, "\n".join(map(str, mismatches))
    return result, surfaces, rows


# ---------------------------------------------------------------------------
# PR-1 .. PR-16 (section 7 of the reconciliation document)
# ---------------------------------------------------------------------------

# id: (fills (contract, side, count, decided day, fill day, quote), marks {contract: (day, close)},
#      expected {contract: (net, average, realized, unrealized, total)})
SCENARIOS = {
    "PR-1": (
        [(OCT, "BUY", 1, 12, 13, "25000")],
        {OCT: (19, "25100")},
        {OCT: (1, "25000", "0", "6500", "6500")},
    ),
    "PR-2": (
        [(OCT, "SELL", 1, 12, 13, "25000")],
        {OCT: (19, "24900")},
        {OCT: (-1, "25000", "0", "6500", "6500")},
    ),
    "PR-3": (
        [(OCT, "BUY", 1, 12, 13, "25000")],
        {OCT: (19, "24900")},
        {OCT: (1, "25000", "0", "-6500", "-6500")},
    ),
    "PR-4": (
        [(OCT, "SELL", 1, 12, 13, "25000")],
        {OCT: (19, "25100")},
        {OCT: (-1, "25000", "0", "-6500", "-6500")},
    ),
    "PR-5": (
        [(OCT, "BUY", 1, 12, 13, "25000"), (OCT, "SELL", 1, 13, 14, "25200")],
        {OCT: (19, "25300")},
        {OCT: (0, None, "13000", "0", "13000")},
    ),
    "PR-6": (
        [(OCT, "SELL", 1, 12, 13, "25000"), (OCT, "BUY", 1, 13, 14, "25200")],
        {OCT: (19, "25300")},
        {OCT: (0, None, "-13000", "0", "-13000")},
    ),
    "PR-7": (
        [(OCT, "BUY", 2, 12, 13, "25000"), (OCT, "SELL", 1, 13, 14, "25100")],
        {OCT: (19, "25050")},
        {OCT: (1, "25000", "6500", "3250", "9750")},
    ),
    "PR-8": (
        [
            (OCT, "BUY", 1, 12, 13, "25000"),
            (OCT, "BUY", 2, 13, 14, "25300"),
            (OCT, "SELL", 1, 14, 15, "25500"),
        ],
        {OCT: (19, "25100")},
        {OCT: (2, "25200", "19500", "-13000", "6500")},
    ),
    "PR-11": (
        [(OCT, "BUY", 1, 12, 13, "25000"), (OCT, "SELL", 2, 13, 14, "25200")],
        {OCT: (19, "25100")},
        {OCT: (-1, "25200", "13000", "6500", "19500")},
    ),
    "PR-12": (
        [(OCT, "BUY", 1, 12, 13, "25000"), (NOV, "SELL", 1, 12, 13, "25300")],
        {OCT: (19, "25100"), NOV: (19, "25200")},
        {OCT: (1, "25000", "0", "6500", "6500"), NOV: (-1, "25300", "0", "7500", "7500")},
    ),
    "PR-13": (
        [(OCT, "BUY", 1, 12, 13, "25000"), (OCT, "BUY", 2, 13, 14, "25001")],
        {OCT: (19, "25001")},
        {OCT: (3, "75002/3", "0", "65", "65")},
    ),
}
CUTOFF = _day(19)


def build(tmp_path: Path, name: str, fills, marks, *, economics: bool = True) -> Path:
    book = Book(tmp_path / f"{name}.sqlite3")
    for contract, side, count, decided, filled, quote in fills:
        if economics and contract in POINT_VALUE:
            book.economics(contract)
        book.trade(contract, side, count, decided, filled, quote)
    for contract, (day, close) in marks.items():
        book.bar(contract, day, close, close)
    return book.path


def _assert_expected(result: independent.Verification, expected: dict) -> None:
    assert {str(row.contract) for row in result.contracts} == set(expected)
    for contract, (net, average, realized, unrealized, total) in expected.items():
        row = result.contract(independent.ContractKey(*_split(contract)))
        assert row.net_contracts == net
        assert row.average_entry == (Fraction(average) if average else None)
        assert (row.realized, row.unrealized, row.total) == tuple(
            map(Fraction, (realized, unrealized, total))
        )
        assert row.mark is None if net == 0 else row.mark.approval == "UNKNOWN"


def _split(contract: str) -> tuple[str, str, str]:
    product, rest = contract.split("@")
    return (product, *rest.split(" "))


@pytest.mark.parametrize("scenario", sorted(SCENARIOS, key=lambda s: int(s[3:])))
def test_scenario_reconciles_with_every_northstar_surface(tmp_path: Path, scenario: str) -> None:
    fills, marks, expected = SCENARIOS[scenario]
    database = build(tmp_path, scenario, fills, marks)

    result, surfaces, rows = reconcile(scenario, database, CUTOFF)

    _assert_expected(result, expected)
    assert result.pending_orders == () and result.findings == ()
    outcomes = {row.outcome for row in rows}
    if scenario == "PR-13":
        precision = [r for r in rows if r.outcome == "PROVISIONAL_PRECISION"]
        assert {r.field for r in precision} == {"average_entry", "unrealized", "total"}
        assert all(
            Fraction(1, 10**22) < Fraction(r.difference) < THRESHOLD
            for r in precision
            if r.field != "average_entry"
        )
    else:
        assert outcomes <= {"EXACT", "NOT_APPLICABLE", "NOT_EXPOSED"}, outcomes
    assert all(s.status == "available" for s in surfaces), [s.status for s in surfaces]


def test_pr7_and_pr11_detect_partial_closes_and_reversals(tmp_path: Path) -> None:
    partial, _ = verify(build(tmp_path, "partial", *SCENARIOS["PR-7"][:2]), CUTOFF)
    reversal, _ = verify(build(tmp_path, "reversal", *SCENARIOS["PR-11"][:2]), CUTOFF)
    (p,), (r,) = partial.contracts, reversal.contracts
    assert (p.partial_closes, p.reversals) == (1, 0)
    assert (r.partial_closes, r.reversals) == (0, 1)


def test_pr13_records_the_exact_value_northstars_decimal_and_the_difference(tmp_path) -> None:
    database = build(tmp_path, "pr13-precision", *SCENARIOS["PR-13"][:2])
    result, _ = verify(database, CUTOFF)
    status = northstar.paper_status(database, CUTOFF)

    (row,) = result.contracts
    reported = status.contracts[OCT].unrealized
    assert row.unrealized == 65 and row.average_entry == Fraction(75002, 3)
    assert reported == "64.99999999999999999999935"
    assert abs(Fraction(reported) - row.unrealized) == Fraction(65, 10**23)
    # The same difference on a terminating basis is a mismatch, not precision.
    assert classify(Fraction(65), reported, precision_eligible=False)[0] == "MISMATCH"
    assert classify(Fraction(65), "64.99", precision_eligible=True)[0] == "MISMATCH"


@pytest.mark.parametrize(
    "error",
    [
        "point value 66 not 65",
        "quantity 3 not 2",
        "mark 25150 not 25100",
        "cutoff day 15 not 19",
        "realized 2e-15 INR off on a repeating basis",
    ],
)
def test_the_threshold_never_hides_a_real_error(tmp_path: Path, error: str) -> None:
    """Doctor the *expected* side; every surface must then disagree, tolerance or not."""
    scenario = "PR-13" if "repeating" in error else "PR-8"
    database = build(tmp_path, "doctored", *SCENARIOS[scenario][:2])
    result, before = verify(database, CUTOFF)
    surfaces = [
        northstar.paper_status(database, CUTOFF),
        northstar.dashboard(database, OCT, CUTOFF),
    ]
    (row,) = result.contracts
    if error.startswith("point value"):
        row = replace(row, realized=row.realized * 66 / 65, unrealized=row.unrealized * 66 / 65)
    elif error.startswith("quantity"):
        row = replace(row, net_contracts=3)
    elif error.startswith("mark"):
        row = replace(row, mark=replace(row.mark, close=Fraction(25150)))
    elif error.startswith("cutoff"):
        result = replace(result, cutoff_instant=independent.parse_instant(_day(15), "cutoff"))
    else:
        row = replace(row, realized=row.realized + Fraction(2, 10**15))
    result = replace(result, contracts=(row,))

    rows = compare("doctored", result, surfaces, before, record=False)

    assert any(r.outcome == "MISMATCH" for r in rows), error
    assert not any(
        r.outcome == "PROVISIONAL_PRECISION"
        and r.field in ("net_contracts", "mark_quote", "cutoff")
        for r in rows
    )


def test_pr9_a_pending_order_is_not_an_execution(tmp_path: Path) -> None:
    book = Book(tmp_path / "pr9.sqlite3")
    book.economics(OCT)
    book.bar(OCT, 13, "25000", "25000")
    book.trade(OCT, "BUY", 1, 13)  # decided, never filled

    result, surfaces, _ = reconcile("PR-9", book.path, CUTOFF)

    assert result.contracts == () and result.fills_visible == 0
    assert [p.side for p in result.pending_orders] == ["BUY"]
    assert surfaces[0].pending_orders == 1 and surfaces[0].contracts == {}


def test_pr10_a_flat_empty_portfolio(tmp_path: Path) -> None:
    book = Book(tmp_path / "pr10.sqlite3")
    result, surfaces, _ = reconcile("PR-10", book.path, CUTOFF)
    assert result.contracts == () and result.pending_orders == ()
    assert "No filled contracts" in surfaces[0].raw
    # An empty database has no contract economics, so the analysis withholds its paper part
    # rather than reporting a zero: unavailable, never a pass.
    assert surfaces[2].status == "unavailable"
    assert surfaces[2].raw["paper"]["reason"] == "contract economics not configured"


def test_pr14_gross_only_fees_and_slippage_are_not_applicable(tmp_path: Path) -> None:
    database = build(tmp_path, "pr14", *SCENARIOS["PR-1"][:2])
    result, surfaces, _ = reconcile("PR-14", database, CUTOFF)
    assert result.net_pnl.startswith("NOT_APPLICABLE")
    assert "P&L (gross simulated)" in surfaces[0].raw
    row = surfaces[1].raw["pnl"]["rows"][0]
    assert not any(word in key for key in row for word in ("fee", "net", "slippage", "tax"))


def test_pr15_an_expired_open_contract_is_marked_at_a_stale_close(tmp_path: Path) -> None:
    op = _open_long(tmp_path, "pr15")  # fake Upstox: LONG 1 filled at the E-7 open
    after_expiry = "2026-10-27T18:30:00Z"  # 00:00 IST on 2026-10-28

    result, _, rows = reconcile("PR-15", op.database, after_expiry)

    (row,) = result.contracts
    assert row.net_contracts == 1 and row.unrealized_status == "AVAILABLE"
    assert row.mark.point_in_time == _close(_E8_BAR + 1)  # E-7, 2026-10-15
    assert row.mark.approval == "UNKNOWN"
    assert (result.cutoff_instant - row.mark.instant).days >= 12  # stale, and reported as such
    assert any(r.field == "mark_instant" and r.outcome == "EXACT" for r in rows)


def test_pr16_missing_economics_is_an_error_on_both_sides(tmp_path: Path) -> None:
    database = build(tmp_path, "pr16-economics", *SCENARIOS["PR-1"][:2], economics=False)
    before = preserved(database)

    with pytest.raises(independent.MissingEconomics) as raised:
        independent.verify(database, _PORTFOLIO.identity, _STRATEGY.identity, CUTOFF)

    assert str(raised.value.contract) == OCT and preserved(database) == before
    cli = northstar.paper_status(database, CUTOFF)
    dash = northstar.dashboard(database, OCT, CUTOFF)
    assert cli.status.startswith("exit 4; P&L unavailable")
    # The position is still known and reported; only its P&L is withheld.
    position = cli.contracts[OCT]
    assert (position.net_contracts, position.average_entry) == (1, "25000")
    assert position.realized == position.unrealized == northstar.NOT_EXPOSED
    assert dash.raw["pnl"]["status"] == "unavailable" and dash.raw["pnl"]["missing_contract"]
    RECORDS.append(
        {
            "scenario": "PR-16a missing economics",
            "verifier": "MissingEconomics",
            "surfaces": {"cli paper status": cli.status, "dashboard": dash.status},
            "outcome": "UNAVAILABLE on both sides",
        }
    )


def test_pr16_an_open_position_without_a_mark_is_unavailable_never_zero(tmp_path: Path) -> None:
    book = Book(tmp_path / "pr16-mark.sqlite3")
    book.economics(OCT)
    book.trade(OCT, "BUY", 1, 12, 13, "25000")
    with closing(sqlite3.connect(book.path)) as connection:  # the fill's own bar, removed
        connection.execute("DELETE FROM futures_ohlcv")  # synthetic copy only: no mark exists
        connection.commit()

    result, _, rows = reconcile("PR-16b", book.path, CUTOFF)

    (row,) = result.contracts
    assert row.unrealized is None and row.total is None
    assert row.unrealized_status == "UNAVAILABLE_NO_MARK"
    assert any("no stored close" in finding for finding in result.findings)
    assert {r.outcome for r in rows if r.field == "unrealized"} <= {"UNAVAILABLE", "NOT_EXPOSED"}


# ---------------------------------------------------------------------------
# End to end: the production CLI with INDIA-7's fake Upstox
# ---------------------------------------------------------------------------


def test_end_to_end_pending_then_reversal_through_the_production_cli(tmp_path: Path) -> None:
    op = _operator(tmp_path, "e2e")  # bootstrap bars 1..20
    assert _daily(op, final_through=26, go_live=21).code == 0  # HOLD BUY BUY SELL HOLD HOLD

    pending, _, _ = reconcile("E2E PR-9 (cutoff bar 22)", op.database, _close(22))
    assert pending.contracts == () and [p.side for p in pending.pending_orders] == ["BUY"]

    result, _, _ = reconcile("E2E PR-11 (cutoff bar 26)", op.database, _close(26))
    (row,) = result.contracts
    # Hand values: BUY 1 at the bar-23 open 25103; SELL 2 at the bar-25 open 24003;
    # SHORT 1 from 24003 marked at the bar-26 close 24000; 65 INR per point.
    assert (row.net_contracts, row.average_entry, row.reversals) == (-1, 24003, 1)
    assert row.realized == (24003 - 25103) * 65 == -71500
    assert row.unrealized == (24000 - 24003) * -1 * 65 == 195
    assert result.findings == ()


# ---------------------------------------------------------------------------
# Chronology
# ---------------------------------------------------------------------------


def test_equal_fill_instants_follow_order_identity(tmp_path: Path) -> None:
    book = Book(tmp_path / "equal.sqlite3")
    book.economics(OCT)
    book.trade(OCT, "BUY", 1, 12, 13, "25000", identity="order-0")
    # Both decided before the day-15 bar (no day-14 bar exists), so both fill at its open.
    book.trade(OCT, "SELL", 1, 13, 15, "25100", identity="order-a")
    book.trade(OCT, "BUY", 1, 14, 15, "25100", identity="order-b")
    book.bar(OCT, 19, "25200", "25200")

    result, _, _ = reconcile("C-1 equal instants", book.path, CUTOFF)

    (row,) = result.contracts
    # Canonical: SELL (order-a) closes the long (+100 points), then BUY reopens at 25100.
    assert (row.net_contracts, row.average_entry, row.realized) == (1, 25100, 6500)
    # The other order would give LONG 1 at 25050 with 3250 realized: the order matters.
    assert row.unrealized == 6500


def test_facts_after_the_cutoff_are_invisible(tmp_path: Path) -> None:
    book = Book(tmp_path / "after.sqlite3")
    book.economics(OCT)
    book.trade(OCT, "BUY", 1, 12, 13, "25000")
    book.bar(OCT, 15, "25100", "25100")  # the mark at the cutoff
    book.trade(OCT, "SELL", 1, 15, 16, "25300")  # decided at the cutoff, filled after it
    book.bar(OCT, 16, "25300", "25400")  # a stored mark after the cutoff
    book.trade(OCT, "BUY", 1, 16)  # decided after the cutoff

    result, _, _ = reconcile("C-2 after the cutoff", book.path, _day(15))

    (row,) = result.contracts
    assert (row.net_contracts, row.realized, row.unrealized) == (1, 0, 6500)
    assert row.mark.point_in_time == _day(15) and row.mark.close == 25100
    assert [(p.side, p.decided_at) for p in result.pending_orders] == [("SELL", _day(15))]


def _copy(tmp_path: Path, name: str) -> Path:
    source = build(tmp_path, name + "-source", *SCENARIOS["PR-8"][:2])
    target = tmp_path / f"{name}.sqlite3"
    shutil.copyfile(source, target)
    return target


def _write(database: Path, *statements: tuple[str, tuple]) -> None:
    """Raw writes on a disposable synthetic copy, to plant a defect."""
    with closing(sqlite3.connect(database)) as connection:
        for sql, params in statements:
            connection.execute(sql, params)
        connection.commit()


def test_storage_order_does_not_change_the_result(tmp_path: Path) -> None:
    database = _copy(tmp_path, "reordered")
    expected, _ = verify(database, CUTOFF)
    with closing(sqlite3.connect(database)) as connection:
        rows = connection.execute("SELECT * FROM futures_paper_fills").fetchall()
        connection.execute("DELETE FROM futures_paper_fills")
        connection.executemany("INSERT INTO futures_paper_fills VALUES (?, ?, ?, ?)", rows[::-1])
        connection.commit()

    reordered, _ = verify(database, CUTOFF)

    assert reordered.contracts == expected.contracts


@pytest.mark.parametrize(
    ("defect", "error", "statements"),
    [
        (
            "fill not after its decision",
            independent.InconsistentFacts,
            [("UPDATE futures_paper_fills SET filled_at = ? WHERE rowid = 1", (_day(12),))],
        ),
        (
            "fill without its order",
            independent.InconsistentFacts,
            [
                (
                    "INSERT INTO futures_paper_fills VALUES ('orphan', 'no-such-order', '1', ?)",
                    (_day(13),),
                )
            ],
        ),
        (
            "malformed quote",
            independent.MalformedFact,
            [("UPDATE futures_paper_fills SET fill_quote = '25,000' WHERE rowid = 1", ())],
        ),
        (
            "instant without offset",
            independent.MalformedFact,
            [
                (
                    "UPDATE futures_paper_fills SET filled_at = ? WHERE rowid = 1",
                    ("2026-10-13T10:00:00",),
                )
            ],
        ),
        (
            "non-positive point value",
            independent.MalformedFact,
            [("UPDATE futures_contract_economics SET point_value_amount = '0'", ())],
        ),
        (
            "foreign strategy in the portfolio",
            independent.InconsistentFacts,
            [("UPDATE futures_paper_orders SET strategy_identity = 'other' WHERE rowid = 1", ())],
        ),
    ],
    ids=lambda value: value if isinstance(value, str) else "",
)
def test_inconsistent_or_malformed_facts_fail_explicitly(tmp_path, defect, error, statements):
    database = _copy(tmp_path, "defect")
    _write(database, *statements)
    before = preserved(database)

    with pytest.raises(error):
        independent.verify(database, _PORTFOLIO.identity, _STRATEGY.identity, CUTOFF)

    assert preserved(database) == before


def test_a_missing_database_is_an_error_and_is_not_created(tmp_path: Path) -> None:
    missing = tmp_path / "absent.sqlite3"
    with pytest.raises(independent.VerificationError):
        independent.verify(missing, _PORTFOLIO.identity, _STRATEGY.identity, CUTOFF)
    assert not missing.exists()


# ---------------------------------------------------------------------------
# Independence
# ---------------------------------------------------------------------------


def test_the_verifier_imports_only_the_standard_library() -> None:
    tree = ast.parse(VERIFIER.read_text(encoding="utf-8"))
    imported = {
        (node.module if isinstance(node, ast.ImportFrom) else alias.name).split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import | ast.ImportFrom)
        for alias in node.names
    }
    assert imported - {"__future__"} <= set(sys.stdlib_module_names), imported
    assert not any(name.startswith("northstar") for name in imported)


def test_importing_the_verifier_loads_no_northstar_module() -> None:
    probe = (
        "import sys; sys.path.insert(0, sys.argv[1]); import independent_pnl_verifier; "
        "print(sorted(m for m in sys.modules if m.startswith(('northstar', 'pnl_reporting'))))"
    )
    loaded = subprocess.run(
        [sys.executable, "-c", probe, str(VERIFIER.parent)],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert loaded == "[]"


def test_the_verifier_does_not_reuse_northstars_fold() -> None:
    source = VERIFIER.read_text(encoding="utf-8")
    for northstar_name in (
        "_transition",
        "average_entry.value",
        "localcontext",
        "_BASIS_CONTEXT",
        "BuildFuturesPaperPortfolioUseCase",
        "CalculateFuturesRealizedPnl",
    ):
        assert northstar_name not in source
