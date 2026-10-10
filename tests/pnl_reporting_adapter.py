"""Northstar's reported paper P&L, read through its own CLI and HTTP surfaces (M1.4.4.2).

This is the *application side* of the independent reconciliation: it calls
Northstar exactly as an operator or the dashboard does and normalises what
each surface reports, per contract. It computes nothing. The independent
expected values come from ``independent_pnl_verifier``, which must never
import this module or Northstar.

A field a surface does not report is ``NOT_EXPOSED``, never a value; an
amount the surface reports as unavailable is ``None``.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from northstar_core.derivatives import ExpirationDate
from northstar_core.foundation.value_objects import ExchangeCode, Symbol
from northstar_core.futures import FuturesContract, FuturesProductReference
from northstar_core.paper_trading import FuturesContractCount
from test_futures_dashboard import _get
from test_india7_nifty_incremental_operations_acceptance import _PORTFOLIO, _STRATEGY

from northstar_api.app import create_app
from northstar_api.cli import main
from northstar_api.runtime import build_database_runtime
from northstar_api.settings import (
    DashboardSettings,
    FuturesDailyBarFinalityMode,
    FuturesSessionOperationSettings,
)

NOT_EXPOSED = "NOT_EXPOSED"
PORTFOLIO = _PORTFOLIO.identity
STRATEGY = _STRATEGY.identity


@dataclass
class Reported:
    """One surface's report for one contract; NOT_EXPOSED where it reports nothing."""

    net_contracts: object = NOT_EXPOSED
    average_entry: object = NOT_EXPOSED
    realized: object = NOT_EXPOSED
    unrealized: object = NOT_EXPOSED
    mark_quote: object = NOT_EXPOSED
    mark_instant: object = NOT_EXPOSED
    total: object = NOT_EXPOSED
    point_value: object = NOT_EXPOSED


@dataclass
class Surface:
    name: str
    status: str  # "available", or why the surface reported no P&L
    contracts: dict[str, Reported] = field(default_factory=dict)
    pending_orders: object = NOT_EXPOSED
    fills: object = NOT_EXPOSED
    cutoff: object = NOT_EXPOSED
    raw: object = None


def contract_of(text: str) -> FuturesContract:
    """``NIFTY@NSE 2026-10-27`` -> FuturesContract (an identity, not accounting)."""
    product, rest = text.split("@")
    exchange, expiration = rest.split(" ")
    return FuturesContract(
        FuturesProductReference(Symbol(product), ExchangeCode(exchange)), ExpirationDate(expiration)
    )


def _cli(argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()

    def no_clock():
        raise AssertionError("a read-only report must not read the clock")

    code = main(argv, env={}, stdout=out, stderr=err, clock=no_clock)
    return int(code), out.getvalue(), err.getvalue()


# ---------------------------------------------------------------------------
# CLI: northstar paper status / economics show
# ---------------------------------------------------------------------------

_POSITION = re.compile(r"^(\S+@\S+ \S+)(?: \(command contract\))?: (LONG|SHORT) (\d+) @ (\S+)$")
_MONEY = re.compile(r"^  (Realized|Unrealized) P&L: (-?[0-9.]+) (\w+)")
_MARK = re.compile(r"^  Mark: (\S+) \(daily close at (\S+)\)$")


def paper_status(database: Path, cutoff: str) -> Surface:
    code, out, err = _cli(
        ["paper", "status", "--database", str(database), "--strategy", STRATEGY,
         "--portfolio", PORTFOLIO, "--as-of", cutoff]
    )  # fmt: skip
    lines = out.splitlines()
    surface = Surface(
        "cli paper status", "available" if code == 0 else f"exit {code}: {err.strip()}"
    )
    surface.raw = out + ("\n--- stderr ---\n" + err if err else "")
    surface.cutoff = next((ln.split(": ", 1)[1] for ln in lines if ln.startswith("Cutoff: ")), None)
    surface.fills = next((int(ln.split(": ")[1]) for ln in lines if ln.startswith("Fills: ")), None)
    surface.pending_orders = next(
        (int(ln.split(": ")[1]) for ln in lines if ln.startswith("Pending: ")), None
    )
    section = None
    current: Reported | None = None
    for line in lines:
        if line in ("PORTFOLIO (all contracts)", "P&L (gross simulated)", "SUMMARY"):
            section = line
            continue
        if section == "PORTFOLIO (all contracts)" and (match := _POSITION.match(line)):
            sign = 1 if match[2] == "LONG" else -1
            row = surface.contracts.setdefault(match[1], Reported())
            row.net_contracts, row.average_entry = sign * int(match[3]), match[4]
        elif section == "P&L (gross simulated)":
            if re.fullmatch(r"\S+@\S+ \S+", line):
                current = surface.contracts.setdefault(line, Reported())
                current.mark_quote = current.mark_instant = None
                if current.net_contracts == NOT_EXPOSED:
                    current.net_contracts, current.average_entry = 0, None
            elif current is not None and (match := _MONEY.match(line)):
                setattr(current, match[1].lower(), match[2])
            elif current is not None and line == "  Unrealized P&L: unavailable":
                current.unrealized = None
            elif current is not None and (match := _MARK.match(line)):
                current.mark_quote, current.mark_instant = match[1], match[2]
            elif line == "P&L: unavailable":
                reason = next((ln for ln in lines if ln.startswith("Reason: ")), "no reason")
                surface.status = f"exit {code}; P&L unavailable: {reason}"
    return surface


def economics_show(database: Path, contract: str) -> str | None:
    product, rest = contract.split("@")
    exchange, expiration = rest.split(" ")
    code, out, _ = _cli(
        ["economics", "show", "--database", str(database), "--product", product,
         "--exchange", exchange, "--expiration", expiration]
    )  # fmt: skip
    match = re.search(r"^Point value: (\S+) (\w+) /", out, flags=re.M)
    return match[1] if code == 0 and match else None


# ---------------------------------------------------------------------------
# HTTP: GET /futures/dashboard and /futures/analysis
# ---------------------------------------------------------------------------


def _app(database: Path, contract: FuturesContract):
    settings = DashboardSettings(
        database=database,
        web_origin="http://localhost:5173",
        contract=contract,
        strategy=_STRATEGY,
        portfolio=_PORTFOLIO,
        target=FuturesContractCount(1),
        operations=FuturesSessionOperationSettings(
            FuturesDailyBarFinalityMode("disabled"), None, date(2026, 10, 12)
        ),
    )
    return create_app(settings, runtime=build_database_runtime(database))


def _http(database: Path, contract: str, path: str, cutoff: str) -> tuple[int, dict]:
    response = _get(_app(database, contract_of(contract)), path, f"as_of={cutoff}")
    return response.status, response.json


def dashboard(database: Path, contract: str, cutoff: str) -> Surface:
    status, body = _http(database, contract, "/futures/dashboard", cutoff)
    surface = Surface("dashboard", f"http {status}" if status != 200 else body["pnl"]["status"])
    surface.raw = body
    if status != 200:
        return surface
    surface.cutoff = body["freshness"]["cutoff"]
    surface.pending_orders = len(body["portfolio"]["pending_orders"])
    if body["pnl"]["status"] != "available":
        surface.status = f"P&L {body['pnl']['status']}: {body['pnl']['reason']}"
    for row in body["pnl"]["rows"]:
        key = _key(row["contract"])
        rep = surface.contracts.setdefault(key, Reported())
        rep.realized, rep.unrealized = row["realized_pnl"], row["unrealized_pnl"]
        rep.mark_quote, rep.mark_instant = row["mark_quote"], row["mark_instant"]
        rep.net_contracts, rep.average_entry = 0, None
    for position in body["portfolio"]["positions"]:
        rep = surface.contracts.setdefault(_key(position["contract"]), Reported())
        sign = 1 if position["direction"] == "LONG" else -1
        rep.net_contracts = sign * int(position["net_contracts"])
        rep.average_entry = position["average_entry"]
    return surface


def analysis(database: Path, contract: str, cutoff: str) -> Surface:
    """The analysis covers the selected contract only."""
    status, body = _http(database, contract, "/futures/analysis", cutoff)
    surface = Surface("analysis", f"http {status}" if status != 200 else body["paper"]["status"])
    surface.raw = body
    if status != 200 or body["paper"]["status"] != "available":
        return surface
    surface.cutoff = body["context"].get("cutoff", NOT_EXPOSED)
    paper = body["paper"]
    rep = surface.contracts.setdefault(contract, Reported())
    rep.realized = paper["portfolio"]["canonical_realized_pnl"]["amount"]
    unrealized = paper["portfolio"]["unrealized_pnl"]
    rep.unrealized = unrealized["amount"] if unrealized is not None else None
    exposure = paper["portfolio"]["open_exposure"]
    if exposure is None:
        rep.net_contracts, rep.average_entry, rep.mark_quote, rep.mark_instant = 0, None, None, None
    else:
        # Unlike the dashboard (unsigned count plus direction), the analysis reports a
        # signed net_contracts next to its direction; both must agree.
        net = int(exposure["net_contracts"])
        if (net > 0) != (exposure["direction"] == "LONG"):
            raise AssertionError(f"analysis direction {exposure['direction']} contradicts {net}")
        rep.net_contracts = net
        rep.average_entry = exposure["average_entry"]
        rep.mark_quote, rep.mark_instant = exposure["mark_quote"], exposure["mark_instant"]
    curve = body["equity_curve"]
    # The curve's last point is a per-contract total only if it is valued at the reported mark.
    if curve and (rep.mark_instant in (None, curve[-1]["instant"])):
        rep.total = curve[-1]["total_pnl"]["amount"]
    return surface


def _key(contract: dict) -> str:
    return f"{contract['product']}@{contract['exchange']} {contract['expiration']}"
