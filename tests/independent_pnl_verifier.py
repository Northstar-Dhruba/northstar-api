"""Independent verifier of futures paper P&L, from persisted facts only (M1.4.4.2).

This module recomputes, for one paper portfolio and strategy at one cutoff,
each dated contract's signed position, average entry, gross realized P&L and
gross unrealized P&L. It exists to check Northstar's accounting, so it shares
none of it:

- it imports the Python standard library only -- no ``northstar_*`` package,
  neither the accounting use cases nor the repositories that decode the rows
  (enforced by ``test_independent_pnl_reconciliation.py``);
- it reads the SQLite file read-only (``mode=ro``) and never writes;
- it computes with exact ``fractions.Fraction``, never a decimal context;
- it parses every instant to an aware UTC datetime and never orders text.

The specification is section 4 of
``northstar-docs/operations/Indian-Futures-Independent-Paper-PnL-Reconciliation.md``.
The fold is written as a *total entry cost* per contract rather than an
average: adding raises the cost by ``contracts * quote``; closing ``c``
contracts releases ``c / |n|`` of the cost and realizes the difference to the
fill quote; a reversal restarts the cost at the fill quote. The average entry
is only ever derived, as cost / |n|. That is the same economics as an
average-entry fold, reached by a different route.

Facts it cannot use are errors, never zeros: malformed values, a fill without
its order, a fill not after its decision, a foreign strategy in the
portfolio, or missing economics for a filled contract. An open position with
no stored close at or before the cutoff is reported unavailable. Whether a
mark came from an operator-approved session is not persisted, so its approval
is always reported as ``UNKNOWN``.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from fractions import Fraction
from pathlib import Path

APPROVAL_UNKNOWN = "UNKNOWN"
NET_NOT_APPLICABLE = "NOT_APPLICABLE: gross only; no fee, tax or slippage model exists"
MARK_SOURCE = "futures_ohlcv 1d close_value"

_DECIMAL_TEXT = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?")
_COUNT_TEXT = re.compile(r"[1-9][0-9]*")
_SIDES = {"BUY": 1, "SELL": -1}


class VerificationError(Exception):
    """A source fact the verifier cannot use; nothing is assumed in its place."""


class MalformedFact(VerificationError):
    """A stored value that does not parse as the fact it must be."""


class InconsistentFacts(VerificationError):
    """Stored facts that contradict each other."""


class MissingEconomics(VerificationError):
    """A filled contract has no stored point value."""

    def __init__(self, contract: ContractKey) -> None:
        super().__init__(f"no contract economics stored for {contract}")
        self.contract = contract


@dataclass(frozen=True, order=True)
class ContractKey:
    product: str
    exchange: str
    expiration: str

    def __str__(self) -> str:
        return f"{self.product}@{self.exchange} {self.expiration}"


@dataclass(frozen=True)
class Mark:
    """The stored close used to value an open position, with its provenance."""

    close: Fraction
    close_text: str
    point_in_time: str
    instant: datetime
    source: str = MARK_SOURCE
    approval: str = APPROVAL_UNKNOWN


@dataclass(frozen=True)
class ContractValuation:
    contract: ContractKey
    currency: str
    point_value: Fraction
    net_contracts: int
    average_entry: Fraction | None
    realized: Fraction
    unrealized: Fraction | None
    unrealized_status: str  # FLAT, AVAILABLE or UNAVAILABLE_NO_MARK
    mark: Mark | None
    fills_applied: int
    partial_closes: int
    reversals: int
    non_terminating_basis: bool

    @property
    def total(self) -> Fraction | None:
        """Realized plus unrealized; unknown while unrealized is unknown."""
        return None if self.unrealized is None else self.realized + self.unrealized


@dataclass(frozen=True)
class PendingOrder:
    order_identity: str
    contract: ContractKey
    side: str
    contracts: int
    decided_at: str


@dataclass(frozen=True)
class Verification:
    database: str
    portfolio: str
    strategy: str
    cutoff: str
    cutoff_instant: datetime
    contracts: tuple[ContractValuation, ...]
    pending_orders: tuple[PendingOrder, ...]
    fills_visible: int
    findings: tuple[str, ...]
    net_pnl: str = NET_NOT_APPLICABLE

    def contract(self, key: ContractKey) -> ContractValuation:
        return next(row for row in self.contracts if row.contract == key)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def parse_instant(text: object, what: str) -> datetime:
    if not isinstance(text, str):
        raise MalformedFact(f"{what} is not text: {text!r}")
    try:
        value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise MalformedFact(f"{what} is not an ISO instant: {text!r}") from error
    if value.utcoffset() is None:
        raise MalformedFact(f"{what} has no UTC offset: {text!r}")
    return value.astimezone(UTC)


def _decimal(text: object, what: str) -> Fraction:
    if not isinstance(text, str) or not _DECIMAL_TEXT.fullmatch(text):
        raise MalformedFact(f"{what} is not a plain decimal: {text!r}")
    return Fraction(text)


def _count(text: object, what: str) -> int:
    if not isinstance(text, str) or not _COUNT_TEXT.fullmatch(text):
        raise MalformedFact(f"{what} is not a positive whole contract count: {text!r}")
    return int(text)


def terminates(value: Fraction) -> bool:
    """Whether ``value`` has a finite decimal expansion (denominator 2^a * 5^b)."""
    denominator = value.denominator
    for prime in (2, 5):
        while denominator % prime == 0:
            denominator //= prime
    return denominator == 1


# ---------------------------------------------------------------------------
# Reading (read-only)
# ---------------------------------------------------------------------------


def _connect(database: Path) -> sqlite3.Connection:
    if not database.is_file():
        raise VerificationError(f"database not found: {database}")
    connection = sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only = ON")
    return connection


def _orders(connection, portfolio: str, strategy: str) -> dict[str, dict]:
    orders: dict[str, dict] = {}
    rows = connection.execute(
        "SELECT order_identity, product_code, exchange_code, expiration_date, side, contracts, "
        "strategy_identity, decided_at FROM futures_paper_orders WHERE portfolio_identity = ?",
        (portfolio,),
    ).fetchall()
    for identity, product, exchange, expiration, side, count, owner, decided in rows:
        if owner != strategy:
            raise InconsistentFacts(
                f"portfolio {portfolio} holds order {identity} of strategy {owner}, not {strategy}"
            )
        if side not in _SIDES:
            raise MalformedFact(f"order {identity} side is {side!r}")
        orders[identity] = {
            "identity": identity,
            "contract": ContractKey(product, exchange, expiration),
            "side": side,
            "contracts": _count(count, f"order {identity} contracts"),
            "decided_at": decided,
            "decided": parse_instant(decided, f"order {identity} decided_at"),
        }
    return orders


def _fills(connection, orders: dict[str, dict]) -> list[dict]:
    known = {
        row[0] for row in connection.execute("SELECT order_identity FROM futures_paper_orders")
    }
    fills, seen = [], set()
    for identity, order_identity, quote, filled_at in connection.execute(
        "SELECT fill_identity, order_identity, fill_quote, filled_at FROM futures_paper_fills"
    ).fetchall():
        if order_identity not in known:
            raise InconsistentFacts(f"fill {identity} has no stored order {order_identity}")
        if order_identity not in orders:
            continue  # another portfolio's fill
        if order_identity in seen:
            raise InconsistentFacts(f"order {order_identity} has more than one fill")
        seen.add(order_identity)
        order = orders[order_identity]
        instant = parse_instant(filled_at, f"fill {identity} filled_at")
        if instant <= order["decided"]:
            raise InconsistentFacts(
                f"fill {identity} at {filled_at} is not after its decision {order['decided_at']}"
            )
        fills.append(
            {
                "identity": identity,
                "order": order,
                "quote": _decimal(quote, f"fill {identity} fill_quote"),
                "filled_at": filled_at,
                "instant": instant,
            }
        )
    return fills


def _economics(connection, contract: ContractKey) -> tuple[Fraction, str]:
    row = connection.execute(
        "SELECT point_value_amount, settlement_currency FROM futures_contract_economics "
        "WHERE product_code = ? AND exchange_code = ? AND expiration_date = ?",
        (contract.product, contract.exchange, contract.expiration),
    ).fetchone()
    if row is None:
        raise MissingEconomics(contract)
    point_value = _decimal(row[0], f"{contract} point value")
    if point_value <= 0:
        raise MalformedFact(f"{contract} point value is not positive: {row[0]!r}")
    return point_value, row[1]


def _bars(connection, contract: ContractKey) -> list[tuple[datetime, str, Fraction, Fraction, str]]:
    rows = connection.execute(
        "SELECT point_in_time, open_value, close_value FROM futures_ohlcv WHERE product_code = ? "
        "AND exchange_code = ? AND expiration_date = ? AND timeframe = '1d'",
        (contract.product, contract.exchange, contract.expiration),
    ).fetchall()
    bars = [
        (
            parse_instant(when, f"{contract} bar point_in_time"),
            when,
            _decimal(open_, f"{contract} bar open at {when}"),
            _decimal(close, f"{contract} bar close at {when}"),
            close,
        )
        for when, open_, close in rows
    ]
    return sorted(bars, key=lambda bar: bar[0])


# ---------------------------------------------------------------------------
# The fold: total entry cost per contract
# ---------------------------------------------------------------------------


class _Book:
    def __init__(self) -> None:
        self.quantity = 0  # signed contracts: + long, - short
        self.cost = Fraction(0)  # total entry cost of |quantity| contracts, in quote points
        self.realized_points = Fraction(0)
        self.fills = self.partials = self.reversals = 0
        self.non_terminating = False

    def apply(self, signed: int, quote: Fraction) -> None:
        self.fills += 1
        held = abs(self.quantity)
        if held == 0 or (self.quantity > 0) == (signed > 0):
            self.cost += abs(signed) * quote
            self.quantity += signed
        else:
            closing = min(abs(signed), held)
            released = self.cost * closing / held
            direction = 1 if self.quantity > 0 else -1
            self.realized_points += (quote * closing - released) * direction
            self.cost -= released
            after = self.quantity + signed
            if after == 0:
                self.cost = Fraction(0)
            elif (after > 0) != (self.quantity > 0):
                self.reversals += 1
                self.cost = abs(after) * quote
            else:
                self.partials += 1
            self.quantity = after
        if self.quantity and not terminates(self.cost / abs(self.quantity)):
            self.non_terminating = True


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify(database: Path, portfolio: str, strategy: str, cutoff: str) -> Verification:
    """Recompute every filled contract of ``portfolio`` at ``cutoff``; read-only."""
    cutoff_instant = parse_instant(cutoff, "cutoff")
    findings: list[str] = []
    connection = _connect(Path(database))
    try:
        orders = _orders(connection, portfolio, strategy)
        fills = _fills(connection, orders)
        visible = sorted(
            (fill for fill in fills if fill["instant"] <= cutoff_instant),
            key=lambda fill: (fill["instant"], fill["order"]["identity"]),
        )
        filled_by_cutoff = {fill["order"]["identity"] for fill in visible}
        pending = tuple(
            PendingOrder(o["identity"], o["contract"], o["side"], o["contracts"], o["decided_at"])
            for o in sorted(orders.values(), key=lambda o: (o["decided"], o["identity"]))
            if o["decided"] <= cutoff_instant and o["identity"] not in filled_by_cutoff
        )

        books: dict[ContractKey, _Book] = {}
        for fill in visible:
            order = fill["order"]
            books.setdefault(order["contract"], _Book()).apply(
                _SIDES[order["side"]] * order["contracts"], fill["quote"]
            )

        results = []
        for contract in sorted(books):
            book = books[contract]
            point_value, currency = _economics(connection, contract)
            bars = _bars(connection, contract)
            findings += _execution_findings(contract, bars, visible)
            observable = [bar for bar in bars if bar[0] <= cutoff_instant]
            mark = (
                Mark(observable[-1][3], observable[-1][4], observable[-1][1], observable[-1][0])
                if observable
                else None
            )
            average = book.cost / abs(book.quantity) if book.quantity else None
            if book.quantity == 0:
                unrealized, status = Fraction(0), "FLAT"
            elif mark is None:
                unrealized, status = None, "UNAVAILABLE_NO_MARK"
                findings.append(f"{contract}: open position with no stored close by the cutoff")
            else:
                unrealized = (mark.close - average) * book.quantity * point_value
                status = "AVAILABLE"
            results.append(
                ContractValuation(
                    contract=contract,
                    currency=currency,
                    point_value=point_value,
                    net_contracts=book.quantity,
                    average_entry=average,
                    realized=book.realized_points * point_value,
                    unrealized=unrealized,
                    unrealized_status=status,
                    mark=mark if book.quantity else None,
                    fills_applied=book.fills,
                    partial_closes=book.partials,
                    reversals=book.reversals,
                    non_terminating_basis=book.non_terminating,
                )
            )
    finally:
        connection.close()
    return Verification(
        database=str(database),
        portfolio=portfolio,
        strategy=strategy,
        cutoff=cutoff,
        cutoff_instant=cutoff_instant,
        contracts=tuple(results),
        pending_orders=pending,
        fills_visible=len(visible),
        findings=tuple(findings),
    )


def _execution_findings(contract: ContractKey, bars, visible) -> list[str]:
    """Cross-check each fill against the next-session OPEN rule; report, never adjust."""
    findings = []
    for fill in visible:
        if fill["order"]["contract"] != contract:
            continue
        later = [bar for bar in bars if bar[0] > fill["order"]["decided"]]
        if not later:
            findings.append(
                f"{contract}: fill {fill['identity']} has no stored bar after its decision"
            )
        elif later[0][0] != fill["instant"] or later[0][2] != fill["quote"]:
            findings.append(
                f"{contract}: fill {fill['identity']} is not the OPEN of the first bar after its "
                f"decision ({later[0][1]} open {later[0][2]})"
            )
    return findings
