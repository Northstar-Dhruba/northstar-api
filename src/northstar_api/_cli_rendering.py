"""Plain-text rendering of operational futures paper-trading facts.

Only facts are rendered: research actions, execution state and P&L. Nothing
here advises. Instants are canonical UTC PointInTime text, and every Money
amount carries its own currency; amounts in different currencies are never
combined.
"""

from __future__ import annotations

from northstar_application.application_services import (
    FuturesContractPnl,
    FuturesPaperTradingDecisionResult,
    FuturesPaperTradingReport,
    FuturesPaperTradingSessionResult,
    FuturesPaperTradingValuation,
)
from northstar_core.foundation.value_objects import Money
from northstar_core.futures import FuturesContract, FuturesProductEconomics
from northstar_core.paper_trading import FuturesPaperOrder, FuturesPaperPortfolio

POLICY = "built-in directional MVP"
PRICE_BASIS = "OPEN of next synced session"


def money(value: Money) -> str:
    return f"{value.amount} {value.currency}"


def economics_lines(economics: FuturesProductEconomics) -> list[str]:
    point_value = economics.point_value
    return [
        f"Product: {economics.reference}",
        f"Point value: {point_value.amount} {point_value.currency} / quote-point / contract",
    ]


def context_lines(
    *,
    strategy: str,
    portfolio: str,
    cutoff: str,
    contract: FuturesContract | None = None,
    target: int | None = None,
) -> list[str]:
    lines = ["", "CONTEXT"]
    if contract is not None:
        lines.append(f"Contract: {contract}")
    lines += [f"Strategy: {strategy}", f"Policy: {POLICY}", f"Portfolio: {portfolio}"]
    if target is not None:
        lines.append(f"Target: {target}")
    lines.append(f"Cutoff: {cutoff}")
    return lines


def _current_result(
    session: FuturesPaperTradingSessionResult,
) -> FuturesPaperTradingDecisionResult | None:
    record = session.frozen_record
    if record is None:
        return None
    return next((result for result in session.run.results if result.record == record), None)


def decision_lines(session: FuturesPaperTradingSessionResult) -> list[str]:
    record = session.frozen_record
    if record is None:
        return [
            "",
            "DECISION",
            "Decision: unavailable",
            "Reason: insufficient persisted daily history / warm-up",
        ]
    return [
        "",
        "DECISION",
        f"Action: {record.result.recommendation.action.value}",
        f"Decision instant: {record.decision_instant}",
    ]


def _execution_lines(result: FuturesPaperTradingDecisionResult) -> list[str]:
    intent = result.order.intent
    lines = [
        f"State: {'FILLED' if result.fill is not None else 'PENDING'}",
        f"Side: {intent.side.value}",
        f"Contracts: {intent.contracts.value}",
        f"ID: {result.order.identity.identity}",
    ]
    if result.fill is not None:
        lines += [
            f"Simulated fill price: {result.fill.fill_quote.value}",
            f"Price basis: {PRICE_BASIS}",
            f"Fill observable from: {result.fill.filled_at}",
        ]
    return lines


def order_lines(session: FuturesPaperTradingSessionResult) -> list[str]:
    """Render the execution state of this session's own decision only."""
    lines = ["", "ORDER"]
    current = _current_result(session)
    if current is None:
        return [*lines, "State: no current decision"]
    if current.order is None:
        reason = current.decision.no_intent_reason.value.replace("_", " ")
        return [*lines, "State: NO ACTION", f"Reason: {reason}"]
    return [*lines, *_execution_lines(current)]


def history_lines(session: FuturesPaperTradingSessionResult) -> list[str]:
    """Render every order of the run, where earlier decisions' fills become visible."""
    report: FuturesPaperTradingReport = session.report
    lines = [
        "",
        "ORDERS FOR THIS CONTRACT AND STRATEGY",
        f"Decisions: {report.decision_count}, orders: {report.order_count}, "
        f"filled: {report.fill_count}, pending: {report.pending_order_count}",
    ]
    for result in session.run.results:
        if result.order is not None:
            lines.append(f"Decision {result.record.decision_instant}:")
            lines += [f"  {line}" for line in _execution_lines(result)]
    return lines


def portfolio_lines(
    portfolio: FuturesPaperPortfolio, command_contract: FuturesContract | None = None
) -> list[str]:
    lines = ["", "PORTFOLIO (all contracts)"]
    if not portfolio.positions:
        return [*lines, "Flat"]
    for position in portfolio.positions:
        direction = "LONG" if position.net_contracts > 0 else "SHORT"
        marker = " (command contract)" if position.contract == command_contract else ""
        lines.append(
            f"{position.contract}{marker}: {direction} {abs(position.net_contracts)} "
            f"@ {position.average_entry.value}"
        )
    return lines


def _row_lines(row: FuturesContractPnl) -> list[str]:
    lines = [str(row.contract), f"  Realized P&L: {money(row.realized_pnl)}"]
    if row.position is None:
        return [*lines, f"  Unrealized P&L: {money(row.unrealized_pnl)} (no open position)"]
    if row.unrealized_pnl is None:
        return [
            *lines,
            "  Unrealized P&L: unavailable",
            "  Reason: no synced daily close observable by cutoff",
        ]
    return [
        *lines,
        f"  Mark: {row.mark_quote.value} (daily close at {row.mark_instant})",
        f"  Unrealized P&L: {money(row.unrealized_pnl)}",
    ]


def pnl_lines(valuation: FuturesPaperTradingValuation) -> list[str]:
    lines = ["", "P&L (gross simulated)"]
    if not valuation.contracts:
        return [*lines, "No filled contracts"]
    for row in valuation.contracts:
        lines += _row_lines(row)
    return lines


def pnl_unavailable_lines(reason: str) -> list[str]:
    return ["", "P&L (gross simulated)", "P&L: unavailable", f"Reason: {reason}"]


def execution_summary_lines(
    orders: int, fills: int, pending: tuple[FuturesPaperOrder, ...]
) -> list[str]:
    lines = ["", "SUMMARY", f"Orders: {orders}", f"Fills: {fills}", f"Pending: {len(pending)}"]
    for order in pending:
        intent = order.intent
        lines.append(
            f"PENDING {intent.contract}: {intent.side.value} {intent.contracts.value} "
            f"decided {intent.decided_at} ID {order.identity.identity}"
        )
    return lines
