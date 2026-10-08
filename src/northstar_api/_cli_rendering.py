"""Plain-text rendering of operational futures paper-trading facts.

Only facts are rendered: research actions, execution state and P&L. Nothing
here advises. Instants are canonical UTC PointInTime text, and every Money
amount carries its own currency; amounts in different currencies are never
combined.
"""

from __future__ import annotations

from datetime import date

from northstar_application.application_services import (
    FuturesContractPnl,
    FuturesPaperTradingDecisionResult,
    FuturesPaperTradingReport,
    FuturesPaperTradingSessionResult,
    FuturesPaperTradingValuation,
    OptionDailyAcquisitionResult,
)
from northstar_core.foundation.value_objects import Money
from northstar_core.futures import FuturesContract, FuturesContractEconomics
from northstar_core.options import (
    OptionChainEntry,
    OptionChainSnapshot,
    OptionContract,
    OptionContractEconomics,
    OptionProductReference,
    OptionRight,
)
from northstar_core.paper_trading import FuturesPaperOrder, FuturesPaperPortfolio
from northstar_infrastructure.market_data import (
    StoredOptionProviderListing,
    UpstoxOptionMasterSnapshot,
)

POLICY = "built-in directional MVP"
PRICE_BASIS = "OPEN of next synced session"


def money(value: Money) -> str:
    return f"{value.amount} {value.currency}"


def economics_lines(economics: FuturesContractEconomics) -> list[str]:
    point_value = economics.point_value
    return [
        f"Contract: {economics.contract}",
        f"Point value: {point_value.amount} {point_value.currency} / quote-point / contract",
    ]


def option_economics_lines(economics: OptionContractEconomics) -> list[str]:
    point_value = economics.point_value
    return [
        f"Contract: {economics.contract}",
        f"Point value: {point_value.amount} {point_value.currency} / premium-point / contract",
    ]


def option_instrument_sync_lines(
    product: OptionProductReference, snapshot: UpstoxOptionMasterSnapshot
) -> list[str]:
    """Summarize one listing sync without claiming how many listings were new."""
    return [
        "OPTION INSTRUMENTS: SYNCED",
        f"Provider: {snapshot.provider}",
        f"Product: {product}",
        f"Snapshot: {snapshot.snapshot_sha256}",
        f"Master records: {snapshot.record_count}",
        f"{product.product_code} option records: {snapshot.option_record_count}",
        "Reference persistence: completed",
    ]


def option_instrument_lines(listing: StoredOptionProviderListing) -> list[str]:
    return [
        "OPTION INSTRUMENT: READY",
        f"Contract: {listing.contract}",
        f"Provider: {listing.provider}",
        f"Instrument key: {listing.instrument_key}",
        f"Exchange lot size: {listing.exchange_lot_size}",
        f"Established snapshot: {listing.established_snapshot_sha256}",
        f"Established at: {listing.established_at}",
    ]


def option_market_data_sync_header_lines(
    provider: str, contract: OptionContract, start: date, end: date
) -> list[str]:
    return [
        "OPTIONS MARKET DATA SYNC",
        f"Provider: {provider}",
        f"Contract: {contract}",
        f"Date range: {start} .. {end} (trading dates, historical endpoint only)",
    ]


def option_market_data_sync_lines(result: OptionDailyAcquisitionResult) -> list[str]:
    """Summarize one option sync; a missing candle is never called a no-trade session.

    The composite store persists exactly one raw open-interest record beside
    each bar or nothing at all, so the open-interest count is the bar count.
    """
    missing = result.missing_trading_dates
    lines = [
        "SYNC: COMPLETED",
        f"Sessions in range: {result.session_count}",
        f"Sessions with a persisted daily bar: {result.daily_bar_count} "
        "(identical bars already stored count as persisted)",
        f"Sessions without a provider candle: {len(missing)}",
    ]
    if missing:
        lines.append(f"Missing trading dates: {', '.join(day.isoformat() for day in missing)}")
    return [
        *lines,
        f"Provider open-interest records persisted: {result.daily_bar_count} "
        "(raw provider values, not normalized)",
        "Finality: not assessed",
    ]


def _chain_cell(entry: OptionChainEntry | None) -> str:
    if entry is None:
        return "no known listing"
    if entry.daily_bar is None:
        return "no daily bar"
    return f"C={entry.daily_bar.close} V={entry.daily_bar.volume}"


def option_chain_lines(snapshot: OptionChainSnapshot, trading_date: date) -> list[str]:
    """Render one chain, a strike per row; an absent bar is never called a no-trade session."""
    rows: dict[str, dict[OptionRight, OptionChainEntry]] = {}
    for entry in snapshot.entries:
        rows.setdefault(str(entry.contract.strike), {})[entry.contract.right] = entry
    table = [
        (strike, _chain_cell(row.get(OptionRight.CALL)), _chain_cell(row.get(OptionRight.PUT)))
        for strike, row in rows.items()
    ]
    strike_width = max(len("STRIKE"), *(len(strike) for strike, _, _ in table)) + 4
    call_width = max(len("CALL"), *(len(call) for _, call, _ in table)) + 4
    observed = sum(1 for entry in snapshot.entries if entry.daily_bar is not None)
    return [
        f"OPTION CHAIN: {snapshot.product} {snapshot.expiration_date}",
        f"Trading date: {trading_date.isoformat()}",
        f"As of: {snapshot.as_of} (session close)",
        f"Listed contracts known by as-of: {len(snapshot.entries)}",
        f"Strikes: {len(rows)}",
        f"Contracts with a daily bar: {observed}",
        f"Contracts with no daily bar: {len(snapshot.entries) - observed}",
        "",
        f"{'STRIKE':<{strike_width}}{'CALL':<{call_width}}PUT",
        *(f"{strike:<{strike_width}}{call:<{call_width}}{put}" for strike, call, put in table),
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
    if result.decision.expiry_flatten:
        # Closes the position before the protected pre-expiry window, whatever the action.
        lines.insert(3, "Expiry flatten: yes")
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
