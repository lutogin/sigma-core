"""Conservative execution and accounting primitives for historical simulation.

The live bot submits marketable limit orders at the best bid/ask. Historical
OHLCV does not contain the order book, so the backtest must explicitly estimate
the missing half-spread and adverse slippage instead of assuming fills at the
mid/close price.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Tuple

OrderSide = Literal["buy", "sell"]
SpreadSide = Literal["long", "short"]


@dataclass(frozen=True)
class ExecutionAssumptions:
    """Costs which cannot be recovered from OHLCV candles."""

    fee_rate: float = 0.0004
    half_spread_bps: float = 2.0
    slippage_bps: float = 1.0

    def __post_init__(self) -> None:
        if self.fee_rate < 0:
            raise ValueError("fee_rate must be non-negative")
        if self.half_spread_bps < 0:
            raise ValueError("half_spread_bps must be non-negative")
        if self.slippage_bps < 0:
            raise ValueError("slippage_bps must be non-negative")


@dataclass(frozen=True)
class SpreadPnl:
    """Auditable two-leg PnL breakdown."""

    coin_exit_fill: float
    primary_exit_fill: float
    coin_pnl: float
    primary_pnl: float
    gross_pnl: float
    fees: float
    net_pnl: float
    gross_notional: float


def spread_order_sides(
    spread_side: SpreadSide, *, closing: bool = False
) -> Tuple[OrderSide, OrderSide]:
    """Return (coin_side, primary_side) for opening or closing a spread."""
    if spread_side not in ("long", "short"):
        raise ValueError(f"Unsupported spread side: {spread_side}")

    if spread_side == "long":
        opening = ("buy", "sell")
    else:
        opening = ("sell", "buy")

    if not closing:
        return opening
    return tuple("sell" if side == "buy" else "buy" for side in opening)  # type: ignore[return-value]


def executable_price(
    reference_price: float,
    side: OrderSide,
    assumptions: ExecutionAssumptions,
) -> float:
    """Apply estimated spread and adverse slippage to a mid/close reference."""
    if reference_price <= 0:
        raise ValueError("reference_price must be positive")
    if side not in ("buy", "sell"):
        raise ValueError(f"Unsupported order side: {side}")

    adverse_bps = assumptions.half_spread_bps + assumptions.slippage_bps
    multiplier = 1.0 + adverse_bps / 10_000
    if side == "sell":
        multiplier = 1.0 - adverse_bps / 10_000
    return reference_price * multiplier


def entry_fill_prices(
    *,
    spread_side: SpreadSide,
    coin_reference_price: float,
    primary_reference_price: float,
    assumptions: ExecutionAssumptions,
) -> Tuple[float, float]:
    """Calculate adverse opening fills for both legs."""
    coin_side, primary_side = spread_order_sides(spread_side)
    return (
        executable_price(coin_reference_price, coin_side, assumptions),
        executable_price(primary_reference_price, primary_side, assumptions),
    )


def calculate_spread_pnl(
    *,
    spread_side: SpreadSide,
    coin_entry_fill: float,
    primary_entry_fill: float,
    coin_entry_notional: float,
    primary_entry_notional: float,
    coin_exit_reference: float,
    primary_exit_reference: float,
    assumptions: ExecutionAssumptions,
) -> SpreadPnl:
    """Calculate futures PnL from notional sizes without multiplying by leverage.

    Leverage affects required margin, not the PnL of a position whose sizes are
    already expressed as exchange notional.
    """
    if (
        min(
            coin_entry_fill,
            primary_entry_fill,
            coin_entry_notional,
            primary_entry_notional,
        )
        <= 0
    ):
        raise ValueError("Entry prices and notionals must be positive")

    coin_exit_side, primary_exit_side = spread_order_sides(spread_side, closing=True)
    coin_exit_fill = executable_price(coin_exit_reference, coin_exit_side, assumptions)
    primary_exit_fill = executable_price(
        primary_exit_reference, primary_exit_side, assumptions
    )

    coin_quantity = coin_entry_notional / coin_entry_fill
    primary_quantity = primary_entry_notional / primary_entry_fill

    coin_direction = 1.0 if spread_side == "long" else -1.0
    primary_direction = -coin_direction
    coin_pnl = coin_direction * coin_quantity * (coin_exit_fill - coin_entry_fill)
    primary_pnl = (
        primary_direction * primary_quantity * (primary_exit_fill - primary_entry_fill)
    )
    gross_pnl = coin_pnl + primary_pnl

    entry_fees = assumptions.fee_rate * (coin_entry_notional + primary_entry_notional)
    exit_fees = assumptions.fee_rate * (
        abs(coin_quantity * coin_exit_fill) + abs(primary_quantity * primary_exit_fill)
    )
    fees = entry_fees + exit_fees

    return SpreadPnl(
        coin_exit_fill=coin_exit_fill,
        primary_exit_fill=primary_exit_fill,
        coin_pnl=coin_pnl,
        primary_pnl=primary_pnl,
        gross_pnl=gross_pnl,
        fees=fees,
        net_pnl=gross_pnl - fees,
        gross_notional=coin_entry_notional + primary_entry_notional,
    )
