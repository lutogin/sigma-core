import pytest

from backtests.execution_model import (
    ExecutionAssumptions,
    calculate_spread_pnl,
    entry_fill_prices,
    executable_price,
    spread_order_sides,
)


def test_market_buy_is_worse_than_reference_and_sell_is_lower() -> None:
    assumptions = ExecutionAssumptions(
        fee_rate=0.0004, half_spread_bps=2.0, slippage_bps=1.0
    )

    assert executable_price(100.0, "buy", assumptions) == pytest.approx(100.03)
    assert executable_price(100.0, "sell", assumptions) == pytest.approx(99.97)


@pytest.mark.parametrize(
    ("spread_side", "opening", "closing"),
    [
        ("long", ("buy", "sell"), ("sell", "buy")),
        ("short", ("sell", "buy"), ("buy", "sell")),
    ],
)
def test_spread_leg_sides_are_symmetric(spread_side, opening, closing) -> None:
    assert spread_order_sides(spread_side) == opening
    assert spread_order_sides(spread_side, closing=True) == closing


def test_pnl_uses_notional_and_is_independent_of_leverage() -> None:
    assumptions = ExecutionAssumptions(
        fee_rate=0.0, half_spread_bps=0.0, slippage_bps=0.0
    )
    result = calculate_spread_pnl(
        spread_side="long",
        coin_entry_fill=100.0,
        primary_entry_fill=2_000.0,
        coin_entry_notional=1_000.0,
        primary_entry_notional=1_000.0,
        coin_exit_reference=110.0,
        primary_exit_reference=2_000.0,
        assumptions=assumptions,
    )

    assert result.coin_pnl == pytest.approx(100.0)
    assert result.primary_pnl == pytest.approx(0.0)
    assert result.net_pnl == pytest.approx(100.0)


def test_flat_market_loses_roundtrip_costs() -> None:
    assumptions = ExecutionAssumptions(
        fee_rate=0.0004, half_spread_bps=2.0, slippage_bps=1.0
    )
    coin_entry, primary_entry = entry_fill_prices(
        spread_side="long",
        coin_reference_price=100.0,
        primary_reference_price=2_000.0,
        assumptions=assumptions,
    )
    result = calculate_spread_pnl(
        spread_side="long",
        coin_entry_fill=coin_entry,
        primary_entry_fill=primary_entry,
        coin_entry_notional=1_000.0,
        primary_entry_notional=1_000.0,
        coin_exit_reference=100.0,
        primary_exit_reference=2_000.0,
        assumptions=assumptions,
    )

    assert result.gross_pnl < 0
    assert result.fees > 0
    assert result.net_pnl < result.gross_pnl
