import pytest

from src.domain.trading.position_sizing import calculate_coin_notional


def test_configured_usdt_is_the_one_x_base_before_halflife() -> None:
    sizing = calculate_coin_notional(
        base_notional=10_000.0,
        size_multiplier=0.5,
        equity=40_000.0,
        max_coin_notional_pct=0.525,
    )

    assert sizing.desired_notional == pytest.approx(5_000.0)
    assert sizing.final_notional == pytest.approx(5_000.0)


def test_final_coin_cap_is_applied_after_halflife() -> None:
    sizing = calculate_coin_notional(
        base_notional=10_000.0,
        size_multiplier=3.0,
        equity=40_000.0,
        max_coin_notional_pct=0.525,
    )

    assert sizing.desired_notional == pytest.approx(30_000.0)
    assert sizing.equity_cap_notional == pytest.approx(21_000.0)
    assert sizing.final_notional == pytest.approx(21_000.0)
