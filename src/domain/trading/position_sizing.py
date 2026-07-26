from dataclasses import dataclass


@dataclass(frozen=True)
class CoinNotionalSizing:
    """Auditable COIN-leg sizing shared by live and backtest execution."""

    base_notional: float
    size_multiplier: float
    desired_notional: float
    equity_cap_notional: float
    final_notional: float


def calculate_coin_notional(
    *,
    base_notional: float,
    size_multiplier: float,
    equity: float,
    max_coin_notional_pct: float,
) -> CoinNotionalSizing:
    """
    Apply Half-Life sizing to the configured base, then cap the final COIN leg.

    The configured USDT amount is the 1.0x position. The equity percentage is a
    final safety ceiling, not a replacement for that configured base.
    """
    safe_base = max(0.0, base_notional)
    safe_multiplier = max(0.0, size_multiplier)
    desired = safe_base * safe_multiplier
    equity_cap = max(0.0, equity) * max_coin_notional_pct
    return CoinNotionalSizing(
        base_notional=safe_base,
        size_multiplier=safe_multiplier,
        desired_notional=desired,
        equity_cap_notional=equity_cap,
        final_notional=min(desired, equity_cap),
    )
