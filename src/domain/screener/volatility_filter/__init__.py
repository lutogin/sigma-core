"""
Volatility Filter module for market safety checks.

Monitors ETH volatility to detect dangerous market conditions:
- High volatility (storm warning)
- Flash crash/pump detection
"""

from src.domain.screener.volatility_filter.service import (
    VolatilityFilterService,
    VolatilityCheckResult,
)

__all__ = ["VolatilityFilterService", "VolatilityCheckResult"]
