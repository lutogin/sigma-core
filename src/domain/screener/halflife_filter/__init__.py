"""
Half-Life Filter module for mean-reversion speed detection.

The Half-Life measures how quickly a spread reverts to its mean.
For stat-arb, we want spreads that revert fast enough relative to our holding period.
Rule: Half-Life <= 0.5 * MAX_POSITION_BARS (in same time units)
"""

from src.domain.screener.halflife_filter.halflife_filter import HalfLifeFilterService

__all__ = ["HalfLifeFilterService"]
