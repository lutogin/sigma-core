"""
Hurst Filter module for mean-reversion detection.

The Hurst exponent measures the tendency of a time series to revert to mean
or to trend. For stat-arb, we want mean-reverting spreads (H < 0.5).
"""

from src.domain.screener.hurst_filter.service import HurstFilterService

__all__ = ["HurstFilterService"]
