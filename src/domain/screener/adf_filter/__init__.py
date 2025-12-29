"""
ADF Filter module for stationarity detection.

The Augmented Dickey-Fuller test checks if a time series is stationary.
For stat-arb, we want stationary spreads (p-value < 0.05).
"""

from src.domain.screener.adf_filter.adf_filter import ADFFilterService

__all__ = ["ADFFilterService"]
