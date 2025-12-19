"""
Data Loader Module.

Loads and merges data from pairs and optimization repositories,
ranks pairs for statistical arbitrage trading.
"""

from .async_data_loader import AsyncDataLoaderService
from .ohlcv_repository import OHLCVRepository

__all__ = [
    "AsyncDataLoaderService",
    "OHLCVRepository",
]

