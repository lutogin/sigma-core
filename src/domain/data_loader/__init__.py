"""
Data Loader Module.

Loads and merges data from pairs and optimization repositories,
ranks pairs for statistical arbitrage trading.
"""

from .async_service import AsyncDataLoaderService
from .ohlcv_repository import OHLCVRepository

__all__ = [
    "AsyncDataLoaderService",
    "OHLCVRepository",
]

