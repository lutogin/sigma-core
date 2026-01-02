"""
Trading Pairs Management.

Manages trading pairs configuration stored in MongoDB.
"""

from src.domain.trading_pairs.models import TradingPair
from src.domain.trading_pairs.repository import TradingPairRepository

__all__ = [
    "TradingPair",
    "TradingPairRepository",
]

