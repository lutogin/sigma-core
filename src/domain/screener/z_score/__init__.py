"""
Z-Score module for statistical arbitrage.

Calculates spread and z-score for trading signals.
"""

from src.domain.screener.z_score.z_score import ZScoreService, ZScoreResult

__all__ = ["ZScoreService", "ZScoreResult"]
