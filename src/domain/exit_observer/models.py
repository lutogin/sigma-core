"""
Exit Observer Models.

Data classes for real-time exit monitoring.
"""

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class ExitWatchStatus(str, Enum):
    """Status of an exit watch."""

    WATCHING = "watching"  # Actively monitoring for TP/SL
    EXITED_TP = "exited_tp"  # Exit triggered by take profit
    EXITED_SL = "exited_sl"  # Exit triggered by stop loss
    CLOSED = "closed"  # Position closed externally


@dataclass
class ExitWatch:
    """
    State model for monitoring an open position for exit conditions.

    Stores all data needed to:
    1. Calculate live Z-score from current prices
    2. Detect TP/SL conditions in real-time
    3. Emit exit signals when conditions are met

    Live Z-score formula:
        current_spread = log(coin_price) - beta * log(primary_price)
        z_score = (current_spread - spread_mean) / spread_std
    """

    # Symbol identifiers
    coin_symbol: str
    primary_symbol: str

    # Spread direction
    spread_side: str  # "long" or "short"

    # Tracking state
    status: ExitWatchStatus = ExitWatchStatus.WATCHING

    # Spread statistics for live Z calculation
    beta: float = 0.0  # Hedge ratio for spread calculation
    spread_mean: float = 0.0  # Rolling mean of spread
    spread_std: float = 0.0  # Rolling std of spread

    # Entry metrics
    entry_z_score: float = 0.0  # Z-score at entry
    correlation: float = 0.0
    hurst: float = 0.0

    # Thresholds
    z_tp_threshold: float = 0.0  # Take profit when |Z| <= this
    z_sl_threshold: float = 4.0  # Stop loss when |Z| >= this

    # Live prices (updated via WebSocket)
    coin_price: float = 0.0
    primary_price: float = 0.0

    # Timing
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_update_at: Optional[datetime] = None

    # =========================================================================
    # Computed Properties
    # =========================================================================

    @property
    def current_spread(self) -> float:
        """
        Calculate current spread from live prices.

        Formula: spread = log(coin_price) - beta * log(primary_price)
        """
        if self.coin_price <= 0 or self.primary_price <= 0:
            return 0.0
        return math.log(self.coin_price) - self.beta * math.log(self.primary_price)

    @property
    def current_z_score(self) -> float:
        """
        Calculate current Z-score in real-time.

        Formula: z = (current_spread - spread_mean) / spread_std
        """
        if self.spread_std == 0:
            return 0.0
        return (self.current_spread - self.spread_mean) / self.spread_std

    @property
    def watch_duration_seconds(self) -> float:
        """Time elapsed since watch started (in seconds)."""
        return (datetime.now(timezone.utc) - self.created_at).total_seconds()

    @property
    def watch_duration_minutes(self) -> float:
        """Time elapsed since watch started (in minutes)."""
        return self.watch_duration_seconds / 60.0

    def is_tp_condition(self) -> bool:
        """Check if take profit condition is met."""
        return abs(self.current_z_score) <= self.z_tp_threshold

    def is_sl_condition(self) -> bool:
        """Check if stop loss condition is met."""
        return abs(self.current_z_score) >= self.z_sl_threshold

    def __repr__(self) -> str:
        return (
            f"ExitWatch({self.coin_symbol}, "
            f"status={self.status.value}, "
            f"entry_z={self.entry_z_score:.2f}, "
            f"current_z={self.current_z_score:.2f}, "
            f"duration={self.watch_duration_minutes:.1f}m)"
        )
