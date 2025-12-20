"""
Entry Observer Models.

Data classes for the Trailing Entry feature.
"""

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class WatchStatus(str, Enum):
    """Status of a watch candidate."""

    WATCHING = "watching"  # Actively monitoring for reversal
    ENTERED = "entered"  # Entry executed
    CANCELLED = "cancelled"  # Watch cancelled (timeout, false alarm, etc.)


@dataclass
class WatchCandidate:
    """
    State model for a monitored entry candidate.

    Stores all data needed to:
    1. Calculate live Z-score from current prices
    2. Track maximum Z reached (for pullback detection)
    3. Determine when to enter or cancel

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
    status: WatchStatus = WatchStatus.WATCHING
    max_z: float = 0.0  # Maximum |Z| reached during watch

    # Spread statistics for live Z calculation (from last screener scan)
    beta: float = 0.0  # Hedge ratio for spread calculation
    spread_mean: float = 0.0  # Rolling mean of spread
    spread_std: float = 0.0  # Rolling std of spread

    # Original signal metrics
    initial_z: float = 0.0  # Z-score when watch started
    correlation: float = 0.0
    hurst: float = 0.0

    # Thresholds
    z_entry_threshold: float = 2.1
    z_tp_threshold: float = 0.0
    z_sl_threshold: float = 4.5

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

    # =========================================================================
    # Serialization (for Redis storage)
    # =========================================================================

    def to_dict(self) -> dict:
        """Serialize to dictionary for Redis storage."""
        return {
            "coin_symbol": self.coin_symbol,
            "primary_symbol": self.primary_symbol,
            "spread_side": self.spread_side,
            "status": self.status.value,
            "max_z": self.max_z,
            "beta": self.beta,
            "spread_mean": self.spread_mean,
            "spread_std": self.spread_std,
            "initial_z": self.initial_z,
            "correlation": self.correlation,
            "hurst": self.hurst,
            "z_entry_threshold": self.z_entry_threshold,
            "z_tp_threshold": self.z_tp_threshold,
            "z_sl_threshold": self.z_sl_threshold,
            "coin_price": self.coin_price,
            "primary_price": self.primary_price,
            "created_at": self.created_at.isoformat(),
            "last_update_at": (
                self.last_update_at.isoformat() if self.last_update_at else None
            ),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "WatchCandidate":
        """Deserialize from dictionary (from Redis)."""
        return cls(
            coin_symbol=data["coin_symbol"],
            primary_symbol=data["primary_symbol"],
            spread_side=data["spread_side"],
            status=WatchStatus(data.get("status", "watching")),
            max_z=data.get("max_z", 0.0),
            beta=data.get("beta", 0.0),
            spread_mean=data.get("spread_mean", 0.0),
            spread_std=data.get("spread_std", 0.0),
            initial_z=data.get("initial_z", 0.0),
            correlation=data.get("correlation", 0.0),
            hurst=data.get("hurst", 0.0),
            z_entry_threshold=data.get("z_entry_threshold", 2.1),
            z_tp_threshold=data.get("z_tp_threshold", 0.0),
            z_sl_threshold=data.get("z_sl_threshold", 4.5),
            coin_price=data.get("coin_price", 0.0),
            primary_price=data.get("primary_price", 0.0),
            created_at=datetime.fromisoformat(data["created_at"]),
            last_update_at=(
                datetime.fromisoformat(data["last_update_at"])
                if data.get("last_update_at")
                else None
            ),
        )

    def __repr__(self) -> str:
        return (
            f"WatchCandidate({self.coin_symbol}, "
            f"status={self.status.value}, "
            f"max_z={self.max_z:.2f}, "
            f"current_z={self.current_z_score:.2f}, "
            f"duration={self.watch_duration_minutes:.1f}m)"
        )
