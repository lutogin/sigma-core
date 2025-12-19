"""
Position State Models.

Data models for spread positions and cooldowns.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class SpreadSide(str, Enum):
    """
    Spread position side.

    - LONG spread: Buy COIN, Sell PRIMARY (expect Z to rise toward 0)
    - SHORT spread: Sell COIN, Buy PRIMARY (expect Z to fall toward 0)
    """

    LONG = "long"
    SHORT = "short"


@dataclass
class SpreadPosition:
    """
    Active spread position.

    Represents a statistical arbitrage spread trade with two legs:
    - COIN leg (altcoin being traded)
    - PRIMARY leg (ETH - the hedge)
    """

    # Identifiers
    id: Optional[str] = None  # MongoDB ObjectId as string
    coin_symbol: str = ""  # e.g., "ADA/USDT:USDT"
    primary_symbol: str = ""  # e.g., "ETH/USDT:USDT"

    # Spread direction
    side: SpreadSide = SpreadSide.LONG

    # Entry metrics
    entry_z_score: float = 0.0
    entry_beta: float = 0.0
    entry_correlation: float = 0.0
    entry_hurst: float = 0.0

    # Position sizing
    coin_size_usdt: float = 0.0  # COIN leg size in USDT
    primary_size_usdt: float = 0.0  # PRIMARY leg size in USDT
    leverage: int = 1

    # Entry prices
    coin_entry_price: float = 0.0
    primary_entry_price: float = 0.0

    # Thresholds (stored for exit logic)
    z_tp_threshold: float = 0.0
    z_sl_threshold: float = 4.5

    # Timestamps
    opened_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Status
    is_active: bool = True

    def to_dict(self) -> dict:
        """Convert to dictionary for MongoDB storage."""
        data = asdict(self)
        data["side"] = self.side.value
        data["opened_at"] = self.opened_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "SpreadPosition":
        """Create from MongoDB document."""
        # Handle ObjectId
        doc_id = data.pop("_id", None)
        if doc_id:
            data["id"] = str(doc_id)

        # Handle enum
        if "side" in data and isinstance(data["side"], str):
            data["side"] = SpreadSide(data["side"])

        # Handle datetime
        if "opened_at" in data:
            if isinstance(data["opened_at"], str):
                data["opened_at"] = datetime.fromisoformat(data["opened_at"])
            elif isinstance(data["opened_at"], datetime):
                if data["opened_at"].tzinfo is None:
                    data["opened_at"] = data["opened_at"].replace(tzinfo=timezone.utc)

        return cls(**data)

    @property
    def total_size_usdt(self) -> float:
        """Total position size (both legs)."""
        return self.coin_size_usdt + self.primary_size_usdt

    def duration_hours(self, now: Optional[datetime] = None) -> float:
        """Calculate how long position has been open in hours."""
        if now is None:
            now = datetime.now(timezone.utc)
        return (now - self.opened_at).total_seconds() / 3600

    def duration_minutes(self, now: Optional[datetime] = None) -> float:
        """Calculate how long position has been open in minutes."""
        if now is None:
            now = datetime.now(timezone.utc)
        return (now - self.opened_at).total_seconds() / 60


@dataclass
class SymbolCooldown:
    """
    Symbol cooldown entry.

    After a position is closed with STOP_LOSS or CORRELATION_DROP,
    the symbol is placed in cooldown to prevent immediate re-entry.
    """

    # Identifiers
    id: Optional[str] = None  # MongoDB ObjectId as string
    symbol: str = ""  # e.g., "ADA/USDT:USDT"

    # Cooldown timing
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Reason for cooldown
    exit_reason: str = ""  # "STOP_LOSS" or "CORRELATION_DROP"

    def to_dict(self) -> dict:
        """Convert to dictionary for MongoDB storage."""
        return {
            "symbol": self.symbol,
            "started_at": self.started_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "exit_reason": self.exit_reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SymbolCooldown":
        """Create from MongoDB document."""
        # Handle ObjectId
        doc_id = data.pop("_id", None)
        if doc_id:
            data["id"] = str(doc_id)

        # Handle datetime
        for field_name in ["started_at", "expires_at"]:
            if field_name in data:
                if isinstance(data[field_name], str):
                    data[field_name] = datetime.fromisoformat(data[field_name])
                elif isinstance(data[field_name], datetime):
                    if data[field_name].tzinfo is None:
                        data[field_name] = data[field_name].replace(tzinfo=timezone.utc)

        return cls(**data)

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        """Check if cooldown has expired."""
        if now is None:
            now = datetime.now(timezone.utc)
        return now >= self.expires_at

    def remaining_minutes(self, now: Optional[datetime] = None) -> float:
        """Get remaining cooldown time in minutes."""
        if now is None:
            now = datetime.now(timezone.utc)
        remaining = (self.expires_at - now).total_seconds() / 60
        return max(0, remaining)
