"""
Trading Pairs Models.

Data models for trading pairs configuration stored in MongoDB.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4


@dataclass
class TradingPair:
    """
    Trading pair configuration.

    Represents a coin pair that can be traded against the primary pair (ETH).
    Stored in MongoDB for dynamic configuration without redeployment.
    """

    # Identifiers
    id: Optional[str] = None  # MongoDB ObjectId as string
    symbol: str = ""  # e.g., "LINK/USDT:USDT"

    # Status
    is_active: bool = True  # Whether pair is enabled for trading

    # Metadata
    ecosystem: str = "ETH"  # Ecosystem (ETH for ETH-correlated pairs)
    notes: str = ""  # Optional notes about the pair

    # Timestamps
    added_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        """Convert to dictionary for MongoDB storage."""
        data = asdict(self)
        data["added_at"] = self.added_at.isoformat()
        data["updated_at"] = self.updated_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "TradingPair":
        """Create from MongoDB document."""
        data = dict(data)
        # Handle ObjectId
        doc_id = data.pop("_id", None)
        if doc_id:
            data["id"] = str(doc_id)

        # Handle datetime fields
        for field_name in ["added_at", "updated_at"]:
            if field_name in data:
                if isinstance(data[field_name], str):
                    data[field_name] = datetime.fromisoformat(data[field_name])
                elif isinstance(data[field_name], datetime):
                    if data[field_name].tzinfo is None:
                        data[field_name] = data[field_name].replace(tzinfo=timezone.utc)

        return cls(**data)

    @property
    def base_symbol(self) -> str:
        """
        Extract base symbol from full pair.

        Example: "LINK/USDT:USDT" -> "LINK"
        """
        if "/" in self.symbol:
            return self.symbol.split("/")[0]
        return self.symbol

    def __repr__(self) -> str:
        status = "active" if self.is_active else "inactive"
        return f"TradingPair({self.symbol}, {status})"


@dataclass
class TradingPairVersion:
    """Immutable, auditable set of pairs produced by one walk-forward run."""

    version_id: str = field(default_factory=lambda: uuid4().hex)
    symbols: list[str] = field(default_factory=list)
    ecosystem: str = "ETH"
    source_result: str = ""
    source_sha256: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)
    strategy_config: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "TradingPairVersion":
        values = dict(data)
        values.pop("_id", None)
        created_at = values.get("created_at")
        if isinstance(created_at, str):
            values["created_at"] = datetime.fromisoformat(created_at)
        elif isinstance(created_at, datetime) and created_at.tzinfo is None:
            values["created_at"] = created_at.replace(tzinfo=timezone.utc)
        return cls(**values)
