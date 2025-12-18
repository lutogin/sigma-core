"""
Event Definitions.

Typed events for the trading bot event system.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class EventType(str, Enum):
    """Event types."""

    # Signals from OpportunityObserver
    POSITION_ENTRY = "position_entry"
    POSITION_EXIT = "position_exit"
    OPPORTUNITY_ENTRY = "opportunity_entry"
    OPPORTUNITY_EXIT = "opportunity_exit"

    # Trade lifecycle events
    TRADE_OPENED = "trade_opened"
    TRADE_CLOSED = "trade_closed"
    TRADE_CLOSED_PARTIALLY = "trade_closed_partially"
    TRADE_SKIPPED = "trade_skipped"
    TRADE_FAILED = "trade_failed"
    TRADE_CLOSE_ERROR = "trade_close_error"


class PositionSide(str, Enum):
    """Position side for entry."""

    LONG = "long"  # Buy Y, Sell X (z < -threshold)
    SHORT = "short"  # Sell Y, Buy X (z > +threshold)


class ExitReason(str, Enum):
    """Reason for position exit."""

    TAKE_PROFIT = "take_profit"  # Mean reversion complete (|z| < exit_threshold)
    STOP_LOSS = "stop_loss"  # Extreme divergence (|z| > stop_loss_threshold)
    MANUAL = "manual"  # Manual close
    PAIR_EXPIRED = "pair_expired"  # Pair no longer observed


@dataclass
class BaseEvent:
    """Base event class."""

    event_type: EventType
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class TradeEntryEvent(BaseEvent):
    """
    Signal to open a trade.

    Emitted when z-score crosses entry threshold:
    - z > +entry_threshold → SHORT spread
    - z < -entry_threshold → LONG spread
    """

    event_type: EventType = field(default=EventType.POSITION_ENTRY, init=False)

    # Pair info
    pair_id: str = ""
    symbol_x: str = ""
    symbol_y: str = ""

    # Position direction
    side: PositionSide = PositionSide.LONG

    # Market data at signal time
    z_score: float = 0.0
    beta: float = 1.0
    alpha: float = 0.0
    spread: float = 0.0

    # Current prices
    price_x: float = 0.0
    price_y: float = 0.0

    # Kalman stats
    observations: int = 0

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "pair_id": self.pair_id,
                "symbol_x": self.symbol_x,
                "symbol_y": self.symbol_y,
                "side": self.side.value,
                "z_score": round(self.z_score, 4),
                "beta": round(self.beta, 6),
                "alpha": round(self.alpha, 6),
                "spread": round(self.spread, 6),
                "price_x": self.price_x,
                "price_y": self.price_y,
                "observations": self.observations,
            }
        )
        return base


@dataclass
class TradeExitEvent(BaseEvent):
    """
    Signal to close a trade.

    Emitted when:
    - |z| < exit_threshold → TAKE_PROFIT (mean reversion complete)
    - |z| > stop_loss_threshold → STOP_LOSS (extreme divergence)
    """

    event_type: EventType = field(default=EventType.POSITION_EXIT, init=False)

    # Pair info
    pair_id: str = ""
    symbol_x: str = ""
    symbol_y: str = ""

    # Exit reason
    reason: ExitReason = ExitReason.TAKE_PROFIT

    # Position that is being closed
    position_side: PositionSide = PositionSide.LONG

    # Market data at signal time
    z_score: float = 0.0
    beta: float = 1.0
    spread: float = 0.0

    # Entry data (for P&L calculation)
    entry_z_score: float = 0.0
    entry_price_x: float = 0.0
    entry_price_y: float = 0.0

    # Current prices
    price_x: float = 0.0
    price_y: float = 0.0

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "pair_id": self.pair_id,
                "symbol_x": self.symbol_x,
                "symbol_y": self.symbol_y,
                "reason": self.reason.value,
                "position_side": self.position_side.value,
                "z_score": round(self.z_score, 4),
                "beta": round(self.beta, 6),
                "spread": round(self.spread, 6),
                "entry_z_score": round(self.entry_z_score, 4),
                "price_x": self.price_x,
                "price_y": self.price_y,
            }
        )
        return base


@dataclass
class OpportunityEntryEvent(BaseEvent):
    """Signal emitted by ObserverCron prior to trading filter."""

    event_type: EventType = field(default=EventType.OPPORTUNITY_ENTRY, init=False)

    # Pair info
    pair_id: str = ""
    symbol_x: str = ""
    symbol_y: str = ""

    # Position direction
    side: PositionSide = PositionSide.LONG

    # Market data at signal time
    z_score: float = 0.0
    beta: float = 1.0
    alpha: float = 0.0
    spread: float = 0.0

    # Current prices
    price_x: float = 0.0
    price_y: float = 0.0

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "pair_id": self.pair_id,
                "symbol_x": self.symbol_x,
                "symbol_y": self.symbol_y,
                "side": self.side.value,
                "z_score": round(self.z_score, 4),
                "beta": round(self.beta, 6),
                "alpha": round(self.alpha, 6),
                "spread": round(self.spread, 6),
                "price_x": self.price_x,
                "price_y": self.price_y,
            }
        )
        return base


@dataclass
class OpportunityExitEvent(BaseEvent):
    """Exit signal emitted by ObserverCron prior to trading filter."""

    event_type: EventType = field(default=EventType.OPPORTUNITY_EXIT, init=False)

    # Pair info
    pair_id: str = ""
    symbol_x: str = ""
    symbol_y: str = ""

    # Exit reason
    reason: ExitReason = ExitReason.TAKE_PROFIT

    # Position that is being closed
    position_side: PositionSide = PositionSide.LONG

    # Market data at signal time
    z_score: float = 0.0
    beta: float = 1.0
    spread: float = 0.0

    # Entry data (for P&L calculation)
    entry_z_score: float = 0.0
    entry_price_x: float = 0.0
    entry_price_y: float = 0.0

    # Current prices
    price_x: float = 0.0
    price_y: float = 0.0

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "pair_id": self.pair_id,
                "symbol_x": self.symbol_x,
                "symbol_y": self.symbol_y,
                "reason": self.reason.value,
                "position_side": self.position_side.value,
                "z_score": round(self.z_score, 4),
                "beta": round(self.beta, 6),
                "spread": round(self.spread, 6),
                "entry_z_score": round(self.entry_z_score, 4),
                "price_x": self.price_x,
                "price_y": self.price_y,
            }
        )
        return base


# =============================================================================
# Trade Lifecycle Events
# =============================================================================


class TradeSkipReason(str, Enum):
    """Reason why a trade was skipped."""

    MAX_POSITIONS_REACHED = "max_positions_reached"
    SYMBOL_OVERLAP = "symbol_overlap"
    INSUFFICIENT_BALANCE = "insufficient_balance"
    TRADING_DISABLED = "trading_disabled"


@dataclass
class TradeOpenedEvent(BaseEvent):
    """
    Emitted when a trade (2 positions) is successfully opened.
    """

    event_type: EventType = field(default=EventType.TRADE_OPENED, init=False)

    # Pair info
    pair_id: str = ""
    symbol_x: str = ""
    symbol_y: str = ""

    # Position direction (from entry signal)
    side: PositionSide = PositionSide.LONG

    # Order IDs
    order_id_x: str = ""
    order_id_y: str = ""

    # Executed sizes
    size_x: float = 0.0
    size_y: float = 0.0

    # Executed prices
    price_x: float = 0.0
    price_y: float = 0.0

    # Signal data
    z_score: float = 0.0
    beta: float = 1.0

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "pair_id": self.pair_id,
                "symbol_x": self.symbol_x,
                "symbol_y": self.symbol_y,
                "side": self.side.value,
                "order_id_x": self.order_id_x,
                "order_id_y": self.order_id_y,
                "size_x": self.size_x,
                "size_y": self.size_y,
                "price_x": self.price_x,
                "price_y": self.price_y,
                "z_score": round(self.z_score, 4),
                "beta": round(self.beta, 6),
            }
        )
        return base


@dataclass
class TradeClosedEvent(BaseEvent):
    """
    Emitted when a trade (2 positions) is fully closed.
    """

    event_type: EventType = field(default=EventType.TRADE_CLOSED, init=False)

    # Pair info
    pair_id: str = ""
    symbol_x: str = ""
    symbol_y: str = ""

    # Close order IDs
    order_id_x: str = ""
    order_id_y: str = ""

    # Exit reason
    reason: ExitReason = ExitReason.TAKE_PROFIT

    # Signal data at close
    z_score: float = 0.0

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "pair_id": self.pair_id,
                "symbol_x": self.symbol_x,
                "symbol_y": self.symbol_y,
                "order_id_x": self.order_id_x,
                "order_id_y": self.order_id_y,
                "reason": self.reason.value,
                "z_score": round(self.z_score, 4),
            }
        )
        return base


@dataclass
class TradeClosedPartiallyEvent(BaseEvent):
    """
    Emitted when only one leg of a trade was closed.
    """

    event_type: EventType = field(default=EventType.TRADE_CLOSED_PARTIALLY, init=False)

    # Pair info
    pair_id: str = ""
    symbol_x: str = ""
    symbol_y: str = ""

    # Which leg was closed
    closed_symbol: str = ""
    closed_order_id: str = ""

    # Which leg is still open (missing)
    missing_symbol: str = ""

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "pair_id": self.pair_id,
                "symbol_x": self.symbol_x,
                "symbol_y": self.symbol_y,
                "closed_symbol": self.closed_symbol,
                "closed_order_id": self.closed_order_id,
                "missing_symbol": self.missing_symbol,
            }
        )
        return base


@dataclass
class TradeSkippedEvent(BaseEvent):
    """
    Emitted when a trade signal is skipped.
    """

    event_type: EventType = field(default=EventType.TRADE_SKIPPED, init=False)

    # Pair info
    pair_id: str = ""
    symbol_x: str = ""
    symbol_y: str = ""

    # Why skipped
    reason: TradeSkipReason = TradeSkipReason.MAX_POSITIONS_REACHED
    details: str = ""

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "pair_id": self.pair_id,
                "symbol_x": self.symbol_x,
                "symbol_y": self.symbol_y,
                "reason": self.reason.value,
                "details": self.details,
            }
        )
        return base


@dataclass
class TradeFailedEvent(BaseEvent):
    """
    Emitted when a trade fails to open (e.g., one leg failed).
    """

    event_type: EventType = field(default=EventType.TRADE_FAILED, init=False)

    # Pair info
    pair_id: str = ""
    symbol_x: str = ""
    symbol_y: str = ""

    # Error details
    error_message: str = ""
    failed_symbol: str = ""

    # Rollback info (if applicable)
    rollback_order_id: Optional[str] = None

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "pair_id": self.pair_id,
                "symbol_x": self.symbol_x,
                "symbol_y": self.symbol_y,
                "error_message": self.error_message,
                "failed_symbol": self.failed_symbol,
                "rollback_order_id": self.rollback_order_id,
            }
        )
        return base


@dataclass
class TradeCloseErrorEvent(BaseEvent):
    """
    Emitted when a trade close operation fails.
    """

    event_type: EventType = field(default=EventType.TRADE_CLOSE_ERROR, init=False)

    # Pair info
    pair_id: str = ""
    symbol_x: str = ""
    symbol_y: str = ""

    # Error details
    error_message: str = ""
    failed_symbol: str = ""

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "pair_id": self.pair_id,
                "symbol_x": self.symbol_x,
                "symbol_y": self.symbol_y,
                "error_message": self.error_message,
                "failed_symbol": self.failed_symbol,
            }
        )
        return base
