"""
Event Emitter Infrastructure Layer.

Centralized event system for the stat-arb trading bot.
Abstraction layer over pyee for easy replacement.

Events:
- PendingEntrySignalEvent: Entry candidate detected, start trailing entry watch
- WatchStartedEvent: New watch added by EntryObserver (for notifications)
- EntrySignalEvent: Signal to open a spread position (after reversal confirmation)
- ExitSignalEvent: Signal to close a spread position (TP, SL, etc.)
- WatchCancelledEvent: Watch cancelled without entry (timeout, false alarm)
- ScanCompleteEvent: Emitted after each scan cycle
- MarketUnsafeEvent: Volatility filter triggered
"""

from .events import (
    # Core
    BaseEvent,
    EventType,
    # Enums
    SpreadSide,
    PositionSide,  # Legacy alias
    ExitReason,
    SignalSkipReason,
    TradeSkipReason,  # Legacy alias
    WatchCancelReason,
    # Trading signal events
    SpreadLeg,
    PendingEntrySignalEvent,
    WatchStartedEvent,
    EntrySignalEvent,
    ExitSignalEvent,
    SignalSkippedEvent,
    WatchCancelledEvent,
    WatchTimeoutCooldownEvent,
    # Trade lifecycle events
    TradeOpenedEvent,
    TradeClosedEvent,
    TradeFailedEvent,
    TradeCloseErrorEvent,
    # System events
    MarketUnsafeEvent,
    ScanCompleteEvent,
    ErrorEvent,
)
from .emitter import EventEmitter

__all__ = [
    # Core
    "BaseEvent",
    "EventType",
    "EventEmitter",
    # Enums
    "SpreadSide",
    "PositionSide",
    "ExitReason",
    "SignalSkipReason",
    "TradeSkipReason",
    "WatchCancelReason",
    # Trading signal events
    "SpreadLeg",
    "PendingEntrySignalEvent",
    "WatchStartedEvent",
    "EntrySignalEvent",
    "ExitSignalEvent",
    "SignalSkippedEvent",
    "WatchCancelledEvent",
    "WatchTimeoutCooldownEvent",
    # Trade lifecycle events
    "TradeOpenedEvent",
    "TradeClosedEvent",
    "TradeFailedEvent",
    "TradeCloseErrorEvent",
    # System events
    "MarketUnsafeEvent",
    "ScanCompleteEvent",
    "ErrorEvent",
]
