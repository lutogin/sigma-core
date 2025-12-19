"""
Event Emitter Infrastructure Layer.

Centralized event system for the stat-arb trading bot.
Abstraction layer over pyee for easy replacement.

Events:
- EntrySignalEvent: Signal to open a spread position (contains both legs)
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
    # Spread position events
    SpreadLeg,
    EntrySignalEvent,
    SignalSkippedEvent,
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
    # Spread events
    "SpreadLeg",
    "EntrySignalEvent",
    "SignalSkippedEvent",
    # System events
    "MarketUnsafeEvent",
    "ScanCompleteEvent",
    "ErrorEvent",
]
