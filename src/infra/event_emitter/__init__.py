"""
Event Emitter Infrastructure Layer.

Centralized event system for the trading bot.
Abstraction layer over pyee for easy replacement.

Events:
- PositionEntryEvent: Signal to open a position
- PositionExitEvent: Signal to close a position (profit or stop-loss)
"""

from .events import (
    BaseEvent,
    TradeEntryEvent,
    TradeExitEvent,
    OpportunityEntryEvent,
    OpportunityExitEvent,
    EventType,
    PositionSide,
    ExitReason,
    # Trade lifecycle events
    TradeOpenedEvent,
    TradeClosedEvent,
    TradeClosedPartiallyEvent,
    TradeSkippedEvent,
    TradeFailedEvent,
    TradeCloseErrorEvent,
    TradeSkipReason,
)
from .emitter import EventEmitter

__all__ = [
    "BaseEvent",
    "TradeEntryEvent",
    "TradeExitEvent",
    "OpportunityEntryEvent",
    "OpportunityExitEvent",
    "EventType",
    "PositionSide",
    "ExitReason",
    "EventEmitter",
    # Trade lifecycle
    "TradeOpenedEvent",
    "TradeClosedEvent",
    "TradeClosedPartiallyEvent",
    "TradeSkippedEvent",
    "TradeFailedEvent",
    "TradeCloseErrorEvent",
    "TradeSkipReason",
]

