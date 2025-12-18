"""
Event Emitter.

Abstraction layer over pyee for centralized event management.
Easy to replace with another event library if needed.
"""

from typing import Callable, Any, Dict, List
from pyee.asyncio import AsyncIOEventEmitter

from .events import BaseEvent, EventType


class EventEmitter:
    """
    Centralized event emitter for the trading bot.
    
    Wraps pyee.AsyncIOEventEmitter for:
    - Type-safe event emission
    - Easy replacement with another library
    - Centralized event logging
    
    Usage:
        emitter = EventEmitter(logger)
        
        # Subscribe to events
        @emitter.on(EventType.POSITION_ENTRY)
        async def on_entry(event: PositionEntryEvent):
            print(f"Entry: {event.pair_id}")
        
        # Emit events
        await emitter.emit(entry_event)
    """
    
    def __init__(self, logger: Any = None):
        """
        Initialize event emitter.
        
        :param logger: Optional logger for event logging
        """
        self._emitter = AsyncIOEventEmitter()
        self._logger = logger
        self._event_count: Dict[str, int] = {}
    
    def on(self, event_type: EventType, handler: Callable = None):
        """
        Subscribe to an event type.
        
        Can be used as decorator:
            @emitter.on(EventType.POSITION_ENTRY)
            async def handler(event):
                ...
        
        Or directly:
            emitter.on(EventType.POSITION_ENTRY, handler)
        """
        return self._emitter.on(event_type.value, handler)
    
    def once(self, event_type: EventType, handler: Callable = None):
        """Subscribe to an event type (one-time)."""
        return self._emitter.once(event_type.value, handler)
    
    def off(self, event_type: EventType, handler: Callable = None):
        """Unsubscribe from an event type."""
        return self._emitter.remove_listener(event_type.value, handler)
    
    def remove_all_listeners(self, event_type: EventType = None):
        """Remove all listeners for an event type (or all)."""
        if event_type:
            self._emitter.remove_all_listeners(event_type.value)
        else:
            self._emitter.remove_all_listeners()
    
    def emit(self, event: BaseEvent) -> None:
        """
        Emit an event.
        
        :param event: Event to emit (must be a BaseEvent subclass)
        """
        event_type = event.event_type.value
        
        # Track event count
        self._event_count[event_type] = self._event_count.get(event_type, 0) + 1
        
        # Log event
        if self._logger:
            self._logger.debug(f"📤 Event: {event_type} | {event.to_dict()}")
        
        # Emit to subscribers
        self._emitter.emit(event_type, event)
    
    def get_listener_count(self, event_type: EventType) -> int:
        """Get number of listeners for an event type."""
        return len(self._emitter.listeners(event_type.value))
    
    def get_event_count(self, event_type: EventType = None) -> Dict[str, int]:
        """Get count of emitted events."""
        if event_type:
            return {event_type.value: self._event_count.get(event_type.value, 0)}
        return dict(self._event_count)
    
    def get_stats(self) -> Dict:
        """Get emitter statistics."""
        return {
            "events_emitted": self._event_count,
            "listeners": {
                et.value: self.get_listener_count(et)
                for et in EventType
            },
        }

