"""
Entry Observer Module.

Trailing Entry logic for Statistical Arbitrage.
Monitors entry candidates in real-time and enters only after reversal confirmation.

Components:
- EntryObserverService: Main service for trailing entry logic
- WatchCandidate: State model for a monitored entry candidate
"""

from .models import WatchCandidate
from .entry_observer import EntryObserverService

__all__ = [
    "EntryObserverService",
    "WatchCandidate",
]
