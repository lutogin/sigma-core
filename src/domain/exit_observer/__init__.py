"""
Exit Observer Module.

Real-time monitoring of open positions for TP/SL exit conditions.
"""

from src.domain.exit_observer.exit_observer import ExitObserverService
from src.domain.exit_observer.models import ExitWatch, ExitWatchStatus

__all__ = [
    "ExitObserverService",
    "ExitWatch",
    "ExitWatchStatus",
]
