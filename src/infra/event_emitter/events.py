"""
Event Definitions for Statistical Arbitrage.

Typed events for the stat-arb trading bot event system.
All events are dataclasses with full type hints.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class EventType(str, Enum):
    """Event types for stat-arb system."""

    # Screener signals
    ENTRY_SIGNAL = "entry_signal"  # Valid entry signal detected
    NO_SIGNAL = "no_signal"  # Scan complete, no valid signals

    # Position management (for future TradeExecutor)
    POSITION_OPENED = "position_opened"
    POSITION_CLOSED = "position_closed"
    POSITION_FAILED = "position_failed"

    # Market conditions
    MARKET_UNSAFE = "market_unsafe"  # Volatility filter triggered
    MARKET_SAFE = "market_safe"

    # System
    SCAN_COMPLETE = "scan_complete"
    ERROR = "error"


class SpreadSide(str, Enum):
    """
    Spread position side.

    For stat-arb, we trade the SPREAD, not individual assets:
    - LONG spread: Buy COIN, Sell PRIMARY (when Z <= -entry, expect Z to rise to 0)
    - SHORT spread: Sell COIN, Buy PRIMARY (when Z >= +entry, expect Z to fall to 0)
    """

    LONG = "long"  # Z negative -> expect mean reversion up
    SHORT = "short"  # Z positive -> expect mean reversion down


class ExitReason(str, Enum):
    """Reason for position exit."""

    TAKE_PROFIT = "take_profit"  # Mean reversion complete (|Z| <= tp_threshold)
    STOP_LOSS = "stop_loss"  # Extreme divergence (|Z| >= sl_threshold)
    CORRELATION_DROP = "correlation_drop"  # Pair decoupled from primary
    TIMEOUT = "timeout"  # Position held too long
    HURST_TRENDING = "hurst_trending"  # Spread became trending
    MANUAL = "manual"  # Manual close


class SignalSkipReason(str, Enum):
    """Reason why an entry signal was skipped."""

    MAX_POSITIONS_REACHED = "max_positions_reached"
    COOLDOWN_ACTIVE = "cooldown_active"
    INSUFFICIENT_BALANCE = "insufficient_balance"
    HURST_TRENDING = "hurst_trending"  # Spread not mean-reverting
    MARKET_UNSAFE = "market_unsafe"  # Volatility filter
    TRADING_DISABLED = "trading_disabled"


# =============================================================================
# Base Event
# =============================================================================


@dataclass
class BaseEvent:
    """Base event class with timestamp."""

    event_type: EventType
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        """Convert event to dictionary for logging/serialization."""
        return {
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
        }


# =============================================================================
# Entry Signal Event (main event for trade execution)
# =============================================================================


@dataclass
class SpreadLeg:
    """
    One leg of a spread position.

    For stat-arb we have two legs:
    - COIN leg (the altcoin we're trading)
    - PRIMARY leg (ETH - the hedge)
    """

    symbol: str  # e.g., "ADA/USDT:USDT"
    side: str  # "buy" or "sell"
    size_usdt: float  # Position size in USDT
    price: float  # Current price for reference


@dataclass
class EntrySignalEvent(BaseEvent):
    """
    Entry signal event - contains all info needed to open a spread position.

    This is emitted by ScreenerService when a valid entry candidate is found.
    The TradeExecutor should subscribe to this event and execute the trade.
    """

    event_type: EventType = field(default=EventType.ENTRY_SIGNAL, init=False)

    # Symbol info
    coin_symbol: str = ""  # e.g., "ADA/USDT:USDT"
    primary_symbol: str = ""  # e.g., "ETH/USDT:USDT"

    # Spread direction
    spread_side: SpreadSide = SpreadSide.LONG  # LONG or SHORT spread

    # Signal metrics
    z_score: float = 0.0  # Current Z-score that triggered entry
    beta: float = 0.0  # Hedge ratio (β)
    correlation: float = 0.0  # Current correlation

    # Hurst exponent (mean-reversion quality)
    hurst: float = 0.0

    # Position sizing (suggested, executor may adjust)
    suggested_coin_size_usdt: float = 0.0
    suggested_primary_size_usdt: float = 0.0  # = coin_size * |beta|

    # Current prices (for reference)
    coin_price: float = 0.0
    primary_price: float = 0.0

    # Thresholds for exit (for executor to know)
    z_tp_threshold: float = 0.0  # Take profit when |Z| <= this
    z_sl_threshold: float = 4.5  # Stop loss when |Z| >= this

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "coin_symbol": self.coin_symbol,
                "primary_symbol": self.primary_symbol,
                "spread_side": self.spread_side.value,
                "z_score": self.z_score,
                "beta": self.beta,
                "correlation": self.correlation,
                "hurst": self.hurst,
                "suggested_coin_size_usdt": self.suggested_coin_size_usdt,
                "suggested_primary_size_usdt": self.suggested_primary_size_usdt,
                "coin_price": self.coin_price,
                "primary_price": self.primary_price,
                "z_tp_threshold": self.z_tp_threshold,
                "z_sl_threshold": self.z_sl_threshold,
            }
        )
        return base

    @property
    def coin_leg(self) -> SpreadLeg:
        """Get COIN leg details."""
        # Long spread = Buy COIN, Short spread = Sell COIN
        side = "buy" if self.spread_side == SpreadSide.LONG else "sell"
        return SpreadLeg(
            symbol=self.coin_symbol,
            side=side,
            size_usdt=self.suggested_coin_size_usdt,
            price=self.coin_price,
        )

    @property
    def primary_leg(self) -> SpreadLeg:
        """Get PRIMARY (hedge) leg details."""
        # Long spread = Sell PRIMARY, Short spread = Buy PRIMARY
        side = "sell" if self.spread_side == SpreadSide.LONG else "buy"
        return SpreadLeg(
            symbol=self.primary_symbol,
            side=side,
            size_usdt=self.suggested_primary_size_usdt,
            price=self.primary_price,
        )


# =============================================================================
# Other Events
# =============================================================================


@dataclass
class SignalSkippedEvent(BaseEvent):
    """Event when an entry signal was skipped."""

    event_type: EventType = field(default=EventType.NO_SIGNAL, init=False)

    coin_symbol: str = ""
    z_score: float = 0.0
    skip_reason: SignalSkipReason = SignalSkipReason.TRADING_DISABLED
    details: Optional[str] = None

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "coin_symbol": self.coin_symbol,
                "z_score": self.z_score,
                "skip_reason": self.skip_reason.value,
                "details": self.details,
            }
        )
        return base


@dataclass
class MarketUnsafeEvent(BaseEvent):
    """Event when market volatility is too high."""

    event_type: EventType = field(default=EventType.MARKET_UNSAFE, init=False)

    primary_symbol: str = ""
    volatility: float = 0.0
    volatility_threshold: float = 0.0
    change_4h: Optional[float] = None
    change_threshold: Optional[float] = None
    reason: str = ""

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "primary_symbol": self.primary_symbol,
                "volatility": self.volatility,
                "volatility_threshold": self.volatility_threshold,
                "change_4h": self.change_4h,
                "change_threshold": self.change_threshold,
                "reason": self.reason,
            }
        )
        return base


@dataclass
class ScanCompleteEvent(BaseEvent):
    """Event when scan is complete."""

    event_type: EventType = field(default=EventType.SCAN_COMPLETE, init=False)

    symbols_scanned: int = 0
    symbols_after_correlation_filter: int = 0
    entry_candidates: int = 0
    signals_emitted: int = 0

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "symbols_scanned": self.symbols_scanned,
                "symbols_after_correlation_filter": self.symbols_after_correlation_filter,
                "entry_candidates": self.entry_candidates,
                "signals_emitted": self.signals_emitted,
            }
        )
        return base


@dataclass
class ErrorEvent(BaseEvent):
    """Event for errors."""

    event_type: EventType = field(default=EventType.ERROR, init=False)

    error_type: str = ""
    message: str = ""
    details: Optional[str] = None

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "error_type": self.error_type,
                "message": self.message,
                "details": self.details,
            }
        )
        return base


# Legacy alias for backward compatibility
PositionSide = SpreadSide
TradeSkipReason = SignalSkipReason
