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

    # Trading signals
    PENDING_ENTRY_SIGNAL = (
        "pending_entry_signal"  # Entry candidate detected, start watching
    )
    ENTRY_SIGNAL = (
        "entry_signal"  # Valid entry signal detected (after reversal confirmation)
    )
    EXIT_SIGNAL = "exit_signal"  # Exit signal detected (TP, SL, etc.)
    NO_SIGNAL = "no_signal"  # Scan complete, no valid signals
    WATCH_CANCELLED = (
        "watch_cancelled"  # Watch cancelled without entry (timeout/false alarm)
    )
    WATCH_TIMEOUT_COOLDOWN = (
        "watch_timeout_cooldown"  # Watch timed out, apply cooldown to symbol
    )
    WATCH_STARTED = (
        "watch_started"  # New watch added by EntryObserver (for notifications)
    )

    # Position management (for future TradeExecutor)
    POSITION_OPENED = "position_opened"
    POSITION_CLOSED = "position_closed"
    POSITION_FAILED = "position_failed"

    # Trade lifecycle events (from TradingService)
    TRADE_OPENED = "trade_opened"  # Spread trade successfully opened
    TRADE_CLOSED = "trade_closed"  # Spread trade fully closed
    TRADE_FAILED = "trade_failed"  # Failed to open spread
    TRADE_CLOSE_ERROR = "trade_close_error"  # Error closing spread

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


class WatchCancelReason(str, Enum):
    """Reason why a watch was cancelled without entry."""

    TIMEOUT = "timeout"  # Watch exceeded max duration (45 min)
    FALSE_ALARM = "false_alarm"  # Z-score returned to normal without entry
    SL_HIT = "sl_hit"  # Z-score exceeded stop-loss threshold
    ALREADY_WATCHING = "already_watching"  # Symbol is already being watched
    MAX_WATCHES_REACHED = "max_watches_reached"  # Too many concurrent watches
    CORRELATION_DROP = "correlation_drop"  # Correlation dropped below threshold
    HURST_TRENDING = "hurst_trending"  # Spread became trending (Hurst >= threshold)


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

    # Mean-reversion quality metrics
    hurst: float = 0.0  # Hurst exponent (< 0.45 = mean-reverting)
    halflife: float = 0.0  # Half-life in bars (used for dynamic position sizing)

    # Spread statistics for real-time Z calculation
    spread_mean: float = 0.0
    spread_std: float = 0.0

    # Current prices (for reference)
    coin_price: float = 0.0
    primary_price: float = 0.0

    # Thresholds for exit (for executor to know)
    z_tp_threshold: float = 0.0  # Take profit when |Z| <= this
    z_sl_threshold: float = 4.0  # Stop loss when |Z| >= this

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
                "halflife": self.halflife,
                "spread_mean": self.spread_mean,
                "spread_std": self.spread_std,
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
            size_usdt=0.0,  # Size calculated by TradingService
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
            size_usdt=0.0,  # Size calculated by TradingService
            price=self.primary_price,
        )


# =============================================================================
# Pending Entry Signal Event (for Trailing Entry logic)
# =============================================================================


@dataclass
class PendingEntrySignalEvent(BaseEvent):
    """
    Pending entry signal event - entry candidate for trailing entry observation.

    Emitted by OrchestratorService when Z-score crosses entry threshold.
    The EntryObserverService subscribes to start live monitoring for reversal.

    Contains all data needed to calculate live Z-score:
    - Spread mean and std for Z calculation
    - Beta for spread calculation
    - Current prices as baseline
    """

    event_type: EventType = field(default=EventType.PENDING_ENTRY_SIGNAL, init=False)

    # Symbol info
    coin_symbol: str = ""  # e.g., "ARB/USDT:USDT"
    primary_symbol: str = ""  # e.g., "ETH/USDT:USDT"

    # Spread direction
    spread_side: SpreadSide = SpreadSide.LONG  # LONG or SHORT spread

    # Signal metrics at detection time
    z_score: float = 0.0  # Z-score when threshold was crossed
    beta: float = 0.0  # Hedge ratio (β) for spread calculation
    correlation: float = 0.0  # Current correlation
    hurst: float = 0.0  # Hurst exponent
    halflife: float = 0.0  # Half-life in bars (used for dynamic position sizing)

    # Spread statistics for live Z-score calculation
    # (last values from rolling window)
    spread_mean: float = 0.0  # Rolling mean of spread
    spread_std: float = 0.0  # Rolling std of spread

    # Current prices (for reference)
    coin_price: float = 0.0
    primary_price: float = 0.0

    # Thresholds
    z_entry_threshold: float = 2.1  # Entry threshold
    z_tp_threshold: float = 0.25  # Take profit when |Z| <= this
    z_sl_threshold: float = 4.0  # Stop loss when |Z| >= this

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
                "halflife": self.halflife,
                "spread_mean": self.spread_mean,
                "spread_std": self.spread_std,
                "coin_price": self.coin_price,
                "primary_price": self.primary_price,
                "z_entry_threshold": self.z_entry_threshold,
                "z_tp_threshold": self.z_tp_threshold,
                "z_sl_threshold": self.z_sl_threshold,
            }
        )
        return base


# =============================================================================
# Watch Started Event (for notifications - emitted only on NEW watch)
# =============================================================================


@dataclass
class WatchStartedEvent(BaseEvent):
    """
    Watch started event - emitted only when EntryObserver adds a NEW watch.

    Unlike PendingEntrySignalEvent which is emitted every 15m scan,
    this event is emitted ONLY once when a symbol is first added to watch list.
    Used for Telegram notifications to avoid spam.

    Contains same data as PendingEntrySignalEvent.
    """

    event_type: EventType = field(default=EventType.WATCH_STARTED, init=False)

    # Symbol info
    coin_symbol: str = ""  # e.g., "ARB/USDT:USDT"
    primary_symbol: str = ""  # e.g., "ETH/USDT:USDT"

    # Spread direction
    spread_side: SpreadSide = SpreadSide.LONG  # LONG or SHORT spread

    # Signal metrics at detection time
    z_score: float = 0.0  # Z-score when threshold was crossed
    beta: float = 0.0  # Hedge ratio (β) for spread calculation
    correlation: float = 0.0  # Current correlation
    hurst: float = 0.0  # Hurst exponent
    halflife: float = 0.0  # Half-life in bars

    # Spread statistics for Z-score calculation
    spread_mean: float = 0.0  # Rolling mean of spread
    spread_std: float = 0.0  # Rolling std of spread

    # Current prices
    coin_price: float = 0.0
    primary_price: float = 0.0

    # Thresholds
    z_entry_threshold: float = 2.1
    z_tp_threshold: float = 0.25
    z_sl_threshold: float = 4.0

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
                "halflife": self.halflife,
                "spread_mean": self.spread_mean,
                "spread_std": self.spread_std,
                "coin_price": self.coin_price,
                "primary_price": self.primary_price,
                "z_entry_threshold": self.z_entry_threshold,
                "z_tp_threshold": self.z_tp_threshold,
                "z_sl_threshold": self.z_sl_threshold,
            }
        )
        return base


# =============================================================================
# Watch Cancelled Event
# =============================================================================


@dataclass
class WatchCancelledEvent(BaseEvent):
    """
    Event when a watch is cancelled without entry.

    Reasons:
    - TIMEOUT: Watch exceeded 45 minutes without reversal
    - FALSE_ALARM: Z-score returned to normal (<entry_threshold) without entry
    - SL_HIT: Z-score exceeded stop-loss threshold
    """

    event_type: EventType = field(default=EventType.WATCH_CANCELLED, init=False)

    # Symbol info
    coin_symbol: str = ""
    primary_symbol: str = ""

    # Cancellation reason
    reason: WatchCancelReason = WatchCancelReason.FALSE_ALARM

    # Stats at cancellation
    max_z_reached: float = 0.0  # Maximum |Z| during watch
    final_z: float = 0.0  # Z-score at cancellation
    watch_duration_seconds: float = 0.0  # How long we were watching

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "coin_symbol": self.coin_symbol,
                "primary_symbol": self.primary_symbol,
                "reason": self.reason.value,
                "max_z_reached": self.max_z_reached,
                "final_z": self.final_z,
                "watch_duration_seconds": self.watch_duration_seconds,
            }
        )
        return base


@dataclass
class WatchTimeoutCooldownEvent(BaseEvent):
    """
    Event when a watch times out and symbol should be placed in cooldown.

    This prevents immediate re-entry after a failed trailing entry attempt.
    The symbol will be blocked from new entries for the cooldown period.
    """

    event_type: EventType = field(default=EventType.WATCH_TIMEOUT_COOLDOWN, init=False)

    # Symbol info
    coin_symbol: str = ""
    primary_symbol: str = ""

    # Stats at timeout
    max_z_reached: float = 0.0
    final_z: float = 0.0
    watch_duration_seconds: float = 0.0

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "coin_symbol": self.coin_symbol,
                "primary_symbol": self.primary_symbol,
                "max_z_reached": self.max_z_reached,
                "final_z": self.final_z,
                "watch_duration_seconds": self.watch_duration_seconds,
            }
        )
        return base


# =============================================================================
# Exit Signal Event
# =============================================================================


@dataclass
class ExitSignalEvent(BaseEvent):
    """
    Exit signal event - contains all info needed to close a spread position.

    This is emitted by OrchestratorService when an exit condition is detected:
    - TAKE_PROFIT: |Z| <= tp_threshold (mean reversion complete)
    - STOP_LOSS: |Z| >= sl_threshold (extreme divergence)
    - CORRELATION_DROP: pair fell below correlation threshold
    - TIMEOUT: position held too long
    - HURST_TRENDING: spread became trending

    The TradingService should subscribe to this event and close the position.
    """

    event_type: EventType = field(default=EventType.EXIT_SIGNAL, init=False)

    # Symbol info
    coin_symbol: str = ""  # e.g., "ADA/USDT:USDT"
    primary_symbol: str = ""  # e.g., "ETH/USDT:USDT"

    # Exit reason
    exit_reason: ExitReason = ExitReason.MANUAL

    # Current metrics at exit
    current_z_score: float = 0.0  # Current Z-score
    current_correlation: float = 0.0  # Current correlation

    # Current prices (for reference/logging)
    coin_price: float = 0.0
    primary_price: float = 0.0

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "coin_symbol": self.coin_symbol,
                "primary_symbol": self.primary_symbol,
                "exit_reason": self.exit_reason.value,
                "current_z_score": self.current_z_score,
                "current_correlation": self.current_correlation,
                "coin_price": self.coin_price,
                "primary_price": self.primary_price,
            }
        )
        return base


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


# =============================================================================
# Trade Lifecycle Events (from TradingService)
# =============================================================================


@dataclass
class TradeOpenedEvent(BaseEvent):
    """Event when a spread trade is successfully opened."""

    event_type: EventType = field(default=EventType.TRADE_OPENED, init=False)

    # Symbol info
    coin_symbol: str = ""
    primary_symbol: str = ""

    # Trade details
    spread_side: SpreadSide = SpreadSide.LONG
    z_score: float = 0.0
    beta: float = 0.0
    correlation: float = 0.0
    hurst: float = 0.0
    halflife: float = 0.0  # Half-life in bars (used for dynamic position sizing)

    # Spread statistics for real-time Z calculation
    spread_mean: float = 0.0
    spread_std: float = 0.0

    # Position sizes
    coin_size_usdt: float = 0.0
    primary_size_usdt: float = 0.0

    # Fill prices
    coin_price: float = 0.0
    primary_price: float = 0.0

    # Order IDs
    coin_order_id: str = ""
    primary_order_id: str = ""

    # Thresholds
    z_tp_threshold: float = 0.25
    z_sl_threshold: float = 4.0

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
                "halflife": self.halflife,
                "spread_mean": self.spread_mean,
                "spread_std": self.spread_std,
                "coin_size_usdt": self.coin_size_usdt,
                "primary_size_usdt": self.primary_size_usdt,
                "coin_price": self.coin_price,
                "primary_price": self.primary_price,
                "coin_order_id": self.coin_order_id,
                "primary_order_id": self.primary_order_id,
            }
        )
        return base


@dataclass
class TradeClosedEvent(BaseEvent):
    """Event when a spread trade is fully closed."""

    event_type: EventType = field(default=EventType.TRADE_CLOSED, init=False)

    # Symbol info
    coin_symbol: str = ""
    primary_symbol: str = ""

    # Exit details
    exit_reason: ExitReason = ExitReason.MANUAL
    spread_side: SpreadSide = SpreadSide.LONG

    # Entry metrics (for P&L calculation)
    entry_z_score: float = 0.0
    exit_z_score: float = 0.0

    # Prices
    coin_entry_price: float = 0.0
    primary_entry_price: float = 0.0
    coin_exit_price: float = 0.0
    primary_exit_price: float = 0.0

    # Position sizes
    coin_size_usdt: float = 0.0
    primary_size_usdt: float = 0.0

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "coin_symbol": self.coin_symbol,
                "primary_symbol": self.primary_symbol,
                "exit_reason": self.exit_reason.value,
                "spread_side": self.spread_side.value,
                "entry_z_score": self.entry_z_score,
                "exit_z_score": self.exit_z_score,
                "coin_entry_price": self.coin_entry_price,
                "primary_entry_price": self.primary_entry_price,
                "coin_exit_price": self.coin_exit_price,
                "primary_exit_price": self.primary_exit_price,
                "coin_size_usdt": self.coin_size_usdt,
                "primary_size_usdt": self.primary_size_usdt,
            }
        )
        return base


@dataclass
class TradeFailedEvent(BaseEvent):
    """Event when a trade fails to open."""

    event_type: EventType = field(default=EventType.TRADE_FAILED, init=False)

    # Symbol info
    coin_symbol: str = ""
    primary_symbol: str = ""

    # Error details
    error_message: str = ""
    failed_leg: str = ""  # "coin", "primary", or "both"
    rollback_performed: bool = False

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "coin_symbol": self.coin_symbol,
                "primary_symbol": self.primary_symbol,
                "error_message": self.error_message,
                "failed_leg": self.failed_leg,
                "rollback_performed": self.rollback_performed,
            }
        )
        return base


@dataclass
class TradeCloseErrorEvent(BaseEvent):
    """Event when there's an error closing a trade."""

    event_type: EventType = field(default=EventType.TRADE_CLOSE_ERROR, init=False)

    # Symbol info
    coin_symbol: str = ""
    primary_symbol: str = ""

    # Exit context
    exit_reason: ExitReason = ExitReason.MANUAL

    # Error details
    error_message: str = ""
    coin_closed: bool = False
    primary_closed: bool = False

    def to_dict(self) -> dict:
        base = super().to_dict()
        base.update(
            {
                "coin_symbol": self.coin_symbol,
                "primary_symbol": self.primary_symbol,
                "exit_reason": self.exit_reason.value,
                "error_message": self.error_message,
                "coin_closed": self.coin_closed,
                "primary_closed": self.primary_closed,
            }
        )
        return base


# Legacy alias for backward compatibility
PositionSide = SpreadSide
TradeSkipReason = SignalSkipReason
