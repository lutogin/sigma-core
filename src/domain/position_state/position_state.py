"""
Position State Service.

Manages spread positions state, cooldowns, and timeouts.
Provides high-level API for position state management.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from src.domain.position_state.models import SpreadPosition, SymbolCooldown, SpreadSide
from src.domain.position_state.repository import PositionStateRepository
from src.infra.event_emitter import EventEmitter, ExitReason


class PositionStateService:
    """
    Service for managing spread position state.

    Responsibilities:
    - Track open positions with entry time
    - Manage symbol cooldowns after SL/CORRELATION_DROP
    - Check for position timeouts
    - Provide position state queries
    """

    def __init__(
        self,
        repository: PositionStateRepository,
        event_emitter: EventEmitter,
        logger,
        timeframe: str = "15m",
        cooldown_bars: int = 16,  # 4 hours for 15m timeframe
        max_position_bars: int = 244,  # ~61 hours for 15m timeframe
    ):
        """
        Initialize Position State Service.

        Args:
            repository: MongoDB repository for persistence.
            event_emitter: Event emitter for timeout events.
            logger: Application logger.
            timeframe: Trading timeframe (e.g., "15m").
            cooldown_bars: Number of bars for cooldown after SL/CORRELATION_DROP.
            max_position_bars: Maximum bars before position timeout.
        """
        self._repository = repository
        self._event_emitter = event_emitter
        self._logger = logger
        self._timeframe = timeframe
        self._cooldown_bars = cooldown_bars
        self._max_position_bars = max_position_bars

        # Calculate durations in minutes
        self._timeframe_minutes = self._get_timeframe_minutes(timeframe)
        self._cooldown_minutes = cooldown_bars * self._timeframe_minutes
        self._max_position_minutes = max_position_bars * self._timeframe_minutes

        self._logger.info(
            f"PositionStateService initialized | "
            f"timeframe={timeframe} | "
            f"cooldown={cooldown_bars} bars ({self._cooldown_minutes} min) | "
            f"max_position={max_position_bars} bars ({self._max_position_minutes} min)"
        )

    @staticmethod
    def _get_timeframe_minutes(timeframe: str) -> int:
        """Convert timeframe string to minutes."""
        multipliers = {"m": 1, "h": 60, "d": 1440}
        unit = timeframe[-1].lower()
        value = int(timeframe[:-1])
        return value * multipliers.get(unit, 1)

    # =========================================================================
    # Position Management
    # =========================================================================

    def register_position(
        self,
        coin_symbol: str,
        primary_symbol: str,
        side: SpreadSide,
        entry_z_score: float,
        entry_beta: float,
        entry_correlation: float,
        entry_hurst: float,
        coin_size_usdt: float,
        primary_size_usdt: float,
        coin_entry_price: float,
        primary_entry_price: float,
        z_tp_threshold: float,
        z_sl_threshold: float,
        coin_contracts: float = 0.0,
        primary_contracts: float = 0.0,
        leverage: int = 1,
    ) -> SpreadPosition:
        """
        Register a new open position.

        Args:
            coin_symbol: Coin symbol (e.g., "ADA/USDT:USDT").
            primary_symbol: Primary symbol (e.g., "ETH/USDT:USDT").
            side: Spread side (LONG or SHORT).
            entry_z_score: Z-score at entry.
            entry_beta: Beta (hedge ratio) at entry.
            entry_correlation: Correlation at entry.
            entry_hurst: Hurst exponent at entry.
            coin_size_usdt: COIN leg size in USDT.
            primary_size_usdt: PRIMARY leg size in USDT.
            coin_entry_price: COIN entry price.
            primary_entry_price: PRIMARY entry price.
            z_tp_threshold: Take profit Z threshold.
            z_sl_threshold: Stop loss Z threshold.
            coin_contracts: COIN leg size in contracts.
            primary_contracts: PRIMARY leg size in contracts.
            leverage: Position leverage.

        Returns:
            Created SpreadPosition.
        """
        position = SpreadPosition(
            coin_symbol=coin_symbol,
            primary_symbol=primary_symbol,
            side=side,
            entry_z_score=entry_z_score,
            entry_beta=entry_beta,
            entry_correlation=entry_correlation,
            entry_hurst=entry_hurst,
            coin_size_usdt=coin_size_usdt,
            primary_size_usdt=primary_size_usdt,
            coin_contracts=coin_contracts,
            primary_contracts=primary_contracts,
            coin_entry_price=coin_entry_price,
            primary_entry_price=primary_entry_price,
            z_tp_threshold=z_tp_threshold,
            z_sl_threshold=z_sl_threshold,
            leverage=leverage,
            opened_at=datetime.now(timezone.utc),
            is_active=True,
        )

        self._repository.save_position(position)

        self._logger.info(
            f"📝 Position registered | {coin_symbol} ({side.value}) | "
            f"Z={entry_z_score:.2f} | β={entry_beta:.3f} | "
            f"coin_contracts={coin_contracts:.6f} | primary_contracts={primary_contracts:.6f}"
        )

        return position

    def close_position(
        self,
        coin_symbol: str,
        exit_reason: ExitReason,
    ) -> Optional[SpreadPosition]:
        """
        Close (deactivate) a position and apply cooldown if needed.

        Args:
            coin_symbol: Coin symbol to close.
            exit_reason: Reason for closing.

        Returns:
            Closed SpreadPosition or None if not found.
        """
        position = self._repository.get_position_by_symbol(coin_symbol)
        if not position:
            self._logger.warning(f"Position not found for close: {coin_symbol}")
            return None

        # Deactivate position
        self._repository.deactivate_position(coin_symbol)

        # Apply cooldown for adverse exits (SL, CORRELATION_DROP, TIMEOUT, HURST_TRENDING)
        # These indicate the pair is not behaving as expected - prevent immediate re-entry
        cooldown_reasons = (
            ExitReason.STOP_LOSS,
            ExitReason.CORRELATION_DROP,
            ExitReason.TIMEOUT,
            ExitReason.HURST_TRENDING,
        )
        if exit_reason in cooldown_reasons:
            self._apply_cooldown(coin_symbol, exit_reason)

        self._logger.info(
            f"📝 Position closed | {coin_symbol} | "
            f"reason={exit_reason.value} | "
            f"duration={position.duration_hours():.1f}h"
        )

        return position

    def get_position(self, coin_symbol: str) -> Optional[SpreadPosition]:
        """Get active position for a coin symbol."""
        return self._repository.get_position_by_symbol(coin_symbol)

    def get_active_positions(self) -> List[SpreadPosition]:
        """Get all active positions."""
        return self._repository.get_active_positions()

    def count_active_positions(self) -> int:
        """Count active positions."""
        return self._repository.count_active_positions()

    def get_active_symbols(self) -> set:
        """Get set of all active symbols (both coin and primary)."""
        return self._repository.get_active_symbols()

    def get_active_coin_symbols(self) -> set:
        """Get set of active COIN symbols only (excluding PRIMARY)."""
        positions = self._repository.get_active_positions()
        return {pos.coin_symbol.lower() for pos in positions}

    def has_position(self, coin_symbol: str) -> bool:
        """Check if there's an active position for a coin symbol."""
        return self._repository.get_position_by_symbol(coin_symbol) is not None

    # =========================================================================
    # Cooldown Management
    # =========================================================================

    def _apply_cooldown(self, symbol: str, exit_reason: ExitReason) -> None:
        """Apply cooldown to a symbol after adverse exit (SL, CORRELATION_DROP, TIMEOUT, HURST_TRENDING)."""
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(minutes=self._cooldown_minutes)

        cooldown = SymbolCooldown(
            symbol=symbol,
            started_at=now,
            expires_at=expires_at,
            exit_reason=exit_reason.value,
        )

        self._repository.save_cooldown(cooldown)

        self._logger.warning(
            f"⏸️ Cooldown applied | {symbol} | "
            f"reason={exit_reason.value} | "
            f"expires={expires_at.strftime('%H:%M:%S')} | "
            f"duration={self._cooldown_minutes} min"
        )

    def is_in_cooldown(self, symbol: str) -> bool:
        """Check if a symbol is currently in cooldown."""
        return self._repository.is_in_cooldown(symbol)

    def get_cooldown_remaining(self, symbol: str) -> float:
        """
        Get remaining cooldown time in minutes.

        Returns:
            Remaining minutes, or 0 if not in cooldown.
        """
        cooldown = self._repository.get_cooldown(symbol)
        if cooldown is None:
            return 0
        return cooldown.remaining_minutes()

    def get_active_cooldowns(self) -> List[SymbolCooldown]:
        """Get all active cooldowns."""
        return self._repository.get_active_cooldowns()

    def cleanup_expired_cooldowns(self) -> int:
        """Remove expired cooldowns."""
        return self._repository.cleanup_expired_cooldowns()

    # =========================================================================
    # Timeout Management
    # =========================================================================

    def check_timeouts(self) -> List[Tuple[SpreadPosition, float]]:
        """
        Check for positions that have exceeded max duration.

        Returns:
            List of (position, duration_minutes) tuples for timed-out positions.
        """
        now = datetime.now(timezone.utc)
        positions = self._repository.get_active_positions()
        timed_out = []

        for position in positions:
            duration_minutes = position.duration_minutes(now)
            if duration_minutes >= self._max_position_minutes:
                timed_out.append((position, duration_minutes))
                self._logger.warning(
                    f"⏰ Position timeout detected | {position.coin_symbol} | "
                    f"duration={duration_minutes:.0f} min "
                    f"(max={self._max_position_minutes} min)"
                )

        return timed_out

    def get_positions_near_timeout(
        self,
        warning_threshold_pct: float = 0.9,
    ) -> List[Tuple[SpreadPosition, float, float]]:
        """
        Get positions approaching timeout.

        Args:
            warning_threshold_pct: Percentage of max duration to trigger warning.

        Returns:
            List of (position, duration_minutes, remaining_minutes) tuples.
        """
        now = datetime.now(timezone.utc)
        positions = self._repository.get_active_positions()
        warning_threshold = self._max_position_minutes * warning_threshold_pct
        near_timeout = []

        for position in positions:
            duration_minutes = position.duration_minutes(now)
            remaining = self._max_position_minutes - duration_minutes

            if duration_minutes >= warning_threshold:
                near_timeout.append((position, duration_minutes, remaining))

        return near_timeout

    # =========================================================================
    # Validation
    # =========================================================================

    def can_open_position(
        self,
        coin_symbol: str,
        primary_symbol: str,
        max_spreads: int,
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if a new position can be opened.

        Args:
            coin_symbol: Coin symbol to check.
            primary_symbol: Primary symbol to check.
            max_spreads: Maximum allowed spreads.

        Returns:
            Tuple of (can_open, reason_if_not).
        """
        # Check cooldown
        if self.is_in_cooldown(coin_symbol):
            remaining = self.get_cooldown_remaining(coin_symbol)
            return False, f"Symbol in cooldown ({remaining:.0f} min remaining)"

        # Check if already have position for this symbol
        if self.has_position(coin_symbol):
            return False, "Position already open for this symbol"

        # Check symbol overlap with existing positions
        # NOTE: We only check COIN overlap, not PRIMARY (ETH)
        # because PRIMARY is shared across all spreads
        active_coin_symbols = self.get_active_coin_symbols()
        coin_symbol_lower = coin_symbol.lower()

        if coin_symbol_lower in active_coin_symbols:
            return False, f"COIN symbol already in use: {coin_symbol}"

        # Check max spreads
        active_count = self.count_active_positions()
        if active_count >= max_spreads:
            return False, f"Max spreads reached ({active_count}/{max_spreads})"

        return True, None

    # =========================================================================
    # Initialization
    # =========================================================================

    def initialize(self) -> None:
        """Initialize service (create indexes, cleanup expired cooldowns)."""
        self._repository.create_indexes()
        self.cleanup_expired_cooldowns()
        self._logger.info("PositionStateService initialized")
