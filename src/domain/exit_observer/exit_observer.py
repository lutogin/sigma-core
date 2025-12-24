"""
Exit Observer Service.

Real-time monitoring of open positions for TP/SL exit conditions.

Instead of waiting for the next 15-minute scan to check exit conditions,
we monitor positions via WebSocket and exit immediately when TP/SL is hit.

Logic:
1. Subscribe to TradeOpenedEvent when a position is opened
2. Subscribe to WebSocket price updates for coin and primary
3. Recalculate Z-score in real-time (every 1 second with debounce)
4. Emit ExitSignalEvent when:
   - |Z| <= z_tp_threshold (take profit - mean reversion complete)
   - |Z| >= z_sl_threshold (stop loss - extreme divergence)
5. Unsubscribe when TradeClosedEvent is received

This improves exit timing by reacting immediately instead of waiting for scans.
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.domain.exit_observer.models import ExitWatch, ExitWatchStatus
from src.infra.event_emitter import (
    EventEmitter,
    EventType,
    TradeOpenedEvent,
    TradeClosedEvent,
    ExitSignalEvent,
    ExitReason,
    SpreadSide,
)
from src.integrations.exchange import BinanceClient


class ExitObserverService:
    """
    Service for real-time exit monitoring of open positions.

    Monitors open positions via WebSocket and emits ExitSignalEvent
    when TP or SL conditions are met.
    """

    def __init__(
        self,
        event_emitter: EventEmitter,
        exchange_client: BinanceClient,
        position_state_service,  # PositionStateService - avoid circular import
        logger: Any,
        primary_symbol: str = "ETH/USDT:USDT",
        debounce_seconds: float = 1.0,
    ):
        """
        Initialize Exit Observer Service.

        Args:
            event_emitter: Event emitter for pub/sub.
            exchange_client: Binance client for WebSocket subscriptions.
            position_state_service: Service for accessing stored positions.
            logger: Logger instance.
            primary_symbol: Primary trading pair (e.g., "ETH/USDT:USDT").
            debounce_seconds: Minimum time between Z-score checks.
        """
        self._emitter = event_emitter
        self._exchange = exchange_client
        self._position_state = position_state_service
        self._logger = logger

        self._primary_symbol = primary_symbol
        self._debounce = debounce_seconds

        # Active watches: coin_symbol -> ExitWatch
        self._watches: Dict[str, ExitWatch] = {}

        # WebSocket tasks: coin_symbol -> asyncio.Task
        self._ws_tasks: Dict[str, asyncio.Task] = {}

        # Shared primary price (ETH always needed)
        self._primary_price: float = 0.0
        self._primary_ws_task: Optional[asyncio.Task] = None

        # Debounce control: coin_symbol -> last_check_timestamp
        self._last_check: Dict[str, float] = {}

        # Lock for thread-safe operations
        self._lock = asyncio.Lock()

        self._is_running = False

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> None:
        """Start the Exit Observer service."""
        if self._is_running:
            self._logger.warning("ExitObserverService is already running")
            return

        # Subscribe to trade events
        self._emitter.on(EventType.TRADE_OPENED, self._on_trade_opened)
        self._emitter.on(EventType.TRADE_CLOSED, self._on_trade_closed)

        self._is_running = True

        # Restore monitoring for existing positions (after deploy)
        await self._restore_from_positions()

        self._logger.info(
            f"🎯 ExitObserverService started | "
            f"debounce={self._debounce}s | "
            f"active_watches={len(self._watches)}"
        )

    async def _restore_from_positions(self) -> None:
        """
        Restore exit monitoring for existing open positions.

        Called at startup to resume monitoring after deploy/restart.
        Reads active positions from PositionStateService and creates
        WebSocket subscriptions for each.
        """
        try:
            positions = self._position_state.get_active_positions()

            if not positions:
                self._logger.info("No active positions to restore for exit monitoring")
                return

            self._logger.info(
                f"🔄 Restoring exit monitoring for {len(positions)} positions..."
            )

            for position in positions:
                coin = position.coin_symbol

                # Skip if already watching (shouldn't happen, but safety check)
                if coin in self._watches:
                    continue

                # Skip if spread_mean/spread_std not available (old positions)
                if position.spread_std == 0:
                    self._logger.warning(
                        f"⚠️ Cannot restore {coin} - missing spread statistics"
                    )
                    continue

                # Create ExitWatch from stored position
                watch = ExitWatch(
                    coin_symbol=coin,
                    primary_symbol=position.primary_symbol,
                    spread_side=position.side.value,
                    beta=position.entry_beta,
                    spread_mean=position.spread_mean,
                    spread_std=position.spread_std,
                    entry_z_score=position.entry_z_score,
                    correlation=position.entry_correlation,
                    hurst=position.entry_hurst,
                    z_tp_threshold=position.z_tp_threshold,
                    z_sl_threshold=position.z_sl_threshold,
                    coin_price=position.coin_entry_price,
                    primary_price=position.primary_entry_price,
                )

                self._watches[coin] = watch

                # Subscribe to WebSocket
                await self._subscribe_coin(coin)

                self._logger.info(
                    f"✅ Restored exit monitoring for {coin} | "
                    f"entry_Z={position.entry_z_score:.2f} | "
                    f"TP={position.z_tp_threshold:.2f} | SL={position.z_sl_threshold:.2f}"
                )

            # Subscribe to primary if we have watches
            if self._watches and self._primary_ws_task is None:
                await self._subscribe_primary()

        except Exception as e:
            self._logger.error(f"Failed to restore exit monitoring: {e}")

    async def stop(self) -> None:
        """Stop the Exit Observer service."""
        if not self._is_running:
            return

        # Unsubscribe from events
        self._emitter.off(EventType.TRADE_OPENED, self._on_trade_opened)
        self._emitter.off(EventType.TRADE_CLOSED, self._on_trade_closed)

        # Cancel all WebSocket tasks
        for task in self._ws_tasks.values():
            task.cancel()
        self._ws_tasks.clear()

        if self._primary_ws_task:
            self._primary_ws_task.cancel()
            self._primary_ws_task = None

        # Clear state
        self._watches.clear()
        self._last_check.clear()

        self._is_running = False
        self._logger.info("🎯 ExitObserverService stopped")

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._is_running

    @property
    def active_watch_count(self) -> int:
        """Get number of active watches."""
        return len(self._watches)

    def get_active_watches(self) -> Dict[str, ExitWatch]:
        """Get all active watches."""
        return dict(self._watches)

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _on_trade_opened(self, event: TradeOpenedEvent) -> None:
        """
        Handle TradeOpenedEvent - start monitoring for exit conditions.

        Args:
            event: Trade opened event with position details.
        """
        coin = event.coin_symbol

        async with self._lock:
            # Check if already watching this symbol
            if coin in self._watches:
                self._logger.warning(f"Already watching {coin} for exit, updating")
                # Update existing watch
                await self._cleanup_watch(coin)

            # Create ExitWatch
            watch = ExitWatch(
                coin_symbol=coin,
                primary_symbol=event.primary_symbol,
                spread_side=event.spread_side.value,
                beta=event.beta,
                spread_mean=event.spread_mean,
                spread_std=event.spread_std,
                entry_z_score=event.z_score,
                correlation=event.correlation,
                hurst=event.hurst,
                z_tp_threshold=event.z_tp_threshold,
                z_sl_threshold=event.z_sl_threshold,
                coin_price=event.coin_price,
                primary_price=event.primary_price,
            )

            # Store in memory
            self._watches[coin] = watch

            self._logger.info(
                f"👁️ Started exit monitoring {coin} | "
                f"entry_Z={event.z_score:.2f} | "
                f"TP={event.z_tp_threshold:.2f} | SL={event.z_sl_threshold:.2f} | "
                f"side={event.spread_side.value}"
            )

        # Subscribe to WebSocket feeds (outside lock to avoid blocking)
        await self._subscribe_coin(coin)

        # Subscribe to primary if not already
        if self._primary_ws_task is None:
            await self._subscribe_primary()

    async def _on_trade_closed(self, event: TradeClosedEvent) -> None:
        """
        Handle TradeClosedEvent - stop monitoring this position.

        Args:
            event: Trade closed event.
        """
        coin = event.coin_symbol

        if coin not in self._watches:
            return

        self._logger.info(
            f"🔒 Position closed externally, stopping exit monitor for {coin}"
        )
        await self._cleanup_watch(coin)

    # =========================================================================
    # WebSocket Management
    # =========================================================================

    async def _subscribe_coin(self, coin: str) -> None:
        """Subscribe to book ticker for a coin symbol."""

        async def on_update(data: Dict[str, Any]) -> None:
            """Callback for coin price updates."""
            try:
                watch = self._watches.get(coin)
                if not watch:
                    return

                # Extract prices, handle None/0 values
                bid_price = data.get("bid_price")
                ask_price = data.get("ask_price")

                # Skip if bid or ask is None/0 (partial update from WS)
                if not bid_price or not ask_price:
                    return

                # Calculate mid price
                mid_price = (float(bid_price) + float(ask_price)) / 2
                if mid_price <= 0:
                    return

                watch.coin_price = mid_price
                watch.last_update_at = datetime.now(timezone.utc)
                await self._process_price_update(coin)

            except Exception as e:
                self._logger.error(f"Error in exit WS callback for {coin}: {e}")

        try:
            task = await self._exchange.subscribe_book_ticker(coin, on_update)
            self._ws_tasks[coin] = task
            self._logger.debug(f"Subscribed to book ticker for {coin} (exit monitor)")
        except Exception as e:
            self._logger.error(f"Failed to subscribe to {coin} for exit: {e}")

    async def _subscribe_primary(self) -> None:
        """Subscribe to book ticker for primary symbol (ETH)."""

        async def on_primary_update(data: Dict[str, Any]) -> None:
            """Callback for primary price updates."""
            try:
                # Extract prices, handle None/0 values
                bid_price = data.get("bid_price")
                ask_price = data.get("ask_price")

                # Skip if bid or ask is None/0 (partial update from WS)
                if not bid_price or not ask_price:
                    return

                # Calculate mid price
                mid_price = (float(bid_price) + float(ask_price)) / 2
                if mid_price <= 0:
                    return

                self._primary_price = mid_price

                # Update primary price in all watches
                for watch in self._watches.values():
                    watch.primary_price = self._primary_price

            except Exception as e:
                self._logger.error(
                    f"Error in primary WS callback for {self._primary_symbol}: {e}"
                )

        try:
            self._primary_ws_task = await self._exchange.subscribe_book_ticker(
                self._primary_symbol, on_primary_update
            )
            self._logger.debug(
                f"Subscribed to book ticker for {self._primary_symbol} (exit monitor)"
            )
        except Exception as e:
            self._logger.error(f"Failed to subscribe to {self._primary_symbol}: {e}")

    async def _unsubscribe_coin(self, coin: str) -> None:
        """Unsubscribe from coin WebSocket."""
        if coin in self._ws_tasks:
            self._ws_tasks[coin].cancel()
            del self._ws_tasks[coin]
            self._logger.debug(f"Unsubscribed from {coin} (exit monitor)")

        # If no more watches, unsubscribe from primary too
        if not self._watches and self._primary_ws_task:
            self._primary_ws_task.cancel()
            self._primary_ws_task = None
            self._logger.debug(
                f"Unsubscribed from {self._primary_symbol} (exit monitor)"
            )

    # =========================================================================
    # Core Logic - Price Update Processing
    # =========================================================================

    async def _process_price_update(self, coin: str) -> None:
        """
        Process a price update for a coin.

        Checks exit conditions:
        1. Apply debounce (min 1 second between checks)
        2. Calculate live Z-score
        3. Check TP condition: |Z| <= z_tp_threshold
        4. Check SL condition: |Z| >= z_sl_threshold
        """
        now = time.time()

        # Debounce - check no more than once per second
        if coin in self._last_check:
            if now - self._last_check[coin] < self._debounce:
                return

        self._last_check[coin] = now

        watch = self._watches.get(coin)
        if not watch:
            return

        # Skip if prices not ready
        if watch.coin_price <= 0 or watch.primary_price <= 0:
            return

        # Calculate current Z-score
        live_z = watch.current_z_score
        abs_z = abs(live_z)

        # 1. Check TAKE PROFIT condition
        if abs_z <= watch.z_tp_threshold:
            self._logger.info(
                f"✅ {coin} TP hit! | "
                f"entry_Z={watch.entry_z_score:.2f}, current_Z={live_z:.2f}, "
                f"threshold={watch.z_tp_threshold:.2f}"
            )
            await self._emit_exit_signal(watch, ExitReason.TAKE_PROFIT, live_z)
            return

        # 2. Check STOP LOSS condition
        if abs_z >= watch.z_sl_threshold:
            self._logger.info(
                f"🛑 {coin} SL hit! | "
                f"entry_Z={watch.entry_z_score:.2f}, current_Z={live_z:.2f}, "
                f"threshold={watch.z_sl_threshold:.2f}"
            )
            await self._emit_exit_signal(watch, ExitReason.STOP_LOSS, live_z)
            return

    # =========================================================================
    # Exit Signal Emission
    # =========================================================================

    async def _emit_exit_signal(
        self, watch: ExitWatch, reason: ExitReason, current_z: float
    ) -> None:
        """
        Emit exit signal for TradingService to close the position.

        Args:
            watch: Exit watch with position details.
            reason: Exit reason (TP or SL).
            current_z: Current Z-score at exit.
        """
        coin = watch.coin_symbol

        # Update watch status
        if reason == ExitReason.TAKE_PROFIT:
            watch.status = ExitWatchStatus.EXITED_TP
        else:
            watch.status = ExitWatchStatus.EXITED_SL

        # Emit ExitSignalEvent
        await self._emitter.emit(
            ExitSignalEvent(
                coin_symbol=coin,
                primary_symbol=watch.primary_symbol,
                exit_reason=reason,
                current_z_score=current_z,
                current_correlation=watch.correlation,
                coin_price=watch.coin_price,
                primary_price=watch.primary_price,
            )
        )

        self._logger.info(
            f"📤 Exit signal emitted | {coin} | "
            f"reason={reason.value} | Z={current_z:.2f} | "
            f"duration={watch.watch_duration_minutes:.1f}min"
        )

        # Cleanup (will be fully cleaned when TradeClosedEvent arrives)
        # But we stop monitoring immediately to avoid duplicate signals
        await self._cleanup_watch(coin)

    # =========================================================================
    # Cleanup
    # =========================================================================

    async def _cleanup_watch(self, coin: str) -> None:
        """Cleanup watch state."""
        async with self._lock:
            # Remove from memory
            if coin in self._watches:
                del self._watches[coin]

            if coin in self._last_check:
                del self._last_check[coin]

        # Unsubscribe from WebSocket
        await self._unsubscribe_coin(coin)

    # =========================================================================
    # Status Methods
    # =========================================================================

    def get_watch_status(self, coin: str) -> Optional[Dict]:
        """
        Get status of a specific exit watch.

        Args:
            coin: Coin symbol.

        Returns:
            Watch status dict or None.
        """
        watch = self._watches.get(coin)
        if not watch:
            return None

        return {
            "coin_symbol": coin,
            "status": watch.status.value,
            "entry_z": watch.entry_z_score,
            "current_z": watch.current_z_score,
            "z_tp_threshold": watch.z_tp_threshold,
            "z_sl_threshold": watch.z_sl_threshold,
            "duration_minutes": watch.watch_duration_minutes,
        }

    def get_all_watch_statuses(self) -> Dict[str, Dict]:
        """Get status of all active exit watches."""
        result = {}
        for coin in self._watches:
            status = self.get_watch_status(coin)
            if status:
                result[coin] = status
        return result
