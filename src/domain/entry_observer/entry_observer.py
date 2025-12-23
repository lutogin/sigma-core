"""
Entry Observer Service.

Trailing Entry logic for Statistical Arbitrage.

Instead of entering immediately when Z-score crosses threshold,
we monitor the spread in real-time and enter only after reversal confirmation.

Logic:
1. Receive PendingEntrySignalEvent when Z crosses entry threshold
2. Subscribe to WebSocket price updates for coin and primary
3. Recalculate Z-score in real-time (every 1 second with debounce)
4. Track maximum |Z| reached
5. Enter when |Z| pulls back by PULLBACK points from maximum
6. Cancel if:
   - Timeout (45 minutes) exceeded
   - Z returns to normal (< entry_threshold) - false alarm
   - Z exceeds SL threshold

This improves Sharpe Ratio by avoiding "catching falling knives".
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.domain.entry_observer.models import WatchCandidate, WatchStatus
from src.infra.event_emitter import (
    EventEmitter,
    EventType,
    PendingEntrySignalEvent,
    EntrySignalEvent,
    WatchCancelledEvent,
    WatchCancelReason,
    SpreadSide,
)
from src.integrations.exchange import BinanceClient


class EntryObserverService:
    """
    Service for trailing entry logic with real-time monitoring.

    Monitors entry candidates via WebSocket and enters only after
    reversal confirmation (pullback from peak Z-score).
    """

    def __init__(
        self,
        event_emitter: EventEmitter,
        exchange_client: BinanceClient,
        logger: Any,
        primary_symbol: str = "ETH/USDT:USDT",
        z_entry_threshold: float = 2.0,
        z_sl_threshold: float = 4.5,
        pullback: float = 0.3,
        watch_timeout_seconds: int = 2700,  # 45 minutes
        debounce_seconds: float = 1.0,
        max_watches: int = 10,
    ):
        """
        Initialize Entry Observer Service.

        Args:
            event_emitter: Event emitter for pub/sub.
            exchange_client: Binance client for WebSocket subscriptions.
            logger: Logger instance.
            primary_symbol: Primary trading pair (e.g., "ETH/USDT:USDT").
            z_entry_threshold: Z-score threshold for entry.
            z_sl_threshold: Z-score stop-loss threshold.
            pullback: Pullback in Z-score points to confirm reversal.
            watch_timeout_seconds: Maximum watch duration before cancellation.
            debounce_seconds: Minimum time between Z-score recalculations.
            max_watches: Maximum concurrent watches (limit WebSocket connections).
        """
        self._emitter = event_emitter
        self._exchange = exchange_client
        self._logger = logger

        self._primary_symbol = primary_symbol
        self._z_entry = z_entry_threshold
        self._z_sl = z_sl_threshold
        self._pullback = pullback
        self._timeout = watch_timeout_seconds
        self._debounce = debounce_seconds
        self._max_watches = max_watches

        # Active watches: coin_symbol -> WatchCandidate
        self._watches: Dict[str, WatchCandidate] = {}

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
        """
        Start the Entry Observer service.

        - Subscribe to PendingEntrySignalEvent
        """
        if self._is_running:
            self._logger.warning("EntryObserverService is already running")
            return

        # Subscribe to pending entry signals
        self._emitter.on(EventType.PENDING_ENTRY_SIGNAL, self._on_pending_signal)

        self._is_running = True

        self._logger.info(
            f"🔭 EntryObserverService started | "
            f"pullback={self._pullback} | "
            f"timeout={self._timeout/60:.0f}min | "
            f"max_watches={self._max_watches} | "
            f"active_watches={len(self._watches)}"
        )

    async def stop(self) -> None:
        """Stop the Entry Observer service."""
        if not self._is_running:
            return

        # Unsubscribe from events
        self._emitter.off(EventType.PENDING_ENTRY_SIGNAL, self._on_pending_signal)

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
        self._logger.info("🔭 EntryObserverService stopped")

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._is_running

    @property
    def active_watch_count(self) -> int:
        """Get number of active watches."""
        return len(self._watches)

    def get_active_watches(self) -> Dict[str, WatchCandidate]:
        """Get all active watches."""
        return dict(self._watches)

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _on_pending_signal(self, event: PendingEntrySignalEvent) -> None:
        """
        Handle PendingEntrySignalEvent - start monitoring for reversal.

        Args:
            event: Pending entry signal with initial Z-score and spread stats.
        """
        coin = event.coin_symbol

        async with self._lock:
            # Check if already watching this symbol
            if coin in self._watches:
                self._logger.debug(f"Already watching {coin}, skipping")
                return

            # Check max watches limit
            if len(self._watches) >= self._max_watches:
                self._logger.warning(
                    f"Max watches ({self._max_watches}) reached, skipping {coin}"
                )
                await self._emitter.emit(
                    WatchCancelledEvent(
                        coin_symbol=coin,
                        primary_symbol=event.primary_symbol,
                        reason=WatchCancelReason.MAX_WATCHES_REACHED,
                        max_z_reached=abs(event.z_score),
                        final_z=event.z_score,
                        watch_duration_seconds=0,
                    )
                )
                return

            # Create WatchCandidate
            watch = WatchCandidate(
                coin_symbol=coin,
                primary_symbol=event.primary_symbol,
                spread_side=event.spread_side.value,
                max_z=abs(event.z_score),
                beta=event.beta,
                spread_mean=event.spread_mean,
                spread_std=event.spread_std,
                initial_z=event.z_score,
                correlation=event.correlation,
                hurst=event.hurst,
                z_entry_threshold=event.z_entry_threshold,
                z_tp_threshold=event.z_tp_threshold,
                z_sl_threshold=event.z_sl_threshold,
                coin_price=event.coin_price,
                primary_price=event.primary_price,
            )

            # Store in memory
            self._watches[coin] = watch

            self._logger.info(
                f"👀 Started watching {coin} | "
                f"Z={event.z_score:.2f} | max_z={watch.max_z:.2f} | "
                f"side={event.spread_side.value} | "
                f"pullback_target={watch.max_z - self._pullback:.2f}"
            )

        # Subscribe to WebSocket feeds (outside lock to avoid blocking)
        await self._subscribe_coin(coin)

        # Subscribe to primary if not already
        if self._primary_ws_task is None:
            await self._subscribe_primary()

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
                self._logger.error(f"Error in coin WS callback for {coin}: {e}")

        try:
            task = await self._exchange.subscribe_book_ticker(coin, on_update)
            self._ws_tasks[coin] = task
            self._logger.debug(f"Subscribed to book ticker for {coin}")
        except Exception as e:
            self._logger.error(f"Failed to subscribe to {coin}: {e}")

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
            self._logger.debug(f"Subscribed to book ticker for {self._primary_symbol}")
        except Exception as e:
            self._logger.error(f"Failed to subscribe to {self._primary_symbol}: {e}")

    async def _unsubscribe_coin(self, coin: str) -> None:
        """Unsubscribe from coin WebSocket."""
        if coin in self._ws_tasks:
            self._ws_tasks[coin].cancel()
            del self._ws_tasks[coin]
            self._logger.debug(f"Unsubscribed from {coin}")

        # If no more watches, unsubscribe from primary too
        if not self._watches and self._primary_ws_task:
            self._primary_ws_task.cancel()
            self._primary_ws_task = None
            self._logger.debug(f"Unsubscribed from {self._primary_symbol}")

    # =========================================================================
    # Core Logic - Price Update Processing
    # =========================================================================

    async def _process_price_update(self, coin: str) -> None:
        """
        Process a price update for a coin.

        Implements the trailing entry algorithm:
        1. Apply debounce (min 1 second between checks)
        2. Calculate live Z-score
        3. Check conditions (timeout, false alarm, SL, reversal)
        4. Update max_z or execute entry
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

        # Calculate current Z-score
        live_z = watch.current_z_score
        abs_z = abs(live_z)

        # 1. Check timeout (45 minutes)
        if watch.watch_duration_seconds > self._timeout:
            self._logger.info(
                f"⏰ {coin} watch timeout after {watch.watch_duration_minutes:.1f}min | "
                f"max_z={watch.max_z:.2f}, final_z={live_z:.2f}"
            )
            await self._cancel_watch(coin, WatchCancelReason.TIMEOUT, live_z)
            return

        # 2. Check false alarm - Z returned to normal
        if abs_z < self._z_entry:
            self._logger.info(
                f"❌ {coin} false alarm - Z returned to normal | "
                f"max_z={watch.max_z:.2f}, final_z={live_z:.2f}"
            )
            await self._cancel_watch(coin, WatchCancelReason.FALSE_ALARM, live_z)
            return

        # 3. Check SL hit - Z exceeded stop-loss
        if abs_z >= self._z_sl:
            self._logger.info(
                f"🛑 {coin} SL hit - Z exceeded threshold | "
                f"max_z={watch.max_z:.2f}, final_z={live_z:.2f}, sl={self._z_sl}"
            )
            await self._cancel_watch(coin, WatchCancelReason.SL_HIT, live_z)
            return

        # 4. Check if spread is still widening (update max)
        if abs_z > watch.max_z:
            watch.max_z = abs_z
            self._logger.debug(
                f"📈 {coin} new peak Z: {abs_z:.2f} | "
                f"pullback_target={abs_z - self._pullback:.2f}"
            )
            return

        # 5. Check for reversal (pullback from max)
        if abs_z <= watch.max_z - self._pullback:
            self._logger.info(
                f"✅ {coin} reversal confirmed! | "
                f"peak={watch.max_z:.2f}, current={abs_z:.2f}, "
                f"pullback={watch.max_z - abs_z:.2f}"
            )
            await self._execute_entry(watch, live_z)

    # =========================================================================
    # Entry Execution
    # =========================================================================

    async def _execute_entry(self, watch: WatchCandidate, current_z: float) -> None:
        """
        Execute entry after reversal confirmation.

        Emits EntrySignalEvent for TradingService to execute the trade.
        """
        coin = watch.coin_symbol

        # Create EntrySignalEvent
        event = EntrySignalEvent(
            coin_symbol=coin,
            primary_symbol=watch.primary_symbol,
            spread_side=(
                SpreadSide.LONG if watch.spread_side == "long" else SpreadSide.SHORT
            ),
            z_score=current_z,
            beta=watch.beta,
            correlation=watch.correlation,
            hurst=watch.hurst,
            coin_price=watch.coin_price,
            primary_price=watch.primary_price,
            z_tp_threshold=watch.z_tp_threshold,
            z_sl_threshold=watch.z_sl_threshold,
        )

        # Update watch status
        watch.status = WatchStatus.ENTERED

        # Emit entry signal
        await self._emitter.emit(event)

        # Cleanup
        await self._cleanup_watch(coin)

        self._logger.info(
            f"🎯 Entry executed | {coin} | "
            f"Z={current_z:.2f} | peak_was={watch.max_z:.2f} | "
            f"duration={watch.watch_duration_minutes:.1f}min"
        )

    # =========================================================================
    # Watch Cancellation
    # =========================================================================

    async def _cancel_watch(
        self, coin: str, reason: WatchCancelReason, final_z: float
    ) -> None:
        """
        Cancel a watch without entry.

        Args:
            coin: Coin symbol.
            reason: Reason for cancellation.
            final_z: Z-score at cancellation.
        """
        watch = self._watches.get(coin)
        if not watch:
            return

        # Update status
        watch.status = WatchStatus.CANCELLED

        # Emit cancellation event
        await self._emitter.emit(
            WatchCancelledEvent(
                coin_symbol=coin,
                primary_symbol=watch.primary_symbol,
                reason=reason,
                max_z_reached=watch.max_z,
                final_z=final_z,
                watch_duration_seconds=watch.watch_duration_seconds,
            )
        )

        # Cleanup
        await self._cleanup_watch(coin)

    async def _cleanup_watch(self, coin: str) -> None:
        """Cleanup watch state after entry or cancellation."""
        async with self._lock:
            # Remove from memory
            if coin in self._watches:
                del self._watches[coin]

            if coin in self._last_check:
                del self._last_check[coin]

        # Unsubscribe from WebSocket
        await self._unsubscribe_coin(coin)

    # =========================================================================
    # Manual Operations
    # =========================================================================

    async def cancel_watch_manual(self, coin: str) -> bool:
        """
        Manually cancel a watch (e.g., via Telegram command).

        Args:
            coin: Coin symbol to cancel.

        Returns:
            True if watch was cancelled, False if not found.
        """
        if coin not in self._watches:
            return False

        watch = self._watches[coin]
        await self._cancel_watch(
            coin, WatchCancelReason.FALSE_ALARM, watch.current_z_score
        )
        return True

    def get_watch_status(self, coin: str) -> Optional[Dict]:
        """
        Get status of a specific watch.

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
            "max_z": watch.max_z,
            "current_z": watch.current_z_score,
            "pullback_remaining": watch.max_z - abs(watch.current_z_score),
            "pullback_target": self._pullback,
            "duration_minutes": watch.watch_duration_minutes,
            "timeout_remaining_minutes": (self._timeout - watch.watch_duration_seconds)
            / 60,
        }

    def get_all_watch_statuses(self) -> Dict[str, Dict]:
        """Get status of all active watches."""
        result = {}
        for coin in self._watches:
            status = self.get_watch_status(coin)
            if status:
                result[coin] = status
        return result
