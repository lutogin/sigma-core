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
   - Normal pullback: used when max_z <= z_sl
   - Extreme pullback: used when max_z > z_sl (larger pullback for extreme signals)
6. Cancel if:
   - Timeout (45 minutes) exceeded
   - Z returns to normal (< entry_threshold) - false alarm
   - Z exceeds extreme level threshold

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
    WatchStartedEvent,
    EntrySignalEvent,
    WatchCancelledEvent,
    WatchCancelReason,
    WatchTimeoutCooldownEvent,
    SpreadSide,
    MarketUnsafeEvent,
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
        z_sl_threshold: float = 4.0,
        z_extreme_level: float = 6.0,  # Max Z for watch cancellation (above z_sl)
        pullback: float = 0.2,
        pullback_extreme: float = 0.6,  # Pullback for extreme signals (|Z| > z_sl)
        watch_timeout_seconds: int = 2700,  # 45 minutes
        debounce_seconds: float = 1.0,
        max_watches: int = 10,
        false_alarm_hysteresis: float = 0.2,  # Cancel only if Z drops this much below threshold
    ):
        """
        Initialize Entry Observer Service.

        Args:
            event_emitter: Event emitter for pub/sub.
            exchange_client: Binance client for WebSocket subscriptions.
            logger: Logger instance.
            primary_symbol: Primary trading pair (e.g., "ETH/USDT:USDT").
            z_entry_threshold: Z-score threshold for entry.
            z_sl_threshold: Z-score threshold for determining pullback type (normal vs extreme).
            z_extreme_level: Maximum Z-score before watch is cancelled (SL HIT).
                             Allows signals above z_sl (4.0) up to this level (6.0) to continue.
            pullback: Pullback in Z-score points to confirm reversal (normal signals).
            pullback_extreme: Pullback in Z-score points for extreme signals (when |Z| > z_sl).
            watch_timeout_seconds: Maximum watch duration before cancellation.
            debounce_seconds: Minimum time between Z-score recalculations.
            max_watches: Maximum concurrent watches (limit WebSocket connections).
            false_alarm_hysteresis: Cancel watch only if |Z| drops this much below entry threshold.
                                    Prevents premature cancellation during pullback to entry target.
        """
        self._emitter = event_emitter
        self._exchange = exchange_client
        self._logger = logger

        self._primary_symbol = primary_symbol
        self._z_entry = z_entry_threshold
        self._z_sl = z_sl_threshold
        self._z_extreme_level = z_extreme_level
        self._pullback = pullback
        self._pullback_extreme = pullback_extreme
        self._timeout = watch_timeout_seconds
        self._debounce = debounce_seconds
        self._max_watches = max_watches
        self._false_alarm_hysteresis = false_alarm_hysteresis

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

        # Subscribe to market unsafe events to clear watches on volatility
        self._emitter.on(EventType.MARKET_UNSAFE, self._on_market_unsafe)

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
        self._emitter.off(EventType.MARKET_UNSAFE, self._on_market_unsafe)

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

    @property
    def pullback(self) -> float:
        """Get pullback threshold."""
        return self._pullback

    def get_active_watches(self) -> Dict[str, WatchCandidate]:
        """Get all active watches."""
        return dict(self._watches)

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _on_pending_signal(self, event: PendingEntrySignalEvent) -> None:
        """
        Handle PendingEntrySignalEvent - start monitoring for reversal.

        FROZEN ANCHOR TRAILING:
        When already watching a symbol, we DO NOT update the anchor parameters
        (beta, spread_mean, spread_std, max_z). We only validate that the signal
        is still alive using the NEW scan's Z-score.

        Why? Because trailing entry is a reaction to a SPECIFIC extremum,
        not to a constantly updating model. Updating parameters every 15m
        causes "moving goalposts" where the entry target keeps shifting
        and trades never execute.

        For NEW watches: anchor the parameters at detection time.
        For EXISTING watches: only validate, don't update anchor.

        Args:
            event: Pending entry signal with initial Z-score and spread stats.
        """
        coin = event.coin_symbol

        async with self._lock:
            # =====================================================================
            # EXISTING WATCH: Validate only, keep frozen anchor
            # =====================================================================
            if coin in self._watches:
                watch = self._watches[coin]

                # LIVENESS CHECK: Is the signal still valid on new scan?
                # We use the NEW Z-score from event, but FROZEN threshold from watch
                # This prevents cancellation due to threshold drift
                invalidation_level = (
                    watch.z_entry_threshold - self._false_alarm_hysteresis
                )
                abs_event_z = abs(event.z_score)

                if abs_event_z < invalidation_level:
                    # Signal disappeared on new scan - cancel watch
                    self._logger.info(
                        f"❌ Watch {coin} CANCELLED by scan | "
                        f"New scan Z={event.z_score:.2f} < invalidation={invalidation_level:.2f} "
                        f"(frozen_threshold={watch.z_entry_threshold:.2f}) | "
                        f"Signal lost, cancelling frozen anchor (max_z={watch.max_z:.2f})"
                    )
                    self._watches.pop(coin, None)
                    asyncio.create_task(
                        self._cancel_watch_after_param_update(
                            coin, watch, event.z_score
                        )
                    )
                    return

                # Signal still valid - KEEP FROZEN ANCHOR for Z-score calculation
                # beta, spread_mean, spread_std, z_entry_threshold stay FROZEN (affect Z calculation)
                # max_z is updated ONLY from real-time WebSocket data (_process_price_update)
                # NOT from scan (which uses new params that would cause "Parameter Jump")
                #
                # ONLY halflife can be updated - it affects position sizing, not trailing logic
                old_halflife = watch.halflife
                watch.halflife = event.halflife

                # Determine pullback amount based on max_z
                pullback_amount = (
                    self._pullback_extreme
                    if watch.max_z > watch.z_sl_threshold
                    else self._pullback
                )
                entry_target = watch.max_z - pullback_amount
                self._logger.info(
                    f"🛡️ Watch {coin} FROZEN | scan_Z={abs_event_z:.2f} | "
                    f"max_z={watch.max_z:.2f} | entry_target={entry_target:.2f} "
                    f"(using {'extreme' if watch.max_z > watch.z_sl_threshold else 'normal'} pullback={pullback_amount:.2f}) | "
                    f"HL: {old_halflife:.1f}→{watch.halflife:.1f}"
                )
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
                halflife=event.halflife,
                z_entry_threshold=event.z_entry_threshold,
                z_tp_threshold=event.z_tp_threshold,
                z_sl_threshold=event.z_sl_threshold,
                coin_price=event.coin_price,
                primary_price=event.primary_price,
            )

            # Store in memory
            self._watches[coin] = watch

            # Determine pullback amount based on initial signal strength
            initial_pullback = (
                self._pullback_extreme
                if abs(event.z_score) > watch.z_sl_threshold
                else self._pullback
            )
            pullback_target = watch.max_z - initial_pullback

            self._logger.info(
                f"👀 Started watching {coin} | "
                f"Z={event.z_score:.2f} | max_z={watch.max_z:.2f} | "
                f"side={event.spread_side.value} | "
                f"pullback_target={pullback_target:.2f} "
                f"(using {'extreme' if abs(event.z_score) > watch.z_sl_threshold else 'normal'} pullback={initial_pullback:.2f})"
            )

            # Emit WatchStartedEvent for notifications (only on NEW watch)
            await self._emitter.emit(
                WatchStartedEvent(
                    coin_symbol=coin,
                    primary_symbol=event.primary_symbol,
                    spread_side=event.spread_side,
                    z_score=event.z_score,
                    beta=event.beta,
                    correlation=event.correlation,
                    hurst=event.hurst,
                    halflife=event.halflife,
                    spread_mean=event.spread_mean,
                    spread_std=event.spread_std,
                    coin_price=event.coin_price,
                    primary_price=event.primary_price,
                    z_entry_threshold=event.z_entry_threshold,
                    z_tp_threshold=event.z_tp_threshold,
                    z_sl_threshold=event.z_sl_threshold,
                )
            )

        # Subscribe to WebSocket feeds (outside lock to avoid blocking)
        await self._subscribe_coin(coin)

        # Subscribe to primary if not already
        if self._primary_ws_task is None:
            await self._subscribe_primary()

    async def _cancel_watch_after_param_update(
        self, coin: str, watch: WatchCandidate, final_z: float
    ) -> None:
        """Cancel watch after parameter update invalidated the signal."""
        # Emit cancellation event
        await self._emitter.emit(
            WatchCancelledEvent(
                coin_symbol=coin,
                primary_symbol=watch.primary_symbol,
                reason=WatchCancelReason.FALSE_ALARM,
                max_z_reached=watch.max_z,
                final_z=final_z,
                watch_duration_seconds=watch.watch_duration_seconds,
            )
        )
        # Unsubscribe from WebSocket
        await self._unsubscribe_coin(coin)

    async def _on_market_unsafe(self, event: MarketUnsafeEvent) -> None:
        """
        Handle MarketUnsafeEvent - cancel all active watches on volatility trigger.

        When market becomes unsafe (high volatility), we should stop watching
        all pending entries as the spread behavior is unpredictable.
        """
        if not self._watches:
            return  # No watches to cancel

        cancelled_count = len(self._watches)

        self._logger.warning(
            f"⚠️ VOLATILITY UNSAFE: Cancelling {cancelled_count} active watches | "
            f"reason={event.reason}"
        )

        async with self._lock:
            # Cancel all watches
            for coin, watch in list(self._watches.items()):
                # Emit cancellation event
                await self._emitter.emit(
                    WatchCancelledEvent(
                        coin_symbol=coin,
                        primary_symbol=watch.primary_symbol,
                        reason=WatchCancelReason.FALSE_ALARM,  # Use FALSE_ALARM as closest reason
                        max_z_reached=watch.max_z,
                        final_z=watch.current_z_score,
                        watch_duration_seconds=watch.watch_duration_seconds,
                    )
                )

                # Cancel WebSocket task for this coin
                if coin in self._ws_tasks:
                    self._ws_tasks[coin].cancel()
                    del self._ws_tasks[coin]

                self._logger.info(
                    f"❌ Watch cancelled (VOLATILITY): {coin} | "
                    f"max_z={watch.max_z:.2f} | duration={watch.watch_duration_seconds/60:.1f}min"
                )

            # Clear all watches
            self._watches.clear()
            self._last_check.clear()

        self._logger.info(
            f"🧹 Cleared {cancelled_count} watches due to volatility filter"
        )

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
        3. Check conditions in order:
           a. FIRST: Check for reversal (pullback from max) - ENTRY
           b. Check timeout
           c. Check false alarm (Z dropped significantly below threshold with hysteresis)
           d. Check SL hit
           e. Update max_z if still widening

        False Alarm Logic (with hysteresis):
        - We only cancel if |Z| drops SIGNIFICANTLY below entry threshold
        - This prevents cancellation when Z passes through threshold zone on way to pullback target
        - Example: threshold=2.6, max_z=2.8, pullback=0.3 → target=2.5
          If Z drops to 2.55, it's below threshold but still valid for entry at 2.5
        - With hysteresis=0.3, we only cancel if |Z| < (threshold - 0.3) = 2.3
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

        # Use dynamic threshold from the watch (set from PendingEntrySignalEvent)
        z_entry = watch.z_entry_threshold
        z_sl = watch.z_sl_threshold

        # Calculate dynamic pullback based on signal strength
        # If max_z exceeded z_sl, use extreme pullback (larger), otherwise use normal pullback
        if watch.max_z > z_sl:
            pullback_amount = self._pullback_extreme
        else:
            pullback_amount = self._pullback

        # Calculate pullback target for this watch
        pullback_target = watch.max_z - pullback_amount

        # 1. FIRST: Check for reversal (pullback from max) - ENTRY CONDITION
        # This must be checked first to capture the entry moment
        if abs_z <= pullback_target:
            self._logger.info(
                f"✅ {coin} reversal confirmed! | "
                f"peak={watch.max_z:.2f}, current={abs_z:.2f}, "
                f"pullback={watch.max_z - abs_z:.2f} "
                f"(using {'extreme' if watch.max_z > z_sl else 'normal'} pullback={pullback_amount:.2f})"
            )
            await self._execute_entry(watch, live_z)
            return

        # 2. Check timeout
        if watch.watch_duration_seconds > self._timeout:
            self._logger.info(
                f"⏰ {coin} watch timeout after {watch.watch_duration_minutes:.1f}min | "
                f"max_z={watch.max_z:.2f}, final_z={live_z:.2f}"
            )
            # Emit cooldown event so PositionStateService can apply cooldown
            await self._emitter.emit(
                WatchTimeoutCooldownEvent(
                    coin_symbol=coin,
                    primary_symbol=watch.primary_symbol,
                    max_z_reached=watch.max_z,
                    final_z=live_z,
                    watch_duration_seconds=watch.watch_duration_seconds,
                )
            )
            await self._cancel_watch(coin, WatchCancelReason.TIMEOUT, live_z)
            return

        # 3. Check false alarm with HYSTERESIS
        # Only cancel if Z dropped SIGNIFICANTLY below threshold (not just slightly)
        # This prevents cancellation when Z is on its way to pullback target
        # Also skip if pullback_target is still reachable (below current threshold)
        false_alarm_level = z_entry - self._false_alarm_hysteresis
        if abs_z < false_alarm_level:
            self._logger.info(
                f"❌ {coin} false alarm - Z dropped significantly | "
                f"max_z={watch.max_z:.2f}, final_z={live_z:.2f}, "
                f"threshold={z_entry:.2f}, hysteresis_level={false_alarm_level:.2f}"
            )
            await self._cancel_watch(coin, WatchCancelReason.FALSE_ALARM, live_z)
            return

        # 4. Check SL hit - Z exceeded extreme level (not z_sl!)
        # We allow signals above z_sl (4.0) up to z_extreme_level (6.0)
        # Only cancel if Z exceeds the extreme level
        if abs_z >= self._z_extreme_level:
            self._logger.info(
                f"🛑 {coin} SL hit - Z exceeded extreme level | "
                f"max_z={watch.max_z:.2f}, final_z={live_z:.2f}, "
                f"extreme_level={self._z_extreme_level}"
            )
            await self._cancel_watch(coin, WatchCancelReason.SL_HIT, live_z)
            return

        # 5. Check if spread is still widening (update max)
        if abs_z > watch.max_z:
            watch.max_z = abs_z
            # Determine pullback amount based on new max_z
            new_pullback = self._pullback_extreme if abs_z > z_sl else self._pullback
            self._logger.debug(
                f"📈 {coin} new peak Z: {abs_z:.2f} | "
                f"pullback_target={abs_z - new_pullback:.2f} "
                f"(using {'extreme' if abs_z > z_sl else 'normal'} pullback={new_pullback:.2f})"
            )

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
            halflife=watch.halflife,
            spread_mean=watch.spread_mean,
            spread_std=watch.spread_std,
            coin_price=watch.coin_price,
            primary_price=watch.primary_price,
            z_tp_threshold=max(watch.z_tp_threshold, current_z * 0.1),  # dynamic TP
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

    async def remove_watch_by_filter(
        self, coin: str, reason: WatchCancelReason
    ) -> bool:
        """
        Remove a watch because it failed screener filters (Hurst/Correlation).

        Called by OrchestratorService when a watched pair drops out of filters.
        Only unsubscribes from the coin's WebSocket, keeps ETH if other pairs need it.

        Args:
            coin: Coin symbol to remove.
            reason: Reason for removal (CORRELATION_DROP or HURST_TRENDING).

        Returns:
            True if watch was removed, False if not found.
        """
        if coin not in self._watches:
            return False

        watch = self._watches[coin]
        final_z = watch.current_z_score

        self._logger.info(
            f"🚫 Removing watch {coin} | reason={reason.value} | "
            f"max_z={watch.max_z:.2f} | final_z={final_z:.2f}"
        )

        await self._cancel_watch(coin, reason, final_z)
        return True

    def has_watch(self, coin: str) -> bool:
        """Check if a coin is currently being watched."""
        return coin in self._watches

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
