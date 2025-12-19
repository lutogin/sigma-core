"""
Trading Service.

Executes trades based on signals from OpportunityObserver.

A trade consists of 2 opposite positions on 2 different futures instruments:
- LONG spread: Buy Y (symbol_y), Sell X (symbol_x)
- SHORT spread: Sell Y (symbol_y), Buy X (symbol_x)

Architecture:
1. Listens to TradeEntryEvent and TradeExitEvent from OpportunityObserver
2. Validates conditions (balance, max positions, symbol overlap)
3. Opens/closes positions atomically with retry logic
4. Emits lifecycle events (TradeOpenedEvent, TradeClosedEvent, etc.)
"""

import asyncio
from typing import Any, Optional, Set, Dict, List
from decimal import Decimal

from src.infra.event_emitter import (
    EventEmitter,
    EventType,
    TradeEntryEvent,
    TradeExitEvent,
    TradeOpenedEvent,
    TradeClosedEvent,
    TradeClosedPartiallyEvent,
    TradeSkippedEvent,
    TradeFailedEvent,
    TradeCloseErrorEvent,
    TradeSkipReason,
    PositionSide,
)
from src.infra.utils import async_retry, RetryError
from src.integrations.exchange import (
    BinanceClient,
    OrderSide,
    Position,
    Order,
    TradeSide,
)


class TradingServiceExample:
    """
    Trading service that executes trades based on OpportunityObserver signals.

    Responsibilities:
    - Listen to entry/exit signals
    - Validate trading conditions (balance, positions, overlaps)
    - Execute atomic position opens/closes
    - Handle failures with rollback
    - Emit trade lifecycle events
    """

    def __init__(
        self,
        event_emitter: EventEmitter,
        exchange_client: BinanceClient,
        logger: Any,
        allow_trading: bool = False,
        max_positions: int = 6,
        position_size_pct: float = 0.2,
        leverage: int = 1,
    ):
        """
        Initialize trading service.

        :param event_emitter: Event emitter for pub/sub
        :param exchange_client: Binance exchange client
        :param logger: Logger instance
        :param allow_trading: Enable/disable real trading
        :param max_positions: Maximum number of open positions (not trades)
        :param position_size_pct: Position size as % of available balance
        :param leverage: Default leverage for positions
        """
        self._emitter = event_emitter
        self._exchange = exchange_client
        self._logger = logger
        self._allow_trading = allow_trading
        self._max_positions = max_positions
        self._position_size_pct = position_size_pct
        self._leverage = leverage

        self._is_running = False
        self._active_symbols: Set[str] = set()
        # Track open trades: pair_id -> PositionSide
        self._open_trades: Dict[str, PositionSide] = {}

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> None:
        """
        Start the trading service.

        - Loads current open positions from exchange
        - Populates _active_symbols to prevent race conditions
        - Subscribes to entry/exit events

        Note: Open trades should be set externally via set_open_trades()
        before calling start(). Trade sync is handled by OpportunityObserverCron.
        """
        if self._is_running:
            self._logger.warning("TradingService is already running")
            return

        # Load current open positions and populate _active_symbols
        await self._sync_active_symbols()

        # Subscribe to entry/exit events
        self._emitter.on(EventType.POSITION_ENTRY, self._on_entry_signal)
        self._emitter.on(EventType.POSITION_EXIT, self._on_exit_signal)

        self._is_running = True
        self._logger.info(
            f"🚀 TradingService started | "
            f"trading={'enabled' if self._allow_trading else 'disabled'} | "
            f"max_positions={self._max_positions} | "
            f"position_size={self._position_size_pct * 100:.0f}% | "
            f"active_symbols={len(self._active_symbols)} | "
            f"open_trades={len(self._open_trades)}"
        )

    async def _sync_active_symbols(self) -> None:
        """
        Sync _active_symbols with current open positions from exchange.

        Called on start() to prevent race conditions with existing positions.
        """
        try:
            positions = await self._exchange.get_positions(skip_zero=True)
            self._active_symbols = {p.symbol.lower() for p in positions}

            if self._active_symbols:
                self._logger.info(
                    f"📋 Loaded {len(self._active_symbols)} active symbols: "
                    f"{', '.join(sorted(self._active_symbols))}"
                )
        except Exception as e:
            self._logger.warning(f"Failed to sync active symbols: {e}")
            self._active_symbols = set()

    def set_open_trades(self, trades: Dict[str, PositionSide]) -> None:
        """
        Set open trades from external source.

        Called by app.py after OpportunityObserverCron.sync_open_trades_from_exchange()
        to inject the synced state before starting the service.

        :param trades: Dict mapping pair_id to PositionSide
        """
        self._open_trades = trades.copy()
        self._logger.info(
            f"📋 Set {len(self._open_trades)} open trades: "
            f"{', '.join(f'{k}={v.value}' for k, v in self._open_trades.items()) or 'none'}"
        )

    def stop(self) -> None:
        """Stop the trading service (unsubscribe from events)."""
        if not self._is_running:
            return

        self._emitter.off(EventType.POSITION_ENTRY, self._on_entry_signal)
        self._emitter.off(EventType.POSITION_EXIT, self._on_exit_signal)

        self._is_running = False
        self._logger.info("🛑 TradingService stopped")

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _on_entry_signal(self, event: TradeEntryEvent) -> None:
        """
        Handle trade entry signal.

        Validates conditions and opens atomic positions.
        """
        self._logger.info(
            f"📨 Entry signal received | "
            f"pair={event.pair_id} | side={event.side.value} | z={event.z_score:.4f}"
        )

        # Check if trading is enabled
        if not self._allow_trading:
            self._logger.info("⚠️ Trading disabled - skipping entry")
            self._emit_skipped(
                event, TradeSkipReason.TRADING_DISABLED, "Trading is disabled"
            )
            return

        try:
            # 1. Check if trade already open for this pair
            if event.pair_id in self._open_trades:
                self._logger.info(
                    f"⚠️ Trade already open for {event.pair_id}, skipping entry signal"
                )
                return

            # 2. Check symbol overlap against internal tracking (prevents race condition)
            signal_symbols = {event.symbol_x.lower(), event.symbol_y.lower()}
            overlap = self._active_symbols & signal_symbols

            if overlap:
                self._logger.warning(f"⚠️ Symbol overlap (internal): {overlap}")
                self._emit_skipped(
                    event,
                    TradeSkipReason.SYMBOL_OVERLAP,
                    f"Overlapping symbols: {', '.join(overlap)}",
                )
                return

            # 3. Check max positions (use internal tracking)
            if len(self._active_symbols) >= self._max_positions:
                self._logger.warning(
                    f"⚠️ Max positions reached ({len(self._active_symbols)}/{self._max_positions})"
                )
                self._emit_skipped(
                    event,
                    TradeSkipReason.MAX_POSITIONS_REACHED,
                    f"Current: {len(self._active_symbols)}, Max: {self._max_positions}",
                )
                return

            # 4. Reserve symbols BEFORE opening positions (prevents race condition)
            self._active_symbols.add(event.symbol_x.lower())
            self._active_symbols.add(event.symbol_y.lower())
            self._logger.debug(
                f"Reserved symbols: {event.symbol_x}, {event.symbol_y} | "
                f"active={len(self._active_symbols)}"
            )

            # 5. Check balance
            balance = await self._exchange.get_balance("USDT")
            available = balance.free

            if available <= 0:
                self._logger.warning("⚠️ No available balance")
                self._release_symbols(event.symbol_x, event.symbol_y)
                self._emit_skipped(
                    event,
                    TradeSkipReason.INSUFFICIENT_BALANCE,
                    "Available balance is 0",
                )
                return

            # Calculate position size in USDT
            position_size_usdt = available * self._position_size_pct

            # Need 2 positions, so check if we have enough for both
            min_required = position_size_usdt * 2

            if available < min_required:
                self._logger.warning(
                    f"⚠️ Insufficient balance for trade | "
                    f"available={available:.2f} | required={min_required:.2f}"
                )
                self._release_symbols(event.symbol_x, event.symbol_y)
                self._emit_skipped(
                    event,
                    TradeSkipReason.INSUFFICIENT_BALANCE,
                    f"Need {min_required:.2f} USDT, have {available:.2f}",
                )
                return

            # 6. Open atomic positions
            await self._open_trade(event, position_size_usdt)

        except Exception as e:
            self._logger.error(f"❌ Error processing entry signal: {e}")
            self._release_symbols(event.symbol_x, event.symbol_y)
            self._emitter.emit(
                TradeFailedEvent(
                    pair_id=event.pair_id,
                    symbol_x=event.symbol_x,
                    symbol_y=event.symbol_y,
                    error_message=str(e),
                    failed_symbol="",
                )
            )

    async def _on_exit_signal(self, event: TradeExitEvent) -> None:
        """
        Handle trade exit signal.

        Finds and closes positions for the pair.
        """
        self._logger.info(
            f"📨 Exit signal received | "
            f"pair={event.pair_id} | reason={event.reason.value} | z={event.z_score:.4f}"
        )

        if not self._allow_trading:
            self._logger.info("⚠️ Trading disabled - skipping exit")
            return

        try:
            # Check if trade is actually open
            if event.pair_id not in self._open_trades:
                self._logger.warning(
                    f"⚠️ Exit signal for {event.pair_id} but no open trade found, skipping"
                )
                return

            await self._close_trade(event)

            # Remove from open trades and release symbols
            self._open_trades.pop(event.pair_id, None)
            self._release_symbols(event.symbol_x, event.symbol_y)

        except Exception as e:
            self._logger.error(f"❌ Error processing exit signal: {e}")
            self._emitter.emit(
                TradeCloseErrorEvent(
                    pair_id=event.pair_id,
                    symbol_x=event.symbol_x,
                    symbol_y=event.symbol_y,
                    error_message=str(e),
                    failed_symbol="",
                )
            )

    # =========================================================================
    # Trade Execution
    # =========================================================================

    async def _open_trade(
        self, event: TradeEntryEvent, position_size_usdt: float
    ) -> None:
        """
        Open a trade (2 atomic positions).

        LONG spread: Buy Y, Sell X
        SHORT spread: Sell Y, Buy X
        """
        symbol_x = event.symbol_x
        symbol_y = event.symbol_y

        # Determine position sides based on spread direction
        if event.side == PositionSide.LONG:
            # LONG spread: Buy Y, Sell X
            side_y = OrderSide.BUY
            side_x = OrderSide.SELL
        else:
            # SHORT spread: Sell Y, Buy X
            side_y = OrderSide.SELL
            side_x = OrderSide.BUY

        self._logger.info(
            f"🎯 Opening trade | {symbol_y} ({side_y}) + {symbol_x} ({side_x}) | "
            f"size={position_size_usdt:.2f} USDT each"
        )

        # Calculate amounts in base currency
        amount_x = await self._exchange.calculate_amount_from_usdt(
            symbol_x, position_size_usdt
        )
        amount_y = await self._exchange.calculate_amount_from_usdt(
            symbol_y, position_size_usdt
        )

        # Open positions atomically
        order_x: Optional[Order] = None
        order_y: Optional[Order] = None

        try:
            # Cast side_x and side_y to OrderSide to match expected argument types
            results = await asyncio.gather(
                self._open_position_with_retry(
                    symbol_x, OrderSide(side_x), float(amount_x)
                ),
                self._open_position_with_retry(
                    symbol_y, OrderSide(side_y), float(amount_y)
                ),
                return_exceptions=True,
            )

            result_x, result_y = results

            # Check for failures
            x_success = isinstance(result_x, Order)
            y_success = isinstance(result_y, Order)

            if x_success:
                order_x = result_x if isinstance(result_x, Order) else None
            if y_success:
                order_y = result_y if isinstance(result_y, Order) else None

            # Both succeeded
            if x_success and y_success:
                self._logger.info(
                    f"✅ Trade opened | {symbol_y} order={order_y.id if order_y else None} | {symbol_x} order={order_x.id if order_x else None}"
                )

                # Mark trade as open
                self._open_trades[event.pair_id] = event.side

                self._emitter.emit(
                    TradeOpenedEvent(
                        pair_id=event.pair_id,
                        symbol_x=symbol_x,
                        symbol_y=symbol_y,
                        side=event.side,
                        order_id_x=order_x.id if order_x is not None else "",
                        order_id_y=order_y.id if order_y is not None else "",
                        size_x=float(amount_x),
                        size_y=float(amount_y),
                        price_x=order_x.price if order_x is not None else 0.0,
                        price_y=order_y.price if order_y is not None else 0.0,
                        z_score=event.z_score,
                        beta=event.beta,
                    )
                )
                return

            # X succeeded but Y failed → rollback X
            if x_success and not y_success:
                error_y = (
                    result_y
                    if isinstance(result_y, Exception)
                    else Exception("Unknown error")
                )
                self._logger.error(f"❌ Y position failed: {error_y}")

                await self._rollback_position(symbol_x, order_x)
                self._release_symbols(symbol_x, symbol_y)

                self._emitter.emit(
                    TradeFailedEvent(
                        pair_id=event.pair_id,
                        symbol_x=symbol_x,
                        symbol_y=symbol_y,
                        error_message=str(error_y),
                        failed_symbol=symbol_y,
                        rollback_order_id=order_x.id if order_x else None,
                    )
                )
                return

            # Y succeeded but X failed → rollback Y
            if y_success and not x_success:
                error_x = (
                    result_x
                    if isinstance(result_x, Exception)
                    else Exception("Unknown error")
                )
                self._logger.error(f"❌ X position failed: {error_x}")

                await self._rollback_position(symbol_y, order_y)
                self._release_symbols(symbol_x, symbol_y)

                self._emitter.emit(
                    TradeFailedEvent(
                        pair_id=event.pair_id,
                        symbol_x=symbol_x,
                        symbol_y=symbol_y,
                        error_message=str(error_x),
                        failed_symbol=symbol_x,
                        rollback_order_id=order_y.id if order_y else None,
                    )
                )
                return

            # Both failed
            error_x = (
                result_x if isinstance(result_x, Exception) else Exception("Unknown")
            )
            error_y = (
                result_y if isinstance(result_y, Exception) else Exception("Unknown")
            )

            self._logger.error(
                f"❌ Both positions failed | X: {error_x} | Y: {error_y}"
            )
            self._release_symbols(symbol_x, symbol_y)

            self._emitter.emit(
                TradeFailedEvent(
                    pair_id=event.pair_id,
                    symbol_x=symbol_x,
                    symbol_y=symbol_y,
                    error_message=f"X: {error_x}, Y: {error_y}",
                    failed_symbol="both",
                )
            )

        except Exception as e:
            self._logger.error(f"❌ Unexpected error opening trade: {e}")

            # Rollback any successful positions
            if order_x:
                await self._rollback_position(symbol_x, order_x)
            if order_y:
                await self._rollback_position(symbol_y, order_y)

            self._release_symbols(symbol_x, symbol_y)

            self._emitter.emit(
                TradeFailedEvent(
                    pair_id=event.pair_id,
                    symbol_x=symbol_x,
                    symbol_y=symbol_y,
                    error_message=str(e),
                    failed_symbol="unknown",
                )
            )

    async def _close_trade(self, event: TradeExitEvent) -> None:
        """
        Close a trade (find and close both positions).
        """
        symbol_x = event.symbol_x
        symbol_y = event.symbol_y

        self._logger.info(f"🎯 Closing trade | {symbol_y} + {symbol_x}")

        # Get current positions
        positions = await self._exchange.get_positions(skip_zero=True)

        # Find positions for our symbols
        position_x: Optional[Position] = None
        position_y: Optional[Position] = None

        for pos in positions:
            if pos.symbol.lower() == symbol_x.lower():
                position_x = pos
            elif pos.symbol.lower() == symbol_y.lower():
                position_y = pos

        found_x = position_x is not None
        found_y = position_y is not None

        # Both positions found
        if found_x and found_y:
            results = await asyncio.gather(
                self._close_position_with_retry(symbol_x),
                self._close_position_with_retry(symbol_y),
                return_exceptions=True,
            )

            result_x, result_y = results
            x_success = isinstance(result_x, Order)
            y_success = isinstance(result_y, Order)

            if x_success and y_success:
                self._logger.info(f"✅ Trade closed | {symbol_y} + {symbol_x}")

                self._emitter.emit(
                    TradeClosedEvent(
                        pair_id=event.pair_id,
                        symbol_x=symbol_x,
                        symbol_y=symbol_y,
                        order_id_x=(
                            str(getattr(result_x, "id"))
                            if x_success and getattr(result_x, "id", None) is not None
                            else ""
                        ),
                        order_id_y=(
                            str(getattr(result_y, "id"))
                            if y_success and getattr(result_y, "id", None) is not None
                            else ""
                        ),
                        reason=event.reason,
                        z_score=event.z_score,
                    )
                )
            else:
                # At least one failed
                errors = []
                if not x_success:
                    errors.append(f"X: {result_x}")
                if not y_success:
                    errors.append(f"Y: {result_y}")

                self._logger.error(f"❌ Failed to close some positions: {errors}")

                self._emitter.emit(
                    TradeCloseErrorEvent(
                        pair_id=event.pair_id,
                        symbol_x=symbol_x,
                        symbol_y=symbol_y,
                        error_message=", ".join(errors),
                        failed_symbol="partial",
                    )
                )
            return

        # Only one leg found
        if found_x or found_y:
            found_symbol = symbol_x if found_x else symbol_y
            missing_symbol = symbol_y if found_x else symbol_x

            self._logger.warning(
                f"⚠️ Only one leg found | closing {found_symbol} | missing {missing_symbol}"
            )

            try:
                order = await self._close_position_with_retry(found_symbol)

                self._emitter.emit(
                    TradeClosedPartiallyEvent(
                        pair_id=event.pair_id,
                        symbol_x=symbol_x,
                        symbol_y=symbol_y,
                        closed_symbol=found_symbol,
                        closed_order_id=order.id,
                        missing_symbol=missing_symbol,
                    )
                )
            except Exception as e:
                self._logger.error(f"❌ Failed to close {found_symbol}: {e}")

                self._emitter.emit(
                    TradeCloseErrorEvent(
                        pair_id=event.pair_id,
                        symbol_x=symbol_x,
                        symbol_y=symbol_y,
                        error_message=str(e),
                        failed_symbol=found_symbol,
                    )
                )
            return

        # No positions found
        self._logger.warning(f"⚠️ No positions found for pair {event.pair_id}")

    # =========================================================================
    # Position Operations with Retry
    # =========================================================================

    async def _open_position_with_retry(
        self,
        symbol: str,
        side: TradeSide,
        amount: float,
    ) -> Order:
        """Open a position with retry logic."""
        return await async_retry(
            lambda: self._exchange.open_position(
                symbol=symbol,
                side=side,
                amount=amount,
                leverage=self._leverage,
            ),
            max_retries=3,
            base_delay_ms=500,
            max_delay_ms=2000,
        )

    async def _close_position_with_retry(self, symbol: str) -> Order:
        """Close a position with retry logic."""
        return await async_retry(
            lambda: self._exchange.flash_close_position(symbol),
            max_retries=3,
            base_delay_ms=500,
            max_delay_ms=2000,
        )

    async def _rollback_position(self, symbol: str, order: Optional[Order]) -> None:
        """Rollback (close) a position after partial trade failure."""
        if not order:
            return

        self._logger.warning(f"🔄 Rolling back position | {symbol} | order={order.id}")

        try:
            await async_retry(
                lambda: self._exchange.flash_close_position(symbol),
                max_retries=3,
                base_delay_ms=1000,
                max_delay_ms=5000,
            )
            self._logger.info(f"✅ Rollback successful | {symbol}")
        except RetryError as e:
            self._logger.error(
                f"🚨 CRITICAL: Rollback failed | {symbol} | Manual intervention required! | {e}"
            )

    # =========================================================================
    # Helpers
    # =========================================================================

    def _emit_skipped(
        self,
        event: TradeEntryEvent,
        reason: TradeSkipReason,
        details: str,
    ) -> None:
        """Emit a TradeSkippedEvent."""
        self._emitter.emit(
            TradeSkippedEvent(
                pair_id=event.pair_id,
                symbol_x=event.symbol_x,
                symbol_y=event.symbol_y,
                reason=reason,
                details=details,
            )
        )

    def _release_symbols(self, symbol_x: str, symbol_y: str) -> None:
        """
        Release symbols from _active_symbols.

        Called when:
        - Trade fails to open
        - Trade is closed
        - Balance check fails
        """
        self._active_symbols.discard(symbol_x.lower())
        self._active_symbols.discard(symbol_y.lower())
        self._logger.debug(
            f"Released symbols: {symbol_x}, {symbol_y} | "
            f"active={len(self._active_symbols)}"
        )
