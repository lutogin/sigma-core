"""
Trading Service.

Executes trades based on entry/exit signals from OrchestratorService.

A spread trade consists of 2 opposite positions (delta-neutral):
- LONG spread: Buy COIN, Sell PRIMARY
- SHORT spread: Sell COIN, Buy PRIMARY

Architecture:
1. Listens to EntrySignalEvent from OrchestratorService
2. Calculates position sizes based on configured USDT amount and beta
3. Opens positions atomically with ACID guarantees
4. Rolls back on partial failures to maintain delta-neutrality
5. Uses PositionStateService for cooldown and timeout management
"""

import asyncio
from typing import Any, Optional

from src.domain.position_state import (
    PositionStateService,
    SpreadSide as StateSpreadSide,
)
from src.infra.event_emitter import (
    EventEmitter,
    EventType,
    EntrySignalEvent,
    SpreadSide,
    ExitReason,
)
from src.integrations.exchange import BinanceClient, Order, OrderSide


class TradingService:
    """
    Trading service that executes spread trades based on screener signals.

    Responsibilities:
    - Listen to entry signals from OrchestratorService
    - Calculate position sizes (USDT -> contracts)
    - Execute atomic position opens (ACID)
    - Handle failures with rollback to maintain delta-neutrality
    - Check position timeouts and manage cooldowns via PositionStateService
    """

    def __init__(
        self,
        event_emitter: EventEmitter,
        exchange_client: BinanceClient,
        position_state_service: PositionStateService,
        logger: Any,
        allow_trading: bool = False,
        position_size_usdt: float = 100.0,
        leverage: int = 1,
        max_open_spreads: int = 5,
        primary_symbol: str = "ETH/USDT:USDT",
    ):
        """
        Initialize trading service.

        Args:
            event_emitter: Event emitter for pub/sub.
            exchange_client: Binance exchange client.
            position_state_service: Service for position state management.
            logger: Logger instance.
            allow_trading: Enable/disable real trading.
            position_size_usdt: Base position size in USDT for COIN leg.
            leverage: Default leverage for positions.
            max_open_spreads: Maximum number of open spread positions.
            primary_symbol: Primary trading pair (e.g., "ETH/USDT:USDT").
        """
        self._emitter = event_emitter
        self._exchange = exchange_client
        self._position_state = position_state_service
        self._logger = logger
        self._allow_trading = allow_trading
        self._position_size_usdt = position_size_usdt
        self._leverage = leverage
        self._max_open_spreads = max_open_spreads
        self._primary_symbol = primary_symbol

        self._is_running = False

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> None:
        """
        Start the trading service.

        - Initializes position state service
        - Subscribes to entry signal events
        """
        if self._is_running:
            self._logger.warning("TradingService is already running")
            return

        # Initialize position state
        self._position_state.initialize()

        # Subscribe to entry signals
        self._emitter.on(EventType.ENTRY_SIGNAL, self._on_entry_signal)

        self._is_running = True

        active_count = self._position_state.count_active_positions()
        cooldowns = self._position_state.get_active_cooldowns()

        self._logger.info(
            f"🚀 TradingService started | "
            f"trading={'enabled' if self._allow_trading else 'DISABLED'} | "
            f"position_size={self._position_size_usdt} USDT | "
            f"leverage={self._leverage}x | "
            f"max_spreads={self._max_open_spreads} | "
            f"active_positions={active_count} | "
            f"active_cooldowns={len(cooldowns)}"
        )

    async def stop(self) -> None:
        """Stop the trading service."""
        if not self._is_running:
            return

        self._emitter.off(EventType.ENTRY_SIGNAL, self._on_entry_signal)
        self._is_running = False
        self._logger.info("🛑 TradingService stopped")

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._is_running

    # =========================================================================
    # Timeout Check (called by Orchestrator before scan)
    # =========================================================================

    async def check_and_close_timeouts(self) -> int:
        """
        Check for timed-out positions and close them.

        Should be called by OrchestratorService before each scan cycle.

        Returns:
            Number of positions closed due to timeout.
        """
        timed_out = self._position_state.check_timeouts()

        if not timed_out:
            return 0

        closed_count = 0
        for position, duration_minutes in timed_out:
            self._logger.warning(
                f"⏰ Closing position due to TIMEOUT | {position.coin_symbol} | "
                f"duration={duration_minutes:.0f} min"
            )

            try:
                await self._close_spread(
                    position.coin_symbol,
                    position.primary_symbol,
                    ExitReason.TIMEOUT,
                )
                closed_count += 1
            except Exception as e:
                self._logger.exception(
                    f"❌ Failed to close timed-out position {position.coin_symbol}: {e}"
                )

        return closed_count

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _on_entry_signal(self, event: EntrySignalEvent) -> None:
        """
        Handle entry signal from OrchestratorService.

        Validates conditions using PositionStateService and opens atomic spread positions.
        """
        self._logger.info(
            f"📨 Entry signal received | "
            f"coin={event.coin_symbol} | side={event.spread_side.value} | "
            f"z={event.z_score:.4f} | β={event.beta:.4f}"
        )

        # Check if trading is enabled
        if not self._allow_trading:
            self._logger.info("⚠️ Trading disabled - skipping entry")
            return

        try:
            # 1. Check if can open position (cooldown, overlap, max spreads)
            can_open, reason = self._position_state.can_open_position(
                coin_symbol=event.coin_symbol,
                primary_symbol=event.primary_symbol,
                max_spreads=self._max_open_spreads,
            )

            if not can_open:
                self._logger.info(f"⚠️ Cannot open position: {reason}")
                return

            # 2. Check balance
            balance = await self._exchange.get_balance("USDT")
            available = balance.free

            # Calculate required USDT for both legs
            coin_size_usdt = self._position_size_usdt
            primary_size_usdt = coin_size_usdt * abs(event.beta)
            total_required = coin_size_usdt + primary_size_usdt

            if available < total_required:
                self._logger.warning(
                    f"⚠️ Insufficient balance | "
                    f"available={available:.2f} | required={total_required:.2f}"
                )
                return

            # 3. Open atomic spread positions
            await self._open_spread(event, coin_size_usdt, primary_size_usdt)

        except Exception as e:
            self._logger.exception(f"❌ Error processing entry signal: {e}")

    # =========================================================================
    # Trade Execution (ACID)
    # =========================================================================

    async def _open_spread(
        self,
        event: EntrySignalEvent,
        coin_size_usdt: float,
        primary_size_usdt: float,
    ) -> None:
        """
        Open a spread trade (2 atomic positions).

        LONG spread: Buy COIN, Sell PRIMARY
        SHORT spread: Sell COIN, Buy PRIMARY

        Uses ACID principles - if one leg fails, the other is rolled back.
        """
        coin_symbol = event.coin_symbol
        primary_symbol = event.primary_symbol

        # Determine position sides based on spread direction
        if event.spread_side == SpreadSide.LONG:
            # LONG spread: Buy COIN, Sell PRIMARY
            coin_side = OrderSide.BUY
            primary_side = OrderSide.SELL
        else:
            # SHORT spread: Sell COIN, Buy PRIMARY
            coin_side = OrderSide.SELL
            primary_side = OrderSide.BUY

        self._logger.info(
            f"🎯 Opening spread | "
            f"{coin_symbol} ({coin_side.value}) {coin_size_usdt:.2f} USDT | "
            f"{primary_symbol} ({primary_side.value}) {primary_size_usdt:.2f} USDT"
        )

        # Calculate amounts in base currency (contracts)
        amount_coin = await self._exchange.calculate_amount_from_usdt(
            coin_symbol, coin_size_usdt
        )
        amount_primary = await self._exchange.calculate_amount_from_usdt(
            primary_symbol, primary_size_usdt
        )

        self._logger.debug(
            f"Calculated amounts: {coin_symbol}={amount_coin}, "
            f"{primary_symbol}={amount_primary}"
        )

        # Open positions atomically
        order_coin: Optional[Order] = None
        order_primary: Optional[Order] = None

        try:
            # Execute both positions in parallel
            results = await asyncio.gather(
                self._open_position_with_retry(
                    coin_symbol, coin_side, float(amount_coin)
                ),
                self._open_position_with_retry(
                    primary_symbol, primary_side, float(amount_primary)
                ),
                return_exceptions=True,
            )

            result_coin, result_primary = results

            # Check results
            coin_success = isinstance(result_coin, Order)
            primary_success = isinstance(result_primary, Order)

            if coin_success:
                order_coin = result_coin
            if primary_success:
                order_primary = result_primary

            # Both succeeded - register position in state service
            if coin_success and primary_success:
                coin_order_id = order_coin.id if order_coin else "N/A"
                primary_order_id = order_primary.id if order_primary else "N/A"

                # Get actual fill prices from orders
                coin_price = order_coin.price if order_coin else event.coin_price
                primary_price = (
                    order_primary.price if order_primary else event.primary_price
                )

                # Convert SpreadSide to StateSpreadSide
                state_side = (
                    StateSpreadSide.LONG
                    if event.spread_side == SpreadSide.LONG
                    else StateSpreadSide.SHORT
                )

                # Register position in state service
                self._position_state.register_position(
                    coin_symbol=coin_symbol,
                    primary_symbol=primary_symbol,
                    side=state_side,
                    entry_z_score=event.z_score,
                    entry_beta=event.beta,
                    entry_correlation=event.correlation,
                    entry_hurst=event.hurst,
                    coin_size_usdt=coin_size_usdt,
                    primary_size_usdt=primary_size_usdt,
                    coin_entry_price=coin_price,
                    primary_entry_price=primary_price,
                    z_tp_threshold=event.z_tp_threshold,
                    z_sl_threshold=event.z_sl_threshold,
                    leverage=self._leverage,
                )

                self._logger.info(
                    f"✅ Spread opened | {coin_symbol} order={coin_order_id} | "
                    f"{primary_symbol} order={primary_order_id}"
                )
                return

            # COIN succeeded but PRIMARY failed -> rollback COIN
            if coin_success and not primary_success:
                error_primary = (
                    result_primary
                    if isinstance(result_primary, Exception)
                    else Exception("Unknown error")
                )
                self._logger.error(f"❌ PRIMARY position failed: {error_primary}")

                await self._rollback_position(coin_symbol, order_coin)
                self._log_release_symbols(coin_symbol, primary_symbol)
                return

            # PRIMARY succeeded but COIN failed -> rollback PRIMARY
            if primary_success and not coin_success:
                error_coin = (
                    result_coin
                    if isinstance(result_coin, Exception)
                    else Exception("Unknown error")
                )
                self._logger.error(f"❌ COIN position failed: {error_coin}")

                await self._rollback_position(primary_symbol, order_primary)
                self._log_release_symbols(coin_symbol, primary_symbol)
                return

            # Both failed
            error_coin = (
                result_coin
                if isinstance(result_coin, Exception)
                else Exception("Unknown")
            )
            error_primary = (
                result_primary
                if isinstance(result_primary, Exception)
                else Exception("Unknown")
            )

            self._logger.error(
                f"❌ Both positions failed | COIN: {error_coin} | PRIMARY: {error_primary}"
            )
            self._log_release_symbols(coin_symbol, primary_symbol)

        except Exception as e:
            self._logger.exception(f"❌ Unexpected error opening spread: {e}")

            # Rollback any successful positions
            if order_coin:
                await self._rollback_position(coin_symbol, order_coin)
            if order_primary:
                await self._rollback_position(primary_symbol, order_primary)

            self._log_release_symbols(coin_symbol, primary_symbol)

    async def _open_position_with_retry(
        self,
        symbol: str,
        side: OrderSide,
        amount: float,
        max_retries: int = 3,
    ) -> Order:
        """
        Open a position with retry logic using limit orders.

        Args:
            symbol: Trading symbol.
            side: Order side.
            amount: Amount in base currency.
            max_retries: Maximum retry attempts.

        Returns:
            Filled Order.
        """
        last_error: Optional[Exception] = None

        for attempt in range(max_retries):
            try:
                order = await self._exchange.open_position_limit(
                    symbol=symbol,
                    side=side,
                    amount=amount,
                    leverage=self._leverage,
                    max_retries=5,
                    fallback_to_market=True,
                )
                return order

            except Exception as e:
                last_error = e
                self._logger.warning(
                    f"Position open attempt {attempt + 1}/{max_retries} failed for "
                    f"{symbol}: {e}"
                )

                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))

        raise last_error or Exception(f"Failed to open position for {symbol}")

    async def _rollback_position(self, symbol: str, order: Optional[Order]) -> None:
        """
        Rollback (close) a position after partial spread failure.

        This is critical for maintaining delta-neutrality.
        """
        if not order:
            return

        self._logger.warning(f"🔄 Rolling back position | {symbol} | order={order.id}")

        try:
            await self._exchange.flash_close_position(symbol)
            self._logger.info(f"✅ Rollback successful | {symbol}")

        except Exception as e:
            self._logger.error(
                f"🚨 CRITICAL: Rollback failed | {symbol} | "
                f"Manual intervention required! | {e}"
            )

    # =========================================================================
    # Spread Close
    # =========================================================================

    async def _close_spread(
        self,
        coin_symbol: str,
        primary_symbol: str,
        exit_reason: ExitReason,
    ) -> bool:
        """
        Close a spread position (both legs).

        Args:
            coin_symbol: Coin symbol to close.
            primary_symbol: Primary symbol to close.
            exit_reason: Reason for closing.

        Returns:
            True if closed successfully.
        """
        self._logger.info(
            f"🔒 Closing spread | {coin_symbol} | reason={exit_reason.value}"
        )

        try:
            # Close both legs in parallel
            results = await asyncio.gather(
                self._exchange.flash_close_position(coin_symbol),
                self._exchange.flash_close_position(primary_symbol),
                return_exceptions=True,
            )

            coin_result, primary_result = results
            coin_success = not isinstance(coin_result, Exception)
            primary_success = not isinstance(primary_result, Exception)

            if not coin_success:
                self._logger.error(
                    f"❌ Failed to close COIN position {coin_symbol}: {coin_result}"
                )
            if not primary_success:
                self._logger.error(
                    f"❌ Failed to close PRIMARY position {primary_symbol}: {primary_result}"
                )

            # Update position state (applies cooldown if SL/CORRELATION_DROP)
            self._position_state.close_position(coin_symbol, exit_reason)

            if coin_success and primary_success:
                self._logger.info(
                    f"✅ Spread closed | {coin_symbol} | reason={exit_reason.value}"
                )
                return True
            else:
                self._logger.warning(
                    f"⚠️ Partial spread close | {coin_symbol} | "
                    f"coin={'ok' if coin_success else 'FAILED'} | "
                    f"primary={'ok' if primary_success else 'FAILED'}"
                )
                return False

        except Exception as e:
            self._logger.exception(f"❌ Error closing spread {coin_symbol}: {e}")
            return False

    async def close_position_with_reason(
        self,
        coin_symbol: str,
        exit_reason: ExitReason,
    ) -> bool:
        """
        Public method to close a position with a given exit reason.

        Used by OrchestratorService when detecting exit conditions (TP, SL, etc.)

        Args:
            coin_symbol: Coin symbol of the spread to close.
            exit_reason: Reason for closing.

        Returns:
            True if closed successfully.
        """
        position = self._position_state.get_position(coin_symbol)
        if not position:
            self._logger.warning(f"No active position found for {coin_symbol}")
            return False

        return await self._close_spread(
            coin_symbol=position.coin_symbol,
            primary_symbol=position.primary_symbol,
            exit_reason=exit_reason,
        )

    # =========================================================================
    # Helpers
    # =========================================================================

    def _log_release_symbols(self, coin_symbol: str, primary_symbol: str) -> None:
        """Log symbol release (state is managed by PositionStateService)."""
        self._logger.debug(f"Released symbols: {coin_symbol}, {primary_symbol}")
