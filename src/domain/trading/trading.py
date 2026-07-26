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
import math
from typing import Any, Optional
from uuid import uuid4

from src.domain.position_state import (
    PositionStateService,
    SpreadSide as StateSpreadSide,
)
from src.infra.event_emitter import (
    BaseEvent,
    EventEmitter,
    EventType,
    EntrySignalEvent,
    ExitSignalEvent,
    SpreadSide,
    ExitReason,
    TradeOpenedEvent,
    TradeClosedEvent,
    TradeFailedEvent,
    TradeCloseErrorEvent,
)
from src.integrations.exchange import BinanceClient, Order, OrderSide, TradeSide


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
        leverage: int = 3,
        max_open_spreads: int = 3,
        primary_symbol: str = "ETH/USDT:USDT",
        target_halflife_bars: float = 12.0,
        min_size_multiplier: float = 0.5,
        max_size_multiplier: float = 2.0,
        max_coin_notional_pct: float = 0.10,
        max_margin_utilization: float = 0.50,
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
            target_halflife_bars: Target half-life (baseline for position sizing).
            min_size_multiplier: Minimum position size multiplier (slow reversion).
            max_size_multiplier: Maximum position size multiplier (fast reversion).
        """
        if leverage <= 0 or max_open_spreads <= 0:
            raise ValueError("leverage and max_open_spreads must be positive")
        if position_size_usdt <= 0:
            raise ValueError("position_size_usdt must be positive")
        if not 0 < max_coin_notional_pct <= 1:
            raise ValueError("max_coin_notional_pct must be in (0, 1]")
        if not 0 < max_margin_utilization <= 1:
            raise ValueError("max_margin_utilization must be in (0, 1]")

        self._emitter = event_emitter
        self._exchange = exchange_client
        self._position_state = position_state_service
        self._logger = logger
        self._allow_trading = allow_trading
        self._position_size_usdt = position_size_usdt
        self._leverage = leverage
        self._max_open_spreads = max_open_spreads
        self._primary_symbol = primary_symbol

        # Dynamic position sizing based on Half-Life
        self._target_halflife = target_halflife_bars
        self._min_size_mult = min_size_multiplier
        self._max_size_mult = max_size_multiplier
        self._max_coin_notional_pct = max_coin_notional_pct
        self._max_margin_utilization = max_margin_utilization

        self._is_running = False
        self._execution_ready = False
        self._entry_lock = asyncio.Lock()
        self._close_locks: dict[str, asyncio.Lock] = {}

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> None:
        """
        Start the trading service.

        - Initializes position state service
        - Subscribes to entry and exit signal events
        """
        if self._is_running:
            self._logger.warning("TradingService is already running")
            return

        # Initialize position state
        self._position_state.initialize()
        active_count = self._position_state.count_active_positions()

        # Existing positions must remain closable even when new entries are
        # disabled. Validate private exchange state whenever either is true.
        if self._allow_trading or active_count > 0:
            await self._prepare_execution()

        # Subscribe to trading signals
        self._emitter.on(EventType.ENTRY_SIGNAL, self._on_entry_signal)
        self._emitter.on(EventType.EXIT_SIGNAL, self._on_exit_signal)

        self._is_running = True

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
        self._emitter.off(EventType.EXIT_SIGNAL, self._on_exit_signal)
        self._is_running = False
        self._logger.info("🛑 TradingService stopped")

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._is_running

    @property
    def is_trading_allowed(self) -> bool:
        """Check if trading is currently allowed."""
        return self._allow_trading

    async def enable_trading(self) -> None:
        """Validate exchange state, then enable new entries at runtime."""
        await self._prepare_execution()
        self._allow_trading = True
        self._logger.info("✅ New entries ENABLED via runtime control")

    def disable_trading(self) -> None:
        """Disable new entries while preserving risk-reducing exits."""
        self._allow_trading = False
        self._logger.info("🛑 New entries DISABLED; exits remain enabled")

    async def _prepare_execution(self) -> None:
        """Fail closed unless exchange mode and persisted exposure agree."""
        await self._exchange.connect()
        if not await self._exchange.get_position_mode():
            self._execution_ready = False
            raise RuntimeError(
                "Binance Hedge Mode is required; refusing live execution"
            )
        await self._assert_exchange_state_matches_storage()
        self._execution_ready = True

    async def _assert_exchange_state_matches_storage(self) -> None:
        expected: dict[tuple[str, str], float] = {}
        for position in self._position_state.get_active_positions():
            coin_side = "long" if position.side.value == "long" else "short"
            primary_side = "short" if position.side.value == "long" else "long"
            if not position.coin_leg_closed:
                key = (position.coin_symbol, coin_side)
                expected[key] = expected.get(key, 0.0) + position.coin_contracts
            if not position.primary_leg_closed:
                key = (position.primary_symbol, primary_side)
                expected[key] = expected.get(key, 0.0) + position.primary_contracts

        actual: dict[tuple[str, str], float] = {}
        for position in await self._exchange.get_positions(skip_zero=True):
            key = (position.symbol, position.side)
            actual[key] = actual.get(key, 0.0) + position.contracts

        mismatches = []
        for key in sorted(set(expected) | set(actual)):
            expected_amount = expected.get(key, 0.0)
            actual_amount = actual.get(key, 0.0)
            tolerance = max(1e-8, expected_amount * 1e-6)
            if not math.isclose(
                expected_amount,
                actual_amount,
                rel_tol=1e-6,
                abs_tol=tolerance,
            ):
                mismatches.append(
                    f"{key[0]}:{key[1]} stored={expected_amount:.12g} "
                    f"exchange={actual_amount:.12g}"
                )

        if mismatches:
            self._execution_ready = False
            raise RuntimeError(
                "Exchange exposure does not match persisted spread state: "
                + "; ".join(mismatches)
            )

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
                closed = await self._close_spread(
                    position.coin_symbol,
                    position.primary_symbol,
                    ExitReason.TIMEOUT,
                )
                if closed:
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
        if not self._execution_ready:
            self._logger.error(
                "Live execution has not passed startup reconciliation; "
                "skipping entry"
            )
            return

        async with self._entry_lock:
            await self._process_entry_signal(event)

    async def _process_entry_signal(self, event: EntrySignalEvent) -> None:
        """Validate risk and execute one serialized entry request."""
        try:
            # 1. Check if can open position (cooldown, overlap, max spreads)
            can_open, reason = self._position_state.can_open_position(
                coin_symbol=event.coin_symbol,
                primary_symbol=event.primary_symbol,
                max_spreads=self._max_open_spreads,
            )

            if not can_open:
                self._logger.warning(f"⚠️ Cannot open position: {reason}")
                return

            if (
                not math.isfinite(event.beta)
                or abs(event.beta) <= 0
                or not math.isfinite(event.halflife)
                or event.halflife <= 0
            ):
                self._logger.error(
                    f"Invalid sizing inputs for {event.coin_symbol}: "
                    f"beta={event.beta}, halflife={event.halflife}"
                )
                return

            # 2. Calculate dynamic position size based on half-life
            size_multiplier = self._calculate_size_multiplier(event.halflife)
            balance = await self._exchange.get_balance("USDT")
            equity = max(0.0, balance.total)
            base_size = min(
                self._position_size_usdt,
                equity * self._max_coin_notional_pct,
            )
            coin_size_usdt = base_size * size_multiplier
            primary_size_usdt = coin_size_usdt * abs(event.beta)
            total_required = coin_size_usdt + primary_size_usdt

            self._logger.info(
                f"📊 Position sizing: requested={self._position_size_usdt:.0f}, "
                f"risk_capped_base={base_size:.0f} × "
                f"mult={size_multiplier:.2f}x (HL={event.halflife:.1f}) = "
                f"coin={coin_size_usdt:.2f} USDT"
            )

            # 3. Check balance (margin required = notional / leverage)
            available = balance.free
            margin_required = total_required / self._leverage
            margin_cap = equity * self._max_margin_utilization

            if (
                equity <= 0
                or available < margin_required
                or balance.used + margin_required > margin_cap
            ):
                self._logger.warning(
                    f"⚠️ Margin/risk cap blocked entry | "
                    f"available={available:.2f} | "
                    f"used={balance.used:.2f} | "
                    f"required={margin_required:.2f} | cap={margin_cap:.2f} "
                    f"(notional={total_required:.2f} / {self._leverage}x)"
                )
                return

            # 4. Open atomic spread positions
            await self._open_spread(event, coin_size_usdt, primary_size_usdt)

        except Exception as e:
            self._logger.exception(f"❌ Error processing entry signal: {e}")

    async def _on_exit_signal(self, event: ExitSignalEvent) -> None:
        """
        Handle exit signal from OrchestratorService.

        Closes the spread position for the given symbol.
        """
        self._logger.info(
            f"📨 Exit signal received | "
            f"coin={event.coin_symbol} | reason={event.exit_reason.value} | "
            f"z={event.current_z_score:.4f}"
        )

        try:
            # Get position from state
            position = self._position_state.get_position(event.coin_symbol)
            if not position:
                self._logger.warning(
                    f"⚠️ No active position found for {event.coin_symbol}"
                )
                return

            # Close the spread
            await self._close_spread(
                coin_symbol=event.coin_symbol,
                primary_symbol=event.primary_symbol,
                exit_reason=event.exit_reason,
            )

        except Exception as e:
            self._logger.exception(f"❌ Error processing exit signal: {e}")

    # =========================================================================
    # Trade Execution (ACID)
    # =========================================================================

    async def _safe_emit(self, event: BaseEvent) -> None:
        """
        Emit event without breaking the trade lifecycle on emitter errors.

        Trading state and exchange side-effects are already committed by the time
        these lifecycle events are emitted, so emitter failures should be logged
        but must not crash the workflow.
        """
        try:
            await self._emitter.emit(event)
        except Exception as e:
            self._logger.exception(
                f"Failed to emit {event.event_type.value} for "
                f"{getattr(event, 'coin_symbol', 'N/A')}: {e}"
            )

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

        # Store as floats for position tracking
        coin_contracts = float(amount_coin)
        primary_contracts = float(amount_primary)

        self._logger.debug(
            f"Calculated amounts: {coin_symbol}={coin_contracts}, "
            f"{primary_symbol}={primary_contracts}"
        )

        # Open positions atomically
        order_coin: Optional[Order] = None
        order_primary: Optional[Order] = None
        operation_id = uuid4().hex[:16]

        try:
            # Execute both positions in parallel
            results = await asyncio.gather(
                self._open_position_with_retry(
                    coin_symbol,
                    coin_side,
                    coin_contracts,
                    client_order_id=f"sg-e-{operation_id}-c",
                ),
                self._open_position_with_retry(
                    primary_symbol,
                    primary_side,
                    primary_contracts,
                    client_order_id=f"sg-e-{operation_id}-p",
                ),
                return_exceptions=True,
            )

            result_coin, result_primary = results

            # Check results
            if isinstance(result_coin, Order):
                order_coin = result_coin
            if isinstance(result_primary, Order):
                order_primary = result_primary
            coin_success = self._is_filled_order(order_coin)
            primary_success = self._is_filled_order(order_primary)

            # Both succeeded - register position in state service
            if coin_success and primary_success:
                coin_order_id = order_coin.id if order_coin else "N/A"
                primary_order_id = order_primary.id if order_primary else "N/A"

                # Get actual fill prices from orders
                coin_price = order_coin.price if order_coin else event.coin_price
                primary_price = (
                    order_primary.price if order_primary else event.primary_price
                )
                actual_coin_contracts = float(order_coin.filled)
                actual_primary_contracts = float(order_primary.filled)
                actual_coin_notional = (
                    actual_coin_contracts * coin_price
                    if coin_price > 0
                    else coin_size_usdt
                )
                actual_primary_notional = (
                    actual_primary_contracts * primary_price
                    if primary_price > 0
                    else primary_size_usdt
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
                    entry_halflife=event.halflife,
                    coin_size_usdt=actual_coin_notional,
                    primary_size_usdt=actual_primary_notional,
                    coin_contracts=actual_coin_contracts,
                    primary_contracts=actual_primary_contracts,
                    coin_entry_price=coin_price,
                    primary_entry_price=primary_price,
                    z_tp_threshold=event.z_tp_threshold,
                    z_sl_threshold=event.z_sl_threshold,
                    spread_mean=event.spread_mean,
                    spread_std=event.spread_std,
                    leverage=self._leverage,
                )

                self._logger.info(
                    f"✅ Spread opened | {coin_symbol} order={coin_order_id} | "
                    f"{primary_symbol} order={primary_order_id}"
                )

                # Emit TradeOpenedEvent
                await self._safe_emit(
                    TradeOpenedEvent(
                        coin_symbol=coin_symbol,
                        primary_symbol=primary_symbol,
                        spread_side=event.spread_side,
                        z_score=event.z_score,
                        beta=event.beta,
                        correlation=event.correlation,
                        hurst=event.hurst,
                        halflife=event.halflife,
                        spread_mean=event.spread_mean,
                        spread_std=event.spread_std,
                        coin_size_usdt=actual_coin_notional,
                        primary_size_usdt=actual_primary_notional,
                        coin_price=coin_price,
                        primary_price=primary_price,
                        coin_order_id=coin_order_id,
                        primary_order_id=primary_order_id,
                        z_tp_threshold=event.z_tp_threshold,
                        z_sl_threshold=event.z_sl_threshold,
                    ),
                )
                return

            # At least one leg failed. Roll back every leg with a confirmed
            # fill, including partial fills, and never close an entire shared
            # PRIMARY hedge.
            error_coin = (
                result_coin
                if isinstance(result_coin, Exception)
                else Exception(
                    f"unfilled/partial order status={getattr(order_coin, 'status', None)}"
                )
            )
            error_primary = (
                result_primary
                if isinstance(result_primary, Exception)
                else Exception(
                    "unfilled/partial order "
                    f"status={getattr(order_primary, 'status', None)}"
                )
            )

            self._logger.error(
                f"❌ Spread entry incomplete | COIN: {error_coin} | "
                f"PRIMARY: {error_primary}"
            )
            rollback_results = []
            if order_coin and order_coin.filled > 0:
                rollback_results.append(
                    await self._rollback_position(coin_symbol, order_coin)
                )
            if order_primary and order_primary.filled > 0:
                rollback_results.append(
                    await self._rollback_position(primary_symbol, order_primary)
                )
            self._log_release_symbols(coin_symbol, primary_symbol)

            await self._safe_emit(
                TradeFailedEvent(
                    coin_symbol=coin_symbol,
                    primary_symbol=primary_symbol,
                    error_message=f"COIN: {error_coin} | PRIMARY: {error_primary}",
                    failed_leg=(
                        "primary"
                        if coin_success
                        else "coin" if primary_success else "both"
                    ),
                    rollback_performed=bool(rollback_results) and all(rollback_results),
                ),
            )

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
        client_order_id: str,
    ) -> Order:
        """
        Open a position through the exchange's bounded IOC retry loop.

        Args:
            symbol: Trading symbol.
            side: Order side.
            amount: Amount in base currency.
            client_order_id: Stable operation id prefix for recovery/audit.

        Returns:
            Filled Order.
        """
        return await self._exchange.open_position_limit(
            symbol=symbol,
            side=side,
            amount=amount,
            leverage=self._leverage,
            max_retries=5,
            fallback_to_market=False,
            client_order_id=client_order_id,
        )

    @staticmethod
    def _is_filled_order(order: Optional[Order]) -> bool:
        return bool(
            order
            and order.status == "closed"
            and order.filled > 0
            and order.remaining <= max(1e-12, order.amount * 1e-8)
        )

    async def _rollback_position(self, symbol: str, order: Optional[Order]) -> bool:
        """
        Rollback (close) a position after partial spread failure.

        This is critical for maintaining delta-neutrality.
        """
        if not order or order.filled <= 0:
            return False

        self._logger.warning(f"🔄 Rolling back position | {symbol} | order={order.id}")

        try:
            close_side: TradeSide = "sell" if order.side.lower() == "buy" else "buy"
            close_order = await self._exchange.flash_close_position(
                symbol,
                amount=order.filled,
                close_side=close_side,
                client_order_id=f"sg-r-{uuid4().hex[:20]}",
            )
            if not self._is_filled_order(close_order):
                raise RuntimeError(f"rollback order {close_order.id} not fully filled")
            self._logger.info(f"✅ Rollback successful | {symbol}")
            return True

        except Exception as e:
            self._logger.error(
                f"🚨 CRITICAL: Rollback failed | {symbol} | "
                f"Manual intervention required! | {e}"
            )
            self._allow_trading = False
            self._execution_ready = False
            return False

    # =========================================================================
    # Spread Close
    # =========================================================================

    async def _close_spread(
        self,
        coin_symbol: str,
        primary_symbol: str,
        exit_reason: ExitReason,
    ) -> bool:
        """Serialize duplicate exit signals for the same spread."""
        lock = self._close_locks.setdefault(coin_symbol, asyncio.Lock())
        async with lock:
            return await self._close_spread_locked(
                coin_symbol,
                primary_symbol,
                exit_reason,
            )

    async def _close_spread_locked(
        self,
        coin_symbol: str,
        primary_symbol: str,
        exit_reason: ExitReason,
    ) -> bool:
        """
        Close a spread position (both legs).

        COIN is closed entirely, PRIMARY is closed only by the amount
        that was opened for this specific spread (partial close).

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

        # Get position data before closing (for event and partial close amount)
        position = self._position_state.get_position(coin_symbol)

        if not position:
            self._logger.warning(f"No position found for {coin_symbol}")
            return False

        primary_symbol = position.primary_symbol
        coin_close_side: TradeSide = "sell" if position.side.value == "long" else "buy"
        primary_close_side: TradeSide = (
            "buy" if position.side.value == "long" else "sell"
        )

        try:
            self._logger.info(
                f"Closing exact spread quantities | "
                f"COIN {position.coin_contracts:.6f} "
                f"(already_closed={position.coin_leg_closed}) | "
                f"PRIMARY {position.primary_contracts:.6f} "
                f"(already_closed={position.primary_leg_closed})"
            )

            operation_id = uuid4().hex[:16]
            calls = []
            labels = []
            if not position.coin_leg_closed:
                labels.append("coin")
                calls.append(
                    self._exchange.flash_close_position(
                        coin_symbol,
                        amount=position.coin_contracts,
                        close_side=coin_close_side,
                        client_order_id=f"sg-x-{operation_id}-c",
                    )
                )
            if not position.primary_leg_closed:
                labels.append("primary")
                calls.append(
                    self._exchange.flash_close_position(
                        primary_symbol,
                        amount=position.primary_contracts,
                        close_side=primary_close_side,
                        client_order_id=f"sg-x-{operation_id}-p",
                    )
                )

            raw_results = await asyncio.gather(
                *calls,
                return_exceptions=True,
            )
            results = dict(zip(labels, raw_results))
            errors = []

            coin_success = position.coin_leg_closed
            primary_success = position.primary_leg_closed
            for leg in labels:
                result = results[leg]
                if isinstance(result, Order) and self._is_filled_order(result):
                    self._position_state.mark_leg_closed(coin_symbol, leg)
                    if leg == "coin":
                        coin_success = True
                    else:
                        primary_success = True
                else:
                    error = (
                        result
                        if isinstance(result, Exception)
                        else RuntimeError(
                            f"order {getattr(result, 'id', 'N/A')} not fully filled"
                        )
                    )
                    errors.append(f"{leg.upper()}: {error}")
                    self._logger.error(
                        f"❌ Failed to close {leg.upper()} leg for "
                        f"{coin_symbol}: {error}"
                    )

            if coin_success and primary_success:
                self._position_state.close_position(coin_symbol, exit_reason)

                self._logger.info(
                    f"✅ Spread closed | {coin_symbol} | reason={exit_reason.value} | "
                    f"PRIMARY partial close: {position.primary_contracts:.6f} contracts"
                )

                coin_result = results.get("coin")
                primary_result = results.get("primary")
                await self._safe_emit(
                    TradeClosedEvent(
                        coin_symbol=coin_symbol,
                        primary_symbol=primary_symbol,
                        exit_reason=exit_reason,
                        spread_side=(
                            SpreadSide.LONG
                            if position.side.value == "long"
                            else SpreadSide.SHORT
                        ),
                        entry_z_score=position.entry_z_score,
                        exit_z_score=0.0,
                        coin_entry_price=position.coin_entry_price,
                        primary_entry_price=position.primary_entry_price,
                        coin_exit_price=(
                            coin_result.price if isinstance(coin_result, Order) else 0.0
                        ),
                        primary_exit_price=(
                            primary_result.price
                            if isinstance(primary_result, Order)
                            else 0.0
                        ),
                        coin_size_usdt=position.coin_size_usdt,
                        primary_size_usdt=position.primary_size_usdt,
                    ),
                )
                return True

            self._logger.warning(
                f"⚠️ Partial spread close | {coin_symbol} | "
                f"coin={'closed' if coin_success else 'OPEN'} | "
                f"primary={'closed' if primary_success else 'OPEN'}"
            )
            await self._safe_emit(
                TradeCloseErrorEvent(
                    coin_symbol=coin_symbol,
                    primary_symbol=primary_symbol,
                    exit_reason=exit_reason,
                    error_message=" | ".join(errors),
                    coin_closed=coin_success,
                    primary_closed=primary_success,
                ),
            )
            return False

        except Exception as e:
            self._logger.exception(f"❌ Error closing spread {coin_symbol}: {e}")
            self._allow_trading = False
            self._execution_ready = False

            await self._safe_emit(
                TradeCloseErrorEvent(
                    coin_symbol=coin_symbol,
                    primary_symbol=primary_symbol,
                    exit_reason=exit_reason,
                    error_message=str(e),
                    coin_closed=position.coin_leg_closed,
                    primary_closed=position.primary_leg_closed,
                ),
            )
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

    def _calculate_size_multiplier(self, halflife: float) -> float:
        """
        Calculate position size multiplier based on half-life.

        Formula: multiplier = sqrt(TargetHalfLife / CurrentHalfLife)
        Using sqrt dampens extreme values:
        - Fast reversion (low HL) → larger size, but not as aggressive
        - Slow reversion (high HL) → smaller size, but not as punishing

        Example with target=12:
        - HL=3  → sqrt(12/3)  = sqrt(4)  = 2.0x (capped)
        - HL=6  → sqrt(12/6)  = sqrt(2)  = 1.41x
        - HL=12 → sqrt(12/12) = sqrt(1)  = 1.0x (baseline)
        - HL=24 → sqrt(12/24) = sqrt(0.5)= 0.71x
        - HL=48 → sqrt(12/48) = sqrt(0.25)= 0.5x (floor)

        Args:
            halflife: Half-life in bars for the spread.

        Returns:
            Position size multiplier (clamped to [min, max]).
        """
        import math

        if halflife <= 0:
            # Invalid halflife, use base size (multiplier = 1.0)
            self._logger.error(f"Invalid halflife={halflife}, using multiplier=1.0")
            return 1.0

        # Use sqrt to dampen extreme multipliers
        raw_multiplier = math.sqrt(self._target_halflife / halflife)

        # Clamp to limits
        clamped = max(self._min_size_mult, min(self._max_size_mult, raw_multiplier))

        self._logger.debug(
            f"Size multiplier: sqrt({self._target_halflife} / "
            f"{halflife:.1f}) = {raw_multiplier:.2f} → "
            f"clamped={clamped:.2f}x"
        )

        return clamped

    def _log_release_symbols(self, coin_symbol: str, primary_symbol: str) -> None:
        """Log symbol release (state is managed by PositionStateService)."""
        self._logger.debug(f"Released symbols: {coin_symbol}, {primary_symbol}")
