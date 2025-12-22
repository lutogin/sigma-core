"""
Integration tests for EntryObserverService.

Tests trailing entry logic with REAL WebSocket connections to Binance.
Verifies:
1. WebSocket subscription works
2. Z-score calculation is correct
3. Entry execution triggers on pullback (reversal)

Run with:
    pytest src/domain/entry_observer/tests/test_entry_observer_integration.py -v -s

Note: Uses real Binance WebSocket connections, no mocking.
"""

import pytest
import asyncio
import math
from typing import List

from loguru import logger

from src.domain.entry_observer import EntryObserverService, WatchCandidate
from src.domain.entry_observer.models import WatchStatus
from src.infra.event_emitter import (
    EventEmitter,
    EventType,
    PendingEntrySignalEvent,
    EntrySignalEvent,
    WatchCancelledEvent,
    SpreadSide,
)
from src.integrations.exchange import BinanceClient


class TestEntryObserverIntegration:
    """Integration tests for EntryObserverService with real WebSockets."""

    @pytest.mark.asyncio
    async def test_websocket_subscription_and_price_updates(
        self,
        binance_client: BinanceClient,
        event_emitter: EventEmitter,
        test_logger,
        primary_symbol: str,
        test_coin_symbol: str,
    ):
        """
        Test that WebSocket subscriptions work and prices are updated.
        """
        service = EntryObserverService(
            event_emitter=event_emitter,
            exchange_client=binance_client,
            logger=test_logger,
            primary_symbol=primary_symbol,
            z_entry_threshold=0.1,
            z_sl_threshold=100.0,
            pullback=0.05,
            watch_timeout_seconds=120,
            debounce_seconds=0.5,
            max_watches=5,
        )

        async with binance_client:
            await service.start()
            assert service.is_running, "Service should be running"

            # Verify service subscribed to events
            listener_count = event_emitter.get_listener_count(EventType.PENDING_ENTRY_SIGNAL)
            test_logger.info(f"Listeners for PENDING_ENTRY_SIGNAL after start: {listener_count}")
            assert listener_count >= 1, "Service should subscribe to PENDING_ENTRY_SIGNAL"

            coin_price = float(await binance_client.get_current_price(test_coin_symbol))
            primary_price = float(await binance_client.get_current_price(primary_symbol))

            test_logger.info(f"Initial prices: {test_coin_symbol}={coin_price}, {primary_symbol}={primary_price}")

            beta = 0.5
            spread = math.log(coin_price) - beta * math.log(primary_price)
            # Set spread_mean so that current Z will be above entry threshold
            # Z = (spread - spread_mean) / spread_std
            # We want Z > 0.1, so spread_mean should be less than spread
            spread_std = 0.01
            spread_mean = spread - (2.5 * spread_std)  # This gives Z = 2.5

            pending_event = PendingEntrySignalEvent(
                coin_symbol=test_coin_symbol,
                primary_symbol=primary_symbol,
                spread_side=SpreadSide.SHORT,
                z_score=2.5,
                beta=beta,
                correlation=0.85,
                hurst=0.35,
                spread_mean=spread_mean,
                spread_std=spread_std,
                coin_price=coin_price,
                primary_price=primary_price,
                z_entry_threshold=0.1,
                z_tp_threshold=0.0,
                z_sl_threshold=100.0,
            )

            test_logger.info(f"📤 Emitting PendingEntrySignalEvent for {test_coin_symbol}")

            await event_emitter.emit(pending_event)

            # Give event loop time to process the async handler and WebSocket to connect
            await asyncio.sleep(3.5)

            test_logger.info(f"Active watches: {service.active_watch_count}, keys: {list(service.get_active_watches().keys())}")

            assert service.active_watch_count == 1, "Should have 1 active watch"
            assert test_coin_symbol in service.get_active_watches()

            watch = service.get_active_watches()[test_coin_symbol]
            test_logger.info(
                f"Watch status: coin_price={watch.coin_price:.4f}, "
                f"primary_price={watch.primary_price:.4f}, "
                f"current_z={watch.current_z_score:.4f}"
            )

            assert watch.coin_price > 0, "Coin price should be updated via WebSocket"
            assert watch.primary_price > 0, "Primary price should be updated via WebSocket"

            await asyncio.sleep(2)

            watch = service.get_active_watches()[test_coin_symbol]
            test_logger.info(
                f"After 2s: coin_price={watch.coin_price:.4f}, "
                f"primary_price={watch.primary_price:.4f}, "
                f"current_z={watch.current_z_score:.4f}, "
                f"max_z={watch.max_z:.4f}"
            )

            await service.stop()
            assert not service.is_running
            assert service.active_watch_count == 0

        test_logger.info("✅ WebSocket subscription test passed!")


    @pytest.mark.asyncio
    async def test_z_score_calculation_accuracy(
        self,
        binance_client: BinanceClient,
        event_emitter: EventEmitter,
        test_logger,
        primary_symbol: str,
        test_coin_symbol: str,
    ):
        """Test that Z-score is calculated correctly from live prices."""
        service = EntryObserverService(
            event_emitter=event_emitter,
            exchange_client=binance_client,
            logger=test_logger,
            primary_symbol=primary_symbol,
            z_entry_threshold=0.1,
            z_sl_threshold=100.0,
            pullback=0.05,
            watch_timeout_seconds=120,
            debounce_seconds=0.5,
            max_watches=5,
        )

        async with binance_client:
            await service.start()

            coin_price = float(await binance_client.get_current_price(test_coin_symbol))
            primary_price = float(await binance_client.get_current_price(primary_symbol))

            beta = 0.6
            current_spread = math.log(coin_price) - beta * math.log(primary_price)
            spread_mean = current_spread - 0.02
            spread_std = 0.01

            expected_z = (current_spread - spread_mean) / spread_std
            test_logger.info(f"Expected initial Z-score: {expected_z:.4f}")

            pending_event = PendingEntrySignalEvent(
                coin_symbol=test_coin_symbol,
                primary_symbol=primary_symbol,
                spread_side=SpreadSide.SHORT if expected_z > 0 else SpreadSide.LONG,
                z_score=expected_z,
                beta=beta,
                correlation=0.85,
                hurst=0.35,
                spread_mean=spread_mean,
                spread_std=spread_std,
                coin_price=coin_price,
                primary_price=primary_price,
                z_entry_threshold=0.1,
                z_tp_threshold=0.0,
                z_sl_threshold=100.0,
            )

            await event_emitter.emit(pending_event)
            await asyncio.sleep(3)

            watch = service.get_active_watches().get(test_coin_symbol)
            assert watch is not None, "Watch should exist"

            actual_spread = math.log(watch.coin_price) - beta * math.log(watch.primary_price)
            expected_z_from_watch = (actual_spread - spread_mean) / spread_std

            test_logger.info(
                f"Z-score verification:\n"
                f"  Watch.current_z_score: {watch.current_z_score:.4f}\n"
                f"  Calculated from prices: {expected_z_from_watch:.4f}\n"
                f"  Difference: {abs(watch.current_z_score - expected_z_from_watch):.6f}"
            )

            assert abs(watch.current_z_score - expected_z_from_watch) < 0.001

            await service.stop()

        test_logger.info("✅ Z-score calculation test passed!")


    @pytest.mark.asyncio
    async def test_entry_execution_on_pullback(
        self,
        binance_client: BinanceClient,
        event_emitter: EventEmitter,
        test_logger,
        primary_symbol: str,
        test_coin_symbol: str,
    ):
        """Test that entry is executed when Z-score pulls back from maximum."""
        entry_signals_received: List[EntrySignalEvent] = []

        async def on_entry_signal(event: EntrySignalEvent):
            test_logger.info(f"🎯 Entry signal received: {event.coin_symbol}, Z={event.z_score:.4f}")
            entry_signals_received.append(event)

        event_emitter.on(EventType.ENTRY_SIGNAL, on_entry_signal)

        pullback = 0.1

        service = EntryObserverService(
            event_emitter=event_emitter,
            exchange_client=binance_client,
            logger=test_logger,
            primary_symbol=primary_symbol,
            z_entry_threshold=0.5,
            z_sl_threshold=100.0,
            pullback=pullback,
            watch_timeout_seconds=120,
            debounce_seconds=0.1,
            max_watches=5,
        )

        async with binance_client:
            await service.start()

            coin_price = float(await binance_client.get_current_price(test_coin_symbol))
            primary_price = float(await binance_client.get_current_price(primary_symbol))

            beta = 0.5
            current_spread = math.log(coin_price) - beta * math.log(primary_price)

            initial_z = 2.0
            spread_std = 0.01
            spread_mean = current_spread - (initial_z * spread_std)

            pending_event = PendingEntrySignalEvent(
                coin_symbol=test_coin_symbol,
                primary_symbol=primary_symbol,
                spread_side=SpreadSide.SHORT,
                z_score=initial_z,
                beta=beta,
                correlation=0.85,
                hurst=0.35,
                spread_mean=spread_mean,
                spread_std=spread_std,
                coin_price=coin_price,
                primary_price=primary_price,
                z_entry_threshold=0.5,
                z_tp_threshold=0.0,
                z_sl_threshold=100.0,
            )

            await event_emitter.emit(pending_event)
            await asyncio.sleep(2)

            watch = service.get_active_watches().get(test_coin_symbol)
            assert watch is not None, "Watch should exist"

            test_logger.info(f"Watch created: max_z={watch.max_z:.4f}, current_z={watch.current_z_score:.4f}")

            # Simulate peak by setting max_z higher than current
            current_abs_z = abs(watch.current_z_score)
            watch.max_z = current_abs_z + pullback + 0.05

            test_logger.info(
                f"Simulated peak: max_z={watch.max_z:.4f}, "
                f"current_z={current_abs_z:.4f}, "
                f"pullback_needed={pullback}, "
                f"will_trigger={current_abs_z <= watch.max_z - pullback}"
            )

            # Force price update processing
            service._last_check[test_coin_symbol] = 0
            await service._process_price_update(test_coin_symbol)

            await asyncio.sleep(1)

            assert len(entry_signals_received) == 1, f"Should receive 1 entry signal, got {len(entry_signals_received)}"

            entry_event = entry_signals_received[0]
            assert entry_event.coin_symbol == test_coin_symbol
            assert entry_event.spread_side == SpreadSide.SHORT

            test_logger.info(
                f"Entry executed: {entry_event.coin_symbol}, "
                f"Z={entry_event.z_score:.4f}, "
                f"side={entry_event.spread_side.value}"
            )

            assert test_coin_symbol not in service.get_active_watches()

            await service.stop()

        test_logger.info("✅ Entry execution on pullback test passed!")


    @pytest.mark.asyncio
    async def test_watch_cancellation_on_false_alarm(
        self,
        binance_client: BinanceClient,
        event_emitter: EventEmitter,
        test_logger,
        primary_symbol: str,
        test_coin_symbol: str,
    ):
        """Test that watch is cancelled when Z-score returns to normal."""
        cancelled_events: List[WatchCancelledEvent] = []

        async def on_watch_cancelled(event: WatchCancelledEvent):
            test_logger.info(f"❌ Watch cancelled: {event.coin_symbol}, reason={event.reason.value}")
            cancelled_events.append(event)

        event_emitter.on(EventType.WATCH_CANCELLED, on_watch_cancelled)

        z_entry_threshold = 10.0

        service = EntryObserverService(
            event_emitter=event_emitter,
            exchange_client=binance_client,
            logger=test_logger,
            primary_symbol=primary_symbol,
            z_entry_threshold=z_entry_threshold,
            z_sl_threshold=100.0,
            pullback=0.1,
            watch_timeout_seconds=120,
            debounce_seconds=0.1,
            max_watches=5,
        )

        async with binance_client:
            await service.start()

            coin_price = float(await binance_client.get_current_price(test_coin_symbol))
            primary_price = float(await binance_client.get_current_price(primary_symbol))

            beta = 0.5
            current_spread = math.log(coin_price) - beta * math.log(primary_price)
            spread_std = 0.01
            spread_mean = current_spread - (1.0 * spread_std)

            pending_event = PendingEntrySignalEvent(
                coin_symbol=test_coin_symbol,
                primary_symbol=primary_symbol,
                spread_side=SpreadSide.SHORT,
                z_score=11.0,
                beta=beta,
                correlation=0.85,
                hurst=0.35,
                spread_mean=spread_mean,
                spread_std=spread_std,
                coin_price=coin_price,
                primary_price=primary_price,
                z_entry_threshold=z_entry_threshold,
                z_tp_threshold=0.0,
                z_sl_threshold=100.0,
            )

            await event_emitter.emit(pending_event)
            await asyncio.sleep(2)

            service._last_check[test_coin_symbol] = 0
            await service._process_price_update(test_coin_symbol)

            await asyncio.sleep(1)

            assert len(cancelled_events) == 1, "Should receive cancellation event"
            assert cancelled_events[0].reason.value == "false_alarm"
            assert test_coin_symbol not in service.get_active_watches()

            await service.stop()

        test_logger.info("✅ False alarm cancellation test passed!")


    @pytest.mark.asyncio
    async def test_continuous_z_score_monitoring(
        self,
        binance_client: BinanceClient,
        event_emitter: EventEmitter,
        test_logger,
        primary_symbol: str,
        test_coin_symbol: str,
    ):
        """Test that Z-score is continuously monitored via WebSocket."""
        service = EntryObserverService(
            event_emitter=event_emitter,
            exchange_client=binance_client,
            logger=test_logger,
            primary_symbol=primary_symbol,
            z_entry_threshold=0.01,
            z_sl_threshold=1000.0,
            pullback=100.0,
            watch_timeout_seconds=120,
            debounce_seconds=0.5,
            max_watches=5,
        )

        async with binance_client:
            await service.start()

            coin_price = float(await binance_client.get_current_price(test_coin_symbol))
            primary_price = float(await binance_client.get_current_price(primary_symbol))

            beta = 0.5
            current_spread = math.log(coin_price) - beta * math.log(primary_price)
            spread_std = 0.001
            # Set spread_mean so Z stays above entry threshold
            spread_mean = current_spread - (0.5 * spread_std)  # Z = 0.5

            pending_event = PendingEntrySignalEvent(
                coin_symbol=test_coin_symbol,
                primary_symbol=primary_symbol,
                spread_side=SpreadSide.SHORT,
                z_score=0.5,
                beta=beta,
                correlation=0.85,
                hurst=0.35,
                spread_mean=spread_mean,
                spread_std=spread_std,
                coin_price=coin_price,
                primary_price=primary_price,
                z_entry_threshold=0.01,
                z_tp_threshold=0.0,
                z_sl_threshold=1000.0,
            )

            await event_emitter.emit(pending_event)

            z_samples = []
            price_samples = []

            for i in range(10):
                await asyncio.sleep(1)
                watch = service.get_active_watches().get(test_coin_symbol)
                if watch:
                    z_samples.append(watch.current_z_score)
                    price_samples.append((watch.coin_price, watch.primary_price))
                    test_logger.info(
                        f"Sample {i+1}: Z={watch.current_z_score:.4f}, "
                        f"coin={watch.coin_price:.4f}, "
                        f"primary={watch.primary_price:.4f}, "
                        f"max_z={watch.max_z:.4f}"
                    )

            assert len(z_samples) >= 5, "Should collect at least 5 Z-score samples"

            unique_coin_prices = len(set(p[0] for p in price_samples))
            test_logger.info(f"Unique coin prices observed: {unique_coin_prices}")

            await service.stop()

        test_logger.info("✅ Continuous Z-score monitoring test passed!")


    @pytest.mark.asyncio
    async def test_multiple_watches_simultaneously(
        self,
        binance_client: BinanceClient,
        event_emitter: EventEmitter,
        test_logger,
        primary_symbol: str,
    ):
        """Test monitoring multiple coins simultaneously."""
        test_coins = ["ARB/USDT:USDT", "OP/USDT:USDT", "DOGE/USDT:USDT"]

        service = EntryObserverService(
            event_emitter=event_emitter,
            exchange_client=binance_client,
            logger=test_logger,
            primary_symbol=primary_symbol,
            z_entry_threshold=0.01,
            z_sl_threshold=1000.0,
            pullback=100.0,
            watch_timeout_seconds=120,
            debounce_seconds=0.5,
            max_watches=10,
        )

        async with binance_client:
            await service.start()

            prices = {}
            for coin in test_coins:
                prices[coin] = float(await binance_client.get_current_price(coin))
            primary_price = float(await binance_client.get_current_price(primary_symbol))

            for coin in test_coins:
                beta = 0.5
                spread = math.log(prices[coin]) - beta * math.log(primary_price)
                # Set spread_mean so Z stays above entry threshold
                spread_mean = spread - (0.5 * 0.001)  # Z = 0.5

                pending_event = PendingEntrySignalEvent(
                    coin_symbol=coin,
                    primary_symbol=primary_symbol,
                    spread_side=SpreadSide.SHORT,
                    z_score=0.5,
                    beta=beta,
                    correlation=0.85,
                    hurst=0.35,
                    spread_mean=spread_mean,
                    spread_std=0.001,
                    coin_price=prices[coin],
                    primary_price=primary_price,
                    z_entry_threshold=0.01,
                    z_tp_threshold=0.0,
                    z_sl_threshold=1000.0,
                )
                await event_emitter.emit(pending_event)
                await asyncio.sleep(0.5)

            await asyncio.sleep(3)

            assert service.active_watch_count == len(test_coins)

            watches = service.get_active_watches()
            for coin in test_coins:
                assert coin in watches, f"{coin} should be watched"
                watch = watches[coin]
                assert watch.coin_price > 0, f"{coin} should have price"
                test_logger.info(f"{coin}: price={watch.coin_price:.4f}, Z={watch.current_z_score:.4f}")

            await service.stop()
            assert service.active_watch_count == 0

        test_logger.info("✅ Multiple watches test passed!")


class TestEntryObserverEdgeCases:
    """Edge case tests for EntryObserverService."""

    @pytest.mark.asyncio
    async def test_duplicate_pending_signal_ignored(
        self,
        binance_client: BinanceClient,
        event_emitter: EventEmitter,
        test_logger,
        primary_symbol: str,
        test_coin_symbol: str,
    ):
        """Test that duplicate pending signals for same coin are ignored."""
        service = EntryObserverService(
            event_emitter=event_emitter,
            exchange_client=binance_client,
            logger=test_logger,
            primary_symbol=primary_symbol,
            z_entry_threshold=0.01,
            z_sl_threshold=1000.0,
            pullback=100.0,
            watch_timeout_seconds=120,
            debounce_seconds=0.5,
            max_watches=5,
        )

        async with binance_client:
            await service.start()

            coin_price = float(await binance_client.get_current_price(test_coin_symbol))
            primary_price = float(await binance_client.get_current_price(primary_symbol))

            pending_event = PendingEntrySignalEvent(
                coin_symbol=test_coin_symbol,
                primary_symbol=primary_symbol,
                spread_side=SpreadSide.SHORT,
                z_score=0.5,
                beta=0.5,
                correlation=0.85,
                hurst=0.35,
                spread_mean=0.0,
                spread_std=0.01,
                coin_price=coin_price,
                primary_price=primary_price,
                z_entry_threshold=0.01,
                z_tp_threshold=0.0,
                z_sl_threshold=1000.0,
            )

            await event_emitter.emit(pending_event)
            await asyncio.sleep(1)
            await event_emitter.emit(pending_event)
            await asyncio.sleep(1)

            assert service.active_watch_count == 1, "Duplicate signal should be ignored"

            await service.stop()

        test_logger.info("✅ Duplicate signal test passed!")

    @pytest.mark.asyncio
    async def test_max_watches_limit(
        self,
        binance_client: BinanceClient,
        event_emitter: EventEmitter,
        test_logger,
        primary_symbol: str,
    ):
        """Test that max watches limit is enforced."""
        max_watches = 2
        test_coins = ["ARB/USDT:USDT", "OP/USDT:USDT", "DOGE/USDT:USDT"]

        cancelled_events: List[WatchCancelledEvent] = []

        async def on_cancelled(event: WatchCancelledEvent):
            cancelled_events.append(event)

        event_emitter.on(EventType.WATCH_CANCELLED, on_cancelled)

        service = EntryObserverService(
            event_emitter=event_emitter,
            exchange_client=binance_client,
            logger=test_logger,
            primary_symbol=primary_symbol,
            z_entry_threshold=0.01,
            z_sl_threshold=1000.0,
            pullback=100.0,
            watch_timeout_seconds=120,
            debounce_seconds=0.5,
            max_watches=max_watches,
        )

        async with binance_client:
            await service.start()

            primary_price = float(await binance_client.get_current_price(primary_symbol))

            for coin in test_coins:
                coin_price = float(await binance_client.get_current_price(coin))
                pending_event = PendingEntrySignalEvent(
                    coin_symbol=coin,
                    primary_symbol=primary_symbol,
                    spread_side=SpreadSide.SHORT,
                    z_score=0.5,
                    beta=0.5,
                    correlation=0.85,
                    hurst=0.35,
                    spread_mean=0.0,
                    spread_std=0.01,
                    coin_price=coin_price,
                    primary_price=primary_price,
                    z_entry_threshold=0.01,
                    z_tp_threshold=0.0,
                    z_sl_threshold=1000.0,
                )
                await event_emitter.emit(pending_event)
                await asyncio.sleep(0.5)

            await asyncio.sleep(1)

            assert service.active_watch_count == max_watches

            assert len(cancelled_events) == 1, "One watch should be cancelled"
            assert cancelled_events[0].reason.value == "max_watches_reached"

            await service.stop()

        test_logger.info("✅ Max watches limit test passed!")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
