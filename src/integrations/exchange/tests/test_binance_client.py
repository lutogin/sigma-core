"""
Integration tests for BinanceClient.

Tests real network connections to Binance Futures API.
Requires network access.

Run with:
    pytest src/integrations/exchange/tests/ -v
    pytest src/integrations/exchange/tests/test_binance_client.py -v -k "test_websocket"
"""

import pytest
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List

from src.integrations.exchange import BinanceClient
from src.integrations.exchange.binance import FundingRateData, SymbolInfo, TradeSide


# ============================================================================
# Connection Tests
# ============================================================================


class TestBinanceConnection:
    """Test connection and basic connectivity."""

    @pytest.mark.asyncio
    async def test_connect_and_ping(self, binance_client: BinanceClient):
        """Test that we can connect and ping the exchange."""
        await binance_client.connect()

        assert binance_client.is_connected is True

        result = await binance_client.ping()
        assert result is True

        await binance_client.disconnect()
        assert binance_client.is_connected is False

    @pytest.mark.asyncio
    async def test_context_manager(self, binance_client: BinanceClient):
        """Test async context manager."""
        async with binance_client:
            assert binance_client.is_connected is True

            result = await binance_client.ping()
            assert result is True

        assert binance_client.is_connected is False

    @pytest.mark.asyncio
    async def test_get_server_time(self, binance_client: BinanceClient):
        """Test server time retrieval."""
        async with binance_client:
            server_time = await binance_client.get_server_time()

            assert server_time is not None
            assert isinstance(server_time, datetime)
            assert server_time.tzinfo == timezone.utc

            now = datetime.now(timezone.utc)
            diff = abs((now - server_time).total_seconds())
            assert diff < 5, f"Server time diff too large: {diff}s"


# ============================================================================
# Markets Tests
# ============================================================================


class TestLoadMarkets:
    """Test market loading functionality."""

    @pytest.mark.asyncio
    async def test_load_markets(self, binance_client: BinanceClient):
        """Test that markets can be loaded."""
        async with binance_client:
            markets = await binance_client.load_markets()

            assert markets is not None
            assert len(markets) > 0
            assert "BTC/USDT:USDT" in markets
            assert "ETH/USDT:USDT" in markets

            print(f"\n✅ Loaded {len(markets)} markets")

    # @pytest.mark.asyncio
    # async def test_set_position_mode(self, binance_client: BinanceClient):
    #     """Test that markets can be loaded."""
    #     async with binance_client:
    #         resp = await binance_client.set_position_mode(hedge_mode=True)

    #         assert resp is not None

    #         print(f"\nMode: {resp}")

    @pytest.mark.asyncio
    async def test_symbol_info_structure(
        self, binance_client: BinanceClient, test_symbol: str
    ):
        """Test that symbol info has correct structure."""
        async with binance_client:
            info = await binance_client.get_symbol_info(test_symbol)

            assert info is not None
            assert isinstance(info, SymbolInfo)
            assert info.symbol == test_symbol
            assert info.base_asset == "BTC"
            assert info.quote_asset == "USDT"
            assert info.price_precision > 0
            assert info.quantity_precision > 0
            assert info.tick_size > 0
            assert info.step_size > 0
            assert info.min_qty > 0
            assert info.min_notional > 0

            print(f"\n✅ Symbol info for {test_symbol}:")
            print(f"   Price precision: {info.price_precision}")
            print(f"   Tick size: {info.tick_size}")
            print(f"   Min notional: {info.min_notional}")

    @pytest.mark.asyncio
    async def test_get_tradable_symbols(self, binance_client: BinanceClient):
        """Test getting tradable symbols."""
        async with binance_client:
            symbols = await binance_client.get_tradable_symbols(exclude_leveraged=True)

            assert len(symbols) > 0

            for symbol in symbols[:10]:
                assert "UP" not in symbol
                assert "DOWN" not in symbol

            print(f"\n✅ Found {len(symbols)} tradable symbols")


# ============================================================================
# Funding Rate Tests
# ============================================================================


class TestFundingRate:
    """Test funding rate functionality."""

    @pytest.mark.asyncio
    async def test_get_current_funding_rate(
        self, binance_client: BinanceClient, test_symbol: str
    ):
        """Test getting current funding rate for a symbol."""
        async with binance_client:
            funding = await binance_client.get_current_funding_rate(test_symbol)

            assert funding is not None
            assert isinstance(funding, FundingRateData)
            assert funding.symbol == test_symbol
            assert isinstance(funding.funding_rate, float)
            assert funding.funding_interval_hours in [4, 8]

            print(f"\n✅ Funding rate for {test_symbol}:")
            print(f"   Rate: {funding.funding_rate:.6f} ({funding.rate_pct:.4f}%)")
            print(f"   Next funding: {funding.funding_time}")

    @pytest.mark.asyncio
    async def test_get_funding_rates_batch(
        self, binance_client: BinanceClient, test_symbols: list
    ):
        """Test getting funding rates for multiple symbols."""
        async with binance_client:
            rates = await binance_client.get_funding_rates_batch(test_symbols)

            assert len(rates) > 0

            symbols_found = {r.symbol for r in rates}
            for symbol in test_symbols:
                assert symbol in symbols_found

            print(f"\n✅ Got funding rates for {len(rates)} symbols")

    @pytest.mark.asyncio
    async def test_get_all_funding_rates(self, binance_client: BinanceClient):
        """Test getting all funding rates."""
        async with binance_client:
            rates = await binance_client.get_funding_rates_batch()

            assert len(rates) > 50

            rates_sorted = sorted(rates, key=lambda x: x.funding_rate)
            print(f"\n✅ Got {len(rates)} funding rates")
            print(
                f"   Lowest: {rates_sorted[0].symbol} = {rates_sorted[0].rate_pct:.4f}%"
            )
            print(
                f"   Highest: {rates_sorted[-1].symbol} = {rates_sorted[-1].rate_pct:.4f}%"
            )


# ============================================================================
# WebSocket Tests
# ============================================================================


class TestWebSocket:
    """Test WebSocket streaming functionality."""

    @pytest.mark.asyncio
    async def test_subscribe_book_ticker(
        self, binance_client: BinanceClient, test_symbol: str
    ):
        """Test subscribing to book ticker (bid/ask) updates."""
        received: List[Dict[str, Any]] = []

        async def on_update(data: Dict[str, Any]):
            received.append(data)

        async with binance_client:
            task = await binance_client.subscribe_book_ticker(test_symbol, on_update)

            try:
                await asyncio.wait_for(
                    self._wait_for_updates(received, 5), timeout=10.0
                )
            except asyncio.TimeoutError:
                pass

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert len(received) > 0

        update = received[0]
        assert "bid_price" in update
        assert "ask_price" in update
        assert update["ask_price"] > update["bid_price"]

        print(f"\n✅ Received {len(received)} book ticker updates")
        print(f"   Sample: bid={update['bid_price']}, ask={update['ask_price']}")

    @pytest.mark.asyncio
    async def test_subscribe_mark_price(
        self, binance_client: BinanceClient, test_symbol: str
    ):
        """Test subscribing to mark price updates."""
        received: List[Dict[str, Any]] = []

        async def on_update(data: Dict[str, Any]):
            received.append(data)

        async with binance_client:
            task = await binance_client.subscribe_mark_price(
                test_symbol, on_update, "1s"
            )

            try:
                await asyncio.wait_for(
                    self._wait_for_updates(received, 2), timeout=10.0
                )
            except asyncio.TimeoutError:
                pass

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert len(received) > 0

        update = received[0]
        assert "mark_price" in update
        assert "funding_rate" in update
        assert update["mark_price"] > 0

        print(f"\n✅ Received {len(received)} mark price updates")
        print(f"   Mark price: {update['mark_price']}")
        print(f"   Funding rate: {update['funding_rate']:.6f}")

    @pytest.mark.asyncio
    async def test_subscribe_agg_trade(
        self, binance_client: BinanceClient, test_symbol: str
    ):
        """Test subscribing to aggregated trades."""
        received: List[Dict[str, Any]] = []

        async def on_update(data: Dict[str, Any]):
            received.append(data)

        async with binance_client:
            task = await binance_client.subscribe_agg_trade(test_symbol, on_update)

            try:
                await asyncio.wait_for(
                    self._wait_for_updates(received, 10), timeout=10.0
                )
            except asyncio.TimeoutError:
                pass

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert len(received) > 0

        update = received[0]
        assert "price" in update
        assert "quantity" in update
        assert update["price"] > 0

        print(f"\n✅ Received {len(received)} trade updates")
        print(f"   Sample: price={update['price']}, qty={update['quantity']}")

    @pytest.mark.asyncio
    async def test_subscribe_multi_book_ticker(
        self, binance_client: BinanceClient, test_symbols: list
    ):
        """Test subscribing to multiple book tickers."""
        received: List[Dict[str, Any]] = []
        symbols_seen: set = set()

        async def on_update(data: Dict[str, Any]):
            received.append(data)
            symbols_seen.add(data["symbol"])

        async with binance_client:
            task = await binance_client.subscribe_multi_book_ticker(
                test_symbols, on_update
            )

            try:
                await asyncio.wait_for(
                    self._wait_for_all_symbols(symbols_seen, test_symbols), timeout=15.0
                )
            except asyncio.TimeoutError:
                pass

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert len(received) > 0

        print(f"\n✅ Multi-stream: {len(received)} updates")
        print(f"   Symbols seen: {symbols_seen}")

    async def _wait_for_updates(self, updates: list, min_count: int):
        while len(updates) < min_count:
            await asyncio.sleep(0.1)

    async def _wait_for_all_symbols(self, seen: set, expected: list):
        while not all(s in seen for s in expected):
            await asyncio.sleep(0.1)


# ============================================================================
# Market Data Tests
# ============================================================================


class TestMarketData:
    """Test market data retrieval."""

    @pytest.mark.asyncio
    async def test_get_market_data(
        self, binance_client: BinanceClient, test_symbol: str
    ):
        """Test getting current market data."""
        async with binance_client:
            market = await binance_client.get_market_data(test_symbol)

            assert market.symbol == test_symbol
            assert market.bid > 0
            assert market.ask > 0
            assert market.ask > market.bid

            spread_pct = (market.ask - market.bid) / market.mid_price * 100

            print(f"\n✅ Market data for {test_symbol}:")
            print(f"   Bid: {market.bid}, Ask: {market.ask}")
            print(f"   Spread: {spread_pct:.4f}%")

    @pytest.mark.asyncio
    async def test_get_current_price(
        self, binance_client: BinanceClient, test_symbol: str
    ):
        """Test getting current price."""
        async with binance_client:
            price = await binance_client.get_current_price(test_symbol)

            assert price > 0
            print(f"\n✅ Current price for {test_symbol}: {price}")


# ============================================================================
# Trading Operations Tests
# ============================================================================


class TestTradingOperations:
    """Test trading operations (open position, TP/SL, close)."""

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Opens and closes real positions - use with caution")
    async def test_open_position_set_tp_sl_and_close(
        self, binance_client: BinanceClient
    ):
        """
        Test opening a position, setting TP/SL, and closing it.

        WARNING: This test opens and closes real positions on Binance.
        Use with caution and only on testnet or with small amounts.
        """
        SYMBOL = "XRP/USDT:USDT"
        AMOUNT = 3.0
        SIDE: TradeSide = "BUY"  # "buy" for long, "sell" for short
        SHOULD_OPEN_POSITION = False
        SHOULD_CLOSE_POSITION = False

        async with binance_client:
            if SHOULD_OPEN_POSITION:
                # Open position
                open_order = await binance_client.open_position(
                    symbol=SYMBOL, side=SIDE, amount=AMOUNT
                )

                assert open_order is not None
                assert open_order.side.upper() == SIDE.upper()
                print(f"\n✅ Position opened: {open_order.id}")

            # Wait for position to be fully processed
            await asyncio.sleep(1.5)

            # Get position
            position = await binance_client.get_position(SYMBOL)

            assert position is not None
            assert position.symbol == SYMBOL
            assert position.contracts > 0
            assert position.entry_price > 0
            # assert position.liquidation_price > 0

            # Log position details
            print("\n📊 Position details:")
            print(f"   Symbol: {position.symbol}")
            print(f"   Side: {position.side}")
            print(f"   Contracts: {position.contracts}")
            print(f"   Size: {position.size}")
            print(f"   Entry Price: {position.entry_price}")
            print(f"   Mark Price: {position.mark_price}")
            print(f"   Liquidation Price: {position.liquidation_price}")
            print(f"   Unrealized PnL: {position.unrealized_pnl}")
            print(f"   Leverage: {position.leverage}x")
            print(f"   Margin Type: {position.margin_type}")

            # Calculate TP and SL prices
            # For LONG (buy): TP above entry, SL above liquidation
            # For SHORT (sell): TP below entry, SL below liquidation
            if SIDE.lower() == "buy":
                tp_price = position.entry_price * 1.15  # 15% TP
                sl_price = position.entry_price * 0.85  # 15% SL
            else:
                tp_price = position.entry_price * 0.85  # 15% TP
                sl_price = position.entry_price * 1.15  # 15% below liquidation

            print("\n🎯 Setting TP/SL:")
            print(f"   Take Profit: {tp_price:.6f}")
            print(f"   Stop Loss: {sl_price:.6f}")

            # Set Take Profit
            tp_order = await binance_client.set_take_profit(
                symbol=SYMBOL, side=SIDE, take_profit_price=tp_price
            )
            assert tp_order is not None
            print(f"   ✅ TP order created: {tp_order.id}")

            # Set Stop Loss
            sl_order = await binance_client.set_stop_loss(
                symbol=SYMBOL, side=SIDE, stop_price=sl_price
            )
            assert sl_order is not None
            print(f"   ✅ SL order created: {sl_order.id}")

            # Close position if requested
            if SHOULD_CLOSE_POSITION:
                close_order = await binance_client.flash_close_position(SYMBOL)
                assert close_order is not None
                print(f"\n✅ Position closed successfully: {close_order.id}")

                # Verify position is closed
                await asyncio.sleep(1.0)
                closed_position = await binance_client.get_position(SYMBOL)
                if closed_position:
                    assert closed_position.contracts == 0, "Position should be closed"
                else:
                    print("   ✅ Position fully closed (no position found)")


class TestTradingOperationsByLimitOrders:
    """Test trading operations (open position, TP/SL, close)."""

    @pytest.mark.asyncio
    # @pytest.mark.skip(reason="Opens and closes real positions - use with caution")
    async def test_open_position_limit_set_tp_sl_and_close(
        self, binance_client: BinanceClient
    ):
        """
        Test opening a position, setting TP/SL, and closing it.

        WARNING: This test opens and closes real positions on Binance.
        Use with caution and only on testnet or with small amounts.
        """
        SYMBOL = "XRP/USDT:USDT"
        AMOUNT = 3.0
        SIDE: TradeSide = "BUY"  # "buy" for long, "sell" for short
        SHOULD_OPEN_POSITION = True
        SHOULD_CLOSE_POSITION = True

        async with binance_client:
            if SHOULD_OPEN_POSITION:
                # Open position
                open_order = await binance_client.open_position_limit(
                    symbol=SYMBOL, side=SIDE, amount=AMOUNT
                )

                assert open_order is not None
                assert open_order.side.upper() == SIDE.upper()
                print(f"\n✅ Position opened: {open_order.id}")

            # Wait for position to be fully processed
            await asyncio.sleep(3)

            # Get position
            position = await binance_client.get_position(SYMBOL)

            assert position is not None
            assert position.symbol == SYMBOL
            assert position.contracts > 0
            assert position.entry_price > 0
            # assert position.liquidation_price > 0

            # Log position details
            print("\n📊 Position details:")
            print(f"   Symbol: {position.symbol}")
            print(f"   Side: {position.side}")
            print(f"   Contracts: {position.contracts}")
            print(f"   Size: {position.size}")
            print(f"   Entry Price: {position.entry_price}")
            print(f"   Mark Price: {position.mark_price}")
            print(f"   Liquidation Price: {position.liquidation_price}")
            print(f"   Unrealized PnL: {position.unrealized_pnl}")
            print(f"   Leverage: {position.leverage}x")
            print(f"   Margin Type: {position.margin_type}")

            # Calculate TP and SL prices
            # For LONG (buy): TP above entry, SL above liquidation
            # For SHORT (sell): TP below entry, SL below liquidation
            if SIDE.lower() == "buy":
                tp_price = position.entry_price * 1.15  # 15% TP
                sl_price = position.entry_price * 0.85  # 15% SL
            else:
                tp_price = position.entry_price * 0.85  # 15% TP
                sl_price = position.entry_price * 1.15  # 15% below liquidation

            print("\n🎯 Setting TP/SL:")
            print(f"   Take Profit: {tp_price:.6f}")
            print(f"   Stop Loss: {sl_price:.6f}")

            # Set Take Profit
            tp_order = await binance_client.set_take_profit(
                symbol=SYMBOL, side=SIDE, take_profit_price=tp_price
            )
            assert tp_order is not None
            print(f"   ✅ TP order created: {tp_order.id}")

            # Set Stop Loss
            sl_order = await binance_client.set_stop_loss(
                symbol=SYMBOL, side=SIDE, stop_price=sl_price
            )
            assert sl_order is not None
            print(f"   ✅ SL order created: {sl_order.id}")

            # Close position if requested
            if SHOULD_CLOSE_POSITION:
                close_order = await binance_client.flash_close_position(SYMBOL)
                assert close_order is not None
                print(f"\n✅ Position closed successfully: {close_order.id}")

                # Verify position is closed
                await asyncio.sleep(1.0)
                closed_position = await binance_client.get_position(SYMBOL)
                if closed_position:
                    assert closed_position.contracts == 0, "Position should be closed"
                else:
                    print("   ✅ Position fully closed (no position found)")


# ============================================================================
# Hedge Mode Tests
# ============================================================================


class TestHedgeMode:
    """
    Test Hedge Mode functionality with multiple spreads sharing PRIMARY (ETH).

    This tests the core stat-arb scenario where:
    - Multiple COIN positions are opened against ETH
    - ETH has both LONG and SHORT positions simultaneously
    - Closing one spread only closes the corresponding ETH position side
    """

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Opens real positions - run manually with caution")
    async def test_hedge_mode_two_spreads(self, binance_client: BinanceClient):
        """
        Test opening two opposite spreads and closing them independently.

        Spread 1 (LONG): Buy LINK, Sell ETH -> ETH SHORT
        Spread 2 (SHORT): Sell UNI, Buy ETH -> ETH LONG

        Then close Spread 1 and verify ETH SHORT is closed but ETH LONG remains.
        Finally close Spread 2.
        """
        # Configuration
        LINK_SYMBOL = "LINK/USDT:USDT"
        UNI_SYMBOL = "UNI/USDT:USDT"
        ETH_SYMBOL = "ETH/USDT:USDT"

        # Minimum amounts (check Binance for current minimums)
        LINK_AMOUNT = 2.0  # ~$7-8
        UNI_AMOUNT = 4.0  # ~$7-8
        ETH_AMOUNT_1 = 0.01  # For spread 1 (~$35)
        ETH_AMOUNT_2 = 0.01  # For spread 2 (~$35)

        async with binance_client:
            # Verify Hedge Mode is enabled
            is_hedge = await binance_client.get_position_mode()
            print(f"\n🔧 Position Mode: {'Hedge' if is_hedge else 'One-way'}")
            assert is_hedge, "Hedge Mode must be enabled for this test"

            # ================================================================
            # Step 1: Open Spread 1 (LONG spread: Buy LINK, Sell ETH)
            # ================================================================
            print("\n" + "=" * 60)
            print("📈 Opening Spread 1: LONG (Buy LINK, Sell ETH)")
            print("=" * 60)

            # Buy LINK (LONG position)
            link_order = await binance_client.open_position_limit(
                symbol=LINK_SYMBOL,
                side="buy",
                amount=LINK_AMOUNT,
            )
            print(f"   ✅ LINK LONG opened: {link_order.id}")

            # Sell ETH (SHORT position)
            eth_short_order = await binance_client.open_position_limit(
                symbol=ETH_SYMBOL,
                side="sell",
                amount=ETH_AMOUNT_1,
            )
            print(f"   ✅ ETH SHORT opened: {eth_short_order.id}")

            await asyncio.sleep(1.0)

            # ================================================================
            # Step 2: Open Spread 2 (SHORT spread: Sell UNI, Buy ETH)
            # ================================================================
            print("\n" + "=" * 60)
            print("📉 Opening Spread 2: SHORT (Sell UNI, Buy ETH)")
            print("=" * 60)

            # Sell UNI (SHORT position)
            uni_order = await binance_client.open_position_limit(
                symbol=UNI_SYMBOL,
                side="sell",
                amount=UNI_AMOUNT,
            )
            print(f"   ✅ UNI SHORT opened: {uni_order.id}")

            # Buy ETH (LONG position)
            eth_long_order = await binance_client.open_position_limit(
                symbol=ETH_SYMBOL,
                side="buy",
                amount=ETH_AMOUNT_2,
            )
            print(f"   ✅ ETH LONG opened: {eth_long_order.id}")

            await asyncio.sleep(1.0)

            # ================================================================
            # Step 3: Verify all 4 positions are open
            # ================================================================
            print("\n" + "=" * 60)
            print("🔍 Verifying all positions")
            print("=" * 60)

            # Get all positions
            link_pos = await binance_client.get_position(LINK_SYMBOL, "long")
            uni_pos = await binance_client.get_position(UNI_SYMBOL, "short")
            eth_positions = await binance_client.get_positions(
                ETH_SYMBOL, skip_zero=True
            )

            # Verify LINK LONG
            assert link_pos is not None, "LINK LONG position should exist"
            assert link_pos.contracts > 0, "LINK should have contracts"
            assert link_pos.side == "long", "LINK should be LONG"
            print(f"   ✅ LINK LONG: {link_pos.contracts} contracts")

            # Verify UNI SHORT
            assert uni_pos is not None, "UNI SHORT position should exist"
            assert uni_pos.contracts > 0, "UNI should have contracts"
            assert uni_pos.side == "short", "UNI should be SHORT"
            print(f"   ✅ UNI SHORT: {uni_pos.contracts} contracts")

            # Verify ETH has both LONG and SHORT
            eth_sides = {p.side for p in eth_positions}
            assert "long" in eth_sides, "ETH should have LONG position"
            assert "short" in eth_sides, "ETH should have SHORT position"
            print(f"   ✅ ETH positions: {len(eth_positions)}")
            for pos in eth_positions:
                print(f"      - {pos.side.upper()}: {pos.contracts} contracts")

            # ================================================================
            # Step 4: Close Spread 1 (Close LINK entirely, close ETH SHORT)
            # ================================================================
            print("\n" + "=" * 60)
            print("🔒 Closing Spread 1: LINK + ETH SHORT")
            print("=" * 60)

            # Close LINK entirely
            link_close = await binance_client.flash_close_position(LINK_SYMBOL)
            print(f"   ✅ LINK closed: {link_close.id}")

            # Close ETH SHORT (partial close with specific side)
            # close_side="buy" means we're buying to close a SHORT position
            eth_short_close = await binance_client.flash_close_position(
                ETH_SYMBOL,
                amount=ETH_AMOUNT_1,
                close_side="buy",  # Buy to close SHORT
            )
            print(f"   ✅ ETH SHORT closed: {eth_short_close.id}")

            await asyncio.sleep(1.0)

            # ================================================================
            # Step 5: Verify Spread 1 is closed, Spread 2 remains
            # ================================================================
            print("\n" + "=" * 60)
            print("🔍 Verifying Spread 1 closed, Spread 2 remains")
            print("=" * 60)

            # LINK should be closed
            link_pos_after = await binance_client.get_position(LINK_SYMBOL, "long")
            assert (
                link_pos_after is None or link_pos_after.contracts == 0
            ), "LINK should be closed"
            print("   ✅ LINK position closed")

            # UNI should still be open
            uni_pos_after = await binance_client.get_position(UNI_SYMBOL, "short")
            assert uni_pos_after is not None, "UNI should still exist"
            assert uni_pos_after.contracts > 0, "UNI should still have contracts"
            print(f"   ✅ UNI SHORT still open: {uni_pos_after.contracts} contracts")

            # ETH SHORT should be closed, ETH LONG should remain
            eth_positions_after = await binance_client.get_positions(
                ETH_SYMBOL, skip_zero=True
            )
            eth_sides_after = {p.side for p in eth_positions_after}

            # ETH SHORT should be gone (or zero)
            eth_short_after = await binance_client.get_position(ETH_SYMBOL, "short")
            assert (
                eth_short_after is None or eth_short_after.contracts == 0
            ), "ETH SHORT should be closed"
            print("   ✅ ETH SHORT closed")

            # ETH LONG should remain
            assert "long" in eth_sides_after, "ETH LONG should still exist"
            eth_long_after = await binance_client.get_position(ETH_SYMBOL, "long")
            assert eth_long_after.contracts > 0, "ETH LONG should have contracts"
            print(f"   ✅ ETH LONG still open: {eth_long_after.contracts} contracts")

            # ================================================================
            # Step 6: Close Spread 2 (Close UNI entirely, close ETH LONG)
            # ================================================================
            print("\n" + "=" * 60)
            print("🔒 Closing Spread 2: UNI + ETH LONG")
            print("=" * 60)

            # Close UNI entirely
            uni_close = await binance_client.flash_close_position(UNI_SYMBOL)
            print(f"   ✅ UNI closed: {uni_close.id}")

            # Close ETH LONG (partial close with specific side)
            # close_side="sell" means we're selling to close a LONG position
            eth_long_close = await binance_client.flash_close_position(
                ETH_SYMBOL,
                amount=ETH_AMOUNT_2,
                close_side="sell",  # Sell to close LONG
            )
            print(f"   ✅ ETH LONG closed: {eth_long_close.id}")

            await asyncio.sleep(1.0)

            # ================================================================
            # Step 7: Verify all positions are closed
            # ================================================================
            print("\n" + "=" * 60)
            print("🔍 Verifying all positions closed")
            print("=" * 60)

            # All positions should be closed
            final_link = await binance_client.get_position(LINK_SYMBOL)
            final_uni = await binance_client.get_position(UNI_SYMBOL)
            final_eth = await binance_client.get_positions(ETH_SYMBOL, skip_zero=True)

            assert (
                final_link is None or final_link.contracts == 0
            ), "LINK should be closed"
            assert final_uni is None or final_uni.contracts == 0, "UNI should be closed"
            assert len(final_eth) == 0, "ETH should have no positions"

            print("   ✅ LINK: closed")
            print("   ✅ UNI: closed")
            print("   ✅ ETH: all positions closed")

            print("\n" + "=" * 60)
            print("✅ HEDGE MODE TEST PASSED!")
            print("=" * 60)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
