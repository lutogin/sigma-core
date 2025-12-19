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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
