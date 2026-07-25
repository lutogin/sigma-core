from decimal import Decimal
from types import MethodType

import pytest

from src.integrations.exchange import (
    BinanceClient,
    ExchangeConfig,
    MarketData,
    Order,
)


class _Logger:
    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


def _order(order_id: str, filled: float, price: float) -> Order:
    return Order(
        id=order_id,
        client_order_id=order_id,
        symbol="LINK/USDT:USDT",
        side="buy",
        type="limit",
        price=price,
        amount=filled,
        filled=filled,
        remaining=0,
        status="closed",
        timestamp=0,
    )


def test_order_mapping_uses_actual_average_fill_price() -> None:
    client = BinanceClient(ExchangeConfig(), _Logger())
    order = client._map_order_response(
        "LINK/USDT:USDT",
        {
            "orderId": 1,
            "side": "BUY",
            "type": "MARKET",
            "price": "0",
            "avgPrice": "12.34",
            "origQty": "2",
            "executedQty": "2",
            "status": "FILLED",
        },
    )

    assert order.price == 12.34


def test_multiple_child_fills_are_aggregated_by_quantity_and_price() -> None:
    combined = BinanceClient._combine_fill_orders(
        "LINK/USDT:USDT",
        [
            _order("first", 4.0, 99.0),
            _order("second", 6.0, 101.0),
        ],
        requested_amount=10.0,
    )

    assert combined.status == "closed"
    assert combined.filled == 10.0
    assert combined.remaining == 0.0
    assert combined.price == pytest.approx(100.2)


@pytest.mark.asyncio
async def test_ambiguous_create_error_recovers_by_client_order_id() -> None:
    client = BinanceClient(ExchangeConfig(), _Logger())

    class _Api:
        async def futures_create_order(self, **_params):
            raise ConnectionError("response lost")

        async def futures_get_order(self, **params):
            assert params["origClientOrderId"] == "stable-id"
            return {"orderId": 42, "status": "FILLED"}

    recovered = await client._create_futures_order_with_recovery(
        _Api(),
        binance_symbol="LINKUSDT",
        params={
            "symbol": "LINKUSDT",
            "newClientOrderId": "stable-id",
        },
        client_order_id="stable-id",
    )

    assert recovered["orderId"] == 42


@pytest.mark.asyncio
async def test_ioc_entry_returns_aggregate_of_partial_fills() -> None:
    client = BinanceClient(ExchangeConfig(), _Logger())
    create_calls = []

    class _Api:
        async def futures_create_order(self, **params):
            create_calls.append(params)
            fill = 4.0 if len(create_calls) == 1 else 6.0
            return {
                "orderId": len(create_calls),
                "clientOrderId": params["newClientOrderId"],
                "side": params["side"],
                "type": params["type"],
                "avgPrice": "99" if len(create_calls) == 1 else "101",
                "origQty": str(params["quantity"]),
                "executedQty": str(fill),
                "status": "EXPIRED" if len(create_calls) == 1 else "FILLED",
            }

    api = _Api()

    async def get_client(self):
        return api

    async def noop(*_args, **_kwargs):
        return None

    async def amount_precision(_symbol, amount):
        return Decimal(str(amount))

    async def price_precision(_symbol, price):
        return Decimal(str(price))

    async def market_data(_symbol):
        return MarketData(
            symbol="LINK/USDT:USDT",
            bid=99.0,
            ask=100.0,
            last=99.5,
            timestamp=0,
        )

    client._get_client = MethodType(get_client, client)
    client.set_leverage = noop
    client.set_margin_type = noop
    client.amount_to_precision = amount_precision
    client.price_to_precision = price_precision
    client.get_market_data = market_data
    client.cancel_order = noop

    order = await client.open_position_limit(
        "LINK/USDT:USDT",
        "buy",
        10.0,
        max_retries=1,
        fallback_to_market=False,
        client_order_id="entry-test",
    )

    assert order.status == "closed"
    assert order.filled == 10.0
    assert order.price == pytest.approx(100.2)
    assert [call["timeInForce"] for call in create_calls] == ["IOC", "IOC"]
