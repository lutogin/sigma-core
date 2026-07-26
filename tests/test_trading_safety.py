from types import MethodType, SimpleNamespace

import pytest

from src.domain.position_state import SpreadPosition, SpreadSide as StateSpreadSide
from src.domain.trading.trading import TradingService
from src.infra.event_emitter import (
    EntrySignalEvent,
    ExitReason,
    ExitSignalEvent,
    SpreadSide,
)
from src.integrations.exchange import Balance, Order, Position


class _Logger:
    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


class _Emitter:
    def __init__(self):
        self.events = []

    def on(self, *_args):
        pass

    def off(self, *_args):
        pass

    async def emit(self, event):
        self.events.append(event)


class _PositionState:
    def __init__(self, position=None):
        self.position = position
        self.closed = False
        self.marked = []

    def get_position(self, _symbol):
        return None if self.closed else self.position

    def get_active_positions(self):
        return [] if self.closed or self.position is None else [self.position]

    def mark_leg_closed(self, _symbol, leg):
        self.marked.append(leg)
        setattr(self.position, f"{leg}_leg_closed", True)
        return True

    def close_position(self, _symbol, _reason):
        self.closed = True

    def can_open_position(self, **_kwargs):
        return True, None


def _filled_order(
    symbol: str,
    *,
    side: str,
    amount: float,
    price: float = 100.0,
) -> Order:
    return Order(
        id=f"{symbol}-{side}",
        client_order_id="test",
        symbol=symbol,
        side=side,
        type="market",
        price=price,
        amount=amount,
        filled=amount,
        remaining=0.0,
        status="closed",
        timestamp=0,
    )


def _spread_position() -> SpreadPosition:
    return SpreadPosition(
        coin_symbol="LINK/USDT:USDT",
        primary_symbol="ETH/USDT:USDT",
        side=StateSpreadSide.LONG,
        coin_contracts=10.0,
        primary_contracts=2.0,
        coin_size_usdt=1_000.0,
        primary_size_usdt=600.0,
    )


def _service(exchange, state, *, allow_trading=False) -> TradingService:
    return TradingService(
        event_emitter=_Emitter(),
        exchange_client=exchange,
        position_state_service=state,
        logger=_Logger(),
        allow_trading=allow_trading,
        position_size_usdt=1_000.0,
        leverage=5,
        max_coin_notional_pct=0.10,
        max_margin_utilization=0.50,
    )


@pytest.mark.asyncio
async def test_execution_fails_closed_without_hedge_mode() -> None:
    async def connect():
        return None

    async def get_position_mode():
        return False

    service = _service(
        SimpleNamespace(
            connect=connect,
            get_position_mode=get_position_mode,
        ),
        _PositionState(),
    )

    with pytest.raises(RuntimeError, match="Hedge Mode"):
        await service._prepare_execution()

    assert service._execution_ready is False


@pytest.mark.asyncio
async def test_disabling_entries_does_not_disable_position_exits() -> None:
    position = _spread_position()
    state = _PositionState(position)
    service = _service(SimpleNamespace(), state, allow_trading=False)
    calls = []

    async def fake_close(self, coin_symbol, primary_symbol, exit_reason):
        calls.append((coin_symbol, primary_symbol, exit_reason))
        return True

    service._close_spread = MethodType(fake_close, service)
    await service._on_exit_signal(
        ExitSignalEvent(
            coin_symbol=position.coin_symbol,
            primary_symbol=position.primary_symbol,
            exit_reason=ExitReason.STOP_LOSS,
        )
    )

    assert calls == [
        (
            "LINK/USDT:USDT",
            "ETH/USDT:USDT",
            ExitReason.STOP_LOSS,
        )
    ]


@pytest.mark.asyncio
async def test_partial_close_is_persisted_and_retry_skips_closed_leg() -> None:
    position = _spread_position()
    state = _PositionState(position)
    calls = []
    primary_attempts = 0

    async def flash_close_position(symbol, **kwargs):
        nonlocal primary_attempts
        calls.append((symbol, kwargs))
        if symbol == "ETH/USDT:USDT":
            primary_attempts += 1
            if primary_attempts == 1:
                raise RuntimeError("temporary primary close failure")
            return _filled_order(symbol, side="buy", amount=2.0, price=300.0)
        return _filled_order(symbol, side="sell", amount=10.0, price=100.0)

    exchange = SimpleNamespace(flash_close_position=flash_close_position)
    service = _service(exchange, state)

    first = await service._close_spread(
        position.coin_symbol,
        position.primary_symbol,
        ExitReason.STOP_LOSS,
    )
    second = await service._close_spread(
        position.coin_symbol,
        position.primary_symbol,
        ExitReason.STOP_LOSS,
    )

    assert first is False
    assert second is True
    assert state.marked == ["coin", "primary"]
    assert state.closed is True
    assert [symbol for symbol, _kwargs in calls].count("LINK/USDT:USDT") == 1
    assert calls[0][1]["amount"] == 10.0
    assert calls[0][1]["close_side"] == "sell"


@pytest.mark.asyncio
async def test_primary_rollback_closes_only_the_filled_order_quantity() -> None:
    calls = []

    async def flash_close_position(symbol, **kwargs):
        calls.append((symbol, kwargs))
        return _filled_order(symbol, side="buy", amount=2.0)

    service = _service(
        SimpleNamespace(flash_close_position=flash_close_position),
        _PositionState(),
    )
    opened = _filled_order(
        "ETH/USDT:USDT",
        side="sell",
        amount=2.0,
    )

    assert await service._rollback_position("ETH/USDT:USDT", opened) is True
    assert calls[0][1]["amount"] == 2.0
    assert calls[0][1]["close_side"] == "buy"


@pytest.mark.asyncio
async def test_startup_reconciliation_aggregates_shared_primary_hedges() -> None:
    first = _spread_position()
    second = SpreadPosition(
        coin_symbol="AAVE/USDT:USDT",
        primary_symbol="ETH/USDT:USDT",
        side=StateSpreadSide.LONG,
        coin_contracts=3.0,
        primary_contracts=1.5,
    )
    state = _PositionState()
    state.get_active_positions = lambda: [first, second]
    exchange_positions = [
        Position("LINK/USDT:USDT", "long", 10, 10, 0, 0, 0, 5, "cross", 0),
        Position("AAVE/USDT:USDT", "long", 3, 3, 0, 0, 0, 5, "cross", 0),
        Position("ETH/USDT:USDT", "short", 3.5, 3.5, 0, 0, 0, 5, "cross", 0),
    ]

    async def get_positions(**_kwargs):
        return exchange_positions

    service = _service(SimpleNamespace(get_positions=get_positions), state)
    await service._assert_exchange_state_matches_storage()


@pytest.mark.asyncio
async def test_live_margin_cap_matches_backtest_risk_gate() -> None:
    state = _PositionState()
    opened = []

    async def get_balance(_asset):
        return Balance(asset="USDT", free=5_100, used=4_900, total=10_000)

    service = _service(SimpleNamespace(get_balance=get_balance), state)

    async def fake_open(self, event, coin_size, primary_size):
        opened.append((event, coin_size, primary_size))

    service._open_spread = MethodType(fake_open, service)
    await service._process_entry_signal(
        EntrySignalEvent(
            coin_symbol="LINK/USDT:USDT",
            primary_symbol="ETH/USDT:USDT",
            spread_side=SpreadSide.LONG,
            beta=1.0,
            halflife=12.0,
        )
    )

    assert opened == []
