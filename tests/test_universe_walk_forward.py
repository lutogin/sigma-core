import asyncio
from datetime import datetime, timedelta, timezone
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from backtests.run_backtest import BacktestConfig, Trade
from backtests.run_universe_walk_forward import (
    CoinTrainResult,
    UniverseWalkForwardRunner,
    _InMemoryOHLCVCacheLoader,
)


def _train_result(
    coin: str,
    *,
    trades: int,
    pnl: float,
    score: float,
) -> CoinTrainResult:
    return CoinTrainResult(
        coin=coin,
        symbol=f"{coin}/USDT:USDT",
        net_pnl=pnl,
        gross_pnl=pnl,
        total_trades=trades,
        winning_trades=trades,
        win_rate=100.0,
        max_drawdown=0.0,
        max_drawdown_pct=0.0,
        sharpe_ratio=score,
        profit_factor=float("inf"),
        avg_trade_pnl=pnl / trades,
        costs=0.0,
        score=score,
    )


def _bare_runner() -> UniverseWalkForwardRunner:
    runner = UniverseWalkForwardRunner.__new__(UniverseWalkForwardRunner)
    runner.top_k = 2
    runner.min_trades_train = 3
    runner.allow_negative_train_selection = False
    runner.allow_sparse_train_selection = False
    runner.workers = 2
    runner.kill_loss_streak = 3
    runner.kill_negative_r = 1.0
    runner.base_config = BacktestConfig(
        initial_balance=10_000,
        position_size_usdt=1_000,
    )
    runner._print_lock = asyncio.Lock()
    return runner


def test_sparse_one_trade_winner_is_not_selected_by_default() -> None:
    runner = _bare_runner()
    results = [
        _train_result("LUCKY", trades=1, pnl=100, score=100),
        _train_result("ROBUST", trades=4, pnl=20, score=1),
    ]

    selected, _ = runner._select_coins(results)
    assert selected == ["ROBUST"]

    runner.allow_sparse_train_selection = True
    selected_with_fallback, _ = runner._select_coins(results)
    assert selected_with_fallback == ["ROBUST", "LUCKY"]


@pytest.mark.asyncio
async def test_trade_phase_keeps_independent_account_per_coin() -> None:
    runner = _bare_runner()
    calls = []

    async def fake_single_coin(
        self,
        coin,
        _start,
        _end,
    ):
        calls.append(coin)
        return SimpleNamespace(trades=[])

    runner._run_single_coin_backtest = MethodType(fake_single_coin, runner)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    results = await runner._run_trade_phase(
        start,
        start + timedelta(days=7),
        ["LINK", "AAVE"],
    )

    assert set(calls) == {"LINK", "AAVE"}
    assert [result.coin for result in results] == ["LINK", "AAVE"]


def test_kill_switch_discards_trades_after_trigger_on_same_coin() -> None:
    runner = _bare_runner()
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    trades = [
        Trade(
            symbol="LINK/USDT:USDT",
            side="long",
            entry_time=start + timedelta(hours=index),
            exit_time=start + timedelta(hours=index + 1),
            entry_z_score=-2.5,
            exit_z_score=-3.0,
            entry_price=10.0,
            exit_price=9.9,
            size_usdt=1_000,
            pnl=-10.0,
            pnl_pct=-0.5,
            exit_reason="SL",
            duration_hours=1.0,
        )
        for index in range(4)
    ]

    kept, killed, reason = runner._apply_online_kill_switch(trades)

    assert killed is True
    assert reason == "LOSS_STREAK_3"
    assert len(kept) == 3


@pytest.mark.asyncio
async def test_range_cache_reuses_primed_data_for_overlapping_windows() -> None:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=10)
    index = pd.date_range(start, end, freq="15min")
    frame = pd.DataFrame({"close": range(len(index))}, index=index)
    base_loader = SimpleNamespace(
        load_ohlcv_bulk=AsyncMock(return_value={"ETH": frame}),
        load_ohlcv_with_cache=AsyncMock(),
    )
    loader = _InMemoryOHLCVCacheLoader(base_loader)
    await loader.prime_ohlcv_bulk(["ETH"], start, end)

    first = await loader.load_ohlcv_bulk(
        ["ETH"],
        start + timedelta(days=1),
        start + timedelta(days=5),
    )
    second = await loader.load_ohlcv_bulk(
        ["ETH"],
        start + timedelta(days=3),
        start + timedelta(days=8),
    )

    assert not first["ETH"].empty
    assert not second["ETH"].empty
    base_loader.load_ohlcv_bulk.assert_awaited_once()
    base_loader.load_ohlcv_with_cache.assert_not_awaited()


@pytest.mark.asyncio
async def test_range_cache_fetches_only_uncovered_extension() -> None:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    middle = start + timedelta(days=5)
    end = start + timedelta(days=10)

    async def load_range(**kwargs):
        index = pd.date_range(
            kwargs["start_time"],
            kwargs["end_time"],
            freq="1min",
        )
        return pd.DataFrame({"close": range(len(index))}, index=index)

    base_loader = SimpleNamespace(
        load_ohlcv_bulk=AsyncMock(),
        load_ohlcv_with_cache=AsyncMock(side_effect=load_range),
    )
    loader = _InMemoryOHLCVCacheLoader(base_loader)

    await loader.load_ohlcv_with_cache(
        "ETH",
        0,
        "1m",
        start_time=start,
        end_time=middle,
    )
    extended = await loader.load_ohlcv_with_cache(
        "ETH",
        0,
        "1m",
        start_time=start + timedelta(days=2),
        end_time=end,
    )

    assert extended.index.min() >= start + timedelta(days=2)
    assert extended.index.max() < end
    assert base_loader.load_ohlcv_with_cache.await_count == 2
    extension_call = base_loader.load_ohlcv_with_cache.await_args_list[1].kwargs
    assert extension_call["start_time"] == middle
    assert extension_call["end_time"] == end
