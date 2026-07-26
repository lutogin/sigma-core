import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from backtests.run_backtest import BacktestConfig, Trade
from backtests.run_universe_walk_forward import (
    CoinTrainResult,
    UniverseWFResult,
    UniverseWalkForwardRunner,
    WFStepResult,
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


def test_report_keeps_flat_steps_separate_from_losses(tmp_path, capsys) -> None:
    runner = _bare_runner()
    runner.coins = []
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)

    steps = [
        WFStepResult(
            step_num=index,
            train_start=start,
            train_end=start + timedelta(days=1),
            trade_start=start + timedelta(days=1),
            trade_end=start + timedelta(days=2),
            train_results=[],
            selected_coins=[],
            selection_scores={},
            trade_results=[],
            portfolio_pnl=pnl,
            portfolio_dd=0.0,
        )
        for index, pnl in enumerate((1.0, 0.0, -1.0), 1)
    ]
    result = UniverseWFResult(
        start_date=start,
        end_date=start + timedelta(days=2),
        train_days=1,
        trade_days=1,
        top_k=2,
        min_trades_train=3,
        rank_metric="netPnL",
        steps=steps,
        total_portfolio_pnl=0.0,
        total_trades=0,
        max_portfolio_dd=0.0,
        coin_selection_turnover=0.0,
        live_selection_start=start,
        live_selection_end=start + timedelta(days=2),
        live_selection_results=[],
        live_selected_coins=[],
        live_selection_scores={},
    )

    runner._print_final_report(result)
    assert "Losing Steps:            1/3" in capsys.readouterr().out

    output_path = tmp_path / "result.json"
    runner.save_results(result, str(output_path))
    summary = json.loads(output_path.read_text())["summary"]
    assert summary["profitable_steps"] == 1
    assert summary["losing_steps"] == 1
    assert summary["flat_steps"] == 1


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


@pytest.mark.asyncio
async def test_train_phase_fails_closed_if_any_coin_errors() -> None:
    runner = _bare_runner()
    runner.coins = ["LINK", "BROKEN"]
    runner.rank_metric = "netPnL"

    async def fake_single_coin(self, coin, _start, _end):
        if coin == "BROKEN":
            raise RuntimeError("incomplete data")
        return SimpleNamespace(total_trades=0)

    runner._run_single_coin_backtest = MethodType(fake_single_coin, runner)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(RuntimeError, match="failed closed"):
        await runner._run_train_phase(start, start + timedelta(days=7))


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
