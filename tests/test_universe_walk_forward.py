import asyncio
from datetime import datetime, timedelta, timezone
from types import MethodType, SimpleNamespace

import pytest

from backtests.run_backtest import BacktestConfig, Trade
from backtests.run_universe_walk_forward import (
    CoinTrainResult,
    UniverseWalkForwardRunner,
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
