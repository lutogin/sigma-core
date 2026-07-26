import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from backtests.run_backtest import BacktestConfig, Trade
from backtests.run_walk_forward_backtest import WalkForwardRunner


def test_json_report_preserves_actual_position_sizing(capsys) -> None:
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    trade = Trade(
        symbol="LINK/USDT:USDT",
        side="long",
        entry_time=start,
        exit_time=start + timedelta(hours=1),
        entry_z_score=-2.5,
        exit_z_score=-0.2,
        entry_price=10.0,
        exit_price=10.1,
        size_usdt=10_000.0,
        pnl=100.0,
        pnl_pct=0.5,
        exit_reason="TP",
        duration_hours=1.0,
        gross_notional=18_000.0,
        margin_used=1_800.0,
        coin_notional=10_000.0,
        primary_notional=8_000.0,
        size_multiplier=1.0,
    )
    result = SimpleNamespace(
        total_pnl=100.0,
        total_pnl_pct=0.25,
        total_trades=1,
        winning_trades=1,
        win_rate=1.0,
        max_drawdown=0.0,
        max_drawdown_pct=0.0,
        sharpe_ratio=1.0,
        trades=[trade],
    )
    runner = WalkForwardRunner({}, BacktestConfig(), json_output=True)
    runner.results = [("2025-January", result)]

    runner._output_json()

    payload = json.loads(capsys.readouterr().out)
    reported = payload["monthly"][0]["trades_detail"][0]
    assert reported["coin_notional"] == 10_000.0
    assert reported["primary_notional"] == 8_000.0
    assert reported["gross_notional"] == 18_000.0
