from datetime import datetime, timezone

from scripts.update_trading_pairs_from_wf import (
    PromotionPolicy,
    evaluate_promotion,
)


def _result(*, pnl: float = 25.0) -> dict:
    return {
        "metadata": {
            "account_model": "independent_per_coin",
            "end_date": "2026-07-20T00:00:00+00:00",
            "strategy_config": {
                "use_live_exit": True,
                "use_trailing_entry": True,
                "use_adf_filter": True,
                "use_funding_filter": True,
                "use_ohlc_pseudo_ticks": False,
                "half_spread_bps": 2.0,
                "slippage_bps": 1.0,
                "maker_fee": 0.0002,
                "taker_fee": 0.0004,
                "use_limit_orders": False,
            },
        },
        "summary": {"total_portfolio_pnl": pnl},
        "live_selection": {
            "selected_coins": ["LINK", "AAVE", "LUCKY"],
        },
        "coin_period_stats": [
            {
                "coin": "LINK",
                "trade_net_pnl": 15.0,
                "trade_total_trades": 4,
                "selection_rate": 0.75,
                "killed_count": 0,
            },
            {
                "coin": "AAVE",
                "trade_net_pnl": 10.0,
                "trade_total_trades": 3,
                "selection_rate": 0.50,
                "killed_count": 0,
            },
            {
                "coin": "LUCKY",
                "trade_net_pnl": 100.0,
                "trade_total_trades": 1,
                "selection_rate": 0.25,
                "killed_count": 0,
            },
        ],
        "steps": [
            {"portfolio_pnl": 1.0},
            {"portfolio_pnl": 1.0},
            {"portfolio_pnl": 1.0},
            {"portfolio_pnl": 1.0},
            {"portfolio_pnl": -1.0},
            {"portfolio_pnl": -1.0},
        ],
    }


def test_pair_promotion_accepts_only_robust_oos_candidates() -> None:
    decision = evaluate_promotion(
        _result(),
        active_symbols=[],
        policy=PromotionPolicy(),
        now=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert decision.can_activate is True
    assert decision.symbols == [
        "LINK/USDT:USDT",
        "AAVE/USDT:USDT",
    ]


def test_pair_promotion_rejects_stale_or_negative_run() -> None:
    result = _result(pnl=-1.0)
    decision = evaluate_promotion(
        result,
        active_symbols=[],
        policy=PromotionPolicy(max_result_age_days=3),
        now=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert decision.can_activate is False
    assert any("stale" in error for error in decision.errors)
    assert any("not positive" in error for error in decision.errors)


def test_pair_promotion_blocks_excessive_live_turnover() -> None:
    decision = evaluate_promotion(
        _result(),
        active_symbols=[
            "UNI/USDT:USDT",
            "MORPHO/USDT:USDT",
            "ENS/USDT:USDT",
        ],
        policy=PromotionPolicy(max_turnover=0.5),
        now=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert decision.can_activate is False
    assert any("turnover" in error for error in decision.errors)


def test_pair_promotion_does_not_cherry_pick_oos_winner() -> None:
    result = _result()
    result["live_selection"]["selected_coins"] = ["LUCKY", "AAVE"]

    decision = evaluate_promotion(
        result,
        active_symbols=[],
        policy=PromotionPolicy(min_pairs=1),
        now=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert decision.symbols == ["AAVE/USDT:USDT"]


def test_pair_promotion_requires_enough_oos_steps_and_current_selection() -> None:
    result = _result()
    result["steps"] = [{}]
    result["live_selection"] = {}

    decision = evaluate_promotion(
        result,
        active_symbols=[],
        policy=PromotionPolicy(),
        now=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert decision.can_activate is False
    assert any("OOS steps" in error for error in decision.errors)
    assert any("live selection" in error for error in decision.errors)


def test_pair_promotion_rejects_optimistic_execution_assumptions() -> None:
    result = _result()
    result["metadata"]["strategy_config"]["slippage_bps"] = 0

    decision = evaluate_promotion(
        result,
        active_symbols=[],
        policy=PromotionPolicy(),
        now=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert decision.can_activate is False
    assert any("slippage" in error for error in decision.errors)


def test_pair_promotion_validates_fee_used_by_execution_model() -> None:
    result = _result()
    result["metadata"]["strategy_config"]["taker_fee"] = 0

    decision = evaluate_promotion(
        result,
        active_symbols=[],
        policy=PromotionPolicy(),
        now=datetime(2026, 7, 26, tzinfo=timezone.utc),
    )

    assert decision.can_activate is False
    assert any("taker_fee" in error for error in decision.errors)
