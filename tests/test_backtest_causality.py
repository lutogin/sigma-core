from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from backtests.backtest_shared import compute_trailing_pullback_calibration
from backtests.run_backtest import (
    BacktestConfig,
    HistoricalFundingCache,
    StatArbBacktest,
    iter_synchronized_minute_prices,
)
from src.integrations.exchange.binance import RateLimiter


class NullLogger:
    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


def _ohlcv(index, values):
    return pd.DataFrame(
        {
            "open": values,
            "high": values,
            "low": values,
            "close": values,
            "volume": [1.0] * len(values),
        },
        index=pd.DatetimeIndex(index),
    )


def test_minute_close_is_observed_at_close_time_not_open_time() -> None:
    minute_open = pd.Timestamp("2025-01-01T12:00:00Z")
    coin = _ohlcv([minute_open], [101.0])
    primary = _ohlcv([minute_open], [2_001.0])

    observations = list(
        iter_synchronized_minute_prices(coin, primary, use_ohlc_pseudo_ticks=False)
    )

    assert observations == [(minute_open + timedelta(minutes=1), 101.0, 2_001.0)]


def test_alignment_never_backfills_prelisting_history() -> None:
    index = pd.date_range("2025-01-01", periods=4, freq="15min", tz="UTC")
    primary = _ohlcv(index, [100.0, 101.0, 102.0, 103.0])
    listed_later = _ohlcv(index[2:], [10.0, 11.0])

    backtest = StatArbBacktest.__new__(StatArbBacktest)
    backtest.primary_pair = "ETH/USDT:USDT"
    aligned = backtest._align_data(
        {
            "ETH/USDT:USDT": primary,
            "NEW/USDT:USDT": listed_later,
        }
    )

    assert pd.isna(aligned["NEW/USDT:USDT"].iloc[0]["close"])
    assert pd.isna(aligned["NEW/USDT:USDT"].iloc[1]["close"])
    assert aligned["NEW/USDT:USDT"].iloc[2]["close"] == 10.0


def test_funding_uses_raw_events_without_leverage_or_union_duplication() -> None:
    cache = HistoricalFundingCache(NullLogger())
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    cache._cache = {
        "COIN": [
            (start + timedelta(hours=4), 0.001),
            (start + timedelta(hours=8), 0.001),
        ],
        "ETH": [(start + timedelta(hours=8), 0.0005)],
    }
    cache._intervals = {"COIN": 4, "ETH": 8}

    pnl_1x = cache.calculate_funding_pnl(
        coin_symbol="COIN",
        primary_symbol="ETH",
        entry_time=start,
        exit_time=start + timedelta(hours=9),
        spread_side="long",
        coin_size_usdt=1_000.0,
        primary_size_usdt=1_000.0,
        leverage=1,
    )
    pnl_10x = cache.calculate_funding_pnl(
        coin_symbol="COIN",
        primary_symbol="ETH",
        entry_time=start,
        exit_time=start + timedelta(hours=9),
        spread_side="long",
        coin_size_usdt=1_000.0,
        primary_size_usdt=1_000.0,
        leverage=10,
    )

    # Long coin pays 2 × $1; short ETH receives $0.50.
    assert pnl_1x == pytest.approx(-1.5)
    assert pnl_10x == pytest.approx(pnl_1x)


def test_latest_funding_is_normalized_only_for_entry_comparison() -> None:
    cache = HistoricalFundingCache(NullLogger())
    ts = datetime(2025, 1, 1, 4, tzinfo=timezone.utc)
    cache._cache = {"COIN": [(ts, 0.001)]}
    cache._intervals = {"COIN": 4}

    assert cache.get_rate_at("COIN", ts, normalize_to_8h=False) == pytest.approx(0.001)
    assert cache.get_rate_at("COIN", ts) == pytest.approx(0.002)


@pytest.mark.asyncio
async def test_historical_funding_load_fails_closed_on_transport_error() -> None:
    cache = HistoricalFundingCache(NullLogger())

    class BrokenExchange:
        async def get_historical_funding_rates(self, **_kwargs):
            raise ConnectionError("funding endpoint unavailable")

    with pytest.raises(RuntimeError, match="COIN"):
        await cache.load(
            BrokenExchange(),
            ["COIN"],
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2025, 2, 1, tzinfo=timezone.utc),
        )


def test_rate_limiter_can_be_constructed_without_an_event_loop() -> None:
    limiter = RateLimiter(requests_per_second=10.0, burst_size=20)
    assert limiter._tokens == 20.0


def test_fixed_notional_applies_halflife_before_final_equity_cap() -> None:
    backtest = StatArbBacktest.__new__(StatArbBacktest)
    backtest.config = BacktestConfig(
        initial_balance=40_000.0,
        position_size_usdt=10_000.0,
        max_coin_notional_pct=0.525,
    )
    backtest.balance = 40_000.0

    assert backtest._get_base_position_size() == pytest.approx(10_000.0)
    assert backtest._get_position_sizing(0.5).final_notional == pytest.approx(5_000.0)
    assert backtest._get_position_sizing(2.1).final_notional == pytest.approx(
        21_000.0
    )
    assert backtest._get_position_sizing(3.0).final_notional == pytest.approx(
        21_000.0
    )


def test_backtest_uses_same_z_pullback_as_live_observer() -> None:
    normal, extreme, scale = compute_trailing_pullback_calibration(
        timeframe="15m",
        trailing_pullback_base=0.2,
        trailing_pullback_extreme_base=0.6,
    )

    assert normal == 0.2
    assert extreme == 0.6
    assert scale == 1.0
