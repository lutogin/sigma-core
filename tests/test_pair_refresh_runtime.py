from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from src.domain.screener.screener import ScreenerService


class _Logger:
    def debug(self, *_args, **_kwargs):
        pass

    def info(self, *_args, **_kwargs):
        pass

    def warning(self, *_args, **_kwargs):
        pass

    def error(self, *_args, **_kwargs):
        pass


def test_removed_pair_stays_in_scan_until_its_position_closes() -> None:
    screener = ScreenerService.__new__(ScreenerService)
    screener._logger = _Logger()
    screener._consistent_pairs = ["FALLBACK/USDT:USDT"]
    screener._trading_pair_repository = SimpleNamespace(
        get_active_symbols=lambda: ["LINK/USDT:USDT"]
    )
    screener._position_state = SimpleNamespace(
        get_active_positions=lambda: [
            SimpleNamespace(coin_symbol="AAVE/USDT:USDT"),
        ]
    )

    assert screener._get_trading_pairs() == [
        "LINK/USDT:USDT",
        "AAVE/USDT:USDT",
    ]


def test_pair_loading_falls_back_to_config_when_repository_is_empty() -> None:
    screener = ScreenerService.__new__(ScreenerService)
    screener._logger = _Logger()
    screener._consistent_pairs = ["UNI/USDT:USDT"]
    screener._trading_pair_repository = SimpleNamespace(get_active_symbols=lambda: [])
    screener._position_state = None

    assert screener._get_trading_pairs() == ["UNI/USDT:USDT"]


@pytest.mark.asyncio
async def test_live_loader_does_not_globally_truncate_pair_histories() -> None:
    full_index = pd.date_range(
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        periods=10,
        freq="15min",
    )
    recent_index = full_index[-3:]

    class _Loader:
        async def load_ohlcv_bulk(self, **_kwargs):
            return {
                "ETH/USDT:USDT": pd.DataFrame(
                    {"close": range(10, 20)},
                    index=full_index,
                ),
                "LINK/USDT:USDT": pd.DataFrame(
                    {"close": range(20, 30)},
                    index=full_index,
                ),
                "NEW/USDT:USDT": pd.DataFrame(
                    {"close": [1, 2, 3]},
                    index=recent_index,
                ),
            }

    screener = ScreenerService.__new__(ScreenerService)
    screener._logger = _Logger()
    screener._data_loader = _Loader()
    screener._lookback_window_days = 3
    screener._enable_stability_filter = False
    screener._stability_windows_days = []
    screener._enable_beta_drift_guard = False
    screener._primary_pair = "ETH/USDT:USDT"
    screener._timeframe = "15m"
    screener._consistent_pairs = [
        "LINK/USDT:USDT",
        "NEW/USDT:USDT",
    ]
    screener._trading_pair_repository = None
    screener._position_state = None

    loaded = await screener._load_ohlcv_data()

    assert len(loaded["ETH/USDT:USDT"]) == 10
    assert len(loaded["LINK/USDT:USDT"]) == 10
    assert len(loaded["NEW/USDT:USDT"]) == 3


@pytest.mark.asyncio
async def test_live_loader_rejects_pair_without_latest_synchronized_candle() -> None:
    full_index = pd.date_range(
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        periods=10,
        freq="15min",
    )

    class _Loader:
        async def load_ohlcv_bulk(self, **_kwargs):
            return {
                "ETH/USDT:USDT": pd.DataFrame(
                    {"close": range(10, 20)},
                    index=full_index,
                ),
                "STALE/USDT:USDT": pd.DataFrame(
                    {"close": range(20, 29)},
                    index=full_index[:-1],
                ),
            }

    screener = ScreenerService.__new__(ScreenerService)
    screener._logger = _Logger()
    screener._data_loader = _Loader()
    screener._lookback_window_days = 3
    screener._enable_stability_filter = False
    screener._stability_windows_days = []
    screener._enable_beta_drift_guard = False
    screener._primary_pair = "ETH/USDT:USDT"
    screener._timeframe = "15m"
    screener._consistent_pairs = ["STALE/USDT:USDT"]
    screener._trading_pair_repository = None
    screener._position_state = None

    loaded = await screener._load_ohlcv_data()

    assert set(loaded) == {"ETH/USDT:USDT"}
