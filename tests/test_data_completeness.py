from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from src.domain.data_loader.async_data_loader import AsyncDataLoaderService
from src.integrations.exchange import BinanceClient, ExchangeConfig


class _Logger:
    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


def _frame(index: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1.0,
        },
        index=index,
    )


@pytest.mark.asyncio
async def test_loader_rejects_internal_candle_gap_after_refetch() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(minutes=3)
    cached = _frame(
        pd.DatetimeIndex(
            [
                start,
                start + timedelta(minutes=2),
            ]
        )
    )
    repository = SimpleNamespace(
        load_all_symbols_data=MagicMock(return_value={"ETH": cached}),
        save_data_bulk=MagicMock(),
    )
    exchange = SimpleNamespace(fetch_ohlcv=AsyncMock(return_value=pd.DataFrame()))
    loader = AsyncDataLoaderService(
        logger=_Logger(),
        exchange_client=exchange,
        ohlcv_repository=repository,
    )

    with pytest.raises(RuntimeError, match="missing 1m candles"):
        await loader.load_ohlcv_bulk(
            ["ETH"],
            start,
            end,
            timeframe="1m",
        )


@pytest.mark.asyncio
async def test_binance_history_raises_instead_of_returning_partial_data() -> None:
    client = BinanceClient(ExchangeConfig(), _Logger())

    class _Api:
        async def futures_klines(self, **_params):
            raise TimeoutError("history timed out")

    async def get_client():
        return _Api()

    client._get_client = get_client
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(RuntimeError, match="Failed to fetch OHLCV"):
        await client.fetch_ohlcv(
            "ETH/USDT:USDT",
            "1m",
            start,
            start + timedelta(minutes=1),
            max_retries=1,
        )
