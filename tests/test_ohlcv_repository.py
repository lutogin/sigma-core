from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd

from src.domain.data_loader.ohlcv_repository import OHLCVRepository


def test_save_data_uses_multi_value_upsert_and_reports_all_rows() -> None:
    database = MagicMock()
    database.execute_values.side_effect = lambda _query, params: len(params)
    repository = OHLCVRepository.__new__(OHLCVRepository)
    repository.db = database
    repository.logger = MagicMock()
    index = pd.DatetimeIndex(
        [
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.05, 2.05],
            "volume": [100.0, 200.0],
        },
        index=index,
    )

    affected = repository.save_data("ETH/USDT:USDT", "1m", frame)

    assert affected == 2
    query, params = database.execute_values.call_args.args
    assert "VALUES %s" in query
    assert len(params) == 2
    assert params[0][:3] == (
        "ETH/USDT:USDT",
        "1m",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def test_load_data_preserves_exact_half_open_range() -> None:
    database = MagicMock()
    database.fetch_all.return_value = []
    repository = OHLCVRepository.__new__(OHLCVRepository)
    repository.db = database
    repository.logger = MagicMock()
    start = datetime(2026, 1, 1, 12, 34, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 13, 45, tzinfo=timezone.utc)

    repository.load_data("ETH/USDT:USDT", "1m", start, end)

    query, params = database.fetch_all.call_args.args
    assert "timestamp < %s" in query
    assert params == ("ETH/USDT:USDT", "1m", start, end)
