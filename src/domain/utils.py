"""
Domain utilities.

Shared constants and helper functions for domain services.
"""

# Timeframe to candles per day mapping
CANDLES_PER_DAY = {
    "1m": 1440,
    "3m": 480,
    "5m": 288,
    "15m": 96,
    "30m": 48,
    "1h": 24,
    "2h": 12,
    "4h": 6,
    "6h": 4,
    "8h": 3,
    "12h": 2,
    "1d": 1,
}

# Timeframe to minutes mapping
TIMEFRAME_TO_MINUTES = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
}


def get_timeframe_minutes(timeframe: str) -> int:
    """
    Get the duration of a timeframe in minutes.

    Args:
        timeframe: Candle timeframe (e.g., "15m", "1h", "4h").

    Returns:
        Number of minutes for one candle.
    """
    return TIMEFRAME_TO_MINUTES.get(timeframe, 15)


def calculate_lookback_window(lookback_window_days: int, timeframe: str) -> int:
    """
    Calculate lookback window in number of candles.

    Args:
        lookback_window_days: Number of days for lookback window.
        timeframe: Candle timeframe (e.g., "15m", "1h").

    Returns:
        Number of candles for the lookback window.
    """
    candles_per_day = CANDLES_PER_DAY.get(timeframe, 96)
    return lookback_window_days * candles_per_day
