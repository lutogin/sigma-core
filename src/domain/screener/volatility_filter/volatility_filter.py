"""
Volatility Filter Service.

Monitors ETH volatility as a proxy for overall market conditions.
When ETH is volatile, we stop all trading to avoid getting caught
in flash crashes, pumps, or other dangerous market conditions.

Two protection levels:
1. "Storm Warning" - Market is too choppy, high rolling volatility
2. "Flash Crash/Pump" - Sharp move in short time period
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class VolatilityCheckResult:
    """Result of volatility check."""

    is_safe: bool
    current_volatility: float
    price_change_4h: float
    stop_reason: Optional[str] = None
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


class VolatilityFilterService:
    """
    Volatility Filter for market safety.

    Uses ETH as a proxy for overall market conditions.
    If ETH is volatile, we don't trade anything.

    Metrics:
    - Rolling volatility (std dev of returns) over 6 hours
    - Flash crash/pump detection (4h price change)
    """

    # Default thresholds
    DEFAULT_VOLATILITY_WINDOW = 24  # 6 hours (24 x 15min candles)
    DEFAULT_VOLATILITY_THRESHOLD = 0.008  # 0.8% std dev = "Storm"
    DEFAULT_CRASH_WINDOW = 16  # 4 hours (16 x 15min candles)
    DEFAULT_CRASH_THRESHOLD = 0.05  # 5% move = "Flash Crash/Pump"

    def __init__(
        self,
        logger,
        primary_pair: str = "ETH/USDT:USDT",
        timeframe: str = "15m",
        volatility_window: Optional[int] = None,
        volatility_threshold: Optional[float] = None,
        crash_window: Optional[int] = None,
        crash_threshold: Optional[float] = None,
    ):
        """
        Initialize VolatilityFilterService.

        Args:
            logger: Logger instance
            primary_pair: Symbol to monitor (default: ETH)
            timeframe: Timeframe for candles (default: 15m)
            volatility_window: Rolling window for std dev (default: 24 candles = 6h)
            volatility_threshold: Max allowed volatility (default: 0.008 = 0.8%)
            crash_window: Window for crash detection (default: 16 candles = 4h)
            crash_threshold: Max allowed 4h move (default: 0.05 = 5%)
        """
        self._logger = logger
        self._primary_pair = primary_pair
        self._timeframe = timeframe

        # Thresholds
        self._volatility_window = volatility_window or self.DEFAULT_VOLATILITY_WINDOW
        self._volatility_threshold = (
            volatility_threshold or self.DEFAULT_VOLATILITY_THRESHOLD
        )
        self._crash_window = crash_window or self.DEFAULT_CRASH_WINDOW
        self._crash_threshold = crash_threshold or self.DEFAULT_CRASH_THRESHOLD

    def check(self, ohlcv: pd.DataFrame) -> VolatilityCheckResult:
        """
        Check if market is safe for trading.

        Args:
            ohlcv: DataFrame with OHLCV data for the monitored pair (ETH).
                   Must have 'close' column and enough rows for calculations.

        Returns:
            VolatilityCheckResult with safety status and metrics.
        """
        min_required = max(self._volatility_window, self._crash_window) + 1

        if len(ohlcv) < min_required:
            self._logger.warning(
                f"Insufficient data for volatility check. "
                f"Need {min_required} candles, have {len(ohlcv)}"
            )
            # Default to safe if we don't have enough data
            return VolatilityCheckResult(
                is_safe=True,
                current_volatility=0.0,
                price_change_4h=0.0,
                stop_reason="INSUFFICIENT_DATA",
            )

        # Calculate log prices
        close_prices = ohlcv["close"]
        log_prices = pd.Series(np.log(close_prices.values), index=close_prices.index)

        # 1. Calculate rolling volatility (std dev of returns)
        returns = log_prices.diff()
        rolling_std = returns.rolling(window=self._volatility_window).std()
        current_volatility = rolling_std.iloc[-1]

        # 2. Calculate price change over crash window (4h)
        price_change_4h = abs(
            log_prices.iloc[-1] - log_prices.iloc[-self._crash_window]
        )

        # Check conditions
        is_safe = True
        stop_reason = None

        if round(current_volatility, 2) > self._volatility_threshold:
            is_safe = False
            stop_reason = (
                f"HIGH_VOLATILITY: {current_volatility:.4f} > "
                f"{self._volatility_threshold} ({current_volatility * 100:.2f}%)"
            )
            self._logger.warning(f"⛔ VOLATILITY FILTER: {stop_reason}")

        elif price_change_4h > self._crash_threshold:
            is_safe = False
            stop_reason = (
                f"FLASH_CRASH_PUMP: {price_change_4h * 100:.1f}% move in 4h > "
                f"{self._crash_threshold * 100:.0f}%"
            )
            self._logger.warning(f"⛔ VOLATILITY FILTER: {stop_reason}")

        if is_safe:
            self._logger.debug(
                f"✅ Market safe: volatility={current_volatility:.4f}, "
                f"4h_change={price_change_4h * 100:.2f}%"
            )

        return VolatilityCheckResult(
            is_safe=is_safe,
            current_volatility=current_volatility,
            price_change_4h=price_change_4h,
            stop_reason=stop_reason,
        )

    async def check_from_loader(
        self,
        data_loader,
        lookback_hours: int = 8,
    ) -> VolatilityCheckResult:
        """
        Load data and check volatility.

        Convenience method that loads ETH data from the data loader
        and performs the volatility check.

        Args:
            data_loader: AsyncDataLoaderService instance
            lookback_hours: How many hours of data to load (default: 8)

        Returns:
            VolatilityCheckResult with safety status and metrics.
        """
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=lookback_hours)

        # Load ETH data
        data = await data_loader.load_ohlcv_bulk(
            symbols=[self._primary_pair],
            start_time=start_time,
            end_time=end_time,
            batch_size=1,
            timeframe=self._timeframe,
        )

        if not data or self._primary_pair not in data:
            self._logger.warning(
                f"Failed to load {self._primary_pair} data for volatility check"
            )
            return VolatilityCheckResult(
                is_safe=True,  # Default safe if can't check
                current_volatility=0.0,
                price_change_4h=0.0,
                stop_reason="DATA_LOAD_FAILED",
            )

        return self.check(data[self._primary_pair])

    def log_status(self, result: VolatilityCheckResult) -> None:
        """Log volatility status in a formatted way."""
        if result.is_safe:
            self._logger.info(
                f"🟢 MARKET STATUS: SAFE | "
                f"Volatility: {result.current_volatility:.4f} "
                f"(limit: {self._volatility_threshold}) | "
                f"4h Change: {result.price_change_4h * 100:.2f}% "
                f"(limit: {self._crash_threshold * 100:.0f}%)"
            )
        else:
            self._logger.warning(f"🔴 MARKET STATUS: UNSAFE | {result.stop_reason}")
