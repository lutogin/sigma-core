"""
Hurst Filter Service.

Calculates the Hurst exponent for spread time series to determine
if the spread is mean-reverting (tradeable) or trending (not tradeable).

Hurst Exponent interpretation:
- H < 0.5: Mean-reverting (anti-persistent) - GOOD for stat-arb
- H = 0.5: Random walk (Brownian motion) - NEUTRAL
- H > 0.5: Trending (persistent) - BAD for stat-arb

We use a threshold of 0.45 to be conservative and only trade
strongly mean-reverting spreads.
"""

from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

# Try to import hurst library, fallback to manual R/S calculation
try:
    from hurst import compute_Hc

    HURST_AVAILABLE = True
except ImportError:
    HURST_AVAILABLE = False


class HurstFilterService:
    """
    Hurst exponent filter for stat-arb spread quality.

    Calculates the Hurst exponent for each spread to determine
    if it's suitable for mean-reversion trading.
    """

    # Default threshold: only trade if H < 0.45 (strongly mean-reverting)
    DEFAULT_HURST_THRESHOLD = 0.45
    DEFAULT_LOOKBACK_CANDLES = 300

    def __init__(
        self,
        logger,
        hurst_threshold: Optional[float] = None,
        lookback_candles: Optional[int] = None,
    ):
        """
        Initialize HurstFilterService.

        Args:
            logger: Logger instance
            hurst_threshold: Maximum Hurst value for trading (default: 0.45)
            lookback_candles: Number of candles for calculation (default: 300)
        """
        self._logger = logger
        self._hurst_threshold = hurst_threshold or self.DEFAULT_HURST_THRESHOLD
        self._lookback_candles = lookback_candles or self.DEFAULT_LOOKBACK_CANDLES

        if not HURST_AVAILABLE:
            self._logger.warning(
                "hurst library not installed. Using fallback R/S calculation. "
                "Install with: pip install hurst"
            )

    def calculate(
        self,
        spread_series: pd.Series,
        simplified: bool = True,
    ) -> float:
        """
        Calculate Hurst exponent for a spread series.

        Args:
            spread_series: Time series of spread values (log price differences)
            simplified: Use simplified calculation (faster, default: True)

        Returns:
            Hurst exponent (float between 0 and 1)
        """
        # Use last N candles
        series = spread_series.tail(self._lookback_candles).dropna()

        if len(series) < 100:
            self._logger.warning(
                f"Insufficient data for Hurst calculation. "
                f"Need 100+, have {len(series)}. Returning 0.5 (neutral)"
            )
            return 0.5

        try:
            if HURST_AVAILABLE:
                # Use 'random_walk' because spread is already in log space
                # 'price' mode would do log(series) which fails for negative values
                H, _, _ = compute_Hc(
                    series.values, kind="random_walk", simplified=simplified
                )
            else:
                H = self._calculate_hurst_rs(series.values)

            return float(H)

        except Exception as e:
            self._logger.warning(f"Hurst calculation failed: {e}. Returning 0.5")
            return 0.5

    def calculate_for_spread(
        self,
        coin_log_prices: pd.Series,
        primary_log_prices: pd.Series,
        beta: float,
    ) -> float:
        """
        Calculate Hurst exponent for a stat-arb spread.

        Spread = log(COIN) - beta * log(PRIMARY)

        Args:
            coin_log_prices: Log prices of the coin
            primary_log_prices: Log prices of primary pair (ETH)
            beta: Hedge ratio from regression

        Returns:
            Hurst exponent for the spread
        """
        # Calculate spread
        spread = coin_log_prices - beta * primary_log_prices

        return self.calculate(spread)

    def is_mean_reverting(self, hurst: float) -> bool:
        """
        Check if Hurst exponent indicates mean-reversion.

        Args:
            hurst: Hurst exponent value

        Returns:
            True if spread is mean-reverting (H < threshold)
        """
        return float(hurst) < self._hurst_threshold

    def _calculate_hurst_rs(self, series: np.ndarray) -> float:
        """
        Fallback R/S (Rescaled Range) calculation for Hurst exponent.

        Uses the classic R/S analysis method.
        Note: Input is already a spread (log prices), so we use diff() for returns.
        """
        n = len(series)

        # Calculate returns as simple differences (spread is already in log space)
        returns = np.diff(series)
        returns = returns[~np.isnan(returns)]

        if len(returns) < 20:
            return 0.5

        # R/S calculation over different time scales
        max_k = min(int(n / 4), 100)
        k_values = []
        rs_values = []

        for k in range(10, max_k, 5):
            rs_list = []
            for start in range(0, len(returns) - k, k):
                subset = returns[start : start + k]

                # Mean-adjusted cumulative sum
                mean = np.mean(subset)
                y = np.cumsum(subset - mean)

                # Range
                r = np.max(y) - np.min(y)

                # Standard deviation
                s = np.std(subset, ddof=1)

                if s > 0:
                    rs_list.append(r / s)

            if rs_list:
                k_values.append(np.log(k))
                rs_values.append(np.log(np.mean(rs_list)))

        if len(k_values) < 3:
            return 0.5

        # Linear regression to get Hurst exponent
        k_arr = np.array(k_values)
        rs_arr = np.array(rs_values)

        # H = slope of log(R/S) vs log(k)
        slope = np.polyfit(k_arr, rs_arr, 1)[0]

        # Clamp to valid range
        return float(np.clip(slope, 0.0, 1.0))

    @property
    def threshold(self) -> float:
        """Get current Hurst threshold."""
        return self._hurst_threshold
