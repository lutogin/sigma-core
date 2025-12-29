"""
Half-Life Filter Service.

Calculates the Half-Life of mean reversion for spread time series
using the Ornstein-Uhlenbeck process.

Half-Life interpretation:
- HL = time for spread to revert halfway to its mean
- Lower HL = faster mean reversion = better for stat-arb
- Rule: HL <= 0.5 * MAX_POSITION_BARS (if holding 24h, HL must be <= 12h)

This is a complementary filter to ADF:
- ADF tests for stationarity (no unit root)
- Half-Life measures mean-reversion speed

Both should pass for high-quality stat-arb spreads.
"""

from typing import Optional

import numpy as np
import pandas as pd

# Try to import statsmodels for OLS regression
try:
    import statsmodels.api as sm

    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False


class HalfLifeFilterService:
    """
    Half-Life filter for stat-arb spread mean-reversion speed.

    Calculates the Half-Life of mean reversion using Ornstein-Uhlenbeck
    process to determine if spread reverts fast enough for trading.
    """

    DEFAULT_MAX_BARS = 48  # Default: 0.5 * 96 (MAX_POSITION_BARS)
    DEFAULT_LOOKBACK_CANDLES = 300

    def __init__(
        self,
        logger,
        max_bars: Optional[float] = None,
        lookback_candles: Optional[int] = None,
    ):
        """
        Initialize HalfLifeFilterService.

        Args:
            logger: Logger instance
            max_bars: Maximum acceptable half-life in bars (default: 48)
            lookback_candles: Number of candles for calculation (default: 300)
        """
        self._logger = logger
        self._max_bars = max_bars or self.DEFAULT_MAX_BARS
        self._lookback_candles = lookback_candles or self.DEFAULT_LOOKBACK_CANDLES

        if not STATSMODELS_AVAILABLE:
            self._logger.warning(
                "statsmodels library not installed. Half-Life filter disabled. "
                "Install with: pip install statsmodels"
            )

    @property
    def is_available(self) -> bool:
        """Check if Half-Life calculation is available (statsmodels installed)."""
        return STATSMODELS_AVAILABLE

    @property
    def threshold(self) -> float:
        """Get current max half-life threshold in bars."""
        return self._max_bars

    def calculate(self, spread_series: pd.Series) -> float:
        """
        Calculate Half-Life of mean reversion for a spread series.

        Uses Ornstein-Uhlenbeck process:
        - Regression: diff = lambda * lag + mu
        - Half-life formula: HL = -ln(2) / lambda

        Args:
            spread_series: Time series of spread values (log price differences)

        Returns:
            Half-Life in bars (float)
            Returns float("inf") if:
            - Calculation fails
            - lambda >= 0 (not mean-reverting, diverging or random walk)
        """
        if not STATSMODELS_AVAILABLE:
            return float("inf")

        # Use last N candles
        series = spread_series.tail(self._lookback_candles).dropna()

        if len(series) < 50:
            self._logger.warning(
                f"Insufficient data for Half-Life calculation. "
                f"Need 50+, have {len(series)}. Returning inf"
            )
            return float("inf")

        try:
            # Build DataFrame for regression
            df_spread = series.to_frame(name="spread")
            df_spread["lag"] = df_spread["spread"].shift(1)
            df_spread["diff"] = df_spread["spread"] - df_spread["lag"]
            df_spread.dropna(inplace=True)

            if len(df_spread) < 30:
                self._logger.warning(
                    f"Half-Life: insufficient regression data ({len(df_spread)} rows)"
                )
                return float("inf")

            # Regression: diff = lambda * lag + mu
            Y = df_spread["diff"]
            X = df_spread["lag"]
            X = sm.add_constant(X)  # Add intercept (mu)

            model = sm.OLS(Y, X)
            results = model.fit()

            lam = results.params["lag"]

            # If lambda >= 0, spread is not mean-reverting
            # (diverging or random walk)
            if lam >= 0:
                self._logger.debug(f"Half-Life: lambda={lam:.6f} >= 0, not mean-reverting")
                return float("inf")

            # Half-life formula: HL = -ln(2) / lambda
            half_life = -np.log(2) / lam

            return max(0.0, half_life)

        except Exception as e:
            self._logger.warning(f"Half-Life calculation failed: {e}. Returning inf")
            return float("inf")

    def calculate_for_spread(
        self,
        coin_log_prices: pd.Series,
        primary_log_prices: pd.Series,
        beta: float,
    ) -> float:
        """
        Calculate Half-Life for a stat-arb spread.

        Spread = log(COIN) - beta * log(PRIMARY)

        Args:
            coin_log_prices: Log prices of the coin
            primary_log_prices: Log prices of primary pair (ETH)
            beta: Hedge ratio from regression

        Returns:
            Half-Life in bars for the spread
        """
        # Align series by index
        aligned_coin = coin_log_prices.dropna()
        aligned_primary = primary_log_prices.reindex(aligned_coin.index).dropna()
        aligned_coin = aligned_coin.reindex(aligned_primary.index)

        if len(aligned_coin) < 50:
            self._logger.warning(
                f"Half-Life: insufficient aligned data ({len(aligned_coin)} candles)"
            )
            return float("inf")

        # Calculate spread
        spread = aligned_coin - beta * aligned_primary

        return self.calculate(spread)

    def is_acceptable(self, half_life: float) -> bool:
        """
        Check if Half-Life is acceptable for trading.

        Args:
            half_life: Half-Life in bars

        Returns:
            True if half_life <= threshold (fast enough mean reversion)
        """
        return half_life <= self._max_bars
