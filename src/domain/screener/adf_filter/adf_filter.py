"""
ADF Filter Service.

Calculates the Augmented Dickey-Fuller test p-value for spread time series
to determine if the spread is stationary (tradeable) or non-stationary.

ADF Test interpretation:
- p-value < 0.05: Stationary (reject null hypothesis of unit root) - GOOD for stat-arb
- p-value >= 0.05: Non-stationary (cannot reject unit root) - BAD for stat-arb

This is a complementary filter to Hurst exponent:
- Hurst measures mean-reversion tendency
- ADF tests for stationarity (no unit root)

Both should pass for high-quality stat-arb spreads.
"""

from typing import Optional

import numpy as np
import pandas as pd

# Try to import statsmodels for ADF test
try:
    from statsmodels.tsa.stattools import adfuller

    ADF_AVAILABLE = True
except ImportError:
    ADF_AVAILABLE = False


class ADFFilterService:
    """
    ADF (Augmented Dickey-Fuller) filter for stat-arb spread stationarity.

    Calculates the ADF test p-value for each spread to determine
    if it's stationary and suitable for mean-reversion trading.
    """

    # Default threshold: only trade if p-value < 0.08 (stationary at 92% confidence)
    DEFAULT_PVALUE_THRESHOLD = 0.08
    DEFAULT_LOOKBACK_CANDLES = 300

    def __init__(
        self,
        logger,
        pvalue_threshold: Optional[float] = None,
        lookback_candles: Optional[int] = None,
    ):
        """
        Initialize ADFFilterService.

        Args:
            logger: Logger instance
            pvalue_threshold: Maximum p-value for trading (default: 0.05)
            lookback_candles: Number of candles for calculation (default: 300)
        """
        self._logger = logger
        self._pvalue_threshold = pvalue_threshold or self.DEFAULT_PVALUE_THRESHOLD
        self._lookback_candles = lookback_candles or self.DEFAULT_LOOKBACK_CANDLES

        if not ADF_AVAILABLE:
            self._logger.warning(
                "statsmodels library not installed. ADF filter disabled. "
                "Install with: pip install statsmodels"
            )

    @property
    def is_available(self) -> bool:
        """Check if ADF test is available (statsmodels installed)."""
        return ADF_AVAILABLE

    @property
    def threshold(self) -> float:
        """Get current p-value threshold."""
        return self._pvalue_threshold

    def calculate(self, spread_series: pd.Series) -> float:
        """
        Calculate ADF test p-value for a spread series.

        Args:
            spread_series: Time series of spread values (log price differences)

        Returns:
            ADF p-value (float between 0 and 1)
            Returns 1.0 (non-stationary) if calculation fails
        """
        if not ADF_AVAILABLE:
            return 1.0  # Fail safe - assume non-stationary

        # Use last N candles
        series = spread_series.tail(self._lookback_candles).dropna()

        if len(series) < 50:
            self._logger.warning(
                f"Insufficient data for ADF calculation. "
                f"Need 50+, have {len(series)}. Returning 1.0 (non-stationary)"
            )
            return 1.0

        try:
            # ADF test returns: (adf_stat, pvalue, usedlag, nobs, crit_values, icbest)
            result = adfuller(series.values, autolag="AIC")
            pvalue = float(result[1])
            return pvalue

        except Exception as e:
            self._logger.warning(f"ADF calculation failed: {e}. Returning 1.0")
            return 1.0

    def calculate_for_spread(
        self,
        coin_log_prices: pd.Series,
        primary_log_prices: pd.Series,
        beta: float,
    ) -> float:
        """
        Calculate ADF p-value for a stat-arb spread.

        Spread = log(COIN) - beta * log(PRIMARY)

        Args:
            coin_log_prices: Log prices of the coin
            primary_log_prices: Log prices of primary pair (ETH)
            beta: Hedge ratio from regression

        Returns:
            ADF p-value for the spread
        """
        # Calculate spread
        spread = coin_log_prices - beta * primary_log_prices

        return self.calculate(spread)

    def is_stationary(self, pvalue: float) -> bool:
        """
        Check if ADF p-value indicates stationarity.

        Args:
            pvalue: ADF test p-value

        Returns:
            True if spread is stationary (p-value < threshold)
        """
        return pvalue < self._pvalue_threshold
