"""
Correlation Service.

Calculates rolling beta and correlation between crypto pairs
for statistical arbitrage strategy.
"""

from typing import Dict
from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.domain.utils import calculate_lookback_window


@dataclass
class CorrelationResult:
    """Result of correlation calculation for a single symbol."""
    symbol: str
    rolling_beta: pd.Series
    rolling_corr: pd.Series
    latest_beta: float
    latest_corr: float


class CorrelationService:
    """
    Service for calculating rolling beta and correlation metrics.

    Uses log returns and rolling OLS to calculate:
    - Beta (hedge coefficient): β = Cov(COIN, PRIMARY) / Var(PRIMARY)
    - Correlation: for safety filtering
    """

    def __init__(
        self,
        logger,
        lookback_window_days: int,
        timeframe: str = "15m",
    ):
        """
        Initialize CorrelationService.

        Args:
            logger: Application logger.
            lookback_window_days: Number of days for lookback window.
            timeframe: Candle timeframe (e.g., "15m", "1h").
        """
        self._logger = logger
        self._lookback_window_days = lookback_window_days
        self._timeframe = timeframe
        self._lookback_window = calculate_lookback_window(
            lookback_window_days, timeframe
        )

    def calculate(
        self,
        primary_symbol: str,
        ohlcv: Dict[str, pd.DataFrame],
    ) -> Dict[str, CorrelationResult]:
        """
        Calculate rolling beta and correlation for all symbols relative to primary.

        Args:
            primary_symbol: Primary symbol to compare against (e.g., "ETH/USDT:USDT").
            ohlcv: Dictionary mapping symbol -> OHLCV DataFrame.
                   Each DataFrame must have 'close' column.

        Returns:
            Dictionary mapping symbol -> CorrelationResult.
        """
        self._logger.info(
            f"Calculating correlation for {len(ohlcv)} symbols "
            f"against {primary_symbol} (window={self._lookback_window} candles)"
        )

        # Step 1: Preprocess - calculate log returns
        log_returns = self._preprocess_log_returns(ohlcv)

        if primary_symbol not in log_returns.columns:
            self._logger.error(f"Primary symbol {primary_symbol} not found in data")
            return {}

        # Step 2: Calculate rolling beta and correlation for each symbol
        results = self._calculate_rolling_metrics(
            log_returns=log_returns,
            primary_symbol=primary_symbol,
        )

        self._logger.info(f"Calculated correlation for {len(results)} symbols")
        return results

    def _preprocess_log_returns(
        self,
        ohlcv: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """
        Preprocess OHLCV data to log returns.

        Takes close prices and calculates log returns:
        log_return = log(price_t / price_{t-1}) = log(price_t) - log(price_{t-1})

        Args:
            ohlcv: Dictionary mapping symbol -> OHLCV DataFrame.

        Returns:
            DataFrame with log returns, columns are symbols, index is time.
        """
        close_prices = {}

        for symbol, df in ohlcv.items():
            if df.empty:
                self._logger.warning(f"Empty DataFrame for {symbol}, skipping")
                continue

            # Get close price column (handle different column name cases)
            close_col = None
            for col in ["close", "Close", "CLOSE"]:
                if col in df.columns:
                    close_col = col
                    break

            if close_col is None:
                self._logger.warning(f"No close column found for {symbol}, skipping")
                continue

            close_prices[symbol] = df[close_col]

        if not close_prices:
            self._logger.error("No valid close prices found")
            return pd.DataFrame()

        # Build DataFrame with close prices
        df_close = pd.DataFrame(close_prices)

        # Calculate log prices and then log returns (diff of log prices)
        log_prices = pd.DataFrame(np.log(df_close), columns=df_close.columns, index=df_close.index)
        log_returns = log_prices.diff().dropna()

        self._logger.debug(
            f"Preprocessed {len(df_close.columns)} symbols, "
            f"{len(log_returns)} log return observations"
        )

        return log_returns

    def _calculate_rolling_metrics(
        self,
        log_returns: pd.DataFrame,
        primary_symbol: str,
    ) -> Dict[str, CorrelationResult]:
        """
        Calculate rolling beta and correlation for each symbol.

        Beta = Cov(COIN, PRIMARY) / Var(PRIMARY)
        This is the hedge coefficient from OLS regression.

        Args:
            log_returns: DataFrame with log returns for all symbols.
            primary_symbol: Primary symbol column name.

        Returns:
            Dictionary mapping symbol -> CorrelationResult.
        """
        results = {}
        primary_returns = log_returns[primary_symbol]

        for symbol in log_returns.columns:
            if symbol == primary_symbol:
                continue

            coin_returns = log_returns[symbol]

            # Rolling covariance between coin and primary
            rolling_cov = coin_returns.rolling(
                window=self._lookback_window
            ).cov(primary_returns)

            # Rolling variance of primary
            rolling_var = primary_returns.rolling(
                window=self._lookback_window
            ).var()

            # Beta = Cov / Var
            rolling_beta = rolling_cov / rolling_var

            # Rolling correlation for safety filter
            rolling_corr = coin_returns.rolling(
                window=self._lookback_window
            ).corr(primary_returns)

            # Get latest values (drop NaN from rolling window)
            valid_beta = rolling_beta.dropna()
            valid_corr = rolling_corr.dropna()

            latest_beta = valid_beta.iloc[-1] if len(valid_beta) > 0 else np.nan
            latest_corr = valid_corr.iloc[-1] if len(valid_corr) > 0 else np.nan

            results[symbol] = CorrelationResult(
                symbol=symbol,
                rolling_beta=rolling_beta,
                rolling_corr=rolling_corr,
                latest_beta=latest_beta,
                latest_corr=latest_corr,
            )

        return results
