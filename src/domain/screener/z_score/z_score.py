"""
Z-Score Service.

Calculates spread (synthetic asset) and z-score for statistical arbitrage.
The spread represents the "clean" movement of a coin, isolated from
the primary symbol's (e.g., ETH) market influence.

Formula:
    Spread = LogPrice_COIN - β × LogPrice_PRIMARY
    Z-Score = (Spread - Mean(Spread)) / Std(Spread)
"""

from dataclasses import dataclass
from typing import Dict

import numpy as np
import pandas as pd

from src.domain.screener.correlation.correlation import CorrelationResult
from src.domain.utils import calculate_lookback_window


@dataclass
class ZScoreResult:
    """Result of z-score calculation for a single symbol."""

    symbol: str
    spread_series: pd.Series
    z_score_series: pd.Series
    current_spread: float
    current_z_score: float
    current_beta: float
    current_correlation: float
    dynamic_entry_threshold: float  # Adaptive threshold based on 97th percentile


class ZScoreService:
    """
    Service for calculating spread and z-score metrics.

    The spread is the synthetic asset that represents "clean" price movement
    of a coin, with the primary symbol's influence removed using beta hedge.

    Z-score measures how many standard deviations the current spread
    is from its rolling mean - used for mean-reversion signals.
    """

    def __init__(
        self,
        logger,
        lookback_window_days: int,
        timeframe: str = "15m",
        z_entry_threshold: float = 2.0,
        z_tp_threshold: float = 0.0,
        z_sl_threshold: float = 4.5,
        adaptive_percentile: int = 97,
        dynamic_threshold_window: int = 576,
    ):
        """
        Initialize ZScoreService.

        Args:
            logger: Application logger.
            lookback_window_days: Number of days for lookback window.
            timeframe: Candle timeframe (e.g., "15m", "1h").
            z_entry_threshold: Minimum Z-score threshold for entry (floor for adaptive).
            z_tp_threshold: Z-score threshold for take profit.
            z_sl_threshold: Z-score threshold for stop loss.
            adaptive_percentile: Percentile for adaptive threshold calculation (default 97).
            dynamic_threshold_window: Number of candles for dynamic threshold calculation (default 440 ~4.5 days @ 15m).
        """
        self._logger = logger
        self._lookback_window_days = lookback_window_days
        self._timeframe = timeframe
        self._lookback_window = calculate_lookback_window(
            lookback_window_days, timeframe
        )
        self._z_entry_threshold = z_entry_threshold
        self._z_tp_threshold = z_tp_threshold
        self._z_sl_threshold = z_sl_threshold
        self._adaptive_percentile = adaptive_percentile
        self._dynamic_threshold_window = dynamic_threshold_window

    @property
    def z_entry_threshold(self) -> float:
        """Get Z-score entry threshold."""
        return self._z_entry_threshold

    @property
    def z_tp_threshold(self) -> float:
        """Get Z-score take-profit threshold."""
        return self._z_tp_threshold

    @property
    def z_sl_threshold(self) -> float:
        """Get Z-score stop-loss threshold."""
        return self._z_sl_threshold

    def calculate(
        self,
        primary_symbol: str,
        correlation_results: Dict[str, CorrelationResult],
        ohlcv: Dict[str, pd.DataFrame],
    ) -> Dict[str, ZScoreResult]:
        """
        Calculate spread and z-score for all symbols.

        The spread is calculated as:
            Spread = LogPrice_COIN - β × LogPrice_PRIMARY

        Where β (beta) is the rolling hedge coefficient from correlation results.

        Args:
            primary_symbol: Primary symbol (e.g., "ETH/USDT:USDT").
            correlation_results: Results from CorrelationService.calculate().
            ohlcv: Dictionary mapping symbol -> OHLCV DataFrame with 'close' column.

        Returns:
            Dictionary mapping symbol -> ZScoreResult.
        """
        # self._logger.debug(
        #     f"Calculating z-score for {len(correlation_results)} symbols "
        #     f"(window={self._lookback_window} candles)"
        # )

        # Step 1: Extract log prices from OHLCV data
        log_prices = self._extract_log_prices(ohlcv)

        if primary_symbol not in log_prices.columns:
            self._logger.error(
                f"Primary symbol {primary_symbol} not found in OHLCV data"
            )
            return {}

        primary_log_price = log_prices[primary_symbol]

        # Step 2: Calculate spread and z-score for each symbol
        results: Dict[str, ZScoreResult] = {}

        for symbol, corr_result in correlation_results.items():
            if symbol not in log_prices.columns:
                self._logger.warning(
                    f"Symbol {symbol} not found in OHLCV data, skipping"
                )
                continue

            coin_log_price = log_prices[symbol]
            rolling_beta = corr_result.rolling_beta

            # Align rolling_beta to log_prices index
            # rolling_beta is calculated on log_returns which has 1 fewer point
            # Reindex to log_prices index, forward-fill the first value
            rolling_beta_aligned = rolling_beta.reindex(
                coin_log_price.index, method="ffill"
            )

            # Calculate spread: LogPrice_COIN - β × LogPrice_PRIMARY
            spread_series = coin_log_price - (rolling_beta_aligned * primary_log_price)

            # Calculate z-score for the spread
            mean_spread = spread_series.rolling(window=self._lookback_window).mean()
            std_spread = spread_series.rolling(window=self._lookback_window).std()
            z_score_series = (spread_series - mean_spread) / std_spread

            # Calculate dynamic entry threshold based on historical z-score distribution
            dynamic_threshold = self._calculate_dynamic_threshold(z_score_series)

            # Get current (latest) values
            current_spread = self._get_latest_value(spread_series)
            current_z_score = self._get_latest_value(z_score_series)
            current_beta = corr_result.latest_beta
            current_correlation = corr_result.latest_corr

            results[symbol] = ZScoreResult(
                symbol=symbol,
                spread_series=spread_series,
                z_score_series=z_score_series,
                current_spread=current_spread,
                current_z_score=current_z_score,
                current_beta=current_beta,
                current_correlation=current_correlation,
                dynamic_entry_threshold=dynamic_threshold,
            )

            self._logger.debug(
                f"{symbol}: z-score={current_z_score:.4f}, "
                f"beta={current_beta:.4f}, corr={current_correlation:.4f}, "
                f"dyn_threshold={dynamic_threshold:.2f}"
            )

        return results

    def _extract_log_prices(
        self,
        ohlcv: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """
        Extract log prices from OHLCV data.

        Args:
            ohlcv: Dictionary mapping symbol -> OHLCV DataFrame.

        Returns:
            DataFrame with log prices, columns are symbols, index is time.
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

        # Build DataFrame with close prices and calculate log prices
        df_close = pd.DataFrame(close_prices)
        log_prices = pd.DataFrame(
            np.log(df_close), columns=df_close.columns, index=df_close.index
        )

        return log_prices

    @staticmethod
    def _get_latest_value(series: pd.Series) -> float:
        """
        Get the latest non-NaN value from a series.

        Args:
            series: Pandas Series.

        Returns:
            Latest valid value or NaN if none exists.
        """
        valid = series.dropna()
        return valid.iloc[-1] if len(valid) > 0 else np.nan

    def _calculate_dynamic_threshold(self, z_score_series: pd.Series) -> float:
        """
        Calculate dynamic entry threshold based on historical Z-score distribution.

        Algorithm "Adaptive Upper Bound":
        - Take absolute Z-scores from the dynamic threshold window (440 candles ~4.5 days)
        - Calculate the 97th percentile (value exceeded only 3% of the time)
        - Return max(min_threshold, percentile_97)

        This ensures we only enter when Z-score is truly extreme for THIS pair,
        not just above a static threshold that may be too low for volatile pairs.

        Args:
            z_score_series: Historical Z-score series.

        Returns:
            Dynamic threshold = max(z_entry_threshold, percentile_97)
        """
        # Use the dynamic threshold window (576 candles by default)
        recent_z = z_score_series.tail(self._dynamic_threshold_window).dropna()

        if len(recent_z) < 50:
            self._logger.warning(
                f"Insufficient data for dynamic threshold calculation, "
                f"using static threshold: {self._z_entry_threshold}"
            )
            # Not enough data, use static threshold
            return self._z_entry_threshold

        # Calculate percentile of absolute Z-scores
        abs_z = recent_z.abs()
        hist_threshold = float(np.percentile(abs_z, self._adaptive_percentile))

        # Floor at minimum threshold (statistical significance)
        dynamic_threshold = max(self._z_entry_threshold, hist_threshold)

        return dynamic_threshold

    def format_results(
        self,
        results: Dict[str, "ZScoreResult"],
        sort_by: str = "z_score",
        top_n: int | None = None,
        hurst_values: Dict[str, float] | None = None,
    ) -> str:
        """
        Format z-score results as a pretty table for logging.

        Args:
            results: Dictionary mapping symbol -> ZScoreResult.
            sort_by: Sort key - "z_score", "beta", "correlation", or "symbol".
            top_n: Limit output to top N results (by absolute z-score). None for all.
            hurst_values: Optional dict of symbol -> Hurst exponent (only for entry candidates).

        Returns:
            Formatted string with table of results.
        """
        if not results:
            return "No z-score results to display."

        hurst_values = hurst_values or {}

        # Build list of tuples for sorting
        data = []
        for symbol, res in results.items():
            data.append(
                {
                    "symbol": symbol,
                    "z_score": res.current_z_score,
                    "beta": res.current_beta,
                    "correlation": res.current_correlation,
                    "spread": res.current_spread,
                    "hurst": hurst_values.get(symbol),  # None if not calculated
                }
            )

        # Sort
        if sort_by == "z_score":
            data.sort(
                key=lambda x: abs(x["z_score"]) if not np.isnan(x["z_score"]) else 0,
                reverse=True,
            )
        elif sort_by == "beta":
            data.sort(
                key=lambda x: abs(x["beta"]) if not np.isnan(x["beta"]) else 0,
                reverse=True,
            )
        elif sort_by == "correlation":
            data.sort(
                key=lambda x: x["correlation"] if not np.isnan(x["correlation"]) else 0,
                reverse=True,
            )
        else:
            data.sort(key=lambda x: x["symbol"])

        # Limit results
        if top_n is not None:
            data = data[:top_n]

        # Build table
        lines = []
        header = f"{'Symbol':<20} {'Z-Score':>10} {'β (hedge)':>12} {'Corr':>8} {'Hurst':>8} {'Signal':>12}"
        separator = "-" * len(header)

        lines.append("")
        lines.append(separator)
        lines.append(header)
        lines.append(separator)

        for row in data:
            z = row["z_score"]
            beta = row["beta"]
            corr = row["correlation"]
            hurst = row["hurst"]

            # Determine signal based on z-score
            if np.isnan(z):
                signal = "N/A"
            elif z >= self._z_entry_threshold and z <= self._z_sl_threshold:
                signal = "🔴 SHORT"
            elif z <= -self._z_entry_threshold and z >= -self._z_sl_threshold:
                signal = "🟢 LONG"
            elif abs(z) >= 1.5:
                signal = "⚠️ WATCH"
            else:
                signal = "—"

            z_str = f"{z:>10.4f}" if not np.isnan(z) else f"{'N/A':>10}"
            beta_str = f"{beta:>12.4f}" if not np.isnan(beta) else f"{'N/A':>12}"
            corr_str = f"{corr:>8.4f}" if not np.isnan(corr) else f"{'N/A':>8}"
            # Hurst: show value only for entry candidates, "-" otherwise
            hurst_str = f"{hurst:>8.4f}" if hurst is not None else f"{'—':>8}"

            lines.append(
                f"{row['symbol']:<20} {z_str} {beta_str} {corr_str} {hurst_str} {signal:>12}"
            )

        lines.append(separator)
        lines.append(f"Total: {len(data)} symbols")
        lines.append("")

        return "\n".join(lines)

    def log_results(
        self,
        results: Dict[str, "ZScoreResult"],
        sort_by: str = "z_score",
        top_n: int | None = None,
        hurst_values: Dict[str, float] | None = None,
    ) -> None:
        """
        Log z-score results as a formatted table.

        Args:
            results: Dictionary mapping symbol -> ZScoreResult.
            sort_by: Sort key - "z_score", "beta", "correlation", or "symbol".
            top_n: Limit output to top N results. None for all.
            hurst_values: Optional dict of symbol -> Hurst exponent.
        """
        formatted = self.format_results(
            results, sort_by=sort_by, top_n=top_n, hurst_values=hurst_values
        )
        self._logger.info(f"Z-Score Results:{formatted}")
