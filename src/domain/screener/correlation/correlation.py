"""Rolling cointegration regression and return-correlation service."""

from dataclasses import dataclass, field
from typing import Dict

import numpy as np
import pandas as pd

from src.domain.utils import calculate_lookback_window


@dataclass
class CorrelationResult:
    """Rolling pair-model estimates for one symbol."""

    symbol: str
    rolling_beta: pd.Series
    rolling_corr: pd.Series
    latest_beta: float
    latest_corr: float
    rolling_intercept: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))
    rolling_residual_std: pd.Series = field(
        default_factory=lambda: pd.Series(dtype=float)
    )
    latest_intercept: float = np.nan
    latest_residual_std: float = np.nan


class CorrelationService:
    """Estimate a consistent log-price hedge model and return correlation.

    The hedge ratio, intercept and residual volatility come from one rolling
    OLS regression on log-price levels:

        log(COIN) = intercept + beta * log(PRIMARY) + residual

    Return correlation remains a separate regime/safety filter. Estimating
    beta on returns and applying it to price levels would mix two different
    models and produce a spread unrelated to the stationarity tests.
    """

    def __init__(
        self,
        logger,
        lookback_window_days: int,
        timeframe: str = "15m",
    ):
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
        """Calculate point-in-time rolling estimates relative to primary."""
        log_prices = self._preprocess_log_prices(ohlcv)
        if primary_symbol not in log_prices.columns:
            self._logger.error(f"Primary symbol {primary_symbol} not found in data")
            return {}

        return self._calculate_rolling_metrics(
            log_prices=log_prices,
            log_returns=log_prices.diff(),
            primary_symbol=primary_symbol,
        )

    def _preprocess_log_prices(
        self,
        ohlcv: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """Extract positive closes without truncating older pair histories."""
        close_prices = {}
        for symbol, df in ohlcv.items():
            if df.empty:
                self._logger.warning(f"Empty DataFrame for {symbol}, skipping")
                continue

            close_col = next(
                (column for column in ("close", "Close", "CLOSE") if column in df),
                None,
            )
            if close_col is None:
                self._logger.warning(f"No close column found for {symbol}, skipping")
                continue

            prices = pd.to_numeric(df[close_col], errors="coerce")
            close_prices[symbol] = prices.where(prices > 0)

        if not close_prices:
            self._logger.error("No valid close prices found")
            return pd.DataFrame()

        log_prices = np.log(pd.DataFrame(close_prices))
        self._logger.debug(
            f"[Correlation] Preprocessed {len(log_prices.columns)} symbols, "
            f"{len(log_prices)} timestamps"
        )
        return log_prices

    def _calculate_rolling_metrics(
        self,
        log_prices: pd.DataFrame,
        log_returns: pd.DataFrame,
        primary_symbol: str,
    ) -> Dict[str, CorrelationResult]:
        """Calculate rolling OLS parameters and return correlation."""
        results: Dict[str, CorrelationResult] = {}
        window = self._lookback_window

        for symbol in log_prices.columns:
            if symbol == primary_symbol:
                continue

            pair_prices = pd.concat(
                [
                    log_prices[symbol].rename("coin"),
                    log_prices[primary_symbol].rename("primary"),
                ],
                axis=1,
                join="inner",
            ).dropna()
            pair_returns = pd.concat(
                [
                    log_returns[symbol].rename("coin"),
                    log_returns[primary_symbol].rename("primary"),
                ],
                axis=1,
                join="inner",
            ).dropna()
            if len(pair_prices) < window or len(pair_returns) < window:
                continue

            coin_prices = pair_prices["coin"]
            primary_prices = pair_prices["primary"]
            rolling_cov = coin_prices.rolling(window, min_periods=window).cov(
                primary_prices
            )
            rolling_var_primary = primary_prices.rolling(
                window, min_periods=window
            ).var()
            rolling_beta = rolling_cov / rolling_var_primary.replace(0, np.nan)

            rolling_intercept = (
                coin_prices.rolling(window, min_periods=window).mean()
                - rolling_beta
                * primary_prices.rolling(window, min_periods=window).mean()
            )

            rolling_var_coin = coin_prices.rolling(window, min_periods=window).var()
            residual_variance = rolling_var_coin - rolling_beta * rolling_cov
            if window > 2:
                residual_variance *= (window - 1) / (window - 2)
            rolling_residual_std = np.sqrt(residual_variance.clip(lower=0))

            rolling_corr = (
                pair_returns["coin"]
                .rolling(window, min_periods=window)
                .corr(pair_returns["primary"])
            )

            latest_beta = self._latest(rolling_beta)
            latest_corr = self._latest(rolling_corr)
            latest_intercept = self._latest(rolling_intercept)
            latest_residual_std = self._latest(rolling_residual_std.replace(0, np.nan))

            self._logger.debug(
                f"[Correlation] {symbol}: beta={latest_beta:.4f}, "
                f"intercept={latest_intercept:.4f}, "
                f"residual_std={latest_residual_std:.6f}, "
                f"return_corr={latest_corr:.4f}"
            )
            results[symbol] = CorrelationResult(
                symbol=symbol,
                rolling_beta=rolling_beta,
                rolling_corr=rolling_corr,
                latest_beta=latest_beta,
                latest_corr=latest_corr,
                rolling_intercept=rolling_intercept,
                rolling_residual_std=rolling_residual_std,
                latest_intercept=latest_intercept,
                latest_residual_std=latest_residual_std,
            )

        return results

    @staticmethod
    def _latest(series: pd.Series) -> float:
        valid = series.replace([np.inf, -np.inf], np.nan).dropna()
        return float(valid.iloc[-1]) if not valid.empty else np.nan
