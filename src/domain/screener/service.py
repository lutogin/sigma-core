"""
Orchestrator Service.
"""

from src.domain.screener.correlation.service import CorrelationService
from src.domain.screener.hurst_filter import HurstFilterService
from src.domain.screener.volatility_filter import VolatilityFilterService
from src.domain.screener.z_score import ZScoreResult, ZScoreService
from typing import Dict, Optional
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd

from src.domain.data_loader.async_service import AsyncDataLoaderService


class ScreenerService:
    """
    Orchestrates the statistical arbitrage pipeline.
    """

    def __init__(
        self,
        logger,
        exchange_client,
        data_loader: AsyncDataLoaderService,
        correlation_service: CorrelationService,
        z_score_service: ZScoreService,
        volatility_filter_service: VolatilityFilterService,
        hurst_filter_service: HurstFilterService,
        lookback_window_days: int,
        correlation_threshold: float,
        min_beta: float,
        max_beta: float,
        primary_pair: str,
        consistent_pairs: list[str],
        timeframe: str,
    ):
        """
        Initialize Orchestrator.
        """
        self._logger = logger
        self._exchange = exchange_client
        self._data_loader = data_loader
        self._correlation_service = correlation_service
        self._z_score_service = z_score_service
        self._volatility_filter_service = volatility_filter_service
        self._hurst_filter_service = hurst_filter_service
        self._lookback_window_days = lookback_window_days
        self._correlation_threshold = correlation_threshold
        self._min_beta = min_beta
        self._max_beta = max_beta
        self._primary_pair = primary_pair
        self._consistent_pairs = consistent_pairs
        self._timeframe = timeframe

    async def scan(self) -> Optional[Dict[str, ZScoreResult]]:
        """
        Run the full scan pipeline.

        1. Connect to Exchange
        2. Check Market Volatility (safety filter)
        3. Load Raw OHLCV Data
        4. Calculate Correlation and Z-Score
        5. Filter and return results

        Returns:
            Filtered z-score results, or None if market is unsafe/no data.
        """
        self._logger.info("Starting Scan Pipeline...")

        # 1. Connect
        await self._connect_and_fetch_symbols()

        # 2. Check market volatility (ETH as proxy)
        volatility_result = await self._volatility_filter_service.check_from_loader(
            data_loader=self._data_loader,
            lookback_hours=8,
        )
        self._volatility_filter_service.log_status(volatility_result)

        if not volatility_result.is_safe:
            self._logger.warning(f"⛔ TRADING HALTED: {volatility_result.stop_reason}")
            return None

        # 3. Load Raw OHLCV
        raw_data = await self._load_ohlcv_data()

        if not raw_data:
            self._logger.warning("No data loaded. Aborting scan.")
            return None

        correlation_results = self._correlation_service.calculate(
            primary_symbol=self._primary_pair,
            ohlcv=raw_data,
        )

        if not correlation_results:
            self._logger.warning("No correlation results. Aborting scan.")
            return None

        # 4. Calculate Z-Score for spread signals
        z_score_results = self._z_score_service.calculate(
            primary_symbol=self._primary_pair,
            correlation_results=correlation_results,
            ohlcv=raw_data,
        )

        # 5. Filter by correlation quality
        filtered_results = self._filter_by_correlation(z_score_results)

        # 6. Filter by Hurst exponent (mean-reversion quality)
        filtered_results = self._filter_by_hurst(
            filtered_results, raw_data, correlation_results
        )

        # 7. Log results
        self._z_score_service.log_results(filtered_results, sort_by="z_score")

        self._logger.info(
            f"Scan complete. Processed {len(filtered_results)} symbols "
            f"(filtered from {len(z_score_results)})."
        )

        return filtered_results

    def _filter_by_correlation(self, z_score_results: Dict[str, ZScoreResult]) -> Dict:
        """
        Filter z-score results by correlation quality.

        Symbols with correlation < threshold are excluded because they have
        decoupled from the primary pair (own news, hack, pump, etc.).
        Trading stat-arb on decoupled pairs is high risk.

        Args:
            z_score_results: Dictionary mapping symbol -> ZScoreResult.

        Returns:
            Filtered dictionary with only high-correlation symbols.
        """
        filtered = {}
        skipped = []

        for symbol, result in z_score_results.items():
            if (
                result.current_correlation >= self._correlation_threshold
                and result.current_beta >= self._min_beta
                and result.current_beta <= self._max_beta
            ):
                filtered[symbol] = result
            else:
                skipped.append(f"{symbol} (corr={result.current_correlation:.4f})")

        if skipped:
            self._logger.warning(
                f"Skipped {len(skipped)} symbols with low correlation "
                f"(< {self._correlation_threshold}): {', '.join(skipped)}"
            )

        return filtered

    def _filter_by_hurst(
        self,
        z_score_results: Dict[str, ZScoreResult],
        raw_data: Dict[str, pd.DataFrame],
        correlation_results: Dict,
    ) -> Dict[str, ZScoreResult]:
        """
        Filter z-score results by Hurst exponent (mean-reversion quality).

        Only keeps spreads with H < threshold (mean-reverting).
        Spreads with H > 0.5 are trending and unsuitable for stat-arb.

        Args:
            z_score_results: Dictionary mapping symbol -> ZScoreResult.
            raw_data: Dictionary mapping symbol -> OHLCV DataFrame.
            correlation_results: Dictionary with beta values per symbol.

        Returns:
            Filtered dictionary with only mean-reverting spreads.
        """
        if not self._hurst_filter_service:
            return z_score_results

        filtered = {}
        skipped = []

        primary_df = raw_data.get(self._primary_pair)
        if primary_df is None or primary_df.empty:
            self._logger.warning("No primary pair data for Hurst calculation")
            return z_score_results

        for symbol, result in z_score_results.items():
            coin_df = raw_data.get(symbol)
            if coin_df is None or coin_df.empty:
                skipped.append(f"{symbol} (no data)")
                continue

            # Get beta from correlation results
            corr_result = correlation_results.get(symbol)
            if corr_result is None:
                skipped.append(f"{symbol} (no correlation)")
                continue

            beta = corr_result.beta

            # Calculate Hurst for spread (use log prices)
            hurst = self._hurst_filter_service.calculate_for_spread(
                coin_log_prices=coin_df["close"].apply(np.log),
                primary_log_prices=primary_df["close"].apply(np.log),
                beta=beta,
            )

            if hurst is None:
                skipped.append(f"{symbol} (hurst calculation failed)")
                continue

            if self._hurst_filter_service.is_mean_reverting(hurst):
                filtered[symbol] = result
                self._logger.debug(f"{symbol}: Hurst={hurst:.4f} ✓")
            else:
                skipped.append(f"{symbol} (H={hurst:.4f})")

        if skipped:
            self._logger.warning(
                f"Skipped {len(skipped)} symbols with high Hurst exponent "
                f"(trending spreads): {', '.join(skipped)}"
            )

        return filtered

    async def _connect_and_fetch_symbols(self) -> None:
        if not self._exchange.is_connected:
            await self._exchange.connect()

    async def _load_ohlcv_data(self) -> Dict[str, pd.DataFrame]:
        """
        Load OHLCV data for all symbols using optimized bulk loading.

        Uses single DB query to load cached data, then batch fetches
        missing data from exchange. Reindexes all DataFrames to a common
        time index and forward fills missing values.
        """
        end_time = datetime.now(timezone.utc)
        # Load 2x lookback window + buffer for:
        # 1) rolling_beta calculation (needs lookback_window candles)
        # 2) z_score rolling calculation (needs another lookback_window candles)
        data_window_days = self._lookback_window_days * 2 + 1
        start_time = end_time - timedelta(days=data_window_days)

        self._logger.info(
            f"Loading {data_window_days} days of data "
            f"(2x lookback={self._lookback_window_days} + buffer)"
        )

        # Use optimized bulk loading
        raw_data = await self._data_loader.load_ohlcv_bulk(
            symbols=[self._primary_pair] + self._consistent_pairs,
            start_time=start_time,
            end_time=end_time,
            batch_size=10,
            timeframe=self._timeframe,
        )

        if not raw_data:
            return raw_data

        # Build common time index from primary pair
        if self._primary_pair not in raw_data:
            self._logger.warning(
                f"Primary pair {self._primary_pair} not found in data. "
                "Cannot align time index."
            )
            return raw_data

        common_index = raw_data[self._primary_pair].index

        # Reindex all DataFrames to common time index with forward fill
        aligned_data: Dict[str, pd.DataFrame] = {}
        for symbol, df in raw_data.items():
            if symbol == self._primary_pair:
                aligned_data[symbol] = df
            else:
                # Reindex to common time index and forward fill missing values
                aligned_df = df.reindex(common_index, method="ffill")
                # Drop rows that couldn't be filled (NaN at the beginning)
                aligned_df = aligned_df.dropna()
                aligned_data[symbol] = aligned_df

        self._logger.info(
            f"Aligned {len(aligned_data)} symbols to common time index "
            f"({len(common_index)} candles)"
        )

        return aligned_data
