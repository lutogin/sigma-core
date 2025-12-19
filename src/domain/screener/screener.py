"""
Screener Service.

Pure screening and filtering logic without event emission.
Responsible for:
- Loading and aligning OHLCV data
- Running volatility safety checks
- Calculating correlations and z-scores
- Filtering results by correlation, beta, and Hurst
- Logging and returning structured results
"""

from dataclasses import dataclass
from typing import Dict, Optional
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd

from src.domain.screener.correlation.correlation import (
    CorrelationService,
    CorrelationResult,
)
from src.domain.screener.hurst_filter import HurstFilterService
from src.domain.screener.volatility_filter import (
    VolatilityFilterService,
    VolatilityCheckResult,
)
from src.domain.screener.z_score import ZScoreResult, ZScoreService
from src.domain.data_loader.async_data_loader import AsyncDataLoaderService


@dataclass
class ScanResult:
    """Result of a screening scan."""

    filtered_results: Dict[str, ZScoreResult]
    all_z_score_results: Dict[str, ZScoreResult]
    hurst_values: Dict[str, float]
    raw_data: Dict[str, pd.DataFrame]
    correlation_results: Dict[str, CorrelationResult]
    volatility_result: Optional[VolatilityCheckResult]
    symbols_scanned: int
    symbols_after_correlation: int


class ScreenerService:
    """
    Pure screening service for statistical arbitrage.

    This service is responsible for:
    - Loading and aligning OHLCV data
    - Running volatility safety checks
    - Calculating correlations, z-scores, and Hurst exponents
    - Filtering results by quality metrics
    - Logging results

    Does NOT emit events - that's the Orchestrator's responsibility.
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
        Initialize Screener Service.
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

    # =========================================================================
    # Public Properties (expose Z-score thresholds for Orchestrator)
    # =========================================================================

    @property
    def z_entry_threshold(self) -> float:
        """Get Z-score entry threshold."""
        return self._z_score_service.z_entry_threshold

    @property
    def z_tp_threshold(self) -> float:
        """Get Z-score take-profit threshold."""
        return self._z_score_service.z_tp_threshold

    @property
    def z_sl_threshold(self) -> float:
        """Get Z-score stop-loss threshold."""
        return self._z_score_service.z_sl_threshold

    @property
    def primary_pair(self) -> str:
        """Get primary trading pair."""
        return self._primary_pair

    @property
    def correlation_threshold(self) -> float:
        """Get correlation threshold for pair filtering."""
        return self._correlation_threshold

    # =========================================================================
    # Main Scan Method
    # =========================================================================

    async def scan(self) -> Optional[ScanResult]:
        """
        Run the full scan pipeline.

        1. Connect to Exchange
        2. Check Market Volatility (safety filter)
        3. Load Raw OHLCV Data
        4. Calculate Correlation and Z-Score
        5. Filter and return results

        Returns:
            ScanResult with filtered results and metrics, or None if no data.
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
            self._logger.warning(f"⛔ Market unsafe: {volatility_result.stop_reason}")
            # Return result with volatility info so orchestrator can emit event
            return ScanResult(
                filtered_results={},
                all_z_score_results={},
                hurst_values={},
                raw_data={},
                correlation_results={},
                volatility_result=volatility_result,
                symbols_scanned=0,
                symbols_after_correlation=0,
            )

        # 3. Load Raw OHLCV
        raw_data = await self._load_ohlcv_data()

        if not raw_data:
            self._logger.warning("No data loaded. Aborting scan.")
            return None

        # 4. Calculate Correlation
        correlation_results = self._correlation_service.calculate(
            primary_symbol=self._primary_pair,
            ohlcv=raw_data,
        )

        if not correlation_results:
            self._logger.warning("No correlation results. Aborting scan.")
            return None

        # 5. Calculate Z-Score for spread signals
        z_score_results = self._z_score_service.calculate(
            primary_symbol=self._primary_pair,
            correlation_results=correlation_results,
            ohlcv=raw_data,
        )

        symbols_scanned = len(z_score_results)

        # 6. Filter by correlation quality
        filtered_results = self._filter_by_correlation(z_score_results)
        symbols_after_corr = len(filtered_results)

        # 7. Filter by Hurst exponent (ONLY for entry candidates with valid Z-score signal)
        filtered_results, hurst_values = self._filter_by_hurst(
            filtered_results, raw_data, correlation_results
        )

        # 8. Log results
        self._log_results(
            filtered_results,
            sort_by="z_score",
            hurst_values=hurst_values,
        )

        self._logger.info(
            f"Scan complete. Processed {len(filtered_results)} symbols "
            f"(filtered from {symbols_scanned})."
        )

        return ScanResult(
            filtered_results=filtered_results,
            all_z_score_results=z_score_results,
            hurst_values=hurst_values,
            raw_data=raw_data,
            correlation_results=correlation_results,
            volatility_result=volatility_result,
            symbols_scanned=symbols_scanned,
            symbols_after_correlation=symbols_after_corr,
        )

    # =========================================================================
    # Private Methods - Filtering
    # =========================================================================

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
    ) -> tuple[Dict[str, ZScoreResult], Dict[str, float]]:
        """
        Filter z-score results by Hurst exponent (mean-reversion quality).

        ONLY checks Hurst for entry candidates:
        - |Z| >= entry_threshold AND |Z| <= sl_threshold

        Args:
            z_score_results: Dictionary mapping symbol -> ZScoreResult.
            raw_data: Dictionary mapping symbol -> OHLCV DataFrame.
            correlation_results: Dictionary with beta values per symbol.

        Returns:
            Tuple of:
            - Filtered dictionary with only mean-reverting spreads
            - Hurst values dict for all checked symbols (for logging)
        """
        hurst_values: Dict[str, float] = {}  # symbol -> hurst (for logging)

        if not self._hurst_filter_service:
            return z_score_results, hurst_values

        # Get Z-score thresholds from z_score_service
        z_entry = self._z_score_service.z_entry_threshold
        z_sl = self._z_score_service.z_sl_threshold

        filtered = {}
        skipped = []

        primary_df = raw_data.get(self._primary_pair)
        if primary_df is None or primary_df.empty:
            self._logger.warning("No primary pair data for Hurst calculation")
            return z_score_results, hurst_values

        for symbol, result in z_score_results.items():
            z = result.current_z_score

            # Check if this is an entry candidate
            # Entry candidate: |Z| >= entry_threshold AND |Z| <= sl_threshold
            is_entry_candidate = (
                not np.isnan(z) and abs(z) >= z_entry and abs(z) <= z_sl
            )

            if not is_entry_candidate:
                # Not an entry candidate - keep in results without Hurst check
                filtered[symbol] = result
                continue

            # This is an entry candidate - check Hurst
            coin_df = raw_data.get(symbol)
            if coin_df is None or coin_df.empty:
                skipped.append(f"{symbol} (no data)")
                continue

            # Get beta from correlation results
            corr_result = correlation_results.get(symbol)
            if corr_result is None:
                skipped.append(f"{symbol} (no correlation)")
                continue

            beta = corr_result.latest_beta

            # Calculate Hurst for spread (use log prices)
            hurst = self._hurst_filter_service.calculate_for_spread(
                coin_log_prices=coin_df["close"].apply(np.log),
                primary_log_prices=primary_df["close"].apply(np.log),
                beta=beta,
            )

            hurst_values[symbol] = hurst  # Store for logging

            if hurst is None:
                skipped.append(f"{symbol} (hurst calculation failed)")
                continue

            if self._hurst_filter_service.is_mean_reverting(hurst):
                filtered[symbol] = result
                self._logger.info(
                    f"✅ {symbol}: Z={z:.2f}, Hurst={hurst:.4f} < {self._hurst_filter_service.threshold} (mean-reverting)"
                )
            else:
                skipped.append(f"{symbol} (Z={z:.2f}, H={hurst:.4f})")
                self._logger.warning(
                    f"⚠️ {symbol}: Z={z:.2f}, Hurst={hurst:.4f} >= {self._hurst_filter_service.threshold} (trending, skip)"
                )

        if skipped:
            self._logger.warning(
                f"Skipped {len(skipped)} entry candidates with high Hurst: {', '.join(skipped)}"
            )

        return filtered, hurst_values

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

    def _log_results(
        self,
        results: Dict[str, ZScoreResult],
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
        formatted = self._format_results(
            results, sort_by=sort_by, top_n=top_n, hurst_values=hurst_values
        )
        self._logger.info(f"Z-Score Results:{formatted}")

    def _format_results(
        self,
        results: Dict[str, ZScoreResult],
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
        z_entry = self._z_score_service.z_entry_threshold
        z_sl = self._z_score_service.z_sl_threshold

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
            elif z >= z_entry and z <= z_sl:
                signal = "🔴 SHORT"
            elif z <= -z_entry and z >= -z_sl:
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
