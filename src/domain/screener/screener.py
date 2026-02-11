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

import asyncio
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
from src.domain.screener.adf_filter import ADFFilterService
from src.domain.screener.halflife_filter import HalfLifeFilterService
from src.domain.screener.volatility_filter import (
    VolatilityFilterService,
    VolatilityCheckResult,
)
from src.domain.screener.z_score import ZScoreResult, ZScoreService
from src.domain.data_loader.async_data_loader import AsyncDataLoaderService
from src.domain.trading_pairs import TradingPairRepository
from src.domain.utils import get_timeframe_minutes


@dataclass
class ScanResult:
    """Result of a screening scan."""

    filtered_results: Dict[str, ZScoreResult]
    all_z_score_results: Dict[str, ZScoreResult]
    hurst_values: Dict[str, float]
    adf_pvalues: Dict[str, float]
    halflife_values: Dict[str, float]
    raw_data: Dict[str, pd.DataFrame]
    correlation_results: Dict[str, CorrelationResult]
    volatility_result: Optional[VolatilityCheckResult]
    symbols_scanned: int
    symbols_after_correlation: int


@dataclass
class LastScanState:
    """
    Internal state holding the last scan results (raw data only).

    Formatting is done by CommunicatorService to maintain SOLID principles.
    """

    scan_result: Optional[ScanResult] = None
    scan_time: Optional[datetime] = None
    hurst_values: Optional[Dict[str, float]] = None
    adf_pvalues: Optional[Dict[str, float]] = None
    halflife_values: Optional[Dict[str, float]] = None
    dynamic_thresholds: Optional[Dict[str, float]] = None  # symbol -> dynamic_entry_threshold

    def is_empty(self) -> bool:
        """Check if state is empty (no scan performed yet)."""
        return self.scan_result is None

    def get_age_seconds(self) -> float:
        """Get age of scan in seconds."""
        if self.scan_time is None:
            return float('inf')
        return (datetime.now(timezone.utc) - self.scan_time).total_seconds()


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
        adf_filter_service: Optional[ADFFilterService],
        halflife_filter_service: Optional[HalfLifeFilterService],
        lookback_window_days: int,
        correlation_threshold: float,
        min_beta: float,
        max_beta: float,
        primary_pair: str,
        consistent_pairs: list[str],
        timeframe: str,
        trading_pair_repository: Optional[TradingPairRepository] = None,
        enable_beta_drift_guard: bool = True,
        beta_drift_short_days: int = 3,
        beta_drift_long_days: int = 9,
        beta_drift_max_relative: float = 0.35,
        enable_stability_filter: bool = True,
        stability_windows_days: Optional[list[int]] = None,
        stability_min_pass_windows: int = 2,
    ):
        """
        Initialize Screener Service.

        Args:
            trading_pair_repository: Optional repository for dynamic pair loading.
                                     If provided, pairs are loaded from MongoDB.
                                     Falls back to consistent_pairs if not provided
                                     or if repository returns empty list.
        """
        self._logger = logger
        self._exchange = exchange_client
        self._data_loader = data_loader
        self._correlation_service = correlation_service
        self._z_score_service = z_score_service
        self._volatility_filter_service = volatility_filter_service
        self._hurst_filter_service = hurst_filter_service
        self._adf_filter_service = adf_filter_service
        self._halflife_filter_service = halflife_filter_service
        self._lookback_window_days = lookback_window_days
        self._correlation_threshold = correlation_threshold
        self._min_beta = min_beta
        self._max_beta = max_beta
        self._primary_pair = primary_pair
        self._consistent_pairs = consistent_pairs
        self._timeframe = timeframe
        self._trading_pair_repository = trading_pair_repository
        self._bars_per_day = max(1, 24 * 60 // get_timeframe_minutes(timeframe))

        # Beta drift guard (regime stability)
        self._enable_beta_drift_guard = enable_beta_drift_guard
        self._beta_drift_short_days = max(1, beta_drift_short_days)
        self._beta_drift_long_days = max(
            self._beta_drift_short_days + 1, beta_drift_long_days
        )
        self._beta_drift_max_relative = max(0.0, beta_drift_max_relative)

        # Stability-over-time windows
        self._enable_stability_filter = enable_stability_filter
        windows = stability_windows_days or [3, 6, 9]
        self._stability_windows_days = sorted({int(w) for w in windows if int(w) > 0})
        if not self._stability_windows_days:
            self._stability_windows_days = [3, 6, 9]
        self._stability_min_pass_windows = max(1, stability_min_pass_windows)

        # Internal state for last scan results
        self._last_scan_state = LastScanState()

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

    @property
    def last_scan_state(self) -> LastScanState:
        """Get last scan state (raw data)."""
        return self._last_scan_state

    @property
    def z_score_service(self) -> ZScoreService:
        """Get Z-score service for threshold access."""
        return self._z_score_service

    @property
    def adf_threshold(self) -> Optional[float]:
        """Get ADF p-value threshold for stationarity."""
        if self._adf_filter_service and self._adf_filter_service.is_available:
            return self._adf_filter_service.threshold
        return None

    @property
    def halflife_threshold(self) -> Optional[float]:
        """Get half-life threshold (max bars for mean reversion)."""
        if self._halflife_filter_service and self._halflife_filter_service.is_available:
            return self._halflife_filter_service.threshold
        return None

    @property
    def hurst_threshold(self) -> Optional[float]:
        """Get Hurst exponent threshold for mean reversion."""
        if self._hurst_filter_service:
            return self._hurst_filter_service.threshold
        return None

    @property
    def adf_filter_service(self) -> Optional[ADFFilterService]:
        """Expose ADF filter service for orchestrator-level re-checks."""
        return self._adf_filter_service

    @property
    def halflife_filter_service(self) -> Optional[HalfLifeFilterService]:
        """Expose Half-Life filter service for orchestrator-level re-checks."""
        return self._halflife_filter_service

    # =========================================================================
    # Trading Pairs
    # =========================================================================

    def _get_trading_pairs(self) -> list[str]:
        """
        Get list of trading pairs to scan.

        Priority:
        1. Trading pair repository (MongoDB) if available and has data
        2. Fallback to consistent_pairs from config

        Returns:
            List of trading pair symbols.
        """
        if self._trading_pair_repository is not None:
            try:
                pairs = self._trading_pair_repository.get_active_symbols()
                if pairs:
                    self._logger.debug(
                        f"Loaded {len(pairs)} trading pairs from MongoDB"
                    )
                    return pairs
                else:
                    self._logger.warning(
                        "No active trading pairs in MongoDB, using config fallback"
                    )
            except Exception as e:
                self._logger.error(
                    f"Failed to load trading pairs from MongoDB: {e}, using config fallback"
                )

        # Fallback to config
        return self._consistent_pairs

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
                adf_pvalues={},
                halflife_values={},
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

        # 4. Calculate Correlation (CPU-bound, run in thread pool to not block event loop)
        correlation_results = await asyncio.get_event_loop().run_in_executor(
            None,  # Use default ThreadPoolExecutor
            lambda: self._correlation_service.calculate(
                primary_symbol=self._primary_pair,
                ohlcv=raw_data,
            )
        )

        if not correlation_results:
            self._logger.warning("No correlation results. Aborting scan.")
            return None

        # 5. Calculate Z-Score for spread signals (CPU-bound, run in thread pool)
        z_score_results = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self._z_score_service.calculate(
                primary_symbol=self._primary_pair,
                correlation_results=correlation_results,
                ohlcv=raw_data,
            )
        )

        symbols_scanned = len(z_score_results)

        # 6. Filter by correlation quality (+ optional beta drift guard)
        filtered_results = self._filter_by_correlation(
            z_score_results, correlation_results
        )
        symbols_after_corr = len(filtered_results)

        # 7. Stability-over-time filter across multiple windows
        filtered_results, _ = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self._filter_by_stability(
                filtered_results, raw_data
            )
        )

        # 8. Filter by Hurst exponent (CPU-bound, run in thread pool)
        filtered_results, hurst_values = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self._filter_by_hurst(
                filtered_results, raw_data, correlation_results
            )
        )

        # 9. Filter by Half-Life mean-reversion speed (CPU-bound, run in thread pool)
        filtered_results, halflife_values = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self._filter_by_halflife(
                filtered_results, raw_data, correlation_results, hurst_values
            )
        )

        # 10. Filter by ADF stationarity (CPU-bound, run in thread pool)
        filtered_results, adf_pvalues = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self._filter_by_adf(
                filtered_results, raw_data, correlation_results, halflife_values
            )
        )

        # 11. Log results
        formatted_table = self._format_results(
            filtered_results,
            sort_by="z_score",
            hurst_values=hurst_values,
            adf_pvalues=adf_pvalues,
            halflife_values=halflife_values,
        )
        self._logger.info(f"Z-Score Results:{formatted_table}")

        self._logger.info(
            f"Scan complete. Processed {len(filtered_results)} symbols "
            f"(filtered from {symbols_scanned})."
        )

        scan_result = ScanResult(
            filtered_results=filtered_results,
            all_z_score_results=z_score_results,
            hurst_values=hurst_values,
            adf_pvalues=adf_pvalues,
            halflife_values=halflife_values,
            raw_data=raw_data,
            correlation_results=correlation_results,
            volatility_result=volatility_result,
            symbols_scanned=symbols_scanned,
            symbols_after_correlation=symbols_after_corr,
        )

        # Extract dynamic thresholds from z_score_results
        dynamic_thresholds = {
            symbol: result.dynamic_entry_threshold
            for symbol, result in z_score_results.items()
        }

        # Store last scan state (raw data only, formatting done by CommunicatorService)
        self._last_scan_state = LastScanState(
            scan_result=scan_result,
            scan_time=datetime.now(timezone.utc),
            hurst_values=hurst_values,
            adf_pvalues=adf_pvalues,
            halflife_values=halflife_values,
            dynamic_thresholds=dynamic_thresholds,
        )

        return scan_result

    # =========================================================================
    # Private Methods - Filtering
    # =========================================================================

    def _evaluate_beta_drift(
        self, correlation_result: CorrelationResult
    ) -> tuple[bool, Optional[float], Optional[float], Optional[float]]:
        """
        Evaluate beta drift between short and long windows.

        Returns:
            (is_stable, short_beta, long_beta, relative_drift)
        """
        if not self._enable_beta_drift_guard:
            return True, None, None, None

        beta_series = correlation_result.rolling_beta.dropna()
        short_bars = self._beta_drift_short_days * self._bars_per_day
        long_bars = self._beta_drift_long_days * self._bars_per_day

        if len(beta_series) < long_bars:
            # Not enough history yet - don't block signal.
            return True, None, None, None

        short_beta = float(beta_series.tail(short_bars).median())
        long_beta = float(beta_series.tail(long_bars).median())

        if not np.isfinite(short_beta) or not np.isfinite(long_beta):
            return True, short_beta, long_beta, None

        denom = max(abs(long_beta), 1e-6)
        relative_drift = abs(short_beta - long_beta) / denom
        is_stable = relative_drift <= self._beta_drift_max_relative
        return is_stable, short_beta, long_beta, relative_drift

    def _estimate_window_beta(
        self,
        coin_log_prices: pd.Series,
        primary_log_prices: pd.Series,
    ) -> Optional[float]:
        """Estimate beta on a fixed window using log-return covariance."""
        returns = pd.concat(
            [coin_log_prices.diff(), primary_log_prices.diff()],
            axis=1,
            keys=["coin", "primary"],
        ).dropna()

        if len(returns) < 30:
            return None

        var_primary = returns["primary"].var()
        if var_primary is None or not np.isfinite(var_primary) or var_primary <= 0:
            return None

        cov = returns["coin"].cov(returns["primary"])
        if cov is None or not np.isfinite(cov):
            return None

        beta = float(cov / var_primary)
        if not np.isfinite(beta):
            return None
        return beta

    def _filter_by_stability(
        self,
        z_score_results: Dict[str, ZScoreResult],
        raw_data: Dict[str, pd.DataFrame],
    ) -> tuple[Dict[str, ZScoreResult], Dict[str, int]]:
        """
        Stability-over-time filter across multiple windows.

        A symbol passes when at least N windows pass:
        - beta in configured range
        - Half-Life <= threshold
        - ADF p-value < threshold
        """
        stability_passes: Dict[str, int] = {}

        if not self._enable_stability_filter:
            return z_score_results, stability_passes

        if (
            self._adf_filter_service is None
            or not self._adf_filter_service.is_available
            or self._halflife_filter_service is None
            or not self._halflife_filter_service.is_available
        ):
            return z_score_results, stability_passes

        filtered: Dict[str, ZScoreResult] = {}
        skipped: list[str] = []
        z_sl = self._z_score_service.z_sl_threshold
        adf_threshold = self._adf_filter_service.threshold
        halflife_threshold = self._halflife_filter_service.threshold

        primary_df = raw_data.get(self._primary_pair)
        if primary_df is None or primary_df.empty:
            return z_score_results, stability_passes

        for symbol, result in z_score_results.items():
            z = result.current_z_score
            z_entry = result.dynamic_entry_threshold
            is_entry_candidate = (
                not np.isnan(z) and abs(z) >= z_entry and abs(z) <= z_sl
            )
            if not is_entry_candidate:
                filtered[symbol] = result
                continue

            coin_df = raw_data.get(symbol)
            if coin_df is None or coin_df.empty:
                skipped.append(f"{symbol} (no data)")
                continue

            pass_count = 0
            checked = 0

            for days in self._stability_windows_days:
                bars = days * self._bars_per_day
                if len(primary_df) < bars or len(coin_df) < bars:
                    continue

                coin_close = coin_df["close"].tail(bars)
                primary_close = primary_df["close"].tail(bars)
                if (coin_close <= 0).any() or (primary_close <= 0).any():
                    continue

                coin_log = np.log(coin_close)
                primary_log = np.log(primary_close)
                beta_window = self._estimate_window_beta(coin_log, primary_log)
                if beta_window is None:
                    continue

                checked += 1

                if not (self._min_beta <= beta_window <= self._max_beta):
                    continue

                halflife = self._halflife_filter_service.calculate_for_spread(
                    coin_log_prices=coin_log,
                    primary_log_prices=primary_log,
                    beta=beta_window,
                )
                adf_pvalue = self._adf_filter_service.calculate_for_spread(
                    coin_log_prices=coin_log,
                    primary_log_prices=primary_log,
                    beta=beta_window,
                )

                if (
                    halflife is not None
                    and adf_pvalue is not None
                    and halflife <= halflife_threshold
                    and adf_pvalue < adf_threshold
                ):
                    pass_count += 1

            required = min(self._stability_min_pass_windows, checked) if checked > 0 else 0
            if required == 0 or pass_count >= required:
                filtered[symbol] = result
                stability_passes[symbol] = pass_count
            else:
                skipped.append(f"{symbol} ({pass_count}/{checked})")
                self._logger.warning(
                    f"⚠️ {symbol}: Stability-over-time failed "
                    f"(pass={pass_count}/{checked}, required={required})"
                )

        if skipped:
            self._logger.warning(
                f"Skipped {len(skipped)} entry candidates by stability-over-time: {', '.join(skipped)}"
            )

        return filtered, stability_passes

    def _filter_by_correlation(
        self,
        z_score_results: Dict[str, ZScoreResult],
        correlation_results: Dict[str, CorrelationResult],
    ) -> Dict[str, ZScoreResult]:
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
            corr_result = correlation_results.get(symbol)
            if (
                result.current_correlation >= self._correlation_threshold
                and result.current_beta >= self._min_beta
                and result.current_beta <= self._max_beta
            ):
                if corr_result is not None:
                    (
                        beta_stable,
                        beta_short,
                        beta_long,
                        beta_drift,
                    ) = self._evaluate_beta_drift(corr_result)
                    if not beta_stable:
                        skipped.append(
                            f"{symbol} (beta drift={beta_drift:.2f}, short={beta_short:.3f}, long={beta_long:.3f})"
                        )
                        self._logger.warning(
                            f"⚠️ {symbol}: beta drift guard failed | "
                            f"short={beta_short:.4f}, long={beta_long:.4f}, "
                            f"drift={beta_drift:.2f} > {self._beta_drift_max_relative:.2f}"
                        )
                        continue

                filtered[symbol] = result
            else:
                skipped.append(f"{symbol} (corr={result.current_correlation:.4f})")

        if skipped:
            self._logger.info(
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
        - |Z| >= dynamic_entry_threshold AND |Z| <= sl_threshold

        Uses dynamic_entry_threshold from each symbol's ZScoreResult.

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

        # Get SL threshold from z_score_service (static)
        z_sl = self._z_score_service.z_sl_threshold

        filtered = {}
        skipped = []

        primary_df = raw_data.get(self._primary_pair)
        if primary_df is None or primary_df.empty:
            self._logger.warning("No primary pair data for Hurst calculation")
            return z_score_results, hurst_values

        for symbol, result in z_score_results.items():
            z = result.current_z_score

            # Use dynamic entry threshold for this symbol
            z_entry = result.dynamic_entry_threshold

            # Check if this is an entry candidate
            # Entry candidate: |Z| >= dynamic_entry_threshold AND |Z| <= sl_threshold
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
                    f"✅ {symbol}: Z={z:.2f} (threshold={z_entry:.2f}), Hurst={hurst:.4f} < {self._hurst_filter_service.threshold} (mean-reverting)"
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

    def _filter_by_adf(
        self,
        z_score_results: Dict[str, ZScoreResult],
        raw_data: Dict[str, pd.DataFrame],
        correlation_results: Dict,
        halflife_values: Dict[str, float],
    ) -> tuple[Dict[str, ZScoreResult], Dict[str, float]]:
        """
        Filter z-score results by ADF stationarity test.

        ONLY checks ADF for entry candidates that passed Half-Life filter:
        - Symbol must be in halflife_values (was checked by Half-Life)
        - p-value < threshold indicates stationarity

        This is a complementary filter to Half-Life:
        - Half-Life measures mean-reversion speed
        - ADF tests for stationarity (no unit root)

        Args:
            z_score_results: Dictionary mapping symbol -> ZScoreResult.
            raw_data: Dictionary mapping symbol -> OHLCV DataFrame.
            correlation_results: Dictionary with beta values per symbol.
            halflife_values: Dictionary of symbols that passed Half-Life filter.

        Returns:
            Tuple of:
            - Filtered dictionary with only stationary spreads
            - ADF p-values dict for all checked symbols (for logging)
        """
        adf_pvalues: Dict[str, float] = {}

        if not self._adf_filter_service or not self._adf_filter_service.is_available:
            return z_score_results, adf_pvalues

        filtered = {}
        skipped = []

        primary_df = raw_data.get(self._primary_pair)
        if primary_df is None or primary_df.empty:
            self._logger.warning("No primary pair data for ADF calculation")
            return z_score_results, adf_pvalues

        for symbol, result in z_score_results.items():
            # Only check ADF for symbols that passed Half-Life filter
            # (i.e., symbols that are in halflife_values)
            if symbol not in halflife_values:
                # Not an entry candidate or didn't pass Half-Life - keep without ADF check
                filtered[symbol] = result
                continue

            # This symbol passed Half-Life - now check ADF
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

            # Calculate ADF p-value for spread
            pvalue = self._adf_filter_service.calculate_for_spread(
                coin_log_prices=coin_df["close"].apply(np.log),
                primary_log_prices=primary_df["close"].apply(np.log),
                beta=beta,
            )

            adf_pvalues[symbol] = pvalue

            z = result.current_z_score
            halflife = halflife_values.get(symbol, float("inf"))

            if self._adf_filter_service.is_stationary(pvalue):
                filtered[symbol] = result
                self._logger.info(
                    f"✅ {symbol}: Z={z:.2f}, HL={halflife:.1f}, ADF p={pvalue:.4f} < {self._adf_filter_service.threshold} (stationary)"
                )
            else:
                skipped.append(f"{symbol} (Z={z:.2f}, HL={halflife:.1f}, ADF p={pvalue:.4f})")
                self._logger.warning(
                    f"⚠️ {symbol}: Z={z:.2f}, HL={halflife:.1f}, ADF p={pvalue:.4f} >= {self._adf_filter_service.threshold} (non-stationary, skip)"
                )

        if skipped:
            self._logger.warning(
                f"Skipped {len(skipped)} entry candidates with high ADF p-value: {', '.join(skipped)}"
            )

        return filtered, adf_pvalues

    def _filter_by_halflife(
        self,
        z_score_results: Dict[str, ZScoreResult],
        raw_data: Dict[str, pd.DataFrame],
        correlation_results: Dict,
        hurst_values: Dict[str, float],
    ) -> tuple[Dict[str, ZScoreResult], Dict[str, float]]:
        """
        Filter z-score results by Half-Life mean-reversion speed.

        ONLY checks Half-Life for entry candidates that passed Hurst filter:
        - Symbol must be in hurst_values (was checked by Hurst)
        - Half-Life <= threshold indicates fast enough mean reversion

        This is a complementary filter to Hurst:
        - Hurst measures mean-reversion tendency (H < 0.45)
        - Half-Life measures mean-reversion speed

        Args:
            z_score_results: Dictionary mapping symbol -> ZScoreResult.
            raw_data: Dictionary mapping symbol -> OHLCV DataFrame.
            correlation_results: Dictionary with beta values per symbol.
            hurst_values: Dictionary of symbols that passed Hurst filter.

        Returns:
            Tuple of:
            - Filtered dictionary with only fast-reverting spreads
            - Half-Life values dict for all checked symbols (for logging)
        """
        halflife_values: Dict[str, float] = {}

        if not self._halflife_filter_service or not self._halflife_filter_service.is_available:
            return z_score_results, halflife_values

        filtered = {}
        skipped = []

        primary_df = raw_data.get(self._primary_pair)
        if primary_df is None or primary_df.empty:
            self._logger.warning("No primary pair data for Half-Life calculation")
            return z_score_results, halflife_values

        for symbol, result in z_score_results.items():
            # Only check Half-Life for symbols that passed Hurst filter
            # (i.e., symbols that are in hurst_values)
            if symbol not in hurst_values:
                # Not an entry candidate or didn't pass Hurst - keep without Half-Life check
                filtered[symbol] = result
                continue

            # This symbol passed Hurst - now check Half-Life
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

            # Calculate Half-Life for spread
            halflife = self._halflife_filter_service.calculate_for_spread(
                coin_log_prices=coin_df["close"].apply(np.log),
                primary_log_prices=primary_df["close"].apply(np.log),
                beta=beta,
            )

            halflife_values[symbol] = halflife

            z = result.current_z_score
            hurst = hurst_values.get(symbol, 0.5)

            if self._halflife_filter_service.is_acceptable(halflife):
                filtered[symbol] = result
                self._logger.info(
                    f"✅ {symbol}: Z={z:.2f}, Hurst={hurst:.3f}, HL={halflife:.1f} bars "
                    f"<= {self._halflife_filter_service.threshold} (fast reversion)"
                )
            else:
                skipped.append(f"{symbol} (Z={z:.2f}, H={hurst:.3f}, HL={halflife:.1f})")
                self._logger.warning(
                    f"⚠️ {symbol}: Z={z:.2f}, Hurst={hurst:.3f}, HL={halflife:.1f} bars "
                    f"> {self._halflife_filter_service.threshold} (slow reversion, skip)"
                )

        if skipped:
            self._logger.warning(
                f"Skipped {len(skipped)} entry candidates with high Half-Life: {', '.join(skipped)}"
            )

        return filtered, halflife_values

    async def _connect_and_fetch_symbols(self) -> None:
        if not self._exchange.is_connected:
            await self._exchange.connect()

    async def _load_ohlcv_data(self) -> Dict[str, pd.DataFrame]:
        """
        Load OHLCV data for all symbols using optimized bulk loading.

        Uses single DB query to load cached data, then batch fetches
        missing data from exchange.

        IMPORTANT: Only uses intersection of timestamps across all symbols.
        No forward fill - only real closed candles are used.
        """
        end_time = datetime.now(timezone.utc)
        # Calculate required data window:
        # - DYNAMIC_THRESHOLD_WINDOW_BARS (440) for dynamic threshold calculation
        # - LOOKBACK_WINDOW for rolling z-score calculation
        # - LOOKBACK_WINDOW for rolling beta calculation
        # Total: 440 + 288 + 288 = 1016 candles minimum for 15m timeframe
        #
        # For 15m: 1016 candles = ~10.6 days
        # Base warmup: 3x lookback + buffer.
        data_window_days = self._lookback_window_days * 3 + 2

        # Extend warmup for stability/beta-drift windows if enabled.
        if self._enable_stability_filter and self._stability_windows_days:
            data_window_days = max(data_window_days, max(self._stability_windows_days) + 2)
        if self._enable_beta_drift_guard:
            data_window_days = max(data_window_days, self._beta_drift_long_days + 2)
        start_time = end_time - timedelta(days=data_window_days)

        # Get trading pairs (from MongoDB if available, otherwise from config)
        trading_pairs = self._get_trading_pairs()

        self._logger.info(
            f"Loading {data_window_days} days of data for {len(trading_pairs)} pairs "
            f"(3x lookback={self._lookback_window_days} + buffer for rolling calculations)"
        )

        # Use optimized bulk loading
        raw_data = await self._data_loader.load_ohlcv_bulk(
            symbols=[self._primary_pair] + trading_pairs,
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

        primary_index = raw_data[self._primary_pair].index

        # Find common timestamps across ALL symbols (intersection)
        # This ensures we only use timestamps where ALL symbols have data
        common_index = primary_index
        for symbol, df in raw_data.items():
            if symbol != self._primary_pair:
                common_index = common_index.intersection(df.index)

        self._logger.info(
            f"Common timestamps: {len(common_index)} "
            f"(primary has {len(primary_index)})"
        )

        if len(common_index) == 0:
            self._logger.error("No common timestamps found across symbols!")
            return {}

        # Align all DataFrames to common index (no ffill - only real data)
        aligned_data: Dict[str, pd.DataFrame] = {}
        for symbol, df in raw_data.items():
            aligned_df = df.loc[common_index].copy()

            # Verify no NaN values
            nan_count = aligned_df.isna().sum().sum()
            if nan_count > 0:
                self._logger.warning(
                    f"{symbol}: {nan_count} NaN values found after alignment, dropping"
                )
                aligned_df = aligned_df.dropna()

            aligned_data[symbol] = aligned_df

        # Log per-symbol candle count for debugging
        candle_counts = {s: len(df) for s, df in aligned_data.items()}
        min_candles = min(candle_counts.values()) if candle_counts else 0
        max_candles = max(candle_counts.values()) if candle_counts else 0

        self._logger.info(
            f"Aligned {len(aligned_data)} symbols to common time index "
            f"({len(common_index)} candles, per-symbol: {min_candles}-{max_candles})"
        )

        # Log first/last timestamp for primary pair (for debugging timezone issues)
        if self._primary_pair in aligned_data:
            primary_df = aligned_data[self._primary_pair]
            if not primary_df.empty:
                first_ts = primary_df.index[0]
                last_ts = primary_df.index[-1]
                last_close = primary_df["close"].iloc[-1]
                self._logger.info(
                    f"📊 Data range: {first_ts.isoformat()} → {last_ts.isoformat()} "
                    f"({len(primary_df)} candles for {self._primary_pair}) | "
                    f"last_close=${last_close:.2f}"
                )

        return aligned_data

    def _format_results(
        self,
        results: Dict[str, ZScoreResult],
        sort_by: str = "z_score",
        top_n: int | None = None,
        hurst_values: Dict[str, float] | None = None,
        adf_pvalues: Dict[str, float] | None = None,
        halflife_values: Dict[str, float] | None = None,
    ) -> str:
        """
        Format z-score results as a pretty table for logging.

        Args:
            results: Dictionary mapping symbol -> ZScoreResult.
            sort_by: Sort key - "z_score", "beta", "correlation", or "symbol".
            top_n: Limit output to top N results (by absolute z-score). None for all.
            hurst_values: Optional dict of symbol -> Hurst exponent (only for entry candidates).
            adf_pvalues: Optional dict of symbol -> ADF p-value (only for entry candidates).
            halflife_values: Optional dict of symbol -> Half-Life in bars (only for entry candidates).

        Returns:
            Formatted string with table of results.
        """
        if not results:
            return "No z-score results to display."

        hurst_values = hurst_values or {}
        adf_pvalues = adf_pvalues or {}
        halflife_values = halflife_values or {}
        z_sl = self._z_score_service.z_sl_threshold

        # Build list of tuples for sorting
        data = []
        for symbol, res in results.items():
            data.append(
                {
                    "symbol": symbol,
                    "z_score": res.current_z_score,
                    "dyn_threshold": res.dynamic_entry_threshold,
                    "beta": res.current_beta,
                    "correlation": res.current_correlation,
                    "spread": res.current_spread,
                    "hurst": hurst_values.get(symbol),  # None if not calculated
                    "adf": adf_pvalues.get(symbol),  # None if not calculated
                    "halflife": halflife_values.get(symbol),  # None if not calculated
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
        header = f"{'Symbol':<20} {'Z-Score':>10} {'DynThr':>8} {'β (hedge)':>12} {'Corr':>8} {'Hurst':>8} {'ADF p':>8} {'HL':>6} {'Signal':>12}"
        separator = "-" * len(header)

        lines.append("")
        lines.append(separator)
        lines.append(header)
        lines.append(separator)

        for row in data:
            z = row["z_score"]
            dyn_thr = row["dyn_threshold"]
            beta = row["beta"]
            corr = row["correlation"]
            hurst = row["hurst"]
            adf = row["adf"]
            halflife = row["halflife"]

            # Determine signal based on z-score and dynamic threshold
            if np.isnan(z):
                signal = "N/A"
            elif z >= dyn_thr and z <= z_sl:
                signal = "🔴 SHORT"
            elif z <= -dyn_thr and z >= -z_sl:
                signal = "🟢 LONG"
            elif abs(z) >= 1.5:
                signal = "⚠️ WATCH"
            else:
                signal = "—"

            z_str = f"{z:>10.4f}" if not np.isnan(z) else f"{'N/A':>10}"
            dyn_thr_str = f"{dyn_thr:>8.2f}" if not np.isnan(dyn_thr) else f"{'N/A':>8}"
            beta_str = f"{beta:>12.4f}" if not np.isnan(beta) else f"{'N/A':>12}"
            corr_str = f"{corr:>8.4f}" if not np.isnan(corr) else f"{'N/A':>8}"
            # Hurst: show value only for entry candidates, "-" otherwise
            hurst_str = f"{hurst:>8.4f}" if hurst is not None else f"{'—':>8}"
            # ADF: show value only for entry candidates, "-" otherwise
            adf_str = f"{adf:>8.4f}" if adf is not None else f"{'—':>8}"
            # Half-Life: show value only for entry candidates, "-" otherwise
            hl_str = f"{halflife:>6.1f}" if halflife is not None and halflife != float("inf") else f"{'—':>6}"

            lines.append(
                f"{row['symbol']:<20} {z_str} {dyn_thr_str} {beta_str} {corr_str} {hurst_str} {adf_str} {hl_str} {signal:>12}"
            )

        lines.append(separator)
        lines.append(f"Total: {len(data)} symbols")
        lines.append("")

        return "\n".join(lines)
