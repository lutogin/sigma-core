"""
Orchestrator Service.

Coordinates the scanning pipeline and emits trading events.
Responsible for:
- Running the screener scan cycle
- Checking STRUCTURAL exit conditions (CORRELATION_DROP, HURST_TRENDING)
- Checking entry conditions for new positions
- Removing watched pairs from EntryObserver if they fail filters
- Emitting PendingEntrySignalEvent for trailing entry (EntryObserver will handle)
- Emitting ExitSignalEvent for structural failures

NOTE: Z-Score TP/SL exits are handled ONLY by ExitObserver using frozen
entry parameters to avoid the "moving goalposts" problem.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

from src.domain.position_state import PositionStateService, SpreadPosition
from src.domain.screener import ScreenerService
from src.domain.screener.correlation.correlation import CorrelationResult
from src.domain.screener.hurst_filter import HurstFilterService
from src.domain.screener.funding_filter import FundingFilterService
from src.domain.screener.z_score import ZScoreResult
from src.infra.event_emitter import (
    EventEmitter,
    PendingEntrySignalEvent,
    ExitSignalEvent,
    ExitReason,
    SpreadSide,
    MarketUnsafeEvent,
    ScanCompleteEvent,
    WatchCancelReason,
)

if TYPE_CHECKING:
    from src.domain.entry_observer import EntryObserverService


class OrchestratorService:
    """
    Orchestrates the statistical arbitrage scanning pipeline.

    This service coordinates the screener, checks exit/entry conditions,
    and emits trading events. It acts as the main entry point for the
    scanning process.

    STRUCTURAL Exit conditions (checked by Orchestrator):
    - CORRELATION_DROP: correlation < min_correlation (spread relationship broken)
    - HURST_TRENDING: Hurst >= hurst_threshold (spread became trending)
    - TIMEOUT: position held longer than max_position_bars (handled by TradingService)

    Z-Score Exit conditions (checked ONLY by ExitObserver):
    - TAKE_PROFIT: |Z| <= tp_threshold (mean reversion complete)
    - STOP_LOSS: |Z| >= sl_threshold (extreme divergence)

    NOTE: Z-Score TP/SL are handled by ExitObserver using "frozen" entry parameters
    (beta, std) to avoid the "moving goalposts" problem where recalculated values
    would give different Z-scores than the actual position's risk profile.

    Entry conditions (checked after exits):
    - |Z| >= entry_threshold AND |Z| < sl_threshold
    - Correlation >= min_correlation
    - Hurst < hurst_threshold (mean-reverting)
    """

    def __init__(
        self,
        logger,
        screener_service: ScreenerService,
        event_emitter: EventEmitter,
        position_state_service: PositionStateService,
        hurst_filter_service: HurstFilterService,
        primary_pair: str,
        entry_observer_service: Optional["EntryObserverService"] = None,
        funding_filter_service: Optional[FundingFilterService] = None,
        hurst_watch_threshold: float = 0.46,
        correlation_exit_threshold: float = 0.75,
        correlation_watch_threshold: float = 0.77,
        hurst_trending_for_exit: float = 0.46,
        hurst_trending_confirm_scans: int = 2,
        adf_exit_confirm_scans: int = 2,
        halflife_exit_confirm_scans: int = 2,
        z_score_progress_exit_threshold: float = 0.30,
        z_extreme_level: float = 5.0,
    ):
        """
        Initialize Orchestrator Service.

        Args:
            logger: Application logger.
            screener_service: Service for scanning and filtering pairs.
            event_emitter: Event emitter for trading events.
            position_state_service: Service for position state management.
            hurst_filter_service: Service for Hurst exponent calculation.
            primary_pair: Primary trading pair (e.g., "ETH/USDT:USDT").
            entry_observer_service: Optional service for trailing entry monitoring.
            funding_filter_service: Optional service for funding rate filtering.
            hurst_watch_threshold: Threshold for Hurst threshold on watches.
                Entry requires H < threshold
            correlation_exit_threshold: Relaxed correlation threshold for exiting positions.
                Entry requires >= MIN_CORRELATION (0.8), exit only when < this (0.75).
            correlation_watch_threshold: Relaxed correlation threshold for watches.
                Entry requires >= MIN_CORRELATION (0.8), remove watch only when < this (0.77).
            z_extreme_level: Maximum Z-score to allow entry (replaces z_sl check).
                Allows signals above z_sl (4.0) up to this level (5.0) to enter trailing entry.
        """
        self._logger = logger
        self._screener_service = screener_service
        self._event_emitter = event_emitter
        self._position_state = position_state_service
        self._hurst_filter = hurst_filter_service
        self._primary_pair = primary_pair
        self._entry_observer = entry_observer_service
        self._funding_filter = funding_filter_service
        self._hurst_watch_threshold = hurst_watch_threshold
        self._correlation_exit_threshold = correlation_exit_threshold
        self._correlation_watch_threshold = correlation_watch_threshold
        self._hurst_trending_for_exit = hurst_trending_for_exit
        self._hurst_trending_confirm_scans = hurst_trending_confirm_scans
        self._adf_exit_confirm_scans = adf_exit_confirm_scans
        self._halflife_exit_confirm_scans = halflife_exit_confirm_scans
        self._z_score_progress_exit_threshold = z_score_progress_exit_threshold
        self._z_extreme_level = z_extreme_level

        # Internal state: track consecutive Hurst violations per symbol
        # Key: coin_symbol, Value: consecutive count of hurst >= threshold
        self._hurst_violation_counts: Dict[str, int] = {}
        self._adf_violation_counts: Dict[str, int] = {}
        self._halflife_violation_counts: Dict[str, int] = {}

    async def run(self) -> None:
        """
        Run the orchestrator - main entry point.

        This method runs the full scan pipeline:
        1. Run screener to get current Z-scores and correlations
        2. Check exit conditions for open positions
        3. Check entry conditions for new positions
        4. Emit appropriate events
        """
        self._logger.info("🚀 Orchestrator starting scan cycle...")

        try:
            result = await self._run_scan_cycle()

            if result is None:
                self._logger.info("Scan cycle completed with no tradeable signals.")
            else:
                self._logger.info(
                    f"Scan cycle completed. "
                    f"Exits: {result.exit_signals_emitted}, "
                    f"Entries: {result.entry_signals_emitted}"
                )

        except Exception as e:
            self._logger.exception(f"Orchestrator scan cycle failed: {e}")
            raise

    async def _run_scan_cycle(self) -> Optional["ScanCycleResult"]:
        """
        Run a single scan cycle.

        Returns:
            ScanCycleResult with metrics, or None if scan was aborted.
        """
        # 1. Run screener scan to get current data
        scan_result = await self._screener_service.scan()

        # Check if scan was aborted (no data)
        if scan_result is None:
            return None

        # 2. Handle market unsafe condition
        if scan_result.volatility_result and not scan_result.volatility_result.is_safe:
            await self._emit_market_unsafe_event(scan_result.volatility_result)
            return None

        # Get thresholds from screener
        z_entry = self._screener_service.z_entry_threshold
        z_tp = self._screener_service.z_tp_threshold
        z_sl = self._screener_service.z_sl_threshold
        min_correlation = self._screener_service.correlation_threshold

        # 3. Check exit conditions for open positions FIRST
        exit_signals = await self._check_exit_conditions(
            z_score_results=scan_result.all_z_score_results,
            raw_data=scan_result.raw_data,
            correlation_results=scan_result.correlation_results,
            adf_pvalues=scan_result.adf_pvalues,
            halflife_values=scan_result.halflife_values,
            hurst_values=scan_result.hurst_values,
        )

        # 4. Check watched pairs in EntryObserver - remove if they failed filters
        await self._check_watched_pairs(
            z_score_results=scan_result.all_z_score_results,
            filtered_results=scan_result.filtered_results,
            _raw_data=scan_result.raw_data,
            _min_correlation=min_correlation,
            adf_pvalues=scan_result.adf_pvalues,
            halflife_values=scan_result.halflife_values,
            hurst_values=scan_result.hurst_values,
        )

        # 5. Check entry conditions for new positions
        entry_signals = await self._check_entry_conditions(
            filtered_results=scan_result.filtered_results,
            hurst_values=scan_result.hurst_values,
            halflife_values=scan_result.halflife_values,
            raw_data=scan_result.raw_data,
            _z_entry=z_entry,
            z_sl=z_sl,
            z_tp=z_tp,
        )

        # 6. Emit scan complete event
        await self._event_emitter.emit(
            ScanCompleteEvent(
                symbols_scanned=scan_result.symbols_scanned,
                symbols_after_correlation_filter=scan_result.symbols_after_correlation,
                entry_candidates=len(entry_signals),
                signals_emitted=len(exit_signals) + len(entry_signals),
            )
        )

        return ScanCycleResult(
            symbols_scanned=scan_result.symbols_scanned,
            symbols_after_filters=len(scan_result.filtered_results),
            exit_signals_emitted=len(exit_signals),
            entry_signals_emitted=len(entry_signals),
        )

    # =========================================================================
    # Watched Pairs Checking (EntryObserver)
    # =========================================================================

    async def _check_watched_pairs(
        self,
        z_score_results: Dict[str, ZScoreResult],
        filtered_results: Dict[str, ZScoreResult],
        _raw_data: Dict[str, pd.DataFrame],
        _min_correlation: float,
        adf_pvalues: Dict[str, float],
        halflife_values: Dict[str, float],
        hurst_values: Dict[str, float],
    ) -> None:
        """
        Check if any watched pairs in EntryObserver should be removed.

        A watched pair should be removed if:
        1. Symbol is NOT in filtered_results - determine specific reason:
           - ADF_NON_STATIONARY: failed ADF test
           - HALFLIFE_TOO_SLOW: half-life too long
           - HURST_TRENDING: spread became trending
           - CORRELATION_DROP: correlation dropped
        2. FALSE_ALARM: |Z| < dynamic_entry_threshold (signal disappeared)

        This prevents entering positions on pairs that no longer meet criteria.
        """
        if self._entry_observer is None:
            return

        # Get currently watched symbols
        watched = self._entry_observer.get_active_watches()
        if not watched:
            return

        for coin_symbol in list(watched.keys()):
            remove_reason = None
            z_result = z_score_results.get(coin_symbol)

            # FIRST CHECK: If symbol is NOT in filtered_results, determine which filter failed
            if coin_symbol not in filtered_results:
                # Determine specific filter that failed using data from ScanResult
                remove_reason = self._determine_failed_filter(
                    coin_symbol=coin_symbol,
                    z_result=z_result,
                    adf_pvalue=adf_pvalues.get(coin_symbol),
                    halflife=halflife_values.get(coin_symbol),
                    hurst=hurst_values.get(coin_symbol),
                    is_watch=True,  # Use watch thresholds (with tolerance)
                )

            # Check if Z-score dropped below entry threshold (signal disappeared)
            elif z_result is not None:
                if abs(z_result.current_z_score) < z_result.dynamic_entry_threshold:
                    self._logger.warning(
                        f"❌ Watch {coin_symbol} signal DISAPPEARED | "
                        f"|Z|={abs(z_result.current_z_score):.2f} < threshold={z_result.dynamic_entry_threshold:.2f}"
                    )
                    self._logger.warning(
                        f"🔍 Z-score move to normal for {coin_symbol}. But still observe trailing entry."
                    )
                    # mute false alarm (when z-score returned to normal without entry)
                    # remove_reason = WatchCancelReason.FALSE_ALARM

            # Remove from EntryObserver if failed any filter
            if remove_reason:
                await self._entry_observer.remove_watch_by_filter(
                    coin_symbol, remove_reason
                )

    # =========================================================================
    # Exit Condition Checking
    # =========================================================================

    async def _check_exit_conditions(
        self,
        z_score_results: Dict[str, ZScoreResult],
        raw_data: Dict[str, pd.DataFrame],
        correlation_results: Dict[str, CorrelationResult],
        adf_pvalues: Dict[str, float],
        halflife_values: Dict[str, float],
        hurst_values: Dict[str, float],
    ) -> List[ExitSignalEvent]:
        """
        Check STRUCTURAL exit conditions for all open positions.

        Orchestrator checks structural failures that invalidate the spread:
        - ADF_NON_STATIONARY: spread stopped being stationary
        - HALFLIFE_TOO_SLOW: mean reversion became too slow
        - HURST_TRENDING: spread became trending
        - CORRELATION_DROP: pair decoupled

        Z-Score based exits (TP/SL) are handled ONLY by ExitObserver using
        the "frozen" parameters from position entry. This prevents the
        "moving goalposts" problem where recalculated beta/std would give
        different Z-scores than the actual position's risk profile.

        TIMEOUT is handled by TradingService/PlannerService.

        Returns:
            List of emitted ExitSignalEvent.
        """
        exit_signals: List[ExitSignalEvent] = []
        open_positions = self._position_state.get_active_positions()

        if not open_positions:
            return exit_signals

        self._logger.info(
            f"🔍 Checking exit conditions for {len(open_positions)} open positions"
        )

        primary_df = raw_data.get(self._primary_pair)
        primary_price = primary_df["close"].iloc[-1] if primary_df is not None else 0.0

        for position in open_positions:
            exit_reason = None
            coin_symbol = position.coin_symbol

            # Get current Z-score result for this symbol
            z_result = z_score_results.get(coin_symbol)

            if z_result is None:
                self._logger.error(f"No Z-score data for open position {coin_symbol}")
                continue

            current_z = z_result.current_z_score
            current_corr = z_result.current_correlation

            adf_pvalue, halflife, hurst = self._calculate_missing_filter_metrics(
                coin_symbol=coin_symbol,
                position=position,
                z_result=z_result,
                raw_data=raw_data,
                correlation_results=correlation_results,
                adf_pvalue=adf_pvalues.get(coin_symbol),
                halflife=halflife_values.get(coin_symbol),
                hurst=hurst_values.get(coin_symbol),
            )

            if adf_pvalue is not None:
                adf_pvalues[coin_symbol] = adf_pvalue
            if halflife is not None:
                halflife_values[coin_symbol] = halflife
            if hurst is not None:
                hurst_values[coin_symbol] = hurst

            exit_reason = self._determine_failed_filter_for_exit(
                coin_symbol=coin_symbol,
                position=position,
                z_result=z_result,
                adf_pvalue=adf_pvalue,
                halflife=halflife,
                hurst=hurst,
            )

            # NOTE: Z-Score TP/SL checks are NOT done here!
            # ExitObserver handles TP/SL using frozen entry parameters (beta, std)
            # to avoid "moving goalposts" problem with recalculated values.

            if exit_reason:
                # Clean up violation counters for this position
                self._hurst_violation_counts.pop(coin_symbol, None)
                self._adf_violation_counts.pop(coin_symbol, None)
                self._halflife_violation_counts.pop(coin_symbol, None)

                coin_df = raw_data.get(coin_symbol)
                coin_price = coin_df["close"].iloc[-1] if coin_df is not None else 0.0

                event = ExitSignalEvent(
                    coin_symbol=coin_symbol,
                    primary_symbol=self._primary_pair,
                    exit_reason=exit_reason,
                    current_z_score=current_z,
                    current_correlation=current_corr,
                    coin_price=coin_price,
                    primary_price=primary_price,
                )

                await self._event_emitter.emit(event)
                exit_signals.append(event)

                self._logger.info(
                    f"📤 Emitted EXIT_SIGNAL: {coin_symbol} | "
                    f"reason={exit_reason.value} | Z={current_z:.2f}"
                )

        return exit_signals

    # =========================================================================
    # Entry Condition Checking
    # =========================================================================

    async def _check_entry_conditions(
        self,
        filtered_results: Dict[str, ZScoreResult],
        hurst_values: Dict[str, float],
        halflife_values: Dict[str, float],
        raw_data: Dict[str, pd.DataFrame],
        _z_entry: float,
        z_sl: float,
        z_tp: float,
    ) -> List[PendingEntrySignalEvent]:
        """
        Check entry conditions for new positions.

        Entry conditions:
        - |Z| >= dynamic_entry_threshold (adaptive per-symbol threshold)
        - |Z| < z_extreme_level (not too extreme, allows signals above z_sl)
        - Symbol not already in position
        - Symbol not in cooldown
        - Funding cost is acceptable (not toxic)

        The dynamic_entry_threshold is calculated as:
        max(z_entry_threshold, percentile_95 of historical |Z|)

        Emits PendingEntrySignalEvent for EntryObserver to monitor.
        The EntryObserver will monitor in real-time and emit EntrySignalEvent
        when reversal is confirmed (trailing entry logic).

        Returns:
            List of emitted PendingEntrySignalEvent.
        """
        entry_signals = []
        candidates_for_funding_check = []

        primary_df = raw_data.get(self._primary_pair)
        primary_price = primary_df["close"].iloc[-1] if primary_df is not None else 0.0

        # First pass: collect candidates that pass basic filters
        for symbol, result in filtered_results.items():
            z = result.current_z_score

            if np.isnan(z):
                continue

            # Use dynamic threshold for this symbol (adaptive upper bound)
            dynamic_threshold = result.dynamic_entry_threshold

            # Check entry condition: |Z| >= dynamic_threshold AND |Z| < z_extreme_level
            # Note: z_extreme_level (5.0) > z_sl (4.0) to allow extreme signals
            if not (abs(z) >= dynamic_threshold and abs(z) < self._z_extreme_level):
                continue

            # Check if already have position for this symbol
            if self._position_state.has_position(symbol):
                continue

            # Check cooldown
            if self._position_state.is_in_cooldown(symbol):
                remaining = self._position_state.get_cooldown_remaining(symbol)
                self._logger.debug(
                    f"⏸️ {symbol} in cooldown ({remaining:.0f} min remaining)"
                )
                continue

            # Determine spread side based on Z-score sign
            spread_side = SpreadSide.LONG if z < 0 else SpreadSide.SHORT
            candidates_for_funding_check.append((symbol, result, spread_side))

        # Second pass: check funding for all candidates
        if self._funding_filter and candidates_for_funding_check:
            pairs_with_sides = [
                (symbol, f"{spread_side.value.upper()}_SPREAD")
                for symbol, _, spread_side in candidates_for_funding_check
            ]
            funding_results = await self._funding_filter.check_batch(pairs_with_sides)
        else:
            funding_results = {}

        # Third pass: emit signals for candidates that pass funding filter
        for symbol, result, spread_side in candidates_for_funding_check:
            # Check funding filter
            if self._funding_filter and symbol in funding_results:
                funding_check = funding_results[symbol]
                if not funding_check.is_safe:
                    self._logger.info(
                        f"⛔ {symbol} blocked by FUNDING filter | "
                        f"Net cost: {funding_check.net_cost_pct:.3f}% per 8h"
                    )
                    continue

            z = result.current_z_score
            dynamic_threshold = result.dynamic_entry_threshold

            # Get current price
            coin_df = raw_data.get(symbol)
            coin_price = coin_df["close"].iloc[-1] if coin_df is not None else 0.0

            # Get spread mean and std for live Z-score calculation
            spread_mean = self._get_spread_mean(result)
            spread_std = self._get_spread_std(result)

            # Create PendingEntrySignalEvent for trailing entry monitoring
            event = PendingEntrySignalEvent(
                coin_symbol=symbol,
                primary_symbol=self._primary_pair,
                spread_side=spread_side,
                z_score=z,
                beta=result.current_beta,
                correlation=result.current_correlation,
                hurst=hurst_values.get(symbol, 0.0),
                halflife=halflife_values.get(symbol, 0.0),
                spread_mean=spread_mean,
                spread_std=spread_std,
                coin_price=coin_price,
                primary_price=primary_price,
                z_entry_threshold=dynamic_threshold,
                z_tp_threshold=z_tp,
                z_sl_threshold=z_sl,
            )

            await self._event_emitter.emit(event)
            entry_signals.append(event)

            self._logger.info(
                f"👀 Emitted PENDING_ENTRY_SIGNAL: {symbol} "
                f"({spread_side.value.upper()}) Z={z:.2f} | "
                f"dyn_threshold={dynamic_threshold:.2f} | "
                f"EntryObserver will monitor for reversal"
            )

        return entry_signals

    def _get_spread_mean(self, result: ZScoreResult) -> float:
        """
        Get the latest spread mean from Z-score result.

        The spread_series contains the raw spread values.
        We need to get the mean from the rolling window to calculate live Z-score.
        """
        if result.spread_series is None or result.spread_series.empty:
            return 0.0

        # Get the lookback window from screener
        lookback = getattr(self._screener_service, "_lookback_window", 288)

        # Calculate rolling mean and get the latest value
        rolling_mean = result.spread_series.rolling(window=lookback).mean()
        valid_values = rolling_mean.dropna()

        if len(valid_values) > 0:
            return float(valid_values.iloc[-1])
        return 0.0

    def _get_spread_std(self, result: ZScoreResult) -> float:
        """
        Get the latest spread std from Z-score result.

        The spread_series contains the raw spread values.
        We need to get the std from the rolling window to calculate live Z-score.
        """
        if result.spread_series is None or result.spread_series.empty:
            return 0.0

        # Get the lookback window from screener
        lookback = getattr(self._screener_service, "_lookback_window", 288)

        # Calculate rolling std and get the latest value
        rolling_std = result.spread_series.rolling(window=lookback).std()
        valid_values = rolling_std.dropna()

        if len(valid_values) > 0:
            return float(valid_values.iloc[-1])
        return 0.0

    # =========================================================================
    # Structural Filter Helpers
    # =========================================================================

    def _resolve_beta_for_metrics(
        self,
        coin_symbol: str,
        position: SpreadPosition,
        z_result: Optional[ZScoreResult],
        correlation_results: Dict[str, CorrelationResult],
    ) -> Optional[float]:
        """
        Resolve hedge ratio (beta) for structural metric recalculation.

        Priority:
        1. Latest beta from correlation scan
        2. Current beta from z-score result
        3. Frozen entry beta from position
        """
        corr_result = correlation_results.get(coin_symbol)
        if corr_result is not None and np.isfinite(corr_result.latest_beta):
            return float(corr_result.latest_beta)

        if z_result is not None and np.isfinite(z_result.current_beta):
            return float(z_result.current_beta)

        if np.isfinite(position.entry_beta):
            return float(position.entry_beta)

        return None

    def _calculate_missing_filter_metrics(
        self,
        coin_symbol: str,
        position: SpreadPosition,
        z_result: Optional[ZScoreResult],
        raw_data: Dict[str, pd.DataFrame],
        correlation_results: Dict[str, CorrelationResult],
        adf_pvalue: Optional[float],
        halflife: Optional[float],
        hurst: Optional[float],
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """
        Ensure ADF/Half-Life/Hurst are available for open positions.

        Scan pipeline computes these mainly for entry candidates. For open positions
        we recalculate missing values so structural degradation is observed even when
        |Z| is outside entry-candidate zone.
        """
        needs_adf = self._screener_service.adf_threshold is not None and adf_pvalue is None
        needs_halflife = (
            self._screener_service.halflife_threshold is not None and halflife is None
        )
        needs_hurst = self._screener_service.hurst_threshold is not None and hurst is None

        if not (needs_adf or needs_halflife or needs_hurst):
            return adf_pvalue, halflife, hurst

        coin_df = raw_data.get(coin_symbol)
        primary_df = raw_data.get(self._primary_pair)
        if (
            coin_df is None
            or primary_df is None
            or coin_df.empty
            or primary_df.empty
            or "close" not in coin_df
            or "close" not in primary_df
        ):
            return adf_pvalue, halflife, hurst

        beta = self._resolve_beta_for_metrics(
            coin_symbol=coin_symbol,
            position=position,
            z_result=z_result,
            correlation_results=correlation_results,
        )
        if beta is None:
            return adf_pvalue, halflife, hurst

        coin_close = coin_df["close"]
        primary_close = primary_df["close"]
        if (coin_close <= 0).any() or (primary_close <= 0).any():
            return adf_pvalue, halflife, hurst

        coin_log = np.log(coin_close)
        primary_log = np.log(primary_close)

        if needs_hurst and self._hurst_filter is not None:
            hurst = self._hurst_filter.calculate_for_spread(
                coin_log_prices=coin_log,
                primary_log_prices=primary_log,
                beta=beta,
            )

        halflife_filter = self._screener_service.halflife_filter_service
        if (
            needs_halflife
            and halflife_filter is not None
            and halflife_filter.is_available
        ):
            halflife = halflife_filter.calculate_for_spread(
                coin_log_prices=coin_log,
                primary_log_prices=primary_log,
                beta=beta,
            )

        adf_filter = self._screener_service.adf_filter_service
        if needs_adf and adf_filter is not None and adf_filter.is_available:
            adf_pvalue = adf_filter.calculate_for_spread(
                coin_log_prices=coin_log,
                primary_log_prices=primary_log,
                beta=beta,
            )

        return adf_pvalue, halflife, hurst

    def _check_adf_exit_with_confirmation(
        self,
        coin_symbol: str,
        adf_pvalue: float,
        adf_threshold: float,
    ) -> bool:
        """Confirm ADF degradation across multiple scans before forcing exit."""
        if adf_pvalue >= adf_threshold:
            self._adf_violation_counts[coin_symbol] = (
                self._adf_violation_counts.get(coin_symbol, 0) + 1
            )
            consecutive = self._adf_violation_counts[coin_symbol]
            if consecutive >= self._adf_exit_confirm_scans:
                self._logger.warning(
                    f"⛔ Position {coin_symbol} EXIT (ADF) | "
                    f"p-value={adf_pvalue:.4f} >= {adf_threshold:.2f} "
                    f"for {consecutive}/{self._adf_exit_confirm_scans} scans"
                )
                return True
            self._logger.info(
                f"⚠️ {coin_symbol} ADF degradation "
                f"({consecutive}/{self._adf_exit_confirm_scans}) | "
                f"p-value={adf_pvalue:.4f}"
            )
            return False

        self._adf_violation_counts.pop(coin_symbol, None)
        return False

    def _check_halflife_exit_with_confirmation(
        self,
        coin_symbol: str,
        halflife: float,
        halflife_threshold: float,
    ) -> bool:
        """Confirm Half-Life degradation across multiple scans before forcing exit."""
        if halflife > halflife_threshold:
            self._halflife_violation_counts[coin_symbol] = (
                self._halflife_violation_counts.get(coin_symbol, 0) + 1
            )
            consecutive = self._halflife_violation_counts[coin_symbol]
            if consecutive >= self._halflife_exit_confirm_scans:
                self._logger.warning(
                    f"⛔ Position {coin_symbol} EXIT (HALFLIFE) | "
                    f"HL={halflife:.1f} bars > {halflife_threshold:.1f} "
                    f"for {consecutive}/{self._halflife_exit_confirm_scans} scans"
                )
                return True
            self._logger.info(
                f"⚠️ {coin_symbol} Half-Life degradation "
                f"({consecutive}/{self._halflife_exit_confirm_scans}) | "
                f"HL={halflife:.1f} bars"
            )
            return False

        self._halflife_violation_counts.pop(coin_symbol, None)
        return False

    # =========================================================================
    # General Helpers
    # =========================================================================

    def _determine_failed_filter(
        self,
        coin_symbol: str,
        z_result: Optional[ZScoreResult],
        adf_pvalue: Optional[float],
        halflife: Optional[float],
        hurst: Optional[float],
        is_watch: bool = True,
    ) -> WatchCancelReason:
        """
        Determine which specific filter caused a symbol to be excluded.

        Uses pre-calculated values from ScanResult (no direct filter calls).
        Checks filters in order of priority:
        1. ADF (non-stationary)
        2. Half-life (too slow)
        3. Hurst (trending)
        4. Correlation (drop)

        Args:
            coin_symbol: Symbol being checked
            z_result: Z-score result if available
            adf_pvalue: Pre-calculated ADF p-value from ScanResult
            halflife: Pre-calculated half-life from ScanResult
            hurst: Pre-calculated Hurst exponent from ScanResult
            is_watch: True for watches (use tolerance), False for positions

        Returns:
            Specific WatchCancelReason for the filter that failed
        """
        if z_result is None:
            self._logger.info(f"⛔ Watch {coin_symbol} REMOVED | No Z-score data")
            return WatchCancelReason.FALSE_ALARM

        # Get thresholds from screener_service
        adf_threshold = self._screener_service.adf_threshold
        halflife_threshold = self._screener_service.halflife_threshold
        hurst_threshold = self._screener_service.hurst_threshold

        # 1. Check ADF filter
        if adf_threshold is not None and adf_pvalue is not None:
            if adf_pvalue >= adf_threshold:
                self._logger.info(
                    f"⛔ Watch {coin_symbol} failed ADF filter | "
                    f"p-value={adf_pvalue:.4f} >= {adf_threshold:.2f} (non-stationary)"
                )
                self._logger.warning(
                    f"⚠️ ADF is non-stationary for {coin_symbol}. But still observe trailing entry."
                )
                # mute false alarm (when ADF is non-stationary) for now
                # return WatchCancelReason.ADF_NON_STATIONARY

        # 2. Check Half-life filter
        if halflife_threshold is not None and halflife is not None:
            if halflife > halflife_threshold:
                self._logger.info(
                    f"⛔ Watch {coin_symbol} failed HALFLIFE filter | "
                    f"HL={halflife:.1f} bars > {halflife_threshold:.1f} (too slow)"
                )
                # mute false alarm (when Half-life is too slow) for now
                # return WatchCancelReason.HALFLIFE_TOO_SLOW
                self._logger.warning(
                    f"⚠️ Half-life is too slow for {coin_symbol}. But still observe trailing entry."
                )

        # 3. Check Hurst filter (with tolerance for watches)
        if hurst_threshold is not None and hurst is not None:
            if hurst >= self._hurst_watch_threshold:
                self._logger.info(
                    f"⛔ Watch {coin_symbol} failed HURST filter | "
                    f"H={hurst:.3f} >= {self._hurst_watch_threshold:.3f} (trending)"
                )
                return WatchCancelReason.HURST_TRENDING

        # 4. Check Correlation
        corr_threshold = (
            self._correlation_watch_threshold
            if is_watch
            else self._correlation_exit_threshold
        )
        if z_result.current_correlation < corr_threshold:
            self._logger.info(
                f"⛔ Watch {coin_symbol} failed CORRELATION filter | "
                f"corr={z_result.current_correlation:.3f} < {corr_threshold:.2f}"
            )
            return WatchCancelReason.CORRELATION_DROP

        # Default: couldn't determine specific reason
        self._logger.info(f"⛔ Watch {coin_symbol} REMOVED | Failed unknown filter")
        return WatchCancelReason.FALSE_ALARM

    def _check_hurst_exit_with_confirmation(
        self,
        coin_symbol: str,
        position: SpreadPosition,
        hurst: float,
        current_z: float,
        current_corr: float,
    ) -> bool:
        """
        Check if position should exit due to Hurst trending with confirmation logic.

        Exit conditions (ALL must be true):
        1. hurst >= hurst_trending_for_exit for hurst_confirm_scans consecutive scans
        2. corr < correlation_exit_threshold
        3. time_in_position >= 6h (24 bars for 15m)
        4. progress < z_score_progress_exit_threshold
        5. abs(z) > 1.2 (not close to normal TP)

        Progress calculation:
        - entry_abs = abs(position.entry_z_score)
        - curr_abs = abs(current_z_score)
        - progress = (entry_abs - curr_abs) / entry_abs

        Returns:
            True if should exit due to HURST_TRENDING, False otherwise
        """
        # Check if Hurst is above trending threshold
        if hurst >= self._hurst_trending_for_exit:
            # Increment consecutive counter
            self._hurst_violation_counts[coin_symbol] = (
                self._hurst_violation_counts.get(coin_symbol, 0) + 1
            )

            consecutive = self._hurst_violation_counts[coin_symbol]
            self._logger.debug(
                f"📊 {coin_symbol} Hurst violation #{consecutive}/{self._hurst_trending_confirm_scans} | "
                f"H={hurst:.3f} >= {self._hurst_trending_for_exit:.3f}"
            )

            # Check if we have enough consecutive violations
            if consecutive >= self._hurst_trending_confirm_scans:
                # # Now check additional conditions
                # should_exit, _ = self._evaluate_hurst_exit_conditions(
                #     coin_symbol=coin_symbol,
                #     position=position,
                #     hurst=hurst,
                #     current_z=current_z,
                #     current_corr=current_corr,
                #     consecutive=consecutive,
                # )

                should_exit = True  # Simplified for now: exit after confirmation! without extra checks
                return bool(should_exit)
            else:
                self._logger.info(
                    f"⚠️ {coin_symbol} Hurst trending ({consecutive}/{self._hurst_trending_confirm_scans}) | "
                    f"H={hurst:.3f}, waiting for confirmation..."
                )
                return False
        else:
            # Hurst is below threshold - reset counter
            if coin_symbol in self._hurst_violation_counts:
                old_count = self._hurst_violation_counts[coin_symbol]
                del self._hurst_violation_counts[coin_symbol]
                if old_count > 0:
                    self._logger.debug(
                        f"✅ {coin_symbol} Hurst normalized | H={hurst:.3f} - reset counter from {old_count}"
                    )
            return False

    def _evaluate_hurst_exit_conditions(
        self,
        coin_symbol: str,
        position: SpreadPosition,
        hurst: float,
        current_z: float,
        current_corr: float,
        consecutive: int,
    ) -> tuple[bool, str]:
        """
        Evaluate all conditions for Hurst-based exit.

        Returns:
            Tuple of (should_exit: bool, reason: str)
        """
        conditions_met = []
        conditions_failed = []

        # 1. ✅ Hurst trending confirmed (already checked)
        conditions_met.append(
            f"Hurst={hurst:.3f} >= {self._hurst_trending_for_exit:.3f} "
            f"({consecutive}x consecutive)"
        )

        # 2. Check correlation < threshold
        corr_ok = current_corr < self._correlation_exit_threshold
        if corr_ok:
            conditions_met.append(
                f"corr={current_corr:.3f} < {self._correlation_exit_threshold:.2f}"
            )
        else:
            conditions_failed.append(
                f"corr={current_corr:.3f} >= {self._correlation_exit_threshold:.2f}"
            )

        # # 3. Check time_in_position >= 6h (24 bars for 15m)
        # min_hours = 6
        # now = datetime.now(timezone.utc)
        # time_in_position = (now - position.opened_at).total_seconds() / 3600
        # bars_in_position = int(time_in_position * 4)  # 4 bars per hour for 15m
        # time_ok = time_in_position >= min_hours
        # if time_ok:
        #     conditions_met.append(
        #         f"time={time_in_position:.1f}h >= {min_hours}h ({bars_in_position} bars)"
        #     )
        # else:
        #     conditions_failed.append(f"time={time_in_position:.1f}h < {min_hours}h")

        # # 4. Check progress < threshold
        # entry_abs = abs(position.entry_z_score)
        # curr_abs = abs(current_z)
        # if entry_abs > 0:
        #     progress = (entry_abs - curr_abs) / entry_abs
        # else:
        #     progress = 0.0
        # progress_ok = progress < self._z_score_progress_exit_threshold
        # if progress_ok:
        #     conditions_met.append(
        #         f"progress={progress:.1%} < {self._z_score_progress_exit_threshold:.1%}"
        #     )
        # else:
        #     conditions_failed.append(
        #         f"progress={progress:.1%} >= {self._z_score_progress_exit_threshold:.1%} (good progress)"
        #     )

        # # 5. Check abs(z) > 1.2 (not close to normal TP)
        # z_not_near_tp = curr_abs > 1.2
        # if z_not_near_tp:
        #     conditions_met.append(f"|Z|={curr_abs:.2f} > 1.2")
        # else:
        #     conditions_failed.append(f"|Z|={curr_abs:.2f} <= 1.2 (near TP)")

        # ALL conditions must be met for exit
        # all_conditions_met = corr_ok and time_ok and progress_ok and z_not_near_tp
        all_conditions_met = corr_ok

        if all_conditions_met:
            self._logger.warning(
                f"⛔ {coin_symbol} HURST_TRENDING EXIT confirmed | "
                + " | ".join(conditions_met)
            )
            # Reset counter after exit decision
            self._hurst_violation_counts.pop(coin_symbol, None)
            return True, "all_conditions_met"
        else:
            self._logger.info(
                f"⚠️ {coin_symbol} Hurst trending but conditions not met | "
                f"✅ {conditions_met} | ❌ {conditions_failed}"
            )
            return False, f"failed: {conditions_failed}"

    def _determine_failed_filter_for_exit(
        self,
        coin_symbol: str,
        position: SpreadPosition,
        z_result: Optional[ZScoreResult],
        adf_pvalue: Optional[float],
        halflife: Optional[float],
        hurst: Optional[float],
    ) -> Optional[ExitReason]:
        """
        Determine which specific filter caused a position to require exit.

        Uses pre-calculated values from ScanResult (no direct filter calls).
        """
        if z_result is None:
            self._logger.warning(
                f"Skipping structural exit check for {coin_symbol}: no Z-score data"
            )
            return None

        # Get thresholds from screener_service
        adf_threshold = self._screener_service.adf_threshold
        halflife_threshold = self._screener_service.halflife_threshold

        # 1. Check ADF filter
        if adf_threshold is not None:
            if adf_pvalue is not None:
                if self._check_adf_exit_with_confirmation(
                    coin_symbol=coin_symbol,
                    adf_pvalue=adf_pvalue,
                    adf_threshold=adf_threshold,
                ):
                    return ExitReason.ADF_NON_STATIONARY
            else:
                self._adf_violation_counts.pop(coin_symbol, None)

        # 2. Check Half-life filter
        if halflife_threshold is not None:
            if halflife is not None:
                if self._check_halflife_exit_with_confirmation(
                    coin_symbol=coin_symbol,
                    halflife=halflife,
                    halflife_threshold=halflife_threshold,
                ):
                    return ExitReason.HALFLIFE_TOO_SLOW
            else:
                self._halflife_violation_counts.pop(coin_symbol, None)

        # 3. Check Hurst filter with confirmation logic
        # Exit requires: consecutive violations + corr drop + time in position + poor progress + not near TP
        if hurst is not None:
            current_z = z_result.current_z_score
            current_corr = z_result.current_correlation

            if self._check_hurst_exit_with_confirmation(
                coin_symbol=coin_symbol,
                position=position,
                hurst=hurst,
                current_z=current_z,
                current_corr=current_corr,
            ):
                return ExitReason.HURST_TRENDING
        else:
            self._hurst_violation_counts.pop(coin_symbol, None)

        # 4. Check Correlation
        if z_result.current_correlation < self._correlation_exit_threshold:
            self._logger.info(
                f"⛔ Position {coin_symbol} EXIT (CORRELATION) | "
                f"corr={z_result.current_correlation:.3f} < {self._correlation_exit_threshold:.2f}"
            )
            return ExitReason.CORRELATION_DROP

        return None

    async def _emit_market_unsafe_event(self, volatility_result) -> None:
        """Emit market unsafe event when volatility filter triggers."""
        self._logger.warning(f"⛔ TRADING HALTED: {volatility_result.stop_reason}")

        await self._event_emitter.emit(
            MarketUnsafeEvent(
                primary_symbol=self._primary_pair,
                volatility=volatility_result.current_vol or 0.0,
                volatility_threshold=volatility_result.threshold_vol or 0.0,
                change_4h=volatility_result.current_4h_change,
                change_threshold=volatility_result.threshold_4h_change,
                reason=volatility_result.stop_reason or "",
            )
        )


class ScanCycleResult:
    """Result of a single scan cycle."""

    def __init__(
        self,
        symbols_scanned: int,
        symbols_after_filters: int,
        exit_signals_emitted: int,
        entry_signals_emitted: int,
    ):
        self.symbols_scanned = symbols_scanned
        self.symbols_after_filters = symbols_after_filters
        self.exit_signals_emitted = exit_signals_emitted
        self.entry_signals_emitted = entry_signals_emitted
