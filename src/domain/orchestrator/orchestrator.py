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

from typing import TYPE_CHECKING, Dict, List, Optional
import numpy as np
import pandas as pd

from src.domain.position_state import PositionStateService
from src.domain.screener import ScreenerService
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
        hurst_watch_tolerance: float = 0.02,
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
            hurst_watch_tolerance: Tolerance for Hurst threshold on watches/positions.
                Entry requires H < threshold, but holding allows H < threshold + tolerance.
                Default 0.02 means entry at 0.45, hold until 0.47.
        """
        self._logger = logger
        self._screener_service = screener_service
        self._event_emitter = event_emitter
        self._position_state = position_state_service
        self._hurst_filter = hurst_filter_service
        self._primary_pair = primary_pair
        self._entry_observer = entry_observer_service
        self._funding_filter = funding_filter_service
        self._hurst_watch_tolerance = hurst_watch_tolerance

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
            filtered_results=scan_result.filtered_results,
            raw_data=scan_result.raw_data,
            min_correlation=min_correlation,
        )

        # 4. Check watched pairs in EntryObserver - remove if they failed filters
        await self._check_watched_pairs(
            z_score_results=scan_result.all_z_score_results,
            filtered_results=scan_result.filtered_results,
            raw_data=scan_result.raw_data,
            min_correlation=min_correlation,
        )

        # 5. Check entry conditions for new positions
        entry_signals = await self._check_entry_conditions(
            filtered_results=scan_result.filtered_results,
            hurst_values=scan_result.hurst_values,
            raw_data=scan_result.raw_data,
            z_entry=z_entry,
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
        raw_data: Dict[str, pd.DataFrame],
        min_correlation: float,
    ) -> None:
        """
        Check if any watched pairs in EntryObserver should be removed.

        A watched pair should be removed if:
        1. CORRELATION_DROP: symbol not in filtered_results (correlation < threshold)
        2. HURST_TRENDING: Hurst >= hurst_threshold (spread became trending)
        3. FALSE_ALARM: |Z| < dynamic_entry_threshold (signal disappeared)

        This prevents entering positions on pairs that no longer meet criteria.
        """
        if self._entry_observer is None:
            return

        # Get currently watched symbols
        watched = self._entry_observer.get_active_watches()
        if not watched:
            return

        primary_df = raw_data.get(self._primary_pair)

        for coin_symbol, watch_info in list(watched.items()):
            remove_reason = None
            z_result = z_score_results.get(coin_symbol)

            # Check if symbol dropped from correlation filter
            if coin_symbol not in filtered_results:
                current_corr = z_result.current_correlation if z_result else 0.0

                self._logger.info(
                    f"🔻 Watch {coin_symbol} failed CORRELATION filter | "
                    f"corr={current_corr:.3f} < {min_correlation}"
                )
                remove_reason = WatchCancelReason.CORRELATION_DROP

            elif z_result is not None:
                # Check if Z-score dropped below entry threshold (signal disappeared)
                current_z = z_result.current_z_score
                dynamic_threshold = z_result.dynamic_entry_threshold

                if abs(current_z) < dynamic_threshold:
                    self._logger.info(
                        f"❌ Watch {coin_symbol} signal DISAPPEARED | "
                        f"|Z|={abs(current_z):.2f} < threshold={dynamic_threshold:.2f}"
                    )
                    remove_reason = WatchCancelReason.FALSE_ALARM

                # Check Hurst filter (only if no other reason yet)
                # Use TOLERANCE for watches - we allow Hurst to rise slightly
                # Entry requires H < threshold, but holding allows H < threshold + tolerance
                elif self._hurst_filter is not None:
                    coin_df = raw_data.get(coin_symbol)

                    if coin_df is not None and primary_df is not None:
                        hurst = self._hurst_filter.calculate_for_spread(
                            coin_log_prices=coin_df["close"].apply(np.log),
                            primary_log_prices=primary_df["close"].apply(np.log),
                            beta=z_result.current_beta,
                        )

                        # Use relaxed threshold for watches (threshold + tolerance)
                        max_allowed_hurst = self._hurst_filter.threshold + self._hurst_watch_tolerance

                        if hurst is not None and hurst >= max_allowed_hurst:
                            self._logger.info(
                                f"📈 Watch {coin_symbol} failed HURST filter (w/ tolerance) | "
                                f"H={hurst:.3f} >= {max_allowed_hurst:.3f} "
                                f"(entry_thresh={self._hurst_filter.threshold})"
                            )
                            remove_reason = WatchCancelReason.HURST_TRENDING

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
        filtered_results: Dict[str, ZScoreResult],
        raw_data: Dict[str, pd.DataFrame],
        min_correlation: float,
    ) -> List[ExitSignalEvent]:
        """
        Check STRUCTURAL exit conditions for all open positions.

        Orchestrator checks only structural failures that invalidate the spread:
        1. CORRELATION_DROP: correlation < min_correlation (spread relationship broken)
        2. HURST_TRENDING: Hurst >= threshold (spread became trending, not mean-reverting)

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

            # 1. STRUCTURAL CHECK: Correlation drop
            # If correlation dropped below threshold, the spread relationship is broken
            if coin_symbol not in filtered_results:
                exit_reason = ExitReason.CORRELATION_DROP
                self._logger.info(
                    f"🔻 CORRELATION_DROP detected | {coin_symbol} | "
                    f"corr={current_corr:.3f} < {min_correlation}"
                )

            # 2. STRUCTURAL CHECK: Hurst trending
            # If spread became trending, mean-reversion assumption is invalid
            # Use TOLERANCE for open positions - we allow Hurst to rise slightly
            # Entry requires H < threshold, but holding allows H < threshold + tolerance
            if exit_reason is None and self._hurst_filter is not None:
                coin_df = raw_data.get(coin_symbol)
                if coin_df is not None and primary_df is not None:
                    hurst = self._hurst_filter.calculate_for_spread(
                        coin_log_prices=coin_df["close"].apply(np.log),
                        primary_log_prices=primary_df["close"].apply(np.log),
                        beta=z_result.current_beta,
                    )

                    # Use relaxed threshold for positions (threshold + tolerance)
                    max_allowed_hurst = self._hurst_filter.threshold + self._hurst_watch_tolerance

                    if hurst is not None and hurst >= max_allowed_hurst:
                        exit_reason = ExitReason.HURST_TRENDING
                        self._logger.info(
                            f"📈 HURST_TRENDING detected | {coin_symbol} | "
                            f"H={hurst:.3f} >= {max_allowed_hurst:.3f} "
                            f"(entry_thresh={self._hurst_filter.threshold})"
                        )

            # NOTE: Z-Score TP/SL checks are NOT done here!
            # ExitObserver handles TP/SL using frozen entry parameters (beta, std)
            # to avoid "moving goalposts" problem with recalculated values.

            if exit_reason:
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
        raw_data: Dict[str, pd.DataFrame],
        z_entry: float,
        z_sl: float,
        z_tp: float,
    ) -> List[PendingEntrySignalEvent]:
        """
        Check entry conditions for new positions.

        Entry conditions:
        - |Z| >= dynamic_entry_threshold (adaptive per-symbol threshold)
        - |Z| < z_sl (not too extreme)
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

            # Check entry condition: |Z| >= dynamic_threshold AND |Z| < sl
            if not (abs(z) >= dynamic_threshold and abs(z) < z_sl):
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
    # Helpers
    # =========================================================================

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
