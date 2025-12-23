"""
Orchestrator Service.

Coordinates the scanning pipeline and emits trading events.
Responsible for:
- Running the screener scan cycle
- Checking exit conditions for open positions (TP, SL, CORRELATION_DROP, TIMEOUT, HURST_TRENDING)
- Checking entry conditions for new positions
- Emitting PendingEntrySignalEvent for trailing entry (EntryObserver will handle)
- Emitting ExitSignalEvent for position closes
"""

from typing import Dict, List, Optional
import numpy as np
import pandas as pd

from src.domain.position_state import PositionStateService
from src.domain.screener import ScreenerService
from src.domain.screener.hurst_filter import HurstFilterService
from src.domain.screener.z_score import ZScoreResult
from src.infra.event_emitter import (
    EventEmitter,
    PendingEntrySignalEvent,
    ExitSignalEvent,
    ExitReason,
    SpreadSide,
    MarketUnsafeEvent,
    ScanCompleteEvent,
)


class OrchestratorService:
    """
    Orchestrates the statistical arbitrage scanning pipeline.

    This service coordinates the screener, checks exit/entry conditions,
    and emits trading events. It acts as the main entry point for the
    scanning process.

    Exit conditions (checked first):
    - TAKE_PROFIT: |Z| <= tp_threshold (mean reversion complete)
    - STOP_LOSS: |Z| >= sl_threshold (extreme divergence)
    - CORRELATION_DROP: correlation < min_correlation
    - HURST_TRENDING: Hurst >= hurst_threshold (spread became trending)
    - TIMEOUT: position held longer than max_position_bars (handled by TradingService)

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
        """
        self._logger = logger
        self._screener_service = screener_service
        self._event_emitter = event_emitter
        self._position_state = position_state_service
        self._hurst_filter = hurst_filter_service
        self._primary_pair = primary_pair

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
            z_tp=z_tp,
            z_sl=z_sl,
            min_correlation=min_correlation,
        )

        # 4. Check entry conditions for new positions
        entry_signals = await self._check_entry_conditions(
            filtered_results=scan_result.filtered_results,
            hurst_values=scan_result.hurst_values,
            raw_data=scan_result.raw_data,
            z_entry=z_entry,
            z_sl=z_sl,
            z_tp=z_tp,
        )

        # 5. Emit scan complete event
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
    # Exit Condition Checking
    # =========================================================================

    async def _check_exit_conditions(
        self,
        z_score_results: Dict[str, ZScoreResult],
        filtered_results: Dict[str, ZScoreResult],
        raw_data: Dict[str, pd.DataFrame],
        z_tp: float,
        z_sl: float,
        min_correlation: float,
    ) -> List[ExitSignalEvent]:
        """
        Check exit conditions for all open positions.

        Exit conditions:
        1. TAKE_PROFIT: |Z| <= z_tp (mean reversion complete)
        2. STOP_LOSS: |Z| >= z_sl (extreme divergence)
        3. CORRELATION_DROP: symbol not in filtered_results (correlation < threshold)
        4. HURST_TRENDING: Hurst >= hurst_threshold (spread became trending)
        5. TIMEOUT: handled by TradingService/PlannerService

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
            current_z = 0.0
            current_corr = 0.0

            coin_symbol = position.coin_symbol

            # Get current Z-score result for this symbol
            z_result = z_score_results.get(coin_symbol)

            if z_result is None:
                # No Z-score data - this shouldn't happen, but handle it
                self._logger.error(f"No Z-score data for open position {coin_symbol}")
                continue

            current_z = z_result.current_z_score
            current_corr = z_result.current_correlation

            # Check if symbol dropped from correlation filter
            if coin_symbol not in filtered_results:
                exit_reason = ExitReason.CORRELATION_DROP
                self._logger.info(
                    f"🔻 CORRELATION_DROP detected | {coin_symbol} | "
                    f"corr={current_corr:.3f} < {min_correlation}"
                )
            else:
                # Check Z-score conditions based on position side
                if position.side.value == "long":
                    # Long spread: entered at Z <= -entry, expect Z to rise toward 0
                    # TP when Z >= -tp (close to 0)
                    # SL when Z <= -sl (diverged more)
                    if current_z >= -z_tp:
                        exit_reason = ExitReason.TAKE_PROFIT
                    elif current_z <= -z_sl:
                        exit_reason = ExitReason.STOP_LOSS
                else:
                    # Short spread: entered at Z >= entry, expect Z to fall toward 0
                    # TP when Z <= tp (close to 0)
                    # SL when Z >= sl (diverged more)
                    if current_z <= z_tp:
                        exit_reason = ExitReason.TAKE_PROFIT
                    elif current_z >= z_sl:
                        exit_reason = ExitReason.STOP_LOSS

            # Check Hurst if no other exit reason yet
            if exit_reason is None and self._hurst_filter is not None:
                coin_df = raw_data.get(coin_symbol)
                if coin_df is not None and primary_df is not None:
                    # Calculate current Hurst for the spread
                    hurst = self._hurst_filter.calculate_for_spread(
                        coin_log_prices=coin_df["close"].apply(np.log),
                        primary_log_prices=primary_df["close"].apply(np.log),
                        beta=z_result.current_beta,
                    )

                    if not self._hurst_filter.is_mean_reverting(hurst):
                        exit_reason = ExitReason.HURST_TRENDING
                        self._logger.info(
                            f"📈 HURST_TRENDING detected | {coin_symbol} | "
                            f"H={hurst:.3f} >= {self._hurst_filter.threshold}"
                        )

            if exit_reason:
                # Get current prices
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
        - |Z| >= z_entry (enough deviation for mean reversion)
        - |Z| < z_sl (not too extreme)
        - Symbol not already in position
        - Symbol not in cooldown

        Emits PendingEntrySignalEvent for EntryObserver to monitor.
        The EntryObserver will monitor in real-time and emit EntrySignalEvent
        when reversal is confirmed (trailing entry logic).

        Returns:
            List of emitted PendingEntrySignalEvent.
        """
        entry_signals = []

        primary_df = raw_data.get(self._primary_pair)
        primary_price = primary_df["close"].iloc[-1] if primary_df is not None else 0.0

        for symbol, result in filtered_results.items():
            z = result.current_z_score

            if np.isnan(z):
                continue

            # Check entry condition: |Z| >= entry AND |Z| < sl
            if not (abs(z) >= z_entry and abs(z) < z_sl):
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

            # Get current price
            coin_df = raw_data.get(symbol)
            coin_price = coin_df["close"].iloc[-1] if coin_df is not None else 0.0

            # Determine spread side based on Z-score sign
            # Negative Z -> LONG spread (buy COIN, sell PRIMARY)
            # Positive Z -> SHORT spread (sell COIN, buy PRIMARY)
            spread_side = SpreadSide.LONG if z < 0 else SpreadSide.SHORT

            # Get spread mean and std for live Z-score calculation
            # These are the latest values from the rolling window
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
                z_entry_threshold=z_entry,
                z_tp_threshold=z_tp,
                z_sl_threshold=z_sl,
            )

            await self._event_emitter.emit(event)
            entry_signals.append(event)

            self._logger.info(
                f"👀 Emitted PENDING_ENTRY_SIGNAL: {symbol} "
                f"({spread_side.value.upper()}) Z={z:.2f} | "
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
