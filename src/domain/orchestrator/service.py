"""
Orchestrator Service.

Coordinates the scanning pipeline and emits trading events.
Responsible for:
- Running the screener scan cycle
- Emitting trading events based on scan results
- Market safety event handling
"""

from typing import Dict, Optional
import numpy as np
import pandas as pd

from src.domain.screener import ScreenerService
from src.domain.screener.z_score import ZScoreResult
from src.infra.event_emitter import (
    EventEmitter,
    EntrySignalEvent,
    SpreadSide,
    MarketUnsafeEvent,
    ScanCompleteEvent,
)


class OrchestratorService:
    """
    Orchestrates the statistical arbitrage scanning pipeline.

    This service coordinates the screener and emits trading events.
    It acts as the main entry point for the scanning process.
    """

    def __init__(
        self,
        logger,
        screener_service: ScreenerService,
        event_emitter: EventEmitter,
        primary_pair: str,
    ):
        """
        Initialize Orchestrator Service.

        Args:
            logger: Application logger.
            screener_service: Service for scanning and filtering pairs.
            event_emitter: Event emitter for trading events.
            primary_pair: Primary trading pair (e.g., "ETH/USDT:USDT").
        """
        self._logger = logger
        self._screener_service = screener_service
        self._event_emitter = event_emitter
        self._primary_pair = primary_pair

    async def run(self) -> None:
        """
        Run the orchestrator - main entry point.

        This method runs the full scan pipeline and emits events based on results.
        """
        self._logger.info("🚀 Orchestrator starting scan cycle...")

        try:
            result = await self._run_scan_cycle()

            if result is None:
                self._logger.info("Scan cycle completed with no tradeable signals.")
            else:
                self._logger.info(
                    f"Scan cycle completed. "
                    f"Processed {result.symbols_after_filters} symbols, "
                    f"{result.signals_emitted} signals emitted."
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
        # 1. Run screener scan
        scan_result = await self._screener_service.scan()

        # Check if scan was aborted (no data)
        if scan_result is None:
            return None

        # 2. Handle market unsafe condition
        if scan_result.volatility_result and not scan_result.volatility_result.is_safe:
            await self._emit_market_unsafe_event(scan_result.volatility_result)
            return None

        # 3. Find entry candidates and emit signals
        entry_candidates = self._get_entry_candidates(scan_result.filtered_results)
        signals_emitted = await self._emit_entry_signals(
            entry_candidates, scan_result.hurst_values, scan_result.raw_data
        )

        # 4. Emit scan complete event
        await self._event_emitter.emit(
            ScanCompleteEvent(
                symbols_scanned=scan_result.symbols_scanned,
                symbols_after_correlation_filter=scan_result.symbols_after_correlation,
                entry_candidates=len(entry_candidates),
                signals_emitted=signals_emitted,
            )
        )

        return ScanCycleResult(
            symbols_scanned=scan_result.symbols_scanned,
            symbols_after_filters=len(scan_result.filtered_results),
            entry_candidates=len(entry_candidates),
            signals_emitted=signals_emitted,
        )

    def _get_entry_candidates(
        self, z_score_results: Dict[str, ZScoreResult]
    ) -> Dict[str, ZScoreResult]:
        """
        Get symbols that are valid entry candidates.

        Entry candidate: |Z| >= entry_threshold AND |Z| <= sl_threshold
        """
        z_entry = self._screener_service.z_entry_threshold
        z_sl = self._screener_service.z_sl_threshold

        candidates = {}
        for symbol, result in z_score_results.items():
            z = result.current_z_score
            if not np.isnan(z) and abs(z) >= z_entry and abs(z) <= z_sl:
                candidates[symbol] = result

        return candidates

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

    async def _emit_entry_signals(
        self,
        candidates: Dict[str, ZScoreResult],
        hurst_values: Dict[str, float],
        raw_data: Dict[str, pd.DataFrame],
    ) -> int:
        """
        Emit entry signal events for each valid candidate.

        Returns:
            Number of signals emitted.
        """
        signals_emitted = 0
        primary_df = raw_data.get(self._primary_pair)
        primary_price = primary_df["close"].iloc[-1] if primary_df is not None else 0.0

        z_tp = self._screener_service.z_tp_threshold
        z_sl = self._screener_service.z_sl_threshold

        for symbol, result in candidates.items():
            coin_df = raw_data.get(symbol)
            coin_price = coin_df["close"].iloc[-1] if coin_df is not None else 0.0

            # Determine spread side based on Z-score sign
            # Negative Z -> LONG spread (buy COIN, sell PRIMARY)
            # Positive Z -> SHORT spread (sell COIN, buy PRIMARY)
            spread_side = (
                SpreadSide.LONG if result.current_z_score < 0 else SpreadSide.SHORT
            )

            event = EntrySignalEvent(
                coin_symbol=symbol,
                primary_symbol=self._primary_pair,
                spread_side=spread_side,
                z_score=result.current_z_score,
                beta=result.current_beta,
                correlation=result.current_correlation,
                hurst=hurst_values.get(symbol, 0.0),
                coin_price=coin_price,
                primary_price=primary_price,
                z_tp_threshold=z_tp,
                z_sl_threshold=z_sl,
            )

            await self._event_emitter.emit(event)
            signals_emitted += 1

            self._logger.info(
                f"📡 Emitted ENTRY_SIGNAL: {symbol} "
                f"({spread_side.value.upper()}) Z={result.current_z_score:.2f}"
            )

        return signals_emitted


class ScanCycleResult:
    """Result of a single scan cycle."""

    def __init__(
        self,
        symbols_scanned: int,
        symbols_after_filters: int,
        entry_candidates: int,
        signals_emitted: int,
    ):
        self.symbols_scanned = symbols_scanned
        self.symbols_after_filters = symbols_after_filters
        self.entry_candidates = entry_candidates
        self.signals_emitted = signals_emitted
