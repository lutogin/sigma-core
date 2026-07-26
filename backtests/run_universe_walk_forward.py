#!/usr/bin/env python3
"""
Universe Walk-Forward Backtest Runner.

Real walk-forward test with coin selection:
1. Train phase: Run backtest for ALL coins, rank by net metrics
2. Trade phase: Test ONLY selected topK coins, sum portfolio results
3. Kill switch: Disable coin if lossStreak >= 3 or netPnl < -1R within trade window

This simulates the real process of selecting coins based on past performance
and trading them in the next period WITHOUT lookahead bias.

Usage:
    python run_universe_walk_forward.py --start 2025-07-01 --end 2026-01-01
    python run_universe_walk_forward.py --start 2025-07-01 --end 2026-01-01 --trainDays 30 --tradeDays 7 --topK 10
"""

import argparse
import asyncio
import json
import sys
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import pandas as pd

# Add the parent directory to sys.path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.domain.data_loader.async_data_loader import AsyncDataLoaderService
from src.infra.container import Container

try:
    from backtests import run_backtest as run_backtest_module
    from backtests.backtest_shared import (
        build_backtest_config_kwargs,
        build_backtest_services,
    )
    from backtests.run_backtest import (
        BacktestConfig,
        BacktestResult,
        HistoricalFundingCache,
        StatArbBacktest,
        Trade,
    )
except ImportError:
    import run_backtest as run_backtest_module
    from backtest_shared import (
        build_backtest_config_kwargs,
        build_backtest_services,
    )
    from run_backtest import (
        BacktestConfig,
        BacktestResult,
        HistoricalFundingCache,
        StatArbBacktest,
        Trade,
    )


class _SilentLogger:
    """No-op logger for high-volume inner backtests."""

    def _noop(self, *args, **kwargs):
        return None

    def __getattr__(self, _name):
        return self._noop


class _InMemoryOHLCVCacheLoader:
    """
    Range-aware in-memory cache wrapper for AsyncDataLoaderService.

    Rolling train windows overlap heavily. Keeping covered time ranges avoids
    re-reading the same ETH/coin rows for every window and coalesces concurrent
    lazy 1m requests.
    """

    def __init__(self, base_loader: AsyncDataLoaderService):
        self._base_loader = base_loader
        self._single_cache: Dict[Tuple[str, str, int, str, str], Any] = {}
        self._range_cache: Dict[
            Tuple[str, str],
            Tuple[datetime, datetime, Any],
        ] = {}
        self._range_locks: Dict[Tuple[str, str], asyncio.Lock] = {}

    @staticmethod
    def _dt_key(value: Optional[datetime]) -> str:
        if value is None:
            return ""
        return value.isoformat()

    @staticmethod
    def _safe_copy(df):
        return df.copy(deep=True) if hasattr(df, "copy") else df

    @classmethod
    def _slice(
        cls,
        df,
        start_time: datetime,
        end_time: datetime,
    ):
        if df is None or not hasattr(df, "index"):
            return cls._safe_copy(df)
        selected = df.loc[(df.index >= start_time) & (df.index < end_time)]
        return cls._safe_copy(selected)

    async def prime_ohlcv_bulk(
        self,
        symbols: List[str],
        start_time: datetime,
        end_time: datetime,
        timeframe: str = "15m",
    ) -> None:
        """Load the complete WF range once before overlapping train windows."""
        fetched = await self._base_loader.load_ohlcv_bulk(
            symbols=symbols,
            start_time=start_time,
            end_time=end_time,
            batch_size=10,
            timeframe=timeframe,
        )
        for symbol, df in fetched.items():
            self._range_cache[(symbol, timeframe)] = (
                start_time,
                end_time,
                self._safe_copy(df),
            )

    async def load_ohlcv_bulk(
        self,
        symbols: List[str],
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 10,
        timeframe: str = "15m",
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        missing: List[str] = []

        for symbol in symbols:
            cached = self._range_cache.get((symbol, timeframe))
            if cached is not None and cached[0] <= start_time and cached[1] >= end_time:
                result[symbol] = self._slice(cached[2], start_time, end_time)
            else:
                missing.append(symbol)

        if missing:
            loaded = await asyncio.gather(
                *(
                    self.load_ohlcv_with_cache(
                        symbol=symbol,
                        num_bars=0,
                        timeframe=timeframe,
                        start_time=start_time,
                        end_time=end_time,
                    )
                    for symbol in missing
                )
            )
            for symbol, df in zip(missing, loaded):
                if df is not None:
                    result[symbol] = df

        return result

    async def load_ohlcv_with_cache(
        self,
        symbol: str,
        num_bars: int,
        timeframe: str = "15m",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ):
        if start_time is not None and end_time is not None:
            range_key = (symbol, timeframe)
            lock = self._range_locks.setdefault(range_key, asyncio.Lock())
            async with lock:
                cached = self._range_cache.get(range_key)
                if (
                    cached is not None
                    and cached[0] <= start_time
                    and cached[1] >= end_time
                ):
                    return self._slice(cached[2], start_time, end_time)

                if cached is None:
                    df = await self._base_loader.load_ohlcv_with_cache(
                        symbol=symbol,
                        num_bars=num_bars,
                        timeframe=timeframe,
                        start_time=start_time,
                        end_time=end_time,
                    )
                    self._range_cache[range_key] = (
                        start_time,
                        end_time,
                        self._safe_copy(df),
                    )
                    return self._safe_copy(df)

                covered_start, covered_end, covered_df = cached
                pieces = [covered_df]
                new_start = min(start_time, covered_start)
                new_end = max(end_time, covered_end)

                if start_time < covered_start:
                    pieces.append(
                        await self._base_loader.load_ohlcv_with_cache(
                            symbol=symbol,
                            num_bars=0,
                            timeframe=timeframe,
                            start_time=start_time,
                            end_time=covered_start,
                        )
                    )
                if end_time > covered_end:
                    pieces.append(
                        await self._base_loader.load_ohlcv_with_cache(
                            symbol=symbol,
                            num_bars=0,
                            timeframe=timeframe,
                            start_time=covered_end,
                            end_time=end_time,
                        )
                    )

                valid_pieces = [
                    piece
                    for piece in pieces
                    if piece is not None and not getattr(piece, "empty", False)
                ]
                if valid_pieces:
                    merged = pd.concat(valid_pieces).sort_index()
                    merged = merged[~merged.index.duplicated(keep="last")]
                else:
                    merged = covered_df
                self._range_cache[range_key] = (new_start, new_end, merged)
                return self._slice(merged, start_time, end_time)

        key = (
            symbol,
            timeframe,
            int(num_bars),
            self._dt_key(start_time),
            self._dt_key(end_time),
        )
        cached = self._single_cache.get(key)
        if cached is not None:
            return self._safe_copy(cached)

        df = await self._base_loader.load_ohlcv_with_cache(
            symbol=symbol,
            num_bars=num_bars,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
        )
        self._single_cache[key] = self._safe_copy(df)
        return self._safe_copy(df)


_ORIGINAL_RUN_BACKTEST_PRINT = getattr(run_backtest_module, "print", print)


def _set_run_backtest_print_enabled(enabled: bool) -> None:
    """
    Enable/disable noisy print output inside run_backtest module.

    Using redirect_stdout per task is not safe with parallel asyncio workers because
    stdout is process-global and tasks can interleave while awaiting.
    """
    if enabled:
        run_backtest_module.print = _ORIGINAL_RUN_BACKTEST_PRINT
    else:
        run_backtest_module.print = lambda *args, **kwargs: None


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class CoinTrainResult:
    """Training phase result for a single coin."""

    coin: str
    symbol: str
    net_pnl: float
    gross_pnl: float
    total_trades: int
    winning_trades: int
    win_rate: float
    max_drawdown: float
    max_drawdown_pct: float
    sharpe_ratio: float
    profit_factor: float
    avg_trade_pnl: float
    costs: float  # fees + negative funding
    score: float  # ranking score (net_pnl / max(1, abs(max_drawdown)))
    trades: List[Trade] = field(default_factory=list)


@dataclass
class CoinTradeResult:
    """Trade phase result for a single coin."""

    coin: str
    symbol: str
    net_pnl: float
    total_trades: int
    winning_trades: int
    win_rate: float
    max_drawdown: float
    was_killed: bool  # True if coin was disabled by kill switch
    kill_reason: Optional[str] = None  # "LOSS_STREAK" or "NEGATIVE_PNL"
    trades: List[Trade] = field(default_factory=list)


@dataclass
class WFStepResult:
    """Result for a single walk-forward step (train + trade)."""

    step_num: int
    train_start: datetime
    train_end: datetime
    trade_start: datetime
    trade_end: datetime

    # Training phase
    train_results: List[CoinTrainResult]
    selected_coins: List[str]  # topK coins selected for trading
    selection_scores: Dict[str, float]  # coin -> score

    # Trading phase
    trade_results: List[CoinTradeResult]
    portfolio_pnl: float
    portfolio_dd: float


@dataclass
class UniverseWFResult:
    """Final result of universe walk-forward test."""

    start_date: datetime
    end_date: datetime
    train_days: int
    trade_days: int
    top_k: int
    min_trades_train: int
    rank_metric: str

    steps: List[WFStepResult]

    # Aggregated metrics
    total_portfolio_pnl: float
    total_trades: int
    max_portfolio_dd: float
    coin_selection_turnover: float  # How often coins change between steps

    # Train-only selection ending exactly at the result timestamp. This is the
    # candidate set for the next live period; historical OOS steps qualify it.
    live_selection_start: datetime
    live_selection_end: datetime
    live_selection_results: List[CoinTrainResult]
    live_selected_coins: List[str]
    live_selection_scores: Dict[str, float]


# =============================================================================
# Universe Walk-Forward Runner
# =============================================================================


class UniverseWalkForwardRunner:
    """
    Run walk-forward test with coin selection.

    Process:
    1. For each WF step:
       a. Train: Run backtest for ALL coins on train window
       b. Rank: Filter by minTrades/netPnl, rank by score
       c. Select: Pick topK coins
       d. Trade: Run backtest for selected coins on trade window
       e. Kill switch: Disable coins with lossStreak >= 3 or netPnl < -1R
    2. Aggregate portfolio results
    """

    def __init__(
        self,
        services: Dict,
        base_config: BacktestConfig,
        coins: List[str],
        train_days: int = 60,
        trade_days: int = 14,
        top_k: int = 5,
        min_trades_train: int = 3,
        rank_metric: str = "netPnL",
        kill_loss_streak: int = 3,
        kill_negative_r: float = 1.0,
        workers: int = 6,
        allow_negative_train_selection: bool = False,
        allow_sparse_train_selection: bool = False,
        verbose_coin_backtests: bool = False,
    ):
        self.services = services
        self.base_config = base_config
        self.coins = coins
        self.train_days = train_days
        self.trade_days = trade_days
        self.top_k = top_k
        self.min_trades_train = min_trades_train
        self.rank_metric = rank_metric
        self.kill_loss_streak = kill_loss_streak
        self.kill_negative_r = kill_negative_r
        self.workers = workers
        self.allow_negative_train_selection = allow_negative_train_selection
        self.allow_sparse_train_selection = allow_sparse_train_selection
        self.verbose_coin_backtests = verbose_coin_backtests

        # Silence inner backtest logs by default to keep universe WF readable/fast.
        _set_run_backtest_print_enabled(self.verbose_coin_backtests)

        # Shared cached OHLCV loader to avoid repeated DB/cache reads
        # for identical symbol/window requests across coins.
        loader_logger = (
            self.services["logger"] if self.verbose_coin_backtests else _SilentLogger()
        )
        shared_loader = AsyncDataLoaderService(
            logger=loader_logger,
            exchange_client=self.services["exchange"],
            ohlcv_repository=self.services["ohlcv_repository"],
        )
        self._cached_data_loader = _InMemoryOHLCVCacheLoader(shared_loader)

        self.steps: List[WFStepResult] = []
        self._print_lock = asyncio.Lock()

    def _base_r_value(self) -> float:
        """Base risk unit for kill-switch thresholding."""
        if self.base_config.position_size_usdt > 0:
            return self.base_config.position_size_usdt
        return self.base_config.initial_balance * self.base_config.position_size_pct

    def _apply_online_kill_switch(
        self,
        trades: List[Trade],
    ) -> Tuple[List[Trade], bool, Optional[str]]:
        """Keep trades only until the independent coin account is disabled."""
        if not trades:
            return trades, False, None

        ordered_trades = sorted(trades, key=lambda trade: trade.exit_time)
        kept: List[Trade] = []
        running_pnl = 0.0
        loss_streak = 0
        kill_floor = -self.kill_negative_r * max(
            1e-6,
            self._base_r_value(),
        )

        for trade in ordered_trades:
            kept.append(trade)
            running_pnl += trade.pnl
            loss_streak = loss_streak + 1 if trade.pnl < 0 else 0

            if loss_streak >= self.kill_loss_streak:
                return kept, True, f"LOSS_STREAK_{loss_streak}"
            if running_pnl < kill_floor:
                return kept, True, f"NEGATIVE_PNL_{running_pnl:.0f}"

        return kept, False, None

    def _rebuild_trade_metrics(
        self,
        trades: List[Trade],
    ) -> Tuple[float, int, int, float, float]:
        """Recalculate net pnl/trades/winrate/max drawdown from trade list."""
        if not trades:
            return 0.0, 0, 0, 0.0, 0.0

        net_pnl = sum(t.pnl for t in trades)
        total_trades = len(trades)
        winning_trades = sum(1 for t in trades if t.pnl > 0)
        win_rate = (winning_trades / total_trades * 100.0) if total_trades > 0 else 0.0

        # Trade-level equity curve drawdown
        cumulative = 0.0
        peak = 0.0
        max_drawdown = 0.0
        for trade in trades:
            cumulative += trade.pnl
            peak = max(peak, cumulative)
            max_drawdown = min(max_drawdown, cumulative - peak)

        return net_pnl, total_trades, winning_trades, win_rate, max_drawdown

    async def run(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> UniverseWFResult:
        """Run the full walk-forward test."""
        print("\n" + "=" * 100)
        print("🚀 UNIVERSE WALK-FORWARD BACKTEST")
        print(f"   Period: {start_date.date()} to {end_date.date()}")
        print(f"   Train: {self.train_days} days | Trade: {self.trade_days} days")
        print(f"   TopK: {self.top_k} | MinTrades: {self.min_trades_train}")
        print(f"   Rank Metric: {self.rank_metric}")
        print(
            f"   Kill Switch: lossStreak >= {self.kill_loss_streak} or PnL < -{self.kill_negative_r}R"
        )
        print(
            f"   Selection: allow_negative_train={'ON' if self.allow_negative_train_selection else 'OFF'}"
        )
        print(
            f"   Sparse fallback: {'ON' if self.allow_sparse_train_selection else 'OFF'}"
        )
        print(f"   Parallel workers: {self.workers}")
        print(
            f"   Execution Path: trailing={'ON' if self.base_config.use_trailing_entry else 'OFF'}, "
            f"live_exit={'ON' if self.base_config.use_live_exit else 'OFF'}"
        )
        print(
            f"   1m Data Loading: {'LAZY (on-demand)' if self.base_config.lazy_load_minute_data else 'EAGER (preload all)'}"
        )
        print(
            f"   Coin backtest logs: {'VERBOSE' if self.verbose_coin_backtests else 'QUIET'}"
        )
        print(f"   Coins: {len(self.coins)}")
        print("=" * 100 + "\n")

        settings = self.services["settings"]
        warmup_days = settings.LOOKBACK_WINDOW_DAYS * 3 + 2
        if settings.ENABLE_STABILITY_FILTER and settings.STABILITY_WINDOWS_DAYS:
            warmup_days = max(
                warmup_days,
                max(settings.STABILITY_WINDOWS_DAYS) + 2,
            )
        if settings.ENABLE_BETA_DRIFT_GUARD:
            warmup_days = max(
                warmup_days,
                settings.LOOKBACK_WINDOW_DAYS + settings.BETA_DRIFT_LONG_DAYS + 2,
            )
        preload_started = time.perf_counter()
        await self._cached_data_loader.prime_ohlcv_bulk(
            symbols=[
                settings.PRIMARY_PAIR,
                *(f"{coin}/USDT:USDT" for coin in self.coins),
            ],
            start_time=start_date - timedelta(days=warmup_days),
            end_time=end_date,
            timeframe=settings.TIMEFRAME,
        )
        print(
            f"📦 Primed shared 15m range cache in "
            f"{time.perf_counter() - preload_started:.1f}s\n"
        )

        # Generate WF windows
        self.steps = []
        windows = self._generate_windows(start_date, end_date)
        print(f"📅 Generated {len(windows)} walk-forward steps\n")

        # Run each step
        for step_num, (train_start, train_end, trade_start, trade_end) in enumerate(
            windows, 1
        ):
            print(f"\n{'='*80}")
            print(f"📊 STEP {step_num}/{len(windows)}")
            print(f"   Train: {train_start.date()} → {train_end.date()}")
            print(f"   Trade: {trade_start.date()} → {trade_end.date()}")
            print("=" * 80)

            step_result = await self._run_step(
                step_num=step_num,
                train_start=train_start,
                train_end=train_end,
                trade_start=trade_start,
                trade_end=trade_end,
            )
            self.steps.append(step_result)

            # Print step summary
            self._print_step_summary(step_result)

        # Build a current train-only selection for the period immediately after
        # this result. Historical OOS performance is used later as a gate, not
        # as the source of pair identities.
        live_selection_start = end_date - timedelta(days=self.train_days)
        print("\n" + "=" * 80)
        print("🔭 NEXT LIVE SELECTION")
        print(f"   Train only: {live_selection_start.date()} → {end_date.date()}")
        print("=" * 80)
        live_selection_results = await self._run_train_phase(
            live_selection_start,
            end_date,
        )
        live_selected_coins, live_selection_scores = self._select_coins(
            live_selection_results
        )
        print(
            "   Current candidates: "
            + (", ".join(live_selected_coins) if live_selected_coins else "none")
        )

        # Calculate final results
        result = self._calculate_final_results(
            start_date,
            end_date,
            live_selection_start=live_selection_start,
            live_selection_results=live_selection_results,
            live_selected_coins=live_selected_coins,
            live_selection_scores=live_selection_scores,
        )

        # Print final report
        self._print_final_report(result)

        return result

    def _generate_windows(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> List[Tuple[datetime, datetime, datetime, datetime]]:
        """
        Generate rolling WF windows.

        Returns list of (train_start, train_end, trade_start, trade_end) tuples.
        Uses exclusive end dates for clarity.
        """
        windows = []

        # First trade window starts after first train window
        t = start_date + timedelta(days=self.train_days)

        while t + timedelta(days=self.trade_days) <= end_date:
            train_start = t - timedelta(days=self.train_days)
            train_end = t  # exclusive
            trade_start = t
            trade_end = t + timedelta(days=self.trade_days)  # exclusive

            windows.append((train_start, train_end, trade_start, trade_end))

            # Move to next step
            t = trade_end

        return windows

    async def _run_step(
        self,
        step_num: int,
        train_start: datetime,
        train_end: datetime,
        trade_start: datetime,
        trade_end: datetime,
    ) -> WFStepResult:
        """Run a single WF step: train → select → trade."""
        step_started = time.perf_counter()

        # Phase 1: Train - run backtest for ALL coins
        print(f"\n📈 TRAIN PHASE: Testing {len(self.coins)} coins...")
        train_started = time.perf_counter()
        train_results = await self._run_train_phase(train_start, train_end)
        train_elapsed = time.perf_counter() - train_started

        # Phase 2: Select - filter and rank coins
        selected_coins, selection_scores = self._select_coins(train_results)
        print(f"\n🎯 SELECTED {len(selected_coins)} coins: {', '.join(selected_coins)}")

        # Phase 3: Trade - run backtest for selected coins only
        print(f"\n📊 TRADE PHASE: Testing {len(selected_coins)} selected coins...")
        trade_started = time.perf_counter()
        trade_results = await self._run_trade_phase(
            trade_start, trade_end, selected_coins
        )
        trade_elapsed = time.perf_counter() - trade_started

        # Each coin intentionally has an independent account so its OOS
        # quality is not affected by another coin's signal ordering.
        portfolio_pnl = sum(r.net_pnl for r in trade_results)
        portfolio_dd = min((r.max_drawdown for r in trade_results), default=0)

        step_elapsed = time.perf_counter() - step_started
        print(
            f"⏱️ Step timing: total={step_elapsed:.1f}s | "
            f"train={train_elapsed:.1f}s | trade={trade_elapsed:.1f}s"
        )

        return WFStepResult(
            step_num=step_num,
            train_start=train_start,
            train_end=train_end,
            trade_start=trade_start,
            trade_end=trade_end,
            train_results=train_results,
            selected_coins=selected_coins,
            selection_scores=selection_scores,
            trade_results=trade_results,
            portfolio_pnl=portfolio_pnl,
            portfolio_dd=portfolio_dd,
        )

    async def _run_train_phase(
        self,
        train_start: datetime,
        train_end: datetime,
    ) -> List[CoinTrainResult]:
        """Run backtest for all coins in parallel."""
        semaphore = asyncio.Semaphore(self.workers)

        async def run_coin(
            coin: str, idx: int
        ) -> Tuple[
            Literal["with_trades", "no_trades", "error"], Optional[CoinTrainResult]
        ]:
            async with semaphore:
                try:
                    result = await self._run_single_coin_backtest(
                        coin, train_start, train_end
                    )
                    if result and result.total_trades > 0:
                        reliability = result.total_trades / (
                            result.total_trades + max(3, self.min_trades_train)
                        )
                        if self.rank_metric == "netSharpe":
                            raw_score = result.sharpe_ratio
                        else:  # netPnL (default)
                            dd_divisor = max(1.0, abs(result.max_drawdown))
                            raw_score = result.total_pnl / dd_divisor
                        score = raw_score * reliability

                        estimated_costs = sum(
                            trade.fees + max(0.0, -trade.funding_pnl)
                            for trade in result.trades
                        )
                        gross_pnl_estimated = sum(
                            trade.pnl + trade.fees - trade.funding_pnl
                            for trade in result.trades
                        )

                        train_result = CoinTrainResult(
                            coin=coin,
                            symbol=f"{coin}/USDT:USDT",
                            net_pnl=result.total_pnl,
                            gross_pnl=gross_pnl_estimated,
                            total_trades=result.total_trades,
                            winning_trades=result.winning_trades,
                            win_rate=result.win_rate,
                            max_drawdown=result.max_drawdown,
                            max_drawdown_pct=result.max_drawdown_pct,
                            sharpe_ratio=result.sharpe_ratio,
                            profit_factor=result.profit_factor,
                            avg_trade_pnl=result.avg_trade_pnl,
                            costs=estimated_costs,
                            score=score,
                            trades=result.trades,
                        )

                        async with self._print_lock:
                            emoji = "🟢" if result.total_pnl > 0 else "🔴"
                            print(
                                f"  [{idx}/{len(self.coins)}] {emoji} {coin}: "
                                f"PnL=${result.total_pnl:+.2f} | "
                                f"Trades={result.total_trades} | "
                                f"Score={score:.2f}"
                            )

                        return "with_trades", train_result
                    else:
                        async with self._print_lock:
                            print(f"  [{idx}/{len(self.coins)}] ⚪ {coin}: No trades")
                        return "no_trades", None

                except Exception as e:
                    async with self._print_lock:
                        print(f"  [{idx}/{len(self.coins)}] ❌ {coin}: {e}")
                    return "error", None

        # Run all coins in parallel
        tasks = [run_coin(coin, i) for i, coin in enumerate(self.coins, 1)]
        coin_results = await asyncio.gather(*tasks)

        no_trades_count = sum(1 for status, _ in coin_results if status == "no_trades")
        error_count = sum(1 for status, _ in coin_results if status == "error")
        results = [
            r for status, r in coin_results if status == "with_trades" and r is not None
        ]
        print(
            f"  ↳ Train diagnostics: with_trades={len(results)} | "
            f"no_trades={no_trades_count} | errors={error_count}"
        )
        if error_count:
            raise RuntimeError(
                f"Train phase failed closed: {error_count} coin backtest(s) errored"
            )

        return results

    def _select_coins(
        self,
        train_results: List[CoinTrainResult],
    ) -> Tuple[List[str], Dict[str, float]]:
        """
        Filter and rank coins, return topK.

        Filters:
        - trades >= minTradesTrain
        - netPnl > 0 (optional but recommended)

        Ranking by score (netPnL / max(1, abs(maxDD)) or netSharpe).
        """
        if not train_results:
            return [], {}

        selected: List[CoinTrainResult] = []
        selected_set: set[str] = set()

        def _rank(items: List[CoinTrainResult]) -> List[CoinTrainResult]:
            return sorted(items, key=lambda r: r.score, reverse=True)

        def _add_ranked(items: List[CoinTrainResult]) -> int:
            added = 0
            for item in _rank(items):
                if item.coin in selected_set:
                    continue
                selected.append(item)
                selected_set.add(item.coin)
                added += 1
                if len(selected) >= self.top_k:
                    break
            return added

        strict_positive = [
            r
            for r in train_results
            if r.total_trades >= self.min_trades_train and r.net_pnl > 0
        ]
        strict_added = _add_ranked(strict_positive)

        # For sparse strategies, fill remaining slots with profitable candidates
        # that have at least one trade in train.
        if len(selected) < self.top_k and self.allow_sparse_train_selection:
            relaxed_positive = [
                r for r in train_results if r.total_trades >= 1 and r.net_pnl > 0
            ]
            relaxed_added = _add_ranked(relaxed_positive)
            if relaxed_added > 0 and self.min_trades_train > 1:
                print(
                    f"  ℹ️ Selection fallback: backfilled topK with profitable coins "
                    f"(trades>=1) due sparse strict candidates (strict={strict_added})."
                )

        # Optional negative-PnL fallback (explicitly opt-in).
        if len(selected) < self.top_k and self.allow_negative_train_selection:
            non_positive = [r for r in train_results if r.total_trades >= 1]
            negative_added = _add_ranked(non_positive)
            if negative_added > 0:
                print(
                    "  ⚠️ Selection fallback: included non-positive train PnL candidates "
                    "(--allow-negative-train-selection=true)."
                )

        if not selected:
            print(
                "  ⚠️ Selection produced 0 candidates: no train-window trades passed "
                "current criteria."
            )

        coins = [r.coin for r in selected]
        scores = {r.coin: r.score for r in selected}

        return coins, scores

    async def _run_trade_phase(
        self,
        trade_start: datetime,
        trade_end: datetime,
        selected_coins: List[str],
    ) -> List[CoinTradeResult]:
        """Run each selected coin on its own account, then apply its kill switch."""
        semaphore = asyncio.Semaphore(self.workers)

        async def run_coin(
            coin: str,
            idx: int,
        ) -> Optional[CoinTradeResult]:
            async with semaphore:
                try:
                    result = await self._run_single_coin_backtest(
                        coin,
                        trade_start,
                        trade_end,
                    )
                    if result is None:
                        return None

                    (
                        effective_trades,
                        was_killed,
                        kill_reason,
                    ) = self._apply_online_kill_switch(result.trades)
                    (
                        net_pnl,
                        total_trades,
                        winning_trades,
                        win_rate,
                        max_drawdown,
                    ) = self._rebuild_trade_metrics(effective_trades)

                    trade_result = CoinTradeResult(
                        coin=coin,
                        symbol=f"{coin}/USDT:USDT",
                        net_pnl=net_pnl,
                        total_trades=total_trades,
                        winning_trades=winning_trades,
                        win_rate=win_rate,
                        max_drawdown=max_drawdown,
                        was_killed=was_killed,
                        kill_reason=kill_reason,
                        trades=effective_trades,
                    )
                    async with self._print_lock:
                        emoji = "🟢" if net_pnl > 0 else "🔴"
                        kill_str = f" ⚠️ KILLED: {kill_reason}" if was_killed else ""
                        print(
                            f"  [{idx}/{len(selected_coins)}] {emoji} {coin}: "
                            f"PnL=${net_pnl:+.2f} | "
                            f"Trades={total_trades}{kill_str}"
                        )
                    return trade_result
                except Exception as exc:
                    async with self._print_lock:
                        print(f"  [{idx}/{len(selected_coins)}] ❌ {coin}: {exc}")
                    raise RuntimeError(f"OOS backtest failed for {coin}") from exc

        coin_results = await asyncio.gather(
            *(run_coin(coin, idx) for idx, coin in enumerate(selected_coins, 1))
        )
        return [result for result in coin_results if result is not None]

    def _build_isolated_backtest_services(
        self,
        config: Optional[BacktestConfig] = None,
    ) -> Dict[str, Any]:
        """
        Build per-run service instances.

        Service instances stay isolated between train coins and OOS windows.
        """
        settings = self.services["settings"]
        logger = (
            self.services["logger"] if self.verbose_coin_backtests else _SilentLogger()
        )
        return build_backtest_services(
            settings=settings,
            logger=logger,
            exchange_client=self.services["exchange"],
            ohlcv_repository=self.services["ohlcv_repository"],
            config=config or self.base_config,
            data_loader_override=self._cached_data_loader,
            funding_cache=self.services.get("funding_cache"),
        )

    async def _run_single_coin_backtest(
        self,
        coin: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Optional[BacktestResult]:
        """Run backtest for a single coin."""
        symbol = f"{coin}/USDT:USDT"
        effective_end = end_date - timedelta(seconds=1)

        # Copy base strategy config and override only per-coin specifics.
        config = replace(
            self.base_config,
            max_spreads=1,
            consistent_pairs=[symbol],
        )

        # Create backtester with isolated stateful services
        local_services = self._build_isolated_backtest_services(config)
        backtester = StatArbBacktest(config=config, **local_services)

        return await backtester.run(start_date, effective_end)

    def _print_step_summary(self, step: WFStepResult) -> None:
        """Print summary for a WF step."""
        print(f"\n📋 STEP {step.step_num} SUMMARY:")
        print(f"   Train: {len(step.train_results)} coins tested")
        print(f"   Selected: {', '.join(step.selected_coins)}")
        print(f"   Trade PnL: ${step.portfolio_pnl:+.2f}")

        # Show killed coins
        killed = [r for r in step.trade_results if r.was_killed]
        if killed:
            print(f"   ⚠️ Killed: {', '.join(r.coin for r in killed)}")

    def _calculate_final_results(
        self,
        start_date: datetime,
        end_date: datetime,
        *,
        live_selection_start: datetime,
        live_selection_results: List[CoinTrainResult],
        live_selected_coins: List[str],
        live_selection_scores: Dict[str, float],
    ) -> UniverseWFResult:
        """Calculate aggregated results."""
        total_pnl = sum(step.portfolio_pnl for step in self.steps)
        total_trades = sum(
            sum(r.total_trades for r in s.trade_results) for s in self.steps
        )
        max_dd = min((step.portfolio_dd for step in self.steps), default=0)

        # Calculate turnover (how often coins change)
        turnover = 0.0
        if len(self.steps) > 1:
            changes = 0
            for i in range(1, len(self.steps)):
                prev_set = set(self.steps[i - 1].selected_coins)
                curr_set = set(self.steps[i].selected_coins)
                changes += len(prev_set.symmetric_difference(curr_set))
            turnover = changes / (len(self.steps) - 1) / self.top_k

        return UniverseWFResult(
            start_date=start_date,
            end_date=end_date,
            train_days=self.train_days,
            trade_days=self.trade_days,
            top_k=self.top_k,
            min_trades_train=self.min_trades_train,
            rank_metric=self.rank_metric,
            steps=self.steps,
            total_portfolio_pnl=total_pnl,
            total_trades=total_trades,
            max_portfolio_dd=max_dd,
            coin_selection_turnover=turnover,
            live_selection_start=live_selection_start,
            live_selection_end=end_date,
            live_selection_results=live_selection_results,
            live_selected_coins=live_selected_coins,
            live_selection_scores=live_selection_scores,
        )

    def _print_final_report(self, result: UniverseWFResult) -> None:
        """Print final walk-forward report."""
        print("\n" + "=" * 100)
        print("                         UNIVERSE WALK-FORWARD FINAL REPORT")
        print("=" * 100)

        print(f"\n📅 Period: {result.start_date.date()} → {result.end_date.date()}")
        print(f"   Train: {result.train_days} days | Trade: {result.trade_days} days")
        print(f"   TopK: {result.top_k} | MinTrades: {result.min_trades_train}")
        print(f"   Rank Metric: {result.rank_metric}")

        print("\n" + "-" * 100)
        print("STEP-BY-STEP RESULTS")
        print("-" * 100)
        print(
            f"{'Step':<6} | {'Train Window':<25} | {'Trade Window':<25} | "
            f"{'Selected':<30} | {'PnL':>12}"
        )
        print("-" * 100)

        for step in result.steps:
            train_str = f"{step.train_start.date()} → {step.train_end.date()}"
            trade_str = f"{step.trade_start.date()} → {step.trade_end.date()}"
            coins_str = ", ".join(step.selected_coins[:3])
            if len(step.selected_coins) > 3:
                coins_str += f" +{len(step.selected_coins) - 3}"

            if step.portfolio_pnl > 0:
                emoji = "🟢"
            elif step.portfolio_pnl < 0:
                emoji = "🔴"
            else:
                emoji = "⚪"
            print(
                f"{step.step_num:<6} | {train_str:<25} | {trade_str:<25} | "
                f"{coins_str:<30} | {emoji} ${step.portfolio_pnl:>+10.2f}"
            )

        print("-" * 100)

        # Aggregated metrics
        print("\n" + "=" * 100)
        print("PORTFOLIO SUMMARY")
        print("=" * 100)
        print(f"  Total Portfolio PnL:     ${result.total_portfolio_pnl:+,.2f}")
        print(f"  Total Trades:            {result.total_trades}")
        print(f"  Max Portfolio Drawdown:  ${result.max_portfolio_dd:,.2f}")
        print(f"  Selection Turnover:      {result.coin_selection_turnover * 100:.1f}%")
        print(
            "  Next Live Candidates:    "
            + (
                ", ".join(result.live_selected_coins)
                if result.live_selected_coins
                else "none"
            )
        )

        # Keep no-trade/zero-PnL windows separate from actual losing windows.
        profitable_steps = sum(1 for s in result.steps if s.portfolio_pnl > 0)
        losing_steps = sum(1 for s in result.steps if s.portfolio_pnl < 0)
        flat_steps = len(result.steps) - profitable_steps - losing_steps
        print(f"  Profitable Steps:        {profitable_steps}/{len(result.steps)}")
        print(f"  Losing Steps:            {losing_steps}/{len(result.steps)}")
        print(f"  Flat/No-trade Steps:     {flat_steps}/{len(result.steps)}")

        # Most selected coins
        coin_counts: Dict[str, int] = {}
        for step in result.steps:
            for coin in step.selected_coins:
                coin_counts[coin] = coin_counts.get(coin, 0) + 1

        print("\n" + "-" * 100)
        print("MOST FREQUENTLY SELECTED COINS")
        print("-" * 100)
        sorted_coins = sorted(coin_counts.items(), key=lambda x: x[1], reverse=True)
        for coin, count in sorted_coins[:15]:
            pct = count / len(result.steps) * 100
            print(
                f"  {coin:<15} | Selected {count}/{len(result.steps)} steps ({pct:.0f}%)"
            )

        # Coins that were killed
        killed_coins: Dict[str, int] = {}
        for step in result.steps:
            for r in step.trade_results:
                if r.was_killed:
                    killed_coins[r.coin] = killed_coins.get(r.coin, 0) + 1

        if killed_coins:
            print("\n" + "-" * 100)
            print("COINS KILLED BY KILL SWITCH")
            print("-" * 100)
            for coin, count in sorted(
                killed_coins.items(), key=lambda x: x[1], reverse=True
            ):
                print(f"  {coin:<15} | Killed {count} times")

        best_period = self._build_best_coins_over_period(result, top_n=10)
        if best_period:
            print("\n" + "-" * 100)
            print("BEST COINS OVER PERIOD (OOS shortlist)")
            print("-" * 100)
            for item in best_period:
                print(
                    f"  {item['coin']:<15} | "
                    f"selection_rate={item['selection_rate']:.1%} | "
                    f"trade_pnl=${item['trade_net_pnl']:+.2f} | "
                    f"trades={item['trade_total_trades']}"
                )
        else:
            print("\n" + "-" * 100)
            print("BEST COINS OVER PERIOD (OOS shortlist)")
            print("-" * 100)
            print("  No positive out-of-sample coin results on selected windows.")

        best_train = self._build_best_train_candidates_over_period(result, top_n=10)
        if best_train:
            print("\n" + "-" * 100)
            print("BEST TRAIN CANDIDATES OVER PERIOD")
            print("-" * 100)
            for item in best_train:
                print(
                    f"  {item['coin']:<15} | "
                    f"selection_rate={item['selection_rate']:.1%} | "
                    f"train_pnl=${item['train_net_pnl']:+.2f} | "
                    f"train_steps={item['train_steps_tested']}"
                )

        ranked_fallback = self._build_ranked_fallback_over_period(result, top_n=10)
        if ranked_fallback:
            print("\n" + "-" * 100)
            print("RANKED FALLBACK OVER PERIOD")
            print("-" * 100)
            for item in ranked_fallback:
                print(
                    f"  {item['coin']:<15} | "
                    f"selection_rate={item['selection_rate']:.1%} | "
                    f"trade_pnl=${item['trade_net_pnl']:+.2f} | "
                    f"train_pnl=${item['train_net_pnl']:+.2f} | "
                    f"trade_trades={item['trade_total_trades']}"
                )

    def _build_coin_period_stats(
        self, result: UniverseWFResult
    ) -> List[Dict[str, Any]]:
        """Build per-coin aggregate stats across all WF steps."""
        total_steps = max(1, len(result.steps))
        stats: Dict[str, Dict[str, Any]] = {}

        def ensure_coin(coin: str) -> Dict[str, Any]:
            if coin not in stats:
                stats[coin] = {
                    "coin": coin,
                    "train_steps_tested": 0,
                    "train_net_pnl": 0.0,
                    "train_avg_score_all": 0.0,
                    "selected_steps": 0,
                    "selection_rate": 0.0,
                    "avg_train_score_selected": 0.0,
                    "trade_steps": 0,
                    "trade_total_trades": 0,
                    "trade_winning_trades": 0,
                    "trade_win_rate": 0.0,
                    "trade_net_pnl": 0.0,
                    "killed_count": 0,
                }
            return stats[coin]

        for step in result.steps:
            selected_set = set(step.selected_coins)
            for train_result in step.train_results:
                row = ensure_coin(train_result.coin)
                row["train_steps_tested"] += 1
                row["train_net_pnl"] += train_result.net_pnl
                row["train_avg_score_all"] += train_result.score
                if train_result.coin in selected_set:
                    row["selected_steps"] += 1
                    row["avg_train_score_selected"] += train_result.score

            for trade_result in step.trade_results:
                row = ensure_coin(trade_result.coin)
                row["trade_steps"] += 1
                row["trade_total_trades"] += trade_result.total_trades
                row["trade_winning_trades"] += trade_result.winning_trades
                row["trade_net_pnl"] += trade_result.net_pnl
                if trade_result.was_killed:
                    row["killed_count"] += 1

        for row in stats.values():
            if row["train_steps_tested"] > 0:
                row["train_avg_score_all"] /= row["train_steps_tested"]
            if row["selected_steps"] > 0:
                row["avg_train_score_selected"] /= row["selected_steps"]
            row["selection_rate"] = row["selected_steps"] / total_steps
            if row["trade_total_trades"] > 0:
                row["trade_win_rate"] = (
                    row["trade_winning_trades"] / row["trade_total_trades"] * 100.0
                )
            else:
                row["trade_win_rate"] = 0.0

        return sorted(
            stats.values(),
            key=lambda r: (
                r["selection_rate"],
                r["trade_net_pnl"],
                r["trade_total_trades"],
            ),
            reverse=True,
        )

    def _build_best_coins_over_period(
        self, result: UniverseWFResult, top_n: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Build shortlist of best OOS (trade-window) coins across the full WF period.
        """
        period_stats = self._build_coin_period_stats(result)
        filtered = [
            row
            for row in period_stats
            if row["trade_total_trades"] > 0 and row["trade_net_pnl"] > 0
        ]
        filtered.sort(
            key=lambda r: (
                r["trade_net_pnl"],
                r["trade_win_rate"],
                r["selection_rate"],
                r["trade_total_trades"],
            ),
            reverse=True,
        )
        return filtered[:top_n]

    def _build_best_train_candidates_over_period(
        self, result: UniverseWFResult, top_n: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Build train-window shortlist.

        Useful when OOS shortlist is empty due very sparse signals.
        """
        period_stats = self._build_coin_period_stats(result)
        filtered = [
            row
            for row in period_stats
            if row["train_steps_tested"] > 0 and row["train_net_pnl"] > 0
        ]
        filtered.sort(
            key=lambda r: (
                r["selection_rate"],
                r["train_net_pnl"],
                r["avg_train_score_selected"],
            ),
            reverse=True,
        )
        return filtered[:top_n]

    def _build_ranked_fallback_over_period(
        self, result: UniverseWFResult, top_n: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Build always-available ranking list (including non-positive PnL).

        Useful when strict OOS shortlist is empty in sparse/hostile regimes.
        """
        period_stats = self._build_coin_period_stats(result)
        filtered = [row for row in period_stats if row["train_steps_tested"] > 0]
        filtered.sort(
            key=lambda r: (
                r["selection_rate"],
                r["trade_net_pnl"],
                r["train_net_pnl"],
                r["trade_total_trades"],
            ),
            reverse=True,
        )
        return filtered[:top_n]

    def save_results(self, result: UniverseWFResult, filepath: str) -> None:
        """Save results to JSON file."""
        coin_period_stats = self._build_coin_period_stats(result)
        best_coins = self._build_best_coins_over_period(result, top_n=20)
        best_train_candidates = self._build_best_train_candidates_over_period(
            result, top_n=20
        )
        ranked_fallback = self._build_ranked_fallback_over_period(result, top_n=20)

        data = {
            "metadata": {
                "start_date": result.start_date.isoformat(),
                "end_date": result.end_date.isoformat(),
                "train_days": result.train_days,
                "trade_days": result.trade_days,
                "top_k": result.top_k,
                "min_trades_train": result.min_trades_train,
                "rank_metric": result.rank_metric,
                "account_model": "independent_per_coin",
                "strategy_config": asdict(self.base_config),
                "execution_assumptions": {
                    "fee_rate": (
                        self.base_config.maker_fee
                        if self.base_config.use_limit_orders
                        else self.base_config.taker_fee
                    ),
                    "fee_type": (
                        "maker" if self.base_config.use_limit_orders else "taker"
                    ),
                    "half_spread_bps": self.base_config.half_spread_bps,
                    "slippage_bps": self.base_config.slippage_bps,
                },
                "universe_size": len(self.coins),
            },
            "summary": {
                "total_portfolio_pnl": result.total_portfolio_pnl,
                "total_trades": result.total_trades,
                "max_portfolio_dd": result.max_portfolio_dd,
                "coin_selection_turnover": result.coin_selection_turnover,
                "profitable_steps": sum(
                    1 for step in result.steps if step.portfolio_pnl > 0
                ),
                "losing_steps": sum(
                    1 for step in result.steps if step.portfolio_pnl < 0
                ),
                "flat_steps": sum(
                    1 for step in result.steps if step.portfolio_pnl == 0
                ),
            },
            "best_coins_over_period": best_coins,
            "best_train_candidates_over_period": best_train_candidates,
            "ranked_fallback_over_period": ranked_fallback,
            "coin_period_stats": coin_period_stats,
            "live_selection": {
                "train_start": result.live_selection_start.isoformat(),
                "train_end": result.live_selection_end.isoformat(),
                "selected_coins": result.live_selected_coins,
                "selection_scores": result.live_selection_scores,
                "train_results": [
                    {
                        "coin": row.coin,
                        "net_pnl": row.net_pnl,
                        "total_trades": row.total_trades,
                        "max_drawdown": row.max_drawdown,
                        "sharpe_ratio": row.sharpe_ratio,
                        "profit_factor": row.profit_factor,
                        "costs": row.costs,
                        "score": row.score,
                    }
                    for row in result.live_selection_results
                ],
            },
            "steps": [
                {
                    "step_num": s.step_num,
                    "train_start": s.train_start.isoformat(),
                    "train_end": s.train_end.isoformat(),
                    "trade_start": s.trade_start.isoformat(),
                    "trade_end": s.trade_end.isoformat(),
                    "selected_coins": s.selected_coins,
                    "selection_scores": s.selection_scores,
                    "portfolio_pnl": s.portfolio_pnl,
                    "portfolio_dd": s.portfolio_dd,
                    "trade_results": [
                        {
                            "coin": r.coin,
                            "net_pnl": r.net_pnl,
                            "total_trades": r.total_trades,
                            "win_rate": r.win_rate,
                            "was_killed": r.was_killed,
                            "kill_reason": r.kill_reason,
                        }
                        for r in s.trade_results
                    ],
                }
                for s in result.steps
            ],
        }

        output_path = Path(filepath)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        print(f"\n💾 Results saved to: {filepath}")


# =============================================================================
# Main Entry Point
# =============================================================================


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Universe Walk-Forward Backtest Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_universe_walk_forward.py --start 2025-07-01 --end 2026-01-01
  python run_universe_walk_forward.py --start 2025-07-01 --end 2026-01-01 --trainDays 30 --tradeDays 7
  python run_universe_walk_forward.py --start 2025-07-01 --end 2026-01-01 --topK 5 --rankMetric netSharpe
        """,
    )

    parser.add_argument(
        "--start", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", type=str, default=None, help="End date (YYYY-MM-DD). Default: today"
    )
    parser.add_argument(
        "--trainDays", type=int, default=60, help="Training window in days. Default: 60"
    )
    parser.add_argument(
        "--tradeDays", type=int, default=14, help="Trading window in days. Default: 14"
    )
    parser.add_argument(
        "--topK", type=int, default=5, help="Number of top coins to select. Default: 5"
    )
    parser.add_argument(
        "--minTradesTrain",
        type=int,
        default=3,
        help="Minimum trades in train phase to qualify. Default: 3",
    )
    parser.add_argument(
        "--rankMetric",
        type=str,
        default="netPnL",
        choices=["netPnL", "netSharpe"],
        help="Metric for ranking coins. Default: netPnL",
    )
    parser.add_argument(
        "--balance", type=float, default=10000.0, help="Initial balance. Default: 10000"
    )
    parser.add_argument(
        "--leverage", type=int, default=None, help="Leverage. Default: from settings"
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=None,
        help="Explicit settings file. Default: project .env",
    )
    parser.add_argument(
        "--half-spread-bps",
        type=float,
        default=2.0,
        help="Estimated half-spread paid per fill. Default: 2 bps",
    )
    parser.add_argument(
        "--slippage-bps",
        type=float,
        default=1.0,
        help="Additional adverse slippage per fill. Default: 1 bp",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        help="Number of parallel workers. Default: 6",
    )
    parser.add_argument(
        "--coinsFile",
        type=str,
        default="backtests/eth_ecosystem_universe.json",
        help="JSON file with coin list. Default: ETH ecosystem universe",
    )
    parser.add_argument(
        "--killLossStreak",
        type=int,
        default=3,
        help="Kill coin after N consecutive losses. Default: 3",
    )
    parser.add_argument(
        "--killNegativeR",
        type=float,
        default=1.0,
        help="Kill coin if PnL < -N*R (R = avg position size). Default: 1.0",
    )
    parser.add_argument(
        "--use-trailing-entry",
        type=str,
        default="true",
        help="Use trailing entry in helper backtests (true/false). Default: true",
    )
    parser.add_argument(
        "--use-live-exit",
        type=str,
        default="true",
        help="Use live-exit emulation (1m) in helper backtests (true/false). Default: true",
    )
    parser.add_argument(
        "--lazy-minute-data",
        type=str,
        default="true",
        help="Load 1m data lazily (on first watch/position) instead of eager preload (true/false). Default: true",
    )
    parser.add_argument(
        "--min-universe-volume-usdt",
        type=float,
        default=0.0,
        help="Optional 24h minimum volume filter for universe symbols. Default: 0 (disabled)",
    )
    parser.add_argument(
        "--allow-negative-train-selection",
        type=str,
        default="false",
        help="Allow fallback selection with non-positive train PnL (true/false). Default: false",
    )
    parser.add_argument(
        "--allow-sparse-train-selection",
        type=str,
        default="false",
        help="Backfill topK with positive one-trade candidates (true/false). Default: false",
    )
    parser.add_argument(
        "--verbose-coin-backtests",
        type=str,
        default="false",
        help="Show full inner logs for each coin backtest (true/false). Default: false",
    )

    args = parser.parse_args()

    if args.trainDays <= 0 or args.tradeDays <= 0:
        parser.error("--trainDays and --tradeDays must be positive")
    if args.topK <= 0 or args.minTradesTrain <= 0:
        parser.error("--topK and --minTradesTrain must be positive")
    if args.balance <= 0 or args.workers <= 0:
        parser.error("--balance and --workers must be positive")
    if args.killLossStreak <= 0 or args.killNegativeR < 0:
        parser.error("kill-switch values must be positive/non-negative")
    if args.half_spread_bps < 0 or args.slippage_bps < 0:
        parser.error("execution cost assumptions must be non-negative")

    def _parse_bool(value: str, flag_name: str) -> bool:
        normalized = value.strip().lower()
        if normalized in ("1", "true", "yes", "y", "on"):
            return True
        if normalized in ("0", "false", "no", "n", "off"):
            return False
        raise ValueError(f"Invalid value for {flag_name}: {value!r}. Use true/false.")

    # Parse dates
    start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if args.end:
        end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc)
    if start_date >= end_date:
        print("Error: start date must be before end date")
        sys.exit(1)

    # Load coins list
    coins_file = Path(args.coinsFile)
    if not coins_file.exists():
        print(f"Error: {args.coinsFile} not found")
        sys.exit(1)

    with coins_file.open(encoding="utf-8") as f:
        coins = json.load(f)
    if not isinstance(coins, list) or not all(
        isinstance(coin, str) and coin.strip() for coin in coins
    ):
        parser.error("--coinsFile must contain a JSON array of non-empty strings")
    coins = list(dict.fromkeys(coin.strip().upper() for coin in coins))

    print(f"Loaded {len(coins)} coins from {args.coinsFile}")

    # Initialize container
    container = Container().init(args.env_file)
    settings = container.settings
    logger = container.logger

    # Create base config
    try:
        use_trailing_entry = _parse_bool(
            args.use_trailing_entry, "--use-trailing-entry"
        )
        use_live_exit = _parse_bool(args.use_live_exit, "--use-live-exit")
        lazy_minute_data = _parse_bool(args.lazy_minute_data, "--lazy-minute-data")
        allow_negative_train_selection = _parse_bool(
            args.allow_negative_train_selection, "--allow-negative-train-selection"
        )
        allow_sparse_train_selection = _parse_bool(
            args.allow_sparse_train_selection,
            "--allow-sparse-train-selection",
        )
        verbose_coin_backtests = _parse_bool(
            args.verbose_coin_backtests, "--verbose-coin-backtests"
        )
    except ValueError as exc:
        print(f"Error: {exc}")
        sys.exit(1)

    base_config_kwargs = build_backtest_config_kwargs(
        settings=settings,
        initial_balance=args.balance,
        position_size_usdt=settings.POSITION_SIZE_USDT,
        position_size_pct=(
            settings.POSITION_SIZE_USDT / args.balance if args.balance > 0 else 0.01
        ),
        leverage=args.leverage or settings.EXCHANGE_DEFAULT_LEVERAGE,
        max_spreads=settings.MAX_OPEN_SPREADS,
        consistent_pairs=[],
        use_funding_filter=True,
        use_trailing_entry=use_trailing_entry,
        use_live_exit=use_live_exit,
        use_dynamic_tp=True,
        lazy_load_minute_data=lazy_minute_data,
        use_adf_filter=True,
        extra_overrides={
            "half_spread_bps": args.half_spread_bps,
            "slippage_bps": args.slippage_bps,
            "use_ohlc_pseudo_ticks": False,
        },
    )
    base_config = BacktestConfig(**base_config_kwargs)

    # Connect to exchange
    exchange = container.exchange_client
    await exchange.connect()

    try:
        # Keep only symbols that exist on current Binance USDT-M universe
        tradable_symbols = set(
            await exchange.get_tradable_symbols(
                exclude_leveraged=True,
                min_volume_usdt=args.min_universe_volume_usdt,
            )
        )
        normalized_coins = []
        skipped = []
        for coin in coins:
            symbol = f"{str(coin).upper()}/USDT:USDT"
            if symbol in tradable_symbols:
                normalized_coins.append(str(coin).upper())
            else:
                skipped.append(str(coin).upper())

        if skipped:
            print(
                f"⚠️ Skipped {len(skipped)} non-tradable symbols: "
                + ", ".join(skipped[:20])
                + (" ..." if len(skipped) > 20 else "")
            )
        coins = normalized_coins
        if not coins:
            print("Error: no tradable symbols left in universe after filtering.")
            return
        print(f"Using {len(coins)} tradable symbols after exchange validation.")

        funding_cache = None
        if base_config.use_funding_filter:
            funding_cache = HistoricalFundingCache(logger=logger)
            funding_start = start_date - timedelta(
                days=settings.LOOKBACK_WINDOW_DAYS * 3 + 2
            )
            await funding_cache.load(
                exchange_client=exchange,
                symbols=[
                    settings.PRIMARY_PAIR,
                    *(f"{coin}/USDT:USDT" for coin in coins),
                ],
                start_date=funding_start,
                end_date=end_date,
            )

        # Create services
        services = {
            "logger": logger,
            "settings": settings,
            "exchange": exchange,
            "ohlcv_repository": container.ohlcv_repository,
            "funding_cache": funding_cache,
        }

        # Create and run runner
        runner = UniverseWalkForwardRunner(
            services=services,
            base_config=base_config,
            coins=coins,
            train_days=args.trainDays,
            trade_days=args.tradeDays,
            top_k=args.topK,
            min_trades_train=args.minTradesTrain,
            rank_metric=args.rankMetric,
            kill_loss_streak=args.killLossStreak,
            kill_negative_r=args.killNegativeR,
            workers=args.workers,
            allow_negative_train_selection=allow_negative_train_selection,
            allow_sparse_train_selection=allow_sparse_train_selection,
            verbose_coin_backtests=verbose_coin_backtests,
        )

        result = await runner.run(start_date, end_date)

        # Save results
        output_file = (
            f"backtests/results/universe_wf_{args.start}_{args.end or 'now'}.json"
        )
        runner.save_results(result, output_file)

    finally:
        _set_run_backtest_print_enabled(True)
        await exchange.disconnect()
        container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
