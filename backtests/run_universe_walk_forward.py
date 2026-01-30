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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add the parent directory to sys.path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.domain.data_loader.async_data_loader import AsyncDataLoaderService
from src.domain.screener.correlation import CorrelationService
from src.domain.screener.z_score import ZScoreService
from src.domain.screener.volatility_filter import VolatilityFilterService
from src.domain.screener.hurst_filter import HurstFilterService
from src.domain.screener.adf_filter import ADFFilterService
from src.domain.screener.halflife_filter import HalfLifeFilterService
from src.infra.container import Container

from run_backtest import (
    BacktestConfig,
    BacktestResult,
    StatArbBacktest,
    Trade,
)


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
        train_days: int = 30,
        trade_days: int = 7,
        top_k: int = 10,
        min_trades_train: int = 15,
        rank_metric: str = "netPnL",
        kill_loss_streak: int = 3,
        kill_negative_r: float = 1.0,
        workers: int = 10,
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

        self.steps: List[WFStepResult] = []
        self._print_lock = asyncio.Lock()

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
        print(f"   Kill Switch: lossStreak >= {self.kill_loss_streak} or PnL < -{self.kill_negative_r}R")
        print(f"   Coins: {len(self.coins)}")
        print("=" * 100 + "\n")

        # Generate WF windows
        windows = self._generate_windows(start_date, end_date)
        print(f"📅 Generated {len(windows)} walk-forward steps\n")

        # Run each step
        for step_num, (train_start, train_end, trade_start, trade_end) in enumerate(windows, 1):
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

        # Calculate final results
        result = self._calculate_final_results(start_date, end_date)

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

        # Phase 1: Train - run backtest for ALL coins
        print(f"\n📈 TRAIN PHASE: Testing {len(self.coins)} coins...")
        train_results = await self._run_train_phase(train_start, train_end)

        # Phase 2: Select - filter and rank coins
        selected_coins, selection_scores = self._select_coins(train_results)
        print(f"\n🎯 SELECTED {len(selected_coins)} coins: {', '.join(selected_coins)}")

        # Phase 3: Trade - run backtest for selected coins only
        print(f"\n📊 TRADE PHASE: Testing {len(selected_coins)} selected coins...")
        trade_results = await self._run_trade_phase(
            trade_start, trade_end, selected_coins
        )

        # Calculate portfolio metrics
        portfolio_pnl = sum(r.net_pnl for r in trade_results)
        portfolio_dd = min((r.max_drawdown for r in trade_results), default=0)

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
        results = []

        async def run_coin(coin: str, idx: int) -> Optional[CoinTrainResult]:
            async with semaphore:
                try:
                    result = await self._run_single_coin_backtest(
                        coin, train_start, train_end
                    )
                    if result and result.total_trades > 0:
                        # Calculate score
                        if self.rank_metric == "netSharpe":
                            score = result.sharpe_ratio
                        else:  # netPnL (default)
                            # score = netPnL / max(1, abs(maxDD))
                            dd_divisor = max(1.0, abs(result.max_drawdown))
                            score = result.total_pnl / dd_divisor

                        train_result = CoinTrainResult(
                            coin=coin,
                            symbol=f"{coin}/USDT:USDT",
                            net_pnl=result.total_pnl,
                            gross_pnl=sum(t.pnl + abs(t.funding_pnl) for t in result.trades),
                            total_trades=result.total_trades,
                            winning_trades=result.winning_trades,
                            win_rate=result.win_rate,
                            max_drawdown=result.max_drawdown,
                            max_drawdown_pct=result.max_drawdown_pct,
                            sharpe_ratio=result.sharpe_ratio,
                            profit_factor=result.profit_factor,
                            avg_trade_pnl=result.avg_trade_pnl,
                            costs=result.total_pnl - sum(t.pnl for t in result.trades),
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

                        return train_result
                    else:
                        async with self._print_lock:
                            print(f"  [{idx}/{len(self.coins)}] ⚪ {coin}: No trades")
                        return None

                except Exception as e:
                    async with self._print_lock:
                        print(f"  [{idx}/{len(self.coins)}] ❌ {coin}: {e}")
                    return None

        # Run all coins in parallel
        tasks = [run_coin(coin, i) for i, coin in enumerate(self.coins, 1)]
        coin_results = await asyncio.gather(*tasks)

        # Filter out None results
        results = [r for r in coin_results if r is not None]

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
        # Filter
        filtered = [
            r for r in train_results
            if r.total_trades >= self.min_trades_train and r.net_pnl > 0
        ]

        if not filtered:
            # Fallback: relax netPnl > 0 requirement
            filtered = [
                r for r in train_results
                if r.total_trades >= self.min_trades_train
            ]

        if not filtered:
            # Fallback: relax minTrades requirement
            filtered = [r for r in train_results if r.total_trades > 0]

        # Sort by score descending
        filtered.sort(key=lambda r: r.score, reverse=True)

        # Select topK
        selected = filtered[:self.top_k]

        coins = [r.coin for r in selected]
        scores = {r.coin: r.score for r in selected}

        return coins, scores

    async def _run_trade_phase(
        self,
        trade_start: datetime,
        trade_end: datetime,
        selected_coins: List[str],
    ) -> List[CoinTradeResult]:
        """Run backtest for selected coins with kill switch."""
        semaphore = asyncio.Semaphore(self.workers)
        results = []

        async def run_coin(coin: str, idx: int) -> Optional[CoinTradeResult]:
            async with semaphore:
                try:
                    result = await self._run_single_coin_backtest(
                        coin, trade_start, trade_end
                    )

                    if result:
                        # Check kill switch conditions
                        was_killed = False
                        kill_reason = None

                        if result.trades:
                            # Check loss streak
                            loss_streak = 0
                            max_loss_streak = 0
                            for trade in result.trades:
                                if trade.pnl < 0:
                                    loss_streak += 1
                                    max_loss_streak = max(max_loss_streak, loss_streak)
                                else:
                                    loss_streak = 0

                            if max_loss_streak >= self.kill_loss_streak:
                                was_killed = True
                                kill_reason = f"LOSS_STREAK_{max_loss_streak}"

                            # Check negative PnL threshold
                            # -1R means losing more than 1x average trade size
                            avg_size = self.base_config.initial_balance * self.base_config.position_size_pct
                            if result.total_pnl < -self.kill_negative_r * avg_size:
                                was_killed = True
                                kill_reason = f"NEGATIVE_PNL_{result.total_pnl:.0f}"

                        trade_result = CoinTradeResult(
                            coin=coin,
                            symbol=f"{coin}/USDT:USDT",
                            net_pnl=result.total_pnl,
                            total_trades=result.total_trades,
                            winning_trades=result.winning_trades,
                            win_rate=result.win_rate,
                            max_drawdown=result.max_drawdown,
                            was_killed=was_killed,
                            kill_reason=kill_reason,
                            trades=result.trades,
                        )

                        async with self._print_lock:
                            emoji = "🟢" if result.total_pnl > 0 else "🔴"
                            kill_str = f" ⚠️ KILLED: {kill_reason}" if was_killed else ""
                            print(
                                f"  [{idx}/{len(selected_coins)}] {emoji} {coin}: "
                                f"PnL=${result.total_pnl:+.2f} | "
                                f"Trades={result.total_trades}{kill_str}"
                            )

                        return trade_result
                    else:
                        return CoinTradeResult(
                            coin=coin,
                            symbol=f"{coin}/USDT:USDT",
                            net_pnl=0,
                            total_trades=0,
                            winning_trades=0,
                            win_rate=0,
                            max_drawdown=0,
                            was_killed=False,
                            trades=[],
                        )

                except Exception as e:
                    async with self._print_lock:
                        print(f"  [{idx}/{len(selected_coins)}] ❌ {coin}: {e}")
                    return None

        # Run selected coins in parallel
        tasks = [run_coin(coin, i) for i, coin in enumerate(selected_coins, 1)]
        coin_results = await asyncio.gather(*tasks)

        # Filter out None results
        results = [r for r in coin_results if r is not None]

        return results


    async def _run_single_coin_backtest(
        self,
        coin: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Optional[BacktestResult]:
        """Run backtest for a single coin."""
        symbol = f"{coin}/USDT:USDT"

        # Create config for this coin
        config = BacktestConfig(
            initial_balance=self.base_config.initial_balance,
            position_size_pct=self.base_config.position_size_pct,
            max_spreads=1,
            leverage=self.base_config.leverage,
            z_entry_threshold=self.base_config.z_entry_threshold,
            z_tp_threshold=self.base_config.z_tp_threshold,
            z_sl_threshold=self.base_config.z_sl_threshold,
            min_correlation=self.base_config.min_correlation,
            consistent_pairs=[symbol],
            use_trailing_entry=False,  # Disable for speed
            use_live_exit=False,  # Disable for speed
            use_funding_filter=self.base_config.use_funding_filter,
            use_adf_filter=self.base_config.use_adf_filter,
        )

        # Create backtester
        backtester = StatArbBacktest(config=config, **self.services)

        try:
            result = await backtester.run(start_date, end_date)
            return result
        except Exception:
            return None

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
    ) -> UniverseWFResult:
        """Calculate aggregated results."""
        total_pnl = sum(s.portfolio_pnl for s in self.steps)
        total_trades = sum(
            sum(r.total_trades for r in s.trade_results)
            for s in self.steps
        )
        max_dd = min((s.portfolio_dd for s in self.steps), default=0)

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

            emoji = "🟢" if step.portfolio_pnl > 0 else "🔴"
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

        # Profitable vs losing steps
        profitable_steps = sum(1 for s in result.steps if s.portfolio_pnl > 0)
        losing_steps = sum(1 for s in result.steps if s.portfolio_pnl <= 0)
        print(f"  Profitable Steps:        {profitable_steps}/{len(result.steps)}")
        print(f"  Losing Steps:            {losing_steps}/{len(result.steps)}")

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
            print(f"  {coin:<15} | Selected {count}/{len(result.steps)} steps ({pct:.0f}%)")

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
            for coin, count in sorted(killed_coins.items(), key=lambda x: x[1], reverse=True):
                print(f"  {coin:<15} | Killed {count} times")

    def save_results(self, result: UniverseWFResult, filepath: str) -> None:
        """Save results to JSON file."""
        data = {
            "metadata": {
                "start_date": result.start_date.isoformat(),
                "end_date": result.end_date.isoformat(),
                "train_days": result.train_days,
                "trade_days": result.trade_days,
                "top_k": result.top_k,
                "min_trades_train": result.min_trades_train,
                "rank_metric": result.rank_metric,
            },
            "summary": {
                "total_portfolio_pnl": result.total_portfolio_pnl,
                "total_trades": result.total_trades,
                "max_portfolio_dd": result.max_portfolio_dd,
                "coin_selection_turnover": result.coin_selection_turnover,
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

        with open(filepath, "w", encoding="utf-8") as f:
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
        "--trainDays", type=int, default=30, help="Training window in days. Default: 30"
    )
    parser.add_argument(
        "--tradeDays", type=int, default=7, help="Trading window in days. Default: 7"
    )
    parser.add_argument(
        "--topK", type=int, default=10, help="Number of top coins to select. Default: 10"
    )
    parser.add_argument(
        "--minTradesTrain",
        type=int,
        default=15,
        help="Minimum trades in train phase to qualify. Default: 15",
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
        "--workers",
        type=int,
        default=10,
        help="Number of parallel workers. Default: 10",
    )
    parser.add_argument(
        "--coinsFile",
        type=str,
        default="backtests/to_test.json",
        help="JSON file with coin list. Default: backtests/to_test.json",
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

    args = parser.parse_args()

    # Parse dates
    start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if args.end:
        end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc)

    # Load coins list
    coins_file = Path(args.coinsFile)
    if not coins_file.exists():
        print(f"Error: {args.coinsFile} not found")
        sys.exit(1)

    with open(coins_file, encoding="utf-8") as f:
        coins = json.load(f)

    print(f"Loaded {len(coins)} coins from {args.coinsFile}")

    # Initialize container
    container = Container().init()
    settings = container.settings
    logger = container.logger

    # Create base config
    base_config = BacktestConfig(
        initial_balance=args.balance,
        leverage=args.leverage or settings.EXCHANGE_DEFAULT_LEVERAGE,
        z_entry_threshold=settings.Z_ENTRY_THRESHOLD,
        z_tp_threshold=settings.Z_TP_THRESHOLD,
        z_sl_threshold=settings.Z_SL_THRESHOLD,
        min_correlation=settings.MIN_CORRELATION,
        use_funding_filter=True,
        use_adf_filter=True,
    )

    # Connect to exchange
    exchange = container.exchange_client
    await exchange.connect()

    try:
        # Create services
        services = {
            "settings": settings,
            "data_loader": AsyncDataLoaderService(
                logger, exchange, container.ohlcv_repository
            ),
            "correlation_service": CorrelationService(
                logger, settings.LOOKBACK_WINDOW_DAYS, settings.TIMEFRAME
            ),
            "z_score_service": ZScoreService(
                logger,
                settings.LOOKBACK_WINDOW_DAYS,
                settings.TIMEFRAME,
                settings.Z_ENTRY_THRESHOLD,
                settings.Z_TP_THRESHOLD,
                settings.Z_SL_THRESHOLD,
            ),
            "volatility_filter_service": VolatilityFilterService(
                logger=logger,
                primary_pair=settings.PRIMARY_PAIR,
                timeframe=settings.TIMEFRAME,
                volatility_window=settings.VOLATILITY_WINDOW,
                volatility_threshold=settings.VOLATILITY_THRESHOLD,
                crash_window=settings.VOLATILITY_CRASH_WINDOW,
                crash_threshold=settings.VOLATILITY_CRASH_THRESHOLD,
            ),
            "hurst_filter_service": HurstFilterService(logger),
            "adf_filter_service": ADFFilterService(
                logger,
                pvalue_threshold=settings.ADF_PVALUE_THRESHOLD,
                lookback_candles=settings.ADF_LOOKBACK_CANDLES,
            ),
            "halflife_filter_service": HalfLifeFilterService(
                logger,
                max_bars=settings.HALFLIFE_MAX_BARS,
                lookback_candles=settings.HALFLIFE_LOOKBACK_CANDLES,
            ),
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
        )

        result = await runner.run(start_date, end_date)

        # Save results
        output_file = (
            f"backtests/results/universe_wf_{args.start}_{args.end or 'now'}.json"
        )
        runner.save_results(result, output_file)

    finally:
        await exchange.disconnect()
        container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
