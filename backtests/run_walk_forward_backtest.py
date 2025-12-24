#!/usr/bin/env python3
"""
Walk-Forward Statistical Arbitrage Backtest Runner.

Splits the backtest period into monthly chunks to simulate:
1. Performance consistency analysis
2. Identification of best performing pairs per period

Usage:
    python run_walk_forward_backtest.py --start 2024-01-01 --end 2024-12-31
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from dateutil.relativedelta import relativedelta

# Add the parent directory to sys.path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.domain.data_loader.async_data_loader import AsyncDataLoaderService
from src.domain.screener.correlation import CorrelationService
from src.domain.screener.z_score import ZScoreService
from src.domain.screener.volatility_filter import VolatilityFilterService
from src.domain.screener.hurst_filter import HurstFilterService
from src.infra.container import Container

# Import from existing backtest script
from run_backtest import (
    BacktestConfig,
    BacktestResult,
    StatArbBacktest,
    print_symbol_stats,
    SymbolStats
)


class WalkForwardRunner:
    def __init__(self, services, base_config: BacktestConfig, json_output: bool = False):
        self.services = services
        self.base_config = base_config
        self.results: List[Tuple[str, BacktestResult]] = []
        self.json_output = json_output

    async def run(self, start_date: datetime, end_date: datetime):
        """Run walk-forward analysis month by month."""
        current_start = start_date

        if not self.json_output:
            print("\n" + "=" * 80)
            print("🚀 STARTING WALK-FORWARD ANALYSIS")
            print(f"   Overall Period: {start_date.date()} to {end_date.date()}")
            print("=" * 80 + "\n")

        while current_start < end_date:
            # Calculate month bounds
            next_month = current_start + relativedelta(months=1)
            current_end = next_month - timedelta(days=1)  # End of current month

            # Clip to overall end date
            if current_end > end_date:
                current_end = end_date

            # If start >= end (can happen on last partial month if logic is off), break
            if current_start >= current_end:
                break

            period_name = current_start.strftime("%Y-%B")
            if not self.json_output:
                print(f"\n📅 Analyzing Period: {period_name} ({current_start.date()} to {current_end.date()})")

            # Run backtest for this slice
            backtester = StatArbBacktest(
                config=self.base_config,
                **self.services
            )

            try:
                result = await backtester.run(current_start, current_end)
                self.results.append((period_name, result))

                # Print short summary for the month
                if not self.json_output:
                    self._print_period_summary(period_name, result)

            except Exception as e:
                if not self.json_output:
                    print(f"❌ Error analysis for {period_name}: {e}")
                    import traceback
                    traceback.print_exc()

            # Move next
            current_start = next_month

        if self.json_output:
            self._output_json()
        else:
            self._print_final_analysis()

    def _output_json(self):
        """Output results as JSON for programmatic consumption."""
        import json

        if not self.results:
            print(json.dumps({"error": "no_results", "monthly": [], "summary": {}}))
            return

        total_pnl = sum(r.total_pnl for _, r in self.results)
        total_trades = sum(r.total_trades for _, r in self.results)
        total_wins = sum(r.winning_trades for _, r in self.results)

        monthly_data = []
        for period, res in self.results:
            monthly_data.append({
                "period": period,
                "pnl": res.total_pnl,
                "pnl_pct": res.total_pnl_pct,
                "trades": res.total_trades,
                "winning_trades": res.winning_trades,
                "win_rate": res.win_rate,
                "max_drawdown": res.max_drawdown,
                "max_drawdown_pct": res.max_drawdown_pct,
                "sharpe_ratio": res.sharpe_ratio,
            })

        output = {
            "monthly": monthly_data,
            "summary": {
                "total_pnl": total_pnl,
                "total_trades": total_trades,
                "total_wins": total_wins,
                "win_rate": total_wins / total_trades if total_trades > 0 else 0,
                "profitable_months": sum(1 for _, r in self.results if r.total_pnl > 0),
                "losing_months": sum(1 for _, r in self.results if r.total_pnl < 0),
                "max_drawdown": min((r.max_drawdown for _, r in self.results), default=0),
                "max_drawdown_pct": min((r.max_drawdown_pct for _, r in self.results), default=0),
                "avg_sharpe": sum(r.sharpe_ratio for _, r in self.results) / len(self.results) if self.results else 0,
            }
        }

        print(json.dumps(output))

    def _print_period_summary(self, period: str, result: BacktestResult):
        """Print concise summary for the period."""
        print(f"\n   📝 {period} Summary:")
        print(f"      PnL: ${result.total_pnl:+.2f} ({result.total_pnl_pct:+.2f}%)")
        print(f"      Trades: {result.total_trades} (WR: {result.win_rate*100:.1f}%)")

        # Best pair
        if result.symbol_stats:
            best_pair = result.symbol_stats[0]
            print(f"      🏆 Best Pair: {best_pair.symbol} (+${best_pair.total_pnl:.2f})")
        else:
            print("      ⚠️ No trades this month")

    def _print_final_analysis(self):
        """Aggregated reports."""
        if not self.results:
            print("No results to analyze.")
            return

        print("\n" + "=" * 100)
        print("                               WALK-FORWARD ANALYSIS REPORT")
        print("=" * 100)

        # 1. Monthly Breakdown Table
        print("\n1️⃣  MONTHLY BREAKDOWN")
        print("-" * 100)
        print(f"{'Period':<15} | {'PnL ($)':>10} | {'PnL (%)':>8} | {'Trades':>6} | {'WR %':>6} | {'Best Pair':<20} | {'Worst Pair':<20}")
        print("-" * 100)

        total_pnl = 0
        total_trades = 0

        # Track pair consistency
        pair_profitability: Dict[str, List[float]] = {}  # symbol -> list of pnl per month
        pair_appearances: Dict[str, int] = {} # symbol -> count of months traded

        for period, res in self.results:
            total_pnl += res.total_pnl
            total_trades += res.total_trades

            best_pair_str = "-"
            worst_pair_str = "-"

            if res.symbol_stats:
                best = res.symbol_stats[0]
                worst = res.symbol_stats[-1]
                best_pair_str = f"{best.symbol} (${int(best.total_pnl)})"
                worst_pair_str = f"{worst.symbol} (${int(worst.total_pnl)})"

                # Track stats
                for s in res.symbol_stats:
                    if s.symbol not in pair_profitability:
                        pair_profitability[s.symbol] = []
                        pair_appearances[s.symbol] = 0

                    pair_profitability[s.symbol].append(s.total_pnl)
                    pair_appearances[s.symbol] += 1

            print(f"{period:<15} | {res.total_pnl:>10.2f} | {res.total_pnl_pct:>7.2f}% | {res.total_trades:>6} | {res.win_rate*100:>6.1f} | {best_pair_str:<20} | {worst_pair_str:<20}")

        print("-" * 100)
        print(f"{'TOTAL':<15} | {total_pnl:>10.2f} | {'-':>8} | {total_trades:>6} | {'-':>6} |")

        # 2. Consistency Analysis
        print("\n2️⃣  PAIR CONSISTENCY ANALYSIS (Top 10)")
        print(f"    (Pairs that appeared in multiple months and were profitable)")
        print("-" * 100)
        print(f"{'Symbol':<15} | {'Months Traded':>13} | {'Profitable Months':>17} | {'Total PnL':>12} | {'Avg Monthly PnL':>15}")
        print("-" * 100)

        # Calculate consistency metrics
        consistency_data = []
        for symbol, pnls in pair_profitability.items():
            months_traded = pair_appearances[symbol]
            profitable_months = sum(1 for p in pnls if p > 0)
            total_pnl_pair = sum(pnls)
            avg_pnl = total_pnl_pair / months_traded if months_traded > 0 else 0

            consistency_data.append({
                "symbol": symbol,
                "months": months_traded,
                "profitable_months": profitable_months,
                "total_pnl": total_pnl_pair,
                "avg_pnl": avg_pnl,
                "reliability": profitable_months / months_traded if months_traded > 0 else 0
            })

        # Sort: Primary by Profitable Months, Secondary by Total PnL
        consistency_data.sort(key=lambda x: (x["profitable_months"], x["total_pnl"]), reverse=True)

        for item in consistency_data[:15]:
            # Highlight consistent winners (e.g., > 50% profitable months)
            marker = "🌟" if item["reliability"] >= 0.7 else " "
            print(f"{marker} {item['symbol']:<13} | {item['months']:>13} | {item['profitable_months']:>17} | {item['total_pnl']:>12.2f} | {item['avg_pnl']:>15.2f}")


async def main():
    parser = argparse.ArgumentParser(description="Walk-Forward Backtest Runner")
    parser.add_argument("--start", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--balance", type=float, default=10000.0, help="Initial balance")
    parser.add_argument("--leverage", type=int, default=None, help="Leverage")
    parser.add_argument("--coin", type=str, default=None, help="Single coin to test (e.g., LINK). Will test COIN/USDT:USDT")
    parser.add_argument("--json-output", action="store_true", help="Output results as JSON (for programmatic use)")

    args = parser.parse_args()

    # Parse dates
    start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if args.end:
        end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc)

    # Init container
    container = Container().init()
    settings = container.settings
    logger = container.logger

    # If --coin is provided, override consistent_pairs
    consistent_pairs = None
    if args.coin:
        consistent_pairs = [f"{args.coin}/USDT:USDT"]

    # Config
    config = BacktestConfig(
        initial_balance=args.balance,
        leverage=args.leverage or BacktestConfig.leverage,
        z_entry_threshold=settings.Z_ENTRY_THRESHOLD,
        z_tp_threshold=settings.Z_TP_THRESHOLD,
        z_sl_threshold=settings.Z_SL_THRESHOLD,
        min_correlation=settings.MIN_CORRELATION,
        consistent_pairs=consistent_pairs or [],
    )

    # Services
    exchange = container.exchange_client
    await exchange.connect()

    try:
        services = {
            "settings": settings,
            "data_loader": AsyncDataLoaderService(logger, exchange, container.ohlcv_repository),
            "correlation_service": CorrelationService(logger, settings.LOOKBACK_WINDOW_DAYS, settings.TIMEFRAME),
            "z_score_service": ZScoreService(logger, settings.LOOKBACK_WINDOW_DAYS, settings.TIMEFRAME, settings.Z_ENTRY_THRESHOLD, settings.Z_TP_THRESHOLD, settings.Z_SL_THRESHOLD),
            "volatility_filter_service": VolatilityFilterService(logger, settings.PRIMARY_PAIR, settings.TIMEFRAME),
            "hurst_filter_service": HurstFilterService(logger),
        }

        runner = WalkForwardRunner(services, config, json_output=args.json_output)
        await runner.run(start_date, end_date)

    finally:
        await exchange.disconnect()
        container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
