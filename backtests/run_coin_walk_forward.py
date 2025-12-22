#!/usr/bin/env python3
"""
Coin-by-Coin Walk-Forward Backtest Runner.

Tests each coin from to_test.json individually against ETH/USDT:USDT
using walk-forward analysis (month by month).

Usage:
    python run_coin_walk_forward.py --start 2024-01-01 --end 2024-12-31
    python run_coin_walk_forward.py --start 2024-06-01 --end 2024-12-31 --balance 50000
"""

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

from dateutil.relativedelta import relativedelta

from run_backtest import (
    BacktestConfig,
    BacktestResult,
    StatArbBacktest,
)

# Add the parent directory to sys.path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.domain.data_loader.async_data_loader import AsyncDataLoaderService
from src.domain.screener.correlation import CorrelationService
from src.domain.screener.hurst_filter import HurstFilterService
from src.domain.screener.volatility_filter import VolatilityFilterService
from src.domain.screener.z_score import ZScoreService
from src.infra.container import Container


@dataclass
class MonthlyResult:
    """Results for a single month."""
    period: str  # "2024-January"
    start_date: str
    end_date: str
    pnl: float
    pnl_pct: float
    trades: int
    win_rate: float
    max_drawdown: float
    max_drawdown_pct: float
    sharpe_ratio: float


@dataclass
class CoinResult:
    """Aggregated results for a single coin."""
    symbol: str
    total_pnl: float
    total_pnl_pct: float
    total_trades: int
    overall_win_rate: float
    profitable_months: int
    losing_months: int
    max_drawdown: float
    max_drawdown_pct: float
    avg_monthly_pnl: float
    best_month_pnl: float
    worst_month_pnl: float
    sharpe_ratio: float
    monthly_results: List[MonthlyResult]


class CoinWalkForwardRunner:
    """Run walk-forward backtest for each coin individually."""

    def __init__(
        self,
        coins: List[str],
        start_date: datetime,
        end_date: datetime,
        base_config: BacktestConfig,
        services: dict,
        exchange_client,
    ):
        self.coins = coins
        self.start_date = start_date
        self.end_date = end_date
        self.base_config = base_config
        self.services = services
        self.exchange_client = exchange_client
        self.results: List[CoinResult] = []
        self.skipped_coins: List[str] = []

    async def run(self):
        """Run backtest for each coin."""
        print("\n" + "=" * 100)
        print("🚀 COIN-BY-COIN WALK-FORWARD BACKTEST")
        print(f"   Period: {self.start_date.date()} to {self.end_date.date()}")
        print(f"   Testing {len(self.coins)} coins against ETH/USDT:USDT")
        print("=" * 100)

        for idx, coin in enumerate(self.coins, 1):
            print(f"\n{'='*100}")
            print(f"[{idx}/{len(self.coins)}] Testing: {coin}")
            print(f"{'='*100}")

            # Check if coin exists on Binance
            symbol = f"{coin}/USDT:USDT"
            if not self._is_symbol_available(symbol):
                print(f"⚠️  {symbol} not available on Binance - SKIPPING")
                self.skipped_coins.append(coin)
                continue

            try:
                coin_result = await self._test_coin(coin)
                self.results.append(coin_result)
                self._print_coin_summary(coin_result)
            except Exception as e:
                print(f"❌ Error testing {coin}: {e}")
                import traceback
                traceback.print_exc()

        self._print_final_summary()
        self._save_results()

    def _is_symbol_available(self, symbol: str) -> bool:
        """Check if symbol is available on Binance."""
        if not hasattr(self.exchange_client, '_markets_cache'):
            print(f"⚠️  Warning: Cannot check market availability (no cache)")
            return True  # Assume available if we can't check

        markets = self.exchange_client._markets_cache
        if not markets:
            print(f"⚠️  Warning: Markets cache is empty")
            return True

        return symbol in markets

    async def _test_coin(self, coin: str) -> CoinResult:
        """Run walk-forward backtest for a single coin."""
        symbol = f"{coin}/USDT:USDT"
        monthly_results = []

        current_start = self.start_date
        total_pnl: float = 0
        total_trades = 0
        total_wins = 0
        max_dd: float = 0
        max_dd_pct: float = 0

        while current_start < self.end_date:
            # Calculate month bounds
            next_month = current_start + relativedelta(months=1)
            current_end = min(next_month - timedelta(days=1), self.end_date)

            if current_start >= current_end:
                break

            period_name = current_start.strftime("%Y-%B")

            # Create config with single coin
            config = BacktestConfig(
                consistent_pairs=[symbol],
                initial_balance=self.base_config.initial_balance,
                position_size_pct=self.base_config.position_size_pct,
                max_spreads=1,  # Only one coin at a time
                z_entry_threshold=self.base_config.z_entry_threshold,
                z_tp_threshold=self.base_config.z_tp_threshold,
                z_sl_threshold=self.base_config.z_sl_threshold,
                min_correlation=self.base_config.min_correlation,
                leverage=self.base_config.leverage,
                cooldown_bars=self.base_config.cooldown_bars,
                max_position_bars=self.base_config.max_position_bars,
            )

            # Run backtest
            backtester = StatArbBacktest(
                config=config,
                **self.services,
            )

            try:
                result = await backtester.run(current_start, current_end)

                # Accumulate stats
                total_pnl += result.total_pnl
                total_trades += result.total_trades
                total_wins += result.winning_trades
                max_dd = min(max_dd, result.max_drawdown)
                max_dd_pct = min(max_dd_pct, result.max_drawdown_pct)

                # Store monthly result
                monthly_results.append(
                    MonthlyResult(
                        period=period_name,
                        start_date=current_start.strftime("%Y-%m-%d"),
                        end_date=current_end.strftime("%Y-%m-%d"),
                        pnl=result.total_pnl,
                        pnl_pct=result.total_pnl_pct,
                        trades=result.total_trades,
                        win_rate=result.win_rate,
                        max_drawdown=result.max_drawdown,
                        max_drawdown_pct=result.max_drawdown_pct,
                        sharpe_ratio=result.sharpe_ratio,
                    )
                )

                print(
                    f"  {period_name}: PnL=${result.total_pnl:+.2f} ({result.total_pnl_pct:+.2f}%), "
                    f"Trades={result.total_trades}, WR={result.win_rate*100:.1f}%"
                )

            except Exception as e:
                print(f"  ⚠️  {period_name}: Error - {e}")

            current_start = next_month

        # Calculate aggregated metrics
        profitable_months = sum(1 for m in monthly_results if m.pnl > 0)
        losing_months = sum(1 for m in monthly_results if m.pnl < 0)
        avg_monthly_pnl = total_pnl / len(monthly_results) if monthly_results else 0
        best_month = max(monthly_results, key=lambda m: m.pnl) if monthly_results else None
        worst_month = min(monthly_results, key=lambda m: m.pnl) if monthly_results else None
        overall_wr = total_wins / total_trades if total_trades > 0 else 0

        # Calculate overall Sharpe (average of monthly Sharpes)
        avg_sharpe = (
            sum(m.sharpe_ratio for m in monthly_results) / len(monthly_results)
            if monthly_results
            else 0
        )

        return CoinResult(
            symbol=symbol,
            total_pnl=total_pnl,
            total_pnl_pct=(total_pnl / self.base_config.initial_balance) * 100,
            total_trades=total_trades,
            overall_win_rate=overall_wr,
            profitable_months=profitable_months,
            losing_months=losing_months,
            max_drawdown=max_dd,
            max_drawdown_pct=max_dd_pct,
            avg_monthly_pnl=avg_monthly_pnl,
            best_month_pnl=best_month.pnl if best_month else 0,
            worst_month_pnl=worst_month.pnl if worst_month else 0,
            sharpe_ratio=avg_sharpe,
            monthly_results=monthly_results,
        )

    def _print_coin_summary(self, result: CoinResult):
        """Print summary for a single coin."""
        print(f"\n  📊 {result.symbol} Summary:")
        print(f"     Total PnL: ${result.total_pnl:+.2f} ({result.total_pnl_pct:+.2f}%)")
        print(f"     Trades: {result.total_trades} (WR: {result.overall_win_rate*100:.1f}%)")
        print(
            f"     Months: {result.profitable_months} profitable, {result.losing_months} losing"
        )
        print(f"     Best Month: ${result.best_month_pnl:+.2f}")
        print(f"     Worst Month: ${result.worst_month_pnl:+.2f}")
        print(f"     Max DD: ${result.max_drawdown:.2f} ({result.max_drawdown_pct:.2f}%)")
        print(f"     Sharpe: {result.sharpe_ratio:.2f}")

    def _print_final_summary(self):
        """Print final summary with rankings."""
        if not self.results:
            print("\n❌ No results to display")
            if self.skipped_coins:
                print(f"\n⚠️  Skipped {len(self.skipped_coins)} coins (not available on Binance):")
                for coin in self.skipped_coins:
                    print(f"   - {coin}")
            return

        print("\n" + "=" * 120)
        print("                                    FINAL SUMMARY - ALL COINS")
        print("=" * 120)

        # Show skipped coins if any
        if self.skipped_coins:
            print(f"\n⚠️  Skipped {len(self.skipped_coins)} coins (not available on Binance):")
            for coin in self.skipped_coins:
                print(f"   - {coin}")
            print()

        # Sort by total PnL
        sorted_results = sorted(self.results, key=lambda r: r.total_pnl, reverse=True)

        print("\n🏆 RANKING BY TOTAL PnL")
        print("-" * 120)
        print(
            f"{'Rank':<5} | {'Symbol':<20} | {'Total PnL':>12} | {'PnL %':>8} | "
            f"{'Trades':>7} | {'WR %':>6} | {'Prof/Loss':>10} | {'Sharpe':>7} | {'Max DD %':>9}"
        )
        print("-" * 120)

        for rank, result in enumerate(sorted_results, 1):
            emoji = "🟢" if result.total_pnl > 0 else "🔴"
            months_str = f"{result.profitable_months}/{result.losing_months}"

            print(
                f"{rank:<5} | {emoji} {result.symbol:<17} | "
                f"${result.total_pnl:>11.2f} | {result.total_pnl_pct:>7.2f}% | "
                f"{result.total_trades:>7} | {result.overall_win_rate*100:>5.1f}% | "
                f"{months_str:>10} | {result.sharpe_ratio:>7.2f} | {result.max_drawdown_pct:>8.2f}%"
            )

        # Top 10 and Bottom 10
        print("\n" + "=" * 120)
        print("🌟 TOP 10 BEST PERFORMERS")
        print("-" * 120)
        for i, result in enumerate(sorted_results[:10], 1):
            print(
                f"{i}. {result.symbol:<20} | PnL: ${result.total_pnl:>10.2f} | "
                f"Profitable Months: {result.profitable_months}/{result.profitable_months + result.losing_months}"
            )

        print("\n" + "=" * 120)
        print("⚠️  BOTTOM 10 WORST PERFORMERS")
        print("-" * 120)
        for i, result in enumerate(sorted_results[-10:][::-1], 1):
            print(
                f"{i}. {result.symbol:<20} | PnL: ${result.total_pnl:>10.2f} | "
                f"Losing Months: {result.losing_months}/{result.profitable_months + result.losing_months}"
            )

        # Overall statistics
        print("\n" + "=" * 120)
        print("📈 OVERALL STATISTICS")
        print("-" * 120)
        total_pnl_all = sum(r.total_pnl for r in self.results)
        profitable_coins = sum(1 for r in self.results if r.total_pnl > 0)
        losing_coins = sum(1 for r in self.results if r.total_pnl < 0)
        avg_pnl = total_pnl_all / len(self.results) if self.results else 0

        print(f"  Total Coins Tested: {len(self.results)}")
        print(f"  Profitable Coins: {profitable_coins} ({profitable_coins/len(self.results)*100:.1f}%)")
        print(f"  Losing Coins: {losing_coins} ({losing_coins/len(self.results)*100:.1f}%)")
        print(f"  Total PnL (all coins): ${total_pnl_all:+.2f}")
        print(f"  Average PnL per coin: ${avg_pnl:+.2f}")

        # Best consistency (most profitable months)
        print("\n" + "=" * 120)
        print("🎯 MOST CONSISTENT PERFORMERS (by profitable months)")
        print("-" * 120)
        consistent = sorted(
            self.results,
            key=lambda r: (r.profitable_months, r.total_pnl),
            reverse=True,
        )[:10]

        for i, result in enumerate(consistent, 1):
            consistency_pct = (
                result.profitable_months
                / (result.profitable_months + result.losing_months)
                * 100
                if (result.profitable_months + result.losing_months) > 0
                else 0
            )
            print(
                f"{i}. {result.symbol:<20} | "
                f"Profitable: {result.profitable_months}/{result.profitable_months + result.losing_months} months ({consistency_pct:.1f}%) | "
                f"Total PnL: ${result.total_pnl:+.2f}"
            )

    def _save_results(self):
        """Save results to JSON file."""
        output_file = "coin_walk_forward_results.json"

        # Convert results to dict
        results_dict = {
            "metadata": {
                "start_date": self.start_date.strftime("%Y-%m-%d"),
                "end_date": self.end_date.strftime("%Y-%m-%d"),
                "total_coins_requested": len(self.coins),
                "total_coins_tested": len(self.results),
                "total_coins_skipped": len(self.skipped_coins),
                "initial_balance": self.base_config.initial_balance,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            },
            "skipped_coins": self.skipped_coins,
            "coins": [
                {
                    "symbol": r.symbol,
                    "total_pnl": r.total_pnl,
                    "total_pnl_pct": r.total_pnl_pct,
                    "total_trades": r.total_trades,
                    "overall_win_rate": r.overall_win_rate,
                    "profitable_months": r.profitable_months,
                    "losing_months": r.losing_months,
                    "max_drawdown": r.max_drawdown,
                    "max_drawdown_pct": r.max_drawdown_pct,
                    "avg_monthly_pnl": r.avg_monthly_pnl,
                    "best_month_pnl": r.best_month_pnl,
                    "worst_month_pnl": r.worst_month_pnl,
                    "sharpe_ratio": r.sharpe_ratio,
                    "monthly_results": [
                        {
                            "period": m.period,
                            "start_date": m.start_date,
                            "end_date": m.end_date,
                            "pnl": m.pnl,
                            "pnl_pct": m.pnl_pct,
                            "trades": m.trades,
                            "win_rate": m.win_rate,
                            "max_drawdown": m.max_drawdown,
                            "max_drawdown_pct": m.max_drawdown_pct,
                            "sharpe_ratio": m.sharpe_ratio,
                        }
                        for m in r.monthly_results
                    ],
                }
                for r in self.results
            ],
        }

        with open(output_file, "w") as f:
            json.dump(results_dict, f, indent=2)

        print(f"\n💾 Results saved to: {output_file}")


async def main():
    parser = argparse.ArgumentParser(
        description="Coin-by-Coin Walk-Forward Backtest Runner"
    )
    parser.add_argument(
        "--start", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", type=str, default=None, help="End date (YYYY-MM-DD). Default: now"
    )
    parser.add_argument(
        "--balance", type=float, default=10000.0, help="Initial balance. Default: 10000"
    )
    parser.add_argument(
        "--leverage", type=int, default=None, help="Leverage. Default: from settings"
    )
    parser.add_argument(
        "--coins-file",
        type=str,
        default="to_test.json",
        help="JSON file with coin list. Default: to_test.json",
    )

    args = parser.parse_args()

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
    coins_file = Path(args.coins_file)
    if not coins_file.exists():
        print(f"Error: {args.coins_file} not found")
        sys.exit(1)

    with open(coins_file) as f:
        coins = json.load(f)

    print(f"Loaded {len(coins)} coins from {args.coins_file}")

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
        cooldown_bars=settings.COOLDOWN_BARS,
        max_position_bars=settings.MAX_POSITION_BARS,
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
                logger, settings.PRIMARY_PAIR, settings.TIMEFRAME
            ),
            "hurst_filter_service": HurstFilterService(logger),
        }

        # Run walk-forward for all coins
        runner = CoinWalkForwardRunner(
            coins=coins,
            start_date=start_date,
            end_date=end_date,
            base_config=base_config,
            services=services,
            exchange_client=exchange,
        )

        await runner.run()

    finally:
        await exchange.disconnect()
        container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
