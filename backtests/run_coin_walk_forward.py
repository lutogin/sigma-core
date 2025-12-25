#!/usr/bin/env python3
"""
Coin-by-Coin Walk-Forward Backtest Runner (Parallel Subprocess Workers).

Spawns separate Python processes for each coin to maximize parallelism.
Each worker runs run_walk_forward_backtest.py --coin X --json-output.

Usage:
    python run_coin_walk_forward.py --start 2024-01-01 --end 2024-12-31
    python run_coin_walk_forward.py --start 2024-06-01 --end 2024-12-31 --workers 5
"""

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

# Default number of parallel workers
DEFAULT_WORKERS = 10


@dataclass
class MonthlyResult:
    """Results for a single month."""
    period: str
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
    coin: str
    symbol: str
    total_pnl: float
    total_pnl_pct: float
    total_trades: int
    overall_win_rate: float
    profitable_months: int
    losing_months: int
    max_drawdown: float
    max_drawdown_pct: float
    avg_sharpe: float
    monthly_results: List[MonthlyResult]
    error: Optional[str] = None


class CoinWalkForwardRunner:
    """Run walk-forward backtest for each coin via subprocess workers."""

    def __init__(
        self,
        coins: List[str],
        start_date: str,
        end_date: str,
        balance: float,
        leverage: Optional[int],
        workers: int = DEFAULT_WORKERS,
    ):
        self.coins = coins
        self.start_date = start_date
        self.end_date = end_date
        self.balance = balance
        self.leverage = leverage
        self.workers = workers
        self.results: List[CoinResult] = []
        self.failed_coins: List[str] = []
        self._print_lock = asyncio.Lock()
        self._results_lock = asyncio.Lock()

    async def run(self):
        """Run backtest for each coin using subprocess workers."""
        print("\n" + "=" * 100)
        print("🚀 COIN-BY-COIN WALK-FORWARD BACKTEST (SUBPROCESS WORKERS)")
        print(f"   Period: {self.start_date} to {self.end_date}")
        print(f"   Testing {len(self.coins)} coins")
        print(f"   Workers: {self.workers}")
        print(f"   Balance: ${self.balance:,.0f}")
        print("=" * 100 + "\n")

        # Create semaphore for limiting concurrent workers
        semaphore = asyncio.Semaphore(self.workers)

        # Create tasks for all coins
        tasks = [
            self._worker(coin, idx, len(self.coins), semaphore)
            for idx, coin in enumerate(self.coins, 1)
        ]

        # Run all tasks concurrently (limited by semaphore)
        await asyncio.gather(*tasks, return_exceptions=True)

        self._print_final_summary()
        self._save_results()

    async def _worker(
        self,
        coin: str,
        idx: int,
        total: int,
        semaphore: asyncio.Semaphore,
    ):
        """Worker that spawns subprocess for a single coin."""
        async with semaphore:
            async with self._print_lock:
                print(f"[{idx}/{total}] 🔄 Starting: {coin}")

            try:
                result = await self._run_backtest_subprocess(coin)

                async with self._results_lock:
                    self.results.append(result)

                async with self._print_lock:
                    if result.error:
                        print(f"[{idx}/{total}] ❌ {coin}: {result.error}")
                    else:
                        emoji = "🟢" if result.total_pnl > 0 else "🔴"
                        print(
                            f"[{idx}/{total}] {emoji} {coin}: "
                            f"PnL=${result.total_pnl:+.2f} ({result.total_pnl_pct:+.2f}%) | "
                            f"Trades={result.total_trades} WR={result.overall_win_rate*100:.1f}% | "
                            f"Months: {result.profitable_months}W/{result.losing_months}L"
                        )

            except Exception as e:
                async with self._print_lock:
                    print(f"[{idx}/{total}] ❌ {coin}: Exception - {e}")
                async with self._results_lock:
                    self.failed_coins.append(coin)

    async def _run_backtest_subprocess(self, coin: str) -> CoinResult:
        """Run walk-forward backtest for a single coin via subprocess."""
        symbol = f"{coin}/USDT:USDT"

        # Build command - run from backtests directory
        cmd = [
            sys.executable,
            "run_walk_forward_backtest.py",
            "--start", self.start_date,
            "--end", self.end_date,
            "--balance", str(self.balance),
            "--coin", coin,
            "--json-output",
        ]

        if self.leverage:
            cmd.extend(["--leverage", str(self.leverage)])

        # Get backtests directory path
        backtests_dir = Path(__file__).parent

        # Run subprocess from backtests directory
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=backtests_dir,
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_msg = stderr.decode().strip() if stderr else f"Exit code {process.returncode}"
            return CoinResult(
                coin=coin,
                symbol=symbol,
                total_pnl=0,
                total_pnl_pct=0,
                total_trades=0,
                overall_win_rate=0,
                profitable_months=0,
                losing_months=0,
                max_drawdown=0,
                max_drawdown_pct=0,
                avg_sharpe=0,
                monthly_results=[],
                error=error_msg[:200],
            )

        # Parse JSON output
        try:
            # Find JSON in output - it should be the last line starting with {
            output = stdout.decode()
            lines = output.strip().split('\n')

            # Find the JSON line (last line that starts with '{')
            json_line = None
            for line in reversed(lines):
                line = line.strip()
                if line.startswith('{'):
                    json_line = line
                    break

            if not json_line:
                raise ValueError("No JSON found in output")

            data = json.loads(json_line)

            if "error" in data:
                return CoinResult(
                    coin=coin,
                    symbol=symbol,
                    total_pnl=0,
                    total_pnl_pct=0,
                    total_trades=0,
                    overall_win_rate=0,
                    profitable_months=0,
                    losing_months=0,
                    max_drawdown=0,
                    max_drawdown_pct=0,
                    avg_sharpe=0,
                    monthly_results=[],
                    error=data["error"],
                )

            summary = data.get("summary", {})
            monthly = data.get("monthly", [])

            monthly_results = [
                MonthlyResult(
                    period=m["period"],
                    pnl=m["pnl"],
                    pnl_pct=m["pnl_pct"],
                    trades=m["trades"],
                    win_rate=m["win_rate"],
                    max_drawdown=m["max_drawdown"],
                    max_drawdown_pct=m["max_drawdown_pct"],
                    sharpe_ratio=m["sharpe_ratio"],
                )
                for m in monthly
            ]

            return CoinResult(
                coin=coin,
                symbol=symbol,
                total_pnl=summary.get("total_pnl", 0),
                total_pnl_pct=(summary.get("total_pnl", 0) / self.balance) * 100,
                total_trades=summary.get("total_trades", 0),
                overall_win_rate=summary.get("win_rate", 0),
                profitable_months=summary.get("profitable_months", 0),
                losing_months=summary.get("losing_months", 0),
                max_drawdown=summary.get("max_drawdown", 0),
                max_drawdown_pct=summary.get("max_drawdown_pct", 0),
                avg_sharpe=summary.get("avg_sharpe", 0),
                monthly_results=monthly_results,
            )

        except Exception as e:
            return CoinResult(
                coin=coin,
                symbol=symbol,
                total_pnl=0,
                total_pnl_pct=0,
                total_trades=0,
                overall_win_rate=0,
                profitable_months=0,
                losing_months=0,
                max_drawdown=0,
                max_drawdown_pct=0,
                avg_sharpe=0,
                monthly_results=[],
                error=f"Parse error: {e}",
            )

    def _print_final_summary(self):
        """Print final summary with rankings."""
        # Filter successful results
        successful = [r for r in self.results if not r.error]
        failed = [r for r in self.results if r.error]

        if not successful:
            print("\n❌ No successful results to display")
            if failed:
                print(f"\n⚠️  Failed {len(failed)} coins:")
                for r in failed:
                    print(f"   - {r.coin}: {r.error}")
            return

        print("\n" + "=" * 120)
        print("                                    FINAL SUMMARY - ALL COINS")
        print("=" * 120)

        if failed:
            print(f"\n⚠️  Failed {len(failed)} coins:")
            for r in failed[:10]:
                print(f"   - {r.coin}: {r.error}")
            if len(failed) > 10:
                print(f"   ... and {len(failed) - 10} more")
            print()

        # Sort by total PnL
        sorted_results = sorted(successful, key=lambda r: r.total_pnl, reverse=True)

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
                f"{months_str:>10} | {result.avg_sharpe:>7.2f} | {result.max_drawdown_pct:>8.2f}%"
            )

        # Top 10 and Bottom 10
        print("\n" + "=" * 120)
        print("🌟 TOP 10 BEST PERFORMERS")
        print("-" * 120)
        for i, result in enumerate(sorted_results[:10], 1):
            total_months = result.profitable_months + result.losing_months
            print(
                f"{i}. {result.symbol:<20} | PnL: ${result.total_pnl:>10.2f} | "
                f"Profitable Months: {result.profitable_months}/{total_months}"
            )

        print("\n" + "=" * 120)
        print("⚠️  BOTTOM 10 WORST PERFORMERS")
        print("-" * 120)
        for i, result in enumerate(sorted_results[-10:][::-1], 1):
            total_months = result.profitable_months + result.losing_months
            print(
                f"{i}. {result.symbol:<20} | PnL: ${result.total_pnl:>10.2f} | "
                f"Losing Months: {result.losing_months}/{total_months}"
            )

        # Overall statistics
        print("\n" + "=" * 120)
        print("📈 OVERALL STATISTICS")
        print("-" * 120)
        total_pnl_all = sum(r.total_pnl for r in successful)
        profitable_coins = sum(1 for r in successful if r.total_pnl > 0)
        losing_coins = sum(1 for r in successful if r.total_pnl < 0)
        avg_pnl = total_pnl_all / len(successful) if successful else 0

        print(f"  Total Coins Tested: {len(successful)} (failed: {len(failed)})")
        print(f"  Profitable Coins: {profitable_coins} ({profitable_coins/len(successful)*100:.1f}%)")
        print(f"  Losing Coins: {losing_coins} ({losing_coins/len(successful)*100:.1f}%)")
        print(f"  Total PnL (all coins): ${total_pnl_all:+.2f}")
        print(f"  Average PnL per coin: ${avg_pnl:+.2f}")

        # Best consistency
        print("\n" + "=" * 120)
        print("🎯 MOST CONSISTENT PERFORMERS (by profitable months)")
        print("-" * 120)
        consistent = sorted(
            successful,
            key=lambda r: (r.profitable_months, r.total_pnl),
            reverse=True,
        )[:10]

        for i, result in enumerate(consistent, 1):
            total_months = result.profitable_months + result.losing_months
            consistency_pct = (
                result.profitable_months / total_months * 100
                if total_months > 0
                else 0
            )
            print(
                f"{i}. {result.symbol:<20} | "
                f"Profitable: {result.profitable_months}/{total_months} months ({consistency_pct:.1f}%) | "
                f"Total PnL: ${result.total_pnl:+.2f}"
            )

    def _save_results(self):
        """Save results to JSON file."""
        output_file = f"backtests/results/coin_walk_forward_{self.start_date}-{self.end_date}.json"

        successful = [r for r in self.results if not r.error]
        failed = [r for r in self.results if r.error]

        results_dict = {
            "metadata": {
                "start_date": self.start_date,
                "end_date": self.end_date,
                "total_coins_requested": len(self.coins),
                "total_coins_tested": len(successful),
                "total_coins_failed": len(failed),
                "initial_balance": self.balance,
                "workers": self.workers,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            },
            "failed_coins": [{"coin": r.coin, "error": r.error} for r in failed],
            "coins": [
                {
                    "coin": r.coin,
                    "symbol": r.symbol,
                    "total_pnl": r.total_pnl,
                    "total_pnl_pct": r.total_pnl_pct,
                    "total_trades": r.total_trades,
                    "overall_win_rate": r.overall_win_rate,
                    "profitable_months": r.profitable_months,
                    "losing_months": r.losing_months,
                    "max_drawdown": r.max_drawdown,
                    "max_drawdown_pct": r.max_drawdown_pct,
                    "avg_sharpe": r.avg_sharpe,
                    "monthly_results": [
                        {
                            "period": m.period,
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
                for r in successful
            ],
        }

        with open(output_file, "w") as f:
            json.dump(results_dict, f, indent=2)

        print(f"\n💾 Results saved to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Coin-by-Coin Walk-Forward Backtest (Parallel Subprocess Workers)"
    )
    parser.add_argument(
        "--start", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", type=str, default=None, help="End date (YYYY-MM-DD). Default: today"
    )
    parser.add_argument(
        "--balance", type=float, default=10000.0, help="Initial balance. Default: 10000"
    )
    parser.add_argument(
        "--leverage", type=int, default=None, help="Leverage. Default: from settings"
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Number of parallel workers. Default: {DEFAULT_WORKERS}"
    )
    parser.add_argument(
        "--coins-file",
        type=str,
        default="backtests/to_test.json",
        help="JSON file with coin list. Default: backtests/to_test.json",
    )

    args = parser.parse_args()

    # Validate dates
    start_date = args.start
    if args.end:
        end_date = args.end
    else:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Load coins list
    coins_file = Path(args.coins_file)
    if not coins_file.exists():
        print(f"Error: {args.coins_file} not found")
        sys.exit(1)

    with open(coins_file) as f:
        coins = json.load(f)

    print(f"Loaded {len(coins)} coins from {args.coins_file}")

    # Run
    runner = CoinWalkForwardRunner(
        coins=coins,
        start_date=start_date,
        end_date=end_date,
        balance=args.balance,
        leverage=args.leverage,
        workers=args.workers,
    )

    asyncio.run(runner.run())


if __name__ == "__main__":
    main()
