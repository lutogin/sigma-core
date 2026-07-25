#!/usr/bin/env python3
"""Run the recurring ETH-universe WF job and safely publish its pair set."""

import argparse
import fcntl
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recurring walk-forward pair refresh with overlap lock",
    )
    parser.add_argument("--lookback-days", type=int, default=186)
    parser.add_argument("--train-days", type=int, default=60)
    parser.add_argument("--trade-days", type=int, default=14)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--min-trades", type=int, default=3)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--min-volume-usdt", type=float, default=10_000_000)
    parser.add_argument(
        "--coins-file",
        default="backtests/eth_ecosystem_universe.json",
    )
    parser.add_argument("--env-file", default=None)
    parser.add_argument(
        "--activate",
        action="store_true",
        help="Activate a passing version; otherwise only save a candidate",
    )
    args = parser.parse_args()

    if any(
        value <= 0
        for value in (
            args.lookback_days,
            args.train_days,
            args.trade_days,
            args.top_k,
            args.min_trades,
            args.workers,
        )
    ):
        parser.error("day, pair, trade, and worker values must be positive")
    if args.min_volume_usdt < 0:
        parser.error("--min-volume-usdt must be non-negative")
    if args.lookback_days < args.train_days + args.trade_days:
        parser.error("lookback must contain at least one train+trade window")

    results_dir = PROJECT_ROOT / "backtests" / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    lock_path = results_dir / ".pair_refresh.lock"

    with lock_path.open("w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Another pair refresh is already running; exiting")
            return 3

        end = datetime.now(timezone.utc).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        start = end - timedelta(days=args.lookback_days)
        start_arg = start.strftime("%Y-%m-%d")
        end_arg = end.strftime("%Y-%m-%d")

        universe_command = [
            sys.executable,
            str(PROJECT_ROOT / "backtests" / "run_universe_walk_forward.py"),
            "--start",
            start_arg,
            "--end",
            end_arg,
            "--trainDays",
            str(args.train_days),
            "--tradeDays",
            str(args.trade_days),
            "--topK",
            str(args.top_k),
            "--minTradesTrain",
            str(args.min_trades),
            "--workers",
            str(args.workers),
            "--coinsFile",
            args.coins_file,
            "--min-universe-volume-usdt",
            str(args.min_volume_usdt),
        ]
        if args.env_file:
            universe_command.extend(["--env-file", args.env_file])

        completed = subprocess.run(
            universe_command,
            cwd=PROJECT_ROOT,
            check=False,
        )
        if completed.returncode != 0:
            print(f"Walk-forward failed with exit code {completed.returncode}")
            return completed.returncode

        result_path = results_dir / f"universe_wf_{start_arg}_{end_arg}.json"
        publish_command = [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "update_trading_pairs_from_wf.py"),
            str(result_path),
        ]
        if args.env_file:
            publish_command.extend(["--env-file", args.env_file])
        if args.activate:
            publish_command.append("--activate")

        published = subprocess.run(
            publish_command,
            cwd=PROJECT_ROOT,
            check=False,
        )
        return published.returncode


if __name__ == "__main__":
    raise SystemExit(main())
