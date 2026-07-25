#!/usr/bin/env python3
"""Create or activate a versioned live pair set from a WF result."""

import argparse
import hashlib
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.domain.trading_pairs import TradingPairVersion
from src.infra.container import Container


@dataclass(frozen=True)
class PromotionPolicy:
    min_pairs: int = 2
    max_pairs: int = 5
    min_oos_steps: int = 6
    min_profitable_step_rate: float = 0.50
    min_oos_trades: int = 2
    min_selection_rate: float = 0.20
    max_turnover: float = 0.60
    max_result_age_days: int = 21
    require_positive_aggregate_pnl: bool = True
    min_half_spread_bps: float = 1.0
    min_slippage_bps: float = 0.5


@dataclass(frozen=True)
class PromotionDecision:
    symbols: list[str]
    errors: list[str]
    warnings: list[str]
    turnover: Optional[float]

    @property
    def can_activate(self) -> bool:
        return bool(self.symbols) and not self.errors


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _finite_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def evaluate_promotion(
    result: dict[str, Any],
    *,
    active_symbols: list[str],
    policy: PromotionPolicy,
    now: Optional[datetime] = None,
) -> PromotionDecision:
    """Apply fail-closed gates to an independent-account WF result."""
    errors: list[str] = []
    warnings: list[str] = []
    metadata = result.get("metadata", {})
    summary = result.get("summary", {})
    stats = result.get("coin_period_stats", [])
    steps = result.get("steps", [])
    live_selection = result.get("live_selection", {})
    if not isinstance(metadata, dict):
        metadata = {}
        errors.append("result metadata is malformed")
    if not isinstance(summary, dict):
        summary = {}
        errors.append("result summary is malformed")
    if not isinstance(stats, list):
        stats = []
        errors.append("result coin_period_stats is malformed")
    if not isinstance(steps, list):
        steps = []
        errors.append("result steps are malformed")
    if not isinstance(live_selection, dict):
        live_selection = {}
        errors.append("result live_selection is malformed")

    if metadata.get("account_model") != "independent_per_coin":
        errors.append("result does not declare independent_per_coin account model")

    end_raw = metadata.get("end_date")
    if not end_raw:
        errors.append("result metadata has no end_date")
    else:
        try:
            current = now or datetime.now(timezone.utc)
            age_days = (
                current - _parse_timestamp(str(end_raw))
            ).total_seconds() / 86400
            if age_days < -1:
                errors.append("result end_date is in the future")
            elif age_days > policy.max_result_age_days:
                errors.append(
                    f"result is stale ({age_days:.1f}d > "
                    f"{policy.max_result_age_days}d)"
                )
        except (TypeError, ValueError):
            errors.append("result end_date is invalid")

    aggregate_pnl = _finite_float(summary.get("total_portfolio_pnl"))
    if policy.require_positive_aggregate_pnl and (
        aggregate_pnl is None or aggregate_pnl <= 0
    ):
        errors.append("aggregate out-of-sample PnL is not positive")

    if len(steps) < policy.min_oos_steps:
        errors.append(
            f"only {len(steps)} OOS steps completed; minimum is {policy.min_oos_steps}"
        )
    if steps:
        profitable_steps = sum(
            1
            for step in steps
            if isinstance(step, dict)
            and (_finite_float(step.get("portfolio_pnl")) or 0.0) > 0
        )
        profitable_step_rate = profitable_steps / len(steps)
        if profitable_step_rate < policy.min_profitable_step_rate:
            errors.append(
                f"profitable OOS step rate {profitable_step_rate:.1%} is below "
                f"{policy.min_profitable_step_rate:.1%}"
            )

    strategy_config = metadata.get("strategy_config")
    if not isinstance(strategy_config, dict):
        errors.append("result has no strategy_config")
    else:
        for flag in (
            "use_live_exit",
            "use_trailing_entry",
            "use_adf_filter",
            "use_funding_filter",
        ):
            if strategy_config.get(flag) is not True:
                errors.append(f"realism gate requires {flag}=true")
        if strategy_config.get("use_ohlc_pseudo_ticks") is not False:
            errors.append("realism gate requires use_ohlc_pseudo_ticks=false")

        half_spread = _finite_float(strategy_config.get("half_spread_bps"))
        slippage = _finite_float(strategy_config.get("slippage_bps"))
        commission = _finite_float(strategy_config.get("commission_rate"))
        if half_spread is None or half_spread < policy.min_half_spread_bps:
            errors.append(
                f"half-spread assumption must be >= {policy.min_half_spread_bps} bps"
            )
        if slippage is None or slippage < policy.min_slippage_bps:
            errors.append(
                f"slippage assumption must be >= {policy.min_slippage_bps} bps"
            )
        if commission is None or commission <= 0:
            errors.append("commission_rate must be positive")

    current_candidates = live_selection.get("selected_coins")
    if not isinstance(current_candidates, list) or not current_candidates:
        errors.append("result has no current train-only live selection")
        current_candidates = []

    stats_by_coin = {
        str(row.get("coin", "")).upper(): row for row in stats if isinstance(row, dict)
    }
    eligible_by_coin: dict[str, dict[str, Any]] = {}
    for coin in current_candidates:
        normalized_coin = str(coin).upper()
        row = stats_by_coin.get(normalized_coin)
        if row is None:
            continue
        trade_net_pnl = _finite_float(row.get("trade_net_pnl"))
        selection_rate = _finite_float(row.get("selection_rate"))
        if (
            trade_net_pnl is not None
            and trade_net_pnl > 0
            and int(row.get("trade_total_trades", 0)) >= policy.min_oos_trades
            and selection_rate is not None
            and selection_rate >= policy.min_selection_rate
            and int(row.get("killed_count", 0)) == 0
        ):
            eligible_by_coin[normalized_coin] = row

    # Preserve the ranking from the current trailing train window. OOS metrics
    # only qualify candidates and never reorder/cherry-pick them.
    symbols = [
        f"{str(coin).upper()}/USDT:USDT"
        for coin in current_candidates
        if str(coin).upper() in eligible_by_coin
    ][: policy.max_pairs]
    if len(symbols) < policy.min_pairs:
        errors.append(
            f"only {len(symbols)} pairs passed; minimum is {policy.min_pairs}"
        )

    active = set(active_symbols)
    candidate = set(symbols)
    turnover: Optional[float] = None
    if active and candidate:
        turnover = len(active.symmetric_difference(candidate)) / max(
            1,
            len(active.union(candidate)),
        )
        if turnover > policy.max_turnover:
            errors.append(
                f"pair turnover {turnover:.1%} exceeds {policy.max_turnover:.1%}"
            )
    elif not active:
        warnings.append("no active pair version exists; turnover gate skipped")

    return PromotionDecision(
        symbols=symbols,
        errors=errors,
        warnings=warnings,
        turnover=turnover,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Promote a universe walk-forward result into MongoDB",
    )
    parser.add_argument("result", nargs="?", help="Universe WF JSON result")
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--activate", action="store_true")
    parser.add_argument("--rollback", action="store_true")
    parser.add_argument("--list", action="store_true", dest="list_versions")
    parser.add_argument("--min-pairs", type=int, default=2)
    parser.add_argument("--max-pairs", type=int, default=5)
    parser.add_argument("--min-oos-steps", type=int, default=6)
    parser.add_argument("--min-profitable-step-rate", type=float, default=0.50)
    parser.add_argument("--min-oos-trades", type=int, default=2)
    parser.add_argument("--min-selection-rate", type=float, default=0.20)
    parser.add_argument("--max-turnover", type=float, default=0.60)
    parser.add_argument("--max-result-age-days", type=int, default=21)
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    if sum((bool(args.result), args.rollback, args.list_versions)) != 1:
        parser.error("provide exactly one of RESULT, --rollback, or --list")
    if args.min_pairs <= 0 or args.max_pairs < args.min_pairs:
        parser.error("pair limits must be positive and max-pairs >= min-pairs")
    if args.min_oos_steps <= 0 or args.min_oos_trades <= 0:
        parser.error("OOS step/trade minimums must be positive")
    if not 0 <= args.min_selection_rate <= 1:
        parser.error("--min-selection-rate must be between 0 and 1")
    if not 0 <= args.min_profitable_step_rate <= 1:
        parser.error("--min-profitable-step-rate must be between 0 and 1")
    if not 0 <= args.max_turnover <= 1:
        parser.error("--max-turnover must be between 0 and 1")
    if args.max_result_age_days < 0:
        parser.error("--max-result-age-days must be non-negative")

    container = Container().init(args.env_file)
    repository = container.trading_pair_repository
    try:
        repository.create_indexes()

        if args.list_versions:
            active_id = repository.get_active_version_id()
            for version in repository.list_versions():
                marker = "*" if version.version_id == active_id else " "
                print(
                    f"{marker} {version.version_id} "
                    f"{version.created_at.isoformat()} "
                    f"{','.join(version.symbols)}"
                )
            return 0

        if args.rollback:
            target = repository.rollback()
            print(f"Rolled back active trading pairs to {target}")
            return 0

        result_path = Path(args.result).expanduser().resolve()
        raw = result_path.read_bytes()
        result = json.loads(raw)
        policy = PromotionPolicy(
            min_pairs=args.min_pairs,
            max_pairs=args.max_pairs,
            min_oos_steps=args.min_oos_steps,
            min_profitable_step_rate=args.min_profitable_step_rate,
            min_oos_trades=args.min_oos_trades,
            min_selection_rate=args.min_selection_rate,
            max_turnover=args.max_turnover,
            max_result_age_days=args.max_result_age_days,
        )
        active_version = repository.get_active_version()
        decision = evaluate_promotion(
            result,
            active_symbols=(
                active_version.symbols if active_version is not None else []
            ),
            policy=policy,
        )
        for warning in decision.warnings:
            print(f"WARNING: {warning}")
        if decision.errors:
            for error in decision.errors:
                print(f"REJECTED: {error}")
            return 2

        digest = hashlib.sha256(raw).hexdigest()
        end_date = _parse_timestamp(result["metadata"]["end_date"])
        version_id = f"wf-{end_date:%Y%m%d}-{digest[:10]}"
        version = repository.get_version(version_id)
        if version is None:
            version = TradingPairVersion(
                version_id=version_id,
                symbols=decision.symbols,
                source_result=str(result_path),
                source_sha256=digest,
                metrics={
                    "summary": result.get("summary", {}),
                    "policy": asdict(policy),
                    "turnover": decision.turnover,
                },
                strategy_config=result.get("metadata", {}).get(
                    "strategy_config",
                    {},
                ),
            )
            repository.create_version(version)
        else:
            print(f"Candidate version already exists: {version_id}")

        print(f"Candidate {version_id}: {', '.join(version.symbols)}")
        if args.activate:
            previous = repository.activate_version(version_id)
            print(f"Activated {version_id}; previous={previous}")
        else:
            print("Candidate saved but not activated (pass --activate to promote)")
        return 0
    finally:
        container.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
