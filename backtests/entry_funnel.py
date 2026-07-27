"""
Per-filter rejection accounting for the backtest entry pipeline.

A backtest that reports "0 trades" says nothing about which gate consumed the
signal. This module counts every drop-off point so a sparse universe can be
diagnosed without re-instrumenting the simulator by hand.

Counters are plain ints keyed by symbol, so the whole structure survives the
pickle round-trip used by the universe walk-forward process workers.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union

# Ordered along the real entry pipeline, so a report reads top-down.
STAGES: Tuple[str, ...] = (
    # Bar-level screening (runs for every symbol on every closed 15m candle).
    "evaluated",
    "reject_correlation",
    "reject_beta_range",
    "reject_beta_drift",
    "reject_no_data",
    "reject_stability",
    # Candidate screening inside _check_entries.
    "reject_open_position",
    "reject_active_watch",
    "reject_cooldown",
    "reject_z_nan",
    "reject_z_below_threshold",
    "reject_z_extreme",
    "signal",
    "reject_hurst",
    "reject_halflife",
    "reject_adf_unavailable",
    "reject_adf_bh",
    "reject_funding_missing",
    "reject_funding_toxic",
    "reject_max_spreads",
    # Trailing entry outcomes.
    "watch_started",
    "watch_entered",
    "watch_timeout",
    "watch_false_alarm",
    "watch_sl_hit",
    "watch_correlation_drop",
    "watch_no_data",
    "watch_volatility_cancelled",
    "watch_no_margin",
    # Terminal.
    "reject_margin",
    "entered",
)

_STAGE_SET = frozenset(STAGES)

# Stages that consume a symbol before it ever becomes an entry candidate.
_PRE_SIGNAL_STAGES: Tuple[str, ...] = (
    "reject_correlation",
    "reject_beta_range",
    "reject_beta_drift",
    "reject_no_data",
    "reject_stability",
    "reject_open_position",
    "reject_active_watch",
    "reject_cooldown",
    "reject_z_nan",
    "reject_z_below_threshold",
    "reject_z_extreme",
)

# Stages that consume an already-qualified signal before a position exists.
_POST_SIGNAL_STAGES: Tuple[str, ...] = (
    "reject_hurst",
    "reject_halflife",
    "reject_adf_unavailable",
    "reject_adf_bh",
    "reject_funding_missing",
    "reject_funding_toxic",
    "reject_max_spreads",
    "reject_margin",
)

_WATCH_OUTCOME_STAGES: Tuple[str, ...] = (
    "watch_entered",
    "watch_timeout",
    "watch_false_alarm",
    "watch_sl_hit",
    "watch_correlation_drop",
    "watch_no_data",
    "watch_volatility_cancelled",
    "watch_no_margin",
)

# Watch cancellation reasons used by the simulator, mapped to funnel stages.
WATCH_REASON_STAGES: Dict[str, str] = {
    "TIMEOUT": "watch_timeout",
    "FALSE_ALARM": "watch_false_alarm",
    "SL_HIT": "watch_sl_hit",
    "CORRELATION_DROP": "watch_correlation_drop",
    "HURST_TRENDING": "watch_correlation_drop",
    "SIGNAL_LOST": "watch_false_alarm",
    "PARAM_INVALIDATED": "watch_false_alarm",
    "NO_DATA": "watch_no_data",
    "NO_MARGIN": "watch_no_margin",
}


class EntryFunnel:
    """Mutable per-symbol counters for every entry-pipeline drop-off."""

    def __init__(self) -> None:
        self.bars_processed: int = 0
        self.bars_volatility_blocked: int = 0
        self.bars_max_spreads_blocked: int = 0
        self._per_symbol: Dict[str, Counter] = defaultdict(Counter)

    # -- recording ---------------------------------------------------------

    def count_bar(self) -> None:
        self.bars_processed += 1

    def count_volatility_block(self) -> None:
        self.bars_volatility_blocked += 1

    def count_max_spreads_block(self) -> None:
        self.bars_max_spreads_blocked += 1

    def record(self, symbol: str, stage: str, count: int = 1) -> None:
        """Increment one stage counter. Unknown stages fail loudly."""
        if stage not in _STAGE_SET:
            raise ValueError(f"Unknown entry funnel stage: {stage!r}")
        if count:
            self._per_symbol[symbol][stage] += count

    def record_watch_reason(self, symbol: str, reason: Optional[str]) -> None:
        """Map a watch removal reason onto its funnel stage."""
        if reason is None:
            self.record(symbol, "watch_entered")
            return
        stage = WATCH_REASON_STAGES.get(reason)
        if stage is not None:
            self.record(symbol, stage)

    # -- reading -----------------------------------------------------------

    @property
    def symbols(self) -> List[str]:
        return sorted(self._per_symbol)

    def for_symbol(self, symbol: str) -> Dict[str, int]:
        return dict(self._per_symbol.get(symbol, Counter()))

    def totals(self) -> Dict[str, int]:
        combined: Counter = Counter()
        for counts in self._per_symbol.values():
            combined.update(counts)
        return {stage: combined.get(stage, 0) for stage in STAGES}

    # -- serialization -----------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "bars_processed": self.bars_processed,
            "bars_volatility_blocked": self.bars_volatility_blocked,
            "bars_max_spreads_blocked": self.bars_max_spreads_blocked,
            "per_symbol": {
                symbol: dict(counts)
                for symbol, counts in self._per_symbol.items()
                if counts
            },
        }

    @classmethod
    def from_dict(cls, payload: Optional[Mapping[str, Any]]) -> "EntryFunnel":
        funnel = cls()
        if not payload:
            return funnel
        funnel.bars_processed = int(payload.get("bars_processed", 0))
        funnel.bars_volatility_blocked = int(
            payload.get("bars_volatility_blocked", 0)
        )
        funnel.bars_max_spreads_blocked = int(
            payload.get("bars_max_spreads_blocked", 0)
        )
        for symbol, counts in (payload.get("per_symbol") or {}).items():
            for stage, value in counts.items():
                if stage in _STAGE_SET:
                    funnel._per_symbol[symbol][stage] += int(value)
        return funnel

    def merge(
        self, other: Optional[Union["EntryFunnel", Mapping[str, Any]]]
    ) -> None:
        """Accumulate another funnel (object or serialized dict) in place."""
        if other is None:
            return
        source = (
            other if isinstance(other, EntryFunnel) else EntryFunnel.from_dict(other)
        )
        self.bars_processed += source.bars_processed
        self.bars_volatility_blocked += source.bars_volatility_blocked
        self.bars_max_spreads_blocked += source.bars_max_spreads_blocked
        for symbol, counts in source._per_symbol.items():
            self._per_symbol[symbol].update(counts)


def summarize_totals(totals: Mapping[str, int]) -> Dict[str, int]:
    """Collapse stage counters into the few numbers worth eyeballing."""
    return {
        "evaluated": totals.get("evaluated", 0),
        "pre_signal_rejected": sum(
            totals.get(stage, 0) for stage in _PRE_SIGNAL_STAGES
        ),
        "signal": totals.get("signal", 0),
        "post_signal_rejected": sum(
            totals.get(stage, 0) for stage in _POST_SIGNAL_STAGES
        ),
        "watch_started": totals.get("watch_started", 0),
        "entered": totals.get("entered", 0),
    }


def top_blockers(
    counts: Mapping[str, int],
    limit: int = 3,
    exclude: Iterable[str] = ("reject_z_below_threshold",),
) -> List[Tuple[str, int]]:
    """
    Return the biggest rejection stages for one symbol.

    `reject_z_below_threshold` is excluded by default: it fires on almost every
    bar by construction and drowns out the gates that are actually selective.
    """
    excluded = set(exclude)
    blockers = [
        (stage, counts.get(stage, 0))
        for stage in STAGES
        if stage.startswith("reject_") and stage not in excluded
    ]
    blockers = [item for item in blockers if item[1] > 0]
    blockers.sort(key=lambda item: item[1], reverse=True)
    return blockers[:limit]


def format_funnel_report(
    funnel: EntryFunnel,
    *,
    title: str = "ENTRY FUNNEL",
    width: int = 78,
) -> List[str]:
    """Render the aggregate funnel as printable lines."""
    totals = funnel.totals()
    evaluated = totals.get("evaluated", 0)
    signal = totals.get("signal", 0)

    lines: List[str] = ["", "-" * width, title, "-" * width]

    lines.append(f"  bars processed:              {funnel.bars_processed:>10,}")
    if funnel.bars_processed:
        vol_pct = funnel.bars_volatility_blocked / funnel.bars_processed * 100
        cap_pct = funnel.bars_max_spreads_blocked / funnel.bars_processed * 100
        lines.append(
            f"  bars blocked by volatility:  "
            f"{funnel.bars_volatility_blocked:>10,} ({vol_pct:.1f}%)"
        )
        lines.append(
            f"  bars blocked by max spreads: "
            f"{funnel.bars_max_spreads_blocked:>10,} ({cap_pct:.1f}%)"
        )

    if evaluated == 0:
        lines.append("  no symbol observations recorded")
        lines.append("-" * width)
        return lines

    lines.append("")
    lines.append(
        f"  {'stage':<28}{'count':>12}{'% evaluated':>14}{'% of signals':>14}"
    )

    for stage in STAGES:
        count = totals.get(stage, 0)
        if count == 0 and stage not in ("evaluated", "signal", "entered"):
            continue
        eval_pct = f"{count / evaluated * 100:.2f}%"
        signal_pct = (
            f"{count / signal * 100:.1f}%"
            if signal > 0 and stage in _POST_SIGNAL_STAGES + _WATCH_OUTCOME_STAGES
            else ""
        )
        marker = "→" if stage in ("evaluated", "signal", "entered") else " "
        lines.append(
            f"  {marker} {stage:<26}{count:>12,}{eval_pct:>14}{signal_pct:>14}"
        )

    lines.append("-" * width)
    return lines


def format_symbol_funnel_table(
    funnel: EntryFunnel,
    *,
    title: str = "ENTRY FUNNEL BY COIN",
    width: int = 110,
    limit: Optional[int] = None,
) -> List[str]:
    """Render one row per symbol: how far it got and what stopped it."""
    rows: List[Tuple[str, Dict[str, int]]] = [
        (symbol, funnel.for_symbol(symbol)) for symbol in funnel.symbols
    ]
    if not rows:
        return []

    # Coins that never produced a signal are the ones worth explaining first.
    rows.sort(
        key=lambda item: (
            item[1].get("entered", 0),
            item[1].get("watch_started", 0),
            item[1].get("signal", 0),
        )
    )
    if limit is not None:
        rows = rows[:limit]

    lines: List[str] = ["", "-" * width, title, "-" * width]
    lines.append(
        f"  {'coin':<14}{'evaluated':>11}{'signals':>9}{'watches':>9}"
        f"{'entered':>9}  top blockers (excl. z-below-threshold)"
    )
    for symbol, counts in rows:
        base = symbol.split("/")[0]
        blockers = top_blockers(counts)
        blockers_str = (
            ", ".join(
                f"{stage.removeprefix('reject_')}={value:,}"
                for stage, value in blockers
            )
            or "-"
        )
        lines.append(
            f"  {base:<14}"
            f"{counts.get('evaluated', 0):>11,}"
            f"{counts.get('signal', 0):>9,}"
            f"{counts.get('watch_started', 0):>9,}"
            f"{counts.get('entered', 0):>9,}"
            f"  {blockers_str}"
        )
    lines.append("-" * width)
    return lines
