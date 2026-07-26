"""Multiple-testing controls shared by live scans and backtests."""

from typing import Mapping, Set

import numpy as np


def benjamini_hochberg_passes(
    pvalues: Mapping[str, float],
    false_discovery_rate: float,
) -> Set[str]:
    """Return hypotheses passing the Benjamini-Hochberg step-up procedure.

    Running many ADF tests at a raw 5% threshold creates false pair discoveries
    as the universe grows. BH controls the expected false-discovery proportion
    while retaining more power than a Bonferroni correction.
    """
    if not 0 < false_discovery_rate <= 1:
        raise ValueError("false_discovery_rate must be in (0, 1]")

    finite = sorted(
        (
            (symbol, float(pvalue))
            for symbol, pvalue in pvalues.items()
            if np.isfinite(pvalue) and 0 <= pvalue <= 1
        ),
        key=lambda item: item[1],
    )
    total = len(finite)
    cutoff_rank = 0
    for rank, (_, pvalue) in enumerate(finite, start=1):
        if pvalue <= false_discovery_rate * rank / total:
            cutoff_rank = rank

    return {symbol for symbol, _ in finite[:cutoff_rank]}
