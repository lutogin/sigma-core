"""
Funding Filter Module.

Filters pairs based on funding rate costs to avoid toxic funding environments.
"""

from .funding_filter import FundingFilterService, FundingCheckResult

__all__ = [
    "FundingFilterService",
    "FundingCheckResult",
]
