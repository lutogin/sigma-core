"""
Funding Filter Service.

Filters pairs based on net funding cost to avoid toxic funding environments.
We block entry if predicted funding costs exceed threshold per 8 hours.

Logic:
- For LONG_SPREAD (Long COIN, Short ETH): Net Cost = ETH_Rate - COIN_Rate
- For SHORT_SPREAD (Short COIN, Long ETH): Net Cost = COIN_Rate - ETH_Rate

Binance funding convention:
- Rate > 0: Longs pay, Shorts receive
- Rate < 0: Longs receive, Shorts pay
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from src.integrations.exchange import BinanceClient


@dataclass
class FundingCheckResult:
    """Result of funding rate check for a pair."""

    is_safe: bool
    coin_symbol: str
    coin_funding_rate: float
    eth_funding_rate: float
    net_funding_cost: float  # Negative = we pay, Positive = we receive
    spread_side: str  # "LONG_SPREAD" or "SHORT_SPREAD"
    block_reason: Optional[str] = None
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)

    @property
    def net_cost_pct(self) -> float:
        """Net funding cost as percentage."""
        return self.net_funding_cost * 100


class FundingFilterService:
    """
    Funding Filter for spread trading.

    Blocks entry when net funding cost is too high (toxic environment).

    Default threshold: -0.05% per 8h (5x standard rate against us).
    Standard Binance rate is 0.01%, so we allow up to 5x before blocking.
    """

    # Default: block if we lose more than 0.05% per 8h on funding
    DEFAULT_MAX_FUNDING_COST = -0.0005  # -0.05%

    def __init__(
        self,
        logger,
        exchange_client: "BinanceClient",
        primary_symbol: str = "ETH/USDT:USDT",
        max_funding_cost_threshold: Optional[float] = None,
    ):
        """
        Initialize FundingFilterService.

        Args:
            logger: Logger instance
            exchange_client: BinanceClient for fetching funding rates
            primary_symbol: Primary/index symbol (ETH)
            max_funding_cost_threshold: Max allowed net funding cost per 8h
                                        (negative = loss). Default: -0.0005 (-0.05%)
        """
        self._logger = logger
        self._exchange_client = exchange_client
        self._primary_symbol = primary_symbol
        self._max_funding_cost = (
            max_funding_cost_threshold
            if max_funding_cost_threshold is not None
            else self.DEFAULT_MAX_FUNDING_COST
        )

    @property
    def threshold(self) -> float:
        """Get the funding cost threshold."""
        return self._max_funding_cost

    def _normalize_rate_to_8h(self, rate: float, interval_hours: int) -> float:
        """
        Normalize funding rate to 8-hour equivalent.

        Binance has different funding intervals: 1h, 2h, 4h, 8h.
        We normalize everything to 8h for consistent comparison.

        Args:
            rate: Funding rate for the given interval
            interval_hours: Funding interval in hours (1, 2, 4, or 8)

        Returns:
            Rate normalized to 8-hour period
        """
        if interval_hours <= 0:
            return rate
        multiplier = 8 / interval_hours
        return rate * multiplier

    async def check(
        self,
        coin_symbol: str,
        spread_side: str,
    ) -> FundingCheckResult:
        """
        Check if funding environment is safe for entry.

        Args:
            coin_symbol: The coin symbol (e.g., "LINK/USDT:USDT")
            spread_side: "LONG_SPREAD" or "SHORT_SPREAD"

        Returns:
            FundingCheckResult with safety status and metrics.
        """
        # Fetch current funding rates
        coin_funding = await self._exchange_client.get_current_funding_rate(coin_symbol)
        eth_funding = await self._exchange_client.get_current_funding_rate(
            self._primary_symbol
        )

        if coin_funding is None or eth_funding is None:
            self._logger.warning(
                f"Failed to fetch funding rates for {coin_symbol} or {self._primary_symbol}"
            )
            # Default to safe if we can't check (don't block on API errors)
            return FundingCheckResult(
                is_safe=True,
                coin_symbol=coin_symbol,
                coin_funding_rate=0.0,
                eth_funding_rate=0.0,
                net_funding_cost=0.0,
                spread_side=spread_side,
                block_reason="FUNDING_DATA_UNAVAILABLE",
            )

        # Normalize rates to 8-hour equivalent
        coin_rate_8h = self._normalize_rate_to_8h(
            coin_funding.funding_rate, coin_funding.funding_interval_hours
        )
        eth_rate_8h = self._normalize_rate_to_8h(
            eth_funding.funding_rate, eth_funding.funding_interval_hours
        )

        # Calculate net funding profit/cost based on spread side (using 8h normalized rates)
        net_funding = self._calculate_net_funding(coin_rate_8h, eth_rate_8h, spread_side)

        # Check if funding is toxic
        is_safe = net_funding >= self._max_funding_cost
        block_reason = None

        if not is_safe:
            block_reason = (
                f"TOXIC_FUNDING: Net cost {net_funding * 100:.3f}% per 8h "
                f"< threshold {self._max_funding_cost * 100:.3f}%"
            )
            self._logger.warning(
                f"⛔ FUNDING FILTER: {coin_symbol} ({spread_side}) | {block_reason} | "
                f"COIN={coin_rate_8h * 100:.4f}%/8h (interval={coin_funding.funding_interval_hours}h), "
                f"ETH={eth_rate_8h * 100:.4f}%/8h (interval={eth_funding.funding_interval_hours}h)"
            )
        else:
            self._logger.debug(
                f"✅ Funding OK: {coin_symbol} ({spread_side}) | "
                f"Net={net_funding * 100:.4f}%/8h | "
                f"COIN={coin_rate_8h * 100:.4f}%/8h, ETH={eth_rate_8h * 100:.4f}%/8h"
            )

        return FundingCheckResult(
            is_safe=is_safe,
            coin_symbol=coin_symbol,
            coin_funding_rate=coin_rate_8h,  # Store normalized 8h rate
            eth_funding_rate=eth_rate_8h,    # Store normalized 8h rate
            net_funding_cost=net_funding,
            spread_side=spread_side,
            block_reason=block_reason,
        )

    def _calculate_net_funding(
        self,
        coin_rate: float,
        eth_rate: float,
        spread_side: str,
    ) -> float:
        """
        Calculate net funding profit/cost for the spread.

        Positive = we receive funding (good)
        Negative = we pay funding (bad)

        Args:
            coin_rate: Funding rate for coin (e.g., 0.0001 = 0.01%)
            eth_rate: Funding rate for ETH
            spread_side: "LONG_SPREAD", "SHORT_SPREAD", "LONG", or "SHORT"

        Returns:
            Net funding as decimal (e.g., -0.0005 = -0.05%)
        """
        # Normalize spread_side to handle both formats
        side_upper = spread_side.upper()
        is_long = side_upper in ("LONG_SPREAD", "LONG")
        is_short = side_upper in ("SHORT_SPREAD", "SHORT")

        if is_long:
            # Long COIN (pay if rate > 0), Short ETH (receive if rate > 0)
            # Net = what we receive from ETH short - what we pay for COIN long
            return eth_rate - coin_rate
        elif is_short:
            # Short COIN (receive if rate > 0), Long ETH (pay if rate > 0)
            # Net = what we receive from COIN short - what we pay for ETH long
            return coin_rate - eth_rate
        else:
            self._logger.error(f"Unknown spread_side: {spread_side}")
            return 0.0

    async def check_batch(
        self,
        pairs_with_sides: list[tuple[str, str]],
    ) -> dict[str, FundingCheckResult]:
        """
        Check funding for multiple pairs at once.

        Args:
            pairs_with_sides: List of (coin_symbol, spread_side) tuples

        Returns:
            Dict mapping coin_symbol to FundingCheckResult
        """
        results = {}

        # Get all unique symbols we need funding for
        symbols_needed = set()
        for coin_symbol, _ in pairs_with_sides:
            symbols_needed.add(coin_symbol)
        symbols_needed.add(self._primary_symbol)

        # Fetch all funding rates in batch
        all_rates = await self._exchange_client.get_funding_rates_batch(
            list(symbols_needed)
        )

        # Build lookup dict with FundingRateData objects (need interval for normalization)
        rate_lookup = {r.symbol: r for r in all_rates}

        eth_data = rate_lookup.get(self._primary_symbol)
        eth_rate_8h = (
            self._normalize_rate_to_8h(eth_data.funding_rate, eth_data.funding_interval_hours)
            if eth_data else 0.0
        )

        for coin_symbol, spread_side in pairs_with_sides:
            coin_data = rate_lookup.get(coin_symbol)

            if coin_data is None:
                self._logger.warning(
                    f"Funding rate not found for {coin_symbol}, defaulting to 0"
                )
                coin_rate_8h = 0.0
            else:
                coin_rate_8h = self._normalize_rate_to_8h(
                    coin_data.funding_rate, coin_data.funding_interval_hours
                )

            net_funding = self._calculate_net_funding(coin_rate_8h, eth_rate_8h, spread_side)
            is_safe = net_funding >= self._max_funding_cost
            block_reason = None

            if not is_safe:
                block_reason = (
                    f"TOXIC_FUNDING: Net cost {net_funding * 100:.3f}% per 8h "
                    f"< threshold {self._max_funding_cost * 100:.3f}%"
                )
                self._logger.warning(
                    f"⛔ FUNDING FILTER: {coin_symbol} ({spread_side}) | {block_reason}"
                )

            results[coin_symbol] = FundingCheckResult(
                is_safe=is_safe,
                coin_symbol=coin_symbol,
                coin_funding_rate=coin_rate_8h,  # Normalized to 8h
                eth_funding_rate=eth_rate_8h,    # Normalized to 8h
                net_funding_cost=net_funding,
                spread_side=spread_side,
                block_reason=block_reason,
            )

        return results
