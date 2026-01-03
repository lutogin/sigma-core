#!/usr/bin/env python3
"""
Statistical Arbitrage Backtest Runner.

Usage:
    python run_backtest.py --start 2024-06-01 --end 2024-12-15
    python run_backtest.py --start 2024-09-01  # end defaults to now

Loads historical data and simulates the stat-arb strategy on each 15m candle.
"""

import argparse
import asyncio
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import math

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Add the parent directory to sys.path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.domain.data_loader import AsyncDataLoaderService
from src.domain.screener.correlation import CorrelationService
from src.domain.screener.hurst_filter import HurstFilterService
from src.domain.screener.adf_filter import ADFFilterService
from src.domain.screener.halflife_filter import HalfLifeFilterService
from src.domain.screener.volatility_filter import VolatilityFilterService
from src.domain.screener.z_score import ZScoreService, ZScoreResult
from src.infra.container import Container
from src.domain.utils import get_timeframe_minutes


# =============================================================================
# Historical Funding Rate Cache
# =============================================================================


class HistoricalFundingCache:
    """
    Cache for historical funding rates.

    Loads funding rates once at backtest start and provides
    lookup by symbol and timestamp.
    """

    def __init__(self, logger):
        self._logger = logger
        # symbol -> list of (funding_time, rate_8h_normalized)
        self._cache: Dict[str, List[Tuple[datetime, float]]] = {}
        # symbol -> funding_interval_hours (for normalization)
        self._intervals: Dict[str, int] = {}

    async def load(
        self,
        exchange_client,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
    ) -> None:
        """
        Load historical funding rates for all symbols.

        Args:
            exchange_client: BinanceClient instance
            symbols: List of symbols to load funding for
            start_date: Start date for funding data
            end_date: End date for funding data
        """
        self._logger.info(
            f"Loading historical funding rates for {len(symbols)} symbols..."
        )

        for symbol in symbols:
            try:
                rates = await exchange_client.get_historical_funding_rates(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                )

                if rates:
                    # Store interval for this symbol
                    self._intervals[symbol] = rates[0].funding_interval_hours

                    # Normalize all rates to 8h and store
                    normalized = []
                    for r in rates:
                        rate_8h = self._normalize_to_8h(
                            r.funding_rate, r.funding_interval_hours
                        )
                        normalized.append((r.funding_time, rate_8h))

                    # Sort by time
                    normalized.sort(key=lambda x: x[0])
                    self._cache[symbol] = normalized

                    self._logger.debug(
                        f"  {symbol}: {len(rates)} funding records, "
                        f"interval={self._intervals[symbol]}h"
                    )
                else:
                    self._logger.warning(f"  {symbol}: No funding data found")
                    self._cache[symbol] = []

                # Rate limit protection
                await asyncio.sleep(0.1)

            except Exception as e:
                self._logger.warning(f"  {symbol}: Failed to load funding - {e}")
                self._cache[symbol] = []

        total_records = sum(len(v) for v in self._cache.values())
        self._logger.info(f"Loaded {total_records} total funding records")

    def _normalize_to_8h(self, rate: float, interval_hours: int) -> float:
        """Normalize funding rate to 8-hour equivalent."""
        if interval_hours <= 0:
            return rate
        return rate * (8 / interval_hours)

    def get_rate_at(self, symbol: str, timestamp: datetime) -> Optional[float]:
        """
        Get the funding rate (normalized to 8h) at a specific timestamp.

        Returns the most recent funding rate before or at the timestamp.

        Args:
            symbol: Symbol to look up
            timestamp: Timestamp to get rate for

        Returns:
            Funding rate normalized to 8h, or None if not found
        """
        if symbol not in self._cache or not self._cache[symbol]:
            return None

        rates = self._cache[symbol]

        # Binary search for the most recent rate <= timestamp
        left, right = 0, len(rates) - 1
        result = None

        while left <= right:
            mid = (left + right) // 2
            if rates[mid][0] <= timestamp:
                result = rates[mid][1]
                left = mid + 1
            else:
                right = mid - 1

        return result

    def calculate_net_funding(
        self,
        coin_symbol: str,
        primary_symbol: str,
        timestamp: datetime,
        spread_side: str,
    ) -> Optional[float]:
        """
        Calculate net funding cost for a spread at a specific timestamp.

        Args:
            coin_symbol: Coin symbol (e.g., "LINK/USDT:USDT")
            primary_symbol: Primary/index symbol (e.g., "ETH/USDT:USDT")
            timestamp: Timestamp to calculate for
            spread_side: "long" or "short"

        Returns:
            Net funding cost (negative = we pay), or None if data unavailable
        """
        coin_rate = self.get_rate_at(coin_symbol, timestamp)
        primary_rate = self.get_rate_at(primary_symbol, timestamp)

        if coin_rate is None or primary_rate is None:
            return None

        if spread_side == "long":
            # Long COIN (pay if rate > 0), Short PRIMARY (receive if rate > 0)
            return primary_rate - coin_rate
        else:
            # Short COIN (receive if rate > 0), Long PRIMARY (pay if rate > 0)
            return coin_rate - primary_rate

    def calculate_funding_pnl(
        self,
        coin_symbol: str,
        primary_symbol: str,
        entry_time: datetime,
        exit_time: datetime,
        spread_side: str,
        coin_size_usdt: float,
        primary_size_usdt: float,
        leverage: int = 1,
    ) -> float:
        """
        Calculate total funding PnL for a position over its lifetime.

        Funding is paid/received at each funding interval (8h normalized).
        We sum up all funding payments between entry and exit.

        Args:
            coin_symbol: Coin symbol
            primary_symbol: Primary symbol (ETH)
            entry_time: Position entry time
            exit_time: Position exit time
            spread_side: "long" or "short"
            coin_size_usdt: COIN leg size in USDT
            primary_size_usdt: PRIMARY leg size in USDT
            leverage: Position leverage

        Returns:
            Total funding PnL in USDT (positive = received, negative = paid)
        """
        if coin_symbol not in self._cache or primary_symbol not in self._cache:
            return 0.0

        total_funding_pnl = 0.0

        # Get all funding events for COIN between entry and exit
        coin_rates = self._cache.get(coin_symbol, [])
        primary_rates = self._cache.get(primary_symbol, [])

        # Build a set of funding timestamps that occurred during position
        funding_times = set()

        for ts, _ in coin_rates:
            if entry_time <= ts < exit_time:
                funding_times.add(ts)

        for ts, _ in primary_rates:
            if entry_time <= ts < exit_time:
                funding_times.add(ts)

        # For each funding event, calculate the PnL
        for funding_ts in sorted(funding_times):
            coin_rate = self.get_rate_at(coin_symbol, funding_ts)
            primary_rate = self.get_rate_at(primary_symbol, funding_ts)

            if coin_rate is None or primary_rate is None:
                continue

            # Calculate funding for each leg
            # Funding PnL = position_size * funding_rate * leverage
            # Sign depends on position direction

            if spread_side == "long":
                # Long COIN: pay if rate > 0, receive if rate < 0
                coin_funding = -coin_size_usdt * coin_rate * leverage
                # Short PRIMARY: receive if rate > 0, pay if rate < 0
                primary_funding = primary_size_usdt * primary_rate * leverage
            else:
                # Short COIN: receive if rate > 0, pay if rate < 0
                coin_funding = coin_size_usdt * coin_rate * leverage
                # Long PRIMARY: pay if rate > 0, receive if rate < 0
                primary_funding = -primary_size_usdt * primary_rate * leverage

            total_funding_pnl += coin_funding + primary_funding

        return total_funding_pnl


# =============================================================================
# Backtest Configuration
# =============================================================================


@dataclass
class BacktestConfig:
    """Backtest configuration parameters."""

    # Capital
    initial_balance: float = 40_000.0  # Starting capital in USDT
    position_size_pct: float = 0.33  # 3% of capital per spread
    max_spreads: int = 3  # Maximum concurrent spread positions

    # Strategy thresholds (from settings)
    z_entry_threshold: float = 2.0  # |Z| >= this to enter
    z_tp_threshold: float = 0.25  # |Z| <= this to take profit
    z_sl_threshold: float = 4.0  # |Z| >= this to stop loss
    min_correlation: float = 0.8  # Minimum correlation to trade

    # Fees
    maker_fee: float = 0.0002  # 0.02% maker fee
    taker_fee: float = 0.0004  # 0.04% taker fee
    use_limit_orders: bool = True  # Use maker fees (limit orders)

    # Risk
    leverage: int = 5  # Leverage multiplier

    # Cooldown after adverse exits (SL, CORRELATION_DROP, TIMEOUT, HURST_TRENDING)
    # 2 bars = 30 minutes for 15m timeframe
    cooldown_bars: int = 16  # 4 hours

    # Maximum position duration before forced exit (in bars)
    # 144 bars = 32 hours for 15m timeframe
    # 96 bars = 24 hours for 15m timeframe
    max_position_bars: int = 96  # 24 hours

    # Funding filter
    # Block entry if net funding cost exceeds this threshold per 8h
    # -0.0005 = -0.05% (5x standard rate of 0.01%)
    max_funding_cost_threshold: float = -0.0005
    use_funding_filter: bool = True  # Enable/disable funding filter

    # Trading pairs (from settings or CLI)
    consistent_pairs: List[str] = field(default_factory=list)

    use_dynamic_tp: bool = True  # Use dynamic take profit

    # Hurst tolerance for open positions (hysteresis)
    # Entry requires H < threshold (0.45), holding allows H < threshold + tolerance (0.50)
    hurst_watch_tolerance: float = 0.005

    # Trailing Entry settings (simulation of live EntryObserverService)
    use_trailing_entry: bool = False  # Enable trailing entry simulation
    trailing_pullback: float = 0.05  # Z-score pullback for reversal confirmation
    trailing_timeout_minutes: int = 240  # Max watch duration before cancellation
    false_alarm_hysteresis: float = (
        0.35  # Cancel watch only if Z drops this much below threshold
    )

    use_adf_filter: bool = True  # Enable ADF filter


@dataclass
class Position:
    """
    Active spread position (two legs).

    For stat-arb we trade the SPREAD:
    - Long spread: Long COIN, Short β×PRIMARY (expect Z to rise toward 0)
    - Short spread: Short COIN, Long β×PRIMARY (expect Z to fall toward 0)
    """

    symbol: str
    side: str  # "long" or "short" (spread direction)
    entry_z_score: float
    entry_beta: float
    size_usdt: float  # Notional size of the spread
    entry_time: datetime
    entry_bar_idx: int  # Bar index at entry (for timeout tracking)

    # Leg 1: COIN
    coin_entry_price: float
    coin_size: float  # In USDT terms

    # Leg 2: PRIMARY (hedge)
    primary_entry_price: float
    primary_size: float  # = coin_size * beta, in USDT terms

    # Peak Z-score during trailing entry (for dynamic TP calculation)
    # If trailing entry was used, this is the max |Z| before pullback
    # Otherwise equals abs(entry_z_score)
    peak_z_score: float = 0.0


@dataclass
class BacktestWatchCandidate:
    """
    Watch candidate for trailing entry simulation.

    Tracks a symbol after Z-score crosses entry threshold,
    waiting for pullback confirmation before actual entry.
    """

    symbol: str
    side: str  # "long" or "short"
    initial_z: float
    max_z: float  # Maximum |Z| reached during watch
    start_bar_idx: int
    start_time: datetime
    beta: float
    spread_mean: float
    spread_mean: float
    spread_std: float
    z_entry_threshold: float
    z_sl_threshold: float
    correlation: float


@dataclass
class Trade:
    """Completed trade record."""

    symbol: str
    side: str
    entry_time: datetime
    exit_time: datetime
    entry_z_score: float
    exit_z_score: float
    entry_price: float
    exit_price: float
    size_usdt: float
    pnl: float
    pnl_pct: float
    exit_reason: str  # "TP", "SL", "CORRELATION_DROP"
    duration_hours: float
    funding_pnl: float = 0.0  # PnL from funding payments (positive = received)


@dataclass
class SymbolStats:
    """Per-symbol trading statistics."""

    symbol: str
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_pnl: float
    avg_pnl: float
    profit_factor: float
    avg_duration_hours: float
    tp_count: int
    sl_count: int
    other_exits: int


@dataclass
class BacktestResult:
    """Backtest results summary."""

    # Performance
    initial_balance: float
    final_balance: float
    total_pnl: float
    total_pnl_pct: float

    # Trades
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float

    # Risk metrics
    max_drawdown: float
    max_drawdown_pct: float
    sharpe_ratio: float
    profit_factor: float
    avg_trade_pnl: float
    avg_trade_duration_hours: float

    # By exit reason
    trades_by_reason: Dict[str, int] = field(default_factory=dict)

    # Equity curve
    equity_curve: pd.Series = field(default_factory=pd.Series)
    drawdown_curve: pd.Series = field(default_factory=pd.Series)

    # All trades
    trades: List[Trade] = field(default_factory=list)

    # Per-symbol statistics
    symbol_stats: List["SymbolStats"] = field(default_factory=list)

    # Funding filter stats
    funding_blocked_entries: int = 0
    total_funding_pnl: float = 0.0  # Total funding PnL across all trades

    # Trailing entry stats
    trailing_cancelled_entries: int = 0  # Watches cancelled (timeout, false_alarm, SL)


class StatArbBacktest:
    """
    Statistical Arbitrage Backtester.

    Simulates the spread trading strategy on historical data.
    For each 15m candle:
    1. Calculate correlation and z-score
    2. Check for entry signals
    3. Check for exit signals on open positions
    4. Track PnL and equity
    """

    def __init__(
        self,
        config: BacktestConfig,
        settings,
        data_loader: AsyncDataLoaderService,
        correlation_service: CorrelationService,
        z_score_service: ZScoreService,
        volatility_filter_service: VolatilityFilterService,
        hurst_filter_service: HurstFilterService,
        adf_filter_service: Optional[ADFFilterService] = None,
        halflife_filter_service: Optional[HalfLifeFilterService] = None,
        funding_cache: Optional[HistoricalFundingCache] = None,
    ):
        self.config = config
        self.settings = settings
        self.data_loader = data_loader
        self.correlation_service = correlation_service
        self.z_score_service = z_score_service
        self.volatility_filter_service = volatility_filter_service
        self.hurst_filter_service = hurst_filter_service
        self.adf_filter_service = adf_filter_service
        self.halflife_filter_service = halflife_filter_service
        self.funding_cache = funding_cache

        self.primary_pair = settings.PRIMARY_PAIR
        self.consistent_pairs = config.consistent_pairs
        self.timeframe = settings.TIMEFRAME
        self.lookback_window_days = settings.LOOKBACK_WINDOW_DAYS

        # Calculate timeframe in minutes for time-based TP coefficient
        self._timeframe_minutes = get_timeframe_minutes(self.timeframe)

        # State
        self.balance = config.initial_balance
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.equity_history: List[Tuple[datetime, float]] = []

        # Cooldown tracking: symbol -> unlock_time (datetime)
        # Symbols in cooldown are blocked from entry after SL or CORRELATION_DROP
        self.symbol_cooldowns: Dict[str, datetime] = {}

        # Funding filter stats
        self.funding_blocked_count: int = 0

        # Trailing entry state
        self.watch_candidates: Dict[str, BacktestWatchCandidate] = {}
        self.trailing_cancelled_count: int = 0  # Stats: cancelled watches

    async def run(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> BacktestResult:
        """
        Run backtest from start_date to end_date.

        Args:
            start_date: Backtest start date
            end_date: Backtest end date

        Returns:
            BacktestResult with performance metrics
        """
        print(f"Starting backtest: {start_date.date()} to {end_date.date()}")

        # Calculate how much data we need:
        # - DYNAMIC_THRESHOLD_WINDOW_BARS (440) for dynamic threshold calculation
        # - LOOKBACK_WINDOW for rolling z-score calculation (288 for 3 days @ 15m)
        # - LOOKBACK_WINDOW for rolling beta calculation (288 for 3 days @ 15m)
        # Total: 440 + 288 + 288 = 1016 candles minimum
        # For 15m: 1016 candles = ~10.6 days
        # We use 3x lookback + 2 days buffer to be safe
        warmup_days = self.lookback_window_days * 3 + 2
        data_start = start_date - timedelta(days=warmup_days)

        print(
            f"Loading data from {data_start.date()} (warmup: {warmup_days} days for rolling calculations)"
        )

        # Load all data upfront
        all_symbols = [self.primary_pair] + self.consistent_pairs
        raw_data = await self.data_loader.load_ohlcv_bulk(
            symbols=all_symbols,
            start_time=data_start,
            end_time=end_date,
            batch_size=10,
            timeframe=self.timeframe,
        )

        if not raw_data or self.primary_pair not in raw_data:
            raise ValueError("Failed to load data for primary pair")

        # Align all data to primary pair's index
        aligned_data = self._align_data(raw_data)

        # Get candle timestamps for iteration
        primary_df = aligned_data[self.primary_pair]
        all_timestamps = primary_df.index.tolist()

        # Find start index (after warmup period)
        start_idx = None
        for i, ts in enumerate(all_timestamps):
            if ts >= start_date:
                start_idx = i
                break

        if start_idx is None:
            raise ValueError(f"No data found after {start_date}")

        # Calculate minimum candles needed for calculations:
        # - rolling beta needs LOOKBACK_WINDOW candles
        # - rolling z-score needs another LOOKBACK_WINDOW candles
        # - dynamic threshold needs DYNAMIC_THRESHOLD_WINDOW_BARS (440) candles
        # Total: 440 + 288 + 288 = 1016 candles for 15m timeframe
        timeframe_minutes = get_timeframe_minutes(self.timeframe)
        candles_per_day = 24 * 60 // timeframe_minutes
        # Use 3x lookback to ensure enough data for all rolling calculations
        min_candles = candles_per_day * self.lookback_window_days * 3

        if start_idx < min_candles:
            print(
                f"Not enough warmup candles. Need {min_candles}, have {start_idx}. "
                f"Adjusting start index."
            )
            start_idx = min_candles

        print(
            f"Backtesting {len(all_timestamps) - start_idx} candles "
            f"({(len(all_timestamps) - start_idx) / candles_per_day:.1f} days)"
        )

        # Initialize equity tracking
        self.balance = self.config.initial_balance
        self.positions = {}
        self.trades = []
        self.equity_history = [(all_timestamps[start_idx], self.balance)]
        self.symbol_cooldowns = {}  # Reset cooldowns

        # Main backtest loop
        for i in range(start_idx, len(all_timestamps)):
            current_time = all_timestamps[i]

            # Slice data up to current candle (inclusive)
            window_data = {
                symbol: df.iloc[: i + 1] for symbol, df in aligned_data.items()
            }

            # Run strategy logic
            await self._process_candle(current_time, i, window_data)

            # Record equity (balance + unrealized PnL)
            equity = self._calculate_equity(window_data)
            self.equity_history.append((current_time, equity))

            # Progress logging every 1000 candles
            if (i - start_idx) % 1000 == 0:
                progress = (i - start_idx) / (len(all_timestamps) - start_idx) * 100
                print(
                    f"Progress: {progress:.1f}% | "
                    f"Balance: ${self.balance:.2f} | "
                    f"Open positions: {len(self.positions)} | "
                    f"Trades: {len(self.trades)}"
                )

        # Close any remaining positions at end
        await self._close_all_positions(
            all_timestamps[-1], aligned_data, "BACKTEST_END"
        )

        # Calculate results
        result = self._calculate_results()

        return result

    def _align_data(self, raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Align all data to common time index with forward fill."""
        common_index = raw_data[self.primary_pair].index

        aligned = {}
        for symbol, df in raw_data.items():
            if symbol == self.primary_pair:
                aligned[symbol] = df
            else:
                # Reindex to common index and forward fill missing values
                aligned_df = df.reindex(common_index, method="ffill")

                # Check for NaN values after reindex
                nan_count = aligned_df["close"].isna().sum()
                if nan_count > 0:
                    # Forward fill to handle any remaining NaNs
                    aligned_df = aligned_df.ffill()
                    # If still NaN at the start (no data before), backfill from first valid value
                    aligned_df = aligned_df.bfill()

                    remaining_nan = aligned_df["close"].isna().sum()
                    if remaining_nan > 0:
                        print(
                            f"  ❌ {symbol}: Has {remaining_nan} NaN values after fill - SKIPPING"
                        )
                        continue

                aligned[symbol] = aligned_df

        return aligned

    async def _process_candle(
        self,
        current_time: datetime,
        current_bar_idx: int,
        window_data: Dict[str, pd.DataFrame],
    ) -> None:
        """Process a single candle: check exits, then entries."""

        # Calculate correlation and z-score
        correlation_results = self.correlation_service.calculate(
            primary_symbol=self.primary_pair,
            ohlcv=window_data,
        )

        if not correlation_results:
            return

        z_score_results = self.z_score_service.calculate(
            primary_symbol=self.primary_pair,
            correlation_results=correlation_results,
            ohlcv=window_data,
        )

        # Filter by correlation
        filtered_results = self._filter_by_correlation(z_score_results)

        # Step 1: Check exits for open positions
        await self._check_exits(
            current_time,
            current_bar_idx,
            window_data,
            filtered_results,
            z_score_results,
            correlation_results,
        )

        # Step 2: Check market volatility (skip entries if market is unsafe)
        if self.primary_pair in window_data:
            volatility_result = self.volatility_filter_service.check(
                window_data[self.primary_pair]
            )
            if not volatility_result.is_safe:
                # Market is unsafe - don't open new positions
                return

        # Step 3: Check entries for new positions
        await self._check_entries(
            current_time,
            current_bar_idx,
            window_data,
            filtered_results,
            correlation_results,
            all_z_score_results=z_score_results,  # Pass for Re-Validate & Reset
        )

    def _filter_by_correlation(
        self,
        z_score_results: Dict[str, ZScoreResult],
    ) -> Dict[str, ZScoreResult]:
        """Filter by correlation threshold."""
        return {
            symbol: result
            for symbol, result in z_score_results.items()
            if result.current_correlation >= self.config.min_correlation
        }

    def _check_hurst_for_symbol(
        self,
        symbol: str,
        window_data: Dict[str, pd.DataFrame],
        correlation_results: Dict,
    ) -> bool:
        """
        Check if a single symbol's spread is mean-reverting.

        This is the final filter before entry - only called when:
        1. Symbol has valid Z-score signal
        2. No position limit exceeded
        3. No cooldown active

        Args:
            symbol: Symbol to check
            window_data: OHLCV data
            correlation_results: Correlation results with beta values

        Returns:
            True if spread is mean-reverting (H < threshold), False otherwise
        """
        if not self.hurst_filter_service:
            return True  # No filter = allow all

        primary_df = window_data.get(self.primary_pair)
        coin_df = window_data.get(symbol)

        if primary_df is None or coin_df is None:
            return False

        if primary_df.empty or coin_df.empty:
            return False

        # Get beta from correlation results
        corr_result = correlation_results.get(symbol)
        if corr_result is None:
            return False

        beta = corr_result.latest_beta

        # Calculate Hurst for spread (use log prices)
        hurst = self.hurst_filter_service.calculate_for_spread(
            coin_log_prices=coin_df["close"].apply(np.log),
            primary_log_prices=primary_df["close"].apply(np.log),
            beta=beta,
        )

        if hurst is None:
            return False

        is_mean_reverting = self.hurst_filter_service.is_mean_reverting(hurst)

        if is_mean_reverting:
            print(
                f"▶️ {symbol} Hurst={hurst:.3f} < {self.hurst_filter_service.threshold} "
                f"(mean-reverting, allow entry)"
            )
        # else:
        #     print(
        #         f"  ⚠️  {symbol} Hurst={hurst:.3f} > {self.hurst_filter_service.threshold} "
        #         f"(trending spread, skip entry)"
        #     )

        return is_mean_reverting

    def _check_adf_for_symbol(
        self,
        symbol: str,
        window_data: Dict[str, pd.DataFrame],
        correlation_results: Dict,
    ) -> bool:
        """
        Check if a single symbol's spread is stationary (ADF test).

        This is an additional filter after Hurst - only called when:
        1. Symbol passed Hurst filter (is mean-reverting)
        2. ADF p-value < threshold indicates stationarity

        Args:
            symbol: Symbol to check
            window_data: OHLCV data
            correlation_results: Correlation results with beta values

        Returns:
            True if spread is stationary (p-value < threshold), False otherwise
        """
        if (
            not self.adf_filter_service
            or not self.adf_filter_service.is_available
            or not self.config.use_adf_filter
        ):
            return True  # No filter = allow all

        primary_df = window_data.get(self.primary_pair)
        coin_df = window_data.get(symbol)

        if primary_df is None or coin_df is None:
            return False

        if primary_df.empty or coin_df.empty:
            return False

        # Get beta from correlation results
        corr_result = correlation_results.get(symbol)
        if corr_result is None:
            return False

        beta = corr_result.latest_beta

        # Calculate ADF p-value for spread
        pvalue = self.adf_filter_service.calculate_for_spread(
            coin_log_prices=coin_df["close"].apply(np.log),
            primary_log_prices=primary_df["close"].apply(np.log),
            beta=beta,
        )

        is_stationary = self.adf_filter_service.is_stationary(pvalue)

        if is_stationary:
            print(
                f"▶️ {symbol} ADF p={pvalue:.4f} < {self.adf_filter_service.threshold} "
                f"(stationary, allow entry)"
            )
        else:
            print(
                f"  ⚠️  {symbol} ADF p={pvalue:.4f} >= {self.adf_filter_service.threshold} "
                f"(non-stationary, skip entry)"
            )

        return is_stationary

    def _check_halflife_for_symbol(
        self,
        symbol: str,
        window_data: Dict[str, pd.DataFrame],
        correlation_results: Dict,
    ) -> bool:
        """
        Check if a single symbol's spread has acceptable Half-Life.

        This is an additional filter after Hurst - only called when:
        1. Symbol passed Hurst filter (is mean-reverting)
        2. Half-Life <= threshold indicates fast enough mean reversion

        Args:
            symbol: Symbol to check
            window_data: OHLCV data
            correlation_results: Correlation results with beta values

        Returns:
            True if spread has acceptable Half-Life (<= threshold), False otherwise
        """
        if not self.halflife_filter_service:
            print(f"  ℹ️  {symbol} Half-Life filter not configured, skipping check")
            return True  # No filter = allow all

        if not self.halflife_filter_service.is_available:
            print(
                f"  ℹ️  {symbol} Half-Life filter unavailable (statsmodels not installed)"
            )
            return True  # No filter = allow all

        primary_df = window_data.get(self.primary_pair)
        coin_df = window_data.get(symbol)

        if primary_df is None or coin_df is None:
            print(f"  ⚠️  {symbol} Half-Life: no data available")
            return False

        if primary_df.empty or coin_df.empty:
            print(f"  ⚠️  {symbol} Half-Life: empty dataframes")
            return False

        # Get beta from correlation results
        corr_result = correlation_results.get(symbol)
        if corr_result is None:
            print(f"  ⚠️  {symbol} Half-Life: no correlation result")
            return False

        beta = corr_result.latest_beta

        # Debug: check data sizes
        coin_log_prices = coin_df["close"].apply(np.log)
        primary_log_prices = primary_df["close"].apply(np.log)

        # Calculate Half-Life for spread
        halflife = self.halflife_filter_service.calculate_for_spread(
            coin_log_prices=coin_log_prices,
            primary_log_prices=primary_log_prices,
            beta=beta,
        )

        is_acceptable = self.halflife_filter_service.is_acceptable(halflife)

        if is_acceptable:
            print(
                f"▶️ {symbol} HL={halflife:.1f} bars <= {self.halflife_filter_service.threshold} "
                f"(fast reversion, allow entry) [data: {len(coin_log_prices)} candles, β={beta:.3f}]"
            )
        else:
            print(
                f"  ⚠️  {symbol} HL={halflife:.1f} bars > {self.halflife_filter_service.threshold} "
                f"(slow reversion, skip entry) [data: {len(coin_log_prices)} candles, β={beta:.3f}]"
            )

        return is_acceptable

    def _check_hurst_exit(
        self,
        symbol: str,
        window_data: Dict[str, pd.DataFrame],
        correlation_results: Dict,
    ) -> bool:
        """
        Check if an open position's spread became trending (Hurst >= threshold + tolerance).

        Uses relaxed threshold (threshold + tolerance) for open positions to prevent
        closing good trades due to micro-fluctuations in Hurst calculation.
        Entry requires H < 0.45 (strict), holding allows H < 0.52 (with tolerance).

        Args:
            symbol: Symbol to check
            window_data: OHLCV data
            correlation_results: Correlation results with beta values

        Returns:
            True if spread became trending (should exit), False otherwise
        """
        if not self.hurst_filter_service:
            return False  # No filter = don't exit

        primary_df = window_data.get(self.primary_pair)
        coin_df = window_data.get(symbol)

        if primary_df is None or coin_df is None:
            return False

        if primary_df.empty or coin_df.empty:
            return False

        # Get beta from correlation results
        corr_result = correlation_results.get(symbol)
        if corr_result is None:
            return False

        beta = corr_result.latest_beta

        # Calculate Hurst for spread (use log prices)
        hurst = self.hurst_filter_service.calculate_for_spread(
            coin_log_prices=coin_df["close"].apply(np.log),
            primary_log_prices=primary_df["close"].apply(np.log),
            beta=beta,
        )

        if hurst is None:
            return False

        # Use relaxed threshold for open positions (threshold + tolerance)
        max_allowed_hurst = (
            self.hurst_filter_service.threshold + self.config.hurst_watch_tolerance
        )
        is_trending = hurst >= max_allowed_hurst

        if is_trending:
            print(
                f"📈 {symbol} Hurst={hurst:.3f} >= {max_allowed_hurst:.2f} "
                f"(spread became trending, exit position)"
            )

        return is_trending

        return is_trending

    def _get_time_based_tp_coefficient(self, bars_held: int) -> float:
        """
        Calculate time-based coefficient for dynamic TP threshold.

        As position ages, we accept less reversion (higher coefficient = easier TP).
        The coefficient is multiplied by peak_z_score to get the TP threshold.

        Example with peak_z = 3.0:
        - 0-4h:   coef=0.1 -> TP at Z=0.3 (wait for ideal ~90% reversion)
        - 4-12h:  coef=0.3 -> TP at Z=0.9 (accept ~70% reversion)
        - 12-24h: coef=0.5 -> TP at Z=1.5 (accept ~50% reversion)
        - 24h+:   TIME_STOP -> close at market (handled separately)

        Args:
            bars_held: Number of bars the position has been held.

        Returns:
            Coefficient to multiply peak_z_score by.
            Returns None (via special value) for time stop after 24h.
        """
        # Convert bars to hours
        hours = bars_held * self._timeframe_minutes / 60

        if hours <= 4:
            return 0.1  # Wait for ideal (~90% reversion)
        elif hours < 12:
            return 0.3  # Accept ~70% reversion after 4h
        elif hours < 24:
            return 0.5  # Accept ~50% reversion after 12h
        else:
            return 1.0  # Time stop - will be handled by TIMEOUT check

    async def _check_exits(
        self,
        current_time: datetime,
        current_bar_idx: int,
        window_data: Dict[str, pd.DataFrame],
        filtered_results: Dict[str, ZScoreResult],
        all_results: Dict[str, ZScoreResult],
        correlation_results: Dict,
    ) -> None:
        """Check if any open positions should be closed."""
        positions_to_close = []

        for symbol, position in self.positions.items():
            exit_reason = None
            z_result = all_results.get(symbol)

            # Check position timeout first
            bars_held = current_bar_idx - position.entry_bar_idx
            if bars_held >= self.config.max_position_bars:
                exit_reason = "TIMEOUT"
            elif z_result is None:
                # No z-score data - shouldn't happen, but close if it does
                exit_reason = "NO_DATA"
            elif symbol not in filtered_results:
                # Correlation dropped below threshold
                exit_reason = "CORRELATION_DROP"
            else:
                z = z_result.current_z_score

                if self.config.use_dynamic_tp:
                    # Dynamic TP based on time in position
                    # TP threshold = max(peak_z * time_coef, z_tp_threshold)
                    # 0-4h:   coef=0.1 -> wait for ~90% reversion
                    # 4-12h:  coef=0.3 -> accept ~70% reversion
                    # 12-24h: coef=0.5 -> accept ~50% reversion
                    # 24h+:   handled by TIMEOUT
                    # Minimum TP level is always z_tp_threshold
                    time_coef = self._get_time_based_tp_coefficient(bars_held)
                    reference_z = (
                        position.peak_z_score
                        if position.peak_z_score > 0
                        else abs(position.entry_z_score)
                    )
                    dynamic_tp = max(
                        reference_z * time_coef, self.config.z_tp_threshold
                    )
                else:
                    dynamic_tp = self.config.z_tp_threshold

                if position.side == "long":
                    # Long: entered at Z <= -entry, exit at Z >= -tp or Z <= -sl
                    if z >= -dynamic_tp:  # Z moving toward 0 from negative
                        exit_reason = "TP"
                    elif z <= -self.config.z_sl_threshold:
                        exit_reason = "SL"
                else:
                    # Short: entered at Z >= entry, exit at Z <= tp or Z >= sl
                    if z <= dynamic_tp:  # Z moving toward 0 from positive
                        exit_reason = "TP"
                    elif z >= self.config.z_sl_threshold:
                        exit_reason = "SL"

            # Check Hurst if no other exit reason yet (spread became trending)
            if exit_reason is None and self.hurst_filter_service is not None:
                if self._check_hurst_exit(symbol, window_data, correlation_results):
                    exit_reason = "HURST_TRENDING"

            if exit_reason:
                positions_to_close.append((symbol, exit_reason))

        # Close positions
        for symbol, exit_reason in positions_to_close:
            await self._close_position(symbol, current_time, window_data, exit_reason)

    async def _check_entries(
        self,
        current_time: datetime,
        current_bar_idx: int,
        window_data: Dict[str, pd.DataFrame],
        filtered_results: Dict[str, ZScoreResult],
        correlation_results: Dict,
        all_z_score_results: Optional[Dict[str, ZScoreResult]] = None,
    ) -> None:
        """Check for new entry signals using dynamic threshold."""
        if len(self.positions) >= self.config.max_spreads:
            return  # Max positions reached

        # Process existing watches first (trailing entry logic)
        # Pass z_score_results for Re-Validate & Reset
        if self.config.use_trailing_entry:
            await self._process_watches(
                current_time,
                current_bar_idx,
                window_data,
                correlation_results,
                z_score_results=all_z_score_results,
            )

        # entry_candidates list of (z_abs, symbol, side, z_result, dyn_threshold)
        entry_candidates = []

        for symbol, z_result in filtered_results.items():
            if symbol in self.positions:
                continue  # Already have position in this symbol

            if symbol in self.watch_candidates:
                continue  # Already watching this symbol

            if symbol == self.primary_pair:
                continue  # Don't trade primary pair directly

            # Check cooldown
            if symbol in self.symbol_cooldowns:
                if current_time < self.symbol_cooldowns[symbol]:
                    continue  # Symbol still in cooldown
                else:
                    # Cooldown expired, remove from dict
                    del self.symbol_cooldowns[symbol]

            z = z_result.current_z_score

            if np.isnan(z):
                continue

            # Use dynamic threshold for this symbol (adaptive upper bound)
            dyn_threshold = z_result.dynamic_entry_threshold

            # Check if signal is valid entry candidate using dynamic threshold
            # Entry: |Z| >= dynamic_threshold AND |Z| <= sl_threshold
            if abs(z) >= dyn_threshold and abs(z) <= self.config.z_sl_threshold:
                side = "short" if z >= dyn_threshold else "long"
                entry_candidates.append((abs(z), symbol, side, z_result, dyn_threshold))

        # Sort candidates by Z-score strength (highest first)
        entry_candidates.sort(key=lambda x: x[0], reverse=True)

        # Open positions for valid candidates up to max_spreads
        for _, symbol, side, z_result, dyn_threshold in entry_candidates:
            if len(self.positions) >= self.config.max_spreads:
                break

            # Final filter: Check Hurst exponent (mean-reversion quality)
            if not self._check_hurst_for_symbol(
                symbol, window_data, correlation_results
            ):
                continue  # Spread is trending, skip entry

            # Additional filter: Check Half-Life (after Hurst passes)
            if not self._check_halflife_for_symbol(
                symbol, window_data, correlation_results
            ):
                continue  # Spread reverts too slowly, skip entry

            # Additional filter: Check ADF stationarity (after Half-Life passes)
            if not self._check_adf_for_symbol(symbol, window_data, correlation_results):
                continue  # Spread is non-stationary, skip entry

            # Funding filter: Check if funding cost is acceptable
            if self.config.use_funding_filter and self.funding_cache is not None:
                net_funding = self.funding_cache.calculate_net_funding(
                    coin_symbol=symbol,
                    primary_symbol=self.primary_pair,
                    timestamp=current_time,
                    spread_side=side,
                )

                if (
                    net_funding is not None
                    and net_funding < self.config.max_funding_cost_threshold
                ):
                    self.funding_blocked_count += 1
                    print(
                        f"⛔ FUNDING FILTER: {symbol} ({side.upper()}) blocked | "
                        f"Net cost: {net_funding * 100:.3f}%/8h < {self.config.max_funding_cost_threshold * 100:.3f}%"
                    )
                    continue  # Toxic funding, skip entry

            if self.config.use_trailing_entry:
                # Start watch instead of immediate entry
                self._start_watch(
                    symbol=symbol,
                    side=side,
                    z_result=z_result,
                    current_time=current_time,
                    current_bar_idx=current_bar_idx,
                )
            else:
                # Immediate entry (original behavior)
                await self._open_position(
                    symbol=symbol,
                    side=side,
                    z_result=z_result,
                    current_time=current_time,
                    current_bar_idx=current_bar_idx,
                    window_data=window_data,
                    dynamic_threshold=dyn_threshold,
                )

    # =========================================================================
    # Trailing Entry Methods
    # =========================================================================

    def _start_watch(
        self,
        symbol: str,
        side: str,
        z_result: ZScoreResult,
        current_time: datetime,
        current_bar_idx: int,
    ) -> None:
        """Start watching a symbol for trailing entry."""
        # Calculate spread_mean and spread_std from the spread_series
        # Use the lookback window for consistency with z-score calculation
        lookback = min(len(z_result.spread_series), 288)  # ~3 days of 15m candles
        recent_spread = z_result.spread_series.tail(lookback).dropna()
        spread_mean = float(recent_spread.mean()) if len(recent_spread) > 0 else 0.0
        spread_std = float(recent_spread.std()) if len(recent_spread) > 1 else 1.0

        watch = BacktestWatchCandidate(
            symbol=symbol,
            side=side,
            initial_z=z_result.current_z_score,
            max_z=abs(z_result.current_z_score),
            start_bar_idx=current_bar_idx,
            start_time=current_time,
            beta=z_result.current_beta,
            spread_mean=spread_mean,
            spread_std=spread_std,
            z_entry_threshold=z_result.dynamic_entry_threshold,
            z_sl_threshold=self.config.z_sl_threshold,
            correlation=z_result.current_correlation,
        )
        self.watch_candidates[symbol] = watch
        print(
            f"👀 WATCH START {symbol} | Z={z_result.current_z_score:.2f} | "
            f"side={side} | pullback_target={watch.max_z - self.config.trailing_pullback:.2f}"
        )

    async def _process_watches(
        self,
        current_time: datetime,
        current_bar_idx: int,
        window_data: Dict[str, pd.DataFrame],
        correlation_results: Dict,
        z_score_results: Optional[Dict[str, ZScoreResult]] = None,
    ) -> None:
        """
        Process trailing entry logic for all watches using 1m candles.

        Also handles Re-Validate & Reset when new scan data is available:
        - Updates watch parameters (beta, spread_mean, spread_std)
        - Validates signal still exists with new parameters
        - Resets max_z to avoid Parameter Jump trap
        """
        watches_to_remove = []

        for symbol, watch in list(self.watch_candidates.items()):
            # =====================================================================
            # Re-Validate & Reset: Update parameters from new scan if available
            # =====================================================================
            if z_score_results and symbol in z_score_results:
                z_result = z_score_results[symbol]

                # Store old values for logging
                old_beta = watch.beta
                old_std = watch.spread_std
                old_max_z = watch.max_z

                # Update calculation parameters
                watch.beta = z_result.current_beta
                watch.correlation = z_result.current_correlation
                watch.z_entry_threshold = z_result.dynamic_entry_threshold

                # Recalculate spread_mean and spread_std
                lookback = min(len(z_result.spread_series), 288)
                recent_spread = z_result.spread_series.tail(lookback).dropna()
                if len(recent_spread) > 0:
                    watch.spread_mean = float(recent_spread.mean())
                if len(recent_spread) > 1:
                    watch.spread_std = float(recent_spread.std())

                # Recalculate Z with new parameters
                coin_price = window_data[symbol]["close"].iloc[-1]
                primary_price = window_data[self.primary_pair]["close"].iloc[-1]
                new_z = self._calculate_live_z(
                    coin_price,
                    primary_price,
                    watch.beta,
                    watch.spread_mean,
                    watch.spread_std,
                )
                abs_new_z = abs(new_z)

                # RE-VALIDATE: Check if signal still valid
                if abs_new_z < watch.z_entry_threshold:
                    print(
                        f"🔄❌ WATCH {symbol} INVALIDATED after param update | "
                        f"new_Z={new_z:.2f} < threshold={watch.z_entry_threshold:.2f} | "
                        f"std: {old_std:.4f}→{watch.spread_std:.4f}"
                    )
                    watches_to_remove.append((symbol, "PARAM_INVALIDATED"))
                    continue

                # RESET: Signal still valid - reset max_z to avoid Parameter Jump
                watch.max_z = abs_new_z

                # Log significant parameter changes
                if (
                    abs(old_beta - watch.beta) > 0.01
                    or abs(old_std - watch.spread_std) > 0.0001
                ):
                    print(
                        f"🔄 Updated watch {symbol} | "
                        f"β: {old_beta:.3f}→{watch.beta:.3f} | "
                        f"std: {old_std:.4f}→{watch.spread_std:.4f} | "
                        f"Z={new_z:.2f} | max_z: {old_max_z:.2f}→{watch.max_z:.2f} (RESET)"
                    )

            # =====================================================================
            # Check timeout
            # =====================================================================
            watch_duration_minutes = (
                current_time - watch.start_time
            ).total_seconds() / 60
            if watch_duration_minutes >= self.config.trailing_timeout_minutes:
                print(
                    f"⏰ WATCH TIMEOUT {symbol} after {watch_duration_minutes:.0f}min | "
                    f"max_z={watch.max_z:.2f}"
                )
                watches_to_remove.append((symbol, "TIMEOUT"))
                continue

            # =====================================================================
            # Load 1m candles and simulate trailing entry
            # =====================================================================
            coin_1m = await self._load_minute_candles_for_watch(
                symbol, watch.start_time, current_time
            )

            if coin_1m is None or coin_1m.empty:
                # If can't load 1m data, use 15m data for rough trailing
                coin_price = window_data[symbol]["close"].iloc[-1]
                primary_price = window_data[self.primary_pair]["close"].iloc[-1]
                live_z = self._calculate_live_z(
                    coin_price,
                    primary_price,
                    watch.beta,
                    watch.spread_mean,
                    watch.spread_std,
                )
                abs_z = abs(live_z)

                # Check pullback on 15m (ENTRY - first priority)
                if abs_z <= watch.max_z - self.config.trailing_pullback:
                    await self._execute_watch_entry(
                        watch, current_time, live_z, current_bar_idx, window_data
                    )
                    watches_to_remove.append((symbol, None))
                # Check false alarm with HYSTERESIS
                elif (
                    abs_z < watch.z_entry_threshold - self.config.false_alarm_hysteresis
                ):
                    print(
                        f"❌ WATCH FALSE_ALARM on 15m {symbol} | Z={live_z:.2f} | "
                        f"threshold={watch.z_entry_threshold:.2f} | "
                        f"hysteresis_level={watch.z_entry_threshold - self.config.false_alarm_hysteresis:.2f}"
                    )
                    watches_to_remove.append((symbol, "FALSE_ALARM"))
                elif abs_z >= watch.z_sl_threshold:
                    print(f"🛑 WATCH SL_HIT on 15m {symbol} | Z={live_z:.2f}")
                    watches_to_remove.append((symbol, "SL_HIT"))
                elif abs_z > watch.max_z:
                    watch.max_z = abs_z
                continue

            # Get primary 1m data for same period
            primary_1m = await self._load_minute_candles_for_watch(
                self.primary_pair, watch.start_time, current_time
            )

            if primary_1m is None or primary_1m.empty:
                continue

            # Simulate trailing entry on 1m data
            entry_result = self._simulate_trailing_on_minute_data(
                watch, coin_1m, primary_1m
            )

            if entry_result is not None:
                action, entry_time, entry_z = entry_result
                if action == "ENTER":
                    # Execute entry at the minute-level entry point
                    await self._execute_watch_entry(
                        watch, entry_time, entry_z, current_bar_idx, window_data
                    )
                    watches_to_remove.append((symbol, None))

                    # =========================================================
                    # FIX: Check intraday exit on remaining 1m candles
                    # This prevents "intraday blindness" - missing TP/SL that
                    # occurs in the same 15m candle after entry
                    # =========================================================
                    await self._check_intraday_exit_after_entry(
                        symbol=symbol,
                        entry_time=entry_time,
                        candle_end_time=current_time,
                        coin_1m=coin_1m,
                        primary_1m=primary_1m,
                        watch=watch,
                        window_data=window_data,
                    )
                else:
                    # Watch cancelled (FALSE_ALARM, SL_HIT)
                    print(
                        f"❌ WATCH CANCEL {symbol} | reason={action} | Z={entry_z:.2f}"
                    )
                    watches_to_remove.append((symbol, action))

        # Remove processed watches
        for symbol, reason in watches_to_remove:
            if symbol in self.watch_candidates:
                del self.watch_candidates[symbol]
            if reason and reason in (
                "FALSE_ALARM",
                "SL_HIT",
                "TIMEOUT",
                "PARAM_INVALIDATED",
            ):
                self.trailing_cancelled_count += 1
                # Add cooldown for cancelled watches
                if self.config.cooldown_bars > 0:
                    cooldown_minutes = (
                        self.config.cooldown_bars * self._timeframe_minutes
                    )
                    self.symbol_cooldowns[symbol] = current_time + timedelta(
                        minutes=cooldown_minutes
                    )

    def _simulate_trailing_on_minute_data(
        self,
        watch: BacktestWatchCandidate,
        coin_1m: pd.DataFrame,
        primary_1m: pd.DataFrame,
    ) -> Optional[Tuple[str, datetime, float]]:
        """
        Simulate trailing entry on 1m candles.

        Uses same logic as EntryObserverService._process_price_update():
        1. Check pullback (ENTRY) - FIRST priority
        2. Check false alarm with HYSTERESIS
        3. Check SL hit
        4. Update max_z if still widening

        Returns:
            Tuple of (action, timestamp, z_score) where action is:
            - "ENTER": Entry confirmed after pullback
            - "FALSE_ALARM": Z dropped significantly below entry threshold
            - "SL_HIT": Z exceeded stop-loss
            - None: Still watching
        """
        max_z = watch.max_z

        # Calculate false alarm level with hysteresis
        false_alarm_level = watch.z_entry_threshold - self.config.false_alarm_hysteresis

        # Align data
        common_index = coin_1m.index.intersection(primary_1m.index)

        for ts in common_index:
            coin_price = coin_1m.loc[ts, "close"]
            primary_price = primary_1m.loc[ts, "close"]

            # Calculate live Z-score
            live_z = self._calculate_live_z(
                coin_price,
                primary_price,
                watch.beta,
                watch.spread_mean,
                watch.spread_std,
            )
            abs_z = abs(live_z)

            # 1. Check pullback (ENTRY) - FIRST priority
            if abs_z <= max_z - self.config.trailing_pullback:
                return ("ENTER", ts, live_z)

            # 2. Check false alarm with HYSTERESIS
            # Only cancel if Z dropped SIGNIFICANTLY below threshold
            if abs_z < false_alarm_level:
                return ("FALSE_ALARM", ts, live_z)

            # 3. Check SL hit
            if abs_z >= watch.z_sl_threshold:
                return ("SL_HIT", ts, live_z)

            # 4. Update max_z if still widening
            if abs_z > max_z:
                max_z = abs_z

        # Update max_z in watch for next iteration
        watch.max_z = max_z
        return None

    def _calculate_live_z(
        self,
        coin_price: float,
        primary_price: float,
        beta: float,
        spread_mean: float,
        spread_std: float,
    ) -> float:
        """Calculate Z-score from prices (same as WatchCandidate.current_z_score)."""
        if coin_price <= 0 or primary_price <= 0 or spread_std == 0:
            return 0.0
        current_spread = math.log(coin_price) - beta * math.log(primary_price)
        return (current_spread - spread_mean) / spread_std

    async def _load_minute_candles_for_watch(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> Optional[pd.DataFrame]:
        """Load 1m candles for a symbol during watch period."""
        try:
            # Calculate number of bars needed
            duration_minutes = int((end_time - start_time).total_seconds() / 60) + 5

            # Use data_loader to fetch 1m data with caching
            df = await self.data_loader.load_ohlcv_with_cache(
                symbol=symbol,
                num_bars=duration_minutes,
                timeframe="1m",
                start_time=start_time,
                end_time=end_time,
            )
            return df if not df.empty else None
        except Exception as e:
            return None

    async def _execute_watch_entry(
        self,
        watch: BacktestWatchCandidate,
        entry_time: datetime,
        entry_z: float,
        current_bar_idx: int,
        window_data: Dict[str, pd.DataFrame],
    ) -> None:
        """Execute entry after trailing pullback confirmation."""
        symbol = watch.symbol

        # Get current prices from window_data (15m)
        coin_price = window_data[symbol]["close"].iloc[-1]
        primary_price = window_data[self.primary_pair]["close"].iloc[-1]

        # Calculate position size
        position_size = self.balance * self.config.position_size_pct

        # Position sizing
        beta = abs(watch.beta)
        coin_size = position_size
        primary_size = position_size * beta

        # Create position with peak_z from trailing watch
        position = Position(
            symbol=symbol,
            side=watch.side,
            entry_z_score=entry_z,
            entry_beta=watch.beta,
            size_usdt=position_size,
            entry_time=entry_time,
            entry_bar_idx=current_bar_idx,
            coin_entry_price=coin_price,
            coin_size=coin_size,
            primary_entry_price=primary_price,
            primary_size=primary_size,
            peak_z_score=watch.max_z,  # Store peak Z from trailing
        )

        self.positions[symbol] = position

        watch_duration = (entry_time - watch.start_time).total_seconds() / 60
        print(
            f"✅ WATCH ENTRY {symbol} | Z={entry_z:.2f} (peak={watch.max_z:.2f}) | "
            f"pullback={watch.max_z - abs(entry_z):.2f} | "
            f"watch_duration={watch_duration:.0f}min | "
            f"β={beta:.3f}"
        )
        self._print_portfolio_state()

    async def _check_intraday_exit_after_entry(
        self,
        symbol: str,
        entry_time: datetime,
        candle_end_time: datetime,
        coin_1m: pd.DataFrame,
        primary_1m: pd.DataFrame,
        watch: BacktestWatchCandidate,
        window_data: Dict[str, pd.DataFrame],
    ) -> None:
        """
        Check for TP/SL on remaining 1m candles after entry within same 15m bar.

        This fixes "intraday blindness" - in production, if we enter at 12:05
        and price hits TP at 12:10, we exit immediately. But in backtest,
        _check_exits only runs at 12:15 (next 15m bar), missing the intraday TP.

        This method simulates the remaining 1m candles after entry_time
        and closes the position if TP/SL is hit.
        """
        position = self.positions.get(symbol)
        if position is None:
            return

        # Get remaining 1m candles after entry
        remaining_coin = coin_1m.loc[coin_1m.index > entry_time]
        remaining_primary = primary_1m.loc[primary_1m.index > entry_time]

        if remaining_coin.empty or remaining_primary.empty:
            return

        # Align indices
        common_index = remaining_coin.index.intersection(remaining_primary.index)
        if common_index.empty:
            return

        # Calculate dynamic TP threshold (bars_held = 0 for same-candle exit)
        if self.config.use_dynamic_tp:
            time_coef = self._get_time_based_tp_coefficient(0)
            reference_z = (
                position.peak_z_score
                if position.peak_z_score > 0
                else abs(position.entry_z_score)
            )
            dynamic_tp = max(reference_z * time_coef, self.config.z_tp_threshold)
        else:
            dynamic_tp = self.config.z_tp_threshold

        z_sl = self.config.z_sl_threshold

        for ts in common_index:
            coin_price = remaining_coin.loc[ts, "close"]
            primary_price = remaining_primary.loc[ts, "close"]

            # Calculate live Z-score
            live_z = self._calculate_live_z(
                coin_price,
                primary_price,
                watch.beta,
                watch.spread_mean,
                watch.spread_std,
            )

            exit_reason = None

            if position.side == "long":
                # Long: entered at Z <= -entry, exit at Z >= -tp or Z <= -sl
                if live_z >= -dynamic_tp:
                    exit_reason = "TP"
                elif live_z <= -z_sl:
                    exit_reason = "SL"
            else:
                # Short: entered at Z >= entry, exit at Z <= tp or Z >= sl
                if live_z <= dynamic_tp:
                    exit_reason = "TP"
                elif live_z >= z_sl:
                    exit_reason = "SL"

            if exit_reason:
                # Close position at this 1m timestamp
                print(
                    f"⚡ INTRADAY {exit_reason} {symbol} | "
                    f"entry_time={entry_time.strftime('%H:%M')} | "
                    f"exit_time={ts.strftime('%H:%M')} | "
                    f"Z={live_z:.2f} | tp_threshold={dynamic_tp:.2f}"
                )
                await self._close_position_at_time(
                    symbol=symbol,
                    exit_time=ts,
                    coin_price=coin_price,
                    primary_price=primary_price,
                    exit_reason=exit_reason,
                    exit_z=live_z,
                )
                return

    async def _close_position_at_time(
        self,
        symbol: str,
        exit_time: datetime,
        coin_price: float,
        primary_price: float,
        exit_reason: str,
        exit_z: float,
    ) -> None:
        """
        Close position at a specific time with given prices.

        Used for intraday exits where we have exact 1m prices.
        """
        position = self.positions.pop(symbol)

        # Calculate PnL for each leg
        coin_pct_change = (
            coin_price - position.coin_entry_price
        ) / position.coin_entry_price
        primary_pct_change = (
            primary_price - position.primary_entry_price
        ) / position.primary_entry_price

        coin_pnl = position.coin_size * coin_pct_change * self.config.leverage
        primary_pnl = position.primary_size * primary_pct_change * self.config.leverage

        # Calculate spread PnL based on position side
        if position.side == "long":
            raw_pnl = coin_pnl - primary_pnl
        else:
            raw_pnl = -coin_pnl + primary_pnl

        # Subtract fees
        fee_rate = (
            self.config.maker_fee
            if self.config.use_limit_orders
            else self.config.taker_fee
        )
        total_fees = (position.coin_size + position.primary_size) * fee_rate * 2

        # Calculate funding PnL (usually 0 for intraday exits)
        funding_pnl = 0.0
        if self.funding_cache is not None:
            funding_pnl = self.funding_cache.calculate_funding_pnl(
                coin_symbol=symbol,
                primary_symbol=self.primary_pair,
                entry_time=position.entry_time,
                exit_time=exit_time,
                spread_side=position.side,
                coin_size_usdt=position.coin_size,
                primary_size_usdt=position.primary_size,
                leverage=self.config.leverage,
            )

        pnl = raw_pnl - total_fees + funding_pnl
        self.balance += pnl

        duration = (exit_time - position.entry_time).total_seconds() / 3600

        trade = Trade(
            symbol=symbol,
            side=position.side,
            entry_time=position.entry_time,
            exit_time=exit_time,
            entry_z_score=position.entry_z_score,
            exit_z_score=exit_z,
            entry_price=position.coin_entry_price,
            exit_price=coin_price,
            size_usdt=position.size_usdt,
            pnl=pnl,
            pnl_pct=pnl / position.size_usdt * 100,
            exit_reason=exit_reason,
            duration_hours=duration,
            funding_pnl=funding_pnl,
        )

        self.trades.append(trade)

        # Add cooldown for adverse exits
        cooldown_reasons = ("SL", "CORRELATION_DROP", "TIMEOUT", "HURST_TRENDING")
        if exit_reason in cooldown_reasons and self.config.cooldown_bars > 0:
            cooldown_minutes = self.config.cooldown_bars * get_timeframe_minutes(
                self.timeframe
            )
            unlock_time = exit_time + timedelta(minutes=cooldown_minutes)
            self.symbol_cooldowns[symbol] = unlock_time

        emoji = "🔺" if pnl >= 0 else "🔻"
        print(f"{emoji} {pnl:.2f} | Reason: {exit_reason} | Duration: {duration:.2f}h")
        self._print_portfolio_state()

    async def _open_position(
        self,
        symbol: str,
        side: str,
        z_result: ZScoreResult,
        current_time: datetime,
        current_bar_idx: int,
        window_data: Dict[str, pd.DataFrame],
        dynamic_threshold: float,
    ) -> None:
        """
        Open a spread position (two legs).

        For stat-arb:
        - Long spread (Z <= -entry): Long COIN, Short PRIMARY (hedged by beta)
        - Short spread (Z >= entry): Short COIN, Long PRIMARY (hedged by beta)

        The PnL comes from the spread reverting to mean, not from directional price moves.
        """
        # Calculate position size
        position_size = self.balance * self.config.position_size_pct

        # Get current prices
        coin_price = window_data[symbol]["close"].iloc[-1]
        primary_price = window_data[self.primary_pair]["close"].iloc[-1]

        # Calculate hedge ratio (beta)
        beta = abs(z_result.current_beta)  # Use absolute value for sizing

        # Position sizing:
        # - Full position_size goes to COIN leg
        # - Hedge (PRIMARY/ETH) = position_size * beta
        # Example: $3000 position, beta=0.8 → COIN=$3000, ETH=$2400
        coin_size = position_size
        primary_size = position_size * beta

        # Create position (peak_z = entry_z for immediate entry)
        position = Position(
            symbol=symbol,
            side=side,
            entry_z_score=z_result.current_z_score,
            entry_beta=z_result.current_beta,
            size_usdt=position_size,
            entry_time=current_time,
            entry_bar_idx=current_bar_idx,
            coin_entry_price=coin_price,
            coin_size=coin_size,
            primary_entry_price=primary_price,
            primary_size=primary_size,
            peak_z_score=abs(z_result.current_z_score),  # No trailing, peak = entry
        )

        self.positions[symbol] = position

        print(
            f"OPEN {side.upper()} SPREAD {symbol} | "
            f"Z={z_result.current_z_score:.2f} (th={dynamic_threshold:.2f}) β={beta:.3f} | "
            f"Coin: ${coin_size:.2f} @ {coin_price:.4f} | "
            f"Hedge: ${primary_size:.2f} @ {primary_price:.4f}"
        )
        self._print_portfolio_state()

    async def _close_position(
        self,
        symbol: str,
        current_time: datetime,
        window_data: Dict[str, pd.DataFrame],
        exit_reason: str,
    ) -> None:
        """
        Close a spread position (both legs).

        PnL calculation for spread:
        - Long spread: PnL = coin_pnl - primary_pnl (long coin, short primary)
        - Short spread: PnL = -coin_pnl + primary_pnl (short coin, long primary)

        Where coin_pnl = coin_size * (exit_price - entry_price) / entry_price

        Also includes funding PnL accumulated during position lifetime.
        """
        position = self.positions.pop(symbol)

        # Get exit prices
        coin_exit_price = window_data[symbol]["close"].iloc[-1]
        primary_exit_price = window_data[self.primary_pair]["close"].iloc[-1]

        # Calculate PnL for each leg
        coin_pct_change = (
            coin_exit_price - position.coin_entry_price
        ) / position.coin_entry_price
        primary_pct_change = (
            primary_exit_price - position.primary_entry_price
        ) / position.primary_entry_price

        coin_pnl = position.coin_size * coin_pct_change * self.config.leverage
        primary_pnl = position.primary_size * primary_pct_change * self.config.leverage

        # Calculate spread PnL based on position side
        if position.side == "long":
            # Long spread = Long COIN, Short PRIMARY
            # Profit when COIN goes up relative to PRIMARY
            raw_pnl = coin_pnl - primary_pnl
        else:
            # Short spread = Short COIN, Long PRIMARY
            # Profit when PRIMARY goes up relative to COIN
            raw_pnl = -coin_pnl + primary_pnl

        # Subtract fees (both legs, entry + exit = 4 transactions)
        fee_rate = (
            self.config.maker_fee
            if self.config.use_limit_orders
            else self.config.taker_fee
        )
        # 4 transactions: open coin, open primary, close coin, close primary
        total_fees = (position.coin_size + position.primary_size) * fee_rate * 2

        # Calculate funding PnL
        funding_pnl = 0.0
        if self.funding_cache is not None:
            funding_pnl = self.funding_cache.calculate_funding_pnl(
                coin_symbol=symbol,
                primary_symbol=self.primary_pair,
                entry_time=position.entry_time,
                exit_time=current_time,
                spread_side=position.side,
                coin_size_usdt=position.coin_size,
                primary_size_usdt=position.primary_size,
                leverage=self.config.leverage,
            )

        # Total PnL = spread PnL - fees + funding PnL
        pnl = raw_pnl - total_fees + funding_pnl

        # Update balance
        self.balance += pnl

        # Record trade
        duration = (current_time - position.entry_time).total_seconds() / 3600

        trade = Trade(
            symbol=symbol,
            side=position.side,
            entry_time=position.entry_time,
            exit_time=current_time,
            entry_z_score=position.entry_z_score,
            exit_z_score=np.nan,  # Would need to recalculate
            entry_price=position.coin_entry_price,
            exit_price=coin_exit_price,
            size_usdt=position.size_usdt,
            pnl=pnl,
            pnl_pct=pnl / position.size_usdt * 100,
            exit_reason=exit_reason,
            duration_hours=duration,
            funding_pnl=funding_pnl,
        )

        self.trades.append(trade)

        # Add cooldown for adverse exits (SL, CORRELATION_DROP, TIMEOUT, HURST_TRENDING)
        # These indicate the pair is not behaving as expected - prevent immediate re-entry
        cooldown_reasons = ("SL", "CORRELATION_DROP", "TIMEOUT", "HURST_TRENDING")
        if exit_reason in cooldown_reasons and self.config.cooldown_bars > 0:
            cooldown_minutes = self.config.cooldown_bars * get_timeframe_minutes(
                self.timeframe
            )
            unlock_time = current_time + timedelta(minutes=cooldown_minutes)
            self.symbol_cooldowns[symbol] = unlock_time
            print(
                f"⏸️  {symbol} in cooldown for {cooldown_minutes} min "
                f"(until {unlock_time.strftime('%H:%M')})"
            )

        emoji = "🔺" if pnl >= 0 else "🔻"
        funding_str = f" | Funding: ${funding_pnl:+.2f}" if funding_pnl != 0 else ""

        print(
            f"{emoji} {pnl:.2f} | Reason: {exit_reason} | Duration: {duration:.1f}h{funding_str}"
        )
        self._print_portfolio_state()

    async def _close_all_positions(
        self,
        current_time: datetime,
        window_data: Dict[str, pd.DataFrame],
        reason: str,
    ) -> None:
        """Close all open positions."""
        symbols = list(self.positions.keys())
        for symbol in symbols:
            await self._close_position(symbol, current_time, window_data, reason)

    def _calculate_equity(self, window_data: Dict[str, pd.DataFrame]) -> float:
        """
        Calculate current equity (balance + unrealized PnL from spread positions).
        """
        equity = self.balance

        for symbol, position in self.positions.items():
            if symbol not in window_data or self.primary_pair not in window_data:
                continue

            # Get current prices
            coin_price = window_data[symbol]["close"].iloc[-1]
            primary_price = window_data[self.primary_pair]["close"].iloc[-1]

            # Calculate unrealized PnL for each leg
            coin_pct_change = (
                coin_price - position.coin_entry_price
            ) / position.coin_entry_price
            primary_pct_change = (
                primary_price - position.primary_entry_price
            ) / position.primary_entry_price

            coin_pnl = position.coin_size * coin_pct_change * self.config.leverage
            primary_pnl = (
                position.primary_size * primary_pct_change * self.config.leverage
            )

            # Calculate spread unrealized PnL
            if position.side == "long":
                unrealized_pnl = coin_pnl - primary_pnl
            else:
                unrealized_pnl = -coin_pnl + primary_pnl

            equity += unrealized_pnl

        return equity

    def _print_portfolio_state(self):
        """Print current portfolio state (active hedges)."""
        if not self.positions:
            print("   Empty Portfolio")
            return

        print("   📂 PORTFOLIO STATE:")
        total_primary_exposure = 0.0

        for symbol, pos in self.positions.items():
            # For LONG spread (Long Coin, Short Hedge), primary exposure is SHORT (-size)
            # For SHORT spread (Short Coin, Long Hedge), primary exposure is LONG (+size)
            primary_direction = "SHORT" if pos.side == "long" else "LONG"
            primary_sign = -1 if pos.side == "long" else 1
            entry_exposure = pos.primary_size * primary_sign

            total_primary_exposure += entry_exposure

            print(
                f"      • {symbol:<10} ({pos.side.upper()}): "
                f"Coin ${pos.coin_size:.0f} | "
                f"Hedge {self.primary_pair} ${pos.primary_size:.0f} ({primary_direction})"
            )

        direction = "LONG" if total_primary_exposure > 0 else "SHORT"
        print(
            f"      ∑ Net Hedge Exposure: {direction} ${abs(total_primary_exposure):.2f}"
        )

    def _calculate_results(self) -> BacktestResult:
        """Calculate final backtest results."""
        # Build equity curve
        equity_df = pd.DataFrame(self.equity_history, columns=["time", "equity"])
        equity_df.set_index("time", inplace=True)
        equity_curve = equity_df["equity"]

        # Calculate drawdown
        rolling_max = equity_curve.expanding().max()
        drawdown = equity_curve - rolling_max
        drawdown_pct = (drawdown / rolling_max) * 100

        max_drawdown = drawdown.min()
        max_drawdown_pct = drawdown_pct.min()

        # Trade statistics
        total_trades = len(self.trades)
        winning_trades = sum(1 for t in self.trades if t.pnl > 0)
        losing_trades = sum(1 for t in self.trades if t.pnl <= 0)
        win_rate = winning_trades / total_trades if total_trades > 0 else 0

        # PnL metrics
        total_pnl = sum(t.pnl for t in self.trades)
        total_pnl_pct = (
            (self.balance - self.config.initial_balance)
            / self.config.initial_balance
            * 100
        )

        avg_trade_pnl = total_pnl / total_trades if total_trades > 0 else 0
        avg_duration = (
            sum(t.duration_hours for t in self.trades) / total_trades
            if total_trades > 0
            else 0
        )

        # Profit factor
        gross_profit = sum(t.pnl for t in self.trades if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in self.trades if t.pnl < 0))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        # Sharpe ratio (daily returns)
        equity_returns = equity_curve.pct_change().dropna()
        if len(equity_returns) > 0 and equity_returns.std() > 0:
            # Annualize: 365 days * 96 candles per day (15m)
            periods_per_year = 365 * 96
            sharpe_ratio = (equity_returns.mean() / equity_returns.std()) * np.sqrt(
                periods_per_year
            )
        else:
            sharpe_ratio = 0

        # Trades by exit reason
        trades_by_reason = {}
        for trade in self.trades:
            reason = trade.exit_reason
            trades_by_reason[reason] = trades_by_reason.get(reason, 0) + 1

        # Per-symbol statistics
        symbol_stats = self._calculate_symbol_stats()

        return BacktestResult(
            initial_balance=self.config.initial_balance,
            final_balance=self.balance,
            total_pnl=total_pnl,
            total_pnl_pct=total_pnl_pct,
            total_trades=total_trades,
            winning_trades=winning_trades,
            losing_trades=losing_trades,
            win_rate=win_rate,
            max_drawdown=max_drawdown,
            max_drawdown_pct=max_drawdown_pct,
            sharpe_ratio=sharpe_ratio,
            profit_factor=profit_factor,
            avg_trade_pnl=avg_trade_pnl,
            avg_trade_duration_hours=avg_duration,
            trades_by_reason=trades_by_reason,
            equity_curve=equity_curve,
            drawdown_curve=drawdown_pct,
            trades=self.trades,
            symbol_stats=symbol_stats,
            funding_blocked_entries=self.funding_blocked_count,
            total_funding_pnl=sum(t.funding_pnl for t in self.trades),
            trailing_cancelled_entries=self.trailing_cancelled_count,
        )

    def _calculate_symbol_stats(self) -> List[SymbolStats]:
        """Calculate per-symbol trading statistics."""
        # Group trades by symbol
        trades_by_symbol: Dict[str, List[Trade]] = {}
        for trade in self.trades:
            if trade.symbol not in trades_by_symbol:
                trades_by_symbol[trade.symbol] = []
            trades_by_symbol[trade.symbol].append(trade)

        symbol_stats = []
        for symbol, trades in trades_by_symbol.items():
            total = len(trades)
            wins = sum(1 for t in trades if t.pnl > 0)
            losses = sum(1 for t in trades if t.pnl <= 0)

            total_pnl = sum(t.pnl for t in trades)
            avg_pnl = total_pnl / total if total > 0 else 0

            gross_profit = sum(t.pnl for t in trades if t.pnl > 0)
            gross_loss = abs(sum(t.pnl for t in trades if t.pnl < 0))
            pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

            avg_duration = (
                sum(t.duration_hours for t in trades) / total if total > 0 else 0
            )

            tp_count = sum(1 for t in trades if t.exit_reason == "TP")
            sl_count = sum(1 for t in trades if t.exit_reason == "SL")
            other_exits = total - tp_count - sl_count

            stats = SymbolStats(
                symbol=symbol,
                total_trades=total,
                winning_trades=wins,
                losing_trades=losses,
                win_rate=wins / total if total > 0 else 0,
                total_pnl=total_pnl,
                avg_pnl=avg_pnl,
                profit_factor=pf,
                avg_duration_hours=avg_duration,
                tp_count=tp_count,
                sl_count=sl_count,
                other_exits=other_exits,
            )
            symbol_stats.append(stats)

        # Sort by total PnL descending (best performers first)
        symbol_stats.sort(key=lambda s: s.total_pnl, reverse=True)

        return symbol_stats


def print_symbol_stats(symbol_stats: List[SymbolStats]) -> None:
    """Print per-symbol trading statistics table."""
    print("\n" + "=" * 110)
    print("                              PER-SYMBOL STATISTICS (vs ETH)")
    print("=" * 110)

    # Table header
    header = (
        f"{'Symbol':<18} │ {'Trades':>6} │ {'Win%':>6} │ "
        f"{'PnL':>10} │ {'Avg PnL':>9} │ {'PF':>6} │ "
        f"{'TP':>4} │ {'SL':>4} │ {'Other':>5} │ {'Avg Dur':>8}"
    )
    print(header)
    print("-" * 110)

    for stats in symbol_stats:
        # Emoji based on profitability
        if stats.total_pnl > 0:
            emoji = "🟢"
        elif stats.total_pnl < 0:
            emoji = "🔴"
        else:
            emoji = "⚪"

        # Format profit factor (cap at 99.99 for display)
        pf_str = (
            f"{min(stats.profit_factor, 99.99):.2f}"
            if stats.profit_factor != float("inf")
            else "∞"
        )

        row = (
            f"{emoji} {stats.symbol:<15} │ "
            f"{stats.total_trades:>6} │ "
            f"{stats.win_rate * 100:>5.1f}% │ "
            f"${stats.total_pnl:>+9.2f} │ "
            f"${stats.avg_pnl:>+8.2f} │ "
            f"{pf_str:>6} │ "
            f"{stats.tp_count:>4} │ "
            f"{stats.sl_count:>4} │ "
            f"{stats.other_exits:>5} │ "
            f"{stats.avg_duration_hours:>6.1f}h"
        )
        print(row)

    print("-" * 110)

    # Summary row
    total_trades = sum(s.total_trades for s in symbol_stats)
    total_pnl = sum(s.total_pnl for s in symbol_stats)
    total_wins = sum(s.winning_trades for s in symbol_stats)
    overall_wr = total_wins / total_trades * 100 if total_trades > 0 else 0

    profitable_symbols = sum(1 for s in symbol_stats if s.total_pnl > 0)
    losing_symbols = sum(1 for s in symbol_stats if s.total_pnl < 0)

    print(f"\n  📊 Symbols traded: {len(symbol_stats)}")
    print(f"  🟢 Profitable: {profitable_symbols}  |  🔴 Losing: {losing_symbols}")
    print(
        f"  📈 Overall: {total_trades} trades, {overall_wr:.1f}% win rate, ${total_pnl:+.2f} PnL"
    )


def print_results(result: BacktestResult) -> None:
    """Print backtest results summary."""
    print("\n" + "=" * 70)
    print("                    BACKTEST RESULTS SUMMARY")
    print("=" * 70)

    print("\n📊 PERFORMANCE")
    print("-" * 40)
    print(f"  Initial Balance:     ${result.initial_balance:,.2f}")
    print(f"  Final Balance:       ${result.final_balance:,.2f}")
    print(
        f"  Total PnL:           ${result.total_pnl:,.2f} ({result.total_pnl_pct:+.2f}%)"
    )

    print("\n📈 TRADE STATISTICS")
    print("-" * 40)
    print(f"  Total Trades:        {result.total_trades}")
    print(f"  Winning Trades:      {result.winning_trades}")
    print(f"  Losing Trades:       {result.losing_trades}")
    print(f"  Win Rate:            {result.win_rate * 100:.1f}%")
    print(f"  Avg Trade PnL:       ${result.avg_trade_pnl:.2f}")
    print(f"  Avg Trade Duration:  {result.avg_trade_duration_hours:.1f} hours")

    print("\n⚠️  RISK METRICS")
    print("-" * 40)
    print(
        f"  Max Drawdown:        ${result.max_drawdown:,.2f} ({result.max_drawdown_pct:.2f}%)"
    )
    print(f"  Sharpe Ratio:        {result.sharpe_ratio:.2f}")
    print(f"  Profit Factor:       {result.profit_factor:.2f}")

    print("\n🏷️  EXITS BY REASON")
    print("-" * 40)
    for reason, count in sorted(result.trades_by_reason.items()):
        pct = count / result.total_trades * 100 if result.total_trades > 0 else 0
        emoji = "✅" if reason == "TP" else "❌" if reason == "SL" else "⚠️"
        print(f"  {emoji} {reason:20s} {count:5d} ({pct:.1f}%)")

    # Funding filter stats
    if result.funding_blocked_entries > 0 or result.total_funding_pnl != 0:
        print("\n💸 FUNDING")
        print("-" * 40)
        if result.funding_blocked_entries > 0:
            print(f"  Entries blocked:     {result.funding_blocked_entries}")
        if result.total_funding_pnl != 0:
            funding_emoji = "🟢" if result.total_funding_pnl >= 0 else "🔴"
            print(
                f"  Total Funding PnL:   {funding_emoji} ${result.total_funding_pnl:+,.2f}"
            )

    # Print per-symbol statistics
    if result.symbol_stats:
        print_symbol_stats(result.symbol_stats)

    print("\n" + "=" * 70)


def plot_results(result: BacktestResult, save_path: Optional[str] = None) -> None:
    """Plot equity curve and drawdown."""
    _, axes = plt.subplots(3, 1, figsize=(14, 10))

    # 1. Equity Curve (with datetime x-axis)
    ax1 = axes[0]
    result.equity_curve.plot(ax=ax1, color="blue", linewidth=1)
    ax1.axhline(y=result.initial_balance, color="gray", linestyle="--", alpha=0.5)
    ax1.set_title("Equity Curve", fontsize=12, fontweight="bold")
    ax1.set_ylabel("Equity (USDT)")
    ax1.set_xlabel("")
    ax1.grid(True, alpha=0.3)
    ax1.fill_between(
        result.equity_curve.index,
        result.initial_balance,
        result.equity_curve.values,
        alpha=0.3,
        color="green" if result.final_balance > result.initial_balance else "red",
    )
    # Format x-axis for dates
    ax1.tick_params(axis="x", rotation=45)

    # 2. Drawdown (with datetime x-axis)
    ax2 = axes[1]
    result.drawdown_curve.plot(ax=ax2, color="red", linewidth=1)
    ax2.fill_between(
        result.drawdown_curve.index,
        0,
        result.drawdown_curve.values,
        alpha=0.3,
        color="red",
    )
    ax2.set_title("Drawdown (%)", fontsize=12, fontweight="bold")
    ax2.set_ylabel("Drawdown (%)")
    ax2.set_xlabel("")
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis="x", rotation=45)

    # 3. Trade PnL distribution (separate x-axis - trade numbers)
    ax3 = axes[2]
    if result.trades:
        pnls = [t.pnl for t in result.trades]
        colors = ["green" if p > 0 else "red" for p in pnls]
        trade_nums = list(range(1, len(pnls) + 1))
        ax3.bar(trade_nums, pnls, color=colors, alpha=0.7)
        ax3.axhline(y=0, color="black", linestyle="-", linewidth=0.5)
        ax3.set_title("Individual Trade PnL", fontsize=12, fontweight="bold")
        ax3.set_xlabel("Trade #")
        ax3.set_ylabel("PnL (USDT)")
        ax3.grid(True, alpha=0.3)
        # Set x-axis to show trade numbers properly
        if len(pnls) <= 50:
            ax3.set_xticks(trade_nums)
        else:
            # For many trades, show fewer ticks
            step = max(1, len(pnls) // 20)
            ax3.set_xticks(trade_nums[::step])

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches="tight")
        print(f"\n📊 Chart saved to: {save_path}")

    plt.show()


def export_trades(result: BacktestResult, filepath: str) -> None:
    """Export trades to CSV."""
    if not result.trades:
        print("No trades to export.")
        return

    data = []
    for t in result.trades:
        data.append(
            {
                "symbol": t.symbol,
                "side": t.side,
                "entry_time": t.entry_time,
                "exit_time": t.exit_time,
                "entry_z": t.entry_z_score,
                "exit_z": t.exit_z_score,
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "size_usdt": t.size_usdt,
                "pnl": t.pnl,
                "pnl_pct": t.pnl_pct,
                "funding_pnl": t.funding_pnl,
                "exit_reason": t.exit_reason,
                "duration_hours": t.duration_hours,
            }
        )

    df = pd.DataFrame(data)
    df.to_csv(filepath, index=False)
    print(f"\n📄 Trades exported to: {filepath}")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Statistical Arbitrage Backtest Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_backtest.py --start 2024-06-01 --end 2024-12-15
  python run_backtest.py --start 2024-09-01
  python run_backtest.py --start 2024-01-01 --balance 50000 --risk 0.05
        """,
    )

    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="Backtest start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="Backtest end date (YYYY-MM-DD). Default: now",
    )
    parser.add_argument(
        "--balance",
        type=float,
        default=10000.0,
        help="Initial balance in USDT. Default: 10000",
    )
    parser.add_argument(
        "--risk",
        type=float,
        default=None,
        help="Position size as fraction of balance.",
    )
    parser.add_argument(
        "--max-spreads",
        type=int,
        default=1,
        help="Maximum concurrent spread positions (1 = one coin vs ETH).",
    )
    parser.add_argument(
        "--leverage",
        type=int,
        default=None,
        help="Leverage multiplier. Default: from settings",
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Disable chart plotting",
    )
    parser.add_argument(
        "--export",
        type=str,
        default=None,
        help="Export trades to CSV file",
    )
    parser.add_argument(
        "--pairs",
        type=str,
        default=None,
        help="Comma-separated list of coin symbols (e.g., 'sol,arb,dot'). Default: from settings.CONSISTENT_PAIRS",
    )

    parser.add_argument(
        "--use-dynamic-tp",
        type=str,
        default=True,
        help="Use dynamic TP by time base exponent",
    )

    parser.add_argument(
        "--no-funding-filter",
        action="store_true",
        help="Disable funding rate filter",
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

    # Initialize container
    container = Container().init()
    settings = container.settings
    logger = container.logger

    # Parse pairs from CLI or use settings
    if args.pairs:
        # Convert comma-separated symbols to full pair format
        # e.g., "sol,arb,dot" -> ["SOL/USDT:USDT", "ARB/USDT:USDT", "DOT/USDT:USDT"]
        symbols = [s.strip().upper() for s in args.pairs.split(",")]
        consistent_pairs = [f"{symbol}/USDT:USDT" for symbol in symbols]
        print(f"\n📋 Using pairs from CLI: {', '.join(consistent_pairs)}")
    else:
        consistent_pairs = settings.CONSISTENT_PAIRS
        print(f"\n📋 Using pairs from settings: {len(consistent_pairs)} pairs")

    # Create backtest config
    config = BacktestConfig(
        consistent_pairs=consistent_pairs,
        use_dynamic_tp=args.use_dynamic_tp,
        max_position_bars=settings.MAX_POSITION_BARS,
        use_funding_filter=not args.no_funding_filter,
        max_funding_cost_threshold=settings.MAX_FUNDING_COST_THRESHOLD,
    )

    print("\n" + "=" * 70)
    print("              STATISTICAL ARBITRAGE BACKTEST")
    print("=" * 70)
    print(f"\n  Start Date:          {start_date.date()}")
    print(f"  End Date:            {end_date.date()}")
    print(f"  Initial Balance:     ${config.initial_balance:,.2f}")
    print(f"  Position Size:       {config.position_size_pct * 100:.1f}%")
    print(
        f"  Max Spreads:         {config.max_spreads} (COIN vs {settings.PRIMARY_PAIR})"
    )
    print(f"  Leverage:            {config.leverage}x")
    print(f"\n  Z Entry Threshold:   ±{config.z_entry_threshold}")
    print(f"  Z TP Threshold:      ±{config.z_tp_threshold}")
    print(f"  Z SL Threshold:      ±{config.z_sl_threshold}")
    print(f"  Min Correlation:     {config.min_correlation}")
    print(f"\n  Primary Pair:        {settings.PRIMARY_PAIR}")
    print(f"  Trading Pairs:       {len(config.consistent_pairs)} pairs")
    for pair in config.consistent_pairs:
        print(f"    • {pair}")
    print(f"\n  Funding Filter:      {'ON' if config.use_funding_filter else 'OFF'}")
    if config.use_funding_filter:
        print(
            f"  Max Funding Cost:    {config.max_funding_cost_threshold * 100:.3f}% per 8h"
        )
    print(f"\n  Half-Life Filter:    ON")
    print(f"  HL Max Bars:         {settings.HALFLIFE_MAX_BARS} bars")
    print(f"  HL Lookback:         {settings.HALFLIFE_LOOKBACK_CANDLES} candles")
    print("=" * 70 + "\n")

    # Connect to exchange for data loading
    exchange = container.exchange_client
    await exchange.connect()

    try:
        # Create data loader with exchange
        data_loader = AsyncDataLoaderService(
            logger=logger,
            exchange_client=exchange,
            ohlcv_repository=container.ohlcv_repository,
        )

        # Create correlation and z-score services
        correlation_service = CorrelationService(
            logger=logger,
            lookback_window_days=settings.LOOKBACK_WINDOW_DAYS,
            timeframe=settings.TIMEFRAME,
        )

        z_score_service = ZScoreService(
            logger=logger,
            lookback_window_days=settings.LOOKBACK_WINDOW_DAYS,
            timeframe=settings.TIMEFRAME,
            z_entry_threshold=settings.Z_ENTRY_THRESHOLD,
            z_tp_threshold=settings.Z_TP_THRESHOLD,
            z_sl_threshold=settings.Z_SL_THRESHOLD,
        )

        # Create volatility filter service
        volatility_filter_service = VolatilityFilterService(
            logger=logger,
            primary_pair=settings.PRIMARY_PAIR,
            timeframe=settings.TIMEFRAME,
            volatility_window=settings.VOLATILITY_WINDOW,
            volatility_threshold=settings.VOLATILITY_THRESHOLD,
            crash_window=settings.VOLATILITY_CRASH_WINDOW,
            crash_threshold=settings.VOLATILITY_CRASH_THRESHOLD,
        )

        # Create Hurst filter service
        hurst_filter_service = HurstFilterService(
            logger=logger,
        )

        # Create ADF filter service
        adf_filter_service = ADFFilterService(
            logger=logger,
            pvalue_threshold=settings.ADF_PVALUE_THRESHOLD,
            lookback_candles=settings.ADF_LOOKBACK_CANDLES,
        )

        # Create Half-Life filter service
        halflife_filter_service = HalfLifeFilterService(
            logger=logger,
            max_bars=settings.HALFLIFE_MAX_BARS,
            lookback_candles=settings.HALFLIFE_LOOKBACK_CANDLES,
        )
        print(
            f"\n⏱️ Half-Life filter: max_bars={settings.HALFLIFE_MAX_BARS}, available={halflife_filter_service.is_available}"
        )

        # Load historical funding rates if funding filter is enabled
        funding_cache = None
        if config.use_funding_filter:
            print("\n💸 Loading historical funding rates...")
            funding_cache = HistoricalFundingCache(logger=logger)

            # Load funding for all symbols + primary pair
            all_funding_symbols = [settings.PRIMARY_PAIR] + consistent_pairs

            # Calculate data range (need funding from warmup period too)
            # Use same warmup as main data: 3x lookback + 2 days
            warmup_days = settings.LOOKBACK_WINDOW_DAYS * 3 + 2
            funding_start = start_date - timedelta(days=warmup_days)

            await funding_cache.load(
                exchange_client=exchange,
                symbols=all_funding_symbols,
                start_date=funding_start,
                end_date=end_date,
            )
            print(
                f"  Funding filter threshold: {config.max_funding_cost_threshold * 100:.3f}% per 8h"
            )
        else:
            print("\n💸 Funding filter: DISABLED")

        # Create and run backtester
        backtester = StatArbBacktest(
            config=config,
            settings=settings,
            data_loader=data_loader,
            correlation_service=correlation_service,
            z_score_service=z_score_service,
            volatility_filter_service=volatility_filter_service,
            hurst_filter_service=hurst_filter_service,
            adf_filter_service=adf_filter_service,
            halflife_filter_service=halflife_filter_service,
            funding_cache=funding_cache,
        )

        result = await backtester.run(start_date, end_date)

        # Print results
        print_results(result)

        # Export trades if requested
        if args.export:
            export_trades(result, args.export)

        # Plot if not disabled
        if not args.no_plot:
            plot_results(
                result,
                save_path=f"backtests/results/{start_date.strftime("%Y-%m-%d")}-{end_date.strftime("%Y-%m-%d")}.png",
            )

    finally:
        await exchange.disconnect()
        container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
