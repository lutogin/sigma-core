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
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.domain.data_loader.async_service import AsyncDataLoaderService
from src.domain.screener.correlation import CorrelationService
from src.domain.screener.volatility_filter import VolatilityFilterService
from src.domain.screener.z_score import ZScoreService, ZScoreResult
from src.infra.container import Container
from src.domain.utils import get_timeframe_minutes


# =============================================================================
# Backtest Configuration
# =============================================================================


@dataclass
class BacktestConfig:
    """Backtest configuration parameters."""

    # Capital
    initial_balance: float = 10_000.0  # Starting capital in USDT
    position_size_pct: float = 0.3  # 3% of capital per spread
    max_spreads: int = 1  # Maximum concurrent spread positions (1 = one coin vs ETH)

    # Strategy thresholds (from settings)
    z_entry_threshold: float = 2.5  # |Z| >= this to enter
    z_tp_threshold: float = 0.2  # |Z| <= this to take profit
    z_sl_threshold: float = 4.0  # |Z| >= this to stop loss
    min_correlation: float = 0.8  # Minimum correlation to trade

    # Fees
    maker_fee: float = 0.0002  # 0.02% maker fee
    taker_fee: float = 0.0004  # 0.04% taker fee
    use_limit_orders: bool = True  # Use maker fees (limit orders)

    # Risk
    leverage: int = 3  # Leverage multiplier

    # Cooldown after SL or CORRELATION_DROP (in bars)
    # 2 bars = 30 minutes for 15m timeframe
    cooldown_bars: int = 16  # 4 hours

    # Maximum position duration before forced exit (in bars)
    # 96 bars = 24 hours for 15m timeframe
    max_position_bars: int = 96  # 24 hours


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
    ):
        self.config = config
        self.settings = settings
        self.data_loader = data_loader
        self.correlation_service = correlation_service
        self.z_score_service = z_score_service
        self.volatility_filter_service = volatility_filter_service

        self.primary_pair = settings.PRIMARY_PAIR
        self.consistent_pairs = settings.CONSISTENT_PAIRS
        self.timeframe = settings.TIMEFRAME
        self.lookback_window_days = settings.LOOKBACK_WINDOW_DAYS

        # State
        self.balance = config.initial_balance
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.equity_history: List[Tuple[datetime, float]] = []

        # Cooldown tracking: symbol -> unlock_time (datetime)
        # Symbols in cooldown are blocked from entry after SL or CORRELATION_DROP
        self.symbol_cooldowns: Dict[str, datetime] = {}

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

        # Calculate how much data we need
        # We need lookback_window_days * 2 + 1 before start_date for calculations
        warmup_days = self.lookback_window_days * 2 + 1
        data_start = start_date - timedelta(days=warmup_days)

        print(f"Loading data from {data_start.date()} (warmup: {warmup_days} days)")

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

        # Calculate minimum candles needed for calculations
        timeframe_minutes = get_timeframe_minutes(self.timeframe)
        candles_per_day = 24 * 60 // timeframe_minutes
        min_candles = candles_per_day * self.lookback_window_days * 2

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
                aligned_df = df.reindex(common_index, method="ffill")
                aligned_df = aligned_df.dropna()
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
            current_time, current_bar_idx, window_data, filtered_results
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

    async def _check_exits(
        self,
        current_time: datetime,
        current_bar_idx: int,
        window_data: Dict[str, pd.DataFrame],
        filtered_results: Dict[str, ZScoreResult],
        all_results: Dict[str, ZScoreResult],
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

                if position.side == "long":
                    # Long: entered at Z <= -entry, exit at Z >= tp or Z <= -sl
                    if z >= self.config.z_tp_threshold:
                        exit_reason = "TP"
                    elif z <= -self.config.z_sl_threshold:
                        exit_reason = "SL"
                else:
                    # Short: entered at Z >= entry, exit at Z <= tp or Z >= sl
                    if z <= self.config.z_tp_threshold:
                        exit_reason = "TP"
                    elif z >= self.config.z_sl_threshold:
                        exit_reason = "SL"

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
    ) -> None:
        """Check for new entry signals."""
        # Only one spread position at a time (one coin vs ETH)
        if len(self.positions) >= self.config.max_spreads:
            return  # Already have a spread open

        # Find the best signal (highest |z-score|)
        best_signal = None
        best_z_abs = 0

        for symbol, z_result in filtered_results.items():
            if symbol in self.positions:
                continue  # Already have position in this symbol

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

            # Check if signal is valid
            if abs(z) >= self.config.z_entry_threshold:
                if abs(z) > best_z_abs:
                    best_z_abs = abs(z)
                    side = "short" if z >= self.config.z_entry_threshold else "long"
                    best_signal = (symbol, side, z_result)

        # Open position for the best signal only
        if best_signal:
            symbol, side, z_result = best_signal
            await self._open_position(
                symbol=symbol,
                side=side,
                z_result=z_result,
                current_time=current_time,
                current_bar_idx=current_bar_idx,
                window_data=window_data,
            )

    async def _open_position(
        self,
        symbol: str,
        side: str,
        z_result: ZScoreResult,
        current_time: datetime,
        current_bar_idx: int,
        window_data: Dict[str, pd.DataFrame],
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

        # Create position
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
        )

        self.positions[symbol] = position

        print(
            f"OPEN {side.upper()} SPREAD {symbol} | "
            f"Z={z_result.current_z_score:.2f} β={beta:.3f} | "
            f"Coin: ${coin_size:.2f} @ {coin_price:.4f} | "
            f"Hedge: ${primary_size:.2f} @ {primary_price:.4f}"
        )

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
        pnl = raw_pnl - total_fees

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
        )

        self.trades.append(trade)

        # Add cooldown for SL or CORRELATION_DROP exits
        if exit_reason in ("SL", "CORRELATION_DROP") and self.config.cooldown_bars > 0:
            cooldown_minutes = self.config.cooldown_bars * get_timeframe_minutes(
                self.timeframe
            )
            unlock_time = current_time + timedelta(minutes=cooldown_minutes)
            self.symbol_cooldowns[symbol] = unlock_time
            print(
                f"⏸️  {symbol} in cooldown for {cooldown_minutes} min "
                f"(until {unlock_time.strftime('%H:%M')})"
            )

        print(
            f"CLOSE {position.side.upper()} SPREAD {symbol} | "
            f"PnL: ${pnl:.2f} ({trade.pnl_pct:+.2f}%) | "
            f"Coin: {coin_pct_change*100:+.2f}% | "
            f"Primary: {primary_pct_change*100:+.2f}% | "
            f"Reason: {exit_reason} | "
            f"Duration: {duration:.1f}h"
        )

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

    # Create backtest config
    config = BacktestConfig(
        initial_balance=args.balance,
        position_size_pct=args.risk or BacktestConfig.position_size_pct,
        max_spreads=args.max_spreads,
        leverage=args.leverage or BacktestConfig.leverage,
        z_entry_threshold=settings.Z_ENTRY_THRESHOLD,
        z_tp_threshold=settings.Z_TP_THRESHOLD,
        z_sl_threshold=settings.Z_SL_THRESHOLD,
        min_correlation=settings.MIN_CORRELATION,
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
    print(f"  Trading Pairs:       {len(settings.CONSISTENT_PAIRS)}")
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
        )

        # Create and run backtester
        backtester = StatArbBacktest(
            config=config,
            settings=settings,
            data_loader=data_loader,
            correlation_service=correlation_service,
            z_score_service=z_score_service,
            volatility_filter_service=volatility_filter_service,
        )

        result = await backtester.run(start_date, end_date)

        # Print results
        print_results(result)

        # Export trades if requested
        if args.export:
            export_trades(result, args.export)

        # Plot if not disabled
        if not args.no_plot:
            plot_results(result, save_path="backtest_results.png")

    finally:
        await exchange.disconnect()
        container.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
