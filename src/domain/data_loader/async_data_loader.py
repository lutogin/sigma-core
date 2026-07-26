"""
Async Data Loader Service.

Non-blocking data loading from pairs and optimization repositories.
Merges and ranks pairs for statistical arbitrage.
Also handles OHLCV data loading with optional caching via TimescaleDB.
"""

from typing import Optional, List, Dict, Tuple
from datetime import datetime, timedelta, timezone
import pandas as pd
import asyncio

from src.domain.utils import get_timeframe_minutes


class AsyncDataLoaderService:
    """
    Async service for loading and ranking trading pairs.

    Non-blocking operations for use with WebSocket connections
    and real-time trading.

    """

    def __init__(
        self,
        logger,
        exchange_client=None,
        ohlcv_repository=None,
    ):
        """
        Initialize async data loader.

        :param logger: Logger instance
        :param exchange_client: Exchange client for OHLCV data (optional)
        :param ohlcv_repository: OHLCV repository for caching (optional, TimescaleDB)
        """
        self._logger = logger
        self._exchange = exchange_client
        self._ohlcv_repo = ohlcv_repository

    # =========================================================================
    # OHLCV Data Loading (Caching Layer)
    # =========================================================================

    async def load_ohlcv_with_cache(
        self,
        symbol: str,
        num_bars: int,
        timeframe: str = "15m",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Load OHLCV data with caching support.

        Strategy:
        1. Try to load from TimescaleDB cache first
        2. Check for missing date ranges
        3. Fetch missing data from exchange
        4. Save to cache
        5. Return merged OHLCV DataFrame

        :param symbol: Trading symbol (e.g., "BTC", "ETH")
        :param num_bars: Number of bars to load
        :param timeframe: Candle timeframe (default: "15m")
        :param start_time: Start time (default: num_bars * timeframe ago)
        :param end_time: End time (default: now)
        :return: OHLCV DataFrame with datetime index and columns: open, high, low, close, volume
        """
        if not self._exchange:
            raise ValueError("Exchange client required for OHLCV loading")

        if end_time is None:
            end_time = datetime.now(timezone.utc)

        if start_time is None:
            # Calculate start time based on timeframe
            timeframe_minutes = get_timeframe_minutes(timeframe)
            start_time = end_time - timedelta(minutes=num_bars * timeframe_minutes)

        result = await self.load_ohlcv_bulk(
            symbols=[symbol],
            start_time=start_time,
            end_time=end_time,
            batch_size=1,
            timeframe=timeframe,
        )
        return result.get(symbol, pd.DataFrame())

    async def load_ohlcv_pair_with_cache(
        self,
        symbol_x: str,
        symbol_y: str,
        num_bars: int,
        timeframe: str = "15m",
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load OHLCV data for a pair of symbols with caching.

        :param symbol_x: First symbol (e.g., "BTC")
        :param symbol_y: Second symbol (e.g., "ETH")
        :param num_bars: Number of bars to load
        :param timeframe: Candle timeframe (default: "15m")
        :return: Tuple of (df_x, df_y) DataFrames
        """
        now = datetime.now(timezone.utc)
        timeframe_minutes = get_timeframe_minutes(timeframe)
        start_time = now - timedelta(minutes=num_bars * timeframe_minutes)

        # Load both concurrently
        df_x_task = asyncio.create_task(
            self.load_ohlcv_with_cache(
                symbol=symbol_x,
                num_bars=num_bars,
                timeframe=timeframe,
                start_time=start_time,
                end_time=now,
            )
        )
        df_y_task = asyncio.create_task(
            self.load_ohlcv_with_cache(
                symbol=symbol_y,
                num_bars=num_bars,
                timeframe=timeframe,
                start_time=start_time,
                end_time=now,
            )
        )

        df_x, df_y = await asyncio.gather(df_x_task, df_y_task)

        return df_x, df_y

    async def load_ohlcv_bulk(
        self,
        symbols: List[str],
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 10,
        timeframe: str = "15m",
    ) -> Dict[str, pd.DataFrame]:
        """
        Load OHLCV data for multiple symbols efficiently.

        Optimized strategy:
        1. Load ALL cached data in one DB query (no symbol filter)
        2. Identify which symbols are missing or have incomplete data
        3. Batch fetch missing data from exchange
        4. Bulk insert all fetched data to cache
        5. Return merged result

        IMPORTANT: Only returns CLOSED candles. No NaN values allowed.

        :param symbols: List of symbols to load
        :param start_time: Start time
        :param end_time: End time
        :param batch_size: Number of concurrent exchange requests
        :param timeframe: Candle timeframe (default: "15m")
        :return: Dictionary mapping symbol to DataFrame
        """
        if not self._exchange:
            raise ValueError("Exchange client required for OHLCV loading")

        self._logger.info(
            f"Bulk loading OHLCV for {len(symbols)} symbols "
            f"({start_time.date()} to {end_time.date()})"
        )

        result: Dict[str, pd.DataFrame] = {}
        timeframe_minutes = get_timeframe_minutes(timeframe)

        # Step 1: Load cached data for requested symbols in one query
        if self._ohlcv_repo:
            try:
                result = self._ohlcv_repo.load_all_symbols_data(
                    interval=timeframe,
                    start_date=start_time,
                    end_date=end_time,
                    symbols=symbols,  # Filter at DB level
                )
                self._logger.info(f"Loaded {len(result)} symbols from cache")
            except Exception as e:
                self._logger.warning(f"Error bulk loading from cache: {e}")
                result = {}

        # Step 2: Build expected time index (all closed candles in range)
        expected_index = self._build_expected_index(
            start_time, end_time, timeframe_minutes
        )
        self._logger.debug(f"Expected {len(expected_index)} candles in range")

        # Step 3: Identify symbols that need fetching
        symbols_to_fetch: List[str] = []
        symbols_missing_ranges: Dict[str, List[tuple]] = (
            {}
        )  # symbol -> [(start, end), ...]

        for symbol in symbols:
            if symbol not in result or result[symbol].empty:
                # No data at all - fetch full range
                symbols_to_fetch.append(symbol)
                symbols_missing_ranges[symbol] = [(start_time, end_time)]
            else:
                df = result[symbol]
                # Find missing candles by comparing with expected index
                missing_ranges = self._find_missing_ranges(
                    pd.DatetimeIndex(df.index), expected_index, timeframe_minutes
                )
                if missing_ranges:
                    symbols_to_fetch.append(symbol)
                    symbols_missing_ranges[symbol] = missing_ranges
                    self._logger.debug(
                        f"{symbol}: found {len(missing_ranges)} missing range(s)"
                    )

        self._logger.info(f"Need to fetch data for {len(symbols_to_fetch)} symbols")

        # Step 4: Fetch missing data from exchange in batches
        if symbols_to_fetch:
            fetched_data: Dict[str, pd.DataFrame] = {}

            for i in range(0, len(symbols_to_fetch), batch_size):
                batch = symbols_to_fetch[i : i + batch_size]
                self._logger.info(
                    f"Fetching batch {i // batch_size + 1}/"
                    f"{(len(symbols_to_fetch) - 1) // batch_size + 1} "
                    f"({len(batch)} symbols)"
                )

                # Create fetch tasks for batch - fetch all missing ranges
                tasks = []
                task_info = []  # Track which symbol each task belongs to

                for symbol in batch:
                    for range_start, range_end in symbols_missing_ranges.get(
                        symbol, []
                    ):
                        tasks.append(
                            self._exchange.fetch_ohlcv(
                                symbol=symbol,
                                interval=timeframe,
                                start_date=range_start,
                                end_date=range_end,
                            )
                        )
                        task_info.append(symbol)

                # Execute batch concurrently
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                # Process results
                symbol_dfs: Dict[str, List[pd.DataFrame]] = {}
                for symbol, fetch_result in zip(task_info, batch_results):
                    if isinstance(fetch_result, Exception):
                        raise RuntimeError(
                            f"OHLCV fetch failed for {symbol}"
                        ) from fetch_result
                    elif (
                        isinstance(fetch_result, pd.DataFrame)
                        and not fetch_result.empty
                    ):
                        if symbol not in symbol_dfs:
                            symbol_dfs[symbol] = []
                        symbol_dfs[symbol].append(fetch_result)

                # Merge fetched data for each symbol
                for symbol, dfs in symbol_dfs.items():
                    merged_fetch = pd.concat(dfs).sort_index()
                    merged_fetch = merged_fetch[
                        ~merged_fetch.index.duplicated(keep="last")
                    ]

                    fetched_data[symbol] = merged_fetch

                    # Merge with existing cached data
                    if symbol in result and not result[symbol].empty:
                        merged = pd.concat([result[symbol], merged_fetch])
                        merged = merged[~merged.index.duplicated(keep="last")]
                        merged = merged.sort_index()
                        result[symbol] = merged
                    else:
                        result[symbol] = merged_fetch

                # Rate limiting
                if i + batch_size < len(symbols_to_fetch):
                    await asyncio.sleep(1.0)

            # Step 5: Bulk insert fetched data to cache
            if self._ohlcv_repo and fetched_data:
                try:
                    self._ohlcv_repo.save_data_bulk(timeframe, fetched_data)
                    self._logger.info(f"Cached {len(fetched_data)} symbols to database")
                except Exception as e:
                    self._logger.warning(f"Error bulk saving to cache: {e}")

        # Step 6: Final validation - ensure no NaN and proper alignment
        final_result = {}
        for symbol in symbols:
            if symbol in result and not result[symbol].empty:
                df = result[symbol]

                # Filter to expected range
                df = df[(df.index >= start_time) & (df.index < end_time)]

                # Drop any rows with NaN values
                df = df.dropna()

                if not df.empty:
                    available_expected = expected_index[
                        expected_index >= df.index.min()
                    ]
                    missing_after_listing = available_expected.difference(df.index)
                    if len(missing_after_listing) > 0:
                        raise RuntimeError(
                            f"{symbol} has {len(missing_after_listing)} missing "
                            f"{timeframe} candles after listing"
                        )
                    final_result[symbol] = df
                    # Calculate close time of last candle (open_time + timeframe)
                    last_candle_close = df.index.max() + timedelta(
                        minutes=timeframe_minutes
                    )
                    self._logger.debug(
                        f"{symbol}: {len(df)} candles "
                        f"(open: {df.index.min().isoformat()} to {df.index.max().isoformat()}, "
                        f"last_close: {last_candle_close.isoformat()})"
                    )
                else:
                    self._logger.warning(f"{symbol}: DataFrame empty after validation")
            else:
                self._logger.warning(f"{symbol}: Not found in result after loading")

        self._logger.info(
            f"Bulk load complete: {len(final_result)}/{len(symbols)} symbols loaded"
        )

        return final_result

    def _build_expected_index(
        self,
        start_time: datetime,
        end_time: datetime,
        timeframe_minutes: int,
    ) -> pd.DatetimeIndex:
        """
        Build expected time index for the given range.

        Only includes timestamps for CLOSED candles (candle close time <= now).
        """
        now = datetime.now(timezone.utc)

        # Round start_time down to nearest candle boundary
        start_ts = start_time.replace(second=0, microsecond=0)
        start_minute = (start_ts.minute // timeframe_minutes) * timeframe_minutes
        start_ts = start_ts.replace(minute=start_minute)

        # Generate all expected timestamps
        timestamps = []
        current = start_ts

        while current < end_time:
            # Only include if candle is closed (current + timeframe <= now)
            candle_close_time = current + timedelta(minutes=timeframe_minutes)
            if candle_close_time <= now:
                timestamps.append(current)
            current += timedelta(minutes=timeframe_minutes)

        return pd.DatetimeIndex(timestamps, tz=timezone.utc)

    def _find_missing_ranges(
        self,
        actual_index: pd.DatetimeIndex,
        expected_index: pd.DatetimeIndex,
        timeframe_minutes: int,
    ) -> List[tuple]:
        """
        Find missing time ranges by comparing actual vs expected index.

        Returns list of (start, end) tuples for missing ranges.
        """
        if len(expected_index) == 0:
            return []

        # Find missing timestamps
        missing = expected_index.difference(actual_index)

        if len(missing) == 0:
            return []

        # Group consecutive missing timestamps into ranges
        ranges = []
        missing_sorted = missing.sort_values()

        range_start = missing_sorted[0]
        prev_ts = range_start

        for ts in missing_sorted[1:]:
            # Check if consecutive (within 1 candle interval)
            expected_next = prev_ts + timedelta(minutes=timeframe_minutes)
            if ts <= expected_next + timedelta(minutes=1):  # Small tolerance
                prev_ts = ts
            else:
                # Gap found - close current range and start new one
                ranges.append(
                    (range_start, prev_ts + timedelta(minutes=timeframe_minutes))
                )
                range_start = ts
                prev_ts = ts

        # Close last range
        ranges.append((range_start, prev_ts + timedelta(minutes=timeframe_minutes)))

        return ranges
