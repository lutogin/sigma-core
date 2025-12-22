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

        df_result = pd.DataFrame()

        # Try cache first
        if self._ohlcv_repo:
            try:
                df_result = self._ohlcv_repo.load_data(
                    symbol=symbol,
                    interval=timeframe,
                    start_date=start_time,
                    end_date=end_time,
                )

                # Check for missing ranges
                missing_ranges = self._ohlcv_repo.get_missing_date_ranges(
                    symbol, timeframe, start_time, end_time
                )

                if missing_ranges:
                    self._logger.debug(
                        f"Found {len(missing_ranges)} missing ranges for {symbol}, "
                        f"fetching from exchange"
                    )

                    for range_start, range_end in missing_ranges:
                        missing_df = await self._exchange.fetch_ohlcv(
                            symbol=symbol,
                            interval=timeframe,
                            start_date=range_start,
                            end_date=range_end,
                        )

                        if not missing_df.empty:
                            # Save to cache
                            self._ohlcv_repo.save_data(symbol, timeframe, missing_df)

                            # Merge with result
                            if df_result.empty:
                                df_result = missing_df
                            else:
                                df_result = pd.concat(
                                    [df_result, missing_df]
                                ).sort_index()
                                df_result = df_result[
                                    ~df_result.index.duplicated(keep="last")
                                ]

            except Exception as e:
                self._logger.warning(
                    f"Error loading from cache for {symbol}: {e}, "
                    f"falling back to exchange"
                )
                df_result = pd.DataFrame()

        # Fallback to exchange if cache unavailable or empty
        if df_result.empty:
            self._logger.debug(
                f"Loading {symbol} OHLCV from exchange "
                f"({start_time.isoformat()} to {end_time.isoformat()})"
            )

            df_result = await self._exchange.fetch_ohlcv(
                symbol=symbol,
                interval=timeframe,
                start_date=start_time,
                end_date=end_time,
            )

            # Save to cache if available
            if self._ohlcv_repo and not df_result.empty:
                try:
                    self._ohlcv_repo.save_data(symbol, timeframe, df_result)
                except Exception as e:
                    self._logger.warning(f"Failed to save {symbol} to cache: {e}")

        return df_result

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

        # Step 2: Identify symbols that need fetching
        # A symbol needs fetching if:
        # - Not in cache at all
        # - Has incomplete date range (missing start or end)
        # - Latest data is older than one candle interval
        symbols_to_fetch: List[str] = []
        symbols_need_old_data: Dict[str, datetime] = {}  # symbol -> cached_start
        expected_start = start_time.replace(hour=0, minute=0, second=0, microsecond=0)

        # Dynamic freshness threshold based on timeframe
        # Data is stale if older than 1 candle + small buffer (2 min)
        now = datetime.now(timezone.utc)
        timeframe_minutes = get_timeframe_minutes(timeframe)
        freshness_buffer = timedelta(minutes=timeframe_minutes + 2)
        freshness_threshold = now - freshness_buffer

        self._logger.debug(
            f"Expected range: {expected_start.date()} to {now.isoformat()}, "
            f"freshness threshold: {freshness_threshold.isoformat()} "
            f"(timeframe={timeframe}, buffer={timeframe_minutes + 2}min)"
        )

        for symbol in symbols:
            if symbol not in result:
                symbols_to_fetch.append(symbol)
            else:
                df = result[symbol]
                if df.empty:
                    symbols_to_fetch.append(symbol)
                else:
                    # Check if data covers the expected range
                    data_start = df.index.min()
                    data_end = df.index.max()

                    # Check if missing OLD data (start doesn't cover expected range)
                    if data_start > expected_start + timedelta(days=1):
                        self._logger.debug(
                            f"{symbol}: missing old data (cached_start={data_start.isoformat()}, "
                            f"expected={expected_start.isoformat()})"
                        )
                        symbols_need_old_data[symbol] = data_start
                        symbols_to_fetch.append(symbol)
                    # Check if data is stale (older than 1 candle interval)
                    elif data_end < freshness_threshold:
                        self._logger.debug(
                            f"{symbol}: data stale (last={data_end.isoformat()}, "
                            f"threshold={freshness_threshold.isoformat()})"
                        )
                        symbols_to_fetch.append(symbol)

        self._logger.info(
            f"Need to fetch {len(symbols_to_fetch)} symbols from exchange"
        )

        # Step 3: Fetch missing data from exchange in batches with rate limiting
        if symbols_to_fetch:
            fetched_data: Dict[str, pd.DataFrame] = {}

            for i in range(0, len(symbols_to_fetch), batch_size):
                batch = symbols_to_fetch[i : i + batch_size]
                self._logger.info(
                    f"Fetching batch {i // batch_size + 1}/"
                    f"{(len(symbols_to_fetch) - 1) // batch_size + 1} "
                    f"({len(batch)} symbols)"
                )

                # Create fetch tasks for batch
                tasks = []
                for symbol in batch:
                    # Determine fetch range based on what's missing
                    if symbol in symbols_need_old_data:
                        # Missing old data - fetch from start_time to cached_start
                        fetch_start = start_time
                        fetch_end = symbols_need_old_data[symbol]
                        self._logger.debug(
                            f"{symbol}: fetching old data {fetch_start.date()} to {fetch_end.date()}"
                        )
                    elif symbol in result and not result[symbol].empty:
                        # Only missing new data - fetch from last cached point
                        fetch_start = result[symbol].index.max()
                        fetch_end = end_time
                    else:
                        # No cached data - fetch full range
                        fetch_start = start_time
                        fetch_end = end_time

                    tasks.append(
                        self._exchange.fetch_ohlcv(
                            symbol=symbol,
                            interval=timeframe,
                            start_date=fetch_start,
                            end_date=fetch_end,
                        )
                    )

                # Execute batch concurrently
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                for symbol, fetch_result in zip(batch, batch_results):
                    if isinstance(fetch_result, Exception):
                        self._logger.warning(
                            f"Error fetching {symbol}: {repr(fetch_result)}"
                        )
                    elif (
                        isinstance(fetch_result, pd.DataFrame)
                        and not fetch_result.empty
                    ):
                        fetched_data[symbol] = fetch_result
                        # Merge with existing cached data if present
                        if symbol in result and not result[symbol].empty:
                            merged = pd.concat([result[symbol], fetch_result])
                            merged = merged[~merged.index.duplicated(keep="last")]
                            merged = merged.sort_index()
                            result[symbol] = merged
                        else:
                            result[symbol] = fetch_result

                # Rate limiting: wait between batches to avoid API ban
                if i + batch_size < len(symbols_to_fetch):
                    await asyncio.sleep(1.0)

            # Step 4: Bulk insert fetched data to cache
            if self._ohlcv_repo and fetched_data:
                try:
                    self._ohlcv_repo.save_data_bulk(timeframe, fetched_data)
                    self._logger.info(f"Cached {len(fetched_data)} symbols to database")
                except Exception as e:
                    self._logger.warning(f"Error bulk saving to cache: {e}")

        # Filter to only requested symbols
        final_result = {}
        for symbol in symbols:
            if symbol in result:
                if not result[symbol].empty:
                    final_result[symbol] = result[symbol]
                else:
                    self._logger.warning(f"{symbol}: DataFrame is empty after loading")
            else:
                self._logger.warning(f"{symbol}: Not found in result after loading")

        self._logger.info(
            f"Bulk load complete: {len(final_result)}/{len(symbols)} symbols loaded"
        )

        return final_result
