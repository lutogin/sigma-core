"""
OHLCV Repository - TimescaleDB storage for OHLCV data.

Replaces CSV-based CacheManager with database storage.
Supports multiple symbols and intervals.
"""

import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Set, Dict

from src.infra.timescale import TimescaleDB


def _ensure_utc(dt: datetime) -> datetime:
    """Ensure datetime is UTC. Convert tz-naive to UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class OHLCVRepository:
    """
    OHLCV data repository backed by TimescaleDB.

    Provides the same interface as CacheManager but stores data
    in TimescaleDB instead of CSV files.

    Key features:
    - Stores OHLCV data with symbol and interval
    - Efficient date range queries
    - Automatic deduplication on insert
    - Bulk insert support

    Usage:
        repo = OHLCVRepository(db, logger)

        # Save data
        repo.save_data("BTC/USDT:USDT", "1h", df)

        # Load data
        df = repo.load_data("BTC/USDT:USDT", "1h", start_date, end_date)
    """

    TABLE_NAME = "ohlcv"

    def __init__(self, db: TimescaleDB, logger):
        """
        Initialize OHLCV repository.

        :param db: TimescaleDB connection (DI)
        :param logger: Logger instance (DI)
        """
        self.db = db
        self.logger = logger

        # Ensure connection is established
        self.db.connect()

    def save_data(self, symbol: str, interval: str, df: pd.DataFrame) -> int:
        """
        Save OHLCV data to database.

        Uses INSERT ... ON CONFLICT to handle duplicates.

        :param symbol: Trading symbol (e.g., "BTC/USDT:USDT")
        :param interval: Candle interval (e.g., "1h")
        :param df: DataFrame with OHLCV data (index=timestamp)
        :return: Number of rows inserted/updated
        """
        if df.empty:
            return 0

        # Prepare data for batch insert
        params_list = []

        for timestamp, row in df.iterrows():
            # Handle both datetime index and DatetimeIndex
            if isinstance(timestamp, pd.Timestamp):
                ts = timestamp.to_pydatetime()
            else:
                ts = timestamp

            # Normalize timestamp to UTC (ensure tz-aware)
            ts = _ensure_utc(ts)

            params_list.append((
                symbol,
                interval,
                ts,
                float(row.get('open', row.get('Open', 0))),
                float(row.get('high', row.get('High', 0))),
                float(row.get('low', row.get('Low', 0))),
                float(row.get('close', row.get('Close', 0))),
                float(row.get('volume', row.get('Volume', 0))),
            ))

        # Batch upsert
        query = """
            INSERT INTO ohlcv (symbol, interval, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, interval, timestamp)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """

        rows_affected = self.db.execute_many(query, params_list)
        return rows_affected

    def load_data(
        self,
        symbol: str,
        interval: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Load OHLCV data from database.

        :param symbol: Trading symbol
        :param interval: Candle interval
        :param start_date: Start date (inclusive)
        :param end_date: End date (inclusive)
        :return: DataFrame with OHLCV data
        """
        # Normalize dates to UTC start of day
        start = _ensure_utc(start_date).replace(hour=0, minute=0, second=0, microsecond=0)
        # End of day for end_date (UTC)
        end = _ensure_utc(end_date).replace(hour=23, minute=59, second=59, microsecond=999999)

        query = """
            SELECT timestamp, open, high, low, close, volume
            FROM ohlcv
            WHERE symbol = %s
              AND interval = %s
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY timestamp ASC
        """

        rows = self.db.fetch_all(query, (symbol, interval, start, end))

        if not rows:
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df.set_index('timestamp', inplace=True)

        # Convert index to datetime and ensure UTC timezone
        df.index = pd.to_datetime(df.index, utc=True)
        # If timestamps from DB are tz-naive, assume they're UTC
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')
        else:
            df.index = df.index.tz_convert('UTC')

        return df

    def get_cached_dates(self, symbol: str, interval: str) -> Set[datetime]:
        """
        Get set of dates for which data exists.

        :param symbol: Trading symbol
        :param interval: Candle interval
        :return: Set of dates with data (all UTC, tz-aware)
        """
        query = """
            SELECT DISTINCT DATE(timestamp) as date
            FROM ohlcv
            WHERE symbol = %s AND interval = %s
            ORDER BY date
        """

        rows = self.db.fetch_all(query, (symbol, interval))

        result = set()
        for row in rows:
            # row[0] is a date object from PostgreSQL (tz-naive)
            date_obj = row[0]
            if isinstance(date_obj, datetime):
                # If it's already a datetime, ensure it's UTC
                dt = _ensure_utc(date_obj).replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                # If it's a date object, combine with min time and add UTC timezone
                dt = datetime.combine(date_obj, datetime.min.time(), tzinfo=timezone.utc)
            result.add(dt)

        return result

    def get_missing_date_ranges(
        self,
        symbol: str,
        interval: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """
        Determine which date ranges are missing from cache.

        :param symbol: Trading symbol
        :param interval: Candle interval
        :param start_date: Start date
        :param end_date: End date
        :return: List of (start, end) tuples with missing ranges
        """
        # Normalize input dates to UTC
        start_date = _ensure_utc(start_date)
        end_date = _ensure_utc(end_date)

        # Get all dates we need (all UTC, tz-aware)
        all_dates = self._get_date_range(start_date, end_date)

        # Get dates we have (all UTC, tz-aware)
        cached_dates = self.get_cached_dates(symbol, interval)

        # Normalize all dates to UTC for comparison (normalize to start of day)
        all_dates_normalized = {
            _ensure_utc(d).replace(hour=0, minute=0, second=0, microsecond=0)
            for d in all_dates
        }
        cached_dates_normalized = {
            _ensure_utc(d).replace(hour=0, minute=0, second=0, microsecond=0)
            for d in cached_dates
        }

        # Find missing dates
        missing_dates = sorted(all_dates_normalized - cached_dates_normalized)

        if not missing_dates:
            return []

        # Group consecutive dates into ranges
        return self._group_consecutive_dates(missing_dates)

    def _get_date_range(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """Generate list of dates in range (UTC)."""
        dates = []
        current = _ensure_utc(start_date).replace(hour=0, minute=0, second=0, microsecond=0)
        end = _ensure_utc(end_date).replace(hour=0, minute=0, second=0, microsecond=0)

        while current <= end:
            dates.append(current)
            current += timedelta(days=1)

        return dates

    def _group_consecutive_dates(self, dates: List[datetime]) -> List[Tuple[datetime, datetime]]:
        """Group consecutive dates into ranges."""
        if not dates:
            return []

        # Ensure all dates are UTC and tz-aware
        dates = [_ensure_utc(d).replace(hour=0, minute=0, second=0, microsecond=0) for d in dates]
        dates.sort()

        ranges = []
        range_start = dates[0]
        range_end = dates[0]

        for date in dates[1:]:
            # Ensure both dates are tz-aware for comparison
            date = _ensure_utc(date).replace(hour=0, minute=0, second=0, microsecond=0)
            range_end_utc = _ensure_utc(range_end).replace(hour=0, minute=0, second=0, microsecond=0)

            if (date - range_end_utc).days == 1:
                range_end = date
            else:
                ranges.append((range_start, range_end))
                range_start = date
                range_end = date

        ranges.append((range_start, range_end))
        return ranges

    def load_cached_data(
        self,
        symbol: str,
        interval: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Load cached data (alias for load_data for CacheManager compatibility).

        :param symbol: Trading symbol
        :param interval: Candle interval
        :param start_date: Start date
        :param end_date: End date
        :return: DataFrame with OHLCV data
        """
        return self.load_data(symbol, interval, start_date, end_date)

    def load_all_symbols_data(
        self,
        interval: str,
        start_date: datetime,
        end_date: datetime,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Load OHLCV data for symbols in one query.

        This is much more efficient than loading per-symbol when you need
        data for many symbols.

        :param interval: Candle interval (e.g., "1h")
        :param start_date: Start date (inclusive)
        :param end_date: End date (inclusive)
        :param symbols: Optional list of symbols to filter (loads all if None)
        :return: Dictionary mapping symbol to DataFrame
        """
        start = _ensure_utc(start_date).replace(hour=0, minute=0, second=0, microsecond=0)
        end = _ensure_utc(end_date).replace(hour=23, minute=59, second=59, microsecond=999999)

        if symbols:
            # Filter by specific symbols using IN clause
            placeholders = ", ".join(["%s"] * len(symbols))
            query = f"""
                SELECT symbol, timestamp, open, high, low, close, volume
                FROM ohlcv
                WHERE interval = %s
                  AND timestamp >= %s
                  AND timestamp <= %s
                  AND symbol IN ({placeholders})
                ORDER BY symbol, timestamp ASC
            """
            params = (interval, start, end, *symbols)
        else:
            # Load all symbols
            query = """
                SELECT symbol, timestamp, open, high, low, close, volume
                FROM ohlcv
                WHERE interval = %s
                  AND timestamp >= %s
                  AND timestamp <= %s
                ORDER BY symbol, timestamp ASC
            """
            params = (interval, start, end)

        rows = self.db.fetch_all(query, params)

        if not rows:
            return {}

        # Group by symbol
        result: Dict[str, pd.DataFrame] = {}
        current_symbol = None
        current_data = []

        for row in rows:
            symbol, timestamp, open_p, high, low, close, volume = row

            if current_symbol != symbol:
                if current_symbol is not None and current_data:
                    result[current_symbol] = self._rows_to_dataframe(current_data)
                current_symbol = symbol
                current_data = []

            current_data.append((timestamp, open_p, high, low, close, volume))

        # Don't forget the last symbol
        if current_symbol is not None and current_data:
            result[current_symbol] = self._rows_to_dataframe(current_data)

        self.logger.info(f"Bulk loaded {len(result)} symbols from cache")
        return result

    def _rows_to_dataframe(self, rows: List[Tuple]) -> pd.DataFrame:
        """Convert rows to DataFrame with proper timestamp index."""
        df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df.set_index('timestamp', inplace=True)
        df.index = pd.to_datetime(df.index, utc=True)
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')
        else:
            df.index = df.index.tz_convert('UTC')
        return df

    def get_symbol_date_coverage(
        self,
        interval: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Tuple[datetime, datetime]]:
        """
        Get the min/max timestamp for each symbol in the date range.

        Used to identify which symbols have incomplete data.

        :param interval: Candle interval
        :param start_date: Start date
        :param end_date: End date
        :return: Dict mapping symbol to (min_timestamp, max_timestamp)
        """
        start = _ensure_utc(start_date).replace(hour=0, minute=0, second=0, microsecond=0)
        end = _ensure_utc(end_date).replace(hour=23, minute=59, second=59, microsecond=999999)

        query = """
            SELECT symbol, MIN(timestamp), MAX(timestamp)
            FROM ohlcv
            WHERE interval = %s
              AND timestamp >= %s
              AND timestamp <= %s
            GROUP BY symbol
        """

        rows = self.db.fetch_all(query, (interval, start, end))

        result = {}
        for row in rows:
            symbol, min_ts, max_ts = row
            result[symbol] = (_ensure_utc(min_ts), _ensure_utc(max_ts))

        return result

    def save_data_bulk(self, interval: str, data: Dict[str, pd.DataFrame]) -> int:
        """
        Bulk save OHLCV data for multiple symbols.

        More efficient than calling save_data for each symbol.

        :param interval: Candle interval
        :param data: Dictionary mapping symbol to DataFrame
        :return: Total number of rows inserted/updated
        """
        if not data:
            return 0

        # Collect all params
        params_list = []

        for symbol, df in data.items():
            if df.empty:
                continue

            for timestamp, row in df.iterrows():
                if isinstance(timestamp, pd.Timestamp):
                    ts = timestamp.to_pydatetime()
                else:
                    ts = timestamp

                ts = _ensure_utc(ts)

                params_list.append((
                    symbol,
                    interval,
                    ts,
                    float(row.get('open', row.get('Open', 0))),
                    float(row.get('high', row.get('High', 0))),
                    float(row.get('low', row.get('Low', 0))),
                    float(row.get('close', row.get('Close', 0))),
                    float(row.get('volume', row.get('Volume', 0))),
                ))

        if not params_list:
            return 0

        query = """
            INSERT INTO ohlcv (symbol, interval, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, interval, timestamp)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """

        rows_affected = self.db.execute_many(query, params_list)
        self.logger.info(f"Bulk saved {rows_affected} rows for {len(data)} symbols")
        return rows_affected

