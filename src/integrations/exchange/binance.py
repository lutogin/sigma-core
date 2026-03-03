"""
Binance Exchange Client - Async-first implementation.

Centralized client for all Binance Futures USDT-M API interactions.
Uses python-binance AsyncClient for non-blocking operations.

Features:
- Market data (OHLCV, order book, ticker)
- Funding rates (current and historical)
- Trading (open/close positions, TP/SL)
- Precision handling for amounts and prices
- Rate limiting for API requests
- Retry logic for transient failures

All operations are async to avoid blocking the event loop.
"""

import asyncio
import random
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any, Callable, Awaitable, Literal, Union
from dataclasses import dataclass
from enum import Enum
import time

import pandas as pd
from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException


# =============================================================================
# Rate Limiter
# =============================================================================


class RateLimiter:
    """
    Token bucket rate limiter for API requests.

    Binance Futures limits:
    - 2400 request weight per minute
    - Most endpoints cost 1-5 weight
    - OHLCV (klines) costs 5 weight

    We use conservative limits to avoid hitting the cap.
    """

    def __init__(
        self,
        requests_per_second: float = 10.0,
        burst_size: int = 20,
    ):
        self._rate = requests_per_second
        self._burst = burst_size
        self._tokens = float(burst_size)
        self._last_update = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()

    async def acquire(self, weight: int = 1) -> None:
        """Acquire tokens, waiting if necessary."""
        async with self._lock:
            while True:
                now = asyncio.get_event_loop().time()
                elapsed = now - self._last_update
                self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
                self._last_update = now

                if self._tokens >= weight:
                    self._tokens -= weight
                    return

                # Wait for tokens to replenish
                wait_time = (weight - self._tokens) / self._rate
                await asyncio.sleep(wait_time)


@dataclass
class ExchangeConfig:
    """Exchange configuration parameters."""

    api_key: str = ""
    api_secret: str = ""
    testnet: bool = False
    default_leverage: int = 1
    margin_type: str = "cross"
    quote_currency: str = "USDT"


class OrderSide(str, Enum):
    """Order side."""

    BUY = "BUY"
    SELL = "SELL"


# Type alias for side parameter (accepts both string literals and OrderSide enum)
TradeSide = Union[Literal["buy", "sell", "BUY", "SELL"], OrderSide]


class MarginType(str, Enum):
    """Margin type."""

    CROSS = "CROSSED"
    ISOLATED = "ISOLATED"


@dataclass
class FundingRateData:
    """Funding rate record."""

    symbol: str
    funding_rate: float  # e.g., 0.0001 = 0.01%
    funding_time: datetime
    funding_interval_hours: int = 8

    @property
    def rate_pct(self) -> float:
        """Funding rate as percentage."""
        return self.funding_rate * 100


@dataclass
class MarketData:
    """Real-time market data."""

    symbol: str
    bid: float
    ask: float
    last: float
    timestamp: int

    @property
    def mid_price(self) -> float:
        """Mid price between bid and ask."""
        return (self.bid + self.ask) / 2


@dataclass
class Balance:
    """Account balance."""

    asset: str
    free: float
    used: float
    total: float


@dataclass
class Position:
    """Futures position."""

    symbol: str
    side: str  # "long" or "short"
    size: float  # Position size in base currency
    contracts: float  # Number of contracts
    entry_price: float
    mark_price: float
    unrealized_pnl: float
    leverage: int
    margin_type: str  # "cross" or "isolated"
    liquidation_price: float

    @property
    def is_open(self) -> bool:
        return self.contracts > 0


@dataclass
class Order:
    """Order response."""

    id: str
    client_order_id: str
    symbol: str
    side: str
    type: str
    price: float
    amount: float
    filled: float
    remaining: float
    status: str
    timestamp: int
    close_position: bool = False
    reduce_only: bool = False


@dataclass
class SymbolInfo:
    """Symbol trading info with precision rules."""

    symbol: str
    base_asset: str
    quote_asset: str
    price_precision: int
    quantity_precision: int
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal


class BinanceClient:
    """
    Binance Futures USDT-M async client.

    All methods are async for non-blocking I/O operations.
    Uses python-binance AsyncClient internally.
    """

    def __init__(
        self,
        config: ExchangeConfig,
        logger,
    ):
        """
        Initialize Binance client.

        :param api_key: Binance API key
        :param api_secret: Binance API secret
        :param testnet: Use testnet or production
        :param default_leverage: Default leverage for positions
        :param default_margin_type: Default margin type (ISOLATED/CROSSED)
        :param quote_currency: Quote currency for trading pairs
        :param logger: Logger instance (DI)
        """
        self._api_key = config.api_key
        self._api_secret = config.api_secret
        self._testnet = config.testnet
        self._default_leverage = config.default_leverage
        self._default_margin_type = config.margin_type
        self._quote_currency = config.quote_currency
        self.logger = logger

        self._client: Optional[AsyncClient] = None
        self._client_lock = asyncio.Lock()
        
        self._markets_cache: Dict[str, SymbolInfo] = {}
        self._funding_intervals: Dict[str, int] = {}
        self._is_connected = False

        # Rate limiter: 10 requests/sec with burst of 20
        # Conservative to avoid hitting Binance limits (2400 weight/min)
        self._rate_limiter = RateLimiter(requests_per_second=10.0, burst_size=20)

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def api_key(self) -> str:
        return self._api_key

    @property
    def api_secret(self) -> str:
        return self._api_secret

    @property
    def testnet(self) -> bool:
        return self._testnet

    @property
    def default_leverage(self) -> int:
        return self._default_leverage

    @property
    def default_margin_type(self) -> str:
        return self._default_margin_type

    @property
    def quote_currency(self) -> str:
        return self._quote_currency

    # =========================================================================
    # Client initialization
    # =========================================================================

    async def _get_client(self) -> AsyncClient:
        """Get or create async client."""
        async with self._client_lock:
            if self._client is None:
                try:
                    # Default bootstrap path from python-binance (uses spot ping/time).
                    self._client = await AsyncClient.create(
                        api_key=self.api_key or "",
                        api_secret=self.api_secret or "",
                        testnet=self.testnet,
                        requests_params={"timeout": 60},  # Increased from 30 to 60 seconds
                    )
                except Exception as exc:
                    # Some environments cannot resolve api.binance.com while futures endpoints
                    # (fapi.binance.com / testnet.binancefuture.com) are available.
                    # Fallback to futures-only bootstrap to avoid hard dependency on spot DNS.
                    err = str(exc).lower()
                    spot_dns_issue = "api.binance.com" in err or "nodename nor servname" in err
                    if not spot_dns_issue:
                        raise

                    self.logger.warning(
                        f"Spot API bootstrap failed ({exc}). "
                        "Falling back to futures-only bootstrap."
                    )

                    client = AsyncClient(
                        api_key=self.api_key or "",
                        api_secret=self.api_secret or "",
                        testnet=self.testnet,
                        requests_params={"timeout": 60},
                    )
                    # Keep timestamp offset for signed futures requests.
                    server_time = await client.futures_time()
                    client.timestamp_offset = server_time["serverTime"] - int(time.time() * 1000)
                    self._client = client
            return self._client

    def _is_transient_network_error(self, error: Exception) -> bool:
        """Best-effort classification for retryable transport/network failures."""
        msg = str(error).lower()
        transient_markers = (
            "closing transport",
            "connection reset",
            "server disconnected",
            "cannot connect to host",
            "temporarily unavailable",
            "connector is closed",
            "broken pipe",
        )
        return any(marker in msg for marker in transient_markers)

    async def _invalidate_client(self, reason: str) -> None:
        """
        Detach the current client so next request creates a fresh connection pool.

        The old client is NOT closed immediately — closing the aiohttp connector
        cancels all in-flight requests on it (`CancelledError`), which cascades
        through concurrent coroutines sharing the same client.  Instead we
        schedule a deferred close, giving in-flight requests time to finish or
        fail on their own (and be retried with a fresh client).
        """
        async with self._client_lock:
            if self._client is None:
                return

            self.logger.warning(f"Resetting Binance HTTP client ({reason})")
            old_client = self._client
            self._client = None

            asyncio.create_task(self._deferred_close_client(old_client))

    async def _deferred_close_client(self, client: "AsyncClient") -> None:
        """Close a detached client after a grace period for in-flight requests."""
        await asyncio.sleep(10)
        try:
            await client.close_connection()
        except Exception:
            pass

    async def connect(self) -> None:
        """Connect and load markets."""
        try:
            self.logger.info("Connecting to Binance USDT-M Futures...")

            client = await self._get_client()

            # Test connection
            await client.futures_ping()

            # Load markets
            await self.load_markets()

            # # Enable Hedge Mode (dual side position)
            # await self.set_position_mode(hedge_mode=True)

            self._is_connected = True
            self.logger.info(
                f"Connected to Binance. Loaded {len(self._markets_cache)} markets"
            )

        except Exception as e:
            self.logger.error(f"Failed to connect to Binance: {e}")
            raise

    async def disconnect(self) -> None:
        """Disconnect and cleanup."""
        try:
            self.logger.info("Disconnecting from Binance...")

            if self._client:
                await self._client.close_connection()
                self._client = None

            # Clear caches
            self._markets_cache.clear()
            self._funding_intervals.clear()

            self._is_connected = False
            self.logger.info("Disconnected from Binance")

        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")

    @property
    def is_connected(self) -> bool:
        return self._is_connected and len(self._markets_cache) > 0

    async def _ensure_connected(self) -> None:
        """Ensure client is connected."""
        if not self.is_connected:
            await self.connect()

    # =========================================================================
    # Markets & Symbols
    # =========================================================================

    async def load_markets(self) -> Dict[str, SymbolInfo]:
        """
        Load all markets from exchange.

        :return: Markets dictionary
        """
        try:
            self.logger.info("Loading Binance USDT-M Futures markets...")

            client = await self._get_client()
            exchange_info = await client.futures_exchange_info()

            self._markets_cache.clear()

            for symbol_info in exchange_info.get("symbols", []):
                # Only perpetual contracts with TRADING status
                if (
                    symbol_info.get("contractType") != "PERPETUAL"
                    or symbol_info.get("status") != "TRADING"
                ):
                    continue

                # Only USDT pairs
                if symbol_info.get("quoteAsset") != self.quote_currency:
                    continue

                # Parse filters
                tick_size = Decimal("0.01")
                step_size = Decimal("0.001")
                min_qty = Decimal("0.001")
                max_qty = Decimal("1000000")
                min_notional = Decimal("5")

                for f in symbol_info.get("filters", []):
                    if f["filterType"] == "PRICE_FILTER":
                        tick_size = Decimal(str(f.get("tickSize", "0.01")))
                    elif f["filterType"] == "LOT_SIZE":
                        step_size = Decimal(str(f.get("stepSize", "0.001")))
                        min_qty = Decimal(str(f.get("minQty", "0.001")))
                        max_qty = Decimal(str(f.get("maxQty", "1000000")))
                    elif f["filterType"] == "MIN_NOTIONAL":
                        min_notional = Decimal(str(f.get("notional", "5")))

                binance_symbol = symbol_info["symbol"]
                standard_symbol = self._symbol_from_binance(binance_symbol)

                self._markets_cache[standard_symbol] = SymbolInfo(
                    symbol=standard_symbol,
                    base_asset=symbol_info["baseAsset"],
                    quote_asset=symbol_info["quoteAsset"],
                    price_precision=symbol_info.get("pricePrecision", 2),
                    quantity_precision=symbol_info.get("quantityPrecision", 3),
                    tick_size=tick_size,
                    step_size=step_size,
                    min_qty=min_qty,
                    max_qty=max_qty,
                    min_notional=min_notional,
                )

            self.logger.info(f"Loaded {len(self._markets_cache)} markets")
            return self._markets_cache

        except Exception as e:
            self.logger.error(f"Failed to load markets: {e}")
            raise

    async def get_tradable_symbols(
        self, exclude_leveraged: bool = True, min_volume_usdt: float = 0
    ) -> List[str]:
        """
        Get list of tradable futures symbols.

        :param exclude_leveraged: Exclude leveraged tokens
        :param min_volume_usdt: Minimum 24h volume in USDT (e.g., 10_000_000 for 10M)
        :return: List of symbols
        """
        await self._ensure_connected()

        leveraged_tokens = ["UP", "DOWN", "BULL", "BEAR", "3L", "3S", "2L", "2S"]

        # Get 24h volume for all symbols in one request if filtering by volume
        volume_by_symbol: Dict[str, float] = {}
        if min_volume_usdt > 0:
            try:
                client = await self._get_client()
                tickers = await client.futures_ticker()

                for ticker in tickers:
                    binance_symbol = ticker.get("symbol", "")
                    quote_volume = float(ticker.get("quoteVolume", 0))
                    standard_symbol = self._symbol_from_binance(binance_symbol)
                    volume_by_symbol[standard_symbol] = quote_volume

                self.logger.debug(
                    f"Loaded 24h volume for {len(volume_by_symbol)} symbols"
                )
            except Exception as e:
                self.logger.error(f"Failed to load 24h volumes: {e}")
                # If failed, don't filter by volume
                min_volume_usdt = 0

        symbols = []
        for symbol, info in self._markets_cache.items():
            # Exclude leveraged tokens
            if exclude_leveraged:
                if any(token in info.base_asset for token in leveraged_tokens):
                    continue

            # Filter by volume
            if min_volume_usdt > 0:
                volume = volume_by_symbol.get(symbol, 0)
                if volume < min_volume_usdt:
                    continue

            symbols.append(symbol)

        if min_volume_usdt > 0:
            self.logger.info(
                f"Filtered to {len(symbols)} symbols with 24h volume >= "
                f"${min_volume_usdt:,.0f}"
            )

        return sorted(symbols)

    async def get_symbol_info(self, symbol: str) -> Optional[SymbolInfo]:
        """Get symbol info."""
        await self._ensure_connected()
        return self._markets_cache.get(symbol)

    def _symbol_to_binance(self, symbol: str) -> str:
        """
        Convert our symbol format to Binance format.
        BTC/USDT:USDT -> BTCUSDT
        """
        return symbol.replace("/", "").replace(":USDT", "")

    def _symbol_from_binance(self, binance_symbol: str) -> str:
        """
        Convert Binance symbol format to our format.
        BTCUSDT -> BTC/USDT:USDT
        """
        if not binance_symbol.endswith("USDT"):
            return binance_symbol

        base = binance_symbol[:-4]
        return f"{base}/USDT:USDT"

    # =========================================================================
    # OHLCV Data
    # =========================================================================

    async def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        start_date: datetime,
        end_date: datetime,
        limit: int = 1000,
        max_retries: int = 3,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV data with pagination, rate limiting, and retry logic.

        :param symbol: Trading symbol
        :param interval: Timeframe (e.g., '1h', '4h', '1d')
        :param start_date: Start date
        :param end_date: End date
        :param limit: Limit per request
        :param max_retries: Maximum retry attempts for transient failures
        :return: DataFrame with OHLCV data
        """
        binance_symbol = self._symbol_to_binance(symbol)

        # Ensure dates are UTC for timestamp conversion
        start_date_utc = (
            start_date if start_date.tzinfo else start_date.replace(tzinfo=timezone.utc)
        )
        end_date_utc = (
            end_date if end_date.tzinfo else end_date.replace(tzinfo=timezone.utc)
        )

        # Cap limit at 1500 (Binance Futures max) to avoid API errors
        limit = min(limit, 1500)

        all_data: List[Any] = []
        current_since = int(start_date_utc.timestamp() * 1000)
        end_timestamp = int((end_date_utc + timedelta(days=1)).timestamp() * 1000)
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        while current_since < end_timestamp:
            # Retry loop for transient failures
            candles = None
            last_error: Optional[Exception] = None

            for attempt in range(max_retries):
                try:
                    # Rate limiting - wait for token before making request
                    # OHLCV requests have weight of 5
                    await self._rate_limiter.acquire(weight=5)
                    client = await self._get_client()

                    candles = await client.futures_klines(
                        symbol=binance_symbol,
                        interval=interval,
                        startTime=current_since,
                        endTime=end_timestamp,
                        limit=limit,
                    )
                    break  # Success, exit retry loop

                except asyncio.CancelledError:
                    wait_time = (attempt + 1) * 2
                    self.logger.warning(
                        f"Request cancelled for {symbol} (stale session), "
                        f"attempt {attempt + 1}/{max_retries}, retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                    continue

                except (asyncio.TimeoutError, TimeoutError) as e:
                    last_error = e
                    wait_time = (attempt + 1) * 2  # Exponential backoff: 2, 4, 6 sec
                    self.logger.warning(
                        f"Timeout fetching OHLCV for {symbol}, "
                        f"attempt {attempt + 1}/{max_retries}, "
                        f"retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                    await self._invalidate_client(reason=f"timeout while fetching {symbol}")

                except BinanceAPIException as e:
                    # Rate limit error - wait longer
                    if e.code == -1015:  # Too many requests
                        wait_time = 30
                        self.logger.warning(
                            f"Rate limit hit for {symbol}, waiting {wait_time}s..."
                        )
                        await asyncio.sleep(wait_time)
                        last_error = e
                    else:
                        # Other API errors - don't retry
                        self.logger.error(f"API error fetching OHLCV for {symbol}: {e}")
                        return self._ohlcv_to_dataframe(all_data)

                except Exception as e:
                    last_error = e
                    if self._is_transient_network_error(e):
                        wait_time = min(20.0, (2 ** attempt) + random.uniform(0.2, 1.0))
                        self.logger.warning(
                            f"Transient network error fetching OHLCV for {symbol}: {repr(e)} | "
                            f"attempt {attempt + 1}/{max_retries}, retrying in {wait_time:.1f}s"
                        )
                        await self._invalidate_client(
                            reason=f"transient network error while fetching {symbol}"
                        )
                        await asyncio.sleep(wait_time)
                    else:
                        self.logger.warning(
                            f"Error fetching OHLCV for {symbol}: {repr(e)}, "
                            f"attempt {attempt + 1}/{max_retries}"
                        )
                        await asyncio.sleep((attempt + 1) * 2)

            # Check if all retries failed
            if candles is None:
                self.logger.error(
                    f"Failed to fetch OHLCV for {symbol} after {max_retries} attempts: "
                    f"{repr(last_error)}"
                )
                break

            if not candles:
                break

            # Filter data:
            # 1. Only candles that started before end_timestamp
            # 2. Only CLOSED candles (close_time <= now)
            #    k[6] is close_time in ms - candle is closed when close_time is in the past
            filtered = [
                k for k in candles
                if k[0] < end_timestamp and k[6] <= now_ms
            ]
            all_data.extend(filtered)

            if len(candles) < limit or not filtered:
                break

            current_since = candles[-1][0] + 1

        return self._ohlcv_to_dataframe(all_data)

    def _ohlcv_to_dataframe(self, ohlcv_data: list) -> pd.DataFrame:
        """
        Convert OHLCV list to DataFrame.

        All timestamps are normalized to UTC timezone.
        """
        if not ohlcv_data:
            return pd.DataFrame()

        df = pd.DataFrame(
            ohlcv_data,
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "trades",
                "taker_buy_base",
                "taker_buy_quote",
                "ignore",
            ],
        )

        # Convert timestamp to datetime with UTC timezone
        # Binance timestamps are in milliseconds and represent UTC time
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df[["timestamp", "open", "high", "low", "close", "volume"]]
        df.set_index("timestamp", inplace=True)

        # Convert to float
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        return df

    # =========================================================================
    # Market Data
    # =========================================================================

    async def get_market_data(self, symbol: str) -> MarketData:
        """Get current market data (bid/ask/last)."""
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)

        try:
            ticker = await client.futures_orderbook_ticker(symbol=binance_symbol)

            return MarketData(
                symbol=symbol,
                bid=float(ticker["bidPrice"]),
                ask=float(ticker["askPrice"]),
                last=float(ticker["bidPrice"]),  # Use bid as last
                timestamp=int(
                    ticker.get("time", asyncio.get_event_loop().time() * 1000)
                ),
            )
        except Exception as e:
            self.logger.error(f"Failed to get market data for {symbol}: {e}")
            raise

    async def get_current_price(self, symbol: str) -> Decimal:
        """Get current price."""
        market = await self.get_market_data(symbol)
        return Decimal(str(market.mid_price))

    async def get_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        """Get order book."""
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)

        try:
            depth = await client.futures_order_book(symbol=binance_symbol, limit=limit)

            return {
                "symbol": symbol,
                "bids": [[float(b[0]), float(b[1])] for b in depth["bids"]],
                "asks": [[float(a[0]), float(a[1])] for a in depth["asks"]],
                "timestamp": int(asyncio.get_event_loop().time() * 1000),
            }
        except Exception as e:
            self.logger.error(f"Failed to get order book for {symbol}: {e}")
            raise

    # =========================================================================
    # Funding Rates
    # =========================================================================

    def get_funding_interval_hours(self, symbol: str) -> int:
        """
        Get funding interval in hours for a symbol.

        :param symbol: Trading symbol
        :return: Funding interval in hours
        """
        binance_symbol = self._symbol_to_binance(symbol)

        # Check cache first
        if binance_symbol in self._funding_intervals:
            return self._funding_intervals[binance_symbol]

        # Default to 8 hours for most symbols
        self._funding_intervals[binance_symbol] = 8
        return 8

    async def get_current_funding_rate(self, symbol: str) -> Optional[FundingRateData]:
        """
        Get current funding rate for a symbol.

        :param symbol: Trading symbol
        :return: FundingRateData or None
        """
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)

        try:
            mark_price = await client.futures_mark_price(symbol=binance_symbol)

            if mark_price:
                interval = self.get_funding_interval_hours(symbol)

                return FundingRateData(
                    symbol=symbol,
                    funding_rate=float(mark_price.get("lastFundingRate", 0)),
                    funding_time=datetime.fromtimestamp(
                        int(mark_price.get("nextFundingTime", 0)) / 1000,
                        tz=timezone.utc,
                    ),
                    funding_interval_hours=interval,
                )

            return None

        except Exception as e:
            self.logger.error(f"Failed to get funding rate for {symbol}: {e}")
            return None

    async def get_funding_rates_batch(
        self, symbols: Optional[List[str]] = None
    ) -> List[FundingRateData]:
        """
        Get current funding rates for multiple symbols.

        :param symbols: List of symbols (None for all)
        :return: List of FundingRateData
        """
        client = await self._get_client()

        try:
            mark_prices = await client.futures_mark_price()

            results = []
            for item in mark_prices:
                binance_symbol = item["symbol"]
                standard_symbol = self._symbol_from_binance(binance_symbol)

                # Filter by symbols if provided
                if symbols and standard_symbol not in symbols:
                    continue

                # Only USDT pairs
                if not binance_symbol.endswith("USDT"):
                    continue

                funding_rate = item.get("lastFundingRate")
                if funding_rate is None or funding_rate == "":
                    continue

                interval = self.get_funding_interval_hours(standard_symbol)

                results.append(
                    FundingRateData(
                        symbol=standard_symbol,
                        funding_rate=float(funding_rate),
                        funding_time=datetime.fromtimestamp(
                            int(item.get("nextFundingTime", 0)) / 1000, tz=timezone.utc
                        ),
                        funding_interval_hours=interval,
                    )
                )

            return results

        except Exception as e:
            self.logger.error(f"Failed to get funding rates batch: {e}")
            return []

    async def get_historical_funding_rates(
        self,
        symbol: str,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        limit: int = 1000,
    ) -> List[FundingRateData]:
        """
        Get historical funding rates for a symbol.

        :param symbol: Trading symbol
        :param start_date: Start date
        :param end_date: End date (default: now)
        :param limit: Limit per request
        :return: List of FundingRateData
        """
        if end_date is None:
            end_date = datetime.now(timezone.utc)

        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)

        try:
            # Ensure dates are UTC for timestamp conversion
            start_date_utc = (
                start_date
                if start_date.tzinfo
                else start_date.replace(tzinfo=timezone.utc)
            )
            end_date_utc = (
                end_date if end_date.tzinfo else end_date.replace(tzinfo=timezone.utc)
            )

            all_rates = []
            current_since = int(start_date_utc.timestamp() * 1000)
            end_ts = int(end_date_utc.timestamp() * 1000)

            while current_since < end_ts:
                funding_data = await client.futures_funding_rate(
                    symbol=binance_symbol,
                    startTime=current_since,
                    endTime=end_ts,
                    limit=limit,
                )

                if not funding_data:
                    break

                interval = self.get_funding_interval_hours(symbol)

                for item in funding_data:
                    all_rates.append(
                        FundingRateData(
                            symbol=symbol,
                            funding_rate=float(item["fundingRate"]),
                            funding_time=datetime.fromtimestamp(
                                int(item["fundingTime"]) / 1000, tz=timezone.utc
                            ),
                            funding_interval_hours=interval,
                        )
                    )

                if len(funding_data) < limit:
                    break

                current_since = int(funding_data[-1]["fundingTime"]) + 1
                await asyncio.sleep(0.1)

            return all_rates

        except Exception as e:
            self.logger.error(
                f"Failed to get historical funding rates for {symbol}: {e}"
            )
            return []

    async def get_average_funding_rate(
        self, symbol: str, days: int = 30
    ) -> Optional[float]:
        """
        Get average funding rate over a period.

        :param symbol: Trading symbol
        :param days: Number of days
        :return: Average funding rate
        """
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days)

        rates = await self.get_historical_funding_rates(symbol, start_date, end_date)

        if not rates:
            return None

        return sum(r.funding_rate for r in rates) / len(rates)

    # =========================================================================
    # Precision & Calculations
    # =========================================================================

    async def amount_to_precision(self, symbol: str, amount: Decimal) -> Decimal:
        """
        Round amount to exchange precision (step size).

        :param symbol: Trading symbol
        :param amount: Amount to round
        :return: Rounded amount
        """
        info = await self.get_symbol_info(symbol)
        if not info:
            return amount.quantize(Decimal("0.001"), rounding=ROUND_DOWN)

        # Round to step size
        step = info.step_size
        result = (amount / step).to_integral_value(rounding=ROUND_DOWN) * step

        # Ensure minimum quantity
        if result < info.min_qty:
            result = info.min_qty

        return result

    async def price_to_precision(self, symbol: str, price: Decimal) -> Decimal:
        """
        Round price to exchange precision (tick size).

        :param symbol: Trading symbol
        :param price: Price to round
        :return: Rounded price
        """
        info = await self.get_symbol_info(symbol)
        if not info:
            return price.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        # Round to tick size
        tick = info.tick_size
        result = (price / tick).to_integral_value(rounding=ROUND_DOWN) * tick

        return result

    async def calculate_amount_from_usdt(
        self, symbol: str, usdt_amount: float
    ) -> Decimal:
        """
        Calculate base asset amount from USDT amount.

        :param symbol: Trading symbol
        :param usdt_amount: Amount in USDT
        :return: Amount in base asset
        """
        price = await self.get_current_price(symbol)
        amount = Decimal(str(usdt_amount)) / price
        return await self.amount_to_precision(symbol, amount)

    async def calculate_usdt_from_amount(self, symbol: str, amount: float) -> Decimal:
        """
        Calculate USDT value from base asset amount.

        :param symbol: Trading symbol
        :param amount: Amount in base asset
        :return: Value in USDT
        """
        price = await self.get_current_price(symbol)
        return Decimal(str(amount)) * price

    def calculate_coins_from_contracts(
        self, symbol: str, contracts: Decimal
    ) -> Decimal:
        """
        Convert contracts to coins (base asset).
        For Binance USDT-M futures, contract size is 1, so contracts = coins.
        """
        return contracts

    async def calculate_contracts_from_coins(
        self, symbol: str, coins: Decimal
    ) -> Decimal:
        """
        Convert coins (base asset) to contracts with precision.
        For Binance USDT-M futures, contract size is 1, so coins = contracts.
        """
        return await self.amount_to_precision(symbol, coins)

    async def get_min_order_size(self, symbol: str) -> Decimal:
        """Get minimum order size for symbol."""
        info = await self.get_symbol_info(symbol)
        if not info:
            return Decimal("0.001")
        return info.min_qty

    async def get_min_notional(self, symbol: str) -> Decimal:
        """Get minimum notional value for symbol."""
        info = await self.get_symbol_info(symbol)
        if not info:
            return Decimal("5")
        return info.min_notional

    # =========================================================================
    # Account & Positions
    # =========================================================================

    async def get_balances(self) -> List[Balance]:
        """Get account balances."""
        await self._ensure_connected()
        client = await self._get_client()

        try:
            account = await client.futures_account()

            balances = []
            for asset in account.get("assets", []):
                balance = Balance(
                    asset=asset["asset"],
                    free=float(asset.get("availableBalance", 0)),
                    used=float(asset.get("initialMargin", 0)),
                    total=float(asset.get("walletBalance", 0)),
                )
                balances.append(balance)

            return balances

        except Exception as e:
            self.logger.error(f"Failed to get balances: {e}")
            raise

    async def get_balance(self, asset: str = "USDT") -> Balance:
        """Get balance for specific asset."""
        balances = await self.get_balances()
        for balance in balances:
            if balance.asset == asset:
                return balance

        return Balance(asset=asset, free=0, used=0, total=0)

    # =========================================================================
    # Position Mode (Hedge Mode)
    # =========================================================================

    async def get_position_mode(self) -> bool:
        """
        Get current position mode.

        Returns:
            True if Hedge Mode (dual side position) is enabled,
            False if One-way Mode.
        """
        await self._ensure_connected()
        client = await self._get_client()

        try:
            response = await client.futures_get_position_mode()
            # Response: {"dualSidePosition": true/false}
            return response.get("dualSidePosition", False)

        except Exception as e:
            self.logger.error(f"Failed to get position mode: {e}")
            raise

    async def set_position_mode(self, hedge_mode: bool = True) -> bool:
        """
        Set position mode (Hedge Mode or One-way Mode).

        Args:
            hedge_mode: True for Hedge Mode (dual side position),
                       False for One-way Mode.

        Returns:
            True if mode was changed or already set correctly.
        """
        await self._ensure_connected()
        client = await self._get_client()

        try:
            # Check current mode first
            current_mode = await self.get_position_mode()

            if current_mode == hedge_mode:
                mode_name = "Hedge Mode" if hedge_mode else "One-way Mode"
                self.logger.info(f"Position mode already set to {mode_name}")
                return True

            # Change mode
            # dualSidePosition: "true" for Hedge Mode, "false" for One-way Mode
            response = await client.futures_change_position_mode(
                dualSidePosition=str(hedge_mode).lower()
            )

            # Response: {"code": 200, "msg": "success"}
            if response.get("code") == 200 or response.get("msg") == "success":
                mode_name = "Hedge Mode" if hedge_mode else "One-way Mode"
                self.logger.info(f"Position mode changed to {mode_name}")
                return True

            self.logger.error(f"Unexpected response from position mode change: {response}")
            return False

        except Exception as e:
            error_msg = str(e)
            # Error -4059: No need to change position side (already set)
            if "-4059" in error_msg:
                mode_name = "Hedge Mode" if hedge_mode else "One-way Mode"
                self.logger.info(f"Position mode already set to {mode_name}")
                return True

            self.logger.error(f"Failed to set position mode: {e}")
            raise

    async def get_positions(
        self, symbol: Optional[str] = None, skip_zero: bool = True
    ) -> List[Position]:
        """
        Get open positions.

        In Hedge Mode, returns separate LONG and SHORT positions for each symbol.
        """
        await self._ensure_connected()
        client = await self._get_client()

        try:
            account = await client.futures_account()

            positions = []
            for pos in account.get("positions", []):
                position_amt = float(pos.get("positionAmt", 0))

                if skip_zero and position_amt == 0:
                    continue

                binance_symbol = pos["symbol"]
                standard_symbol = self._symbol_from_binance(binance_symbol)

                if symbol and standard_symbol != symbol:
                    continue

                # In Hedge Mode, positionSide indicates LONG/SHORT
                # In One-way Mode, positionSide is BOTH
                position_side = pos.get("positionSide", "BOTH")

                if position_side == "BOTH":
                    # One-way mode: determine side from position amount
                    side = "long" if position_amt > 0 else "short"
                else:
                    # Hedge mode: use positionSide
                    side = position_side.lower()

                positions.append(
                    Position(
                        symbol=standard_symbol,
                        side=side,
                        size=abs(position_amt),
                        contracts=abs(position_amt),
                        entry_price=float(pos.get("entryPrice", 0)),
                        mark_price=float(pos.get("markPrice", 0)),
                        unrealized_pnl=float(pos.get("unrealizedProfit", 0)),
                        leverage=int(pos.get("leverage", 1)),
                        margin_type=(
                            "isolated" if pos.get("isolated", False) else "cross"
                        ),
                        liquidation_price=float(pos.get("liquidationPrice", 0)),
                    )
                )

            return positions

        except Exception as e:
            self.logger.error(f"Failed to get positions: {e}")
            raise

    async def get_position(
        self, symbol: str, position_side: Optional[str] = None
    ) -> Optional[Position]:
        """
        Get position for specific symbol.

        Args:
            symbol: Trading symbol.
            position_side: 'long' or 'short' for Hedge Mode.
                          If None, returns first non-zero position found.
        """
        positions = await self.get_positions(symbol=symbol, skip_zero=False)

        if not positions:
            return None

        if position_side:
            # Find specific side
            for pos in positions:
                if pos.side == position_side.lower() and pos.contracts > 0:
                    return pos
            return None

        # Return first non-zero position
        for pos in positions:
            if pos.contracts > 0:
                return pos

        return positions[0] if positions else None

    # =========================================================================
    # Trading Operations
    # =========================================================================

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        """Set leverage for symbol."""
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)

        try:
            await client.futures_change_leverage(
                symbol=binance_symbol, leverage=leverage
            )
            self.logger.info(f"Set leverage to {leverage}x for {symbol}")

        except BinanceAPIException as e:
            if e.code == -4028:
                self.logger.debug(f"Leverage already set to {leverage}x for {symbol}")
            else:
                raise

    async def set_margin_type(self, symbol: str, margin_type: str) -> None:
        """Set margin type for symbol."""
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)
        binance_margin = (
            MarginType.ISOLATED.value
            if margin_type == "isolated"
            else MarginType.CROSS.value
        )

        try:
            await client.futures_change_margin_type(
                symbol=binance_symbol, marginType=binance_margin
            )
            self.logger.info(f"Set margin type to {margin_type} for {symbol}")

        except BinanceAPIException as e:
            if e.code == -4046:
                self.logger.debug(
                    f"Margin type already set to {margin_type} for {symbol}"
                )
            else:
                raise

    async def open_position(
        self,
        symbol: str,
        side: TradeSide,
        amount: float,
        leverage: Optional[int] = None,
        position_side: Optional[Literal["LONG", "SHORT", "BOTH"]] = None,
    ) -> Order:
        """
        Open a position.

        Args:
            symbol: Trading symbol.
            side: Order side ('buy' or 'sell').
            amount: Amount in contracts.
            leverage: Leverage to use (optional).
            position_side: Position side for Hedge Mode.
                          - 'LONG': Open/add to long position (side must be 'buy')
                          - 'SHORT': Open/add to short position (side must be 'sell')
                          - 'BOTH': One-way mode (default for backward compatibility)
                          If None, auto-determines based on side for Hedge Mode.
        """
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)

        try:
            lev = leverage or self.default_leverage
            await self.set_leverage(symbol, lev)
            await self.set_margin_type(symbol, self.default_margin_type)

            precise_amount = await self.amount_to_precision(
                symbol, Decimal(str(amount))
            )

            # Determine position_side for Hedge Mode
            if position_side is None:
                # Auto-determine: BUY opens LONG, SELL opens SHORT
                side_upper = side.upper() if isinstance(side, str) else side.value
                position_side = "LONG" if side_upper == "BUY" else "SHORT"

            self.logger.info(
                f"Opening {side} position for {symbol}: {precise_amount} "
                f"(positionSide={position_side})"
            )

            response = await client.futures_create_order(
                symbol=binance_symbol,
                side=side.upper(),
                type="MARKET",
                quantity=float(precise_amount),
                positionSide=position_side,
            )

            return self._map_order_response(symbol, response)

        except Exception as e:
            self.logger.error(f"Failed to open position for {symbol}: {e}")
            raise

    async def open_position_limit(
        self,
        symbol: str,
        side: TradeSide,
        amount: float,
        leverage: Optional[int] = None,
        max_retries: int = 5,
        retry_interval: float = 0.5,
        fallback_to_market: bool = True,
        max_slippage_percent: float = 0.1,
        position_side: Optional[Literal["LONG", "SHORT", "BOTH"]] = None,
    ) -> Order:
        """
        Open position with limit order, ensuring execution (like market but cheaper).

        Uses aggressive limit pricing to get maker fees while guaranteeing fill.
        For stat-arb where every basis point matters.

        Strategy:
        1. Place limit order at best bid/ask (aggressive, should fill immediately)
        2. If not filled, chase the price with updated limit orders
        3. After max_retries, optionally fallback to market order

        :param symbol: Trading symbol
        :param side: Order side (buy/sell)
        :param amount: Amount to trade
        :param leverage: Leverage (optional, uses default)
        :param max_retries: Maximum retry attempts for price chasing
        :param retry_interval: Seconds between retries
        :param fallback_to_market: If True, use market order after retries exhausted
        :param max_slippage_percent: Max allowed price movement before fallback (0.1 = 0.1%)
        :param position_side: Position side for Hedge Mode ('LONG', 'SHORT', or 'BOTH')
        :return: Filled Order
        """
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)
        side_upper = side.upper() if isinstance(side, str) else side.value

        try:
            # Setup leverage and margin
            lev = leverage or self.default_leverage
            await self.set_leverage(symbol, lev)
            await self.set_margin_type(symbol, self.default_margin_type)

            precise_amount = await self.amount_to_precision(
                symbol, Decimal(str(amount))
            )
            remaining_amount = float(precise_amount)

            # Determine position_side for Hedge Mode
            if position_side is None:
                position_side = "LONG" if side_upper == "BUY" else "SHORT"

            # Get initial market data for price reference
            market_data = await self.get_market_data(symbol)
            initial_price = market_data.ask if side_upper == "BUY" else market_data.bid

            self.logger.info(
                f"Opening {side_upper} position for {symbol}: {precise_amount} "
                f"(limit, initial_price={initial_price:.6f}, positionSide={position_side})"
            )

            current_order_id: Optional[str] = None

            for attempt in range(max_retries + 1):
                # Get current best price
                market_data = await self.get_market_data(symbol)

                if side_upper == "BUY":
                    # For BUY: use ask price to ensure immediate fill
                    # Adding tiny premium to cross the spread
                    limit_price = market_data.ask
                else:
                    # For SELL: use bid price to ensure immediate fill
                    limit_price = market_data.bid

                # Check slippage
                price_change_pct = (
                    abs(limit_price - initial_price) / initial_price * 100
                )
                if price_change_pct > max_slippage_percent:
                    self.logger.warning(
                        f"Price moved {price_change_pct:.3f}% (max {max_slippage_percent}%), "
                        f"{'falling back to market' if fallback_to_market else 'aborting'}"
                    )
                    if fallback_to_market:
                        return await self.open_position(
                            symbol,
                            side,
                            remaining_amount,
                            leverage,
                            position_side,
                        )
                    raise ValueError(f"Price slippage exceeded {max_slippage_percent}%")

                precise_price = await self.price_to_precision(
                    symbol, Decimal(str(limit_price))
                )

                # Cancel previous order if exists
                if current_order_id:
                    try:
                        await self.cancel_order(symbol, current_order_id)
                        self.logger.debug(f"Cancelled stale order {current_order_id}")
                    except Exception:
                        pass  # Order might already be filled or cancelled

                # Place limit order with IOC (Immediate-Or-Cancel) for fast execution
                # Or use regular LIMIT for potential maker rebate
                try:
                    response = await client.futures_create_order(
                        symbol=binance_symbol,
                        side=side_upper,
                        type="LIMIT",
                        quantity=float(precise_amount),
                        price=float(precise_price),
                        positionSide=position_side,
                        timeInForce="GTC",  # Good-Till-Cancel
                    )

                    order = self._map_order_response(symbol, response)
                    current_order_id = order.id

                    self.logger.debug(
                        f"Placed limit order {order.id} at {precise_price} "
                        f"(attempt {attempt + 1}/{max_retries + 1})"
                    )

                    # Wait and check if filled
                    await asyncio.sleep(retry_interval)

                    # Check order status
                    updated_order = await self.get_order(symbol, order.id)

                    if updated_order.status == "closed":
                        self.logger.info(
                            f"Limit order filled for {symbol}: "
                            f"{precise_amount} @ {updated_order.price}"
                        )
                        return updated_order

                    if updated_order.filled > 0:
                        # Partially filled - calculate remaining
                        remaining = float(precise_amount) - updated_order.filled
                        if remaining <= 0:
                            return updated_order

                        self.logger.info(
                            f"Partial fill: {updated_order.filled}/{precise_amount}, "
                            f"continuing with {remaining}"
                        )
                        remaining_amount = remaining
                        precise_amount = await self.amount_to_precision(
                            symbol, Decimal(str(remaining))
                        )

                except BinanceAPIException as e:
                    self.logger.error(f"Order attempt failed: {e}")
                    if e.code == -2021:  # Order would immediately match and take
                        # This is actually fine for our purpose
                        pass
                    else:
                        raise

            # Exhausted retries - cancel any pending order and fallback
            if current_order_id:
                try:
                    await self.cancel_order(symbol, current_order_id)
                except Exception:
                    pass

            if fallback_to_market:
                self.logger.warning(
                    f"Limit order not filled after {max_retries + 1} attempts, "
                    f"falling back to market order"
                )
                return await self.open_position(
                    symbol,
                    side,
                    remaining_amount,
                    leverage,
                    position_side,
                )

            raise TimeoutError(
                f"Limit order for {symbol} not filled after {max_retries + 1} attempts"
            )

        except Exception as e:
            # Cancel any pending order on error
            if current_order_id:
                try:
                    await self.cancel_order(symbol, current_order_id)
                except Exception:
                    pass
            self.logger.error(f"Failed to open limit position for {symbol}: {e}")
            raise

    async def close_position(
        self,
        symbol: str,
        side: TradeSide,
        amount: float,
        position_side: Optional[Literal["LONG", "SHORT", "BOTH"]] = None,
    ) -> Order:
        """
        Close a position.

        Args:
            symbol: Trading symbol.
            side: Order side ('buy' to close SHORT, 'sell' to close LONG).
            amount: Amount in contracts to close.
            position_side: Position side for Hedge Mode.
                          - 'LONG': Close long position (side must be 'sell')
                          - 'SHORT': Close short position (side must be 'buy')
                          - 'BOTH': One-way mode
                          If None, auto-determines based on side.
        """
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)
        side_upper = side.upper() if isinstance(side, str) else side.value

        try:
            precise_amount = await self.amount_to_precision(
                symbol, Decimal(str(amount))
            )

            # Determine position_side for Hedge Mode
            # To close LONG, we SELL with positionSide=LONG
            # To close SHORT, we BUY with positionSide=SHORT
            if position_side is None:
                position_side = "LONG" if side_upper == "SELL" else "SHORT"

            self.logger.info(
                f"Closing position for {symbol}: {side_upper} {precise_amount} "
                f"(positionSide={position_side})"
            )

            response = await client.futures_create_order(
                symbol=binance_symbol,
                side=side_upper,
                type="MARKET",
                quantity=float(precise_amount),
                positionSide=position_side,
            )

            return self._map_order_response(symbol, response)

        except Exception as e:
            self.logger.error(f"Failed to close position for {symbol}: {e}")
            raise

    async def close_position_limit(
        self,
        symbol: str,
        side: TradeSide,
        amount: float,
        max_retries: int = 5,
        retry_interval: float = 0.5,
        fallback_to_market: bool = True,
        max_slippage_percent: float = 0.1,
        position_side: Optional[Literal["LONG", "SHORT", "BOTH"]] = None,
    ) -> Order:
        """
        Close position with limit order, ensuring execution (like market but cheaper).

        Uses aggressive limit pricing to get maker fees while guaranteeing fill.
        For stat-arb where every basis point matters.

        :param symbol: Trading symbol
        :param side: Order side (buy/sell) - the side that CLOSES the position
        :param amount: Amount to close
        :param max_retries: Maximum retry attempts for price chasing
        :param retry_interval: Seconds between retries
        :param fallback_to_market: If True, use market order after retries exhausted
        :param max_slippage_percent: Max allowed price movement before fallback
        :param position_side: Position side for Hedge Mode ('LONG', 'SHORT', or 'BOTH')
        :return: Filled Order
        """
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)
        side_upper = side.upper() if isinstance(side, str) else side.value

        try:
            precise_amount = await self.amount_to_precision(
                symbol, Decimal(str(amount))
            )
            remaining_amount = float(precise_amount)

            # Determine position_side for Hedge Mode
            if position_side is None:
                position_side = "LONG" if side_upper == "SELL" else "SHORT"

            # Get initial market data for price reference
            market_data = await self.get_market_data(symbol)
            initial_price = market_data.ask if side_upper == "BUY" else market_data.bid

            self.logger.info(
                f"Closing position for {symbol}: {side_upper} {precise_amount} "
                f"(limit, initial_price={initial_price:.6f}, positionSide={position_side})"
            )

            current_order_id: Optional[str] = None

            for attempt in range(max_retries + 1):
                # Get current best price
                market_data = await self.get_market_data(symbol)

                if side_upper == "BUY":
                    limit_price = market_data.ask
                else:
                    limit_price = market_data.bid

                # Check slippage
                price_change_pct = (
                    abs(limit_price - initial_price) / initial_price * 100
                )
                if price_change_pct > max_slippage_percent:
                    self.logger.warning(
                        f"Price moved {price_change_pct:.3f}% (max {max_slippage_percent}%), "
                        f"{'falling back to market' if fallback_to_market else 'aborting'}"
                    )
                    if fallback_to_market:
                        return await self.close_position(
                            symbol,
                            side,
                            remaining_amount,
                            position_side,
                        )
                    raise ValueError(f"Price slippage exceeded {max_slippage_percent}%")

                precise_price = await self.price_to_precision(
                    symbol, Decimal(str(limit_price))
                )

                # Cancel previous order if exists
                if current_order_id:
                    try:
                        await self.cancel_order(symbol, current_order_id)
                    except Exception:
                        pass

                try:
                    response = await client.futures_create_order(
                        symbol=binance_symbol,
                        side=side_upper,
                        type="LIMIT",
                        quantity=float(precise_amount),
                        price=float(precise_price),
                        positionSide=position_side,
                        timeInForce="GTC",
                    )

                    order = self._map_order_response(symbol, response)
                    current_order_id = order.id

                    self.logger.debug(
                        f"Placed close limit order {order.id} at {precise_price} "
                        f"(attempt {attempt + 1}/{max_retries + 1})"
                    )

                    await asyncio.sleep(retry_interval)

                    updated_order = await self.get_order(symbol, order.id)

                    if updated_order.status == "closed":
                        self.logger.info(
                            f"Close limit order filled for {symbol}: "
                            f"{precise_amount} @ {updated_order.price}"
                        )
                        return updated_order

                    if updated_order.filled > 0:
                        remaining = float(precise_amount) - updated_order.filled
                        if remaining <= 0:
                            return updated_order
                        remaining_amount = remaining
                        precise_amount = await self.amount_to_precision(
                            symbol, Decimal(str(remaining))
                        )

                except BinanceAPIException as e:
                    self.logger.error(f"Close order attempt failed: {e}")
                    if e.code != -2021:
                        raise

            # Exhausted retries
            if current_order_id:
                try:
                    await self.cancel_order(symbol, current_order_id)
                except Exception:
                    pass

            if fallback_to_market:
                self.logger.warning(
                    f"Close limit order not filled after {max_retries + 1} attempts, "
                    f"falling back to market order"
                )
                return await self.close_position(
                    symbol, side, remaining_amount, position_side
                )

            raise TimeoutError(
                f"Close limit order for {symbol} not filled after {max_retries + 1} attempts"
            )

        except Exception as e:
            if current_order_id:
                try:
                    await self.cancel_order(symbol, current_order_id)
                except Exception:
                    pass
            self.logger.error(f"Failed to close limit position for {symbol}: {e}")
            raise

    async def flash_close_position(
        self,
        symbol: str,
        amount: Optional[float] = None,
        close_side: Optional[TradeSide] = None,
    ) -> Order:
        """
        Close position immediately.

        Args:
            symbol: Trading symbol.
            amount: Amount in contracts to close. If None, closes entire position.
            close_side: Order side for closing ('buy' or 'sell').
                        - To close a LONG position, use 'sell'
                        - To close a SHORT position, use 'buy'
                        If None, auto-detects from current position.

        Returns:
            Executed close order.
        """
        position = await self.get_position(symbol)

        if amount is None:
            # Close entire position
            if not position or position.contracts == 0:
                raise ValueError(f"No open position for {symbol}")

            determined_side: TradeSide = "sell" if position.side == "long" else "buy"
            # position_side is opposite: SELL closes LONG, BUY closes SHORT
            position_side: Literal["LONG", "SHORT"] = (
                "LONG" if position.side == "long" else "SHORT"
            )
            order = await self.close_position(
                symbol, determined_side, position.contracts, position_side
            )
            await self.cancel_all_orders(symbol)
        else:
            # Partial close
            if close_side is None:
                # Auto-detect from position
                if not position or position.contracts == 0:
                    raise ValueError(f"No open position for {symbol}")
                determined_side = "sell" if position.side == "long" else "buy"
                position_side = "LONG" if position.side == "long" else "SHORT"
            else:
                determined_side = close_side
                # Determine position_side from close_side
                # BUY closes SHORT, SELL closes LONG
                side_upper = (
                    close_side.upper()
                    if isinstance(close_side, str)
                    else close_side.value
                )
                position_side = "SHORT" if side_upper == "BUY" else "LONG"

            order = await self.close_position(
                symbol, determined_side, amount, position_side
            )
            # Don't cancel all orders on partial close

        return order

    async def close_all_positions(self) -> Dict[str, Any]:
        """
        Close all open positions immediately using market orders.

        Returns:
            Dictionary with results:
            {
                "total_positions": int,
                "closed_successfully": int,
                "failed": int,
                "results": List[Dict[str, Any]]
            }
        """
        await self._ensure_connected()

        try:
            # Get all open positions
            positions = await self.get_positions(skip_zero=True)

            if not positions:
                self.logger.info("No open positions to close")
                return {
                    "total_positions": 0,
                    "closed_successfully": 0,
                    "failed": 0,
                    "results": []
                }

            self.logger.info(f"Closing {len(positions)} open positions...")

            results = []
            closed_count = 0
            failed_count = 0

            # Close each position
            for position in positions:
                try:
                    self.logger.info(
                        f"Closing {position.symbol} {position.side.upper()} "
                        f"({position.contracts} contracts)"
                    )

                    # Use flash_close_position to close entire position
                    order = await self.flash_close_position(position.symbol)

                    results.append({
                        "symbol": position.symbol,
                        "side": position.side,
                        "contracts": position.contracts,
                        "status": "success",
                        "order_id": order.id,
                        "error": None
                    })
                    closed_count += 1

                    self.logger.info(
                        f"✅ Closed {position.symbol} successfully (order: {order.id})"
                    )

                except Exception as e:
                    error_msg = str(e)
                    results.append({
                        "symbol": position.symbol,
                        "side": position.side,
                        "contracts": position.contracts,
                        "status": "failed",
                        "order_id": None,
                        "error": error_msg
                    })
                    failed_count += 1

                    self.logger.error(
                        f"❌ Failed to close {position.symbol}: {error_msg}"
                    )

                # Small delay between closes to avoid rate limits
                await asyncio.sleep(0.1)

            # Summary
            self.logger.info(
                f"Close all positions completed: "
                f"{closed_count} success, {failed_count} failed"
            )

            return {
                "total_positions": len(positions),
                "closed_successfully": closed_count,
                "failed": failed_count,
                "results": results
            }

        except Exception as e:
            self.logger.error(f"Failed to close all positions: {e}")
            raise

    async def set_stop_loss(
        self,
        symbol: str,
        side: TradeSide,
        stop_price: float,
        cancel_existing: bool = True,
        position_side: Optional[Literal["LONG", "SHORT", "BOTH"]] = None,
    ) -> Order:
        """
        Set stop loss for position.

        :param symbol: Trading symbol
        :param side: Position side ('buy' for long, 'sell' for short) - determines close direction
        :param stop_price: Stop loss price
        :param cancel_existing: Cancel existing SL order first (default: True)
        :param position_side: Position side for Hedge Mode ('LONG', 'SHORT', or 'BOTH')
        """
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)
        side_lower = side.lower() if isinstance(side, str) else side.value.lower()
        close_side = "SELL" if side_lower == "buy" else "BUY"

        # Determine position_side for Hedge Mode
        if position_side is None:
            # buy position (long) -> positionSide=LONG
            # sell position (short) -> positionSide=SHORT
            position_side = "LONG" if side_lower == "buy" else "SHORT"

        try:
            # Cancel existing SL orders only
            if cancel_existing:
                orders = await self.get_open_orders(symbol)
                for order in orders:
                    # Cancel only entire position SL orders (both Market and Limit)
                    if order.type in ["stop_market", "stop"] and order.close_position:
                        await self.cancel_order(symbol, order.id)
                        self.logger.info(f"Cancelled existing SL order {order.id}")

            precise_price = float(
                await self.price_to_precision(symbol, Decimal(str(stop_price)))
            )

            self.logger.info(
                f"Setting stop loss for {symbol} at {precise_price} "
                f"(positionSide={position_side})"
            )

            response = await client.futures_create_order(
                symbol=binance_symbol,
                side=close_side,
                type="STOP_MARKET",
                stopPrice=precise_price,
                closePosition=True,
                positionSide=position_side,
                workingType="MARK_PRICE",
            )
            return self._map_order_response(symbol, response)

        except Exception as e:
            self.logger.error(f"Failed to set stop loss for {symbol}: {e}")
            raise

    async def set_take_profit(
        self,
        symbol: str,
        side: TradeSide,
        take_profit_price: float,
        cancel_existing: bool = True,
        position_side: Optional[Literal["LONG", "SHORT", "BOTH"]] = None,
    ) -> Order:
        """
        Set take profit for position.

        :param symbol: Trading symbol
        :param side: Position side ('buy' for long, 'sell' for short) - determines close direction
        :param take_profit_price: Take profit price
        :param cancel_existing: Cancel existing TP order first (default: True)
        :param position_side: Position side for Hedge Mode ('LONG', 'SHORT', or 'BOTH')
        """
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)
        side_lower = side.lower() if isinstance(side, str) else side.value.lower()
        close_side = "SELL" if side_lower == "buy" else "BUY"

        # Determine position_side for Hedge Mode
        if position_side is None:
            position_side = "LONG" if side_lower == "buy" else "SHORT"

        try:
            # Cancel existing TP orders only
            if cancel_existing:
                orders = await self.get_open_orders(symbol)
                for order in orders:
                    # Cancel only entire position TP orders (both Market and Limit)
                    if (
                        order.type in ["take_profit_market", "take_profit"]
                        and order.close_position
                    ):
                        await self.cancel_order(symbol, order.id)
                        self.logger.info(f"Cancelled existing TP order {order.id}")

            precise_price = float(
                await self.price_to_precision(symbol, Decimal(str(take_profit_price)))
            )

            self.logger.info(
                f"Setting take profit for {symbol} at {precise_price} "
                f"(positionSide={position_side})"
            )

            response = await client.futures_create_order(
                symbol=binance_symbol,
                side=close_side,
                type="TAKE_PROFIT_MARKET",
                stopPrice=precise_price,
                closePosition=True,
                positionSide=position_side,
                workingType="MARK_PRICE",
            )

            return self._map_order_response(symbol, response)

        except Exception as e:
            self.logger.error(f"Failed to set take profit for {symbol}: {e}")
            raise

    async def set_stop_loss_and_take_profit(
        self,
        symbol: str,
        side: TradeSide,
        stop_loss_price: float,
        take_profit_price: float,
        position_side: Optional[Literal["LONG", "SHORT", "BOTH"]] = None,
    ) -> Dict[str, Order]:
        """
        Set both stop loss and take profit for position.

        :param symbol: Trading symbol
        :param side: Position side ('buy' for long, 'sell' for short)
        :param stop_loss_price: Stop loss price
        :param take_profit_price: Take profit price
        :param position_side: Position side for Hedge Mode ('LONG', 'SHORT', or 'BOTH')
        """
        # Each method will cancel its own order type
        sl_task = self.set_stop_loss(symbol, side, stop_loss_price, True, position_side)
        tp_task = self.set_take_profit(
            symbol, side, take_profit_price, True, position_side
        )

        sl_order, tp_order = await asyncio.gather(sl_task, tp_task)

        return {"sl_order": sl_order, "tp_order": tp_order}

    async def cancel_order(self, symbol: str, order_id: str) -> None:
        """Cancel an order."""
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)

        try:
            await client.futures_cancel_order(
                symbol=binance_symbol, orderId=int(order_id)
            )
            self.logger.info(f"Cancelled order {order_id} for {symbol}")

        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            raise

    async def cancel_all_orders(self, symbol: str) -> None:
        """Cancel all open orders for symbol."""
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)

        try:
            await client.futures_cancel_all_open_orders(symbol=binance_symbol)
            self.logger.info(f"Cancelled all orders for {symbol}")

        except Exception as e:
            self.logger.error(f"Failed to cancel all orders for {symbol}: {e}")

    async def get_order(self, symbol: str, order_id: str) -> Order:
        """Get order by ID."""
        client = await self._get_client()
        binance_symbol = self._symbol_to_binance(symbol)

        try:
            response = await client.futures_get_order(
                symbol=binance_symbol, orderId=int(order_id)
            )

            return self._map_order_response(symbol, response)

        except Exception as e:
            self.logger.error(f"Failed to get order {order_id}: {e}")
            raise

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders."""
        client = await self._get_client()

        try:
            if symbol:
                binance_symbol = self._symbol_to_binance(symbol)
                orders = await client.futures_get_open_orders(symbol=binance_symbol)
            else:
                orders = await client.futures_get_open_orders()

            return [
                self._map_order_response(self._symbol_from_binance(o["symbol"]), o)
                for o in orders
            ]

        except Exception as e:
            self.logger.error(f"Failed to get open orders: {e}")
            raise

    def _map_order_response(self, symbol: str, response: Dict) -> Order:
        """Map Binance order response to Order dataclass."""
        status_map = {
            "NEW": "open",
            "PARTIALLY_FILLED": "open",
            "FILLED": "closed",
            "CANCELED": "canceled",
            "REJECTED": "canceled",
            "EXPIRED": "canceled",
        }

        return Order(
            id=str(response.get("orderId", "")),
            client_order_id=response.get("clientOrderId", ""),
            symbol=symbol,
            side=response.get("side", "").lower(),
            type=response.get("type", "").lower(),
            price=float(response.get("price", 0) or response.get("stopPrice", 0)),
            amount=float(response.get("origQty", 0)),
            filled=float(response.get("executedQty", 0)),
            remaining=float(response.get("origQty", 0))
            - float(response.get("executedQty", 0)),
            status=status_map.get(response.get("status", "NEW"), "open"),
            timestamp=int(response.get("updateTime", 0) or response.get("time", 0)),
            close_position=bool(response.get("closePosition", False)),
            reduce_only=bool(response.get("reduceOnly", False)),
        )

    # =========================================================================
    # Utility
    # =========================================================================

    async def ping(self) -> bool:
        """Check connection to exchange."""
        try:
            client = await self._get_client()
            await client.futures_ping()
            return True
        except Exception:
            return False

    async def get_server_time(self) -> Optional[datetime]:
        """Get server time."""
        try:
            client = await self._get_client()
            result = await client.futures_time()
            return datetime.fromtimestamp(result["serverTime"] / 1000, tz=timezone.utc)
        except Exception:
            return None

    # =========================================================================
    # WebSocket Streaming
    # =========================================================================

    # Reconnect settings
    WS_RECONNECT_DELAY_INITIAL = 1.0  # Initial delay in seconds
    WS_RECONNECT_DELAY_MAX = 60.0  # Maximum delay
    WS_RECONNECT_DELAY_MULTIPLIER = 2.0  # Exponential backoff multiplier

    async def _ws_with_reconnect(
        self,
        stream_names: List[str],
        handler: Callable[[Dict[str, Any]], Awaitable[None]],
        stream_description: str,
    ) -> None:
        """
        WebSocket connection wrapper with automatic reconnection.

        :param stream_names: List of stream names to subscribe to
        :param handler: Async handler for parsed messages
        :param stream_description: Description for logging
        """
        reconnect_delay = self.WS_RECONNECT_DELAY_INITIAL

        while True:
            try:
                client = await self._get_client()
                bsm = BinanceSocketManager(client)

                async with bsm.futures_multiplex_socket(stream_names) as stream:
                    self.logger.info(f"📡 Connected: {stream_description}")
                    reconnect_delay = (
                        self.WS_RECONNECT_DELAY_INITIAL
                    )  # Reset on successful connect

                    while True:
                        try:
                            msg = await stream.recv()
                            if msg:
                                await handler(msg)
                        except asyncio.CancelledError:
                            self.logger.info(f"🔌 Unsubscribed: {stream_description}")
                            return  # Exit completely
                        except Exception as e:
                            self.logger.error(
                                f"WebSocket recv error ({stream_description}): {e}"
                            )
                            break  # Break inner loop to reconnect

            except asyncio.CancelledError:
                self.logger.info(f"🔌 Cancelled: {stream_description}")
                return  # Exit completely
            except Exception as e:
                self.logger.error(
                    f"WebSocket connection error ({stream_description}): {e}"
                )

            # Reconnect with exponential backoff
            self.logger.info(
                f"🔄 Reconnecting in {reconnect_delay:.1f}s: {stream_description}"
            )
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(
                reconnect_delay * self.WS_RECONNECT_DELAY_MULTIPLIER,
                self.WS_RECONNECT_DELAY_MAX,
            )

    async def subscribe_mark_price(
        self,
        symbol: str,
        callback: Callable[[Dict[str, Any]], Awaitable[None]],
        update_speed: str = "1s",
    ) -> asyncio.Task:
        """
        Subscribe to mark price updates via WebSocket.

        Automatically reconnects on connection loss.

        :param symbol: Trading symbol (e.g., "BTC/USDT:USDT")
        :param callback: Async callback function for price updates
        :param update_speed: Update speed ("1s" or "3s")
        :return: Task that can be cancelled to stop subscription
        """
        binance_symbol = self._symbol_to_binance(symbol).lower()
        standard_symbol = symbol
        stream_name = f"{binance_symbol}@markPrice@{update_speed}"

        async def _handler(msg: Dict[str, Any]) -> None:
            data = msg.get("data", msg) if msg else None
            if data and data.get("e") == "markPriceUpdate":
                parsed = {
                    "symbol": standard_symbol,
                    "mark_price": float(data["p"]),
                    "index_price": float(data.get("i", 0)),
                    "funding_rate": float(data.get("r", 0)),
                    "next_funding_time": int(data.get("T", 0)),
                    "timestamp": int(data["E"]),
                }
                await callback(parsed)

        task = asyncio.create_task(
            self._ws_with_reconnect([stream_name], _handler, f"mark price: {symbol}")
        )
        return task

    async def subscribe_agg_trade(
        self, symbol: str, callback: Callable[[Dict[str, Any]], Awaitable[None]]
    ) -> asyncio.Task:
        """
        Subscribe to aggregated trade updates via WebSocket.

        Automatically reconnects on connection loss.
        Provides real-time trade data (price, quantity, time).

        :param symbol: Trading symbol (e.g., "BTC/USDT:USDT")
        :param callback: Async callback function for trade updates
        :return: Task that can be cancelled to stop subscription
        """
        binance_symbol = self._symbol_to_binance(symbol).lower()
        standard_symbol = symbol
        stream_name = f"{binance_symbol}@aggTrade"

        async def _handler(msg: Dict[str, Any]) -> None:
            data = msg.get("data", msg) if msg else None
            if data and data.get("e") == "aggTrade":
                parsed = {
                    "symbol": standard_symbol,
                    "price": float(data["p"]),
                    "quantity": float(data["q"]),
                    "trade_time": int(data["T"]),
                    "is_buyer_maker": data["m"],
                    "timestamp": int(data["E"]),
                }
                await callback(parsed)

        task = asyncio.create_task(
            self._ws_with_reconnect([stream_name], _handler, f"agg trade: {symbol}")
        )
        return task

    async def subscribe_book_ticker(
        self, symbol: str, callback: Callable[[Dict[str, Any]], Awaitable[None]]
    ) -> asyncio.Task:
        """
        Subscribe to best bid/ask updates via WebSocket.

        Automatically reconnects on connection loss.
        Fastest way to get current price data.

        :param symbol: Trading symbol (e.g., "BTC/USDT:USDT")
        :param callback: Async callback function for bid/ask updates
        :return: Task that can be cancelled to stop subscription
        """
        binance_symbol = self._symbol_to_binance(symbol).lower()
        standard_symbol = symbol
        stream_name = f"{binance_symbol}@bookTicker"

        async def _handler(msg: Dict[str, Any]) -> None:
            data = msg.get("data", msg) if msg else None
            if data and "s" in data:
                parsed = {
                    "symbol": standard_symbol,
                    "bid_price": float(data["b"]),
                    "bid_qty": float(data["B"]),
                    "ask_price": float(data["a"]),
                    "ask_qty": float(data["A"]),
                    "timestamp": int(data.get("E", 0)) or int(data.get("T", 0)),
                }
                await callback(parsed)

        task = asyncio.create_task(
            self._ws_with_reconnect([stream_name], _handler, f"book ticker: {symbol}")
        )
        return task

    async def subscribe_multi_book_ticker(
        self, symbols: List[str], callback: Callable[[Dict[str, Any]], Awaitable[None]]
    ) -> asyncio.Task:
        """
        Subscribe to book ticker for multiple symbols via combined stream.

        Automatically reconnects on connection loss.
        More efficient than multiple individual subscriptions.

        :param symbols: List of trading symbols
        :param callback: Async callback function for bid/ask updates
        :return: Task that can be cancelled to stop subscription
        """
        # Build symbol to standard name mapping
        symbol_map = {self._symbol_to_binance(s): s for s in symbols}

        # Build stream names
        streams = [f"{self._symbol_to_binance(s).lower()}@bookTicker" for s in symbols]

        async def _handler(msg: Dict[str, Any]) -> None:
            if msg and "data" in msg:
                data = msg["data"]
            else:
                data = msg

            if data and "s" in data:
                binance_sym = data["s"]
                standard_sym = symbol_map.get(
                    binance_sym, self._symbol_from_binance(binance_sym)
                )

                parsed = {
                    "symbol": standard_sym,
                    "bid_price": float(data["b"]),
                    "bid_qty": float(data["B"]),
                    "ask_price": float(data["a"]),
                    "ask_qty": float(data["A"]),
                    "timestamp": int(data.get("E", 0)) or int(data.get("T", 0)),
                }
                await callback(parsed)

        task = asyncio.create_task(
            self._ws_with_reconnect(streams, _handler, f"{len(symbols)} book tickers")
        )
        return task

    # =========================================================================
    # Context Manager
    # =========================================================================

    async def __aenter__(self) -> "BinanceClient":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.disconnect()
