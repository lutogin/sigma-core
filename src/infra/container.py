"""
Simple Dependency Injection Container.

Uses lazy initialization to avoid circular imports.
All dependencies are created on first access.
"""

from typing import Any


class Container:
    """
    DI Container with lazy initialization.

    Usage:
        container = Container()
        container.init()
    """

    def __init__(self):
        self._instances: dict[str, Any] = {}
        self._settings = None
        self._initialized = False

    def init(self) -> "Container":
        """
        Initialize the container with configuration.

        Returns:
            Self for chaining
        """
        from src.config import load_settings

        self._settings = load_settings()
        self._initialized = True
        return self

    def _check_initialized(self) -> None:
        """Check that container is initialized."""
        if not self._initialized:
            raise RuntimeError("Container not initialized. Call init() first.")

    # =========================================================================
    # Settings & Logger
    # =========================================================================

    @property
    def settings(self):
        """Get application settings."""
        self._check_initialized()
        return self._settings

    @property
    def logger(self):
        """Get configured logger."""
        if "logger" not in self._instances:
            from src.infra.logger import logger

            self._instances["logger"] = logger
        return self._instances["logger"]

    # =========================================================================
    # Infrastructure
    # =========================================================================

    @property
    def mongo_db(self):
        """Get MongoDB database connection."""
        self._check_initialized()
        if "mongo_db" not in self._instances:
            from src.infra.mongo import MongoDatabase

            self._instances["mongo_db"] = MongoDatabase(
                uri=self._settings.MONGODB_URI,
                database_name=self._settings.MONGODB_DATABASE,
                logger=self.logger,
            )
        return self._instances["mongo_db"]

    @property
    def timescale_db(self):
        """Get TimescaleDB connection."""
        self._check_initialized()
        if "timescale_db" not in self._instances:
            from src.infra.timescale import TimescaleDB

            db = TimescaleDB(db_url=self._settings.TIMESCALE_DB_URL, logger=self.logger)
            db.connect()
            self._instances["timescale_db"] = db
        return self._instances["timescale_db"]

    @property
    def ohlcv_repository(self):
        """Get OHLCV repository (TimescaleDB storage for price data)."""
        self._check_initialized()
        if "ohlcv_repository" not in self._instances:
            from src.domain.data_loader import OHLCVRepository

            self._instances["ohlcv_repository"] = OHLCVRepository(
                db=self.timescale_db, logger=self.logger
            )
        return self._instances["ohlcv_repository"]

    @property
    def screener_service(self):
        """Get Orchestrator Service."""
        self._check_initialized()
        if "screener_service" not in self._instances:
            from src.domain.screener import ScreenerService

            # Ensure data loader is available
            # Warning: AsyncDataLoaderService expects dependencies too.
            # Let's verify we have a way to get it.
            # It seems container doesn't have explicit property for AsyncDataLoaderService yet
            # based on my viewing of container.py earlier.
            # I need to check if I need to add it or if it's created inline.
            # Checking container.py again...
            # The user listed @[src/domain/data_loader/async_service.py]
            # but I don't recall seeing it in container properties.
            # Let's add it.

            from src.domain.data_loader.async_service import AsyncDataLoaderService

            data_loader = AsyncDataLoaderService(
                logger=self.logger,
                exchange_client=self.exchange_client,
                ohlcv_repository=self.ohlcv_repository,
            )

            self._instances["screener_service"] = ScreenerService(
                logger=self.logger,
                exchange_client=self.exchange_client,
                correlation_service=self.correlation_service,
                z_score_service=self.z_score_service,
                data_loader=data_loader,
                lookback_window_days=self._settings.LOOKBACK_WINDOW_DAYS,
                correlation_threshold=self._settings.MIN_CORRELATION,
                primary_pair=self._settings.PRIMARY_PAIR,
                consistent_pairs=self._settings.CONSISTENT_PAIRS,
                timeframe=self._settings.TIMEFRAME,
            )
        return self._instances["screener_service"]

    @property
    def correlation_service(self):
        """Get Correlation Service for calculating rolling beta and correlation."""
        self._check_initialized()
        if "correlation_service" not in self._instances:
            from src.domain.correlation import CorrelationService

            self._instances["correlation_service"] = CorrelationService(
                logger=self.logger,
                lookback_window_days=self._settings.LOOKBACK_WINDOW_DAYS,
                timeframe=self._settings.TIMEFRAME,
            )
        return self._instances["correlation_service"]

    @property
    def z_score_service(self):
        """Get Z-Score Service for calculating spread and z-score."""
        self._check_initialized()
        if "z_score_service" not in self._instances:
            from src.domain.z_score import ZScoreService

            self._instances["z_score_service"] = ZScoreService(
                logger=self.logger,
                lookback_window_days=self._settings.LOOKBACK_WINDOW_DAYS,
                timeframe=self._settings.TIMEFRAME,
            )
        return self._instances["z_score_service"]

    # =========================================================================
    # Exchange
    # =========================================================================

    @property
    def exchange_client(self):
        """Get exchange client (Binance Futures)."""
        self._check_initialized()
        if "exchange_client" not in self._instances:
            from src.integrations.exchange import BinanceClient, ExchangeConfig

            config = ExchangeConfig(
                api_key=self._settings.EXCHANGE_API_KEY,
                api_secret=self._settings.EXCHANGE_API_SECRET,
                testnet=self._settings.EXCHANGE_TESTNET,
                default_leverage=self._settings.EXCHANGE_DEFAULT_LEVERAGE,
                margin_type=self._settings.EXCHANGE_MARGIN_TYPE,
                quote_currency=self._settings.EXCHANGE_QUOTE_CURRENCY,
            )
            self._instances["exchange_client"] = BinanceClient(
                config=config, logger=self.logger
            )
        return self._instances["exchange_client"]

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def shutdown_async(self) -> None:
        """Cleanup resources (async version)."""
        if "mongo_db" in self._instances:
            self._instances["mongo_db"].disconnect()
        if "timescale_db" in self._instances:
            self._instances["timescale_db"].close()
        self._instances.clear()

    def shutdown(self) -> None:
        """Cleanup resources (sync version)."""
        if "mongo_db" in self._instances:
            self._instances["mongo_db"].disconnect()
        if "timescale_db" in self._instances:
            self._instances["timescale_db"].close()
        self._instances.clear()
