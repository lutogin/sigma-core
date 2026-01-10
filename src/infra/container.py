"""
Simple Dependency Injection Container.

Uses lazy initialization to avoid circular imports.
All dependencies are created on first access.
"""

from typing import Any

from src.config.settings import Settings


class Container:
    """
    DI Container with lazy initialization.

    Usage:
        container = Container()
        container.init()
    """

    def __init__(self):
        self._instances: dict[str, Any] = {}
        self._settings: Settings = None  # type: ignore
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

    def has_instance(self, name: str) -> bool:
        """Check if a service instance exists."""
        return name in self._instances

    # =========================================================================
    # Settings & Logger
    # =========================================================================

    @property
    def settings(self) -> Settings:
        """Get application settings."""
        self._check_initialized()
        return self._settings

    @property
    def logger(self):
        """Get configured logger."""
        if "logger" not in self._instances:
            from src.infra.logger import logger, setup_logger

            # Setup logger with settings from container
            setup_logger(
                level=self._settings.LOG_LEVEL,
                log_to_file=self._settings.LOG_TO_FILE,
                log_dir=self._settings.LOG_DIRECTORY,
                loki_host=self._settings.LOKI_HOST,
                loki_user=self._settings.LOKI_USER,
                loki_token=self._settings.LOKI_TOKEN,
                app_name=self._settings.LOKI_APP_NAME,
                env=self._settings.ENV,
            )
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

            db = MongoDatabase(
                uri=self._settings.MONGODB_URI,
                database_name=self._settings.MONGODB_DATABASE,
                logger=self.logger,
            )
            db.connect()
            self._instances["mongo_db"] = db
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

    # @property
    # def redis_cache(self):
    #     """Get Redis cache connection."""
    #     self._check_initialized()
    #     if "redis_cache" not in self._instances:
    #         from src.infra.redis import RedisCache

    #         redis_cache = RedisCache(
    #             redis_url=self._settings.REDIS_URL,
    #             logger=self.logger,
    #         )
    #         # Note: Redis connection is async, so we can't connect here
    #         # Connections will be established when RedisCache.connect() is called
    #         # This can be done in the application startup if needed
    #         self._instances["redis_cache"] = redis_cache
    #     return self._instances["redis_cache"]

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
    def position_state_repository(self):
        """Get Position State repository (MongoDB storage for positions and cooldowns)."""
        self._check_initialized()
        if "position_state_repository" not in self._instances:
            from src.domain.position_state import PositionStateRepository

            self._instances["position_state_repository"] = PositionStateRepository(
                mongo_db=self.mongo_db,
                logger=self.logger,
            )
        return self._instances["position_state_repository"]

    @property
    def trading_pair_repository(self):
        """Get Trading Pair repository (MongoDB storage for trading pairs config)."""
        self._check_initialized()
        if "trading_pair_repository" not in self._instances:
            from src.domain.trading_pairs import TradingPairRepository

            self._instances["trading_pair_repository"] = TradingPairRepository(
                mongo_db=self.mongo_db,
                logger=self.logger,
            )
        return self._instances["trading_pair_repository"]

    @property
    def position_state_service(self):
        """Get Position State service for managing positions and cooldowns."""
        self._check_initialized()
        if "position_state_service" not in self._instances:
            from src.domain.position_state import PositionStateService

            self._instances["position_state_service"] = PositionStateService(
                repository=self.position_state_repository,
                event_emitter=self.event_emitter,
                logger=self.logger,
                timeframe=self._settings.TIMEFRAME,
                cooldown_bars=self._settings.COOLDOWN_BARS,
                max_position_bars=self._settings.MAX_POSITION_BARS,
            )
        return self._instances["position_state_service"]

    @property
    def event_emitter(self):
        """Get EventEmitter for pub/sub events."""
        if "event_emitter" not in self._instances:
            from src.infra.event_emitter import EventEmitter

            self._instances["event_emitter"] = EventEmitter(logger=self.logger)
        return self._instances["event_emitter"]

    @property
    def scheduler_service(self):
        """Get SchedulerService for task scheduling."""
        if "scheduler_service" not in self._instances:
            from src.infra.scheduler import SchedulerService

            self._instances["scheduler_service"] = SchedulerService(logger=self.logger)
        return self._instances["scheduler_service"]

    @property
    def telegram_service(self):
        """Get Telegram service for notifications."""
        self._check_initialized()
        if "telegram_service" not in self._instances:
            from src.integrations.telegram import TelegramService

            self._instances["telegram_service"] = TelegramService(
                bot_token=self._settings.TELEGRAM_BOT_TOKEN,
                admin_chat_id=self._settings.TELEGRAM_ADMIN_CHAT_ID,
            )
        return self._instances["telegram_service"]

    @property
    def communicator_service(self):
        """Get Communicator service for trade notifications."""
        self._check_initialized()
        if "communicator_service" not in self._instances:
            from src.infra.communicator import CommunicatorService

            self._instances["communicator_service"] = CommunicatorService(
                event_emitter=self.event_emitter,
                telegram_service=self.telegram_service,
                screener_service=self.screener_service,
                binance_client=self.exchange_client,
                logger=self.logger,
                entry_observer_service=self.entry_observer_service,
                exit_observer_service=self.exit_observer_service,
            )
        return self._instances["communicator_service"]

    @property
    def screener_service(self):
        """Get Screener Service."""
        self._check_initialized()
        if "screener_service" not in self._instances:
            from src.domain.screener import ScreenerService

            from src.domain.data_loader.async_data_loader import AsyncDataLoaderService

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
                volatility_filter_service=self.volatility_filter_service,
                hurst_filter_service=self.hurst_filter_service,
                adf_filter_service=self.adf_filter_service,
                halflife_filter_service=self.halflife_filter_service,
                data_loader=data_loader,
                lookback_window_days=self._settings.LOOKBACK_WINDOW_DAYS,
                correlation_threshold=self._settings.MIN_CORRELATION,
                min_beta=self._settings.MIN_BETA,
                max_beta=self._settings.MAX_BETA,
                primary_pair=self._settings.PRIMARY_PAIR,
                consistent_pairs=self._settings.CONSISTENT_PAIRS,
                timeframe=self._settings.TIMEFRAME,
                trading_pair_repository=self.trading_pair_repository,
            )
        return self._instances["screener_service"]

    @property
    def orchestrator_service(self):
        """Get Orchestrator Service for running scan cycles and emitting events."""
        self._check_initialized()
        if "orchestrator_service" not in self._instances:
            from src.domain.orchestrator import OrchestratorService

            self._instances["orchestrator_service"] = OrchestratorService(
                logger=self.logger,
                screener_service=self.screener_service,
                event_emitter=self.event_emitter,
                position_state_service=self.position_state_service,
                hurst_filter_service=self.hurst_filter_service,
                primary_pair=self._settings.PRIMARY_PAIR,
                entry_observer_service=self.entry_observer_service,
                funding_filter_service=self.funding_filter_service,
                hurst_watch_threshold=self._settings.HURST_WATCH_THRESHOLD,
                correlation_exit_threshold=self._settings.CORRELATION_EXIT_THRESHOLD,
                correlation_watch_threshold=self._settings.CORRELATION_WATCH_THRESHOLD,
                hurst_trending_for_exit=self._settings.HURST_TRENDING_FOR_EXIT,
                hurst_trending_confirm_scans=self._settings.HURST_TRENDING_CONFIRM_SCANS,
            )
        return self._instances["orchestrator_service"]

    @property
    def entry_observer_service(self):
        """Get Entry Observer Service for trailing entry logic."""
        self._check_initialized()
        if "entry_observer_service" not in self._instances:
            from src.domain.entry_observer import EntryObserverService

            self._instances["entry_observer_service"] = EntryObserverService(
                event_emitter=self.event_emitter,
                exchange_client=self.exchange_client,
                logger=self.logger,
                primary_symbol=self._settings.PRIMARY_PAIR,
                z_entry_threshold=self._settings.Z_ENTRY_THRESHOLD,
                z_sl_threshold=self._settings.Z_SL_THRESHOLD,
                pullback=self._settings.TRAILING_ENTRY_PULLBACK,
                watch_timeout_seconds=self._settings.TRAILING_ENTRY_TIMEOUT_MINUTES
                * 60,
                max_watches=self._settings.MAX_OPEN_SPREADS,
                false_alarm_hysteresis=self._settings.FALSE_ALARM_HYSTERESIS,
            )
        return self._instances["entry_observer_service"]

    @property
    def exit_observer_service(self):
        """Get Exit Observer Service for real-time TP/SL monitoring."""
        self._check_initialized()
        if "exit_observer_service" not in self._instances:
            from src.domain.exit_observer import ExitObserverService
            from src.domain.utils import get_timeframe_minutes

            # Calculate max position duration in minutes
            timeframe_minutes = get_timeframe_minutes(self._settings.TIMEFRAME)
            max_position_minutes = self._settings.MAX_POSITION_BARS * timeframe_minutes

            self._instances["exit_observer_service"] = ExitObserverService(
                event_emitter=self.event_emitter,
                exchange_client=self.exchange_client,
                position_state_service=self.position_state_service,
                logger=self.logger,
                primary_symbol=self._settings.PRIMARY_PAIR,
                debounce_seconds=1.0,
                max_position_minutes=max_position_minutes,
                trailing_sl_offset=self._settings.TRAILING_SL_OFFSET,
                trailing_sl_activation=self._settings.TRAILING_SL_ACTIVATION,
                z_entry_threshold=self._settings.Z_ENTRY_THRESHOLD,
            )
        return self._instances["exit_observer_service"]

    @property
    def trading_service(self):
        """Get Trading Service for executing trades based on signals."""
        self._check_initialized()
        if "trading_service" not in self._instances:
            from src.domain.trading import TradingService

            self._instances["trading_service"] = TradingService(
                event_emitter=self.event_emitter,
                exchange_client=self.exchange_client,
                position_state_service=self.position_state_service,
                logger=self.logger,
                allow_trading=self._settings.ALLOW_TRADING,
                position_size_usdt=self._settings.POSITION_SIZE_USDT,
                leverage=self._settings.EXCHANGE_DEFAULT_LEVERAGE,
                max_open_spreads=self._settings.MAX_OPEN_SPREADS,
                primary_symbol=self._settings.PRIMARY_PAIR,
                target_halflife_bars=self._settings.TARGET_HALFLIFE_BARS,
                min_size_multiplier=self._settings.MIN_SIZE_MULTIPLIER,
                max_size_multiplier=self._settings.MAX_SIZE_MULTIPLIER,
            )
        return self._instances["trading_service"]

    @property
    def planner_service(self):
        """Get Planner Service for scheduling periodic tasks."""
        self._check_initialized()
        if "planner_service" not in self._instances:
            from src.domain.planner import PlannerService

            self._instances["planner_service"] = PlannerService(
                logger=self.logger,
                scheduler_service=self.scheduler_service,
                orchestrator_service=self.orchestrator_service,
                trading_service=self.trading_service,
                entry_observer_service=self.entry_observer_service,
                scan_cron_expression=self._settings.SCAN_CRON_EXPRESSION,
            )
        return self._instances["planner_service"]

    @property
    def correlation_service(self):
        """Get Correlation Service for calculating rolling beta and correlation."""
        self._check_initialized()
        if "correlation_service" not in self._instances:
            from src.domain.screener.correlation import CorrelationService

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
            from src.domain.screener.z_score import ZScoreService

            self._instances["z_score_service"] = ZScoreService(
                logger=self.logger,
                lookback_window_days=self._settings.LOOKBACK_WINDOW_DAYS,
                timeframe=self._settings.TIMEFRAME,
                z_entry_threshold=self._settings.Z_ENTRY_THRESHOLD,
                z_tp_threshold=self._settings.Z_TP_THRESHOLD,
                z_sl_threshold=self._settings.Z_SL_THRESHOLD,
                adaptive_percentile=self._settings.ADAPTIVE_PERCENTILE,
                dynamic_threshold_window=self._settings.DYNAMIC_THRESHOLD_WINDOW_BARS,
                threshold_ema_alpha_up=self._settings.THRESHOLD_EMA_ALPHA_UP,
                threshold_ema_alpha_down=self._settings.THRESHOLD_EMA_ALPHA_DOWN,
            )
        return self._instances["z_score_service"]

    @property
    def volatility_filter_service(self):
        """Get Volatility Filter Service for market safety checks."""
        self._check_initialized()
        if "volatility_filter_service" not in self._instances:
            from src.domain.screener.volatility_filter import VolatilityFilterService

            self._instances["volatility_filter_service"] = VolatilityFilterService(
                logger=self.logger,
                primary_pair=self._settings.PRIMARY_PAIR,
                timeframe=self._settings.TIMEFRAME,
                volatility_window=self._settings.VOLATILITY_WINDOW,
                volatility_threshold=self._settings.VOLATILITY_THRESHOLD,
                crash_window=self._settings.VOLATILITY_CRASH_WINDOW,
                crash_threshold=self._settings.VOLATILITY_CRASH_THRESHOLD,
            )
        return self._instances["volatility_filter_service"]

    @property
    def hurst_filter_service(self):
        """Get Hurst Filter Service for mean-reversion detection."""
        self._check_initialized()
        if "hurst_filter_service" not in self._instances:
            from src.domain.screener.hurst_filter import HurstFilterService

            self._instances["hurst_filter_service"] = HurstFilterService(
                logger=self.logger,
            )
        return self._instances["hurst_filter_service"]

    @property
    def adf_filter_service(self):
        """Get ADF Filter Service for stationarity detection."""
        self._check_initialized()
        if "adf_filter_service" not in self._instances:
            from src.domain.screener.adf_filter import ADFFilterService

            self._instances["adf_filter_service"] = ADFFilterService(
                logger=self.logger,
                pvalue_threshold=self._settings.ADF_PVALUE_THRESHOLD,
                lookback_candles=self._settings.ADF_LOOKBACK_CANDLES,
            )
        return self._instances["adf_filter_service"]

    @property
    def halflife_filter_service(self):
        """Get Half-Life Filter Service for mean-reversion speed detection."""
        self._check_initialized()
        if "halflife_filter_service" not in self._instances:
            from src.domain.screener.halflife_filter import HalfLifeFilterService

            self._instances["halflife_filter_service"] = HalfLifeFilterService(
                logger=self.logger,
                max_bars=self._settings.HALFLIFE_MAX_BARS,
                lookback_candles=self._settings.HALFLIFE_LOOKBACK_CANDLES,
            )
        return self._instances["halflife_filter_service"]

    @property
    def funding_filter_service(self):
        """Get Funding Filter Service for toxic funding detection."""
        self._check_initialized()
        if "funding_filter_service" not in self._instances:
            from src.domain.screener.funding_filter import FundingFilterService

            self._instances["funding_filter_service"] = FundingFilterService(
                logger=self.logger,
                exchange_client=self.exchange_client,
                primary_symbol=self._settings.PRIMARY_PAIR,
                max_funding_cost_threshold=self._settings.MAX_FUNDING_COST_THRESHOLD,
            )
        return self._instances["funding_filter_service"]

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
        # Stop entry observer (cancel WebSocket tasks)
        if "entry_observer_service" in self._instances:
            await self._instances["entry_observer_service"].stop()

        # Stop exit observer (cancel WebSocket tasks)
        if "exit_observer_service" in self._instances:
            await self._instances["exit_observer_service"].stop()

        # Stop trading service
        if "trading_service" in self._instances:
            await self._instances["trading_service"].stop()

        # Disconnect databases
        if "mongo_db" in self._instances:
            self._instances["mongo_db"].disconnect()
        if "timescale_db" in self._instances:
            self._instances["timescale_db"].close()
        if "redis_cache" in self._instances:
            await self._instances["redis_cache"].disconnect()
        if "exchange_client" in self._instances:
            await self._instances["exchange_client"].disconnect()
        self._instances.clear()

    def shutdown(self) -> None:
        """Cleanup resources (sync version)."""
        if "mongo_db" in self._instances:
            self._instances["mongo_db"].disconnect()
        if "timescale_db" in self._instances:
            self._instances["timescale_db"].close()
        # Redis: try to disconnect if connected (best effort for sync shutdown)
        if "redis_cache" in self._instances:
            redis_cache = self._instances["redis_cache"]
            if hasattr(redis_cache, "is_connected") and redis_cache.is_connected:
                # Note: Full disconnect requires async, but we can try basic cleanup
                try:
                    import asyncio

                    # This is a best effort - in practice, Redis connections
                    # will be cleaned up by the OS when the process exits
                    pass
                except Exception:
                    pass
        self._instances.clear()
