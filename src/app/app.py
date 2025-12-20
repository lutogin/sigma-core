"""
Application initialization and lifecycle management.

Trading Bot Application - async, long-running.
Continuously scans for cointegrated pairs and executes trades.
Uses scheduler for periodic task execution.
"""

import asyncio
import signal
import sys

from src.infra.container import Container
from src.infra.logger import setup_logger


class Application:
    """
    Main trading bot application class.

    Responsible for:
    - Initializing DI container
    - Setting up logging
    - Running the planner (continuous operation)
    - Graceful shutdown handling

    This bot runs continuously, scanning and trading.
    """

    def __init__(self) -> None:
        self._container: Container | None = None
        self._shutdown_event: asyncio.Event | None = None

    @property
    def container(self) -> Container:
        """Get the DI container."""
        if self._container is None:
            raise RuntimeError("Application not initialized. Call init() first.")
        return self._container

    def init(
        self,
        log_level: str | None = None,
        log_to_file: bool | None = None,
    ) -> "Application":
        """
        Initialize the application.

        Args:
            log_level: Logging level (overrides .env if provided)
            log_to_file: Whether to write logs to file (overrides .env if provided)

        Returns:
            Self for chaining
        """
        # Create and configure DI container first (loads .env)
        self._container = Container()
        self._container.init()

        # Get settings and logger from container
        settings = self._container.settings
        logger = self._container.logger

        # Use provided values or fall back to settings
        log_level = log_level or settings.LOG_LEVEL
        log_to_file = log_to_file if log_to_file is not None else settings.LOG_TO_FILE

        # Setup logging (with Loki for production if configured)
        setup_logger(
            level=log_level,
            log_to_file=log_to_file,
            log_dir=settings.LOG_DIRECTORY,
            loki_host=settings.LOKI_HOST,
            loki_user=settings.LOKI_USER,
            loki_token=settings.LOKI_TOKEN,
            app_name=settings.LOKI_APP_NAME,
        )
        logger.info("Initializing scanner application...")

        logger.info("Application initialized successfully")
        return self

    def run(self) -> None:
        """
        Run the trading bot.

        Workflow:
        1. Initialize services (TradingService, Planner)
        2. Start Planner (schedules periodic scans)
        3. Run until shutdown signal (Ctrl+C)
        """
        if self._container is None:
            raise RuntimeError("Application not initialized. Call init() first.")

        logger = self._container.logger
        logger.info("🚀 Starting Trading Bot...")

        exit_code = 0

        async def run_bot():
            """Run the async bot with proper signal handling."""
            self._shutdown_event = asyncio.Event()

            # Setup signal handlers
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(
                    sig, lambda: asyncio.create_task(self._signal_handler())
                )

            # Get services (triggers database connections)
            planner = self._container.planner_service
            trading_service = self._container.trading_service

            # Connect to async services that need explicit connection
            # Access redis_cache property to create instance, then connect
            redis_cache = self._container.redis_cache
            await redis_cache.connect()

            try:
                # Start trading service (subscribes to events)
                await trading_service.start()

                # Run planner (blocks until shutdown)
                await planner.run()

            except asyncio.CancelledError:
                logger.info("Bot cancelled")
            finally:
                await self._shutdown_async()

        try:
            asyncio.run(run_bot())

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            exit_code = 130
        except Exception as e:
            logger.exception(f"Bot failed: {e}")
            exit_code = 1

        logger.info(f"Exiting with code {exit_code}")
        sys.exit(exit_code)

    async def _signal_handler(self) -> None:
        """Handle shutdown signals."""
        if self._container:
            self._container.logger.info("Received shutdown signal...")

        # Stop planner
        if self._container and self._container.has_instance("planner_service"):
            await self._container.planner_service.stop()

    async def _shutdown_async(self) -> None:
        """Gracefully shutdown the application (async version)."""
        if self._container is None:
            return

        logger = self._container.logger
        logger.info("Shutting down bot...")

        # Stop trading service
        try:
            if self._container.has_instance("trading_service"):
                await self._container.trading_service.stop()
        except Exception as e:
            logger.warning(f"Error stopping trading service: {e}")

        # Stop scheduler
        try:
            if self._container.has_instance("scheduler_service"):
                await self._container.scheduler_service.stop()
        except Exception as e:
            logger.warning(f"Error stopping scheduler: {e}")

        # Disconnect from exchange
        try:
            if self._container.has_instance("exchange_client"):
                await self._container.exchange_client.disconnect()
        except Exception as e:
            logger.warning(f"Error disconnecting from exchange: {e}")

        self._container.shutdown()
        logger.info("Bot shutdown complete")

    def shutdown(self) -> None:
        """Gracefully shutdown the application (sync version)."""
        if self._container is None:
            return

        self._container.logger.info("Shutting down bot...")
        self._container.shutdown()
        self._container.logger.info("Bot shutdown complete")


# Global application instance
app = Application()
