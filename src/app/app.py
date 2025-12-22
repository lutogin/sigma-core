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

    def init(self) -> "Application":
        """
        Initialize the application.

        Returns:
            Self for chaining
        """
        # Create and configure DI container (loads .env and sets up logger)
        self._container = Container()
        self._container.init()

        # Get logger from container (triggers logger setup)
        logger = self._container.logger
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
            communicator_service = self._container.communicator_service
            telegram_service = self._container.telegram_service

            # Connect to async services that need explicit connection
            # Access redis_cache property to create instance, then connect
            redis_cache = self._container.redis_cache
            await redis_cache.connect()

            try:
                # Start communicator service (subscribes to events)
                communicator_service.start()

                # Start trading service (subscribes to events)
                await trading_service.start()

                # Start Telegram bot polling in background
                if telegram_service.is_enabled():
                    await telegram_service.start_polling_background()
                    logger.info("Telegram bot polling started")

                    # Register callbacks for Telegram buttons
                    telegram_service.register_callback(
                        "get_opportunities",
                        communicator_service.send_opportunities
                    )
                    telegram_service.register_callback(
                        "get_positions",
                        communicator_service.send_positions
                    )
                    telegram_service.register_callback(
                        "get_balances",
                        communicator_service.send_balance
                    )
                    telegram_service.register_callback(
                        "close_all_positions",
                        communicator_service.close_all_positions
                    )

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

        # Stop communicator service
        try:
            if self._container.has_instance("communicator_service"):
                self._container.communicator_service.stop()
        except Exception as e:
            logger.warning(f"Error stopping communicator service: {e}")

        # Stop Telegram polling
        try:
            if self._container.has_instance("telegram_service"):
                await self._container.telegram_service.stop_polling()
        except Exception as e:
            logger.warning(f"Error stopping Telegram polling: {e}")

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
