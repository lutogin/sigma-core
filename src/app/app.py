"""
Application initialization and lifecycle management.

Scanner Application - synchronous.
Finds cointegrated pairs, runs walk-forward optimization, saves to DB, exits.
Sends results to trading service (Omega Trade) via HTTP POST.
"""

import sys

from src.infra.container import Container
from src.infra.logger import setup_logger


class Application:
    """
    Main scanner application class.

    Responsible for:
    - Initializing DI container
    - Setting up logging
    - Running cointegration scanner
    - Saving results to database
    - Exiting after completion

    This is the SCANNER part of the bot.
    Trading bot (WebSocket, live trading) will be separate.
    """

    def __init__(self) -> None:
        self._container: Container | None = None

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
        Run the scanner pipeline.

        Workflow:
        1. Initialize Orchestrator
        2. Run Scan (Connect, Load, Screen, Output)
        3. Exit
        """
        import asyncio

        if self._container is None:
            raise RuntimeError("Application not initialized. Call init() first.")

        logger = self._container.logger

        logger.info("🚀 Starting Cointegration Scanner...")

        exit_code = 0

        async def run_pipeline():
            """Run the async pipeline with proper cleanup."""
            orchestrator = self._container.orchestrator_service
            try:
                await orchestrator.run()
            finally:
                await self._shutdown_async()

        try:
            asyncio.run(run_pipeline())

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            exit_code = 130
        except Exception as e:
            logger.exception(f"Scanner failed: {e}")
            exit_code = 1

        logger.info(f"Exiting with code {exit_code}")
        sys.exit(exit_code)

    async def _shutdown_async(self) -> None:
        """Gracefully shutdown the application (async version)."""
        if self._container is None:
            return

        logger = self._container.logger
        logger.info("Shutting down scanner...")

        # Disconnect from exchange (async)
        try:
            if (
                hasattr(self._container, "_instances")
                and "exchange_client" in self._container._instances
            ):
                await self._container.exchange_client.disconnect()
        except Exception as e:
            logger.warning(f"Error disconnecting from exchange: {e}")

        self._container.shutdown()

        logger.info("Scanner shutdown complete")

    def shutdown(self) -> None:
        """Gracefully shutdown the application (sync version, deprecated)."""
        if self._container is None:
            return

        logger = self._container.logger
        logger.info("Shutting down scanner...")

        self._container.shutdown()

        logger.info("Scanner shutdown complete")


# Global application instance
app = Application()
