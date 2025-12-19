"""
Planner Service.

Schedules and coordinates all periodic tasks for the trading bot.
Uses SchedulerService for cron/interval based task execution.
"""

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.domain.orchestrator import OrchestratorService
    from src.infra.scheduler import SchedulerService


class PlannerService:
    """
    Planner service for scheduling periodic bot tasks.

    Responsible for:
    - Scheduling OrchestratorService.run on 15-minute intervals
    - Managing the bot's continuous operation lifecycle
    - Coordinating scheduled tasks
    """

    # Scan every 15 minutes + 1 second (to ensure candle is closed)
    SCAN_INTERVAL_SECONDS = 15 * 60 + 1  # 901 seconds

    def __init__(
        self,
        logger,
        scheduler_service: "SchedulerService",
        orchestrator_service: "OrchestratorService",
    ):
        """
        Initialize Planner Service.

        Args:
            logger: Application logger (DI).
            scheduler_service: Scheduler for task management.
            orchestrator_service: Orchestrator to run scans.
        """
        self._logger = logger
        self._scheduler = scheduler_service
        self._orchestrator = orchestrator_service
        self._shutdown_event: asyncio.Event | None = None

    async def run(self) -> None:
        """
        Start the planner and keep the bot running.

        This method:
        1. Runs an initial scan immediately
        2. Schedules periodic scans every 15m01s
        3. Keeps the bot alive until shutdown signal
        """
        self._logger.info("🗓️  Planner starting...")
        self._shutdown_event = asyncio.Event()

        # Schedule periodic scans
        self._schedule_tasks()

        # Start the scheduler
        await self._scheduler.start()

        # Run initial scan immediately
        self._logger.info("📡 Running initial scan...")
        await self._run_scan_job()

        self._logger.info(
            f"⏰ Next scan scheduled in {self.SCAN_INTERVAL_SECONDS}s "
            f"({self.SCAN_INTERVAL_SECONDS // 60}m {self.SCAN_INTERVAL_SECONDS % 60}s)"
        )

        # Keep running until shutdown
        self._logger.info("🤖 Bot is running. Press Ctrl+C to stop.")
        await self._shutdown_event.wait()

        self._logger.info("🛑 Planner shutting down...")

    async def stop(self) -> None:
        """Signal the planner to stop."""
        self._logger.info("Stopping planner...")
        if self._shutdown_event:
            self._shutdown_event.set()
        await self._scheduler.stop()

    def _schedule_tasks(self) -> None:
        """Schedule all periodic tasks."""
        # Schedule scan job every 15m01s
        self._scheduler.schedule_interval_job(
            name="orchestrator_scan",
            func=self._run_scan_job,
            seconds=self.SCAN_INTERVAL_SECONDS,
            replace_existing=True,
        )
        self._logger.info(
            f"📅 Scheduled 'orchestrator_scan' every {self.SCAN_INTERVAL_SECONDS}s"
        )

    async def _run_scan_job(self) -> None:
        """Execute the orchestrator scan."""
        try:
            await self._orchestrator.run()
        except Exception as e:
            self._logger.error(f"Scan job failed: {e}", exc_info=True)
