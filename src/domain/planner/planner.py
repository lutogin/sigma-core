"""
Planner Service.

Schedules and coordinates all periodic tasks for the trading bot.
Uses SchedulerService for cron/interval based task execution.
"""

import asyncio
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from src.domain.orchestrator import OrchestratorService
    from src.domain.trading import TradingService
    from src.infra.scheduler import SchedulerService


class PlannerService:
    """
    Planner service for scheduling periodic bot tasks.

    Responsible for:
    - Scheduling OrchestratorService.run on 15-minute intervals
    - Checking position timeouts before each scan
    - Managing the bot's continuous operation lifecycle
    - Coordinating scheduled tasks
    """

    def __init__(
        self,
        logger,
        scheduler_service: "SchedulerService",
        orchestrator_service: "OrchestratorService",
        scan_cron_expression: str,
        trading_service: Optional["TradingService"] = None,
    ):
        """
        Initialize Planner Service.

        Args:
            logger: Application logger (DI).
            scheduler_service: Scheduler for task management.
            orchestrator_service: Orchestrator to run scans.
            scan_cron_expression: Cron expression for scan schedule.
            trading_service: Trading service for timeout checks (optional).
        """
        self._logger = logger
        self._scheduler = scheduler_service
        self._orchestrator = orchestrator_service
        self._scan_cron_expression = scan_cron_expression
        self._trading_service = trading_service
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
            f"⏰ Next scans scheduled at minute 00, 15, 30, 45 of each hour (cron: {self._scan_cron_expression})"
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
        # Schedule scan job using cron (every 15 minutes at minute 00, 15, 30, 45)
        self._scheduler.schedule_cron_job(
            name="orchestrator_scan",
            func=self._run_scan_job,
            cron_expression=self._scan_cron_expression,
            replace_existing=True,
        )
        self._logger.info(
            f"📅 Scheduled 'orchestrator_scan' with cron: {self._scan_cron_expression}"
        )

    async def _run_scan_job(self) -> None:
        """Execute the orchestrator scan with pre-checks."""
        try:
            # 1. Check for position timeouts before scanning
            if self._trading_service:
                timeouts_closed = await self._trading_service.check_and_close_timeouts()
                if timeouts_closed > 0:
                    self._logger.info(
                        f"⏰ Closed {timeouts_closed} timed-out position(s)"
                    )

            # 2. Run the orchestrator scan
            await self._orchestrator.run()

        except Exception as e:
            self._logger.error(f"Scan job failed: {e}", exc_info=True)
