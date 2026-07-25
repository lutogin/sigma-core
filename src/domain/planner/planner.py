"""
Planner Service.

Schedules and coordinates all periodic tasks for the trading bot.
Uses SchedulerService for cron/interval based task execution.
"""

import asyncio
from typing import TYPE_CHECKING, Optional

from src.infra.healthcheck import clear_health, mark_healthy

if TYPE_CHECKING:
    from src.domain.entry_observer import EntryObserverService
    from src.domain.orchestrator import OrchestratorService
    from src.domain.trading import TradingService
    from src.infra.scheduler import SchedulerService


class PlannerService:
    """
    Planner service for scheduling periodic bot tasks.

    Responsible for:
    - Scheduling OrchestratorService.run on 15-minute intervals
    - Checking position timeouts before each scan
    - Starting EntryObserverService for trailing entry monitoring
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
        entry_observer_service: Optional["EntryObserverService"] = None,
        health_file: str = "/tmp/sigma-core-health",
    ):
        """
        Initialize Planner Service.

        Args:
            logger: Application logger (DI).
            scheduler_service: Scheduler for task management.
            orchestrator_service: Orchestrator to run scans.
            scan_cron_expression: Cron expression for scan schedule.
            trading_service: Trading service for timeout checks (optional).
            entry_observer_service: Entry observer for trailing entry logic (optional).
            health_file: Marker updated only after a successful scanner cycle.
        """
        self._logger = logger
        self._scheduler = scheduler_service
        self._orchestrator = orchestrator_service
        self._scan_cron_expression = scan_cron_expression
        self._trading_service = trading_service
        self._entry_observer = entry_observer_service
        self._health_file = health_file
        self._shutdown_event: asyncio.Event | None = None

    async def run(self) -> None:
        """
        Start the planner and keep the bot running.

        This method:
        1. Starts the EntryObserverService for trailing entry monitoring
        2. Runs an initial scan immediately
        3. Schedules periodic scans every 15m
        4. Keeps the bot alive until shutdown signal
        """
        self._logger.info("🗓️  Planner starting...")
        self._shutdown_event = asyncio.Event()
        clear_health(self._health_file)

        # Start EntryObserverService (subscribes to PendingEntrySignalEvent)
        if self._entry_observer:
            await self._entry_observer.start()
            self._logger.info("👀 EntryObserverService started (trailing entry mode)")

        # Schedule periodic scans
        self._schedule_tasks()

        # Start the scheduler
        await self._scheduler.start()

        # Run initial scan immediately
        self._logger.info("📡 Running initial scan...")
        initial_scan_succeeded = await self._run_scan_job()
        if not initial_scan_succeeded:
            self._logger.error(
                "Initial scan failed; process will remain unhealthy until a scan succeeds"
            )

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

        # Stop EntryObserverService first (cancel WebSocket subscriptions)
        if self._entry_observer:
            await self._entry_observer.stop()
            self._logger.info("👀 EntryObserverService stopped")

        if self._shutdown_event:
            self._shutdown_event.set()
        await self._scheduler.stop()
        clear_health(self._health_file)

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

    async def _run_scan_job(self) -> bool:
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
            mark_healthy(self._health_file)
            return True

        except Exception as e:
            self._logger.error(f"Scan job failed: {e}", exc_info=True)
            return False
