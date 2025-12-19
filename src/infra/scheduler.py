"""
Scheduler Service - Async task scheduling with APScheduler.

Lightweight wrapper for scheduling periodic async tasks.
"""

from typing import Callable, List, Optional
from datetime import datetime
import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.job import Job
from apscheduler.events import EVENT_JOB_ERROR, JobExecutionEvent


class SchedulerService:
    """
    Async scheduler service for managing scheduled tasks.

    Features:
    - Interval-based scheduling
    - Async task execution
    - Graceful shutdown handling

    Usage:
        scheduler = SchedulerService(logger)
        scheduler.schedule_interval_job("my_job", my_async_func, seconds=60)
        await scheduler.start()
    """

    def __init__(self, logger, timezone: str = "UTC"):
        """
        Initialize scheduler.

        Args:
            logger: Logger instance (DI).
            timezone: Default timezone for jobs.
        """
        self._logger = logger
        self._timezone = timezone
        self._is_running = False

        self._scheduler = AsyncIOScheduler(
            timezone=timezone,
            job_defaults={
                "coalesce": True,  # Combine missed executions into one
                "max_instances": 1,  # Only one instance of each job at a time
                "misfire_grace_time": 60,  # Allow 60 seconds for missed jobs
            },
        )

        # Register error listener
        self._scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._is_running

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> None:
        """Start the scheduler."""
        if self._is_running:
            self._logger.warning("Scheduler is already running")
            return

        self._scheduler.start()
        self._is_running = True
        self._logger.info("🕐 Scheduler started")

    async def stop(self, wait: bool = True) -> None:
        """
        Stop the scheduler.

        Args:
            wait: Wait for running jobs to complete.
        """
        if not self._is_running:
            return

        self._logger.info("Stopping scheduler...")

        try:
            loop = asyncio.get_running_loop()
            if loop.is_closed():
                self._logger.warning("Event loop closed, skipping scheduler shutdown")
                self._is_running = False
                return
        except RuntimeError:
            self._logger.warning("No running event loop, skipping scheduler shutdown")
            self._is_running = False
            return

        try:
            self._scheduler.shutdown(wait=wait)
        except RuntimeError as e:
            if "Event loop is closed" in str(e):
                self._logger.warning("Event loop closed during shutdown")
            else:
                raise

        self._is_running = False
        self._logger.info("Scheduler stopped")

    # =========================================================================
    # Job Scheduling
    # =========================================================================

    def schedule_interval_job(
        self,
        name: str,
        func: Callable,
        seconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
        args: Optional[List] = None,
        kwargs: Optional[dict] = None,
        start_date: Optional[datetime] = None,
        replace_existing: bool = True,
    ) -> Optional[Job]:
        """
        Schedule a job to run at fixed intervals.

        Args:
            name: Unique job name.
            func: Function to execute (async supported).
            seconds: Interval in seconds.
            minutes: Interval in minutes.
            hours: Interval in hours.
            args: Positional arguments.
            kwargs: Keyword arguments.
            start_date: When to start (None = now).
            replace_existing: Replace if exists.

        Returns:
            Job instance or None on failure.
        """
        if self.has_job(name) and not replace_existing:
            self._logger.warning(f"Job '{name}' already exists")
            return None

        try:
            trigger = IntervalTrigger(
                seconds=seconds,
                minutes=minutes,
                hours=hours,
                start_date=start_date,
                timezone=self._timezone,
            )

            job = self._scheduler.add_job(
                func,
                trigger=trigger,
                id=name,
                name=name,
                args=args or [],
                kwargs=kwargs or {},
                replace_existing=replace_existing,
            )

            interval_parts = []
            if hours:
                interval_parts.append(f"{hours}h")
            if minutes:
                interval_parts.append(f"{minutes}m")
            if seconds:
                interval_parts.append(f"{seconds}s")

            self._logger.debug(
                f"Scheduled interval job '{name}' every {' '.join(interval_parts)}"
            )
            return job

        except Exception as e:
            self._logger.error(f"Failed to schedule job '{name}': {e}")
            return None

    def schedule_cron_job(
        self,
        name: str,
        func: Callable,
        cron_expression: str,
        args: Optional[List] = None,
        kwargs: Optional[dict] = None,
        start_date: Optional[datetime] = None,
        replace_existing: bool = True,
    ) -> Optional[Job]:
        """
        Schedule a job using cron expression.

        Args:
            name: Unique job name.
            func: Function to execute (async supported).
            cron_expression: Cron expression (e.g., '*/15 * * * *' for every 15 minutes).
            args: Positional arguments.
            kwargs: Keyword arguments.
            start_date: When to start (None = now).
            replace_existing: Replace if exists.

        Returns:
            Job instance or None on failure.
        """
        if self.has_job(name) and not replace_existing:
            self._logger.warning(f"Job '{name}' already exists")
            return None

        try:
            # Parse cron expression (minute hour day month day_of_week)
            parts = cron_expression.split()
            if len(parts) != 5:
                raise ValueError(
                    f"Invalid cron expression '{cron_expression}'. Expected 5 parts: minute hour day month day_of_week"
                )

            minute, hour, day, month, day_of_week = parts

            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                start_date=start_date,
                timezone=self._timezone,
            )

            job = self._scheduler.add_job(
                func,
                trigger=trigger,
                id=name,
                name=name,
                args=args or [],
                kwargs=kwargs or {},
                replace_existing=replace_existing,
            )

            self._logger.debug(
                f"Scheduled cron job '{name}' with expression '{cron_expression}'"
            )
            return job

        except Exception as e:
            self._logger.error(f"Failed to schedule cron job '{name}': {e}")
            return None

    # =========================================================================
    # Job Management
    # =========================================================================

    def pause_job(self, name: str) -> bool:
        """Pause a job."""
        job = self._scheduler.get_job(name)
        if job:
            job.pause()
            self._logger.debug(f"Paused job: {name}")
            return True
        return False

    def resume_job(self, name: str) -> bool:
        """Resume a paused job."""
        job = self._scheduler.get_job(name)
        if job:
            job.resume()
            self._logger.debug(f"Resumed job: {name}")
            return True
        return False

    def remove_job(self, name: str) -> bool:
        """Remove a job."""
        try:
            self._scheduler.remove_job(name)
            self._logger.info(f"Removed job: {name}")
            return True
        except Exception:
            return False

    def has_job(self, name: str) -> bool:
        """Check if a job exists."""
        return self._scheduler.get_job(name) is not None

    def get_all_jobs(self) -> List[str]:
        """Get names of all jobs."""
        return [job.id for job in self._scheduler.get_jobs()]

    # =========================================================================
    # Event Handlers
    # =========================================================================

    def _on_job_error(self, event: JobExecutionEvent) -> None:
        """Handle job execution error."""
        self._logger.error(
            f"Job '{event.job_id}' failed: {event.exception}",
            exc_info=event.exception,
        )
