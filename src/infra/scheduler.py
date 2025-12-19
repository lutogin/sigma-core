"""
Scheduler Service - Task scheduling with APScheduler.

Supports cron expressions, async execution, and process-based parallelism
to avoid GIL limitations for CPU-bound tasks.
"""

from typing import Callable, Dict, List, Optional, Any, Coroutine
from datetime import datetime
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.job import Job
from apscheduler.events import (
    EVENT_JOB_EXECUTED,
    EVENT_JOB_ERROR,
    EVENT_JOB_MISSED,
    JobExecutionEvent,
)


class SchedulerService:
    """
    Scheduler service for managing scheduled tasks.

    Features:
    - Cron-based scheduling
    - Interval-based scheduling
    - One-time scheduled tasks
    - Async task execution
    - Process pool for CPU-bound tasks (avoids GIL)
    - Thread pool for I/O-bound tasks

    Usage:
        scheduler = SchedulerService(logger)

        # Add a cron job
        scheduler.schedule_cron_job(
            name="daily_scan",
            cron_expression="0 8 * * *",  # Every day at 8:00
            func=my_async_function,
            args=[arg1, arg2]
        )

        # Start the scheduler
        await scheduler.start()
    """

    def __init__(
        self,
        logger,
        timezone: str = "UTC",
        max_workers: int = 4,
        use_process_pool: bool = True
    ):
        """
        Initialize scheduler.

        :param logger: Logger instance (DI, required)
        :param timezone: Default timezone for jobs
        :param max_workers: Max workers for executor pool
        :param use_process_pool: Use ProcessPoolExecutor for CPU-bound tasks
        """
        self.logger = logger
        self.timezone = timezone
        self.is_running = False

        # Create executor for parallel execution
        if use_process_pool:
            self._executor = ProcessPoolExecutor(max_workers=max_workers)
        else:
            self._executor = ThreadPoolExecutor(max_workers=max_workers)

        # Create async scheduler
        self._scheduler = AsyncIOScheduler(
            timezone=timezone,
            executors={
                'default': {'type': 'asyncio'},
                'processpool': {'type': 'processpool', 'max_workers': max_workers},
                'threadpool': {'type': 'threadpool', 'max_workers': max_workers},
            },
            job_defaults={
                'coalesce': True,  # Combine missed executions into one
                'max_instances': 1,  # Only one instance of each job at a time
                'misfire_grace_time': 60,  # Allow 60 seconds for missed jobs
            }
        )

        # Register event listeners
        self._scheduler.add_listener(
            self._on_job_executed,
            EVENT_JOB_EXECUTED
        )
        self._scheduler.add_listener(
            self._on_job_error,
            EVENT_JOB_ERROR
        )
        self._scheduler.add_listener(
            self._on_job_missed,
            EVENT_JOB_MISSED
        )

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self) -> None:
        """Start the scheduler."""
        if self.is_running:
            self.logger.warning("Scheduler is already running")
            return

        self._scheduler.start()
        self.is_running = True
        self.logger.info("🕐 Scheduler started")

    async def stop(self, wait: bool = True) -> None:
        """
        Stop the scheduler.

        :param wait: Wait for running jobs to complete
        """
        if not self.is_running:
            return

        self.logger.info("Stopping scheduler...")
        try:
            # Check if event loop is still open
            try:
                loop = asyncio.get_running_loop()
                if loop.is_closed():
                    self.logger.warning("Event loop is closed, skipping scheduler shutdown")
                    self.is_running = False
                    return
            except RuntimeError:
                # No running loop
                self.logger.warning("No running event loop, skipping scheduler shutdown")
                self.is_running = False
                return
            
            # Try to shutdown scheduler
            try:
                self._scheduler.shutdown(wait=wait)
            except RuntimeError as e:
                if "Event loop is closed" in str(e):
                    self.logger.warning("Event loop closed during shutdown, skipping scheduler cleanup")
                else:
                    raise
            
            self._executor.shutdown(wait=wait)
            self.is_running = False
            self.logger.info("Scheduler stopped")
        except Exception as e:
            self.logger.warning(f"Error stopping scheduler: {e}")
            self.is_running = False

    # =========================================================================
    # Job Scheduling
    # =========================================================================

    def schedule_cron_job(
        self,
        name: str,
        cron_expression: str,
        func: Callable,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        executor: str = 'default',
        replace_existing: bool = True
    ) -> Optional[Job]:
        """
        Schedule a job using cron expression.

        :param name: Unique job name
        :param cron_expression: Cron expression (e.g., "0 8 * * *" for 8:00 daily)
        :param func: Function to execute (sync or async)
        :param args: Positional arguments for the function
        :param kwargs: Keyword arguments for the function
        :param executor: Executor to use ('default', 'processpool', 'threadpool')
        :param replace_existing: Replace if job with same name exists
        :return: Job instance or None
        """
        if self.has_job(name) and not replace_existing:
            self.logger.warning(f"Job '{name}' already exists")
            return None

        try:
            # Parse cron expression
            trigger = CronTrigger.from_crontab(cron_expression, timezone=self.timezone)

            job = self._scheduler.add_job(
                func,
                trigger=trigger,
                id=name,
                name=name,
                args=args or [],
                kwargs=kwargs or {},
                executor=executor,
                replace_existing=replace_existing
            )

            self.logger.info(
                f"📅 Scheduled cron job '{name}' with expression: {cron_expression}"
            )
            return job

        except Exception as e:
            self.logger.error(f"Failed to schedule job '{name}': {e}")
            return None

    def schedule_interval_job(
        self,
        name: str,
        func: Callable,
        seconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        executor: str = 'default',
        start_date: Optional[datetime] = None,
        replace_existing: bool = True
    ) -> Optional[Job]:
        """
        Schedule a job to run at fixed intervals.

        :param name: Unique job name
        :param func: Function to execute
        :param seconds: Interval in seconds
        :param minutes: Interval in minutes
        :param hours: Interval in hours
        :param args: Positional arguments
        :param kwargs: Keyword arguments
        :param executor: Executor to use
        :param start_date: When to start (None = now)
        :param replace_existing: Replace if exists
        :return: Job instance or None
        """
        if self.has_job(name) and not replace_existing:
            self.logger.warning(f"Job '{name}' already exists")
            return None

        try:
            trigger = IntervalTrigger(
                seconds=seconds,
                minutes=minutes,
                hours=hours,
                start_date=start_date,
                timezone=self.timezone
            )

            job = self._scheduler.add_job(
                func,
                trigger=trigger,
                id=name,
                name=name,
                args=args or [],
                kwargs=kwargs or {},
                executor=executor,
                replace_existing=replace_existing
            )

            interval_str = []
            if hours:
                interval_str.append(f"{hours}h")
            if minutes:
                interval_str.append(f"{minutes}m")
            if seconds:
                interval_str.append(f"{seconds}s")

            self.logger.info(
                f"📅 Scheduled interval job '{name}' every {' '.join(interval_str)}"
            )
            return job

        except Exception as e:
            self.logger.error(f"Failed to schedule job '{name}': {e}")
            return None

    def schedule_once(
        self,
        name: str,
        func: Callable,
        run_date: datetime,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        executor: str = 'default'
    ) -> Optional[Job]:
        """
        Schedule a one-time job.

        :param name: Unique job name
        :param func: Function to execute
        :param run_date: When to run
        :param args: Positional arguments
        :param kwargs: Keyword arguments
        :param executor: Executor to use
        :return: Job instance or None
        """
        try:
            trigger = DateTrigger(run_date=run_date, timezone=self.timezone)

            job = self._scheduler.add_job(
                func,
                trigger=trigger,
                id=name,
                name=name,
                args=args or [],
                kwargs=kwargs or {},
                executor=executor
            )

            self.logger.info(
                f"📅 Scheduled one-time job '{name}' at {run_date}"
            )
            return job

        except Exception as e:
            self.logger.error(f"Failed to schedule job '{name}': {e}")
            return None

    # =========================================================================
    # Job Management
    # =========================================================================

    def pause_job(self, name: str) -> bool:
        """Pause a job."""
        job = self._scheduler.get_job(name)
        if job:
            job.pause()
            self.logger.debug(f"Paused job: {name}")
            return True
        self.logger.warning(f"Job '{name}' not found")
        return False

    def resume_job(self, name: str) -> bool:
        """Resume a paused job."""
        job = self._scheduler.get_job(name)
        if job:
            job.resume()
            self.logger.debug(f"Resumed job: {name}")
            return True
        self.logger.warning(f"Job '{name}' not found")
        return False

    def remove_job(self, name: str) -> bool:
        """Remove a job completely."""
        try:
            self._scheduler.remove_job(name)
            self.logger.info(f"Removed job: {name}")
            return True
        except Exception:
            self.logger.warning(f"Job '{name}' not found")
            return False

    def run_job_now(self, name: str) -> bool:
        """Trigger immediate execution of a job."""
        job = self._scheduler.get_job(name)
        if job:
            job.modify(next_run_time=datetime.now(tz=job.trigger.timezone))
            self.logger.debug(f"Triggered immediate execution: {name}")
            return True
        self.logger.warning(f"Job '{name}' not found")
        return False

    # =========================================================================
    # Job Queries
    # =========================================================================

    def has_job(self, name: str) -> bool:
        """Check if a job exists."""
        return self._scheduler.get_job(name) is not None

    def get_job(self, name: str) -> Optional[Job]:
        """Get a job by name."""
        return self._scheduler.get_job(name)

    def get_all_jobs(self) -> List[str]:
        """Get names of all jobs."""
        return [job.id for job in self._scheduler.get_jobs()]

    def get_job_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed job information.

        :return: Dict with job info or None
        """
        job = self._scheduler.get_job(name)
        if not job:
            return None

        return {
            'id': job.id,
            'name': job.name,
            'next_run_time': job.next_run_time,
            'trigger': str(job.trigger),
            'executor': job.executor,
            'pending': job.pending,
        }

    def get_pending_jobs(self) -> List[Dict[str, Any]]:
        """Get all pending jobs with their next run times."""
        jobs = []
        for job in self._scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time,
            })
        return sorted(jobs, key=lambda x: x['next_run_time'] or datetime.max)

    # =========================================================================
    # Event Handlers
    # =========================================================================

    def _on_job_executed(self, event: JobExecutionEvent) -> None:
        """Handle successful job execution."""
        self.logger.debug(
            f"Job '{event.job_id}' executed successfully"
        )

    def _on_job_error(self, event: JobExecutionEvent) -> None:
        """Handle job execution error."""
        self.logger.error(
            f"Job '{event.job_id}' failed with error: {event.exception}",
            exc_info=event.exception
        )

    def _on_job_missed(self, event: JobExecutionEvent) -> None:
        """Handle missed job execution."""
        self.logger.warning(
            f"Job '{event.job_id}' missed scheduled run time"
        )


class SyncSchedulerService:
    """
    Synchronous scheduler for non-async applications.

    Uses BackgroundScheduler which runs in a separate thread.
    """

    def __init__(
        self,
        logger,
        timezone: str = "UTC",
        max_workers: int = 4
    ):
        """Initialize sync scheduler."""
        self.logger = logger
        self.timezone = timezone
        self.is_running = False

        self._scheduler = BackgroundScheduler(
            timezone=timezone,
            executors={
                'default': {'type': 'threadpool', 'max_workers': max_workers},
                'processpool': {'type': 'processpool', 'max_workers': max_workers},
            },
            job_defaults={
                'coalesce': True,
                'max_instances': 1,
                'misfire_grace_time': 60,
            }
        )

        # Register event listeners
        self._scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)

    def start(self) -> None:
        """Start the scheduler."""
        if self.is_running:
            self.logger.warning("Scheduler is already running")
            return

        self._scheduler.start()
        self.is_running = True
        self.logger.info("🕐 Scheduler started")

    def stop(self, wait: bool = True) -> None:
        """Stop the scheduler."""
        if not self.is_running:
            return

        self._scheduler.shutdown(wait=wait)
        self.is_running = False
        self.logger.info("Scheduler stopped")

    def schedule_cron_job(
        self,
        name: str,
        cron_expression: str,
        func: Callable,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        executor: str = 'default',
        replace_existing: bool = True
    ) -> Optional[Job]:
        """Schedule a cron job."""
        try:
            trigger = CronTrigger.from_crontab(cron_expression, timezone=self.timezone)

            job = self._scheduler.add_job(
                func,
                trigger=trigger,
                id=name,
                name=name,
                args=args or [],
                kwargs=kwargs or {},
                executor=executor,
                replace_existing=replace_existing
            )

            self.logger.info(f"📅 Scheduled cron job '{name}': {cron_expression}")
            return job

        except Exception as e:
            self.logger.error(f"Failed to schedule job '{name}': {e}")
            return None

    def schedule_interval_job(
        self,
        name: str,
        func: Callable,
        seconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
        args: Optional[List] = None,
        kwargs: Optional[Dict] = None,
        executor: str = 'default',
        replace_existing: bool = True
    ) -> Optional[Job]:
        """Schedule an interval job."""
        try:
            trigger = IntervalTrigger(
                seconds=seconds,
                minutes=minutes,
                hours=hours,
                timezone=self.timezone
            )

            job = self._scheduler.add_job(
                func,
                trigger=trigger,
                id=name,
                name=name,
                args=args or [],
                kwargs=kwargs or {},
                executor=executor,
                replace_existing=replace_existing
            )

            self.logger.info(f"📅 Scheduled interval job '{name}'")
            return job

        except Exception as e:
            self.logger.error(f"Failed to schedule job '{name}': {e}")
            return None

    def remove_job(self, name: str) -> bool:
        """Remove a job."""
        try:
            self._scheduler.remove_job(name)
            self.logger.info(f"Removed job: {name}")
            return True
        except Exception:
            return False

    def has_job(self, name: str) -> bool:
        """Check if job exists."""
        return self._scheduler.get_job(name) is not None

    def get_all_jobs(self) -> List[str]:
        """Get all job names."""
        return [job.id for job in self._scheduler.get_jobs()]

    def _on_job_error(self, event: JobExecutionEvent) -> None:
        """Handle job error."""
        self.logger.error(f"Job '{event.job_id}' failed: {event.exception}")

