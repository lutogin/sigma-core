from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.domain.planner.planner import PlannerService


def _planner(tmp_path: Path, orchestrator: AsyncMock) -> PlannerService:
    return PlannerService(
        logger=MagicMock(),
        scheduler_service=MagicMock(),
        orchestrator_service=orchestrator,
        scan_cron_expression="*/15 * * * *",
        health_file=str(tmp_path / "scanner-health"),
    )


@pytest.mark.asyncio
async def test_successful_scan_updates_health_marker(tmp_path: Path) -> None:
    planner = _planner(tmp_path, AsyncMock())

    succeeded = await planner._run_scan_job()

    assert succeeded is True
    assert (tmp_path / "scanner-health").exists()


@pytest.mark.asyncio
async def test_failed_scan_does_not_update_health_marker(tmp_path: Path) -> None:
    orchestrator = AsyncMock()
    orchestrator.run.side_effect = RuntimeError("database unavailable")
    planner = _planner(tmp_path, orchestrator)

    succeeded = await planner._run_scan_job()

    assert succeeded is False
    assert not (tmp_path / "scanner-health").exists()
