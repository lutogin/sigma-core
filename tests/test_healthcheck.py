import os
from pathlib import Path

from src.infra.healthcheck import check_health, clear_health, mark_healthy


def test_health_requires_a_recent_successful_scan(tmp_path: Path) -> None:
    marker = tmp_path / "health"

    healthy, reason = check_health(
        str(marker),
        max_age_seconds=30,
        pid=os.getpid(),
    )

    assert healthy is False
    assert "unavailable" in reason

    mark_healthy(str(marker))
    modified_at = marker.stat().st_mtime

    healthy, reason = check_health(
        str(marker),
        max_age_seconds=30,
        pid=os.getpid(),
        now=modified_at + 10,
    )

    assert healthy is True
    assert reason == "healthy"


def test_health_rejects_stale_marker_and_clear_removes_it(tmp_path: Path) -> None:
    marker = tmp_path / "health"
    mark_healthy(str(marker))
    modified_at = marker.stat().st_mtime

    healthy, reason = check_health(
        str(marker),
        max_age_seconds=30,
        pid=os.getpid(),
        now=modified_at + 31,
    )

    assert healthy is False
    assert "31s old" in reason

    clear_health(str(marker))
    assert not marker.exists()
