"""Process health marker used by Docker and deployment verification."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Optional

DEFAULT_HEALTH_FILE = "/tmp/sigma-core-health"
DEFAULT_MAX_AGE_SECONDS = 1800


def mark_healthy(path: str) -> None:
    """Record the completion time of the latest successful scanner cycle."""
    marker = Path(path)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()


def clear_health(path: str) -> None:
    """Remove a stale marker during startup or graceful shutdown."""
    try:
        Path(path).unlink()
    except FileNotFoundError:
        pass


def check_health(
    path: str,
    max_age_seconds: int,
    *,
    pid: Optional[int] = None,
    now: Optional[float] = None,
) -> tuple[bool, str]:
    """Validate that the process exists and a scanner cycle succeeded recently."""
    process_id = 1 if pid is None else pid
    try:
        os.kill(process_id, 0)
    except (OSError, ValueError) as exc:
        return False, f"process {process_id} is unavailable: {exc}"

    marker = Path(path)
    try:
        modified_at = marker.stat().st_mtime
    except OSError as exc:
        return False, f"health marker is unavailable: {exc}"

    checked_at = time.time() if now is None else now
    age_seconds = checked_at - modified_at
    if age_seconds < -5:
        return False, f"health marker is {abs(age_seconds):.0f}s in the future"
    if age_seconds > max_age_seconds:
        return False, (
            f"last successful scanner cycle is {age_seconds:.0f}s old "
            f"(maximum {max_age_seconds}s)"
        )
    return True, "healthy"


def main() -> int:
    """CLI entry point for the container HEALTHCHECK."""
    path = os.getenv("SIGMA_HEALTH_FILE", DEFAULT_HEALTH_FILE)
    try:
        max_age_seconds = int(
            os.getenv(
                "SIGMA_HEALTH_MAX_AGE_SECONDS",
                str(DEFAULT_MAX_AGE_SECONDS),
            )
        )
    except ValueError:
        print("SIGMA_HEALTH_MAX_AGE_SECONDS must be an integer", file=sys.stderr)
        return 2

    healthy, reason = check_health(path, max_age_seconds)
    if not healthy:
        print(reason, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
