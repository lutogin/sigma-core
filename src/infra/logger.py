"""Loguru logger configuration with Grafana Loki support."""

import os
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

# Remove default handler
logger.remove()


def _short_name(record: dict) -> str:
    """Extract short module name from full path (last 2 parts)."""
    name = record.get("name", "")
    parts = name.split(".")
    # Keep last 2 parts: e.g. "opportunity_observer.observer"
    if len(parts) > 2:
        return ".".join(parts[-2:])
    return name


def _format_record(record: dict) -> str:
    """Custom format function for shorter module names."""
    short_name = _short_name(record)
    record["extra"]["short_name"] = short_name
    return (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{extra[short_name]}</cyan>:<cyan>{function}</cyan> | "
        "<level>{message}</level>\n{exception}"
    )


def _format_record_file(record: dict) -> str:
    """Custom format function for file logging."""
    short_name = _short_name(record)
    record["extra"]["short_name"] = short_name
    return (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
        "{level: <8} | "
        "{extra[short_name]}:{function} | "
        "{message}\n{exception}"
    )


def _format_record_loki(record: dict) -> str:
    """Custom format function for Loki logging (same as file, but without colors)."""
    short_name = _short_name(record)
    record["extra"]["short_name"] = short_name
    return (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
        "{level: <8} | "
        "{extra[short_name]}:{function} | "
        "{message}"
    )


# Legacy formats (for reference)
LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)

LOG_FORMAT_FILE = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
    "{level: <8} | "
    "{name}:{function}:{line} | "
    "{message}"
)


def setup_loki(
    host: str,
    user: str,
    token: str,
    app_name: str = "omega-bot",
    level: str = "INFO",
) -> None:
    """
    Setup Loki logging using loguru-loki-handler.

    Args:
        host: Loki host URL (e.g., https://logs-prod-012.grafana.net)
        user: Loki username
        token: Loki API token
        app_name: Application name label
        level: Minimum log level to send
    """
    try:
        from loguru_loki_handler import loki_handler
    except ImportError:
        logger.warning("loguru-loki-handler not installed. Loki logging disabled.")
        return

    try:
        # Build Loki URL
        # Note: loguru-loki-handler does not support 'auth' param,
        # so we must embed credentials in the URL.

        base_url = host.rstrip("/")
        if "://" not in base_url:
            base_url = f"https://{base_url}"

        if user and token:
            from urllib.parse import urlparse, quote

            # Embed auth in URL: https://user:token@host...
            parsed = urlparse(base_url)
            # Encode user/pass to handle special chars safe for URL
            safe_user = quote(user, safe='')
            safe_token = quote(token, safe='')

            # Reconstruct netloc with auth
            new_netloc = f"{safe_user}:{safe_token}@{parsed.netloc}"
            base_url = parsed._replace(netloc=new_netloc).geturl()

        loki_url = f"{base_url}/loki/api/v1/push"

        # Create labels
        labels = {
            "app": app_name,
            "env": "prod",
        }

        # Add Loki sink to loguru
        logger.add(
            loki_handler(loki_url, labels),
            level=level,
            format=_format_record_loki,  # Use custom text format instead of JSON
            serialize=False,  # Send as text, not JSON
            backtrace=True,
            diagnose=False,  # Don't include source in prod
        )

        # Log success (masking token in URL for log)
        safe_log_url = loki_url
        if user and token:
            safe_log_url = loki_url.replace(token, "***")

        logger.info(f"📡 Loki logging enabled → {safe_log_url}")

    except Exception as e:
        logger.warning(f"Failed to setup Loki logging: {e}")


def setup_logger(
    level: str = "INFO",
    log_to_file: bool = True,
    log_dir: str = "logs",
    rotation: str = "10 MB",
    retention: str = "7 days",
    loki_host: Optional[str] = None,
    loki_user: Optional[str] = None,
    loki_token: Optional[str] = None,
    app_name: str = "omega-bot",
    env: str = "local",
) -> None:
    """
    Configure the logger.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to write logs to file
        log_dir: Directory for log files
        rotation: When to rotate log files
        retention: How long to keep old log files
        loki_host: Grafana Loki host URL (for production)
        loki_user: Loki username
        loki_token: Loki API token
        app_name: Application name for Loki labels
        env: Environment name (only 'prod' enables Loki)
    """
    # Console handler with colors (short module names)
    logger.add(
        sys.stderr,
        format=_format_record,
        level=level,
        colorize=True,
        backtrace=True,
        diagnose=True,
    )

    if log_to_file:
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)

        # Info and above to main log
        logger.add(
            log_path / "bot.log",
            format=_format_record_file,
            level="INFO",
            rotation=rotation,
            retention=retention,
            compression="zip",
            backtrace=True,
            diagnose=True,
        )

        # Errors to separate file
        logger.add(
            log_path / "errors.log",
            format=_format_record_file,
            level="ERROR",
            rotation=rotation,
            retention=retention,
            compression="zip",
            backtrace=True,
            diagnose=True,
        )

        # Debug log (if debug level enabled)
        if level == "DEBUG":
            logger.add(
                log_path / "debug.log",
                format=_format_record_file,
                level="DEBUG",
                rotation=rotation,
                retention="3 days",
                compression="zip",
            )

    # Setup Loki only for production environment
    if env.lower() == "prod" and loki_host and loki_user and loki_token:
        setup_loki(
            host=loki_host,
            user=loki_user,
            token=loki_token,
            app_name=app_name,
            level=level,
        )
    elif loki_host and env.lower() != "prod":
        logger.debug(f"Loki configured but ENV={env} != 'prod'. Skipping Loki setup.")


# Export configured logger
__all__ = ["logger", "setup_logger", "setup_loki"]
