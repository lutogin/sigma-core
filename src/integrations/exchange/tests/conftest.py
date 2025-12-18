"""
Pytest fixtures for exchange integration tests.

Loads configuration from .env file in project root.
"""

import os
import pytest
import asyncio
from pathlib import Path
from typing import Generator

from loguru import logger

# Load .env file from project root
try:
    from dotenv import load_dotenv

    # Find project root (3 levels up from tests/)
    project_root = Path(__file__).parent.parent.parent.parent.parent
    env_path = project_root / ".env"

    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f"Loaded .env from {env_path}")
    else:
        logger.warning(f".env not found at {env_path}")
except ImportError:
    logger.warning("python-dotenv not installed, skipping .env loading")


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_logger():
    """Provide a logger for tests."""
    return logger


@pytest.fixture
def binance_client(test_logger):
    """
    Create BinanceClient for integration tests.

    Uses configuration from .env file:
    - EXCHANGE_API_KEY
    - EXCHANGE_API_SECRET
    - EXCHANGE_TESTNET
    - EXCHANGE_DEFAULT_LEVERAGE
    - EXCHANGE_MARGIN_TYPE
    """
    from src.integrations.exchange import BinanceClient

    # Load from environment variables
    api_key = os.getenv("EXCHANGE_API_KEY", "")
    api_secret = os.getenv("EXCHANGE_API_SECRET", "")
    testnet = os.getenv("EXCHANGE_TESTNET", "false").lower() in ("true", "1", "yes")
    default_leverage = int(os.getenv("EXCHANGE_DEFAULT_LEVERAGE", "1"))
    margin_type = os.getenv("EXCHANGE_MARGIN_TYPE", "cross")

    # Log config (without secrets)
    test_logger.info("BinanceClient config:")
    test_logger.info(
        f"  API Key: {'***' + api_key[-4:] if len(api_key) > 4 else '(empty)'}"
    )
    test_logger.info(f"  Testnet: {testnet}")
    test_logger.info(f"  Leverage: {default_leverage}x")
    test_logger.info(f"  Margin Type: {margin_type}")

    client = BinanceClient(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        default_leverage=default_leverage,
        default_margin_type=margin_type,
        quote_currency="USDT",
        logger=test_logger,
    )
    return client


@pytest.fixture
def binance_client_public(test_logger):
    """
    Create BinanceClient for public endpoints only (no API keys).

    Use this fixture for tests that don't require authentication.
    """
    from src.integrations.exchange import BinanceClient

    client = BinanceClient(
        api_key="",
        api_secret="",
        testnet=False,
        default_leverage=1,
        default_margin_type="cross",
        quote_currency="USDT",
        logger=test_logger,
    )
    return client


@pytest.fixture
def test_symbol() -> str:
    """Standard test symbol."""
    return "BTC/USDT:USDT"


@pytest.fixture
def test_symbols() -> list:
    """Multiple test symbols."""
    return ["BTC/USDT:USDT", "ETH/USDT:USDT"]
