"""
Pytest fixtures for Entry Observer integration tests.
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

    project_root = Path(__file__).parent.parent.parent.parent.parent
    env_path = project_root / ".env"

    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f"Loaded .env from {env_path}")
except ImportError:
    pass


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
    """Create BinanceClient for integration tests."""
    from src.integrations.exchange import BinanceClient, ExchangeConfig

    api_key = os.getenv("EXCHANGE_API_KEY", "")
    api_secret = os.getenv("EXCHANGE_API_SECRET", "")
    testnet = os.getenv("EXCHANGE_TESTNET", "false").lower() in ("true", "1", "yes")

    config = ExchangeConfig(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        default_leverage=1,
        margin_type="cross",
        quote_currency="USDT",
    )

    client = BinanceClient(config, logger=test_logger)
    return client


@pytest.fixture
def event_emitter(test_logger):
    """Create EventEmitter for tests."""
    from src.infra.event_emitter import EventEmitter

    return EventEmitter(logger=test_logger)


@pytest.fixture
def primary_symbol() -> str:
    """Primary symbol (ETH)."""
    return "ETH/USDT:USDT"


@pytest.fixture
def test_coin_symbol() -> str:
    """Test coin symbol."""
    return "ARB/USDT:USDT"
