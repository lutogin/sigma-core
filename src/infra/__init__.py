"""Infrastructure module - core infrastructure components."""

from src.infra.logger import logger, setup_logger
from src.infra.mongo import MongoDatabase
from src.infra.scheduler import SchedulerService, AsyncIOScheduler
from src.infra.timescale import TimescaleDB
from src.infra.container import Container

# Note: Other infra components are imported directly to avoid circular imports:
#   from src.infra.container import Container
#   from src.infra.cache_manager import CacheManager
#   from src.infra.mongo import MongoDatabase
#   from src.infra.timescale import TimescaleDB
#
# Data loading (OHLCV repository, exchange loader):
#   from src.domain.data_loader import OHLCVRepository, ExchangeDataLoader
#
# Exchange integrations:
#   from src.integrations.exchange import BinanceClient

__all__ = [
    "logger", 
    "setup_logger",
    "MongoDatabase",
    "SchedulerService",
    "AsyncIOScheduler",
    "TimescaleDB",
    "Container",
]

