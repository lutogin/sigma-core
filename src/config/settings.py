"""Application settings loaded from environment variables."""

import os
from pathlib import Path
from typing import Optional, Union

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


# Project root directory (calculated once at module load)
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()


def _resolve_path(path: str) -> str:
    """
    Resolve a path relative to project root.

    If path is absolute, returns as-is.
    If path is relative, resolves from PROJECT_ROOT.
    """
    p = Path(path)
    if p.is_absolute():
        return str(p)
    return str(PROJECT_ROOT / p)


class Settings:
    """
    Application settings loaded from .env file.

    All configuration values are loaded from environment variables
    with sensible defaults.
    """

    ENV: str = "local"

    WORKERS_AMOUNT: int = 10

    # Screener Settings
    TIMEFRAME: str = "15m"
    PRIMARY_PAIR: str = "ETH/USDT:USDT"
    CONSISTENT_PAIRS: list[str] = [
        "XRP/USDT:USDT",
        "ADA/USDT:USDT",
        "HYPE/USDT:USDT",
        "LINK/USDT:USDT",
        "OP/USDT:USDT",
        "UNI/USDT:USDT",
        "MNT/USDT:USDT",
        "DOGE/USDT:USDT",
        "DOT/USDT:USDT",
        "AAVE/USDT:USDT",
        "ENA/USDT:USDT",
        "LDO/USDT:USDT",
        "OPT/USDT:USDT",
        "ARB/USDT:USDT",
    ]

    MAX_BETA: float = 2.0
    MIN_BETA: float = 0.5

    # Scan settings
    LOOKBACK_WINDOW_DAYS: int = 3  # 3 days (244 candles for 15m timeframe)
    # Correlation settings
    MIN_CORRELATION: float = 0.8  # Если корреляция упала ниже, пару не торгуем!
    # Z-Score settings
    Z_ENTRY_THRESHOLD: float = 2.1  # Вход
    Z_TP_THRESHOLD: float = 0.0  # Выход
    Z_SL_THRESHOLD: float = 4.5  # Стоп-лосс
    # Beta settings
    MAX_BETA: float = 2.0
    MIN_BETA: float = 0.5
    # Hurst settings
    HURST_THRESHOLD: float = 0.45  # Максимальный Hurst для спрэдов
    HURST_LOOKBACK_CANDLES: int = 300  # 300 свечей для расчета Hurst

    # Position sizing
    POSITION_SIZE_USDT: float = 100.0  # USDT размер позиции на COIN ногу

    # Trading settings
    ALLOW_TRADING: bool = False  # Enable/disable real trading
    MAX_OPEN_SPREADS: int = 5  # Maximum number of open spread positions

    # Exchange Settings
    EXCHANGE_NAME: str = "binance"
    EXCHANGE_API_KEY: str = ""
    EXCHANGE_API_SECRET: str = ""
    EXCHANGE_TESTNET: bool = False
    EXCHANGE_DEFAULT_LEVERAGE: int = 1
    EXCHANGE_MARGIN_TYPE: str = "cross"  # "cross" or "isolated"
    EXCHANGE_QUOTE_CURRENCY: str = "USDT"

    # Logging Settings
    LOG_LEVEL: str = "DEBUG"
    LOG_TO_FILE: bool = True
    LOG_DIRECTORY: str = "logs"

    # Loki Settings (Grafana Cloud)
    LOKI_HOST: str = ""  # e.g., https://logs-prod-012.grafana.net
    LOKI_USER: str = ""  # Grafana Cloud user ID
    LOKI_TOKEN: str = ""  # Grafana Cloud API token
    LOKI_APP_NAME: str = "alpha-bot"

    # MongoDB Settings
    MONGODB_URI: str = "mongodb://localhost:27017"
    MONGODB_DATABASE: str = "alpha_bot"

    # PostgreSQL/TimescaleDB Settings
    TIMESCALE_DB_URL: str = "postgresql://localhost:5432/alpha_bot"

    def __init__(self):
        """Initialize settings from environment variables."""
        self.ENV = os.getenv("ENV", "local")
        self.WORKERS_AMOUNT = int(os.getenv("WORKERS_AMOUNT", "10"))
        self.CORRELATION_THRESHOLD = float(os.getenv("CORRELATION_THRESHOLD", "0.84"))

        self.TIMEFRAME = os.getenv("TIMEFRAME", "15m")
        self.PRIMARY_PAIR = os.getenv("PRIMARY_PAIR", "ETH/USDT:USDT")
        # Remove duplicates while preserving order
        pairs = os.getenv("CONSISTENT_PAIRS", "").split(", ")
        self.CONSISTENT_PAIRS = list(dict.fromkeys(p for p in pairs if p))

        self.LOOKBACK_WINDOW_DAYS = int(os.getenv("LOOKBACK_WINDOW_DAYS", "3"))
        self.MIN_CORRELATION = float(os.getenv("MIN_CORRELATION", "0.8"))
        self.Z_ENTRY_THRESHOLD = float(os.getenv("Z_ENTRY_THRESHOLD", "2.1"))
        self.Z_TP_THRESHOLD = float(os.getenv("Z_TP_THRESHOLD", "0.0"))
        self.Z_SL_THRESHOLD = float(os.getenv("Z_SL_THRESHOLD", "4.5"))

        self.MIN_BETA = float(os.getenv("MIN_BETA", "0.5"))
        self.MAX_BETA = float(os.getenv("MAX_BETA", "2"))

        self.HURST_THRESHOLD = float(os.getenv("HURST_THRESHOLD", "0.45"))
        self.HURST_LOOKBACK_CANDLES = int(os.getenv("HURST_LOOKBACK_CANDLES", "300"))

        # Position sizing
        self.POSITION_SIZE_USDT = float(os.getenv("POSITION_SIZE_USDT", "100.0"))

        # Trading
        self.ALLOW_TRADING = os.getenv("ALLOW_TRADING", "false").lower() == "true"
        self.MAX_OPEN_SPREADS = int(os.getenv("MAX_OPEN_SPREADS", "5"))

        # Exchange
        self.EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "binance")
        self.EXCHANGE_API_KEY = os.getenv("EXCHANGE_API_KEY", "")
        self.EXCHANGE_API_SECRET = os.getenv("EXCHANGE_API_SECRET", "")
        self.EXCHANGE_TESTNET = os.getenv("EXCHANGE_TESTNET", "false").lower() == "true"
        self.EXCHANGE_DEFAULT_LEVERAGE = int(
            os.getenv("EXCHANGE_DEFAULT_LEVERAGE", "1")
        )
        self.EXCHANGE_MARGIN_TYPE = os.getenv("EXCHANGE_MARGIN_TYPE", "cross")
        self.EXCHANGE_QUOTE_CURRENCY = os.getenv("EXCHANGE_QUOTE_CURRENCY", "USDT")

        # Logging
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.LOG_TO_FILE = os.getenv("LOG_TO_FILE", "true").lower() == "true"
        self.LOG_DIRECTORY = _resolve_path(os.getenv("LOG_DIRECTORY", "logs"))

        # Loki (Grafana Cloud)
        self.LOKI_HOST = os.getenv("LOKI_HOST", "")
        self.LOKI_USER = os.getenv("LOKI_USER", "")
        self.LOKI_TOKEN = os.getenv("LOKI_TOKEN", "")
        self.LOKI_APP_NAME = os.getenv("LOKI_APP_NAME", "alpha-bot")

        # MongoDB
        self.MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "alpha-simple-bot")

        # PostgreSQL/TimescaleDB
        self.TIMESCALE_DB_URL = os.getenv(
            "TIMESCALE_DB_URL", "postgresql://localhost:5432/alpha_bot"
        )

    def to_dict(self) -> dict:
        """Convert settings to dictionary."""
        return {
            "exchange": {
                "name": self.EXCHANGE_NAME,
            },
        }


def load_settings(env_file: Optional[Union[str, Path]] = None) -> Settings:
    """
    Load settings from .env file.

    Args:
        env_file: Path to .env file. If None, searches for .env in project root.

    Returns:
        Settings instance with loaded values.
    """
    if env_file is None:
        env_file = PROJECT_ROOT / ".env"
    else:
        env_file = Path(env_file)

    if load_dotenv is None:
        # python-dotenv not installed, skip loading
        pass
    elif env_file.exists():
        load_dotenv(env_file)
    # Note: logging happens later after logger is configured

    return Settings()
