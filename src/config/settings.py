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
        "LINK/USDT:USDT",
        "UNI/USDT:USDT",
        "AAVE/USDT:USDT",
        "RENDER/USDT:USDT",
        "TURBO/USDT:USDT",
        "ENS/USDT:USDT",
        "FET/USDT:USDT",
        "MORPHO/USDT:USDT",
        "SPX/USDT:USDT",
    ]

    # Scan settings
    LOOKBACK_WINDOW_DAYS: int = 3  # 3 days (244 candles for 15m timeframe)
    # Correlation settings
    MIN_CORRELATION: float = 0.8  # Если корреляция упала ниже, пару не торгуем!
    # Z-Score settings
    Z_ENTRY_THRESHOLD: float = 0  # Воход в мониторинг PendingEntrySignalEvent
    Z_TP_THRESHOLD: float = 0.0  # Выход
    Z_SL_THRESHOLD: float = 4.5  # Стоп-лосс ExitSignalEvent
    # Beta settings
    MAX_BETA: float = 2.0
    MIN_BETA: float = 0.5
    # Hurst settings
    HURST_THRESHOLD: float = 0.45  # Максимальный Hurst для спрэдов
    HURST_LOOKBACK_CANDLES: int = 300  # 300 свечей для расчета Hurst

    # Volatility filter
    VOLATILITY_WINDOW: int = 24  # 6 hours (24 x 15min candles)
    VOLATILITY_THRESHOLD: float = 0.008
    VOLATILITY_CRASH_WINDOW: int = 16  # 4 hours (16 x 15min candles)
    VOLATILITY_CRASH_THRESHOLD: float = 0.05

    # Funding filter
    # Block entry if net funding cost exceeds this threshold per 8h
    # -0.0005 = -0.05% (5x standard rate of 0.01%)
    MAX_FUNDING_COST_THRESHOLD: float = -0.0005

    # Adaptive threshold settings
    ADAPTIVE_PERCENTILE: int = (
        95  # Percentile for dynamic Z-score threshold (95 = top 5%)
    )
    DYNAMIC_THRESHOLD_WINDOW_BARS: int = 440
    THRESHOLD_EMA_ALPHA: float = 0.1  # EMA smoothing factor (0.1 = 10% new, 90% old)
    # Trailing Entry settings (Smart Entry)
    TRAILING_ENTRY_PULLBACK: float = 0.2  # Z-score pullback for reversal confirmation
    TRAILING_ENTRY_TIMEOUT_MINUTES: int = 45  # Max watch duration before cancellation
    FALSE_ALARM_HYSTERESIS: float = 0.2  # Cancel watch only if Z drops this much below threshold

    # Position sizing
    POSITION_SIZE_USDT: float = 100.0  # USDT размер позиции на COIN ногу

    # Trading settings
    ALLOW_TRADING: bool = False  # Enable/disable real trading
    MAX_OPEN_SPREADS: int = 5  # Maximum number of open spread positions

    # Position state settings
    COOLDOWN_BARS: int = 16  # Cooldown after SL/CORRELATION_DROP (16 bars = 4h for 15m)
    MAX_POSITION_BARS: int = 96  # Max position duration before timeout (~24h for 15m)

    # Planner/Scan settings
    SCAN_CRON_EXPRESSION: str = "*/15 * * * *"  # Every 15 minutes

    # Exchange Settings
    EXCHANGE_NAME: str = "binance"
    EXCHANGE_API_KEY: str = ""
    EXCHANGE_API_SECRET: str = ""
    EXCHANGE_TESTNET: bool = False
    EXCHANGE_DEFAULT_LEVERAGE: int = 1
    EXCHANGE_MARGIN_TYPE: str = "cross"  # "cross" or "isolated"
    EXCHANGE_QUOTE_CURRENCY: str = "USDT"

    # Telegram Settings
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_ADMIN_CHAT_ID: str = "0"

    # Logging Settings
    LOG_LEVEL: str = "DEBUG"
    LOG_TO_FILE: bool = True
    LOG_DIRECTORY: str = "logs"

    # Loki Settings (Grafana Cloud)
    LOKI_HOST: str = ""  # e.g., https://logs-prod-012.grafana.net
    LOKI_USER: str = ""  # Grafana Cloud user ID
    LOKI_TOKEN: str = ""  # Grafana Cloud API token
    LOKI_APP_NAME: str = "sigma-bot"

    # MongoDB Settings
    MONGODB_URI: str = "mongodb://localhost:27017"
    MONGODB_DATABASE: str = "alpha_bot"

    # PostgreSQL/TimescaleDB Settings
    TIMESCALE_DB_URL: str = "postgresql://localhost:5432/alpha_bot"

    # Redis
    REDIS_URL: str = "redis://localhost:6379"

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

        # Screener
        self.LOOKBACK_WINDOW_DAYS = int(os.getenv("LOOKBACK_WINDOW_DAYS", "3"))
        self.MIN_CORRELATION = float(os.getenv("MIN_CORRELATION", "0.8"))
        self.Z_ENTRY_THRESHOLD = float(os.getenv("Z_ENTRY_THRESHOLD", "2.0"))
        self.Z_TP_THRESHOLD = float(os.getenv("Z_TP_THRESHOLD", "0.0"))
        self.Z_SL_THRESHOLD = float(os.getenv("Z_SL_THRESHOLD", "4.5"))
        # Beta
        self.MIN_BETA = float(os.getenv("MIN_BETA", "0.5"))
        self.MAX_BETA = float(os.getenv("MAX_BETA", "2"))
        # Hurst
        self.HURST_THRESHOLD = float(os.getenv("HURST_THRESHOLD", "0.45"))
        self.HURST_LOOKBACK_CANDLES = int(os.getenv("HURST_LOOKBACK_CANDLES", "300"))
        # Volatility filter
        self.VOLATILITY_WINDOW = int(os.getenv("VOLATILITY_WINDOW", "24"))
        self.VOLATILITY_THRESHOLD = float(os.getenv("VOLATILITY_THRESHOLD", "0.008"))
        self.VOLATILITY_CRASH_WINDOW = int(os.getenv("VOLATILITY_CRASH_WINDOW", "16"))
        self.VOLATILITY_CRASH_THRESHOLD = float(
            os.getenv("VOLATILITY_CRASH_THRESHOLD", "0.05")
        )

        # Funding filter
        self.MAX_FUNDING_COST_THRESHOLD = float(
            os.getenv("MAX_FUNDING_COST_THRESHOLD", "-0.0005")
        )

        # Adaptive threshold
        self.ADAPTIVE_PERCENTILE = int(os.getenv("ADAPTIVE_PERCENTILE", "95"))

        self.DYNAMIC_THRESHOLD_WINDOW_BARS = int(
            os.getenv("DYNAMIC_THRESHOLD_WINDOW_BARS", "440")
        )
        self.THRESHOLD_EMA_ALPHA = float(os.getenv("THRESHOLD_EMA_ALPHA", "0.1"))

        # Trailing Entry (Smart Entry)
        self.TRAILING_ENTRY_PULLBACK = float(
            os.getenv("TRAILING_ENTRY_PULLBACK", "0.2")
        )
        self.TRAILING_ENTRY_TIMEOUT_MINUTES = int(
            os.getenv("TRAILING_ENTRY_TIMEOUT_MINUTES", "60")
        )
        self.FALSE_ALARM_HYSTERESIS = float(
            os.getenv("FALSE_ALARM_HYSTERESIS", "0.3")
        )

        # Position sizing
        self.POSITION_SIZE_USDT = float(os.getenv("POSITION_SIZE_USDT", "100.0"))

        # Trading
        self.ALLOW_TRADING = os.getenv("ALLOW_TRADING", "false").lower() == "true"
        self.MAX_OPEN_SPREADS = int(os.getenv("MAX_OPEN_SPREADS", "3"))

        # Position state
        self.COOLDOWN_BARS = int(os.getenv("COOLDOWN_BARS", "16"))
        self.MAX_POSITION_BARS = int(os.getenv("MAX_POSITION_BARS", "96"))

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

        # Telegram
        self.TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.TELEGRAM_ADMIN_CHAT_ID = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")

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

        # Redis
        self.REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

    def to_dict(self) -> dict:
        """Convert settings to dictionary."""
        return {
            "exchange": {
                "name": self.EXCHANGE_NAME,
            },
        }

    def log_trading_config(self, logger) -> None:
        """Log trading configuration at startup."""
        logger.info("=" * 60)
        logger.info("📋 TRADING CONFIGURATION")
        logger.info("=" * 60)
        logger.info(f"ENV: {self.ENV}")
        logger.info(f"TIMEFRAME: {self.TIMEFRAME}")
        logger.info(f"PRIMARY_PAIR: {self.PRIMARY_PAIR}")
        logger.info(f"CONSISTENT_PAIRS: {len(self.CONSISTENT_PAIRS)} pairs")
        logger.info("-" * 40)
        logger.info("📊 Screener Settings:")
        logger.info(f"  LOOKBACK_WINDOW_DAYS: {self.LOOKBACK_WINDOW_DAYS}")
        logger.info(f"  MIN_CORRELATION: {self.MIN_CORRELATION}")
        logger.info("-" * 40)
        logger.info("📈 Z-Score Settings:")
        logger.info(f"  Z_ENTRY_THRESHOLD: {self.Z_ENTRY_THRESHOLD}")
        logger.info(f"  Z_TP_THRESHOLD: {self.Z_TP_THRESHOLD}")
        logger.info(f"  Z_SL_THRESHOLD: {self.Z_SL_THRESHOLD}")
        logger.info(f"  ADAPTIVE_PERCENTILE: {self.ADAPTIVE_PERCENTILE}")
        logger.info(
            f"  DYNAMIC_THRESHOLD_WINDOW_BARS: {self.DYNAMIC_THRESHOLD_WINDOW_BARS}"
        )
        logger.info(f"  THRESHOLD_EMA_ALPHA: {self.THRESHOLD_EMA_ALPHA}")
        logger.info("-" * 40)
        logger.info("📉 Beta Settings:")
        logger.info(f"  MIN_BETA: {self.MIN_BETA}")
        logger.info(f"  MAX_BETA: {self.MAX_BETA}")
        logger.info("-" * 40)
        logger.info("🔬 Hurst Settings:")
        logger.info(f"  HURST_THRESHOLD: {self.HURST_THRESHOLD}")
        logger.info(f"  HURST_LOOKBACK_CANDLES: {self.HURST_LOOKBACK_CANDLES}")
        logger.info("-" * 40)
        logger.info("💸 Funding Filter Settings:")
        logger.info(
            f"  MAX_FUNDING_COST_THRESHOLD: {self.MAX_FUNDING_COST_THRESHOLD} "
            f"({self.MAX_FUNDING_COST_THRESHOLD * 100:.3f}% per 8h)"
        )
        logger.info("-" * 40)
        logger.info("🎯 Trailing Entry Settings:")
        logger.info(f"  TRAILING_ENTRY_PULLBACK: {self.TRAILING_ENTRY_PULLBACK}")
        logger.info(
            f"  TRAILING_ENTRY_TIMEOUT_MINUTES: {self.TRAILING_ENTRY_TIMEOUT_MINUTES}"
        )
        logger.info("-" * 40)
        logger.info("💰 Position Settings:")
        logger.info(f"  POSITION_SIZE_USDT: {self.POSITION_SIZE_USDT}")
        logger.info(f"  MAX_OPEN_SPREADS: {self.MAX_OPEN_SPREADS}")
        logger.info(f"  COOLDOWN_BARS: {self.COOLDOWN_BARS}")
        logger.info(f"  MAX_POSITION_BARS: {self.MAX_POSITION_BARS}")
        logger.info(f"  ALLOW_TRADING: {self.ALLOW_TRADING}")
        logger.info("=" * 60)


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
