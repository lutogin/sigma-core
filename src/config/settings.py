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
    CORRELATION_WATCH_THRESHOLD: float = (
        0.77  # Remove watch if correlation drops below this
    )
    # Correlation hysteresis thresholds (relaxed thresholds for existing positions/watches)
    # Entry requires >= MIN_CORRELATION, but holding/watching allows lower values
    CORRELATION_EXIT_THRESHOLD: float = (
        0.75  # Exit position if correlation drops below this
    )

    # Z-Score settings
    Z_ENTRY_THRESHOLD: float = (
        2.1  # Воход в мониторинг PendingEntrySignalEvent, минимальный порог для входа. Далее он расчитывается динамически по 95-му перцентилю за последние 440 баров.
    )
    Z_TP_THRESHOLD: float = 0.25  # Выход
    Z_SL_THRESHOLD: float = (
        4.0  # Стоп-лосс ExitSignalEvent. Так же служит как safe z-score при всплесках
    )
    Z_SL_EXTREME_OFFSET: float = (
        0.5  # Offset для экстремальных входов: если entry_z > Z_SL_THRESHOLD, то SL = entry_z + offset
    )
    Z_SCORE_PROGRESS_EXIT_THRESHOLD: float = (
        0.30  # Выход если Z-score прогрессирует ниже этого порога в совокупности с другими фильтрами
    )

    # Beta settings
    MAX_BETA: float = 2.0
    MIN_BETA: float = 0.5

    # Hurst settings
    HURST_THRESHOLD: float = 0.45  # Максимальный Hurst для спрэдов (вход)
    HURST_WATCH_THRESHOLD: float = (
        0.46  # Tolerance для watches и открытых позиций (0.45 + 0.01 = 0.46)
    )
    HURST_TRENDING_FOR_EXIT: float = (
        0.46  # Threshold для выхода из позиции (0.45 + 0.02 = 0.47)
    )
    HURST_TRENDING_CONFIRM_SCANS: int = (
        2  # Количество сканов для подтверждения trending
    )
    HURST_LOOKBACK_CANDLES: int = 300  # 300 свечей для расчета Hurst

    # ADF settings
    ADF_PVALUE_THRESHOLD: float = 0.08  # Максимальный p-value для стационарности
    ADF_LOOKBACK_CANDLES: int = 300  # 300 свечей для расчета ADF
    ADF_EXIT_CONFIRM_SCANS: int = 2  # Подтверждение ADF-деградации для выхода

    # Half-Life settings
    HALFLIFE_MAX_BARS: float = 48.0  # 0.5 * MAX_POSITION_BARS (96) = 48 bars = 12h
    HALFLIFE_LOOKBACK_CANDLES: int = 300  # 300 свечей для расчета Half-Life
    HALFLIFE_EXIT_CONFIRM_SCANS: int = (
        2  # Подтверждение деградации Half-Life для выхода
    )

    # Dynamic Position Sizing based on Half-Life
    # Size = BaseSize × (TargetHalfLife / CurrentHalfLife)
    # Fast reversion (low HL) → larger size, slow reversion (high HL) → smaller size
    TARGET_HALFLIFE_BARS: float = 12.0  # Target HL = 12 bars (3h for 15m) as baseline
    MIN_SIZE_MULTIPLIER: float = 0.5  # Minimum multiplier (slow reversion protection)
    MAX_SIZE_MULTIPLIER: float = 2.0  # Maximum multiplier (fast reversion cap)

    # Volatility filter
    VOLATILITY_WINDOW: int = 24  # 6 hours (24 x 15min candles)
    VOLATILITY_THRESHOLD: float = 0.012
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
    # Asymmetric EMA for threshold smoothing
    # alpha_up = slow increase (don't miss opportunities when vol spikes)
    # alpha_down = faster decrease (capture more entries when vol drops)
    THRESHOLD_EMA_ALPHA_UP: float = 0.01  # Slow rise: 1% new, 99% old
    THRESHOLD_EMA_ALPHA_DOWN: float = 0.05  # Faster fall: 5% new, 95% old

    # Trailing Entry settings (Smart Entry)
    TRAILING_ENTRY_PULLBACK: float = 0.2  # Z-score pullback for reversal confirmation
    TRAILING_ENTRY_PULLBACK_EXTREME: float = (
        0.6  # Z-score pullback for extreme signals (when |Z| > z_sl)
    )
    TRAILING_ENTRY_TIMEOUT_MINUTES: int = 45  # Max watch duration before cancellation
    FALSE_ALARM_HYSTERESIS: float = (
        0.2  # Cancel watch only if Z drops this much below threshold
    )
    Z_EXTREME_LEVEL: float = 5.0  # Maximum Z-score to allow entry (replaces z_sl check)

    # Trailing Stop Loss settings (smart SL that follows favorable moves)
    # When Z-score moves in our favor, we tighten the SL to lock in gains
    # Example: Entry at Z=3.0, activation=1.0 → trail starts when Z drops to 2.0
    # If Z drops to 1.0, new SL = max(entry_threshold, 1.0 + 1.5) = 2.5
    TRAILING_SL_OFFSET: float = 1.0  # Offset from min_z_reached for new SL
    TRAILING_SL_ACTIVATION: float = (
        1.0  # Min Z recovery from entry before trailing SL activates
    )

    # Position sizing
    POSITION_SIZE_USDT: float = 1000.0  # USDT размер позиции на COIN ногу

    # Trading settings
    ALLOW_TRADING: bool = True  # Enable/disable real trading
    MAX_OPEN_SPREADS: int = 6  # Maximum number of open spread positions

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
    EXCHANGE_DEFAULT_LEVERAGE: int = 5
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
    LOKI_APP_NAME: str = "sigma-core"

    # MongoDB Settings
    MONGODB_URI: str = "mongodb://localhost:27017"
    MONGODB_DATABASE: str = "sigma-core"

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
        # Correlation hysteresis
        self.CORRELATION_EXIT_THRESHOLD = float(
            os.getenv("CORRELATION_EXIT_THRESHOLD", "0.70")
        )
        self.CORRELATION_WATCH_THRESHOLD = float(
            os.getenv("CORRELATION_WATCH_THRESHOLD", "0.75")
        )
        self.Z_ENTRY_THRESHOLD = float(os.getenv("Z_ENTRY_THRESHOLD", "2.0"))
        self.Z_TP_THRESHOLD = float(os.getenv("Z_TP_THRESHOLD", "0.25"))
        self.Z_SL_THRESHOLD = float(os.getenv("Z_SL_THRESHOLD", "0"))
        self.Z_SL_EXTREME_OFFSET = float(os.getenv("Z_SL_EXTREME_OFFSET", "0.5"))
        self.Z_SCORE_PROGRESS_EXIT_THRESHOLD = float(
            os.getenv("Z_SCORE_PROGRESS_EXIT_THRESHOLD", "0.30")
        )
        # Beta
        self.MIN_BETA = float(os.getenv("MIN_BETA", "0.5"))
        self.MAX_BETA = float(os.getenv("MAX_BETA", "2"))
        # Hurst
        self.HURST_THRESHOLD = float(os.getenv("HURST_THRESHOLD", "0.45"))
        self.HURST_WATCH_THRESHOLD = float(os.getenv("HURST_WATCH_THRESHOLD", "0.46"))
        self.HURST_TRENDING_FOR_EXIT = float(
            os.getenv("HURST_TRENDING_FOR_EXIT", "0.47")
        )
        self.HURST_TRENDING_CONFIRM_SCANS = int(
            os.getenv(
                "HURST_TRENDING_CONFIRM_SCANS",
                os.getenv("HURST_CONFIRM_SCANS", "2"),
            )
        )
        # Backward-compatible alias for legacy code paths.
        self.HURST_CONFIRM_SCANS = self.HURST_TRENDING_CONFIRM_SCANS
        self.HURST_LOOKBACK_CANDLES = int(os.getenv("HURST_LOOKBACK_CANDLES", "300"))
        # ADF
        self.ADF_PVALUE_THRESHOLD = float(os.getenv("ADF_PVALUE_THRESHOLD", "0.05"))
        self.ADF_LOOKBACK_CANDLES = int(os.getenv("ADF_LOOKBACK_CANDLES", "300"))
        self.ADF_EXIT_CONFIRM_SCANS = int(os.getenv("ADF_EXIT_CONFIRM_SCANS", "2"))
        # Half-Life
        self.HALFLIFE_MAX_BARS = float(os.getenv("HALFLIFE_MAX_BARS", "48.0"))
        self.HALFLIFE_LOOKBACK_CANDLES = int(
            os.getenv("HALFLIFE_LOOKBACK_CANDLES", "300")
        )
        self.HALFLIFE_EXIT_CONFIRM_SCANS = int(
            os.getenv("HALFLIFE_EXIT_CONFIRM_SCANS", "2")
        )

        # Dynamic Position Sizing based on Half-Life
        self.TARGET_HALFLIFE_BARS = float(os.getenv("TARGET_HALFLIFE_BARS", "12.0"))
        self.MIN_SIZE_MULTIPLIER = float(os.getenv("MIN_SIZE_MULTIPLIER", "0.5"))
        self.MAX_SIZE_MULTIPLIER = float(os.getenv("MAX_SIZE_MULTIPLIER", "2.0"))

        # Trailing Stop Loss
        self.TRAILING_SL_OFFSET = float(os.getenv("TRAILING_SL_OFFSET", "1.3"))
        self.TRAILING_SL_ACTIVATION = float(os.getenv("TRAILING_SL_ACTIVATION", "1.4"))
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
        # Asymmetric EMA for threshold smoothing
        self.THRESHOLD_EMA_ALPHA_UP = float(os.getenv("THRESHOLD_EMA_ALPHA_UP", "0.01"))
        self.THRESHOLD_EMA_ALPHA_DOWN = float(
            os.getenv("THRESHOLD_EMA_ALPHA_DOWN", "0.05")
        )

        # Trailing Entry (Smart Entry)
        self.TRAILING_ENTRY_PULLBACK = float(
            os.getenv("TRAILING_ENTRY_PULLBACK", "0.2")
        )
        self.TRAILING_ENTRY_PULLBACK_EXTREME = float(
            os.getenv("TRAILING_ENTRY_PULLBACK_EXTREME", "0.6")
        )
        self.TRAILING_ENTRY_TIMEOUT_MINUTES = int(
            os.getenv("TRAILING_ENTRY_TIMEOUT_MINUTES", "60")
        )
        self.FALSE_ALARM_HYSTERESIS = float(os.getenv("FALSE_ALARM_HYSTERESIS", "0.3"))
        self.Z_EXTREME_LEVEL = float(os.getenv("Z_EXTREME_LEVEL", "5.0"))

        # Position sizing
        self.POSITION_SIZE_USDT = float(os.getenv("POSITION_SIZE_USDT", "1000.0"))

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
            os.getenv("EXCHANGE_DEFAULT_LEVERAGE", "5")
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
        self.MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "sigma-core")

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
        logger.info(
            f"  CORRELATION_EXIT_THRESHOLD: {self.CORRELATION_EXIT_THRESHOLD} (hysteresis for positions)"
        )
        logger.info(
            f"  CORRELATION_WATCH_THRESHOLD: {self.CORRELATION_WATCH_THRESHOLD} (hysteresis for watches)"
        )
        logger.info("-" * 40)
        logger.info("📈 Z-Score Settings:")
        logger.info(f"  Z_ENTRY_THRESHOLD: {self.Z_ENTRY_THRESHOLD}")
        logger.info(f"  Z_TP_THRESHOLD: {self.Z_TP_THRESHOLD}")
        logger.info(f"  Z_SL_THRESHOLD: {self.Z_SL_THRESHOLD}")
        logger.info(f"  ADAPTIVE_PERCENTILE: {self.ADAPTIVE_PERCENTILE}")
        logger.info(
            f"  DYNAMIC_THRESHOLD_WINDOW_BARS: {self.DYNAMIC_THRESHOLD_WINDOW_BARS}"
        )
        logger.info(
            f"  THRESHOLD_EMA_ALPHA: ↑{self.THRESHOLD_EMA_ALPHA_UP} / ↓{self.THRESHOLD_EMA_ALPHA_DOWN}"
        )
        logger.info(
            f"  Z_SCORE_PROGRESS_EXIT_THRESHOLD: {self.Z_SCORE_PROGRESS_EXIT_THRESHOLD}"
        )
        logger.info("-" * 40)
        logger.info("📉 Beta Settings:")
        logger.info(f"  MIN_BETA: {self.MIN_BETA}")
        logger.info(f"  MAX_BETA: {self.MAX_BETA}")
        logger.info("-" * 40)
        logger.info("🔬 Hurst Settings:")
        logger.info(f"  HURST_THRESHOLD: {self.HURST_THRESHOLD}")
        logger.info(
            f"  HURST_WATCH_THRESHOLD: {self.HURST_WATCH_THRESHOLD} (hold threshold: {self.HURST_WATCH_THRESHOLD})"
        )
        logger.info(f"  HURST_TRENDING_FOR_EXIT: {self.HURST_TRENDING_FOR_EXIT}")
        logger.info(
            f"  HURST_TRENDING_CONFIRM_SCANS: {self.HURST_TRENDING_CONFIRM_SCANS}"
        )
        logger.info(f"  HURST_LOOKBACK_CANDLES: {self.HURST_LOOKBACK_CANDLES}")
        logger.info("-" * 40)
        logger.info("📊 ADF Settings:")
        logger.info(f"  ADF_PVALUE_THRESHOLD: {self.ADF_PVALUE_THRESHOLD}")
        logger.info(f"  ADF_LOOKBACK_CANDLES: {self.ADF_LOOKBACK_CANDLES}")
        logger.info(f"  ADF_EXIT_CONFIRM_SCANS: {self.ADF_EXIT_CONFIRM_SCANS}")
        logger.info("-" * 40)
        logger.info("⏱️ Half-Life Settings:")
        logger.info(f"  HALFLIFE_MAX_BARS: {self.HALFLIFE_MAX_BARS}")
        logger.info(f"  HALFLIFE_LOOKBACK_CANDLES: {self.HALFLIFE_LOOKBACK_CANDLES}")
        logger.info(
            f"  HALFLIFE_EXIT_CONFIRM_SCANS: {self.HALFLIFE_EXIT_CONFIRM_SCANS}"
        )
        logger.info(f"  TARGET_HALFLIFE_BARS: {self.TARGET_HALFLIFE_BARS}")
        logger.info(
            f"  SIZE_MULTIPLIER: {self.MIN_SIZE_MULTIPLIER}x - {self.MAX_SIZE_MULTIPLIER}x"
        )
        logger.info("-" * 40)
        logger.info("🎯 Trailing SL Settings:")
        logger.info(f"  TRAILING_SL_OFFSET: {self.TRAILING_SL_OFFSET}")
        logger.info(f"  TRAILING_SL_ACTIVATION: {self.TRAILING_SL_ACTIVATION}")
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
