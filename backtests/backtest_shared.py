"""
Shared helpers for backtest runners.

This module centralizes:
1. BacktestConfig kwargs construction from settings/CLI-like inputs.
2. Backtester service bundle construction.

Both `run_backtest.py` and `run_universe_walk_forward.py` should use these
helpers to avoid logic drift.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Optional, Sequence, Tuple

from src.domain.data_loader import AsyncDataLoaderService
from src.domain.screener.adf_filter import ADFFilterService
from src.domain.screener.correlation import CorrelationService
from src.domain.screener.halflife_filter import HalfLifeFilterService
from src.domain.screener.hurst_filter import HurstFilterService
from src.domain.screener.volatility_filter import VolatilityFilterService
from src.domain.screener.z_score import ZScoreService
from src.domain.utils import get_timeframe_minutes


def compute_trailing_pullback_calibration(
    *,
    timeframe: str,
    trailing_pullback_base: float,
    trailing_pullback_extreme_base: float,
) -> Tuple[float, float, float]:
    """
    Scale trailing pullback thresholds for 1m pseudo-tick emulation.

    Returns:
        (trailing_pullback, trailing_pullback_extreme, pullback_scale)
    """
    timeframe_minutes = max(1, get_timeframe_minutes(timeframe))
    pullback_scale = math.sqrt(1.0 / timeframe_minutes)
    trailing_pullback = math.floor(trailing_pullback_base * pullback_scale * 100) / 100
    trailing_pullback_extreme = (
        math.floor(trailing_pullback_extreme_base * pullback_scale * 100) / 100
    )
    return trailing_pullback, trailing_pullback_extreme, pullback_scale


def build_backtest_config_kwargs(
    *,
    settings: Any,
    initial_balance: float,
    position_size_pct: float,
    position_size_usdt: float,
    max_spreads: int,
    leverage: int,
    consistent_pairs: Sequence[str],
    use_funding_filter: bool,
    use_trailing_entry: Optional[bool] = None,
    use_live_exit: Optional[bool] = None,
    use_dynamic_tp: Optional[bool] = None,
    lazy_load_minute_data: Optional[bool] = None,
    use_adf_filter: Optional[bool] = None,
    extra_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build kwargs for BacktestConfig with consistent defaults across runners.
    """
    trailing_pullback, trailing_pullback_extreme, _ = compute_trailing_pullback_calibration(
        timeframe=settings.TIMEFRAME,
        trailing_pullback_base=settings.TRAILING_ENTRY_PULLBACK,
        trailing_pullback_extreme_base=settings.TRAILING_ENTRY_PULLBACK_EXTREME,
    )

    kwargs: Dict[str, Any] = {
        "initial_balance": initial_balance,
        "position_size_pct": position_size_pct,
        "position_size_usdt": position_size_usdt,
        "max_spreads": max_spreads,
        "leverage": leverage,
        "z_entry_threshold": settings.Z_ENTRY_THRESHOLD,
        "z_tp_threshold": settings.Z_TP_THRESHOLD,
        "z_sl_threshold": settings.Z_SL_THRESHOLD,
        "min_correlation": settings.MIN_CORRELATION,
        "min_beta": settings.MIN_BETA,
        "max_beta": settings.MAX_BETA,
        "correlation_exit_threshold": settings.CORRELATION_EXIT_THRESHOLD,
        "correlation_watch_threshold": settings.CORRELATION_WATCH_THRESHOLD,
        "cooldown_bars": settings.COOLDOWN_BARS,
        "max_position_bars": settings.MAX_POSITION_BARS,
        "hurst_trending_for_exit": settings.HURST_TRENDING_FOR_EXIT,
        "hurst_confirm_scans": settings.HURST_TRENDING_CONFIRM_SCANS,
        "adf_exit_confirm_scans": settings.ADF_EXIT_CONFIRM_SCANS,
        "halflife_exit_confirm_scans": settings.HALFLIFE_EXIT_CONFIRM_SCANS,
        "halflife_max_bars": settings.HALFLIFE_MAX_BARS,
        "enable_beta_drift_guard": settings.ENABLE_BETA_DRIFT_GUARD,
        "beta_drift_short_days": settings.BETA_DRIFT_SHORT_DAYS,
        "beta_drift_long_days": settings.BETA_DRIFT_LONG_DAYS,
        "beta_drift_max_relative": settings.BETA_DRIFT_MAX_RELATIVE,
        "enable_stability_filter": settings.ENABLE_STABILITY_FILTER,
        "stability_windows_days": settings.STABILITY_WINDOWS_DAYS,
        "stability_min_pass_windows": settings.STABILITY_MIN_PASS_WINDOWS,
        "use_dynamic_position_size": True,
        "target_halflife_bars": settings.TARGET_HALFLIFE_BARS,
        "halflife_multiplier_min": settings.MIN_SIZE_MULTIPLIER,
        "halflife_multiplier_max": settings.MAX_SIZE_MULTIPLIER,
        "use_trailing_sl": True,
        "trailing_sl_offset": settings.TRAILING_SL_OFFSET,
        "trailing_sl_activation": settings.TRAILING_SL_ACTIVATION,
        "z_extreme_level": settings.Z_EXTREME_LEVEL,
        "z_sl_extreme_offset": settings.Z_SL_EXTREME_OFFSET,
        "consistent_pairs": list(consistent_pairs),
        "use_funding_filter": use_funding_filter,
        "max_funding_cost_threshold": settings.MAX_FUNDING_COST_THRESHOLD,
        "use_adf_filter": True if use_adf_filter is None else use_adf_filter,
        "adf_pvalue_threshold": settings.ADF_PVALUE_THRESHOLD,
        "adf_lookback_candles": settings.ADF_LOOKBACK_CANDLES,
        "trailing_pullback": trailing_pullback,
        "trailing_pullback_extreme": trailing_pullback_extreme,
        "trailing_timeout_minutes": settings.TRAILING_ENTRY_TIMEOUT_MINUTES,
        "false_alarm_hysteresis": settings.FALSE_ALARM_HYSTERESIS,
    }

    if use_trailing_entry is not None:
        kwargs["use_trailing_entry"] = use_trailing_entry
    if use_live_exit is not None:
        kwargs["use_live_exit"] = use_live_exit
    if use_dynamic_tp is not None:
        kwargs["use_dynamic_tp"] = use_dynamic_tp
    if lazy_load_minute_data is not None:
        kwargs["lazy_load_minute_data"] = lazy_load_minute_data
    if extra_overrides:
        kwargs.update(extra_overrides)

    return kwargs


def build_backtest_services(
    *,
    settings: Any,
    logger: Any,
    exchange_client: Any,
    ohlcv_repository: Any,
    config: Any,
    data_loader_override: Optional[Any] = None,
    funding_cache: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Build StatArbBacktest dependency bundle.

    Returns kwargs ready to be splatted into `StatArbBacktest(...)`.
    """
    data_loader = data_loader_override or AsyncDataLoaderService(
        logger=logger,
        exchange_client=exchange_client,
        ohlcv_repository=ohlcv_repository,
    )

    correlation_service = CorrelationService(
        logger=logger,
        lookback_window_days=settings.LOOKBACK_WINDOW_DAYS,
        timeframe=settings.TIMEFRAME,
    )

    z_score_service = ZScoreService(
        logger=logger,
        lookback_window_days=settings.LOOKBACK_WINDOW_DAYS,
        timeframe=settings.TIMEFRAME,
        z_entry_threshold=settings.Z_ENTRY_THRESHOLD,
        z_tp_threshold=settings.Z_TP_THRESHOLD,
        z_sl_threshold=settings.Z_SL_THRESHOLD,
        adaptive_percentile=settings.ADAPTIVE_PERCENTILE,
        dynamic_threshold_window=settings.DYNAMIC_THRESHOLD_WINDOW_BARS,
        threshold_ema_alpha_up=settings.THRESHOLD_EMA_ALPHA_UP,
        threshold_ema_alpha_down=settings.THRESHOLD_EMA_ALPHA_DOWN,
    )

    volatility_filter_service = VolatilityFilterService(
        logger=logger,
        primary_pair=settings.PRIMARY_PAIR,
        timeframe=settings.TIMEFRAME,
        volatility_window=settings.VOLATILITY_WINDOW,
        volatility_threshold=settings.VOLATILITY_THRESHOLD,
        crash_window=settings.VOLATILITY_CRASH_WINDOW,
        crash_threshold=settings.VOLATILITY_CRASH_THRESHOLD,
    )

    hurst_filter_service = HurstFilterService(
        logger=logger,
        hurst_threshold=settings.HURST_THRESHOLD,
        lookback_candles=settings.HURST_LOOKBACK_CANDLES,
    )

    adf_filter_service = ADFFilterService(
        logger=logger,
        pvalue_threshold=config.adf_pvalue_threshold or settings.ADF_PVALUE_THRESHOLD,
        lookback_candles=config.adf_lookback_candles
        or settings.ADF_LOOKBACK_CANDLES,
    )

    halflife_filter_service = HalfLifeFilterService(
        logger=logger,
        max_bars=config.halflife_max_bars or settings.HALFLIFE_MAX_BARS,
        lookback_candles=settings.HALFLIFE_LOOKBACK_CANDLES,
    )

    return {
        "settings": settings,
        "data_loader": data_loader,
        "correlation_service": correlation_service,
        "z_score_service": z_score_service,
        "volatility_filter_service": volatility_filter_service,
        "hurst_filter_service": hurst_filter_service,
        "adf_filter_service": adf_filter_service,
        "halflife_filter_service": halflife_filter_service,
        "funding_cache": funding_cache,
    }

