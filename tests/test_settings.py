from src.config.settings import Settings


def test_settings_have_fail_closed_balanced_defaults(monkeypatch) -> None:
    keys = [
        "ALLOW_TRADING",
        "ADF_PVALUE_THRESHOLD",
        "CONSISTENT_PAIRS",
        "EXCHANGE_DEFAULT_LEVERAGE",
        "FALSE_ALARM_HYSTERESIS",
        "HURST_TRENDING_FOR_EXIT",
        "MAX_COIN_NOTIONAL_PCT",
        "MAX_FUNDING_COST_THRESHOLD",
        "MAX_MARGIN_UTILIZATION",
        "MAX_OPEN_SPREADS",
        "MAX_POSITION_BARS",
        "MAX_SIZE_MULTIPLIER",
        "SCAN_CRON_EXPRESSION",
        "SIGMA_HEALTH_FILE",
        "SIGMA_HEALTH_MAX_AGE_SECONDS",
        "TARGET_HALFLIFE_BARS",
        "TRAILING_ENTRY_PULLBACK",
        "TRAILING_ENTRY_TIMEOUT_MINUTES",
        "VOLATILITY_THRESHOLD",
        "Z_EXTREME_LEVEL",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)

    settings = Settings()

    assert settings.ALLOW_TRADING is False
    assert settings.EXCHANGE_DEFAULT_LEVERAGE == 5
    assert settings.MAX_OPEN_SPREADS == 3
    assert settings.MAX_COIN_NOTIONAL_PCT == 0.10
    assert settings.MAX_MARGIN_UTILIZATION == 0.50
    assert settings.MAX_POSITION_BARS == 96
    assert settings.MAX_FUNDING_COST_THRESHOLD == -0.0005
    assert settings.ADF_PVALUE_THRESHOLD == 0.05
    assert settings.HURST_TRENDING_FOR_EXIT == 0.47
    assert settings.VOLATILITY_THRESHOLD == 0.012
    assert settings.Z_EXTREME_LEVEL == 5.0
    assert settings.TARGET_HALFLIFE_BARS == 12.0
    assert settings.MAX_SIZE_MULTIPLIER == 1.25
    assert settings.TRAILING_ENTRY_PULLBACK == 0.2
    assert settings.TRAILING_ENTRY_TIMEOUT_MINUTES == 90
    assert settings.FALSE_ALARM_HYSTERESIS == 0.45
    assert settings.SCAN_CRON_EXPRESSION == "*/15 * * * *"
    assert settings.SIGMA_HEALTH_FILE == "/tmp/sigma-core-health"
    assert settings.SIGMA_HEALTH_MAX_AGE_SECONDS == 1800
    assert settings.CONSISTENT_PAIRS == Settings.CONSISTENT_PAIRS


def test_consistent_pairs_accept_comma_separated_env_without_spaces(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "CONSISTENT_PAIRS",
        "LINK/USDT:USDT,UNI/USDT:USDT, LINK/USDT:USDT",
    )

    settings = Settings()

    assert settings.CONSISTENT_PAIRS == [
        "LINK/USDT:USDT",
        "UNI/USDT:USDT",
    ]
