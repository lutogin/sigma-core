import numpy as np
import pandas as pd
import pytest

from src.domain.screener.correlation import CorrelationService
from src.domain.screener.statistics import benjamini_hochberg_passes
from src.domain.screener.z_score import ZScoreService


class NullLogger:
    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


def _synthetic_cointegrated_prices(
    *,
    periods: int = 600,
    beta: float = 1.25,
    intercept: float = -2.0,
) -> tuple[dict[str, pd.DataFrame], float, float]:
    rng = np.random.default_rng(42)
    primary_log = 8.0 + np.cumsum(rng.normal(0.0, 0.004, periods))
    residual = np.zeros(periods)
    for idx in range(1, periods):
        residual[idx] = 0.82 * residual[idx - 1] + rng.normal(0.0, 0.001)
    coin_log = intercept + beta * primary_log + residual
    index = pd.date_range("2025-01-01", periods=periods, freq="1h", tz="UTC")
    return (
        {
            "ETH/USDT:USDT": pd.DataFrame(
                {"close": np.exp(primary_log)},
                index=index,
            ),
            "COIN/USDT:USDT": pd.DataFrame(
                {"close": np.exp(coin_log)},
                index=index,
            ),
        },
        beta,
        intercept,
    )


def test_pair_model_uses_one_level_regression_for_beta_and_live_z() -> None:
    prices, true_beta, true_intercept = _synthetic_cointegrated_prices()
    correlation = CorrelationService(
        logger=NullLogger(),
        lookback_window_days=4,
        timeframe="1h",
    )
    correlation_results = correlation.calculate("ETH/USDT:USDT", prices)
    model = correlation_results["COIN/USDT:USDT"]

    assert model.latest_beta == pytest.approx(true_beta, abs=0.08)
    assert model.latest_intercept == pytest.approx(true_intercept, abs=0.65)
    assert model.latest_residual_std > 0

    z_scores = ZScoreService(
        logger=NullLogger(),
        lookback_window_days=4,
        timeframe="1h",
        dynamic_threshold_window=100,
    ).calculate("ETH/USDT:USDT", correlation_results, prices)
    result = z_scores["COIN/USDT:USDT"]

    live_z = (
        np.log(prices["COIN/USDT:USDT"]["close"].iloc[-1])
        - model.latest_beta * np.log(prices["ETH/USDT:USDT"]["close"].iloc[-1])
        - model.latest_intercept
    ) / model.latest_residual_std
    assert result.current_z_score == pytest.approx(live_z)


def test_pair_model_does_not_revise_past_estimates_from_future_price() -> None:
    prices, _, _ = _synthetic_cointegrated_prices()
    service = CorrelationService(
        logger=NullLogger(),
        lookback_window_days=4,
        timeframe="1h",
    )
    before = service.calculate("ETH/USDT:USDT", prices)["COIN/USDT:USDT"]

    changed = {symbol: frame.copy() for symbol, frame in prices.items()}
    changed["COIN/USDT:USDT"].iloc[-1, 0] *= 3
    after = service.calculate("ETH/USDT:USDT", changed)["COIN/USDT:USDT"]

    pd.testing.assert_series_equal(
        before.rolling_beta.iloc[:-1],
        after.rolling_beta.iloc[:-1],
    )
    assert before.rolling_beta.iloc[-1] != after.rolling_beta.iloc[-1]


def test_dynamic_threshold_is_restart_stable_and_excludes_current_signal() -> None:
    service = ZScoreService(
        logger=NullLogger(),
        lookback_window_days=1,
        timeframe="1h",
        z_entry_threshold=2.0,
        dynamic_threshold_window=100,
    )
    history = pd.Series(np.linspace(-2.5, 2.5, 200))
    positive_extreme = pd.concat([history, pd.Series([50.0])], ignore_index=True)
    negative_extreme = pd.concat([history, pd.Series([-50.0])], ignore_index=True)

    first = service._calculate_dynamic_threshold("COIN", positive_extreme)
    repeated = service._calculate_dynamic_threshold("COIN", positive_extreme)
    opposite = service._calculate_dynamic_threshold("COIN", negative_extreme)

    assert first == pytest.approx(repeated)
    assert first == pytest.approx(opposite)


def test_adf_multiple_testing_uses_benjamini_hochberg() -> None:
    accepted = benjamini_hochberg_passes(
        {
            "strong": 0.001,
            "also_strong": 0.008,
            "raw_only_false_discovery": 0.04,
            "weak": 0.30,
        },
        false_discovery_rate=0.05,
    )

    assert accepted == {"strong", "also_strong"}
