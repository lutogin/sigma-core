#!/usr/bin/env python3
"""
Test script for Hurst exponent calculation.

Usage:
    python test_hurst.py
"""

import numpy as np
import pandas as pd
import sys

# Add project root to path
sys.path.insert(0, ".")

from loguru import logger
from src.domain.screener.hurst_filter.hurst_filter import HurstFilterService


def generate_mean_reverting_series(n: int = 500, theta: float = 0.5) -> np.ndarray:
    """Generate Ornstein-Uhlenbeck (mean-reverting) process."""
    series = np.zeros(n)
    series[0] = 0
    for i in range(1, n):
        series[i] = series[i - 1] - theta * series[i - 1] + np.random.randn() * 0.1
    return series


def generate_trending_series(n: int = 500, drift: float = 0.01) -> np.ndarray:
    """Generate trending (random walk with drift) series."""
    series = np.cumsum(np.random.randn(n) * 0.1 + drift)
    return series


def generate_random_walk(n: int = 500) -> np.ndarray:
    """Generate pure random walk (H ~ 0.5)."""
    return np.cumsum(np.random.randn(n) * 0.1)


def test_synthetic_series():
    """Test Hurst calculation on synthetic series."""
    print("\n" + "=" * 60)
    print("Testing Hurst Exponent on Synthetic Series")
    print("=" * 60)

    hurst_service = HurstFilterService(logger=logger)

    # Test 1: Mean-reverting series (should have H < 0.5)
    print("\n1. Mean-Reverting Series (Ornstein-Uhlenbeck)")
    print("-" * 40)
    for theta in [0.1, 0.3, 0.5]:
        series = generate_mean_reverting_series(n=500, theta=theta)
        spread = pd.Series(series)
        h = hurst_service.calculate(spread)
        status = "✅ PASS" if h < 0.5 else "❌ FAIL"
        print(f"  θ={theta}: H = {h:.4f} {status} (expected H < 0.5)")

    # Test 2: Trending series (should have H > 0.5)
    print("\n2. Trending Series (Random Walk with Drift)")
    print("-" * 40)
    for drift in [0.01, 0.02, 0.05]:
        series = generate_trending_series(n=500, drift=drift)
        spread = pd.Series(series)
        h = hurst_service.calculate(spread)
        status = "✅ PASS" if h > 0.5 else "❌ FAIL"
        print(f"  drift={drift}: H = {h:.4f} {status} (expected H > 0.5)")

    # Test 3: Random walk (should have H ~ 0.5)
    print("\n3. Pure Random Walk")
    print("-" * 40)
    for i in range(3):
        series = generate_random_walk(n=500)
        spread = pd.Series(series)
        h = hurst_service.calculate(spread)
        status = "✅ OK" if 0.4 < h < 0.6 else "⚠️ CHECK"
        print(f"  run {i+1}: H = {h:.4f} {status} (expected H ~ 0.5)")

    print("\n" + "=" * 60)


def test_spread_calculation():
    """Test Hurst on a simulated spread."""
    print("\n" + "=" * 60)
    print("Testing Hurst on Simulated Spread")
    print("=" * 60)

    hurst_service = HurstFilterService(logger=logger)

    # Simulate COIN and PRIMARY prices
    n = 500
    primary_log = np.cumsum(np.random.randn(n) * 0.02)  # ETH log prices

    # Mean-reverting coin (correlated but mean-reverting spread)
    beta = 0.8
    noise = generate_mean_reverting_series(n=n, theta=0.3)
    coin_log = beta * primary_log + noise

    # Calculate spread
    spread = pd.Series(coin_log - beta * primary_log)

    print(f"\nSpread stats:")
    print(f"  Mean: {spread.mean():.4f}")
    print(f"  Std:  {spread.std():.4f}")
    print(f"  Min:  {spread.min():.4f}")
    print(f"  Max:  {spread.max():.4f}")

    h = hurst_service.calculate(spread)
    is_mr = hurst_service.is_mean_reverting(h)

    print(f"\nHurst exponent: {h:.4f}")
    print(f"Threshold: {hurst_service.threshold}")
    print(f"Is mean-reverting: {is_mr}")

    # Now test via calculate_for_spread
    print("\n\nTesting calculate_for_spread method:")
    print("-" * 40)

    h2 = hurst_service.calculate_for_spread(
        coin_log_prices=pd.Series(coin_log),
        primary_log_prices=pd.Series(primary_log),
        beta=beta,
    )
    print(f"Hurst (via calculate_for_spread): {h2:.4f}")

    print("\n" + "=" * 60)


def test_edge_cases():
    """Test edge cases."""
    print("\n" + "=" * 60)
    print("Testing Edge Cases")
    print("=" * 60)

    hurst_service = HurstFilterService(logger=logger)

    # Test with short series
    print("\n1. Short series (less than 100 points)")
    short_series = pd.Series(np.random.randn(50))
    h = hurst_service.calculate(short_series)
    print(f"  Result: H = {h} (expected 0.5 fallback)")

    # Test with NaN values
    print("\n2. Series with NaN values")
    series_with_nan = pd.Series(np.random.randn(200))
    series_with_nan.iloc[50:70] = np.nan
    h = hurst_service.calculate(series_with_nan)
    print(f"  Result: H = {h:.4f}")

    # Test with constant series
    print("\n3. Constant series")
    constant_series = pd.Series(np.ones(200) * 1.5)
    h = hurst_service.calculate(constant_series)
    print(f"  Result: H = {h:.4f}")

    print("\n" + "=" * 60)


def main():
    print("\n" + "=" * 60)
    print("       HURST EXPONENT TEST SUITE")
    print("=" * 60)

    test_synthetic_series()
    test_spread_calculation()
    test_edge_cases()

    print("\n✅ All tests completed!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
