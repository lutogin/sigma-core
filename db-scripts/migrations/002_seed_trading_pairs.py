#!/usr/bin/env python3
"""
Seed Trading Pairs Migration.

Seeds the trading_pairs collection in MongoDB with initial pairs from settings.
Can be run multiple times safely (uses upsert).

Usage:
    python db-scripts/migrations/002_seed_trading_pairs.py

Environment variables:
    MONGODB_URI: MongoDB connection string (default: mongodb://localhost:27017)
    MONGODB_DATABASE: Database name (default: sigma-core)
"""

import os
import sys
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_settings
from src.infra.mongo import MongoDatabase
from src.domain.trading_pairs import TradingPair, TradingPairRepository


def main():
    """Seed trading pairs from settings to MongoDB."""
    print("=" * 60)
    print("Trading Pairs Migration - Seed from Config")
    print("=" * 60)

    # Load settings
    settings = load_settings()

    print(f"MongoDB URI: {settings.MONGODB_URI}")
    print(f"MongoDB Database: {settings.MONGODB_DATABASE}")
    print(f"Pairs to seed: {len(settings.CONSISTENT_PAIRS)}")
    print()

    # Simple logger for the repository
    class SimpleLogger:
        def info(self, msg):
            print(f"[INFO] {msg}")

        def debug(self, msg):
            pass  # Suppress debug logs

        def warning(self, msg):
            print(f"[WARN] {msg}")

        def error(self, msg):
            print(f"[ERROR] {msg}")

    logger = SimpleLogger()

    # Connect to MongoDB
    mongo_db = MongoDatabase(
        uri=settings.MONGODB_URI,
        database_name=settings.MONGODB_DATABASE,
        logger=logger,
    )

    try:
        mongo_db.connect()

        # Create repository
        repo = TradingPairRepository(mongo_db=mongo_db, logger=logger)

        # Create indexes first
        repo.create_indexes()

        # Show existing pairs
        existing = repo.get_all()
        print(f"Existing pairs in database: {len(existing)}")
        for pair in existing:
            status = "✅ active" if pair.is_active else "❌ inactive"
            print(f"  - {pair.symbol} ({status})")
        print()

        # Seed from settings
        print("Seeding pairs from config...")
        count = repo.seed_from_list(
            symbols=settings.CONSISTENT_PAIRS,
            ecosystem="ETH",
        )
        print(f"Upserted {count} pairs")
        print()

        # Show final state
        final = repo.get_active_symbols()
        print(f"Active pairs after migration: {len(final)}")
        for symbol in final:
            print(f"  - {symbol}")

        print()
        print("=" * 60)
        print("Migration completed successfully!")
        print("=" * 60)

    finally:
        mongo_db.disconnect()


if __name__ == "__main__":
    main()

