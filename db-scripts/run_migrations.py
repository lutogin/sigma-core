#!/usr/bin/env python3
"""
Run database migrations for TimescaleDB.

Usage:
    python scripts/run_migrations.py

Environment:
    TIMESCALE_DB_URL - TimescaleDB connection string (required)
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import load_settings
from src.infra.timescale import TimescaleDB
from loguru import logger


def run_migrations():
    """Run all SQL migrations in order."""
    # Load settings
    settings = load_settings()

    # Connect to database
    db = TimescaleDB(settings, logger)

    try:
        db.connect()

        # Get migrations directory
        migrations_dir = Path(__file__).parent / "migrations"

        if not migrations_dir.exists():
            logger.error(f"Migrations directory not found: {migrations_dir}")
            sys.exit(1)

        # Get all SQL files sorted by name
        migration_files = sorted(migrations_dir.glob("*.sql"))

        if not migration_files:
            logger.info("No migration files found")
            return

        logger.info(f"Found {len(migration_files)} migration(s)")

        # Run each migration
        for migration_file in migration_files:
            logger.info(f"Running migration: {migration_file.name}")

            try:
                with open(migration_file, "r") as f:
                    sql = f.read()

                db.run_migration(sql)
                logger.info(f"✅ {migration_file.name} completed")

            except Exception as e:
                logger.error(f"❌ {migration_file.name} failed: {e}")
                raise

        logger.info("All migrations completed successfully! 🎉")

        # Verify hypertable
        if db.is_hypertable("ohlcv"):
            logger.info("✅ OHLCV hypertable verified")
        else:
            logger.warning("⚠️ OHLCV is not a hypertable. TimescaleDB extension may not be installed.")

    finally:
        db.close()


if __name__ == "__main__":
    print("Script started!", flush=True)
    run_migrations()
    print("Script finished!", flush=True)

