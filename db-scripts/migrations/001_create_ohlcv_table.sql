-- Migration: Create OHLCV hypertable for TimescaleDB
--
-- This creates a time-series optimized table for storing OHLCV data.
-- Supports multiple symbols and intervals.
--
-- Run with: python scripts/run_migrations.py

-- Enable TimescaleDB extension (if not already enabled)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create OHLCV table
CREATE TABLE IF NOT EXISTS ohlcv (
    -- Composite primary key: symbol + interval + timestamp
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,

    -- OHLCV data
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,

    -- Unique constraint
    UNIQUE (symbol, interval, timestamp)
);

-- Convert to hypertable (partitioned by time)
-- chunk_time_interval = 7 days (optimal for daily queries)
SELECT create_hypertable(
    'ohlcv',
    'timestamp',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Create indexes for efficient queries
-- Index for symbol + interval lookups (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_interval
ON ohlcv (symbol, interval, timestamp DESC);

-- Index for date range queries within a symbol
CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_time
ON ohlcv (symbol, timestamp DESC);

-- Enable compression for old data (older than 30 days)
-- This significantly reduces storage requirements
ALTER TABLE ohlcv SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol, interval'
);

-- Add compression policy: compress chunks older than 90 days
SELECT add_compression_policy('ohlcv', INTERVAL '90 days', if_not_exists => TRUE);

-- Create a retention policy (optional, comment out if you want to keep all data)
-- SELECT add_retention_policy('ohlcv', INTERVAL '2 years', if_not_exists => TRUE);

-- Grant permissions (adjust username as needed)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ohlcv TO alpha_bot_user;

COMMENT ON TABLE ohlcv IS 'OHLCV price data for cryptocurrency pairs. Partitioned by timestamp.';
COMMENT ON COLUMN ohlcv.symbol IS 'Trading pair symbol (e.g., BTC/USDT:USDT)';
COMMENT ON COLUMN ohlcv.interval IS 'Candle interval (e.g., 1h, 4h, 1d)';
COMMENT ON COLUMN ohlcv.timestamp IS 'Candle open timestamp (UTC)';

