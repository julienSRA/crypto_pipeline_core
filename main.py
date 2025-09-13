#!/usr/bin/env python3
"""
Main entrypoint for the pipeline.
Runs all collectors, then generates report + CSV export.
"""

import logging
import sqlite3
from pathlib import Path

# Import collectors explicit
from pipeline.collectors import coingecko, defillama, sopr, bybit, mempool, altme

# Reporter + Exporter
from pipeline import reporter, exporter

DB_PATH = Path("data/crypto.db")
LOG = logging.getLogger("pipeline.main")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def migrate(conn: sqlite3.Connection):
    """Create all required tables (safe to run repeatedly)."""
    def ensure_table(ddl: str, name: str):
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        LOG.info("Ensured table: %s", name)

    ensure_table("""
    CREATE TABLE IF NOT EXISTS metrics (
        ts INTEGER PRIMARY KEY,
        sopr REAL,
        stablecoins REAL,
        mempool_tx_count INTEGER,
        mempool_fee_fastest REAL,
        fng INTEGER,
        oi_btc REAL,
        oi_eth REAL,
        funding_btc REAL,
        funding_eth REAL
    )
    """, "metrics")

    ensure_table("""
    CREATE TABLE IF NOT EXISTS coingecko (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        price_usd REAL NOT NULL
    )
    """, "coingecko")

    ensure_table("""
    CREATE TABLE IF NOT EXISTS bybit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        funding REAL,
        open_interest REAL
    )
    """, "bybit")

    ensure_table("""
    CREATE TABLE IF NOT EXISTS sopr (
        ts INTEGER PRIMARY KEY,
        value REAL
    )
    """, "sopr")

    ensure_table("""
    CREATE TABLE IF NOT EXISTS altme (
        ts INTEGER PRIMARY KEY,
        fng INTEGER
    )
    """, "altme")

    ensure_table("""
    CREATE TABLE IF NOT EXISTS mempool (
        ts INTEGER PRIMARY KEY,
        tx_count INTEGER,
        fee_fastest REAL,
        fee_30m REAL
    )
    """, "mempool")

    ensure_table("""
    CREATE TABLE IF NOT EXISTS stablecoins (
        ts INTEGER PRIMARY KEY,
        total REAL,
        usdt REAL,
        usdc REAL
    )
    """, "stablecoins")

    ensure_table("""
    CREATE TABLE IF NOT EXISTS bybit_liquidations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        price REAL,
        qty REAL,
        qty_usd REAL,
        raw TEXT
    )
    """, "bybit_liquidations")

    ensure_table("""
    CREATE TABLE IF NOT EXISTS bybit_liquidations_hourly (
        hour_start INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        total_qty_usd REAL NOT NULL DEFAULT 0,
        events_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (hour_start, symbol, side)
    )
    """, "bybit_liquidations_hourly")

    ensure_table("""
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """, "meta")

    ensure_table("""
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        name TEXT NOT NULL,
        value REAL,
        extra TEXT
    )
    """, "signals")

    LOG.info("✅ Database migration completed at %s", DB_PATH)


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    migrate(conn)

    LOG.info("Starting collectors…")
    coingecko.collect(conn)
    defillama.collect(conn)
    sopr.collect(conn)
    bybit.collect(conn)
    mempool.collect(conn)
    altme.collect(conn)
    LOG.info("✅ All collectors completed.")

    # Reporter
    LOG.info("📊 Generating report…")
    reporter.run(conn)

    # Exporter
    LOG.info("💾 Exporting latest data to CSV…")
    exporter.run(conn)

    conn.close()
    LOG.info("🏁 Pipeline run complete.")


if __name__ == "__main__":
    main()
