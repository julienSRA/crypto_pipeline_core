#!/usr/bin/env python3
# migrate.py
"""
Database migration / autorepair script.
Creates all required tables (safe to run repeatedly).
"""
import sqlite3
import logging
from pathlib import Path

DB_PATH = Path("data/crypto.db")
LOG = logging.getLogger("migrate")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

DDL = {
    "metrics": """
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
    );
    """,
    "coingecko": """
    CREATE TABLE IF NOT EXISTS coingecko (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        price_usd REAL NOT NULL
    );
    """,
    "bybit": """
    CREATE TABLE IF NOT EXISTS bybit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        funding REAL,
        open_interest REAL
    );
    """,
    "sopr": """
    CREATE TABLE IF NOT EXISTS sopr (
        ts INTEGER PRIMARY KEY,
        value REAL
    );
    """,
    "altme": """
    CREATE TABLE IF NOT EXISTS altme (
        ts INTEGER PRIMARY KEY,
        fng INTEGER
    );
    """,
    "mempool": """
    CREATE TABLE IF NOT EXISTS mempool (
        ts INTEGER PRIMARY KEY,
        tx_count INTEGER,
        fee_fastest REAL,
        fee_30m REAL
    );
    """,
    "stablecoins": """
    CREATE TABLE IF NOT EXISTS stablecoins (
        ts INTEGER PRIMARY KEY,
        total REAL,
        usdt REAL,
        usdc REAL
    );
    """,
    # raw liquidations events (detailed)
    "bybit_liquidations": """
    CREATE TABLE IF NOT EXISTS bybit_liquidations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,               -- epoch seconds UTC
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        price REAL,
        qty REAL,
        qty_usd REAL,
        raw TEXT
    );
    """,
    # hourly aggregates for fast reporting
    "bybit_liquidations_hourly": """
    CREATE TABLE IF NOT EXISTS bybit_liquidations_hourly (
        hour_start INTEGER NOT NULL,  -- epoch seconds aligned to hour (UTC)
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        total_qty_usd REAL NOT NULL DEFAULT 0,
        events_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (hour_start, symbol, side)
    );
    """,
    # meta table
    "meta": """
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """,
    # signals table (option A: primary key on (ts, name))
    "signals": """
    CREATE TABLE IF NOT EXISTS signals (
        ts INTEGER NOT NULL,
        name TEXT NOT NULL,
        value REAL,
        classification TEXT,
        PRIMARY KEY (ts, name)
    );
    """
}

def ensure_table(conn: sqlite3.Connection, ddl: str, name: str):
    cur = conn.cursor()
    cur.execute(ddl)
    conn.commit()
    LOG.info("Ensured table: %s", name)

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    for name, ddl in DDL.items():
        ensure_table(conn, ddl, name)
    conn.close()
    LOG.info("✅ Database migration completed at %s", DB_PATH)

if __name__ == "__main__":
    main()
