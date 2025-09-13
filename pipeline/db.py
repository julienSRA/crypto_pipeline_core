"""
pipeline/db.py
Database utilities: initialization, autorepair, pragmas.
Provides helper functions for meta storage and schema compatibility.
"""

import sqlite3
import logging
from pathlib import Path

DB_PATH = Path("data/crypto.db")
LOG = logging.getLogger("pipeline.db")

# --- DDL definitions for autorepair ---
_DDL = {
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
    "bybit_liquidations": """
    CREATE TABLE IF NOT EXISTS bybit_liquidations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        price REAL,
        qty REAL,
        qty_usd REAL,
        raw TEXT
    );
    """,
    "bybit_liquidations_hourly": """
    CREATE TABLE IF NOT EXISTS bybit_liquidations_hourly (
        hour_start INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        total_qty_usd REAL NOT NULL DEFAULT 0,
        events_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (hour_start, symbol, side)
    );
    """,
    "meta": """
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """
}

# --- Connection factory with pragmas ---
def get_conn(path: str | Path = DB_PATH) -> sqlite3.Connection:
    """Open SQLite connection with safe pragmas."""
    conn = sqlite3.connect(str(path), detect_types=sqlite3.PARSE_DECLTYPES)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

# --- Meta helpers ---
def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    cur = conn.cursor()
    cur.execute("SELECT value FROM meta WHERE key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else None

def set_meta(conn: sqlite3.Connection, key: str, value: str):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )
    conn.commit()

# --- Autorepair / init ---
def ensure_tables(conn: sqlite3.Connection):
    """Ensure all tables exist (idempotent)."""
    cur = conn.cursor()
    for name, ddl in _DDL.items():
        cur.execute(ddl)
        LOG.debug("Ensured table: %s", name)
    conn.commit()

def autorepair_schema(conn: sqlite3.Connection):
    """
    Perform schema compatibility fixes.
    Example: rename legacy columns (funding_rate→funding, oi_value→open_interest).
    """
    cur = conn.cursor()
    # Ensure 'funding' column exists
    cur.execute("PRAGMA table_info(bybit)")
    cols = [r[1] for r in cur.fetchall()]
    if "funding" not in cols and "funding_rate" in cols:
        try:
            cur.execute("ALTER TABLE bybit RENAME COLUMN funding_rate TO funding;")
            conn.commit()
            LOG.info("Autorepair: renamed funding_rate → funding")
        except Exception:
            LOG.exception("Failed to rename funding_rate column")

    # Ensure 'open_interest' column exists
    cur.execute("PRAGMA table_info(bybit)")
    cols = [r[1] for r in cur.fetchall()]
    if "open_interest" not in cols and "oi_value" in cols:
        try:
            cur.execute("ALTER TABLE bybit RENAME COLUMN oi_value TO open_interest;")
            conn.commit()
            LOG.info("Autorepair: renamed oi_value → open_interest")
        except Exception:
            LOG.exception("Failed to rename oi_value column")

def init_db(path: str | Path = DB_PATH) -> sqlite3.Connection:
    """Ensure database and schema ready, return connection."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = get_conn(path)
    ensure_tables(conn)
    autorepair_schema(conn)
    LOG.info("✅ Database initialized at %s", path)
    return conn
