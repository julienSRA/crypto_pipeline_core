#!/usr/bin/env python3
# exporter.py
"""
Exporter: dump database tables to CSV.
Standardised entrypoint: run(conn).
"""
import sqlite3
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/crypto.db")
EXPORT_DIR = Path("exports")

LOG = logging.getLogger("exporter")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

TABLES = {
    "metrics": "SELECT * FROM metrics ORDER BY ts DESC",
    "coingecko": "SELECT * FROM coingecko ORDER BY ts DESC",
    "bybit": "SELECT * FROM bybit ORDER BY ts DESC",
    "sopr": "SELECT * FROM sopr ORDER BY ts DESC",
    "altme": "SELECT * FROM altme ORDER BY ts DESC",
    "mempool": "SELECT ts, tx_count, fee_fastest, fee_30m FROM mempool ORDER BY ts DESC",
    "stablecoins": "SELECT ts, total, usdt, usdc FROM stablecoins ORDER BY ts DESC",
    "bybit_liquidations": "SELECT * FROM bybit_liquidations ORDER BY ts DESC LIMIT 1000",
    "bybit_liquidations_hourly": "SELECT * FROM bybit_liquidations_hourly ORDER BY hour_start DESC",
    "signals": "SELECT * FROM signals ORDER BY ts DESC"
}


def export_table(conn: sqlite3.Connection, name: str, query: str, out_dir: Path):
    df = pd.read_sql_query(query, conn)
    out_file = out_dir / f"{name}.csv"
    df.to_csv(out_file, index=False)
    LOG.info("Exported %s (%d rows) -> %s", name, len(df), out_file)


def run(conn: sqlite3.Connection):
    """Standardised entrypoint."""
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")
    session_dir = EXPORT_DIR / f"export_{ts}"
    session_dir.mkdir(parents=True, exist_ok=True)

    for name, query in TABLES.items():
        try:
            export_table(conn, name, query, session_dir)
        except Exception as e:
            LOG.error("Failed to export %s: %s", name, e)

    LOG.info("✅ Export completed to %s", session_dir)


def main():
    conn = sqlite3.connect(str(DB_PATH))
    run(conn)
    conn.close()


if __name__ == "__main__":
    main()
