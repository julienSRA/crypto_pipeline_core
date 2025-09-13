import httpx
import logging
import sqlite3
from datetime import datetime

logger = logging.getLogger("pipeline.collectors.txcount")

URL_BTC = "https://mempool.space/api/blocks"

def collect(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS txcount_btc (
            ts INTEGER PRIMARY KEY,
            tx_count INTEGER NOT NULL
        )
    """)
    ts = int(datetime.utcnow().timestamp())
    tx_count = None
    try:
        r = httpx.get(URL_BTC, timeout=10)
        r.raise_for_status()
        block = r.json()[0]  # dernier bloc
        tx_count = block.get("tx_count")
    except Exception as e:
        logger.error(f"txcount fetch error: {e}")

    if tx_count is not None:
        cur.execute("INSERT OR REPLACE INTO txcount_btc (ts, tx_count) VALUES (?, ?)", (ts, tx_count))
        conn.commit()
        logger.info(f"txcount BTC: {tx_count}")
    else:
        logger.warning("txcount skipped (no valid value)")
