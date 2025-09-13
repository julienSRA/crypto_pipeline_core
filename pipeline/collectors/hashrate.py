import httpx
import logging
import sqlite3
from datetime import datetime

logger = logging.getLogger("pipeline.collectors.hashrate")

URL_MAIN = "https://mempool.space/api/v1/mining/hashrate"
URL_FALLBACK = "https://blockchain.info/q/hashrate"

def collect(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hashrate_btc (
            ts INTEGER PRIMARY KEY,
            hashrate REAL NOT NULL
        )
    """)
    ts = int(datetime.utcnow().timestamp())
    hashrate = None
    try:
        r = httpx.get(URL_MAIN, timeout=10)
        r.raise_for_status()
        data = r.json()
        hashrate = float(data.get("hashrate_7d", 0))  # déjà en EH/s
    except Exception:
        try:
            r = httpx.get(URL_FALLBACK, timeout=10)
            r.raise_for_status()
            hashrate = float(r.text) / 1e18  # fallback = H/s → convertir en EH/s
        except Exception as e:
            logger.error(f"hashrate fetch error: {e}")

    if hashrate is not None and hashrate > 0:
        cur.execute("INSERT OR REPLACE INTO hashrate_btc (ts, hashrate) VALUES (?, ?)", (ts, hashrate))
        conn.commit()
        logger.info(f"hashrate: {hashrate:.2f} EH/s")
    else:
        logger.warning("hashrate skipped (no valid value)")
