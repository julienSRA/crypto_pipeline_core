# pipeline/coingecko.py
import httpx
import logging
import time
import sqlite3

LOG = logging.getLogger("pipeline.collectors.coingecko")

COINS = ["bitcoin", "ethereum", "solana", "chainlink"]

def collect(conn: sqlite3.Connection):
    url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids={','.join(COINS)}&price_change_percentage=24h"
    try:
        r = httpx.get(url, timeout=20.0)
        r.raise_for_status()
        data = r.json()
        ts = int(time.time())
        cur = conn.cursor()
        for item in data:
            cur.execute(
                "INSERT INTO coingecko (ts, symbol, price_usd) VALUES (?,?,?)",
                (
                    ts,
                    item.get("symbol"),
                    float(item.get("current_price", 0.0)),
                ),
            )
        conn.commit()
        LOG.info("coingecko: inserted %d prices", len(data))
    except Exception:
        LOG.exception("coingecko collect failed")
