#!/usr/bin/env python3
"""
Bybit Open Interest & Funding history collector.
Fetches OI/funding for BTCUSDT and ETHUSDT and stores into SQLite.
"""

import sqlite3
import logging
import time
import httpx

logger = logging.getLogger("pipeline.bybit_oi_hist")

API_URL = "https://api.bybit.com/v5/market/open-interest"

SYMBOLS = ["BTCUSDT", "ETHUSDT"]

def collect(conn: sqlite3.Connection):
    """
    Fetch and store open interest & funding for supported symbols.
    """
    for symbol in SYMBOLS:
        try:
            r = httpx.get(API_URL, params={"category": "linear", "symbol": symbol}, timeout=30.0)
            r.raise_for_status()
            data = r.json()
        except Exception:
            logger.exception("Failed to fetch Bybit OI for %s", symbol)
            continue

        try:
            oi_value = None
            result = data.get("result", {})
            if isinstance(result, dict):
                oi_data = result.get("list") or []
                if oi_data:
                    oi_value = float(oi_data[0].get("openInterest", 0))

            ts = int(time.time())
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO bybit (ts, symbol, open_interest)
                VALUES (?, ?, ?)
                """,
                (ts, symbol, oi_value),
            )
            conn.commit()
            logger.info("Inserted OI snapshot for %s (oi=%.2f)", symbol, oi_value or 0)
        except Exception:
            logger.exception("Failed to insert Bybit OI for %s", symbol)
