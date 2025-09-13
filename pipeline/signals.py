# pipeline/signals.py
"""
Signals computation module.

Each signal is stored with its own timestamp (ts) and name in the `signals` table:
  - ts (INTEGER, epoch seconds UTC)
  - name (TEXT, e.g. "sopr", "funding_btc")
  - value (REAL)
  - classification (TEXT, optional qualitative label)

PRIMARY KEY is (ts, name), so multiple signals can coexist at the same ts.
"""

import sqlite3
import logging
from datetime import datetime, timezone
from typing import Dict, Tuple, Optional

logger = logging.getLogger("pipeline.signals")

DB_PATH = "data/crypto.db"

def compute_signals(conn: sqlite3.Connection) -> Dict[str, Tuple[Optional[float], Optional[str]]]:
    """
    Compute trading/market signals based on latest metrics.

    Returns dict {name: (value, classification)}.
    """
    cur = conn.cursor()

    # latest metrics row
    try:
        cur.execute("SELECT * FROM metrics ORDER BY ts DESC LIMIT 1")
        row = cur.fetchone()
    except sqlite3.OperationalError:
        logger.warning("metrics table not found → no signals computed")
        return {}

    if not row:
        return {}

    # Extract columns by name
    colnames = [d[0] for d in cur.description]
    metrics = dict(zip(colnames, row))

    signals: Dict[str, Tuple[Optional[float], Optional[str]]] = {}

    # Example signal: SOPR threshold
    if metrics.get("sopr") is not None:
        val = metrics["sopr"]
        classification = "bullish" if val > 1 else "bearish"
        signals["sopr"] = (val, classification)

    # Example: funding BTC
    if metrics.get("funding_btc") is not None:
        val = metrics["funding_btc"]
        classification = "high" if val > 0.01 else "low"
        signals["funding_btc"] = (val, classification)

    # Example: funding ETH
    if metrics.get("funding_eth") is not None:
        val = metrics["funding_eth"]
        classification = "high" if val > 0.01 else "low"
        signals["funding_eth"] = (val, classification)

    # Example: mempool congestion
    if metrics.get("mempool_tx_count") is not None:
        val = metrics["mempool_tx_count"]
        classification = "congested" if val > 50000 else "normal"
        signals["mempool"] = (val, classification)

    return signals


def store_signals(conn: sqlite3.Connection, signals: Dict[str, Tuple[Optional[float], Optional[str]]], ts: Optional[int] = None):
    """
    Store computed signals into the DB.

    Each signal gets its own row keyed by (ts, name).
    - ts: optional, epoch seconds (UTC). If None, uses now.
    - signals: dict {name: (value, classification)}
    """
    if not signals:
        return

    if ts is None:
        ts = int(datetime.now(timezone.utc).timestamp())

    cur = conn.cursor()
    for name, (val, cls) in signals.items():
        try:
            cur.execute(
                """
                INSERT OR REPLACE INTO signals (ts, name, value, classification)
                VALUES (?, ?, ?, ?)
                """,
                (ts, name, val, cls),
            )
        except Exception:
            logger.exception("Failed to insert signal %s=%s", name, val)
    conn.commit()
    logger.info("Stored %d signals at ts=%s", len(signals), ts)


def latest_signals(conn: sqlite3.Connection) -> Dict[str, Tuple[Optional[float], Optional[str]]]:
    """
    Fetch the latest signals snapshot (max ts).
    Returns {name: (value, classification)}.
    """
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(ts) FROM signals")
        r = cur.fetchone()
        if not r or r[0] is None:
            return {}
        latest_ts = r[0]

        cur.execute("SELECT name, value, classification FROM signals WHERE ts=?", (latest_ts,))
        rows = cur.fetchall()
        return {name: (val, cls) for name, val, cls in rows}
    except sqlite3.OperationalError:
        logger.warning("signals table not found")
        return {}
