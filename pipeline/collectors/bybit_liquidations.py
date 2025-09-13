"""
Bybit Liquidations Writer (prod-safe)
- Ecrit en SQLite (événements + agrégats horaires)
- Flush vers Parquet (optionnel)
- Utilisé par bybit_ws.py
"""

import os
import sqlite3
import asyncio
import logging
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class BybitLiquidationsWriter:
    def __init__(self, db="data/crypto.db", parquet_dir="data/bybit_liquidations",
                 flush_size=100, flush_interval=5, parquet_enabled=True):
        self.db = db
        self.parquet_dir = parquet_dir
        self.flush_size = flush_size
        self.flush_interval = flush_interval
        self.parquet_enabled = parquet_enabled

        os.makedirs(os.path.dirname(db), exist_ok=True)
        os.makedirs(parquet_dir, exist_ok=True)

        self.conn = sqlite3.connect(self.db, check_same_thread=False)

        self.buffer = []
        self.last_flush = datetime.utcnow().timestamp()
        self.lock = asyncio.Lock()

        logger.info(
            "BybitLiquidationsWriter initialized (db=%s parquet=%s parquet_enabled=%s)",
            db, parquet_dir, parquet_enabled
        )

    # -----------------------------------------------------
    # RECORD WRITE
    # -----------------------------------------------------
    async def write_record(self, record):
        async with self.lock:
            try:
                symbol = record.get("symbol") or record.get("s")
                side = record.get("side") or record.get("S", "UNKNOWN")
                price = float(record.get("price") or record.get("p") or 0)
                qty = float(record.get("qty") or record.get("q") or 0)
                ts = int(record.get("ts") or record.get("T") or datetime.utcnow().timestamp() * 1000)

                if not symbol or price == 0 or qty == 0:
                    return

                self.buffer.append({
                    "symbol": symbol.upper(),
                    "side": side.upper(),
                    "price": price,
                    "qty": qty,
                    "time": ts
                })

                now = datetime.utcnow().timestamp()
                if len(self.buffer) >= self.flush_size or (now - self.last_flush) >= self.flush_interval:
                    await self.flush()

            except Exception as e:
                logger.error("Error parsing record: %s", e, exc_info=True)

    # -----------------------------------------------------
    # FLUSH
    # -----------------------------------------------------
    async def flush(self):
        if not self.buffer:
            return

        buf = self.buffer
        self.buffer = []
        self.last_flush = datetime.utcnow().timestamp()

        try:
            df = pd.DataFrame(buf)
            if df.empty:
                return

            # SQLite: raw events
            cur = self.conn.cursor()
            cur.executemany("""
            INSERT INTO bybit_liquidations (symbol, side, price, qty, time)
            VALUES (?, ?, ?, ?, ?)
            """, [(r["symbol"], r["side"], r["price"], r["qty"], r["time"]) for _, r in df.iterrows()])

            # SQLite: aggregates
            for _, r in df.iterrows():
                hour_start = int(r["time"] // 1000 // 3600 * 3600)
                cur.execute("""
                INSERT INTO bybit_liquidations_hourly (hour_start, symbol, side, total_qty_usd, events_count)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(hour_start, symbol, side)
                DO UPDATE SET
                    total_qty_usd = total_qty_usd + excluded.total_qty_usd,
                    events_count = events_count + 1
                """, (hour_start, r["symbol"], r["side"], r["qty"] * r["price"]))

            self.conn.commit()

            # Parquet
            if self.parquet_enabled:
                ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                fname = os.path.join(self.parquet_dir, f"liq_{ts_str}.parquet")
                df.to_parquet(fname, engine="pyarrow", index=False)

            logger.info("Flushed %s records", len(df))

        except Exception as e:
            logger.error("Flush error: %s", e, exc_info=True)

    async def close(self):
        await self.flush()
        self.conn.close()
        logger.info("Writer closed")
