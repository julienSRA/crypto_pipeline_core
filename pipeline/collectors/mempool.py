import httpx, time, logging, sqlite3
LOG = logging.getLogger("pipeline.collectors.mempool")

def collect(conn: sqlite3.Connection):
    try:
        r1 = httpx.get("https://mempool.space/api/v1/fees/recommended", timeout=10.0)
        r1.raise_for_status()
        fees = r1.json()
        r2 = httpx.get("https://mempool.space/api/mempool", timeout=10.0)
        r2.raise_for_status()
        m = r2.json()
        ts = int(time.time())

        tx_count = int(m.get("count") or m.get("mempool_size") or 0)
        fee_fastest = int(fees.get("fastestFee") or fees.get("fastest") or 0)
        fee_30m = int(fees.get("halfHourFee") or fees.get("half") or 0)

        cur = conn.cursor()
        # correspond au schéma migrate.py : (ts, tx_count, fee_fastest, fee_30m)
        cur.execute(
            "INSERT OR REPLACE INTO mempool (ts, tx_count, fee_fastest, fee_30m) VALUES (?,?,?,?)",
            (ts, tx_count, fee_fastest, fee_30m)
        )
        conn.commit()
        LOG.info("mempool: tx_count=%s | fastest=%s | 30m=%s", tx_count, fee_fastest, fee_30m)
    except Exception:
        LOG.exception("mempool: failed")
