import httpx, time, logging, sqlite3
LOG = logging.getLogger("pipeline.collectors.altme")

def collect(conn: sqlite3.Connection):
    try:
        r = httpx.get("https://api.alternative.me/fng/?limit=1", timeout=10.0)
        r.raise_for_status()
        j = r.json()
        data = j.get("data", [])
        if not data:
            return
        v = int(data[0].get("value", 0))
        ts = int(time.time())
        cur = conn.cursor()
        # correspond au schéma migrate.py (table altme avec colonne fng)
        cur.execute("INSERT OR REPLACE INTO altme (ts, fng) VALUES (?,?)", (ts, v))
        conn.commit()
        LOG.info("altme: fng=%s", v)
    except Exception:
        LOG.exception("altme failed")
