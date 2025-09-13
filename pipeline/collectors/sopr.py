import httpx, csv, io, time, logging, sqlite3
LOG = logging.getLogger("pipeline.collectors.sopr")

URL = "https://bitcoin-data.com/v1/sopr/csv"
# Persisted rate-limit: 4 req/hour -> min interval 3600/4 = 900s

from pipeline.db import get_meta, set_meta

def collect(conn: sqlite3.Connection):
    try:
        last = get_meta(conn, "sopr_last_fetch")
        now = int(time.time())
        if last:
            try:
                if now - int(last) < 900:
                    LOG.warning("sopr: skipped to respect rate limit (last fetch too recent)")
                    return
            except Exception:
                pass
        r = httpx.get(URL, timeout=20.0)
        if r.status_code != 200:
            LOG.warning("sopr: HTTP %s", r.status_code)
            return
        text = r.text
        # CSV parse: expect header with d,unixTs,sopr
        f = io.StringIO(text)
        reader = csv.DictReader(f)
        # take last valid line
        last_val = None
        last_ts = None
        for row in reader:
            try:
                # some files have different column names, try to be permissive
                v = row.get("sopr") or row.get("SOPR") or row.get("value")
                d = row.get("d") or row.get("date")
                unix = row.get("unixTs") or row.get("unix")
                if v is None:
                    continue
                val = float(v)
                ts = int(unix) if unix else int(time.time())
                last_val = val
                last_ts = ts
            except Exception:
                continue
        if last_val is not None:
            cur = conn.cursor()
            cur.execute("INSERT OR REPLACE INTO sopr (ts,value) VALUES (?,?)", (last_ts or int(time.time()), last_val))
            conn.commit()
            set_meta(conn, "sopr_last_fetch", str(int(time.time())))
            LOG.info("sopr: %.4f", last_val)
        else:
            LOG.warning("sopr: no valid line parsed")
    except Exception:
        LOG.exception("sopr: error")
