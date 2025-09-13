import httpx, time, logging, sqlite3
LOG = logging.getLogger("pipeline.collectors.bybit")

SYMS = ["BTCUSDT","ETHUSDT"]

def _get_oi(symbol, interval):
    url = f"https://api.bybit.com/v5/market/open-interest?category=linear&symbol={symbol}&interval={interval}&limit=1"
    r = httpx.get(url, timeout=10.0)
    r.raise_for_status()
    j = r.json()
    # path: result.list[0].open_interest
    try:
        lst = j.get("result", {}).get("list", [])
        if not lst:
            return None
        v = lst[0].get("open_interest")
        return float(v) if v is not None else None
    except Exception:
        return None

def _get_funding(symbol):
    url = f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&limit=1"
    r = httpx.get(url, timeout=10.0)
    r.raise_for_status()
    j = r.json()
    try:
        lst = j.get("result", {}).get("list", [])
        if not lst:
            return None
        v = lst[0].get("funding_rate")
        return float(v) if v is not None else None
    except Exception:
        return None

def collect(conn: sqlite3.Connection):
    ts = int(time.time())
    cur = conn.cursor()
    try:
        for s in SYMS:
            funding = None
            oi = None
            try:
                funding = _get_funding(s)
            except Exception:
                LOG.debug("bybit funding failed for %s", s, exc_info=True)
            # try intervals 5min->1h->4h
            for interval in ("5min","1h","4h"):
                try:
                    oi = _get_oi(s, interval)
                    if oi is not None:
                        break
                except Exception:
                    continue
            cur.execute("INSERT OR REPLACE INTO bybit (ts,symbol,funding,open_interest) VALUES (?,?,?,?)", (ts, s, funding, oi))
        conn.commit()
        LOG.info("bybit: metrics saved")
    except Exception:
        LOG.exception("bybit collector failed")
