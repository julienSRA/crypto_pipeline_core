import httpx, time, logging, sqlite3
LOG = logging.getLogger("pipeline.collectors.defillama")

def collect(conn: sqlite3.Connection):
    try:
        r = httpx.get("https://stablecoins.llama.fi/stablecoins", timeout=15.0)
        r.raise_for_status()
        data = r.json()

        ts = int(time.time())
        # le total est dans data["totalCirculatingUSD"]
        total = float(data.get("totalCirculatingUSD") or 0)

        # chercher USDT / USDC dans peggedAssets
        usdt = usdc = 0.0
        for asset in data.get("peggedAssets", []):
            symbol = asset.get("symbol", "").upper()
            circulating = asset.get("circulating") or asset.get("peggedUSD") or 0
            try:
                circulating_val = float(circulating)
            except Exception:
                circulating_val = 0.0
            if symbol == "USDT":
                usdt = circulating_val
            elif symbol == "USDC":
                usdc = circulating_val

        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO stablecoins (ts,total,usdt,usdc) VALUES (?,?,?,?)",
            (ts, total, usdt, usdc)
        )
        conn.commit()
        LOG.info("defillama: stablecoins total=%s usdt=%s usdc=%s", total, usdt, usdc)
    except Exception:
        LOG.exception("defillama: failed")
