#!/usr/bin/env python3
"""
Bybit Liquidations WebSocket Collector (v20, prod-safe)
- Connexion WS Bybit (v5 API, spot/linear auto-détection)
- Flush vers SQLite et Parquet
- Args robustes avec argparse
"""

import asyncio
import json
import logging
import os
import signal
import sys
import argparse
from datetime import datetime

import websockets
from pipeline.collectors.bybit_liquidations import BybitLiquidationsWriter

# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# SERVICE WS
# ---------------------------------------------------------
class BybitWSService:
    def __init__(self, symbols, ws_url=None, db_path="data/crypto.db",
                 parquet_dir="data/bybit_liquidations", flush_size=100,
                 flush_interval=5, subscribe_tpl="liquidation.{}"):
        self.symbols = symbols
        self.ws_url = ws_url or self._auto_detect_url(symbols)
        self.subscribe_tpl = subscribe_tpl

        os.makedirs(parquet_dir, exist_ok=True)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        self.writer = BybitLiquidationsWriter(
            db=db_path,
            parquet_dir=parquet_dir,
            flush_size=flush_size,
            flush_interval=flush_interval
        )

        self.ws = None
        self.stop_event = asyncio.Event()
        self._reconnect_delay = 1

        logger.info(
            "BybitWSService created (symbols=%s url=%s)",
            ",".join(symbols), self.ws_url
        )

    def _auto_detect_url(self, symbols):
        """Détection auto du bon endpoint en fonction des paires"""
        spot_suffixes = {"USDC", "USDT"}
        if all(sym.endswith(tuple(spot_suffixes)) for sym in symbols):
            return "wss://stream.bybit.com/v5/public/linear"
        return "wss://stream.bybit.com/v5/public/linear"

    async def connect(self):
        logger.info("Connecting to Bybit WS %s", self.ws_url)
        try:
            async with websockets.connect(self.ws_url) as ws:
                self.ws = ws
                logger.info("WS connected to %s", self.ws_url)

                # subscribe
                subs = [self.subscribe_tpl.format(sym) for sym in self.symbols]
                await ws.send(json.dumps({"op": "subscribe", "args": subs}))
                logger.info("WS subscribed: %s", subs)

                self._reconnect_delay = 1

                async for msg in ws:
                    await self._handle_message(msg)

        except asyncio.CancelledError:
            logger.info("WS connection cancelled")
            raise
        except Exception as e:
            logger.warning("WS error: %s", e, exc_info=True)
            logger.info("Reconnecting in %s seconds...", self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, 60)
            if not self.stop_event.is_set():
                await self.connect()

    async def _handle_message(self, raw_msg):
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON: %s", raw_msg)
            return

        if "topic" in msg and "data" in msg:
            topic = msg["topic"]
            data = msg["data"]

            if topic.startswith("liquidation."):
                if isinstance(data, dict):
                    await self.writer.write_record(data)
                elif isinstance(data, list):
                    for d in data:
                        await self.writer.write_record(d)

    async def run(self):
        logger.info("Starting BybitWSService")
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop_event.set)

        while not self.stop_event.is_set():
            await self.connect()

        await self.writer.close()
        logger.info("BybitWSService stopped")

    async def stop(self):
        logger.info("Stopping BybitWSService")
        self.stop_event.set()
        if self.ws:
            await self.ws.close()
        await self.writer.close()


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Bybit WebSocket Liquidations Collector")
    parser.add_argument("-s", "--symbols", required=True,
                        help="Liste des symboles séparés par des virgules (ex: BTCUSDT,ETHUSDT)")
    parser.add_argument("--ws-url", help="Endpoint WS Bybit (défaut auto spot/linear)")
    parser.add_argument("--db", dest="db_path", default="data/crypto.db", help="Fichier SQLite")
    parser.add_argument("--parquet-dir", default="data/bybit_liquidations", help="Dossier Parquet")
    parser.add_argument("--flush-size", type=int, default=100, help="Flush après N enregistrements")
    parser.add_argument("--flush-interval", type=int, default=5, help="Flush après N secondes")
    parser.add_argument("--subscribe-tpl", default="liquidation.{}", help="Template de souscription")

    args = parser.parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(",")]

    svc = BybitWSService(
        symbols,
        ws_url=args.ws_url,
        db_path=args.db_path,
        parquet_dir=args.parquet_dir,
        flush_size=args.flush_size,
        flush_interval=args.flush_interval,
        subscribe_tpl=args.subscribe_tpl,
    )

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(svc.run())
    except KeyboardInterrupt:
        logger.info("Interrupted; stopping service")
        loop.run_until_complete(svc.stop())


if __name__ == "__main__":
    sys.exit(main())
