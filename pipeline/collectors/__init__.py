"""
Pipeline collectors package
Expose uniquement les modules batch (collect(conn)).

⚠️ Attention :
- `bybit_ws` est un service temps réel → NE DOIT PAS être importé ici,
  il s’exécute séparément via : python -m pipeline.collectors.bybit_ws
"""

from . import (
    coingecko,
    defillama,
    sopr,
    bybit,
    mempool,
    altme,
    bybit_oi_hist,
    hashrate,
    txcount,
)

__all__ = [
    "coingecko",
    "defillama",
    "sopr",
    "bybit",
    "mempool",
    "altme",
    "bybit_oi_hist",
    "hashrate",
    "txcount",
]
