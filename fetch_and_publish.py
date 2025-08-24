# fetch_and_publish.py
import os, json, time, sys
from datetime import datetime, timezone
from typing import List, Dict, Any
import requests

BINANCE_URL = "https://api.binance.com/api/v3/klines"  # correcte v3 endpoint
UA = "livefeed-bot/1.0 (+https://github.com/)"

# Lees optioneel symbolen/intervals uit env; anders defaults
SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
INTERVALS = os.getenv("INTERVALS", "1m,15m").split(",")
LIMIT = int(os.getenv("LIMIT", "200"))  # aantal candles per serie
OUT_DIR = os.getenv("OUT_DIR", "data")
OUT_FILE = os.path.join(OUT_DIR, "latest.json")
TIMEOUT = 15

session = requests.Session()
session.headers.update({"User-Agent": UA})

def ts_ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

def fetch_klines(symbol: str, interval: str, limit: int) -> List[Dict[str, Any]]:
    """Haal klines op en geef een net lijstje dicts terug."""
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    # eenvoudige retry met backoff
    backoff = 2
    for attempt in range(5):
        try:
            r = session.get(BINANCE_URL, params=params, timeout=TIMEOUT)
            if r.status_code == 451:
                raise RuntimeError(
                    f"451 from Binance (legal/region). Probeer later opnieuw of gebruik alternatief endpoint."
                )
            r.raise_for_status()
            raw = r.json()
            # Kline velden volgens Binance:
            # [0 openTime, 1 open, 2 high, 3 low, 4 close, 5 volume, 6 closeTime, ...]
            out = []
            for k in raw:
                out.append({
                    "t_open": ts_ms_to_iso(k[0]),
                    "open": float(k[1]),
                    "high": float(k[2]),
                    "low":  float(k[3]),
                    "close": float(k[4]),
                    "volume": float(k[5]),
                    "t_close": ts_ms_to_iso(k[6]),
                })
            return out
        except Exception as e:
            if attempt == 4:
                raise
            time.sleep(backoff)
            backoff *= 1.8
    # zou hier nooit komen:
    return []

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    payload = {
        "source": "binance_spot_v3",
        "symbols": [s.upper() for s in SYMBOLS],
        "intervals": INTERVALS,
        "limit": LIMIT,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "series": {}  # series[(symbol, interval)] -> list
    }

    for sym in SYMBOLS:
        for itv in INTERVALS:
            key = f"{sym.upper()}_{itv}"
            data = fetch_klines(sym, itv, LIMIT)
            payload["series"][key] = data

    with open(OUT_FILE, "w") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(f"Wrote {OUT_FILE} with {len(payload['series'])} series.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
