import json
import os
import time
from datetime import datetime, timezone
import requests

# ---------------------------------------
# Config
# ---------------------------------------
BASE = "https://www.okx.com"
ENDPOINT = "/api/v5/market/candles"
BAR = "1m"
LIMIT = 200

# Perpetual swaps op OKX heten <COIN>-USDT-SWAP
PAIRS = {
    "BTCUSDT": "BTC-USDT-SWAP",
    "ETHUSDT": "ETH-USDT-SWAP",
    "SOLUSDT": "SOL-USDT-SWAP",
}

OUT_PATH = "data/latest.json"
TIMEOUT = 15
RETRIES = 3
SLEEP_BETWEEN = 1.5


def fetch_okx_klines(inst_id: str, bar: str = BAR, limit: int = LIMIT):
    """
    Haal klines op bij OKX. Response data is reverse-chronologisch.
    We geven een lijst met candles terug in oplopende tijd.
    """
    params = {"instId": inst_id, "bar": bar, "limit": str(limit)}
    url = BASE + ENDPOINT

    last_err = None
    for _ in range(RETRIES):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            # OKX gebruikt 200 + {"code":"0"} voor success
            r.raise_for_status()
            data = r.json()
            if data.get("code") != "0":
                raise RuntimeError(f"OKX error: {data.get('msg')}")
            rows = data.get("data", [])
            # Elke rij: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm, ...]
            candles = []
            for row in rows:
                ts_ms = int(row[0])
                candles.append({
                    "ts": ts_ms,                              # epoch ms
                    "time": datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).isoformat(),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low":  float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                })
            # Oplopend sorteren op tijd
            candles.sort(key=lambda x: x["ts"])
            return candles
        except Exception as e:
            last_err = e
            time.sleep(SLEEP_BETWEEN)
    raise last_err


def ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def main():
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "exchange": "okx",
        "interval": BAR,
        "source": BASE + ENDPOINT,
        "pairs": {},
    }

    for human, inst in PAIRS.items():
        k = fetch_okx_klines(inst)
        payload["pairs"][human] = k

    ensure_dir(OUT_PATH)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"), indent=2)

    print(f"Wrote {OUT_PATH} with {len(payload['pairs'])} markets")


if __name__ == "__main__":
    main()
