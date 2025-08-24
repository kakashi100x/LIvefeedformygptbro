# fetch_and_publish.py  — OKX perpetuals: 1m + 15m
import json, os, requests, time
from pathlib import Path
from datetime import datetime, timezone

# ---------- Config ----------
OKX_URL = "https://www.okx.com/api/v5/market/candles"
PAIRS = {
    "BTCUSDT": "BTC-USDT-SWAP",
    "ETHUSDT": "ETH-USDT-SWAP",
    "SOLUSDT": "SOL-USDT-SWAP",
}
INTERVALS = ["1m", "15m"]                     # <- beide timeframes
LIMIT = int(os.getenv("LIMIT", "200"))        # aantal candles per tf
OUT = Path("data/latest.json")
TIMEOUT = 20

def fetch_okx(inst_id: str, bar: str, limit: int):
    """Haalt OKX candles (nieuw→oud), normaliseert naar oud→nieuw."""
    r = requests.get(OKX_URL, params={"instId": inst_id, "bar": bar, "limit": str(limit)}, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if data.get("code") != "0":
        raise RuntimeError(f"OKX error: {data.get('msg')}")
    rows = list(reversed(data["data"]))  # oud → nieuw
    out = []
    for row in rows:
        # OKX: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm, ...]
        ts = int(row[0])
        out.append({
            "ts": ts,  # epoch ms
            "time": datetime.fromtimestamp(ts/1000, tz=timezone.utc).isoformat(),
            "open": float(row[1]),
            "high": float(row[2]),
            "low":  float(row[3]),
            "close":float(row[4]),
            "volume": float(row[5]),            # contract volume
            "vol_quote": float(row[7]) if len(row) > 7 and row[7] not in ("", None) else None
        })
    return out

def build_payload():
    bundle = {
        "exchange": "okx-perp",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "intervals": INTERVALS,
        "limit": LIMIT,
        "pairs": {},                            # pairs[symbol][tf] = list[candles]
        "meta": {"instIds": PAIRS, "endpoint": OKX_URL, "tz": "UTC"}
    }
    for sym, inst in PAIRS.items():
        bundle["pairs"][sym] = {}
        for tf in INTERVALS:
            bundle["pairs"][sym][tf] = fetch_okx(inst, tf, LIMIT)
            time.sleep(0.4)  # vriendelijk voor rate limits
    return bundle

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    payload = build_payload()
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {OUT} with {len(payload['pairs'])} markets @ {', '.join(INTERVALS)}")

if __name__ == "__main__":
    main()
