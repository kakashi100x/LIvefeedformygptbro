import json, os, time, requests
from pathlib import Path

# ---------- Config ----------
BASE = "https://www.okx.com/api/v5/market/candles"
PAIRS = {
    "BTCUSDT": "BTC-USDT-SWAP",
    "ETHUSDT": "ETH-USDT-SWAP",
    "SOLUSDT": "SOL-USDT-SWAP",
}
BAR = os.getenv("BAR", "1m")      # 1m, 5m, 15m, etc
LIMIT = int(os.getenv("LIMIT", "200"))
OUT = Path("data/latest.json")

# ---------- Fetch ----------
def get_klines(inst_id: str, bar: str, limit: int):
    """
    OKX returns newest->oldest; we reverse to oldest->newest.
    Timestamps are in ms.
    """
    url = f"{BASE}?instId={inst_id}&bar={bar}&limit={limit}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    rows = r.json()["data"]           # list of lists: [ts, o, h, l, c, vol, ...]
    rows = list(reversed(rows))
    out = []
    for t,o,h,l,c,vol,*_ in rows:
        out.append({
            "ts": int(t),                 # ms
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c),
            "volume": float(vol),
        })
    return out

def build_payload():
    series = {}
    for sym, inst in PAIRS.items():
        series[sym] = get_klines(inst, BAR, LIMIT)
    payload = {
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z",
        "exchange": "okx-perp",
        "interval": BAR,
        "source": "https://www.okx.com/api/v5/market/candles",
        "pairs": series,
        "meta": {
            "notes": "Perpetual (SWAP) contracts on OKX, 1m candles (default).",
            "instIds": PAIRS,
            "timezone": "UTC",
        },
    }
    return payload

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    data = build_payload()
    OUT.write_text(json.dumps(data, indent=2))
    print(f"Wrote {OUT} with {len(data['pairs']['BTCUSDT'])} candles per pair.")

if __name__ == "__main__":
    main()
