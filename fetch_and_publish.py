# fetch_and_publish.py  -- OKX public candles (no API key)
import json, time, pathlib, requests

BASE = "https://www.okx.com/api/v5/market/candles"
# Perpetual swaps (USDT)
INSTRUMENTS = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]
BAR = "1m"
LIMIT = 200

def get_okx_candles(instId: str, bar: str, limit: int):
    params = {"instId": instId, "bar": bar, "limit": str(limit)}
    r = requests.get(BASE, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("code") != "0":
        raise RuntimeError(f"OKX error: {data.get('msg')}")
    # OKX geeft nieuwste -> oudste; velden: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    rows = data["data"]
    norm = []
    for row in reversed(rows):  # oud -> nieuw
        ts_ms = int(row[0])
        o, h, l, c = map(float, row[1:5])
        vol = float(row[5])  # contract volume
        norm.append([ts_ms, o, h, l, c, vol])
    return norm

def build_payload():
    out = {
        "source": "okx_public",
        "bar": BAR,
        "limit": LIMIT,
        "ts": int(time.time()),
        "markets": {}
    }
    for inst in INSTRUMENTS:
        out["markets"][inst] = get_okx_candles(inst, BAR, LIMIT)
    return out

def main():
    payload = build_payload()
    pathlib.Path("data").mkdir(parents=True, exist_ok=True)
    with open("data/latest.json", "w") as f:
        json.dump(payload, f)
    print("Wrote data/latest.json")

if __name__ == "__main__":
    main()
