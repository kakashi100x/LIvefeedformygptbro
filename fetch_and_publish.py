# fetch_and_publish.py
import os, time, hmac, hashlib, json, pathlib, urllib.parse, requests

BASE_URL = "https://api.bybit.com"
ENDPOINT = "/v5/market/kline"  # v5 market candles

SYMBOLS = [
    ("BTCUSDT", "linear"),
    ("ETHUSDT", "linear"),
    ("SOLUSDT", "linear"),
]
INTERVAL = "1"   # 1m
LIMIT = 200
RECV_WINDOW = "5000"

API_KEY = os.environ.get("BYBIT_API_KEY", "")
API_SECRET = os.environ.get("BYBIT_API_SECRET", "")

def sign_v5(secret: str, ts: str, api_key: str, recv_window: str, query_string: str) -> str:
    # v5 sign string: timestamp + api_key + recv_window + queryString
    payload = ts + api_key + recv_window + query_string
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()

def get_klines(symbol: str, category: str):
    ts = str(int(time.time() * 1000))
    q = {
        "category": category,
        "symbol": symbol,
        "interval": INTERVAL,
        "limit": str(LIMIT),
    }
    query_string = urllib.parse.urlencode(q, doseq=True)

    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW,
        "Content-Type": "application/json",
    }

    # Alleen signen wanneer we een key hebben
    if API_KEY and API_SECRET:
        signature = sign_v5(API_SECRET, ts, API_KEY, RECV_WINDOW, query_string)
        headers["X-BAPI-SIGN"] = signature

    url = f"{BASE_URL}{ENDPOINT}?{query_string}"
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit error: {data.get('retMsg')}")
    return data["result"]["list"]  # lijst van candles nieuwste→oudste

def build_payload():
    out = {"source": "bybit_v5", "interval": INTERVAL, "limit": LIMIT, "ts": int(time.time()), "markets": {}}
    for sym, cat in SYMBOLS:
        kl = get_klines(sym, cat)
        # normaliseer naar: [open_time, open, high, low, close, volume]
        # Bybit v5 list-element: [start, open, high, low, close, volume, turnover]
        norm = []
        for row in reversed(kl):  # oud→nieuw
            start, op, hi, lo, cl, vol, _ = row
            norm.append([int(start), float(op), float(hi), float(lo), float(cl), float(vol)])
        out["markets"][sym] = norm
    return out

def main():
    payload = build_payload()
    pathlib.Path("data").mkdir(parents=True, exist_ok=True)
    with open("data/latest.json", "w") as f:
        json.dump(payload, f)
    print("Wrote data/latest.json")

if __name__ == "__main__":
    main()
