# fetch_and_publish.py
import os, csv, json, datetime as dt, requests

SYMBOLS   = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]   # USDT-perp symbols
INTERVALS = ["1m", "15m"]                       # voeg "5m","1h" toe indien gewenst
LIMIT     = 200                                  # aantal candles per sym/interval
OUTDIR    = "ohlc_data"
BASE      = "https://fapi.binance.com"          # Binance Futures (public)

def fetch_klines(symbol, interval, limit=LIMIT):
    url = f"{BASE}/fapi/v1/klines"
    r = requests.get(url, params={"symbol":symbol,"interval":interval,"limit":limit}, timeout=20)
    r.raise_for_status()
    out = []
    for k in r.json():
        ts = int(k[0]) // 1000
        out.append({
            "time":   dt.datetime.utcfromtimestamp(ts).replace(microsecond=0).isoformat() + "Z",
            "open":   k[1],
            "high":   k[2],
            "low":    k[3],
            "close":  k[4],
            "volume": k[5],
        })
    return out

def ensure_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

def append_csv(path, rows):
    header = ["time","open","high","low","close","volume"]
    last_time = None
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                for line in f: pass
                if line.strip():
                    last_time = line.split(",")[0]
        except Exception:
            pass
    write_header = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(header)
        for r in rows:
            if last_time is None or r["time"] > last_time:
                w.writerow([r["time"], r["open"], r["high"], r["low"], r["close"], r["volume"]])

def main():
    ensure_dir(OUTDIR)
    last = {}  # laatste candle per symbol/interval
    for sym in SYMBOLS:
        last[sym] = {}
        for itv in INTERVALS:
            kl = fetch_klines(sym, itv, LIMIT)
            append_csv(os.path.join(OUTDIR, f"{sym}_{itv}.csv"), kl)
            if kl:
                last[sym][itv] = kl[-1]
    snapshot = {
        "timestamp": dt.datetime.utcnow().replace(microsecond=0).isoformat()+"Z",
        "symbols": last
    }
    with open("latest.json","w") as f:
        json.dump(snapshot, f, indent=2)
    print("✅ Updated latest.json & CSVs")

if __name__ == "__main__":
    main()
