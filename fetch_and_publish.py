#!/usr/bin/env python3
import json, time, pathlib, requests
from datetime import datetime, timezone

# Bybit v5 market kline endpoint (USDT-perp "linear" markt)
BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"

# Welke symbols & timeframes we willen (je kunt dit lijstje zelf uitbreiden)
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
# Keys: jouw benamingen -> Bybit interval codes
INTERVALS = {"1m": "1", "5m": "5", "15m": "15"}   # pas aan naar wens

LIMIT = 200   # aantal candles per request (max 200 bij Bybit)

def fetch_klines(symbol: str, interval_code: str, limit: int = LIMIT, category: str = "linear"):
    """
    Haalt candles op bij Bybit. Returned lijst van dicts met standard velden.
    Bybit v5 fields per kline: [start, open, high, low, close, volume, turnover]
    start is ms epoch.
    """
    params = {
        "category": category,
        "symbol": symbol,
        "interval": interval_code,
        "limit": str(limit),
    }
    r = requests.get(BYBIT_KLINE_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit API error for {symbol} {interval_code}: {data.get('retMsg')}")
    rows = data["result"]["list"]  # newest first per docs (we sort for zekerheid)
    # Normaliseer naar oplopend op tijd
    rows = sorted(rows, key=lambda x: int(x[0]))
    out = []
    for row in rows:
        ts_ms, o, h, l, c, vol, turnover = row
        out.append({
            "t": int(ts_ms),                      # ms epoch
            "time_iso": datetime.utcfromtimestamp(int(ts_ms)/1000).replace(tzinfo=timezone.utc).isoformat(),
            "o": float(o),
            "h": float(h),
            "l": float(l),
            "c": float(c),
            "volume": float(vol),                 # contract volume
            "turnover": float(turnover),          # quote volume (USDT)
        })
    return out

def main():
    root = pathlib.Path(__file__).resolve().parent
    out_dir = root / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    bundle = {
        "provider": "bybit",
        "category": "linear",            # USDT perpetuals
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "symbols": {},
        "meta": {
            "source_url": BYBIT_KLINE_URL,
            "limit": LIMIT,
            "interval_map": INTERVALS,
        },
    }

    for sym in SYMBOLS:
        bundle["symbols"][sym] = {}
        for tf, code in INTERVALS.items():
            kl = fetch_klines(sym, code, LIMIT)
            bundle["symbols"][sym][tf] = kl
            time.sleep(0.25)  # vriendelijk voor rate limits

    # Schrijf naar /data/latest.json
    out_file = out_dir / "latest.json"
    tmp = out_file.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(bundle, ensure_ascii=False, separators=(",", ":"), indent=2))
    tmp.replace(out_file)
    print(f"Wrote {out_file} with {len(SYMBOLS)} symbols.")

if __name__ == "__main__":
    main()
