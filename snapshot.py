#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json, time, urllib.request, urllib.error, math
from datetime import datetime, timezone

# -------- Settings --------
SYMBOLS = [
    ("BTC_USDT", "BTC/USDT Perp"),
    ("ETH_USDT", "ETH/USDT Perp"),
    ("SOL_USDT", "SOL/USDT Perp"),
]
GRAN = "Min1"
LIMIT = 60
TIMEOUT = 12
OUT_JSON = "data/summary.json"
OUT_MD   = "SNAPSHOT.md"

# MEXC endpoints (futures/contract)
EP_A = "https://contract.mexc.com/api/v1/contract/kline/{sym}?interval={gran}&limit={lim}"
EP_B = "https://contract.mexc.com/api/v1/contract/kline/{sym}?interval={gran}&limit={lim}&page_size={lim}"
EP_C = "https://contract.mexc.com/api/v1/contract/kline?symbol={sym}&interval={gran}&limit={lim}"  # 404 op sommige regio’s, maar laten staan als fallback

def http_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent":"snapshot/1.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8"))

def parse_mexc_klines(payload):
    """
    Verwacht MEXC ‘array-of-arrays’ OF ‘dict met arrays’.
    Retourneert list[dict]: [{time, open, high, low, close, volume}]
    """
    rows = []
    if isinstance(payload, dict) and payload.get("success") and "data" in payload:
        d = payload["data"]
        keys = ("time","open","high","low","close","vol")
        if all(k in d for k in keys):
            L = min(len(d["time"]), len(d["open"]), len(d["high"]), len(d["low"]), len(d["close"]), len(d["vol"]))
            for i in range(L):
                rows.append({
                    "time": int(d["time"][i]) * (1 if d["time"][i] > 10**11 else 1),  # tijd komt in sec
                    "open": float(d["open"][i]),
                    "high": float(d["high"][i]),
                    "low":  float(d["low"][i]),
                    "close":float(d["close"][i]),
                    "volume": float(d["vol"][i]),
                })
            return rows
        # Anders: kan het array-of-arrays zijn in data?
        if isinstance(d, list) and len(d) and isinstance(d[0], list):
            payload = d  # val door naar else-blok hieronder

    if isinstance(payload, list) and payload and isinstance(payload[0], list):
        # Formaat: [ts, open, high, low, close, volume, ...]
        for arr in payload:
            if len(arr) >= 6:
                ts, o, h, l, c, v = arr[:6]
                rows.append({
                    "time": int(ts),
                    "open": float(o),
                    "high": float(h),
                    "low":  float(l),
                    "close":float(c),
                    "volume": float(v),
                })
        return rows

    raise ValueError("Unexpected kline payload structure")

def fetch_klines(sym: str, gran: str, lim: int):
    last_err = None
    for ep in (EP_A, EP_B, EP_C):
        url = ep.format(sym=sym, gran=gran, lim=lim)
        try:
            payload = http_json(url)
            rows = parse_mexc_klines(payload)
            if not rows:
                raise ValueError("Empty rows")
            return rows, url
        except Exception as e:
            last_err = f"{url} => {e}"
    raise RuntimeError(f"All MEXC kline endpoints failed for {sym}. Last: {last_err}")

def ema(vals, n):
    if not vals: return None
    k = 2/(n+1)
    ema_val = vals[0]
    for x in vals[1:]:
        ema_val = x*k + ema_val*(1-k)
    return ema_val

def pct(a,b):
    if b == 0: return 0.0
    return (a-b)/b*100.0

def build_asset(sym_pair):
    sym, label = sym_pair
    kl, used_url = fetch_klines(sym, GRAN, LIMIT)
    kl_sorted = sorted(kl, key=lambda r: int(r["time"]))
    closes = [r["close"] for r in kl_sorted]
    vols   = [r["volume"] for r in kl_sorted]
    last = kl_sorted[-1]

    ema20 = ema(closes[-20:], 20) if len(closes)>=20 else None
    ema50 = ema(closes[-50:], 50) if len(closes)>=50 else None
    last_close = last["close"]
    vol_sum = sum(vols[-20:]) if len(vols)>=20 else sum(vols)
    bias_15m = "bullish" if (ema20 and last_close>ema20) else ("bearish" if (ema20 and last_close<ema20) else "neutral")

    return {
        "symbol": sym,
        "label": label,
        "price": last_close,
        "updated_ts": int(last["time"]),
        "ema20": ema20,
        "ema50": ema50,
        "bias_15m": bias_15m,
        "vol20_sum": vol_sum,
        "source": used_url
    }

def iso_utc(ts=None):
    if ts is None:
        dt = datetime.now(timezone.utc)
    else:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00","Z")

def write_files(snap):
    # JSON
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(snap, f, indent=2, ensure_ascii=False)

    # MD
    lines = []
    lines.append(f"# Snapshot — {snap['updated_at']}\n")
    for a in snap["assets"]:
        lines.append(f"## {a['label']} ({a['symbol']})")
        lines.append(f"- **Price:** {a['price']}")
        lines.append(f"- **Bias 15m:** {a['bias_15m']}")
        lines.append(f"- **EMA20/EMA50:** {a['ema20']}/{a['ema50']}")
        lines.append(f"- **Vol(20):** {a['vol20_sum']}")
        lines.append(f"- **Data source:** {a['source']}\n")
    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def main():
    assets = [build_asset(s) for s in SYMBOLS]
    snap = {
        "exchange": "MEXC Perpetuals",
        "granularity": GRAN,
        "limit": LIMIT,
        "updated_at": iso_utc(),
        "assets": assets,
    }
    write_files(snap)
    print("OK: snapshot built.")

if __name__ == "__main__":
    main()
