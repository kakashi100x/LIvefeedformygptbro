#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
snapshot.py  —  MEXC Perpetual snapshot
- Fetch 1m & 15m klines for BTC/ETH/SOL (MEXC PERP).
- Compute EMA(20/50/200) and RSI(14) on both TFs.
- Infer 15m bias + short confidence note.
- Save to data/summary.json and SNAPSHOT.md.

Runs fine on GitHub Actions without extra deps.
"""

import os
import json
import math
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# ------------------------
# Config
# ------------------------

BASE = "https://contract.mexc.com"  # MEXC Perpetual API
SYMBOLS = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]
KLINES_LIMIT = 300  # enough for EMA200 on 1m/15m
OUT_JSON = os.path.join("data", "summary.json")
OUT_MD = "SNAPSHOT.md"

SUPPORTED_INTERVALS = {
    "1m": "Min1",
    "15m": "Min15",
}

# ------------------------
# HTTP helpers
# ------------------------

def http_get(url, timeout=12, retries=2, backoff=0.6):
    last_err = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "snapshot/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            last_err = e
            if i < retries:
                time.sleep(backoff * (2 ** i))
            else:
                raise
    raise last_err

def get_kline(symbol: str, gran_key: str, limit: int = 200):
    """
    Fetch MEXC contract klines.
    symbol: "BTC_USDT", "ETH_USDT", "SOL_USDT"
    gran_key: "1m" or "15m" (mapped to MEXC interval strings)
    """
    if gran_key not in SUPPORTED_INTERVALS:
        raise ValueError(f"Unsupported interval {gran_key}")
    gran = SUPPORTED_INTERVALS[gran_key]
    url = f"{BASE}/api/v1/contract/kline/{symbol}?interval={gran}&limit={limit}"
    data = http_get(url)
    # Expected: {"success":true,"code":0,"data":[{...}, ...]}
    if not data or not data.get("success", False) or "data" not in data:
        raise RuntimeError(f"Unexpected kline response for {symbol}: {data!r}")
    return data["data"]

def parse_ohlc_list(rows):
    """
    Converts raw MEXC kline rows to arrays of close prices and meta rows.
    Each row contains keys: time, open, high, low, close, vol, amount
    """
    if not rows:
        return [], []
    # sort by time just in case
    rows = sorted(rows, key=lambda r: int(r["time"]))
    closes = [float(r["close"]) for r in rows]
    return closes, rows

def parse_latest_ohlc(rows):
    if not rows:
        raise ValueError("Empty kline list")
    last = rows[-1]
    return {
        "ts": int(last["time"]),                 # ms epoch
        "open": float(last["open"]),
        "high": float(last["high"]),
        "low": float(last["low"]),
        "close": float(last["close"]),
        "volume": float(last.get("vol", 0)),     # contract volume
        "turnover": float(last.get("amount", 0)) # quote turnover
    }

# ------------------------
# Indicators (no deps)
# ------------------------

def ema(values, period):
    if len(values) < period:
        return [math.nan] * len(values)
    k = 2.0 / (period + 1)
    out = []
    ema_prev = sum(values[:period]) / period
    # seed
    for i in range(period):
        out.append(math.nan)
    out.append(ema_prev)
    for v in values[period+1:]:
        ema_prev = v * k + ema_prev * (1 - k)
        out.append(ema_prev)
    return out

def rsi(values, period=14):
    if len(values) < period + 1:
        return [math.nan] * len(values)
    gains = [0.0]
    losses = [0.0]
    for i in range(1, len(values)):
        ch = values[i] - values[i-1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    # seed average
    avg_gain = sum(gains[1:period+1]) / period
    avg_loss = sum(losses[1:period+1]) / period
    rsis = [math.nan] * (period)
    # first RSI
    rs = avg_gain / avg_loss if avg_loss > 0 else float('inf')
    rsis.append(100 - (100 / (1 + rs)))
    # Wilder smoothing
    for i in range(period+1, len(values)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsis.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsis.append(100 - (100 / (1 + rs)))
    return rsis

# ------------------------
# Bias + confidence
# ------------------------

def infer_bias_15m(close, ema20, ema50, ema200, rsi14):
    """
    Simple, robust bias heuristic:
    - bullish: close > EMA50 and EMA20 > EMA50; bonus if EMA50 > EMA200 and RSI>55
    - bearish: close < EMA50 and EMA20 < EMA50; bonus if EMA50 < EMA200 and RSI<45
    - else: neutral
    Returns (bias, confidence:str)
    """
    c = close[-1]
    e20 = ema20[-1]
    e50 = ema50[-1]
    e200 = ema200[-1]
    r = rsi14[-1]

    score = 0
    if c > e50 and e20 > e50:
        score += 2
        if e50 > e200: score += 1
        if r > 55: score += 1
        bias = "bullish"
    elif c < e50 and e20 < e50:
        score += 2
        if e50 < e200: score += 1
        if r < 45: score += 1
        bias = "bearish"
    else:
        bias = "neutral"

    if bias == "neutral":
        conf = "weak / mixed structure"
    else:
        conf = "strong" if score >= 3 else "moderate"
    return bias, conf

# ------------------------
# Build snapshot per symbol
# ------------------------

def build_symbol_block(symbol: str):
    # 1m
    rows_1m = get_kline(symbol, "1m", KLINES_LIMIT)
    closes_1m, rows_1m_sorted = parse_ohlc_list(rows_1m)
    ema20_1m = ema(closes_1m, 20)
    ema50_1m = ema(closes_1m, 50)
    ema200_1m = ema(closes_1m, 200)
    rsi14_1m = rsi(closes_1m, 14)
    last_1m = parse_latest_ohlc(rows_1m_sorted)

    # 15m
    rows_15m = get_kline(symbol, "15m", KLINES_LIMIT)
    closes_15m, rows_15m_sorted = parse_ohlc_list(rows_15m)
    ema20_15m = ema(closes_15m, 20)
    ema50_15m = ema(closes_15m, 50)
    ema200_15m = ema(closes_15m, 200)
    rsi14_15m = rsi(closes_15m, 14)
    last_15m = parse_latest_ohlc(rows_15m_sorted)

    bias15, conf15 = infer_bias_15m(closes_15m, ema20_15m, ema50_15m, ema200_15m, rsi14_15m)

    return {
        "symbol": symbol,
        "perp": True,
        "timeframes": {
            "1m": {
                "latest": last_1m,
                "ema": {
                    "ema20": ema20_1m[-1],
                    "ema50": ema50_1m[-1],
                    "ema200": ema200_1m[-1],
                },
                "rsi14": rsi14_1m[-1]
            },
            "15m": {
                "latest": last_15m,
                "ema": {
                    "ema20": ema20_15m[-1],
                    "ema50": ema50_15m[-1],
                    "ema200": ema200_15m[-1],
                },
                "rsi14": rsi14_15m[-1],
                "bias": {
                    "state": bias15,
                    "confidence": conf15
                }
            }
        }
    }

# ------------------------
# Snapshot builder
# ------------------------

def now_utc_iso():
    # ISO8601 in UTC with 'Z'
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def build_snapshot():
    assets = []
    for sym in SYMBOLS:
        assets.append(build_symbol_block(sym))
    snap = {
        "updated_at": now_utc_iso(),
        "exchange": "mexc",
        "kind": "perpetual",
        "source": f"{BASE}/api/v1/contract/kline/{{symbol}}?interval={{interval}}",
        "intervals": ["1m", "15m"],
        "assets": assets,
    }
    return snap

# ------------------------
# Writers
# ------------------------

def write_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def write_md(path, snapshot):
    def fmt_ts(ms):
        return datetime.fromtimestamp(ms/1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    rows = []
    rows.append(f"### Live Perp Snapshot (MEXC)\n")
    rows.append(f"_Auto-updated_: {snapshot['updated_at']}\n")
    rows.append("| Pair | TF | Price | TS (UTC) | 15m Bias | RSI(15m) | Vol(1m) |")
    rows.append("|---|---:|---:|---|---|---:|---:|")
    for a in snapshot["assets"]:
        sym = a["symbol"]
        l1 = a["timeframes"]["1m"]["latest"]
        l15 = a["timeframes"]["15m"]["latest"]
        bias = a["timeframes"]["15m"]["bias"]["state"]
        rsi15 = a["timeframes"]["15m"]["rsi14"]
        rows.append(f"| {sym} | 1m | {l1['close']:.2f} | {fmt_ts(l1['ts'])} |  |  | {l1.get('volume', 0):.0f} |")
        rows.append(f"| {sym} | 15m | {l15['close']:.2f} | {fmt_ts(l15['ts'])} | {bias} | {rsi15:.1f} |  |")
    rows.append("\n_Bron: MEXC perpetual futures klines. Intervals: 1m & 15m. Tijden in UTC._\n")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(rows))

# ------------------------
# Main
# ------------------------

def main():
    snap = build_snapshot()
    write_json(OUT_JSON, snap)
    write_md(OUT_MD, snap)
    print(f"Wrote {OUT_JSON} and {OUT_MD}")
    # Minimal console echo (handig in Actions logs)
    for a in snap["assets"]:
        p = a["timeframes"]["1m"]["latest"]["close"]
        print(f"{a['symbol']} 1m close: {p}")

if __name__ == "__main__":
    main()
