#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, time, math, hashlib, datetime as dt
from typing import Dict, Any, List
import urllib.request
import urllib.error

# ----------------------------
# Config
# ----------------------------
EXCHANGE = "MEXC"
BASE = "https://contract.mexc.com"  # MEXC Futures (USDT-M) public API
SYMBOLS = {
    "BTCUSDT": "BTC_USDT",
    "ETHUSDT": "ETH_USDT",
    "SOLUSDT": "SOL_USDT",
}
# Kline granularities: "Min1", "Min15"
KLINES = {"1m": "Min1", "15m": "Min15"}
KLINES_LIMIT = 200  # we berekenen RSI/EMA; we bewaren later alleen laatste 20 in snapshot
TIMEOUT = 7
RETRIES = 2

OUTDIR = "data"
OUTFILE = os.path.join(OUTDIR, "summary.json")

# ----------------------------
# Helpers
# ----------------------------
def http_get(url: str) -> Any:
    last_err = None
    for _ in range(RETRIES + 1):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "snapshot/1.0"}), timeout=TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last_err = e
            time.sleep(0.6)
    raise last_err

def ema(values: List[float], period: int) -> float:
    if len(values) < period or period < 1: return float("nan")
    k = 2 / (period + 1.0)
    ema_val = values[0]
    for v in values[1:]:
        ema_val = v * k + ema_val * (1 - k)
    return ema_val

def rsi(values: List[float], period: int = 14) -> float:
    if len(values) <= period: return float("nan")
    gains, losses = [], []
    for i in range(1, len(values)):
        ch = values[i] - values[i-1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(values)-1):
        avg_gain = (avg_gain*(period-1) + gains[i]) / period
        avg_loss = (avg_loss*(period-1) + losses[i]) / period
    if avg_loss == 0: return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100/(1+rs))

def pct(a: float, b: float) -> float:
    try:
        return (a/b - 1.0) * 100.0
    except Exception:
        return float("nan")

def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

# ----------------------------
# MEXC endpoints (public)
# ----------------------------
def get_ticker(symbol_mexc: str) -> Dict[str, Any]:
    # https://contract.mexc.com/api/v1/contract/ticker?symbol=BTC_USDT
    data = http_get(f"{BASE}/api/v1/contract/ticker?symbol={symbol_mexc}")
    # Response sample (fields used may vary slightly; adjust if needed)
    t = data.get("data", {})
    return {
        "last": float(t.get("lastPrice")) if t.get("lastPrice") is not None else None,
        "bid": float(t.get("bid1")) if t.get("bid1") is not None else None,
        "ask": float(t.get("ask1")) if t.get("ask1") is not None else None,
        "high24h": float(t.get("high24Price")) if t.get("high24Price") is not None else None,
        "low24h": float(t.get("low24Price")) if t.get("low24Price") is not None else None,
        "volume24h": float(t.get("volume24")) if t.get("volume24") is not None else None,
        "change24h_pct": float(t.get("riseFallRate")) if t.get("riseFallRate") is not None else None,  # %
        "indexPrice": float(t.get("indexPrice")) if t.get("indexPrice") is not None else None,
        "fairPrice": float(t.get("fairPrice")) if t.get("fairPrice") is not None else None,  # mark
    }

def get_funding(symbol_mexc: str) -> Dict[str, Any]:
    # https://contract.mexc.com/api/v1/contract/funding-rate?symbol=BTC_USDT
    try:
        data = http_get(f"{BASE}/api/v1/contract/funding-rate?symbol={symbol_mexc}")
        f = data.get("data", {})
        return {
            "fundingRate": float(f.get("fundingRate")) if f.get("fundingRate") is not None else None,
            "nextFundingTime": f.get("nextSettleTime"),  # ms timestamp string
        }
    except Exception:
        return {"fundingRate": None, "nextFundingTime": None}

def get_depth(symbol_mexc: str, limit: int = 5) -> Dict[str, Any]:
    # https://contract.mexc.com/api/v1/contract/depth?symbol=BTC_USDT&limit=5
    try:
        d = http_get(f"{BASE}/api/v1/contract/depth?symbol={symbol_mexc}&limit={limit}")
        data = d.get("data", {})
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        best_bid = float(bids[0][0]) if bids else None
        best_ask = float(asks[0][0]) if asks else None
        spread = (best_ask - best_bid) if (best_bid and best_ask) else None
        spread_bps = (spread / ((best_ask + best_bid)/2) * 10000) if spread and best_bid and best_ask else None
        return {
            "bestBid": best_bid, "bestAsk": best_ask,
            "spread": spread, "spread_bps": spread_bps,
            "bids": bids[:limit], "asks": asks[:limit],
        }
    except Exception:
        return {"bestBid": None, "bestAsk": None, "spread": None, "spread_bps": None, "bids": [], "asks": []}

def get_kline(symbol_mexc: str, gran: str, limit: int) -> List[Dict[str, Any]]:
    # https://contract.mexc.com/api/v1/contract/kline?symbol=BTC_USDT&interval=Min1&limit=200
    raw = http_get(f"{BASE}/api/v1/contract/kline?symbol={symbol_mexc}&interval={gran}&limit={limit}")
    arr = raw.get("data") or []
    # Expected item order: [t, open, high, low, close, volume, ...]  (adjust if API differs)
    kl = []
    for it in arr:
        # be tolerant for dict/array formats
        if isinstance(it, list) and len(it) >= 6:
            ts, o, h, l, c, v = it[0], it[1], it[2], it[3], it[4], it[5]
            kl.append({
                "t": int(ts), "o": float(o), "h": float(h), "l": float(l), "c": float(c), "v": float(v)
            })
        elif isinstance(it, dict):
            kl.append({
                "t": int(it.get("time", 0)),
                "o": float(it.get("open", "nan")),
                "h": float(it.get("high", "nan")),
                "l": float(it.get("low", "nan")),
                "c": float(it.get("close", "nan")),
                "v": float(it.get("volume", "nan")),
            })
    # oldest -> newest per MEXC; ensure sort
    kl.sort(key=lambda x: x["t"])
    return kl

def indicators_from_klines(kl: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = [k["c"] for k in kl]
    last_close = closes[-1] if closes else None
    ema20 = ema(closes[-100:], 20) if closes else float("nan")
    ema50 = ema(closes[-150:], 50) if closes else float("nan")
    rsi14 = rsi(closes[-120:], 14) if closes else float("nan")
    return {
        "lastClose": last_close,
        "ema20": None if math.isnan(ema20) else round(ema20, 6),
        "ema50": None if math.isnan(ema50) else round(ema50, 6),
        "rsi14": None if math.isnan(rsi14) else round(rsi14, 3),
        "trend": ("bullish" if last_close and ema20 and last_close > ema20 else
                  "bearish" if last_close and ema20 and last_close < ema20 else "neutral")
    }

# ----------------------------
# Main
# ----------------------------
def build_snapshot() -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "exchange": EXCHANGE,
        "type": "perpetuals",
        "updated_at": now_utc_iso(),
        "symbols": {}
    }
    for std, mexc in SYMBOLS.items():
        ticker = get_ticker(mexc)
        funding = get_funding(mexc)
        depth = get_depth(mexc, limit=5)

        per_symbol = {
            "symbol": std,
            "mexc_symbol": mexc,
            "last": ticker["last"],
            "bid": depth["bestBid"] or ticker["bid"],
            "ask": depth["bestAsk"] or ticker["ask"],
            "spread": depth["spread"],
            "spread_bps": depth["spread_bps"],
            "index": ticker["indexPrice"],
            "mark": ticker["fairPrice"],
            "high24h": ticker["high24h"],
            "low24h": ticker["low24h"],
            "volume24h": ticker["volume24h"],
            "change24h_pct": ticker["change24h_pct"],  # %
            "fundingRate": funding["fundingRate"],
            "nextFundingTime": funding["nextFundingTime"],
            "klines": {},
        }

        # Kliness + lite-indicators
        for label, gran in KLINES.items():
            kl = get_kline(mexc, gran, KLINES_LIMIT)
            inds = indicators_from_klines(kl)
            # bewaar laatste 20 candles voor snapshot (memory friendly)
            per_symbol["klines"][label] = {
                "last20": kl[-20:],
                "indicators": inds,
            }

        # basis vs index/mark (basis = mark/index - 1 in bps):
        if per_symbol["index"] and per_symbol["mark"]:
            basis_bps = (per_symbol["mark"]/per_symbol["index"] - 1.0) * 10000
        else:
            basis_bps = None
        per_symbol["basis_bps"] = basis_bps

        out["symbols"][std] = per_symbol

    # compacte “digest” voor pricechek
    out["digest"] = {
        k: {
            "last": v["last"],
            "bid": v["bid"],
            "ask": v["ask"],
            "spread_bps": v["spread_bps"],
            "change24h_pct": v["change24h_pct"],
            "fundingRate": v["fundingRate"],
            "basis_bps": v["basis_bps"],
            "1m": v["klines"]["1m"]["indicators"],
            "15m": v["klines"]["15m"]["indicators"],
        } for k, v in out["symbols"].items()
    }
    return out

def file_hash(path: str) -> str:
    if not os.path.exists(path): return ""
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()

def main():
    os.makedirs(OUTDIR, exist_ok=True)
    before = file_hash(OUTFILE)
    snap = build_snapshot()
    with open(OUTFILE, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)
    after = file_hash(OUTFILE)
    changed = (before != after)
    # print een korte logregel (handig voor Actions)
    print(f"snapshot_written changed={changed} updated_at={snap['updated_at']}")
    # exit 0 altijd; commit/doen we in workflow
    return 0

if __name__ == "__main__":
    sys.exit(main())
