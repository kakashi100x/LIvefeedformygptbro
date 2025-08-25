#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Snapshot builder for MEXC USDT-perpetuals (BTC/ETH/SOL).
- Pulls 1m and 15m klines from MEXC contract API
- Parses array-based kline rows by index (NOT dict keys)
- Writes a machine-friendly JSON at data/summary.json
- Writes a human snapshot table at SNAPSHOT.md
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any
import urllib.request
import urllib.error

# ----------------------
# Config
# ----------------------
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]         # MEXC contract symbols (USDT-margined perps)
INTERVALS = {"1m": "Min1", "15m": "Min15"}          # our granularity -> MEXC granularity
KLINES_LIMIT = 200                                  # enough to compute quick stats if we want later
TIMEOUT = 15
BASE = "https://contract.mexc.com"

OUT_JSON = Path("data/summary.json")
OUT_MD = Path("SNAPSHOT.md")

# ----------------------
# HTTP helpers
# ----------------------
def http_get(url: str) -> Any:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "snapshot/1.0"},
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        data = resp.read()
    return json.loads(data)

# ----------------------
# MEXC API
# ----------------------
def get_klines(symbol: str, gran: str, limit: int) -> List[List[Any]]:
    """
    Returns list of klines, where each kline row is an ARRAY:
      [0]=ts(ms), [1]=open, [2]=high, [3]=low, [4]=close, [5]=volume(base), [6]=turnover(quote)
    """
    mexc_gran = INTERVALS[gran]  # "1m" -> "Min1", "15m" -> "Min15"
    url = f"{BASE}/api/v1/contract/kline/{symbol}?interval={mexc_gran}&limit={limit}"
    obj = http_get(url)
    # MEXC wraps the list under "data" or returns list directly depending on endpoint version;
    # normalize to a plain list of rows:
    if isinstance(obj, dict) and "data" in obj:
        rows = obj["data"]
    else:
        rows = obj
    if not isinstance(rows, list):
        raise ValueError("Unexpected kline payload")
    return rows

# ----------------------
# Parsing helpers (ARRAY indices!)
# ----------------------
def parse_ohlc_list(rows: List[List[Any]]):
    """Return closes list (ascending by time) + sorted rows."""
    if not rows:
        return [], []
    rows_sorted = sorted(rows, key=lambda r: int(r[0]))  # sort by ts (ms)
    closes = [float(r[4]) for r in rows_sorted]
    return closes, rows_sorted

def parse_latest_ohlc(rows: List[List[Any]]) -> Dict[str, Any]:
    """Return dict for the LAST kline (most recent)."""
    if not rows:
        raise ValueError("Empty kline list")
    last = sorted(rows, key=lambda r: int(r[0]))[-1]
    return {
        "ts": int(last[0]),
        "open": float(last[1]),
        "high": float(last[2]),
        "low":  float(last[3]),
        "close": float(last[4]),
        "volume": float(last[5]) if len(last) > 5 and last[5] is not None else None,       # base vol
        "turnover": float(last[6]) if len(last) > 6 and last[6] is not None else None,     # quote vol
    }

def iso_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def pretty_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

# ----------------------
# Snapshot assembly
# ----------------------
def build_symbol_block(symbol: str) -> Dict[str, Any]:
    # 1m and 15m klines
    rows_1m = get_klines(symbol, "1m", KLINES_LIMIT)
    rows_15m = get_klines(symbol, "15m", KLINES_LIMIT)

    closes_1m, rows_1m_sorted = parse_ohlc_list(rows_1m)
    closes_15m, rows_15m_sorted = parse_ohlc_list(rows_15m)

    latest_1m = parse_latest_ohlc(rows_1m_sorted)
    latest_15m = parse_latest_ohlc(rows_15m_sorted)

    # Simple 15m bias hint (last close vs previous)
    bias_15m = "neutral"
    if len(closes_15m) >= 2:
        if closes_15m[-1] > closes_15m[-2]:
            bias_15m = "bullish"
        elif closes_15m[-1] < closes_15m[-2]:
            bias_15m = "bearish"

    return {
        "symbol": symbol,
        "latest_1m": latest_1m,
        "latest_15m": latest_15m,
        "close_prev_15m": float(closes_15m[-2]) if len(closes_15m) >= 2 else None,
        "bias_15m": bias_15m,
    }

def build_snapshot() -> Dict[str, Any]:
    assets = {}
    for sym in SYMBOLS:
        assets[sym] = build_symbol_block(sym)

    now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")
    return {
        "updated_at": now_utc,
        "exchange": "mexc-perp",
        "intervals": ["1m", "15m"],
        "source": f"{BASE}/api/v1/contract/kline/{{symbol}}",
        "assets": assets,
    }

# ----------------------
# Output writers
# ----------------------
def write_json(snap: Dict[str, Any]):
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)

def write_markdown(snap: Dict[str, Any]):
    lines = []
    lines.append("# Live Perp Snapshot (MEXC)")
    lines.append("")
    lines.append(f"_Auto-updated:_ `{snap['updated_at']}`  \n_Intervals:_ 1m & 15m  \n_Source:_ MEXC contract klines")
    lines.append("")
    lines.append("| Pair | TF | Price | Prev 15m close | Candle time (UTC) | Volume (base) | Turnover (quote) | Bias 15m |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---|")
    for sym in SYMBOLS:
        a = snap["assets"][sym]
        l1 = a["latest_1m"]
        l15 = a["latest_15m"]
        prev15 = a["close_prev_15m"]
        lines.append(
            f"| {sym} | 1m | {l1['close']:,.4f} | — | {pretty_utc(l1['ts'])} | "
            f"{(l1['volume'] or 0):,.2f} | {(l1['turnover'] or 0):,.2f} | — |"
        )
        lines.append(
            f"| {sym} | 15m | {l15['close']:,.4f} | {(prev15 if prev15 is not None else 0):,.4f} | {pretty_utc(l15['ts'])} | "
            f"{(l15['volume'] or 0):,.2f} | {(l15['turnover'] or 0):,.2f} | {a['bias_15m']} |"
        )
    lines.append("")
    lines.append("_Note: Values are MEXC USDT-perp klines; times in UTC_")
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")

# ----------------------
# Main
# ----------------------
def main():
    try:
        snap = build_snapshot()
        write_json(snap)
        write_markdown(snap)
        # Helpful console output for Action logs
        print("Wrote:", OUT_JSON.as_posix(), "and", OUT_MD.as_posix())
        # Quick echo of latest prices
        for sym in SYMBOLS:
            close = snap["assets"][sym]["latest_1m"]["close"]
            print(f"{sym} 1m close: {close}")
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        raise SystemExit(f"HTTP error fetching klines: {e}")
    except Exception as e:
        raise SystemExit(e)

if __name__ == "__main__":
    main()
