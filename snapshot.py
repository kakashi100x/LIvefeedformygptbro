#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Snapshot builder for MEXC USDT-M Perpetuals (BTC/ETH/SOL).
Outputs:
  - data/summary.json
  - SNAPSHOT.md

Robust against several possible kline payload shapes from MEXC.
"""

from __future__ import annotations
import json
import os
import sys
from typing import Any, Dict, List, Tuple
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from datetime import datetime, timezone

# -----------------------------
# Config
# -----------------------------
EXCHANGE = "mexc"
BASE = "https://contract.mexc.com"
ASSETS = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]  # MEXC USDT-M perp symbols
INTERVALS = {
    "1m": "Min1",
    "15m": "Min15",
}
KLINES_LIMIT = 50  # we only need recent few; keep some headroom
TIMEOUT = 12

# output files
SUMMARY_JSON = os.path.join("data", "summary.json")
SNAPSHOT_MD = "SNAPSHOT.md"

# -----------------------------
# HTTP helper
# -----------------------------
def http_get(url: str) -> Any:
    req = Request(url, headers={"User-Agent": "snapshot/1.0"})
    try:
        with urlopen(req, timeout=TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} for {url}") from e
    except URLError as e:
        raise RuntimeError(f"Network error for {url}: {e}") from e

# -----------------------------
# Payload normalization
# -----------------------------
def normalize_klines(obj: Any) -> List[Any]:
    """
    Accepts:
      1) {"data":[...]}
      2) {"success":true,"code":0,"data":[...]}
      3) [...] (raw list)
    Returns the list inside.
    """
    if isinstance(obj, dict):
        if "data" in obj:
            rows = obj["data"]
        elif "success" in obj and "code" in obj:
            rows = obj.get("data", [])
        else:
            # Sometimes MEXC returns {"code":200,"msg":"success","data":[...]}
            if "msg" in obj and "data" in obj:
                rows = obj["data"]
            else:
                raise ValueError(f"Unexpected kline payload: {obj}")
    elif isinstance(obj, list):
        rows = obj
    else:
        raise ValueError(f"Unexpected kline payload type: {type(obj)}")

    if not isinstance(rows, list):
        raise ValueError(f"Klines not list, got: {type(rows)}")
    return rows

def kline_fields(row: Any) -> Tuple[int, float, float, float, float, float, float]:
    """
    Return tuple: (ts_ms, open, high, low, close, volume, turnover)
    Row may be:
      • list-like: [ts, open, high, low, close, volume, turnover, ...]
      • dict-like with keys among: t/time, o/open, h/high, l/low, c/close, v/vol/volume, turnover/turnoverVol
    Missing volume/turnover fallback to 0.0.
    """
    ts = o = h = l = c = v = q = None

    if isinstance(row, list) or isinstance(row, tuple):
        # Positional best-guess
        # Common MEXC format: [ts, open, high, low, close, volume, turnover]
        if len(row) >= 5:
            ts = int(row[0])
            o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4])
        if len(row) >= 6:
            v = float(row[5]) if row[5] is not None else 0.0
        if len(row) >= 7:
            q = float(row[6]) if row[6] is not None else 0.0

    elif isinstance(row, dict):
        # timestamp
        ts = row.get("t") or row.get("time") or row.get("T") or row.get("timestamp")
        ts = int(ts) if ts is not None else 0

        # prices
        o = row.get("o") or row.get("open")
        h = row.get("h") or row.get("high")
        l = row.get("l") or row.get("low")
        c = row.get("c") or row.get("close")

        # volumes
        v = row.get("v") or row.get("vol") or row.get("volume") or 0
        q = row.get("turnover") or row.get("turnoverVol") or row.get("quoteVolume") or 0

        # cast
        o = float(o); h = float(h); l = float(l); c = float(c)
        v = float(v) if v is not None else 0.0
        q = float(q) if q is not None else 0.0
    else:
        raise ValueError(f"Unknown kline row type: {type(row)}")

    # Safety casts
    ts = int(ts)
    o = float(o); h = float(h); l = float(l); c = float(c)
    v = float(v) if v is not None else 0.0
    q = float(q) if q is not None else 0.0

    return ts, o, h, l, c, v, q

# -----------------------------
# API
# -----------------------------
def get_klines(symbol: str, gran: str, limit: int) -> List[Any]:
    mexc_gran = INTERVALS[gran]
    url = f"{BASE}/api/v1/contract/kline/{symbol}?interval={mexc_gran}&limit={limit}"
    obj = http_get(url)
    return normalize_klines(obj)

# -----------------------------
# Builders
# -----------------------------
def iso_utc(dt: datetime) -> str:
    return dt.replace(microsecond=0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def ms_to_iso_utc(ts_ms: int) -> str:
    return iso_utc(datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc))

def build_symbol_block(symbol: str) -> Dict[str, Any]:
    """
    Build latest info for a symbol for 1m and 15m.
    """
    out: Dict[str, Any] = {"symbol": symbol}
    for gran in ("1m", "15m"):
        rows = get_klines(symbol, gran, limit=KLINES_LIMIT)
        if not rows:
            out[gran] = {"error": "empty klines"}
            continue

        # parse and sort by time
        parsed = [kline_fields(r) for r in rows]
        parsed.sort(key=lambda x: x[0])  # sort by ts_ms

        last = parsed[-1]
        ts_ms, o, h, l, c, v, q = last

        # previous close (if any)
        prev_close = parsed[-2][4] if len(parsed) >= 2 else c

        out[gran] = {
            "price": c,
            "prev_close": prev_close,
            "candle_time_utc": ms_to_iso_utc(ts_ms),
            "volume": v,
            "turnover": q,
            "open": o,
            "high": h,
            "low": l,
            "raw_count": len(parsed),
        }
    return out

def build_snapshot() -> Dict[str, Any]:
    now_iso = iso_utc(datetime.utcnow())
    data: Dict[str, Any] = {
        "updated_at": now_iso,
        "exchange": EXCHANGE,
        "source": f"{BASE}/api/v1/contract/kline/{{symbol}}?interval={{Min1|Min15}}",
        "notes": "USDT-M perpetuals; symbols: BTC_USDT, ETH_USDT, SOL_USDT",
        "pairs": [],
    }

    for sym in ASSETS:
        block = build_symbol_block(sym)
        data["pairs"].append(block)

    return data

# -----------------------------
# Writers
# -----------------------------
def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_snapshot_md(path: str, snap: Dict[str, Any]) -> None:
    lines: List[str] = []
    lines.append("# Live Perp Snapshot (MEXC)")
    lines.append("")
    lines.append(f"_Auto-updated_: **{snap['updated_at']}** (UTC)")
    lines.append("")
    lines.append("| Pair | TF | Price | Prev | Candle time (UTC) | Volume | Turnover |")
    lines.append("|---|---:|---:|---:|---|---:|---:|")

    def fmt_pair(sym: str) -> str:
        # BTC_USDT -> BTCUSDT look
        return sym.replace("_", "")

    for p in snap["pairs"]:
        sym = p["symbol"]
        for tf in ("1m", "15m"):
            d = p.get(tf, {})
            price = d.get("price", "")
            prev = d.get("prev_close", "")
            t = d.get("candle_time_utc", "")
            vol = d.get("volume", "")
            q = d.get("turnover", "")
            lines.append(f"| {fmt_pair(sym)} | {tf} | {price} | {prev} | {t} | {vol} | {q} |")

    lines.append("")
    lines.append("Bron: MEXC USDT-M perpetuals — Intervals: 1m & 15m — Tijden in UTC")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

# -----------------------------
# Main
# -----------------------------
def main() -> None:
    snap = build_snapshot()
    write_json(SUMMARY_JSON, snap)
    write_snapshot_md(SNAPSHOT_MD, snap)
    print(f"Wrote {SUMMARY_JSON} and {SNAPSHOT_MD} @ {snap['updated_at']}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Make the reason visible in Actions logs
        print(str(e))
        sys.exit(1)
