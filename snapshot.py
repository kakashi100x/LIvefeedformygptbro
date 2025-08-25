#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Builds a fresh snapshot for BTC/ETH/SOL perpetuals from MEXC Contract (perp) API.

Outputs:
  - data/summary.json
  - SNAPSHOT.md

Robustness:
  - Tries multiple MEXC kline endpoints (query & path style)
  - Accepts klines returned as list OR wrapped in dicts under "data"
  - Accepts row shapes: dict with o/h/l/c/time/volume (or o/c/h/l/t/v) OR list-indexed rows
  - Graceful fallbacks and clear errors for GitHub Actions logs
"""

import json
import os
import sys
import time
import math
import traceback
from typing import Any, Dict, List, Tuple
from datetime import datetime, timezone

import urllib.request
import urllib.error

# -------------------------
# Config
# -------------------------
SYMBOLS = {
    "BTCUSDT": "BTC_USDT",
    "ETHUSDT": "ETH_USDT",
    "SOLUSDT": "SOL_USDT",
}

# hoeveel 1m candles meenemen in snapshot (laatste N)
KLINES_LIMIT = 60  # 1 uur aan 1m, prima voor 15m bias etc.

TIMEOUT = 15  # seconds
UA = "snapshot/1.1 (+github-actions)"

# MEXC Contract (perp) klines – we proberen 2 varianten
KLINE_ENDPOINTS = [
    # query-variant
    "https://contract.mexc.com/api/v1/contract/kline?symbol={sym}&interval=Min1&limit={limit}",
    # path-variant (sommige mirrors/documentatie gebruiken dit pad)
    "https://contract.mexc.com/api/v1/contract/kline/{sym}?interval=Min1&limit={limit}",
]

# optioneel: ticker voor extra sanity info (niet hard afhankelijk)
TICKER_ENDPOINT = "https://contract.mexc.com/api/v1/contract/ticker?symbol={sym}"


# -------------------------
# HTTP helper
# -------------------------
def http_get(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        data = resp.read()
    try:
        return json.loads(data.decode("utf-8"))
    except Exception:
        # laat raw string zien in foutlog indien geen JSON
        raise ValueError(f"Non-JSON response from {url}: {data[:200]!r}")


# -------------------------
# Kline normalisatie
# -------------------------
def normalize_klines(obj: Any) -> List[Any]:
    """
    Accepteert:
      1) {"data":[...]}
      2) {"data":{...}}       <-- enkele candle in dict → wrap in list (FIX)
      3) {"success":true,"code":0,"data":[...]}
      4) [... ] (raw list)
    Retourneert altijd een list met rows.
    """
    rows = None
    if isinstance(obj, dict):
        # Meest voorkomend bij MEXC: data onder "data"
        if "data" in obj:
            rows = obj["data"]
        # Sommige gateways zetten success/code erbij
        elif "success" in obj and "code" in obj:
            rows = obj.get("data", [])
        else:
            # Soms geven ze een foutobject terug — laat het zien
            raise ValueError(f"Unexpected kline payload: {obj}")

        # *** Belangrijke FIX: enkele dict → lijst maken
        if isinstance(rows, dict):
            rows = [rows]

    elif isinstance(obj, list):
        rows = obj
    else:
        raise ValueError(f"Unexpected kline payload type: {type(obj)}")

    if not isinstance(rows, list):
        raise ValueError(f"Klines not list, got {type(rows)}")

    return rows


def parse_kline_row(row: Any) -> Tuple[int, float, float, float, float, float]:
    """
    Zet 1 row om naar (ts_ms, open, high, low, close, volume)

    Ondersteunt:
      - dict met keys: time/ts/T, o/open, h/high, l/low, c/close, v/volume/amount/turnover
      - list: [time, open, high, low, close, volume, ...] (indices 0..5)
    """
    def to_f(x):
        if x is None:
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        return float(str(x))

    def to_i(x):
        if x is None:
            return 0
        if isinstance(x, int):
            return x
        return int(float(str(x)))

    if isinstance(row, dict):
        # normaliseer keys lower
        lower = {str(k).lower(): v for k, v in row.items()}
        # tijd
        ts = lower.get("time", lower.get("t", lower.get("ts", lower.get("timestamp"))))
        ts = to_i(ts)
        # MEXC geeft ms; als het seconden zijn, opschalen
        if ts < 10_000_000_000:  # ~ 2001 in sec, dus te laag → ms nodig
            ts *= 1000

        # ohlc
        o = lower.get("open", lower.get("o"))
        h = lower.get("high", lower.get("h"))
        l = lower.get("low",  lower.get("l"))
        c = lower.get("close", lower.get("c"))

        # volume: pak wat beschikbaar is
        vol = (
            lower.get("volume")
            or lower.get("v")
            or lower.get("amount")
            or lower.get("turnover")
            or lower.get("turnover24h")
            or 0
        )
        return ts, to_f(o), to_f(h), to_f(l), to_f(c), to_f(vol)

    if isinstance(row, list):
        # meest gangbaar: [t, o, h, l, c, v, ...]
        if len(row) < 6:
            raise ValueError(f"Unexpected short kline list: {row}")
        ts = to_i(row[0])
        if ts < 10_000_000_000:
            ts *= 1000
        o, h, l, c, v = to_f(row[1]), to_f(row[2]), to_f(row[3]), to_f(row[4]), to_f(row[5])
        return ts, o, h, l, c, v

    raise ValueError(f"Unsupported kline row type: {type(row)}")


# -------------------------
# Business helpers
# -------------------------
def iso_utc(ts_ms: int) -> str:
    return datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0


def fetch_klines_for_symbol(symbol_q: str) -> Tuple[List[Tuple[int, float, float, float, float, float]], str]:
    """
    Probeert de endpoints in KLINE_ENDPOINTS, retourneert:
      - lijst met tuples (ts_ms, o, h, l, c, v)
      - de effectieve bron-URL (debug)
    """
    last_err = None
    for tpl in KLINE_ENDPOINTS:
        url = tpl.format(sym=symbol_q, limit=KLINES_LIMIT)
        try:
            raw = http_get(url)
            rows = normalize_klines(raw)
            parsed = [parse_kline_row(r) for r in rows]
            # sorteer op tijd
            parsed.sort(key=lambda x: x[0])
            return parsed, url
        except Exception as e:
            last_err = f"{url} -> {e}"
            continue
    raise RuntimeError(f"All MEXC kline endpoints failed for {symbol_q}. Last: {last_err}")


def build_asset_block(symbol_name: str, symbol_mexc: str) -> Tuple[Dict[str, Any], List[str]]:
    """
    Bouwt de data voor één asset.
    Returns:
      - asset dict
      - gebruikte bronnen (urls)
    """
    sources = []
    klines, used_url = fetch_klines_for_symbol(symbol_mexc)
    sources.append(used_url)

    if not klines:
        raise RuntimeError(f"No klines for {symbol_name}")

    # laatste candle
    ts, o, h, l, c, v = klines[-1]
    prev_close = klines[-2][4] if len(klines) >= 2 else o
    change_15m = None
    # simpele 15m-change: vergelijk laatste close met close 15 candles terug indien beschikbaar
    if len(klines) >= 16:
        change_15m = pct(c, klines[-16][4])

    asset = {
        "pair": symbol_name,
        "tf": "1m",
        "price": c,
        "prev_close": prev_close,
        "change_1m_pct": pct(c, prev_close),
        "change_15m_pct": change_15m,
        "candle_time_utc": iso_utc(ts),
        "volume_last": v,
        # compacte 1m-ohlc voor de laatste N (tijd, o, h, l, c, v)
        "klines_1m": [
            [t, float(oo), float(hh), float(ll), float(cc), float(vv)]
            for (t, oo, hh, ll, cc, vv) in klines[-KLINES_LIMIT:]
        ],
    }

    return asset, sources


def build_snapshot() -> Tuple[Dict[str, Any], List[str]]:
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    snapshot_pairs = {}
    used_sources = set()

    for human, mexc in SYMBOLS.items():
        block, srcs = build_asset_block(human, mexc)
        snapshot_pairs[human] = block
        for s in srcs:
            used_sources.add(s)

    snapshot = {
        "updated_at": now_iso,
        "exchange": "mexc",
        "kind": "perpetual",
        "interval": "1m",
        "pairs": snapshot_pairs,
        "sources": sorted(used_sources),
        "note": "OKX PERP replaced by MEXC PERP per user preference.",
    }
    return snapshot, sorted(used_sources)


def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_snapshot_md(path: str, snap: Dict[str, Any]) -> None:
    pairs = snap["pairs"]
    lines = []
    lines.append("# Live Perp Snapshot (MEXC)")
    lines.append("")
    lines.append(f"_Auto-updated_: **{snap['updated_at']}** (UTC)")
    lines.append("")
    lines.append("| Pair | TF | Price | % 1m | % 15m | Candle time (UTC) | Volume (last 1m) |")
    lines.append("|:----:|:--:|------:|-----:|------:|:------------------:|-----------------:|")

    for k in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
        a = pairs[k]
        ch1 = f"{a['change_1m_pct']:.2f}%" if a["change_1m_pct"] is not None else "-"
        ch15 = f"{a['change_15m_pct']:.2f}%" if a["change_15m_pct"] is not None else "-"
        lines.append(
            f"| {k} | {a['tf']} | {a['price']:.6f} | {ch1} | {ch15} | {a['candle_time_utc']} | {a['volume_last']:.4f} |"
        )

    lines.append("")
    lines.append("**Bron:** MEXC perpetual klines (1m). Tijden in UTC.")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps({"sources": snap.get("sources", [])}, ensure_ascii=False, indent=2))
    lines.append("```")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main() -> None:
    try:
        snap, _src = build_snapshot()
        write_json("data/summary.json", snap)
        write_snapshot_md("SNAPSHOT.md", snap)
        print("Snapshot built OK")
    except Exception as e:
        print(str(e))
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
