#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MEXC Perp snapshot -> data/summary.json + SNAPSHOT.md

Coins: BTC_USDT, ETH_USDT, SOL_USDT
TFs:  1m (laatste close/prev) + 15m (laatste bar + %change)

Robuust tegen MEXC responses waar 'data' soms als string i.p.v. lijst terugkomt.
"""

import os
import json
import ast
import time
import math
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------
SYMBOLS = [
    ("BTC_USDT", "BTCUSDT"),
    ("ETH_USDT", "ETHUSDT"),
    ("SOL_USDT", "SOLUSDT"),
]

KLINES_LIMIT_1M = 60       # laatste 60 minuten
KLINES_LIMIT_15M = 200     # ~2 dagen
TIMEOUT = 15
UA = "snapshot/1.0 (+https://github.com/kakashi100x/LIvefeedformygptbro)"

OUT_DIR = "data"
SUMMARY_JSON = os.path.join(OUT_DIR, "summary.json")
SNAPSHOT_MD = "SNAPSHOT.md"

# ---------------------------------------------------------
# HTTP
# ---------------------------------------------------------
def http_get(url: str, timeout: int = TIMEOUT) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        # Soms komt er geen zuivere JSON terug; keer dan bytes/str terug
        return raw.decode("utf-8", errors="replace")

# ---------------------------------------------------------
# Normalisatie helpers
# ---------------------------------------------------------
def parse_data_maybe_string(obj: Any) -> Any:
    """
    Sommige MEXC responses hebben 'data' als string met een array-inhoud.
    Probeer JSON -> anders ast.literal_eval -> anders raise.
    """
    if isinstance(obj, str):
        # probeer JSON
        try:
            return json.loads(obj)
        except json.JSONDecodeError:
            pass
        # probeer Python literal (safe)
        try:
            return ast.literal_eval(obj)
        except Exception:
            # als het er uitziet als iets heel lang: kort maken in error
            preview = obj[:200] + ("..." if len(obj) > 200 else "")
            raise ValueError(f"Kline 'data' was string but not parseable: {preview}")
    return obj

def normalize_klines(payload: Any) -> List[list]:
    """
    Zorg dat we altijd een lijst van rijen teruggeven.
    Toegestane vormen:
    - dict met 'data' (kan list of string zijn)
    - lijst (rechtstreeks klines)
    - string (probeer alsnog te parsen)
    Elke rij moet minimaal [ts, open, high, low, close, volume] bevatten.
    """
    obj = payload

    # Als de hele payload een string is, probeer te parsen
    obj = parse_data_maybe_string(obj)

    rows = None
    if isinstance(obj, dict):
        # MEXC structuur: {'success': True, 'code': 0, 'data': [...]}
        rows = obj.get("data")
        rows = parse_data_maybe_string(rows)
    elif isinstance(obj, list):
        rows = obj
    else:
        raise ValueError(f"Unexpected kline payload type: {type(obj)}")

    if isinstance(rows, dict):
        # zeer zeldzaam, maar maak lijst van single dict
        rows = [rows]

    if not isinstance(rows, list):
        raise ValueError(f"Klines not list, got {type(rows)}")

    # controle: elk item moet indexeerbaar zijn
    if len(rows) == 0:
        return []

    if not isinstance(rows[0], (list, tuple)):
        # Soms komt er nog 1 niveau string omheen
        rows = parse_data_maybe_string(rows)
        if not isinstance(rows, list) or (rows and not isinstance(rows[0], (list, tuple))):
            raise ValueError("Unexpected kline row structure")

    return rows

# ---------------------------------------------------------
# Kline ophalen (1m, 15m)
# ---------------------------------------------------------
def fetch_klines_for_symbol(symbol: str) -> Dict[str, Tuple[List[list], str]]:
    """
    Haal 1m en 15m op. Return dict:
    { '1m': (rows, used_url), '15m': (rows, used_url) }
    Gooit RuntimeError als beide endpoints falen voor een TF.
    """
    base = "https://contract.mexc.com/api/v1/contract/kline/{sym}?interval={interval}&limit={limit}"
    endpoints = {
        "1m": base.format(sym=symbol, interval="Min1", limit=KLINES_LIMIT_1M),
        "15m": base.format(sym=symbol, interval="Min15", limit=KLINES_LIMIT_15M),
    }

    out: Dict[str, Tuple[List[list], str]] = {}

    for tf, url in endpoints.items():
        last_err = None
        used = None
        for attempt in range(2):  # simpele retry
            try:
                payload = http_get(url)
                rows = normalize_klines(payload)
                used = url
                out[tf] = (rows, used)
                break
            except Exception as e:
                last_err = e
                time.sleep(0.7)
        if tf not in out:
            raise RuntimeError(f"All MEXC kline endpoints failed for {symbol}. Last: {url} => {last_err}")

    return out

# ---------------------------------------------------------
# Data extractie helpers
# ---------------------------------------------------------
def ts_to_iso_utc(ts_sec: float) -> str:
    # MEXC timestamps lijken in seconden te komen
    dt = datetime.fromtimestamp(float(ts_sec), tz=timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def candle_from_rows(rows: List[list]) -> Dict[str, Any]:
    """
    Verwacht MEXC kline: [ts, open, high, low, close, volume]
    """
    if not rows:
        return {}
    last = rows[-1]
    prev = rows[-2] if len(rows) > 1 else last

    try:
        ts = float(last[0])
        o = float(last[1]); h = float(last[2]); l = float(last[3]); c = float(last[4])
        vol = float(last[5])
        pclose = float(prev[4]) if prev is not last else c
    except Exception as e:
        raise ValueError(f"Unexpected row format: {last} ({e})")

    return {
        "time": ts_to_iso_utc(ts),
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "prev_close": pclose,
        "volume": vol,
    }

def pct_change(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b * 100.0

# ---------------------------------------------------------
# Snapshot bouwen
# ---------------------------------------------------------
def build_asset_block(human: str, mexc_symbol: str) -> Dict[str, Any]:
    kl = fetch_klines_for_symbol(mexc_symbol)

    one = candle_from_rows(kl["1m"][0])
    f15_rows = kl["15m"][0]
    f15 = candle_from_rows(f15_rows)

    # 15m %change t.o.v. vorige 15m close
    change15 = pct_change(f15["close"], f15["prev_close"])

    return {
        "pair": human,
        "tf": {
            "1m": {
                "price": one["close"],
                "prev": one["prev_close"],
                "candle_time": one["time"],
            },
            "15m": {
                "price": f15["close"],
                "prev": f15["prev_close"],
                "candle_time": f15["time"],
                "volume": f15["volume"],
                "change_pct": round(change15, 3),
            },
        },
        "sources": {
            "m1": kl["1m"][1],
            "m15": kl["15m"][1],
        },
    }

def build_snapshot() -> Dict[str, Any]:
    assets = []
    for mexc_symbol, human in SYMBOLS:
        block = build_asset_block(human, mexc_symbol)
        assets.append(block)

    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "updated_at": now_iso,
        "exchange": "mexc",
        "market": "perpetual",
        "note": "prices from MEXC contract kline endpoints; times are UTC",
        "data": assets,
    }

# ---------------------------------------------------------
# Output writers
# ---------------------------------------------------------
def ensure_outdir():
    if not os.path.isdir(OUT_DIR):
        os.makedirs(OUT_DIR, exist_ok=True)

def write_summary_json(obj: Dict[str, Any]):
    ensure_outdir()
    with open(SUMMARY_JSON, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_snapshot_md(obj: Dict[str, Any]):
    lines = []
    lines.append("# Live Perp Snapshot (MEXC)")
    lines.append("")
    lines.append(f"_Auto-updated_: **{obj['updated_at']}** (UTC)")
    lines.append("")
    lines.append("| Pair | TF | Price | Prev | Candle time (UTC) | Volume (15m) | Δ15m % |")
    lines.append("|------|----|------:|-----:|-------------------|-------------:|------:|")

    for a in obj["data"]:
        # 1m
        lines.append(f"| {a['pair']} | 1m | {a['tf']['1m']['price']:,} | {a['tf']['1m']['prev']:,} | {a['tf']['1m']['candle_time']} |  |  |".replace(",", ""))
        # 15m
        vol = a["tf"]["15m"]["volume"]
        chg = a["tf"]["15m"]["change_pct"]
        lines.append(f"| {a['pair']} | 15m | {a['tf']['15m']['price']:,} | {a['tf']['15m']['prev']:,} | {a['tf']['15m']['candle_time']} | {vol:,} | {chg:+.3f}% |".replace(",", ""))

    lines.append("")
    lines.append("_Bron: MEXC perpetual klines (1m & 15m). Tijden in UTC._")

    with open(SNAPSHOT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
def main():
    snap = build_snapshot()
    write_summary_json(snap)
    write_snapshot_md(snap)
    # log voor GitHub Actions
    print("Snapshot written:", SUMMARY_JSON, "and", SNAPSHOT_MD)

if __name__ == "__main__":
    main()
