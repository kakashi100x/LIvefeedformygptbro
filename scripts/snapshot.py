#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Snapshot fetcher for MEXC USDT-M perpetuals (BTC/ETH/SOL).
- Reads public ticker from MEXC contract API
- Is tolerant to field/key variations (lastPrice / last / price, volume24 / turnoverOf24h, etc.)
- Writes data/summary.json (machine readable)
- Writes SNAPSHOT.md (human readable)
"""

from __future__ import annotations
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any, List
import requests

# -------- Settings --------
API_BASE = "https://contract.mexc.com"
TIMEOUT = 10
PAIRS = [
    ("BTCUSDT", "BTC_USDT"),
    ("ETHUSDT", "ETH_USDT"),
    ("SOLUSDT", "SOL_USDT"),
]

SUMMARY_PATH = os.path.join("data", "summary.json")
SNAPSHOT_MD = "SNAPSHOT.md"


def _get_first(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _to_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def fetch_ticker(symbol_contract: str) -> Dict[str, Any]:
    """
    Fetch ticker for a single contract symbol like 'BTC_USDT'.
    Tries the documented endpoint and normalizes fields.
    """
    url = f"{API_BASE}/api/v1/contract/ticker"
    r = requests.get(url, params={"symbol": symbol_contract}, timeout=TIMEOUT)
    r.raise_for_status()
    payload = r.json()

    # MEXC usually returns: {"success":true,"code":0,"data":[{...}]}
    data_obj = payload.get("data")
    if isinstance(data_obj, list):
        data = data_obj[0] if data_obj else {}
    elif isinstance(data_obj, dict):
        data = data_obj
    else:
        data = {}

    # Normalize fields with fallbacks
    last = _to_float(_get_first(data, ["lastPrice", "last_price", "last", "price"]))
    bid1 = _to_float(_get_first(data, ["bid1", "bestBid", "bidPrice"]))
    ask1 = _to_float(_get_first(data, ["ask1", "bestAsk", "askPrice"]))

    # Volume keys vary across docs/responses
    vol24 = _to_float(
        _get_first(
            data,
            [
                "volume24",        # common in MEXC futures
                "vol24",
                "turnover24h",
                "turnoverOf24h",   # <-- previous key that caused KeyError
                "quoteVolume24h",
            ],
        )
    )

    # Timestamp
    ts = _get_first(data, ["timestamp", "ts", "time"])
    try:
        ts = int(ts)
    except Exception:
        ts = None

    # Build normalized result
    return {
        "price": last,
        "bid": bid1,
        "ask": ask1,
        "volume24h": vol24,
        "raw": data,
        "source": f"{url}?symbol={symbol_contract}",
        "source_type": "MEXC_PERP",
        "timestamp_ms": ts,
    }


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def cet_from_utc_iso(iso_utc: str) -> str:
    # CET/CEST human label only; computation stays UTC to avoid tz db
    dt = datetime.strptime(iso_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    # Display label; we don’t convert here because GitHub runner may lack tzdata.
    # You can convert in the consumer if needed.
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def write_summary(summary: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(SUMMARY_PATH), exist_ok=True)
    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)


def write_snapshot_md(summary: Dict[str, Any]) -> None:
    rows = []
    for sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
        r = summary["perps"].get(sym, {})
        rows.append(
            f"| {sym} | 1m | {r.get('price','-')} | {r.get('prev_price','-')} | {r.get('ts_candle','-')} | {r.get('volume24h','-')} |"
        )

    md = f"""# Live Perp Snapshot (MEXC)

Auto-updated: {summary['updated_at']} (UTC)

| Pair    | TF | Price | v.s. prev | Candle time (UTC) | Volume (24h) |
|---------|----|-------|-----------|-------------------|--------------|
{os.linesep.join(rows)}

Bron: MEXC USDT-M perpetuals • Interval: 1m & 10m • Tijden in UTC
"""
    with open(SNAPSHOT_MD, "w", encoding="utf-8") as f:
        f.write(md)


def main():
    results: Dict[str, Any] = {}
    for sym_display, sym_contract in PAIRS:
        try:
            res = fetch_ticker(sym_contract)
        except Exception as e:
            res = {"error": str(e), "source_type": "MEXC_PERP"}
        results[sym_display] = res

    now_utc = utc_now_iso()

    # You can plug in prev values by reading old file; keep simple for now.
    summary = {
        "exchange": "mexc",
        "market": "perp",
        "updated_at": now_utc,  # UTC ISO
        "updated_at_cet_label": cet_from_utc_iso(now_utc),
        "perps": {
            k: {
                "price": v.get("price"),
                "bid": v.get("bid"),
                "ask": v.get("ask"),
                "volume24h": v.get("volume24h"),
                "source": v.get("source"),
                "timestamp_ms": v.get("timestamp_ms"),
                "error": v.get("error"),
            }
            for k, v in results.items()
        },
    }

    write_summary(summary)
    write_snapshot_md(summary)
    print("Snapshot written:", SUMMARY_PATH, "and", SNAPSHOT_MD)


if __name__ == "__main__":
    main()
