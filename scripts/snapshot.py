#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Snapshot script for MEXC USDT-M perpetuals (BTC/ETH/SOL).

- Fetches latest ticker for BTC_USDT, ETH_USDT, SOL_USDT from MEXC Contract API
- Writes data/summary.json and SNAPSHOT.md
- Always updates updated_at (UTC) so the commit has a diff each run
- Tolerant to API field-name variations (turnoverOf24h, turnover24h, volume24h, etc.)
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import requests

API_BASE = "https://contract.mexc.com/api/v1"
SYMBOLS = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]  # USDT-M perps
PAIR_MAP = {
    "BTC_USDT": "BTCUSDT",
    "ETH_USDT": "ETHUSDT",
    "SOL_USDT": "SOLUSDT",
}

# Simple retry config
TIMEOUT = 10
RETRIES = 3
SLEEP_BETWEEN = 0.8


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get(url: str, params: Optional[dict] = None) -> Any:
    last_exc: Optional[Exception] = None
    for _ in range(RETRIES):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            time.sleep(SLEEP_BETWEEN)
    raise RuntimeError(f"HTTP failed for {url}: {last_exc}")


def fetch_tickers(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Calls MEXC contract ticker endpoint per symbol and normalizes fields.
    Endpoint reference (public): /contract/ticker?symbol=BTC_USDT
    """
    out: Dict[str, Dict[str, Any]] = {}

    for sym in symbols:
        url = f"{API_BASE}/contract/ticker"
        data = _get(url, params={"symbol": sym})

        # MEXC often returns {"success":true,"code":0,"data":[{...}]}
        raw = None
        if isinstance(data, dict) and "data" in data:
            d = data["data"]
            if isinstance(d, list) and d:
                raw = d[0]
            elif isinstance(d, dict):
                raw = d
        if not raw:
            raise RuntimeError(f"No ticker data for {sym}: {data}")

        # Field extraction with fallbacks
        def pick_float(obj: dict, *keys: str, default: float = 0.0) -> float:
            for k in keys:
                if k in obj and obj[k] is not None:
                    try:
                        return float(obj[k])
                    except Exception:
                        pass
            return default

        # Common possible keys seen in MEXC responses
        last = pick_float(raw, "lastPrice", "last", "price")
        bid = pick_float(raw, "bid1", "bestBidPrice", "bid")
        ask = pick_float(raw, "ask1", "bestAskPrice", "ask")
        # 24h turnover/volume (quote or base) – take whatever is available
        vol_base = pick_float(raw, "volume", "volume24h", "vol24h", "baseVolume", "quantity", "amount24h")
        vol_quote = pick_float(raw, "turnoverOf24h", "turnover24h", "quoteVolume", "amount", "quoteAmount24h")
        # funding and index/mark (optional)
        funding = pick_float(raw, "fundingRate", "fundingRateLast", default=0.0)
        mark = pick_float(raw, "markPrice", "fairPrice", default=last)
        indexp = pick_float(raw, "indexPrice", default=last)

        out[sym] = {
            "symbol": sym,
            "pair": PAIR_MAP.get(sym, sym.replace("_", "")),
            "exchange": "mexc",
            "market": "perpetual",
            "price": last,
            "bid": bid,
            "ask": ask,
            "mark": mark,
            "index": indexp,
            "fundingRate": funding,
            "volume24h_base": vol_base,
            "volume24h_quote": vol_quote,
        }

    return out


def write_files(tickers: Dict[str, Dict[str, Any]]) -> None:
    Path("data").mkdir(exist_ok=True)

    updated_at = _now_utc_iso()

    # ---- JSON summary ----
    summary = {
        "exchange": "mexc",
        "market": "perpetual",
        "updated_at": updated_at,
        "pairs": {
            tickers[k]["pair"]: {
                "price": tickers[k]["price"],
                "bid": tickers[k]["bid"],
                "ask": tickers[k]["ask"],
                "mark": tickers[k]["mark"],
                "index": tickers[k]["index"],
                "fundingRate": tickers[k]["fundingRate"],
                "volume24h_base": tickers[k]["volume24h_base"],
                "volume24h_quote": tickers[k]["volume24h_quote"],
            }
            for k in tickers
        },
        "source": f"{API_BASE}/contract/ticker",
    }

    with open("data/summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # ---- Markdown snapshot ----
    def fmt_row(t: Dict[str, Any]) -> str:
        return (
            f"| {t['pair']} | 1m | "
            f"{t['price']:.2f} | {t['bid']:.2f}/{t['ask']:.2f} | "
            f"{t['fundingRate']:.6f} | {t['volume24h_quote']:.2f} |"
        )

    lines = []
    lines.append("# Live Perp Snapshot (MEXC)")
    lines.append("")
    lines.append(f"Auto-updated: **{updated_at}** (UTC)")
    lines.append("")
    lines.append("| Pair | TF | Price | Bid/Ask | Funding | 24h Turnover (quote) |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    # order BTC/ETH/SOL for readability
    for sym in ["BTC_USDT", "ETH_USDT", "SOL_USDT"]:
        lines.append(fmt_row(tickers[sym]))
    lines.append("")
    lines.append(f"Source: `{API_BASE}/contract/ticker` • Market: **MEXC USDT-M Perpetuals**")
    lines.append("")

    with open("SNAPSHOT.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main() -> None:
    try:
        tickers = fetch_tickers(SYMBOLS)
        write_files(tickers)
        print("Snapshot written: data/summary.json & SNAPSHOT.md")
    except Exception as e:
        # Fail the job with a clear message
        print(f"[snapshot] ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
