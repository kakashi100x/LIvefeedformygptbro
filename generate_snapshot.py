# generate_snapshot.py — maak een leesbare Markdown snapshot uit data/latest.json
import json, os
from datetime import datetime, timezone

JSON_PATH = "data/latest.json"
OUT_PATH = "SNAPSHOT.md"

def fmt_price(x):
    # simpele formatter (meer decimalen voor SOL/ETH als nodig)
    if x >= 1000: return f"{x:,.0f}"
    if x >= 100:  return f"{x:,.2f}"
    if x >= 10:   return f"{x:,.2f}"
    return f"{x:,.4f}"

def load_latest():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def last_candle(series):
    # verwacht lijst [{ts, time, open, high, low, close, volume}, ...] oud->nieuw
    return series[-1] if series else None

def prev_close(series):
    return series[-2]["close"] if series and len(series) >= 2 else None

def build_md(d):
    updated_at = d.get("updated_at", "")
    pairs = d["pairs"]  # {"BTCUSDT":{"1m":[...], "15m":[...]}, ...}
    lines = []
    lines.append(f"# Live Perp Snapshot (OKX)  \n*Auto-updated · {updated_at}*")
    lines.append("")
    lines.append("| Pair | TF | Price | Δ vs prev | Candle time (UTC) | Volume |")
    lines.append("|---|---|---:|---:|---|---:|")

    order = ["BTCUSDT","ETHUSDT","SOLUSDT"]
    for sym in order:
        tf_map = pairs.get(sym, {})
        for tf in ["1m","15m"]:
            arr = tf_map.get(tf, [])
            lc = last_candle(arr)
            if not lc:
                lines.append(f"| {sym} | {tf} | — | — | — | — |")
                continue
            pc = prev_close(arr)
            price = lc["close"]
            delta = (price - pc) if pc is not None else None
            ts_iso = lc.get("time","")
            vol = lc.get("volume",0.0)
            dstr = f"{delta:+.4f}" if delta is not None else "—"
            lines.append(f"| **{sym}** | {tf} | **{fmt_price(price)}** | {dstr} | {ts_iso} | {vol:.2f} |")

    lines.append("\n> Bron: OKX perpetual swaps · Interval: 1m & 15m · Tijden in UTC")
    return "\n".join(lines)

def main():
    data = load_latest()
    md = build_md(data)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"Wrote {OUT_PATH}")

if __name__ == "__main__":
    main()
