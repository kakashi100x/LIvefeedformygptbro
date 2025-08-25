#!/usr/bin/env python3
import json, os, time, datetime, ccxt

# ----- settings -----
SYMBOLS = {
    "BTC": "BTC/USDT:USDT",  # MEXC USDT-M perpetual
    "ETH": "ETH/USDT:USDT",
    "SOL": "SOL/USDT:USDT",
}
OUT_JSON = "data/summary.json"
OUT_MD   = "SNAPSHOT.md"

def utc_now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def main():
    ex = ccxt.mexc({
        "options": {"defaultType": "swap"},  # use perpetual swaps
        "enableRateLimit": True,
    })
    ex.load_markets()

    perps = {}
    notes = []
    for key, sym in SYMBOLS.items():
        try:
            ticker = ex.fetch_ticker(sym)
            last = ticker.get("last") or ticker.get("info", {}).get("lastPrice")
            perps[key] = {"mexc": float(last)}
        except Exception as e:
            notes.append(f"{key} fetch error: {type(e).__name__}: {e}")
    
    payload = {
        "exchange": "MEXC",
        "market_type": "USDT_PERP",
        "updated_at": utc_now_iso(),  # UTC
        "perps": perps,               # {"BTC":{"mexc":12345.6}, ...}
        "notes": notes,
    }

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(payload, f, indent=2)

    # simple human snapshot
    def fmt(v): 
        return "—" if v is None else f"{v:,.4f}".replace(",", "X").replace(".", ",").replace("X", ".")
    lines = []
    lines.append(f"# Snapshot — MEXC USDT-Perps\n")
    lines.append(f"**Updated (UTC):** {payload['updated_at']}\n")
    for k in ["BTC","ETH","SOL"]:
        v = payload["perps"].get(k, {}).get("mexc")
        lines.append(f"- {k}: {fmt(v)}")
    if notes:
        lines.append("\nNotes:")
        for n in notes:
            lines.append(f"- {n}")
    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

if __name__ == "__main__":
    main()
