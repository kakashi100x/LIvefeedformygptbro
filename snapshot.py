# snapshot.py  (MEXC Perp – debug ready)
# Run: python snapshot.py
import os, json, time, math, traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
import urllib.request
import urllib.error

# ---------- Settings ----------
SYMBOLS = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]          # MEXC contract symbols
GRANS   = [("Min1", 60), ("Min15", 96)]                 # (interval, limit)
TIMEOUT = 15

# Probeer meerdere endpoint-varianten; MEXC geeft soms verschillend terug
KLINE_URLS = [
    "https://contract.mexc.com/api/v1/contract/kline/{symbol}?interval={gran}&limit={limit}",
    "https://contract.mexc.com/api/v1/contract/kline/{symbol}?interval={gran}&limit={limit}&page_size={limit}",
    "https://contract-api.mexc.com/api/v1/contract/kline/{symbol}?interval={gran}&limit={limit}",
    # query-style variant
    "https://contract.mexc.com/api/v1/contract/kline?symbol={symbol}&interval={gran}&limit={limit}",
]

OUT_JSON = "data/summary.json"
OUT_MD   = "SNAPSHOT.md"

# ---------- Utils ----------
def now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def http_get(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "snapshot/1.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        raw = resp.read()
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        # kan ook list zijn als pure JSON array
        return json.loads(raw)

def as_float(x: Any) -> float:
    if isinstance(x, (int, float)): return float(x)
    if isinstance(x, str): return float(x.strip())
    raise ValueError(f"Cannot convert to float: {type(x)} {x}")

def as_int(x: Any) -> int:
    if isinstance(x, (int, float)): return int(x)
    if isinstance(x, str): return int(float(x.strip()))
    raise ValueError(f"Cannot convert to int: {type(x)} {x}")

# ---------- Kline normalisatie ----------
def extract_kline_list(payload: Any) -> List[Any]:
    """
    Probeer verschillende MEXC vormen te vinden en retourneer een list met rows.
    - direct list (list[list] of list[dict])
    - {"success": true, "code": 0, "data": [...]}
    - {"data": {"time": [...], "open": [...], ...}}  (kolom-georiënteerd)
    """
    # debug: laat kop zien
    print("DEBUG payload head:", str(payload)[:500])

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        # veel voorkomende vorm
        if "data" in payload:
            data = payload["data"]
            if isinstance(data, list):
                return data
            # kolom-georiënteerd: arrays per key
            if isinstance(data, dict):
                keys = data.keys()
                if all(k in data for k in ("time","open","high","low","close","volume")):
                    rows = []
                    n = min(len(data["time"]), len(data["open"]), len(data["high"]),
                            len(data["low"]), len(data["close"]), len(data["volume"]))
                    for i in range(n):
                        rows.append([
                            data["time"][i], data["open"][i], data["high"][i],
                            data["low"][i], data["close"][i], data["volume"][i],
                        ])
                    return rows
        # sommige responses hebben 'success','code','data'
        if "success" in payload and "data" in payload:
            d = payload["data"]
            if isinstance(d, list):
                return d

    raise ValueError("Unexpected kline payload structure")

def row_to_ohlcv(row: Any) -> Tuple[int,float,float,float,float,float]:
    """
    Ondersteun meerdere rijen-vormen.
    """
    if isinstance(row, list) and len(row) >= 6:
        ts, o, h, l, c, v = row[:6]
    elif isinstance(row, dict):
        # t/o/h/l/c/v of time/open/high/low/close/volume
        tkey = "t" if "t" in row else "time"
        okey = "o" if "o" in row else "open"
        hkey = "h" if "h" in row else "high"
        lkey = "l" if "l" in row else "low"
        ckey = "c" if "c" in row else "close"
        vkey = "v" if "v" in row else "volume"
        ts, o, h, l, c, v = row.get(tkey), row.get(okey), row.get(hkey), row.get(lkey), row.get(ckey), row.get(vkey)
    else:
        raise ValueError("Unexpected kline row structure")

    # timestamp kan ms of s zijn
    ts = as_int(ts)
    if ts > 10_000_000_000:  # ms
        ts //= 1000

    return ts, as_float(o), as_float(h), as_float(l), as_float(c), as_float(v)

def normalize_klines(payload: Any) -> List[Dict[str, Any]]:
    raw_rows = extract_kline_list(payload)
    norm = []
    for r in raw_rows:
        try:
            ts,o,h,l,c,v = row_to_ohlcv(r)
            norm.append({"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except Exception as e:
            # sla rommel over, maar laat iets zien
            print("WARN skip row:", r if isinstance(r, list) else str(r)[:240], "=>", e)
            continue
    if not norm:
        raise ValueError("No valid klines after normalization")
    # sorteer op tijd (oud -> nieuw)
    norm.sort(key=lambda x: x["time"])
    return norm

# ---------- Fetcher met retries ----------
def fetch_klines_for_symbol(symbol: str, gran: str, limit: int) -> Tuple[List[Dict[str,Any]], str]:
    last_err = None
    for url_tpl in KLINE_URLS:
        url = url_tpl.format(symbol=symbol, gran=gran, limit=limit)
        try:
            payload = http_get(url)
            print(f"DEBUG fetched from: {url}")
            print("DEBUG raw preview:", str(payload)[:500])
            rows = normalize_klines(payload)
            # sanity: minstens 2 candles
            if len(rows) < 2:
                raise ValueError("Too few klines")
            return rows[-limit:], url
        except Exception as e:
            last_err = f"{url} => {e}"
            print("DEBUG endpoint failed:", last_err)
            continue
    raise RuntimeError(f"All MEXC kline endpoints failed for {symbol}. Last: {last_err}")

# ---------- Snapshot builders ----------
def last_close(kl: List[Dict[str,Any]]) -> Dict[str,Any]:
    k = kl[-1]
    return {
        "time": k["time"],
        "close": k["close"],
        "open": k["open"],
        "high": k["high"],
        "low": k["low"],
        "volume": k["volume"],
    }

def block_for_symbol(symbol: str) -> Dict[str, Any]:
    asset = {"symbol": symbol, "klines": {}}
    sources_used = {}
    for gran, lim in GRANS:
        kl, src = fetch_klines_for_symbol(symbol, gran, lim)
        asset["klines"][gran] = {
            "limit": lim,
            "rows": kl,
            "last": last_close(kl),
            "source": src,
        }
        sources_used[gran] = src
    # price for summary
    price = asset["klines"]["Min1"]["last"]["close"]
    asset["price"] = price
    return asset

def build_snapshot() -> Dict[str, Any]:
    assets = []
    for sym in SYMBOLS:
        assets.append(block_for_symbol(sym))
    snap = {
        "exchange": "MEXC",
        "market": "USDT-Perpetual",
        "updated_at": now_iso_utc(),
        "assets": assets
    }
    return snap

# ---------- Output ----------
def write_json(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",",":"), indent=2)

def write_md(path: str, snap: Dict[str,Any]):
    lines = []
    lines.append(f"# Live Perp Snapshot (MEXC)\n")
    lines.append(f"_Auto-updated: {snap['updated_at']}_\n")
    lines.append("| Pair | TF | Price | Time (UTC) | Vol |")
    lines.append("|---|---:|---:|---:|---:|")
    for a in snap["assets"]:
        pair = a["symbol"]
        for tf in ["Min1","Min15"]:
            last = a["klines"][tf]["last"]
            t = datetime.utcfromtimestamp(last["time"]).strftime("%Y-%m-%d %H:%M:%S")
            lines.append(f"| {pair} | {tf} | {last['close']:.6f} | {t} | {last['volume']:.2f} |")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

# ---------- Main ----------
def main():
    try:
        snap = build_snapshot()
        write_json(OUT_JSON, snap)
        write_md(OUT_MD, snap)
        print("OK wrote:", OUT_JSON, "and", OUT_MD)
    except Exception as e:
        print("FATAL:", e)
        traceback.print_exc()
        # zorg dat Actions faalt (zodat je het ziet)
        raise

if __name__ == "__main__":
    main()
