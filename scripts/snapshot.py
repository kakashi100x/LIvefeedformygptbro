import requests
import json
import os
from datetime import datetime, timezone

# MEXC perpetual symbols
symbols = {
    "BTC": "BTC_USDT",
    "ETH": "ETH_USDT",
    "SOL": "SOL_USDT"
}

base_url = "https://contract.mexc.com/api/v1/contract/ticker"

results = {}

for coin, symbol in symbols.items():
    url = f"{base_url}?symbol={symbol}"
    r = requests.get(url)
    if r.status_code == 200:
        data = r.json()
        results[coin] = {
            "lastPrice": float(data['data']['lastPrice']),
            "bid1": float(data['data']['bid1']),
            "ask1": float(data['data']['ask1']),
            "volume24h": float(data['data']['turnoverOf24h'])
        }
    else:
        results[coin] = {"error": f"Failed to fetch {coin}"}

# Voeg timestamp toe
results["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

# Output map
os.makedirs("data", exist_ok=True)

# Schrijf JSON
with open("data/summary.json", "w") as f:
    json.dump(results, f, indent=2)

print("Snapshot updated:", results["updated_at"])
