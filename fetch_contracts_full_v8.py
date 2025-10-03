import pandas as pd
import requests
import json
import time

print("🌐 Fetching verified Solidity sources across major explorers (v8)...")

# Load dataset
df = pd.read_csv("data_raw/contracts/resolved_addresses.csv")

# Normalize potential naming issues
if "chain" not in df.columns and "chains" in df.columns:
    df = df.rename(columns={"chains": "chain"})

def safe_json_parse(value):
    """Parse strings that look like lists (['...']) or JSON arrays."""
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return []
    try:
        value = value.replace("'", '"')
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    except:
        return [value]

# Apply robust parsing
df["chain"] = df["chain"].apply(safe_json_parse)
df["addresses"] = df["addresses"].apply(safe_json_parse)

# Supported explorers (EVM)
EXPLORERS = {
    "ethereum": "https://api.etherscan.io/api",
    "bsc": "https://api.bscscan.com/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "polygon": "https://api.polygonscan.com/api",
    "optimism": "https://api-optimistic.etherscan.io/api",
    "base": "https://api.basescan.org/api",
    "avalanche": "https://api.snowtrace.io/api",
}

API_KEY = "YourApiKeyToken"

# Fetch logic
records = []

for _, row in df.iterrows():
    name = row.get("protocol_name", "Unknown")
    chains = row.get("chain", [])
    addrs = row.get("addresses", [])

    if not chains or not addrs:
        print(f"⚠️ Skipping {name}: no valid chain/address data.")
        continue

    # Align lists
    for idx in range(min(len(chains), len(addrs))):
        chain = str(chains[idx]).lower()
        addr = str(addrs[idx])

        if chain not in EXPLORERS:
            print(f"⚠️ Skipping {name} ({chain}): unsupported chain.")
            continue

        url = f"{EXPLORERS[chain]}?module=contract&action=getsourcecode&address={addr}&apikey={API_KEY}"
        print(f"🔍 Fetching verified contract for {name} on {chain} ({addr[:10]}...)")

        try:
            r = requests.get(url, timeout=15)
            data = r.json()
            result = data.get("result", [])
            if isinstance(result, list) and len(result) > 0:
                code = result[0].get("SourceCode", "")
                comp = result[0].get("CompilerVersion", "")
                records.append({
                    "protocol_name": name,
                    "chain": chain,
                    "address": addr,
                    "compiler": comp,
                    "source_code": code
                })
                print(f"✅ Verified contract found for {name} ({chain})")
            else:
                print(f"❌ No verified contract for {name} ({chain})")

        except Exception as e:
            print(f"💥 Error fetching {name} ({chain}): {e}")

        time.sleep(1.2)

# Save
out_df = pd.DataFrame(records)
out_path = "data_raw/contracts/fetched_contract_sources.csv"
out_df.to_csv(out_path, index=False)
print(f"💾 Saved {len(out_df)} verified contracts → {out_path}")
