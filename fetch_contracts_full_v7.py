import pandas as pd
import requests
import json
import time

# -----------------------------
# Explorer API endpoints
# -----------------------------
EXPLORERS = {
    "ethereum": "https://api.etherscan.io/api",
    "bsc": "https://api.bscscan.com/api",
    "polygon": "https://api.polygonscan.com/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "optimism": "https://api-optimistic.etherscan.io/api",
    "avalanche": "https://api.snowtrace.io/api",
    "base": "https://api.basescan.org/api"
}

# Use your own Etherscan API key
API_KEY = "YourAPIKeyHere"

# -----------------------------
# Load address registry
# -----------------------------
df = pd.read_csv("data_raw/contracts/resolved_addresses.csv")

# Normalize chain column
if "chain" not in df.columns:
    if "chains" in df.columns:
        df = df.rename(columns={"chains": "chain"})
print(f"✅ Detected chain column: '{[c for c in df.columns if 'chain' in c][0]}'")

results = []

# -----------------------------
# Iterate through protocols
# -----------------------------
for _, row in df.iterrows():
    name = row["protocol_name"]
    chains = row["chain"]

    try:
        chains = json.loads(chains.replace("'", '"')) if isinstance(chains, str) else []
    except:
        chains = [chains] if isinstance(chains, str) else []

    addresses = []
    try:
        addresses = json.loads(row["addresses"].replace("'", '"'))
    except Exception as e:
        print(f"⚠️ Skipping {name}: address parse error → {e}")
        continue

    print(f"\n🔍 Fetching verified contracts for {name}...")

    for chain, addr_list in zip(chains, addresses):
        chain = chain.lower()
        if chain not in EXPLORERS:
            print(f"⏩ Skipping {name} ({chain}): unsupported chain.")
            continue

        api = EXPLORERS[chain]
        for addr in addr_list if isinstance(addr_list, list) else [addr_list]:
            params = {
                "module": "contract",
                "action": "getsourcecode",
                "address": addr,
                "apikey": API_KEY
            }
            try:
                r = requests.get(api, params=params, timeout=20)
                data = r.json()
                if "result" in data and len(data["result"]) > 0:
                    src = data["result"][0].get("SourceCode", "")
                    if src:
                        results.append({
                            "protocol": name,
                            "chain": chain,
                            "address": addr,
                            "source_code": src
                        })
                        print(f"✅ Saved {name} contract from {chain}.")
                    else:
                        print(f"⚠️ {name} ({chain}): verified but empty source.")
                else:
                    print(f"❌ No verified source for {name} ({chain}).")
            except Exception as e:
                print(f"❌ Error fetching {name} ({chain}): {e}")

            time.sleep(0.25)  # Rate limit buffer

# -----------------------------
# Save results
# -----------------------------
out_path = "data_raw/contracts/fetched_contract_sources.csv"
pd.DataFrame(results).to_csv(out_path, index=False)
print(f"\n💾 Saved {len(results)} verified contracts → {out_path}")
