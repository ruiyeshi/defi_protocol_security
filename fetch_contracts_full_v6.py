import pandas as pd, requests, json, os, time

# ================================================
#  Fetch verified Solidity source code (multi-chain)
# ================================================
print("🌍 Fetching verified Solidity source code using resolved addresses...")

# --- Load your address file
addr_df = pd.read_csv("data_raw/contracts/resolved_addresses.csv")

# --- Detect columns dynamically
if "chains" in addr_df.columns and "addresses" in addr_df.columns:
    print("✅ Detected chain column: 'chains'")
else:
    raise ValueError("No valid chain/address columns found in resolved_addresses.csv")

# --- Create output directory
os.makedirs("data_raw/contracts/", exist_ok=True)
out_file = "data_raw/contracts/fetched_contract_sources.csv"

results = []

# --- API keys for multiple explorers
EXPLORERS = {
    "ethereum": "https://api.etherscan.io/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "polygon": "https://api.polygonscan.com/api",
    "bsc": "https://api.bscscan.com/api",
    "base": "https://api.basescan.org/api",
    "optimism": "https://api-optimistic.etherscan.io/api",
    "avalanche": "https://api.snowtrace.io/api",
}

# --- Replace with your own API key(s)
API_KEY = os.getenv("ETHERSCAN_API_KEY", "YourEtherscanKeyHere")

# --- Iterate over all protocols
for _, row in addr_df.iterrows():
    protocol = row["protocol_name"]

    try:
        # Parse JSON-like strings safely
        chains = json.loads(row["chains"].replace("'", '"'))
        addresses = json.loads(row["addresses"].replace("'", '"'))
    except Exception as e:
        print(f"⚠️ Could not parse row for {protocol}: {e}")
        continue

    if not isinstance(addresses, list) or len(addresses) == 0:
        print(f"⚠️ Skipping {protocol}: no valid addresses.")
        continue

    found_any = False

    for entry in addresses:
        try:
            chain_hint, addr = entry.split(":", 1)
        except:
            continue

        chain_hint = chain_hint.lower()
        addr = addr.strip()

        if chain_hint not in EXPLORERS:
            print(f"⚠️ Skipping {protocol} ({chain_hint}): unsupported chain.")
            continue

        url = f"{EXPLORERS[chain_hint]}?module=contract&action=getsourcecode&address={addr}&apikey={API_KEY}"
        try:
            resp = requests.get(url, timeout=20)
            data = resp.json()
            if "result" in data and data["result"]:
                code = data["result"][0].get("SourceCode", "")
                if code:
                    print(f"✅ {protocol} ({chain_hint}) source found — {len(code)} chars")
                    results.append({
                        "protocol_name": protocol,
                        "chain": chain_hint,
                        "address": addr,
                        "source_len": len(code)
                    })
                    found_any = True
                    break
            else:
                print(f"❌ {protocol} ({chain_hint}): no verified source.")
        except Exception as e:
            print(f"❌ {protocol} ({chain_hint}): request error: {e}")
            continue

    if not found_any:
        print(f"⚠️ No verified contract found for {protocol} on any chain.")

    time.sleep(0.3)

# --- Save output
pd.DataFrame(results).to_csv(out_file, index=False)
print(f"💾 Saved {len(results)} verified contracts → {out_file}")
