import os
import pandas as pd
import requests
import json

# ========== CONFIG ==========
API_KEY = os.getenv("ETHERSCAN_API_KEY")  # from .env
INPUT_FILE = "data_raw/contracts/verified_contracts.csv"
OUTPUT_FILE = "data_raw/contracts/verified_sources.csv"

print("🌐 Fetching verified Solidity source code for DeFi protocols...")

# Load dataset
df = pd.read_csv(INPUT_FILE)
results = []

for _, row in df.iterrows():
    protocol = row["protocol_name"]
    chain_raw = str(row.get("chain", "")).strip()

    # Handle malformed chain entries
    if not chain_raw or chain_raw.lower() in ["nan", "none"]:
        print(f"⚠️ Skipping {protocol}: missing chain info")
        continue

    # Normalize chain list safely
    try:
        if chain_raw.startswith("["):
            chain_list = json.loads(chain_raw.replace("'", '"'))
        else:
            chain_list = [chain_raw]
    except Exception:
        chain_list = [chain_raw]

    for chain in chain_list:
        chain = chain.strip()
        print(f"🔍 Fetching {protocol} on {chain}...")

        # For now, only pull Ethereum contracts
        if chain.lower() not in ["ethereum", "eth", "mainnet"]:
            print(f"⏭️ Skipping {protocol}: non-Ethereum chain ({chain})")
            continue

        url = f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={row.get('contract_address','')}&apikey={API_KEY}"

        try:
            r = requests.get(url, timeout=10)
            data = r.json()
            if data.get("status") == "1" and data.get("result"):
                contract_data = data["result"][0]
                results.append({
                    "protocol_name": protocol,
                    "chain": chain,
                    "compiler": contract_data.get("CompilerVersion"),
                    "source_code": contract_data.get("SourceCode"),
                    "contract_name": contract_data.get("ContractName"),
                })
                print(f"✅ Pulled source for {protocol}")
            else:
                print(f"⚠️ No verified source for {protocol}")
        except Exception as e:
            print(f"❌ Error fetching {protocol}: {e}")

# Save results
if results:
    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Saved verified sources → {OUTPUT_FILE} ({len(results)} entries)")
else:
    print("\n⚠️ No verified contracts found or fetched.")
