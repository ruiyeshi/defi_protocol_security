# fetch_contracts_full_v4.py
import os
import json
import pandas as pd
import requests
from time import sleep

#  Extended mapping of Etherscan-style APIs for EVM-compatible chains
ETHERSCAN_ENDPOINTS = {
    "ethereum": "https://api.etherscan.io/api",
    "bsc": "https://api.bscscan.com/api",
    "polygon": "https://api.polygonscan.com/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "optimism": "https://api-optimistic.etherscan.io/api",
    "avalanche": "https://api.snowtrace.io/api",
    "base": "https://api.basescan.org/api",
    "fantom": "https://api.ftmscan.com/api",
    "gnosis": "https://api.gnosisscan.io/api",
    "rsk": "https://api.rsk.co/api"
}

#  Optional: Load API keys from environment variables
API_KEYS = {
    "ethereum": os.getenv("ETHERSCAN_API_KEY", ""),
    "bsc": os.getenv("BSCSCAN_API_KEY", ""),
    "polygon": os.getenv("POLYGONSCAN_API_KEY", ""),
    "arbitrum": os.getenv("ARBISCAN_API_KEY", ""),
    "optimism": os.getenv("OPTIMISM_API_KEY", ""),
    "avalanche": os.getenv("AVALANCHE_API_KEY", ""),
    "base": os.getenv("BASESCAN_API_KEY", ""),
    "fantom": os.getenv("FTMSCAN_API_KEY", ""),
    "gnosis": os.getenv("GNOSIS_API_KEY", ""),
    "rsk": os.getenv("RSKSCAN_API_KEY", "")
}

# 📦 Load your list of DeFi protocols
df = pd.read_csv("data_raw/contracts/verified_contracts.csv")

results = []
print("🌐 Fetching verified Solidity contract sources (multi-chain mode)...")

for _, row in df.iterrows():
    protocol = str(row["protocol_name"])
    chains_raw = str(row["chain"])
    addr = str(row.get("contract_address", "")).strip()

    # Handle multi-chain list safely
    try:
        chain_list = json.loads(chains_raw.replace("'", '"')) if "[" in chains_raw else [chains_raw]
    except Exception:
        chain_list = [chains_raw]

    found = False

    for chain in chain_list:
        chain = chain.strip().lower()
        if not chain:
            continue

        if chain not in ETHERSCAN_ENDPOINTS:
            print(f"⚠️ Skipping {protocol} (unsupported or non-EVM chain: {chain})")
            continue

        url = ETHERSCAN_ENDPOINTS[chain]
        key = API_KEYS.get(chain, "")

        print(f"🔍 Fetching verified source for {protocol} on {chain.title()}...")
        params = {
            "module": "contract",
            "action": "getsourcecode",
            "address": addr,
            "apikey": key
        }

        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()

            if data.get("status") == "1" and data["result"]:
                source = data["result"][0].get("SourceCode", "")
                if source:
                    folder = f"data_raw/contracts/code/{protocol.lower().replace(' ', '_')}/"
                    os.makedirs(folder, exist_ok=True)
                    filepath = os.path.join(folder, f"{chain}.sol")
                    with open(filepath, "w", encoding="utf-8") as f:
                        f.write(source)
                    print(f"✅ Saved source: {filepath}")
                    results.append({
                        "protocol_name": protocol,
                        "chain": chain,
                        "contract_address": addr,
                        "source_path": filepath
                    })
                    found = True
                    break
                else:
                    print(f"⚠️ No verified source found for {protocol} on {chain.title()}.")
            else:
                print(f"❌ No verified source (API status {data.get('status')}) for {protocol} on {chain.title()}.")
        except Exception as e:
            print(f"💥 Error fetching {protocol} ({chain}): {e}")
        sleep(1.5)

    if not found:
        print(f"🚫 No verified contract found for {protocol} on any chain.")

# 💾 Save the registry of successful downloads
if results:
    df_out = pd.DataFrame(results)
    out_path = "data_raw/contracts/contract_registry_v4.csv"
    df_out.to_csv(out_path, index=False)
    print(f"\n💾 Saved registry → {out_path} ({len(df_out)} entries)")
else:
    print("\n⚠️ No verified contracts were fetched.")
