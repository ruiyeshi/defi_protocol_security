import requests
import pandas as pd
import time

# 🌍 Multi-chain API endpoints
EXPLORERS = {
    "ethereum": "https://api.etherscan.io/api",
    "bsc": "https://api.bscscan.com/api",
    "polygon": "https://api.polygonscan.com/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "optimism": "https://api-optimistic.etherscan.io/api",
    "base": "https://api.basescan.org/api",
    "avalanche": "https://api.snowtrace.io/api"
}

API_KEY = "YourAPIKeyHere"  # 🔑 You can reuse your Etherscan key for all explorers

# 📂 Input: verified_contracts.csv from Step 2C
df = pd.read_csv("data_raw/contracts/verified_contracts.csv")

results = []
print("🌐 Fetching verified Solidity source code from multi-chain explorers...")

for _, row in df.iterrows():
    protocol = row["protocol_name"]
    chain_hint = str(row["chain"]).lower()
    found = False

    for chain, api_url in EXPLORERS.items():
        if chain not in chain_hint and chain_hint not in chain:
            continue  # skip unrelated explorers

        print(f"🔎 Checking {protocol} on {chain.title()}...")
        try:
            # Dummy address list for testing (replace with actual resolver output later)
            # In production, you'd pull from resolved_addresses.csv
            address = None

            # Try fetching contract source
            url = f"{api_url}?module=contract&action=getsourcecode&address={address}&apikey={API_KEY}"
            resp = requests.get(url, timeout=15)
            data = resp.json()

            if data.get("status") == "1" and len(data["result"]) > 0:
                contract = data["result"][0]
                results.append({
                    "protocol_name": protocol,
                    "chain": chain,
                    "address": address,
                    "contract_name": contract.get("ContractName", ""),
                    "compiler": contract.get("CompilerVersion", ""),
                    "verified": True
                })
                print(f"✅ Verified contract found for {protocol} on {chain.title()}")
                found = True
                break
            else:
                print(f"⚠️ No verified source for {protocol} on {chain.title()}")

        except Exception as e:
            print(f"❌ Error fetching {protocol} on {chain.title()}: {e}")

        time.sleep(0.5)

    if not found:
        print(f"🚫 No verified contract found for {protocol} on any chain.")

# 💾 Save results
if results:
    df_out = pd.DataFrame(results)
    df_out.to_csv("data_raw/contracts/verified_contracts_multichain.csv", index=False)
    print(f"\n✅ Saved multi-chain verified contracts → data_raw/contracts/verified_contracts_multichain.csv ({len(df_out)} entries)")
else:
    print("\n⚠️ No verified contracts found on any chain.")
