from config_loader import load_api_key
API_KEY = load_api_key()
import pandas as pd, requests, time
from tqdm import tqdm

chains = {
    "ethereum": "https://api.etherscan.io/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "optimism": "https://api-optimistic.etherscan.io/api",
    "bsc": "https://api.bscscan.com/api",
    "polygon": "https://api.polygonscan.com/api",
    "base": "https://api.basescan.org/api"
}

API_KEYS = ["YOUR_ETHERSCAN_KEY", "YOUR_BSCSCAN_KEY"]  # you can reuse same key for all

df = pd.read_csv("data_raw/contracts/defillama_top_protocols.csv")
rows = []

for _, row in tqdm(df.iterrows(), total=len(df)):
    name = row["name"]
    for chain, base_url in chains.items():
        try:
            # Example address field – update if you have contract_address column
            address = row.get("contract_address", "")
            if not isinstance(address, str) or not address:
                continue
            url = f"{base_url}?module=contract&action=getsourcecode&address={address}&apikey={API_KEYS[0]}"
            r = requests.get(url).json()
            if r.get("result") and "SourceCode" in r["result"][0] and r["result"][0]["SourceCode"]:
                rows.append({
                    "protocol_name": name,
                    "chain": chain,
                    "contract_address": address,
                    "source_code": r["result"][0]["SourceCode"]
                })
                break
            time.sleep(0.2)
        except Exception as e:
            continue

out = pd.DataFrame(rows)
out.to_csv("data_raw/contracts/verified_contracts_multichain.csv", index=False)
print(f"✅ Saved verified contracts → data_raw/contracts/verified_contracts_multichain.csv ({len(out)} rows)")