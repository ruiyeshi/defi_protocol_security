# fetch_contracts_full_v2.py
import os, requests, pandas as pd, time, json
from urllib.parse import quote

os.makedirs("data_raw/contracts/source_code", exist_ok=True)
verified_csv = "data_raw/contracts/verified_contracts.csv"

print("🌍 Fetching verified Solidity source code for protocols...")

df = pd.read_csv(verified_csv)
chains = {
    "ethereum": "https://api.etherscan.io/api",
    "bsc": "https://api.bscscan.com/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "polygon": "https://api.polygonscan.com/api"
}

api_key = os.getenv("ETHERSCAN_API_KEY")
downloaded = []

for _, row in df.iterrows():
    name = row["protocol_name"].lower().replace(" ", "-")
    chain_list = json.loads(row["chain"].replace("'", '"')) if isinstance(row["chain"], str) else [row["chain"]]
    for chain in chain_list:
        chain_key = str(chain).lower()
        if chain_key not in chains:
            continue
        url = f"{chains[chain_key]}?module=contract&action=getsourcecode&address={row.get('address','')}&apikey={api_key}"
        try:
            r = requests.get(url, timeout=15)
            data = r.json()
            if "result" in data and data["result"]:
                src = data["result"][0].get("SourceCode")
                if src:
                    file_path = f"data_raw/contracts/source_code/{name}_{chain_key}.sol"
                    with open(file_path, "w") as f:
                        f.write(src)
                    downloaded.append((name, chain_key))
                    print(f"✅ Saved {name} ({chain_key})")
            time.sleep(0.2)
        except Exception as e:
            print(f"⚠️ {name} ({chain_key}) failed: {e}")

print(f"\n✨ Downloaded {len(downloaded)} verified source files → data_raw/contracts/source_code/")
pd.DataFrame(downloaded, columns=["protocol","chain"]).to_csv("data_raw/contracts/download_log.csv", index=False)
