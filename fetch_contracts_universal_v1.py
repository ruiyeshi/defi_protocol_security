from config_loader import load_api_key
API_KEY = load_api_key()
import requests
import pandas as pd
from tqdm import tqdm
import time

OUT_CSV = "data_raw/contracts/verified_contracts_universal.csv"

# Load top protocols (from DeFiLlama)
df = pd.read_csv("data_raw/contracts/defillama_top_protocols.csv")

rows = []
print("🌍 Fetching verified contracts from Etherscan and other explorers...")

for _, row in tqdm(df.iterrows(), total=len(df)):
    name = row["name"]
    chain = (row.get("chain", "") or "").lower()

    # Define EVM-compatible explorers
    explorers = {
        "ethereum": "https://api.etherscan.io/api",
        "arbitrum": "https://api.arbiscan.io/api",
        "optimism": "https://api-optimistic.etherscan.io/api",
        "polygon": "https://api.polygonscan.com/api",
        "bsc": "https://api.bscscan.com/api",
        "avalanche": "https://api.snowtrace.io/api",
        "fantom": "https://api.ftmscan.com/api",
        "base": "https://api.basescan.org/api"
    }

    if chain not in explorers:
        continue

    # Query the explorer
    url = explorers[chain]
    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": row.get("address", ""),
        "apikey": "YourEtherscanAPIkey"
    }

    try:
        res = requests.get(url, params=params, timeout=10)
        data = res.json().get("result", [])
        if data and isinstance(data, list) and data[0].get("SourceCode"):
            rows.append({
                "protocol_name": name,
                "chain": chain,
                "contract_address": row.get("address", ""),
                "source_code": data[0]["SourceCode"],
                "compiler_version": data[0].get("CompilerVersion", ""),
                "contract_name": data[0].get("ContractName", "")
            })
        time.sleep(0.25)
    except Exception as e:
        print(f"❌ {name}: {e}")

# Save results
pd.DataFrame(rows).to_csv(OUT_CSV, index=False)
print(f"✅ Saved verified contracts → {OUT_CSV} ({len(rows)} rows)")
