import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

# Load your API key
load_dotenv()
API_KEY = os.getenv("ETHERSCAN_API_KEY")

# Base endpoints for Etherscan-family explorers
API_DOMAINS = {
    "ethereum": "https://api.etherscan.io/api",
    "bsc": "https://api.bscscan.com/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "optimism": "https://api-optimistic.etherscan.io/api",
    "polygon": "https://api.polygonscan.com/api",
    "avalanche": "https://api.snowtrace.io/api"
}

# Load your master CSV
contracts = pd.read_csv("data_raw/contracts/master_contracts.csv")

# Prepare output folders
os.makedirs("data_raw/contracts", exist_ok=True)
master_outfile = "data_raw/contracts/verified_contracts.csv"

# Store results
records = []

# Iterate through all listed contracts
for i, row in contracts.iterrows():
    category = row["category"]
    name = row["protocol_name"]
    chain = row["chain"]
    address = row["contract_address"]

    api = API_DOMAINS.get(chain.lower())
    if not api:
        print(f"⚠️ Skipping {name}: unsupported chain {chain}")
        continue

    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": API_KEY
    }

    print(f"🔍 Fetching {name} ({chain}) ...")
    r = requests.get(api, params=params)
    data = r.json().get("result", [{}])[0]

    # Handle invalid responses
    if not isinstance(data, dict) or "SourceCode" not in data:
        print(f"⚠️ Failed for {name} ({chain})")
        continue

    # Save source file
    source_code = data.get("SourceCode", "")
    verified = bool(source_code.strip())
    compiler = data.get("CompilerVersion", "")
    source_path = f"data_raw/contracts/{category}/{name}_{chain}.sol"

    os.makedirs(os.path.dirname(source_path), exist_ok=True)
    with open(source_path, "w", encoding="utf-8") as f:
        f.write(source_code)

    # Record metadata
    records.append({
        "category": category,
        "protocol_name": name,
        "chain": chain,
        "contract_address": address,
        "compiler_version": compiler,
        "verified": verified,
        "source_file": source_path,
        "proxy_pattern": "EIP-1967" if "implementation" in source_code.lower() else (
            "delegatecall" if "delegatecall" in source_code.lower() else "none"
        ),
        "lines_of_code": len(source_code.splitlines())
    })

    print(f"✅ Saved {name} → {source_path}")

    time.sleep(0.2)  # Rate limit: safe pacing

# Save all results to a master CSV
df = pd.DataFrame(records)
df.to_csv(master_outfile, index=False)
print(f"\n📊 All verified contract metadata saved to: {master_outfile}")