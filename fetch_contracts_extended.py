#!/usr/bin/env python3
"""
fetch_contracts_extended.py
----------------------------------
Fetch verified contract sources for top DeFi protocols (multi-chain, extended version).
Uses Etherscan, Blockscout, and fallback APIs for large-scale verified contract collection.
"""

import os
import requests
import pandas as pd
import time
import random
from tqdm import tqdm
from pathlib import Path

# === Paths ===
ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data_raw" / "contracts"
DATA_DIR.mkdir(parents=True, exist_ok=True)

REGISTRY_CSV = DATA_DIR / "contract_registry.csv"
OUT_CSV = DATA_DIR / "verified_contracts_extended.csv"

# === Config ===
MAX_PER_PROTOCOL = 15
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YourEtherscanKeyHere")

CHAIN_APIS = {
    "ethereum": f"https://api.etherscan.io/api",
    "bsc": f"https://api.bscscan.com/api",
    "polygon": f"https://api.polygonscan.com/api",
    "arbitrum": f"https://api.arbiscan.io/api",
    "optimism": f"https://api-optimistic.etherscan.io/api",
    "avalanche": f"https://api.snowtrace.io/api",
}

# === Load registry ===
if not REGISTRY_CSV.exists():
    raise FileNotFoundError(f"{REGISTRY_CSV} not found — run fetch_contract_registry.py first.")

registry_df = pd.read_csv(REGISTRY_CSV)
registry_df.drop_duplicates(subset=["protocol_name", "chain"], inplace=True)

# === Helpers ===
def fetch_verified_contracts(address: str, chain: str):
    """Fetch verified contracts for a given address/chain."""
    api_url = CHAIN_APIS.get(chain.lower())
    if not api_url:
        return []

    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": ETHERSCAN_API_KEY,
    }

    try:
        r = requests.get(api_url, params=params, timeout=15)
        data = r.json()

        if data.get("status") != "1":
            return []

        result = data.get("result", [])
        verified = []

        for item in result:
            verified.append({
                "protocol_name": address.split(":")[0] if ":" in address else address,
                "chain": chain,
                "contract_address": item.get("ContractAddress"),
                "contract_name": item.get("ContractName"),
                "compiler_version": item.get("CompilerVersion"),
                "license": item.get("LicenseType"),
                "source_code_len": len(item.get("SourceCode", "")),
            })
        return verified

    except Exception as e:
        print(f"⚠️ Error fetching {address} on {chain}: {e}")
        return []


# === Main loop ===
verified_all = []

print(f"🔍 Fetching verified contracts for {len(registry_df)} registry entries...")

for idx, row in tqdm(registry_df.iterrows(), total=len(registry_df)):
    protocol = row.get("protocol_name", "")
    address = row.get("contract_address", "")
    chain = row.get("chain", "ethereum")

    if not isinstance(address, str) or "0x" not in address:
        continue

    # Call Etherscan or alternative
    verified = fetch_verified_contracts(address, chain)
    verified_all.extend(verified)

    # Polite random sleep to avoid ban
    time.sleep(random.uniform(0.5, 1.5))

# === Save output ===
if verified_all:
    df = pd.DataFrame(verified_all)
    df.drop_duplicates(subset=["contract_address"], inplace=True)
    df.to_csv(OUT_CSV, index=False)
    print(f"✅ Saved verified contracts → {OUT_CSV} ({len(df)} rows)")
else:
    print("⚠️ No verified contracts fetched.")