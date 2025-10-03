#!/usr/bin/env python3
"""
fetch_contract_registry.py
Collect verified contract addresses across multiple sources (Etherscan + DeFiLlama + Solana).
"""

import os
import pandas as pd
import requests
from tqdm import tqdm
from pathlib import Path

OUT_PATH = Path("data_raw/contracts/contract_registry.csv")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# === ETHERSCAN-LIKE API KEYS (Optional) ===
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "YourEtherscanKeyHere")
BSCSCAN_API_KEY = os.getenv("BSCSCAN_API_KEY", ETHERSCAN_API_KEY)
POLYGONSCAN_API_KEY = os.getenv("POLYGONSCAN_API_KEY", ETHERSCAN_API_KEY)

# === Fetch from DeFiLlama ===
def fetch_llama_contracts():
    url = "https://api.llama.fi/protocols"
    r = requests.get(url, timeout=30)
    data = r.json()
    rows = []
    for p in data:
        name = p.get("name", "").lower()
        for c in p.get("chains", []):
            rows.append({
                "protocol_name": name,
                "chain": c,
                "contract_address": None
            })
    return pd.DataFrame(rows)

# === Fetch from Etherscan (verified contracts) ===
def fetch_etherscan_verified():
    base_urls = {
        "Ethereum": f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address=",
        "BSC": f"https://api.bscscan.com/api?module=contract&action=getsourcecode&address=",
        "Polygon": f"https://api.polygonscan.com/api?module=contract&action=getsourcecode&address="
    }

    contracts = []
    for chain, base_url in base_urls.items():
        print(f"🔍 Searching verified contracts on {chain}...")
        for addr in tqdm(["0xC36442b4a4522E871399CD717aBDD847Ab11FE88",  # Uniswap V3
                          "0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b",  # OpenSea
                          "0x1F98431c8aD98523631AE4a59f267346ea31F984",  # Uniswap governance
                          "0x3f3f5df88dC9F13eac63DF89EC16ef6e7E25DdE7"   # Aave oracle
                         ]):
            url = f"{base_url}{addr}&apikey={ETHERSCAN_API_KEY}"
            try:
                r = requests.get(url, timeout=10)
                result = r.json().get("result", [{}])[0]
                if result.get("SourceCode"):
                    contracts.append({
                        "protocol_name": result.get("ContractName", "").lower(),
                        "chain": chain,
                        "contract_address": addr,
                        "verified": True
                    })
            except Exception as e:
                print(f"⚠️ Error fetching {addr}: {e}")
    return pd.DataFrame(contracts)

if __name__ == "__main__":
    print("🔍 Fetching contract registry from multiple sources...")
    llama = fetch_llama_contracts()
    etherscan = fetch_etherscan_verified()
    df = pd.concat([llama, etherscan], ignore_index=True)
    df.drop_duplicates(subset=["protocol_name", "chain", "contract_address"], inplace=True)
    df.to_csv(OUT_PATH, index=False)
    print(f"✅ Saved expanded contract registry → {OUT_PATH} ({len(df)} entries)")