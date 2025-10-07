#!/usr/bin/env python3
"""
fetch_contracts_from_expanded.py
---------------------------------------------------
Fetch verified Solidity source codes for expanded verified contract list
(≈ 4,000+ addresses). Supports Base, Blast, and other Etherscan forks.
Includes:
 - resume-safe operation
 - polite rate limiting
 - automatic chain fallback
 - source code metrics (lines, functions, events)
"""

import os
import re
import time
import json
import random
import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path

# === Paths ===
ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data_raw" / "contracts"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INPUT_CSV = DATA_DIR / "verified_contracts_expanded.csv"
OUTPUT_CSV = DATA_DIR / "verified_contracts_from_expanded.csv"
BACKUP_JSON = DATA_DIR / "fetch_progress_backup.json"

# === API KEYS ===
API_KEYS = {
    "ethereum": os.getenv("ETHERSCAN_API_KEY", "YourEtherscanKeyHere"),
    "bsc": os.getenv("BSCSCAN_API_KEY", "YourBscKeyHere"),
    "polygon": os.getenv("POLYGONSCAN_API_KEY", "YourPolygonKeyHere"),
    "arbitrum": os.getenv("ARBISCAN_API_KEY", "YourArbitrumKeyHere"),
    "optimism": os.getenv("OPTIMISMSCAN_API_KEY", "YourOptimismKeyHere"),
    "avalanche": os.getenv("SNOWTRACE_API_KEY", "YourAvalancheKeyHere"),
    "base": os.getenv("BASESCAN_API_KEY", "YourBaseKeyHere"),
    "blast": os.getenv("BLASTSCAN_API_KEY", "YourBlastKeyHere"),
    "scroll": os.getenv("SCROLLSCAN_API_KEY", "YourScrollKeyHere"),
    "linea": os.getenv("LINEASCAN_API_KEY", "YourLineaKeyHere"),
}

# === Chain endpoints ===
CHAIN_APIS = {
    "ethereum": "https://api.etherscan.io/api",
    "bsc": "https://api.bscscan.com/api",
    "polygon": "https://api.polygonscan.com/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "optimism": "https://api-optimistic.etherscan.io/api",
    "avalanche": "https://api.snowtrace.io/api",
    "base": "https://api.basescan.org/api",
    "blast": "https://api.blastscan.io/api",
    "scroll": "https://api.scrollscan.com/api",
    "linea": "https://api.lineascan.build/api",
}

MAX_RETRIES = 3
SLEEP_BETWEEN = (0.6, 1.4)

# === Load CSV ===
if not INPUT_CSV.exists():
    raise FileNotFoundError(f"{INPUT_CSV} not found.")

df = pd.read_csv(INPUT_CSV)
if "address" not in df.columns or "chains" not in df.columns:
    raise ValueError("Missing 'address' or 'chains' columns in verified_contracts_expanded.csv")

print(f"🔍 Fetching verified contracts for {len(df)} total addresses...")

# === Resume logic ===
if OUTPUT_CSV.exists():
    done_df = pd.read_csv(OUTPUT_CSV)
    done_columns = [c.lower().strip() for c in done_df.columns]
    if "contract_address" in done_columns:
        done_addresses = set(done_df[[c for c in done_df.columns if c.lower() == "contract_address"][0]].astype(str))
    elif "address" in done_columns:
        done_addresses = set(done_df[[c for c in done_df.columns if c.lower() == "address"][0]].astype(str))
    else:
        done_addresses = set()
        print("⚠️ No address column in existing output — starting fresh.")

    addr_col = [c for c in df.columns if c.lower() == "address"][0]
    df = df[~df[addr_col].astype(str).isin(done_addresses)]
    print(f"⏩ Resuming: {len(done_addresses)} done, {len(df)} remaining.")
else:
    done_df = pd.DataFrame()
    done_addresses = set()

# === Utility functions ===
def count_functions(code: str) -> int:
    return len(re.findall(r'\bfunction\b', code))

def count_events(code: str) -> int:
    return len(re.findall(r'\bevent\b', code))

def safe_get(api_url, params, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            r = requests.get(api_url, params=params, timeout=25)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"⚠️ API error: {e}")
        delay = 2 ** attempt + random.random()
        print(f"⏳ Retry {attempt+1}/{retries} in {delay:.1f}s...")
        time.sleep(delay)
    return None

def fetch_verified_code(address: str, chain_field: str):
    """
    Fetch verified Solidity source for a given contract address.
    Cleans up and restricts to supported Etherscan chains.
    """
    # Normalize & filter supported chains
    chain_candidates = re.split(r"[;,\|]", chain_field.lower())
    chain_candidates = [c.strip() for c in chain_candidates if c.strip()]
    supported_chains = [c for c in chain_candidates if c in CHAIN_APIS.keys()]

    # Fallback: if none supported, use ethereum
    if not supported_chains:
        print(f"⚠️ Unsupported chains '{chain_field}' — fallback to ethereum")
        supported_chains = ["ethereum"]

    for chain in supported_chains:
        api_url = CHAIN_APIS.get(chain)
        api_key = API_KEYS.get(chain)
        if not api_url or not api_key:
            continue

        params = {
            "module": "contract",
            "action": "getsourcecode",
            "address": address,
            "apikey": api_key,
        }

        data = safe_get(api_url, params)
        if data and data.get("status") == "1" and data.get("result"):
            result = data["result"][0]
            source_code = result.get("SourceCode", "")
            return {
                "chain": chain,
                "contract_address": address,
                "verified": True,
                "lines_of_code": len(source_code.splitlines()),
                "num_functions": count_functions(source_code),
                "num_events": count_events(source_code),
                "compiler_version": result.get("CompilerVersion", ""),
                "license": result.get("LicenseType", ""),
            }

    print(f"⚠️ No verified source found for {address} (chains tried: {', '.join(supported_chains)})")
    return None

# === Main ===
verified_contracts = []

for _, row in tqdm(df.iterrows(), total=len(df)):
    address = str(row["address"]).strip()
    chain = str(row["chains"]).lower().strip()

    if not address or "0x" not in address:
        continue

    print(f"🔎 Fetching {address} on {chain}...")
    result = fetch_verified_code(address, chain)
    if result:
        verified_contracts.append(result)
        if len(verified_contracts) % 50 == 0:
            tmp_df = pd.DataFrame(verified_contracts)
            combined = pd.concat([done_df, tmp_df], ignore_index=True)
            combined.to_csv(OUTPUT_CSV, index=False)
            with open(BACKUP_JSON, "w") as f:
                json.dump({"last_saved": time.time()}, f)
            print(f"💾 Saved progress ({len(combined)} verified so far)")

    time.sleep(random.uniform(*SLEEP_BETWEEN))

# === Save final ===
if verified_contracts:
    final_df = pd.concat([done_df, pd.DataFrame(verified_contracts)], ignore_index=True)
    final_df.drop_duplicates(subset=["contract_address"], inplace=True)
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Done. Verified contracts saved → {OUTPUT_CSV} ({len(final_df)} rows)")
else:
    print("⚠️ No new verified contracts fetched.")