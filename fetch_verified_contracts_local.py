#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

# ✅ Use the file you actually created (change to master_contracts.csv if you prefer)
INFILE = PROJECT_ROOT / "data_raw" / "contracts" / "master_contracts_chain_address.csv"
OUTFILE = PROJECT_ROOT / "data_raw" / "contracts" / "verified_contracts_local.csv"
OUTFILE.parent.mkdir(parents=True, exist_ok=True)

# v2 unified base (set in .env; fallback here)
V2_BASE = os.getenv("EXPLORER_BASE_URL", "https://api.etherscan.io/v2/api").strip()
API_KEY = (os.getenv("EXPLORER_API_KEY") or os.getenv("ETHERSCAN_API_KEY") or "").strip()

TIMEOUT = 25
SLEEP_SEC = float(os.getenv("ETHERSCAN_SLEEP_SEC", "0.25"))
RETRY_SLEEP = 3.0
MAX_RETRIES = 5

CHAINID: Dict[str, int] = {
    "ethereum": 1,
    "optimism": 10,
    "bsc": 56,
    "polygon": 137,
    "fantom": 250,
    "arbitrum": 42161,
    "avalanche": 43114,
    "base": 8453,
    "celo": 42220,
    "linea": 59144,
    "scroll": 534352,
}

CHAIN_ALIASES = {
    "eth": "ethereum",
    "arbitrum one": "arbitrum",
    "op": "optimism",
    "avax": "avalanche",
    "binance": "bsc",
    "binance smart chain": "bsc",
}

def norm_chain(x: Any) -> str:
    s = str(x or "").strip().lower()
    return CHAIN_ALIASES.get(s, s)

def norm_addr(x: Any) -> str:
    return str(x or "").strip()

def request_json_v2(chain: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not API_KEY:
        return None
    if chain not in CHAINID:
        return None

    p = dict(params)
    p["chainid"] = CHAINID[chain]
    p["apikey"] = API_KEY

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(V2_BASE, params=p, timeout=TIMEOUT)
            if r.status_code in (403, 429):
                time.sleep(RETRY_SLEEP * attempt)
                continue
            r.raise_for_status()
            j = r.json()
            time.sleep(SLEEP_SEC)
            return j
        except Exception:
            time.sleep(RETRY_SLEEP * attempt)
    return None

def is_verified(chain: str, addr: str) -> Tuple[bool, str, bool, bool]:
    j = request_json_v2(chain, {
        "module": "contract",
        "action": "getsourcecode",
        "address": addr,
    })

    if not j or "result" not in j:
        return (False, "", False, False)

    res = j.get("result")
    if not isinstance(res, list) or not res or not isinstance(res[0], dict):
        return (False, "", False, False)

    it = res[0]
    contract_name = str(it.get("ContractName", "") or "").strip()
    abi = str(it.get("ABI", "") or "").strip()
    source = str(it.get("SourceCode", "") or "").strip()

    has_abi = bool(abi) and ("not verified" not in abi.lower())
    has_source = bool(source)
    verified = bool(contract_name) and (has_abi or has_source)
    return (verified, contract_name, has_source, has_abi)

def main():
    if not INFILE.exists():
        raise SystemExit(f"Missing input: {INFILE}")

    df = pd.read_csv(INFILE)

    # accept contract_address → address
    if "address" not in df.columns and "contract_address" in df.columns:
        df = df.rename(columns={"contract_address": "address"})

    if "chain" not in df.columns or "address" not in df.columns:
        raise SystemExit("Input must contain columns: chain, address (or contract_address).")

    df["chain"] = df["chain"].map(norm_chain)
    df["address"] = df["address"].map(norm_addr)

    # keep only supported chains
    df = df[df["chain"].isin(CHAINID.keys())].copy()

    # basic address sanity
    df["address"] = df["address"].astype(str).str.strip()
    df = df[df["address"].str.startswith("0x") & (df["address"].str.len() == 42)].copy()

    df = df.drop_duplicates(subset=["chain", "address"]).reset_index(drop=True)

    print("Input rows (unique chain,address):", len(df))
    print("Chains:\n", df["chain"].value_counts().to_string())

    out_rows = []
    for i, row in df.iterrows():
        chain = row["chain"]
        addr = row["address"]
        slug = row["slug"] if "slug" in df.columns else ""

        ok, cname, has_source, has_abi = is_verified(chain, addr)
        if ok:
            out_rows.append({
                "slug": slug,
                "address": addr,
                "chain": chain,
                "source": "getsourcecode_v2",
                "verified": 1,
                "contract_name": cname,
                "has_source": int(has_source),
                "has_abi": int(has_abi),
            })

        if (i + 1) % 50 == 0:
            print(f"checked {i+1}/{len(df)} | verified_kept={len(out_rows)}")

    out = pd.DataFrame(out_rows)
    out.to_csv(OUTFILE, index=False)
    print(f"✅ Wrote {len(out)} verified rows -> {OUTFILE}")

if __name__ == "__main__":
    main()