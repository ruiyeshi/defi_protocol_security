#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import time
import random
from pathlib import Path
import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

# ----- INPUTS -----
# Use the cleaned adapter master by default; fallback to master_contracts.csv
IN_CANDIDATES = [
    ROOT / "data_raw" / "contracts" / "master_contracts_llama_adapters_clean.csv",
    ROOT / "data_raw" / "contracts" / "master_contracts.csv",
]

OUT = ROOT / "data_raw" / "contracts" / "verified_contracts_from_master.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

# ----- RATE LIMIT -----
SLEEP_SEC = float(os.getenv("ETHERSCAN_SLEEP_SEC", "0.35"))
TIMEOUT = int(os.getenv("TIMEOUT", "30"))

def _norm_chain(x: str) -> str:
    x = (x or "").strip().lower()
    aliases = {
        "binance": "bsc",
        "bnb": "bsc",
        "bnbchain": "bsc",
        "matic": "polygon",
    }
    return aliases.get(x, x)

# Chain -> (base_url_env, api_key_env, chainid_for_etherscan_v2_or_none)
# We prefer the unified Etherscan v2 endpoint (EXPLORER_BASE_URL + EXPLORER_API_KEY)
# for most EVM chains. You can still override per-chain by setting the *_BASE_URL
# and *_API_KEY env vars.
SCANNERS = {
    # Unified v2 (chainid required)
    "ethereum": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 1),
    "bsc": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 56),
    "polygon": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 137),
    "fantom": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 250),
    "arbitrum": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 42161),
    "optimism": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 10),
    "base": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 8453),
    "linea": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 59144),
    "scroll": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 534352),
    "blast": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 81457),
    "mantle": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 5000),
    "metis": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 1088),
    "celo": ("EXPLORER_BASE_URL", "EXPLORER_API_KEY", 42220),

    # Avalanche: prefer Snowtrace if you have it; fallback to unified v2.
    "avalanche": ("SNOWTRACE_BASE_URL", "SNOWTRACE_API_KEY", 43114),
}

def pick_input() -> Path:
    for p in IN_CANDIDATES:
        if p.exists():
            return p
    raise SystemExit(f"No input found. Tried: {IN_CANDIDATES}")

def call_getsourcecode(chain: str, address: str) -> dict | None:
    chain = _norm_chain(chain)
    if chain not in SCANNERS:
        return None

    base_env, key_env, chainid = SCANNERS[chain]

    # Default base/key for unified v2
    default_base = os.getenv("EXPLORER_BASE_URL", os.getenv("ETHERSCAN_BASE_URL", "https://api.etherscan.io/v2/api")).strip()
    default_key = (os.getenv("EXPLORER_API_KEY", "").strip() or os.getenv("ETHERSCAN_API_KEY", "").strip())

    # Per-chain override (e.g., Snowtrace, or a dedicated scanner) if provided
    base = os.getenv(base_env, "").strip() or default_base
    key = os.getenv(key_env, "").strip() or default_key

    if not base or not key:
        return None

    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": key,
    }

    # If we are hitting the unified v2 endpoint (or any v2-ish endpoint), include chainid.
    # For Avalanche + Snowtrace, base might be v1; in that case chainid is ignored.
    if chainid is not None and "v2" in base:
        params["chainid"] = chainid

    try:
        r = requests.get(base, params=params, timeout=TIMEOUT)
    except Exception:
        return None

    if r.status_code in (429, 403):
        time.sleep(2.0 + random.random())
        try:
            r = requests.get(base, params=params, timeout=TIMEOUT)
        except Exception:
            return None

    if r.status_code != 200:
        return None

    try:
        return r.json()
    except Exception:
        return None

def is_verified_response(j: dict) -> tuple[bool, str]:
    if not isinstance(j, dict):
        return (False, "")

    result = j.get("result")
    if not isinstance(result, list) or not result:
        return (False, "")

    first = result[0] if isinstance(result[0], dict) else {}
    src = (first.get("SourceCode") or "").strip()
    name = (first.get("ContractName") or "").strip()

    # Some scanners return empty SourceCode for unverified; treat that as not verified.
    return (len(src) > 0, name)

def main():
    infile = pick_input()
    df = pd.read_csv(infile)

    # normalize columns
    if "address" not in df.columns and "contract_address" in df.columns:
        df = df.rename(columns={"contract_address": "address"})
    if "chain" not in df.columns:
        raise SystemExit(f"{infile} must contain column 'chain'")
    if "address" not in df.columns:
        raise SystemExit(f"{infile} must contain column 'address'")

    df["chain"] = df["chain"].fillna("").astype(str).map(_norm_chain)
    df["address"] = df["address"].fillna("").astype(str).str.strip()
    df = df[df["address"].str.startswith("0x") & (df["address"].str.len() == 42)].copy()
    df = df.drop_duplicates(subset=["chain","address"])

    print("Input unique (chain,address):", len(df))
    print("Chains:", df["chain"].value_counts().head(20).to_string())
    print("Has EXPLORER_API_KEY:", bool(os.getenv("EXPLORER_API_KEY") or os.getenv("ETHERSCAN_API_KEY")))

    rows = []
    verified_ct = 0

    for i, r in df.iterrows():
        chain = r["chain"]
        addr = r["address"]

        j = call_getsourcecode(chain, addr)
        if j is None:
            continue

        ok, cname = is_verified_response(j)
        if ok:
            verified_ct += 1
            rows.append({
                "slug": r.get("slug",""),
                "address": addr.lower(),
                "chain": chain,
                "source": "getsourcecode",
                "verified": 1,
                "contract_name": cname,
                "has_source": 1,
            })

        if verified_ct % 200 == 0 and verified_ct > 0:
            print("verified so far:", verified_ct)

        time.sleep(SLEEP_SEC)

    out = pd.DataFrame(rows)
    out.to_csv(OUT, index=False)
    print(f"✅ Wrote {len(out)} verified rows -> {OUT}")

if __name__ == "__main__":
    main()