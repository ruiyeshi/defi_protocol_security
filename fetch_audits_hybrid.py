#!/usr/bin/env python3
"""
fetch_audits_hybrid.py
Fetch audit metadata from DeFiLlama and DeFiSafety APIs (hybrid version).
Handles empty or partial API responses gracefully.
"""

import os
import pandas as pd
import requests
from pathlib import Path

# === Paths ===
ROOT_DIR = Path(__file__).resolve()
while ROOT_DIR.name != "defi_protocol_security" and ROOT_DIR.parent != ROOT_DIR:
    ROOT_DIR = ROOT_DIR.parent

DATA_DIR = ROOT_DIR / "data_raw" / "contracts"
DATA_DIR.mkdir(parents=True, exist_ok=True)
API_OUT = DATA_DIR / "audit_metadata_api.csv"


# === Fetch from DeFiLlama ===
def fetch_defillama_projects():
    print("🌍 Fetching DeFiLlama protocols...")
    url = "https://api.llama.fi/protocols"
    try:
        r = requests.get(url, timeout=30)
        data = r.json()
        df = pd.DataFrame(data)
        df = df.rename(columns={"name": "protocol_name"})
        df = df[["protocol_name", "chain", "category", "tvl", "symbol"]].drop_duplicates()
        print(f"✅ Retrieved {len(df)} protocols from DeFiLlama")
        return df
    except Exception as e:
        print(f"⚠️ Error fetching DeFiLlama data: {e}")
        return pd.DataFrame()


# === Fetch from DeFiSafety ===
def fetch_defisafety_audits():
    print("🧠 Fetching DeFiSafety audit metadata...")
    url = "https://api.defisafety.com/api/projects"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"Status {r.status_code}")
        data = r.json()
        if not data:
            print("⚠️ No results from DeFiSafety API.")
            return pd.DataFrame()
        df = pd.json_normalize(data)
        df = df.rename(columns={"projectName": "protocol_name"})
        cols = ["protocol_name", "auditFirm", "score", "lastAuditDate"]
        for col in cols:
            if col not in df.columns:
                df[col] = None
        df = df[cols]
        print(f"✅ Retrieved {len(df)} audits from DeFiSafety")
        return df
    except Exception as e:
        print(f"⚠️ Failed to fetch from DeFiSafety: {e}")
        return pd.DataFrame()


# === Combine both sources ===
def combine_api_sources():
    llama = fetch_defillama_projects()
    safety = fetch_defisafety_audits()

    # Normalize column names
    if not llama.empty:
        llama.columns = [c.strip().lower() for c in llama.columns]
    if not safety.empty:
        safety.columns = [c.strip().lower() for c in safety.columns]

    # Rename missing identifiers
    if "protocol_name" not in llama.columns:
        if "name" in llama.columns:
            llama.rename(columns={"name": "protocol_name"}, inplace=True)

    if "protocol_name" not in safety.columns:
        if "projectname" in safety.columns:
            safety.rename(columns={"projectname": "protocol_name"}, inplace=True)

    # Handle missing cases
    if llama.empty and safety.empty:
        print("⚠️ Both APIs returned empty — skipping merge.")
        return pd.DataFrame()

    if safety.empty:
        print("⚠️ DeFiSafety returned no results — using DeFiLlama only.")
        llama["audit_firm_safety"] = None
        llama["audit_score_safety"] = None
        llama["audit_date_safety"] = None
        llama.to_csv(API_OUT, index=False)
        print(f"✅ Saved API dataset → {API_OUT} ({len(llama)} rows)")
        return llama

    if llama.empty:
        print("⚠️ DeFiLlama returned no results — using DeFiSafety only.")
        safety.to_csv(API_OUT, index=False)
        print(f"✅ Saved API dataset → {API_OUT} ({len(safety)} rows)")
        return safety

    # Merge
    merged = pd.merge(
        llama,
        safety,
        on="protocol_name",
        how="outer",
        suffixes=("_llama", "_safety"),
    )

    # Fill missing columns if absent
    for col in ["audit_firm_safety", "audit_score_safety", "audit_date_safety"]:
        if col not in merged.columns:
            merged[col] = None

    merged.to_csv(API_OUT, index=False)
    print(f"✅ Saved merged API audit dataset → {API_OUT} ({len(merged)} rows)")
    return merged


# === Main ===
if __name__ == "__main__":
    api_data = combine_api_sources()
    if api_data.empty:
        print("❌ No API data available, skipping next steps.")
    else:
        print(f"🎯 Combined dataset ready — {len(api_data)} total rows.")