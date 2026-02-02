#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
import_external_verified_contracts.py

Reads a folder of "external datasets" (CSVs) that contain contract addresses
(and optionally chain/network), and produces a clean seed pool:

Output:
  data_raw/contracts/external_contract_seeds.csv
    columns: chain, address, source_file, notes

Usage:
  python import_external_verified_contracts.py ~/Desktop/results

If no arg is provided, defaults to ~/Desktop/results
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

OUT = Path("data_raw/contracts/external_contract_seeds.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

CHAIN_ALIASES = {
    "eth": "ethereum",
    "ethereum mainnet": "ethereum",
    "arbitrum one": "arbitrum",
    "op": "optimism",
    "optimistic": "optimism",
    "avax": "avalanche",
    "binance": "bsc",
    "binance smart chain": "bsc",
    "bnb": "bsc",
    "polygon pos": "polygon",
}

# columns we might see
ADDR_COL_CANDIDATES = [
    "address", "contract_address", "contract", "contractaddress", "addr",
    "smart_contract", "smartcontract", "to", "from"
]
CHAIN_COL_CANDIDATES = [
    "chain", "network", "blockchain", "platform", "chain_name", "chainid"
]

def norm_chain(x) -> str:
    s = str(x or "").strip().lower()
    s = CHAIN_ALIASES.get(s, s)
    return s

def norm_addr(x) -> str:
    return str(x or "").strip().lower()

def detect_cols(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    cols = {c.lower(): c for c in df.columns}
    addr_col = None
    chain_col = None

    for c in ADDR_COL_CANDIDATES:
        if c in cols:
            addr_col = cols[c]
            break

    for c in CHAIN_COL_CANDIDATES:
        if c in cols:
            chain_col = cols[c]
            break

    return addr_col, chain_col

def main():
    in_dir = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else Path("~/Desktop/results").expanduser()
    if not in_dir.exists():
        raise SystemExit(f"Input folder not found: {in_dir}")

    files = list(in_dir.rglob("*.csv"))
    if not files:
        raise SystemExit(f"No CSVs found under: {in_dir}")

    all_rows = []
    bad_files = 0

    for f in files:
        try:
            df = pd.read_csv(f, low_memory=False)
        except Exception:
            bad_files += 1
            continue

        addr_col, chain_col = detect_cols(df)
        if not addr_col:
            # skip files without any plausible address column
            continue

        tmp = pd.DataFrame()
        tmp["address"] = df[addr_col].map(norm_addr)

        if chain_col:
            tmp["chain"] = df[chain_col].map(norm_chain)
        else:
            tmp["chain"] = ""

        tmp["source_file"] = str(f)
        tmp["notes"] = f"addr_col={addr_col}" + (f"|chain_col={chain_col}" if chain_col else "|chain_col=None")

        # keep only EVM addresses
        tmp = tmp[tmp["address"].str.match(ADDR_RE, na=False)].copy()

        if not tmp.empty:
            all_rows.append(tmp)

    if not all_rows:
        # still write headers (no garbage rows)
        OUT.write_text("chain,address,source_file,notes\n", encoding="utf-8")
        print(f"⚠️ No addresses extracted. Wrote empty CSV: {OUT}")
        return

    out = pd.concat(all_rows, ignore_index=True)

    # de-dupe: prefer rows with chain known
    out["has_chain"] = (out["chain"].fillna("").astype(str).str.strip() != "").astype(int)
    out = out.sort_values(["address", "has_chain"], ascending=[True, False])
    out = out.drop_duplicates(subset=["address", "chain"], keep="first")
    out = out.drop(columns=["has_chain"])

    # Also create a chain+address unique file (useful for verifiers)
    out = out.drop_duplicates(subset=["chain", "address"], keep="first")

    out.to_csv(OUT, index=False)
    print(f"✅ Wrote {len(out)} rows -> {OUT}")
    print("Top chains:")
    print(out["chain"].replace("", "(blank)").value_counts().head(15).to_string())
    print(f"Files scanned: {len(files)} | unreadable: {bad_files}")

if __name__ == "__main__":
    main()