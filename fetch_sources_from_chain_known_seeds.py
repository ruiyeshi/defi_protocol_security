#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

# INPUT: your chain-known universe (use whichever you trust most)
# Recommend: the chain-known adapter output you printed (≈22k unique)
INFILE = Path(os.getenv("SEEDS_CSV", ROOT / "data_raw/contracts/master_contracts_llama_adapters_chain_known.csv"))

# OUTPUTS
OUT_CSV = ROOT / "data_raw/contracts/fetched_contract_sources.csv"
OUT_VERIFIED = ROOT / "data_raw/contracts/verified_contracts_from_source_cache.csv"
CACHE_DIR = ROOT / "data_raw/contracts/source_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CKPT = ROOT / "outputs/ckpt_fetch_sources.json"
CKPT.parent.mkdir(parents=True, exist_ok=True)

V2_BASE = os.getenv("EXPLORER_BASE_URL", "https://api.etherscan.io/v2/api").strip()
API_KEY = (os.getenv("EXPLORER_API_KEY") or os.getenv("ETHERSCAN_API_KEY") or "").strip()

TIMEOUT = float(os.getenv("FETCH_TIMEOUT", "25"))
SLEEP_SEC = float(os.getenv("FETCH_SLEEP_SEC", "0.20"))
RETRY_SLEEP = float(os.getenv("FETCH_RETRY_SLEEP", "1.5"))
MAX_RETRIES = int(os.getenv("FETCH_MAX_RETRIES", "4"))

# Optional cap while testing
MAX_ADDR = int(os.getenv("FETCH_MAX_ADDR", "0"))  # 0 = all

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
    "mantle": 5000,
    "blast": 81457,
    "metis": 1088,
    "gnosis": 100,
    "sei": 1329,
}

def norm_chain(x: Any) -> str:
    return str(x or "").strip().lower()

def norm_addr(x: Any) -> str:
    return str(x or "").strip().lower()

def load_ckpt() -> dict:
    if CKPT.exists():
        try:
            return json.loads(CKPT.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_ckpt(state: dict) -> None:
    CKPT.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def req_getsource(chainid: int, address: str) -> Optional[dict]:
    if not API_KEY:
        raise SystemExit("Missing API key: set EXPLORER_API_KEY (or ETHERSCAN_API_KEY) in .env")

    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "chainid": chainid,
        "apikey": API_KEY,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(V2_BASE, params=params, timeout=TIMEOUT)
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

def is_verified_payload(j: dict) -> Tuple[bool, str, str, str]:
    """
    Returns: (verified, contract_name, abi, source_code)
    """
    if not isinstance(j, dict):
        return (False, "", "", "")
    res = j.get("result")
    if not isinstance(res, list) or not res:
        return (False, "", "", "")
    it = res[0] if isinstance(res[0], dict) else {}
    cname = str(it.get("ContractName", "") or "").strip()
    abi = str(it.get("ABI", "") or "").strip()
    src = str(it.get("SourceCode", "") or "").strip()

    has_abi = bool(abi) and ("not verified" not in abi.lower())
    has_src = bool(src)
    verified = bool(cname) and (has_src or has_abi)
    return (verified, cname, abi, src)

def flatten_source_to_sol(source_code: str) -> str:
    """
    Best-effort:
    - If SourceCode is JSON-ish (multi-file), extract all inner "content" fields and concatenate.
    - Else return as-is.
    """
    s = (source_code or "").strip()
    if not s:
        return ""

    # Some explorers wrap JSON in extra braces
    s2 = s
    if s2.startswith("{{") and s2.endswith("}}"):
        s2 = s2[1:-1].strip()

    # Detect JSON with "sources"
    if s2.startswith("{") and ("sources" in s2):
        try:
            obj = json.loads(s2)
            sources = obj.get("sources", {})
            chunks = []
            for fname, meta in sources.items():
                content = meta.get("content") if isinstance(meta, dict) else None
                if content:
                    chunks.append(f"// ---- {fname} ----\n{content}\n")
            return "\n".join(chunks).strip()
        except Exception:
            pass

    return s

def main():
    if not INFILE.exists():
        raise SystemExit(f"Missing input: {INFILE}")

    df = pd.read_csv(INFILE, low_memory=False)

    # Normalize to required cols: chain, address
    if "address" not in df.columns and "contract_address" in df.columns:
        df = df.rename(columns={"contract_address": "address"})
    if "chain" not in df.columns or "address" not in df.columns:
        raise SystemExit("Input seeds must contain columns: chain, address (or contract_address)")

    df["chain"] = df["chain"].map(norm_chain)
    df["address"] = df["address"].map(norm_addr)

    df = df[df["chain"].isin(CHAINID.keys())].copy()
    df = df[df["address"].str.startswith("0x") & (df["address"].str.len() == 42)].copy()
    df = df.drop_duplicates(subset=["chain", "address"]).reset_index(drop=True)

    if MAX_ADDR and MAX_ADDR > 0:
        df = df.head(MAX_ADDR).copy()

    print("Seeds (unique chain,address):", len(df))
    print(df["chain"].value_counts().head(20).to_string())

    ck = load_ckpt()
    done = set(ck.get("done", []))  # keys like "chain:address"
    rows: List[dict] = ck.get("rows", [])

    checked = 0
    kept_source = 0

    for _, r in df.iterrows():
        chain = r["chain"]
        addr = r["address"]
        key = f"{chain}:{addr}"
        if key in done:
            continue

        checked += 1
        cid = CHAINID[chain]

        j = req_getsource(cid, addr)
        verified, cname, abi, src = is_verified_payload(j or {})

        # Cache raw JSON always (for reproducibility)
        cdir = CACHE_DIR / chain
        cdir.mkdir(parents=True, exist_ok=True)
        (cdir / f"{addr}.json").write_text(json.dumps(j or {}, ensure_ascii=False), encoding="utf-8")

        # Cache .sol only if we have any source code
        sol_text = flatten_source_to_sol(src) if src else ""
        has_source = 1 if sol_text.strip() else 0
        if has_source:
            kept_source += 1
            (cdir / f"{addr}.sol").write_text(sol_text, encoding="utf-8")

        rows.append({
            "chain": chain,
            "address": addr,
            "chainid": cid,
            "contract_name": cname,
            "verified": int(verified),
            "has_source": int(has_source),
            "has_abi": int(bool(abi) and ("not verified" not in abi.lower())),
            "source_len": int(len(sol_text)),
            "evidence_url": f"{V2_BASE}?module=contract&action=getsourcecode&chainid={cid}&address={addr}",
        })

        done.add(key)

        if checked % 100 == 0:
            print(f"checked {checked} | has_source={kept_source} | ckpt={CKPT}")
            save_ckpt({"done": list(done), "rows": rows})

    save_ckpt({"done": list(done), "rows": rows})

    out = pd.DataFrame(rows).drop_duplicates(subset=["chain", "address"], keep="last")
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)

    tool_ready = out[out["has_source"] == 1].copy()
    tool_ready.to_csv(OUT_VERIFIED, index=False)

    print(f"✅ wrote {OUT_CSV} rows={len(out)}")
    print(f"✅ tool-ready (has_source=1): {len(tool_ready)}")
    print(tool_ready["chain"].value_counts().head(20).to_string())

if __name__ == "__main__":
    main()