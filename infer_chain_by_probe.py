#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
infer_chain_by_probe.py

Purpose:
  Infer chain for blank-chain EVM addresses by probing Etherscan-v2 getsourcecode
  across a prioritized list of chainids. Optionally also outputs verified rows.

Inputs (default):
  data_raw/contracts/master_contracts_llama_adapters.csv

Expected columns:
  address (required)
  chain (optional; blanks will be probed)
  protocol/slug/category/notes (optional)

Outputs:
  data_raw/contracts/adapter_blank_chain_inferred.csv
  data_raw/contracts/verified_contracts_from_probed_blanks.csv

Notes:
  - This is intentionally "best-effort" and can be expensive.
  - Uses checkpoint so you can stop/resume.
  - Only EVM chains supported by Etherscan-v2 family.
"""

from __future__ import annotations

import os
import time
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

# ---------- CONFIG ----------
INFILE = Path(os.getenv("PROBE_INPUT", ROOT / "data_raw/contracts/master_contracts_llama_adapters.csv"))
OUT_INFER = ROOT / "data_raw/contracts/adapter_blank_chain_inferred.csv"
OUT_VER = ROOT / "data_raw/contracts/verified_contracts_from_probed_blanks.csv"

CKPT = ROOT / "outputs/ckpt_probe_blank_chain.json"
CKPT.parent.mkdir(parents=True, exist_ok=True)

# Etherscan v2 base
V2_BASE = os.getenv("EXPLORER_BASE_URL", "https://api.etherscan.io/v2/api").strip()
API_KEY = (os.getenv("EXPLORER_API_KEY") or os.getenv("ETHERSCAN_API_KEY") or "").strip()

# Probe order (highest yield first)
DEFAULT_CHAINS = [
    "ethereum", "arbitrum", "optimism", "base",
    "bsc", "polygon", "fantom", "avalanche",
    "linea", "scroll", "mantle", "blast", "metis", "celo", "gnosis", "sei",
]
PROBE_CHAINS = [c.strip().lower() for c in os.getenv("PROBE_CHAINS", ",".join(DEFAULT_CHAINS)).split(",") if c.strip()]

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

TIMEOUT = float(os.getenv("PROBE_TIMEOUT", "25"))
SLEEP_SEC = float(os.getenv("PROBE_SLEEP_SEC", "0.35"))   # be gentle
RETRY_SLEEP = float(os.getenv("PROBE_RETRY_SLEEP", "2.0"))
MAX_RETRIES = int(os.getenv("PROBE_MAX_RETRIES", "4"))

# How many blank addresses to probe (0 = all)
MAX_ADDR = int(os.getenv("PROBE_MAX_ADDR", "0"))

# ---------- HELPERS ----------
def norm_addr(x: Any) -> str:
    return str(x or "").strip()

def _req(chainid: int, address: str) -> Optional[Dict[str, Any]]:
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

def _is_verified_payload(j: Dict[str, Any]) -> Tuple[bool, str, bool, bool]:
    """
    Return: (verified, contract_name, has_source, has_abi)
    """
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

def load_ckpt() -> Dict[str, Any]:
    if CKPT.exists():
        try:
            return json.loads(CKPT.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_ckpt(state: Dict[str, Any]) -> None:
    CKPT.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------- MAIN ----------
def main():
    if not INFILE.exists():
        raise SystemExit(f"Missing input: {INFILE}")

    df = pd.read_csv(INFILE)

    # normalize columns
    if "address" not in df.columns and "contract_address" in df.columns:
        df = df.rename(columns={"contract_address": "address"})
    if "chain" not in df.columns:
        df["chain"] = ""
    if "protocol" not in df.columns:
        df["protocol"] = df.get("slug", "")

    df["chain"] = df["chain"].fillna("").astype(str).str.strip().str.lower()
    df["address"] = df["address"].map(norm_addr)

    # focus on blank chain only
    blank = df[df["chain"] == ""].copy()
    blank = blank[blank["address"].astype(str).str.startswith("0x") & (blank["address"].astype(str).str.len() == 42)].copy()
    blank = blank.drop_duplicates(subset=["address"]).reset_index(drop=True)

    if MAX_ADDR and MAX_ADDR > 0:
        blank = blank.head(MAX_ADDR).copy()

    print(f"Blank-chain unique addresses to probe: {len(blank)}")
    print("Probe chains order:", PROBE_CHAINS)

    # resume state
    ck = load_ckpt()
    done = set(ck.get("done", []))
    results_infer: List[Dict[str, Any]] = ck.get("results_infer", [])
    results_ver: List[Dict[str, Any]] = ck.get("results_ver", [])

    # map for quick skip if already inferred
    already = {r["address"]: r for r in results_infer if "address" in r}

    checked = 0
    inferred_hits = 0
    verified_hits = 0

    for addr in blank["address"].tolist():
        if addr in done:
            continue

        checked += 1
        found_chain = ""
        found_name = ""
        found_has_source = 0
        found_has_abi = 0
        evidence = ""

        for ch in PROBE_CHAINS:
            if ch not in CHAINID:
                continue
            j = _req(CHAINID[ch], addr)
            ok, cname, hs, ha = _is_verified_payload(j or {})
            if ok:
                found_chain = ch
                found_name = cname
                found_has_source = int(hs)
                found_has_abi = int(ha)
                evidence = f"{V2_BASE}?module=contract&action=getsourcecode&chainid={CHAINID[ch]}&address={addr}"
                break

        if found_chain:
            inferred_hits += 1
            results_infer.append({
                "address": addr,
                "inferred_chain": found_chain,
                "method": "probe_getsourcecode_v2",
                "contract_name": found_name,
                "has_source": found_has_source,
                "has_abi": found_has_abi,
                "evidence_url": evidence,
            })
            results_ver.append({
                "slug": "",
                "address": addr,
                "chain": found_chain,
                "source": "probe_getsourcecode_v2",
                "verified": 1,
                "contract_name": found_name,
                "has_source": found_has_source,
                "has_abi": found_has_abi,
            })
            verified_hits += 1
        else:
            results_infer.append({
                "address": addr,
                "inferred_chain": "",
                "method": "probe_getsourcecode_v2",
                "contract_name": "",
                "has_source": 0,
                "has_abi": 0,
                "evidence_url": "",
            })

        done.add(addr)

        # periodic save
        if checked % 50 == 0:
            print(f"checked {checked} | inferred={inferred_hits} | verified={verified_hits} | ckpt={CKPT}")
            save_ckpt({
                "done": list(done),
                "results_infer": results_infer,
                "results_ver": results_ver,
            })

    # final save
    save_ckpt({
        "done": list(done),
        "results_infer": results_infer,
        "results_ver": results_ver,
    })

    infer_df = pd.DataFrame(results_infer).drop_duplicates(subset=["address"], keep="last")
    ver_df = pd.DataFrame(results_ver).drop_duplicates(subset=["chain","address"], keep="last")

    OUT_INFER.parent.mkdir(parents=True, exist_ok=True)
    infer_df.to_csv(OUT_INFER, index=False)
    ver_df.to_csv(OUT_VER, index=False)

    print(f"✅ wrote {OUT_INFER} rows={len(infer_df)}")
    print(f"✅ wrote {OUT_VER} rows={len(ver_df)}")

if __name__ == "__main__":
    main()