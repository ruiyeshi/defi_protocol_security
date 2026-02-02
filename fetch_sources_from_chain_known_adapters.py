#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, time, json
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

# ===== Inputs / outputs =====
INFILE = Path(os.getenv("SOURCE_FETCH_INPUT", ROOT / "data_raw/contracts/master_contracts_llama_adapters_chain_known.csv"))
OUT_VER = ROOT / "data_raw/contracts/verified_contracts_from_adapters.csv"
OUT_SRC = ROOT / "data_raw/contracts/fetched_contract_sources_adapters.csv"
CKPT = ROOT / "outputs/ckpt_fetch_sources_adapters.json"
CACHE_DIR = ROOT / "data_raw/contracts/source_cache"  # optional per-contract files

for p in [OUT_VER.parent, OUT_SRC.parent, CKPT.parent, CACHE_DIR]:
    p.mkdir(parents=True, exist_ok=True)

# ===== Explorer v2 =====
V2_BASE = os.getenv("EXPLORER_BASE_URL", "https://api.etherscan.io/v2/api").strip()
API_KEY = (os.getenv("EXPLORER_API_KEY") or os.getenv("ETHERSCAN_API_KEY") or "").strip()

# rate limiting
SLEEP_SEC = float(os.getenv("ETHERSCAN_SLEEP_SEC", "0.25"))
TIMEOUT = float(os.getenv("TIMEOUT", "25"))
MAX_RETRIES = int(os.getenv("RETRY_LIMIT", "5"))
RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", "2"))

# batch limit (0 = all)
MAX_ADDR = int(os.getenv("SOURCE_FETCH_MAX_ADDR", "0"))

CHAINID: Dict[str, int] = {
    "ethereum": 1, "optimism": 10, "bsc": 56, "polygon": 137, "fantom": 250,
    "arbitrum": 42161, "avalanche": 43114, "base": 8453, "celo": 42220,
    "linea": 59144, "scroll": 534352, "mantle": 5000, "blast": 81457,
    "metis": 1088, "gnosis": 100, "sei": 1329,
}

# ===== Helpers =====
def _load_ckpt() -> dict:
    if CKPT.exists():
        try:
            return json.loads(CKPT.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_ckpt(state: dict) -> None:
    CKPT.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def _req_json(params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not API_KEY:
        raise SystemExit("Missing API key: set EXPLORER_API_KEY (or ETHERSCAN_API_KEY) in .env")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(V2_BASE, params=params, timeout=TIMEOUT)
            if r.status_code in (403, 429):
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            r.raise_for_status()
            j = r.json()
            time.sleep(SLEEP_SEC)
            return j
        except Exception:
            time.sleep(RETRY_BACKOFF * attempt)
    return None

def get_sourcecode(chain: str, address: str) -> Tuple[bool, Dict[str, Any]]:
    """Return (ok, payload). ok=True means we got a structured result row."""
    cid = CHAINID.get(chain)
    if not cid:
        return (False, {})

    params = {
        "module": "contract",
        "action": "getsourcecode",
        "chainid": cid,
        "address": address,
        "apikey": API_KEY,
    }
    j = _req_json(params)
    if not j or "result" not in j:
        return (False, {})

    res = j.get("result")
    if not isinstance(res, list) or not res or not isinstance(res[0], dict):
        return (False, {})

    return (True, res[0])

def _norm_addr(x: Any) -> str:
    return str(x or "").strip()

def _safe_write_source(chain: str, address: str, source: str) -> Optional[str]:
    """Write source to per-contract file if it looks like Solidity; return filepath or None."""
    if not source or not isinstance(source, str):
        return None
    s = source.strip()
    if not s:
        return None

    # create chain folder
    d = CACHE_DIR / chain
    d.mkdir(parents=True, exist_ok=True)
    fp = d / f"{address.lower()}.sol"
    fp.write_text(s, encoding="utf-8", errors="ignore")
    return str(fp)

def main():
    if not INFILE.exists():
        raise SystemExit(f"Missing input: {INFILE}")

    df = pd.read_csv(INFILE, low_memory=False)

    # standardize columns
    if "address" not in df.columns and "contract_address" in df.columns:
        df = df.rename(columns={"contract_address": "address"})
    if "chain" not in df.columns or "address" not in df.columns:
        raise SystemExit("Input must contain columns: chain, address (or contract_address).")

    df["chain"] = df["chain"].fillna("").astype(str).str.lower().str.strip()
    df["address"] = df["address"].map(_norm_addr)

    # keep only EVM chains we support + address-shaped
    df = df[df["chain"].isin(CHAINID.keys())].copy()
    df = df[df["address"].astype(str).str.startswith("0x") & (df["address"].astype(str).str.len() == 42)].copy()
    df = df.drop_duplicates(subset=["chain","address"]).reset_index(drop=True)

    if MAX_ADDR and MAX_ADDR > 0:
        df = df.head(MAX_ADDR).copy()

    print("Input chain-known unique addresses:", len(df))
    print(df["chain"].value_counts().head(15).to_string())
    print("Using V2:", V2_BASE)

    ck = _load_ckpt()
    done = set(ck.get("done", []))
    ver_rows: List[dict] = ck.get("verified_rows", [])
    src_rows: List[dict] = ck.get("source_rows", [])

    checked = 0
    kept_verified = 0
    kept_source = 0

    for _, row in df.iterrows():
        chain = row["chain"]
        addr = row["address"]
        key = f"{chain}:{addr.lower()}"
        if key in done:
            continue

        checked += 1

        ok, payload = get_sourcecode(chain, addr)
        if not ok:
            done.add(key)
            continue

        contract_name = str(payload.get("ContractName","") or "").strip()
        abi = str(payload.get("ABI","") or "").strip()
        source = str(payload.get("SourceCode","") or "").strip()

        has_abi = bool(abi) and ("not verified" not in abi.lower())
        has_source = bool(source)

        verified = 1 if (contract_name and (has_source or has_abi)) else 0

        ver_rows.append({
            "chain": chain,
            "address": addr.lower(),
            "verified": verified,
            "contract_name": contract_name,
            "has_source": int(has_source),
            "has_abi": int(has_abi),
            "source": "getsourcecode_v2",
        })
        if verified:
            kept_verified += 1

        # store source (optional, but needed for Slither)
        if has_source:
            cache_fp = _safe_write_source(chain, addr, source)
            src_rows.append({
                "chain": chain,
                "address": addr.lower(),
                "contract_name": contract_name,
                "source_code": source,
                "cache_path": cache_fp or "",
            })
            kept_source += 1

        done.add(key)

        if checked % 50 == 0:
            print(f"checked={checked} | verified_kept={kept_verified} | source_kept={kept_source} | ckpt={CKPT}")
            _save_ckpt({"done": list(done), "verified_rows": ver_rows, "source_rows": src_rows})

    _save_ckpt({"done": list(done), "verified_rows": ver_rows, "source_rows": src_rows})

    ver = pd.DataFrame(ver_rows).drop_duplicates(subset=["chain","address"], keep="last")
    src = pd.DataFrame(src_rows).drop_duplicates(subset=["chain","address"], keep="last")

    ver.to_csv(OUT_VER, index=False)
    src.to_csv(OUT_SRC, index=False)

    print(f"✅ wrote {OUT_VER} rows={len(ver)} verified(sum)={int(ver['verified'].fillna(0).astype(int).sum()) if 'verified' in ver.columns else 0}")
    print(f"✅ wrote {OUT_SRC} rows={len(src)} (has source cached)")
    if "verified" in ver.columns:
        print("verified by chain:\n", ver[ver["verified"].astype(int)==1]["chain"].value_counts().head(15).to_string())
    print("source cached by chain:\n", src["chain"].value_counts().head(15).to_string())

if __name__ == "__main__":
    main()