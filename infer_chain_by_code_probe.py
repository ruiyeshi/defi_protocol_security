#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, time, json
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

INFILE = Path(os.getenv("PROBE_INPUT", ROOT / "data_raw/contracts/master_contracts_llama_adapters.csv"))
OUT_INFER = ROOT / "data_raw/contracts/adapter_blank_chain_inferred_by_code.csv"

CKPT = ROOT / "outputs/ckpt_probe_blank_chain_code.json"
CKPT.parent.mkdir(parents=True, exist_ok=True)

V2_BASE = os.getenv("EXPLORER_BASE_URL", "https://api.etherscan.io/v2/api").strip()
API_KEY = (os.getenv("EXPLORER_API_KEY") or os.getenv("ETHERSCAN_API_KEY") or "").strip()

DEFAULT_CHAINS = [
    "ethereum","arbitrum","optimism","base","bsc","polygon","fantom","avalanche",
    "linea","scroll","mantle","blast","metis","celo","gnosis","sei"
]
PROBE_CHAINS = [c.strip().lower() for c in os.getenv("PROBE_CHAINS", ",".join(DEFAULT_CHAINS)).split(",") if c.strip()]

CHAINID: Dict[str, int] = {
    "ethereum": 1, "optimism": 10, "bsc": 56, "polygon": 137, "fantom": 250,
    "arbitrum": 42161, "avalanche": 43114, "base": 8453, "celo": 42220,
    "linea": 59144, "scroll": 534352, "mantle": 5000, "blast": 81457,
    "metis": 1088, "gnosis": 100, "sei": 1329,
}

TIMEOUT = float(os.getenv("PROBE_TIMEOUT", "25"))
SLEEP_SEC = float(os.getenv("PROBE_SLEEP_SEC", "0.20"))
RETRY_SLEEP = float(os.getenv("PROBE_RETRY_SLEEP", "1.5"))
MAX_RETRIES = int(os.getenv("PROBE_MAX_RETRIES", "4"))
MAX_ADDR = int(os.getenv("PROBE_MAX_ADDR", "0"))  # 0 = all

def norm_addr(x: Any) -> str:
    return str(x or "").strip()

def load_ckpt() -> dict:
    if CKPT.exists():
        try:
            return json.loads(CKPT.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_ckpt(state: dict) -> None:
    CKPT.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def req_code(chainid: int, address: str) -> Optional[str]:
    if not API_KEY:
        raise SystemExit("Missing API key: set EXPLORER_API_KEY (or ETHERSCAN_API_KEY) in .env")
    params = {
        "module": "proxy",
        "action": "eth_getCode",
        "address": address,
        "tag": "latest",
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
            # Etherscan proxy returns {"result":"0x..."} typically
            code = j.get("result")
            if isinstance(code, str):
                return code
            return None
        except Exception:
            time.sleep(RETRY_SLEEP * attempt)
    return None

def main():
    if not INFILE.exists():
        raise SystemExit(f"Missing input: {INFILE}")

    df = pd.read_csv(INFILE)
    if "address" not in df.columns and "contract_address" in df.columns:
        df = df.rename(columns={"contract_address": "address"})
    if "chain" not in df.columns:
        df["chain"] = ""

    df["chain"] = df["chain"].fillna("").astype(str).str.strip().str.lower()
    df["address"] = df["address"].map(norm_addr)

    blank = df[df["chain"] == ""].copy()
    blank = blank[blank["address"].astype(str).str.startswith("0x") & (blank["address"].astype(str).str.len() == 42)].copy()
    blank = blank.drop_duplicates(subset=["address"]).reset_index(drop=True)

    if MAX_ADDR and MAX_ADDR > 0:
        blank = blank.head(MAX_ADDR).copy()

    print(f"Blank-chain unique addresses to probe: {len(blank)}")
    print("Probe chains order:", PROBE_CHAINS)

    ck = load_ckpt()
    done = set(ck.get("done", []))
    results: List[dict] = ck.get("results", [])

    checked = 0
    inferred = 0

    for addr in blank["address"].tolist():
        if addr in done:
            continue

        checked += 1
        found_chain = ""
        evidence = ""

        for ch in PROBE_CHAINS:
            cid = CHAINID.get(ch)
            if not cid:
                continue
            code = req_code(cid, addr)
            if code and code != "0x":
                found_chain = ch
                evidence = f"{V2_BASE}?module=proxy&action=eth_getCode&chainid={cid}&address={addr}&tag=latest"
                break

        if found_chain:
            inferred += 1
            results.append({
                "address": addr,
                "inferred_chain": found_chain,
                "method": "probe_eth_getCode_v2",
                "evidence_url": evidence,
            })
        else:
            results.append({
                "address": addr,
                "inferred_chain": "",
                "method": "probe_eth_getCode_v2",
                "evidence_url": "",
            })

        done.add(addr)

        if checked % 50 == 0:
            print(f"checked {checked} | inferred={inferred} | ckpt={CKPT}")
            save_ckpt({"done": list(done), "results": results})

    save_ckpt({"done": list(done), "results": results})

    out = pd.DataFrame(results).drop_duplicates(subset=["address"], keep="last")
    OUT_INFER.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_INFER, index=False)
    print(f"✅ wrote {OUT_INFER} rows={len(out)}")
    print(out["inferred_chain"].value_counts(dropna=False).head(20).to_string())

if __name__ == "__main__":
    main()