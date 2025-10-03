#!/usr/bin/env python3
# /Users/ruiyeshi/defi_protocol_security/run_slither_batch.py
# -*- coding: utf-8 -*-

import os, json, subprocess, logging
import pandas as pd
from pathlib import Path
from tqdm import tqdm

ROOT = Path(__file__).resolve().parent
LOGS = ROOT/"logs"; LOGS.mkdir(exist_ok=True)
logging.basicConfig(filename=LOGS/"slither_run.log", level=logging.INFO, format="%(asctime)s | %(message)s")

CONTRACTS = ROOT/"data_raw/contracts/verified_contracts_expanded.csv"
SCAN_DIR = ROOT/"data_raw/scans_slither"; SCAN_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = ROOT/"outputs/slither_vulnerabilities.csv"; OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

def run_slither_on_source(addr: str, src_file: Path, out_json: Path):
    if out_json.exists(): 
        return True
    cmd = ["slither", str(src_file), "--json", str(out_json)]
    try:
        subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        return out_json.exists()
    except Exception as e:
        logging.info(f"slither fail {addr}: {e}")
        return False

def main():
    if not CONTRACTS.exists():
        raise SystemExit(f"Missing {CONTRACTS}")
    df = pd.read_csv(CONTRACTS)
    if "source_code" not in df.columns:
        raise SystemExit("contracts CSV has no source_code column (expected from fetch step)")
    agg=[]
    for _,r in tqdm(df.iterrows(), total=len(df), desc="Slither"):
        addr=r["contract_address"]; chain=r["chain"]
        src = (ROOT/"data_raw/sources"); src.mkdir(parents=True, exist_ok=True)
        sol_file = src/f"{addr}.sol"
        if not sol_file.exists(): sol_file.write_text(r["source_code"] or "pragma solidity ^0.8.0;")
        out_json = SCAN_DIR/f"{addr}.json"
        ok = run_slither_on_source(addr, sol_file, out_json)
        if not ok: continue
        # parse
        try:
            data=json.loads(out_json.read_text())
            for issue in data.get("results",{}).get("detectors",[]):
                fam = issue.get("check","unknown")
                sev = issue.get("impact","unknown")
                agg.append({
                    "chain": chain, "contract_address": addr,
                    "slither_family": fam, "severity": sev
                })
        except Exception: pass

    if agg:
        out=pd.DataFrame(agg)
        # summarize per contract
        piv = (out
               .assign(cnt=1)
               .pivot_table(index=["chain","contract_address"],
                            columns="slither_family", values="cnt", aggfunc="sum", fill_value=0)
               .reset_index())
        piv["slither_total"]=piv.drop(columns=["chain","contract_address"]).sum(axis=1)
        piv.to_csv(OUT_CSV, index=False)
        print(f"✅ Saved Slither results → {OUT_CSV} ({len(piv)} contracts)")
    else:
        print("⚠️ No Slither findings (or parse errors).")

if __name__=="__main__":
    main()