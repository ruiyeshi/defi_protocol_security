#!/usr/bin/env python3
# /Users/ruiyeshi/defi_protocol_security/run_mythril_batch.py
# -*- coding: utf-8 -*-

import os, json, subprocess, logging, tempfile
import pandas as pd
from pathlib import Path
from tqdm import tqdm

ROOT=Path(__file__).resolve().parent
LOGS=ROOT/"logs"; LOGS.mkdir(exist_ok=True)
logging.basicConfig(filename=LOGS/"mythril_run.log", level=logging.INFO, format="%(asctime)s | %(message)s")

CONTRACTS=ROOT/"data_raw/contracts/verified_contracts_expanded.csv"
SRC_DIR=ROOT/"data_raw/sources"
SCAN_DIR=ROOT/"data_raw/scans_mythril"; SCAN_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV=ROOT/"outputs/mythril_vulnerabilities.csv"; OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

def analyze(sol: Path, out_json: Path) -> bool:
    if out_json.exists(): return True
    cmd=["myth","analyze",str(sol),"--execution-timeout","60","-o","json"]
    try:
        res=subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if res.stdout.strip():
            out_json.write_text(res.stdout)
            return True
    except Exception as e:
        logging.info(f"myth fail {sol.name}: {e}")
    return False

def main():
    if not CONTRACTS.exists(): raise SystemExit("missing contracts CSV")
    df=pd.read_csv(CONTRACTS)
    if not SRC_DIR.exists(): SRC_DIR.mkdir(parents=True, exist_ok=True)

    agg=[]
    for _,r in tqdm(df.iterrows(), total=len(df), desc="Mythril"):
        addr=r["contract_address"]; chain=r["chain"]
        sol=SRC_DIR/f"{addr}.sol"
        if not sol.exists():
            if isinstance(r.get("source_code",""), str) and r["source_code"]:
                sol.write_text(r["source_code"])
            else:
                continue
        out=SCAN_DIR/f"{addr}.json"
        ok=analyze(sol,out)
        if not ok: 
            continue
        try:
            data=json.loads(out.read_text())
            issues=data.get("issues",[])
            for i in issues:
                sl=i.get("description","").splitlines()[0][:120]
                sev=i.get("severity","Unknown")
                agg.append({"chain":chain,"contract_address":addr,"mythril_title":sl,"severity":sev})
        except Exception:
            # plain text or malformed JSON -> count as 0 issues
            pass

    pd.DataFrame(agg).to_csv(OUT_CSV, index=False)
    print(f"✅ Saved Mythril results → {OUT_CSV} ({len(agg)} rows)")

if __name__=="__main__":
    main()