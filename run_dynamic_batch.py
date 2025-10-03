#!/usr/bin/env python3
# /Users/ruiyeshi/defi_protocol_security/run_dynamic_batch.py
# -*- coding: utf-8 -*-

import os, json, subprocess, random
import pandas as pd
from pathlib import Path
from tqdm import tqdm

ROOT=Path(__file__).resolve().parent
CONTRACTS=ROOT/"data_raw/contracts/verified_contracts_expanded.csv"
SRC_DIR=ROOT/"data_raw/sources"
OUT_DIR=ROOT/"data_raw/scans_echidna"; OUT_DIR.mkdir(parents=True, exist_ok=True)

SAMPLE_N=int(os.getenv("ECHIDNA_SAMPLE","100"))

def main():
    df=pd.read_csv(CONTRACTS)
    if len(df)>SAMPLE_N:
        df=df.sample(SAMPLE_N, random_state=7)
    for _,r in tqdm(df.iterrows(), total=len(df), desc="echidna"):
        addr=r["contract_address"]; sol=SRC_DIR/f"{addr}.sol"
        if not sol.exists(): continue
        out=OUT_DIR/f"{addr}.json"
        if out.exists(): continue
        # NOTE: you’ll need a harness & property file; this is placeholder:
        cmd=["echidna","test",str(sol)]
        try:
            subprocess.run(cmd, capture_output=True, text=True, timeout=180)
            out.write_text(json.dumps({"address":addr,"status":"ran"}, indent=2))
        except Exception as e:
            out.write_text(json.dumps({"address":addr,"status":"error","msg":str(e)}))
    print(f"✅ Echidna run finished → {OUT_DIR}")

if __name__=="__main__":
    main()