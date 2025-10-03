#!/usr/bin/env python3
import os, sys
from pathlib import Path
from dotenv import load_dotenv

# --- Auto-detect project root ---
ROOT_DIR = Path(__file__).resolve()
while ROOT_DIR.name != "defi_protocol_security" and ROOT_DIR.parent != ROOT_DIR:
    ROOT_DIR = ROOT_DIR.parent

# --- Add root to Python path ---
sys.path.append(str(ROOT_DIR))

# --- Ensure .env is loaded from root ---
load_dotenv(dotenv_path=ROOT_DIR / ".env")

# --- Define standard folders ---
SRC_DIR = "data_final/contracts/solidity_sources"
OUT_CSV = "data_final/contracts/proxy_patterns.csv"

# --- Make sure folders exist ---
DATA_RAW.mkdir(parents=True, exist_ok=True)
DATA_FINAL.mkdir(parents=True, exist_ok=True)
import os
import re
import pandas as pd

SRC_DIR = "data_final/contracts/solidity_sources"
OUT_CSV = "data_final/contracts/proxy_patterns.csv"

patterns = {
    "delegatecall": r"\.delegatecall",
    "EIP1967": r"EIP1967|eip1967",
    "implementation": r"implementation",
    "proxy_contract": r"Proxy",
    "upgradeable": r"Upgradeable"
}

rows = []

for file in os.listdir(SRC_DIR):
    if not file.endswith(".sol"):
        continue

    path = os.path.join(SRC_DIR, file)
    with open(path, "r", errors="ignore") as f:
        code = f.read()

    matches = {key: bool(re.search(pattern, code, re.IGNORECASE))
               for key, pattern in patterns.items()}

    rows.append({"contract_file": file, **matches})

df = pd.DataFrame(rows)
df.to_csv(OUT_CSV, index=False)
print(f"✅ Saved proxy pattern features → {OUT_CSV} ({len(rows)} rows)")
