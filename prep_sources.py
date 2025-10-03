import os
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent
df = pd.read_csv(ROOT / "outputs" / "verified_contracts_expanded.csv")

SRC_DIR = ROOT / "data_raw" / "sources"
SRC_DIR.mkdir(parents=True, exist_ok=True)

for i, row in df.iterrows():
    source = str(row.get("source_code", "")).strip()
    addr = row.get("contract_address", "").lower()
    if not source or not addr:
        continue

    # clean Solidity wrapper if it's nested JSON-like
    if source.startswith("{{") or source.startswith("["):
        source = "// malformed or multi-file source omitted\n"

    path = SRC_DIR / f"{addr}.sol"
    with open(path, "w", encoding="utf-8") as f:
        f.write(source)

print(f"✅ Wrote {len(os.listdir(SRC_DIR))} Solidity files to {SRC_DIR}")