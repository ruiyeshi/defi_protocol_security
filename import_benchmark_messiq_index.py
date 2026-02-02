#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd
import hashlib

PROJECT_ROOT = Path(__file__).resolve().parent

BENCH_ROOT = PROJECT_ROOT / "data_benchmark"
DATASET_DIRS = [
    BENCH_ROOT / "contract_dataset_ethereum",
    BENCH_ROOT / "contract_dataset_github",
]

OUT_INDEX = BENCH_ROOT / "messiq_contracts_index.csv"
OUT_INDEX.parent.mkdir(parents=True, exist_ok=True)

def file_id(p: Path) -> str:
    # stable id from relative path
    rel = str(p).encode("utf-8")
    return hashlib.sha1(rel).hexdigest()[:16]

def main():
    rows = []
    for ds in DATASET_DIRS:
        if not ds.exists():
            print(f"⚠️ missing dataset folder: {ds}")
            continue

        # scan all .sol
        sols = list(ds.rglob("*.sol"))
        print(f"found {len(sols)} .sol in {ds.name}")

        for p in sols:
            rows.append({
                "dataset": ds.name,                 # contract_dataset_ethereum / contract_dataset_github
                "contract_id": file_id(p),
                "path": str(p.relative_to(PROJECT_ROOT)),
                "filename": p.name,
                "size_bytes": p.stat().st_size,
            })

    df = pd.DataFrame(rows).sort_values(["dataset", "path"]).reset_index(drop=True)
    df.to_csv(OUT_INDEX, index=False)
    print(f"✅ wrote {OUT_INDEX} rows={len(df)}")
    print(df["dataset"].value_counts().to_string())

if __name__ == "__main__":
    main()