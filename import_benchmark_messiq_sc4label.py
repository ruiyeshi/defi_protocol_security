#!/usr/bin/env python3
"""
Import MessiQ SC_4label benchmark into a clean, isolated benchmark folder.

INPUTS (your updated paths):
  - data_raw/contracts/SC_4label.csv
  - data_raw/contracts/messiq_sc4label_meta.csv  (optional; used if exists)

OUTPUTS (NO contamination: goes ONLY to data_external/benchmarks/messiq/):
  - contracts_clean.csv      (one row per benchmark contract)
  - meta_loaded.csv          (copied/cleaned meta if present)
  - contracts/               (solidity files written out; optional)
  - summary.json             (counts)

Usage:
  python import_benchmark_messiq_sc4label.py
  python import_benchmark_messiq_sc4label.py --no-write-sol
"""

from __future__ import annotations
from pathlib import Path
import argparse
import hashlib
import json
import re

import pandas as pd


ROOT = Path(".")
IN_SC4 = ROOT / "data_raw" / "contracts" / "SC_4label.csv"
IN_META = ROOT / "data_raw" / "contracts" / "messiq_sc4label_meta.csv"

OUT_DIR = ROOT / "data_external" / "benchmarks" / "messiq"
OUT_CONTRACTS = OUT_DIR / "contracts_clean.csv"
OUT_META = OUT_DIR / "meta_loaded.csv"
OUT_SOL_DIR = OUT_DIR / "contracts"
OUT_SUMMARY = OUT_DIR / "summary.json"


def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def guess_solc_version(code: str) -> str | None:
    # best-effort pragma parse
    m = re.search(r"pragma\s+solidity\s+([^;]+);", code)
    if not m:
        return None
    return m.group(1).strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-write-sol", action="store_true", help="Do not write .sol files to disk")
    args = ap.parse_args()

    if not IN_SC4.exists():
        raise SystemExit(f"Missing input: {IN_SC4}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if not args.no_write_sol:
        OUT_SOL_DIR.mkdir(parents=True, exist_ok=True)

    # Load SC_4label.csv
    df = pd.read_csv(IN_SC4)

    # Expected columns based on your file preview:
    # ,filename,code,label,label_encoded
    # Sometimes there is an unnamed index column.
    if "code" not in df.columns or "filename" not in df.columns:
        raise SystemExit(f"SC_4label.csv schema unexpected. Columns: {df.columns.tolist()}")

    # Clean
    df["filename"] = df["filename"].astype(str).str.strip()
    df["code"] = df["code"].astype(str)
    df["label"] = df["label"].astype(str).str.strip() if "label" in df.columns else ""
    if "label_encoded" in df.columns:
        df["label_encoded"] = pd.to_numeric(df["label_encoded"], errors="coerce")
    else:
        df["label_encoded"] = pd.NA

    # Drop empty code rows (NO GARBAGE)
    df = df[df["code"].str.len() > 20].copy()

    # Stable benchmark id
    df["code_sha1"] = df["code"].map(sha1_text)
    df["benchmark_id"] = df["filename"].fillna("") + "__" + df["code_sha1"].str[:12]

    # pragma guess
    df["pragma_solidity"] = df["code"].map(guess_solc_version)

    # Write .sol files (optional)
    if not args.no_write_sol:
        for _, row in df.iterrows():
            bid = row["benchmark_id"]
            sol_path = OUT_SOL_DIR / f"{bid}.sol"
            if not sol_path.exists():  # don't rewrite if already there
                sol_path.write_text(row["code"], encoding="utf-8", errors="ignore")

    # Output contracts_clean.csv (minimal, clean)
    keep_cols = [
        "benchmark_id",
        "filename",
        "code_sha1",
        "label",
        "label_encoded",
        "pragma_solidity",
    ]
    out = df[keep_cols].drop_duplicates(subset=["benchmark_id"]).reset_index(drop=True)
    out.to_csv(OUT_CONTRACTS, index=False)

    # Load meta file if present (optional)
    meta_loaded = False
    if IN_META.exists():
        meta = pd.read_csv(IN_META)
        meta.to_csv(OUT_META, index=False)
        meta_loaded = True

    summary = {
        "input_sc4label": str(IN_SC4),
        "input_meta": str(IN_META),
        "meta_loaded": meta_loaded,
        "rows_in_clean": int(len(out)),
        "labels_value_counts": out["label_encoded"].value_counts(dropna=False).to_dict(),
        "wrote_sol_files": (not args.no_write_sol),
        "sol_dir": str(OUT_SOL_DIR),
        "contracts_clean_csv": str(OUT_CONTRACTS),
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"✅ Wrote: {OUT_CONTRACTS}  rows={len(out)}")
    if meta_loaded:
        print(f"✅ Wrote: {OUT_META}  rows={len(pd.read_csv(OUT_META))}")
    print(f"✅ Wrote: {OUT_SUMMARY}")
    if not args.no_write_sol:
        print(f"✅ Solidity files in: {OUT_SOL_DIR}")


if __name__ == "__main__":
    main()