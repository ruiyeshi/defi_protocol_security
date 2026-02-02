#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Run Slither on DeFi contracts with locally cached Solidity source.

Input:  data_raw/contracts/fetched_contract_sources_adapters.csv
        (or any fetched_contract_sources*.csv)

Env vars:
  SRC_CSV      path to input csv (optional)
  OUT_DIR      output folder (default outputs/slither_defi)
  MAX_FILES    max contracts to run (0 = all)
  BATCH_SIZE   how often to checkpoint progress (default 50)
  START_AT     start index (default 0)
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Optional

import pandas as pd


def pick_latest_sources_csv() -> Path:
    cands = sorted(Path("data_raw/contracts").glob("fetched_contract_sources*.csv"))
    if not cands:
        raise SystemExit("❌ No data_raw/contracts/fetched_contract_sources*.csv found.")
    # Prefer adapters if present
    for p in reversed(cands):
        if "adapters" in p.name:
            return p
    return cands[-1]


def detect_source_col(df: pd.DataFrame) -> str:
    for c in ["source_code", "SourceCode", "source", "code", "Source"]:
        if c in df.columns:
            return c
    raise SystemExit(f"❌ No source column found. Columns: {df.columns.tolist()[:40]}")


def safe_slug(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9._-]+", "_", s)
    return s[:200] if s else "contract"


def main() -> None:
    src_csv = Path(os.getenv("SRC_CSV", "")).expanduser() if os.getenv("SRC_CSV") else pick_latest_sources_csv()
    out_dir = Path(os.getenv("OUT_DIR", "outputs/slither_defi"))
    max_files = int(os.getenv("MAX_FILES", "0"))
    batch_size = int(os.getenv("BATCH_SIZE", "50"))
    start_at = int(os.getenv("START_AT", "0"))

    out_dir.mkdir(parents=True, exist_ok=True)
    sol_dir = out_dir / "sol"
    json_dir = out_dir / "json"
    sol_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)

    progress_path = out_dir / "slither_defi_progress.csv"

    df = pd.read_csv(src_csv, low_memory=False)
    src_col = detect_source_col(df)

    # Normalize required cols
    if "address" not in df.columns:
        raise SystemExit("❌ Input csv must contain column: address")
    if "chain" not in df.columns:
        raise SystemExit("❌ Input csv must contain column: chain")

    df["chain"] = df["chain"].fillna("").astype(str).str.strip().str.lower()
    df["address"] = df["address"].fillna("").astype(str).str.strip().str.lower()
    df[src_col] = df[src_col].fillna("").astype(str)

    df = df[(df["chain"] != "") & df["address"].str.startswith("0x") & (df["address"].str.len() == 42)]
    df = df[df[src_col].str.strip() != ""].copy()
    df = df.drop_duplicates(subset=["chain", "address"]).reset_index(drop=True)

    if start_at > 0:
        df = df.iloc[start_at:].reset_index(drop=True)

    if max_files > 0:
        df = df.iloc[:max_files].reset_index(drop=True)

    print("✅ SRC:", src_csv)
    print("✅ OUT:", out_dir)
    print("rows (tool-ready):", len(df))
    print("source_col:", src_col)
    print("by chain (top 15):")
    print(df["chain"].value_counts().head(15).to_string())

    # Resume support
    done_keys = set()
    if progress_path.exists():
        try:
            pold = pd.read_csv(progress_path)
            if {"chain", "address", "ok"}.issubset(set(pold.columns)):
                done = pold[pold["ok"].isin([0, 1])][["chain", "address"]].astype(str)
                done_keys = set(zip(done["chain"], done["address"]))
                print(f"↩️  resume: found {len(done_keys)} already-attempted in {progress_path}")
        except Exception:
            pass

    rows_out = []
    t0 = time.time()

    for i, r in df.iterrows():
        key = (r["chain"], r["address"])
        if key in done_keys:
            continue

        cid = f"{r['chain']}_{r['address']}"
        name = safe_slug(r.get("contract_name", "") or cid)
        sol_path = sol_dir / f"{name}__{cid}.sol"
        out_json_path = json_dir / f"{cid}.json"

        # Write source
        sol_path.write_text(r[src_col], encoding="utf-8", errors="ignore")

        cmd = ["slither", str(sol_path), "--json", "-", "--disable-color"]
        t_start = time.time()
        try:
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            elapsed = time.time() - t_start

            ok = 0
            n_detectors: Optional[int] = None
            stderr = (p.stderr or "").strip()

            if p.returncode == 0 and (p.stdout or "").lstrip().startswith("{"):
                ok = 1
                try:
                    j = json.loads(p.stdout)
                    # try to summarize detectors count
                    dets = j.get("results", {}).get("detectors", None)
                    if isinstance(dets, list):
                        n_detectors = len(dets)
                    out_json_path.write_text(json.dumps(j), encoding="utf-8")
                except Exception:
                    ok = 0

            rows_out.append({
                "chain": r["chain"],
                "address": r["address"],
                "contract_name": r.get("contract_name", ""),
                "ok": ok,
                "n_detectors": n_detectors,
                "elapsed_sec": round(elapsed, 3),
                "json_path": str(out_json_path) if ok == 1 else "",
                "err_tail": stderr[-400:] if stderr else "",
            })

        except subprocess.TimeoutExpired:
            rows_out.append({
                "chain": r["chain"],
                "address": r["address"],
                "contract_name": r.get("contract_name", ""),
                "ok": 0,
                "n_detectors": None,
                "elapsed_sec": 300.0,
                "json_path": "",
                "err_tail": "TIMEOUT",
            })
        except Exception as e:
            rows_out.append({
                "chain": r["chain"],
                "address": r["address"],
                "contract_name": r.get("contract_name", ""),
                "ok": 0,
                "n_detectors": None,
                "elapsed_sec": 0.0,
                "json_path": "",
                "err_tail": f"EXCEPTION: {type(e).__name__}: {e}",
            })

        # checkpoint
        if len(rows_out) >= batch_size:
            _flush(progress_path, rows_out)
            rows_out.clear()
            spent = time.time() - t0
            print(f"checked ~{i+1} | wrote ckpt -> {progress_path} | elapsed {spent/60:.1f} min")

    if rows_out:
        _flush(progress_path, rows_out)
        rows_out.clear()

    print("✅ done. progress:", progress_path)


def _flush(progress_path: Path, rows_out: list[dict]) -> None:
    df_new = pd.DataFrame(rows_out)
    if progress_path.exists():
        df_old = pd.read_csv(progress_path, low_memory=False)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
        # keep last attempt per (chain,address)
        df_all["chain"] = df_all["chain"].astype(str)
        df_all["address"] = df_all["address"].astype(str)
        df_all = df_all.drop_duplicates(subset=["chain", "address"], keep="last")
    else:
        df_all = df_new
    df_all.to_csv(progress_path, index=False)


if __name__ == "__main__":
    main()