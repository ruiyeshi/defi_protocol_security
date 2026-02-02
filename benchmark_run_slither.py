#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, time, subprocess
from pathlib import Path
import pandas as pd

INDEX = Path(os.getenv("INDEX", "data_benchmark/contracts_index.csv"))
OUTDIR = Path(os.getenv("OUTDIR", "outputs/slither_benchmark"))
OUTDIR.mkdir(parents=True, exist_ok=True)

PROGRESS = OUTDIR / "slither_benchmark_progress.csv"
JSONDIR = OUTDIR / "json"
JSONDIR.mkdir(parents=True, exist_ok=True)

MAX_FILES = int(os.getenv("MAX_FILES", "0"))  # 0 = all
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

def tail(s: str, n: int = 400) -> str:
    s = (s or "").strip().replace("\n", " ")
    return s[-n:] if len(s) > n else s

def main():
    if not INDEX.exists():
        raise SystemExit(f"Missing benchmark index: {INDEX}")

    df = pd.read_csv(INDEX, low_memory=False)
    # Expect at least a 'path' column. If yours is different, rename here.
    if "path" not in df.columns:
        raise SystemExit(f"INDEX must have column 'path'. cols={df.columns.tolist()}")

    df["path"] = df["path"].astype(str)
    df = df[df["path"].str.endswith(".sol")].copy()

    if MAX_FILES > 0:
        df = df.head(MAX_FILES).copy()

    done = set()
    rows = []
    if PROGRESS.exists():
        prev = pd.read_csv(PROGRESS, low_memory=False)
        rows = prev.to_dict("records")
        if "path" in prev.columns:
            done = set(prev["path"].astype(str).tolist())

    n = len(df)
    for i, r in df.iterrows():
        path = str(r["path"])
        if path in done:
            continue

        p = Path(path)
        cid = p.stem
        jout = JSONDIR / f"{cid}.json"

        t0 = time.time()
        try:
            cmd = ["slither", str(p), "--json", str(jout), "--disable-color"]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            ok = 1 if proc.returncode == 0 and jout.exists() else 0
            err_tail = tail(proc.stderr)
            elapsed = round(time.time() - t0, 3)

            rows.append({
                "path": path,
                "ok": ok,
                "returncode": proc.returncode,
                "elapsed_sec": elapsed,
                "json_path": str(jout) if ok else "",
                "err_tail": err_tail,
            })
        except Exception as e:
            rows.append({
                "path": path,
                "ok": 0,
                "returncode": -999,
                "elapsed_sec": round(time.time() - t0, 3),
                "json_path": "",
                "err_tail": tail(str(e)),
            })

        if len(rows) % BATCH_SIZE == 0:
            pd.DataFrame(rows).to_csv(PROGRESS, index=False)
            print(f"checked {len(rows)}/{n} | wrote ckpt {PROGRESS}")

    pd.DataFrame(rows).to_csv(PROGRESS, index=False)
    print(f"✅ done. wrote {PROGRESS} rows={len(rows)}")

if __name__ == "__main__":
    main()