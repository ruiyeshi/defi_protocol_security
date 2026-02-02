#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import json
import subprocess
import time
from pathlib import Path
import pandas as pd


def run_slither(sol_path: str, out_json: Path, timeout_sec: int = 120) -> tuple[bool, str]:
    """
    Runs slither on a single Solidity file.
    Returns (ok, msg). Writes slither JSON to out_json if ok.
    """
    out_json.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "slither",
        sol_path,
        "--json",
        str(out_json),
        "--exclude-dependencies",
    ]

    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
        )
        if p.returncode != 0:
            msg = (p.stderr or p.stdout or "").strip()[:4000]
            return False, msg or f"slither failed rc={p.returncode}"
        return True, ""
    except subprocess.TimeoutExpired:
        return False, f"timeout>{timeout_sec}s"
    except FileNotFoundError:
        return False, "slither not found (pip install slither-analyzer)"
    except Exception as e:
        return False, f"error: {e}"


def summarize_slither_json(jpath: Path) -> dict:
    """
    Minimal summary: total findings + by check + by impact
    Slither JSON schema varies; this is defensive.
    """
    try:
        j = json.loads(jpath.read_text())
    except Exception:
        return {"slither_total": 0}

    detectors = j.get("results", {}).get("detectors", []) or []
    total = len(detectors)

    by_check = {}
    by_impact = {}
    for d in detectors:
        check = str(d.get("check", "") or "unknown")
        impact = str(d.get("impact", "") or "unknown")
        by_check[check] = by_check.get(check, 0) + 1
        by_impact[impact] = by_impact.get(impact, 0) + 1

    out = {"slither_total": total}
    # keep top few to avoid huge wide CSV
    for k, v in sorted(by_impact.items(), key=lambda x: -x[1])[:10]:
        out[f"slither_impact__{k}"] = v
    for k, v in sorted(by_check.items(), key=lambda x: -x[1])[:15]:
        out[f"slither_check__{k}"] = v
    return out


def main():
    BENCH_ROOT = Path("data_benchmark/messiq")
    INDEX = BENCH_ROOT / "contracts_index.csv"
    OUT_DIR = BENCH_ROOT / "slither_out"
    OUT_CSV = BENCH_ROOT / "slither_results.csv"
    CKPT = BENCH_ROOT / "ckpt_slither.json"

    MAX_N = int((__import__("os").environ.get("SLITHER_MAX_N") or "0").strip())  # 0 = all
    SLEEP_SEC = float((__import__("os").environ.get("SLITHER_SLEEP_SEC") or "0.05").strip())
    TIMEOUT_SEC = int((__import__("os").environ.get("SLITHER_TIMEOUT_SEC") or "120").strip())

    if not INDEX.exists():
        raise SystemExit(f"Missing index: {INDEX} (run import_benchmark_messiq_index.py first)")

    idx = pd.read_csv(INDEX, low_memory=False)
    if "abspath" not in idx.columns:
        raise SystemExit("contracts_index.csv must have 'abspath' column")

    # checkpoint
    done = set()
    if CKPT.exists():
        try:
            done = set(json.loads(CKPT.read_text()).get("done", []))
        except Exception:
            done = set()

    rows = []
    if OUT_CSV.exists():
        # allow resume: keep previous rows
        try:
            prev = pd.read_csv(OUT_CSV, low_memory=False)
            rows = prev.to_dict("records")
        except Exception:
            rows = []

    total = len(idx)
    if MAX_N > 0:
        idx = idx.head(MAX_N).copy()

    for i, r in idx.iterrows():
        key = f"{r.get('dataset','')}/{r.get('contract_id','')}/{r.get('sha1','')}"
        if key in done:
            continue

        sol = str(r["abspath"])
        out_json = OUT_DIR / f"{r.get('dataset','bench')}_{r.get('contract_id','contract')}_{r.get('sha1','')}.json"

        ok, msg = run_slither(sol, out_json, timeout_sec=TIMEOUT_SEC)

        out_row = dict(r)
        out_row["slither_ok"] = int(ok)
        out_row["slither_err"] = "" if ok else msg

        if ok and out_json.exists():
            out_row.update(summarize_slither_json(out_json))
        else:
            out_row["slither_total"] = 0

        rows.append(out_row)
        done.add(key)

        if len(done) % 50 == 0:
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(rows).to_csv(OUT_CSV, index=False)
            CKPT.write_text(json.dumps({"done": sorted(done)}, indent=2))
            print(f"checked={len(done)} / {len(idx)} | ok={sum(x.get('slither_ok',0) for x in rows)}")

        time.sleep(SLEEP_SEC)

    pd.DataFrame(rows).to_csv(OUT_CSV, index=False)
    CKPT.write_text(json.dumps({"done": sorted(done)}, indent=2))
    print(f"✅ wrote {OUT_CSV} rows={len(rows)} | ok={sum(x.get('slither_ok',0) for x in rows)}")


if __name__ == "__main__":
    main()