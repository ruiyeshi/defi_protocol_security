#!/usr/bin/env python3
"""
Run Slither + Mythril on MessiQ benchmark contracts (benchmark-only; no mixing with your DeFi dataset).

INPUTS:
  data_external/benchmarks/messiq/contracts_clean.csv
  data_external/benchmarks/messiq/contracts/*.sol

OUTPUTS:
  data_external/benchmarks/messiq/tool_findings_long.csv
  data_external/benchmarks/messiq/tool_contract_agg.csv
  data_external/benchmarks/messiq/tool_metrics.json

Usage:
  python benchmark_run_tools_on_messiq.py --limit 50
  python benchmark_run_tools_on_messiq.py --limit 500 --timeout 180
"""

from __future__ import annotations
from pathlib import Path
import argparse
import json
import shutil
import subprocess
import time

import pandas as pd


ROOT = Path(".")
BENCH_DIR = ROOT / "data_external" / "benchmarks" / "messiq"
IN_CONTRACTS = BENCH_DIR / "contracts_clean.csv"
SOL_DIR = BENCH_DIR / "contracts"

OUT_LONG = BENCH_DIR / "tool_findings_long.csv"
OUT_AGG = BENCH_DIR / "tool_contract_agg.csv"
OUT_METRICS = BENCH_DIR / "tool_metrics.json"


def have(cmd: str) -> bool:
    return shutil.which(cmd) is not None


def run_cmd(cmd: list[str], timeout_s: int) -> tuple[int, str, str]:
    p = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_s,
    )
    return p.returncode, p.stdout, p.stderr


def parse_slither_json(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return []
    dets = data.get("results", {}).get("detectors", []) if isinstance(data, dict) else []
    rows = []
    for d in dets:
        rows.append(
            {
                "tool": "slither",
                "finding_id": d.get("check"),
                "severity": d.get("impact") or d.get("severity"),
                "confidence": d.get("confidence"),
                "description": (d.get("description") or "")[:500],
            }
        )
    return rows


def parse_mythril_json(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return []
    issues = data.get("issues", []) if isinstance(data, dict) else []
    rows = []
    for it in issues:
        rows.append(
            {
                "tool": "mythril",
                "finding_id": it.get("swc-id") or it.get("swc_id") or it.get("title"),
                "severity": it.get("severity"),
                "confidence": it.get("confidence"),
                "description": (it.get("description") or it.get("title") or "")[:500],
            }
        )
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="Limit number of contracts (0 = all)")
    ap.add_argument("--timeout", type=int, default=120, help="Per-tool timeout seconds")
    args = ap.parse_args()

    if not IN_CONTRACTS.exists():
        raise SystemExit(f"Missing: {IN_CONTRACTS}. Run import_benchmark_messiq_sc4label.py first.")

    if not SOL_DIR.exists():
        raise SystemExit(f"Missing solidity directory: {SOL_DIR}. Run import script without --no-write-sol.")

    slither_ok = have("slither")
    myth_ok = have("myth")

    if not slither_ok and not myth_ok:
        raise SystemExit("Neither slither nor myth (mythril) found in PATH. Install at least one tool.")

    df = pd.read_csv(IN_CONTRACTS)
    if args.limit and args.limit > 0:
        df = df.head(args.limit).copy()

    findings = []
    started = time.time()

    for i, row in df.iterrows():
        bid = row["benchmark_id"]
        sol_path = SOL_DIR / f"{bid}.sol"
        if not sol_path.exists():
            # No garbage: skip if file missing
            continue

        # --- Slither ---
        if slither_ok:
            out_json = BENCH_DIR / f"_tmp_slither_{bid}.json"
            try:
                rc, _, _ = run_cmd(["slither", str(sol_path), "--json", str(out_json)], timeout_s=args.timeout)
                if rc == 0:
                    for r in parse_slither_json(out_json):
                        findings.append(
                            {
                                "benchmark_id": bid,
                                "filename": row.get("filename"),
                                "label_encoded": row.get("label_encoded"),
                                "label": row.get("label"),
                                **r,
                            }
                        )
            except subprocess.TimeoutExpired:
                pass
            finally:
                if out_json.exists():
                    out_json.unlink(missing_ok=True)

        # --- Mythril ---
        if myth_ok:
            out_json = BENCH_DIR / f"_tmp_myth_{bid}.json"
            try:
                # Mythril CLI varies; this works for many installs:
                rc, _, _ = run_cmd(["myth", "analyze", str(sol_path), "-o", "json"], timeout_s=args.timeout)
                if rc == 0:
                    # myth -o json prints to stdout; but we used stdout capture above.
                    # Re-run to capture stdout reliably:
                    rc2, out2, _ = run_cmd(["myth", "analyze", str(sol_path), "-o", "json"], timeout_s=args.timeout)
                    if rc2 == 0 and out2.strip().startswith("{"):
                        out_json.write_text(out2, encoding="utf-8")
                        for r in parse_mythril_json(out_json):
                            findings.append(
                                {
                                    "benchmark_id": bid,
                                    "filename": row.get("filename"),
                                    "label_encoded": row.get("label_encoded"),
                                    "label": row.get("label"),
                                    **r,
                                }
                            )
            except subprocess.TimeoutExpired:
                pass
            finally:
                if out_json.exists():
                    out_json.unlink(missing_ok=True)

        if (i + 1) % 50 == 0:
            print(f"… processed {i+1}/{len(df)} contracts | findings so far: {len(findings)}")

    long_df = pd.DataFrame(findings)
    if len(long_df) == 0:
        # Write empty but well-formed outputs (no crash)
        long_df = pd.DataFrame(
            columns=[
                "benchmark_id",
                "filename",
                "label_encoded",
                "label",
                "tool",
                "finding_id",
                "severity",
                "confidence",
                "description",
            ]
        )

    long_df.to_csv(OUT_LONG, index=False)

    # Contract-level aggregate
    if len(long_df) > 0:
        agg = (
            long_df.groupby(["benchmark_id", "label_encoded"], dropna=False)
            .agg(
                slither_total=("tool", lambda s: int((s == "slither").sum())),
                mythril_total=("tool", lambda s: int((s == "mythril").sum())),
                any_finding=("tool", lambda s: int(len(s) > 0)),
            )
            .reset_index()
        )
    else:
        agg = pd.DataFrame(columns=["benchmark_id", "label_encoded", "slither_total", "mythril_total", "any_finding"])

    # overlap (>=1 finding from both tools)
    if len(agg) > 0:
        agg["tool_overlap_flag"] = ((agg["slither_total"] > 0) & (agg["mythril_total"] > 0)).astype(int)

    agg.to_csv(OUT_AGG, index=False)

    metrics = {
        "contracts_input": int(len(df)),
        "contracts_with_sol_files": int(sum((SOL_DIR / (bid + ".sol")).exists() for bid in df["benchmark_id"].astype(str))),
        "slither_available": bool(slither_ok),
        "mythril_available": bool(myth_ok),
        "findings_rows": int(len(long_df)),
        "elapsed_seconds": round(time.time() - started, 2),
        "outputs": {
            "tool_findings_long": str(OUT_LONG),
            "tool_contract_agg": str(OUT_AGG),
        },
    }
    OUT_METRICS.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    print(f"✅ Wrote: {OUT_LONG}  rows={len(long_df)}")
    print(f"✅ Wrote: {OUT_AGG}   rows={len(agg)}")
    print(f"✅ Wrote: {OUT_METRICS}")


if __name__ == "__main__":
    main()