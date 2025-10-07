import os
import json
import csv
import subprocess
import pandas as pd
from datetime import datetime
from pathlib import Path

# ========= CONFIG ==========
BASE_DIR = Path("/Users/ruiyeshi/defi_protocol_security")
SRC_DIR = BASE_DIR / "data_raw" / "contracts" / "source_codes"
OUT_JSON = BASE_DIR / "data_raw" / "slither_raw" / "slither_detailed.json"
OUT_CSV = BASE_DIR / "data_raw" / "slither_raw" / "slither_vulnerabilities_detailed.csv"
INPUT_CSV = BASE_DIR / "data_raw" / "contracts" / "verified_contracts_expanded.csv"
# ===========================

# ensure directories exist
OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
SRC_DIR.mkdir(parents=True, exist_ok=True)

# DASP10/SWC mapping (simplified)
CATEGORY_MAP = {
    "reentrancy-eth": "Reentrancy",
    "uninitialized-state": "Uninitialized Variable",
    "shadowing-local": "Shadowing",
    "tx-origin": "Authentication Flaw",
    "controlled-delegatecall": "Delegatecall Injection",
    "integer-overflow": "Arithmetic Issue",
    "arbitrary-send": "Access Control",
    "unchecked-call": "Unchecked External Call",
    "unprotected-upgrade": "Access Control",
    "unprotected-selfdestruct": "Denial of Service"
}

def run_slither_on_contract(contract_path):
    """Run slither on a single Solidity file and return parsed JSON findings"""
    try:
        result = subprocess.run(
            ["slither", contract_path, "--json", "-"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=120
        )
        data = json.loads(result.stdout) if result.stdout.strip() else None
        return data
    except Exception as e:
        print(f"⚠️ Error running Slither on {contract_path}: {e}")
        return None

def parse_slither_output(protocol, address, slither_json):
    """Extract detector-level details"""
    rows = []
    if not slither_json or "results" not in slither_json:
        return rows

    for issue in slither_json["results"].get("detectors", []):
        detector = issue.get("check", "")
        impact = issue.get("impact", "")
        confidence = issue.get("confidence", "")
        elements = ", ".join([el.get("name", "") for el in issue.get("elements", [])])
        source_mapping = ", ".join(
            [f"{el.get('source_mapping', {}).get('filename', '')}:{el.get('source_mapping', {}).get('lines', '')}"
             for el in issue.get("elements", [])]
        )
        category = CATEGORY_MAP.get(detector, "Other")

        rows.append({
            "protocol_name": protocol,
            "contract_address": address,
            "detector": detector,
            "category": category,
            "impact": impact,
            "confidence": confidence,
            "elements": elements,
            "source_mapping": source_mapping,
            "timestamp": datetime.utcnow().isoformat()
        })
    return rows

def main():
    print("🚀 Running detailed Slither batch analysis...")
    df = pd.read_csv(INPUT_CSV)

    if "address" not in df.columns:
        raise ValueError("Missing 'address' column in verified_contracts_expanded.csv")

    all_findings = []
    n_total = len(df)

    for i, row in df.iterrows():
        protocol = row.get("slug", "")
        address = row["address"]

        contract_path = SRC_DIR / f"{address}.sol"
        if not contract_path.exists():
            print(f"⚠️ Missing source: {contract_path}")
            continue

        print(f"🔍 [{i+1}/{n_total}] Analyzing {protocol} ({address[:8]}...)")
        slither_json = run_slither_on_contract(str(contract_path))
        findings = parse_slither_output(protocol, address, slither_json)
        all_findings.extend(findings)

    print(f"✅ Finished analysis for {len(all_findings)} findings.")

    # Save outputs
    with open(OUT_JSON, "w") as f:
        json.dump(all_findings, f, indent=2)

    pd.DataFrame(all_findings).to_csv(OUT_CSV, index=False)
    print(f"🧾 Saved detailed Slither JSON → {OUT_JSON}")
    print(f"📊 Saved detailed CSV summary → {OUT_CSV}")

if __name__ == "__main__":
    main()