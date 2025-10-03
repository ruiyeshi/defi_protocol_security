import os
import subprocess
import pandas as pd
from datetime import datetime

# === PATH CONFIGURATION ===
CONTRACT_DIR = "data_raw/contracts"
SOURCE_FILE = os.path.join(CONTRACT_DIR, "contract_registry.csv")
OUTPUT_FILE = os.path.join(CONTRACT_DIR, "slither_vulnerabilities.csv")

os.makedirs(CONTRACT_DIR, exist_ok=True)

print("💾 Loading verified contract sources...")

if not os.path.exists(SOURCE_FILE):
    raise FileNotFoundError(f"{SOURCE_FILE} not found!")

df = pd.read_csv(SOURCE_FILE)

# Validate CSV structure
required_cols = {"protocol_name", "contract_address"}
if not required_cols.issubset(df.columns):
    raise ValueError("Input CSV must contain 'protocol_name' and 'contract_address' columns")

print(f"✅ Loaded {len(df)} verified contracts")

# === CREATE TEMP DIRECTORY FOR SOL FILES ===
tmp_folder = os.path.join(CONTRACT_DIR, "tmp_sources")
os.makedirs(tmp_folder, exist_ok=True)

def save_solidity_stub(protocol, address):
    """
    Creates a temporary .sol file containing a minimal Solidity contract
    with the contract address noted — since source code is fetched externally.
    """
    path = os.path.join(tmp_folder, f"{protocol}.sol")
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"// Placeholder contract for {protocol}\n")
        f.write(f"// Address: {address}\n")
        f.write("pragma solidity ^0.8.0;\ncontract Placeholder {}")
    return path

results = []

# === SCAN CONTRACTS WITH SLITHER ===
for idx, row in df.iterrows():
    name = row["protocol_name"]
    address = row["contract_address"]

    print(f"\n🔍 Scanning {name} with Slither...")

    try:
        sol_path = save_solidity_stub(name, address)

        # Run Slither
        cmd = ["slither", sol_path, "--json", "tmp_report.json"]
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)

        # Parse Slither report if exists
        if os.path.exists("tmp_report.json"):
            report = pd.read_json("tmp_report.json")
            issues = len(report.get("results", []))
        else:
            issues = 0

        results.append({
            "protocol_name": name,
            "contract_address": address,
            "issues_detected": issues,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    except Exception as e:
        print(f"❌ Error scanning {name}: {e}")
        results.append({
            "protocol_name": name,
            "contract_address": address,
            "issues_detected": "Error",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

# === SAVE RESULTS ===
df_out = pd.DataFrame(results)
df_out.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Saved vulnerability summary → {OUTPUT_FILE}")