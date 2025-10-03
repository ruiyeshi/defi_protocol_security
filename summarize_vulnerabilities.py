 #!/usr/bin/env python3
import os
import pandas as pd

# === PATHS ===
VULN_CSV = "data_raw/contracts/slither_vulnerabilities.csv"
AUDIT_CSV = "data_raw/contracts/audit_metadata_full.csv"
PROXY_CSV = "data_final/contracts/proxy_patterns.csv"
EXPLOIT_CSV = "data_final/contracts/exploit_metadata.csv"
OUT_CSV = "data_final/contracts/defi_security_summary.csv"

# === LOAD DATASETS ===
print("📂 Loading datasets...")

vuln = pd.read_csv(VULN_CSV) if os.path.exists(VULN_CSV) else pd.DataFrame()
audit = pd.read_csv(AUDIT_CSV) if os.path.exists(AUDIT_CSV) else pd.DataFrame()
proxy = pd.read_csv(PROXY_CSV) if os.path.exists(PROXY_CSV) else pd.DataFrame()
exploit = pd.read_csv(EXPLOIT_CSV) if os.path.exists(EXPLOIT_CSV) else pd.DataFrame()

print(f"🧩 Loaded: {len(vuln)} vulnerabilities, {len(audit)} audits, {len(proxy)} proxies, {len(exploit)} exploits")

# === CLEAN & NORMALIZE ===
for df in [vuln, audit, proxy, exploit]:
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.lower().str.strip()

# Clean proxy contract names
if "contract_file" in proxy.columns:
    proxy["contract_file_clean"] = proxy["contract_file"].str.replace(".sol", "", regex=False)
else:
    proxy["contract_file_clean"] = None

# === MERGE DATASETS ===
print("🔗 Merging datasets...")

# Step 1: Merge vulnerability & audit metadata
if not vuln.empty and not audit.empty:
    merged = pd.merge(vuln, audit, on="protocol_name", how="left")
else:
    merged = vuln.copy()

# Step 2: Merge proxy patterns
if not proxy.empty:
    merged = pd.merge(merged, proxy, left_on="protocol_name", right_on="contract_file_clean", how="left")

# Step 3: Merge exploit metadata
if not exploit.empty:
    merged = pd.merge(merged, exploit, on="protocol_name", how="left")

# === FINAL CLEANUP ===
merged = merged.drop_duplicates(subset=["protocol_name"], keep="first")
print(f"✅ Final merged dataset contains {len(merged)} unique protocols")

# === SAVE OUTPUT ===
os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
merged.to_csv(OUT_CSV, index=False)
print(f"💾 Saved summary dataset → {OUT_CSV}")
