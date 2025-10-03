import pandas as pd

# Load both datasets
vuln = pd.read_csv("data_raw/contracts/slither_vulnerabilities.csv")
audit = pd.read_csv("data_raw/contracts/audit_metadata_full.csv")

# Normalize protocol names for matching
vuln["protocol_name"] = vuln["protocol_name"].str.lower().str.strip()
audit["protocol_name"] = audit["protocol_name"].str.lower().str.strip()

# Merge on protocol_name
merged = pd.merge(audit, vuln, on="protocol_name", how="left")

# Save master file
merged.to_csv("data_raw/contracts/defi_security_master_final.csv", index=False)

print(f"✅ Final merged dataset saved → data_raw/contracts/defi_security_master_final.csv ({len(merged)} rows)")