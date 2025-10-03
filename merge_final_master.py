import pandas as pd

# Load datasets
contracts = pd.read_csv("data_raw/contracts/verified_contracts.csv")
audits = pd.read_csv("data_raw/contracts/audit_metadata_certik.csv")
exploits = pd.read_csv("data_raw/contracts/exploit_metadata_final.csv")

print("🔄 Merging verified contracts, audits, and exploits...")

# Normalize column names
for df in [contracts, audits, exploits]:
    df.columns = df.columns.str.lower()
    if "protocol_name" in df.columns:
        df["protocol_name"] = df["protocol_name"].str.lower().str.strip()

# Merge all
merged = (
    contracts
    .merge(audits, on="protocol_name", how="left", suffixes=("", "_audit"))
    .merge(exploits, on="protocol_name", how="left", suffixes=("", "_exploit"))
)

# Save master file
merged.to_csv("data_raw/contracts/defi_security_master.csv", index=False)

print(f"✅ Final master dataset saved with {len(merged)} protocols")
print("📊 Columns:", list(merged.columns))
print(merged.head(10))
