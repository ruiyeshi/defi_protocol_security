import pandas as pd

base = pd.read_csv("data_raw/contracts/verified_contracts.csv")
resolved = pd.read_csv("data_raw/contracts/resolved_addresses.csv")

print(f"🧩 Merging {len(resolved)} resolved entries into verified_contracts.csv...")

merged = pd.merge(base, resolved, on="protocol_name", how="left")
merged.to_csv("data_raw/contracts/verified_contracts_merged.csv", index=False)

print(f"✅ Saved merged contracts → data_raw/contracts/verified_contracts_merged.csv ({len(merged)} rows)")
