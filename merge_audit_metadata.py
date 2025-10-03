import pandas as pd

# Load available audit datasets (ignore missing)
try:
    certik = pd.read_csv("data_raw/contracts/audit_metadata_certik.csv")
except Exception:
    certik = pd.DataFrame()

try:
    defisafety = pd.read_csv("data_raw/contracts/audit_metadata_defisafety.csv")
except Exception:
    defisafety = pd.DataFrame()

try:
    github = pd.read_csv("data_raw/contracts/audit_metadata_github.csv")
except Exception:
    github = pd.DataFrame()

# Merge all sources
df_full = pd.concat([certik, defisafety, github], ignore_index=True)
df_full.drop_duplicates(subset=["protocol_name", "audit_link"], inplace=True)

# Save merged audit metadata
output_path = "data_raw/contracts/audit_metadata_full.csv"
df_full.to_csv(output_path, index=False)

print(f"✅ Combined audit metadata saved → {output_path} ({len(df_full)} entries)")
