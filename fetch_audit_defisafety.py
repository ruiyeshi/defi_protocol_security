import requests
import pandas as pd
import os
import re

# Load CertiK audits as baseline
certik_path = "data_raw/contracts/audit_metadata_certik.csv"
df_certik = pd.read_csv(certik_path)

# Use same protocols
protocols = df_certik["protocol_name"].tolist()

audit_rows = []

for name in protocols:
    name_slug = name.lower().replace(" ", "-")
    url = f"https://defisafety.com/projects/{name_slug}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200 and "Overall Score" in r.text:
            score_match = re.search(r"Overall Score[:\s]+(\d+)", r.text)
            score = int(score_match.group(1)) if score_match else None
            audit_rows.append({
                "protocol_name": name,
                "audit_firm": "DeFiSafety",
                "audit_score": score,
                "audit_link": url,
                "audit_date": None
            })
            print(f"✅ Found DeFiSafety entry for {name}")
        else:
            print(f"⚠️ No DeFiSafety entry for {name}")
    except Exception as e:
        print(f"❌ Error fetching {name}: {e}")

# Convert to DataFrame
df_defisafety = pd.DataFrame(audit_rows)

# Merge with CertiK results
df_full = pd.concat([df_certik, df_defisafety], ignore_index=True)
os.makedirs("data_raw/contracts", exist_ok=True)
df_full.to_csv("data_raw/contracts/audit_metadata_full.csv", index=False)
print("✅ Saved merged audit metadata to audit_metadata_full.csv")