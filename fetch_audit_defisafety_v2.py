import requests
import pandas as pd
from datetime import datetime

print("🌐 Fetching audit metadata from DeFiSafety API...")

# Load your existing verified protocols list
protocols = pd.read_csv("data_raw/contracts/verified_contracts.csv")["protocol_name"].tolist()
records = []

for name in protocols:
    url = f"https://api.defisafety.com/public/project/{name.lower().replace(' ', '-')}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if "score" in data:
                records.append({
                    "protocol_name": name,
                    "audit_firm": "DeFiSafety",
                    "audit_score": data.get("score"),
                    "audit_date": data.get("report_date"),
                    "audit_link": data.get("report_link", f"https://defisafety.com/projects/{name.lower()}"),
                    "source": "DeFiSafety"
                })
                print(f"✅ Found DeFiSafety audit for {name}")
            else:
                print(f"⚠️ No score data for {name}")
        else:
            print(f"❌ Failed ({r.status_code}) for {name}")
    except Exception as e:
        print(f"⚠️ Error fetching {name}: {e}")

df = pd.DataFrame(records)
df.to_csv("data_raw/contracts/audit_metadata_defisafety.csv", index=False)
print(f"\n✅ Saved DeFiSafety audit metadata for {len(df)} protocols.")