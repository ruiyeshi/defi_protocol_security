import requests
import pandas as pd
import time
from datetime import datetime
from pathlib import Path

OUT = Path("data_raw/audit_metadata_certik.csv")
BASE_URL = "https://api.certik.com/v1/projects"

def fetch_certik_projects():
    all_data = []
    offset = 0
    limit = 100

    while True:
        url = f"{BASE_URL}?offset={offset}&limit={limit}"
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            print(f"⚠️ Error {r.status_code} at offset {offset}")
            break
        data = r.json().get("data", [])
        if not data:
            break

        for d in data:
            name = d.get("name", "").lower()
            slug = d.get("slug", "")
            score = d.get("score", None)
            audit_time = d.get("auditTime", None)
            date = (
                datetime.utcfromtimestamp(audit_time / 1000).strftime("%Y-%m-%d")
                if audit_time
                else None
            )
            audit_link = f"https://www.certik.com/projects/{slug}"
            all_data.append(
                {
                    "protocol_name": name,
                    "audit_firm": "CertiK",
                    "audit_score": score,
                    "audit_link": audit_link,
                    "audit_date": date,
                }
            )

        offset += limit
        print(f"✅ Fetched {len(all_data)} so far...")
        time.sleep(0.5)

    df = pd.DataFrame(all_data)
    df.to_csv(OUT, index=False)
    print(f"💾 Saved {len(df)} rows → {OUT}")

if __name__ == "__main__":
    fetch_certik_projects()