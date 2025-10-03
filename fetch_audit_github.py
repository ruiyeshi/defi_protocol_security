# fetch_audit_github.py
import os, requests, pandas as pd, re
from urllib.parse import quote

df = pd.read_csv("data_raw/contracts/verified_contracts.csv")
out = []

print("🔍 Searching GitHub repositories for audit folders...")

headers = {"Accept": "application/vnd.github.v3+json"}
token = os.getenv("GITHUB_TOKEN")
if token:
    headers["Authorization"] = f"token {token}"

for _, row in df.iterrows():
    repo = row.get("github", "")
    if pd.isna(repo) or "github.com" not in repo:
        continue
    name = row["protocol_name"]
    parts = repo.split("github.com/")[-1].split("/")
    if len(parts) < 2:
        continue
    owner, project = parts[0], parts[1]
    url = f"https://api.github.com/repos/{owner}/{project}/contents"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            items = r.json()
            for i in items:
                if re.search(r"(audit|security|report|pdf)", i["name"], re.IGNORECASE):
                    out.append({"protocol_name": name, "audit_link": i["html_url"], "source": "GitHub"})
                    print(f"✅ Found audit link for {name}")
        else:
            print(f"⚠️ {name}: {r.status_code}")
    except Exception as e:
        print(f"⚠️ {name}: {e}")

pd.DataFrame(out).to_csv("data_raw/contracts/audit_metadata_github.csv", index=False)
print(f"\n✨ Saved GitHub audit metadata ({len(out)} entries)")
