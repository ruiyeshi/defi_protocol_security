#!/usr/bin/env python3
"""
Fetch CertiK audit metadata (mirror + fallback scraper).
Output: data_raw/contracts/audit_metadata_certik.csv
"""

import os, sys, time, requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from dotenv import load_dotenv
from tqdm import tqdm

# ────────────────────────────────────────────────
# Auto-detect project root
# ────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve()
while ROOT_DIR.name != "defi_protocol_security" and ROOT_DIR.parent != ROOT_DIR:
    ROOT_DIR = ROOT_DIR.parent
sys.path.append(str(ROOT_DIR))

# Load .env and ensure folders
load_dotenv(dotenv_path=ROOT_DIR / ".env")

DATA_RAW = ROOT_DIR / "data_raw" / "contracts"
DATA_RAW.mkdir(parents=True, exist_ok=True)
OUT_CSV = DATA_RAW / "audit_metadata_certik.csv"

# ────────────────────────────────────────────────
# Helper: fetch via API (mirror)
# ────────────────────────────────────────────────
def fetch_certik_project(name: str):
    url = f"https://api.certik.com/v1/projects?name={name}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 403:
            raise PermissionError("403 Forbidden – API blocked")
        r.raise_for_status()
        data = r.json()
        if data and "data" in data and data["data"]:
            p = data["data"][0]
            return {
                "protocol_name": name,
                "audit_firm": "CertiK",
                "audit_score": p.get("securityScore", None),
                "audit_link": f"https://www.certik.com/projects/{p.get('name','')}",
                "audit_date": p.get("lastAuditTime", None),
            }
    except Exception as e:
        print(f"⚠️ API error for {name}: {e}")
    return None

# ────────────────────────────────────────────────
# Helper: fallback HTML scraper
# ────────────────────────────────────────────────
def scrape_certik_project(name: str):
    slug = name.lower().replace(" ", "-")
    url = f"https://www.certik.com/projects/{slug}"
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            raise ValueError(f"Status {r.status_code}")
        soup = BeautifulSoup(r.text, "html.parser")

        # Score element
        score_elem = soup.find(text=lambda t: "Security Score" in t)
        score = score_elem.strip() if score_elem else None

        # Date pattern
        date = None
        for tag in soup.find_all(string=True):
            if "Audit" in tag and any(x in tag for x in ["202", "20"]):
                date = tag.strip()
                break

        return {
            "protocol_name": name,
            "audit_firm": "CertiK (Scraped)",
            "audit_score": score,
            "audit_link": url,
            "audit_date": date,
        }
    except Exception as e:
        print(f"⚠️ Scraper failed for {name}: {e}")
        return None

# ────────────────────────────────────────────────
# Main logic
# ────────────────────────────────────────────────
def main():
    seed_csv = DATA_RAW / "defillama_top_protocols.csv"
    if not seed_csv.exists():
        print(f"❌ Seed file missing: {seed_csv}")
        return

    df = pd.read_csv(seed_csv)
    names = df["name"].dropna().unique().tolist()
    audit_rows = []

    print(f"🔍 Fetching CertiK audits for {len(names)} protocols...")

    for name in tqdm(names):
        data = fetch_certik_project(name)
        if not data:
            data = scrape_certik_project(name)
        if data:
            audit_rows.append(data)
        time.sleep(0.5)  # Throttle requests

    if audit_rows:
        out_df = pd.DataFrame(audit_rows)
        out_df.to_csv(OUT_CSV, index=False)
        print(f"✅ Saved {len(out_df)} rows → {OUT_CSV}")
    else:
        print("⚠️ No audit data found.")

# ────────────────────────────────────────────────
if __name__ == "__main__":
    main()