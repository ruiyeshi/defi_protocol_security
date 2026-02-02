#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape Code4rena contest repos from GitHub (org: code-423n4) and emit audit events.

Output:
  data_raw/audits/audit_events_code4rena.csv

Requires:
  - GITHUB_TOKEN in .env (recommended; avoids rate limiting)
"""

from pathlib import Path
import os, re
import requests
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

OUT = ROOT / "data_raw" / "audits" / "audit_events_code4rena.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()

ORG = "code-423n4"
API = f"https://api.github.com/orgs/{ORG}/repos"

DATE_RE = re.compile(r"^\d{4}-\d{2}-")  # e.g., 2025-09-succinct

def gh_headers():
    h = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

def fetch_all_repos():
    repos = []
    page = 1
    while True:
        r = requests.get(API, headers=gh_headers(), params={"per_page": 100, "page": page}, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"GitHub API error {r.status_code}: {r.text[:200]}")
        batch = r.json()
        if not batch:
            break
        repos.extend(batch)
        page += 1
    return repos

def main():
    repos = fetch_all_repos()

    rows = []
    for repo in repos:
        name = repo.get("name", "")
        if not DATE_RE.match(name):
            continue

        # repo name format: YYYY-MM-project
        # We'll treat "project" part as protocol_name_raw candidate (better than nothing)
        parts = name.split("-", 2)
        protocol_guess = parts[2] if len(parts) == 3 else name
        # normalize common suffixes (very important for mapping)
        protocol_guess = re.sub(r"-(findings|report|reports)$", "", protocol_guess, flags=re.IGNORECASE)
        protocol_guess = protocol_guess.strip()
        
        rows.append({
            "protocol_name_raw": protocol_guess,
            "source": "code4rena",
            "audit_firm_raw": "Code4rena",
            "audit_score": None,
            "audit_date": repo.get("created_at"),   # ISO string (UTC)
            "evidence_url": repo.get("html_url"),
            "notes": f"repo={name}",
        })

    df = pd.DataFrame(rows)
    df.to_csv(OUT, index=False)
    print(f"✅ Wrote {OUT} | rows={len(df)}")
    print(df["source"].value_counts().to_dict())
    print("Examples:", df.head(10)[["protocol_name_raw","audit_date","evidence_url"]].to_string(index=False))

if __name__ == "__main__":
    main()