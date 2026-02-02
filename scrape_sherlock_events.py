#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scrape_sherlock_events.py

Scrapes Sherlock contest/audit repos from the public GitHub org: sherlock-audit.

Outputs:
  data_raw/audits/audit_events_sherlock.csv

Schema (matches audit_events_long style):
  protocol_name_raw, source, audit_firm_raw, audit_score, audit_date,
  evidence_url, notes, __srcfile

Key upgrades:
  - Normalizes protocol_name_raw (removes -judging, -audit, -contest, -reports, etc.)
  - Uses GitHub API if available, but DOES NOT REQUIRE a token
  - Robust pagination + rate-limit handling
  - Best-effort audit_date derived from repo name (YYYY-MM) else created_at
"""

from __future__ import annotations
from pathlib import Path
import os
import re
import time
import requests
import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "data_raw" / "audits" / "audit_events_sherlock.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

API = "https://api.github.com"
ORG = "sherlock-audit"
SLEEP_SEC = 0.8

# strips common suffixes in sherlock repo names
SUFFIX_RE = re.compile(
    r"(?i)-(judging|contest|audit|audits|report|reports|finding|findings)$"
)

# repo names look like: 2022-08-sentiment, 2022-08-sentiment-judging, etc.
REPO_RE = re.compile(r"^(20\d{2})-(\d{2})-(.+)$")


def _headers() -> dict:
    """
    Token is OPTIONAL. If provided, use it to increase rate limit.
    Accept either GITHUB_TOKEN or GH_TOKEN.
    """
    tok = (os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN") or "").strip()
    h = {"Accept": "application/vnd.github+json"}
    if tok:
        # PATs work fine here.
        h["Authorization"] = f"Bearer {tok}"
    return h


def gh_get(url: str, params: dict | None = None) -> requests.Response:
    while True:
        r = requests.get(url, headers=_headers(), params=params, timeout=60)

        # If unauthenticated, you can still hit 403 with rate-limits; wait and retry.
        if r.status_code in (403, 429):
            reset = r.headers.get("X-RateLimit-Reset")
            remaining = r.headers.get("X-RateLimit-Remaining")
            if remaining == "0" and reset:
                wait = max(int(reset) - int(time.time()), 10)
            else:
                wait = 20
            print(f"⚠️ GitHub {r.status_code} limit — sleeping {wait}s")
            time.sleep(wait)
            continue

        # If user exported a bad token, tell them how to recover.
        if r.status_code == 401:
            raise SystemExit(
                "GitHub API 401 Unauthorized. Your token is wrong or expired.\n"
                "Fix: unset GITHUB_TOKEN (or GH_TOKEN), then re-run. Or export a valid PAT.\n"
                "  unset GITHUB_TOKEN\n"
                "  unset GH_TOKEN\n"
                "  export GITHUB_TOKEN='github_pat_...'\n"
            )

        r.raise_for_status()
        return r


def normalize_protocol_name(raw: str) -> str:
    """
    Normalize a protocol name extracted from repo name.
    Example:
      "sentiment-judging" -> "sentiment"
      "notional-audit" -> "notional"
      "badger-citadel" stays as is
    """
    s = (raw or "").strip().lower()
    s = s.replace("_", "-")
    # repeatedly strip suffixes (some repos end with multiple suffixes)
    prev = None
    while prev != s:
        prev = s
        s = SUFFIX_RE.sub("", s)
    s = s.strip("-").strip()
    return s


def date_from_repo(repo_name: str, created_at: str | None) -> str | None:
    """
    Prefer repo_name prefix date (YYYY-MM-*) -> YYYY-MM-15T00:00:00Z
    Else fall back to created_at (already ISO).
    """
    m = REPO_RE.match(repo_name or "")
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        return f"{y:04d}-{mo:02d}-15T00:00:00Z"

    if created_at:
        # created_at is like 2022-08-25T16:54:30Z
        return created_at

    return None


def main():
    rows = []
    page = 1

    while True:
        url = f"{API}/orgs/{ORG}/repos"
        params = {
            "per_page": 100,
            "page": page,
            "type": "public",
            "sort": "created",
            "direction": "asc",
        }
        data = gh_get(url, params=params).json()
        if not data:
            break

        for repo in data:
            name = repo.get("name", "")
            html_url = repo.get("html_url", "")
            created_at = repo.get("created_at", None)

            m = REPO_RE.match(name)
            if not m:
                # skip non-standard repos in the org
                continue

            proto_raw = m.group(3)  # after YYYY-MM-
            proto_norm = normalize_protocol_name(proto_raw)
            audit_date = date_from_repo(name, created_at)

            rows.append(
                {
                    "protocol_name_raw": proto_norm,
                    "source": "sherlock",
                    "audit_firm_raw": "Sherlock",
                    "audit_score": None,
                    "audit_date": audit_date,
                    "evidence_url": html_url,
                    "notes": f"repo={name}",
                    "__srcfile": "audit_events_sherlock.csv",
                }
            )

        if len(data) < 100:
            break

        page += 1
        time.sleep(SLEEP_SEC)

    df = pd.DataFrame(rows)

    if df.empty:
        df.to_csv(OUT, index=False)
        print(f"✅ Wrote {OUT} | rows=0")
        print("{'sherlock': 0}")
        return

    # de-dupe: same protocol/date/url duplicates are not useful
    df = df.drop_duplicates(subset=["protocol_name_raw", "audit_date", "evidence_url"], keep="first")

    df.to_csv(OUT, index=False)
    print(f"✅ Wrote {OUT} | rows={len(df)}")
    print({"sherlock": len(df)})
    print("Examples:")
    print(df[["protocol_name_raw", "audit_date", "evidence_url"]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()