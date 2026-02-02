#!/usr/bin/env python3
import os
import re
import time
import csv
import json
import random
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

# Auto-load .env so you don't have to export every time
load_dotenv(".env")

GITHUB_TOKEN = (os.getenv("GITHUB_TOKEN") or "").strip()
GITHUB_BASE_URL = (os.getenv("GITHUB_BASE_URL") or "https://api.github.com").strip()

OUT_PATH = os.path.join("data_raw", "audits", "audit_events_github_strict.csv")
CKPT_PATH = os.path.join("outputs", "ckpt_github_strict_search.json")

# Start with a few firms; add later if you want
FIRMS = [
    ("OpenZeppelin", r"\bopen\s*zeppelin\b|\bopenzeppelin\b"),
    ("Trail of Bits", r"\btrail\s*of\s*bits\b|\btrailofbits\b"),
    ("Quantstamp", r"\bquantstamp\b"),
    ("ConsenSys Diligence", r"\bconsensys\s*diligence\b|\bdiligence\b"),
]

# You asked: avoid OR in qualifiers -> one query per path
PATHS = ["audit", "audits", "security", "reports", "review"]
EXTS = ["pdf", "md"]

# Search keywords; you can add more strings later
KEYWORDS = ["openzeppelin", "trailofbits", "quantstamp", "diligence"]

PER_PAGE = 100
MAX_PAGES_PER_QUERY = 2   # keep modest; code-search triggers secondary rate limits fast
FLUSH_EVERY = 200


def ensure_out_header() -> None:
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    if os.path.exists(OUT_PATH):
        return
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "protocol_name_raw",
            "source",
            "audit_firm_raw",
            "audit_score",
            "audit_date",
            "evidence_url",
            "notes",
            "__srcfile",
        ])


def load_ckpt() -> Dict[str, int]:
    if not os.path.exists(CKPT_PATH):
        return {}
    try:
        with open(CKPT_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_ckpt(ckpt: Dict[str, int]) -> None:
    os.makedirs(os.path.dirname(CKPT_PATH), exist_ok=True)
    with open(CKPT_PATH, "w", encoding="utf-8") as f:
        json.dump(ckpt, f, indent=2, sort_keys=True)


def normalize_protocol_guess(s: str) -> str:
    """Make a slug-ish guess from repo/file names."""
    s = (s or "").strip().lower()

    # strip common suffixes
    s = re.sub(r"[-_](audits?|audit[-_]?reports?|security[-_]?review|review|report|reports)$", "", s)
    s = re.sub(r"[-_](findings)$", "", s)
    s = re.sub(r"[-_](judging|contest|results?)$", "", s)

    # keep slug-ish chars
    s = re.sub(r"[^a-z0-9\-_.]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def detect_firm(text: str) -> Optional[str]:
    t = (text or "").lower()
    for firm, pat in FIRMS:
        if re.search(pat, t):
            return firm
    return None


def gh_get(url: str, *, params: dict | None = None, timeout: int = 60, max_tries: int = 12) -> requests.Response:
    """
    Robust GitHub GET:
    - handles 401 (bad token)
    - handles 403/429 (rate/secondary limits) by sleeping and retrying
    - NEVER crashes the run for a transient 403
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "defi-protocol-security/1.0",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    last_err = None
    for attempt in range(1, max_tries + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)

            if 200 <= r.status_code < 300:
                return r

            if r.status_code == 401:
                raise RuntimeError("GitHub API 401 Unauthorized: token missing/invalid, or needs SSO authorization.")

            # primary or secondary rate limit
            if r.status_code in (403, 429):
                retry_after = r.headers.get("Retry-After")
                reset = r.headers.get("X-RateLimit-Reset")

                if retry_after and retry_after.isdigit():
                    sleep_s = int(retry_after)
                elif reset and reset.isdigit():
                    now = int(time.time())
                    sleep_s = max(15, int(reset) - now + 10)
                else:
                    # secondary limit often has no reset -> exponential backoff
                    sleep_s = min(900, 20 * (2 ** (attempt - 1)))

                sleep_s = int(sleep_s * (0.85 + random.random() * 0.3))  # jitter

                try:
                    msg = r.json().get("message", "")
                except Exception:
                    msg = r.text[:200]

                print(f"⚠️ GitHub {r.status_code} (rate/secondary) — sleeping {sleep_s}s | msg={msg}")
                time.sleep(sleep_s)
                continue

            # Other errors: log and stop this request
            try:
                msg = r.json().get("message", "")
            except Exception:
                msg = r.text[:300]
            raise RuntimeError(f"GitHub API error {r.status_code}: {msg}")

        except Exception as e:
            last_err = e
            backoff = min(120, 3 * attempt)
            print(f"⚠️ Request error attempt {attempt}/{max_tries}: {e} — sleeping {backoff}s")
            time.sleep(backoff)

    raise RuntimeError(f"GitHub request failed after {max_tries} tries. Last error: {last_err}")


def search_code(query: str, page: int) -> dict:
    url = f"{GITHUB_BASE_URL}/search/code"
    params = {"q": query, "per_page": PER_PAGE, "page": page}
    resp = gh_get(url, params=params)
    return resp.json()


def main():
    ensure_out_header()
    ckpt = load_ckpt()

    if not GITHUB_TOKEN:
        print("❌ GITHUB_TOKEN missing. Put it in .env as GITHUB_TOKEN=github_pat_xxx")
        return

    buffer_rows: List[List[str]] = []
    flushed = 0

    total_queries = len(KEYWORDS) * len(PATHS) * len(EXTS)
    print(f"Total queries: {total_queries}")

    for kw in KEYWORDS:
        for p in PATHS:
            for ext in EXTS:
                # IMPORTANT: no ORs in qualifiers
                query = f"\"{kw}\" path:{p} extension:{ext}"
                qkey = f"{kw}|{p}|{ext}"
                start_page = int(ckpt.get(qkey, 0)) + 1

                print(f"🔎 Query: {query} (start page {start_page})")

                for page in range(start_page, MAX_PAGES_PER_QUERY + 1):
                    data = search_code(query, page)
                    items = data.get("items", []) or []

                    if not items:
                        ckpt[qkey] = page
                        save_ckpt(ckpt)
                        break

                    for it in items:
                        html_url = it.get("html_url") or ""
                        repo = (it.get("repository") or {}).get("name") or ""
                        path_in_repo = it.get("path") or ""
                        file_name = it.get("name") or ""

                        proto_guess = normalize_protocol_guess(repo) or normalize_protocol_guess(file_name)
                        firm_guess = detect_firm(f"{kw} {repo} {path_in_repo} {file_name}") or kw

                        buffer_rows.append([
                            proto_guess or repo or file_name,
                            "github_strict_search",
                            firm_guess,
                            "",  # score unknown
                            "",  # date unknown
                            html_url,
                            f"kw={kw}; path={p}; ext={ext}; repo={repo}; file={path_in_repo}",
                            "scrape_github_strict_audit_reports.py",
                        ])

                    ckpt[qkey] = page
                    save_ckpt(ckpt)

                    if len(buffer_rows) >= FLUSH_EVERY:
                        with open(OUT_PATH, "a", newline="", encoding="utf-8") as f:
                            csv.writer(f).writerows(buffer_rows)
                        flushed += len(buffer_rows)
                        buffer_rows = []
                        print(f"✅ flushed rows (approx total): {flushed}")

                    time.sleep(1.2)  # slow down to reduce secondary limits

    if buffer_rows:
        with open(OUT_PATH, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(buffer_rows)
        flushed += len(buffer_rows)

    print(f"✅ Done. Appended ~{flushed} rows -> {OUT_PATH}")


if __name__ == "__main__":
    main()