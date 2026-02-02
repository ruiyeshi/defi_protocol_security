#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import csv
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv(".env")
except Exception:
    pass


OUT_CSV = "data_raw/audits/audit_events_firm_archives.csv"

HEADERS = {
    "User-Agent": "defi-protocol-security-audit-scraper/1.0 (academic-research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Roots you provided + OZ root we found
FIRM_ROOTS = [
    {
        "source": "openzeppelin",
        "audit_firm_raw": "OpenZeppelin",
        # Research page where "Security Audits" lives (filtering is JS, but links appear in HTML)
        "start_urls": ["https://www.openzeppelin.com/research"],
        "allow_domains": {"www.openzeppelin.com", "openzeppelin.com"},
    },
    {
        "source": "trailofbits",
        "audit_firm_raw": "Trail of Bits",
        "start_urls": [
            "https://blog.trailofbits.com/categories/audits/",
            "https://www.trailofbits.com/reports/",
        ],
        "allow_domains": {"blog.trailofbits.com", "www.trailofbits.com", "trailofbits.com"},
    },
    {
        "source": "diligence",
        "audit_firm_raw": "ConsenSys Diligence",
        "start_urls": ["https://diligence.security/audits/"],
        "allow_domains": {"diligence.security"},
    },
    {
        "source": "quantstamp",
        "audit_firm_raw": "Quantstamp",
        "start_urls": ["https://certificate.quantstamp.com/"],
        "allow_domains": {"certificate.quantstamp.com"},
    },
]

# What we consider "likely audit/report evidence"
EVIDENCE_PAT = re.compile(
    r"(?i)\b(audit|audits|security[-_ ]?review|review|report|reports|assessment|findings)\b"
)
FILE_PAT = re.compile(r"(?i)\.(pdf|md)$")

# Avoid obvious junk
BLOCK_PAT = re.compile(
    r"(?i)\b(privacy|terms|careers|jobs|contact|cookie|newsletter|tag|category|author|feed|rss)\b"
)


def norm_url(u: str) -> str:
    u = u.strip()
    if not u:
        return ""
    # drop fragments
    u = u.split("#", 1)[0]
    return u


def same_domain(url: str, allow_domains: set[str]) -> bool:
    try:
        host = urlparse(url).netloc.lower()
        return host in allow_domains
    except Exception:
        return False


def get_html(url: str, session: requests.Session, timeout: int = 40) -> str:
    r = session.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def extract_date_guess(text: str) -> str:
    """
    Best-effort date guess (ISO-ish) from page text:
    - 2024-01-31
    - Jan 31, 2024
    """
    if not text:
        return ""
    # ISO date
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})\b", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # Month name date (rough)
    m = re.search(
        r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+(\d{1,2}),\s*(20\d{2})\b",
        text,
        flags=re.IGNORECASE,
    )
    if m:
        mon = m.group(1)[:3].title()
        day = int(m.group(2))
        yr = int(m.group(3))
        months = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,"Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
        mm = months.get(mon, 0)
        if mm:
            return f"{yr:04d}-{mm:02d}-{day:02d}"
    return ""


def pick_links(page_url: str, html: str, firm: dict) -> tuple[list[dict], list[str]]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    next_candidates = []

    # collect links
    for a in soup.select("a[href]"):
        href = norm_url(a.get("href", ""))
        if not href:
            continue

        absu = norm_url(urljoin(page_url, href))
        if not absu.startswith("http"):
            continue
        if not same_domain(absu, firm["allow_domains"]):
            continue

        anchor_text = " ".join(a.get_text(" ", strip=True).split())
        blob = f"{absu} {anchor_text}".strip()

        # pagination candidates
        if re.search(r"(?i)\b(next|older|page)\b", anchor_text) and not BLOCK_PAT.search(absu):
            next_candidates.append(absu)

        # evidence candidates
        if BLOCK_PAT.search(absu):
            continue

        if FILE_PAT.search(absu) or EVIDENCE_PAT.search(blob):
            rows.append(
                {
                    "protocol_name_raw": "",  # filled later by mapping/parsing
                    "source": firm["source"],
                    "audit_firm_raw": firm["audit_firm_raw"],
                    "audit_score": "",
                    "audit_date": "",  # we'll try to guess per-page in a second stage
                    "evidence_url": absu,
                    "notes": f"found_on={page_url}",
                    "__srcfile": os.path.basename(OUT_CSV),
                }
            )

    # also look for rel=next
    ln = soup.find("a", rel=lambda x: x and "next" in x)
    if ln and ln.get("href"):
        next_candidates.append(norm_url(urljoin(page_url, ln["href"])))

    # de-dupe
    seen = set()
    rows2 = []
    for r in rows:
        if r["evidence_url"] in seen:
            continue
        seen.add(r["evidence_url"])
        rows2.append(r)

    next_candidates = list(dict.fromkeys(next_candidates))
    return rows2, next_candidates


def crawl_firm(firm: dict, max_pages: int = 60, sleep_s: float = 1.2) -> list[dict]:
    session = requests.Session()
    out = []
    visited = set()
    queue = list(firm["start_urls"])

    pages = 0
    while queue and pages < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        try:
            html = get_html(url, session=session)
        except Exception as e:
            print(f"⚠️  {firm['source']} failed {url}: {e}")
            continue

        rows, nexts = pick_links(url, html, firm)
        out.extend(rows)

        # Heuristics for pagination:
        # - follow "next/older/page" links, but cap growth
        for n in nexts:
            if n not in visited and same_domain(n, firm["allow_domains"]):
                queue.append(n)

        pages += 1
        if pages % 10 == 0:
            print(f"… {firm['source']} crawled pages={pages}, rows={len(out)}")

        time.sleep(sleep_s)

    # final de-dupe by evidence_url
    uniq = {}
    for r in out:
        uniq[r["evidence_url"]] = r
    return list(uniq.values())


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def main():
    ensure_parent(OUT_CSV)

    all_rows = []
    for firm in FIRM_ROOTS:
        print(f"\n=== Crawling {firm['source']} ({firm['audit_firm_raw']}) ===")
        rows = crawl_firm(firm, max_pages=80, sleep_s=1.1)
        print(f"✅ {firm['source']} harvested candidate links: {len(rows)}")
        all_rows.extend(rows)

    # de-dupe globally
    uniq = {}
    for r in all_rows:
        uniq[(r["source"], r["audit_firm_raw"], r["evidence_url"])] = r
    all_rows = list(uniq.values())

    # write
    cols = [
        "protocol_name_raw",
        "source",
        "audit_firm_raw",
        "audit_score",
        "audit_date",
        "evidence_url",
        "notes",
        "__srcfile",
    ]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in all_rows:
            w.writerow({c: r.get(c, "") for c in cols})

    print(f"\n✅ Wrote: {os.path.abspath(OUT_CSV)} | rows={len(all_rows)}")
    print("Next step: dedupe + map evidence_url -> protocol slug (your mapping pipeline).")


if __name__ == "__main__":
    main()