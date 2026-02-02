#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
import os
import re
import time
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

OUT = Path("data_raw/exploits/exploit_events_rekt.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

BASE = "https://rekt.news/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

COLS = [
    "protocol_name_raw",
    "source",
    "exploit_date",
    "loss_usd",
    "chain",
    "exploit_type",
    "evidence_url",
    "notes",
]

# --- filters ---
BAD_PATH_SNIPPETS = {
    "termandconditions",
    "privacy",
    "contact",
    "about",
    "advertise",
    "disclaimer",
    "imprint",
}
BAD_PROTOCOL_NAMES = {"t&c", "terms", "privacy", "untitled", "rekt", "rekt news", "the one that got away n/a"}

def _get(url: str, timeout: int = 25) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text

def _norm_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    if u.startswith("/"):
        u = urljoin(BASE, u)
    return u

def _is_rekt_domain(u: str) -> bool:
    try:
        return "rekt.news" in urlparse(u).netloc
    except Exception:
        return False

def _is_candidate_post_url(u: str) -> bool:
    """Heuristic: keep real post-like URLs, drop obvious non-posts."""
    if not u or not _is_rekt_domain(u):
        return False

    p = urlparse(u)
    path = (p.path or "").strip("/")
    if not path:
        return False

    low = path.lower()

    # drop common non-post sections
    if low.startswith(("tag/", "category/", "author/", "page/", "feed/")):
        return False
    if any(q in (p.query or "").lower() for q in ["tag=", "category=", "author="]):
        return False
    if any(sn in low for sn in BAD_PATH_SNIPPETS):
        return False

    # must look like a slug (not too short)
    if len(low) < 4:
        return False

    return True

def _extract_title(soup: BeautifulSoup) -> str:
    og = soup.find("meta", property="og:title")
    if og and og.get("content"):
        return og["content"].strip()
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    t = soup.find("title")
    if t and t.get_text(strip=True):
        return t.get_text(strip=True)
    return ""

def _extract_published(soup: BeautifulSoup) -> str:
    # OpenGraph / article meta
    m = soup.find("meta", property="article:published_time")
    if m and m.get("content"):
        return m["content"].strip()

    # JSON-LD datePublished
    for s in soup.find_all("script", type="application/ld+json"):
        txt = (s.get_text(strip=True) or "").strip()
        if not txt:
            continue
        try:
            import json
            data = json.loads(txt)
        except Exception:
            continue

        def find_date(obj):
            if isinstance(obj, dict):
                if isinstance(obj.get("datePublished"), str):
                    return obj["datePublished"].strip()
                if isinstance(obj.get("dateCreated"), str):
                    return obj["dateCreated"].strip()
            return None

        if isinstance(data, dict):
            d = find_date(data)
            if d:
                return d
        elif isinstance(data, list):
            for item in data:
                d = find_date(item)
                if d:
                    return d

    tm = soup.find("time")
    if tm and tm.get("datetime"):
        return tm["datetime"].strip()

    return ""

def _guess_protocol_from_title(title: str) -> str:
    t = (title or "").strip()
    t = re.sub(r"\s*\|\s*rekt\.news\s*$", "", t, flags=re.I)
    t = re.sub(r"\s*[-–—]\s*rekt\s*$", "", t, flags=re.I)
    t = re.sub(r"\s+rekt\s*$", "", t, flags=re.I)
    t = t.strip(" -–—|:").strip()
    return t

def _parse_loss_from_text(text: str):
    """
    Extract the maximum $ number found (rough but works surprisingly well for Rekt posts).
    """
    if not text:
        return None
    matches = re.findall(r"\$\s*([0-9]{1,3}(?:[0-9,]{0,})?(?:\.[0-9]+)?)\s*([kKmMbB])?", text)
    if not matches:
        return None
    best = 0.0
    for num, suf in matches:
        try:
            v = float(num.replace(",", ""))
        except Exception:
            continue
        mult = {"k": 1e3, "m": 1e6, "b": 1e9}.get((suf or "").lower(), 1.0)
        best = max(best, v * mult)
    return best if best > 0 else None

def _discover_from_listing_pages(max_pages: int, timeout: int, sleep_sec: float) -> list[str]:
    urls: list[str] = []
    for page in range(1, max_pages + 1):
        page_url = BASE if page == 1 else f"{BASE}?page={page}"
        try:
            html = _get(page_url, timeout=timeout)
        except Exception:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            u = _norm_url(a["href"])
            if _is_candidate_post_url(u):
                urls.append(u)

        if page % 10 == 0:
            print(f"  listing pages scanned={page}/{max_pages} | urls_collected={len(set(urls))}")
        time.sleep(sleep_sec)

    # stable dedupe
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

def _collect_posts(urls: list[str], timeout: int, sleep_sec: float) -> pd.DataFrame:
    rows = []
    for i, url in enumerate(urls, start=1):
        try:
            html = _get(url, timeout=timeout)
        except Exception:
            continue

        soup = BeautifulSoup(html, "html.parser")
        title = _extract_title(soup)
        proto = _guess_protocol_from_title(title)

        # drop garbage titles
        if not proto or proto.lower().strip() in BAD_PROTOCOL_NAMES:
            continue
        # drop if URL is a known non-incident utility page
        if any(sn in urlparse(url).path.lower() for sn in BAD_PATH_SNIPPETS):
            continue

        published = _extract_published(soup)

        # If there's no date at all, usually not a real incident post
        if not published:
            continue

        text = soup.get_text(" ", strip=True)
        loss = _parse_loss_from_text(text)

        rows.append({
            "protocol_name_raw": proto,
            "source": "rekt",
            "exploit_date": published,
            "loss_usd": loss,
            "chain": "",
            "exploit_type": "",
            "evidence_url": url,
            "notes": f"title={title}",
        })

        if i % 50 == 0:
            print(f"  fetched {i}/{len(urls)} | kept_rows={len(rows)}")
        time.sleep(sleep_sec)

    df = pd.DataFrame(rows, columns=COLS)
    if df.empty:
        return df

    df["protocol_name_raw"] = df["protocol_name_raw"].astype(str).str.strip()
    df["evidence_url"] = df["evidence_url"].astype(str).str.strip()
    df = df[(df["protocol_name_raw"] != "") & (df["evidence_url"] != "")]
    df = df.drop_duplicates(subset=["evidence_url"], keep="first")
    df["exploit_date"] = pd.to_datetime(df["exploit_date"], errors="coerce", utc=True)
    return df

def main():
    # how deep to go on listing pages
    max_pages = int(os.getenv("REKT_MAX_PAGES", "80"))
    # slow down if you get blocked
    sleep_sec = float(os.getenv("REKT_SLEEP_SEC", "0.6"))
    timeout = int(os.getenv("REKT_TIMEOUT", "25"))

    print(f"Rekt: scanning listing pages 1..{max_pages}")
    urls = _discover_from_listing_pages(max_pages=max_pages, timeout=timeout, sleep_sec=sleep_sec)
    print(f"Rekt: candidate URLs discovered = {len(urls)}")

    if not urls:
        pd.DataFrame(columns=COLS).to_csv(OUT, index=False)
        print(f"⚠️ No URLs found. Wrote empty CSV -> {OUT}")
        return

    df = _collect_posts(urls, timeout=timeout, sleep_sec=sleep_sec)
    df.to_csv(OUT, index=False)
    print(f"✅ Wrote {len(df)} rows -> {OUT}")

if __name__ == "__main__":
    main()