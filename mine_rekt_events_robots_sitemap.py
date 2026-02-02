from __future__ import annotations

"""
mine_rekt_events_robots_sitemap.py

Goal:
- Mine rekt.news incident post URLs via robots.txt -> sitemap(s)
- Avoid WP API (often 403)
- Parse each post page for title + published time
- Output clean CSV (NO GARBAGE rows): requires protocol_name_raw + evidence_url

Output:
  data_raw/exploits/exploit_events_rekt.csv

Usage:
  cd ~/defi_protocol_security
  python mine_rekt_events_robots_sitemap.py

Optional env:
  REKT_SLEEP_SEC=0.6
  REKT_TIMEOUT=30
"""

from pathlib import Path
import os
import re
import time
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup  # pip install beautifulsoup4

OUT = Path("data_raw/exploits/exploit_events_rekt.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

BASE = "https://rekt.news/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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


def _get(url: str, timeout: int) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def _is_post_url(u: str) -> bool:
    if not u:
        return False
    p = urlparse(u)
    if p.scheme not in {"http", "https"}:
        return False
    if "rekt.news" not in p.netloc:
        return False

    path = (p.path or "").strip("/")

    # exclude obvious non-post sections
    if not path:
        return False
    if path.startswith(("tag/", "category/", "author/", "page/")):
        return False
    if "/tag/" in p.path or "/category/" in p.path or "/author/" in p.path:
        return False
    if "?tag=" in u or "tag=" in (p.query or ""):
        return False

    # exclude common site pages
    if path in {"about", "contact", "privacy-policy", "disclaimer"}:
        return False

    # heuristic: post slugs are usually not super short
    if len(path) < 4:
        return False

    return True


def _find_sitemaps_from_robots(timeout: int) -> list[str]:
    robots_url = urljoin(BASE, "robots.txt")
    txt = _get(robots_url, timeout=timeout)
    sitemaps: list[str] = []
    for line in txt.splitlines():
        if line.lower().startswith("sitemap:"):
            sm = line.split(":", 1)[1].strip()
            if sm:
                sitemaps.append(sm)
    return sitemaps


def _expand_sitemap(url: str, timeout: int) -> list[str]:
    """
    Accepts sitemap index or urlset.
    Returns post URLs.
    """
    xml = _get(url, timeout=timeout)
    soup = BeautifulSoup(xml, "xml")

    # sitemap index -> recurse children
    if soup.find("sitemapindex"):
        locs = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
        out: list[str] = []
        for child in locs:
            try:
                out.extend(_expand_sitemap(child, timeout=timeout))
            except Exception:
                continue
        return out

    # normal urlset
    locs = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
    return [u for u in locs if _is_post_url(u)]


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


def _extract_published_datetime(soup: BeautifulSoup) -> str:
    m = soup.find("meta", property="article:published_time")
    if m and m.get("content"):
        return m["content"].strip()

    tm = soup.find("time")
    if tm:
        if tm.get("datetime"):
            return tm["datetime"].strip()
        txt = tm.get_text(strip=True)
        if txt:
            return txt

    # fallback: JSON-LD datePublished
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            import json

            data = json.loads(script.get_text(strip=True) or "{}")
            candidates = []
            if isinstance(data, dict):
                candidates = [data]
            elif isinstance(data, list):
                candidates = [x for x in data if isinstance(x, dict)]
            for d in candidates:
                if isinstance(d.get("datePublished"), str) and d["datePublished"].strip():
                    return d["datePublished"].strip()
        except Exception:
            continue

    return ""


def _guess_protocol_from_title(title: str) -> str:
    """
    Rekt titles often look like:
      "Some Protocol - Rekt"
      "Some Protocol Rekt"
      "... | Rekt.news"
    """
    t = (title or "").strip()
    t = re.sub(r"\s*\|\s*rekt\.news\s*$", "", t, flags=re.I)
    t = re.sub(r"\s*[-–—]\s*rekt\s*$", "", t, flags=re.I)
    t = re.sub(r"\s+rekt\s*$", "", t, flags=re.I)
    return t.strip(" -–—|:").strip()


def main() -> None:
    timeout = int(os.getenv("REKT_TIMEOUT", "30"))
    sleep_sec = float(os.getenv("REKT_SLEEP_SEC", "0.6"))

    # 1) robots.txt -> sitemap(s)
    try:
        sitemap_roots = _find_sitemaps_from_robots(timeout=timeout)
    except Exception as e:
        print(f"❌ Failed to read robots.txt: {e}")
        pd.DataFrame(columns=COLS).to_csv(OUT, index=False)
        print(f"⚠️ Wrote empty CSV with headers -> {OUT}")
        return

    if not sitemap_roots:
        print("⚠️ No sitemaps found in robots.txt")
        pd.DataFrame(columns=COLS).to_csv(OUT, index=False)
        print(f"⚠️ Wrote empty CSV with headers -> {OUT}")
        return

    # 2) expand sitemap(s) -> post urls
    post_urls: list[str] = []
    for sm in sitemap_roots:
        try:
            post_urls.extend(_expand_sitemap(sm, timeout=timeout))
        except Exception as e:
            print(f"⚠️ sitemap failed: {sm} | {e}")

    # dedupe
    seen = set()
    urls = []
    for u in post_urls:
        if u not in seen:
            urls.append(u)
            seen.add(u)

    print(f"✅ Found post URLs via robots+sitemaps: {len(urls)}")

    if not urls:
        pd.DataFrame(columns=COLS).to_csv(OUT, index=False)
        print(f"⚠️ No post URLs found; wrote empty CSV with headers -> {OUT}")
        return

    # 3) fetch each post page and parse title/date
    rows = []
    for i, url in enumerate(urls, start=1):
        try:
            html = _get(url, timeout=timeout)
        except Exception:
            continue

        soup = BeautifulSoup(html, "html.parser")
        title = _extract_title(soup)
        if not title:
            continue

        proto = _guess_protocol_from_title(title)
        if not proto or proto.lower() in {"untitled", "rekt", "rekt news"}:
            continue

        published = _extract_published_datetime(soup)

        rows.append(
            {
                "protocol_name_raw": proto,
                "source": "rekt",
                "exploit_date": published,
                "loss_usd": None,
                "chain": "",
                "exploit_type": "",
                "evidence_url": url,
                "notes": f"title={title}",
            }
        )

        if i % 50 == 0:
            print(f"  fetched={i}/{len(urls)} kept_rows={len(rows)}")

        time.sleep(sleep_sec)

    df = pd.DataFrame(rows, columns=COLS)

    # NO GARBAGE: require protocol + url
    if not df.empty:
        df["protocol_name_raw"] = df["protocol_name_raw"].fillna("").astype(str).str.strip()
        df["evidence_url"] = df["evidence_url"].fillna("").astype(str).str.strip()
        df = df[(df["protocol_name_raw"] != "") & (df["evidence_url"] != "")]
        df = df.drop_duplicates(subset=["evidence_url"], keep="first")
        df["exploit_date"] = pd.to_datetime(df["exploit_date"], errors="coerce", utc=True)

    df.to_csv(OUT, index=False)
    print(f"✅ Wrote {len(df)} rows -> {OUT}")


if __name__ == "__main__":
    main()