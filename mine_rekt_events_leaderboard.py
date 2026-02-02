from __future__ import annotations

"""
mine_rekt_events_leaderboard.py

Scrape rekt.news leaderboard (public HTML) to extract exploit events.

Why this works:
- robots.txt has no sitemap lines
- WP JSON API is 403
- leaderboard page is crawlable and contains:
    title + loss + date + link per incident

Output:
  data_raw/exploits/exploit_events_rekt.csv

Columns:
  protocol_name_raw, source, exploit_date, loss_usd, chain, exploit_type, evidence_url, notes
"""

from pathlib import Path
import re
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

OUT = Path("data_raw/exploits/exploit_events_rekt.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

BASE = "https://rekt.news/"
LEADERBOARD = urljoin(BASE, "leaderboard")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
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


def _get_html(url: str, timeout: int = 30) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


def _is_rekt_post_url(u: str) -> bool:
    if not u:
        return False
    p = urlparse(u)
    if "rekt.news" not in p.netloc:
        return False
    path = (p.path or "").strip("/")

    # exclude non-incident pages
    if path in {"leaderboard", "research", "about", "contact"}:
        return False
    if path.startswith(("tag/", "category/", "author/", "page/", "feed/")):
        return False
    if len(path) < 3:
        return False

    return True


def _norm_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if u.startswith("/"):
        u = urljoin(BASE, u)
    if u.startswith("//"):
        u = "https:" + u
    return u


def _parse_money_to_usd(s: str):
    """
    Accepts things like:
      "$1,436,173,027"
      "$14,847,374,246"
      "$55.4m" (if it ever appears)
    Returns float or None.
    """
    if not s:
        return None
    t = str(s).strip().replace(",", "")
    # strict "$123..."
    m = re.search(r"\$\s*([0-9]*\.?[0-9]+)\s*([kKmMbB])?", t)
    if not m:
        return None
    v = float(m.group(1))
    suf = (m.group(2) or "").lower()
    mult = {"k": 1e3, "m": 1e6, "b": 1e9}.get(suf, 1.0)
    return v * mult


def _clean_protocol_from_title(title: str) -> str:
    """
    Examples on leaderboard:
      "Ronin Network - REKT Unaudited"
      "ByBit - Rekt"
      "Wormhole - REKT Neodyme"
    We want protocol_name_raw:
      "Ronin Network"
      "ByBit"
      "Wormhole"
    """
    t = (title or "").strip()

    # Remove trailing audit/status notes after REKT / Rekt
    # Split on " - REKT" or " - Rekt" or " REKT" near the end.
    t = re.sub(r"\s*\|\s*rekt\.news\s*$", "", t, flags=re.I)

    # common patterns
    t = re.sub(r"\s*-\s*rekt\b.*$", "", t, flags=re.I)   # "X - REKT ..."
    t = re.sub(r"\s+rekt\b.*$", "", t, flags=re.I)       # "X REKT ..."

    return t.strip(" -–—|:").strip()


def main():
    try:
        html = _get_html(LEADERBOARD, timeout=30)
    except Exception as e:
        pd.DataFrame(columns=COLS).to_csv(OUT, index=False)
        raise SystemExit(f"❌ Failed to fetch leaderboard: {e}")

    soup = BeautifulSoup(html, "html.parser")

    rows = []

    # The page structure is basically repeated blocks:
    # <a href="...">Title</a>
    # "$AMOUNT | DATE"
    #
    # We'll iterate all <a> and look for a nearby "$... | ..." sibling text.
    for a in soup.find_all("a", href=True):
        href = _norm_url(a.get("href"))
        title = (a.get_text(" ", strip=True) or "").strip()
        if not href or not title:
            continue
        if not _is_rekt_post_url(href):
            continue

        # Find nearby text that contains "$" and "|"
        money_date_text = ""
        parent = a.parent
        if parent:
            blob = parent.get_text(" ", strip=True)
            if "$" in blob and "|" in blob:
                money_date_text = blob
        if not money_date_text:
            # try next siblings in DOM
            sib = a.next_sibling
            if isinstance(sib, str) and ("$" in sib and "|" in sib):
                money_date_text = sib.strip()

        # Parse amount + date from money_date_text
        loss = _parse_money_to_usd(money_date_text)
        # date part after "|"
        dt = ""
        if "|" in money_date_text:
            dt = money_date_text.split("|", 1)[1].strip()

        proto = _clean_protocol_from_title(title)
        if not proto or proto.lower() in {"untitled", "rekt", "rekt news"}:
            continue

        rows.append(
            {
                "protocol_name_raw": proto,
                "source": "rekt",
                "exploit_date": dt,
                "loss_usd": loss,
                "chain": "",
                "exploit_type": "",
                "evidence_url": href,
                "notes": f"leaderboard_title={title}",
            }
        )

    df = pd.DataFrame(rows, columns=COLS)

    # NO GARBAGE enforcement
    if df.empty:
        df = pd.DataFrame(columns=COLS)
        df.to_csv(OUT, index=False)
        print(f"⚠️ No rows parsed; wrote empty CSV with headers -> {OUT}")
        return

    df["protocol_name_raw"] = df["protocol_name_raw"].fillna("").astype(str).str.strip()
    df["evidence_url"] = df["evidence_url"].fillna("").astype(str).str.strip()
    df = df[(df["protocol_name_raw"] != "") & (df["evidence_url"] != "")]
    df = df.drop_duplicates(subset=["evidence_url"], keep="first")

    df["exploit_date"] = pd.to_datetime(df["exploit_date"], errors="coerce", utc=True)

    df.to_csv(OUT, index=False)
    print(f"✅ Wrote {len(df)} rows -> {OUT}")
    print(df["source"].value_counts().to_string())


if __name__ == "__main__":
    main()