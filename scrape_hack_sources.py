"""
scrape_hack_sources.py
=======================

Scrape additional hack incident data from public websites (SlowMist + ChainSec)
and write to hacks_extra.json.

Note: websites change frequently; parsing may require updates.
"""

from __future__ import annotations

import datetime as _dt
import json as _json
import re
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple

import requests  # type: ignore
from bs4 import BeautifulSoup  # type: ignore


# -----------------------------
# Data model
# -----------------------------
@dataclass
class HackEvent:
    protocol: str
    date: _dt.date
    loss_usd: Optional[float]
    attack_method: str
    source: Optional[str] = None


# -----------------------------
# Helpers
# -----------------------------
def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return s


_DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
_MONEY_RE = re.compile(
    r"(?i)\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+(?:\.[0-9]+)?)\s*([kmb]|million|billion)?"
)


def _parse_date_from_text(text: str) -> Optional[_dt.date]:
    m = _DATE_RE.search(text)
    if not m:
        return None
    try:
        return _dt.datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except Exception:
        return None


def _normalize_money_to_float(num_str: str, unit: Optional[str]) -> Optional[float]:
    try:
        x = float(num_str.replace(",", ""))
    except Exception:
        return None
    if not unit:
        return x
    u = unit.lower()
    if u in ("k",):
        return x * 1_000
    if u in ("m", "million"):
        return x * 1_000_000
    if u in ("b", "billion"):
        return x * 1_000_000_000
    return x


def _extract_loss_usd(text: str) -> Optional[float]:
    """
    Tries to extract a USD loss amount from free text.
    Supports:
      $ 3,900,000
      $3.9M
      3.9 million
      Amount stolen/loss: ...
    """
    lower = text.lower()
    if "n/a" in lower:
        return None

    # Prefer segments after keywords
    for key in ["amount of loss", "amount stolen", "loss", "stolen"]:
        if key in lower:
            # take a short window after the keyword
            idx = lower.find(key)
            window = text[idx : idx + 120]
            m = _MONEY_RE.search(window)
            if m:
                return _normalize_money_to_float(m.group(1), m.group(2))

    # Fallback: search anywhere
    m = _MONEY_RE.search(text)
    if m:
        return _normalize_money_to_float(m.group(1), m.group(2))
    return None


def _extract_attack_method(text: str) -> str:
    """
    Extract attack method if present like:
      Attack method: Privilege compromise
    """
    # Robust split (case-insensitive)
    m = re.search(r"(?i)attack\s*method\s*:\s*(.+)", text)
    if not m:
        return ""
    # cut off if other fields appended
    val = m.group(1)
    # stop at common next-field markers
    val = re.split(r"(?i)\b(amount|loss|stolen)\b\s*:", val)[0]
    return val.strip()


# -----------------------------
# SlowMist scraper
# -----------------------------
def scrape_slowmist(debug: bool = False) -> List[HackEvent]:
    """
    Scrape https://hacked.slowmist.io/

    Strategy:
      1) Fetch page
      2) Find "card" containers. First try div.shadow-lg (old layout).
         If empty, use a fallback heuristic: divs containing a date YYYY-MM-DD and an H3.
      3) Parse date from card text via regex; parse protocol from H3 text; parse loss & attack_method from card text.
    """
    url = "https://hacked.slowmist.io/"
    session = _make_session()
    resp = session.get(url, timeout=30)

    if debug:
        print("Fetched SlowMist page")
        print("  Status code:", resp.status_code)
        print("  Page length:", len(resp.text))
        print("  First 200 chars:", resp.text[:200].replace("\n", "\\n"))

    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Primary selector (your original)
    cards = soup.find_all("div", class_="shadow-lg")

    # Fallback if layout changed
    if not cards:
        # find candidate divs that include a date and an h3
        candidates = []
        for div in soup.find_all("div"):
            txt = div.get_text(" ", strip=True)
            if _DATE_RE.search(txt) and div.find("h3"):
                candidates.append(div)
        cards = candidates

    events: List[HackEvent] = []
    seen: set[Tuple[str, str]] = set()

    for card in cards:
        card_text = card.get_text(" ", strip=True)
        date = _parse_date_from_text(card_text)
        h3 = card.find("h3")

        if not date or not h3:
            continue

        protocol = h3.get_text(strip=True)
        protocol = protocol.replace("Hacked target:", "").replace("Hacked target", "").strip()

        loss = _extract_loss_usd(card_text)
        attack_method = _extract_attack_method(card_text)

        key = (protocol.lower(), date.isoformat())
        if key in seen:
            continue
        seen.add(key)

        events.append(
            HackEvent(
                protocol=protocol,
                date=date,
                loss_usd=loss,
                attack_method=attack_method,
                source="SlowMist",
            )
        )

    if debug:
        print(f"SlowMist parsed events: {len(events)}")
        if events[:3]:
            print("SlowMist sample titles:", [e.protocol for e in events[:3]])

    return events


# -----------------------------
# ChainSec scraper
# -----------------------------
def scrape_chainsec(debug: bool = False) -> List[HackEvent]:
    """
    Scrape https://chainsec.io/defi-hacks/

    Strategy:
      - Each hack entry appears as an h3 heading containing:
          Project (Month Day, Year)
      - Next paragraph contains description + sometimes "Amount stolen: ..."
    """
    url = "https://chainsec.io/defi-hacks/"
    session = _make_session()
    resp = session.get(url, timeout=30)

    if debug:
        print("Fetched ChainSec page")
        print("  Status code:", resp.status_code)
        print("  Page length:", len(resp.text))
        print("  First 200 chars:", resp.text[:200].replace("\n", "\\n"))

    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    events: List[HackEvent] = []
    seen: set[Tuple[str, str]] = set()

    for h3 in soup.find_all("h3"):
        title = h3.get_text(" ", strip=True)
        if "(" not in title or ")" not in title:
            continue

        name_part, date_part = title.rsplit("(", 1)
        protocol = name_part.strip()
        date_str = date_part.strip("()").strip()

        # Try common formats
        date: Optional[_dt.date] = None
        for fmt in ("%B %d, %Y", "%b %d, %Y"):
            try:
                date = _dt.datetime.strptime(date_str, fmt).date()
                break
            except ValueError:
                continue
        if not date:
            continue

        p = h3.find_next("p")
        text = p.get_text(" ", strip=True) if p else ""

        loss = _extract_loss_usd(text)
        attack_method = _extract_attack_method(text)  # usually not present on ChainSec, but harmless

        key = (protocol.lower(), date.isoformat())
        if key in seen:
            continue
        seen.add(key)

        events.append(
            HackEvent(
                protocol=protocol,
                date=date,
                loss_usd=loss,
                attack_method=attack_method or "",
                source="ChainSec",
            )
        )

    if debug:
        print(f"ChainSec parsed events: {len(events)}")
        if events[:3]:
            print("ChainSec sample titles:", [e.protocol for e in events[:3]])

    return events


# -----------------------------
# Main
# -----------------------------
def main(argv: List[str]) -> int:
    out_path = "hacks_extra.json"
    debug = False

    # simple args: --out xxx.json --debug
    if "--out" in argv:
        i = argv.index("--out")
        if i + 1 < len(argv):
            out_path = argv[i + 1]
    if "--debug" in argv:
        debug = True

    slowmist_events = scrape_slowmist(debug=debug)
    chainsec_events = scrape_chainsec(debug=debug)

    print(f"Scraped {len(slowmist_events)} events from SlowMist and {len(chainsec_events)} from ChainSec")

    all_events: List[HackEvent] = slowmist_events + chainsec_events

    with open(out_path, "w", encoding="utf-8") as f:
        _json.dump([e.__dict__ for e in all_events], f, default=str, indent=2, ensure_ascii=False)

    print(f"Saved {out_path} with {len(all_events)} total events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
