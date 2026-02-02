#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scrape_bug_bounties.py

Outputs:
  data_raw/governance/bug_bounty_master.csv

Columns:
  slug, has_immunefi, has_hackerone_public, has_bug_bounty_any

Notes:
- Immunefi: we scrape the public bounties listing page and extract program names/links.
- HackerOne: their directory is often JS-rendered / rate-limited; we implement a best-effort scrape.
  If it yields low coverage, that's expected—Immunefi will be your main signal.
"""

from __future__ import annotations

from pathlib import Path
import os
import re
import time
import difflib
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parent
LLAMA = ROOT / "data_raw" / "llama_protocols.csv"
OUTDIR = ROOT / "data_raw" / "governance"
OUT = OUTDIR / "bug_bounty_master.csv"

UA = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0"
)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})


def norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def best_slug_match(name: str, llama_names, llama_slugs, min_ratio: float = 0.90) -> str | None:
    """
    Map a program name -> llama slug using:
      1) exact name match (normalized)
      2) exact slug match (normalized)
      3) fuzzy (difflib) against names
    """
    n = norm(name)
    if not n:
        return None

    # exact name match
    if n in llama_names:
        return llama_names[n]

    # exact slug match
    if n in llama_slugs:
        return llama_slugs[n]

    # fuzzy name match
    candidates = list(llama_names.keys())
    hit = difflib.get_close_matches(n, candidates, n=1, cutoff=min_ratio)
    if hit:
        return llama_names[hit[0]]
    return None


def fetch_immunefi_program_names() -> list[str]:
    """
    Best-effort HTML scrape of Immunefi's public bounty directory.

    NOTE: Immunefi uses URLs like `/bug-bounty/<program>/` (not `/bounty/`).
    We extract program names from:
      - embedded JSON fragments (projectName)
      - <a href="/bug-bounty/...">NAME</a> anchors
      - aria-label/title attributes near /bug-bounty/

    If the site is blocked / returns a tiny placeholder HTML, we'll return empty.
    """
    urls = [
        "https://immunefi.com/bug-bounty/",
        "https://immunefi.com/explore/",
        "https://immunefi.com/bounties/",
    ]

    html = None
    final_url = None
    for u in urls:
        try:
            r = SESSION.get(
                u,
                timeout=30,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                },
            )
            # Immunefi is often JS-rendered; still returns large HTML shell.
            if r.status_code == 200 and len(r.text) > 5000:
                html = r.text
                final_url = r.url
                break
        except Exception:
            pass

    if not html:
        print("⚠️ Immunefi fetch failed (blocked/changed HTML). Returning empty list.")
        return []

    print(f"ℹ️ Immunefi HTML ok from {final_url} (chars={len(html)})")

    names: set[str] = set()

    # 0) Most robust: extract program slugs directly from any /bug-bounty/<slug>/ occurrences.
    # This works even if the visible card titles are not present in HTML.
    for m in re.finditer(r"/bug-bounty/([a-z0-9][a-z0-9\-]{1,80})/", html.lower()):
        slug = m.group(1).strip("-")
        if not slug:
            continue
        # add both slug-form and humanized form as candidates
        names.add(slug)
        names.add(slug.replace("-", " "))

    # 1) JSON-ish embedded fragments
    for m in re.finditer(r'"projectName"\s*:\s*"([^"]+)"', html):
        names.add(m.group(1).strip())

    # 2) Anchor text around /bug-bounty/
    # Example: <a href="/bug-bounty/ens/">ENS</a>
    for m in re.finditer(r'href="(/bug-bounty/[^\"#?]+)"[^>]*>([^<]{2,120})</a>', html, flags=re.IGNORECASE):
        txt = m.group(2).strip()
        if txt.lower() in {"view bounty", "learn more", "apply", "bounty", "bounties"}:
            continue
        if 2 <= len(txt) <= 120:
            names.add(txt)

    # 3) aria-label/title near /bug-bounty/
    for m in re.finditer(r'aria-label="([^"]{2,120})"[^>]*href="/bug-bounty/', html, flags=re.IGNORECASE):
        names.add(m.group(1).strip())
    for m in re.finditer(r'title="([^"]{2,120})"[^>]*href="/bug-bounty/', html, flags=re.IGNORECASE):
        names.add(m.group(1).strip())

    out = sorted(n for n in names if n and len(n) <= 120)
    print(f"✅ Immunefi: extracted {len(out)} raw program-name candidates")
    return out


def fetch_hackerone_program_names_best_effort() -> list[str]:
    """
    Best-effort scrape. HackerOne directory is often JS-rendered.
    We try several URLs and patterns.
    """
    urls = [
        "https://hackerone.com/directory/programs",
        "https://hackerone.com/directory/programs?type=public",
        "https://hackerone.com/directory/programs?sort=published_at:descending",
    ]

    names = set()
    for u in urls:
        try:
            r = SESSION.get(u, timeout=30)
            if r.status_code != 200:
                continue
            html = r.text
            # Try JSON-LD / embedded data fragments
            for m in re.finditer(r'"name"\s*:\s*"([^"]{2,120})"', html):
                nm = m.group(1).strip()
                # heuristic: avoid generic site strings
                if "HackerOne" in nm or "directory" in nm.lower():
                    continue
                names.add(nm)

            # Try visible h3/h2 card titles
            for m in re.finditer(r"<h3[^>]*>([^<]{2,120})</h3>", html):
                names.add(m.group(1).strip())

            # don’t hammer
            time.sleep(1.0)
        except Exception:
            continue

    out = sorted(n for n in names if n and len(n) <= 120)
    print(f"✅ HackerOne: extracted {len(out)} raw name candidates (often low due to JS)")
    return out


def main():
    if not LLAMA.exists():
        raise SystemExit(f"Missing {LLAMA}")

    llama = pd.read_csv(LLAMA)
    llama["name_norm"] = llama["name"].astype(str).map(norm)
    llama["slug_norm"] = llama["slug"].astype(str).map(norm)

    # lookup dicts
    llama_names = dict(zip(llama["name_norm"], llama["slug"]))
    llama_slugs = dict(zip(llama["slug_norm"], llama["slug"]))

    # --- Immunefi ---
    immunefi_programs = fetch_immunefi_program_names()
    immunefi_slugs = []
    for nm in immunefi_programs:
        s = best_slug_match(nm, llama_names, llama_slugs, min_ratio=0.86)
        if s:
            immunefi_slugs.append(s)
    immunefi_set = set(immunefi_slugs)
    print(f"✅ Immunefi: mapped to llama slugs = {len(immunefi_set)}")

    # --- HackerOne (best effort) ---
    h1_programs = fetch_hackerone_program_names_best_effort()
    h1_slugs = []
    for nm in h1_programs:
        s = best_slug_match(nm, llama_names, llama_slugs, min_ratio=0.88)
        if s:
            h1_slugs.append(s)
    h1_set = set(h1_slugs)
    print(f"✅ HackerOne: mapped to llama slugs = {len(h1_set)}")

    # Build master for all llama protocols
    out = llama[["slug"]].copy()
    out["has_immunefi"] = out["slug"].isin(immunefi_set).astype(int)
    out["has_hackerone_public"] = out["slug"].isin(h1_set).astype(int)
    out["has_bug_bounty_any"] = ((out["has_immunefi"] == 1) | (out["has_hackerone_public"] == 1)).astype(int)

    OUTDIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)

    print(f"✅ Wrote: {OUT} | rows={len(out)}")
    print("Stats:")
    print("  has_immunefi=1:", int(out["has_immunefi"].sum()))
    print("  has_hackerone_public=1:", int(out["has_hackerone_public"].sum()))
    print("  has_bug_bounty_any=1:", int(out["has_bug_bounty_any"].sum()))


if __name__ == "__main__":
    main()