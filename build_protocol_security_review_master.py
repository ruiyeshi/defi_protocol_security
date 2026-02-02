#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""build_protocol_security_review_master.py

Creates a *protocol-level* security review master by aggregating audit/contest events.

Inputs:
  - data_raw/audits/audit_events_long.csv
  - OPTIONAL (preferred if exists): data_raw/audits/audit_events_long_plus_firm_archives.csv
  - data_raw/llama_protocols.csv

Output:
  - data_raw/audits/protocol_security_review_master.csv

This script is designed to be robust to messy inputs and repeated reruns.
"""

from __future__ import annotations

from pathlib import Path
import os
import re
from typing import List, Optional

import pandas as pd

# -----------------------
# Paths
# -----------------------
ROOT = Path(__file__).resolve().parent
AUD = ROOT / "data_raw" / "audits"
LLAMA = ROOT / "data_raw" / "llama_protocols.csv"

EVENTS_AUG = AUD / "audit_events_long_plus_firm_archives.csv"
EVENTS_STD = AUD / "audit_events_long.csv"
EVENTS = EVENTS_AUG if EVENTS_AUG.exists() else EVENTS_STD

OUT = AUD / "protocol_security_review_master.csv"

# -----------------------
# Source definitions
# -----------------------
# You can override in shell/env:
#   STRICT_SOURCES="full,defisafety,certik,openzeppelin,trailofbits,diligence,quantstamp,firm_archive"
#   CONTEST_SOURCES="code4rena,sherlock"
STRICT_SOURCES_DEFAULT = [
    "full",
    "defisafety",
    "certik",
    "audit_report_firm",
    "audit_report",
    # firm archive labels used in your pipeline
    "openzeppelin",
    "trailofbits",
    "diligence",
    "consensys diligence",
    "quantstamp",
    "firm_archive",
]
CONTEST_SOURCES_DEFAULT = ["code4rena", "sherlock"]

STRICT_SOURCES = [
    s.strip().lower()
    for s in os.getenv("STRICT_SOURCES", ",".join(STRICT_SOURCES_DEFAULT)).split(",")
    if s.strip()
]
CONTEST_SOURCES = [
    s.strip().lower()
    for s in os.getenv("CONTEST_SOURCES", ",".join(CONTEST_SOURCES_DEFAULT)).split(",")
    if s.strip()
]

# -----------------------
# Firm tiering
# -----------------------
TOP_TIER_FIRMS = {
    "openzeppelin",
    "trail of bits",
    "trailofbits",
    "consensys diligence",
    "diligence",
    "quantstamp",
    "chainsecurity",
    "sigma prime",
    "runtime verification",
}

# -----------------------
# Helpers
# -----------------------

def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def to_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def normalize_firm_list(x) -> List[str]:
    """Parse an audit firm field into a normalized list of lowercase firm names."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return []
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return []

    # list-like in string form
    if s.startswith("[") and s.endswith("]"):
        items = re.findall(r"'([^']+)'|\"([^\"]+)\"", s)
        firms = [a or b for (a, b) in items]
    else:
        firms = re.split(r"[;,/|]+", s)

    firms = [f.strip().lower() for f in firms if f and f.strip()]

    # dedup preserve order
    seen = set()
    out: List[str] = []
    for f in firms:
        if f not in seen:
            out.append(f)
            seen.add(f)
    return out


def is_top_firm(f: str) -> bool:
    f = (f or "").strip().lower()
    if not f:
        return False
    for t in TOP_TIER_FIRMS:
        if t in f:
            return True
    return False


def safe_list_str(v) -> str:
    """Serialize list-like columns safely to a compact string for CSV."""
    if v is None:
        return "[]"
    if isinstance(v, list):
        return "[" + ",".join([str(x) for x in v]) + "]"
    if isinstance(v, str):
        return v
    if isinstance(v, float) and pd.isna(v):
        return "[]"
    # pandas/numpy arrays
    try:
        if hasattr(v, "tolist"):
            vv = v.tolist()
            if isinstance(vv, list):
                return "[" + ",".join([str(x) for x in vv]) + "]"
    except Exception:
        pass
    return str(v)


# -----------------------
# Main
# -----------------------

def main() -> None:
    if not EVENTS.exists():
        raise SystemExit(f"Missing {EVENTS}")
    if not LLAMA.exists():
        raise SystemExit(f"Missing {LLAMA}")

    ev = pd.read_csv(EVENTS)
    llama = pd.read_csv(LLAMA)

    if "source" not in ev.columns:
        raise SystemExit("audit_events_long*.csv must contain column: source")

    # Ensure slug columns exist
    for c in ["slug_final", "slug_mapped", "slug"]:
        if c not in ev.columns:
            ev[c] = pd.NA

    # Build slug_use (final > mapped > raw)
    ev["slug_use"] = (
        ev["slug_final"].astype("string").str.strip().replace({"": pd.NA})
        .fillna(ev["slug_mapped"].astype("string").str.strip().replace({"": pd.NA}))
        .fillna(ev["slug"].astype("string").str.strip().replace({"": pd.NA}))
    )

    # Recompute in_llama based on slug_use
    llama_slugs = set(llama["slug"].astype(str).str.strip())
    ev["in_llama"] = ev["slug_use"].astype(str).isin(llama_slugs).astype(int)

    # Normalize source
    ev["source"] = ev["source"].astype(str).str.lower().str.strip()

    # Filter to DeFi protocol universe
    ev = ev[(ev["slug_use"].notna()) & (ev["slug_use"] != "") & (ev["in_llama"] == 1)].copy()

    # Date column
    date_col = pick_col(ev, ["audit_date", "event_date", "date", "created_at"])
    if date_col is None:
        raise SystemExit("audit_events_long*.csv needs a date-like column (audit_date/event_date).")
    ev["event_dt"] = to_dt(ev[date_col])

    # Firm + score columns
    firm_col = pick_col(ev, ["audit_firm_raw", "audit_firm", "audit_firms", "firm", "firms"])
    score_col = pick_col(ev, ["audit_score", "audit_score_safety", "score"])

    if firm_col:
        ev["firms_norm"] = ev[firm_col].apply(normalize_firm_list)
    else:
        ev["firms_norm"] = [[] for _ in range(len(ev))]

    if score_col:
        ev["score_num"] = pd.to_numeric(ev[score_col], errors="coerce")
    else:
        ev["score_num"] = pd.NA

    # Classify sources
    ev["is_strict"] = ev["source"].isin(STRICT_SOURCES)
    ev["is_contest"] = ev["source"].isin(CONTEST_SOURCES)
    ev["is_broad"] = ev["is_strict"] | ev["is_contest"]

    # -----------------------
    # Aggregate per slug
    # -----------------------
    rows: List[dict] = []

    for slug, g in ev.groupby("slug_use"):
        dts = g["event_dt"]

        has_strict = int(g["is_strict"].any())
        has_contest = int(g["is_contest"].any())
        has_broad = int(g["is_broad"].any())

        num_audits_strict = int(g["is_strict"].sum())
        num_contests = int(g["is_contest"].sum())
        total_events = int(g["is_broad"].sum())

        last_audit = dts[g["is_strict"]].max()
        last_contest = dts[g["is_contest"]].max()
        last_broad = dts[g["is_broad"]].max()

        # Recency in years (strict) — only if we have a dated strict event
        if pd.isna(last_audit):
            recency_years = pd.NA
        else:
            ref = pd.Timestamp.now(tz="UTC")
            recency_years = (ref - last_audit).days / 365.25

        # Firms (strict only)
        strict_firms: List[str] = []
        for firms in g.loc[g["is_strict"], "firms_norm"].tolist():
            strict_firms.extend([f for f in firms if f])
        strict_firms_unique = list(dict.fromkeys(strict_firms))

        audit_firm_count = int(len(strict_firms_unique))
        any_top = int(any(is_top_firm(f) for f in strict_firms_unique)) if strict_firms_unique else 0
        firm_tier = "top-tier" if any_top else ("other" if strict_firms_unique else "unknown")

        # Score: max score observed for that protocol
        score_val = pd.to_numeric(g["score_num"], errors="coerce").max()

        strict_sources_seen = sorted(g.loc[g["is_strict"], "source"].unique().tolist())
        contest_sources_seen = sorted(g.loc[g["is_contest"], "source"].unique().tolist())
        all_sources_seen = sorted(g["source"].unique().tolist())

        rows.append({
            "slug": slug,

            # core
            "has_audit_strict": has_strict,
            "has_contest": has_contest,
            "has_security_review_broad": has_broad,

            # counts
            "num_audits_strict": num_audits_strict,
            "num_contests": num_contests,
            "audit_event_count_strict": num_audits_strict,  # backward compat
            "contest_event_count": num_contests,
            "security_review_event_count_total": total_events,

            # timing
            "last_audit_date_strict": last_audit,
            "last_contest_date": last_contest,
            "last_security_review_date": last_broad,
            "audit_recency_years_strict": recency_years,

            # quality
            "audit_firm_count": audit_firm_count,
            "any_top_firm": any_top,
            "audit_firm_tier": firm_tier,
            "audit_score": score_val,

            # provenance/debug
            "strict_sources_seen": strict_sources_seen,
            "contest_sources_seen": contest_sources_seen,
            "all_sources_seen": all_sources_seen,
            "audit_firms_strict": strict_firms_unique,
        })

    out = pd.DataFrame(rows)

    # Join llama metadata
    llama2 = llama[["slug", "name", "symbol", "category", "tvl", "chains"]].copy()
    out = out.merge(llama2, on="slug", how="left")

    # Reorder columns (friendly)
    front = [
        "slug", "name", "symbol", "category", "tvl", "chains",
        "has_audit_strict", "has_contest", "has_security_review_broad",
        "num_audits_strict", "num_contests",
        "audit_event_count_strict", "contest_event_count", "security_review_event_count_total",
        "last_audit_date_strict", "last_contest_date", "last_security_review_date",
        "audit_recency_years_strict",
        "audit_firm_count", "any_top_firm", "audit_firm_tier", "audit_score",
        "strict_sources_seen", "contest_sources_seen", "all_sources_seen", "audit_firms_strict",
    ]
    cols = [c for c in front if c in out.columns] + [c for c in out.columns if c not in front]
    out = out[cols]

    # Serialize list columns for CSV safety
    for c in ["strict_sources_seen", "contest_sources_seen", "all_sources_seen", "audit_firms_strict"]:
        if c in out.columns:
            out[c] = out[c].apply(safe_list_str)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)

    print(f"✅ Wrote: {OUT} | rows={len(out)}")
    print("Stats:")
    print("  unique slugs:", int(out["slug"].nunique()))
    print("  has_audit_strict=1:", int(out["has_audit_strict"].sum()))
    print("  has_contest=1:", int(out["has_contest"].sum()))
    print("  has_security_review_broad=1:", int(out["has_security_review_broad"].sum()))


if __name__ == "__main__":
    main()