#!/usr/bin/env python3
"""
Build a dated security events table (audits / reviews / contests).

Outputs:
  data_raw/security/security_events.csv

This is the canonical source for timing variables like:
  audited_by_year_end, reviewed_by_year_end, contest_by_year_end
"""

from __future__ import annotations
import argparse
import re
from pathlib import Path
import pandas as pd

# This script lives under: <repo>/data_raw/
BASE = Path(__file__).resolve().parent
AUD = BASE / "audits"
SECURITY_DIR = BASE / "security"

def parse_dt(s: str | None) -> pd.Timestamp | pd.NaT:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return pd.NaT
    # parse as UTC when possible
    return pd.to_datetime(str(s), utc=True, errors="coerce")

def ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df

def load_code4rena(csv_path: Path) -> pd.DataFrame:
    """
    Expect your existing file:
      data_raw/audits/audit_events_code4rena.csv
    It typically includes contest metadata; use the contest end date as event_date_dt.
    """
    df = pd.read_csv(csv_path, low_memory=False)
    df = ensure_cols(df, ["slug", "url", "start_date", "end_date", "contest", "platform"])
    df["event_type"] = "contest"
    df["source"] = "code4rena"
    df["firm_or_platform"] = "Code4rena"
    # prefer end_date; fallback start_date
    df["event_date_raw"] = df["end_date"].fillna(df["start_date"])
    df["event_date_dt"] = df["event_date_raw"].map(parse_dt)
    df["event_date_parseable"] = df["event_date_dt"].notna().astype(int)
    return df[["slug","event_type","source","firm_or_platform","event_date_raw","event_date_dt","event_date_parseable","url"]]

def load_sherlock(csv_path: Path) -> pd.DataFrame:
    """
    Expect:
      data_raw/audits/audit_events_sherlock.csv
    Use contest end date as event date.
    """
    df = pd.read_csv(csv_path, low_memory=False)
    df = ensure_cols(df, ["slug", "url", "start_date", "end_date"])
    df["event_type"] = "contest"
    df["source"] = "sherlock"
    df["firm_or_platform"] = "Sherlock"
    df["event_date_raw"] = df["end_date"].fillna(df["start_date"])
    df["event_date_dt"] = df["event_date_raw"].map(parse_dt)
    df["event_date_parseable"] = df["event_date_dt"].notna().astype(int)
    return df[["slug","event_type","source","firm_or_platform","event_date_raw","event_date_dt","event_date_parseable","url"]]

def load_github_audits(csv_path: Path) -> pd.DataFrame:
    """
    Expect:
      data_raw/audits/audit_events_github_reports.csv or ..._strict.csv
    Use commit_date / report_date if present; else leave unparseable.
    """
    df = pd.read_csv(csv_path, low_memory=False)
    # try multiple possible date column names
    date_col = next((c for c in ["commit_date","report_date","date","published_at"] if c in df.columns), None)
    df = ensure_cols(df, ["slug","url","firm","repo"])
    df["event_type"] = "audit"
    df["source"] = "github"
    df["firm_or_platform"] = df.get("firm", pd.NA)
    df["event_date_raw"] = df[date_col] if date_col else pd.NA
    df["event_date_dt"] = df["event_date_raw"].map(parse_dt)
    df["event_date_parseable"] = df["event_date_dt"].notna().astype(int)
    return df[["slug","event_type","source","firm_or_platform","event_date_raw","event_date_dt","event_date_parseable","url"]]

def load_firm_archives(csv_path: Path) -> pd.DataFrame:
    """
    Expect:
      data_raw/audits/audit_events_firm_archives_enriched.csv
    This often includes publication dates (sometimes).
    """
    df = pd.read_csv(csv_path, low_memory=False)
    date_col = next((c for c in ["date","published_at","report_date"] if c in df.columns), None)
    df = ensure_cols(df, ["slug","url","firm"])
    df["event_type"] = "audit"
    df["source"] = "firm_archive"
    df["firm_or_platform"] = df.get("firm", pd.NA)
    df["event_date_raw"] = df[date_col] if date_col else pd.NA
    df["event_date_dt"] = df["event_date_raw"].map(parse_dt)
    df["event_date_parseable"] = df["event_date_dt"].notna().astype(int)
    return df[["slug","event_type","source","firm_or_platform","event_date_raw","event_date_dt","event_date_parseable","url"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(SECURITY_DIR / "security_events.csv"))
    args = ap.parse_args()

    parts = []
    # contest sources (timing-rich)
    c4 = AUD / "audit_events_code4rena.csv"
    sh = AUD / "audit_events_sherlock.csv"
    if c4.exists(): parts.append(load_code4rena(c4))
    if sh.exists(): parts.append(load_sherlock(sh))

    # GitHub audit reports (date quality varies)
    for fn in ["audit_events_github_reports.csv", "audit_events_github_strict.csv"]:
        p = AUD / fn
        if p.exists():
            parts.append(load_github_audits(p))
            break

    # Firm archives (prefer enriched if present)
    for fn in ["audit_events_firm_archives_enriched.csv", "audit_events_firm_archives.csv"]:
        p = AUD / fn
        if p.exists():
            parts.append(load_firm_archives(p))
            break

    if not parts:
        raise SystemExit(f"No input event files found under: {AUD}")

    ev = pd.concat(parts, ignore_index=True)
    ev["slug"] = ev["slug"].astype(str).str.strip()
    ev = ev.replace({"": pd.NA})
    ev = ev.dropna(subset=["slug"])

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ev.to_csv(out_path, index=False)

    print("Saved:", out_path)
    print("Rows:", len(ev))
    print("Parseable date share:", float(ev["event_date_parseable"].mean()))

if __name__ == "__main__":
    main()