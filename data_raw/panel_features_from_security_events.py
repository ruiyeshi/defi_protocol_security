#!/usr/bin/env python3
"""
Compute timing indicators from security_events.csv and merge into your panel.

Reads:
  data_raw/security/security_events.csv
  data_raw/panel/panel_protocol_year_canonical.csv

Writes:
  data_raw/panel/panel_protocol_year_with_security_events.csv
"""

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", default="data_raw/security/security_events.csv",
                    help="Long-form security events table (slug,event_type,event_date_dt,source,...) to merge into the panel")
    ap.add_argument("--panel", default="data_raw/panel/panel_protocol_year_canonical.csv")
    ap.add_argument("--out", default="data_raw/panel/panel_protocol_year_with_security_events.csv")

    # Fallback inputs: if --events does not exist, we can build a minimal security_events.csv
    # from your existing aggregated masters.
    ap.add_argument("--review_master", default="data_raw/audits/protocol_security_review_master.csv",
                    help="Aggregated review/audit/contest master (has last_*_date columns)")
    ap.add_argument("--audit_master", default="data_raw/audits/audit_master_with_slug_defi_only_dedup.csv",
                    help="Aggregated audit master (has has_audit + last_audit_date, but dates may be sparse)")
    args = ap.parse_args()

    p = pd.read_csv(args.panel, low_memory=False)

    events_path = Path(args.events)
    if events_path.exists():
        ev = pd.read_csv(events_path, low_memory=False)
    else:
        # Build a minimal long-form security events table from aggregated sources.
        rows = []

        # (A) protocol_security_review_master.csv: provides contest + broad review dates (and sometimes strict audit dates).
        rm_path = Path(args.review_master)
        if rm_path.exists():
            rm = pd.read_csv(rm_path, low_memory=False)

            # Contest events (best timing coverage in your current data)
            if "last_contest_date" in rm.columns:
                tmp = rm[["slug", "last_contest_date"]].copy()
                tmp = tmp.rename(columns={"last_contest_date": "event_date_raw"})
                tmp["event_type"] = "contest"
                tmp["source"] = "protocol_security_review_master"
                tmp["firm_or_platform"] = "contest_platform"
                tmp["url"] = ""
                rows.append(tmp)

            # Broad security review events (audit/review/contest combined in that master)
            if "last_security_review_date" in rm.columns:
                tmp = rm[["slug", "last_security_review_date"]].copy()
                tmp = tmp.rename(columns={"last_security_review_date": "event_date_raw"})
                tmp["event_type"] = "review"
                tmp["source"] = "protocol_security_review_master"
                tmp["firm_or_platform"] = "review_or_audit"
                tmp["url"] = ""
                rows.append(tmp)

            # Strict audit last date (may be sparse; still include if present)
            if "last_audit_date_strict" in rm.columns:
                tmp = rm[["slug", "last_audit_date_strict"]].copy()
                tmp = tmp.rename(columns={"last_audit_date_strict": "event_date_raw"})
                tmp["event_type"] = "audit"
                tmp["source"] = "protocol_security_review_master"
                tmp["firm_or_platform"] = "audit_firm"
                tmp["url"] = ""
                rows.append(tmp)

        # (B) audit_master_with_slug...: mostly provides audited_ever; dates are often missing, but include when parseable.
        am_path = Path(args.audit_master)
        if am_path.exists():
            am = pd.read_csv(am_path, low_memory=False)
            if "last_audit_date" in am.columns:
                tmp = am[["slug", "last_audit_date"]].copy()
                tmp = tmp.rename(columns={"last_audit_date": "event_date_raw"})
                tmp["event_type"] = "audit"
                tmp["source"] = "audit_master"
                tmp["firm_or_platform"] = "audit_firm"
                tmp["url"] = ""
                rows.append(tmp)

        if not rows:
            raise FileNotFoundError(
                f"{events_path} not found, and no fallback inputs found at {rm_path} or {am_path}. "
                "Provide --events or ensure the masters exist."
            )

        ev = pd.concat(rows, ignore_index=True)
        ev["event_date_dt"] = pd.to_datetime(ev["event_date_raw"], utc=True, errors="coerce")
        ev["event_date_parseable"] = ev["event_date_dt"].notna().astype(int)

        # keep only parseable dates for timing features
        ev = ev[ev["event_date_parseable"] == 1].copy()

        events_path.parent.mkdir(parents=True, exist_ok=True)
        ev.to_csv(events_path, index=False)
        print("Built and saved:", events_path, "| rows:", len(ev))

    ev["event_date_dt"] = pd.to_datetime(ev["event_date_dt"], utc=True, errors="coerce")
    ev["year"] = ev["event_date_dt"].dt.year

    # For each slug-year, mark whether an event happened by year end
    # Use max(event_date_dt) up to Dec 31 of year
    p["year_end"] = pd.to_datetime(p["year"].astype(int).astype(str) + "-12-31", utc=True)

    # helper: last event date by type up to year end
    def last_date_by_type(event_type: str):
        tmp = ev[ev["event_type"] == event_type].dropna(subset=["event_date_dt"]).copy()
        tmp = tmp.sort_values(["slug","event_date_dt"])
        return tmp

    audits = last_date_by_type("audit")
    contests = last_date_by_type("contest")
    reviews = last_date_by_type("review")  # present if built from protocol_security_review_master

    def merge_last_date(panel: pd.DataFrame, tmp: pd.DataFrame, col_prefix: str):
        # For efficiency, merge on slug then filter by year_end using groupby/ffill
        m = panel[["slug","year","year_end"]].merge(tmp[["slug","event_date_dt"]], on="slug", how="left")
        m = m[m["event_date_dt"].notna()]
        m = m[m["event_date_dt"] <= m["year_end"]]
        last = (m.groupby(["slug","year"], as_index=False)["event_date_dt"].max()
                  .rename(columns={"event_date_dt": f"last_{col_prefix}_date"}))
        panel = panel.merge(last, on=["slug","year"], how="left")
        panel[f"{col_prefix}_by_year_end"] = panel[f"last_{col_prefix}_date"].notna().astype(int)
        return panel

    p = merge_last_date(p, audits, "audit")
    p = merge_last_date(p, contests, "contest")
    p = merge_last_date(p, reviews, "review")

    # “broad security event” = audit OR contest OR review
    p["security_event_by_year_end"] = (
        (p["audit_by_year_end"] == 1) | (p["contest_by_year_end"] == 1) | (p["review_by_year_end"] == 1)
    ).astype(int)

    # lagged versions (recommended for regressions)
    p = p.sort_values(["slug","year"])
    for c in ["audit_by_year_end","contest_by_year_end","security_event_by_year_end"]:
        p[c.replace("_by_year_end","_lag1")] = p.groupby("slug")[c].shift(1)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    p.to_csv(out, index=False)
    print("Saved:", out)
    print("audit_by_year_end mean:", p["audit_by_year_end"].mean())
    print("contest_by_year_end mean:", p["contest_by_year_end"].mean())
    print("review_by_year_end mean:", p["review_by_year_end"].mean())
    print("security_event_by_year_end mean:", p["security_event_by_year_end"].mean())

if __name__ == "__main__":
    main()