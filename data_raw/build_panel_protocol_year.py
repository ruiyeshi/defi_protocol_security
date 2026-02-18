# data_raw/build_panel_protocol_year.py
import argparse
import os
import pandas as pd
import numpy as np

def get_series(df: pd.DataFrame, col: str, default=0):
    """Return df[col] if present else a constant Series aligned to df.index."""
    if col in df.columns:
        return df[col]
    return pd.Series(default, index=df.index)

def norm_key(x: str) -> str:
    """Normalize protocol names for fuzzy joins (lower, strip, alnum only)."""
    if x is None:
        return ""
    s = str(x).lower().strip()
    # keep only alphanumerics
    return "".join(ch for ch in s if ch.isalnum())

def parse_dt(s):
    """Parse datetimes and return tz-naive (UTC-normalized) timestamps.

    Accepts scalars, Series, or array-like. Returns pandas Series/DatetimeIndex
    that is tz-naive (timezone removed) so arithmetic won't crash.
    """
    dt = pd.to_datetime(s, utc=True, errors="coerce")

    # Series/Index -> strip tz
    if hasattr(dt, "dt"):
        return dt.dt.tz_convert(None)

    # Scalar Timestamp -> strip tz if present
    try:
        return dt.tz_convert(None) if getattr(dt, "tzinfo", None) is not None else dt
    except Exception:
        return dt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--m1", required=True, help="data_clean/m1_exploit_audit.csv")
    ap.add_argument("--llama", required=True, help="data_clean/llama_master_dedup.csv (protocol universe)")
    ap.add_argument(
        "--audits",
        required=True,
        help="Audit master (slug-level), e.g. data_raw/audits/audit_master_with_slug_defi_only_dedup.csv",
    )
    ap.add_argument(
        "--reviews",
        required=False,
        default=None,
        help="Optional security review master with strict/broad decomposition, e.g. data_raw/audits/protocol_security_review_master.csv",
    )
    ap.add_argument("--out", required=True, help="data_clean/panel_protocol_year.csv")
    ap.add_argument("--min_year", type=int, default=None)
    ap.add_argument("--max_year", type=int, default=None)
    args = ap.parse_args()

    m1 = pd.read_csv(args.m1)
    llama = pd.read_csv(args.llama)
    audits = pd.read_csv(args.audits)
    reviews = None
    if args.reviews:
        reviews = pd.read_csv(args.reviews)

    # --- Basic cleanup
    if "slug" not in llama.columns:
        raise ValueError("Expected slug in llama universe file")

    # Parse exploit date & year
    m1["exploit_dt"] = parse_dt(m1.get("exploit_dt", m1.get("exploit_date")))
    m1["year"] = m1["exploit_dt"].dt.year

    # If m1 doesn't already contain a slug column, try to map it from protocol name -> llama slug
    if "slug" not in m1.columns:
        # pick the first plausible protocol name column
        candidate_name_cols = [
            "protocol_name_raw",
            "protocol_name",
            "protocol",
            "name",
            "project",
            "protocol_name_raw_x",
            "protocol_name_raw_y",
        ]
        name_col = next((c for c in candidate_name_cols if c in m1.columns), None)
        if name_col is None:
            raise ValueError(
                "m1 exploit file has no 'slug' column and no recognizable protocol name column. "
                f"Available columns: {list(m1.columns)}"
            )

        ll_map = llama[["name", "slug"]].copy()
        ll_map["_k"] = ll_map["name"].map(norm_key)
        ll_map = ll_map.dropna(subset=["slug"]).drop_duplicates(subset=["_k"], keep="first")

        m1["_k"] = m1[name_col].map(norm_key)
        m1 = m1.merge(ll_map[["_k", "slug"]], on="_k", how="left").drop(columns=["_k"])

    # Keep only DeFi protocols that map to a slug (this drops CeFi/infra unmatched events)
    m1_defi = m1[m1["slug"].notna() & (m1["slug"].astype(str).str.len() > 0)].copy()

    # Aggregate exploits -> protocol-year
    g = m1_defi.groupby(["slug", "year"], dropna=False)
    expl_agg = g.agg(
        exploited_this_year=("slug", "size"),
        exploit_count=("slug", "size"),
        total_loss_usd=("loss_usd", "sum"),
        max_loss_usd=("loss_usd", "max"),
    ).reset_index()

    expl_agg["exploited_this_year"] = (expl_agg["exploited_this_year"] > 0).astype(int)

    # --- Panel year range
    min_y = int(np.nanmin(expl_agg["year"])) if args.min_year is None else args.min_year
    max_y = int(np.nanmax(expl_agg["year"])) if args.max_year is None else args.max_year

    slugs = llama[["slug", "name", "category_llama", "tvl", "chains_llama"]].copy()
    years = pd.DataFrame({"year": list(range(min_y, max_y + 1))})

    panel = slugs.assign(_k=1).merge(years.assign(_k=1), on="_k").drop(columns=["_k"])

    # Join exploit outcomes
    panel = panel.merge(expl_agg, on=["slug", "year"], how="left")

    # Fill “no exploit” cells
    panel["exploited_this_year"] = panel["exploited_this_year"].fillna(0).astype(int)
    panel["exploit_count"] = panel["exploit_count"].fillna(0).astype(int)
    panel["total_loss_usd"] = panel["total_loss_usd"].fillna(0.0)
    panel["max_loss_usd"] = panel["max_loss_usd"].fillna(0.0)

    # --- Join audit covariates by slug (left join)
    # 1) From audit master: compute audited_by_year_end using last_audit_date (if available)
    audits_small = audits.copy()

    # Normalize date columns to tz-naive
    if "last_audit_date" in audits_small.columns:
        audits_small["last_audit_date_dt"] = parse_dt(audits_small["last_audit_date"])
    elif "last_audit_date_dt" in audits_small.columns:
        audits_small["last_audit_date_dt"] = parse_dt(audits_small["last_audit_date_dt"])
    else:
        audits_small["last_audit_date_dt"] = pd.NaT

    keep_audit = [c for c in [
        "slug",
        "has_audit",
        "audit_firm_count",
        "any_top_firm",
        "audit_score",
        "audit_score_max",
        "audit_score_mean",
        "last_audit_date_dt",
    ] if c in audits_small.columns]

    audits_small = audits_small[keep_audit].drop_duplicates(subset=["slug"], keep="first")
    panel = panel.merge(audits_small, on="slug", how="left")

    # 2) From review master (optional): strict/broad decomposition + strict dates
    if reviews is not None:
        rv = reviews.copy()
        # Parse strict and broad dates if present
        if "last_audit_date_strict" in rv.columns:
            rv["last_audit_date_strict_dt"] = parse_dt(rv["last_audit_date_strict"])
        else:
            rv["last_audit_date_strict_dt"] = pd.NaT

        if "last_security_review_date" in rv.columns:
            rv["last_security_review_date_dt"] = parse_dt(rv["last_security_review_date"])
        else:
            rv["last_security_review_date_dt"] = pd.NaT

        if "last_contest_date" in rv.columns:
            rv["last_contest_date_dt"] = parse_dt(rv["last_contest_date"])
        else:
            rv["last_contest_date_dt"] = pd.NaT

        keep_rv = [c for c in [
            "slug",
            "has_audit_strict",
            "num_audits_strict",
            "has_contest",
            "num_contests",
            "has_security_review_broad",
            "security_review_event_count_total",
            "audit_firm_count",
            "any_top_firm",
            "audit_firm_tier",
            "audit_score",
            "strict_sources_seen",
            "contest_sources_seen",
            "all_sources_seen",
            "audit_firms_strict",
            "last_audit_date_strict_dt",
            "last_security_review_date_dt",
            "last_contest_date_dt",
        ] if c in rv.columns]
        rv = rv[keep_rv].drop_duplicates(subset=["slug"], keep="first")
        panel = panel.merge(rv, on="slug", how="left", suffixes=("", "_rv"))

    # --- Year-end timestamps (tz-naive) and time-since calculations
    panel["year_end"] = pd.to_datetime(panel["year"].astype(str) + "-12-31", errors="coerce")

    # -----------------------------
    # Audit variables: separate time-invariant vs time-varying
    # -----------------------------
    # Time-invariant "audited ever" (available even if dates are missing)
    panel["audited_ever"] = get_series(panel, "has_audit", default=0).fillna(0).astype(int)

    # Quality flag: do we have a parseable audit date for this slug?
    if "last_audit_date_dt" in panel.columns:
        panel["audit_date_available"] = panel["last_audit_date_dt"].notna().astype(int)
    else:
        panel["audit_date_available"] = 0

    # Time-varying audited_by_year_end is ONLY defined when we have audit dates; otherwise NaN (do not pretend timing)
    if "last_audit_date_dt" in panel.columns and panel["last_audit_date_dt"].notna().any():
        panel["time_since_last_audit_days"] = (panel["year_end"] - panel["last_audit_date_dt"]).dt.days
        panel.loc[panel["time_since_last_audit_days"] < 0, "time_since_last_audit_days"] = np.nan
        panel["audited_by_year_end"] = np.where(
            panel["last_audit_date_dt"].notna(),
            (panel["last_audit_date_dt"] <= panel["year_end"]).astype(int),
            np.nan,
        )
        # Note: audited_by_year_end is intentionally NaN when audit dates are unavailable (see audit_date_available).
    else:
        panel["time_since_last_audit_days"] = np.nan
        panel["audited_by_year_end"] = np.nan

    # audited_strict_by_year_end: if strict date exists use it; otherwise treat has_audit_strict as time-invariant fallback
    if "last_audit_date_strict_dt" in panel.columns and panel["last_audit_date_strict_dt"].notna().any():
        panel["audited_strict_by_year_end"] = (
            panel["last_audit_date_strict_dt"].notna() & (panel["last_audit_date_strict_dt"] <= panel["year_end"])
        ).astype(int)
    else:
        panel["audited_strict_by_year_end"] = get_series(panel, "has_audit_strict", default=0).fillna(0).astype(int)

    # broad security review by year end: use last_security_review_date if available else fallback to has_security_review_broad
    if "last_security_review_date_dt" in panel.columns and panel["last_security_review_date_dt"].notna().any():
        panel["reviewed_broad_by_year_end"] = (
            panel["last_security_review_date_dt"].notna() & (panel["last_security_review_date_dt"] <= panel["year_end"])
        ).astype(int)
    else:
        panel["reviewed_broad_by_year_end"] = get_series(panel, "has_security_review_broad", default=0).fillna(0).astype(int)

    # --- Lag variables for regressions (protocol-year panel)
    panel = panel.sort_values(["slug", "year"]).reset_index(drop=True)

    # Lags of time-varying measures (may be NaN when dates are unavailable)
    panel["audited_strict_lag1"] = panel.groupby("slug")["audited_strict_by_year_end"].shift(1)
    panel["audited_any_lag1"] = panel.groupby("slug")["audited_by_year_end"].shift(1)
    panel["reviewed_broad_lag1"] = panel.groupby("slug")["reviewed_broad_by_year_end"].shift(1)

    # Lag of time-invariant audited_ever (always defined)
    panel["audited_ever_lag1"] = panel.groupby("slug")["audited_ever"].shift(1)

    # Keep lags as NaN for first year; modeling code can decide to fill or drop.

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    panel.to_csv(args.out, index=False)
    print("Saved:", args.out)
    print("Panel rows:", len(panel))
    print("Years:", min_y, "to", max_y)
    print("Protocols:", panel["slug"].nunique())
    print("Exploit-year rate:", panel["exploited_this_year"].mean())

    def _rate(col: str):
        if col not in panel.columns:
            return None
        s = panel[col]
        # pandas mean ignores NaN; report both non-null share and conditional mean
        nonnull = float(s.notna().mean())
        mean_cond = float(s.dropna().mean()) if s.notna().any() else float("nan")
        return nonnull, mean_cond

    if "audited_ever" in panel.columns:
        print("Audited(ever) mean:", float(panel["audited_ever"].mean()))

    if "audit_date_available" in panel.columns:
        print("Audit date available share:", float(panel["audit_date_available"].mean()))

    r = _rate("audited_by_year_end")
    if r is not None:
        nonnull, mean_cond = r
        print("Audited(any) by year-end: non-null share=", nonnull, "| mean (conditional on non-null)=", mean_cond)

    r = _rate("audited_strict_by_year_end")
    if r is not None:
        nonnull, mean_cond = r
        print("Audited(strict) by year-end: non-null share=", nonnull, "| mean (conditional on non-null)=", mean_cond)

    r = _rate("reviewed_broad_by_year_end")
    if r is not None:
        nonnull, mean_cond = r
        print("Reviewed(broad) by year-end: non-null share=", nonnull, "| mean (conditional on non-null)=", mean_cond)

if __name__ == "__main__":
    main()