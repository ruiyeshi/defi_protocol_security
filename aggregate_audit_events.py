#!/usr/bin/env python3
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv(".env")

IN_EVENTS = os.path.join("data_raw", "audits", "audit_events_long.csv")
OUT_MASTER = os.path.join("data_raw", "audits", "audit_master_from_events.csv")

# Your agreed definitions:
STRICT_SOURCES_DEFAULT = {"full", "defisafety", "certik", "audit_report_firm", "github_strict_search"}
CONTEST_SOURCES_DEFAULT = {"code4rena", "sherlock"}

TOP_FIRMS = {
    "OpenZeppelin", "Trail of Bits", "Quantstamp", "ConsenSys Diligence",
    "Sigma Prime", "Runtime Verification", "CertiK"
}


def parse_dt(series: pd.Series) -> pd.Series:
    # robust parse
    return pd.to_datetime(series, errors="coerce", utc=True)


def pick_slug_use(df: pd.DataFrame) -> pd.Series:
    # Your real columns: slug_final, slug_mapped, slug
    s = None
    for c in ["slug_final", "slug_mapped", "slug"]:
        if c in df.columns:
            s = df[c] if s is None else s.fillna(df[c])
    if s is None:
        s = pd.Series([pd.NA] * len(df))
    s = s.astype("string").str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return s


def normalize_firm(x: str) -> str:
    if not isinstance(x, str):
        return ""
    return x.strip()


def any_top_firm(firms) -> int:
    if not firms:
        return 0
    for f in firms:
        if f in TOP_FIRMS:
            return 1
    return 0


def agg_group(g: pd.DataFrame) -> pd.Series:
    # sources present in this slug
    srcs = sorted(set(g["source"].dropna().astype(str)))

    strict_mask = g["source"].isin(STRICT_SOURCES_DEFAULT)
    contest_mask = g["source"].isin(CONTEST_SOURCES_DEFAULT)

    has_audit_strict = int(strict_mask.any())
    has_contest = int(contest_mask.any())
    has_security_review_broad = int(has_audit_strict or has_contest)

    # counts
    audit_event_count_strict = int(strict_mask.sum())
    contest_event_count = int(contest_mask.sum())
    security_review_event_count_total = int((strict_mask | contest_mask).sum())

    # last dates
    dt = g["audit_date_dt"]
    last_audit_date_strict = dt[strict_mask].max()
    last_contest_date = dt[contest_mask].max()
    last_security_review_date = dt[(strict_mask | contest_mask)].max()

    # firm list (strict only for “audit_firms_strict”)
    firms_strict = sorted(set(g.loc[strict_mask, "audit_firm_raw"].dropna().astype(str).map(normalize_firm)))
    firms_all = sorted(set(g["audit_firm_raw"].dropna().astype(str).map(normalize_firm)))

    # DeFiSafety numeric score if present (take max/latest; here: max)
    score = pd.to_numeric(g.loc[strict_mask, "audit_score"], errors="coerce")
    audit_score = score.max() if score.notna().any() else pd.NA

    return pd.Series({
        "slug": g["slug_use"].iloc[0],
        "has_audit_strict": has_audit_strict,
        "has_contest": has_contest,
        "has_security_review_broad": has_security_review_broad,
        "audit_event_count_strict": audit_event_count_strict,
        "contest_event_count": contest_event_count,
        "security_review_event_count_total": security_review_event_count_total,
        "last_audit_date_strict": last_audit_date_strict,
        "last_contest_date": last_contest_date,
        "last_security_review_date": last_security_review_date,
        "audit_firms_strict": firms_strict,
        "audit_firms_all": firms_all,
        "audit_firm_count_strict": len(firms_strict),
        "any_top_firm_strict": any_top_firm(firms_strict),
        "sources": srcs,
    })


def main():
    if not os.path.exists(IN_EVENTS):
        raise FileNotFoundError(f"Missing: {IN_EVENTS}")

    ev = pd.read_csv(IN_EVENTS)
    ev["slug_use"] = pick_slug_use(ev)

    # Use only events that map to a slug AND are in_llama==1 (you agreed)
    if "in_llama" in ev.columns:
        ev = ev[(ev["slug_use"].notna()) & (ev["in_llama"] == 1)]
    else:
        ev = ev[ev["slug_use"].notna()]

    if "audit_firm_raw" not in ev.columns:
        ev["audit_firm_raw"] = pd.NA
    if "audit_score" not in ev.columns:
        ev["audit_score"] = pd.NA
    if "audit_date" not in ev.columns:
        ev["audit_date"] = pd.NA

    ev["audit_date_dt"] = parse_dt(ev["audit_date"])
    ev["source"] = ev["source"].astype("string").str.strip()

    out = ev.groupby("slug_use", dropna=False).apply(agg_group).reset_index(drop=True)

    os.makedirs(os.path.dirname(OUT_MASTER), exist_ok=True)
    out.to_csv(OUT_MASTER, index=False)

    print(f"✅ Wrote {OUT_MASTER} | rows={len(out)}")
    print("Stats:")
    print("  unique slugs:", out["slug"].nunique())
    print("  has_audit_strict=1:", int(out["has_audit_strict"].sum()))
    print("  has_contest=1:", int(out["has_contest"].sum()))
    print("  has_security_review_broad=1:", int(out["has_security_review_broad"].sum()))
    print("  sources:", sorted(set(sum(out["sources"].apply(lambda x: x if isinstance(x, list) else []).tolist(), []))))


if __name__ == "__main__":
    main()