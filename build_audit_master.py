#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent

# Your actual locations (based on your terminal output)
INPUTS = [
    ("api",    ROOT / "data_raw" / "contracts" / "audit_metadata_api.csv"),
    ("certik", ROOT / "data_raw" / "contracts" / "audit_metadata_certik.csv"),
    ("full",   ROOT / "data_raw" / "contracts" / "audit_metadata_full.csv"),
]

OUT_DIR = ROOT / "data_raw" / "audits"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT = OUT_DIR / "audit_master.csv"

# Top firms list (edit freely)
TOP_FIRMS = {
    "trail of bits", "openzeppelin", "quantstamp", "certik", "chainsecurity",
    "peckshield", "consensys diligence", "sigma prime", "runtime verification",
    "halborn", "least authority", "slowmist"
}

SEP_RE = re.compile(r"[;,|/]|(?:\s+&\s+)|(?:\s+and\s+)", re.IGNORECASE)

def clean_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def normalize_firms(x):
    s = clean_str(x)
    if not s:
        return []
    parts = [p.strip() for p in SEP_RE.split(s) if p.strip()]
    parts = [re.sub(r"\s+", " ", p).strip() for p in parts]
    return parts

def parse_score(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    m = re.search(r"(\d+(\.\d+)?)", s)
    return float(m.group(1)) if m else None

def parse_date(x):
    if pd.isna(x):
        return pd.NaT
    s = str(x).strip()
    if not s:
        return pd.NaT
    return pd.to_datetime(s, errors="coerce", utc=True)

def pick_col(df, candidates):
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None

def normalize_one(source, path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"⚠️ Missing: {path} (skipping)")
        return pd.DataFrame()

    df = pd.read_csv(path)
    proto_col = pick_col(df, ["slug", "protocol", "protocol_name", "name"])
    if proto_col is None:
        raise ValueError(f"[{source}] No protocol column found. cols={df.columns.tolist()}")

    chain_col = pick_col(df, ["chain", "chains"])
    cat_col   = pick_col(df, ["category"])
    tvl_col   = pick_col(df, ["tvl"])
    sym_col   = pick_col(df, ["symbol", "ticker"])

    firm_col  = pick_col(df, ["audit_firm", "auditor", "audit_firm_safety", "firm", "audits"])
    score_col = pick_col(df, ["audit_score", "audit_score_safety", "security_score", "score"])
    date_col  = pick_col(df, ["audit_date", "audit_date_safety", "last_audit_date", "date"])

    out = pd.DataFrame({
        "source": source,
        "protocol_name_raw": df[proto_col].astype(str).str.strip(),
        "chain": df[chain_col].astype(str).str.strip() if chain_col else "",
        "category": df[cat_col].astype(str).str.strip() if cat_col else "",
        "tvl": pd.to_numeric(df[tvl_col], errors="coerce") if tvl_col else pd.NA,
        "symbol": df[sym_col].astype(str).str.strip() if sym_col else "",
        "audit_firms": df[firm_col].apply(normalize_firms) if firm_col else [[]]*len(df),
        "audit_score": df[score_col].apply(parse_score) if score_col else [None]*len(df),
        "audit_date": df[date_col].apply(parse_date) if date_col else [pd.NaT]*len(df),
    })

    return out

def main():
    frames = [normalize_one(src, p) for src, p in INPUTS]
    frames = [f for f in frames if not f.empty]
    if not frames:
        raise SystemExit("No audit inputs found.")

    long = pd.concat(frames, ignore_index=True)

    # Aggregate per protocol_name_raw (one row per protocol)
    def agg_firms(series):
        s = set()
        for lst in series:
            for f in lst:
                if f:
                    s.add(f)
        return sorted(s)

    def agg_chains(series):
        s = set()
        for x in series:
            x = clean_str(x)
            if x:
                s.add(x)
        return sorted(s)

    def mode_nonempty(series):
        series = [clean_str(x) for x in series if clean_str(x)]
        if not series:
            return ""
        return pd.Series(series).mode().iat[0] if not pd.Series(series).mode().empty else series[0]

    grouped = (
        long.groupby("protocol_name_raw", as_index=False)
            .agg(
                symbol=("symbol", mode_nonempty),
                category=("category", mode_nonempty),
                chains=("chain", agg_chains),
                tvl_max=("tvl", "max"),

                audit_firms=("audit_firms", agg_firms),
                audit_score_max=("audit_score", "max"),
                audit_score_mean=("audit_score", "mean"),
                last_audit_date=("audit_date", "max"),
                audit_sources=("source", lambda x: sorted(set(x))),
            )
    )

    # Derived variables
    grouped["audit_firm_count"] = grouped["audit_firms"].apply(len)
    grouped["has_audit"] = (grouped["audit_firm_count"] > 0).astype(int)

    grouped["audit_score"] = grouped["audit_score_max"].where(
        grouped["audit_score_max"].notna(),
        grouped["audit_score_mean"]
    )

    def any_top(firms):
        firms_norm = {f.strip().lower() for f in firms}
        return int(len(firms_norm.intersection(TOP_FIRMS)) > 0)

    grouped["any_top_firm"] = grouped["audit_firms"].apply(any_top)

    # Save
    grouped.to_csv(OUT, index=False)
    print(f"✅ Wrote: {OUT} | rows={len(grouped)}")
    print("Stats:")
    print("  unique protocols:", len(grouped))
    print("  has_audit=1:", int(grouped['has_audit'].sum()))
    print("  non-null audit_score:", int(grouped['audit_score'].notna().sum()))
    print("  non-null last_audit_date:", int(grouped['last_audit_date'].notna().sum()))

if __name__ == "__main__":
    main()