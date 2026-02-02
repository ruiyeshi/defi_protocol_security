#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent

EXPANDED = ROOT / "data_raw" / "contracts" / "verified_contracts_expanded.csv"
LOCAL    = ROOT / "data_raw" / "contracts" / "verified_contracts_local.csv"
OUT      = ROOT / "data_raw" / "contracts" / "verified_contracts_merged.csv"

def norm_chain(df: pd.DataFrame) -> pd.Series:
    if "chain" in df.columns:
        s = df["chain"]
    elif "guess" in df.columns:
        s = df["guess"]
    else:
        s = ""
    return s.astype(str).str.lower().str.strip()

def norm_addr(df: pd.DataFrame) -> pd.Series:
    return df["address"].astype(str).str.lower().str.strip()

def parse_ts(df: pd.DataFrame) -> pd.Series:
    if "scrape_ts" not in df.columns:
        return pd.NaT
    return pd.to_datetime(df["scrape_ts"], errors="coerce")

def load_one(path: Path, source_name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    if "address" not in df.columns:
        raise ValueError(f"{path} missing 'address' column.")
    if "slug" not in df.columns:
        # sometimes older files used 'protocol' – normalize to slug
        if "protocol" in df.columns:
            df = df.rename(columns={"protocol": "slug"})
        else:
            df["slug"] = None

    df["source_file"] = source_name
    df["chain"] = norm_chain(df)
    df["address"] = norm_addr(df)
    df["scrape_ts_parsed"] = parse_ts(df)

    # standardize empties
    df["slug"] = df["slug"].astype(str).replace({"nan": ""}).str.strip()
    return df

def pick_best(group: pd.DataFrame) -> pd.Series:
    g = group.copy()

    # score rows: higher is better
    g["has_slug"] = (g["slug"].fillna("").astype(str).str.strip() != "").astype(int)
    g["is_local"] = (g["source_file"] == "local").astype(int)

    # sort by: has_slug desc, scrape_ts desc, is_local desc
    g = g.sort_values(
        by=["has_slug", "scrape_ts_parsed", "is_local"],
        ascending=[False, False, False],
        na_position="last"
    )
    return g.iloc[0]

def main():
    a = load_one(EXPANDED, "expanded")
    b = load_one(LOCAL, "local")

    all_df = pd.concat([a, b], ignore_index=True)

    # drop impossible rows
    all_df = all_df[(all_df["address"].str.len() == 42) & (all_df["address"].str.startswith("0x"))]

    # dedupe by (chain, address)
    merged = (
        all_df
        .groupby(["chain", "address"], as_index=False, dropna=False)
        .apply(lambda g: pick_best(g), include_groups=False)
        .reset_index(drop=True)
    )

    # clean helper cols
    merged = merged.drop(columns=[c for c in ["scrape_ts_parsed", "has_slug", "is_local"] if c in merged.columns])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT, index=False)

    # quick stats
    A = set(zip(a["chain"], a["address"]))
    B = set(zip(b["chain"], b["address"]))
    print(f"✅ Wrote: {OUT}")
    print(f"expanded unique: {len(A)} | local unique: {len(B)} | overlap: {len(A&B)}")
    print(f"only_local: {len(B-A)} | only_expanded: {len(A-B)}")
    print(f"merged unique: {len(merged)}")

if __name__ == "__main__":
    main()