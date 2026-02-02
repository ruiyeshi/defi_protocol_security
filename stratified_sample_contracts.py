#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import math
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent

CONTRACTS_CSV = ROOT / "data_raw" / "contracts" / "verified_contracts_expanded.csv"
META_CSV      = ROOT / "data_raw" / "contracts" / "audit_metadata_api.csv"
OUT_DIR       = ROOT / "outputs"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# CONFIG (tune these)
# -----------------------------
TARGET_N = int(os.getenv("TARGET_N", "25000"))          # desired sample size
MIN_PER_STRATUM = int(os.getenv("MIN_PER_STRATUM", "30"))
MAX_PER_PROTOCOL = int(os.getenv("MAX_PER_PROTOCOL", "500"))
SEED = int(os.getenv("SEED", "42"))

# If you want to force-include these categories only, comma-sep; leave empty to keep all
ALLOW_CATEGORIES = os.getenv("ALLOW_CATEGORIES", "").strip()  # e.g. "Lending,Dexs,CDP,Yield,Derivatives"

# TVL tier cutoffs (USD)
TVL_BINS = [
    (-np.inf, 1e7, "<10M"),
    (1e7, 1e8, "10M-100M"),
    (1e8, 1e9, "100M-1B"),
    (1e9, np.inf, ">=1B"),
]

# -----------------------------
# HELPERS
# -----------------------------
def tvl_tier(x: float) -> str:
    try:
        v = float(x)
    except Exception:
        return "unknown"
    for lo, hi, label in TVL_BINS:
        if lo <= v < hi:
            return label
    return "unknown"

def norm_chain(x: str) -> str:
    if pd.isna(x) or not str(x).strip():
        return "unknown"
    s = str(x).strip().lower()
    # normalize common variants
    if s in ["eth", "ethereum", "mainnet"]:
        return "ethereum"
    if s in ["op", "optimism"]:
        return "optimism"
    if s in ["arb", "arbitrum", "arbitrum-one"]:
        return "arbitrum"
    if s in ["bsc", "binance-smart-chain"]:
        return "bsc"
    if s in ["avax", "avalanche"]:
        return "avalanche"
    if s in ["xdai", "gnosis"]:
        return "gnosis"
    if s in ["base"]:
        return "base"
    return s

def norm_category(x: str) -> str:
    if pd.isna(x) or not str(x).strip():
        return "Other"
    s = str(x).strip()
    # coarse buckets (edit as you like)
    s_low = s.lower()
    if "lend" in s_low:
        return "Lending"
    if "dex" in s_low or "amm" in s_low:
        return "Dexs"
    if "derivative" in s_low or "perp" in s_low or "option" in s_low:
        return "Derivatives"
    if "cdp" in s_low:
        return "CDP"
    if "yield" in s_low or "aggregator" in s_low:
        return "Yield"
    if "bridge" in s_low:
        return "Bridge"
    if "staking" in s_low or "restaking" in s_low:
        return "Staking/Restaking"
    return s  # keep original if not mapped

# -----------------------------
# LOAD DATA
# -----------------------------
if not CONTRACTS_CSV.exists():
    raise SystemExit(f"Missing contracts file: {CONTRACTS_CSV}")

df = pd.read_csv(CONTRACTS_CSV)

# Expect at least: slug, address, guess (guess=chain)
need_cols = {"slug", "address"}
missing = need_cols - set(df.columns)
if missing:
    raise SystemExit(f"Contracts CSV missing columns: {missing}. Found: {list(df.columns)}")

# Deduplicate at the contract level
df["address"] = df["address"].astype(str).str.lower()
df = df.drop_duplicates(subset=["slug", "address"]).copy()

# Join protocol metadata if available
if META_CSV.exists():
    meta = pd.read_csv(META_CSV)
    # meta has protocol_name; your contracts have slug. If you have a mapping, use it.
    # Fallback: try joining on a cleaned name if you stored protocol_name in contracts.
    # For now: if your contracts file includes category/tvl already, prefer that.
    # We will merge only if matching column exists.
    join_key = None
    for k in ["slug", "protocol", "protocol_name", "name"]:
        if k in df.columns and k in meta.columns:
            join_key = k
            break
    if join_key:
        df = df.merge(meta, on=join_key, how="left", suffixes=("", "_meta"))
else:
    meta = None

# Prefer existing columns in df (from your mining rows), otherwise use meta columns if present
category_col_candidates = [c for c in ["category", "category_meta"] if c in df.columns]
tvl_col_candidates = [c for c in ["tvl", "tvl_meta"] if c in df.columns]

if category_col_candidates:
    df["category_final"] = df[category_col_candidates[0]].apply(norm_category)
else:
    df["category_final"] = "Other"

if tvl_col_candidates:
    df["tvl_value"] = pd.to_numeric(df[tvl_col_candidates[0]], errors="coerce")
else:
    df["tvl_value"] = np.nan

# Chain bucket: from "guess" if present, else from "chain" column
if "guess" in df.columns:
    df["chain_bucket"] = df["guess"].apply(norm_chain)
elif "chain" in df.columns:
    df["chain_bucket"] = df["chain"].apply(norm_chain)
else:
    df["chain_bucket"] = "unknown"

df["tvl_tier"] = df["tvl_value"].apply(tvl_tier)

# Optional category filter
if ALLOW_CATEGORIES:
    allow = [x.strip() for x in ALLOW_CATEGORIES.split(",") if x.strip()]
    df = df[df["category_final"].isin(allow)].copy()

# Define stratum
df["stratum"] = df["category_final"].astype(str) + "||" + df["chain_bucket"].astype(str) + "||" + df["tvl_tier"].astype(str)

# -----------------------------
# APPLY PROTOCOL CAP FIRST (so one protocol can’t dominate)
# -----------------------------
rng = np.random.default_rng(SEED)
df["_rand"] = rng.random(len(df))
df = df.sort_values(["slug", "_rand"]).copy()
df = df.groupby("slug", as_index=False).head(MAX_PER_PROTOCOL).copy()
df = df.drop(columns=["_rand"])

# If population < target, return all
population = len(df)
if population <= TARGET_N:
    out_path = OUT_DIR / f"sampled_contracts_{population}.csv"
    df.to_csv(out_path, index=False)
    print(f"✅ Population {population} <= TARGET_N {TARGET_N}. Wrote ALL rows to: {out_path}")
    print("\nChain distribution:")
    print(df["chain_bucket"].value_counts(dropna=False).head(30))
    print("\nCategory distribution:")
    print(df["category_final"].value_counts(dropna=False).head(30))
    raise SystemExit(0)

# -----------------------------
# STRATIFIED ALLOCATION
# -----------------------------
stratum_sizes = df["stratum"].value_counts()
total_pop = stratum_sizes.sum()

# proportional allocation
alloc = (stratum_sizes / total_pop * TARGET_N).apply(math.floor)

# enforce min per stratum, but cannot exceed stratum size
alloc = alloc.apply(lambda x: max(x, MIN_PER_STRATUM))
alloc = alloc.combine(stratum_sizes, func=min)

# If we overshoot target due to mins, trim back from largest strata
current = int(alloc.sum())
if current > TARGET_N:
    # sort strata by (alloc - MIN_PER_STRATUM) descending so we reduce where there is slack
    slack = (alloc - MIN_PER_STRATUM).clip(lower=0)
    order = slack.sort_values(ascending=False).index.tolist()
    i = 0
    while current > TARGET_N and i < len(order):
        s = order[i]
        if alloc[s] > MIN_PER_STRATUM:
            alloc[s] -= 1
            current -= 1
        else:
            i += 1

# If we undershoot target, add to strata with remaining capacity
current = int(alloc.sum())
if current < TARGET_N:
    remaining_capacity = (stratum_sizes - alloc).clip(lower=0)
    order = remaining_capacity.sort_values(ascending=False).index.tolist()
    i = 0
    while current < TARGET_N and i < len(order):
        s = order[i]
        if remaining_capacity[s] > 0:
            alloc[s] += 1
            remaining_capacity[s] -= 1
            current += 1
        else:
            i += 1

# -----------------------------
# DRAW SAMPLE
# -----------------------------
samples = []
for s, k in alloc.items():
    block = df[df["stratum"] == s]
    if k <= 0:
        continue
    take = block.sample(n=int(k), random_state=SEED)
    samples.append(take)

sample_df = pd.concat(samples, ignore_index=True)
# Final safety: if rounding logic caused slight mismatch
sample_df = sample_df.sample(n=TARGET_N, random_state=SEED)

out_path = OUT_DIR / f"sampled_contracts_{TARGET_N//1000}k.csv"
sample_df.to_csv(out_path, index=False)

print(f"✅ Wrote sample: {out_path}")
print(f"Sample size: {len(sample_df)} (target {TARGET_N})")

print("\nTop strata counts:")
print(sample_df["stratum"].value_counts().head(20))

print("\nChain distribution:")
print(sample_df["chain_bucket"].value_counts().head(30))

print("\nCategory distribution:")
print(sample_df["category_final"].value_counts().head(30))
