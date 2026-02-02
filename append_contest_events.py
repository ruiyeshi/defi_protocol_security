#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd
import re, difflib

ROOT = Path(__file__).resolve().parent

LLAMA = ROOT / "data_raw" / "llama_protocols.csv"
BASE_EVENTS = ROOT / "data_raw" / "audits" / "audit_events_long.csv"

C4 = ROOT / "data_raw" / "audits" / "audit_events_code4rena.csv"
SH = ROOT / "data_raw" / "audits" / "audit_events_sherlock.csv"

OUT = ROOT / "data_raw" / "audits" / "audit_events_long.csv"

def norm(s):
    if pd.isna(s): return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def map_to_slug(df: pd.DataFrame, llama: pd.DataFrame) -> pd.DataFrame:
    llama = llama.copy()
    llama["name_norm"] = llama["name"].apply(norm)
    llama["symbol_norm"] = llama["symbol"].apply(norm)
    name_to_slug = dict(zip(llama["name_norm"], llama["slug"]))
    sym_to_slug  = dict(zip(llama["symbol_norm"], llama["slug"]))
    name_candidates = llama["name_norm"].tolist()

    # ensure dtype object to avoid pandas dtype warnings
    if "slug" not in df.columns:
        df["slug"] = pd.Series([None]*len(df), dtype="object")
    else:
        df["slug"] = df["slug"].astype("object")

    proto_norm = df["protocol_name_raw"].apply(norm)

    # exact name match
    mask = df["slug"].isna() | (df["slug"].astype(str).str.strip() == "")
    df.loc[mask, "slug"] = proto_norm[mask].map(name_to_slug)

    # exact symbol match
    mask2 = df["slug"].isna() | (df["slug"].astype(str).str.strip() == "")
    df.loc[mask2, "slug"] = proto_norm[mask2].map(sym_to_slug)

    # fuzzy fallback
    still = df["slug"].isna() | (df["slug"].astype(str).str.strip() == "")
    for idx in df[still].index:
        q = proto_norm.loc[idx]
        if not q:
            continue
        m = difflib.get_close_matches(q, name_candidates, n=1, cutoff=0.90)
        if m:
            df.at[idx, "slug"] = name_to_slug.get(m[0])

    llama_slugs = set(llama["slug"].astype(str))
    df["in_llama"] = df["slug"].astype(str).isin(llama_slugs).astype(int)
    return df

def main():
    if not LLAMA.exists():
        raise SystemExit("Missing llama_protocols.csv. Run fetch_llama_protocols.py first.")

    llama = pd.read_csv(LLAMA)

    frames = []
    if BASE_EVENTS.exists():
        frames.append(pd.read_csv(BASE_EVENTS))
    else:
        print("⚠️ Base audit_events_long.csv not found — creating new one.")

    for p in [C4, SH]:
        if p.exists():
            frames.append(pd.read_csv(p))
        else:
            print(f"⚠️ Missing {p} (skipping)")

    if not frames:
        raise SystemExit("No inputs to append.")

    all_events = pd.concat(frames, ignore_index=True)

    # normalize columns (ensure all expected exist)
    for col in ["protocol_name_raw","source","audit_firm_raw","audit_score","audit_date","evidence_url","notes","slug","in_llama"]:
        if col not in all_events.columns:
            all_events[col] = None

    # map ONLY rows missing slug
    all_events = map_to_slug(all_events, llama)

    # dedupe: same platform + same evidence_url OR (platform+protocol+date)
    all_events["audit_date"] = pd.to_datetime(all_events["audit_date"], errors="coerce", utc=True)
    all_events["dedupe_key"] = (
        all_events["source"].astype(str).fillna("") + "||" +
        all_events["evidence_url"].astype(str).fillna("") + "||" +
        all_events["protocol_name_raw"].astype(str).fillna("")
    )
    all_events = all_events.drop_duplicates(subset=["dedupe_key"]).drop(columns=["dedupe_key"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    all_events.to_csv(OUT, index=False)

    print(f"✅ Updated {OUT} | rows={len(all_events)}")
    print("By source:", all_events["source"].value_counts().to_dict())
    print("Mapped slugs:", int(all_events["slug"].notna().sum()), "/", len(all_events))
    print("In DeFiLlama:", int(all_events["in_llama"].sum()), "/", len(all_events))

if __name__ == "__main__":
    main()