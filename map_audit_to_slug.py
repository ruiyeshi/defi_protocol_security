#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd
import difflib
import re

ROOT = Path(__file__).resolve().parent

AUDIT_IN = ROOT / "data_raw" / "audits" / "audit_master.csv"
LLAMA_IN = ROOT / "data_raw" / "llama_protocols.csv"

OUT_WITH_SLUG = ROOT / "data_raw" / "audits" / "audit_master_with_slug.csv"
OUT_UNMATCHED = ROOT / "data_raw" / "audits" / "unmatched_audit_protocols.csv"
OUT_MANUAL    = ROOT / "data_raw" / "audits" / "manual_name_to_slug.csv"

def norm(s: str) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def best_match(q: str, candidates: list[str], cutoff: float):
    m = difflib.get_close_matches(q, candidates, n=1, cutoff=cutoff)
    return m[0] if m else None

def main():
    if not AUDIT_IN.exists():
        raise SystemExit(f"Missing {AUDIT_IN}. Build audit_master first.")
    if not LLAMA_IN.exists():
        raise SystemExit(f"Missing {LLAMA_IN}. Run fetch_llama_protocols.py first.")

    audits = pd.read_csv(AUDIT_IN)
    llama  = pd.read_csv(LLAMA_IN)

    # prepare llama lookup
    llama["name_norm"]   = llama["name"].apply(norm)
    llama["symbol_norm"] = llama["symbol"].apply(norm)
    llama["slug_norm"]   = llama["slug"].apply(norm)

    # some audits might already be slugs; allow direct slug match too
    audits["proto_norm"] = audits["protocol_name_raw"].apply(norm)

    name_to_slug = dict(zip(llama["name_norm"], llama["slug"]))
    sym_to_slug  = dict(zip(llama["symbol_norm"], llama["slug"]))
    slug_set     = set(llama["slug_norm"])

    audits["slug"] = None

    # (0) direct slug match
    mask = audits["proto_norm"].isin(slug_set)
    audits.loc[mask, "slug"] = audits.loc[mask, "proto_norm"]

    # (1) exact name match
    mask = audits["slug"].isna()
    audits.loc[mask, "slug"] = audits.loc[mask, "proto_norm"].map(name_to_slug)

    # (2) exact symbol match
    mask = audits["slug"].isna()
    audits.loc[mask, "slug"] = audits.loc[mask, "proto_norm"].map(sym_to_slug)

    # (3) fuzzy match on name
    name_candidates = llama["name_norm"].tolist()
    still = audits["slug"].isna()
    for idx in audits[still].index:
        q = audits.at[idx, "proto_norm"]
        if not q:
            continue
        # strict first, then slightly looser
        m = best_match(q, name_candidates, cutoff=0.93) or best_match(q, name_candidates, cutoff=0.88)
        if m:
            audits.at[idx, "slug"] = name_to_slug.get(m)

    # (4) manual overrides (optional)
    if OUT_MANUAL.exists():
        man = pd.read_csv(OUT_MANUAL)
        if {"protocol_name_raw", "slug"}.issubset(set(man.columns)):
            man_map = {norm(r["protocol_name_raw"]): r["slug"] for _, r in man.iterrows() if str(r["slug"]).strip()}
            mask = audits["slug"].isna()
            audits.loc[mask, "slug"] = audits.loc[mask, "proto_norm"].map(man_map)

    audits.to_csv(OUT_WITH_SLUG, index=False)

    unmatched = audits[audits["slug"].isna()].copy()
    unmatched.to_csv(OUT_UNMATCHED, index=False)

    # write a manual mapping template if not exists
    if not OUT_MANUAL.exists():
        tmpl = unmatched[["protocol_name_raw"]].drop_duplicates().copy()
        tmpl["slug"] = ""
        tmpl.to_csv(OUT_MANUAL, index=False)

    print(f"✅ Wrote: {OUT_WITH_SLUG} | rows={len(audits)}")
    print(f"⚠️ Unmatched: {OUT_UNMATCHED} | rows={len(unmatched)}")
    if len(unmatched):
        print("Examples unmatched:", unmatched["protocol_name_raw"].head(20).tolist())

if __name__ == "__main__":
    main()