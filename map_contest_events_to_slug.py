#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import re
import pandas as pd

try:
    from rapidfuzz import process, fuzz
except ImportError:
    process = None
    fuzz = None

ROOT = Path(__file__).resolve().parent
LLAMA = ROOT / "data_raw" / "llama_protocols.csv"
MANUAL = ROOT / "data_raw" / "audits" / "manual_name_to_slug.csv"

INPUTS = [
    ROOT / "data_raw" / "audits" / "audit_events_code4rena.csv",
    ROOT / "data_raw" / "audits" / "audit_events_sherlock.csv",
    ROOT / "data_raw" / "audits" / "audit_events_long.csv",  # optional: if you want to remap everything
]

OUT = ROOT / "data_raw" / "audits" / "audit_events_long_mapped.csv"

def norm_name(x: str) -> str:
    if x is None:
        return ""
    x = str(x).strip().lower()

    # remove common suffixes and noise
    x = re.sub(r"[-_ ]?(findings|report|reports|judging|contest|audit)$", "", x)
    x = re.sub(r"[^a-z0-9]+", " ", x).strip()
    x = re.sub(r"\s+", " ", x)
    return x

def build_index(llama: pd.DataFrame):
    # lowercase forms
    llama["slug_l"] = llama["slug"].astype(str).str.strip().str.lower()
    llama["name_l"] = llama["name"].astype(str).str.strip().str.lower()
    llama["symbol_l"] = llama["symbol"].fillna("").astype(str).str.strip().str.lower()

    slug_map = dict(zip(llama["slug_l"], llama["slug"]))
    name_map = dict(zip(llama["name_l"], llama["slug"]))

    # symbol is not unique; keep first
    sym_map = {}
    for sym, slug in zip(llama["symbol_l"], llama["slug"]):
        if sym and sym not in sym_map:
            sym_map[sym] = slug

    # fuzzy candidates
    candidates = list(set(llama["slug_l"].tolist() + llama["name_l"].tolist()))
    cand_to_slug = {}
    for s, slug in zip(llama["slug_l"], llama["slug"]):
        cand_to_slug[s] = slug
    for n, slug in zip(llama["name_l"], llama["slug"]):
        cand_to_slug[n] = slug

    return slug_map, name_map, sym_map, candidates, cand_to_slug

def fuzzy_match(q: str, candidates, cand_to_slug, score_cutoff=90):
    if not process:
        return None
    match = process.extractOne(q, candidates, scorer=fuzz.WRatio, score_cutoff=score_cutoff)
    if not match:
        return None
    best_text = match[0]
    return cand_to_slug.get(best_text)

def main():
    llama = pd.read_csv(LLAMA)
    slug_map, name_map, sym_map, candidates, cand_to_slug = build_index(llama)

    manual = pd.DataFrame(columns=["protocol_name_raw", "slug"])
    if MANUAL.exists():
        manual = pd.read_csv(MANUAL)
        manual["protocol_name_raw_norm"] = manual["protocol_name_raw"].map(norm_name)
        manual_map = dict(zip(manual["protocol_name_raw_norm"], manual["slug"]))
    else:
        manual_map = {}

    frames = []
    for p in INPUTS:
        if p.exists():
            df = pd.read_csv(p)
            df["__srcfile"] = p.name
            frames.append(df)

    if not frames:
        raise SystemExit("No input files found.")

    events = pd.concat(frames, ignore_index=True)

    # normalize protocol string
    events["protocol_norm"] = events["protocol_name_raw"].map(norm_name)

    # ensure slug column exists
    if "slug" not in events.columns:
        events["slug"] = pd.NA

    # map
    mapped = []
    for raw, proto_norm, sym in zip(
        events["protocol_name_raw"].astype(str),
        events["protocol_norm"].astype(str),
        events.get("symbol", pd.Series([""] * len(events))).fillna("").astype(str).str.lower()
    ):
        if not proto_norm:
            mapped.append(None)
            continue

        # manual override first
        if proto_norm in manual_map:
            mapped.append(manual_map[proto_norm])
            continue

        # exact on slug
        if proto_norm in slug_map:
            mapped.append(slug_map[proto_norm])
            continue

        # exact on name
        if proto_norm in name_map:
            mapped.append(name_map[proto_norm])
            continue

        # exact on symbol
        if sym and sym in sym_map:
            mapped.append(sym_map[sym])
            continue

        # fuzzy
        slug = fuzzy_match(proto_norm, candidates, cand_to_slug, score_cutoff=90)
        mapped.append(slug)

    events["slug_mapped"] = mapped
    events["in_llama"] = events["slug_mapped"].notna().astype(int)

    # keep old slug if you already had it, otherwise use mapped
    events["slug_final"] = events["slug"]
    events.loc[events["slug_final"].isna(), "slug_final"] = events.loc[events["slug_final"].isna(), "slug_mapped"]

    # output
    OUT.parent.mkdir(parents=True, exist_ok=True)
    events.to_csv(OUT, index=False)

    print(f"✅ Wrote {OUT}")
    print("Rows:", len(events))
    print("Mapped:", events["slug_final"].notna().sum(), "rate", round(events["slug_final"].notna().mean(), 4))
    print("In DeFiLlama:", events["in_llama"].sum())

    # show top unmapped names
    unm = events[events["slug_final"].isna()]
    if len(unm):
        top = unm["protocol_name_raw"].value_counts().head(25)
        print("\nTop unmapped protocol_name_raw:")
        print(top.to_string())

if __name__ == "__main__":
    main()