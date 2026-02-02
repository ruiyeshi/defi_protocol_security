#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd
import re
import difflib

ROOT = Path(__file__).resolve().parent

LLAMA = ROOT / "data_raw" / "llama_protocols.csv"

IN_CERTIK = ROOT / "data_raw" / "contracts" / "audit_metadata_certik.csv"
IN_FULL   = ROOT / "data_raw" / "contracts" / "audit_metadata_full.csv"

OUT = ROOT / "data_raw" / "audits" / "audit_events_long.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

MANUAL = ROOT / "data_raw" / "audits" / "manual_name_to_slug.csv"
UNMATCHED = ROOT / "data_raw" / "audits" / "unmatched_audit_events.csv"

SEP_RE = re.compile(r"[;,|/]|(?:\s+&\s+)|(?:\s+and\s+)", re.IGNORECASE)

def norm(s):
    if pd.isna(s): return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def to_dt(x):
    # keep it robust; warnings are okay
    return pd.to_datetime(x, errors="coerce", utc=True)

def parse_score(x):
    if pd.isna(x): return None
    s = str(x).strip()
    if not s: return None
    m = re.search(r"(\d+(\.\d+)?)", s)
    return float(m.group(1)) if m else None

def guess_proto_col(df):
    for c in df.columns:
        if c.lower() in ("protocol_name","protocol","name","project"):
            return c
    return df.columns[0]

def load_manual_map():
    if not MANUAL.exists():
        MANUAL.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"protocol_name_raw": [], "slug": []}).to_csv(MANUAL, index=False)
        return {}
    m = pd.read_csv(MANUAL)
    if not {"protocol_name_raw","slug"}.issubset(set(m.columns)):
        return {}
    return {norm(r["protocol_name_raw"]): str(r["slug"]).strip() for _, r in m.iterrows() if str(r["slug"]).strip()}

def map_to_slug(events: pd.DataFrame, llama: pd.DataFrame) -> pd.DataFrame:
    llama = llama.copy()
    llama["name_norm"] = llama["name"].apply(norm)
    llama["symbol_norm"] = llama["symbol"].apply(norm)
    llama["slug_norm"] = llama["slug"].apply(norm)

    name_to_slug = dict(zip(llama["name_norm"], llama["slug"]))
    sym_to_slug  = dict(zip(llama["symbol_norm"], llama["slug"]))
    slug_set     = set(llama["slug_norm"])
    name_candidates = llama["name_norm"].tolist()

    manual_map = load_manual_map()

    events["proto_norm"] = events["protocol_name_raw"].apply(norm)
    events["slug"] = None

    # (0) manual override first
    events["slug"] = events["proto_norm"].map(manual_map)

    # (1) direct slug
    mask = events["slug"].isna() & events["proto_norm"].isin(slug_set)
    events.loc[mask, "slug"] = events.loc[mask, "proto_norm"]

    # (2) exact name
    mask = events["slug"].isna()
    events.loc[mask, "slug"] = events.loc[mask, "proto_norm"].map(name_to_slug)

    # (3) exact symbol
    mask = events["slug"].isna()
    events.loc[mask, "slug"] = events.loc[mask, "proto_norm"].map(sym_to_slug)

    # (4) fuzzy
    still = events["slug"].isna()
    for idx in events[still].index:
        q = events.at[idx, "proto_norm"]
        if not q:
            continue
        m = difflib.get_close_matches(q, name_candidates, n=1, cutoff=0.90)
        if m:
            events.at[idx, "slug"] = name_to_slug.get(m[0])

    # in_llama flag
    llama_slugs = set(llama["slug"].astype(str))
    events["in_llama"] = events["slug"].astype(str).isin(llama_slugs).astype(int)
    return events.drop(columns=["proto_norm"], errors="ignore")

def main():
    if not LLAMA.exists():
        raise SystemExit("Missing llama_protocols.csv — run fetch_llama_protocols.py first.")
    llama = pd.read_csv(LLAMA)

    frames = []

    # FULL (DeFiSafety-like)
    if IN_FULL.exists():
        df = pd.read_csv(IN_FULL)
        pcol = guess_proto_col(df)
        firm_col  = next((c for c in df.columns if "firm" in c.lower() or "auditor" in c.lower()), None)
        score_col = next((c for c in df.columns if "score" in c.lower()), None)
        date_col  = next((c for c in df.columns if "date" in c.lower()), None)
        url_col   = next((c for c in df.columns if "url" in c.lower() or "link" in c.lower()), None)

        out = pd.DataFrame({
            "protocol_name_raw": df[pcol].astype(str),
            "source": "full",
            "audit_firm_raw": df[firm_col].astype(str) if firm_col else "DeFiSafety",
            "audit_score": df[score_col].apply(parse_score) if score_col else None,
            "audit_date": to_dt(df[date_col]) if date_col else pd.NaT,
            "evidence_url": df[url_col].astype(str) if url_col else "",
            "notes": "",
        })
        frames.append(out)

    # CERTIK
    if IN_CERTIK.exists():
        df = pd.read_csv(IN_CERTIK)
        pcol = guess_proto_col(df)
        score_col = next((c for c in df.columns if "score" in c.lower() or "rating" in c.lower()), None)
        date_col  = next((c for c in df.columns if "date" in c.lower()), None)
        url_col   = next((c for c in df.columns if "url" in c.lower() or "link" in c.lower()), None)

        out = pd.DataFrame({
            "protocol_name_raw": df[pcol].astype(str),
            "source": "certik",
            "audit_firm_raw": "CertiK",
            "audit_score": df[score_col].apply(parse_score) if score_col else None,
            "audit_date": to_dt(df[date_col]) if date_col else pd.NaT,
            "evidence_url": df[url_col].astype(str) if url_col else "",
            "notes": "",
        })
        frames.append(out)

    if not frames:
        raise SystemExit("No event inputs found (certik/full missing).")

    events = pd.concat(frames, ignore_index=True)

    # map but DO NOT DROP
    events = map_to_slug(events, llama)

    # save unmatched list for manual mapping
    unmatched = events[events["slug"].isna()].copy()
    unmatched.to_csv(UNMATCHED, index=False)

    events.to_csv(OUT, index=False)
    print(f"✅ Wrote {OUT} | rows={len(events)}")
    print("By source:", events["source"].value_counts().to_dict())
    print("Mapped slugs:", int(events["slug"].notna().sum()), " / ", len(events))
    print("In DeFiLlama:", int(events["in_llama"].sum()), " / ", len(events))
    print(f"🧩 Manual mapping file: {MANUAL}")
    print(f"⚠️ Unmatched events saved: {UNMATCHED} | rows={len(unmatched)}")

if __name__ == "__main__":
    main()