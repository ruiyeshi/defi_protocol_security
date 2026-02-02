#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent

AUDIT_WITH_SLUG = ROOT / "data_raw" / "audits" / "audit_master_with_slug.csv"
LLAMA = ROOT / "data_raw" / "llama_protocols.csv"
OUT = ROOT / "data_raw" / "audits" / "audit_master_with_slug_defi_only.csv"

def main():
    a = pd.read_csv(AUDIT_WITH_SLUG)
    l = pd.read_csv(LLAMA)[["slug","name","symbol","category","tvl","chains"]]

    # keep only rows where slug exists AND is in DeFiLlama universe
    a = a[a["slug"].notna()].copy()
    a = a.merge(l, on="slug", how="inner", suffixes=("", "_llama"))

    # scope control (drop obvious CeFi categories if you want)
    # If you want strict DeFi only, you can blacklist categories like "CEX" if present.
    # For now we keep everything in DeFiLlama protocols list because that’s your protocol universe.
    a.to_csv(OUT, index=False)
    print(f"✅ Wrote {OUT} | rows={len(a)}")

if __name__ == "__main__":
    main()