#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "data_raw" / "llama_protocols.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

URL = "https://api.llama.fi/protocols"

def main():
    r = requests.get(URL, timeout=60)
    r.raise_for_status()
    raw = r.json()

    df = pd.DataFrame(raw)

    keep = ["slug", "name", "symbol", "category", "tvl", "chains"]
    for k in keep:
        if k not in df.columns:
            df[k] = None
    df = df[keep].copy()

    # normalize chains
    df["chains"] = df["chains"].apply(lambda x: ";".join(x) if isinstance(x, list) else (x if isinstance(x, str) else ""))

    df.to_csv(OUT, index=False)
    print(f"✅ Wrote {OUT} | rows={len(df)} | cols={df.columns.tolist()}")

if __name__ == "__main__":
    main()