#!/usr/bin/env python3
"""
Fetch top DeFi protocols from DeFiLlama and prepare input for contract fetching.
"""

import requests
import pandas as pd
import time
from datetime import datetime

OUT_CSV = "data_raw/contracts/defillama_top_protocols.csv"

print("🌍 Fetching top protocols from DeFiLlama...")

# DeFiLlama API endpoint
url = "https://api.llama.fi/protocols"
response = requests.get(url)
data = response.json()

print(f"✅ Retrieved {len(data)} total protocols")

# Convert to DataFrame
df = pd.DataFrame(data)

# Basic cleaning
df = df[["name", "chain", "category", "url", "tvl", "slug"]]
df = df.sort_values("tvl", ascending=False).reset_index(drop=True)

# Filter out CEXs and duplicates
df = df[df["category"].str.contains("Dex|Lending|Yield|Bridge|Stablecoin|Derivatives", case=False, na=False)]
df = df.drop_duplicates(subset=["name", "chain"])

# Limit to top 150 by TVL
df = df.head(150)

# Save results
df["timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
df.to_csv(OUT_CSV, index=False)

print(f"✅ Saved {len(df)} filtered DeFi protocols → {OUT_CSV}")
