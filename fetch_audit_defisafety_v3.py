import os
import re
import requests
import pandas as pd

print("🌐 Fetching audit metadata from DeFiSafety (with slug mapping)...")

df = pd.read_csv("data_raw/contracts/verified_contracts.csv")

records = []
slug_map = {
    "Aave V3": "aave-v3",
    "PancakeSwap": "pancakeswap",
    "PancakeSwap AMM": "pancakeswap",
    "Uniswap V2": "uniswap-v2",
    "Uniswap V3": "uniswap-v3",
    "Uniswap V4": "uniswap-v4",
    "Compound V3": "compound-v3",
    "dYdX V3": "dydx-v3",
    "Morpho Blue": "morpho-blue",
    "Curve DEX": "curve",
    "Balancer V2": "balancer-v2",
    "Venus Core Pool": "venus",
    "FX Protocol": "fx-protocol",
    "Drift Trade": "drift-protocol",
    "Fluid Lending": "fluid",
    "Maple": "maple-finance",
    "Kame Aggregator": "kame-aggregator",
    "Shibarium": "shibarium",
    "JustLend": "justlend",
    "SparkLend": "spark-lend",
    "Euler V2": "euler-v2",
    "Radiyum AMM": "raydium",
}

for _, row in df.iterrows():
    name = row["protocol_name"]
    slug = slug_map.get(name, name.lower().replace(" ", "-"))
    url = f"https://api.defisafety.com/audit/{slug}"

    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            records.append({
                "protocol_name": name,
                "slug": slug,
                "audit_score": data.get("score", None),
                "audit_date": data.get("audit_date", None),
                "audit_link": data.get("url", None),
                "auditor": data.get("auditor", None),
                "category": row.get("category", None)
            })
            print(f"✅ Found audit for {name} ({slug})")
        else:
            print(f"⚠️ {r.status_code} for {name} ({slug})")

    except Exception as e:
        print(f"❌ Error fetching {name}: {e}")

df_out = pd.DataFrame(records)
os.makedirs("data_raw/contracts", exist_ok=True)
df_out.to_csv("data_raw/contracts/audit_metadata_defisafety.csv", index=False)

print(f"\n✅ Saved DeFiSafety audit metadata for {len(df_out)} protocols → data_raw/contracts/audit_metadata_defisafety.csv")