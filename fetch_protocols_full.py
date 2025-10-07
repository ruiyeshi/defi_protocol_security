# fetch_protocols_full.py
import requests
import pandas as pd

print("🌍 Fetching top DeFi protocols from DeFiLlama...")

# Pull full list of protocols
url = "https://api.llama.fi/protocols"
r = requests.get(url).json()
df = pd.DataFrame(r)

# --- Step 1. Define your 5 target DeFi categories ---
target_categories = ["Dexes", "Lending", "Bridges", "Stablecoins", "Derivatives"]

# --- Step 2. Filter relevant protocols only ---
df = df[df["category"].isin(target_categories)]

# --- Step 3. Keep essential metadata ---
df = df[["name", "category", "chains", "tvl", "url"]]
df = df[df["tvl"] > 0]  # remove inactive protocols
df = df.sort_values("tvl", ascending=False)

# --- Step 4. Sample top 10 per category (balanced sampling) ---
df_balanced = (
    df.groupby("category", group_keys=False)
      .apply(lambda x: x.head(10))
      .reset_index(drop=True)
)

# --- Step 5. Rename and save ---
df_balanced.columns = ["protocol_name", "category", "chain", "tvl", "project_url"]
df_balanced.to_csv("data_raw/contracts/verified_contracts.csv", index=False)

print(f"✅ Saved verified_contracts.csv with {len(df_balanced)} protocols "
      f"across {df_balanced['category'].nunique()} categories.")
print(df_balanced.groupby('category').size())