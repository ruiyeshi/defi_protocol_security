# fetch_protocols_full.py (fixed version)
import requests
import pandas as pd

print("🌍 Fetching top DeFi protocols from DeFiLlama...")
url = "https://api.llama.fi/protocols"
r = requests.get(url).json()
df = pd.DataFrame(r)

# Normalize names for fuzzy matching
df["category"] = df["category"].str.lower()

# Define our 5 major research categories and their aliases
category_aliases = {
    "dex": ["dex", "dexes", "exchange", "swap"],
    "lending": ["lending", "loan", "money market"],
    "bridges": ["bridge", "bridges", "cross-chain"],
    "stablecoins": ["stablecoin", "stablecoins"],
    "derivatives": ["derivative", "derivatives", "perpetual", "options"]
}

# Assign normalized category
def normalize_category(cat):
    for key, aliases in category_aliases.items():
        if any(alias in str(cat) for alias in aliases):
            return key.capitalize()
    return None

df["normalized_category"] = df["category"].apply(normalize_category)
df = df[df["normalized_category"].notna()]

# Select top 10 per category by TVL
df_top = (
    df.groupby("normalized_category", group_keys=False)
      .apply(lambda x: x.sort_values("tvl", ascending=False).head(10))
)

# Export cleaned dataset
df_out = df_top[["name", "normalized_category", "chain", "tvl"]]
df_out.columns = ["protocol_name", "category", "chain", "tvl"]

df_out.to_csv("data_raw/contracts/verified_contracts.csv", index=False)
print(f"✅ Saved verified_contracts.csv with {len(df_out)} protocols across {df_out['category'].nunique()} categories.")
print(df_out['category'].value_counts())
