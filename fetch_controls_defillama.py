import requests
import pandas as pd

# Load your verified contracts from previous steps
verified = pd.read_csv("data_raw/contracts/verified_contracts.csv")

def get_defillama_data():
    url = "https://api.llama.fi/protocols"
    r = requests.get(url).json()
    df = pd.DataFrame(r)
    return df[["name", "category", "chains", "tvl"]]

def match_control(target_row, df):
    chain = target_row["chain"]
    category = target_row["category"]
    tvl_target = target_row.get("tvl", None)

    subset = df[df["category"].str.lower() == category.lower()]
    subset = subset[subset["chains"].apply(lambda x: chain.capitalize() in str(x))]

    if tvl_target and not pd.isna(tvl_target):
        subset = subset[(subset["tvl"] > tvl_target * 0.7) & (subset["tvl"] < tvl_target * 1.3)]

    if subset.empty:
        return None
    return subset.sample(1).iloc[0]["name"]

# Fetch DeFiLlama dataset
df_llama = get_defillama_data()

# Attach TVL values to verified contracts
verified["tvl"] = verified["protocol_name"].map(
    df_llama.set_index("name")["tvl"]
)

# Match controls
verified["control_protocol"] = verified.apply(lambda r: match_control(r, df_llama), axis=1)

# Save
verified.to_csv("data_raw/contracts/verified_with_controls.csv", index=False)
print("✅ Matched control protocols and saved to verified_with_controls.csv")