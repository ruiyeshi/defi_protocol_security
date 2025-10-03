import pandas as pd

df = pd.read_csv("data_raw/contracts/verified_contracts.csv")

print("📊 Summary of verified_contracts.csv")
print("="*40)
print(df.groupby("category")["tvl"].describe()[["count", "mean", "max", "min"]])
print("\n🔗 Chains included:", df["chain"].nunique(), "unique chains")
print("\n🧠 Top 5 protocols by TVL:")
print(df.sort_values("tvl", ascending=False).head(5)[["protocol_name", "category", "chain", "tvl"]])
