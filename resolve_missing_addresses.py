import pandas as pd
import requests
from time import sleep

df = pd.read_csv("data_raw/contracts/verified_contracts.csv")
resolved = []

print("🔍 Resolving missing contract addresses via DeFiLlama API...")

for _, row in df.iterrows():
    name = row["protocol_name"]
    slug = name.lower().replace(" ", "-").replace(".", "")
    url = f"https://api.llama.fi/protocol/{slug}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            print(f"⚠️  Skipping {name}: not found on DeFiLlama.")
            continue
        data = r.json()
        chains = data.get("chainTvls", {}).keys()
        addresses = []
        for chain in chains:
            addr = data.get("address", "")
            if addr:
                addresses.append({"chain": chain, "address": addr})
        if addresses:
            resolved.append({
                "protocol_name": name,
                "chains": [a["chain"] for a in addresses],
                "addresses": [a["address"] for a in addresses]
            })
            print(f"✅ Found {name}: {len(addresses)} addresses")
        else:
            print(f"🚫 No addresses for {name}")
    except Exception as e:
        print(f"💥 Error resolving {name}: {e}")
    sleep(1)

df_out = pd.DataFrame(resolved)
df_out.to_csv("data_raw/contracts/resolved_addresses.csv", index=False)
print(f"\n💾 Saved resolved addresses → data_raw/contracts/resolved_addresses.csv ({len(df_out)} entries)")
