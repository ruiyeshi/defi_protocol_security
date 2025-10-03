import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Load your Etherscan API key
load_dotenv()
API_KEY = os.getenv("ETHERSCAN_API_KEY")

# Example protocol list (replace later with your chosen DeFi contracts)
protocols = {
    "uniswap": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
    "aave": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
    "curve": "0xD533a949740bb3306d119CC777fa900bA034cd52"
}

data = []

for name, address in protocols.items():
    url = f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={API_KEY}"
    r = requests.get(url)
    json_data = r.json()

    if "result" in json_data and len(json_data["result"]) > 0:
        result = json_data["result"][0]
        if isinstance(result, dict):
            data.append({
                "protocol_name": name,
                "contract_address": address,
                "contract_name": result.get("ContractName", ""),
                "compiler_version": result.get("CompilerVersion", ""),
                "verified": bool(result.get("SourceCode")),
                "source_code_length": len(result.get("SourceCode", "")),
            })
        else:
            print(f"⚠️ Unexpected format for {name}")
    else:
        print(f"⚠️ No result returned for {name}")

# Save results
os.makedirs("data_raw/contracts", exist_ok=True)
df = pd.DataFrame(data)
df.to_csv("data_raw/contracts/verified_contracts.csv", index=False)
print("✅ Saved verified contracts to data_raw/contracts/verified_contracts.csv")